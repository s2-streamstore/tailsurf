//! End-to-end CLI tests against in-process HTTP and WebSocket fixtures.

use std::{
    collections::HashMap,
    process::{Command, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::{
    Router,
    extract::{
        Path, Query, State, WebSocketUpgrade,
        ws::{CloseFrame, Message, WebSocket},
    },
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use bytes::Bytes;
use tailsurf::{
    AppendBatch, ClientWriterId, LinkSecret, MAX_WRITER_IN_FLIGHT_RECORDS, RecordPayload, StreamId,
    StreamKind, TsfClient, TsfClientConfig, TsfClientError, WriterId,
    protocol::{
        read::{ReadOptions, ReadStart, ReadStop},
        rest::{StreamMetadata, Visibility},
        ws::frame::{
            CaughtUpPosition, ClientFrame, MAX_RECORD_PAYLOAD_BYTES, OwnedReadRecord, PartHeader,
            ReadBatch, ServerFrame, TSF_WEBSOCKET_PROTOCOL,
        },
    },
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
    process::Command as TokioCommand,
    sync::watch,
    time::{sleep, timeout},
};
use url::Url;

const TEST_STREAM_LINK: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
const TEST_STREAM_ID: &str = "0123456789abcdefghjkmnpqrstvwxyz";

fn canonical_test_link_secret() -> LinkSecret {
    TEST_STREAM_LINK
        .parse()
        .expect("canonical test link secret")
}

fn canonical_test_stream_id() -> StreamId {
    TEST_STREAM_ID.parse().expect("canonical test stream ID")
}

fn test_stream_link(permissions: &str) -> String {
    format!("http://localhost:3000/s/{TEST_STREAM_ID}#{permissions}={TEST_STREAM_LINK}")
}

#[cfg(unix)]
async fn interrupt_process(pid: u32) {
    let signal = TokioCommand::new("kill")
        .args(["-INT", &pid.to_string()])
        .status()
        .await
        .expect("send SIGINT");
    assert!(signal.success());
}

#[cfg(unix)]
async fn read_interrupt_notice(stderr: &mut (impl tokio::io::AsyncBufRead + Unpin)) -> String {
    let mut notice = String::new();
    timeout(Duration::from_secs(5), stderr.read_line(&mut notice))
        .await
        .expect("timed out waiting for interrupt notice")
        .expect("read interrupt notice");
    notice
}

#[cfg(unix)]
async fn wait_for_tsf(child: &mut tokio::process::Child) -> std::process::ExitStatus {
    timeout(Duration::from_secs(5), child.wait())
        .await
        .expect("timed out waiting for tsf")
        .expect("wait for tsf")
}

#[test]
fn update_refuses_an_unmanaged_executable() {
    let update = Command::new(env!("CARGO_BIN_EXE_tsf"))
        .arg("update")
        .output()
        .expect("tsf update");

    assert!(!update.status.success());
    let error = String::from_utf8(update.stderr).expect("stderr UTF-8");
    assert!(error.contains("not managed by the tail.surf installer"));
    assert!(error.contains("cargo install tailsurf-cli --locked"));
}

#[test]
fn renew_rejects_an_overflowing_expiry() {
    let owner_link = test_stream_link("o");

    let renewed = Command::new(env!("CARGO_BIN_EXE_tsf"))
        .args(["renew", owner_link.as_str(), "18446744073709551615s"])
        .output()
        .expect("tsf renew with overflowing expiry");

    assert!(!renewed.status.success());
    let error = String::from_utf8(renewed.stderr).expect("stderr UTF-8");
    assert!(error.contains("stream expiry is too large"));
    assert!(!error.contains("panicked"));
}

#[cfg(unix)]
#[tokio::test]
async fn second_interrupt_aborts_a_stalled_stdin_write() {
    let server = HoldingWriteServer::start(1).await;
    let write_link = test_stream_link("w");
    let mut command = tsf_command(&server.origin);
    command
        .args(["write", write_link.as_str()])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    let mut child = command.spawn().expect("spawn tsf write");
    let mut stdin = child.stdin.take().expect("stdin");
    let mut stderr = BufReader::new(child.stderr.take().expect("stderr"));

    stdin
        .write_all(b"pending acknowledgement\n")
        .await
        .expect("write stdin");
    server.wait_for_records(1).await;

    let pid = child.id().expect("tsf process ID");
    interrupt_process(pid).await;
    let notice = read_interrupt_notice(&mut stderr).await;
    assert!(
        notice.contains("press Ctrl-C again to stop immediately"),
        "stderr={notice}"
    );
    assert!(
        child.try_wait().expect("check tsf process").is_none(),
        "tsf exited before the second interrupt"
    );

    interrupt_process(pid).await;
    let status = wait_for_tsf(&mut child).await;
    drop(stdin);
    assert_eq!(status.code(), Some(130));
}

#[cfg(unix)]
#[tokio::test]
async fn first_interrupt_does_not_drain_an_unbounded_input_backlog() {
    let server = HoldingWriteServer::start(MAX_WRITER_IN_FLIGHT_RECORDS).await;
    let write_link = test_stream_link("w");
    let mut command = tsf_command(&server.origin);
    command
        .args(["write", write_link.as_str()])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    let mut child = command.spawn().expect("spawn tsf write");
    let mut stdin = child.stdin.take().expect("stdin");
    let mut stderr = BufReader::new(child.stderr.take().expect("stderr"));

    let input_records = MAX_WRITER_IN_FLIGHT_RECORDS * 10;
    let input = "123456789012345\n".repeat(input_records);
    timeout(Duration::from_secs(5), stdin.write_all(input.as_bytes()))
        .await
        .expect("timed out filling stdin")
        .expect("write stdin");
    server.wait_for_records(MAX_WRITER_IN_FLIGHT_RECORDS).await;

    let pid = child.id().expect("tsf process ID");
    interrupt_process(pid).await;
    let notice = read_interrupt_notice(&mut stderr).await;
    assert!(notice.contains("Input stopped"), "stderr={notice}");

    server.release_acknowledgements();
    let status = wait_for_tsf(&mut child).await;
    drop(stdin);
    assert_eq!(status.code(), Some(130));

    let written_records = server.attempts().len();
    assert!(
        written_records <= MAX_WRITER_IN_FLIGHT_RECORDS * 2,
        "interrupt drained {written_records} records"
    );
    assert!(written_records < input_records);
}

#[cfg(unix)]
#[tokio::test]
async fn interrupted_command_unblocks_full_output_readers() {
    let server = HoldingWriteServer::start(MAX_WRITER_IN_FLIGHT_RECORDS).await;
    let write_link = test_stream_link("w");
    let mut command = tsf_command(&server.origin);
    command
        .args(["write", write_link.as_str(), "--", "yes", "123456789012345"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    let mut child = command.spawn().expect("spawn tsf command capture");
    let mut stderr = BufReader::new(child.stderr.take().expect("stderr"));

    server.wait_for_records(MAX_WRITER_IN_FLIGHT_RECORDS).await;
    sleep(Duration::from_millis(100)).await;

    let pid = child.id().expect("tsf process ID");
    interrupt_process(pid).await;
    let notice = read_interrupt_notice(&mut stderr).await;
    assert!(notice.contains("Input stopped"), "stderr={notice}");

    server.release_acknowledgements();
    let status = wait_for_tsf(&mut child).await;
    assert_eq!(status.code(), Some(130));
}

#[tokio::test]
async fn write_reconnect_reuses_client_writer_identity_sequence_and_link_secret() {
    let server = FakeWriteServer::start().await;
    let write_link = test_stream_link("w");

    let output = run_tsf_with_origin(
        server.origin.clone(),
        [
            "write",
            write_link.as_str(),
            "--expected-next-seq-num",
            "12",
        ],
        Some("retry me\n"),
    )
    .await;

    assert!(output.status.success(), "stderr={}", output.stderr);
    let attempts = server.append_attempts();
    assert_eq!(attempts.len(), 2);
    assert_eq!(attempts[0].client_writer_id, attempts[1].client_writer_id);
    assert_eq!(attempts[0].link_secret, TEST_STREAM_LINK);
    assert_eq!(attempts[1].link_secret, TEST_STREAM_LINK);
    assert_eq!(attempts[0].expected_next_seq_num, Some(12));
    assert_eq!(attempts[1].expected_next_seq_num, None);
    assert_eq!(attempts[0].writer_seq_num, 0);
    assert_eq!(attempts[1].writer_seq_num, 0);
    assert_eq!(attempts[0].data.as_ref(), b"retry me");
    assert_eq!(attempts[1].data.as_ref(), b"retry me");
    assert_eq!(attempts[0].part, PartHeader::unsplit());
    assert_eq!(attempts[1].part, PartHeader::unsplit());
}

#[tokio::test]
async fn durable_writer_recovery_outlives_the_bounded_operation_retry_count() {
    let server = FakeWriteServer::start_after_failures(3).await;
    let stream_id = canonical_test_stream_id();
    let mut config = TsfClientConfig::new(server.origin.clone()).expect("valid API origin");
    config.bounded_operation_attempts = 1;
    let writer = TsfClient::with_config(config)
        .expect("valid client config")
        .connect_writer(tailsurf::DurableWriterOptions::new(
            stream_id,
            canonical_test_link_secret(),
        ))
        .await
        .expect("writer");
    let ticket = writer
        .submit(test_write_batch(Bytes::from_static(b"retry me")))
        .expect("submit");

    timeout(Duration::from_secs(5), ticket)
        .await
        .expect("writer recovery timed out")
        .expect("durability acknowledgement");
    writer.close().await.expect("writer close");

    let attempts = server.append_attempts();
    assert_eq!(attempts.len(), 4);
    assert!(
        attempts
            .iter()
            .all(|attempt| attempt.client_writer_id == attempts[0].client_writer_id)
    );
    assert!(attempts.iter().all(|attempt| attempt.writer_seq_num == 0));
    assert!(
        attempts
            .iter()
            .all(|attempt| attempt.data.as_ref() == b"retry me")
    );
}

#[tokio::test]
async fn writer_preserves_its_terminal_failure_for_later_submissions() {
    let server = FakeWriteServer::start_terminal().await;
    let writer = connect_default_writer(&server.origin).await;
    let first = writer
        .submit(test_write_batch(Bytes::from_static(b"first")))
        .expect("submit first record");
    let first_error = first.await.expect_err("first record must fail");
    assert_sequence_mismatch(&first_error);

    let later_error = match writer.submit(test_write_batch(Bytes::from_static(b"later"))) {
        Ok(_) => panic!("terminal writer accepted a later record"),
        Err(error) => error,
    };
    assert_sequence_mismatch(&later_error);

    let close_error = writer
        .close()
        .await
        .expect_err("terminal writer close must fail");
    assert_sequence_mismatch(&close_error);
}

#[tokio::test]
async fn writer_queues_submissions_beyond_the_in_flight_window() {
    assert_writer_window(128, Bytes::from_static(b"x")).await;
    assert_writer_window(10, Bytes::from(vec![0_u8; MAX_RECORD_PAYLOAD_BYTES])).await;
}

async fn assert_writer_window(capacity: usize, payload: Bytes) {
    let server = HoldingWriteServer::start(capacity).await;
    let writer = connect_default_writer(&server.origin).await;
    let mut tickets = Vec::new();
    for _ in 0..capacity {
        tickets.push(
            writer
                .submit(test_write_batch(payload.clone()))
                .expect("queue submission"),
        );
    }
    let queued = writer
        .submit(test_write_batch(Bytes::from_static(b"x")))
        .expect("queue beyond the in-flight window");
    server.wait_for_records(capacity).await;

    server.release_acknowledgements();
    for ticket in tickets {
        ticket.await.expect("durability acknowledgement");
    }
    queued.await.expect("queued acknowledgement");
    writer.close().await.expect("writer close");
}

#[tokio::test]
async fn writer_reconnect_resends_only_the_unacknowledged_tail() {
    // The first connection acknowledges record 0 of a three-record batch and then drops the
    // socket. The reconnect must resend only the unacknowledged tail, and the ticket must
    // retain the receipt earned on the first connection.
    let server = HoldingWriteServer::start_partially_acknowledging(3).await;
    let writer = connect_default_writer(&server.origin).await;
    let batch = AppendBatch::from_records(
        (0..3)
            .map(|index| {
                RecordPayload::new(
                    PartHeader::unsplit(),
                    Bytes::from(format!("record-{index}")),
                )
            })
            .collect(),
    )
    .expect("batch");

    let ticket = writer.submit(batch).expect("submit");
    let receipts = ticket.await.expect("acknowledgements across reconnect");
    assert_eq!(
        receipts
            .iter()
            .map(|receipt| receipt.writer_seq_num)
            .collect::<Vec<_>>(),
        [0, 1, 2]
    );
    server.wait_for_records(5).await;

    let attempts = server.attempts();
    let connection_seq_nums = |connection_index: usize| {
        attempts
            .iter()
            .filter(|attempt| attempt.connection_index == connection_index)
            .map(|attempt| attempt.writer_seq_num)
            .collect::<Vec<_>>()
    };
    assert_eq!(connection_seq_nums(0), [0, 1, 2]);
    assert_eq!(
        connection_seq_nums(1),
        [1, 2],
        "reconnect must resend only records still missing an acknowledgement"
    );
    assert!(
        attempts
            .iter()
            .all(|attempt| attempt.client_writer_id == attempts[0].client_writer_id)
    );

    writer.close().await.expect("writer close");
}

#[tokio::test]
async fn writer_paces_an_oversized_batch_under_the_in_flight_window() {
    // One 6 MiB batch (12 x 512 KiB) exceeds the server's 5 MiB sent-but-unacknowledged socket
    // window, so the writer must stop after ten records until acknowledgements arrive.
    let server = HoldingWriteServer::start(10).await;
    let writer = connect_default_writer(&server.origin).await;
    let payload = Bytes::from(vec![7_u8; MAX_RECORD_PAYLOAD_BYTES]);
    let batch = AppendBatch::from_records(
        (0..12)
            .map(|_| RecordPayload::new(PartHeader::unsplit(), payload.clone()))
            .collect(),
    )
    .expect("oversized batch");

    let ticket = writer.submit(batch).expect("batch queued");

    server.wait_for_records(10).await;
    server.release_acknowledgements();
    let receipts = ticket.await.expect("batch durability");
    assert_eq!(receipts.len(), 12);
    assert_eq!(
        receipts
            .iter()
            .map(|receipt| receipt.writer_seq_num)
            .collect::<Vec<_>>(),
        (0..12).collect::<Vec<_>>()
    );
    server.wait_for_records(12).await;
    assert_eq!(
        server
            .attempts()
            .iter()
            .map(|attempt| attempt.writer_seq_num)
            .collect::<Vec<_>>(),
        (0..12).collect::<Vec<_>>()
    );
    // The server kept reading while acknowledgements were withheld, so a record sent ahead of
    // them could not have sat unread in the socket: the overrun flag observes it reliably.
    assert!(
        !server.overrun(),
        "writer must not send past the in-flight window before acknowledgements"
    );
    writer.close().await.expect("writer close");
}

#[tokio::test]
async fn continuous_submissions_cannot_starve_the_acknowledgement_deadline() {
    // Regression: the acknowledgement deadline is absolute, armed when records go on the wire
    // and reset only by a valid ack or a reconnect. A backlog larger than the in-flight window
    // keeps submission commands flowing to the actor; that traffic must not postpone the
    // deadline, so a silent server still triggers a reconnect.
    let server = HoldingWriteServer::start(MAX_WRITER_IN_FLIGHT_RECORDS).await;
    let stream_id = canonical_test_stream_id();
    let mut config = TsfClientConfig::new(server.origin.clone()).expect("valid API origin");
    config.websocket_progress_timeout = Duration::from_millis(100);
    let writer = TsfClient::with_config(config)
        .expect("valid client config")
        .connect_writer(tailsurf::DurableWriterOptions::new(
            stream_id,
            canonical_test_link_secret(),
        ))
        .await
        .expect("writer");

    let producer = writer.producer();
    let submitter = tokio::spawn(async move {
        // Submission intervals well under the operation timeout keep the actor's command
        // channel busy for the whole silent window.
        loop {
            if producer
                .submit(test_write_batch(Bytes::from_static(b"x")))
                .is_err()
            {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    });

    // The server never acknowledges; the deadline must fire within a few timeout lengths no
    // matter how steadily submissions arrive.
    server
        .wait_for_connections(2, Duration::from_millis(10 * 100))
        .await;

    // Let the drained backlog acknowledge so the writer can close cleanly.
    server.release_acknowledgements();
    writer.close().await.expect("writer close");
    submitter.await.expect("submitter task");
}

#[tokio::test]
async fn writer_reconnect_arms_a_fresh_acknowledgement_deadline() {
    // Regression: the acknowledgement deadline belongs to the socket it was armed for. The first
    // connection burns nearly all of it before dropping; the reconnect must be measured by a
    // fresh deadline instead of being finished off by the dead socket's remainder.
    let server =
        StallingWriteServer::start(Duration::from_millis(180), Duration::from_millis(100)).await;
    let stream_id = canonical_test_stream_id();
    let mut config = TsfClientConfig::new(server.origin.clone()).expect("valid API origin");
    config.websocket_progress_timeout = Duration::from_millis(200);
    let writer = TsfClient::with_config(config)
        .expect("valid client config")
        .connect_writer(tailsurf::DurableWriterOptions::new(
            stream_id,
            canonical_test_link_secret(),
        ))
        .await
        .expect("writer");

    let ticket = writer
        .submit(test_write_batch(Bytes::from_static(b"x")))
        .expect("submit");
    let receipts = timeout(Duration::from_secs(5), ticket)
        .await
        .expect("ticket resolves")
        .expect("durability");

    assert_eq!(receipts.len(), 1);
    assert_eq!(
        server.connections(),
        2,
        "the second connection must get a full deadline of its own"
    );
    writer.close().await.expect("writer close");
}

#[tokio::test]
async fn tail_reconnect_after_multi_record_batch_advances_start_and_count() {
    let server = FakeReadServer::start(FakeReadMode::ReconnectAfterBatch).await;
    let read_link = test_stream_link("r");

    let output = run_tsf_until_stdout_contains(
        server.origin.clone(),
        ["tail", "--seq", "0", "--count", "10", read_link.as_str()],
        b"four\n",
        Duration::from_secs(5),
    )
    .await;

    assert_eq!(output.stdout, "one\ntwo\nthree\nfour\n");
    assert_eq!(output.stderr, "");
    let attempts = server.read_attempts();
    assert_eq!(attempts.len(), 2);
    assert_eq!(attempts[0].start, ReadStart::SeqNum(0));
    assert_eq!(attempts[0].count, Some(10));
    // The three-record batch moves the resume position past its last sequence and decrements
    // the remaining count by the full batch length.
    assert_eq!(attempts[1].start, ReadStart::SeqNum(3));
    assert_eq!(attempts[1].count, Some(7));
}

#[tokio::test]
async fn bounded_sse_tail_finishes_after_a_multi_record_batch() {
    let server = FakeSseServer::start_with_mode(FakeSseMode::BatchThenClose).await;
    let read_link = test_stream_link("r");

    let output = run_tsf_with_origin(
        server.origin.clone(),
        ["tail", "--sse", "--count", "10", read_link.as_str()],
        None,
    )
    .await;

    assert!(output.status.success(), "stderr={}", output.stderr);
    assert_eq!(output.stdout, "one\ntwo\nthree\n");
    let attempts = server.attempts();
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].last_event_id, None);
    let query = attempts[0].query.as_deref().expect("SSE query");
    let query = Url::parse(&format!("http://localhost/?{query}")).expect("parse captured query");
    assert_eq!(
        query.query_pairs().collect::<HashMap<_, _>>(),
        HashMap::from([
            ("tail_offset".into(), "0".into()),
            ("count".into(), "10".into()),
        ])
    );
}

#[tokio::test]
async fn sse_wait_zero_finishes_at_the_current_tail() {
    let server = FakeSseServer::start().await;
    let stream_id = canonical_test_stream_id();
    let mut options = ReadOptions::new(stream_id);
    options.start = Some(ReadStart::SeqNum(0));
    options.stop = Some(ReadStop {
        wait_seconds: Some(0),
        ..ReadStop::default()
    });
    options.link_secret = Some(canonical_test_link_secret());

    let mut session = TsfClient::with_api_origin(server.origin.clone())
        .expect("valid API origin")
        .connect_sse_reader(options)
        .await
        .expect("finite SSE session");

    assert!(
        session
            .next_batch()
            .await
            .expect("read current tail")
            .is_none()
    );
    let attempts = server.attempts();
    assert_eq!(attempts.len(), 1);
    assert!(
        attempts[0]
            .query
            .as_deref()
            .is_some_and(|query| query.split('&').any(|pair| pair == "wait=0"))
    );
}

#[tokio::test]
async fn tail_since_is_sent_as_a_timestamp_selector() {
    let server = FakeReadServer::start(FakeReadMode::OneRecord).await;
    let read_link = test_stream_link("r");

    let output = run_tsf_until_stdout_contains(
        server.origin.clone(),
        [
            "tail",
            "--since",
            "2026-06-17T17:30:06Z",
            read_link.as_str(),
        ],
        b"first\n",
        Duration::from_secs(5),
    )
    .await;

    assert_eq!(output.stdout, "first\n");
    assert_eq!(output.stderr, "");
    let attempts = server.read_attempts();
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].start, ReadStart::TimestampMs(1_781_717_406_000));
}

#[tokio::test]
async fn default_read_start_reconnect_before_first_record_retries_the_default() {
    let server = FakeReadServer::start(FakeReadMode::ReconnectBeforeFirstDefault).await;
    let stream_id = canonical_test_stream_id();
    let client = TsfClient::with_api_origin(server.origin.clone()).expect("valid API origin");
    let request = ReadOptions::new(stream_id).with_link_secret(canonical_test_link_secret());
    let mut reader = client.connect_reader(request).await.expect("reader");

    let batch = reader
        .next_batch_with_timeout(Duration::from_secs(5))
        .await
        .expect("read batch")
        .expect("batch");
    let record = batch.first();

    assert_eq!(record.seq_num, 20);
    assert_eq!(record.data, b"default");
    let attempts = server.read_attempts();
    assert_eq!(attempts.len(), 2);
    assert_eq!(attempts[0].start, ReadStart::TailOffset(0));
    assert_eq!(attempts[1].start, ReadStart::TailOffset(0));
}

#[tokio::test]
async fn reader_restarts_retries_after_established_idle_connections() {
    let server = FakeReadServer::start(FakeReadMode::ReconnectTwiceThenRecord).await;
    let stream_id = canonical_test_stream_id();
    let mut config = TsfClientConfig::new(server.origin.clone()).expect("valid API origin");
    config.bounded_operation_attempts = 2;
    let client = TsfClient::with_config(config).expect("valid client config");
    let mut request = ReadOptions::new(stream_id).with_link_secret(canonical_test_link_secret());
    request.start = Some(ReadStart::SeqNum(0));
    let mut reader = client.connect_reader(request).await.expect("reader");

    let batch = timeout(Duration::from_secs(2), reader.next_batch())
        .await
        .expect("bounded reconnects")
        .expect("recovered read")
        .expect("batch after reconnects");
    let record = batch.first();

    assert_eq!(record.seq_num, 0);
    assert_eq!(record.data, b"recovered");
    assert_eq!(server.read_attempts().len(), 3);
}

#[tokio::test]
async fn explicit_read_timeout_covers_reconnect_cycles() {
    let server = FakeReadServer::start(FakeReadMode::SlowReconnectForever).await;
    let stream_id = canonical_test_stream_id();
    let mut config = TsfClientConfig::new(server.origin.clone()).expect("valid API origin");
    config.bounded_operation_attempts = 100;
    let client = TsfClient::with_config(config).expect("valid client config");
    let mut request = ReadOptions::new(stream_id).with_link_secret(canonical_test_link_secret());
    request.start = Some(ReadStart::SeqNum(0));
    let mut reader = client.connect_reader(request).await.expect("reader");

    let error = timeout(
        Duration::from_secs(1),
        reader.next_batch_with_timeout(Duration::from_millis(500)),
    )
    .await
    .expect("absolute read deadline")
    .expect_err("read timeout");

    assert!(matches!(
        error,
        TsfClientError::Timeout {
            operation: "read stream batch"
        }
    ));
    assert!(server.read_attempts().len() >= 2);
}

#[tokio::test]
async fn reader_resumes_pending_reconnect_after_caller_timeout() {
    let server = FakeReadServer::start(FakeReadMode::ReconnectAfterEmptyCaughtUp).await;
    let stream_id = canonical_test_stream_id();
    let client = TsfClient::with_api_origin(server.origin.clone()).expect("valid API origin");
    let mut request = ReadOptions::new(stream_id).with_link_secret(canonical_test_link_secret());
    request.start = Some(ReadStart::TailOffset(2));
    let mut reader = client.connect_reader(request).await.expect("reader");

    let error = reader
        .next_batch_with_timeout(Duration::from_millis(50))
        .await
        .expect_err("caller timeout during reconnect backoff");
    assert!(matches!(
        error,
        TsfClientError::Timeout {
            operation: "read stream batch"
        }
    ));

    let batch = timeout(Duration::from_secs(2), reader.next_batch())
        .await
        .expect("resumed reconnect")
        .expect("read batch")
        .expect("batch");
    let record = batch.first();
    assert_eq!(record.seq_num, 5);
    assert_eq!(record.data, b"stable");
    let attempts = server.read_attempts();
    assert_eq!(attempts.len(), 2);
    assert_eq!(attempts[0].start, ReadStart::TailOffset(2));
    assert_eq!(attempts[1].start, ReadStart::SeqNum(5));
}

#[tokio::test]
async fn count_zero_reads_complete_without_opening_a_socket() {
    let server = FakeReadServer::start(FakeReadMode::OneRecord).await;
    let read_link = test_stream_link("r");

    let replay = run_tsf_with_origin(
        server.origin.clone(),
        ["replay", "--count", "0", read_link.as_str()],
        None,
    )
    .await;
    assert!(replay.status.success(), "stderr={}", replay.stderr);
    assert_eq!(replay.stdout, "");
    assert!(server.read_attempts().is_empty());
}

#[tokio::test]
async fn tail_rejects_ambiguous_start_selectors_before_connecting() {
    let server = FakeReadServer::start(FakeReadMode::OneRecord).await;
    let read_link = test_stream_link("r");

    let output = run_tsf_with_origin(
        server.origin.clone(),
        ["tail", "--last", "10", "--seq", "5", read_link.as_str()],
        None,
    )
    .await;

    assert!(!output.status.success(), "stdout={}", output.stdout);
    assert!(
        output.stderr.contains("cannot be used with"),
        "stderr={}",
        output.stderr
    );
    assert!(server.read_attempts().is_empty());
}

#[tokio::test]
async fn replay_rejects_split_records_above_the_reassembly_limit() {
    let server = FakeReadServer::start(FakeReadMode::ReplaySplitRecord).await;
    let read_link = test_stream_link("r");

    let output = run_tsf_with_origin(
        server.origin.clone(),
        ["replay", "--max-reassembly-bytes", "4", read_link.as_str()],
        None,
    )
    .await;

    assert!(!output.status.success(), "stdout={}", output.stdout);
    assert!(
        output.stderr.contains("failed to assemble logical record"),
        "stderr={}",
        output.stderr
    );
    assert!(
        output
            .stderr
            .contains("record reassembly would use 5 bytes; maximum is 4"),
        "stderr={}",
        output.stderr
    );
}

#[tokio::test]
async fn replay_preserves_non_utf8_stdout_bytes() {
    let server = FakeReadServer::start(FakeReadMode::ReplayBinary).await;
    let read_link = test_stream_link("r");

    let output =
        run_tsf_bytes_with_origin(server.origin.clone(), ["replay", read_link.as_str()], None)
            .await;

    assert!(output.status.success(), "stderr={:?}", output.stderr);
    assert_eq!(
        output.stdout,
        vec![0x00, 0xff, b'b', b'i', b'n', b'\n', 0xf0, 0x28, 0x8c, 0x28]
    );
    assert_eq!(output.stderr, Vec::<u8>::new());
    let attempts = server.read_attempts();
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].until_timestamp_ms, None);
    assert_eq!(attempts[0].wait_seconds, Some(0));
}

struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct TestServer<S> {
    origin: Url,
    state: Arc<S>,
    _task: AbortOnDrop,
}

impl<S> TestServer<S> {
    async fn serve(state: Arc<S>, router: Router) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("address");
        let task = AbortOnDrop(tokio::spawn(async move {
            axum::serve(listener, router).await.expect("test server");
        }));
        Self {
            origin: Url::parse(&format!("http://{address}")).expect("API URL"),
            state,
            _task: task,
        }
    }
}

struct CommandOutput<T> {
    status: std::process::ExitStatus,
    stdout: T,
    stderr: T,
}

async fn run_tsf_with_origin<const N: usize>(
    origin: Url,
    args: [&str; N],
    stdin: Option<&str>,
) -> CommandOutput<String> {
    let output = run_tsf_bytes_with_origin(origin, args, stdin.map(str::as_bytes)).await;
    CommandOutput {
        status: output.status,
        stdout: String::from_utf8(output.stdout).expect("stdout utf8"),
        stderr: String::from_utf8(output.stderr).expect("stderr utf8"),
    }
}

async fn run_tsf_bytes_with_origin<const N: usize>(
    origin: Url,
    args: [&str; N],
    stdin: Option<&[u8]>,
) -> CommandOutput<Vec<u8>> {
    let mut command = tsf_command(&origin);
    command
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if stdin.is_some() {
        command.stdin(Stdio::piped());
    }

    let mut child = command.spawn().expect("spawn tsf");
    if let Some(input) = stdin {
        let mut child_stdin = child.stdin.take().expect("tsf stdin");
        child_stdin.write_all(input).await.expect("write tsf stdin");
        child_stdin.shutdown().await.expect("close tsf stdin");
    }
    let output = timeout(Duration::from_secs(15), child.wait_with_output())
        .await
        .expect("timed out waiting for tsf")
        .expect("tsf output");
    CommandOutput {
        status: output.status,
        stdout: output.stdout,
        stderr: output.stderr,
    }
}

async fn run_tsf_until_stdout_contains<const N: usize>(
    origin: Url,
    args: [&str; N],
    needle: &[u8],
    wait_for: Duration,
) -> CommandOutput<String> {
    let mut command = tsf_command(&origin);
    command
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn().expect("spawn tsf");
    let mut stdout = child.stdout.take().expect("stdout");
    let mut stderr = child.stderr.take().expect("stderr");
    let needle = needle.to_vec();
    let stdout_task = tokio::spawn(async move {
        let mut output = Vec::new();
        let mut byte = [0_u8; 1];
        loop {
            let read = stdout.read(&mut byte).await.expect("read stdout");
            if read == 0 {
                return (output, false);
            }
            output.extend_from_slice(&byte[..read]);
            if output.windows(needle.len()).any(|window| window == needle) {
                return (output, true);
            }
        }
    });
    let stderr_task = tokio::spawn(async move {
        let mut output = Vec::new();
        stderr.read_to_end(&mut output).await.expect("read stderr");
        output
    });

    let (stdout, found) = match timeout(wait_for, stdout_task).await {
        Ok(result) => result.expect("stdout task"),
        Err(_) => {
            child.kill().await.expect("kill timed out tsf");
            let status = child.wait().await.expect("wait for timed out tsf");
            let stdout = Vec::new();
            let stderr = timeout(Duration::from_secs(1), stderr_task)
                .await
                .expect("timed out waiting for stderr")
                .expect("stderr task");
            panic!(
                "timed out waiting for stdout; status={status}; stdout={}; stderr={}",
                String::from_utf8_lossy(&stdout),
                String::from_utf8_lossy(&stderr)
            );
        }
    };
    if !found {
        let status = child.wait().await.expect("wait for exited tsf");
        let stderr = timeout(Duration::from_secs(1), stderr_task)
            .await
            .expect("timed out waiting for stderr")
            .expect("stderr task");
        panic!(
            "process exited before stdout contained expected bytes; status={status}; stdout={}; stderr={}",
            String::from_utf8_lossy(&stdout),
            String::from_utf8_lossy(&stderr)
        );
    }
    child.kill().await.expect("kill tsf tail");
    let status = child.wait().await.expect("wait for tsf tail");
    let stderr = timeout(Duration::from_secs(1), stderr_task)
        .await
        .expect("timed out waiting for stderr")
        .expect("stderr task");

    CommandOutput {
        status,
        stdout: String::from_utf8(stdout).expect("stdout utf8"),
        stderr: String::from_utf8(stderr).expect("stderr utf8"),
    }
}

fn tsf_command(origin: &Url) -> TokioCommand {
    let mut command = TokioCommand::new(env!("CARGO_BIN_EXE_tsf"));
    command
        .arg("--origin")
        .arg(origin.as_str())
        .kill_on_drop(true);
    command
}

async fn connect_default_writer(origin: &Url) -> tailsurf::TsfWriter {
    let stream_id = canonical_test_stream_id();
    TsfClient::with_api_origin(origin.clone())
        .expect("valid API origin")
        .connect_writer(tailsurf::DurableWriterOptions::new(
            stream_id,
            canonical_test_link_secret(),
        ))
        .await
        .expect("writer")
}

fn test_write_batch(data: Bytes) -> AppendBatch {
    AppendBatch::single(PartHeader::unsplit(), data).expect("valid batch")
}

fn assert_sequence_mismatch(error: &TsfClientError) {
    let is_mismatch = match error {
        TsfClientError::SequenceMismatch { .. } => true,
        TsfClientError::AppendDurabilityUnknown(inner) => {
            matches!(**inner, TsfClientError::SequenceMismatch { .. })
        }
        _ => false,
    };
    assert!(is_mismatch, "error={error}");
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum HoldingWriteMode {
    /// Withhold every acknowledgement until the test releases them.
    Hold,
    /// Acknowledge only the first record of the first connection, then drop the socket.
    PartialAckReconnect,
}

struct HoldingWriteState {
    mode: HoldingWriteMode,
    expected_before_ack: usize,
    attempts: Mutex<Vec<HoldingWriteAttempt>>,
    connections: Mutex<usize>,
    /// Sticky release signal: a connection that starts after release must still observe it.
    release: watch::Sender<bool>,
    /// Set when a record arrives past the unacknowledged cap before acknowledgements flow.
    overrun: AtomicBool,
}

#[derive(Clone)]
struct HoldingWriteAttempt {
    connection_index: usize,
    client_writer_id: ClientWriterId,
    writer_seq_num: u64,
}

type HoldingWriteServer = TestServer<HoldingWriteState>;

impl TestServer<HoldingWriteState> {
    async fn start(expected_before_ack: usize) -> Self {
        Self::start_with_mode(expected_before_ack, HoldingWriteMode::Hold).await
    }

    async fn start_partially_acknowledging(expected_before_ack: usize) -> Self {
        Self::start_with_mode(expected_before_ack, HoldingWriteMode::PartialAckReconnect).await
    }

    async fn start_with_mode(expected_before_ack: usize, mode: HoldingWriteMode) -> Self {
        let (release, _) = watch::channel(false);
        let state = Arc::new(HoldingWriteState {
            mode,
            expected_before_ack,
            attempts: Mutex::new(Vec::new()),
            connections: Mutex::new(0),
            release,
            overrun: AtomicBool::new(false),
        });
        let router = Router::new()
            .route(
                "/api/v1/streams/{stream_id}/write",
                get(holding_write_socket),
            )
            .with_state(state.clone());
        Self::serve(state, router).await
    }

    async fn wait_for_records(&self, expected: usize) {
        timeout(Duration::from_secs(5), async {
            loop {
                if self.state.attempts.lock().expect("attempts lock").len() >= expected {
                    return;
                }
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("server received records");
    }

    async fn wait_for_connections(&self, expected: usize, within: Duration) {
        timeout(within, async {
            loop {
                if *self.state.connections.lock().expect("connections lock") >= expected {
                    return;
                }
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("server observed connections");
    }

    fn release_acknowledgements(&self) {
        let _ = self.state.release.send(true);
    }

    fn attempts(&self) -> Vec<HoldingWriteAttempt> {
        self.state.attempts.lock().expect("attempts lock").clone()
    }

    fn overrun(&self) -> bool {
        self.state.overrun.load(Ordering::SeqCst)
    }
}

async fn holding_write_socket(
    State(state): State<Arc<HoldingWriteState>>,
    ws: WebSocketUpgrade,
) -> Response {
    ws.protocols([TSF_WEBSOCKET_PROTOCOL])
        .on_upgrade(move |socket| holding_write_flow(state, socket))
}

async fn holding_write_flow(state: Arc<HoldingWriteState>, mut socket: WebSocket) {
    let Some(Ok(Message::Binary(auth))) = socket.recv().await else {
        return;
    };
    let Ok(ClientFrame::OpenWrite {
        client_writer_id, ..
    }) = ClientFrame::decode_bytes(auth)
    else {
        return;
    };
    // Subscribe before the connection becomes visible so a release can never be missed.
    let mut release = state.release.subscribe();
    let connection_index = {
        let mut connections = state.connections.lock().expect("connections lock");
        let connection_index = *connections;
        *connections += 1;
        connection_index
    };
    if send_server_frame(&mut socket, ServerFrame::Ready(StreamKind::Transcript))
        .await
        .is_err()
    {
        return;
    }

    let first_connection = connection_index == 0;

    // Reads never stop at the unacknowledged cap: a writer sending past the window before
    // acknowledgements must be observed here, not left unread in the socket buffer.
    let mut received = 0_usize;
    let mut first_record_acked = false;
    loop {
        let stop = match state.mode {
            HoldingWriteMode::Hold => *release.borrow_and_update(),
            // Only the first connection reads a full batch before the drop; later connections
            // skip ahead and acknowledge each resent frame as it arrives below.
            HoldingWriteMode::PartialAckReconnect => {
                !first_connection || received >= state.expected_before_ack
            }
        };
        if stop {
            break;
        }
        tokio::select! {
            frame = socket.recv() => {
                let Some(Ok(Message::Binary(append))) = frame else {
                    return;
                };
                let Ok(ClientFrame::AppendBatch(records)) = ClientFrame::decode_bytes(append)
                else {
                    return;
                };
                if records.is_empty() {
                    return;
                }
                received += records.len();
                if received > state.expected_before_ack {
                    state.overrun.store(true, Ordering::SeqCst);
                }
                let first_writer_seq_num = records[0].writer_seq_num;
                state
                    .attempts
                    .lock()
                    .expect("attempts lock")
                    .extend(records.into_iter().map(|record| HoldingWriteAttempt {
                        connection_index,
                        client_writer_id,
                        writer_seq_num: record.writer_seq_num,
                    }));
                if state.mode == HoldingWriteMode::PartialAckReconnect
                    && connection_index == 0
                    && !first_record_acked
                {
                    first_record_acked = true;
                    if send_test_ack(&mut socket, first_writer_seq_num, first_writer_seq_num)
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
            _ = release.changed(), if state.mode == HoldingWriteMode::Hold => {}
        }
    }

    if first_connection && state.mode != HoldingWriteMode::Hold {
        // Drop the socket without further acknowledgements; the writer must reconnect and
        // resend whatever was not acknowledged.
        let _ = socket.send(Message::Close(None)).await;
        return;
    }
    if state.mode != HoldingWriteMode::PartialAckReconnect && received > 0 {
        let last = u64::try_from(received - 1).expect("ack range");
        if send_test_ack(&mut socket, 0, last).await.is_err() {
            return;
        }
    }

    while let Some(Ok(Message::Binary(append))) = socket.recv().await {
        let Ok(ClientFrame::AppendBatch(records)) = ClientFrame::decode_bytes(append) else {
            return;
        };
        let Some(start) = records.first().map(|record| record.writer_seq_num) else {
            return;
        };
        let Some(end) = records.last().map(|record| record.writer_seq_num) else {
            return;
        };
        state
            .attempts
            .lock()
            .expect("attempts lock")
            .extend(records.into_iter().map(|record| HoldingWriteAttempt {
                connection_index,
                client_writer_id,
                writer_seq_num: record.writer_seq_num,
            }));
        if send_test_ack(&mut socket, start, end).await.is_err() {
            return;
        }
    }
}

async fn send_test_ack(socket: &mut WebSocket, start: u64, end: u64) -> Result<(), axum::Error> {
    send_server_frame(
        socket,
        ServerFrame::AppendAck {
            writer_start_seq_num: start,
            writer_end_seq_num: end + 1,
            start_seq_num: start,
            end_seq_num: end + 1,
        },
    )
    .await
}

/// Write socket whose first connection stalls and then drops without acknowledging; every later
/// connection acknowledges each frame after a delay.
struct StallingWriteState {
    stall: Duration,
    ack_delay: Duration,
    connections: AtomicUsize,
}

type StallingWriteServer = TestServer<StallingWriteState>;

impl TestServer<StallingWriteState> {
    async fn start(stall: Duration, ack_delay: Duration) -> Self {
        let state = Arc::new(StallingWriteState {
            stall,
            ack_delay,
            connections: AtomicUsize::new(0),
        });
        let router = Router::new()
            .route(
                "/api/v1/streams/{stream_id}/write",
                get(stalling_write_socket),
            )
            .with_state(state.clone());
        Self::serve(state, router).await
    }

    fn connections(&self) -> usize {
        self.state.connections.load(Ordering::SeqCst)
    }
}

async fn stalling_write_socket(
    State(state): State<Arc<StallingWriteState>>,
    ws: WebSocketUpgrade,
) -> Response {
    ws.protocols([TSF_WEBSOCKET_PROTOCOL])
        .on_upgrade(move |socket| stalling_write_flow(state, socket))
}

async fn stalling_write_flow(state: Arc<StallingWriteState>, mut socket: WebSocket) {
    let Some(Ok(Message::Binary(_open))) = socket.recv().await else {
        return;
    };
    let first_connection = state.connections.fetch_add(1, Ordering::SeqCst) == 0;
    if send_server_frame(&mut socket, ServerFrame::Ready(StreamKind::Transcript))
        .await
        .is_err()
    {
        return;
    }

    while let Some(Ok(Message::Binary(append))) = socket.recv().await {
        let Ok(ClientFrame::AppendBatch(records)) = ClientFrame::decode_bytes(append) else {
            return;
        };
        let Some(start) = records.first().map(|record| record.writer_seq_num) else {
            return;
        };
        let Some(end) = records.last().map(|record| record.writer_seq_num) else {
            return;
        };
        if first_connection {
            sleep(state.stall).await;
            let _ = socket.send(Message::Close(None)).await;
            return;
        }
        sleep(state.ack_delay).await;
        if send_test_ack(&mut socket, start, end).await.is_err() {
            return;
        }
    }
}

async fn send_server_frame(socket: &mut WebSocket, frame: ServerFrame) -> Result<(), axum::Error> {
    socket
        .send(Message::Binary(
            frame.encode().expect("encode server frame"),
        ))
        .await
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AppendAttempt {
    client_writer_id: ClientWriterId,
    link_secret: String,
    expected_next_seq_num: Option<u64>,
    writer_seq_num: u64,
    part: PartHeader,
    data: Bytes,
}

struct FakeWriteState {
    append_attempts: Mutex<Vec<AppendAttempt>>,
    failures_before_ack: usize,
    terminal: bool,
}

type FakeWriteServer = TestServer<FakeWriteState>;

impl TestServer<FakeWriteState> {
    async fn start() -> Self {
        Self::start_with_mode(1, false).await
    }

    async fn start_after_failures(failures_before_ack: usize) -> Self {
        Self::start_with_mode(failures_before_ack, false).await
    }

    async fn start_terminal() -> Self {
        Self::start_with_mode(0, true).await
    }

    async fn start_with_mode(failures_before_ack: usize, terminal: bool) -> Self {
        let state = Arc::new(FakeWriteState {
            append_attempts: Mutex::new(Vec::new()),
            failures_before_ack,
            terminal,
        });
        let router = Router::new()
            .route("/api/v1/streams/{stream_id}/write", get(fake_write_socket))
            .with_state(state.clone());
        Self::serve(state, router).await
    }

    fn append_attempts(&self) -> Vec<AppendAttempt> {
        self.state
            .append_attempts
            .lock()
            .expect("append attempts lock")
            .clone()
    }
}

async fn fake_write_socket(
    State(state): State<Arc<FakeWriteState>>,
    ws: WebSocketUpgrade,
) -> Response {
    ws.protocols([TSF_WEBSOCKET_PROTOCOL])
        .on_upgrade(move |socket| fake_write_flow(state, socket))
}

async fn fake_write_flow(state: Arc<FakeWriteState>, mut socket: WebSocket) {
    let Some(Ok(Message::Binary(auth))) = socket.recv().await else {
        return;
    };
    let ClientFrame::OpenWrite {
        client_writer_id,
        link_secret,
        expected_next_seq_num,
    } = ClientFrame::decode_bytes(auth).expect("auth write")
    else {
        return;
    };
    send_server_frame(&mut socket, ServerFrame::Ready(StreamKind::Transcript))
        .await
        .expect("send ready");

    let Some(Ok(Message::Binary(append))) = socket.recv().await else {
        return;
    };
    let ClientFrame::AppendBatch(mut records) = ClientFrame::decode_bytes(append).expect("append")
    else {
        return;
    };
    if records.len() != 1 {
        return;
    }
    let record = records.remove(0);
    let attempt_count = {
        let mut attempts = state.append_attempts.lock().expect("append attempts lock");
        attempts.push(AppendAttempt {
            client_writer_id,
            link_secret: link_secret.expose_secret().to_owned(),
            expected_next_seq_num,
            writer_seq_num: record.writer_seq_num,
            part: record.part,
            data: record.data,
        });
        attempts.len()
    };

    if state.terminal {
        socket
            .send(Message::Close(Some(CloseFrame {
                code: 1008,
                reason: "sequence_mismatch".into(),
            })))
            .await
            .expect("close terminal writer");
        return;
    }
    if attempt_count <= state.failures_before_ack {
        socket
            .send(Message::Close(Some(CloseFrame {
                code: 1013,
                reason: "retry".into(),
            })))
            .await
            .expect("close retryable writer attempt");
        return;
    }

    send_server_frame(
        &mut socket,
        ServerFrame::AppendAck {
            writer_start_seq_num: record.writer_seq_num,
            writer_end_seq_num: record.writer_seq_num + 1,
            start_seq_num: 0,
            end_seq_num: 1,
        },
    )
    .await
    .expect("send ack");
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SseAttempt {
    query: Option<String>,
    last_event_id: Option<String>,
}

#[derive(Default, Clone, Copy)]
enum FakeSseMode {
    /// Metadata and an empty caught_up, then end of stream.
    #[default]
    CaughtUpThenClose,
    /// Metadata and one three-record read_batch, then end of stream mid-read.
    BatchThenClose,
}

#[derive(Default)]
struct FakeSseState {
    attempts: Mutex<Vec<SseAttempt>>,
    mode: FakeSseMode,
}

type FakeSseServer = TestServer<FakeSseState>;

impl TestServer<FakeSseState> {
    async fn start() -> Self {
        Self::start_with_mode(FakeSseMode::default()).await
    }

    async fn start_with_mode(mode: FakeSseMode) -> Self {
        let state = Arc::new(FakeSseState {
            mode,
            ..FakeSseState::default()
        });
        let router = Router::new()
            .route("/api/v1/streams/{stream_id}/records", get(fake_sse_read))
            .with_state(state.clone());
        Self::serve(state, router).await
    }

    fn attempts(&self) -> Vec<SseAttempt> {
        self.state
            .attempts
            .lock()
            .expect("SSE attempts lock")
            .clone()
    }
}

async fn fake_sse_read(
    State(state): State<Arc<FakeSseState>>,
    Path(stream_id): Path<String>,
    request: axum::extract::Request,
) -> Response {
    let attempt_count = {
        let mut attempts = state.attempts.lock().expect("SSE attempts lock");
        attempts.push(SseAttempt {
            query: request.uri().query().map(str::to_owned),
            last_event_id: request
                .headers()
                .get("last-event-id")
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned),
        });
        attempts.len()
    };
    if attempt_count > 1 {
        return StatusCode::NO_CONTENT.into_response();
    }
    let cursor = "v1,0,0";
    let record = |seq_num: u64, value: &str| {
        format!(
            "{{\"seq_num\":\"{seq_num}\",\"timestamp_ms\":\"1781717406000\",\"writer\":{{\"id\":\"AAAAAAAAAAAAAAAAAAAAAA\",\"seq_num\":\"{seq_num}\"}},\"text\":\"{value}\"}}"
        )
    };
    let events = match state.mode {
        FakeSseMode::CaughtUpThenClose => format!(
            "id: {cursor}\nevent: caught_up\ndata: {{\"next_seq_num\":\"0\",\"last_timestamp_ms\":\"0\"}}\n\n"
        ),
        // A three-record batch with a resume cursor past its last sequence, then EOF: the
        // client must reconnect with this cursor.
        FakeSseMode::BatchThenClose => format!(
            "id: v1,3,3\nevent: read_batch\ndata: {{\"records\":[{},{},{}]}}\n\n",
            record(0, "one"),
            record(1, "two"),
            record(2, "three"),
        ),
    };
    let body = format!(
        "event: stream_metadata\ndata: {{\"stream_id\":\"{stream_id}\",\"kind\":\"transcript\",\"title\":null,\"visibility\":\"private\",\"created_at\":\"2026-08-13T00:00:00Z\",\"expires_at\":\"2026-08-23T00:00:00Z\"}}\n\n{events}"
    );
    (
        StatusCode::OK,
        [("content-type", "text/event-stream")],
        body,
    )
        .into_response()
}

#[derive(Clone, Debug, PartialEq)]
struct ReadAttempt {
    link_secret: String,
    start: ReadStart,
    count: Option<u64>,
    until_timestamp_ms: Option<u64>,
    rate: Option<f64>,
    wait_seconds: Option<u32>,
}

#[derive(Clone, Debug, Default, serde::Deserialize)]
struct TestReadQuery {
    seq_num: Option<u64>,
    timestamp: Option<u64>,
    tail_offset: Option<u64>,
    count: Option<u64>,
    until: Option<u64>,
    rate: Option<f64>,
    wait: Option<u32>,
}

impl TestReadQuery {
    fn start(&self) -> ReadStart {
        if let Some(value) = self.seq_num {
            ReadStart::SeqNum(value)
        } else if let Some(value) = self.timestamp {
            ReadStart::TimestampMs(value)
        } else {
            ReadStart::TailOffset(self.tail_offset.unwrap_or(0))
        }
    }
}

struct FakeReadState {
    read_attempts: Mutex<Vec<ReadAttempt>>,
    mode: FakeReadMode,
}

#[derive(Clone, Copy)]
enum FakeReadMode {
    OneRecord,
    ReconnectAfterBatch,
    ReconnectAfterEmptyCaughtUp,
    ReconnectBeforeFirstDefault,
    ReconnectTwiceThenRecord,
    SlowReconnectForever,
    ReplayBinary,
    ReplaySplitRecord,
}

type FakeReadServer = TestServer<FakeReadState>;

impl TestServer<FakeReadState> {
    async fn start(mode: FakeReadMode) -> Self {
        let state = Arc::new(FakeReadState {
            read_attempts: Mutex::new(Vec::new()),
            mode,
        });
        let router = Router::new()
            .route("/api/v1/streams/{stream_id}/read", get(fake_read_socket))
            .with_state(state.clone());
        Self::serve(state, router).await
    }

    fn read_attempts(&self) -> Vec<ReadAttempt> {
        self.state
            .read_attempts
            .lock()
            .expect("read attempts lock")
            .clone()
    }
}

fn fake_stream_metadata(stream_id: &str, kind: StreamKind) -> StreamMetadata {
    StreamMetadata {
        stream_id: stream_id.parse().expect("fake stream ID"),
        kind,
        title: None,
        visibility: Visibility::Private,
        created_at: "2026-08-13T00:00:00Z".to_owned(),
        expires_at: "2026-08-23T00:00:00Z".to_owned(),
    }
}

async fn fake_read_socket(
    State(state): State<Arc<FakeReadState>>,
    Path(stream_id): Path<String>,
    Query(query): Query<TestReadQuery>,
    ws: WebSocketUpgrade,
) -> Response {
    ws.protocols([TSF_WEBSOCKET_PROTOCOL])
        .on_upgrade(move |socket| fake_read_flow(state, stream_id, query, socket))
}

async fn fake_read_flow(
    state: Arc<FakeReadState>,
    stream_id: String,
    query: TestReadQuery,
    mut socket: WebSocket,
) {
    let Some(Ok(Message::Binary(opening))) = socket.recv().await else {
        return;
    };
    let ClientFrame::OpenRead {
        link_secret: Some(link_secret),
    } = ClientFrame::decode_bytes(opening).expect("open read")
    else {
        return;
    };
    let start = query.start();
    let count = query.count;
    let until_timestamp_ms = query.until;
    let rate = query.rate;
    let wait_seconds = query.wait;
    let attempt_count = {
        let mut attempts = state.read_attempts.lock().expect("read attempts lock");
        attempts.push(ReadAttempt {
            link_secret: link_secret.expose_secret().to_owned(),
            start,
            count,
            until_timestamp_ms,
            rate,
            wait_seconds,
        });
        attempts.len()
    };
    let stream_kind = match state.mode {
        FakeReadMode::ReplayBinary => StreamKind::Bytes,
        _ => StreamKind::Transcript,
    };
    send_server_frame(&mut socket, ServerFrame::Ready(stream_kind))
        .await
        .expect("send ready");
    send_server_frame(
        &mut socket,
        ServerFrame::StreamMetadata(fake_stream_metadata(&stream_id, stream_kind)),
    )
    .await
    .expect("send stream metadata");
    match state.mode {
        FakeReadMode::OneRecord => {
            let first_seq_num = match start {
                ReadStart::SeqNum(value) => value,
                ReadStart::TimestampMs(_) | ReadStart::TailOffset(_) => 0,
            };
            send_read_record(&mut socket, first_seq_num, 0, b"first").await;
        }
        FakeReadMode::ReconnectAfterBatch => {
            if attempt_count == 1 {
                // One frame carrying three records, then a retryable drop: the client must
                // resume after the batch's last sequence with the count reduced by three.
                send_server_frame(
                    &mut socket,
                    ServerFrame::ReadBatch(
                        ReadBatch::try_from_records(
                            [
                                (0, 0, b"one".as_slice()),
                                (1, 1, b"two".as_slice()),
                                (2, 2, b"three".as_slice()),
                            ]
                            .into_iter()
                            .map(|(seq_num, writer_seq_num, data)| OwnedReadRecord {
                                seq_num,
                                timestamp_ms: 1_781_717_406_000 + seq_num,
                                writer_id: WriterId::from_bytes([7; WriterId::BYTE_LEN]),
                                writer_seq_num,
                                part: PartHeader::unsplit(),
                                data: Bytes::copy_from_slice(data),
                            })
                            .collect(),
                        )
                        .expect("test records within batch bounds"),
                    ),
                )
                .await
                .expect("send batch");
                close_retryable_read(&mut socket).await;
            } else {
                send_read_record(&mut socket, 3, 3, b"four").await;
            }
        }
        FakeReadMode::ReconnectAfterEmptyCaughtUp => {
            if attempt_count == 1 {
                send_server_frame(
                    &mut socket,
                    ServerFrame::CaughtUp(CaughtUpPosition {
                        next_seq_num: 5,
                        last_timestamp_ms: 1_781_717_406_010,
                    }),
                )
                .await
                .expect("send empty caught up");
                close_retryable_read(&mut socket).await;
            } else {
                send_read_record(&mut socket, 5, 0, b"stable").await;
            }
        }
        FakeReadMode::ReconnectBeforeFirstDefault => {
            if attempt_count == 1 {
                close_retryable_read(&mut socket).await;
            } else {
                send_read_record(&mut socket, 20, 0, b"default").await;
            }
        }
        FakeReadMode::ReconnectTwiceThenRecord => {
            if attempt_count < 3 {
                close_retryable_read(&mut socket).await;
            } else {
                send_read_record(&mut socket, 0, 0, b"recovered").await;
            }
        }
        FakeReadMode::SlowReconnectForever => {
            sleep(Duration::from_millis(40)).await;
            close_retryable_read(&mut socket).await;
        }
        FakeReadMode::ReplayBinary => {
            send_read_record_with_part(
                &mut socket,
                0,
                0,
                PartHeader::unsplit(),
                &[0x00, 0xff, b'b', b'i', b'n', b'\n'],
            )
            .await;
            send_read_record_with_part(
                &mut socket,
                1,
                1,
                PartHeader::unsplit(),
                &[0xf0, 0x28, 0x8c, 0x28],
            )
            .await;
            socket
                .send(Message::Close(None))
                .await
                .expect("close binary replay socket");
        }
        FakeReadMode::ReplaySplitRecord => {
            send_read_record_with_part(
                &mut socket,
                0,
                0,
                PartHeader::new(0, false).expect("part"),
                b"hel",
            )
            .await;
            send_read_record_with_part(
                &mut socket,
                1,
                1,
                PartHeader::new(1, true).expect("part"),
                b"lo",
            )
            .await;
            socket
                .send(Message::Close(None))
                .await
                .expect("close split replay socket");
        }
    }
}

async fn close_retryable_read(socket: &mut WebSocket) {
    socket
        .send(Message::Close(Some(CloseFrame {
            code: 1013,
            reason: "upstream_unavailable".into(),
        })))
        .await
        .expect("close retryable read");
}

async fn send_read_record(socket: &mut WebSocket, seq_num: u64, writer_seq_num: u64, data: &[u8]) {
    send_read_record_with_part(socket, seq_num, writer_seq_num, PartHeader::unsplit(), data).await
}

async fn send_read_record_with_part(
    socket: &mut WebSocket,
    seq_num: u64,
    writer_seq_num: u64,
    part: PartHeader,
    data: &[u8],
) {
    send_server_frame(
        socket,
        ServerFrame::ReadBatch(
            ReadBatch::try_from_records(vec![OwnedReadRecord {
                seq_num,
                timestamp_ms: 1_781_717_406_000 + seq_num,
                writer_id: WriterId::from_bytes([7; WriterId::BYTE_LEN]),
                writer_seq_num,
                part,
                data: Bytes::copy_from_slice(data),
            }])
            .expect("test record within batch bounds"),
        ),
    )
    .await
    .expect("send read record");
}
