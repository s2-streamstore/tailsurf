//! End-to-end CLI tests against in-process HTTP and WebSocket fixtures.

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt as _;
use std::{
    collections::HashMap,
    fs,
    process::{Command, Stdio},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::{
    Json, Router,
    extract::{
        Path, Query, State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use bytes::Bytes;
use secrecy::ExposeSecret;
use tailsurf::{
    BearerToken, StreamId, TokenId, TokenPermissions, TsfClient, TsfProducerConfig, WriteRecord,
    WriterId,
    protocol::{
        rest::{
            CreateStreamRequest, CreateStreamResponse, IssueTokenRequest, IssueTokenResponse,
            IssuedStreamToken, ListTokensResponse, RequestedRetention, RevokeTokenRequest,
            StreamInfoResponse, StreamTailResponse, StreamTokenStatus, StreamTokenSummary,
            UpdateStreamRequest, Visibility,
        },
        ws::{
            ReadStart, ReadStreamOptions, WriteStreamOptions,
            frame::{
                ClientFrame, MAX_RECORD_BYTES, PartHeader, ReadRecord, ReadTail, RecordFormat,
                ServerFrame, TSF_V3, TSF_WS_PROTOCOL,
            },
        },
    },
    stream_url::StreamLocator,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
    process::Command as TokioCommand,
    sync::Notify,
    time::{sleep, timeout},
};
use url::Url;

const FREE_RETENTION_LIMIT_MESSAGE: &str = "Infinite retention is unavailable for free users.";
const TEST_STREAM_TOKEN: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
const UNKNOWN_STREAM_TOKEN: &str = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBA";

#[test]
fn help_and_version_describe_the_cli() {
    let help = Command::new(env!("CARGO_BIN_EXE_tsf"))
        .arg("--help")
        .output()
        .expect("tsf help");
    assert!(help.status.success());
    let help = String::from_utf8(help.stdout).expect("help UTF-8");
    assert!(help.contains("Create, write, and read tail.surf streams"));
    assert!(help.contains("info        Show current stream metadata"));
    assert!(help.contains("tail        Follow a stream"));
    assert!(help.contains("update      Update an installation managed by the tail.surf installer"));

    let version = Command::new(env!("CARGO_BIN_EXE_tsf"))
        .arg("--version")
        .output()
        .expect("tsf version");
    assert!(version.status.success());
    assert_eq!(
        String::from_utf8(version.stdout).expect("version UTF-8"),
        format!("tsf {}\n", env!("CARGO_PKG_VERSION"))
    );
}

#[test]
fn update_explains_package_manager_ownership_without_installer_receipt() {
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
fn write_help_describes_implicit_creation() {
    let output = Command::new(env!("CARGO_BIN_EXE_tsf"))
        .args(["write", "--help"])
        .output()
        .expect("tsf write --help");
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).expect("help UTF-8");
    assert!(help.contains("Creates a stream when omitted"));
    assert!(!help.contains("--new"));
}

#[test]
fn new_help_describes_mandatory_owner_and_exact_token_files() {
    let output = Command::new(env!("CARGO_BIN_EXE_tsf"))
        .args(["new", "--help"])
        .output()
        .expect("tsf new --help");
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).expect("help UTF-8");
    assert!(help.contains("The owner link is always issued"));
    assert!(help.contains("exact view-only token secret"));
    assert!(help.contains("Requires `--link view`"));
    assert!(help.contains("exact write-only token secret"));
    assert!(help.contains("Requires `--link write`"));
}

#[test]
fn write_rejects_creation_options_with_an_existing_destination() {
    const WRITE_URL: &str = "https://tail.surf/s/0123456789abcdefghjkmnpqrstvwxyz#w=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

    let misplaced_public = Command::new(env!("CARGO_BIN_EXE_tsf"))
        .args(["write", WRITE_URL, "--public"])
        .output()
        .expect("tsf write URL --public");
    assert!(!misplaced_public.status.success());
    assert!(
        String::from_utf8(misplaced_public.stderr)
            .expect("stderr UTF-8")
            .contains("--public cannot be used when writing to an existing stream")
    );

    let misplaced_retention = Command::new(env!("CARGO_BIN_EXE_tsf"))
        .args(["write", WRITE_URL, "--retention", "6h"])
        .output()
        .expect("tsf write URL --retention 6h");
    assert!(!misplaced_retention.status.success());
    assert!(
        String::from_utf8(misplaced_retention.stderr)
            .expect("stderr UTF-8")
            .contains("--retention cannot be used when writing to an existing stream")
    );
}

#[tokio::test]
async fn new_outputs_json_and_token_files() {
    let server = TestServer::start().await;
    let tmp = std::env::temp_dir().join(format!(
        "tsf-cli-test-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    let _ = fs::remove_dir_all(&tmp);
    fs::create_dir_all(&tmp).expect("tmp dir");
    let owner_file = tmp.join("owner.token");
    let read_file = tmp.join("read.token");
    let write_file = tmp.join("write.token");
    #[cfg(unix)]
    {
        fs::write(&owner_file, "old-secret").expect("existing owner token");
        fs::set_permissions(&owner_file, fs::Permissions::from_mode(0o644))
            .expect("existing owner token permissions");
    }

    let output = run_tsf(
        &server,
        [
            "new",
            "--format",
            "json",
            "--link",
            "owner",
            "--link",
            "view",
            "--link",
            "write",
            "--owner-token-file",
            owner_file.to_str().expect("owner path"),
            "--view-token-file",
            read_file.to_str().expect("view path"),
            "--write-token-file",
            write_file.to_str().expect("write path"),
        ],
        None,
    )
    .await;
    assert!(output.status.success(), "stderr={}", output.stderr);
    let json: serde_json::Value = serde_json::from_str(&output.stdout).expect("json output");
    assert!(json["stream_id"].as_str().is_some());
    assert_eq!(json["visibility"], "private");
    assert_eq!(json["retention_secs"], 864_000);
    assert!(json["urls"]["o"].as_str().is_some());
    assert!(json["urls"]["r"].as_str().is_some());
    assert!(json["urls"]["w"].as_str().is_some());
    for (path, permission) in [(&owner_file, "o"), (&read_file, "r"), (&write_file, "w")] {
        let url = json["urls"][permission].as_str().expect("matching URL");
        let locator = StreamLocator::parse(url).expect("matching URL parses");
        let expected = locator.token.expect("matching URL token");
        assert_eq!(
            fs::read_to_string(path).expect("token file"),
            expected.token.expose_secret()
        );
    }
    #[cfg(unix)]
    for path in [&owner_file, &read_file, &write_file] {
        assert_eq!(
            fs::metadata(path)
                .expect("token metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600,
            "{} must only be accessible by its owner",
            path.display()
        );
    }

    fs::remove_dir_all(tmp).expect("cleanup");
    server.abort();
}

#[tokio::test]
async fn new_retries_with_one_canonical_idempotency_key() {
    let server = TestServer::start_with_create_failures(1).await;

    let output = run_tsf(&server, ["new", "--format", "json"], None).await;

    assert!(output.status.success(), "stderr={}", output.stderr);
    let keys = server.create_idempotency_keys();
    assert_eq!(keys.len(), 2);
    let key = keys[0].as_deref().expect("idempotency key");
    assert_eq!(keys[1].as_deref(), Some(key));
    assert_eq!(key.len(), 43);
    assert!(
        key.bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    );

    server.abort();
}

#[tokio::test]
async fn create_stream_suppresses_configured_rest_authorization() {
    let server = TestServer::start().await;
    let client = TsfClient::with_api_base_url_and_rest_bearer_token(
        server.api_url.clone(),
        "configured-account-token",
    );

    client
        .create_stream(&CreateStreamRequest::default())
        .await
        .expect("create stream");

    assert_eq!(server.create_authorizations(), [None]);
    server.abort();
}

#[tokio::test]
async fn new_text_output_covers_visibility_and_explicit_tokens() {
    let server = TestServer::start().await;

    let private = run_tsf(&server, ["new"], None).await;
    assert!(private.status.success(), "stderr={}", private.stderr);
    assert_eq!(
        normalize_created_stream_output(&private.stdout),
        "Created private stream <stream_id>\nRetention: <retention>\n\n  owner <url>\n\nLinks are shown once.\n"
    );
    assert_created_output_urls_parse(&private.stdout, &["o"]);

    let public = run_tsf(&server, ["new", "--public"], None).await;
    assert!(public.status.success(), "stderr={}", public.stderr);
    assert_eq!(
        normalize_created_stream_output(&public.stdout),
        "Created public stream <stream_id>\nRetention: <retention>\n\n  view <url>\n  owner <url>\n\nLinks are shown once.\n"
    );
    assert_created_output_urls_parse(&public.stdout, &["o"]);

    let explicit = run_tsf(
        &server,
        ["new", "--link", "view+write", "--link", "view"],
        None,
    )
    .await;
    assert!(explicit.status.success(), "stderr={}", explicit.stderr);
    assert_eq!(
        normalize_created_stream_output(&explicit.stdout),
        "Created private stream <stream_id>\nRetention: <retention>\n\n  view <url>\n  view+write <url>\n  owner <url>\n\nLinks are shown once.\n"
    );
    assert_created_output_urls_parse(&explicit.stdout, &["o", "rw", "r"]);

    server.abort();
}

#[tokio::test]
async fn new_deduplicates_owner_and_rejects_more_than_three_effective_links() {
    let server = TestServer::start().await;

    let deduplicated = run_tsf(&server, ["new", "--link", "owner", "--link", "view"], None).await;
    assert!(
        deduplicated.status.success(),
        "stderr={}",
        deduplicated.stderr
    );
    assert_created_output_urls_parse(&deduplicated.stdout, &["o", "r"]);

    let too_many = run_tsf(
        &server,
        [
            "new",
            "--link",
            "view",
            "--link",
            "write",
            "--link",
            "view+write",
        ],
        None,
    )
    .await;
    assert!(!too_many.status.success());
    assert!(
        too_many
            .stderr
            .contains("at most 3 initial links may be issued"),
        "stderr={}",
        too_many.stderr
    );
    assert_eq!(server.create_idempotency_keys().len(), 1);

    server.abort();
}

#[tokio::test]
async fn new_token_files_require_the_exact_requested_permission() {
    let server = TestServer::start().await;

    let output = run_tsf(
        &server,
        [
            "new",
            "--link",
            "view+write",
            "--view-token-file",
            "unused.token",
        ],
        None,
    )
    .await;

    assert!(!output.status.success());
    assert!(
        output
            .stderr
            .contains("--view-token-file requires --link view"),
        "stderr={}",
        output.stderr
    );
    assert!(server.create_idempotency_keys().is_empty());

    server.abort();
}

#[tokio::test]
async fn new_and_url_less_write_accept_human_retention_and_surface_free_limits() {
    let server = TestServer::start().await;

    let finite = run_tsf(
        &server,
        ["new", "--retention", "7d", "--format", "json"],
        None,
    )
    .await;
    assert!(finite.status.success(), "stderr={}", finite.stderr);
    let finite_json: serde_json::Value =
        serde_json::from_str(&finite.stdout).expect("finite JSON output");
    assert_eq!(finite_json["retention_secs"], 604_800);

    let write = run_tsf(&server, ["write", "--retention", "6h"], Some("retained\n")).await;
    assert!(write.status.success(), "stderr={}", write.stderr);
    assert!(write.stderr.contains("Retention: 6 hours"));

    let denied = run_tsf(&server, ["new", "--retention", "infinite"], None).await;
    assert!(!denied.status.success());
    assert!(
        denied
            .stderr
            .contains(&format!("free_plan_limit: {FREE_RETENTION_LIMIT_MESSAGE}")),
        "stderr={}",
        denied.stderr
    );

    server.abort();
}

#[tokio::test]
async fn write_without_url_prints_a_public_view_url_on_stdout() {
    let server = TestServer::start().await;
    let output = run_tsf(&server, ["write", "--public"], Some("public\n")).await;

    assert!(output.status.success(), "stderr={}", output.stderr);
    assert_eq!(output.stdout.lines().count(), 1);
    let view_url = Url::parse(output.stdout.trim()).expect("public view URL");
    assert_eq!(
        view_url.origin().ascii_serialization(),
        "http://localhost:3000"
    );
    assert!(view_url.path().starts_with("/s/"));
    assert!(view_url.fragment().is_none());
    assert!(output.stderr.contains("Created public stream"));
    assert!(output.stderr.contains("1 record durable · view "));

    server.abort();
}

#[tokio::test]
async fn write_without_url_then_replay_round_trips_command_output() {
    let server = TestServer::start().await;
    let output = run_tsf(&server, ["write"], Some("hello from cli integration\n")).await;
    assert!(output.status.success(), "stderr={}", output.stderr);
    assert_eq!(output.stdout.lines().count(), 1);
    assert_eq!(
        normalize_created_stream_output(&output.stderr),
        "Created private stream <stream_id>\nRetention: <retention>\n\n  view <url>\n  owner <url>\n\nLinks are shown once.\n<records> durable · view <url>\n"
    );
    assert!(
        output.stderr.contains("1 record durable · view "),
        "stderr={}",
        output.stderr
    );
    let read_url = output.stdout.trim();
    StreamLocator::parse(read_url).expect("valid read URL");

    let replay = run_tsf(&server, ["replay", read_url], None).await;
    assert!(replay.status.success(), "stderr={}", replay.stderr);
    assert_eq!(replay.stdout, "hello from cli integration\n");

    let bounded_tail = run_tsf(
        &server,
        ["tail", "--seq-num", "0", "--count", "1", read_url],
        None,
    )
    .await;
    assert!(
        bounded_tail.status.success(),
        "stderr={}",
        bounded_tail.stderr
    );
    assert_eq!(bounded_tail.stdout, "hello from cli integration\n");

    server.abort();
}

#[tokio::test]
async fn write_without_url_command_streams_output_and_propagates_exit_status() {
    let server = TestServer::start().await;
    let output = run_tsf(
        &server,
        [
            "write",
            "--",
            "sh",
            "-c",
            "printf out; printf err >&2; exit 7",
        ],
        None,
    )
    .await;
    assert_eq!(output.status.code(), Some(7), "stderr={}", output.stderr);
    assert_eq!(output.stdout.lines().count(), 1);
    let read_url = output.stdout.trim();

    let replay = run_tsf(&server, ["replay", read_url], None).await;
    assert!(replay.status.success(), "stderr={}", replay.stderr);
    assert!(replay.stdout.contains("out"), "stdout={}", replay.stdout);
    assert!(replay.stdout.contains("err"), "stdout={}", replay.stdout);

    server.abort();
}

#[tokio::test]
async fn write_defaults_to_lines_and_splits_large_records() {
    let server = TestServer::start().await;
    let mut input = "x".repeat(MAX_RECORD_BYTES + 10);
    input.push('\n');
    input.push_str("tail\n");

    let output = run_tsf(&server, ["write"], Some(input.as_str())).await;
    assert!(output.status.success(), "stderr={}", output.stderr);
    let read_url = output
        .stderr
        .lines()
        .find_map(|line| extract_link_line(line, "view"))
        .expect("read url");
    let locator = StreamLocator::parse(read_url).expect("valid read URL");
    let read_token = locator
        .token_with(TokenPermissions::allows_read)
        .expect("read token");
    let client = TsfClient::with_api_base_url(server.api_url.clone());
    let mut request = ReadStreamOptions::new(locator.stream_id).with_stream_token(read_token);
    request.start = Some(ReadStart::SeqNum(0));
    request.count = Some(3);
    let mut reader = client.connect_reader(request).await.expect("reader");

    let mut records = Vec::new();
    while records.len() < 3 {
        match reader.next_record().await.expect("event") {
            Some(record) => records.push(record),
            None => panic!("reader closed before expected records"),
        }
    }

    assert_eq!(records[0].writer_seq_num, 0);
    assert_eq!(records[0].part, PartHeader::new(0, false).expect("part"));
    assert_eq!(records[0].format, RecordFormat::Transcript);
    assert_eq!(records[0].data.len(), MAX_RECORD_BYTES);
    assert_eq!(records[1].writer_seq_num, 1);
    assert_eq!(records[1].part, PartHeader::new(1, true).expect("part"));
    assert_eq!(records[1].format, RecordFormat::Transcript);
    assert_eq!(records[1].data.len(), 11);
    assert_eq!(records[1].data.last(), Some(&b'\n'));
    assert_eq!(records[2].writer_seq_num, 2);
    assert_eq!(records[2].part, PartHeader::unsplit());
    assert_eq!(records[2].format, RecordFormat::Transcript);
    assert_eq!(records[2].data.as_ref(), b"tail\n");

    server.abort();
}

#[tokio::test]
async fn write_raw_preserves_large_input_across_flush_boundaries() {
    let server = TestServer::start().await;
    let input = "x".repeat(MAX_RECORD_BYTES + 10);

    let output = run_tsf(&server, ["write", "--raw"], Some(input.as_str())).await;
    assert!(output.status.success(), "stderr={}", output.stderr);
    let read_url = output
        .stderr
        .lines()
        .find_map(|line| extract_link_line(line, "view"))
        .expect("read url");
    let locator = StreamLocator::parse(read_url).expect("valid read URL");
    let read_token = locator
        .token_with(TokenPermissions::allows_read)
        .expect("read token");
    let client = TsfClient::with_api_base_url(server.api_url.clone());
    let mut request = ReadStreamOptions::new(locator.stream_id).with_stream_token(read_token);
    request.start = Some(ReadStart::SeqNum(0));
    request.count = Some(16);
    let mut reader = client.connect_reader(request).await.expect("reader");

    let mut records = Vec::new();
    let mut output = Vec::new();
    while output.len() < input.len() {
        match reader.next_record().await.expect("event") {
            Some(record) => {
                output.extend_from_slice(&record.data);
                records.push(record);
            }
            None => panic!("reader closed before expected records"),
        }
    }

    assert_eq!(output, input.as_bytes());
    assert!(
        records
            .iter()
            .all(|record| record.part == PartHeader::unsplit())
    );
    assert!(
        records
            .iter()
            .all(|record| record.format == RecordFormat::Bytes)
    );
    assert!(
        records
            .iter()
            .all(|record| record.data.len() <= MAX_RECORD_BYTES)
    );
    for (index, record) in records.iter().enumerate() {
        assert_eq!(record.writer_seq_num, index as u64);
    }

    server.abort();
}

#[tokio::test]
async fn write_raw_flushes_on_linger() {
    let server = TestServer::start().await;

    let output = run_tsf(
        &server,
        [
            "write",
            "--raw",
            "--",
            "sh",
            "-c",
            "printf a; sleep 0.1; printf b",
        ],
        None,
    )
    .await;
    assert!(output.status.success(), "stderr={}", output.stderr);
    let read_url = output
        .stderr
        .lines()
        .find_map(|line| extract_link_line(line, "view"))
        .expect("read url");
    let locator = StreamLocator::parse(read_url).expect("valid read URL");
    let read_token = locator
        .token_with(TokenPermissions::allows_read)
        .expect("read token");
    let client = TsfClient::with_api_base_url(server.api_url.clone());
    let mut request = ReadStreamOptions::new(locator.stream_id).with_stream_token(read_token);
    request.start = Some(ReadStart::SeqNum(0));
    request.count = Some(2);
    let mut reader = client.connect_reader(request).await.expect("reader");

    let mut data = Vec::new();
    while data.len() < 2 {
        match reader.next_record().await.expect("event") {
            Some(record) => {
                assert_eq!(record.format, RecordFormat::Bytes);
                data.push(record.data);
            }
            None => panic!("reader closed before expected records"),
        }
    }

    assert_eq!(data[0].as_ref(), b"a");
    assert_eq!(data[1].as_ref(), b"b");

    server.abort();
}

#[cfg(unix)]
#[tokio::test]
async fn interrupted_stdin_write_flushes_before_exiting_130() {
    let server = TestServer::start().await;
    let mut command = TokioCommand::new(env!("CARGO_BIN_EXE_tsf"));
    command
        .arg("--api-url")
        .arg(server.api_url.to_string())
        .arg("--web-url")
        .arg("http://localhost:3000")
        .arg("write")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = command.spawn().expect("spawn tsf write");
    let mut stdin = child.stdin.take().expect("stdin");
    let mut stderr = BufReader::new(child.stderr.take().expect("stderr"));
    let mut stderr_output = String::new();
    let read_url = loop {
        let mut line = String::new();
        let read = stderr.read_line(&mut line).await.expect("read created URL");
        assert!(read > 0, "tsf exited before printing a read URL");
        stderr_output.push_str(&line);
        if let Some(url) = extract_link_line(&line, "view") {
            break url.trim_end().to_owned();
        }
    };

    let stream_id = StreamLocator::parse(&read_url)
        .expect("valid read URL")
        .stream_id;
    stdin
        .write_all(b"complete line\npartial line")
        .await
        .expect("write stdin");
    server.wait_for_records(&stream_id, 1).await;
    assert!(
        child.try_wait().expect("check tsf process").is_none(),
        "tsf exited while stdin remained open"
    );
    let pid = child.id().expect("tsf process ID");
    let signal = TokioCommand::new("kill")
        .args(["-INT", &pid.to_string()])
        .status()
        .await
        .expect("send SIGINT");
    assert!(signal.success());

    stderr
        .read_to_string(&mut stderr_output)
        .await
        .expect("read stderr");
    let status = timeout(Duration::from_secs(5), child.wait())
        .await
        .expect("timed out waiting for interrupted tsf")
        .expect("wait for tsf");
    drop(stdin);
    assert_eq!(status.code(), Some(130), "stderr={stderr_output}");

    let replay = run_tsf(&server, ["replay", read_url.as_str()], None).await;
    assert!(replay.status.success(), "stderr={}", replay.stderr);
    assert_eq!(replay.stdout, "complete line\npartial line");
    server.abort();
}

#[tokio::test]
async fn write_reconnect_reuses_writer_identity_and_unacked_sequence() {
    let server = FakeWriteServer::start().await;
    let stream_id = "0123456789abcdefghjkmnpqrstvwxyz"
        .parse::<StreamId>()
        .expect("stream id");
    let write_url = format!("http://localhost:3000/s/{stream_id}#w={TEST_STREAM_TOKEN}");

    let output = run_tsf_with_api_url(
        server.api_url.clone(),
        ["write", write_url.as_str()],
        Some("retry me\n"),
    )
    .await;

    assert!(output.status.success(), "stderr={}", output.stderr);
    let attempts = server.append_attempts();
    assert_eq!(attempts.len(), 2);
    assert_eq!(attempts[0].writer_id, attempts[1].writer_id);
    assert_eq!(attempts[0].bearer_token, TEST_STREAM_TOKEN);
    assert_eq!(attempts[1].bearer_token, TEST_STREAM_TOKEN);
    assert_eq!(attempts[0].writer_seq_num, 0);
    assert_eq!(attempts[1].writer_seq_num, 0);
    assert_eq!(attempts[0].data.as_ref(), b"retry me\n");
    assert_eq!(attempts[1].data.as_ref(), b"retry me\n");
    assert_eq!(attempts[0].part, PartHeader::unsplit());
    assert_eq!(attempts[1].part, PartHeader::unsplit());
    assert_eq!(attempts[0].format, RecordFormat::Transcript);
    assert_eq!(attempts[1].format, RecordFormat::Transcript);

    server.abort();
}

#[tokio::test]
async fn producer_close_is_not_blocked_by_an_unused_reservation() {
    let server = FakeWriteServer::start().await;
    let stream_id = "0123456789abcdefghjkmnpqrstvwxyz"
        .parse::<StreamId>()
        .expect("stream id");
    let client = TsfClient::with_api_base_url(server.api_url.clone());
    let producer = client
        .connect_producer_with_config(
            tailsurf::protocol::ws::WriteStreamOptions::new(
                stream_id,
                WriterId::new_random(),
                "write-secret",
            ),
            TsfProducerConfig {
                max_unacked_bytes: 1,
                max_unacked_records: 1,
                max_reconnect_attempts: 0,
            },
        )
        .await
        .expect("producer");
    let _permit = producer.reserve(1).await.expect("reservation");

    timeout(Duration::from_secs(1), producer.close())
        .await
        .expect("producer close must not wait for reservation")
        .expect("producer close");

    server.abort();
}

#[tokio::test]
async fn default_producer_enforces_record_and_byte_windows() {
    assert_default_producer_window(128, Bytes::from_static(b"x")).await;
    assert_default_producer_window(10, Bytes::from(vec![0_u8; MAX_RECORD_BYTES])).await;
}

async fn assert_default_producer_window(capacity: usize, payload: Bytes) {
    let server = HoldingWriteServer::start(capacity).await;
    let producer = connect_default_producer(&server.api_url).await;
    let record_count = u64::try_from(capacity).expect("window capacity fits u64");
    let mut tickets = Vec::new();
    for writer_seq_num in 0..record_count {
        tickets.push(
            producer
                .submit(test_write_record(writer_seq_num, payload.clone()))
                .await
                .expect("submit within producer window"),
        );
    }
    server.wait_for_records(capacity).await;

    assert!(
        timeout(
            Duration::from_millis(100),
            producer.submit(test_write_record(record_count, Bytes::from_static(b"x"))),
        )
        .await
        .is_err(),
        "submit beyond the producer window must wait for an acknowledgement"
    );

    server.release_acknowledgements();
    for ticket in tickets {
        ticket.await.expect("durability acknowledgement");
    }
    let final_ticket = timeout(
        Duration::from_secs(1),
        producer.submit(test_write_record(record_count, Bytes::from_static(b"x"))),
    )
    .await
    .expect("producer window reopened")
    .expect("final submit");
    final_ticket.await.expect("final acknowledgement");
    producer.close().await.expect("producer close");
    server.abort();
}

#[tokio::test]
async fn producer_reconnect_resends_every_unacknowledged_record_in_order() {
    let server = HoldingWriteServer::start_reconnecting(3).await;
    let producer = connect_default_producer(&server.api_url).await;
    let mut tickets = Vec::new();
    for writer_seq_num in 0..3 {
        tickets.push(
            producer
                .submit(test_write_record(
                    writer_seq_num,
                    Bytes::from(format!("record-{writer_seq_num}")),
                ))
                .await
                .expect("submit"),
        );
    }
    for ticket in tickets {
        ticket.await.expect("acknowledgement after reconnect");
    }
    server.wait_for_records(6).await;

    let attempts = server.attempts();
    assert_eq!(
        attempts
            .iter()
            .map(|attempt| attempt.writer_seq_num)
            .collect::<Vec<_>>(),
        [0, 1, 2, 0, 1, 2]
    );
    assert!(
        attempts
            .iter()
            .all(|attempt| attempt.writer_id == attempts[0].writer_id)
    );
    assert_eq!(attempts[0].data, attempts[3].data);
    assert_eq!(attempts[1].data, attempts[4].data);
    assert_eq!(attempts[2].data, attempts[5].data);

    producer.close().await.expect("producer close");
    server.abort();
}

#[tokio::test]
async fn tail_reconnect_resumes_after_last_s2_sequence() {
    let server = FakeReadServer::start(FakeReadMode::Reconnect).await;
    let stream_id = "0123456789abcdefghjkmnpqrstvwxyz"
        .parse::<StreamId>()
        .expect("stream id");
    let read_url = format!("http://localhost:3000/s/{stream_id}#r={TEST_STREAM_TOKEN}");

    let output = run_tsf_until_stdout_contains(
        server.api_url.clone(),
        ["tail", read_url.as_str()],
        b"first\nsecond\n",
        Duration::from_secs(5),
    )
    .await;

    assert_eq!(output.stdout, "first\nsecond\n");
    assert_eq!(output.stderr, "");
    let attempts = server.read_attempts();
    assert_eq!(attempts.len(), 2);
    assert_eq!(attempts[0].bearer_token, TEST_STREAM_TOKEN);
    assert_eq!(attempts[1].bearer_token, TEST_STREAM_TOKEN);
    assert_eq!(
        attempts[0].query.get("seq_num").map(String::as_str),
        Some("0")
    );
    assert_eq!(attempts[0].query.get("tail_offset"), None);
    assert_eq!(
        attempts[1].query.get("seq_num").map(String::as_str),
        Some("1")
    );
    assert_eq!(attempts[1].query.get("tail_offset"), None);

    server.abort();
}

#[tokio::test]
async fn tail_selector_flags_are_resolved_as_read_query() {
    let tail_offset_server = FakeReadServer::start(FakeReadMode::Reconnect).await;
    let stream_id = "0123456789abcdefghjkmnpqrstvwxyz"
        .parse::<StreamId>()
        .expect("stream id");
    let read_url = format!("http://localhost:3000/s/{stream_id}#r={TEST_STREAM_TOKEN}");

    let tail_offset_output = run_tsf_until_stdout_contains(
        tail_offset_server.api_url.clone(),
        ["tail", "-n", "25", "--count", "7", read_url.as_str()],
        b"first\n",
        Duration::from_secs(5),
    )
    .await;

    assert_eq!(tail_offset_output.stdout, "first\n");
    assert_eq!(tail_offset_output.stderr, "");
    let attempts = tail_offset_server.read_attempts();
    assert_eq!(attempts.len(), 1);
    assert_eq!(
        attempts[0].query.get("seq_num").map(String::as_str),
        Some("0")
    );
    assert_eq!(
        attempts[0].query.get("count").map(String::as_str),
        Some("7")
    );
    assert_eq!(attempts[0].query.get("tail_offset"), None);
    assert_eq!(attempts[0].query.get("timestamp"), None);
    tail_offset_server.abort();

    let seq_server = FakeReadServer::start(FakeReadMode::Reconnect).await;
    let seq_output = run_tsf_until_stdout_contains(
        seq_server.api_url.clone(),
        ["tail", "--seq-num", "42", "--count", "3", read_url.as_str()],
        b"first\n",
        Duration::from_secs(5),
    )
    .await;

    assert_eq!(seq_output.stdout, "first\n");
    assert_eq!(seq_output.stderr, "");
    let attempts = seq_server.read_attempts();
    assert_eq!(attempts.len(), 1);
    assert_eq!(
        attempts[0].query.get("seq_num").map(String::as_str),
        Some("42")
    );
    assert_eq!(
        attempts[0].query.get("count").map(String::as_str),
        Some("3")
    );
    assert_eq!(attempts[0].query.get("tail_offset"), None);
    assert_eq!(attempts[0].query.get("timestamp"), None);
    seq_server.abort();

    let timestamp_server = FakeReadServer::start(FakeReadMode::Reconnect).await;
    let timestamp_output = run_tsf_until_stdout_contains(
        timestamp_server.api_url.clone(),
        ["tail", "--timestamp", "1781717406000", read_url.as_str()],
        b"first\n",
        Duration::from_secs(5),
    )
    .await;

    assert_eq!(timestamp_output.stdout, "first\n");
    assert_eq!(timestamp_output.stderr, "");
    let attempts = timestamp_server.read_attempts();
    assert_eq!(attempts.len(), 1);
    assert_eq!(
        attempts[0].query.get("timestamp").map(String::as_str),
        Some("1781717406000")
    );
    assert_eq!(attempts[0].query.get("tail_offset"), None);
    assert_eq!(attempts[0].query.get("seq_num"), None);
    timestamp_server.abort();
}

#[tokio::test]
async fn tail_offset_reconnect_before_first_record_keeps_the_resolved_position() {
    let server = FakeReadServer::start(FakeReadMode::ReconnectBeforeFirstRecord).await;
    let stream_id = "0123456789abcdefghjkmnpqrstvwxyz"
        .parse::<StreamId>()
        .expect("stream id");
    let read_url = format!("http://localhost:3000/s/{stream_id}#r={TEST_STREAM_TOKEN}");

    let output = run_tsf_until_stdout_contains(
        server.api_url.clone(),
        ["tail", "-n", "2", read_url.as_str()],
        b"stable\n",
        Duration::from_secs(5),
    )
    .await;

    assert_eq!(output.stdout, "stable\n");
    assert_eq!(output.stderr, "");
    let attempts = server.read_attempts();
    assert_eq!(attempts.len(), 2);
    assert!(attempts.iter().all(|attempt| {
        attempt.query.get("seq_num").map(String::as_str) == Some("5")
            && !attempt.query.contains_key("tail_offset")
    }));
    assert_eq!(
        server.tail_bearer_tokens(),
        [Some(TEST_STREAM_TOKEN.to_owned())]
    );

    server.abort();
}

#[tokio::test]
async fn zero_count_reads_complete_without_opening_a_socket() {
    let server = FakeReadServer::start(FakeReadMode::Reconnect).await;
    let stream_id = "0123456789abcdefghjkmnpqrstvwxyz"
        .parse::<StreamId>()
        .expect("stream id");
    let read_url = format!("http://localhost:3000/s/{stream_id}#r={TEST_STREAM_TOKEN}");

    let tail = run_tsf_with_api_url(
        server.api_url.clone(),
        ["tail", "--count", "0", read_url.as_str()],
        None,
    )
    .await;
    assert!(tail.status.success(), "stderr={}", tail.stderr);
    assert_eq!(tail.stdout, "");

    let replay = run_tsf_with_api_url(
        server.api_url.clone(),
        ["replay", "--count", "0", read_url.as_str()],
        None,
    )
    .await;
    assert!(replay.status.success(), "stderr={}", replay.stderr);
    assert_eq!(replay.stdout, "");
    assert!(server.read_attempts().is_empty());
    server.abort();
}

#[tokio::test]
async fn tail_rejects_ambiguous_start_selectors_before_connecting() {
    let server = FakeReadServer::start(FakeReadMode::Reconnect).await;
    let stream_id = "0123456789abcdefghjkmnpqrstvwxyz"
        .parse::<StreamId>()
        .expect("stream id");
    let read_url = format!("http://localhost:3000/s/{stream_id}#r={TEST_STREAM_TOKEN}");

    let output = run_tsf_with_api_url(
        server.api_url.clone(),
        ["tail", "-n", "10", "--seq-num", "5", read_url.as_str()],
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
    server.abort();
}

#[tokio::test]
async fn cli_reports_rest_errors_without_raw_json_body() {
    let server = TestServer::start().await;

    let public = run_tsf(&server, ["new", "--public"], None).await;
    assert!(public.status.success(), "stderr={}", public.stderr);
    let owner_url = public
        .stdout
        .lines()
        .find_map(|line| extract_link_line(line, "owner"))
        .expect("owner URL");
    let bad_owner_url = owner_url
        .split_once("#o=")
        .map(|(prefix, _token)| format!("{prefix}#o={UNKNOWN_STREAM_TOKEN}"))
        .expect("owner fragment");

    let output = run_tsf(&server, ["visibility", &bad_owner_url, "private"], None).await;

    assert!(!output.status.success(), "stdout={}", output.stdout);
    assert!(
        output.stderr.contains("forbidden: owner token required"),
        "stderr={}",
        output.stderr
    );
    assert!(
        !output.stderr.contains(r#""error""#),
        "stderr={}",
        output.stderr
    );
    server.abort();
}

#[tokio::test]
async fn replay_rejects_logical_records_above_configured_limit() {
    let server = FakeReadServer::start(FakeReadMode::ReplaySplitRecord).await;
    let stream_id = "0123456789abcdefghjkmnpqrstvwxyz"
        .parse::<StreamId>()
        .expect("stream id");
    let read_url = format!("http://localhost:3000/s/{stream_id}#r={TEST_STREAM_TOKEN}");

    let output = run_tsf_with_api_url(
        server.api_url.clone(),
        [
            "replay",
            "--max-logical-record-bytes",
            "4",
            read_url.as_str(),
        ],
        None,
    )
    .await;

    assert!(!output.status.success(), "stdout={}", output.stdout);
    assert!(
        output
            .stderr
            .contains("failed to assemble transcript record"),
        "stderr={}",
        output.stderr
    );
    assert!(
        output
            .stderr
            .contains("logical record is 5 bytes; maximum is 4"),
        "stderr={}",
        output.stderr
    );
    server.abort();
}

#[tokio::test]
async fn replay_selector_flags_are_sent_as_bounded_read_query() {
    let stream_id = "0123456789abcdefghjkmnpqrstvwxyz"
        .parse::<StreamId>()
        .expect("stream id");
    let read_url = format!("http://localhost:3000/s/{stream_id}#r={TEST_STREAM_TOKEN}");

    let seq_server = FakeReadServer::start(FakeReadMode::ReplayTranscript).await;
    let seq_output = run_tsf_with_api_url(
        seq_server.api_url.clone(),
        ["replay", "--seq-num", "2", read_url.as_str()],
        None,
    )
    .await;

    assert!(seq_output.status.success(), "stderr={}", seq_output.stderr);
    let attempts = seq_server.read_attempts();
    assert_eq!(attempts.len(), 1);
    assert_eq!(
        attempts[0].query.get("seq_num").map(String::as_str),
        Some("2")
    );
    assert_eq!(
        attempts[0].query.get("until").map(String::as_str),
        Some("3")
    );
    assert_eq!(
        attempts[0].query.get("count").map(String::as_str),
        Some("2")
    );
    assert_eq!(attempts[0].query.get("timestamp"), None);
    seq_server.abort();

    let timestamp_server = FakeReadServer::start(FakeReadMode::ReplayTranscript).await;
    let timestamp_output = run_tsf_with_api_url(
        timestamp_server.api_url.clone(),
        ["replay", "--timestamp", "1781717406000", read_url.as_str()],
        None,
    )
    .await;

    assert!(
        timestamp_output.status.success(),
        "stderr={}",
        timestamp_output.stderr
    );
    let attempts = timestamp_server.read_attempts();
    assert_eq!(attempts.len(), 1);
    assert_eq!(
        attempts[0].query.get("timestamp").map(String::as_str),
        Some("1781717406000")
    );
    assert_eq!(
        attempts[0].query.get("until").map(String::as_str),
        Some("3")
    );
    assert_eq!(attempts[0].query.get("count"), None);
    assert_eq!(attempts[0].query.get("seq_num"), None);
    timestamp_server.abort();

    let count_server = FakeReadServer::start(FakeReadMode::ReplayBinary).await;
    let count_output = run_tsf_bytes_with_api_url(
        count_server.api_url.clone(),
        ["replay", "--count", "1", read_url.as_str()],
    )
    .await;

    assert!(
        count_output.status.success(),
        "stderr={:?}",
        count_output.stderr
    );
    let attempts = count_server.read_attempts();
    assert_eq!(attempts.len(), 1);
    assert_eq!(
        attempts[0].query.get("seq_num").map(String::as_str),
        Some("0")
    );
    assert_eq!(
        attempts[0].query.get("until").map(String::as_str),
        Some("1")
    );
    assert_eq!(
        attempts[0].query.get("count").map(String::as_str),
        Some("1")
    );
    assert_eq!(attempts[0].query.get("timestamp"), None);
    count_server.abort();
}

#[tokio::test]
async fn replay_preserves_non_utf8_stdout_bytes() {
    let server = FakeReadServer::start(FakeReadMode::ReplayBinary).await;
    let stream_id = "0123456789abcdefghjkmnpqrstvwxyz"
        .parse::<StreamId>()
        .expect("stream id");
    let read_url = format!("http://localhost:3000/s/{stream_id}#r={TEST_STREAM_TOKEN}");

    let output =
        run_tsf_bytes_with_api_url(server.api_url.clone(), ["replay", read_url.as_str()]).await;

    assert!(output.status.success(), "stderr={:?}", output.stderr);
    assert_eq!(
        output.stdout,
        vec![0x00, 0xff, b'b', b'i', b'n', b'\n', 0xf0, 0x28, 0x8c, 0x28]
    );
    assert_eq!(output.stderr, Vec::<u8>::new());
    let attempts = server.read_attempts();
    assert_eq!(attempts.len(), 1);
    assert_eq!(
        attempts[0].query.get("until").map(String::as_str),
        Some("1")
    );

    server.abort();
}

#[tokio::test]
async fn owner_commands_manage_visibility_tokens_and_deletion() {
    let server = TestServer::start().await;
    let created = run_tsf(&server, ["new", "--format", "json"], None).await;
    assert!(created.status.success(), "stderr={}", created.stderr);
    let created_json: serde_json::Value =
        serde_json::from_str(&created.stdout).expect("create output");
    let owner_url = created_json["urls"]["o"].as_str().expect("owner URL");

    let info = run_tsf(&server, ["info", owner_url, "--format", "json"], None).await;
    assert!(info.status.success(), "stderr={}", info.stderr);
    let info_json: serde_json::Value =
        serde_json::from_str(&info.stdout).expect("stream info output");
    assert_eq!(info_json["stream_id"], created_json["stream_id"]);
    assert_eq!(info_json["visibility"], "private");
    assert_eq!(info_json["state"], "active");
    assert_eq!(info_json["retention_secs"], 864_000);

    let visibility = run_tsf(
        &server,
        ["visibility", owner_url, "public", "--format", "json"],
        None,
    )
    .await;
    assert!(visibility.status.success(), "stderr={}", visibility.stderr);
    let visibility_json: serde_json::Value =
        serde_json::from_str(&visibility.stdout).expect("visibility output");
    assert_eq!(visibility_json["visibility"], "public");

    let issued = run_tsf(
        &server,
        [
            "link", "issue", owner_url, "--access", "view", "--format", "json",
        ],
        None,
    )
    .await;
    assert!(issued.status.success(), "stderr={}", issued.stderr);
    let issued_json: serde_json::Value =
        serde_json::from_str(&issued.stdout).expect("issue output");
    let issued_url = issued_json["url"].as_str().expect("issued URL");
    StreamLocator::parse(issued_url).expect("issued URL parses");
    let token_id = issued_json["token_id"]
        .as_str()
        .expect("token id")
        .to_owned();

    server.fail_next_token_list();
    let listed = run_tsf(
        &server,
        ["link", "list", owner_url, "--format", "json"],
        None,
    )
    .await;
    assert!(listed.status.success(), "stderr={}", listed.stderr);
    let listed_json: serde_json::Value =
        serde_json::from_str(&listed.stdout).expect("token list output");
    assert_eq!(listed_json["tokens"].as_array().map(Vec::len), Some(2));
    assert_eq!(
        listed_json["tokens"]
            .as_array()
            .and_then(|tokens| tokens.iter().find(|token| token["token_id"] == token_id))
            .map(|token| &token["status"]),
        Some(&serde_json::Value::String("active".to_owned()))
    );

    let revoked = run_tsf(
        &server,
        ["link", "revoke", owner_url, token_id.as_str()],
        None,
    )
    .await;
    assert!(revoked.status.success(), "stderr={}", revoked.stderr);
    assert_eq!(revoked.stdout, "");

    let listed = run_tsf(
        &server,
        ["link", "list", owner_url, "--format", "json"],
        None,
    )
    .await;
    let listed_json: serde_json::Value =
        serde_json::from_str(&listed.stdout).expect("token list output");
    assert_eq!(
        listed_json["tokens"]
            .as_array()
            .and_then(|tokens| tokens.iter().find(|token| token["token_id"] == token_id))
            .map(|token| &token["status"]),
        Some(&serde_json::Value::String("revoked".to_owned()))
    );

    let deleted = run_tsf(&server, ["delete", owner_url], None).await;
    assert!(deleted.status.success(), "stderr={}", deleted.stderr);
    assert_eq!(deleted.stdout, "");

    let after_delete = run_tsf(&server, ["visibility", owner_url, "private"], None).await;
    assert!(
        !after_delete.status.success(),
        "visibility update unexpectedly succeeded after delete"
    );

    server.abort();
}

struct TestServer {
    api_url: Url,
    state: Arc<TestApiState>,
    task: tokio::task::JoinHandle<()>,
}

impl TestServer {
    async fn start() -> Self {
        Self::start_with_create_failures(0).await
    }

    async fn start_with_create_failures(create_failures: usize) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let state = Arc::new(TestApiState {
            create_failures_remaining: Mutex::new(create_failures),
            ..TestApiState::default()
        });
        let router = Router::new()
            .route("/api/v1/streams", post(test_create_stream))
            .route(
                "/api/v1/streams/{stream_id}",
                get(test_get_stream)
                    .patch(test_update_stream)
                    .delete(test_delete_stream),
            )
            .route(
                "/api/v1/streams/{stream_id}/tail",
                get(test_get_stream_tail),
            )
            .route(
                "/api/v1/streams/{stream_id}/tokens",
                get(test_list_tokens)
                    .post(test_issue_token)
                    .delete(test_revoke_token),
            )
            .route("/api/v1/streams/{stream_id}/write", get(test_write_socket))
            .route("/api/v1/streams/{stream_id}/read", get(test_read_socket))
            .with_state(state.clone());
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.expect("server");
        });
        Self {
            api_url: Url::parse(&format!("http://{addr}")).expect("api URL"),
            state,
            task,
        }
    }

    fn fail_next_token_list(&self) {
        *self
            .state
            .token_list_failures_remaining
            .lock()
            .expect("token list failure lock") += 1;
    }

    fn create_idempotency_keys(&self) -> Vec<Option<String>> {
        self.state
            .create_idempotency_keys
            .lock()
            .expect("create idempotency keys lock")
            .clone()
    }

    fn create_authorizations(&self) -> Vec<Option<String>> {
        self.state
            .create_authorizations
            .lock()
            .expect("create authorizations lock")
            .clone()
    }

    async fn wait_for_records(&self, stream_id: &StreamId, expected: usize) {
        let stream_id = stream_id.to_string();
        timeout(Duration::from_secs(5), async {
            loop {
                let observed = self
                    .state
                    .streams
                    .lock()
                    .expect("streams lock")
                    .get(&stream_id)
                    .map_or(0, |stream| stream.records.len());
                if observed >= expected {
                    return;
                }
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("server received records");
    }

    fn abort(self) {
        self.task.abort();
    }
}

#[derive(Default)]
struct TestApiState {
    next_stream: Mutex<u64>,
    next_token: Mutex<u64>,
    create_failures_remaining: Mutex<usize>,
    create_idempotency_keys: Mutex<Vec<Option<String>>>,
    create_authorizations: Mutex<Vec<Option<String>>>,
    token_list_failures_remaining: Mutex<usize>,
    streams: Mutex<HashMap<String, TestStream>>,
}

struct TestStream {
    stream_id: StreamId,
    visibility: Visibility,
    deleted: bool,
    tokens: Vec<TestToken>,
    records: Vec<TestRecord>,
}

#[derive(Clone)]
struct TestToken {
    token_id: TokenId,
    permissions: TokenPermissions,
    token: BearerToken,
    active: bool,
}

#[derive(Clone)]
struct TestRecord {
    s2_seq_num: u64,
    timestamp_ms: u64,
    writer_id: WriterId,
    writer_seq_num: u64,
    part: PartHeader,
    format: RecordFormat,
    data: Bytes,
}

async fn test_create_stream(
    State(state): State<Arc<TestApiState>>,
    headers: HeaderMap,
    Json(request): Json<CreateStreamRequest>,
) -> Response {
    let idempotency_key = headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    state
        .create_idempotency_keys
        .lock()
        .expect("create idempotency keys lock")
        .push(idempotency_key);
    let authorization = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    state
        .create_authorizations
        .lock()
        .expect("create authorizations lock")
        .push(authorization);
    let mut create_failures = state
        .create_failures_remaining
        .lock()
        .expect("create failures lock");
    if *create_failures > 0 {
        *create_failures -= 1;
        return test_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "unavailable",
            "retry create",
        );
    }
    drop(create_failures);

    let retention_secs = match request.retention_secs {
        None => 864_000,
        Some(RequestedRetention::Seconds(seconds)) => seconds,
        Some(RequestedRetention::Infinite) => {
            return test_error(
                StatusCode::FORBIDDEN,
                "free_plan_limit",
                FREE_RETENTION_LIMIT_MESSAGE,
            );
        }
    };
    let stream_id = {
        let mut next_stream = state.next_stream.lock().expect("next stream lock");
        let stream_id = format!("{:032x}", *next_stream)
            .parse::<StreamId>()
            .expect("stream id");
        *next_stream += 1;
        stream_id
    };
    let requested_tokens = request.issue_tokens.unwrap_or_else(|| {
        if request.visibility == Visibility::Public {
            vec![TokenPermissions::owner(), TokenPermissions::write()]
        } else {
            vec![
                TokenPermissions::owner(),
                TokenPermissions::write(),
                TokenPermissions::read(),
            ]
        }
    });
    let tokens = requested_tokens
        .into_iter()
        .map(|permissions| test_issue_stream_token(&state, permissions))
        .collect::<Vec<_>>();
    let response_tokens = tokens
        .iter()
        .map(|token| IssuedStreamToken {
            token_id: token.token_id,
            permissions: token.permissions,
            token: token.token.clone(),
        })
        .collect::<Vec<_>>();
    let mut streams = state.streams.lock().expect("streams lock");
    streams.insert(
        stream_id.to_string(),
        TestStream {
            stream_id,
            visibility: request.visibility,
            deleted: false,
            tokens,
            records: Vec::new(),
        },
    );

    Json(CreateStreamResponse {
        stream_id,
        visibility: request.visibility,
        retention_secs,
        tokens: response_tokens,
    })
    .into_response()
}

async fn test_get_stream(
    State(state): State<Arc<TestApiState>>,
    Path(stream_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let streams = state.streams.lock().expect("streams lock");
    let Some(stream) = streams.get(&stream_id) else {
        return test_error(StatusCode::NOT_FOUND, "not_found", "stream not found");
    };
    if stream.deleted {
        return test_error(StatusCode::CONFLICT, "conflict", "stream is deleted");
    }
    if stream.visibility == Visibility::Private
        && !test_authorized(stream, &headers, TokenPermissions::allows_read)
    {
        return test_error(StatusCode::FORBIDDEN, "forbidden", "read token required");
    }
    Json(test_get_stream_response(stream)).into_response()
}

async fn test_update_stream(
    State(state): State<Arc<TestApiState>>,
    Path(stream_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<UpdateStreamRequest>,
) -> Response {
    let mut streams = state.streams.lock().expect("streams lock");
    let Some(stream) = streams.get_mut(&stream_id) else {
        return test_error(StatusCode::NOT_FOUND, "not_found", "stream not found");
    };
    if stream.deleted {
        return test_error(StatusCode::CONFLICT, "conflict", "stream is deleted");
    }
    if !test_authorized(stream, &headers, TokenPermissions::allows_owner) {
        return test_error(StatusCode::FORBIDDEN, "forbidden", "owner token required");
    }
    if let Some(visibility) = request.visibility {
        stream.visibility = visibility;
    }
    Json(test_get_stream_response(stream)).into_response()
}

async fn test_delete_stream(
    State(state): State<Arc<TestApiState>>,
    Path(stream_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let mut streams = state.streams.lock().expect("streams lock");
    let Some(stream) = streams.get_mut(&stream_id) else {
        return test_error(StatusCode::NOT_FOUND, "not_found", "stream not found");
    };
    if stream.deleted {
        return test_error(StatusCode::CONFLICT, "conflict", "stream is deleted");
    }
    if !test_authorized(stream, &headers, TokenPermissions::allows_owner) {
        return test_error(StatusCode::FORBIDDEN, "forbidden", "owner token required");
    }
    stream.deleted = true;
    StatusCode::NO_CONTENT.into_response()
}

async fn test_get_stream_tail(
    State(state): State<Arc<TestApiState>>,
    Path(stream_id): Path<String>,
) -> Response {
    let streams = state.streams.lock().expect("streams lock");
    let Some(stream) = streams.get(&stream_id) else {
        return test_error(StatusCode::NOT_FOUND, "not_found", "stream not found");
    };
    if stream.deleted {
        return test_error(StatusCode::CONFLICT, "conflict", "stream is deleted");
    }
    Json(StreamTailResponse {
        stream_id: stream.stream_id,
        next_s2_seq_num: stream.records.len() as u64,
        last_timestamp_ms: stream.records.last().map(|_| 1_781_717_406_000),
    })
    .into_response()
}

async fn test_issue_token(
    State(state): State<Arc<TestApiState>>,
    Path(stream_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<IssueTokenRequest>,
) -> Response {
    let mut streams = state.streams.lock().expect("streams lock");
    let Some(stream) = streams.get_mut(&stream_id) else {
        return test_error(StatusCode::NOT_FOUND, "not_found", "stream not found");
    };
    if stream.deleted {
        return test_error(StatusCode::CONFLICT, "conflict", "stream is deleted");
    }
    if !test_authorized(stream, &headers, TokenPermissions::allows_owner) {
        return test_error(StatusCode::FORBIDDEN, "forbidden", "owner token required");
    }
    let token = test_issue_stream_token(&state, request.permissions);
    let response = IssueTokenResponse {
        token_id: token.token_id,
        permissions: token.permissions,
        token: token.token.clone(),
    };
    stream.tokens.push(token);
    Json(response).into_response()
}

async fn test_list_tokens(
    State(state): State<Arc<TestApiState>>,
    Path(stream_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let fail = {
        let mut remaining = state
            .token_list_failures_remaining
            .lock()
            .expect("token list failure lock");
        if *remaining > 0 {
            *remaining -= 1;
            true
        } else {
            false
        }
    };
    if fail {
        return test_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "internal",
            "temporary token inventory failure",
        );
    }
    let streams = state.streams.lock().expect("streams lock");
    let Some(stream) = streams.get(&stream_id) else {
        return test_error(StatusCode::NOT_FOUND, "not_found", "stream not found");
    };
    if stream.deleted {
        return test_error(StatusCode::CONFLICT, "conflict", "stream is deleted");
    }
    if !test_authorized(stream, &headers, TokenPermissions::allows_owner) {
        return test_error(StatusCode::FORBIDDEN, "forbidden", "owner token required");
    }
    Json(ListTokensResponse {
        tokens: stream
            .tokens
            .iter()
            .map(|token| StreamTokenSummary {
                token_id: token.token_id,
                permissions: token.permissions,
                status: if token.active {
                    StreamTokenStatus::Active
                } else {
                    StreamTokenStatus::Revoked
                },
                issued_at: "2026-08-07T12:00:00.000Z".to_owned(),
                expires_at: None,
                revoked_at: (!token.active).then(|| "2026-08-07T12:01:00.000Z".to_owned()),
                is_current: token.active && token.permissions.allows_owner(),
            })
            .collect(),
    })
    .into_response()
}

async fn test_revoke_token(
    State(state): State<Arc<TestApiState>>,
    Path(stream_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<RevokeTokenRequest>,
) -> Response {
    let mut streams = state.streams.lock().expect("streams lock");
    let Some(stream) = streams.get_mut(&stream_id) else {
        return test_error(StatusCode::NOT_FOUND, "not_found", "stream not found");
    };
    if stream.deleted {
        return test_error(StatusCode::CONFLICT, "conflict", "stream is deleted");
    }
    if !test_authorized(stream, &headers, TokenPermissions::allows_owner) {
        return test_error(StatusCode::FORBIDDEN, "forbidden", "owner token required");
    }
    for token in &mut stream.tokens {
        if token.token_id == request.token_id {
            token.active = false;
        }
    }
    StatusCode::NO_CONTENT.into_response()
}

async fn test_write_socket(
    State(state): State<Arc<TestApiState>>,
    Path(stream_id): Path<String>,
    ws: WebSocketUpgrade,
) -> Response {
    ws.protocols([TSF_WS_PROTOCOL])
        .on_upgrade(move |socket| test_write_flow(state, stream_id, socket))
}

async fn test_write_flow(state: Arc<TestApiState>, stream_id: String, mut socket: WebSocket) {
    let Some(Ok(Message::Binary(auth))) = socket.recv().await else {
        return;
    };
    let Ok(ClientFrame::AuthWrite {
        writer_id,
        bearer_token,
    }) = ClientFrame::decode_bytes(auth)
    else {
        return;
    };
    {
        let streams = state.streams.lock().expect("streams lock");
        let Some(stream) = streams.get(&stream_id) else {
            return;
        };
        if stream.deleted
            || !stream.tokens.iter().any(|token| {
                token.active
                    && token.token.expose_secret() == bearer_token.expose_secret()
                    && token.permissions.allows_write()
            })
        {
            return;
        }
    }
    send_server_frame(&mut socket, ServerFrame::Hello { version: TSF_V3 })
        .await
        .expect("send hello");

    while let Some(Ok(Message::Binary(append))) = socket.recv().await {
        let Ok(ClientFrame::AppendRecord {
            writer_seq_num,
            part,
            format,
            data,
        }) = ClientFrame::decode_bytes(append)
        else {
            return;
        };
        let s2_seq_num = {
            let mut streams = state.streams.lock().expect("streams lock");
            let Some(stream) = streams.get_mut(&stream_id) else {
                return;
            };
            let s2_seq_num = stream.records.len() as u64;
            stream.records.push(TestRecord {
                s2_seq_num,
                timestamp_ms: 1_781_717_406_000 + s2_seq_num,
                writer_id,
                writer_seq_num,
                part,
                format,
                data,
            });
            s2_seq_num
        };
        send_server_frame(
            &mut socket,
            ServerFrame::Ack {
                writer_seq_start: writer_seq_num,
                writer_seq_end: writer_seq_num,
                s2_seq_start: s2_seq_num,
                s2_seq_end: s2_seq_num,
            },
        )
        .await
        .expect("send ack");
    }
}

async fn test_read_socket(
    State(state): State<Arc<TestApiState>>,
    Path(stream_id): Path<String>,
    Query(query): Query<HashMap<String, String>>,
    ws: WebSocketUpgrade,
) -> Response {
    ws.protocols([TSF_WS_PROTOCOL])
        .on_upgrade(move |socket| test_read_flow(state, stream_id, query, socket))
}

async fn test_read_flow(
    state: Arc<TestApiState>,
    stream_id: String,
    query: HashMap<String, String>,
    mut socket: WebSocket,
) {
    send_server_frame(&mut socket, ServerFrame::AuthRequired)
        .await
        .expect("send auth required");
    let Some(Ok(Message::Binary(auth))) = socket.recv().await else {
        return;
    };
    let Ok(ClientFrame::AuthRead { bearer_token }) = ClientFrame::decode_bytes(auth) else {
        return;
    };
    let records = {
        let streams = state.streams.lock().expect("streams lock");
        let Some(stream) = streams.get(&stream_id) else {
            return;
        };
        if stream.deleted
            || !stream.tokens.iter().any(|token| {
                token.active
                    && token.token.expose_secret() == bearer_token.expose_secret()
                    && token.permissions.allows_read()
            })
        {
            return;
        }
        test_select_records(stream, &query)
    };
    send_server_frame(&mut socket, ServerFrame::Hello { version: TSF_V3 })
        .await
        .expect("send hello");
    for record in records {
        send_server_frame(
            &mut socket,
            ServerFrame::ReadRecord(ReadRecord {
                s2_seq_num: record.s2_seq_num,
                timestamp_ms: record.timestamp_ms,
                writer_id: record.writer_id,
                writer_seq_num: record.writer_seq_num,
                part: record.part,
                format: record.format,
                data: record.data,
            }),
        )
        .await
        .expect("send record");
    }
    socket
        .send(Message::Close(None))
        .await
        .expect("close read socket");
}

fn test_issue_stream_token(state: &TestApiState, permissions: TokenPermissions) -> TestToken {
    let mut next_token = state.next_token.lock().expect("next token lock");
    let token_id = format!("{:024x}", *next_token)
        .parse::<TokenId>()
        .expect("token id");
    let token = BearerToken::from(format!("{:042}A", *next_token));
    *next_token += 1;
    TestToken {
        token_id,
        permissions,
        token,
        active: true,
    }
}

fn test_get_stream_response(stream: &TestStream) -> StreamInfoResponse {
    StreamInfoResponse {
        stream_id: stream.stream_id,
        basin: "test-basin".to_owned(),
        visibility: stream.visibility,
        state: if stream.deleted { "deleted" } else { "active" }.to_owned(),
        retention_secs: 864_000,
        active_token_count: stream.tokens.iter().filter(|token| token.active).count(),
    }
}

fn test_authorized(
    stream: &TestStream,
    headers: &HeaderMap,
    required: impl Fn(TokenPermissions) -> bool,
) -> bool {
    let Some(bearer_token) = test_bearer_token(headers) else {
        return false;
    };
    stream.tokens.iter().any(|token| {
        token.active && token.token.expose_secret() == bearer_token && required(token.permissions)
    })
}

fn test_bearer_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(axum::http::header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
}

fn test_error(status: StatusCode, code: &str, message: &str) -> Response {
    (
        status,
        Json(serde_json::json!({
            "error": {
                "code": code,
                "message": message
            }
        })),
    )
        .into_response()
}

fn test_select_records(stream: &TestStream, query: &HashMap<String, String>) -> Vec<TestRecord> {
    let mut records = stream.records.clone();
    if let Some(seq_num) = query
        .get("seq_num")
        .and_then(|value| value.parse::<u64>().ok())
    {
        records.retain(|record| record.s2_seq_num >= seq_num);
    } else if let Some(tail_offset) = query
        .get("tail_offset")
        .and_then(|value| value.parse::<usize>().ok())
    {
        let start = records.len().saturating_sub(tail_offset);
        records = records[start..].to_vec();
    }
    if let Some(until) = query
        .get("until")
        .and_then(|value| value.parse::<u64>().ok())
    {
        records.retain(|record| record.s2_seq_num <= until);
    }
    if let Some(count) = query
        .get("count")
        .and_then(|value| value.parse::<usize>().ok())
    {
        records.truncate(count);
    }
    records
}

struct CommandOutput {
    status: std::process::ExitStatus,
    stdout: String,
    stderr: String,
}

async fn run_tsf<const N: usize>(
    server: &TestServer,
    args: [&str; N],
    stdin: Option<&str>,
) -> CommandOutput {
    run_tsf_with_api_url(server.api_url.clone(), args, stdin).await
}

async fn run_tsf_with_api_url<const N: usize>(
    api_url: Url,
    args: [&str; N],
    stdin: Option<&str>,
) -> CommandOutput {
    let args = args.map(str::to_owned).to_vec();
    let output = run_tsf_bytes(api_url, args, stdin.map(|value| value.as_bytes().to_vec())).await;
    CommandOutput {
        status: output.status,
        stdout: String::from_utf8(output.stdout).expect("stdout utf8"),
        stderr: String::from_utf8(output.stderr).expect("stderr utf8"),
    }
}

struct CommandOutputBytes {
    status: std::process::ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

async fn run_tsf_bytes_with_api_url<const N: usize>(
    api_url: Url,
    args: [&str; N],
) -> CommandOutputBytes {
    let args = args.map(str::to_owned).to_vec();
    run_tsf_bytes(api_url, args, None).await
}

async fn run_tsf_bytes(
    api_url: Url,
    args: Vec<String>,
    stdin: Option<Vec<u8>>,
) -> CommandOutputBytes {
    let mut command = TokioCommand::new(env!("CARGO_BIN_EXE_tsf"));
    command
        .arg("--api-url")
        .arg(api_url.to_string())
        .arg("--web-url")
        .arg("http://localhost:3000")
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    if stdin.is_some() {
        command.stdin(Stdio::piped());
    }

    let mut child = command.spawn().expect("spawn tsf");
    if let Some(input) = stdin {
        let mut child_stdin = child.stdin.take().expect("tsf stdin");
        child_stdin
            .write_all(&input)
            .await
            .expect("write tsf stdin");
        child_stdin.shutdown().await.expect("close tsf stdin");
    }
    let output = timeout(Duration::from_secs(15), child.wait_with_output())
        .await
        .expect("timed out waiting for tsf")
        .expect("tsf output");
    CommandOutputBytes {
        status: output.status,
        stdout: output.stdout,
        stderr: output.stderr,
    }
}

async fn run_tsf_until_stdout_contains<const N: usize>(
    api_url: Url,
    args: [&str; N],
    needle: &[u8],
    wait_for: Duration,
) -> CommandOutput {
    let mut command = TokioCommand::new(env!("CARGO_BIN_EXE_tsf"));
    command
        .arg("--api-url")
        .arg(api_url.to_string())
        .arg("--web-url")
        .arg("http://localhost:3000")
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
    assert!(
        found,
        "process exited before stdout contained expected bytes: {}",
        String::from_utf8_lossy(&stdout)
    );
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

fn normalize_created_stream_output(output: &str) -> String {
    output
        .lines()
        .map(|line| {
            if let Some(rest) = line.strip_prefix("Created ") {
                let visibility = rest.split_whitespace().next().unwrap_or_default();
                format!("Created {visibility} stream <stream_id>")
            } else if line.starts_with("Retention:") {
                "Retention: <retention>".to_owned()
            } else if line.starts_with(|c: char| c.is_ascii_digit()) && line.contains(" durable") {
                if line.contains(" · view ") {
                    "<records> durable · view <url>".to_owned()
                } else {
                    "<records> durable".to_owned()
                }
            } else {
                let mut tokens = line.split_whitespace();
                match (tokens.next(), tokens.next()) {
                    (Some(label @ ("view" | "write" | "view+write" | "owner")), Some(url))
                        if url.starts_with("http") =>
                    {
                        format!("  {label} <url>")
                    }
                    _ => line.to_owned(),
                }
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
        + "\n"
}

fn extract_link_line<'a>(line: &'a str, label: &str) -> Option<&'a str> {
    let mut tokens = line.split_whitespace();
    if tokens.next()? != label {
        return None;
    }
    tokens.next().filter(|url| url.starts_with("http"))
}

fn link_label_for(permission: &str) -> &str {
    match permission {
        "o" => "owner",
        "r" => "view",
        "w" => "write",
        "rw" => "view+write",
        other => other,
    }
}

fn assert_created_output_urls_parse(output: &str, expected_permissions: &[&str]) {
    let stream_id = output
        .lines()
        .find_map(|line| line.strip_prefix("Created "))
        .and_then(|rest| rest.split_whitespace().last())
        .expect("created stream line");
    for permission in expected_permissions {
        let label = link_label_for(permission);
        let url = output
            .lines()
            .find_map(|line| extract_link_line(line, label))
            .expect("permission URL line");
        let locator = StreamLocator::parse(url).expect("stream URL parses");
        assert_eq!(locator.stream_id.to_string(), stream_id);
        let permissions = permission
            .parse::<TokenPermissions>()
            .expect("expected permission parses");
        assert!(
            locator
                .token_with(|token_permissions| token_permissions == permissions)
                .is_some(),
            "URL for {permission} did not contain a matching token"
        );
    }
}

async fn connect_default_producer(api_url: &Url) -> tailsurf::TsfProducer {
    let stream_id = "0123456789abcdefghjkmnpqrstvwxyz"
        .parse::<StreamId>()
        .expect("stream id");
    TsfClient::with_api_base_url(api_url.clone())
        .connect_producer(WriteStreamOptions::new(
            stream_id,
            WriterId::new_random(),
            "write-secret",
        ))
        .await
        .expect("producer")
}

fn test_write_record(writer_seq_num: u64, data: Bytes) -> WriteRecord {
    WriteRecord::new(
        writer_seq_num,
        PartHeader::unsplit(),
        RecordFormat::Bytes,
        data,
    )
}

struct HoldingWriteState {
    expected_before_ack: usize,
    attempts: Mutex<Vec<HoldingWriteAttempt>>,
    connections: Mutex<usize>,
    disconnect_first_batch: bool,
    release_acknowledgements: Notify,
}

#[derive(Clone)]
struct HoldingWriteAttempt {
    writer_id: WriterId,
    writer_seq_num: u64,
    data: Bytes,
}

struct HoldingWriteServer {
    api_url: Url,
    state: Arc<HoldingWriteState>,
    task: tokio::task::JoinHandle<()>,
}

impl HoldingWriteServer {
    async fn start(expected_before_ack: usize) -> Self {
        Self::start_with_mode(expected_before_ack, false).await
    }

    async fn start_reconnecting(expected_before_ack: usize) -> Self {
        Self::start_with_mode(expected_before_ack, true).await
    }

    async fn start_with_mode(expected_before_ack: usize, disconnect_first_batch: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let state = Arc::new(HoldingWriteState {
            expected_before_ack,
            attempts: Mutex::new(Vec::new()),
            connections: Mutex::new(0),
            disconnect_first_batch,
            release_acknowledgements: Notify::new(),
        });
        let router = Router::new()
            .route(
                "/api/v1/streams/{stream_id}/write",
                get(holding_write_socket),
            )
            .with_state(state.clone());
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.expect("holding server");
        });
        Self {
            api_url: Url::parse(&format!("http://{addr}")).expect("API URL"),
            state,
            task,
        }
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

    fn release_acknowledgements(&self) {
        self.state.release_acknowledgements.notify_one();
    }

    fn attempts(&self) -> Vec<HoldingWriteAttempt> {
        self.state.attempts.lock().expect("attempts lock").clone()
    }

    fn abort(self) {
        self.task.abort();
    }
}

async fn holding_write_socket(
    State(state): State<Arc<HoldingWriteState>>,
    ws: WebSocketUpgrade,
) -> Response {
    ws.protocols([TSF_WS_PROTOCOL])
        .on_upgrade(move |socket| holding_write_flow(state, socket))
}

async fn holding_write_flow(state: Arc<HoldingWriteState>, mut socket: WebSocket) {
    let Some(Ok(Message::Binary(auth))) = socket.recv().await else {
        return;
    };
    let Ok(ClientFrame::AuthWrite { writer_id, .. }) = ClientFrame::decode_bytes(auth) else {
        return;
    };
    let connection_index = {
        let mut connections = state.connections.lock().expect("connections lock");
        let connection_index = *connections;
        *connections += 1;
        connection_index
    };
    if send_server_frame(&mut socket, ServerFrame::Hello { version: TSF_V3 })
        .await
        .is_err()
    {
        return;
    }

    for _ in 0..state.expected_before_ack {
        let Some(Ok(Message::Binary(append))) = socket.recv().await else {
            return;
        };
        let Ok(ClientFrame::AppendRecord {
            writer_seq_num,
            data,
            ..
        }) = ClientFrame::decode_bytes(append)
        else {
            return;
        };
        state
            .attempts
            .lock()
            .expect("attempts lock")
            .push(HoldingWriteAttempt {
                writer_id,
                writer_seq_num,
                data,
            });
    }

    if state.disconnect_first_batch && connection_index == 0 {
        let _ = socket.send(Message::Close(None)).await;
        return;
    }
    if !state.disconnect_first_batch {
        state.release_acknowledgements.notified().await;
    }
    let last = u64::try_from(state.expected_before_ack - 1).expect("ack range");
    if send_test_ack(&mut socket, 0, last).await.is_err() {
        return;
    }

    while let Some(Ok(Message::Binary(append))) = socket.recv().await {
        let Ok(ClientFrame::AppendRecord {
            writer_seq_num,
            data,
            ..
        }) = ClientFrame::decode_bytes(append)
        else {
            return;
        };
        state
            .attempts
            .lock()
            .expect("attempts lock")
            .push(HoldingWriteAttempt {
                writer_id,
                writer_seq_num,
                data,
            });
        if send_test_ack(&mut socket, writer_seq_num, writer_seq_num)
            .await
            .is_err()
        {
            return;
        }
    }
}

async fn send_test_ack(socket: &mut WebSocket, start: u64, end: u64) -> Result<(), axum::Error> {
    send_server_frame(
        socket,
        ServerFrame::Ack {
            writer_seq_start: start,
            writer_seq_end: end,
            s2_seq_start: start,
            s2_seq_end: end,
        },
    )
    .await
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
    writer_id: WriterId,
    bearer_token: String,
    writer_seq_num: u64,
    part: PartHeader,
    format: RecordFormat,
    data: Bytes,
}

#[derive(Default)]
struct FakeWriteState {
    append_attempts: Mutex<Vec<AppendAttempt>>,
}

struct FakeWriteServer {
    api_url: Url,
    state: Arc<FakeWriteState>,
    task: tokio::task::JoinHandle<()>,
}

impl FakeWriteServer {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let state = Arc::new(FakeWriteState::default());
        let router = Router::new()
            .route("/api/v1/streams/{stream_id}/write", get(fake_write_socket))
            .with_state(state.clone());
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.expect("fake server");
        });
        Self {
            api_url: Url::parse(&format!("http://{addr}")).expect("api URL"),
            state,
            task,
        }
    }

    fn append_attempts(&self) -> Vec<AppendAttempt> {
        self.state
            .append_attempts
            .lock()
            .expect("append attempts lock")
            .clone()
    }

    fn abort(self) {
        self.task.abort();
    }
}

async fn fake_write_socket(
    State(state): State<Arc<FakeWriteState>>,
    ws: WebSocketUpgrade,
) -> Response {
    ws.protocols([TSF_WS_PROTOCOL])
        .on_upgrade(move |socket| fake_write_flow(state, socket))
}

async fn fake_write_flow(state: Arc<FakeWriteState>, mut socket: WebSocket) {
    let Some(Ok(Message::Binary(auth))) = socket.recv().await else {
        return;
    };
    let ClientFrame::AuthWrite {
        writer_id,
        bearer_token,
    } = ClientFrame::decode_bytes(auth).expect("auth write")
    else {
        return;
    };
    send_server_frame(&mut socket, ServerFrame::Hello { version: TSF_V3 })
        .await
        .expect("send hello");

    let Some(Ok(Message::Binary(append))) = socket.recv().await else {
        return;
    };
    let ClientFrame::AppendRecord {
        writer_seq_num,
        part,
        format,
        data,
    } = ClientFrame::decode_bytes(append).expect("append")
    else {
        return;
    };
    let attempt_count = {
        let mut attempts = state.append_attempts.lock().expect("append attempts lock");
        attempts.push(AppendAttempt {
            writer_id,
            bearer_token: bearer_token.expose_secret().to_owned(),
            writer_seq_num,
            part,
            format,
            data,
        });
        attempts.len()
    };

    if attempt_count == 1 {
        socket
            .send(Message::Close(None))
            .await
            .expect("close first attempt");
        return;
    }

    send_server_frame(
        &mut socket,
        ServerFrame::Ack {
            writer_seq_start: writer_seq_num,
            writer_seq_end: writer_seq_num,
            s2_seq_start: 0,
            s2_seq_end: 0,
        },
    )
    .await
    .expect("send ack");
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ReadAttempt {
    bearer_token: String,
    query: HashMap<String, String>,
}

struct FakeReadState {
    read_attempts: Mutex<Vec<ReadAttempt>>,
    tail_bearer_tokens: Mutex<Vec<Option<String>>>,
    mode: FakeReadMode,
}

#[derive(Clone, Copy)]
enum FakeReadMode {
    Reconnect,
    ReconnectBeforeFirstRecord,
    ReplayTranscript,
    ReplayBinary,
    ReplaySplitRecord,
}

struct FakeReadServer {
    api_url: Url,
    state: Arc<FakeReadState>,
    task: tokio::task::JoinHandle<()>,
}

impl FakeReadServer {
    async fn start(mode: FakeReadMode) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let state = Arc::new(FakeReadState {
            read_attempts: Mutex::new(Vec::new()),
            tail_bearer_tokens: Mutex::new(Vec::new()),
            mode,
        });
        let router = Router::new()
            .route("/api/v1/streams/{stream_id}/read", get(fake_read_socket))
            .route("/api/v1/streams/{stream_id}/tail", get(fake_read_tail))
            .with_state(state.clone());
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.expect("fake server");
        });
        Self {
            api_url: Url::parse(&format!("http://{addr}")).expect("api URL"),
            state,
            task,
        }
    }

    fn read_attempts(&self) -> Vec<ReadAttempt> {
        self.state
            .read_attempts
            .lock()
            .expect("read attempts lock")
            .clone()
    }

    fn tail_bearer_tokens(&self) -> Vec<Option<String>> {
        self.state
            .tail_bearer_tokens
            .lock()
            .expect("tail bearer tokens lock")
            .clone()
    }

    fn abort(self) {
        self.task.abort();
    }
}

async fn fake_read_tail(
    State(state): State<Arc<FakeReadState>>,
    Path(stream_id): Path<String>,
    headers: HeaderMap,
) -> Json<serde_json::Value> {
    let bearer_token = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::to_owned);
    state
        .tail_bearer_tokens
        .lock()
        .expect("tail bearer tokens lock")
        .push(bearer_token);
    let next_s2_seq_num = match state.mode {
        FakeReadMode::Reconnect => 0,
        FakeReadMode::ReconnectBeforeFirstRecord => 7,
        FakeReadMode::ReplayTranscript => 4,
        FakeReadMode::ReplayBinary => 2,
        FakeReadMode::ReplaySplitRecord => 2,
    };
    Json(serde_json::json!({
        "stream_id": stream_id,
        "next_s2_seq_num": next_s2_seq_num,
        "last_timestamp_ms": 1781717406000_u64
    }))
}

async fn fake_read_socket(
    State(state): State<Arc<FakeReadState>>,
    Query(query): Query<HashMap<String, String>>,
    ws: WebSocketUpgrade,
) -> Response {
    ws.protocols([TSF_WS_PROTOCOL])
        .on_upgrade(move |socket| fake_read_flow(state, query, socket))
}

async fn fake_read_flow(
    state: Arc<FakeReadState>,
    query: HashMap<String, String>,
    mut socket: WebSocket,
) {
    send_server_frame(&mut socket, ServerFrame::AuthRequired)
        .await
        .expect("send auth required");
    let Some(Ok(Message::Binary(auth))) = socket.recv().await else {
        return;
    };
    let ClientFrame::AuthRead { bearer_token } =
        ClientFrame::decode_bytes(auth).expect("auth read")
    else {
        return;
    };
    let attempt_count = {
        let mut attempts = state.read_attempts.lock().expect("read attempts lock");
        attempts.push(ReadAttempt {
            bearer_token: bearer_token.expose_secret().to_owned(),
            query,
        });
        attempts.len()
    };
    send_server_frame(&mut socket, ServerFrame::Hello { version: TSF_V3 })
        .await
        .expect("send hello");
    send_server_frame(
        &mut socket,
        ServerFrame::ReadTail(ReadTail {
            next_s2_seq_num: 10,
            timestamp_ms: 1_781_717_406_010,
        }),
    )
    .await
    .expect("send read tail");

    match state.mode {
        FakeReadMode::Reconnect => {
            if attempt_count == 1 {
                send_read_record(&mut socket, 0, 0, b"first\n").await;
                send_server_frame(
                    &mut socket,
                    ServerFrame::ReconnectAdvised { deadline_secs: 0 },
                )
                .await
                .expect("send reconnect advised");
            } else {
                send_read_record(&mut socket, 1, 1, b"second\n").await;
            }
        }
        FakeReadMode::ReconnectBeforeFirstRecord => {
            if attempt_count == 1 {
                send_server_frame(
                    &mut socket,
                    ServerFrame::ReconnectAdvised { deadline_secs: 0 },
                )
                .await
                .expect("send reconnect advised");
            } else {
                send_read_record(&mut socket, 5, 0, b"stable\n").await;
            }
        }
        FakeReadMode::ReplayTranscript => {
            send_read_record(&mut socket, 0, 0, b"dedupe\n").await;
            send_read_record(&mut socket, 1, 0, b"dedupe\n").await;
            send_read_record(&mut socket, 2, 1, b"stable\n").await;
            send_read_record(&mut socket, 3, 1, b"changed\n").await;
            socket
                .send(Message::Close(None))
                .await
                .expect("close replay socket");
        }
        FakeReadMode::ReplayBinary => {
            send_read_record_with_format(
                &mut socket,
                0,
                0,
                PartHeader::unsplit(),
                RecordFormat::Bytes,
                &[0x00, 0xff, b'b', b'i', b'n', b'\n'],
            )
            .await;
            send_read_record_with_format(
                &mut socket,
                1,
                1,
                PartHeader::unsplit(),
                RecordFormat::Bytes,
                &[0xf0, 0x28, 0x8c, 0x28],
            )
            .await;
            socket
                .send(Message::Close(None))
                .await
                .expect("close binary replay socket");
        }
        FakeReadMode::ReplaySplitRecord => {
            send_read_record_with_format(
                &mut socket,
                0,
                0,
                PartHeader::new(0, false).expect("part"),
                RecordFormat::Transcript,
                b"hel",
            )
            .await;
            send_read_record_with_format(
                &mut socket,
                1,
                1,
                PartHeader::new(1, true).expect("part"),
                RecordFormat::Transcript,
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

async fn send_read_record(
    socket: &mut WebSocket,
    s2_seq_num: u64,
    writer_seq_num: u64,
    data: &[u8],
) {
    send_read_record_with_format(
        socket,
        s2_seq_num,
        writer_seq_num,
        PartHeader::unsplit(),
        RecordFormat::Transcript,
        data,
    )
    .await
}

async fn send_read_record_with_format(
    socket: &mut WebSocket,
    s2_seq_num: u64,
    writer_seq_num: u64,
    part: PartHeader,
    format: RecordFormat,
    data: &[u8],
) {
    send_server_frame(
        socket,
        ServerFrame::ReadRecord(ReadRecord {
            s2_seq_num,
            timestamp_ms: 1_781_717_406_000 + s2_seq_num,
            writer_id: WriterId::from_bytes([7; WriterId::BYTE_LEN]),
            writer_seq_num,
            part,
            format,
            data: Bytes::copy_from_slice(data),
        }),
    )
    .await
    .expect("send read record");
}
