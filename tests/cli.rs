//! End-to-end CLI tests against in-process HTTP and WebSocket fixtures.

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt as _;
use std::{
    collections::HashMap,
    fs,
    hash::{DefaultHasher, Hash, Hasher},
    process::{Command, Stdio},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::{
    Json, Router,
    extract::{
        Path, Query, State, WebSocketUpgrade,
        ws::{CloseFrame, Message, WebSocket},
    },
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use bytes::Bytes;
use secrecy::ExposeSecret;
use tailsurf::{
    AppendRecord, ClientWriterId, IdempotencyKey, LinkId, LinkPermissions, LinkSecret, RetryPolicy,
    StreamId, StreamTitle, TsfClient, TsfClientConfig, TsfClientError, TsfWriterConfig, WriterId,
    protocol::{
        read::{ReadOptions, ReadStart, ReadStop},
        rest::{
            CreateLinkInput, CreateStreamRequest, CreateStreamResponse, ListLinksResponse,
            StreamLinkCredential, StreamLinkStatus, StreamLinkSummary, StreamMetadata,
            StreamTitleUpdate, UpdateStreamRequest, Visibility,
        },
        ws::{
            WriteStreamOptions,
            frame::{
                CaughtUpPosition, ClientFrame, MAX_RECORD_BYTES, OwnedReadRecord, PartHeader,
                ReadBatch, RecordFormat, ServerFrame, TSF_WEBSOCKET_PROTOCOL,
            },
        },
    },
    stream_url::StreamLocator,
    transcript::DEFAULT_MAX_LOGICAL_RECORD_BYTES,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
    process::Command as TokioCommand,
    sync::Notify,
    time::{sleep, timeout},
};
use url::Url;

const FREE_EXPIRY_LIMIT_MESSAGE: &str = "Free streams can expire at most 10 days from now.";
const TEST_STREAM_LINK: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
const UNKNOWN_STREAM_LINK: &str = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB";

fn canonical_test_link_secret() -> LinkSecret {
    TEST_STREAM_LINK
        .parse()
        .expect("canonical test link secret")
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
    const OWNER_LINK: &str =
        "https://tail.surf/s/0123456789abcdefghjkmnpqrstvwxyz#o=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

    let renewed = Command::new(env!("CARGO_BIN_EXE_tsf"))
        .args(["renew", OWNER_LINK, "18446744073709551615s"])
        .output()
        .expect("tsf renew with overflowing expiry");

    assert!(!renewed.status.success());
    let error = String::from_utf8(renewed.stderr).expect("stderr UTF-8");
    assert!(error.contains("stream expiry is too large"));
    assert!(!error.contains("panicked"));
}

#[tokio::test]
async fn new_outputs_json_and_link_files() {
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
    let owner_file = tmp.join("owner.link");
    let read_file = tmp.join("read.link");
    let write_file = tmp.join("write.link");
    #[cfg(unix)]
    {
        fs::write(&owner_file, "old-secret").expect("existing owner link");
        fs::set_permissions(&owner_file, fs::Permissions::from_mode(0o644))
            .expect("existing owner link permissions");
    }

    let output = run_tsf(
        &server,
        [
            "new",
            "--title",
            "Link file test",
            "--json",
            "--link",
            "owner=owner",
            "--link",
            "reader=read",
            "--link",
            "writer=write",
            "--owner-link-file",
            owner_file.to_str().expect("owner path"),
            "--read-link-file",
            read_file.to_str().expect("read path"),
            "--write-link-file",
            write_file.to_str().expect("write path"),
        ],
        None,
    )
    .await;
    assert!(output.status.success(), "stderr={}", output.stderr);
    let json: serde_json::Value = serde_json::from_str(&output.stdout).expect("json output");
    assert!(json["stream_id"].as_str().is_some());
    assert_eq!(json["title"], "Link file test");
    assert_eq!(json["visibility"], "private");
    assert!(json["expires_at"].as_str().is_some());
    assert_eq!(json["links"].as_array().map(Vec::len), Some(3));
    assert!(
        json["links"]
            .as_array()
            .is_some_and(|links| links.iter().all(|link| link.get("secret").is_none()))
    );
    for (path, label) in [
        (&owner_file, "owner"),
        (&read_file, "reader"),
        (&write_file, "writer"),
    ] {
        let url = created_link_url(&json, label);
        let locator = StreamLocator::parse(url).expect("matching URL parses");
        locator.link.expect("matching URL link");
        assert_eq!(fs::read_to_string(path).expect("link file"), url);
    }
    #[cfg(unix)]
    for path in [&owner_file, &read_file, &write_file] {
        assert_eq!(
            fs::metadata(path)
                .expect("link metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600,
            "{} must only be accessible by its owner",
            path.display()
        );
    }

    let owner_file_arg = format!("@{}", owner_file.display());
    let info = run_tsf(&server, ["info", owner_file_arg.as_str(), "--json"], None).await;
    assert!(info.status.success(), "stderr={}", info.stderr);
    let info_json: serde_json::Value =
        serde_json::from_str(&info.stdout).expect("info JSON from @file link");
    assert_eq!(info_json["stream_id"], json["stream_id"]);

    fs::remove_dir_all(tmp).expect("cleanup");
}

#[tokio::test]
async fn new_prints_created_links_before_a_link_file_error() {
    let server = TestServer::start().await;
    let unwritable_path = std::env::temp_dir().join(format!(
        "tsf-cli-unwritable-link-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    fs::create_dir(&unwritable_path).expect("unwritable link path");

    let output = run_tsf(
        &server,
        [
            "new",
            "--json",
            "--owner-link-file",
            unwritable_path.to_str().expect("link path"),
        ],
        None,
    )
    .await;

    assert!(!output.status.success());
    let json: serde_json::Value = serde_json::from_str(&output.stdout).expect("creation JSON");
    let owner_link = created_link_url(&json, "owner");
    let locator = StreamLocator::parse(owner_link).expect("owner link parses");
    assert!(
        locator
            .link_declaring(|permissions| permissions == LinkPermissions::owner())
            .is_some()
    );
    assert!(
        output.stderr.contains("failed to write owner link file"),
        "stderr={}",
        output.stderr
    );

    fs::remove_dir(&unwritable_path).expect("cleanup");
}

#[tokio::test]
async fn new_retries_with_one_canonical_idempotency_key() {
    let server = TestServer::start_with_create_failures(1).await;

    let output = run_tsf(&server, ["new", "--json"], None).await;

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
}

#[tokio::test]
async fn create_stream_recovers_a_committed_truncated_response() {
    let server = TestServer::start().await;
    server.fail_next_create_body();
    let key = IdempotencyKey::new_random();
    let exposed_key = key.expose_secret().to_owned();
    let request = CreateStreamRequest::default();
    let expected_owner_secret = test_minted_link_secret(&request.links[0].link_id)
        .expose_secret()
        .to_owned();

    let created = TsfClient::with_api_origin(server.api_url.clone())
        .expect("valid API origin")
        .create_stream_with_idempotency_key(&request, &key)
        .await
        .expect("recover committed create");

    assert_eq!(created.links.len(), 1);
    assert_eq!(
        created.links[0].secret.expose_secret(),
        expected_owner_secret
    );
    let observed_keys = server.create_idempotency_keys();
    assert_eq!(observed_keys.len(), 2);
    assert!(
        observed_keys
            .iter()
            .all(|observed| observed.as_deref() == Some(exposed_key.as_str()))
    );
    assert_eq!(server.stream_count(), 1);
}

#[tokio::test]
async fn create_link_recovers_a_committed_truncated_response() {
    let server = TestServer::start().await;
    let client = TsfClient::with_api_origin(server.api_url.clone()).expect("valid API origin");
    let created = client
        .create_stream(&CreateStreamRequest::default())
        .await
        .expect("create stream");
    let owner = &created.links[0].secret;
    let link_id: LinkId = "reader".parse().expect("link ID");
    let request = CreateLinkInput::new(link_id.clone(), LinkPermissions::read(), None);
    let key = IdempotencyKey::new_random();
    let exposed_key = key.expose_secret().to_owned();
    server.fail_next_link_create_body();

    let link = client
        .create_link_with_idempotency_key(&created.stream_id, &request, &key, owner)
        .await
        .expect("recover committed link creation");

    assert_eq!(link.link_id, link_id);
    assert_eq!(link.permissions, LinkPermissions::read());
    assert_eq!(
        link.secret.expose_secret(),
        test_minted_link_secret(&link_id).expose_secret()
    );
    let observed_keys = server.link_create_idempotency_keys();
    assert_eq!(observed_keys, vec![exposed_key.clone(), exposed_key]);
}

#[tokio::test]
async fn create_stream_is_always_anonymous() {
    let server = TestServer::start().await;
    let client = TsfClient::with_api_origin(server.api_url.clone()).expect("valid API origin");

    client
        .create_stream(&CreateStreamRequest::default())
        .await
        .expect("create stream");

    assert_eq!(server.create_authorizations(), [None]);
}

#[tokio::test]
async fn new_text_output_covers_visibility_and_explicit_links() {
    let server = TestServer::start().await;

    let private = run_tsf(&server, ["new"], None).await;
    assert!(private.status.success(), "stderr={}", private.stderr);
    assert_eq!(
        normalize_created_stream_output(&private.stdout),
        "Created private stream <stream_id>\nTitle: Untitled stream\nExpires: <timestamp>\n\n  reader read <url>\n  owner owner <url> (keep private)\n\nLinks are shown once.\n"
    );
    assert_created_links_parse(&private.stdout, &[("reader", "r"), ("owner", "o")]);

    let public = run_tsf(&server, ["new", "--public"], None).await;
    assert!(public.status.success(), "stderr={}", public.stderr);
    assert_eq!(
        normalize_created_stream_output(&public.stdout),
        "Created public stream <stream_id>\nTitle: Untitled stream\nExpires: <timestamp>\n\n  Public read <url> (public)\n  owner owner <url> (keep private)\n\nLinks are shown once.\n"
    );
    assert_created_links_parse(&public.stdout, &[("owner", "o")]);

    let explicit = run_tsf(
        &server,
        [
            "new",
            "--link",
            "combined=read-write",
            "--link",
            "reader=read",
        ],
        None,
    )
    .await;
    assert!(explicit.status.success(), "stderr={}", explicit.stderr);
    assert_eq!(
        normalize_created_stream_output(&explicit.stdout),
        "Created private stream <stream_id>\nTitle: Untitled stream\nExpires: <timestamp>\n\n  reader read <url>\n  combined read-write <url>\n  owner owner <url> (keep private)\n\nLinks are shown once.\n"
    );
    assert_created_links_parse(
        &explicit.stdout,
        &[("owner", "o"), ("combined", "rw"), ("reader", "r")],
    );
}

#[tokio::test]
async fn new_uses_an_explicit_owner_rejects_duplicate_ids_and_limits_links() {
    let server = TestServer::start().await;

    let deduplicated = run_tsf(
        &server,
        ["new", "--link", "admin=owner", "--link", "reader=read"],
        None,
    )
    .await;
    assert!(
        deduplicated.status.success(),
        "stderr={}",
        deduplicated.stderr
    );
    assert_created_links_parse(&deduplicated.stdout, &[("admin", "o"), ("reader", "r")]);

    let duplicate_ids = run_tsf(
        &server,
        ["new", "--link", "same=read", "--link", "same=write"],
        None,
    )
    .await;
    assert!(!duplicate_ids.status.success());
    assert!(
        duplicate_ids
            .stderr
            .contains("initial Link IDs must be unique")
    );

    let too_many = run_tsf(
        &server,
        [
            "new",
            "--link",
            "reader=read",
            "--link",
            "writer=write",
            "--link",
            "combined=read-write",
        ],
        None,
    )
    .await;
    assert!(!too_many.status.success());
    assert!(
        too_many
            .stderr
            .contains("at most 3 initial links may be created"),
        "stderr={}",
        too_many.stderr
    );
    assert_eq!(server.create_idempotency_keys().len(), 1);
}

#[tokio::test]
async fn new_link_files_require_the_exact_requested_permission() {
    let server = TestServer::start().await;

    let output = run_tsf(
        &server,
        [
            "new",
            "--link",
            "combined=read-write",
            "--write-link-file",
            "unused.link",
        ],
        None,
    )
    .await;

    assert!(!output.status.success());
    assert!(
        output
            .stderr
            .contains("--write-link-file requires a link with write permission"),
        "stderr={}",
        output.stderr
    );
    assert!(server.create_idempotency_keys().is_empty());
}

#[tokio::test]
async fn new_accepts_human_expiry_and_surfaces_free_limits() {
    let server = TestServer::start().await;

    let finite = run_tsf(&server, ["new", "--expires", "7d", "--json"], None).await;
    assert!(finite.status.success(), "stderr={}", finite.stderr);
    let finite_json: serde_json::Value =
        serde_json::from_str(&finite.stdout).expect("finite JSON output");
    assert!(finite_json["expires_at"].as_str().is_some());

    let denied = run_tsf(&server, ["new", "--expires", "864001s"], None).await;
    assert!(!denied.status.success());
    assert!(
        denied
            .stderr
            .contains(&format!("free_plan_limit: {FREE_EXPIRY_LIMIT_MESSAGE}")),
        "stderr={}",
        denied.stderr
    );
}

#[tokio::test]
async fn capture_then_replay_round_trips_piped_input() {
    let server = TestServer::start().await;
    let output = run_tsf(&server, [], Some("hello from cli integration\n")).await;
    assert!(output.status.success(), "stderr={}", output.stderr);
    assert_eq!(
        normalize_created_stream_output(&output.stdout),
        "Created private stream <stream_id>\nTitle: Untitled stream\nExpires: <timestamp>\n\n  reader read <url>\n  owner owner <url> (keep private)\n\nLinks are shown once.\n"
    );
    assert!(
        output.stderr.contains("1 record durable"),
        "stderr={}",
        output.stderr
    );
    let owner_link = output
        .stdout
        .lines()
        .find_map(|line| extract_link_line(line, "owner"))
        .expect("owner link");
    let owner_secret = StreamLocator::parse(owner_link)
        .expect("valid owner link")
        .link_declaring(LinkPermissions::allows_owner)
        .expect("owner secret")
        .expose_secret()
        .to_owned();
    assert_eq!(server.write_link_secrets(), [owner_secret]);
    assert_eq!(server.write_expected_next_seq_nums(), [None]);
    let read_link = output
        .stdout
        .lines()
        .find_map(|line| extract_link_line(line, "reader"))
        .expect("read link");
    StreamLocator::parse(read_link).expect("valid read link");

    let replay = run_tsf(&server, ["replay", read_link], None).await;
    assert!(replay.status.success(), "stderr={}", replay.stderr);
    assert_eq!(replay.stdout, "hello from cli integration\n");

    let bounded_tail = run_tsf(
        &server,
        ["tail", "--seq", "0", "--count", "1", read_link],
        None,
    )
    .await;
    assert!(
        bounded_tail.status.success(),
        "stderr={}",
        bounded_tail.stderr
    );
    assert_eq!(bounded_tail.stdout, "hello from cli integration\n");
}

#[tokio::test]
async fn capture_command_streams_output_and_propagates_exit_status() {
    let server = TestServer::start().await;
    let output = run_tsf(
        &server,
        [
            "new",
            "--",
            "sh",
            "-c",
            "printf out; printf err >&2; exit 7",
        ],
        None,
    )
    .await;
    assert_eq!(output.status.code(), Some(7), "stderr={}", output.stderr);
    let read_link = output
        .stdout
        .lines()
        .find_map(|line| extract_link_line(line, "reader"))
        .expect("read link");

    let replay = run_tsf(&server, ["replay", read_link], None).await;
    assert!(replay.status.success(), "stderr={}", replay.stderr);
    assert!(replay.stdout.contains("out"), "stdout={}", replay.stdout);
    assert!(replay.stdout.contains("err"), "stdout={}", replay.stdout);
}

#[tokio::test]
async fn write_defaults_to_lines_and_splits_large_records() {
    let server = TestServer::start().await;
    let mut input = "x".repeat(MAX_RECORD_BYTES + 10);
    input.push('\n');
    input.push_str("tail\n");

    let output = run_tsf(&server, [], Some(input.as_str())).await;
    assert!(output.status.success(), "stderr={}", output.stderr);
    let read_link = output
        .stdout
        .lines()
        .find_map(|line| extract_link_line(line, "reader"))
        .expect("read link");
    let locator = StreamLocator::parse(read_link).expect("valid read link");
    let read_link = locator
        .link_declaring(LinkPermissions::allows_read)
        .expect("read link");
    let client = TsfClient::with_api_origin(server.api_url.clone()).expect("valid API origin");
    let mut request = ReadOptions::new(locator.stream_id).with_link_secret(read_link.clone());
    request.start = Some(ReadStart::SeqNum(0));
    request.stop = Some(ReadStop {
        count: Some(3),
        ..ReadStop::default()
    });
    let mut reader = client.connect_reader(request).await.expect("reader");

    let mut records = Vec::new();
    while records.len() < 3 {
        match reader.next_batch().await.expect("event") {
            Some(batch) => records.extend(batch.iter().map(|record| record.into_owned())),
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
}

#[tokio::test]
async fn write_rejects_a_line_above_the_default_reader_limit_before_appending() {
    let server = TestServer::start().await;
    let mut input = "x".repeat(DEFAULT_MAX_LOGICAL_RECORD_BYTES);
    input.push('\n');

    let output = run_tsf(&server, [], Some(input.as_str())).await;

    assert!(!output.status.success());
    assert!(
        output.stderr.contains(&format!(
            "input line exceeds the configured {DEFAULT_MAX_LOGICAL_RECORD_BYTES}-byte logical record limit"
        )),
        "stderr={}",
        output.stderr
    );
    let read_link = output
        .stdout
        .lines()
        .find_map(|line| extract_link_line(line, "reader"))
        .expect("read link is printed before writing");
    let locator = StreamLocator::parse(read_link).expect("valid read link");
    assert_eq!(server.record_count(&locator.stream_id), 0);
}

#[tokio::test]
async fn write_raw_preserves_large_input_across_flush_boundaries() {
    let server = TestServer::start().await;
    let input = "x".repeat(MAX_RECORD_BYTES + 10);

    let output = run_tsf(&server, ["new", "--raw"], Some(input.as_str())).await;
    assert!(output.status.success(), "stderr={}", output.stderr);
    let read_link = output
        .stdout
        .lines()
        .find_map(|line| extract_link_line(line, "reader"))
        .expect("read link");
    let locator = StreamLocator::parse(read_link).expect("valid read link");
    let read_link = locator
        .link_declaring(LinkPermissions::allows_read)
        .expect("read link");
    let client = TsfClient::with_api_origin(server.api_url.clone()).expect("valid API origin");
    let mut request = ReadOptions::new(locator.stream_id).with_link_secret(read_link.clone());
    request.start = Some(ReadStart::SeqNum(0));
    request.stop = Some(ReadStop {
        count: Some(16),
        ..ReadStop::default()
    });
    let mut reader = client.connect_reader(request).await.expect("reader");

    let mut records = Vec::new();
    let mut output = Vec::new();
    while output.len() < input.len() {
        match reader.next_batch().await.expect("event") {
            Some(batch) => {
                for record in &batch {
                    output.extend_from_slice(record.data);
                    records.push(record.into_owned());
                }
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
}

#[tokio::test]
async fn write_raw_flushes_on_linger() {
    let server = TestServer::start().await;

    let output = run_tsf(
        &server,
        [
            "new",
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
    let read_link = output
        .stdout
        .lines()
        .find_map(|line| extract_link_line(line, "reader"))
        .expect("read link");
    let locator = StreamLocator::parse(read_link).expect("valid read link");
    let read_link = locator
        .link_declaring(LinkPermissions::allows_read)
        .expect("read link");
    let client = TsfClient::with_api_origin(server.api_url.clone()).expect("valid API origin");
    let mut request = ReadOptions::new(locator.stream_id).with_link_secret(read_link.clone());
    request.start = Some(ReadStart::SeqNum(0));
    request.stop = Some(ReadStop {
        count: Some(2),
        ..ReadStop::default()
    });
    let mut reader = client.connect_reader(request).await.expect("reader");

    let mut data = Vec::new();
    while data.len() < 2 {
        match reader.next_batch().await.expect("event") {
            Some(batch) => {
                for record in &batch {
                    assert_eq!(record.format, RecordFormat::Bytes);
                    data.push(Bytes::copy_from_slice(record.data));
                }
            }
            None => panic!("reader closed before expected records"),
        }
    }

    assert_eq!(data[0].as_ref(), b"a");
    assert_eq!(data[1].as_ref(), b"b");
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
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = command.spawn().expect("spawn tsf capture");
    let mut stdin = child.stdin.take().expect("stdin");
    let mut stdout = BufReader::new(child.stdout.take().expect("stdout"));
    let mut stderr = BufReader::new(child.stderr.take().expect("stderr"));
    let mut stderr_output = String::new();
    let read_link = loop {
        let mut line = String::new();
        let read = stdout.read_line(&mut line).await.expect("read created URL");
        assert!(read > 0, "tsf exited before printing a read link");
        if let Some(url) = extract_link_line(&line, "reader") {
            break url.trim_end().to_owned();
        }
    };

    let stream_id = StreamLocator::parse(&read_link)
        .expect("valid read link")
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

    let replay = run_tsf(&server, ["replay", read_link.as_str()], None).await;
    assert!(replay.status.success(), "stderr={}", replay.stderr);
    assert_eq!(replay.stdout, "complete line\npartial line");
}

#[tokio::test]
async fn write_reconnect_reuses_client_writer_identity_sequence_and_link_secret() {
    let server = FakeWriteServer::start().await;
    let stream_id = "0123456789abcdefghjkmnpqrstvwxyz"
        .parse::<StreamId>()
        .expect("stream id");
    let write_link = format!("http://localhost:3000/s/{stream_id}#w={TEST_STREAM_LINK}");

    let output = run_tsf_with_api_url(
        server.api_url.clone(),
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
    assert_eq!(attempts[0].data.as_ref(), b"retry me\n");
    assert_eq!(attempts[1].data.as_ref(), b"retry me\n");
    assert_eq!(attempts[0].part, PartHeader::unsplit());
    assert_eq!(attempts[1].part, PartHeader::unsplit());
    assert_eq!(attempts[0].format, RecordFormat::Transcript);
    assert_eq!(attempts[1].format, RecordFormat::Transcript);
}

#[tokio::test]
async fn writer_preserves_its_terminal_failure_for_later_submissions() {
    let server = FakeWriteServer::start_terminal().await;
    let writer = connect_default_writer(&server.api_url).await;
    let pre_admitted = writer.reserve(1).await.expect("reserve before failure");
    let first = writer
        .submit(test_write_record(0, Bytes::from_static(b"first")))
        .await
        .expect("submit first record");
    let first_error = first.await.expect_err("first record must fail");
    assert_sequence_mismatch(&first_error);

    let pre_admitted_error =
        match pre_admitted.submit(test_write_record(1, Bytes::from_static(b"x"))) {
            Ok(_) => panic!("terminal writer accepted a pre-admitted record"),
            Err(error) => error,
        };
    assert_sequence_mismatch(&pre_admitted_error);

    let later_error = match writer
        .submit(test_write_record(1, Bytes::from_static(b"later")))
        .await
    {
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
async fn writer_close_is_not_blocked_by_an_unused_reservation() {
    let server = FakeWriteServer::start().await;
    let stream_id = "0123456789abcdefghjkmnpqrstvwxyz"
        .parse::<StreamId>()
        .expect("stream id");
    let client = TsfClient::with_api_origin(server.api_url.clone()).expect("valid API origin");
    let writer = client
        .connect_writer_with_config(
            tailsurf::protocol::ws::WriteStreamOptions::new(
                stream_id,
                ClientWriterId::new_random(),
                canonical_test_link_secret(),
            ),
            TsfWriterConfig {
                max_unacked_bytes: 1,
                max_unacked_records: 1,
            },
        )
        .await
        .expect("writer");
    let _permit = writer.reserve(1).await.expect("reservation");

    timeout(Duration::from_secs(1), writer.close())
        .await
        .expect("writer close must not wait for reservation")
        .expect("writer close");
}

#[tokio::test]
async fn default_writer_enforces_record_and_byte_windows() {
    assert_default_writer_window(128, Bytes::from_static(b"x")).await;
    assert_default_writer_window(10, Bytes::from(vec![0_u8; MAX_RECORD_BYTES])).await;
}

async fn assert_default_writer_window(capacity: usize, payload: Bytes) {
    let server = HoldingWriteServer::start(capacity).await;
    let writer = connect_default_writer(&server.api_url).await;
    let record_count = u64::try_from(capacity).expect("window capacity fits u64");
    let mut tickets = Vec::new();
    for writer_seq_num in 0..record_count {
        tickets.push(
            writer
                .submit(test_write_record(writer_seq_num, payload.clone()))
                .await
                .expect("submit within writer window"),
        );
    }
    server.wait_for_records(capacity).await;

    assert!(
        timeout(
            Duration::from_millis(100),
            writer.submit(test_write_record(record_count, Bytes::from_static(b"x"))),
        )
        .await
        .is_err(),
        "submit beyond the writer window must wait for an acknowledgement"
    );

    server.release_acknowledgements();
    for ticket in tickets {
        ticket.await.expect("durability acknowledgement");
    }
    let final_ticket = timeout(
        Duration::from_secs(1),
        writer.submit(test_write_record(record_count, Bytes::from_static(b"x"))),
    )
    .await
    .expect("writer window reopened")
    .expect("final submit");
    final_ticket.await.expect("final acknowledgement");
    writer.close().await.expect("writer close");
}

#[tokio::test]
async fn writer_reconnect_resends_every_unacknowledged_record_in_order() {
    let server = HoldingWriteServer::start_reconnecting(3).await;
    let writer = connect_default_writer(&server.api_url).await;
    let mut tickets = Vec::new();
    for writer_seq_num in 0..3 {
        tickets.push(
            writer
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
            .all(|attempt| attempt.client_writer_id == attempts[0].client_writer_id)
    );
    assert_eq!(attempts[0].data, attempts[3].data);
    assert_eq!(attempts[1].data, attempts[4].data);
    assert_eq!(attempts[2].data, attempts[5].data);

    writer.close().await.expect("writer close");
}

#[tokio::test]
async fn tail_reconnect_resumes_after_last_sequence() {
    let server = FakeReadServer::start(FakeReadMode::Reconnect).await;
    let stream_id = "0123456789abcdefghjkmnpqrstvwxyz"
        .parse::<StreamId>()
        .expect("stream id");
    let read_link = format!("http://localhost:3000/s/{stream_id}#r={TEST_STREAM_LINK}");

    let output = run_tsf_until_stdout_contains(
        server.api_url.clone(),
        ["tail", read_link.as_str()],
        b"first\nsecond\n",
        Duration::from_secs(5),
    )
    .await;

    assert_eq!(output.stdout, "first\nsecond\n");
    assert_eq!(output.stderr, "");
    let attempts = server.read_attempts();
    assert_eq!(attempts.len(), 2);
    assert_eq!(attempts[0].link_secret, TEST_STREAM_LINK);
    assert_eq!(attempts[1].link_secret, TEST_STREAM_LINK);
    assert_eq!(attempts[0].start, ReadStart::TailOffset(0));
    assert_eq!(attempts[1].start, ReadStart::SeqNum(1));
}

#[tokio::test]
async fn tail_reconnect_after_multi_record_batch_advances_start_and_count() {
    let server = FakeReadServer::start(FakeReadMode::ReconnectAfterBatch).await;
    let stream_id = "0123456789abcdefghjkmnpqrstvwxyz"
        .parse::<StreamId>()
        .expect("stream id");
    let read_link = format!("http://localhost:3000/s/{stream_id}#r={TEST_STREAM_LINK}");

    let output = run_tsf_until_stdout_contains(
        server.api_url.clone(),
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
async fn bounded_sse_tail_finishes_at_clean_eof() {
    let server = FakeSseServer::start().await;
    let stream_id = "0123456789abcdefghjkmnpqrstvwxyz";
    let read_link = format!("http://localhost:3000/s/{stream_id}#r={TEST_STREAM_LINK}");

    let output = run_tsf_with_api_url(
        server.api_url.clone(),
        ["tail", "--sse", "--count", "1", read_link.as_str()],
        None,
    )
    .await;

    assert!(output.status.success(), "stderr={}", output.stderr);
    assert_eq!(output.stdout, "");
    let attempts = server.attempts();
    assert_eq!(attempts.len(), 1);
    let query = attempts[0].query.as_deref().expect("SSE query");
    let query = Url::parse(&format!("http://localhost/?{query}")).expect("parse captured query");
    assert_eq!(
        query.query_pairs().collect::<HashMap<_, _>>(),
        HashMap::from([
            ("tail_offset".into(), "0".into()),
            ("count".into(), "1".into()),
        ])
    );
    assert_eq!(attempts[0].last_event_id, None);
    assert!(attempts.iter().all(|attempt| {
        attempt.authorization.as_deref() == Some(&format!("Bearer {TEST_STREAM_LINK}"))
    }));
}

#[tokio::test]
async fn bounded_sse_tail_finishes_after_a_multi_record_batch() {
    let server = FakeSseServer::start_with_mode(FakeSseMode::BatchThenClose).await;
    let stream_id = "0123456789abcdefghjkmnpqrstvwxyz";
    let read_link = format!("http://localhost:3000/s/{stream_id}#r={TEST_STREAM_LINK}");

    let output = run_tsf_with_api_url(
        server.api_url.clone(),
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
    let stream_id = "0123456789abcdefghjkmnpqrstvwxyz"
        .parse::<StreamId>()
        .expect("stream id");
    let mut options = ReadOptions::new(stream_id);
    options.start = Some(ReadStart::SeqNum(0));
    options.stop = Some(ReadStop {
        wait_seconds: Some(0),
        ..ReadStop::default()
    });
    options.link_secret = Some(canonical_test_link_secret());

    let mut session = TsfClient::with_api_origin(server.api_url.clone())
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
    let server = FakeReadServer::start(FakeReadMode::Reconnect).await;
    let stream_id = "0123456789abcdefghjkmnpqrstvwxyz"
        .parse::<StreamId>()
        .expect("stream id");
    let read_link = format!("http://localhost:3000/s/{stream_id}#r={TEST_STREAM_LINK}");

    let output = run_tsf_until_stdout_contains(
        server.api_url.clone(),
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
async fn empty_caught_up_establishes_the_reconnect_position() {
    let server = FakeReadServer::start(FakeReadMode::ReconnectAfterEmptyCaughtUp).await;
    let stream_id = "0123456789abcdefghjkmnpqrstvwxyz"
        .parse::<StreamId>()
        .expect("stream id");
    let read_link = format!("http://localhost:3000/s/{stream_id}#r={TEST_STREAM_LINK}");

    let output = run_tsf_until_stdout_contains(
        server.api_url.clone(),
        ["tail", "-n", "2", read_link.as_str()],
        b"stable\n",
        Duration::from_secs(5),
    )
    .await;

    assert_eq!(output.stdout, "stable\n");
    assert_eq!(output.stderr, "");
    let attempts = server.read_attempts();
    assert_eq!(attempts.len(), 2);
    assert_eq!(attempts[0].start, ReadStart::TailOffset(2));
    assert_eq!(attempts[1].start, ReadStart::SeqNum(5));
}

#[tokio::test]
async fn default_read_start_reconnect_before_first_record_retries_the_default() {
    let server = FakeReadServer::start(FakeReadMode::ReconnectBeforeFirstDefault).await;
    let stream_id = "0123456789abcdefghjkmnpqrstvwxyz"
        .parse::<StreamId>()
        .expect("stream id");
    let client = TsfClient::with_api_origin(server.api_url.clone()).expect("valid API origin");
    let request = ReadOptions::new(stream_id).with_link_secret(canonical_test_link_secret());
    let mut reader = client.connect_reader(request).await.expect("reader");

    let batch = reader
        .next_batch_with_timeout(Duration::from_secs(5))
        .await
        .expect("read batch")
        .expect("batch");
    let record = batch.first();

    assert_eq!(record.seq_num, 20);
    assert_eq!(record.data, b"default\n");
    let attempts = server.read_attempts();
    assert_eq!(attempts.len(), 2);
    assert_eq!(attempts[0].start, ReadStart::TailOffset(0));
    assert_eq!(attempts[1].start, ReadStart::TailOffset(0));
}

#[tokio::test]
async fn reader_restarts_retries_after_established_idle_connections() {
    let server = FakeReadServer::start(FakeReadMode::ReconnectTwiceThenRecord).await;
    let stream_id = "0123456789abcdefghjkmnpqrstvwxyz"
        .parse::<StreamId>()
        .expect("stream id");
    let mut config = TsfClientConfig::new(server.api_url.clone()).expect("valid API origin");
    config.retry_policy = RetryPolicy {
        max_attempts: 2,
        initial_backoff: Duration::ZERO,
        max_backoff: Duration::ZERO,
    };
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
    assert_eq!(record.data, b"recovered\n");
    assert_eq!(server.read_attempts().len(), 3);
}

#[tokio::test]
async fn explicit_read_timeout_covers_reconnect_cycles() {
    let server = FakeReadServer::start(FakeReadMode::SlowReconnectForever).await;
    let stream_id = "0123456789abcdefghjkmnpqrstvwxyz"
        .parse::<StreamId>()
        .expect("stream id");
    let mut config = TsfClientConfig::new(server.api_url.clone()).expect("valid API origin");
    config.retry_policy = RetryPolicy {
        max_attempts: 100,
        initial_backoff: Duration::ZERO,
        max_backoff: Duration::ZERO,
    };
    let client = TsfClient::with_config(config).expect("valid client config");
    let mut request = ReadOptions::new(stream_id).with_link_secret(canonical_test_link_secret());
    request.start = Some(ReadStart::SeqNum(0));
    let mut reader = client.connect_reader(request).await.expect("reader");

    let error = timeout(
        Duration::from_secs(1),
        reader.next_batch_with_timeout(Duration::from_millis(100)),
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
    let stream_id = "0123456789abcdefghjkmnpqrstvwxyz"
        .parse::<StreamId>()
        .expect("stream id");
    let client = TsfClient::with_api_origin(server.api_url.clone()).expect("valid API origin");
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
    assert_eq!(record.data, b"stable\n");
    assert_eq!(server.read_attempts().len(), 2);
}

#[tokio::test]
async fn reader_reconnects_after_configured_idle_timeout() {
    let server = FakeReadServer::start(FakeReadMode::SilentThenRecord).await;
    let stream_id = "0123456789abcdefghjkmnpqrstvwxyz"
        .parse::<StreamId>()
        .expect("stream id");
    let mut config = TsfClientConfig::new(server.api_url.clone()).expect("valid API origin");
    config.websocket_read_idle_timeout = Some(Duration::from_millis(50));
    config.retry_policy = RetryPolicy {
        max_attempts: 3,
        initial_backoff: Duration::ZERO,
        max_backoff: Duration::ZERO,
    };
    let client = TsfClient::with_config(config).expect("valid client config");
    let mut request = ReadOptions::new(stream_id).with_link_secret(canonical_test_link_secret());
    request.start = Some(ReadStart::SeqNum(0));
    let mut reader = client.connect_reader(request).await.expect("reader");

    let batch = timeout(Duration::from_secs(2), reader.next_batch())
        .await
        .expect("idle reconnect")
        .expect("read batch")
        .expect("batch");
    let record = batch.first();

    assert_eq!(record.seq_num, 0);
    assert_eq!(record.data, b"after idle\n");
    assert_eq!(server.read_attempts().len(), 2);
}

#[tokio::test]
async fn count_zero_reads_complete_without_opening_a_socket() {
    let server = FakeReadServer::start(FakeReadMode::Reconnect).await;
    let stream_id = "0123456789abcdefghjkmnpqrstvwxyz"
        .parse::<StreamId>()
        .expect("stream id");
    let read_link = format!("http://localhost:3000/s/{stream_id}#r={TEST_STREAM_LINK}");

    let replay = run_tsf_with_api_url(
        server.api_url.clone(),
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
    let server = FakeReadServer::start(FakeReadMode::Reconnect).await;
    let stream_id = "0123456789abcdefghjkmnpqrstvwxyz"
        .parse::<StreamId>()
        .expect("stream id");
    let read_link = format!("http://localhost:3000/s/{stream_id}#r={TEST_STREAM_LINK}");

    let output = run_tsf_with_api_url(
        server.api_url.clone(),
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
async fn cli_reports_rest_errors_without_raw_json_body() {
    let server = TestServer::start().await;

    let public = run_tsf(&server, ["new", "--public"], None).await;
    assert!(public.status.success(), "stderr={}", public.stderr);
    let owner_link = public
        .stdout
        .lines()
        .find_map(|line| extract_link_line(line, "owner"))
        .expect("owner link");
    let bad_owner_link = owner_link
        .split_once("#o=")
        .map(|(prefix, _secret)| format!("{prefix}#o={UNKNOWN_STREAM_LINK}"))
        .expect("owner fragment");

    let output = run_tsf(&server, ["visibility", &bad_owner_link, "private"], None).await;

    assert!(!output.status.success(), "stdout={}", output.stdout);
    assert!(
        output.stderr.contains("forbidden: owner link required"),
        "stderr={}",
        output.stderr
    );
    assert!(
        !output.stderr.contains(r#""error""#),
        "stderr={}",
        output.stderr
    );
}

#[tokio::test]
async fn replay_rejects_logical_records_above_configured_limit() {
    let server = FakeReadServer::start(FakeReadMode::ReplaySplitRecord).await;
    let stream_id = "0123456789abcdefghjkmnpqrstvwxyz"
        .parse::<StreamId>()
        .expect("stream id");
    let read_link = format!("http://localhost:3000/s/{stream_id}#r={TEST_STREAM_LINK}");

    let output = run_tsf_with_api_url(
        server.api_url.clone(),
        [
            "replay",
            "--max-logical-record-bytes",
            "4",
            read_link.as_str(),
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
}

#[tokio::test]
async fn replay_preserves_non_utf8_stdout_bytes() {
    let server = FakeReadServer::start(FakeReadMode::ReplayBinary).await;
    let stream_id = "0123456789abcdefghjkmnpqrstvwxyz"
        .parse::<StreamId>()
        .expect("stream id");
    let read_link = format!("http://localhost:3000/s/{stream_id}#r={TEST_STREAM_LINK}");

    let output =
        run_tsf_bytes_with_api_url(server.api_url.clone(), ["replay", read_link.as_str()]).await;

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

#[tokio::test]
async fn owner_commands_manage_visibility_links_and_deletion() {
    let server = TestServer::start().await;
    let created = run_tsf(
        &server,
        ["new", "--title", "Deploy log", "--expires", "1d", "--json"],
        None,
    )
    .await;
    assert!(created.status.success(), "stderr={}", created.stderr);
    let created_json: serde_json::Value =
        serde_json::from_str(&created.stdout).expect("create output");
    let owner_link = created_link_url(&created_json, "owner");

    let info = run_tsf(&server, ["info", owner_link, "--json"], None).await;
    assert!(info.status.success(), "stderr={}", info.stderr);
    let info_json: serde_json::Value =
        serde_json::from_str(&info.stdout).expect("stream metadata output");
    assert_eq!(info_json["stream_id"], created_json["stream_id"]);
    assert_eq!(info_json["title"], "Deploy log");
    assert_eq!(info_json["visibility"], "private");
    assert!(info_json.get("state").is_none());
    assert_eq!(info_json["expires_at"], created_json["expires_at"]);

    let renewed = run_tsf(&server, ["renew", owner_link, "2d", "--json"], None).await;
    assert!(renewed.status.success(), "stderr={}", renewed.stderr);
    let renewed_json: serde_json::Value =
        serde_json::from_str(&renewed.stdout).expect("renew output");
    assert_ne!(renewed_json["expires_at"], created_json["expires_at"]);

    let visibility = run_tsf(
        &server,
        ["visibility", owner_link, "public", "--json"],
        None,
    )
    .await;
    assert!(visibility.status.success(), "stderr={}", visibility.stderr);
    let visibility_json: serde_json::Value =
        serde_json::from_str(&visibility.stdout).expect("visibility output");
    assert_eq!(visibility_json["visibility"], "public");

    let titled = run_tsf(
        &server,
        ["title", "set", owner_link, "Deploy log west", "--json"],
        None,
    )
    .await;
    assert!(titled.status.success(), "stderr={}", titled.stderr);
    let titled_json: serde_json::Value =
        serde_json::from_str(&titled.stdout).expect("title output");
    assert_eq!(titled_json["title"], "Deploy log west");

    let cleared = run_tsf(&server, ["title", "clear", owner_link, "--json"], None).await;
    assert!(cleared.status.success(), "stderr={}", cleared.stderr);
    let cleared_json: serde_json::Value =
        serde_json::from_str(&cleared.stdout).expect("title clear output");
    assert!(cleared_json["title"].is_null());

    let created_link = run_tsf(
        &server,
        ["link", "create", owner_link, "deploy-reader=read", "--json"],
        None,
    )
    .await;
    assert!(
        created_link.status.success(),
        "stderr={}",
        created_link.stderr
    );
    let created_link_json: serde_json::Value =
        serde_json::from_str(&created_link.stdout).expect("create link output");
    let created_url = created_link_json["url"].as_str().expect("created URL");
    StreamLocator::parse(created_url).expect("created URL parses");
    let link_id = created_link_json["link_id"]
        .as_str()
        .expect("link id")
        .to_owned();
    assert_eq!(created_link_json["link_id"], "deploy-reader");
    assert!(created_link_json.get("secret").is_none());

    server.fail_next_link_list();
    let listed = run_tsf(&server, ["link", "list", owner_link, "--json"], None).await;
    assert!(listed.status.success(), "stderr={}", listed.stderr);
    let listed_json: serde_json::Value =
        serde_json::from_str(&listed.stdout).expect("link list output");
    assert_eq!(listed_json["links"].as_array().map(Vec::len), Some(3));
    assert_eq!(
        listed_json["links"]
            .as_array()
            .and_then(|links| links.iter().find(|link| link["link_id"] == link_id))
            .map(|link| &link["status"]),
        Some(&serde_json::Value::String("active".to_owned()))
    );

    let revoked = run_tsf(
        &server,
        ["link", "revoke", owner_link, link_id.as_str(), "--json"],
        None,
    )
    .await;
    assert!(revoked.status.success(), "stderr={}", revoked.stderr);
    let revoked_json: serde_json::Value =
        serde_json::from_str(&revoked.stdout).expect("revoke output");
    assert_eq!(revoked_json["link_id"], link_id);
    assert_eq!(revoked_json["status"], "revoked");

    let listed = run_tsf(&server, ["link", "list", owner_link, "--json"], None).await;
    let listed_json: serde_json::Value =
        serde_json::from_str(&listed.stdout).expect("link list output");
    assert_eq!(
        listed_json["links"]
            .as_array()
            .and_then(|links| links.iter().find(|link| link["link_id"] == link_id))
            .map(|link| &link["status"]),
        Some(&serde_json::Value::String("revoked".to_owned()))
    );

    let deleted = run_tsf(&server, ["delete", owner_link, "--yes", "--json"], None).await;
    assert!(deleted.status.success(), "stderr={}", deleted.stderr);
    let deleted_json: serde_json::Value =
        serde_json::from_str(&deleted.stdout).expect("delete output");
    assert_eq!(deleted_json["stream_id"], created_json["stream_id"]);
    assert_eq!(deleted_json["status"], "deleted");

    let after_delete = run_tsf(&server, ["visibility", owner_link, "private"], None).await;
    assert!(
        !after_delete.status.success(),
        "visibility update unexpectedly succeeded after delete"
    );
}

struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn start_server(router: Router) -> (Url, AbortOnDrop) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("address");
    let task = AbortOnDrop(tokio::spawn(async move {
        axum::serve(listener, router).await.expect("test server");
    }));
    (
        Url::parse(&format!("http://{address}")).expect("API URL"),
        task,
    )
}

struct TestServer {
    api_url: Url,
    state: Arc<TestApiState>,
    _task: AbortOnDrop,
}

impl TestServer {
    async fn start() -> Self {
        Self::start_with_create_failures(0).await
    }

    async fn start_with_create_failures(create_failures: usize) -> Self {
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
            .route("/api/v1/streams/{stream_id}/links", get(test_list_links))
            .route(
                "/api/v1/streams/{stream_id}/links/{link_id}",
                axum::routing::put(test_create_link).delete(test_revoke_link),
            )
            .route("/api/v1/streams/{stream_id}/write", get(test_write_socket))
            .route("/api/v1/streams/{stream_id}/read", get(test_read_socket))
            .with_state(state.clone());
        let (api_url, task) = start_server(router).await;
        Self {
            api_url,
            state,
            _task: task,
        }
    }

    fn fail_next_link_list(&self) {
        *self
            .state
            .link_list_failures_remaining
            .lock()
            .expect("link list failure lock") += 1;
    }

    fn fail_next_create_body(&self) {
        *self
            .state
            .create_invalid_json_remaining
            .lock()
            .expect("create body failure lock") += 1;
    }

    fn fail_next_link_create_body(&self) {
        *self
            .state
            .link_create_invalid_json_remaining
            .lock()
            .expect("link create body failure lock") += 1;
    }

    fn create_idempotency_keys(&self) -> Vec<Option<String>> {
        self.state
            .create_idempotency_keys
            .lock()
            .expect("create idempotency keys lock")
            .clone()
    }

    fn link_create_idempotency_keys(&self) -> Vec<String> {
        self.state
            .link_create_idempotency_keys
            .lock()
            .expect("link create idempotency keys lock")
            .clone()
    }

    fn create_authorizations(&self) -> Vec<Option<String>> {
        self.state
            .create_authorizations
            .lock()
            .expect("create authorizations lock")
            .clone()
    }

    fn write_link_secrets(&self) -> Vec<String> {
        self.state
            .write_link_secrets
            .lock()
            .expect("write link secrets lock")
            .clone()
    }

    fn write_expected_next_seq_nums(&self) -> Vec<Option<u64>> {
        self.state
            .write_expected_next_seq_nums
            .lock()
            .expect("write preconditions lock")
            .clone()
    }

    fn stream_count(&self) -> usize {
        self.state.streams.lock().expect("streams lock").len()
    }

    fn record_count(&self, stream_id: &StreamId) -> usize {
        self.state
            .streams
            .lock()
            .expect("streams lock")
            .get(&stream_id.to_string())
            .map_or(0, |stream| stream.records.len())
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
}

#[derive(Default)]
struct TestApiState {
    next_stream: Mutex<u64>,
    create_failures_remaining: Mutex<usize>,
    create_invalid_json_remaining: Mutex<usize>,
    create_responses: Mutex<HashMap<String, CreateStreamResponse>>,
    create_idempotency_keys: Mutex<Vec<Option<String>>>,
    create_authorizations: Mutex<Vec<Option<String>>>,
    link_create_invalid_json_remaining: Mutex<usize>,
    link_create_idempotency_keys: Mutex<Vec<String>>,
    write_link_secrets: Mutex<Vec<String>>,
    write_expected_next_seq_nums: Mutex<Vec<Option<u64>>>,
    link_list_failures_remaining: Mutex<usize>,
    streams: Mutex<HashMap<String, TestStream>>,
}

struct TestStream {
    stream_id: StreamId,
    title: Option<StreamTitle>,
    visibility: Visibility,
    expires_at: String,
    deleted: bool,
    links: Vec<TestLink>,
    records: Vec<TestRecord>,
}

#[derive(Clone)]
struct TestLink {
    link_id: LinkId,
    permissions: LinkPermissions,
    secret: LinkSecret,
    active: bool,
}

#[derive(serde::Deserialize)]
struct TestCreateLinkInput {
    permissions: LinkPermissions,
}

#[derive(Clone)]
struct TestRecord {
    seq_num: u64,
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
        .push(idempotency_key.clone());
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

    if let Some(response) = idempotency_key.as_ref().and_then(|key| {
        state
            .create_responses
            .lock()
            .expect("create responses lock")
            .get(key)
            .cloned()
    }) {
        return Json(response).into_response();
    }

    let expires_in_seconds = request.expires_in_seconds.unwrap_or(864_000);
    if expires_in_seconds > 864_000 {
        return test_error(
            StatusCode::FORBIDDEN,
            "free_plan_limit",
            FREE_EXPIRY_LIMIT_MESSAGE,
        );
    }
    let expires_at = humantime::format_rfc3339_seconds(
        SystemTime::now() + Duration::from_secs(expires_in_seconds),
    )
    .to_string();
    let stream_id = {
        let mut next_stream = state.next_stream.lock().expect("next stream lock");
        let stream_id = format!("{:032x}", *next_stream)
            .parse::<StreamId>()
            .expect("stream id");
        *next_stream += 1;
        stream_id
    };
    let requested_links = request.links;
    let links = requested_links
        .into_iter()
        .map(|link| {
            let secret = test_minted_link_secret(&link.link_id);
            test_store_stream_link(link.link_id, secret, link.permissions)
        })
        .collect::<Vec<_>>();
    let response_links = links
        .iter()
        .map(|link| StreamLinkCredential {
            link_id: link.link_id.clone(),
            permissions: link.permissions,
            secret: link.secret.clone(),
        })
        .collect::<Vec<_>>();
    let mut streams = state.streams.lock().expect("streams lock");
    streams.insert(
        stream_id.to_string(),
        TestStream {
            stream_id,
            title: request.title.clone(),
            visibility: request.visibility,
            expires_at: expires_at.clone(),
            deleted: false,
            links,
            records: Vec::new(),
        },
    );

    let response = CreateStreamResponse {
        stream_id,
        title: request.title,
        visibility: request.visibility,
        created_at: "2026-08-13T00:00:00Z".to_owned(),
        expires_at,
        links: response_links,
    };
    if let Some(key) = idempotency_key {
        state
            .create_responses
            .lock()
            .expect("create responses lock")
            .insert(key, response.clone());
    }
    let mut invalid_json = state
        .create_invalid_json_remaining
        .lock()
        .expect("create body failure lock");
    if *invalid_json > 0 {
        *invalid_json -= 1;
        return (StatusCode::OK, [("content-type", "application/json")], "{").into_response();
    }
    drop(invalid_json);

    Json(response).into_response()
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
        && !test_authorized(stream, &headers, LinkPermissions::allows_read)
    {
        return test_error(StatusCode::FORBIDDEN, "forbidden", "read link required");
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
    if !test_authorized(stream, &headers, LinkPermissions::allows_owner) {
        return test_error(StatusCode::FORBIDDEN, "forbidden", "owner link required");
    }
    if let Some(visibility) = request.visibility {
        stream.visibility = visibility;
    }
    match request.title {
        StreamTitleUpdate::Unchanged => {}
        StreamTitleUpdate::Set(title) => stream.title = Some(title),
        StreamTitleUpdate::Clear => stream.title = None,
    }
    if let Some(expires_at) = request.expires_at {
        stream.expires_at = expires_at;
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
    if !test_authorized(stream, &headers, LinkPermissions::allows_owner) {
        return test_error(StatusCode::FORBIDDEN, "forbidden", "owner link required");
    }
    stream.deleted = true;
    StatusCode::NO_CONTENT.into_response()
}

async fn test_create_link(
    State(state): State<Arc<TestApiState>>,
    Path((stream_id, link_id)): Path<(String, LinkId)>,
    headers: HeaderMap,
    Json(request): Json<TestCreateLinkInput>,
) -> Response {
    let Some(idempotency_key) = headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .filter(|value| value.parse::<IdempotencyKey>().is_ok())
        .map(str::to_owned)
    else {
        return test_error(
            StatusCode::BAD_REQUEST,
            "bad_request",
            "canonical idempotency key required from SDK",
        );
    };
    state
        .link_create_idempotency_keys
        .lock()
        .expect("link create idempotency keys lock")
        .push(idempotency_key);
    let mut streams = state.streams.lock().expect("streams lock");
    let Some(stream) = streams.get_mut(&stream_id) else {
        return test_error(StatusCode::NOT_FOUND, "not_found", "stream not found");
    };
    if stream.deleted {
        return test_error(StatusCode::CONFLICT, "conflict", "stream is deleted");
    }
    if !test_authorized(stream, &headers, LinkPermissions::allows_owner) {
        return test_error(StatusCode::FORBIDDEN, "forbidden", "owner link required");
    }
    let response = if let Some(link) = stream.links.iter().find(|link| link.link_id == link_id) {
        if link.permissions != request.permissions {
            return test_error(
                StatusCode::CONFLICT,
                "conflict",
                "link already exists with different permissions",
            );
        }
        StreamLinkCredential {
            link_id: link.link_id.clone(),
            permissions: link.permissions,
            secret: link.secret.clone(),
        }
    } else {
        let link = test_store_stream_link(
            link_id.clone(),
            test_minted_link_secret(&link_id),
            request.permissions,
        );
        let response = StreamLinkCredential {
            link_id: link.link_id.clone(),
            permissions: link.permissions,
            secret: link.secret.clone(),
        };
        stream.links.push(link);
        response
    };
    drop(streams);
    let mut invalid_json = state
        .link_create_invalid_json_remaining
        .lock()
        .expect("link create body failure lock");
    if *invalid_json > 0 {
        *invalid_json -= 1;
        return (StatusCode::OK, [("content-type", "application/json")], "{").into_response();
    }
    Json(response).into_response()
}

async fn test_list_links(
    State(state): State<Arc<TestApiState>>,
    Path(stream_id): Path<String>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    let fail = {
        let mut remaining = state
            .link_list_failures_remaining
            .lock()
            .expect("link list failure lock");
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
            "temporary link inventory failure",
        );
    }
    let streams = state.streams.lock().expect("streams lock");
    let Some(stream) = streams.get(&stream_id) else {
        return test_error(StatusCode::NOT_FOUND, "not_found", "stream not found");
    };
    if stream.deleted {
        return test_error(StatusCode::CONFLICT, "conflict", "stream is deleted");
    }
    if !test_authorized(stream, &headers, LinkPermissions::allows_owner) {
        return test_error(StatusCode::FORBIDDEN, "forbidden", "owner link required");
    }
    let start = query
        .get("cursor")
        .and_then(|cursor| cursor.parse::<usize>().ok())
        .unwrap_or(0)
        .min(stream.links.len());
    let end = start.saturating_add(2).min(stream.links.len());
    let authorizing_link_id = stream
        .links
        .iter()
        .find(|link| link.active && link.permissions.allows_owner())
        .expect("authorized stream has an active owner link")
        .link_id
        .clone();
    Json(ListLinksResponse {
        authorizing_link_id,
        links: stream
            .links
            .get(start..end)
            .expect("bounded link page")
            .iter()
            .map(|link| StreamLinkSummary {
                link_id: link.link_id.clone(),
                permissions: link.permissions,
                status: if link.active {
                    StreamLinkStatus::Active
                } else {
                    StreamLinkStatus::Revoked
                },
                created_at: "2026-08-07T12:00:00.000Z".to_owned(),
                expires_at: None,
                revoked_at: (!link.active).then(|| "2026-08-07T12:01:00.000Z".to_owned()),
            })
            .collect(),
        next_cursor: (end < stream.links.len()).then(|| end.to_string()),
    })
    .into_response()
}

async fn test_revoke_link(
    State(state): State<Arc<TestApiState>>,
    Path((stream_id, link_id)): Path<(String, LinkId)>,
    headers: HeaderMap,
) -> Response {
    let mut streams = state.streams.lock().expect("streams lock");
    let Some(stream) = streams.get_mut(&stream_id) else {
        return test_error(StatusCode::NOT_FOUND, "not_found", "stream not found");
    };
    if stream.deleted {
        return test_error(StatusCode::CONFLICT, "conflict", "stream is deleted");
    }
    if !test_authorized(stream, &headers, LinkPermissions::allows_owner) {
        return test_error(StatusCode::FORBIDDEN, "forbidden", "owner link required");
    }
    for link in &mut stream.links {
        if link.link_id == link_id {
            link.active = false;
        }
    }
    StatusCode::NO_CONTENT.into_response()
}

async fn test_write_socket(
    State(state): State<Arc<TestApiState>>,
    Path(stream_id): Path<String>,
    ws: WebSocketUpgrade,
) -> Response {
    ws.protocols([TSF_WEBSOCKET_PROTOCOL])
        .on_upgrade(move |socket| test_write_flow(state, stream_id, socket))
}

async fn test_write_flow(state: Arc<TestApiState>, stream_id: String, mut socket: WebSocket) {
    let Some(Ok(Message::Binary(auth))) = socket.recv().await else {
        return;
    };
    let Ok(ClientFrame::OpenWrite {
        client_writer_id,
        link_secret,
        expected_next_seq_num,
    }) = ClientFrame::decode_bytes(auth)
    else {
        return;
    };
    state
        .write_link_secrets
        .lock()
        .expect("write link secrets lock")
        .push(link_secret.expose_secret().to_owned());
    state
        .write_expected_next_seq_nums
        .lock()
        .expect("write preconditions lock")
        .push(expected_next_seq_num);
    {
        let streams = state.streams.lock().expect("streams lock");
        let Some(stream) = streams.get(&stream_id) else {
            return;
        };
        if stream.deleted
            || !stream.links.iter().any(|link| {
                link.active
                    && link.secret.expose_secret() == link_secret.expose_secret()
                    && link.permissions.allows_write()
            })
        {
            return;
        }
    }
    send_server_frame(&mut socket, ServerFrame::Ready)
        .await
        .expect("send ready");

    while let Some(Ok(Message::Binary(append))) = socket.recv().await {
        let Ok(ClientFrame::AppendBatch(records)) = ClientFrame::decode_bytes(append) else {
            return;
        };
        let (writer_start_seq_num, writer_end_seq_num, start_seq_num, end_seq_num) = {
            let mut streams = state.streams.lock().expect("streams lock");
            let Some(stream) = streams.get_mut(&stream_id) else {
                return;
            };
            let start_seq_num = stream.records.len() as u64;
            let Some(writer_start_seq_num) = records.first().map(|record| record.writer_seq_num)
            else {
                return;
            };
            let Some(writer_end_seq_num) = records
                .last()
                .and_then(|record| record.writer_seq_num.checked_add(1))
            else {
                return;
            };
            for record in records {
                let seq_num = stream.records.len() as u64;
                stream.records.push(TestRecord {
                    seq_num,
                    timestamp_ms: 1_781_717_406_000 + seq_num,
                    writer_id: WriterId::from_bytes(*client_writer_id.as_bytes()),
                    writer_seq_num: record.writer_seq_num,
                    part: record.part,
                    format: record.format,
                    data: record.data,
                });
            }
            (
                writer_start_seq_num,
                writer_end_seq_num,
                start_seq_num,
                stream.records.len() as u64,
            )
        };
        send_server_frame(
            &mut socket,
            ServerFrame::AppendAck {
                writer_start_seq_num,
                writer_end_seq_num,
                start_seq_num,
                end_seq_num,
            },
        )
        .await
        .expect("send ack");
    }
}

async fn test_read_socket(
    State(state): State<Arc<TestApiState>>,
    Path(stream_id): Path<String>,
    Query(query): Query<TestReadQuery>,
    ws: WebSocketUpgrade,
) -> Response {
    ws.protocols([TSF_WEBSOCKET_PROTOCOL])
        .on_upgrade(move |socket| test_read_flow(state, stream_id, query, socket))
}

async fn test_read_flow(
    state: Arc<TestApiState>,
    stream_id: String,
    query: TestReadQuery,
    mut socket: WebSocket,
) {
    let Some(Ok(Message::Binary(opening))) = socket.recv().await else {
        return;
    };
    let Ok(ClientFrame::OpenRead {
        link_secret: Some(link_secret),
    }) = ClientFrame::decode_bytes(opening)
    else {
        return;
    };
    let start = query.start();
    let count = query.count;
    let until_timestamp_ms = query.until;
    let (stream_metadata, caught_up, records) = {
        let streams = state.streams.lock().expect("streams lock");
        let Some(stream) = streams.get(&stream_id) else {
            return;
        };
        if stream.deleted
            || !stream.links.iter().any(|link| {
                link.active
                    && link.secret.expose_secret() == link_secret.expose_secret()
                    && link.permissions.allows_read()
            })
        {
            return;
        }
        let caught_up = CaughtUpPosition {
            next_seq_num: stream.records.len() as u64,
            last_timestamp_ms: stream
                .records
                .last()
                .map_or(0, |record| record.timestamp_ms),
        };
        (
            test_read_stream_metadata(stream),
            caught_up,
            test_select_records(stream, start, count, until_timestamp_ms),
        )
    };
    send_server_frame(&mut socket, ServerFrame::Ready)
        .await
        .expect("send ready");
    send_server_frame(&mut socket, ServerFrame::StreamMetadata(stream_metadata))
        .await
        .expect("send stream metadata");
    if !records.is_empty() {
        send_server_frame(
            &mut socket,
            ServerFrame::ReadBatch(
                ReadBatch::try_from_records(
                    records
                        .into_iter()
                        .map(|record| OwnedReadRecord {
                            seq_num: record.seq_num,
                            timestamp_ms: record.timestamp_ms,
                            writer_id: record.writer_id,
                            writer_seq_num: record.writer_seq_num,
                            part: record.part,
                            format: record.format,
                            data: record.data,
                        })
                        .collect(),
                )
                .expect("test records within batch bounds"),
            ),
        )
        .await
        .expect("send records");
    }
    send_server_frame(&mut socket, ServerFrame::CaughtUp(caught_up))
        .await
        .expect("send caught up");
    socket
        .send(Message::Close(None))
        .await
        .expect("close read socket");
}

fn test_store_stream_link(
    link_id: LinkId,
    secret: LinkSecret,
    permissions: LinkPermissions,
) -> TestLink {
    TestLink {
        link_id,
        permissions,
        secret,
        active: true,
    }
}

fn test_minted_link_secret(link_id: &LinkId) -> LinkSecret {
    let mut hasher = DefaultHasher::new();
    link_id.hash(&mut hasher);
    let digest = hasher.finish();
    format!("{digest:016x}{digest:016x}")
        .parse()
        .expect("canonical test link secret")
}

fn test_get_stream_response(stream: &TestStream) -> StreamMetadata {
    StreamMetadata {
        stream_id: stream.stream_id,
        title: stream.title.clone(),
        visibility: stream.visibility,
        created_at: "2026-08-13T00:00:00Z".to_owned(),
        expires_at: stream.expires_at.clone(),
    }
}

fn test_read_stream_metadata(stream: &TestStream) -> StreamMetadata {
    StreamMetadata {
        stream_id: stream.stream_id,
        title: stream.title.clone(),
        visibility: stream.visibility,
        created_at: "2026-08-13T00:00:00Z".to_owned(),
        expires_at: stream.expires_at.clone(),
    }
}

fn test_authorized(
    stream: &TestStream,
    headers: &HeaderMap,
    required: impl Fn(LinkPermissions) -> bool,
) -> bool {
    let Some(link_secret) = test_link_secret(headers) else {
        return false;
    };
    stream.links.iter().any(|link| {
        link.active && link.secret.expose_secret() == link_secret && required(link.permissions)
    })
}

fn test_link_secret(headers: &HeaderMap) -> Option<&str> {
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

fn test_select_records(
    stream: &TestStream,
    start: ReadStart,
    count: Option<u64>,
    until_timestamp_ms: Option<u64>,
) -> Vec<TestRecord> {
    let mut records = stream.records.clone();
    match start {
        ReadStart::SeqNum(seq_num) => records.retain(|record| record.seq_num >= seq_num),
        ReadStart::TimestampMs(timestamp_ms) => {
            records.retain(|record| record.timestamp_ms >= timestamp_ms);
        }
        ReadStart::TailOffset(tail_offset) => {
            let tail_offset = usize::try_from(tail_offset).unwrap_or(usize::MAX);
            let start = records.len().saturating_sub(tail_offset);
            records = records[start..].to_vec();
        }
    }
    if let Some(until_timestamp_ms) = until_timestamp_ms {
        records.retain(|record| record.timestamp_ms < until_timestamp_ms);
    }
    if let Some(count) = count {
        records.truncate(usize::try_from(count).unwrap_or(usize::MAX));
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

fn normalize_created_stream_output(output: &str) -> String {
    output
        .lines()
        .map(|line| {
            if let Some(rest) = line.strip_prefix("Created ") {
                let visibility = rest.split_whitespace().next().unwrap_or_default();
                format!("Created {visibility} stream <stream_id>")
            } else if line.starts_with("Expires:") {
                "Expires: <timestamp>".to_owned()
            } else if line.starts_with(|c: char| c.is_ascii_digit()) && line.contains(" durable") {
                if line.contains(" · read ") {
                    "<records> durable · read <url>".to_owned()
                } else {
                    "<records> durable".to_owned()
                }
            } else {
                let mut links = line.split_whitespace();
                match (links.next(), links.next(), links.next()) {
                    (Some(label), Some(permission), Some(url)) if url.starts_with("http") => {
                        let suffix = match links.next() {
                            Some("(keep") => " (keep private)",
                            Some("(public)") => " (public)",
                            _ => "",
                        };
                        format!("  {label} {permission} <url>{suffix}")
                    }
                    _ => line.to_owned(),
                }
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
        + "\n"
}

fn extract_link_line<'a>(line: &'a str, link_id: &str) -> Option<&'a str> {
    let mut links = line.split_whitespace();
    if links.next()? != link_id {
        return None;
    }
    let _permission = links.next()?;
    links.next().filter(|url| url.starts_with("http"))
}

fn created_link_url<'a>(json: &'a serde_json::Value, link_id: &str) -> &'a str {
    json["links"]
        .as_array()
        .and_then(|links| links.iter().find(|link| link["link_id"] == link_id))
        .and_then(|link| link["url"].as_str())
        .expect("created link URL")
}

fn assert_created_links_parse(output: &str, expected_links: &[(&str, &str)]) {
    let stream_id = output
        .lines()
        .find_map(|line| line.strip_prefix("Created "))
        .and_then(|rest| rest.split_whitespace().last())
        .expect("created stream line");
    for (label, permission) in expected_links {
        let url = output
            .lines()
            .find_map(|line| extract_link_line(line, label))
            .expect("permission URL line");
        let locator = StreamLocator::parse(url).expect("stream URL parses");
        assert_eq!(locator.stream_id.to_string(), stream_id);
        let permissions = permission
            .parse::<LinkPermissions>()
            .expect("expected permission parses");
        assert!(
            locator
                .link_declaring(|link_permissions| link_permissions == permissions)
                .is_some(),
            "URL for {permission} did not contain a matching link"
        );
    }
}

async fn connect_default_writer(api_url: &Url) -> tailsurf::TsfWriter {
    let stream_id = "0123456789abcdefghjkmnpqrstvwxyz"
        .parse::<StreamId>()
        .expect("stream id");
    TsfClient::with_api_origin(api_url.clone())
        .expect("valid API origin")
        .connect_writer(WriteStreamOptions::new(
            stream_id,
            ClientWriterId::new_random(),
            canonical_test_link_secret(),
        ))
        .await
        .expect("writer")
}

fn test_write_record(writer_seq_num: u64, data: Bytes) -> AppendRecord {
    AppendRecord::new(
        writer_seq_num,
        PartHeader::unsplit(),
        RecordFormat::Bytes,
        data,
    )
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

struct HoldingWriteState {
    expected_before_ack: usize,
    attempts: Mutex<Vec<HoldingWriteAttempt>>,
    connections: Mutex<usize>,
    disconnect_first_batch: bool,
    release_acknowledgements: Notify,
}

#[derive(Clone)]
struct HoldingWriteAttempt {
    client_writer_id: ClientWriterId,
    writer_seq_num: u64,
    data: Bytes,
}

struct HoldingWriteServer {
    api_url: Url,
    state: Arc<HoldingWriteState>,
    _task: AbortOnDrop,
}

impl HoldingWriteServer {
    async fn start(expected_before_ack: usize) -> Self {
        Self::start_with_mode(expected_before_ack, false).await
    }

    async fn start_reconnecting(expected_before_ack: usize) -> Self {
        Self::start_with_mode(expected_before_ack, true).await
    }

    async fn start_with_mode(expected_before_ack: usize, disconnect_first_batch: bool) -> Self {
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
        let (api_url, task) = start_server(router).await;
        Self {
            api_url,
            state,
            _task: task,
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
    let connection_index = {
        let mut connections = state.connections.lock().expect("connections lock");
        let connection_index = *connections;
        *connections += 1;
        connection_index
    };
    if send_server_frame(&mut socket, ServerFrame::Ready)
        .await
        .is_err()
    {
        return;
    }

    let mut received = 0;
    while received < state.expected_before_ack {
        let Some(Ok(Message::Binary(append))) = socket.recv().await else {
            return;
        };
        let Ok(ClientFrame::AppendBatch(records)) = ClientFrame::decode_bytes(append) else {
            return;
        };
        if records.is_empty() || records.len() > state.expected_before_ack - received {
            return;
        }
        received += records.len();
        state
            .attempts
            .lock()
            .expect("attempts lock")
            .extend(records.into_iter().map(|record| HoldingWriteAttempt {
                client_writer_id,
                writer_seq_num: record.writer_seq_num,
                data: record.data,
            }));
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
                client_writer_id,
                writer_seq_num: record.writer_seq_num,
                data: record.data,
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
    format: RecordFormat,
    data: Bytes,
}

struct FakeWriteState {
    append_attempts: Mutex<Vec<AppendAttempt>>,
    terminal: bool,
}

struct FakeWriteServer {
    api_url: Url,
    state: Arc<FakeWriteState>,
    _task: AbortOnDrop,
}

impl FakeWriteServer {
    async fn start() -> Self {
        Self::start_with_mode(false).await
    }

    async fn start_terminal() -> Self {
        Self::start_with_mode(true).await
    }

    async fn start_with_mode(terminal: bool) -> Self {
        let state = Arc::new(FakeWriteState {
            append_attempts: Mutex::new(Vec::new()),
            terminal,
        });
        let router = Router::new()
            .route("/api/v1/streams/{stream_id}/write", get(fake_write_socket))
            .with_state(state.clone());
        let (api_url, task) = start_server(router).await;
        Self {
            api_url,
            state,
            _task: task,
        }
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
    send_server_frame(&mut socket, ServerFrame::Ready)
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
            format: record.format,
            data: record.data,
        });
        attempts.len()
    };

    if attempt_count == 1 {
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
        socket
            .send(Message::Close(None))
            .await
            .expect("close first attempt");
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
    authorization: Option<String>,
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

struct FakeSseServer {
    api_url: Url,
    state: Arc<FakeSseState>,
    _task: AbortOnDrop,
}

impl FakeSseServer {
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
        let (api_url, task) = start_server(router).await;
        Self {
            api_url,
            state,
            _task: task,
        }
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
            authorization: request
                .headers()
                .get("authorization")
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
            "{{\"seq_num\":\"{seq_num}\",\"timestamp_ms\":\"1781717406000\",\"writer_id\":\"AAAAAAAAAAAAAAAAAAAAAA\",\"writer_seq_num\":\"{seq_num}\",\"part\":{{\"index\":0,\"is_final\":true}},\"format\":\"transcript\",\"data\":{{\"encoding\":\"utf8\",\"value\":\"{value}\"}}}}"
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
            record(0, "one\\n"),
            record(1, "two\\n"),
            record(2, "three\\n"),
        ),
    };
    let body = format!(
        "event: stream_metadata\ndata: {{\"stream_id\":\"{stream_id}\",\"title\":null,\"visibility\":\"private\",\"created_at\":\"2026-08-13T00:00:00Z\",\"expires_at\":\"2026-08-23T00:00:00Z\"}}\n\n{events}"
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
    Reconnect,
    ReconnectAfterBatch,
    ReconnectAfterEmptyCaughtUp,
    ReconnectBeforeFirstDefault,
    ReconnectTwiceThenRecord,
    SlowReconnectForever,
    SilentThenRecord,
    ReplayBinary,
    ReplaySplitRecord,
}

struct FakeReadServer {
    api_url: Url,
    state: Arc<FakeReadState>,
    _task: AbortOnDrop,
}

impl FakeReadServer {
    async fn start(mode: FakeReadMode) -> Self {
        let state = Arc::new(FakeReadState {
            read_attempts: Mutex::new(Vec::new()),
            mode,
        });
        let router = Router::new()
            .route("/api/v1/streams/{stream_id}/read", get(fake_read_socket))
            .with_state(state.clone());
        let (api_url, task) = start_server(router).await;
        Self {
            api_url,
            state,
            _task: task,
        }
    }

    fn read_attempts(&self) -> Vec<ReadAttempt> {
        self.state
            .read_attempts
            .lock()
            .expect("read attempts lock")
            .clone()
    }
}

fn fake_stream_metadata(stream_id: &str) -> StreamMetadata {
    StreamMetadata {
        stream_id: stream_id.parse().expect("fake stream ID"),
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
    send_server_frame(&mut socket, ServerFrame::Ready)
        .await
        .expect("send ready");
    send_server_frame(
        &mut socket,
        ServerFrame::StreamMetadata(fake_stream_metadata(&stream_id)),
    )
    .await
    .expect("send stream metadata");
    match state.mode {
        FakeReadMode::Reconnect => {
            let first_seq_num = match start {
                ReadStart::SeqNum(value) => value,
                ReadStart::TimestampMs(_) | ReadStart::TailOffset(_) => 0,
            };
            if attempt_count == 1 {
                send_read_record(&mut socket, first_seq_num, 0, b"first\n").await;
                close_retryable_read(&mut socket).await;
            } else {
                send_read_record(&mut socket, first_seq_num, 1, b"second\n").await;
            }
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
                                (0, 0, b"one\n".as_slice()),
                                (1, 1, b"two\n".as_slice()),
                                (2, 2, b"three\n".as_slice()),
                            ]
                            .into_iter()
                            .map(|(seq_num, writer_seq_num, data)| OwnedReadRecord {
                                seq_num,
                                timestamp_ms: 1_781_717_406_000 + seq_num,
                                writer_id: WriterId::from_bytes([7; WriterId::BYTE_LEN]),
                                writer_seq_num,
                                part: PartHeader::unsplit(),
                                format: RecordFormat::Transcript,
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
                send_read_record(&mut socket, 3, 3, b"four\n").await;
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
                send_read_record(&mut socket, 5, 0, b"stable\n").await;
            }
        }
        FakeReadMode::ReconnectBeforeFirstDefault => {
            if attempt_count == 1 {
                close_retryable_read(&mut socket).await;
            } else {
                send_read_record(&mut socket, 20, 0, b"default\n").await;
            }
        }
        FakeReadMode::ReconnectTwiceThenRecord => {
            if attempt_count < 3 {
                close_retryable_read(&mut socket).await;
            } else {
                send_read_record(&mut socket, 0, 0, b"recovered\n").await;
            }
        }
        FakeReadMode::SlowReconnectForever => {
            sleep(Duration::from_millis(40)).await;
            close_retryable_read(&mut socket).await;
        }
        FakeReadMode::SilentThenRecord => {
            if attempt_count == 1 {
                sleep(Duration::from_secs(5)).await;
            } else {
                send_read_record(&mut socket, 0, 0, b"after idle\n").await;
            }
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
    send_read_record_with_format(
        socket,
        seq_num,
        writer_seq_num,
        PartHeader::unsplit(),
        RecordFormat::Transcript,
        data,
    )
    .await
}

async fn send_read_record_with_format(
    socket: &mut WebSocket,
    seq_num: u64,
    writer_seq_num: u64,
    part: PartHeader,
    format: RecordFormat,
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
                format,
                data: Bytes::copy_from_slice(data),
            }])
            .expect("test record within batch bounds"),
        ),
    )
    .await
    .expect("send read record");
}
