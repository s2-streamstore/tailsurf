//! Cross-language REST v1 fixture coverage.

use serde_json::Value;
use tailsurf::protocol::rest::{
    AppendRange, AppendRecordsRequest, CreateStreamRequest, SseCaughtUpEvent, SseReadBatchEvent,
    SseSnapshotBoundaryEvent, StreamMetadata,
};

#[test]
fn rest_v1_fixtures_decode_forward_compatibly() {
    let fixtures: Value =
        serde_json::from_str(include_str!("../fixtures/rest-v1.json")).expect("REST fixture JSON");
    let create: CreateStreamRequest = fixture(&fixtures, "create_request");
    assert_eq!(create.links.len(), 2);
    let stream: StreamMetadata = fixture(&fixtures, "stream_resource");
    assert_eq!(stream.created_at, "2026-08-13T00:00:00Z");
    let append: AppendRecordsRequest = fixture(&fixtures, "append_request");
    assert_eq!(append.client_writer_id, "AAECAwQFBgcICQoLDA0ODw");
    assert_eq!(append.records.len(), 2);
    let acknowledgement: AppendRange = fixture(&fixtures, "append_response");
    assert_eq!(
        (acknowledgement.start_seq_num, acknowledgement.end_seq_num),
        (7, 9)
    );
    let records: SseReadBatchEvent = fixture(&fixtures, "sse_read_batch");
    assert_eq!(records.records.len(), 1);
    assert_eq!(
        fixtures["sse_resume_cursor"].as_str(),
        Some("v1,2,2,2,1786579200000")
    );
    assert_eq!(
        fixtures["sse_snapshot_cursor"].as_str(),
        Some("v1,0,0,2,1786579200000")
    );
    let snapshot: SseSnapshotBoundaryEvent = fixture(&fixtures, "sse_snapshot_boundary");
    assert_eq!(
        (snapshot.end_seq_num, snapshot.last_timestamp_ms),
        (2, 1_786_579_200_000)
    );
    let caught_up: SseCaughtUpEvent = fixture(&fixtures, "sse_caught_up");
    assert_eq!(caught_up.next_seq_num, 8);
}

fn fixture<T: serde::de::DeserializeOwned>(fixtures: &Value, name: &str) -> T {
    serde_json::from_value(fixtures[name].clone()).expect("valid REST fixture")
}
