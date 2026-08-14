//! Cross-language REST v1 fixture coverage.

use serde_json::Value;
use tailsurf::protocol::rest::{
    AppendRecordsRequest, AppendRecordsResponse, CreateStreamRequest, SseCaughtUpEvent,
    SseRecordsEvent, StreamInfoResponse,
};

#[test]
fn rest_v1_fixtures_decode_forward_compatibly() {
    let fixtures: Value =
        serde_json::from_str(include_str!("../fixtures/rest-v1.json")).expect("REST fixture JSON");
    let create: CreateStreamRequest = fixture(&fixtures, "create_request");
    assert_eq!(create.issue_links.len(), 2);
    let stream: StreamInfoResponse = fixture(&fixtures, "stream_resource");
    assert_eq!(stream.created_at, "2026-08-13T00:00:00Z");
    let append: AppendRecordsRequest = fixture(&fixtures, "append_request");
    assert_eq!(append.records.len(), 2);
    let acknowledgement: AppendRecordsResponse = fixture(&fixtures, "append_response");
    assert_eq!((acknowledgement.seq_start, acknowledgement.seq_end), (7, 8));
    let records: SseRecordsEvent = fixture(&fixtures, "sse_records");
    assert_eq!(records.records.len(), 1);
    let caught_up: SseCaughtUpEvent = fixture(&fixtures, "sse_caught_up");
    assert_eq!(caught_up.next_seq_num, 8);
}

fn fixture<T: serde::de::DeserializeOwned>(fixtures: &Value, name: &str) -> T {
    serde_json::from_value(fixtures[name].clone()).expect("valid REST fixture")
}
