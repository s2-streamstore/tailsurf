//! Cross-language REST v1 fixture coverage.

use serde_json::Value;
use tailsurf::protocol::rest::{
    ApiErrorResponse, AppendRange, AppendRecordsRequest, CreateStreamRequest, MAX_LINK_PAGE_ITEMS,
    MAX_REST_ERROR_RESPONSE_BYTES, MAX_REST_RESPONSE_BYTES, MAX_SSE_EVENT_BYTES,
    MAX_SSE_READ_BATCH_PAYLOAD_BYTES, MAX_SSE_READ_BATCH_RECORDS, MAX_SSE_UNTERMINATED_EVENT_BYTES,
    MAX_STATELESS_APPEND_JSON_BYTES, MAX_STATELESS_APPEND_PAYLOAD_BYTES,
    MAX_STATELESS_APPEND_RECORDS, SseCaughtUpData, SseReadBatchData, StreamMetadata,
};

#[test]
fn rest_v1_fixtures_decode_forward_compatibly() {
    let fixtures: Value =
        serde_json::from_str(include_str!("../fixtures/rest-v1.json")).expect("REST fixture JSON");
    assert_eq!(
        fixture_usize(&fixtures, "max_stateless_append_records"),
        MAX_STATELESS_APPEND_RECORDS
    );
    assert_eq!(
        fixture_usize(&fixtures, "max_stateless_append_payload_bytes"),
        MAX_STATELESS_APPEND_PAYLOAD_BYTES
    );
    assert_eq!(
        fixture_usize(&fixtures, "max_stateless_append_json_bytes"),
        MAX_STATELESS_APPEND_JSON_BYTES
    );
    assert_eq!(
        fixture_usize(&fixtures, "max_rest_response_bytes"),
        MAX_REST_RESPONSE_BYTES
    );
    assert_eq!(
        fixture_usize(&fixtures, "max_rest_error_response_bytes"),
        MAX_REST_ERROR_RESPONSE_BYTES
    );
    assert_eq!(
        fixture_usize(&fixtures, "max_link_page_items"),
        MAX_LINK_PAGE_ITEMS
    );
    assert_eq!(
        fixture_usize(&fixtures, "max_sse_read_batch_records"),
        MAX_SSE_READ_BATCH_RECORDS
    );
    assert_eq!(
        fixture_usize(&fixtures, "max_sse_read_batch_payload_bytes"),
        MAX_SSE_READ_BATCH_PAYLOAD_BYTES
    );
    assert_eq!(
        fixture_usize(&fixtures, "max_sse_event_bytes"),
        MAX_SSE_EVENT_BYTES
    );
    assert_eq!(
        fixture_usize(&fixtures, "max_sse_unterminated_event_bytes"),
        MAX_SSE_UNTERMINATED_EVENT_BYTES
    );
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
    let error: ApiErrorResponse = fixture(&fixtures, "error_response");
    assert_eq!(error.error.code, "sequence_mismatch");
    assert_eq!(error.error.actual_next_seq_num, Some(9));
    let records: SseReadBatchData = fixture(&fixtures, "sse_read_batch");
    assert_eq!(records.records.len(), 1);
    assert_eq!(fixtures["sse_resume_cursor"].as_str(), Some("v1,2,2"));
    let caught_up: SseCaughtUpData = fixture(&fixtures, "sse_caught_up");
    assert_eq!(caught_up.next_seq_num, 8);
}

fn fixture<T: serde::de::DeserializeOwned>(fixtures: &Value, name: &str) -> T {
    serde_json::from_value(fixtures[name].clone()).expect("valid REST fixture")
}

fn fixture_usize(fixtures: &Value, name: &str) -> usize {
    fixtures[name].as_u64().expect("numeric limit") as usize
}
