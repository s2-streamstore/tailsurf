//! JSON models for the REST v1 management and HTTP data planes.

use rand::Rng;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{
    LinkId, LinkPermissions, LinkSecret, StreamId, StreamTitle, ids::encode_base64url_32,
    protocol::ws::frame::RecordFormat,
};

/// Whether a stream requires read authorization.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Visibility {
    /// Reads require a read-capable stream link.
    #[default]
    Private,
    /// Reads are anonymous; writes and management still require authorization.
    Public,
}

/// Options for creating a stream.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CreateStreamRequest {
    /// Optional human-facing title.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<StreamTitle>,
    /// Initial stream visibility. Defaults to private.
    #[serde(default)]
    pub visibility: Visibility,
    /// Requested lifetime in seconds, or the service default when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_in_seconds: Option<u64>,
    /// Prepared initial links. At least one must be an owner. At most three are allowed.
    pub links: Vec<InitialStreamLink>,
}

impl Default for CreateStreamRequest {
    fn default() -> Self {
        Self {
            title: None,
            visibility: Visibility::Private,
            expires_in_seconds: None,
            links: vec![InitialStreamLink::new(
                "owner".parse().expect("default owner Link ID is valid"),
                LinkPermissions::owner(),
            )],
        }
    }
}

/// One link requested atomically with stream creation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InitialStreamLink {
    /// Client-chosen immutable Link ID.
    pub link_id: LinkId,
    /// Permissions carried by the link.
    pub permissions: LinkPermissions,
    /// Client-generated secret retained with this prepared request.
    #[serde(serialize_with = "crate::ids::serialize_link_secret")]
    pub secret: LinkSecret,
}

impl InitialStreamLink {
    /// Creates one initial link with an independent random secret.
    pub fn new(link_id: LinkId, permissions: LinkPermissions) -> Self {
        Self {
            link_id,
            permissions,
            secret: random_link_secret(),
        }
    }
}

/// A stream link credential returned during stream creation or link creation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StreamLinkCredential {
    /// Stable non-secret link identifier used for revocation.
    pub link_id: LinkId,
    /// Effective permissions carried by the link.
    pub permissions: LinkPermissions,
    /// Secret link value returned only by the creating request.
    #[serde(serialize_with = "crate::ids::serialize_link_secret")]
    pub secret: LinkSecret,
}

/// Created stream metadata and its atomically created link credentials.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CreateStreamResponse {
    /// Stable stream identifier.
    pub stream_id: StreamId,
    /// Human-facing title when one has been set.
    #[serde(deserialize_with = "deserialize_nullable_stream_title")]
    pub title: Option<StreamTitle>,
    /// Initial visibility.
    pub visibility: Visibility,
    /// Absolute RFC 3339 stream creation timestamp.
    pub created_at: String,
    /// Absolute RFC 3339 stream expiration timestamp.
    pub expires_at: String,
    /// Newly created link credentials.
    pub links: Vec<StreamLinkCredential>,
}

/// Options for creating a stream link.
#[derive(Clone, Debug, Serialize)]
pub struct CreateLinkRequest {
    /// Client-generated stable link identifier carried in the request path.
    #[serde(skip_serializing)]
    pub link_id: LinkId,
    /// Client-generated secret. The same request can be retried safely.
    #[serde(serialize_with = "crate::ids::serialize_link_secret")]
    pub secret: LinkSecret,
    /// Permissions carried by the new link.
    pub permissions: LinkPermissions,
    /// Optional RFC 3339 expiration timestamp.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<String>,
}

impl CreateLinkRequest {
    /// Creates retry-safe link material.
    pub fn new(link_id: LinkId, permissions: LinkPermissions, expires_at: Option<String>) -> Self {
        Self {
            link_id,
            secret: random_link_secret(),
            permissions,
            expires_at,
        }
    }
}

fn random_link_secret() -> LinkSecret {
    let mut secret = [0_u8; 32];
    rand::rng().fill_bytes(&mut secret);
    encode_base64url_32(&secret).into()
}

/// Effective lifecycle state for a stream link.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StreamLinkStatus {
    /// The link can be authorized.
    Active,
    /// The link reached its configured expiration.
    Expired,
    /// The link was explicitly revoked.
    Revoked,
}

/// Non-secret metadata for one stream link.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StreamLinkSummary {
    /// Stable non-secret link identifier used for revocation.
    pub link_id: LinkId,
    /// Effective permissions carried by the link.
    pub permissions: LinkPermissions,
    /// Current effective lifecycle state.
    pub status: StreamLinkStatus,
    /// RFC 3339 creation timestamp.
    pub created_at: String,
    /// RFC 3339 expiration timestamp when configured.
    pub expires_at: Option<String>,
    /// RFC 3339 revocation timestamp when inactive.
    pub revoked_at: Option<String>,
    /// Whether this link authenticated the inventory request.
    pub is_current: bool,
}

/// Non-secret link inventory for a stream.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ListLinksResponse {
    /// Retained link metadata ordered newest first.
    pub links: Vec<StreamLinkSummary>,
    /// Opaque cursor for the next page.
    pub next_cursor: Option<String>,
}

/// Current stream metadata.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StreamMetadata {
    /// Stable stream identifier.
    pub stream_id: StreamId,
    /// Human-facing title when one has been set.
    #[serde(deserialize_with = "deserialize_nullable_stream_title")]
    pub title: Option<StreamTitle>,
    /// Current visibility.
    pub visibility: Visibility,
    /// Absolute RFC 3339 stream creation timestamp.
    pub created_at: String,
    /// Absolute RFC 3339 stream expiration timestamp.
    pub expires_at: String,
}

/// One split record part carried by JSON data-plane APIs.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RestRecordPart {
    /// Zero-based part index.
    pub index: u32,
    /// Whether this part ends the logical record.
    pub is_final: bool,
}

/// JSON encoding for exact record bytes.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "encoding", content = "value", rename_all = "lowercase")]
pub enum RecordData {
    /// UTF-8 text encoded directly in JSON.
    Utf8(String),
    /// Canonical unpadded base64url bytes.
    Base64url(String),
}

/// One record in a stateless atomic append.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AppendJsonRecord {
    /// Split-part metadata, or an implicit unsplit record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub part: Option<RestRecordPart>,
    /// Presentation hint for the payload.
    pub format: RecordFormat,
    /// Exact record bytes and their JSON encoding.
    pub data: RecordData,
}

/// Stateless durable append request.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AppendRecordsRequest {
    /// Canonical client writer ID.
    pub client_writer_id: String,
    /// Writer-local sequence assigned to the first record.
    #[serde(with = "decimal_u64")]
    pub writer_start_seq_num: u64,
    /// Atomic record batch.
    pub records: Vec<AppendJsonRecord>,
    /// Optional expected current exclusive stream end.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "optional_decimal_u64"
    )]
    pub expected_next_seq_num: Option<u64>,
}

/// Durable half-open physical sequence range for an atomic append.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AppendRecordsResponse {
    /// First durable physical sequence number.
    #[serde(with = "decimal_u64")]
    pub start_seq_num: u64,
    /// Exclusive end of the appended physical sequence range.
    #[serde(with = "decimal_u64")]
    pub end_seq_num: u64,
}

/// One record in a batched SSE `records` event.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize)]
pub struct SseReadRecord {
    /// Absolute physical sequence number.
    #[serde(with = "decimal_u64")]
    pub seq_num: u64,
    /// Record timestamp in Unix milliseconds.
    #[serde(with = "decimal_u64")]
    pub timestamp_ms: u64,
    /// Canonical client writer ID.
    pub writer_id: String,
    /// Writer-local sequence number.
    #[serde(with = "decimal_u64")]
    pub writer_seq_num: u64,
    /// Split-part metadata.
    pub part: RestRecordPart,
    /// Presentation hint for the payload.
    pub format: RecordFormat,
    /// Exact record bytes and their JSON encoding.
    pub data: RecordData,
}

/// Payload of a batched SSE `records` event.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize)]
pub struct SseRecordsEvent {
    /// Ordered records in this event.
    pub records: Vec<SseReadRecord>,
}

/// Payload of an SSE `caught_up` event.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize)]
pub struct SseCaughtUpEvent {
    /// Next safe reconnect sequence.
    #[serde(with = "decimal_u64")]
    pub next_seq_num: u64,
    /// Last record timestamp at the captured boundary.
    #[serde(with = "decimal_u64")]
    pub last_timestamp_ms: u64,
}

/// Payload of an SSE `snapshot_boundary` event.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize)]
pub struct SseSnapshotBoundaryEvent {
    /// Exclusive end of the fixed snapshot.
    #[serde(with = "decimal_u64")]
    pub end_seq_num: u64,
    /// Timestamp of the last record at the snapshot boundary.
    #[serde(with = "decimal_u64")]
    pub last_timestamp_ms: u64,
}

mod decimal_u64 {
    use serde::{Deserialize, Deserializer, Serializer};
    pub fn serialize<S: Serializer>(value: &u64, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&value.to_string())
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
        let value = String::deserialize(deserializer)?;
        if value != "0" && value.starts_with('0') {
            return Err(serde::de::Error::custom("non-canonical decimal u64"));
        }
        value.parse().map_err(serde::de::Error::custom)
    }
}

mod optional_decimal_u64 {
    use serde::{Deserialize, Deserializer, Serializer};
    pub fn serialize<S: Serializer>(value: &Option<u64>, serializer: S) -> Result<S::Ok, S::Error> {
        match value {
            Some(value) => serializer.serialize_some(&value.to_string()),
            None => serializer.serialize_none(),
        }
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<u64>, D::Error> {
        let value = Option::<String>::deserialize(deserializer)?;
        value
            .map(|value| {
                if value != "0" && value.starts_with('0') {
                    return Err(serde::de::Error::custom("non-canonical decimal u64"));
                }
                value.parse().map_err(serde::de::Error::custom)
            })
            .transpose()
    }
}

/// Mutable stream settings. Absent fields are preserved.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct UpdateStreamRequest {
    /// Title mutation. An absent field preserves it and JSON `null` clears it.
    #[serde(
        default,
        skip_serializing_if = "StreamTitleUpdate::is_unchanged",
        serialize_with = "serialize_stream_title_update",
        deserialize_with = "deserialize_stream_title_update"
    )]
    pub title: StreamTitleUpdate,
    /// New visibility when present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub visibility: Option<Visibility>,
    /// Later absolute RFC 3339 expiration timestamp when renewing the stream.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<String>,
}

/// Three-state stream title mutation for PATCH requests.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum StreamTitleUpdate {
    /// Preserve the current title by omitting the wire field.
    #[default]
    Unchanged,
    /// Replace the current title.
    Set(StreamTitle),
    /// Clear the current title by sending JSON `null`.
    Clear,
}

impl StreamTitleUpdate {
    fn is_unchanged(&self) -> bool {
        matches!(self, Self::Unchanged)
    }
}

fn serialize_stream_title_update<S>(
    update: &StreamTitleUpdate,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match update {
        StreamTitleUpdate::Set(title) => serializer.serialize_some(title),
        StreamTitleUpdate::Clear => serializer.serialize_none(),
        StreamTitleUpdate::Unchanged => serializer.serialize_unit(),
    }
}

fn deserialize_stream_title_update<'de, D>(deserializer: D) -> Result<StreamTitleUpdate, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(match Option::<StreamTitle>::deserialize(deserializer)? {
        Some(title) => StreamTitleUpdate::Set(title),
        None => StreamTitleUpdate::Clear,
    })
}

fn deserialize_nullable_stream_title<'de, D>(
    deserializer: D,
) -> Result<Option<StreamTitle>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<StreamTitle>::deserialize(deserializer)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn prepares_a_default_owner_for_stream_creation() {
        let request = CreateStreamRequest::default();
        let value = serde_json::to_value(request).expect("serialize create request");

        assert_eq!(value["visibility"], "private");
        assert_eq!(value["links"][0]["link_id"], "owner");
        assert_eq!(value["links"][0]["permissions"], "o");
        assert!(value["links"][0]["secret"].is_string());
    }

    #[test]
    fn serializes_requested_stream_lifetime() {
        let request = CreateStreamRequest {
            expires_in_seconds: Some(604_800),
            ..CreateStreamRequest::default()
        };
        let value = serde_json::to_value(request).expect("serialize create request");
        assert_eq!(value["expires_in_seconds"], json!(604_800));
        assert_eq!(
            serde_json::from_value::<CreateStreamRequest>(value)
                .expect("deserialize create request")
                .expires_in_seconds,
            Some(604_800)
        );
    }

    #[test]
    fn serializes_link_mutations_and_omits_absent_stream_update() {
        let link = CreateLinkRequest::new(
            "reader".parse().expect("Link ID"),
            LinkPermissions::read(),
            None,
        );
        let link = serde_json::to_value(link).expect("serialize link request");
        assert_eq!(link["permissions"], "r");
        assert!(link.get("link_id").is_none());
        assert!(link["secret"].is_string());
        assert_eq!(
            serde_json::to_value(UpdateStreamRequest::default()).expect("serialize update request"),
            json!({})
        );
        assert_eq!(
            serde_json::to_value(UpdateStreamRequest {
                title: StreamTitleUpdate::Clear,
                ..UpdateStreamRequest::default()
            })
            .expect("serialize title clear request"),
            json!({ "title": null })
        );
        assert_eq!(
            serde_json::from_value::<UpdateStreamRequest>(json!({ "title": "Deploy log" }))
                .expect("deserialize title update")
                .title,
            StreamTitleUpdate::Set("Deploy log".parse().expect("title"))
        );
        assert_eq!(
            serde_json::from_value::<UpdateStreamRequest>(json!({ "title": null }))
                .expect("deserialize title clear")
                .title,
            StreamTitleUpdate::Clear
        );
    }
}
