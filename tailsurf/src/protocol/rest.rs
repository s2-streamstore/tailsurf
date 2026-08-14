//! JSON models for the REST v1 management and HTTP data planes.

use rand::Rng;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::ids::encode_base64url_32;
use crate::{LinkId, LinkLabel, LinkPermissions, LinkSecret, StreamId, StreamTitle};

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
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CreateStreamRequest {
    /// Secret recovery material retained with one pending create request.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_optional_link_secret"
    )]
    pub recovery_secret: Option<LinkSecret>,
    /// Optional human-facing title.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<StreamTitle>,
    /// Initial stream visibility. Defaults to private.
    #[serde(default)]
    pub visibility: Visibility,
    /// Requested lifetime in seconds, or the service default when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_in_secs: Option<u64>,
    /// Requested initial link permissions. The service adds an owner link when absent. At most
    /// three effective links are allowed, including the owner.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub issue_links: Option<Vec<InitialStreamLink>>,
}

fn serialize_optional_link_secret<S>(
    secret: &Option<LinkSecret>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match secret {
        Some(secret) => crate::ids::serialize_link_secret(secret, serializer),
        None => serializer.serialize_none(),
    }
}

/// One link requested atomically with stream creation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InitialStreamLink {
    /// Owner-visible label for the link.
    pub label: LinkLabel,
    /// Permissions carried by the link.
    pub permissions: LinkPermissions,
}

/// A stream link issued during stream creation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IssuedStreamLink {
    /// Stable non-secret link identifier used for revocation.
    pub link_id: LinkId,
    /// Owner-visible label for the link.
    pub label: LinkLabel,
    /// Effective permissions carried by the link.
    pub permissions: LinkPermissions,
    /// Secret link value, returned only when issued.
    #[serde(serialize_with = "crate::ids::serialize_link_secret")]
    pub secret: LinkSecret,
}

/// Created stream metadata and any atomically issued links.
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
    /// Newly issued secret links.
    pub links: Vec<IssuedStreamLink>,
}

/// Options for issuing a stream link.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IssueLinkRequest {
    /// Client-generated stable link identifier.
    pub link_id: LinkId,
    /// Client-generated secret. The same request can be retried safely.
    #[serde(serialize_with = "crate::ids::serialize_link_secret")]
    pub secret: LinkSecret,
    /// Owner-visible label for the new link.
    pub label: LinkLabel,
    /// Permissions carried by the new link.
    pub permissions: LinkPermissions,
    /// Optional RFC 3339 expiration timestamp.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<String>,
}

impl IssueLinkRequest {
    /// Creates retry-safe link issuance material.
    pub fn new(label: LinkLabel, permissions: LinkPermissions, expires_at: Option<String>) -> Self {
        let mut secret = [0_u8; 32];
        rand::rng().fill_bytes(&mut secret);
        Self {
            link_id: LinkId::generate(),
            secret: encode_base64url_32(&secret).into(),
            label,
            permissions,
            expires_at,
        }
    }
}

/// A newly issued stream link.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IssueLinkResponse {
    /// Stable non-secret link identifier used for revocation.
    pub link_id: LinkId,
    /// Owner-visible label for the link.
    pub label: LinkLabel,
    /// Effective permissions carried by the link.
    pub permissions: LinkPermissions,
    /// Secret link value, returned only when issued.
    #[serde(serialize_with = "crate::ids::serialize_link_secret")]
    pub secret: LinkSecret,
}

/// A request to rename one stream link.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RenameLinkRequest {
    /// New owner-visible label.
    pub label: LinkLabel,
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

/// Non-secret metadata for one issued stream link.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StreamLinkSummary {
    /// Stable non-secret link identifier used for revocation.
    pub link_id: LinkId,
    /// Owner-visible label for the link.
    pub label: LinkLabel,
    /// Effective permissions carried by the link.
    pub permissions: LinkPermissions,
    /// Current effective lifecycle state.
    pub status: StreamLinkStatus,
    /// RFC 3339 issuance timestamp.
    pub issued_at: String,
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
pub struct StreamInfoResponse {
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

/// Strict tagged JSON record data for append requests.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum AppendRecordData {
    /// UTF-8 transcript data.
    Text {
        /// UTF-8 content.
        text: String,
    },
    /// Canonical unpadded base64url bytes.
    Bytes {
        /// Canonical unpadded base64url content.
        base64: String,
        /// `bytes` or `transcript`.
        #[serde(default = "default_bytes_format")]
        format: String,
    },
}

fn default_bytes_format() -> String {
    "bytes".to_owned()
}

/// One record in a stateless atomic append.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AppendJsonRecord {
    /// Tagged data payload.
    #[serde(flatten)]
    pub data: AppendRecordData,
    /// Split-part metadata, or an implicit unsplit record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub part: Option<RestRecordPart>,
}

/// Stateless durable append request.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AppendRecordsRequest {
    /// Canonical client writer ID.
    pub writer_id: String,
    /// Writer-local sequence assigned to the first record.
    #[serde(with = "decimal_u64")]
    pub first_writer_seq_num: u64,
    /// Atomic record batch.
    pub records: Vec<AppendJsonRecord>,
    /// Optional expected current sequence position.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "optional_decimal_u64"
    )]
    pub match_seq_num: Option<u64>,
}

/// Durable inclusive physical sequence range for an atomic append.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AppendRecordsResponse {
    /// First durable physical sequence number.
    #[serde(with = "decimal_u64")]
    pub seq_start: u64,
    /// Last durable physical sequence number.
    #[serde(with = "decimal_u64")]
    pub seq_end: u64,
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
    /// Tagged data payload.
    pub data: AppendRecordData,
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
    #[serde(default, with = "optional_decimal_u64")]
    pub last_timestamp_ms: Option<u64>,
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
    fn omits_absent_create_stream_options() {
        let request = CreateStreamRequest::default();

        assert_eq!(
            serde_json::to_value(request).expect("serialize create request"),
            json!({ "visibility": "private" })
        );
    }

    #[test]
    fn serializes_requested_stream_lifetime() {
        let request = CreateStreamRequest {
            expires_in_secs: Some(604_800),
            ..CreateStreamRequest::default()
        };
        let value = serde_json::to_value(request).expect("serialize create request");
        assert_eq!(value["expires_in_secs"], json!(604_800));
        assert_eq!(
            serde_json::from_value::<CreateStreamRequest>(value)
                .expect("deserialize create request")
                .expires_in_secs,
            Some(604_800)
        );
    }

    #[test]
    fn serializes_link_mutations_and_omits_absent_stream_update() {
        let link = IssueLinkRequest::new(
            "Reader".parse().expect("label"),
            LinkPermissions::read(),
            None,
        );
        let link = serde_json::to_value(link).expect("serialize link request");
        assert_eq!(link["label"], "Reader");
        assert_eq!(link["permissions"], "r");
        assert!(link["link_id"].is_string());
        assert!(link["secret"].is_string());
        assert_eq!(
            serde_json::to_value(RenameLinkRequest {
                label: "Deploy bot".parse().expect("label"),
            })
            .expect("serialize link rename request"),
            json!({ "label": "Deploy bot" })
        );
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
