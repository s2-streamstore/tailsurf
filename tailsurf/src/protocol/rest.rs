//! JSON request and response models for the REST v1 control plane.

use serde::{Deserialize, Deserializer, Serialize, Serializer};

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
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct CreateStreamRequest {
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
    /// Absolute RFC 3339 stream expiration timestamp.
    pub expires_at: String,
    /// Newly issued secret links.
    pub links: Vec<IssuedStreamLink>,
}

/// Options for issuing a stream link.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IssueLinkRequest {
    /// Owner-visible label for the new link.
    pub label: LinkLabel,
    /// Permissions carried by the new link.
    pub permissions: LinkPermissions,
    /// Optional RFC 3339 expiration timestamp.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<String>,
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
}

/// Current stream metadata.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StreamInfoResponse {
    /// Stable stream identifier.
    pub stream_id: StreamId,
    /// Human-facing title when one has been set.
    #[serde(deserialize_with = "deserialize_nullable_stream_title")]
    pub title: Option<StreamTitle>,
    /// Backing S2 basin name.
    pub basin: String,
    /// Current visibility.
    pub visibility: Visibility,
    /// Current lifecycle state.
    pub state: String,
    /// Absolute RFC 3339 stream expiration timestamp.
    pub expires_at: String,
    /// Number of non-revoked stream links.
    pub active_link_count: usize,
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

/// Current durable tail position for a stream.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StreamTailResponse {
    /// Stable stream identifier.
    pub stream_id: StreamId,
    /// Sequence number assigned to the next durable append.
    pub next_s2_seq_num: u64,
    /// Timestamp of the last record, or `None` for an empty stream.
    pub last_timestamp_ms: Option<u64>,
    /// Short-lived authorization for the next private read connection.
    #[serde(default)]
    pub read_authorization: Option<String>,
}

/// Timestamp and sequence bounds for a stream.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StreamRangeResponse {
    /// Stable stream identifier.
    pub stream_id: StreamId,
    /// Sequence number of the first record, or `None` when empty.
    pub first_s2_seq_num: Option<u64>,
    /// Timestamp of the first record, or `None` when empty.
    pub first_timestamp_ms: Option<u64>,
    /// Sequence number assigned to the next durable append.
    pub next_s2_seq_num: u64,
    /// Timestamp of the last record, or `None` when empty.
    pub last_timestamp_ms: Option<u64>,
    /// Short-lived authorization for the next private read connection.
    #[serde(default)]
    pub read_authorization: Option<String>,
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
        let link = IssueLinkRequest {
            label: "Reader".parse().expect("label"),
            permissions: LinkPermissions::read(),
            expires_at: None,
        };
        assert_eq!(
            serde_json::to_value(link).expect("serialize link request"),
            json!({ "label": "Reader", "permissions": "r" })
        );
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
