//! JSON request and response models for the REST v1 control plane.

use serde::{Deserialize, Serialize};

use crate::{BearerToken, StreamId, TokenId, TokenPermissions};

/// Whether a stream requires read authorization.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Visibility {
    /// Reads require a read-capable stream token.
    #[default]
    Private,
    /// Reads are anonymous; writes and management still require authorization.
    Public,
}

/// Options for creating a stream.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct CreateStreamRequest {
    /// Initial stream visibility. Defaults to private.
    #[serde(default)]
    pub visibility: Visibility,
    /// Requested lifetime in seconds, or the service default when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_in_secs: Option<u64>,
    /// Requested initial token permissions. The service adds an owner token when absent. At most
    /// three effective tokens are allowed, including the owner.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub issue_tokens: Option<Vec<TokenPermissions>>,
}

/// A stream token issued during stream creation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IssuedStreamToken {
    /// Stable non-secret token identifier used for revocation.
    pub token_id: TokenId,
    /// Effective permissions carried by the token.
    pub permissions: TokenPermissions,
    /// Secret token value, returned only when issued.
    #[serde(serialize_with = "crate::ids::serialize_bearer_token")]
    pub token: BearerToken,
}

/// Created stream metadata and any atomically issued tokens.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CreateStreamResponse {
    /// Stable stream identifier.
    pub stream_id: StreamId,
    /// Initial visibility.
    pub visibility: Visibility,
    /// Absolute RFC 3339 stream expiration timestamp.
    pub expires_at: String,
    /// Newly issued secret tokens.
    pub tokens: Vec<IssuedStreamToken>,
}

/// Options for issuing a stream token.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IssueTokenRequest {
    /// Permissions carried by the new token.
    pub permissions: TokenPermissions,
    /// Optional RFC 3339 expiration timestamp.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<String>,
}

/// A newly issued stream token.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IssueTokenResponse {
    /// Stable non-secret token identifier used for revocation.
    pub token_id: TokenId,
    /// Effective permissions carried by the token.
    pub permissions: TokenPermissions,
    /// Secret token value, returned only when issued.
    #[serde(serialize_with = "crate::ids::serialize_bearer_token")]
    pub token: BearerToken,
}

/// A request to revoke one stream token.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RevokeTokenRequest {
    /// Stable non-secret identifier of the token to revoke.
    pub token_id: TokenId,
}

/// Effective lifecycle state for a stream token.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StreamTokenStatus {
    /// The token can be authorized.
    Active,
    /// The token reached its configured expiration.
    Expired,
    /// The token was explicitly revoked.
    Revoked,
}

/// Non-secret metadata for one issued stream token.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StreamTokenSummary {
    /// Stable non-secret token identifier used for revocation.
    pub token_id: TokenId,
    /// Effective permissions carried by the token.
    pub permissions: TokenPermissions,
    /// Current effective lifecycle state.
    pub status: StreamTokenStatus,
    /// RFC 3339 issuance timestamp.
    pub issued_at: String,
    /// RFC 3339 expiration timestamp when configured.
    pub expires_at: Option<String>,
    /// RFC 3339 revocation timestamp when inactive.
    pub revoked_at: Option<String>,
    /// Whether this token authenticated the inventory request.
    pub is_current: bool,
}

/// Non-secret token inventory for a stream.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ListTokensResponse {
    /// Retained token metadata ordered newest first.
    pub tokens: Vec<StreamTokenSummary>,
}

/// Current stream metadata.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StreamInfoResponse {
    /// Stable stream identifier.
    pub stream_id: StreamId,
    /// Backing S2 basin name.
    pub basin: String,
    /// Current visibility.
    pub visibility: Visibility,
    /// Current lifecycle state.
    pub state: String,
    /// Absolute RFC 3339 stream expiration timestamp.
    pub expires_at: String,
    /// Number of non-revoked stream tokens.
    pub active_token_count: usize,
}

/// Mutable stream settings. Absent fields are preserved.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct UpdateStreamRequest {
    /// New visibility when present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub visibility: Option<Visibility>,
    /// Later absolute RFC 3339 expiration timestamp when renewing the stream.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<String>,
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
    fn serializes_token_mutations_and_omits_absent_stream_update() {
        let token = IssueTokenRequest {
            permissions: TokenPermissions::read(),
            expires_at: None,
        };
        let token_id: TokenId = "0123456789abcdefghjkmnpq".parse().expect("token id");

        assert_eq!(
            serde_json::to_value(token).expect("serialize token request"),
            json!({ "permissions": "r" })
        );
        assert_eq!(
            serde_json::to_value(RevokeTokenRequest { token_id })
                .expect("serialize token revocation request"),
            json!({ "token_id": "0123456789abcdefghjkmnpq" })
        );
        assert_eq!(
            serde_json::to_value(UpdateStreamRequest::default()).expect("serialize update request"),
            json!({})
        );
    }
}
