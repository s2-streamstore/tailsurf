//! JSON request and response models for the REST v1 control plane.

use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};

use crate::{BearerToken, StreamId, TokenId, TokenPermissions};

/// Whether a stream requires read authorization.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Visibility {
    /// Reads require an account bearer token or a read-capable stream token.
    #[default]
    Private,
    /// Reads are anonymous; writes and management still require authorization.
    Public,
}

/// Requested S2 record retention for a new stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestedRetention {
    /// Automatically trim records older than this many seconds.
    Seconds(u64),
    /// Retain records indefinitely.
    Infinite,
}

impl Serialize for RequestedRetention {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Seconds(seconds) => serializer.serialize_u64(*seconds),
            Self::Infinite => serializer.serialize_str("infinite"),
        }
    }
}

impl<'de> Deserialize<'de> for RequestedRetention {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum WireRetention {
            Seconds(u64),
            Name(String),
        }

        match WireRetention::deserialize(deserializer)? {
            WireRetention::Seconds(seconds) => Ok(Self::Seconds(seconds)),
            WireRetention::Name(name) if name == "infinite" => Ok(Self::Infinite),
            WireRetention::Name(_) => Err(D::Error::custom(
                "retention must be seconds or \"infinite\"",
            )),
        }
    }
}

/// Options for creating a stream.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct CreateStreamRequest {
    /// Initial stream visibility. Defaults to private.
    #[serde(default)]
    pub visibility: Visibility,
    /// Requested retention, or the service default when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention_secs: Option<RequestedRetention>,
    /// Requested initial token permissions. The service adds an owner token when absent. At most three effective tokens are allowed, including the owner.
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
    /// Effective retention in seconds.
    pub retention_secs: u64,
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
    /// Effective retention in seconds.
    pub retention_secs: u64,
    /// Number of non-revoked stream tokens.
    pub active_token_count: usize,
}

/// Mutable stream settings. Absent fields are preserved.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct UpdateStreamRequest {
    /// New visibility when present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub visibility: Option<Visibility>,
}

/// Current durable tail position for a stream.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StreamTailResponse {
    /// Stable stream identifier.
    pub stream_id: StreamId,
    /// Sequence number assigned to the next durable append.
    pub next_s2_seq_num: u64,
    /// Timestamp of the last retained record, or `None` for an empty stream.
    pub last_timestamp_ms: Option<u64>,
}

/// Retained timestamp and sequence bounds for a stream.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StreamRangeResponse {
    /// Stable stream identifier.
    pub stream_id: StreamId,
    /// Sequence number of the first retained record, or `None` when empty.
    pub first_s2_seq_num: Option<u64>,
    /// Timestamp of the first retained record, or `None` when empty.
    pub first_timestamp_ms: Option<u64>,
    /// Sequence number assigned to the next durable append.
    pub next_s2_seq_num: u64,
    /// Timestamp of the last retained record, or `None` when empty.
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
    fn serializes_finite_and_infinite_retention_requests() {
        for (retention, expected) in [
            (RequestedRetention::Seconds(604_800), json!(604_800)),
            (RequestedRetention::Infinite, json!("infinite")),
        ] {
            let request = CreateStreamRequest {
                retention_secs: Some(retention),
                ..CreateStreamRequest::default()
            };
            let value = serde_json::to_value(request).expect("serialize create request");
            assert_eq!(value["retention_secs"], expected);
            assert_eq!(
                serde_json::from_value::<CreateStreamRequest>(value)
                    .expect("deserialize create request")
                    .retention_secs,
                Some(retention)
            );
        }

        assert!(
            serde_json::from_value::<CreateStreamRequest>(json!({
                "visibility": "private",
                "retention_secs": "forever"
            }))
            .is_err()
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

    #[test]
    fn deserializes_token_inventory_without_secrets() {
        let response: ListTokensResponse = serde_json::from_value(json!({
            "tokens": [{
                "token_id": "0123456789abcdefghjkmnpq",
                "permissions": "o",
                "status": "active",
                "issued_at": "2026-08-07T12:00:00.000Z",
                "expires_at": null,
                "revoked_at": null,
                "is_current": true
            }]
        }))
        .expect("token inventory");

        assert_eq!(response.tokens[0].status, StreamTokenStatus::Active);
        assert!(response.tokens[0].is_current);
    }

    #[test]
    fn deserializes_retained_stream_range() {
        let response: StreamRangeResponse = serde_json::from_value(json!({
            "stream_id": "0123456789abcdefghjkmnpqrstvwxyz",
            "first_s2_seq_num": 4,
            "first_timestamp_ms": 1_786_000_000_000_u64,
            "next_s2_seq_num": 9,
            "last_timestamp_ms": 1_786_000_010_000_u64
        }))
        .expect("stream range");

        assert_eq!(response.first_s2_seq_num, Some(4));
        assert_eq!(response.next_s2_seq_num, 9);
    }
}
