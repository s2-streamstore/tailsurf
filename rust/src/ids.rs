//! Identifiers and link secret values used by the TSF API.

use std::{fmt, str::FromStr};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use rand::Rng;
use secrecy::ExposeSecret;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Stable 160-bit identifier for a stream.
pub type StreamId = ubid::Ubid160;

/// Maximum length of a stream-scoped Link ID.
pub const MAX_LINK_ID_LEN: usize = 64;

/// Client-chosen immutable identifier for a stream link.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct LinkId(String);

impl LinkId {
    /// Returns the canonical Link ID text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for LinkId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for LinkId {
    type Err = LinkIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let bytes = value.as_bytes();
        let is_segment = |byte: u8| byte.is_ascii_lowercase() || byte.is_ascii_digit();
        if bytes.is_empty()
            || bytes.len() > MAX_LINK_ID_LEN
            || !is_segment(bytes[0])
            || !is_segment(bytes[bytes.len() - 1])
            || !bytes.iter().all(|byte| is_segment(*byte) || *byte == b'-')
        {
            return Err(LinkIdError);
        }
        Ok(Self(value.to_owned()))
    }
}

impl Serialize for LinkId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for LinkId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

/// Error returned for a non-canonical Link ID.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error(
    "link ID must be 1 to 64 lowercase letters, digits, or hyphens, without a leading or trailing hyphen"
)]
pub struct LinkIdError;

/// Server-minted 24-byte stream-link credential encoded as unpadded base64url.
///
/// Canonicality is validated once at construction; every consumer can rely on it.
#[derive(Clone)]
pub struct LinkSecret(secrecy::SecretString);

impl LinkSecret {
    /// Length of the canonical unpadded base64url encoding of a 24-byte secret.
    pub const ENCODED_LEN: usize = BASE64URL_24_ENCODED_LEN;

    /// Returns the canonical secret text.
    pub fn expose_secret(&self) -> &str {
        self.0.expose_secret()
    }
}

impl fmt::Debug for LinkSecret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("LinkSecret(REDACTED)")
    }
}

impl FromStr for LinkSecret {
    type Err = LinkSecretError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if !is_canonical_base64url_24(value) {
            return Err(LinkSecretError);
        }
        Ok(Self(value.to_owned().into()))
    }
}

/// Error returned for a non-canonical link secret.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("link secret must be canonical 32-character unpadded base64url")]
pub struct LinkSecretError;

/// Stable 128-bit client-chosen writer identity reused across reconnects.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ClientWriterId([u8; 16]);

impl ClientWriterId {
    /// Encoded client writer ID length.
    pub const BYTE_LEN: usize = 16;

    /// Generates a cryptographically random client writer ID.
    pub fn new_random() -> Self {
        let mut bytes = [0_u8; Self::BYTE_LEN];
        fill_random(&mut bytes);
        Self(bytes)
    }

    /// Creates a client writer ID from its exact binary representation.
    pub const fn from_bytes(bytes: [u8; Self::BYTE_LEN]) -> Self {
        Self(bytes)
    }

    /// Returns the exact binary representation.
    pub const fn as_bytes(&self) -> &[u8; Self::BYTE_LEN] {
        &self.0
    }
}

fn fill_random(bytes: &mut [u8]) {
    rand::rng().fill_bytes(bytes);
}

impl Serialize for ClientWriterId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(self.as_bytes())
    }
}

/// Stable 128-bit server-derived writer identity attached to delivered records.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct WriterId([u8; 16]);

impl WriterId {
    /// Encoded writer ID length.
    pub const BYTE_LEN: usize = 16;

    /// Creates a writer ID from its exact binary representation.
    pub const fn from_bytes(bytes: [u8; Self::BYTE_LEN]) -> Self {
        Self(bytes)
    }

    /// Returns the exact binary representation.
    pub const fn as_bytes(&self) -> &[u8; Self::BYTE_LEN] {
        &self.0
    }
}

impl Serialize for WriterId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(self.as_bytes())
    }
}

pub(crate) fn serialize_link_secret<S>(
    secret: &LinkSecret,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(secret.expose_secret())
}

/// Length of the canonical unpadded base64url encoding of a 256-bit value.
pub(crate) const BASE64URL_32_ENCODED_LEN: usize = 43;
/// Length of the canonical unpadded base64url encoding of a 24-byte value.
pub(crate) const BASE64URL_24_ENCODED_LEN: usize = 32;

/// Encodes a 256-bit value as canonical unpadded base64url.
pub(crate) fn encode_base64url_32(bytes: &[u8; 32]) -> String {
    URL_SAFE_NO_PAD.encode(bytes)
}

pub(crate) fn random_base64url_32() -> String {
    let mut bytes = [0_u8; 32];
    fill_random(&mut bytes);
    encode_base64url_32(&bytes)
}

/// Returns whether `value` is the canonical unpadded base64url encoding of a 256-bit value.
pub(crate) fn is_canonical_base64url_32(value: &str) -> bool {
    if value.len() != BASE64URL_32_ENCODED_LEN {
        return false;
    }
    let mut decoded = [0_u8; 32];
    URL_SAFE_NO_PAD.decode_slice(value, &mut decoded).is_ok()
}

/// Returns whether `value` is the canonical unpadded base64url encoding of a 24-byte value.
fn is_canonical_base64url_24(value: &str) -> bool {
    if value.len() != BASE64URL_24_ENCODED_LEN {
        return false;
    }
    let mut decoded = [0_u8; 24];
    URL_SAFE_NO_PAD.decode_slice(value, &mut decoded).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_exact_24_byte_link_secrets() {
        let canonical = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
        assert!(is_canonical_base64url_24(canonical));
        assert!(!is_canonical_base64url_24(&canonical[..31]));
        assert!(!is_canonical_base64url_24(&format!("{canonical}A")));
    }

    #[test]
    fn link_ids_accept_semantic_slugs() {
        for value in ["owner", "deploy-bot", "a", "a1-b2"] {
            assert_eq!(
                value.parse::<LinkId>().expect("valid Link ID").as_str(),
                value
            );
        }
    }

    #[test]
    fn link_ids_reject_non_canonical_values() {
        for value in ["", "Owner", "-owner", "owner-", "deploy_bot", "deploy bot"] {
            assert!(value.parse::<LinkId>().is_err(), "accepted {value:?}");
        }
        assert!("a".repeat(MAX_LINK_ID_LEN + 1).parse::<LinkId>().is_err());
    }
}
