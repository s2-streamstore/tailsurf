//! Identifiers and link secret values used by the TSF API.

use std::{fmt, str::FromStr};

use base64::{Engine as _, alphabet, engine};
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

/// Secret value carried by a stream link.
pub type LinkSecret = secrecy::SecretString;

/// Stable 128-bit client writer identity reused across reconnects.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct WriterId([u8; 16]);

impl WriterId {
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

/// Strict URL-safe no-pad base64 engine that rejects non-canonical trailing bits.
const STRICT_URL_SAFE_NO_PAD: engine::GeneralPurpose = engine::GeneralPurpose::new(
    &alphabet::URL_SAFE,
    engine::general_purpose::NO_PAD.with_decode_allow_trailing_bits(false),
);

/// Encodes a 256-bit value as canonical unpadded base64url.
pub(crate) fn encode_base64url_32(bytes: &[u8; 32]) -> String {
    STRICT_URL_SAFE_NO_PAD.encode(bytes)
}

/// Returns whether `value` is the canonical unpadded base64url encoding of a 256-bit value.
pub(crate) fn is_canonical_base64url_32(value: &str) -> bool {
    if value.len() != BASE64URL_32_ENCODED_LEN {
        return false;
    }
    let mut decoded = [0_u8; 32];
    STRICT_URL_SAFE_NO_PAD
        .decode_slice(value, &mut decoded)
        .is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_value_encodes_to_all_a() {
        assert_eq!(encode_base64url_32(&[0_u8; 32]), "A".repeat(43));
    }

    #[test]
    fn sequential_bytes_encode_to_known_base64url() {
        let bytes: [u8; 32] = core::array::from_fn(|i| i as u8);
        let encoded = encode_base64url_32(&bytes);

        assert_eq!(encoded, "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8");
        assert!(is_canonical_base64url_32(&encoded));
    }

    #[test]
    fn rejects_malleable_trailing_bits() {
        let canonical = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
        assert!(is_canonical_base64url_32(canonical));

        let mut chars: Vec<char> = canonical.chars().collect();
        for bad in ['B', 'C', 'D'] {
            chars[42] = bad;
            let link: String = chars.iter().collect();
            assert!(!is_canonical_base64url_32(&link), "link={link}");
        }
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
