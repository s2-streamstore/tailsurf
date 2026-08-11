//! Identifiers and secret bearer-token values used by the TSF API.

use base64::{Engine as _, alphabet, engine};
use rand::Rng;
use secrecy::ExposeSecret;
use serde::{Serialize, Serializer};

/// Stable 160-bit identifier for a stream.
pub type StreamId = ubid::Ubid160;

/// Stable 120-bit identifier for an issued stream token.
pub type TokenId = ubid::Ubid120;

/// Secret account or stream bearer token.
pub type BearerToken = secrecy::SecretString;

/// Stable 128-bit identity assigned by a writer and reused across reconnects.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct WriterId([u8; 16]);

impl WriterId {
    /// Encoded writer ID length.
    pub const BYTE_LEN: usize = 16;

    /// Generates a cryptographically random writer ID.
    pub fn new_random() -> Self {
        let mut bytes = [0_u8; Self::BYTE_LEN];
        fill_random(&mut bytes);
        Self(bytes)
    }

    /// Creates a writer ID from its exact binary representation.
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

pub(crate) fn serialize_bearer_token<S>(
    token: &BearerToken,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(token.expose_secret())
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
            let token: String = chars.iter().collect();
            assert!(!is_canonical_base64url_32(&token), "token={token}");
        }
    }
}
