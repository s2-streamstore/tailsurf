//! Identifiers and secret bearer-token values used by the TSF API.

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ubid_ids_round_trip_as_strings() {
        let stream_id: StreamId = "0123456789abcdefghjkmnpqrstvwxyz"
            .parse()
            .expect("stream id");
        let token_id: TokenId = "0123456789abcdefghjkmnpq".parse().expect("token id");

        assert_eq!(stream_id.to_string(), "0123456789abcdefghjkmnpqrstvwxyz");
        assert_eq!(token_id.to_string(), "0123456789abcdefghjkmnpq");
    }

    #[test]
    fn rejects_invalid_ubid_text_ids() {
        assert!("".parse::<StreamId>().is_err());
        assert!("stream id".parse::<StreamId>().is_err());
        assert!("".parse::<TokenId>().is_err());
        assert!("token\tid".parse::<TokenId>().is_err());
    }

    #[test]
    fn writer_ids_are_16_byte_binary_values() {
        let writer_id = WriterId::new_random();

        assert_eq!(writer_id.as_bytes().len(), WriterId::BYTE_LEN);
        assert_eq!(WriterId::from_bytes(*writer_id.as_bytes()), writer_id);
    }
}
