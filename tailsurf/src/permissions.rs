//! Canonical owner, read, and write permissions carried by stream tokens.

use std::{fmt, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

/// Valid permissions for a stream token.
///
/// Owner permission implies read and write and cannot be combined with either bit. String and JSON representations are canonicalized to `o`, `r`, `w`, or `rw`.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TokenPermissions(u8);

impl TokenPermissions {
    const OWNER: u8 = 0b001;
    const READ: u8 = 0b010;
    const WRITE: u8 = 0b100;

    /// Creates permissions from the low three owner/read/write bits.
    ///
    /// Returns `None` for zero, unknown bits, or owner combined with another permission.
    pub const fn new(bits: u8) -> Option<Self> {
        if bits == 0
            || bits & !(Self::OWNER | Self::READ | Self::WRITE) != 0
            || bits & Self::OWNER != 0 && bits != Self::OWNER
        {
            None
        } else {
            Some(Self(bits))
        }
    }

    /// Owner permission, which also authorizes reads and writes.
    pub const fn owner() -> Self {
        Self(Self::OWNER)
    }

    /// Read-only permission.
    pub const fn read() -> Self {
        Self(Self::READ)
    }

    /// Write-only permission.
    pub const fn write() -> Self {
        Self(Self::WRITE)
    }

    /// Combined read and write permission without ownership.
    pub const fn read_write() -> Self {
        Self(Self::READ | Self::WRITE)
    }

    /// Returns the validated bit representation.
    pub const fn bits(self) -> u8 {
        self.0
    }

    /// Returns whether this value grants ownership.
    pub const fn allows_owner(self) -> bool {
        self.0 & Self::OWNER != 0
    }

    /// Returns whether this value grants reads, directly or through ownership.
    pub const fn allows_read(self) -> bool {
        self.allows_owner() || self.0 & Self::READ != 0
    }

    /// Returns whether this value grants writes, directly or through ownership.
    pub const fn allows_write(self) -> bool {
        self.allows_owner() || self.0 & Self::WRITE != 0
    }
}

impl fmt::Display for TokenPermissions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.allows_owner() {
            f.write_str("o")?;
            return Ok(());
        }
        if self.0 & TokenPermissions::READ != 0 {
            f.write_str("r")?;
        }
        if self.0 & TokenPermissions::WRITE != 0 {
            f.write_str("w")?;
        }
        Ok(())
    }
}

impl FromStr for TokenPermissions {
    type Err = PermissionsError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        if input.is_empty() {
            return Err(PermissionsError::Empty);
        }

        let mut bits = 0;
        for ch in input.chars() {
            let bit = match ch {
                'o' => TokenPermissions::OWNER,
                'r' => TokenPermissions::READ,
                'w' => TokenPermissions::WRITE,
                other => return Err(PermissionsError::UnknownPermission(other)),
            };

            if bits & bit != 0 {
                return Err(PermissionsError::DuplicatePermission(ch));
            }
            bits |= bit;
        }

        Self::new(bits).ok_or(PermissionsError::OwnerCannotBeCombined)
    }
}

impl Serialize for TokenPermissions {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for TokenPermissions {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(de::Error::custom)
    }
}

/// Error returned when parsing a stream-token permission string.
#[derive(Clone, Debug, thiserror::Error, Eq, PartialEq)]
pub enum PermissionsError {
    /// No permission was provided.
    #[error("permission string cannot be empty")]
    Empty,
    /// A character other than `o`, `r`, or `w` was provided.
    #[error("unknown stream permission {0:?}")]
    UnknownPermission(char),
    /// A permission appeared more than once.
    #[error("duplicate stream permission {0:?}")]
    DuplicatePermission(char),
    /// Owner was combined with read or write even though it implies both.
    #[error("owner permission cannot be combined with read/write because it already includes them")]
    OwnerCannotBeCombined,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_data_permissions_in_any_order_and_formats_canonically() {
        let permissions: TokenPermissions = "wr".parse().expect("valid permissions");

        assert!(!permissions.allows_owner());
        assert!(permissions.allows_read());
        assert!(permissions.allows_write());
        assert_eq!(permissions.to_string(), "rw");
    }

    #[test]
    fn owner_implies_all_effective_permissions() {
        let permissions: TokenPermissions = "o".parse().expect("valid permissions");

        assert!(permissions.allows_owner());
        assert!(permissions.allows_read());
        assert!(permissions.allows_write());
        assert_eq!(permissions.to_string(), "o");
    }

    #[test]
    fn rejects_empty_unknown_duplicate_and_redundant_owner_permissions() {
        assert_eq!("".parse::<TokenPermissions>(), Err(PermissionsError::Empty));
        assert_eq!(
            "rx".parse::<TokenPermissions>(),
            Err(PermissionsError::UnknownPermission('x'))
        );
        assert_eq!(
            "rr".parse::<TokenPermissions>(),
            Err(PermissionsError::DuplicatePermission('r'))
        );
        assert_eq!(
            "or".parse::<TokenPermissions>(),
            Err(PermissionsError::OwnerCannotBeCombined)
        );
        assert_eq!(
            "ow".parse::<TokenPermissions>(),
            Err(PermissionsError::OwnerCannotBeCombined)
        );
        assert_eq!(
            "orw".parse::<TokenPermissions>(),
            Err(PermissionsError::OwnerCannotBeCombined)
        );
    }

    #[test]
    fn serde_uses_canonical_string_form() {
        let permissions: TokenPermissions =
            serde_json::from_str("\"wr\"").expect("valid permission JSON");

        assert_eq!(permissions.to_string(), "rw");
        assert_eq!(
            serde_json::to_string(&permissions).expect("serialize permissions"),
            "\"rw\""
        );
    }
}
