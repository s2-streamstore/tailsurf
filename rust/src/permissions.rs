//! Canonical owner, read, and write permissions carried by stream links.

use std::{borrow::Cow, fmt, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Valid permissions for a stream link.
///
/// Owner permission implies read and write and cannot be combined with either bit. String and JSON
/// representations are canonicalized to `o`, `r`, `w`, or `rw`.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct LinkPermissions(u8);

impl LinkPermissions {
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

    /// Returns the canonical string representation.
    pub const fn as_str(self) -> &'static str {
        const READ_WRITE: u8 = LinkPermissions::READ | LinkPermissions::WRITE;
        match self.0 {
            Self::OWNER => "o",
            Self::READ => "r",
            Self::WRITE => "w",
            READ_WRITE => "rw",
            // Every constructor validates bits into {o, r, w, rw}; no other value is representable.
            _ => unreachable!(),
        }
    }
}

impl fmt::Display for LinkPermissions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for LinkPermissions {
    type Err = PermissionsError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        if input.is_empty() {
            return Err(PermissionsError::Empty);
        }

        let mut bits = 0;
        for ch in input.chars() {
            let bit = match ch {
                'o' => LinkPermissions::OWNER,
                'r' => LinkPermissions::READ,
                'w' => LinkPermissions::WRITE,
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

impl Serialize for LinkPermissions {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for LinkPermissions {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Cow::<str>::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

/// Error returned when parsing a stream-link permission string.
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
    fn parses_serializes_and_applies_permissions() {
        for (input, canonical, allows) in [
            ("o", "o", [true, true, true]),
            ("r", "r", [false, true, false]),
            ("w", "w", [false, false, true]),
            ("wr", "rw", [false, true, true]),
        ] {
            let permissions: LinkPermissions = input.parse().expect("valid permissions");
            assert_eq!(permissions.to_string(), canonical);
            assert_eq!(
                [
                    permissions.allows_owner(),
                    permissions.allows_read(),
                    permissions.allows_write(),
                ],
                allows
            );
            assert_eq!(
                serde_json::to_string(&permissions).expect("serialize permissions"),
                format!("\"{canonical}\"")
            );
            assert_eq!(
                serde_json::from_str::<LinkPermissions>(&format!("\"{input}\""))
                    .expect("deserialize permissions"),
                permissions
            );
        }
    }

    #[test]
    fn rejects_empty_unknown_duplicate_and_redundant_owner_permissions() {
        assert_eq!("".parse::<LinkPermissions>(), Err(PermissionsError::Empty));
        assert_eq!(
            "rx".parse::<LinkPermissions>(),
            Err(PermissionsError::UnknownPermission('x'))
        );
        assert_eq!(
            "rr".parse::<LinkPermissions>(),
            Err(PermissionsError::DuplicatePermission('r'))
        );
        assert_eq!(
            "or".parse::<LinkPermissions>(),
            Err(PermissionsError::OwnerCannotBeCombined)
        );
        assert_eq!(
            "ow".parse::<LinkPermissions>(),
            Err(PermissionsError::OwnerCannotBeCombined)
        );
        assert_eq!(
            "orw".parse::<LinkPermissions>(),
            Err(PermissionsError::OwnerCannotBeCombined)
        );
    }
}
