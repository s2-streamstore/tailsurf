//! User-provided labels for stream links.

use std::{fmt, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

/// Maximum number of Unicode code points in a link label.
pub const MAX_LINK_LABEL_CODE_POINTS: usize = 64;

/// Owner-visible, user-provided metadata for one stream link.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct LinkLabel(String);

impl LinkLabel {
    /// Returns the label text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for LinkLabel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for LinkLabel {
    type Err = LinkLabelError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let length = input.chars().count();
        if !(1..=MAX_LINK_LABEL_CODE_POINTS).contains(&length) {
            return Err(LinkLabelError::Length);
        }
        if input.trim() != input {
            return Err(LinkLabelError::SurroundingWhitespace);
        }
        if input
            .chars()
            .any(|character| character.is_control() || matches!(character, '\u{2028}' | '\u{2029}'))
        {
            return Err(LinkLabelError::ForbiddenCharacter);
        }
        Ok(Self(input.to_owned()))
    }
}

impl Serialize for LinkLabel {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for LinkLabel {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(de::Error::custom)
    }
}

/// Error returned when parsing a link label.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum LinkLabelError {
    /// The label is empty or exceeds the maximum length.
    #[error("link label must contain 1 to {MAX_LINK_LABEL_CODE_POINTS} Unicode code points")]
    Length,
    /// The label begins or ends with whitespace.
    #[error("link label must not have leading or trailing whitespace")]
    SurroundingWhitespace,
    /// The label contains a control character or line separator.
    #[error("link label must not contain control characters or line breaks")]
    ForbiddenCharacter,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_unicode_code_points_and_whitespace() {
        let emoji = "😀".repeat(MAX_LINK_LABEL_CODE_POINTS);
        assert_eq!(
            emoji.parse::<LinkLabel>().expect("valid label").as_str(),
            emoji
        );
        for invalid in [
            "",
            " padded",
            "padded ",
            "padded\u{a0}",
            "tab\tbreak",
            "nul\0break",
            "line\nbreak",
            "line\u{2028}break",
            "line\u{2029}break",
        ] {
            assert!(invalid.parse::<LinkLabel>().is_err(), "label={invalid:?}");
        }
        assert!(
            "😀"
                .repeat(MAX_LINK_LABEL_CODE_POINTS + 1)
                .parse::<LinkLabel>()
                .is_err()
        );
    }
}
