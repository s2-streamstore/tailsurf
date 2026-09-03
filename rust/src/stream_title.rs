//! User-provided titles for streams.

use std::{fmt, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize, de};

/// Maximum number of Unicode code points in a stream title.
pub const MAX_STREAM_TITLE_CODE_POINTS: usize = 120;

/// Mutable human-facing metadata for one stream.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct StreamTitle(String);

impl StreamTitle {
    /// Returns the title text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for StreamTitle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for StreamTitle {
    type Err = StreamTitleError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let length = input.chars().count();
        if !(1..=MAX_STREAM_TITLE_CODE_POINTS).contains(&length) {
            return Err(StreamTitleError::Length);
        }
        if input.trim() != input {
            return Err(StreamTitleError::SurroundingWhitespace);
        }
        if input
            .chars()
            .any(|character| character.is_control() || matches!(character, '\u{2028}' | '\u{2029}'))
        {
            return Err(StreamTitleError::ForbiddenCharacter);
        }
        Ok(Self(input.to_owned()))
    }
}

impl<'de> Deserialize<'de> for StreamTitle {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(de::Error::custom)
    }
}

/// Error returned when parsing a stream title.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum StreamTitleError {
    /// The title is empty or exceeds the maximum length.
    #[error("stream title must contain 1 to {MAX_STREAM_TITLE_CODE_POINTS} Unicode code points")]
    Length,
    /// The title begins or ends with whitespace.
    #[error("stream title must not have leading or trailing whitespace")]
    SurroundingWhitespace,
    /// The title contains a control character or line separator.
    #[error("stream title must not contain control characters or line breaks")]
    ForbiddenCharacter,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_unicode_code_points_and_whitespace() {
        let emoji = "😀".repeat(MAX_STREAM_TITLE_CODE_POINTS);
        assert_eq!(
            emoji.parse::<StreamTitle>().expect("valid title").as_str(),
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
            assert!(invalid.parse::<StreamTitle>().is_err(), "title={invalid:?}");
        }
        assert!(
            "😀"
                .repeat(MAX_STREAM_TITLE_CODE_POINTS + 1)
                .parse::<StreamTitle>()
                .is_err()
        );
    }
}
