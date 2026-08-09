//! Parsing and construction for human-facing `/s/{stream_id}` share URLs.

use url::{Url, form_urlencoded};

use crate::{BearerToken, StreamId, TokenPermissions};
use secrecy::ExposeSecret;

/// Default origin for Tailsurf share URLs.
pub const DEFAULT_WEB_BASE_URL: &str = "https://tail.surf";

/// Permission label and secret value decoded from a share URL fragment.
#[derive(Clone, Debug)]
pub struct StreamTokenParam {
    /// Permissions named by the fragment key.
    pub permissions: TokenPermissions,
    /// Secret token value decoded from the fragment value.
    pub token: BearerToken,
}

/// Stream ID and optional token extracted from a share URL.
#[derive(Clone, Debug)]
pub struct StreamLocator {
    /// Stream named by the `/s/{stream_id}` path.
    pub stream_id: StreamId,
    /// Optional single token fragment.
    pub token: Option<StreamTokenParam>,
}

impl StreamLocator {
    /// Parses a complete share URL and rejects malformed or ambiguous token fragments.
    pub fn parse(input: &str) -> Result<Self, StreamUrlError> {
        let url = Url::parse(input)?;
        let stream_id = parse_stream_id(&url)?;

        let token = url.fragment().map(parse_fragment).transpose()?.flatten();

        Ok(Self { stream_id, token })
    }

    /// Returns the token only when its permissions satisfy the supplied predicate.
    pub fn token_with(&self, required: impl Fn(TokenPermissions) -> bool) -> Option<&BearerToken> {
        self.token
            .as_ref()
            .filter(|candidate| required(candidate.permissions))
            .map(|candidate| &candidate.token)
    }
}

/// Builds a share URL with a percent-encoded secret token fragment.
pub fn stream_url(
    base_url: &Url,
    stream_id: &StreamId,
    permissions: TokenPermissions,
    token: &BearerToken,
) -> Url {
    let mut url = base_url.clone();
    url.set_path(&format!("/s/{stream_id}"));
    url.set_query(None);

    let fragment = form_urlencoded::Serializer::new(String::new())
        .append_pair(&permissions.to_string(), token.expose_secret())
        .finish();
    url.set_fragment(Some(&fragment));

    url
}

/// Returns the default Tailsurf web origin.
pub fn default_web_base_url() -> Url {
    Url::parse(DEFAULT_WEB_BASE_URL).expect("default tsf web base URL is valid")
}

fn parse_stream_id(url: &Url) -> Result<StreamId, StreamUrlError> {
    let mut segments = url
        .path_segments()
        .ok_or(StreamUrlError::InvalidStreamPath)?;

    match (segments.next(), segments.next(), segments.next()) {
        (Some("s"), Some(stream_id), None) => {
            StreamId::decode(stream_id).map_err(|source| StreamUrlError::InvalidStreamId { source })
        }
        _ => Err(StreamUrlError::InvalidStreamPath),
    }
}

fn parse_fragment(fragment: &str) -> Result<Option<StreamTokenParam>, StreamUrlError> {
    let mut token = None;

    for (key, value) in form_urlencoded::parse(fragment.as_bytes()) {
        if token.is_some() {
            return Err(StreamUrlError::MultipleTokens);
        }
        let permissions = key.parse()?;
        let value = value.into_owned();
        if value.is_empty() {
            return Err(StreamUrlError::InvalidToken);
        }
        token = Some(StreamTokenParam {
            permissions,
            token: value.into(),
        });
    }

    Ok(token)
}

/// Error returned while parsing a Tailsurf share URL.
#[derive(Debug, thiserror::Error)]
pub enum StreamUrlError {
    /// The input is not an absolute URL.
    #[error("invalid stream URL: {0}")]
    Url(#[from] url::ParseError),
    /// The path is not exactly `/s/{stream_id}`.
    #[error("stream URL path must be /s/{{stream_id}}")]
    InvalidStreamPath,
    /// The path contains a malformed stream identifier.
    #[error("stream URL has invalid stream id")]
    InvalidStreamId {
        #[source]
        /// Source UBID decoding failure.
        source: ubid::DecodeError,
    },
    /// The fragment key is not a valid permission string.
    #[error("stream URL fragment has invalid permissions")]
    InvalidPermissions(#[from] crate::PermissionsError),
    /// The fragment contains an empty token value.
    #[error("stream URL fragment has invalid token")]
    InvalidToken,
    /// More than one token parameter appears in the fragment.
    #[error("stream URL fragment contains multiple tokens")]
    MultipleTokens,
}

#[cfg(test)]
mod tests {
    use super::*;

    const STREAM_ID: &str = "0123456789abcdefghjkmnpqrstvwxyz";

    #[test]
    fn parses_share_url_token() {
        let locator = StreamLocator::parse(
            "https://tail.surf/s/0123456789abcdefghjkmnpqrstvwxyz#w=write-token",
        )
        .expect("stream URL");

        assert_eq!(
            locator.stream_id,
            STREAM_ID.parse::<StreamId>().expect("stream id")
        );
        let token = locator.token.expect("token");
        assert_eq!(token.permissions.to_string(), "w");
        assert_eq!(token.token.expose_secret(), "write-token");
    }

    #[test]
    fn parses_percent_encoded_fragment_token_and_ignores_query_params() {
        let locator = StreamLocator::parse(
            "https://tail.surf/s/0123456789abcdefghjkmnpqrstvwxyz?view=raw#o=owner%2Ftoken",
        )
        .expect("stream URL");

        assert_eq!(
            locator.stream_id,
            STREAM_ID.parse::<StreamId>().expect("stream id")
        );
        let token = locator.token.expect("token");
        assert_eq!(token.permissions.to_string(), "o");
        assert_eq!(token.token.expose_secret(), "owner/token");
    }

    #[test]
    fn rejects_invalid_paths_permissions_empty_tokens_and_multiple_tokens() {
        assert!(matches!(
            StreamLocator::parse("https://tail.surf/not-a-stream"),
            Err(StreamUrlError::InvalidStreamPath)
        ));
        assert!(matches!(
            StreamLocator::parse("https://tail.surf/s/0123456789abcdefghjkmnpqrstvwxyz#x=token"),
            Err(StreamUrlError::InvalidPermissions(_))
        ));
        assert!(matches!(
            StreamLocator::parse("https://tail.surf/s/0123456789abcdefghjkmnpqrstvwxyz#r="),
            Err(StreamUrlError::InvalidToken)
        ));
        assert!(matches!(
            StreamLocator::parse(
                "https://tail.surf/s/0123456789abcdefghjkmnpqrstvwxyz#w=write-token&r=read-token"
            ),
            Err(StreamUrlError::MultipleTokens)
        ));
    }

    #[test]
    fn builds_percent_encoded_share_url() {
        let base_url = Url::parse("http://localhost:8787").expect("base URL");
        let stream_id = STREAM_ID.parse::<StreamId>().expect("stream id");
        let token = BearerToken::from("abc-123_456");

        let url = stream_url(&base_url, &stream_id, TokenPermissions::owner(), &token);

        assert_eq!(
            url.as_str(),
            "http://localhost:8787/s/0123456789abcdefghjkmnpqrstvwxyz#o=abc-123_456"
        );
    }
}
