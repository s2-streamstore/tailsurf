//! Parsing and construction for human-facing `/s/{stream_id}` share URLs.

use secrecy::ExposeSecret;
use url::{Url, form_urlencoded};

use crate::{BearerToken, StreamId, TokenPermissions, ids::is_canonical_base64url_32};

/// Default origin for Tailsurf share URLs.
pub const DEFAULT_WEB_BASE_URL: &str = "https://tail.surf";
/// Encoded length of a 256-bit stream token.
pub const STREAM_TOKEN_ENCODED_LENGTH: usize = crate::ids::BASE64URL_32_ENCODED_LEN;
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
        validate_share_scheme(&url)?;
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

/// Builds a share URL after validating the canonical unpadded base64url token.
pub fn stream_url(
    base_url: &Url,
    stream_id: &StreamId,
    permissions: TokenPermissions,
    token: &BearerToken,
) -> Result<Url, StreamUrlError> {
    validate_stream_token(token.expose_secret())?;
    let mut url = base_url.clone();
    validate_share_scheme(&url)?;
    url.set_username("")
        .map_err(|()| StreamUrlError::InvalidBaseUrl)?;
    url.set_password(None)
        .map_err(|()| StreamUrlError::InvalidBaseUrl)?;
    url.set_path(&format!("/s/{stream_id}"));
    url.set_query(None);

    let fragment = form_urlencoded::Serializer::new(String::new())
        .append_pair(&permissions.to_string(), token.expose_secret())
        .finish();
    url.set_fragment(Some(&fragment));

    Ok(url)
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
    let mut pairs = form_urlencoded::parse(fragment.as_bytes());
    let Some((permissions, token)) = pairs.next() else {
        return Ok(None);
    };
    let permissions = permissions.parse()?;
    let token = token.into_owned();
    validate_stream_token(&token)?;
    if pairs.next().is_some() {
        return Err(StreamUrlError::MultipleTokens);
    }

    Ok(Some(StreamTokenParam {
        permissions,
        token: token.into(),
    }))
}

fn validate_stream_token(token: &str) -> Result<(), StreamUrlError> {
    is_canonical_base64url_32(token)
        .then_some(())
        .ok_or(StreamUrlError::InvalidToken)
}

fn validate_share_scheme(url: &Url) -> Result<(), StreamUrlError> {
    if matches!(url.scheme(), "http" | "https") {
        Ok(())
    } else {
        Err(StreamUrlError::InvalidScheme(url.scheme().to_owned()))
    }
}

/// Error returned while parsing a Tailsurf share URL.
#[derive(Debug, thiserror::Error)]
pub enum StreamUrlError {
    /// The input is not an absolute URL.
    #[error("invalid stream URL: {0}")]
    Url(#[from] url::ParseError),
    /// The URL does not use HTTP or HTTPS.
    #[error("stream URL scheme must be http or https, not {0:?}")]
    InvalidScheme(String),
    /// The validated HTTP(S) base URL could not be normalized for output.
    #[error("stream URL base could not be normalized")]
    InvalidBaseUrl,
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
    /// The fragment token is not canonical 256-bit unpadded base64url.
    #[error("stream URL token must be canonical 43-character unpadded base64url")]
    InvalidToken,
    /// More than one token parameter appears in the fragment.
    #[error("stream URL fragment contains multiple tokens")]
    MultipleTokens,
}

#[cfg(test)]
mod tests {
    use super::*;

    const STREAM_ID: &str = "0123456789abcdefghjkmnpqrstvwxyz";
    const TOKEN: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

    #[test]
    fn parses_share_url_token() {
        let locator = StreamLocator::parse(&format!(
            "https://tail.surf/s/0123456789abcdefghjkmnpqrstvwxyz#w={TOKEN}"
        ))
        .expect("stream URL");

        assert_eq!(
            locator.stream_id,
            STREAM_ID.parse::<StreamId>().expect("stream id")
        );
        let token = locator.token.expect("token");
        assert_eq!(token.permissions.to_string(), "w");
        assert_eq!(token.token.expose_secret(), TOKEN);
    }

    #[test]
    fn parses_percent_encoded_fragment_token_and_ignores_query_params() {
        let encoded_token = format!("%41{}", &TOKEN[1..]);
        let locator = StreamLocator::parse(&format!(
            "https://tail.surf/s/0123456789abcdefghjkmnpqrstvwxyz?view=raw#o={encoded_token}"
        ))
        .expect("stream URL");

        assert_eq!(
            locator.stream_id,
            STREAM_ID.parse::<StreamId>().expect("stream id")
        );
        let token = locator.token.expect("token");
        assert_eq!(token.permissions.to_string(), "o");
        assert_eq!(token.token.expose_secret(), TOKEN);
    }

    #[test]
    fn rejects_invalid_paths_permissions_empty_tokens_and_multiple_tokens() {
        assert!(matches!(
            StreamLocator::parse("https://tail.surf/not-a-stream"),
            Err(StreamUrlError::InvalidStreamPath)
        ));
        assert!(matches!(
            StreamLocator::parse(&format!(
                "https://tail.surf/s/0123456789abcdefghjkmnpqrstvwxyz#x={TOKEN}"
            )),
            Err(StreamUrlError::InvalidPermissions(_))
        ));
        assert!(matches!(
            StreamLocator::parse("https://tail.surf/s/0123456789abcdefghjkmnpqrstvwxyz#r="),
            Err(StreamUrlError::InvalidToken)
        ));
        assert!(matches!(
            StreamLocator::parse(
                "https://tail.surf/s/0123456789abcdefghjkmnpqrstvwxyz#r=too-short"
            ),
            Err(StreamUrlError::InvalidToken)
        ));
        assert!(matches!(
            StreamLocator::parse(
                "https://tail.surf/s/0123456789abcdefghjkmnpqrstvwxyz#r=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA+"
            ),
            Err(StreamUrlError::InvalidToken)
        ));
        assert!(matches!(
            StreamLocator::parse(
                "https://tail.surf/s/0123456789abcdefghjkmnpqrstvwxyz#r=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ),
            Err(StreamUrlError::InvalidToken)
        ));
        assert!(matches!(
            StreamLocator::parse(&format!(
                "https://tail.surf/s/0123456789abcdefghjkmnpqrstvwxyz#w={TOKEN}&r={TOKEN}"
            )),
            Err(StreamUrlError::MultipleTokens)
        ));
    }

    #[test]
    fn builds_share_url() {
        let base_url = Url::parse("http://user:password@localhost:8787/old?query=yes#fragment")
            .expect("base URL");
        let stream_id = STREAM_ID.parse::<StreamId>().expect("stream id");
        let token = BearerToken::from(TOKEN);

        let url = stream_url(&base_url, &stream_id, TokenPermissions::owner(), &token)
            .expect("valid stream token");

        assert_eq!(
            url.as_str(),
            format!("http://localhost:8787/s/0123456789abcdefghjkmnpqrstvwxyz#o={TOKEN}")
        );
    }

    #[test]
    fn rejects_invalid_token_when_building_share_url() {
        let base_url = Url::parse("http://localhost:8787").expect("base URL");
        let stream_id = STREAM_ID.parse::<StreamId>().expect("stream id");

        assert!(matches!(
            stream_url(
                &base_url,
                &stream_id,
                TokenPermissions::owner(),
                &BearerToken::from("too-short")
            ),
            Err(StreamUrlError::InvalidToken)
        ));
        assert!(matches!(
            stream_url(
                &base_url,
                &stream_id,
                TokenPermissions::owner(),
                &BearerToken::from("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            ),
            Err(StreamUrlError::InvalidToken)
        ));
    }

    #[test]
    fn rejects_non_http_share_urls_when_parsing_and_building() {
        assert!(matches!(
            StreamLocator::parse(&format!(
                "ftp://tail.surf/s/0123456789abcdefghjkmnpqrstvwxyz#r={TOKEN}"
            )),
            Err(StreamUrlError::InvalidScheme(scheme)) if scheme == "ftp"
        ));

        let base_url = Url::parse("ftp://tail.surf").expect("base URL");
        let stream_id = STREAM_ID.parse::<StreamId>().expect("stream id");
        assert!(matches!(
            stream_url(
                &base_url,
                &stream_id,
                TokenPermissions::read(),
                &BearerToken::from(TOKEN)
            ),
            Err(StreamUrlError::InvalidScheme(scheme)) if scheme == "ftp"
        ));
    }
}
