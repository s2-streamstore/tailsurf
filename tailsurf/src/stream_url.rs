//! Parsing and construction for human-facing `/s/{stream_id}` stream links.

use secrecy::ExposeSecret;
use url::{Url, form_urlencoded};

use crate::{LinkPermissions, LinkSecret, StreamId, ids::is_canonical_base64url_32};

/// Default origin for Tailsurf stream links.
pub const DEFAULT_WEB_BASE_URL: &str = "https://tail.surf";
/// Encoded length of a 256-bit stream link.
pub const LINK_SECRET_ENCODED_LENGTH: usize = crate::ids::BASE64URL_32_ENCODED_LEN;
/// Declared permission and secret value decoded from a stream link fragment.
#[derive(Clone, Debug)]
pub struct StreamLinkParam {
    /// Permissions named by the fragment key.
    pub declared_permissions: LinkPermissions,
    /// Secret link value decoded from the fragment value.
    pub secret: LinkSecret,
}

/// Stream ID and optional authorization extracted from a stream link.
#[derive(Clone, Debug)]
pub struct StreamLocator {
    /// Stream named by the `/s/{stream_id}` path.
    pub stream_id: StreamId,
    /// Optional single link fragment.
    pub link: Option<StreamLinkParam>,
}

impl StreamLocator {
    /// Parses a complete stream link and rejects malformed or ambiguous fragments.
    pub fn parse(input: &str) -> Result<Self, StreamLinkError> {
        let url = Url::parse(input)?;
        validate_web_scheme(&url)?;
        let stream_id = parse_stream_id(&url)?;

        let link = url.fragment().map(parse_fragment).transpose()?.flatten();

        Ok(Self { stream_id, link })
    }

    /// Returns the secret only when the fragment declares matching permissions.
    ///
    /// The server remains authoritative. This check selects a local client mode without a remote
    /// preflight.
    pub fn link_declaring(
        &self,
        required: impl Fn(LinkPermissions) -> bool,
    ) -> Option<&LinkSecret> {
        self.link
            .as_ref()
            .filter(|candidate| required(candidate.declared_permissions))
            .map(|candidate| &candidate.secret)
    }
}

/// Builds a stream link after validating the canonical unpadded base64url secret.
pub fn stream_link(
    base_url: &Url,
    stream_id: &StreamId,
    permissions: LinkPermissions,
    secret: &LinkSecret,
) -> Result<Url, StreamLinkError> {
    validate_link_secret(secret.expose_secret())?;
    let mut url = base_url.clone();
    validate_web_scheme(&url)?;
    url.set_username("")
        .map_err(|()| StreamLinkError::InvalidBaseUrl)?;
    url.set_password(None)
        .map_err(|()| StreamLinkError::InvalidBaseUrl)?;
    url.set_path(&format!("/s/{stream_id}"));
    url.set_query(None);

    let fragment = form_urlencoded::Serializer::new(String::new())
        .append_pair(permissions.as_str(), secret.expose_secret())
        .finish();
    url.set_fragment(Some(&fragment));

    Ok(url)
}

/// Returns the default Tailsurf web origin.
pub fn default_web_base_url() -> Url {
    Url::parse(DEFAULT_WEB_BASE_URL).expect("default tsf web base URL is valid")
}

fn parse_stream_id(url: &Url) -> Result<StreamId, StreamLinkError> {
    let mut segments = url
        .path_segments()
        .ok_or(StreamLinkError::InvalidStreamPath)?;

    match (segments.next(), segments.next(), segments.next()) {
        (Some("s"), Some(stream_id), None) => StreamId::decode(stream_id)
            .map_err(|source| StreamLinkError::InvalidStreamId { source }),
        _ => Err(StreamLinkError::InvalidStreamPath),
    }
}

fn parse_fragment(fragment: &str) -> Result<Option<StreamLinkParam>, StreamLinkError> {
    let mut pairs = form_urlencoded::parse(fragment.as_bytes());
    let Some((permissions, link)) = pairs.next() else {
        return Ok(None);
    };
    let permissions = permissions.parse()?;
    validate_link_secret(&link)?;
    if pairs.next().is_some() {
        return Err(StreamLinkError::MultipleLinks);
    }
    let link = link.into_owned();

    Ok(Some(StreamLinkParam {
        declared_permissions: permissions,
        secret: link.into(),
    }))
}

fn validate_link_secret(secret: &str) -> Result<(), StreamLinkError> {
    is_canonical_base64url_32(secret)
        .then_some(())
        .ok_or(StreamLinkError::InvalidLinkSecret)
}

fn validate_web_scheme(url: &Url) -> Result<(), StreamLinkError> {
    if matches!(url.scheme(), "http" | "https") {
        Ok(())
    } else {
        Err(StreamLinkError::InvalidScheme(url.scheme().to_owned()))
    }
}

/// Error returned while parsing a Tailsurf stream link.
#[derive(Debug, thiserror::Error)]
pub enum StreamLinkError {
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
    /// The fragment link is not canonical 256-bit unpadded base64url.
    #[error("stream link secret must be canonical 43-character unpadded base64url")]
    InvalidLinkSecret,
    /// More than one link parameter appears in the fragment.
    #[error("stream URL fragment contains multiple links")]
    MultipleLinks,
}

#[cfg(test)]
mod tests {
    use super::*;

    const STREAM_ID: &str = "0123456789abcdefghjkmnpqrstvwxyz";
    const SECRET: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

    #[test]
    fn parses_stream_link() {
        let locator = StreamLocator::parse(&format!(
            "https://tail.surf/s/0123456789abcdefghjkmnpqrstvwxyz#w={SECRET}"
        ))
        .expect("stream URL");

        assert_eq!(
            locator.stream_id,
            STREAM_ID.parse::<StreamId>().expect("stream id")
        );
        let link = locator.link.expect("link");
        assert_eq!(link.declared_permissions.to_string(), "w");
        assert_eq!(link.secret.expose_secret(), SECRET);
    }

    #[test]
    fn parses_percent_encoded_link_fragment_and_ignores_query_params() {
        let encoded_secret = format!("%41{}", &SECRET[1..]);
        let locator = StreamLocator::parse(&format!(
            "https://tail.surf/s/0123456789abcdefghjkmnpqrstvwxyz?view=raw#o={encoded_secret}"
        ))
        .expect("stream URL");

        assert_eq!(
            locator.stream_id,
            STREAM_ID.parse::<StreamId>().expect("stream id")
        );
        let link = locator.link.expect("link");
        assert_eq!(link.declared_permissions.to_string(), "o");
        assert_eq!(link.secret.expose_secret(), SECRET);
    }

    #[test]
    fn rejects_invalid_paths_permissions_empty_secrets_and_multiple_links() {
        assert!(matches!(
            StreamLocator::parse("https://tail.surf/not-a-stream"),
            Err(StreamLinkError::InvalidStreamPath)
        ));
        assert!(matches!(
            StreamLocator::parse(&format!(
                "https://tail.surf/s/0123456789abcdefghjkmnpqrstvwxyz#x={SECRET}"
            )),
            Err(StreamLinkError::InvalidPermissions(_))
        ));
        assert!(matches!(
            StreamLocator::parse("https://tail.surf/s/0123456789abcdefghjkmnpqrstvwxyz#r="),
            Err(StreamLinkError::InvalidLinkSecret)
        ));
        assert!(matches!(
            StreamLocator::parse(
                "https://tail.surf/s/0123456789abcdefghjkmnpqrstvwxyz#r=too-short"
            ),
            Err(StreamLinkError::InvalidLinkSecret)
        ));
        assert!(matches!(
            StreamLocator::parse(
                "https://tail.surf/s/0123456789abcdefghjkmnpqrstvwxyz#r=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA+"
            ),
            Err(StreamLinkError::InvalidLinkSecret)
        ));
        assert!(matches!(
            StreamLocator::parse(
                "https://tail.surf/s/0123456789abcdefghjkmnpqrstvwxyz#r=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ),
            Err(StreamLinkError::InvalidLinkSecret)
        ));
        assert!(matches!(
            StreamLocator::parse(&format!(
                "https://tail.surf/s/0123456789abcdefghjkmnpqrstvwxyz#w={SECRET}&r={SECRET}"
            )),
            Err(StreamLinkError::MultipleLinks)
        ));
    }

    #[test]
    fn builds_stream_link() {
        let base_url = Url::parse("http://user:password@localhost:8787/old?query=yes#fragment")
            .expect("base URL");
        let stream_id = STREAM_ID.parse::<StreamId>().expect("stream id");
        let link = LinkSecret::from(SECRET);

        let url = stream_link(&base_url, &stream_id, LinkPermissions::owner(), &link)
            .expect("valid stream link");

        assert_eq!(
            url.as_str(),
            format!("http://localhost:8787/s/0123456789abcdefghjkmnpqrstvwxyz#o={SECRET}")
        );
    }

    #[test]
    fn rejects_invalid_link_secret_when_building_stream_link() {
        let base_url = Url::parse("http://localhost:8787").expect("base URL");
        let stream_id = STREAM_ID.parse::<StreamId>().expect("stream id");

        assert!(matches!(
            stream_link(
                &base_url,
                &stream_id,
                LinkPermissions::owner(),
                &LinkSecret::from("too-short")
            ),
            Err(StreamLinkError::InvalidLinkSecret)
        ));
        assert!(matches!(
            stream_link(
                &base_url,
                &stream_id,
                LinkPermissions::owner(),
                &LinkSecret::from("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            ),
            Err(StreamLinkError::InvalidLinkSecret)
        ));
    }

    #[test]
    fn rejects_non_http_stream_links_when_parsing_and_building() {
        assert!(matches!(
            StreamLocator::parse(&format!(
                "ftp://tail.surf/s/0123456789abcdefghjkmnpqrstvwxyz#r={SECRET}"
            )),
            Err(StreamLinkError::InvalidScheme(scheme)) if scheme == "ftp"
        ));

        let base_url = Url::parse("ftp://tail.surf").expect("base URL");
        let stream_id = STREAM_ID.parse::<StreamId>().expect("stream id");
        assert!(matches!(
            stream_link(
                &base_url,
                &stream_id,
                LinkPermissions::read(),
                &LinkSecret::from(SECRET)
            ),
            Err(StreamLinkError::InvalidScheme(scheme)) if scheme == "ftp"
        ));
    }
}
