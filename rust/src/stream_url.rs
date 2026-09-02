//! Parsing and construction for human-facing record and terminal links.

use url::{Url, form_urlencoded};

use crate::{
    LinkPermissions, LinkSecret, StreamId,
    protocol::{MAX_SAFE_INTEGER_U64, rest::parse_canonical_decimal_u64},
};

/// Encoded length of a 24-byte stream link credential.
pub const LINK_SECRET_ENCODED_LENGTH: usize = LinkSecret::ENCODED_LEN;
/// Declared permission and secret value decoded from a stream link fragment.
#[derive(Clone, Debug)]
pub struct StreamLinkParam {
    /// Permissions named by the fragment key.
    pub declared_permissions: LinkPermissions,
    /// Secret link value decoded from the fragment value.
    pub secret: LinkSecret,
}

/// Browser record anchor decoded from a stream link fragment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StreamAnchor {
    /// Physical sequence number to highlight within the supported selector range.
    pub seq_num: u64,
}

/// Browser workspace selected by a stream link path.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StreamRoute {
    /// The ordinary `/s/{stream_id}` workspace.
    Stream,
    /// The terminal `/t/{stream_id}` workspace.
    Terminal,
}

/// Stream ID and optional client-only state extracted from a stream link.
#[derive(Clone, Debug)]
pub struct StreamLocator {
    /// Stream named by the browser path.
    pub stream_id: StreamId,
    /// Browser workspace selected by the path.
    pub route: StreamRoute,
    /// Optional single link fragment.
    pub link: Option<StreamLinkParam>,
    /// Optional browser record anchor.
    pub anchor: Option<StreamAnchor>,
}

impl StreamLocator {
    /// Parses a complete stream link and rejects malformed or ambiguous fragments.
    pub fn parse(input: &str) -> Result<Self, StreamLinkError> {
        let url = Url::parse(input)?;
        validate_web_scheme(&url)?;
        if url.query().is_some() {
            return Err(StreamLinkError::QueryNotAllowed);
        }
        let (route, stream_id) = parse_stream_path(&url)?;

        let (link, anchor) = url
            .fragment()
            .map(parse_fragment)
            .transpose()?
            .unwrap_or((None, None));

        if route == StreamRoute::Terminal && anchor.is_some() {
            return Err(StreamLinkError::TerminalAnchorNotAllowed);
        }

        Ok(Self {
            stream_id,
            route,
            link,
            anchor,
        })
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
    build_link(base_url, stream_id, permissions, secret, "s")
}

/// Builds a terminal link after validating the canonical unpadded base64url secret.
pub fn terminal_link(
    base_url: &Url,
    stream_id: &StreamId,
    permissions: LinkPermissions,
    secret: &LinkSecret,
) -> Result<Url, StreamLinkError> {
    build_link(base_url, stream_id, permissions, secret, "t")
}

fn build_link(
    base_url: &Url,
    stream_id: &StreamId,
    permissions: LinkPermissions,
    secret: &LinkSecret,
    prefix: &str,
) -> Result<Url, StreamLinkError> {
    let mut url = public_resource_url(base_url, stream_id, prefix)?;

    let fragment = form_urlencoded::Serializer::new(String::new())
        .append_pair(permissions.as_str(), secret.expose_secret())
        .finish();
    url.set_fragment(Some(&fragment));

    Ok(url)
}

/// Builds the fragment-less `/s/{stream_id}` URL, normalizing the base like [`stream_link`].
pub fn public_stream_url(base_url: &Url, stream_id: &StreamId) -> Result<Url, StreamLinkError> {
    public_resource_url(base_url, stream_id, "s")
}

/// Builds the fragment-less `/t/{stream_id}` URL for a public terminal session.
pub fn public_terminal_url(base_url: &Url, stream_id: &StreamId) -> Result<Url, StreamLinkError> {
    public_resource_url(base_url, stream_id, "t")
}

fn public_resource_url(
    base_url: &Url,
    stream_id: &StreamId,
    prefix: &str,
) -> Result<Url, StreamLinkError> {
    let mut url = base_url.clone();
    validate_web_scheme(&url)?;
    url.set_username("")
        .map_err(|()| StreamLinkError::InvalidBaseUrl)?;
    url.set_password(None)
        .map_err(|()| StreamLinkError::InvalidBaseUrl)?;
    url.set_path(&format!("/{prefix}/{stream_id}"));
    url.set_query(None);
    url.set_fragment(None);

    Ok(url)
}

fn parse_stream_path(url: &Url) -> Result<(StreamRoute, StreamId), StreamLinkError> {
    let mut segments = url
        .path_segments()
        .ok_or(StreamLinkError::InvalidStreamPath)?;

    match (segments.next(), segments.next(), segments.next()) {
        (Some(prefix @ ("s" | "t")), Some(stream_id), None) => StreamId::decode(stream_id)
            .map(|stream_id| {
                let route = if prefix == "t" {
                    StreamRoute::Terminal
                } else {
                    StreamRoute::Stream
                };
                (route, stream_id)
            })
            .map_err(|source| StreamLinkError::InvalidStreamId { source }),
        _ => Err(StreamLinkError::InvalidStreamPath),
    }
}

fn parse_fragment(
    fragment: &str,
) -> Result<(Option<StreamLinkParam>, Option<StreamAnchor>), StreamLinkError> {
    if fragment.is_empty() || fragment.split('&').any(str::is_empty) {
        return Err(StreamLinkError::InvalidFragment);
    }
    let mut link = None;
    let mut anchor = None;
    for (key, value) in form_urlencoded::parse(fragment.as_bytes()) {
        if key == "at" {
            if anchor.is_some() {
                return Err(StreamLinkError::MultipleAnchors);
            }
            let seq_num = parse_canonical_decimal_u64(&value)
                .filter(|value| *value <= MAX_SAFE_INTEGER_U64)
                .ok_or(StreamLinkError::InvalidAnchor)?;
            anchor = Some(StreamAnchor { seq_num });
            continue;
        }
        if link.is_some() {
            return Err(StreamLinkError::MultipleLinks);
        }
        let declared_permissions = key.parse()?;
        let secret = value
            .parse()
            .map_err(|_| StreamLinkError::InvalidLinkSecret)?;
        link = Some(StreamLinkParam {
            declared_permissions,
            secret,
        });
    }

    Ok((link, anchor))
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
    /// The path is not exactly `/s/{stream_id}` or `/t/{stream_id}`.
    #[error("stream URL path must be /s/{{stream_id}} or /t/{{stream_id}}")]
    InvalidStreamPath,
    /// The path contains a malformed stream identifier.
    #[error("stream URL has invalid stream id")]
    InvalidStreamId {
        #[source]
        /// Source UBID decoding failure.
        source: ubid::DecodeError,
    },
    /// Browser stream URLs do not carry transport read controls or other query state.
    #[error("stream URLs do not accept query parameters")]
    QueryNotAllowed,
    /// The fragment does not contain a link credential or anchor.
    #[error("stream URL fragment must contain a credential or at")]
    InvalidFragment,
    /// The fragment key is not a valid permission string.
    #[error("stream URL fragment has invalid permissions")]
    InvalidPermissions(#[from] crate::PermissionsError),
    /// The fragment link is not canonical 24-byte unpadded base64url.
    #[error("stream link secret must be canonical 32-character unpadded base64url")]
    InvalidLinkSecret,
    /// More than one link parameter appears in the fragment.
    #[error("stream URL fragment contains multiple links")]
    MultipleLinks,
    /// The `at` fragment value is not a canonical decimal u64.
    #[error("stream URL at anchor must be a canonical decimal u64")]
    InvalidAnchor,
    /// More than one `at` parameter appears in the fragment.
    #[error("stream URL fragment contains multiple at parameters")]
    MultipleAnchors,
    /// Record anchors do not apply to terminal sessions.
    #[error("terminal URLs do not accept record anchors")]
    TerminalAnchorNotAllowed,
}

#[cfg(test)]
mod tests {
    use super::*;

    const STREAM_ID: &str = "0123456789abcdefghjkmnpqrstvwxyz";
    const SECRET: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

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
        assert_eq!(locator.route, StreamRoute::Stream);
        let link = locator.link.expect("link");
        assert_eq!(link.declared_permissions.to_string(), "w");
        assert_eq!(link.secret.expose_secret(), SECRET);
    }

    #[test]
    fn parses_percent_encoded_link_fragment() {
        let encoded_secret = format!("%41{}", &SECRET[1..]);
        let locator = StreamLocator::parse(&format!(
            "https://tail.surf/s/0123456789abcdefghjkmnpqrstvwxyz#o={encoded_secret}"
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
    fn parses_a_composite_client_fragment() {
        let locator =
            StreamLocator::parse(&format!("https://tail.surf/s/{STREAM_ID}#r={SECRET}&at=50"))
                .expect("stream URL");

        assert_eq!(locator.anchor, Some(StreamAnchor { seq_num: 50 }));
        assert_eq!(locator.link.expect("link").secret.expose_secret(), SECRET);
        let anchor_only = StreamLocator::parse(&format!("https://tail.surf/s/{STREAM_ID}#at=0"))
            .expect("anchor URL");
        assert!(anchor_only.link.is_none());
        assert_eq!(anchor_only.anchor, Some(StreamAnchor { seq_num: 0 }));
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
            StreamLocator::parse(&format!(
                "https://tail.surf/s/0123456789abcdefghjkmnpqrstvwxyz#w={SECRET}&r={SECRET}"
            )),
            Err(StreamLinkError::MultipleLinks)
        ));
        for fragment in ["at=01", "at=-1", "at=9007199254740992"] {
            assert!(matches!(
                StreamLocator::parse(&format!(
                    "https://tail.surf/s/0123456789abcdefghjkmnpqrstvwxyz#{fragment}"
                )),
                Err(StreamLinkError::InvalidAnchor)
            ));
        }
        assert!(matches!(
            StreamLocator::parse("https://tail.surf/s/0123456789abcdefghjkmnpqrstvwxyz#at=1&at=2"),
            Err(StreamLinkError::MultipleAnchors)
        ));
        assert!(matches!(
            StreamLocator::parse("https://tail.surf/s/0123456789abcdefghjkmnpqrstvwxyz#at=1&"),
            Err(StreamLinkError::InvalidFragment)
        ));
        assert!(matches!(
            StreamLocator::parse(
                "https://tail.surf/s/0123456789abcdefghjkmnpqrstvwxyz?seq_num=100"
            ),
            Err(StreamLinkError::QueryNotAllowed)
        ));
    }

    #[test]
    fn builds_stream_link() {
        let base_url = Url::parse("http://user:password@localhost:8787/old?query=yes#fragment")
            .expect("base URL");
        let stream_id = STREAM_ID.parse::<StreamId>().expect("stream id");
        let link: LinkSecret = SECRET.parse().expect("canonical secret");

        let url = stream_link(&base_url, &stream_id, LinkPermissions::owner(), &link)
            .expect("valid stream link");

        assert_eq!(
            url.as_str(),
            format!("http://localhost:8787/s/0123456789abcdefghjkmnpqrstvwxyz#o={SECRET}")
        );
    }

    #[test]
    fn parses_and_builds_terminal_links() {
        let base_url = Url::parse("https://tail.surf/ignored").expect("base URL");
        let stream_id = STREAM_ID.parse::<StreamId>().expect("stream id");
        let secret = SECRET.parse::<LinkSecret>().expect("secret");
        let url = terminal_link(
            &base_url,
            &stream_id,
            LinkPermissions::read_write(),
            &secret,
        )
        .expect("terminal link");

        assert_eq!(
            url.as_str(),
            format!("https://tail.surf/t/{STREAM_ID}#rw={SECRET}")
        );
        let locator = StreamLocator::parse(url.as_str()).expect("terminal URL");
        assert_eq!(locator.route, StreamRoute::Terminal);
        assert_eq!(locator.stream_id, stream_id);
        assert_eq!(
            public_terminal_url(&base_url, &stream_id)
                .expect("public terminal URL")
                .as_str(),
            format!("https://tail.surf/t/{STREAM_ID}")
        );
        assert!(matches!(
            StreamLocator::parse(&format!("https://tail.surf/t/{STREAM_ID}#at=1")),
            Err(StreamLinkError::TerminalAnchorNotAllowed)
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
                &SECRET.parse().expect("canonical secret")
            ),
            Err(StreamLinkError::InvalidScheme(scheme)) if scheme == "ftp"
        ));
    }
}
