//! Bounded REST and WebSocket clients for the TSF service.

use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    str::FromStr,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use rand::{Rng, RngExt};
use reqwest::StatusCode;
use secrecy::ExposeSecret;
use serde::{Deserialize, de::DeserializeOwned};
use tokio::{
    net::TcpStream,
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot},
    task::JoinHandle,
    time::{sleep, timeout},
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async_with_config,
    tungstenite::{
        Error as WebSocketError, Message,
        client::IntoClientRequest,
        error::ProtocolError,
        http::{HeaderValue, header::SEC_WEBSOCKET_PROTOCOL},
    },
};
use url::Url;

use crate::{
    LinkId, LinkSecret, StreamId, WriterId,
    ids::{encode_base64url_32, is_canonical_base64url_32},
    protocol::{
        rest::{
            AppendJsonRecord, AppendRecordData, AppendRecordsRequest, AppendRecordsResponse,
            CreateStreamRequest, CreateStreamResponse, IssueLinkRequest, IssueLinkResponse,
            ListLinksResponse, RenameLinkRequest, RestRecordPart, SseCaughtUpEvent, SseReadRecord,
            SseRecordsEvent, StreamInfoResponse, UpdateStreamRequest,
        },
        ws::{
            DEFAULT_READ_TAIL_OFFSET, MAX_PLAYBACK_RATE_PERMILLE, MAX_READ_SELECTOR_VALUE,
            MIN_PLAYBACK_RATE_PERMILLE, ReadStart, ReadStreamOptions, WriteStreamOptions,
            frame::{
                AppendRecord, ClientFrame, FrameCodecError, MAX_APPEND_BATCH_RECORDS,
                MAX_BATCH_PAYLOAD_BYTES, MAX_RECORD_BYTES, PartHeader, ReadCaughtUp, ReadRecord,
                ReadSnapshotBoundary, ReadStreamInfo, RecordFormat, ServerFrame, TSF_WS_PROTOCOL,
            },
        },
    },
};

type ClientWebSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

const API_PREFIX: &str = "/api/v1";

/// Timeouts, retry behavior, and API origin for [`TsfClient`].
#[derive(Clone, Debug)]
pub struct TsfClientConfig {
    /// Service origin without the `/api/v1` namespace.
    pub api_base_url: Url,
    /// Per-request timeout for REST operations.
    pub rest_request_timeout: Duration,
    /// Timeout for establishing and upgrading a WebSocket.
    pub websocket_connect_timeout: Duration,
    /// Timeout for authentication, frame sends, and append acknowledgements.
    pub websocket_operation_timeout: Duration,
    /// Optional idle timeout while waiting for a read frame. Protocol heartbeats reset the timer.
    /// `None` waits indefinitely.
    pub websocket_read_idle_timeout: Option<Duration>,
    /// Retry policy for anonymous stream creation, idempotent metadata reads, socket setup, and
    /// consecutive read reconnects without a delivered record.
    pub retry_policy: RetryPolicy,
}

impl TsfClientConfig {
    /// Creates a configuration with bounded defaults for the supplied API origin.
    pub fn new(api_base_url: Url) -> Self {
        Self {
            api_base_url,
            rest_request_timeout: Duration::from_secs(10),
            websocket_connect_timeout: Duration::from_secs(10),
            websocket_operation_timeout: Duration::from_secs(30),
            websocket_read_idle_timeout: Some(Duration::from_secs(60)),
            retry_policy: RetryPolicy::default(),
        }
    }
}

impl Default for TsfClientConfig {
    fn default() -> Self {
        Self::new(default_api_base_url())
    }
}

/// Exponential-backoff policy for idempotent operations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetryPolicy {
    /// Total attempts including the initial request. Zero is treated as one.
    pub max_attempts: usize,
    /// Delay before the first retry.
    pub initial_backoff: Duration,
    /// Maximum delay between attempts.
    pub max_backoff: Duration,
}

impl RetryPolicy {
    /// Returns a policy that performs exactly one attempt.
    pub fn none() -> Self {
        Self {
            max_attempts: 1,
            initial_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
        }
    }

    fn attempt_count(self) -> usize {
        self.max_attempts.max(1)
    }

    fn next_backoff(self, current: Duration) -> Duration {
        current
            .checked_mul(2)
            .unwrap_or(self.max_backoff)
            .min(self.max_backoff)
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_backoff: Duration::from_millis(200),
            max_backoff: Duration::from_secs(2),
        }
    }
}

/// Cloneable TSF control-plane and v1 data-plane client.
///
/// Anonymous stream creation is retried with one idempotency key. Other mutating REST operations
/// are not retried because a timeout may occur after the service applies the mutation. Metadata
/// reads and initial socket setup use [`RetryPolicy`]. Durable writer recovery is owned by
/// [`TsfProducer`].
#[derive(Clone)]
pub struct TsfClient {
    config: TsfClientConfig,
    http: reqwest::Client,
}

impl TsfClient {
    /// Creates a client for the default [tail.surf](https://tail.surf) API origin.
    pub fn new() -> Self {
        Self::with_config(TsfClientConfig::default())
    }

    /// Creates a client for an explicit API origin with default timeouts.
    pub fn with_api_base_url(api_base_url: Url) -> Self {
        Self::with_config(TsfClientConfig::new(api_base_url))
    }

    /// Creates a client from a complete configuration.
    pub fn with_config(config: TsfClientConfig) -> Self {
        Self {
            config,
            http: reqwest::Client::new(),
        }
    }

    /// Returns the configured API origin without the `/api/v1` namespace.
    pub fn api_base_url(&self) -> &Url {
        &self.config.api_base_url
    }

    /// Returns the complete immutable client configuration.
    pub fn config(&self) -> &TsfClientConfig {
        &self.config
    }

    /// Creates a stream and returns its metadata and newly issued secret links.
    ///
    /// The client generates one idempotency key for this logical call and reuses it while retrying
    /// transient failures according to policy.
    pub async fn create_stream(
        &self,
        request: &CreateStreamRequest,
    ) -> Result<CreateStreamResponse, TsfClientError> {
        let idempotency_key = CreateStreamIdempotencyKey::new_random();
        self.create_stream_with_idempotency_key(request, &idempotency_key)
            .await
    }

    /// Creates a logical stream using a caller-owned idempotency key.
    ///
    /// Recovery also requires the request's separate `recovery_secret`. The idempotency key alone
    /// cannot recover owner credentials.
    pub async fn create_stream_with_idempotency_key(
        &self,
        request: &CreateStreamRequest,
        idempotency_key: &CreateStreamIdempotencyKey,
    ) -> Result<CreateStreamResponse, TsfClientError> {
        let mut request = request.clone();
        request
            .recovery_secret
            .get_or_insert_with(random_link_secret);
        self.retry_when(
            || {
                self.send_json_with_bearer(
                    self.http
                        .post(self.rest_url("/streams"))
                        .header("Idempotency-Key", idempotency_key.expose_secret())
                        .json(&request),
                    "create stream",
                    None,
                )
            },
            TsfClientError::is_recoverable_create_failure,
        )
        .await
    }

    /// Retrieves current stream metadata, retrying transient failures according to policy.
    ///
    /// Private streams require a read-capable stream link. Public streams may pass `None`.
    pub async fn get_stream(
        &self,
        stream_id: &StreamId,
        link_secret: Option<&LinkSecret>,
    ) -> Result<StreamInfoResponse, TsfClientError> {
        self.get_json_with_bearer(format!("/streams/{stream_id}"), "get stream", link_secret)
            .await
    }

    /// Updates owner-controlled stream settings.
    ///
    /// This mutation is attempted once and is not transparently retried.
    pub async fn update_stream(
        &self,
        stream_id: &StreamId,
        request: &UpdateStreamRequest,
        owner_link_secret: &LinkSecret,
    ) -> Result<StreamInfoResponse, TsfClientError> {
        self.retry_transient(|| {
            self.send_json_with_bearer(
                self.http
                    .patch(self.rest_url(&format!("/streams/{stream_id}")))
                    .json(request),
                "update stream",
                Some(owner_link_secret),
            )
        })
        .await
    }

    /// Permanently deletes a stream.
    ///
    /// This mutation is attempted once and is not transparently retried.
    pub async fn delete_stream(
        &self,
        stream_id: &StreamId,
        owner_link_secret: &LinkSecret,
    ) -> Result<(), TsfClientError> {
        self.retry_transient(|| {
            self.send_empty(
                self.http
                    .delete(self.rest_url(&format!("/streams/{stream_id}"))),
                "delete stream",
                Some(owner_link_secret),
            )
        })
        .await
    }

    /// Issues a new secret stream link.
    ///
    /// This mutation is attempted once and is not transparently retried.
    pub async fn issue_link(
        &self,
        stream_id: &StreamId,
        request: &IssueLinkRequest,
        owner_link_secret: &LinkSecret,
    ) -> Result<IssueLinkResponse, TsfClientError> {
        self.retry_transient(|| {
            self.send_json_with_bearer(
                self.http
                    .post(self.rest_url(&format!("/streams/{stream_id}/links")))
                    .json(request),
                "issue link",
                Some(owner_link_secret),
            )
        })
        .await
    }

    /// Lists retained, non-secret link metadata, retrying transient failures according to policy.
    pub async fn list_links(
        &self,
        stream_id: &StreamId,
        owner_link_secret: &LinkSecret,
    ) -> Result<ListLinksResponse, TsfClientError> {
        let mut links = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let suffix = cursor.as_ref().map_or_else(String::new, |cursor| {
                format!(
                    "?limit=100&cursor={}",
                    url::form_urlencoded::byte_serialize(cursor.as_bytes()).collect::<String>()
                )
            });
            let page: ListLinksResponse = self
                .get_json_with_bearer(
                    format!("/streams/{stream_id}/links{suffix}"),
                    "list links",
                    Some(owner_link_secret),
                )
                .await?;
            links.extend(page.links);
            match page.next_cursor {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        Ok(ListLinksResponse {
            links,
            next_cursor: None,
        })
    }

    /// Renames a stream link without changing its identity, secret, permissions, or expiry.
    pub async fn rename_link(
        &self,
        stream_id: &StreamId,
        link_id: &LinkId,
        request: &RenameLinkRequest,
        owner_link_secret: &LinkSecret,
    ) -> Result<(), TsfClientError> {
        self.retry_transient(|| {
            self.send_empty(
                self.http
                    .patch(self.rest_url(&format!("/streams/{stream_id}/links/{link_id}")))
                    .json(request),
                "rename link",
                Some(owner_link_secret),
            )
        })
        .await
    }

    /// Revokes a stream link by its non-secret identifier.
    ///
    /// This mutation is attempted once and is not transparently retried.
    pub async fn revoke_link(
        &self,
        stream_id: &StreamId,
        link_id: &LinkId,
        owner_link_secret: &LinkSecret,
    ) -> Result<(), TsfClientError> {
        self.retry_transient(|| {
            self.send_empty(
                self.http
                    .delete(self.rest_url(&format!("/streams/{stream_id}/links/{link_id}"))),
                "revoke link",
                Some(owner_link_secret),
            )
        })
        .await
    }

    /// Atomically appends one durable JSON batch without opening a WebSocket.
    ///
    /// A retry keeps writer identity and writer sequence numbers stable. An ambiguous response may
    /// create physical duplicates. Logical readers suppress those duplicates.
    pub async fn append_records(
        &self,
        stream_id: &StreamId,
        writer_id: WriterId,
        records: &[WriteRecord],
        match_seq_num: Option<u64>,
        write_link_secret: &LinkSecret,
    ) -> Result<AppendRecordsResponse, TsfClientError> {
        if records.is_empty() || records.len() > 128 {
            return Err(TsfClientError::InvalidStatelessAppend(
                "record count must be between 1 and 128",
            ));
        }
        let first_writer_seq_num = records[0].writer_seq_num;
        let mut json_records = Vec::with_capacity(records.len());
        for (index, record) in records.iter().enumerate() {
            record.validate()?;
            if record.writer_seq_num
                != first_writer_seq_num.checked_add(index as u64).ok_or(
                    TsfClientError::InvalidStatelessAppend("writer sequence overflow"),
                )?
            {
                return Err(TsfClientError::InvalidStatelessAppend(
                    "writer sequence numbers must be contiguous",
                ));
            }
            let data = if record.format == RecordFormat::Transcript {
                match std::str::from_utf8(&record.data) {
                    Ok(text) => AppendRecordData::Text {
                        text: text.to_owned(),
                    },
                    Err(_) => AppendRecordData::Bytes {
                        base64: URL_SAFE_NO_PAD.encode(&record.data),
                        format: "transcript".to_owned(),
                    },
                }
            } else {
                AppendRecordData::Bytes {
                    base64: URL_SAFE_NO_PAD.encode(&record.data),
                    format: "bytes".to_owned(),
                }
            };
            json_records.push(AppendJsonRecord {
                data,
                part: Some(RestRecordPart {
                    index: record.part.index(),
                    is_final: record.part.is_final(),
                }),
            });
        }
        let request = AppendRecordsRequest {
            writer_id: URL_SAFE_NO_PAD.encode(writer_id.as_bytes()),
            first_writer_seq_num,
            records: json_records,
            match_seq_num,
        };
        self.retry_transient(|| {
            self.send_json_with_bearer(
                self.http
                    .post(self.rest_url(&format!("/streams/{stream_id}/records")))
                    .json(&request),
                "append records",
                Some(write_link_secret),
            )
        })
        .await
    }

    /// Connects the standard bounded, reconnecting durable producer.
    pub async fn connect_producer(
        &self,
        options: WriteStreamOptions,
    ) -> Result<TsfProducer, TsfClientError> {
        self.connect_producer_with_config(options, TsfProducerConfig::default())
            .await
    }

    /// Connects a durable producer with explicit in-flight and reconnect bounds.
    pub async fn connect_producer_with_config(
        &self,
        options: WriteStreamOptions,
        config: TsfProducerConfig,
    ) -> Result<TsfProducer, TsfClientError> {
        let session = self.connect_append_session(options.clone()).await?;
        TsfProducer::new(self.clone(), options, session, config)
    }

    /// Connects a low-level append session that sends records and receives ack ranges directly.
    ///
    /// Unlike [`TsfProducer`], this session does not retain or resend unacknowledged records.
    pub async fn connect_append_session(
        &self,
        options: WriteStreamOptions,
    ) -> Result<TsfAppendSession, TsfClientError> {
        let url = self.websocket_url(&format!("/streams/{}/write", options.stream_id))?;
        let connect_timeout = self.config.websocket_connect_timeout;
        let operation_timeout = self.config.websocket_operation_timeout;
        let opening_frame = ClientFrame::OpenWrite {
            writer_id: options.writer_id,
            link_secret: options.link_secret.clone(),
        }
        .encode()?;

        self.retry_transient(|| {
            let url = url.clone();
            let opening_frame = opening_frame.clone();

            async move {
                let mut ws =
                    connect_websocket(url, connect_timeout, operation_timeout, opening_frame)
                        .await?;
                with_timeout(operation_timeout, "writer ready", expect_ready(&mut ws)).await?;

                Ok(TsfAppendSession {
                    ws,
                    operation_timeout,
                })
            }
        })
        .await
    }

    /// Connects a resumable read session at the requested position and bounds.
    pub async fn connect_reader(
        &self,
        mut options: ReadStreamOptions,
    ) -> Result<TsfReadSession, TsfClientError> {
        let ConnectedReadSocket {
            socket,
            stream_info,
            snapshot_boundary,
        } = self.connect_read_socket(options.clone()).await?;
        apply_snapshot_boundary(&mut options, snapshot_boundary);
        Ok(TsfReadSession::new(
            self.clone(),
            options,
            socket,
            stream_info,
            None,
            snapshot_boundary,
        ))
    }

    /// Connects a resumable SSE reader.
    ///
    /// Private credentials stay in the bearer header. Reconnects use the last safe absolute
    /// sequence cursor reported by records or `caught_up`.
    pub async fn connect_sse_reader(
        &self,
        mut options: ReadStreamOptions,
    ) -> Result<TsfSseReadSession, TsfClientError> {
        validate_read_options(&options)?;
        let connection = self.open_sse_connection(&options).await?;
        if let Some(boundary) = connection.snapshot_boundary {
            apply_snapshot_boundary(&mut options, Some(boundary));
        }
        Ok(TsfSseReadSession {
            client: self.clone(),
            options,
            body: connection.body,
            buffer: connection.buffer,
            queued_events: connection.queued_events,
            queued_records: VecDeque::new(),
            stream_info: connection.stream_info.expect("validated stream_info event"),
            last_caught_up: None,
            snapshot_boundary: connection.snapshot_boundary,
            reconnect_attempts: 0,
        })
    }

    async fn open_sse_connection(
        &self,
        options: &ReadStreamOptions,
    ) -> Result<SseConnection, TsfClientError> {
        self.retry_transient(|| async {
            let mut url = self.rest_url(&format!("/streams/{}/records", options.stream_id));
            append_sse_query(&mut url, options);
            let mut request = self.http.get(url).header("Accept", "text/event-stream");
            if let Some(secret) = options.link_secret.as_ref() {
                request = request.bearer_auth(secret.expose_secret());
            }
            if let Some(ReadStart::SeqNum(seq_num)) = options.start {
                request = request.header("Last-Event-ID", seq_num.to_string());
            }
            let response = request.send().await?;
            if !response.status().is_success() {
                return Err(http_status_error(response, "read SSE").await);
            }
            let snapshot_boundary = snapshot_boundary_headers(response.headers())?;
            let mut connection = SseConnection {
                body: Box::pin(response.bytes_stream()),
                buffer: Vec::new(),
                queued_events: VecDeque::new(),
                stream_info: None,
                snapshot_boundary,
            };
            let event = next_sse_event(
                &mut connection.body,
                &mut connection.buffer,
                &mut connection.queued_events,
            )
            .await?
            .ok_or(TsfClientError::InvalidSse(
                "response ended before stream_info",
            ))?;
            if event.event != "stream_info" {
                return Err(TsfClientError::InvalidSse("first event is not stream_info"));
            }
            connection.stream_info = Some(
                serde_json::from_str(&event.data)
                    .map_err(|_| TsfClientError::InvalidSse("invalid stream_info event"))?,
            );
            Ok(connection)
        })
        .await
    }

    async fn connect_read_socket(
        &self,
        options: ReadStreamOptions,
    ) -> Result<ConnectedReadSocket, TsfClientError> {
        if let Some(start) = options.start {
            let value = match start {
                ReadStart::SeqNum(value)
                | ReadStart::TimestampMs(value)
                | ReadStart::TailOffset(value) => value,
            };
            if value > MAX_READ_SELECTOR_VALUE {
                return Err(TsfClientError::InvalidReadSelector {
                    value,
                    maximum: MAX_READ_SELECTOR_VALUE,
                });
            }
        }
        if let Some(rate) = options.playback_rate_permille {
            if !(MIN_PLAYBACK_RATE_PERMILLE..=MAX_PLAYBACK_RATE_PERMILLE).contains(&rate) {
                return Err(TsfClientError::InvalidPlaybackRate {
                    value: rate,
                    minimum: MIN_PLAYBACK_RATE_PERMILLE,
                    maximum: MAX_PLAYBACK_RATE_PERMILLE,
                });
            }
            if options.until.is_none() && !options.snapshot {
                return Err(TsfClientError::PlaybackRequiresUntil);
            }
        }
        if options.snapshot && options.until.is_some() {
            return Err(TsfClientError::SnapshotWithUntil);
        }
        let opening_frame = ClientFrame::OpenRead {
            link_secret: options.link_secret.clone(),
            start: options
                .start
                .unwrap_or(ReadStart::TailOffset(DEFAULT_READ_TAIL_OFFSET)),
            count: options.count,
            until: options.until,
            playback_rate_permille: options.playback_rate_permille,
            snapshot: options.snapshot,
        }
        .encode()?;
        let url = self.websocket_url(&format!("/streams/{}/read", options.stream_id))?;
        let connect_timeout = self.config.websocket_connect_timeout;
        let operation_timeout = self.config.websocket_operation_timeout;
        let read_idle_timeout = self.config.websocket_read_idle_timeout;
        let snapshot = options.snapshot;

        self.retry_transient(|| {
            let url = url.clone();
            let opening_frame = opening_frame.clone();

            async move {
                let mut ws =
                    connect_websocket(url, connect_timeout, operation_timeout, opening_frame)
                        .await?;
                let handshake = with_timeout(
                    operation_timeout,
                    "reader handshake",
                    expect_read_handshake(&mut ws, snapshot),
                )
                .await?;

                Ok(ConnectedReadSocket {
                    socket: ReadSocket {
                        ws,
                        read_idle_timeout,
                        pending_records: VecDeque::new(),
                    },
                    stream_info: handshake.stream_info,
                    snapshot_boundary: handshake.snapshot_boundary,
                })
            }
        })
        .await
    }

    fn rest_url(&self, path: &str) -> Url {
        let mut url = self.config.api_base_url.clone();
        url.set_path(&format!("{API_PREFIX}{path}"));
        url.set_query(None);
        url.set_fragment(None);
        url
    }

    fn apply_rest_auth(
        &self,
        request: reqwest::RequestBuilder,
        link_secret: Option<&LinkSecret>,
    ) -> reqwest::RequestBuilder {
        if let Some(secret) = link_secret {
            request.bearer_auth(secret.expose_secret())
        } else {
            request
        }
    }

    fn websocket_url(&self, path: &str) -> Result<Url, TsfClientError> {
        let mut url = self.rest_url(path);
        let scheme = match url.scheme() {
            "http" => "ws",
            "https" => "wss",
            other => return Err(TsfClientError::InvalidWebSocketScheme(other.to_owned())),
        };
        url.set_scheme(scheme)
            .map_err(|_| TsfClientError::InvalidWebSocketScheme(url.scheme().to_owned()))?;
        Ok(url)
    }

    async fn get_json_with_bearer<T: DeserializeOwned>(
        &self,
        path: String,
        operation: &'static str,
        link_secret: Option<&LinkSecret>,
    ) -> Result<T, TsfClientError> {
        let url = self.rest_url(&path);
        self.retry_transient(|| {
            self.send_json_with_bearer(self.http.get(url.clone()), operation, link_secret)
        })
        .await
    }

    async fn send_json_with_bearer<T: DeserializeOwned>(
        &self,
        request: reqwest::RequestBuilder,
        operation: &'static str,
        link_secret: Option<&LinkSecret>,
    ) -> Result<T, TsfClientError> {
        let response = self
            .apply_rest_auth(request, link_secret)
            .timeout(self.config.rest_request_timeout)
            .send()
            .await?;
        json_response(response, operation).await
    }

    async fn send_empty(
        &self,
        request: reqwest::RequestBuilder,
        operation: &'static str,
        link_secret: Option<&LinkSecret>,
    ) -> Result<(), TsfClientError> {
        let response = self
            .apply_rest_auth(request, link_secret)
            .timeout(self.config.rest_request_timeout)
            .send()
            .await?;
        let status = response.status();
        if status == StatusCode::NO_CONTENT {
            return Ok(());
        }
        Err(http_status_error(response, operation).await)
    }

    async fn retry_transient<T, Fut>(&self, run: impl FnMut() -> Fut) -> Result<T, TsfClientError>
    where
        Fut: Future<Output = Result<T, TsfClientError>>,
    {
        self.retry_when(run, TsfClientError::is_retryable).await
    }

    async fn retry_when<T, Fut>(
        &self,
        mut run: impl FnMut() -> Fut,
        should_retry: impl Fn(&TsfClientError) -> bool,
    ) -> Result<T, TsfClientError>
    where
        Fut: Future<Output = Result<T, TsfClientError>>,
    {
        let retry_policy = self.config.retry_policy;
        let attempts = retry_policy.attempt_count();
        let mut backoff = retry_policy.initial_backoff;

        for attempt in 1..=attempts {
            match run().await {
                Ok(value) => return Ok(value),
                Err(error) if attempt < attempts && should_retry(&error) => {
                    let delay = error.retry_after().unwrap_or_else(|| {
                        let jitter = rand::rng().random_range(0.5_f64..=1.5_f64);
                        backoff.mul_f64(jitter)
                    });
                    if !delay.is_zero() {
                        sleep(delay).await;
                    }
                    backoff = retry_policy.next_backoff(backoff);
                }
                Err(error) => return Err(error),
            }
        }

        unreachable!("retry loop always returns from a non-empty attempt range")
    }
}

impl Default for TsfClient {
    fn default() -> Self {
        Self::new()
    }
}

/// Returns the default `https://tail.surf` API origin.
pub fn default_api_base_url() -> Url {
    Url::parse("https://tail.surf").expect("default tsf API base URL is valid")
}

fn random_link_secret() -> LinkSecret {
    let mut bytes = [0_u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    encode_base64url_32(&bytes).into()
}

/// Non-authorizing idempotency key for one logical stream-creation request.
#[derive(Clone, Debug)]
pub struct CreateStreamIdempotencyKey(LinkSecret);

impl CreateStreamIdempotencyKey {
    /// Generates a cryptographically random canonical 256-bit key.
    pub fn new_random() -> Self {
        let mut bytes = [0_u8; 32];
        rand::rng().fill_bytes(&mut bytes);
        Self(encode_base64url_32(&bytes).into())
    }
}

impl FromStr for CreateStreamIdempotencyKey {
    type Err = InvalidCreateStreamIdempotencyKey;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if is_canonical_base64url_32(value) {
            Ok(Self(value.into()))
        } else {
            Err(InvalidCreateStreamIdempotencyKey)
        }
    }
}

impl ExposeSecret<str> for CreateStreamIdempotencyKey {
    fn expose_secret(&self) -> &str {
        self.0.expose_secret()
    }
}

/// Error returned for a malformed stream-creation idempotency key.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("create idempotency key must be canonical 43-character unpadded base64url")]
pub struct InvalidCreateStreamIdempotencyKey;

/// Low-level authenticated write socket without retained-record recovery.
pub struct TsfAppendSession {
    ws: ClientWebSocket,
    operation_timeout: Duration,
}

/// Maximum payload bytes a producer may retain before acknowledgement.
///
/// This matches the TSF writer socket's hard queued-payload bound.
pub const MAX_PRODUCER_UNACKED_PAYLOAD_BYTES: usize = 5 * 1024 * 1024;
/// Maximum records a producer may retain before acknowledgement.
///
/// This matches the TSF writer socket's hard queued-message bound.
pub const MAX_PRODUCER_UNACKED_RECORDS: usize = 128;

/// Memory, concurrency, and reconnect bounds for [`TsfProducer`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TsfProducerConfig {
    /// Maximum total payload bytes retained until durability acknowledgement. Must not exceed
    /// [`MAX_PRODUCER_UNACKED_PAYLOAD_BYTES`].
    pub max_unacked_bytes: usize,
    /// Maximum number of records retained until durability acknowledgement. Must not exceed
    /// [`MAX_PRODUCER_UNACKED_RECORDS`].
    pub max_unacked_records: usize,
    /// Maximum consecutive producer reconnect attempts before failing pending records.
    pub max_reconnect_attempts: usize,
}

impl TsfProducerConfig {
    fn validate(self) -> Result<Self, TsfClientError> {
        if self.max_unacked_bytes == 0 {
            return Err(TsfClientError::InvalidProducerConfig(
                "max_unacked_bytes must be greater than zero".to_owned(),
            ));
        }
        if self.max_unacked_bytes > MAX_PRODUCER_UNACKED_PAYLOAD_BYTES {
            return Err(TsfClientError::InvalidProducerConfig(format!(
                "max_unacked_bytes must not exceed {}",
                MAX_PRODUCER_UNACKED_PAYLOAD_BYTES
            )));
        }
        if self.max_unacked_records == 0 {
            return Err(TsfClientError::InvalidProducerConfig(
                "max_unacked_records must be greater than zero".to_owned(),
            ));
        }
        if self.max_unacked_records > MAX_PRODUCER_UNACKED_RECORDS {
            return Err(TsfClientError::InvalidProducerConfig(format!(
                "max_unacked_records must not exceed {}",
                MAX_PRODUCER_UNACKED_RECORDS
            )));
        }
        Ok(self)
    }
}

impl Default for TsfProducerConfig {
    fn default() -> Self {
        Self {
            max_unacked_bytes: MAX_PRODUCER_UNACKED_PAYLOAD_BYTES,
            max_unacked_records: MAX_PRODUCER_UNACKED_RECORDS,
            max_reconnect_attempts: 3,
        }
    }
}

/// One physical record submitted by a writer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteRecord {
    /// Writer-local sequence number, reused if this record is retransmitted.
    pub writer_seq_num: u64,
    /// Logical split-part metadata.
    pub part: PartHeader,
    /// Presentation hint for the payload.
    pub format: RecordFormat,
    /// Exact payload bytes, bounded by the TSF physical record limit.
    pub data: Bytes,
}

impl WriteRecord {
    /// Creates a physical record without allocating when the input already owns compatible bytes.
    pub fn new(
        writer_seq_num: u64,
        part: PartHeader,
        format: RecordFormat,
        data: impl IntoRecordData,
    ) -> Self {
        Self {
            writer_seq_num,
            part,
            format,
            data: data.into_record_data(),
        }
    }

    fn validate(&self) -> Result<(), TsfClientError> {
        if self.data.len() > MAX_RECORD_BYTES {
            return Err(FrameCodecError::RecordTooLarge {
                actual: self.data.len(),
                max: MAX_RECORD_BYTES,
            }
            .into());
        }
        Ok(())
    }

    fn unacked_bytes(&self) -> usize {
        self.data.len().max(1)
    }
}

/// Server acknowledgement mapping a contiguous writer range to durable sequence numbers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AppendAck {
    /// First acknowledged writer-local sequence number.
    pub writer_seq_start: u64,
    /// Last acknowledged writer-local sequence number, inclusive.
    pub writer_seq_end: u64,
    /// Durable sequence number assigned to the first acknowledged record.
    pub seq_start: u64,
    /// Durable sequence number assigned to the last acknowledged record, inclusive.
    pub seq_end: u64,
}

impl AppendAck {
    /// Returns whether the inclusive writer range contains a sequence number.
    pub const fn contains_writer_seq(self, writer_seq_num: u64) -> bool {
        self.writer_seq_start <= writer_seq_num && writer_seq_num <= self.writer_seq_end
    }

    /// Returns the number of records when writer and durable ranges are valid and equal in length.
    pub fn record_count(self) -> Result<u64, TsfClientError> {
        let writer_count = inclusive_range_len(self.writer_seq_start, self.writer_seq_end)
            .ok_or(TsfClientError::InvalidAppendAck(self))?;
        let durable_count = inclusive_range_len(self.seq_start, self.seq_end)
            .ok_or(TsfClientError::InvalidAppendAck(self))?;
        if writer_count != durable_count {
            return Err(TsfClientError::InvalidAppendAck(self));
        }
        Ok(writer_count)
    }

    fn validate(self) -> Result<Self, TsfClientError> {
        self.record_count()?;
        Ok(self)
    }
}

/// Durable assignment for one submitted record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AppendReceipt {
    /// Submitted writer-local sequence number.
    pub writer_seq_num: u64,
    /// Durable sequence number assigned by the service.
    pub seq_num: u64,
    /// Ack range that covered this record.
    pub ack: AppendAck,
}

/// Future that resolves when one submitted record is durable or permanently fails.
pub struct AppendTicket {
    rx: oneshot::Receiver<Result<AppendReceipt, TsfClientError>>,
}

impl AppendTicket {
    /// Polls for a completed receipt without registering an async wakeup.
    ///
    /// Returns `None` while the record remains pending.
    pub fn try_recv(&mut self) -> Option<Result<AppendReceipt, TsfClientError>> {
        match self.rx.try_recv() {
            Ok(result) => Some(result),
            Err(oneshot::error::TryRecvError::Empty) => None,
            Err(oneshot::error::TryRecvError::Closed) => {
                Some(Err(TsfClientError::AppendProducerDropped))
            }
        }
    }
}

impl Future for AppendTicket {
    type Output = Result<AppendReceipt, TsfClientError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.rx).poll(cx) {
            Poll::Ready(Ok(result)) => Poll::Ready(result),
            Poll::Ready(Err(_)) => Poll::Ready(Err(TsfClientError::AppendProducerDropped)),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Bounded durable producer that retains unacknowledged records and resends them across transient
/// interruptions.
pub struct TsfProducer {
    cmd_tx: mpsc::Sender<ProducerCommand>,
    byte_permits: Arc<Semaphore>,
    record_permits: Arc<Semaphore>,
    max_unacked_bytes: usize,
    task: Option<JoinHandle<()>>,
}

impl TsfProducer {
    fn new(
        client: TsfClient,
        options: WriteStreamOptions,
        session: TsfAppendSession,
        config: TsfProducerConfig,
    ) -> Result<Self, TsfClientError> {
        let config = config.validate()?;
        let command_capacity = config.max_unacked_records + 1;
        let (cmd_tx, cmd_rx) = mpsc::channel(command_capacity);
        let task = tokio::spawn(run_producer(client, options, session, cmd_rx, config));

        Ok(Self {
            cmd_tx,
            byte_permits: Arc::new(Semaphore::new(config.max_unacked_bytes)),
            record_permits: Arc::new(Semaphore::new(config.max_unacked_records)),
            max_unacked_bytes: config.max_unacked_bytes,
            task: Some(task),
        })
    }

    /// Waits for window capacity, submits a record, and returns its durability ticket.
    pub async fn submit(&self, record: WriteRecord) -> Result<AppendTicket, TsfClientError> {
        let permit = self.reserve(record.unacked_bytes()).await?;
        permit.submit(record)
    }

    /// Reserves one record slot and at least one byte of the unacknowledged window.
    ///
    /// The returned permit owns capacity until it is dropped or submitted.
    pub async fn reserve(&self, bytes: usize) -> Result<WritePermit, TsfClientError> {
        let bytes = bytes.max(1);
        if bytes > self.max_unacked_bytes {
            return Err(TsfClientError::AppendRecordExceedsProducerWindow {
                bytes,
                max_unacked_bytes: self.max_unacked_bytes,
            });
        }

        let record_permit = self
            .record_permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| TsfClientError::AppendProducerClosed)?;
        let byte_permit = self
            .byte_permits
            .clone()
            .acquire_many_owned(bytes as u32)
            .await
            .map_err(|_| TsfClientError::AppendProducerClosed)?;
        let cmd_tx_permit = self
            .cmd_tx
            .clone()
            .reserve_owned()
            .await
            .map_err(|_| TsfClientError::AppendProducerClosed)?;

        Ok(WritePermit {
            cmd_tx_permit,
            byte_permit,
            record_permit,
            reserved_bytes: bytes,
        })
    }

    /// Stops accepting records, waits for every pending durability acknowledgement, and joins the
    /// producer task.
    pub async fn close(mut self) -> Result<(), TsfClientError> {
        let (done_tx, done_rx) = oneshot::channel();
        self.cmd_tx
            .send(ProducerCommand::Close { done_tx })
            .await
            .map_err(|_| TsfClientError::AppendProducerClosed)?;

        let result = done_rx
            .await
            .map_err(|_| TsfClientError::AppendProducerDropped)?;

        if let Some(task) = self.task.take() {
            task.await
                .map_err(|error| TsfClientError::AppendProducerFailed(error.to_string()))?;
        }

        result
    }
}

impl Drop for TsfProducer {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

/// Owned capacity in a producer's record and byte windows.
///
/// Dropping an unused permit releases its capacity.
pub struct WritePermit {
    cmd_tx_permit: mpsc::OwnedPermit<ProducerCommand>,
    byte_permit: OwnedSemaphorePermit,
    record_permit: OwnedSemaphorePermit,
    reserved_bytes: usize,
}

impl WritePermit {
    /// Submits a record no larger than the reserved capacity without awaiting another window slot.
    pub fn submit(self, record: WriteRecord) -> Result<AppendTicket, TsfClientError> {
        record.validate()?;
        let bytes = record.unacked_bytes();
        if bytes > self.reserved_bytes {
            return Err(TsfClientError::AppendRecordExceedsReservedBytes {
                bytes,
                reserved_bytes: self.reserved_bytes,
            });
        }

        let (ack_tx, ack_rx) = oneshot::channel();
        self.cmd_tx_permit.send(ProducerCommand::Submit {
            record,
            ack_tx,
            byte_permit: self.byte_permit,
            record_permit: self.record_permit,
        });

        Ok(AppendTicket { rx: ack_rx })
    }
}

/// Conversion into payload bytes accepted by [`WriteRecord::new`].
pub trait IntoRecordData {
    /// Converts this value into reference-counted immutable bytes.
    fn into_record_data(self) -> Bytes;
}
/// Sealed marker for types that own their bytes and implement `Into<Bytes>`.
trait OwnedIntoBytes: Into<Bytes> {}
impl OwnedIntoBytes for Bytes {}
impl OwnedIntoBytes for Vec<u8> {}
impl OwnedIntoBytes for Box<[u8]> {}
impl OwnedIntoBytes for String {}

impl<T: OwnedIntoBytes> IntoRecordData for T {
    fn into_record_data(self) -> Bytes {
        self.into()
    }
}

impl IntoRecordData for &Bytes {
    fn into_record_data(self) -> Bytes {
        self.clone()
    }
}

impl IntoRecordData for &[u8] {
    fn into_record_data(self) -> Bytes {
        Bytes::copy_from_slice(self)
    }
}

impl<const N: usize> IntoRecordData for &[u8; N] {
    fn into_record_data(self) -> Bytes {
        Bytes::copy_from_slice(&self[..])
    }
}

impl IntoRecordData for &str {
    fn into_record_data(self) -> Bytes {
        Bytes::copy_from_slice(self.as_bytes())
    }
}

impl TsfAppendSession {
    /// Sends one physical record under the operation timeout.
    pub async fn send(&mut self, record: WriteRecord) -> Result<(), TsfClientError> {
        let operation_timeout = self.operation_timeout;

        with_timeout(operation_timeout, "send append frame", async move {
            self.buffer_batch(std::iter::once(&record)).await?;
            self.flush().await
        })
        .await
    }

    /// Encodes one batch into the socket's write buffer, leaving the flush to the caller.
    async fn buffer_batch<'a>(
        &mut self,
        records: impl IntoIterator<Item = &'a WriteRecord>,
    ) -> Result<(), TsfClientError> {
        let records = records
            .into_iter()
            .map(|record| AppendRecord {
                writer_seq_num: record.writer_seq_num,
                part: record.part,
                format: record.format,
                data: record.data.clone(),
            })
            .collect();
        let frame = ClientFrame::AppendBatch(records).encode()?;
        self.ws.feed(Message::Binary(frame)).await?;
        Ok(())
    }

    /// Writes every buffered append batch to the transport in one flush.
    async fn flush(&mut self) -> Result<(), TsfClientError> {
        self.ws.flush().await?;
        Ok(())
    }

    /// Waits for and validates the next durability acknowledgement.
    ///
    /// Returns `None` when the service closes the socket normally before another ack.
    pub async fn next_ack(&mut self) -> Result<Option<AppendAck>, TsfClientError> {
        match with_timeout(
            self.operation_timeout,
            "append acknowledgement",
            next_server_frame(&mut self.ws),
        )
        .await?
        {
            Some(ServerFrame::Ack {
                writer_seq_start,
                writer_seq_end,
                seq_start,
                seq_end,
            }) => AppendAck {
                writer_seq_start,
                writer_seq_end,
                seq_start,
                seq_end,
            }
            .validate()
            .map(Some),
            Some(frame) => Err(TsfClientError::UnexpectedServerFrame(server_frame_name(
                &frame,
            ))),
            None => Ok(None),
        }
    }
}

enum ProducerCommand {
    Submit {
        record: WriteRecord,
        ack_tx: oneshot::Sender<Result<AppendReceipt, TsfClientError>>,
        byte_permit: OwnedSemaphorePermit,
        record_permit: OwnedSemaphorePermit,
    },
    Close {
        done_tx: oneshot::Sender<Result<(), TsfClientError>>,
    },
}

struct PendingAppend {
    record: WriteRecord,
    ack_tx: oneshot::Sender<Result<AppendReceipt, TsfClientError>>,
    _byte_permit: OwnedSemaphorePermit,
    _record_permit: OwnedSemaphorePermit,
}

async fn run_producer(
    client: TsfClient,
    options: WriteStreamOptions,
    mut session: TsfAppendSession,
    mut cmd_rx: mpsc::Receiver<ProducerCommand>,
    config: TsfProducerConfig,
) {
    let mut pending = VecDeque::new();
    let mut close_tx: Option<oneshot::Sender<Result<(), TsfClientError>>> = None;
    let mut reconnect_attempts = 0;

    loop {
        tokio::select! {
            cmd = cmd_rx.recv(), if close_tx.is_none() => {
                match cmd {
                    Some(command) => {
                        let first_new = pending.len();
                        drain_submissions(&mut pending, &mut cmd_rx, &mut close_tx, command);

                        if let Err(error) = send_retained(&mut session, &pending, first_new).await
                            && let Err(error) = recover_pending_appends(
                                &mut session,
                                &client,
                                &options,
                                &pending,
                                config.max_reconnect_attempts,
                                &mut reconnect_attempts,
                                error,
                            )
                            .await
                        {
                            finish_producer_error(&mut pending, &mut close_tx, error);
                            return;
                        }
                    }
                    None => {
                        fail_pending(&mut pending, "append producer dropped");
                        return;
                    }
                }
            }

            ack = session.next_ack(), if !pending.is_empty() => {
                match ack {
                    Ok(Some(ack)) => {
                        if let Err(error) = dispatch_ack(ack, &mut pending) {
                            finish_producer_error(&mut pending, &mut close_tx, error);
                            return;
                        }
                        reconnect_attempts = 0;
                    }
                    Ok(None) => {
                        if let Err(error) = recover_pending_appends(
                            &mut session,
                            &client,
                            &options,
                            &pending,
                            config.max_reconnect_attempts,
                            &mut reconnect_attempts,
                            TsfClientError::WebSocketClosed,
                        )
                        .await
                        {
                            finish_producer_error(&mut pending, &mut close_tx, error);
                            return;
                        }
                    }
                    Err(error) => {
                        if let Err(error) = recover_pending_appends(
                            &mut session,
                            &client,
                            &options,
                            &pending,
                            config.max_reconnect_attempts,
                            &mut reconnect_attempts,
                            error,
                        )
                        .await
                        {
                            finish_producer_error(&mut pending, &mut close_tx, error);
                            return;
                        }
                    }
                }
            }
        }

        if close_tx.is_some() && pending.is_empty() {
            if let Some(close_tx) = close_tx.take() {
                let _ = close_tx.send(Ok(()));
            }
            return;
        }
    }
}

/// Moves the submitted record and every already-queued submission into `pending`.
///
/// This never awaits, so a batch is fully retained before any I/O can fail: a failed or timed-out
/// write leaves every record in `pending` for reconnect resend.
fn drain_submissions(
    pending: &mut VecDeque<PendingAppend>,
    cmd_rx: &mut mpsc::Receiver<ProducerCommand>,
    close_tx: &mut Option<oneshot::Sender<Result<(), TsfClientError>>>,
    first: ProducerCommand,
) {
    let mut command = Some(first);

    while let Some(ProducerCommand::Submit {
        record,
        ack_tx,
        byte_permit,
        record_permit,
    }) = command
    {
        pending.push_back(PendingAppend {
            record,
            ack_tx,
            _byte_permit: byte_permit,
            _record_permit: record_permit,
        });
        command = cmd_rx.try_recv().ok();
    }

    if let Some(ProducerCommand::Close { done_tx }) = command {
        *close_tx = Some(done_tx);
    }
}

/// Writes the records from `from` onwards under one operation timeout and one flush.
async fn send_retained(
    session: &mut TsfAppendSession,
    pending: &VecDeque<PendingAppend>,
    from: usize,
) -> Result<(), TsfClientError> {
    if from >= pending.len() {
        return Ok(());
    }
    let operation_timeout = session.operation_timeout;

    with_timeout(operation_timeout, "send append frames", async move {
        let mut records = pending.iter().skip(from).peekable();
        while records.peek().is_some() {
            let mut batch = Vec::with_capacity(MAX_APPEND_BATCH_RECORDS);
            let mut payload_bytes = 0;
            while batch.len() < MAX_APPEND_BATCH_RECORDS {
                let Some(next) = records.peek() else {
                    break;
                };
                if !batch.is_empty()
                    && next.record.data.len() > MAX_BATCH_PAYLOAD_BYTES - payload_bytes
                {
                    break;
                }
                let next = records.next().expect("peeked record");
                payload_bytes += next.record.data.len();
                batch.push(&next.record);
            }
            session.buffer_batch(batch).await?;
        }
        session.flush().await
    })
    .await
}
async fn recover_pending_appends(
    session: &mut TsfAppendSession,
    client: &TsfClient,
    options: &WriteStreamOptions,
    pending: &VecDeque<PendingAppend>,
    max_reconnect_attempts: usize,
    reconnect_attempts: &mut usize,
    mut error: TsfClientError,
) -> Result<(), TsfClientError> {
    if !error.is_retryable() {
        return Err(error);
    }

    while *reconnect_attempts < max_reconnect_attempts {
        *reconnect_attempts += 1;
        match client.connect_append_session(options.clone()).await {
            Ok(mut connected) => match send_retained(&mut connected, pending, 0).await {
                Ok(()) => {
                    *session = connected;
                    return Ok(());
                }
                Err(next_error) if next_error.is_retryable() => error = next_error,
                Err(next_error) => return Err(next_error),
            },
            Err(next_error) if next_error.is_retryable() => error = next_error,
            Err(next_error) => return Err(next_error),
        }
    }

    Err(error)
}

fn dispatch_ack(
    ack: AppendAck,
    pending: &mut VecDeque<PendingAppend>,
) -> Result<(), TsfClientError> {
    let record_count =
        usize::try_from(ack.record_count()?).map_err(|_| TsfClientError::InvalidAppendAck(ack))?;
    if record_count > pending.len() {
        return Err(TsfClientError::InvalidAppendAck(ack));
    }

    for (item, writer_seq_num) in pending
        .iter()
        .take(record_count)
        .zip(ack.writer_seq_start..=ack.writer_seq_end)
    {
        if item.record.writer_seq_num < writer_seq_num {
            return Err(TsfClientError::AppendNotAcknowledged {
                writer_seq_num: item.record.writer_seq_num,
                ack,
            });
        }
        if item.record.writer_seq_num > writer_seq_num {
            return Err(TsfClientError::InvalidAppendAck(ack));
        }
    }

    for ((item, writer_seq_num), seq_num) in pending
        .drain(..record_count)
        .zip(ack.writer_seq_start..=ack.writer_seq_end)
        .zip(ack.seq_start..=ack.seq_end)
    {
        let _ = item.ack_tx.send(Ok(AppendReceipt {
            writer_seq_num,
            seq_num,
            ack,
        }));
    }

    Ok(())
}

fn finish_producer_error(
    pending: &mut VecDeque<PendingAppend>,
    close_tx: &mut Option<oneshot::Sender<Result<(), TsfClientError>>>,
    error: TsfClientError,
) {
    fail_pending(pending, error.to_string());
    if let Some(close_tx) = close_tx.take() {
        let _ = close_tx.send(Err(error));
    }
}

fn fail_pending(pending: &mut VecDeque<PendingAppend>, message: impl Into<String>) {
    let message = message.into();
    while let Some(pending) = pending.pop_front() {
        let _ = pending
            .ack_tx
            .send(Err(TsfClientError::AppendProducerFailed(message.clone())));
    }
}

fn inclusive_range_len(start: u64, end: u64) -> Option<u64> {
    end.checked_sub(start)?.checked_add(1)
}

/// Resumable reader that advances its sequence position after every delivered record.
///
/// Transient transport and service interruptions reconnect from the next sequence number. Normal
/// completion and configured bounds return `None`; protocol and policy failures surface as errors.
type SseBody = Pin<Box<dyn futures_util::Stream<Item = Result<Bytes, reqwest::Error>> + Send>>;

struct ParsedSseEvent {
    event: String,
    data: String,
}

struct SseConnection {
    body: SseBody,
    buffer: Vec<u8>,
    queued_events: VecDeque<ParsedSseEvent>,
    stream_info: Option<ReadStreamInfo>,
    snapshot_boundary: Option<ReadSnapshotBoundary>,
}

/// Resumable HTTP event-stream reader.
pub struct TsfSseReadSession {
    client: TsfClient,
    options: ReadStreamOptions,
    body: SseBody,
    buffer: Vec<u8>,
    queued_events: VecDeque<ParsedSseEvent>,
    queued_records: VecDeque<ReadRecord>,
    stream_info: ReadStreamInfo,
    last_caught_up: Option<ReadCaughtUp>,
    snapshot_boundary: Option<ReadSnapshotBoundary>,
    reconnect_attempts: usize,
}

impl TsfSseReadSession {
    /// Returns authorized stream metadata from the opening event.
    pub fn stream_info(&self) -> &ReadStreamInfo {
        &self.stream_info
    }

    /// Returns the fixed boundary captured for a snapshot read.
    pub fn snapshot_boundary(&self) -> Option<ReadSnapshotBoundary> {
        self.snapshot_boundary
    }

    /// Returns the most recent reconnect-safe caught-up position.
    pub fn last_caught_up(&self) -> Option<ReadCaughtUp> {
        self.last_caught_up
    }

    /// Returns the next record, reconnecting from the last safe absolute cursor when needed.
    pub async fn next_record(&mut self) -> Result<Option<ReadRecord>, TsfClientError> {
        loop {
            if let Some(record) = self.queued_records.pop_front() {
                self.options.start = record.seq_num.checked_add(1).map(ReadStart::SeqNum);
                if let Some(count) = self.options.count.as_mut() {
                    *count = count.saturating_sub(1);
                }
                return Ok(Some(record));
            }
            if self.options.count == Some(0)
                || matches!((self.options.start, self.options.until), (Some(ReadStart::SeqNum(next)), Some(until)) if next > until)
            {
                return Ok(None);
            }
            let event =
                next_sse_event(&mut self.body, &mut self.buffer, &mut self.queued_events).await?;
            let Some(event) = event else {
                let attempts = self.client.config.retry_policy.attempt_count();
                if self.reconnect_attempts + 1 >= attempts {
                    return Err(TsfClientError::ReadReconnectLimitExceeded {
                        max_connection_attempts: attempts,
                    });
                }
                let delay = self
                    .client
                    .config
                    .retry_policy
                    .initial_backoff
                    .checked_mul(1_u32 << self.reconnect_attempts.min(30))
                    .unwrap_or(self.client.config.retry_policy.max_backoff)
                    .min(self.client.config.retry_policy.max_backoff);
                if !delay.is_zero() {
                    sleep(delay).await;
                }
                self.reconnect_attempts += 1;
                let connection = self.client.open_sse_connection(&self.options).await?;
                self.body = connection.body;
                self.buffer = connection.buffer;
                self.queued_events = connection.queued_events;
                self.stream_info = connection.stream_info.expect("validated stream_info event");
                continue;
            };
            match event.event.as_str() {
                "records" => {
                    let batch: SseRecordsEvent = serde_json::from_str(&event.data)
                        .map_err(|_| TsfClientError::InvalidSse("invalid records event"))?;
                    for record in batch.records {
                        self.queued_records.push_back(sse_read_record(record)?);
                    }
                    self.reconnect_attempts = 0;
                }
                "caught_up" => {
                    let value: SseCaughtUpEvent = serde_json::from_str(&event.data)
                        .map_err(|_| TsfClientError::InvalidSse("invalid caught_up event"))?;
                    let caught_up = ReadCaughtUp {
                        next_seq_num: value.next_seq_num,
                        last_timestamp_ms: value.last_timestamp_ms.unwrap_or(0),
                    };
                    self.options.start = Some(ReadStart::SeqNum(caught_up.next_seq_num));
                    self.last_caught_up = Some(caught_up);
                    self.reconnect_attempts = 0;
                }
                "error" => return Err(TsfClientError::SseTerminal(event.data)),
                "stream_info" => {
                    self.stream_info = serde_json::from_str(&event.data)
                        .map_err(|_| TsfClientError::InvalidSse("invalid stream_info event"))?
                }
                _ => {}
            }
        }
    }
}

/// Resumable WebSocket reader.
pub struct TsfReadSession {
    client: TsfClient,
    options: ReadStreamOptions,
    socket: ReadSocket,
    stream_info: ReadStreamInfo,
    finished: bool,
    last_caught_up: Option<ReadCaughtUp>,
    snapshot_boundary: Option<ReadSnapshotBoundary>,
    no_progress_reconnects: usize,
    reconnect_backoff: Duration,
    pending_reconnect_backoff: Duration,
    reconnect_needed: bool,
}

impl TsfReadSession {
    fn new(
        client: TsfClient,
        options: ReadStreamOptions,
        socket: ReadSocket,
        stream_info: ReadStreamInfo,
        last_caught_up: Option<ReadCaughtUp>,
        snapshot_boundary: Option<ReadSnapshotBoundary>,
    ) -> Self {
        let reconnect_backoff = client.config.retry_policy.initial_backoff;
        Self {
            client,
            options,
            socket,
            stream_info,
            finished: false,
            last_caught_up,
            snapshot_boundary,
            no_progress_reconnects: 0,
            reconnect_backoff,
            pending_reconnect_backoff: Duration::ZERO,
            reconnect_needed: false,
        }
    }

    /// Returns the latest reconnect-safe position reported after preceding records were delivered.
    pub const fn last_caught_up(&self) -> Option<ReadCaughtUp> {
        self.last_caught_up
    }

    /// Returns the fixed exclusive end captured for a snapshot read.
    pub const fn snapshot_boundary(&self) -> Option<ReadSnapshotBoundary> {
        self.snapshot_boundary
    }

    /// Returns metadata supplied by the latest successful read handshake.
    pub const fn stream_info(&self) -> &ReadStreamInfo {
        &self.stream_info
    }

    /// Waits for the next physical record using the configured idle timeout.
    pub async fn next_record(&mut self) -> Result<Option<ReadRecord>, TsfClientError> {
        self.next_record_inner().await
    }

    /// Waits for the next physical record with a caller-supplied timeout for this operation.
    pub async fn next_record_with_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<ReadRecord>, TsfClientError> {
        with_timeout(timeout, "read stream record", self.next_record_inner()).await
    }

    async fn next_record_inner(&mut self) -> Result<Option<ReadRecord>, TsfClientError> {
        loop {
            if self.finished || read_options_exhausted(&self.options) {
                self.finished = true;
                return Ok(None);
            }
            if self.reconnect_needed {
                self.reconnect().await?;
            }

            match self.socket.next_outcome().await {
                Ok(ReadSocketOutcome::Record(record)) => {
                    self.record_delivered(record.seq_num);
                    return Ok(Some(record));
                }
                Ok(ReadSocketOutcome::Records(_)) => {
                    unreachable!("batches are drained by ReadSocket")
                }
                Ok(ReadSocketOutcome::CaughtUp(caught_up)) => {
                    self.options.start = Some(ReadStart::SeqNum(caught_up.next_seq_num));
                    self.last_caught_up = Some(caught_up);
                }
                Ok(ReadSocketOutcome::Closed) => {
                    self.finished = true;
                    return Ok(None);
                }
                Err(error) if error.is_resumable_read_interruption() => {
                    self.require_reconnect()?;
                    self.reconnect().await?;
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn reconnect(&mut self) -> Result<(), TsfClientError> {
        debug_assert!(self.reconnect_needed);
        if !self.pending_reconnect_backoff.is_zero() {
            sleep(self.pending_reconnect_backoff).await;
        }
        let ConnectedReadSocket {
            socket,
            stream_info,
            snapshot_boundary,
        } = self
            .client
            .connect_read_socket(self.options.clone())
            .await?;
        self.socket = socket;
        self.stream_info = stream_info;
        apply_snapshot_boundary(&mut self.options, snapshot_boundary);
        if snapshot_boundary.is_some() {
            self.snapshot_boundary = snapshot_boundary;
        }
        self.pending_reconnect_backoff = Duration::ZERO;
        self.reconnect_needed = false;
        Ok(())
    }

    fn require_reconnect(&mut self) -> Result<(), TsfClientError> {
        if self.reconnect_needed {
            return Ok(());
        }
        let retry_policy = self.client.config.retry_policy;
        let max_reconnects = retry_policy.attempt_count().saturating_sub(1);
        if self.no_progress_reconnects >= max_reconnects {
            return Err(TsfClientError::ReadReconnectLimitExceeded {
                max_connection_attempts: retry_policy.attempt_count(),
            });
        }
        self.no_progress_reconnects += 1;
        self.pending_reconnect_backoff = self.reconnect_backoff;
        self.reconnect_backoff = retry_policy.next_backoff(self.reconnect_backoff);
        self.reconnect_needed = true;
        Ok(())
    }

    fn record_delivered(&mut self, seq_num: u64) {
        self.no_progress_reconnects = 0;
        self.reconnect_backoff = self.client.config.retry_policy.initial_backoff;
        self.pending_reconnect_backoff = Duration::ZERO;
        self.reconnect_needed = false;
        match seq_num.checked_add(1) {
            Some(next_seq_num) => self.options.start = Some(ReadStart::SeqNum(next_seq_num)),
            None => self.finished = true,
        }

        if let Some(count) = self.options.count.as_mut() {
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.finished = true;
            }
        }

        if self.options.until.is_some_and(|until| seq_num >= until) {
            self.finished = true;
        }
    }
}

fn read_options_exhausted(options: &ReadStreamOptions) -> bool {
    options.count == Some(0)
        || matches!(
            (options.start, options.until),
            (Some(ReadStart::SeqNum(start)), Some(until)) if start > until
        )
}

struct ReadSocket {
    ws: ClientWebSocket,
    read_idle_timeout: Option<Duration>,
    pending_records: VecDeque<ReadRecord>,
}

struct ConnectedReadSocket {
    socket: ReadSocket,
    stream_info: ReadStreamInfo,
    snapshot_boundary: Option<ReadSnapshotBoundary>,
}

fn apply_snapshot_boundary(
    options: &mut ReadStreamOptions,
    boundary: Option<ReadSnapshotBoundary>,
) {
    let Some(boundary) = boundary else {
        return;
    };
    options.snapshot = false;
    if boundary.next_seq_num == 0 {
        options.count = Some(0);
    } else {
        options.until = Some(boundary.next_seq_num - 1);
    }
}

impl ReadSocket {
    async fn next_outcome(&mut self) -> Result<ReadSocketOutcome, TsfClientError> {
        loop {
            if let Some(record) = self.pending_records.pop_front() {
                return Ok(ReadSocketOutcome::Record(record));
            }
            let outcome = if let Some(read_idle_timeout) = self.read_idle_timeout {
                with_timeout(
                    read_idle_timeout,
                    "read stream record",
                    next_read_socket_frame(&mut self.ws),
                )
                .await?
            } else {
                next_read_socket_frame(&mut self.ws).await?
            };
            if let Some(ReadSocketOutcome::Records(records)) = outcome {
                self.pending_records.extend(records);
            } else if let Some(outcome) = outcome {
                return Ok(outcome);
            }
        }
    }
}

enum ReadSocketOutcome {
    Record(ReadRecord),
    Records(Vec<ReadRecord>),
    CaughtUp(ReadCaughtUp),
    Closed,
}

async fn connect_websocket(
    url: Url,
    connect_timeout: Duration,
    operation_timeout: Duration,
    opening_frame: Bytes,
) -> Result<ClientWebSocket, TsfClientError> {
    // TSF v1 sends each batch in one message, so Nagle could hold a small append back for an ACK.
    const DISABLE_NAGLE: bool = true;

    let mut request = url.as_str().into_client_request()?;
    request.headers_mut().insert(
        SEC_WEBSOCKET_PROTOCOL,
        HeaderValue::from_static(TSF_WS_PROTOCOL),
    );

    let (mut ws, response) = timeout(
        connect_timeout,
        connect_async_with_config(request, None, DISABLE_NAGLE),
    )
    .await
    .map_err(|_| TsfClientError::Timeout {
        operation: "connect websocket",
    })??;
    let selected_protocol = response
        .headers()
        .get(SEC_WEBSOCKET_PROTOCOL)
        .map(|value| value.to_str().map(str::to_owned))
        .transpose()
        .map_err(|_| TsfClientError::InvalidWebSocketProtocolHeader)?;

    if selected_protocol.as_deref() != Some(TSF_WS_PROTOCOL) {
        return Err(TsfClientError::UnexpectedWebSocketProtocol(
            selected_protocol,
        ));
    }

    timeout(operation_timeout, ws.send(Message::Binary(opening_frame)))
        .await
        .map_err(|_| TsfClientError::Timeout {
            operation: "send opening frame",
        })??;

    Ok(ws)
}

async fn with_timeout<T>(
    duration: Duration,
    operation: &'static str,
    future: impl Future<Output = Result<T, TsfClientError>>,
) -> Result<T, TsfClientError> {
    timeout(duration, future)
        .await
        .map_err(|_| TsfClientError::Timeout { operation })?
}

fn validate_read_options(options: &ReadStreamOptions) -> Result<(), TsfClientError> {
    if let Some(start) = options.start {
        let value = match start {
            ReadStart::SeqNum(value)
            | ReadStart::TimestampMs(value)
            | ReadStart::TailOffset(value) => value,
        };
        if value > MAX_READ_SELECTOR_VALUE {
            return Err(TsfClientError::InvalidReadSelector {
                value,
                maximum: MAX_READ_SELECTOR_VALUE,
            });
        }
    }
    if let Some(rate) = options.playback_rate_permille {
        if !(MIN_PLAYBACK_RATE_PERMILLE..=MAX_PLAYBACK_RATE_PERMILLE).contains(&rate) {
            return Err(TsfClientError::InvalidPlaybackRate {
                value: rate,
                minimum: MIN_PLAYBACK_RATE_PERMILLE,
                maximum: MAX_PLAYBACK_RATE_PERMILLE,
            });
        }
        if options.until.is_none() && !options.snapshot {
            return Err(TsfClientError::PlaybackRequiresUntil);
        }
    }
    if options.snapshot && options.until.is_some() {
        return Err(TsfClientError::SnapshotWithUntil);
    }
    Ok(())
}

fn append_sse_query(url: &mut Url, options: &ReadStreamOptions) {
    let mut query = url.query_pairs_mut();
    if !matches!(options.start, Some(ReadStart::SeqNum(_))) {
        match options.start {
            Some(ReadStart::TimestampMs(value)) => {
                query.append_pair("timestamp_ms", &value.to_string());
            }
            Some(ReadStart::TailOffset(value)) => {
                query.append_pair("tail_offset", &value.to_string());
            }
            _ => {}
        }
    }
    if let Some(value) = options.count {
        query.append_pair("count", &value.to_string());
    }
    if let Some(value) = options.until {
        query.append_pair("until", &value.to_string());
    }
    if let Some(value) = options.playback_rate_permille {
        query.append_pair("playback_rate_permille", &value.to_string());
    }
    if options.snapshot {
        query.append_pair("snapshot", "true");
    }
}

fn snapshot_boundary_headers(
    headers: &reqwest::header::HeaderMap,
) -> Result<Option<ReadSnapshotBoundary>, TsfClientError> {
    let Some(next) = headers.get("x-tsf-snapshot-next-seq-num") else {
        return Ok(None);
    };
    let next_seq_num = next
        .to_str()
        .ok()
        .and_then(|value| value.parse().ok())
        .ok_or(TsfClientError::InvalidSse("invalid snapshot cursor header"))?;
    let last_timestamp_ms = headers
        .get("x-tsf-snapshot-last-timestamp-ms")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
        .unwrap_or(0);
    Ok(Some(ReadSnapshotBoundary {
        next_seq_num,
        last_timestamp_ms,
    }))
}

async fn next_sse_event(
    body: &mut SseBody,
    buffer: &mut Vec<u8>,
    queued: &mut VecDeque<ParsedSseEvent>,
) -> Result<Option<ParsedSseEvent>, TsfClientError> {
    loop {
        if let Some(event) = queued.pop_front() {
            return Ok(Some(event));
        }
        while let Some((index, length)) = sse_boundary(buffer) {
            let block = buffer.drain(..index).collect::<Vec<_>>();
            buffer.drain(..length);
            if let Some(event) = parse_sse_block(&block)? {
                queued.push_back(event);
            }
        }
        if let Some(event) = queued.pop_front() {
            return Ok(Some(event));
        }
        match body.next().await {
            Some(Ok(chunk)) => {
                buffer.extend_from_slice(&chunk);
                if buffer.len() > 2 * 1024 * 1024 {
                    return Err(TsfClientError::InvalidSse("event exceeds 2 MiB"));
                }
            }
            Some(Err(error)) => return Err(error.into()),
            None => return Ok(None),
        }
    }
}

fn sse_boundary(buffer: &[u8]) -> Option<(usize, usize)> {
    for index in 0..buffer.len().saturating_sub(1) {
        if buffer[index..].starts_with(b"\r\n\r\n") {
            return Some((index, 4));
        }
        if buffer[index..].starts_with(b"\n\n") {
            return Some((index, 2));
        }
    }
    None
}

fn parse_sse_block(block: &[u8]) -> Result<Option<ParsedSseEvent>, TsfClientError> {
    let text =
        std::str::from_utf8(block).map_err(|_| TsfClientError::InvalidSse("event is not UTF-8"))?;
    let mut event = "message".to_owned();
    let mut data = Vec::new();
    for line in text.lines() {
        if line.is_empty() || line.starts_with(':') {
            continue;
        }
        let (name, value) = line.split_once(':').map_or((line, ""), |(name, value)| {
            (name, value.strip_prefix(' ').unwrap_or(value))
        });
        match name {
            "event" => event = value.to_owned(),
            "data" => data.push(value),
            _ => {}
        }
    }
    if data.is_empty() {
        Ok(None)
    } else {
        Ok(Some(ParsedSseEvent {
            event,
            data: data.join("\n"),
        }))
    }
}

fn sse_read_record(record: SseReadRecord) -> Result<ReadRecord, TsfClientError> {
    let writer = URL_SAFE_NO_PAD
        .decode(record.writer_id)
        .map_err(|_| TsfClientError::InvalidSse("invalid writer_id"))?;
    let writer: [u8; WriterId::BYTE_LEN] = writer
        .try_into()
        .map_err(|_| TsfClientError::InvalidSse("invalid writer_id length"))?;
    let (format, data) = match record.data {
        AppendRecordData::Text { text } => (RecordFormat::Transcript, Bytes::from(text)),
        AppendRecordData::Bytes { base64, format } => {
            let data = URL_SAFE_NO_PAD
                .decode(base64)
                .map_err(|_| TsfClientError::InvalidSse("invalid record base64"))?;
            let format = match format.as_str() {
                "bytes" => RecordFormat::Bytes,
                "transcript" => RecordFormat::Transcript,
                _ => return Err(TsfClientError::InvalidSse("invalid record format")),
            };
            (format, Bytes::from(data))
        }
    };
    let part = PartHeader::new(record.part.index, record.part.is_final)?;
    Ok(ReadRecord {
        seq_num: record.seq_num,
        timestamp_ms: record.timestamp_ms,
        writer_id: WriterId::from_bytes(writer),
        writer_seq_num: record.writer_seq_num,
        part,
        format,
        data,
    })
}

async fn json_response<T: DeserializeOwned>(
    response: reqwest::Response,
    operation: &'static str,
) -> Result<T, TsfClientError> {
    let status = response.status();
    if !status.is_success() {
        return Err(http_status_error(response, operation).await);
    }

    Ok(response.json().await?)
}

async fn http_status_error(response: reqwest::Response, operation: &'static str) -> TsfClientError {
    let status = response.status();
    let request_id = response
        .headers()
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let retry_after = response
        .headers()
        .get("retry-after")
        .and_then(|value| value.to_str().ok())
        .and_then(parse_retry_after);
    let raw = response.text().await.unwrap_or_default();
    let parsed = serde_json::from_str::<ApiErrorResponse>(&raw).ok();
    let api_code = parsed
        .as_ref()
        .map(|response| response.error.code.clone())
        .filter(|value| !value.is_empty());
    let body = api_error_message(&raw).unwrap_or(raw);
    TsfClientError::HttpStatus {
        operation,
        status,
        body,
        api_code,
        request_id,
        retry_after,
    }
}

fn parse_retry_after(value: &str) -> Option<Duration> {
    value.trim().parse::<u64>().ok().map(Duration::from_secs)
}

fn api_error_message(body: &str) -> Option<String> {
    let response = serde_json::from_str::<ApiErrorResponse>(body).ok()?;
    let code = response.error.code.trim();
    let message = response.error.message.trim();

    match (code.is_empty(), message.is_empty()) {
        (true, true) => None,
        (true, false) => Some(message.to_owned()),
        (false, true) => Some(code.to_owned()),
        (false, false) => Some(format!("{code}: {message}")),
    }
}

#[derive(Deserialize)]
struct ApiErrorResponse {
    error: ApiErrorBody,
}

#[derive(Deserialize)]
struct ApiErrorBody {
    code: String,
    message: String,
}

async fn next_server_frame(
    ws: &mut ClientWebSocket,
) -> Result<Option<ServerFrame>, TsfClientError> {
    loop {
        let Some(message) = ws.next().await else {
            return Ok(None);
        };

        match message? {
            Message::Binary(bytes) => return Ok(Some(ServerFrame::decode_bytes(bytes)?)),
            Message::Close(Some(close)) if u16::from(close.code) == 1000 => return Ok(None),
            Message::Close(Some(close)) => {
                return Err(TsfClientError::WebSocketClosedWithReason {
                    code: u16::from(close.code),
                    reason: close.reason.to_string(),
                });
            }
            Message::Close(None) => return Ok(None),
            Message::Ping(_) | Message::Pong(_) => {}
            Message::Text(_) => return Err(TsfClientError::UnexpectedTextMessage),
            Message::Frame(_) => {}
        }
    }
}

async fn next_read_socket_frame(
    ws: &mut ClientWebSocket,
) -> Result<Option<ReadSocketOutcome>, TsfClientError> {
    match next_server_frame(ws).await? {
        Some(ServerFrame::ReadBatch(records)) => Ok(Some(ReadSocketOutcome::Records(records))),
        Some(ServerFrame::CaughtUp(caught_up)) => Ok(Some(ReadSocketOutcome::CaughtUp(caught_up))),
        Some(ServerFrame::Heartbeat) => Ok(None),
        Some(frame) => Err(TsfClientError::UnexpectedServerFrame(server_frame_name(
            &frame,
        ))),
        None => Ok(Some(ReadSocketOutcome::Closed)),
    }
}

async fn expect_ready(ws: &mut ClientWebSocket) -> Result<(), TsfClientError> {
    match next_server_frame(ws).await? {
        Some(ServerFrame::Ready) => Ok(()),
        Some(frame) => Err(TsfClientError::UnexpectedServerFrame(server_frame_name(
            &frame,
        ))),
        None => Err(TsfClientError::WebSocketClosed),
    }
}

struct ReadHandshake {
    stream_info: ReadStreamInfo,
    snapshot_boundary: Option<ReadSnapshotBoundary>,
}

async fn expect_read_handshake(
    ws: &mut ClientWebSocket,
    snapshot: bool,
) -> Result<ReadHandshake, TsfClientError> {
    expect_ready(ws).await?;
    let stream_info = match next_server_frame(ws).await? {
        Some(ServerFrame::StreamInfo(stream_info)) => stream_info,
        Some(frame) => {
            return Err(TsfClientError::UnexpectedServerFrame(server_frame_name(
                &frame,
            )));
        }
        None => return Err(TsfClientError::WebSocketClosed),
    };
    let snapshot_boundary = if snapshot {
        match next_server_frame(ws).await? {
            Some(ServerFrame::SnapshotBoundary(boundary)) => Some(boundary),
            Some(frame) => {
                return Err(TsfClientError::UnexpectedServerFrame(server_frame_name(
                    &frame,
                )));
            }
            None => return Err(TsfClientError::WebSocketClosed),
        }
    } else {
        None
    };
    Ok(ReadHandshake {
        stream_info,
        snapshot_boundary,
    })
}

fn server_frame_name(frame: &ServerFrame) -> &'static str {
    match frame {
        ServerFrame::Ready => "ready",
        ServerFrame::Ack { .. } => "ack",
        ServerFrame::ReadBatch(_) => "read batch",
        ServerFrame::Heartbeat => "heartbeat",
        ServerFrame::CaughtUp(_) => "caught up",
        ServerFrame::StreamInfo(_) => "stream info",
        ServerFrame::SnapshotBoundary(_) => "snapshot boundary",
    }
}

/// Error surfaced by REST operations, socket setup, reads, and durable producers.
#[derive(Debug, thiserror::Error)]
pub enum TsfClientError {
    /// HTTP transport or response-decoding failure.
    #[error("HTTP client error: {0}")]
    Http(#[from] reqwest::Error),
    /// Non-success HTTP response.
    #[error("HTTP {operation} failed with {status}: {body}")]
    HttpStatus {
        /// Stable operation label.
        operation: &'static str,
        /// Returned HTTP status.
        status: StatusCode,
        /// Parsed API error or fallback response body.
        body: String,
        /// Stable API error code when the response was JSON.
        api_code: Option<String>,
        /// Server request ID used for support and tracing.
        request_id: Option<String>,
        /// Server-requested retry delay.
        retry_after: Option<Duration>,
    },
    /// Stateless append input violates the local protocol contract.
    #[error("invalid stateless append: {0}")]
    InvalidStatelessAppend(&'static str),
    /// SSE response violated the public event contract.
    #[error("invalid SSE response: {0}")]
    InvalidSse(&'static str),
    /// The server ended an SSE session with a stable terminal error event.
    #[error("SSE terminal error: {0}")]
    SseTerminal(String),
    /// A bounded client operation exceeded its timeout.
    #[error("{operation} timed out")]
    Timeout {
        /// Stable operation label.
        operation: &'static str,
    },
    /// WebSocket transport, TLS, handshake, or protocol failure.
    #[error("WebSocket error: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),
    /// TSF binary frame encoding or decoding failure.
    #[error("frame codec error: {0}")]
    Frame(#[from] FrameCodecError),
    /// The configured API URL cannot map to `ws` or `wss`.
    #[error("cannot derive WebSocket URL from scheme {0:?}")]
    InvalidWebSocketScheme(String),
    /// The server returned a non-text WebSocket protocol header.
    #[error("server selected invalid WebSocket protocol header")]
    InvalidWebSocketProtocolHeader,
    /// The server did not select `tsf.v1` during upgrade.
    #[error("server selected unsupported WebSocket protocol {0:?}")]
    UnexpectedWebSocketProtocol(Option<String>),
    /// The server closed without a non-normal close reason.
    #[error("server closed the WebSocket")]
    WebSocketClosed,
    /// The server sent a non-normal close code and reason.
    #[error("server closed the WebSocket with code {code}: {reason}")]
    WebSocketClosedWithReason {
        /// WebSocket close code.
        code: u16,
        /// Stable server close reason when available.
        reason: String,
    },
    /// The server returned an invalid or mismatched ack range.
    #[error("server sent invalid append acknowledgement {0:?}")]
    InvalidAppendAck(AppendAck),
    /// An ack skipped a pending writer-local sequence number.
    #[error("server acknowledgement advanced past writer seq {writer_seq_num}: {ack:?}")]
    AppendNotAcknowledged {
        /// Pending writer-local sequence number omitted by the ack.
        writer_seq_num: u64,
        /// Invalid ack that advanced past the pending record.
        ack: AppendAck,
    },
    /// Producer bounds are zero or not representable by the semaphore implementation.
    #[error("invalid append producer config: {0}")]
    InvalidProducerConfig(String),
    /// A requested reservation is larger than the entire producer byte window.
    #[error("append record reserves {bytes} bytes, above producer window {max_unacked_bytes}")]
    AppendRecordExceedsProducerWindow {
        /// Requested reservation size.
        bytes: usize,
        /// Configured producer byte window.
        max_unacked_bytes: usize,
    },
    /// A record is larger than its previously acquired reservation.
    #[error("append record uses {bytes} bytes, above reserved capacity {reserved_bytes}")]
    AppendRecordExceedsReservedBytes {
        /// Actual record accounting size.
        bytes: usize,
        /// Capacity owned by the permit.
        reserved_bytes: usize,
    },
    /// The producer command channel is closed.
    #[error("append producer is closed")]
    AppendProducerClosed,
    /// The producer task ended before resolving a pending ticket.
    #[error("append producer dropped with unacknowledged records")]
    AppendProducerDropped,
    /// The producer background task failed or could not be joined.
    #[error("append producer failed: {0}")]
    AppendProducerFailed(String),
    /// Consecutive read connections ended or requested reconnect without delivering a record.
    #[error(
        "read stream made no record progress across {max_connection_attempts} consecutive connection attempts"
    )]
    ReadReconnectLimitExceeded {
        /// Configured maximum consecutive connection attempts, including the initial connection.
        max_connection_attempts: usize,
    },
    /// A selector exceeds the range supported by the active data adapter.
    #[error("read selector {value} exceeds the supported maximum {maximum}")]
    InvalidReadSelector {
        /// Requested selector value.
        value: u64,
        /// Largest supported selector value.
        maximum: u64,
    },
    /// A timestamp playback rate is outside the protocol range.
    #[error("playback rate {value} must be between {minimum} and {maximum} permille")]
    InvalidPlaybackRate {
        /// Requested playback rate.
        value: u64,
        /// Slowest accepted playback rate.
        minimum: u64,
        /// Fastest accepted playback rate.
        maximum: u64,
    },
    /// Timestamp playback needs a stable inclusive ending sequence.
    #[error("playback rate requires an inclusive until sequence")]
    PlaybackRequiresUntil,
    /// A snapshot request also supplied an explicit ending sequence.
    #[error("snapshot and until are mutually exclusive")]
    SnapshotWithUntil,
    /// The service sent a valid TSF frame that is not allowed at this protocol state.
    #[error("server sent unexpected {0} frame")]
    UnexpectedServerFrame(&'static str),
    /// The server sent a text WebSocket message instead of one binary TSF frame.
    #[error("server sent an unexpected text WebSocket message")]
    UnexpectedTextMessage,
}

impl TsfClientError {
    /// Returns the request ID attached to an HTTP failure.
    pub fn request_id(&self) -> Option<&str> {
        match self {
            Self::HttpStatus { request_id, .. } => request_id.as_deref(),
            _ => None,
        }
    }

    /// Returns the stable API code attached to an HTTP failure.
    pub fn api_code(&self) -> Option<&str> {
        match self {
            Self::HttpStatus { api_code, .. } => api_code.as_deref(),
            _ => None,
        }
    }

    /// Returns the server-requested retry delay.
    pub fn retry_after(&self) -> Option<Duration> {
        match self {
            Self::HttpStatus { retry_after, .. } => *retry_after,
            _ => None,
        }
    }
    /// Returns whether retrying a failed create with the same idempotency key and request is safe
    /// and may succeed.
    pub fn is_recoverable_create_failure(&self) -> bool {
        match self {
            Self::Http(error) => {
                error.is_timeout() || error.is_connect() || error.is_body() || error.is_decode()
            }
            Self::HttpStatus { status, .. } => is_retryable_http_status(status.as_u16()),
            _ => false,
        }
    }

    fn is_retryable(&self) -> bool {
        match self {
            Self::Http(error) => error.is_timeout() || error.is_connect(),
            Self::HttpStatus { status, .. } => is_retryable_http_status(status.as_u16()),
            Self::Timeout { .. } => true,
            Self::WebSocket(error) => is_retryable_websocket_error(error),
            Self::WebSocketClosed => true,
            Self::WebSocketClosedWithReason { code, .. } => is_retryable_close_code(*code),
            _ => false,
        }
    }

    fn is_resumable_read_interruption(&self) -> bool {
        match self {
            Self::Timeout { .. } => true,
            Self::WebSocket(error) => is_retryable_websocket_error(error),
            Self::WebSocketClosed => true,
            Self::WebSocketClosedWithReason { code, .. } => is_retryable_close_code(*code),
            _ => false,
        }
    }
}

fn is_retryable_close_code(code: u16) -> bool {
    matches!(code, 1000 | 1001 | 1005 | 1006 | 1011..=1015)
}

fn is_retryable_http_status(status: u16) -> bool {
    matches!(status, 408 | 425 | 429 | 500 | 502 | 503 | 504)
}

fn is_retryable_websocket_error(error: &WebSocketError) -> bool {
    match error {
        WebSocketError::ConnectionClosed
        | WebSocketError::Io(_)
        | WebSocketError::Tls(_)
        | WebSocketError::WriteBufferFull(_) => true,
        WebSocketError::Protocol(ProtocolError::ResetWithoutClosingHandshake) => true,
        WebSocketError::Http(response) => is_retryable_http_status(response.status().as_u16()),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use tokio_tungstenite::connect_async;

    use super::*;

    async fn connected_websockets() -> (ClientWebSocket, WebSocketStream<TcpStream>) {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind WebSocket listener");
        let address = listener.local_addr().expect("WebSocket listener address");
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept WebSocket client");
            tokio_tungstenite::accept_async(stream)
                .await
                .expect("accept WebSocket handshake")
        });
        let (client, _) = connect_async(format!("ws://{address}"))
            .await
            .expect("connect WebSocket client");

        (client, server.await.expect("join WebSocket server"))
    }

    #[test]
    fn create_idempotency_keys_validate_and_redact_debug_output() {
        let key = CreateStreamIdempotencyKey::new_random();
        let exposed = key.expose_secret().to_owned();

        assert!(is_canonical_base64url_32(&exposed));
        assert_eq!(
            exposed
                .parse::<CreateStreamIdempotencyKey>()
                .expect("canonical key")
                .expose_secret(),
            exposed
        );
        assert!(!format!("{key:?}").contains(&exposed));
        assert!(matches!(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".parse::<CreateStreamIdempotencyKey>(),
            Err(InvalidCreateStreamIdempotencyKey)
        ));
    }

    #[tokio::test]
    async fn read_handshake_returns_metadata() {
        let (mut client, mut server) = connected_websockets().await;
        let stream_info = ReadStreamInfo {
            stream_id: "00000000000000000000000000000000"
                .parse()
                .expect("stream ID"),
            title: None,
            visibility: crate::protocol::rest::Visibility::Private,
            created_at: "2026-08-13T00:00:00Z".to_owned(),
            expires_at: "2026-08-23T00:00:00Z".to_owned(),
        };
        let expected_stream_info = stream_info.clone();
        let sender = tokio::spawn(async move {
            for frame in [ServerFrame::Ready, ServerFrame::StreamInfo(stream_info)] {
                server
                    .send(Message::Binary(
                        frame.encode().expect("encode handshake frame"),
                    ))
                    .await
                    .expect("send handshake frame");
            }
        });

        let handshake = expect_read_handshake(&mut client, false)
            .await
            .expect("read handshake");

        assert_eq!(handshake.stream_info, expected_stream_info);
        assert_eq!(handshake.snapshot_boundary, None);
        sender.await.expect("join handshake sender");
    }

    #[tokio::test]
    async fn read_idle_timeout_resets_on_protocol_heartbeat() {
        let (client, mut server) = connected_websockets().await;
        let sender = tokio::spawn(async move {
            for _ in 0..12 {
                sleep(Duration::from_millis(20)).await;
                server
                    .send(Message::Binary(
                        ServerFrame::Heartbeat.encode().expect("encode heartbeat"),
                    ))
                    .await
                    .expect("send heartbeat");
            }
            server
                .send(Message::Binary(
                    ServerFrame::CaughtUp(ReadCaughtUp {
                        next_seq_num: 42,
                        last_timestamp_ms: 1_786_377_600_000,
                    })
                    .encode()
                    .expect("encode caught up"),
                ))
                .await
                .expect("send caught up");
        });
        let mut socket = ReadSocket {
            ws: client,
            read_idle_timeout: Some(Duration::from_millis(100)),
            pending_records: VecDeque::new(),
        };

        let outcome = socket.next_outcome().await.expect("caught-up outcome");

        assert!(matches!(
            outcome,
            ReadSocketOutcome::CaughtUp(ReadCaughtUp {
                next_seq_num: 42,
                last_timestamp_ms: 1_786_377_600_000,
            })
        ));
        sender.await.expect("join heartbeat sender");
    }

    #[tokio::test]
    async fn explicit_read_timeout_does_not_reset_on_protocol_heartbeat() {
        let (client, mut server) = connected_websockets().await;
        let sender = tokio::spawn(async move {
            loop {
                sleep(Duration::from_millis(20)).await;
                server
                    .send(Message::Binary(
                        ServerFrame::Heartbeat.encode().expect("encode heartbeat"),
                    ))
                    .await
                    .expect("send heartbeat");
            }
        });
        let mut socket = ReadSocket {
            ws: client,
            read_idle_timeout: Some(Duration::from_secs(1)),
            pending_records: VecDeque::new(),
        };

        let result = with_timeout(
            Duration::from_millis(100),
            "read stream record",
            socket.next_outcome(),
        )
        .await;

        assert!(matches!(
            result,
            Err(TsfClientError::Timeout {
                operation: "read stream record"
            })
        ));
        sender.abort();
    }

    #[tokio::test]
    async fn read_idle_timeout_still_rejects_a_silent_connection() {
        let (client, server) = connected_websockets().await;
        let server = tokio::spawn(async move {
            let _server = server;
            sleep(Duration::from_secs(1)).await;
        });
        let mut socket = ReadSocket {
            ws: client,
            read_idle_timeout: Some(Duration::from_millis(50)),
            pending_records: VecDeque::new(),
        };

        let result = socket.next_outcome().await;

        assert!(matches!(
            result,
            Err(TsfClientError::Timeout {
                operation: "read stream record"
            })
        ));
        server.abort();
    }

    #[test]
    fn retry_policy_always_attempts_at_least_once() {
        let retry_policy = RetryPolicy {
            max_attempts: 0,
            initial_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
        };

        assert_eq!(retry_policy.attempt_count(), 1);
    }

    #[test]
    fn producer_window_cannot_exceed_server_queue_contract() {
        let default = TsfProducerConfig::default();
        assert_eq!(
            default.max_unacked_bytes,
            MAX_PRODUCER_UNACKED_PAYLOAD_BYTES
        );
        assert_eq!(default.max_unacked_records, MAX_PRODUCER_UNACKED_RECORDS);
        assert!(default.validate().is_ok());

        for config in [
            TsfProducerConfig {
                max_unacked_bytes: MAX_PRODUCER_UNACKED_PAYLOAD_BYTES + 1,
                ..TsfProducerConfig::default()
            },
            TsfProducerConfig {
                max_unacked_records: MAX_PRODUCER_UNACKED_RECORDS + 1,
                ..TsfProducerConfig::default()
            },
        ] {
            assert!(matches!(
                config.validate(),
                Err(TsfClientError::InvalidProducerConfig(_))
            ));
        }
    }

    #[test]
    fn builds_versioned_rest_urls_from_api_origin() {
        let client = TsfClient::with_api_base_url(
            Url::parse("http://localhost:8787/ignored?query=yes#fragment").expect("API origin"),
        );

        assert_eq!(
            client.rest_url("/streams").as_str(),
            "http://localhost:8787/api/v1/streams"
        );
    }

    #[test]
    fn builds_path_only_versioned_websocket_urls() {
        let client =
            TsfClient::with_api_base_url(Url::parse("https://example.com").expect("API origin"));

        assert_eq!(
            client
                .websocket_url("/streams/0123456789abcdefghjkmnpqrstvwxyz/read")
                .expect("WebSocket URL")
                .as_str(),
            "wss://example.com/api/v1/streams/0123456789abcdefghjkmnpqrstvwxyz/read"
        );
    }

    #[tokio::test]
    async fn rejects_read_selectors_outside_the_adapter_range() {
        let client =
            TsfClient::with_api_base_url(Url::parse("http://localhost").expect("API origin"));
        let mut options = ReadStreamOptions::new(
            "0123456789abcdefghjkmnpqrstvwxyz"
                .parse()
                .expect("stream ID"),
        );
        options.start = Some(ReadStart::TailOffset(MAX_READ_SELECTOR_VALUE + 1));

        assert!(matches!(
            client.connect_reader(options).await,
            Err(TsfClientError::InvalidReadSelector {
                value,
                maximum: MAX_READ_SELECTOR_VALUE,
            }) if value == MAX_READ_SELECTOR_VALUE + 1
        ));
    }

    #[test]
    fn append_ack_counts_inclusive_matching_ranges() {
        let ack = AppendAck {
            writer_seq_start: 7,
            writer_seq_end: 9,
            seq_start: 42,
            seq_end: 44,
        };

        assert_eq!(ack.record_count().expect("record count"), 3);
        assert_eq!(ack.validate().expect("valid ack"), ack);
    }

    #[test]
    fn append_ack_rejects_mismatched_range_lengths() {
        let ack = AppendAck {
            writer_seq_start: 7,
            writer_seq_end: 9,
            seq_start: 42,
            seq_end: 43,
        };

        assert!(matches!(
            ack.record_count(),
            Err(TsfClientError::InvalidAppendAck(error_ack)) if error_ack == ack
        ));
    }

    #[test]
    fn dispatch_ack_rejects_more_records_than_are_pending() {
        let permits = Arc::new(Semaphore::new(2));
        let (ack_tx, _ack_rx) = oneshot::channel();
        let record = WriteRecord::new(7, PartHeader::unsplit(), RecordFormat::Bytes, Bytes::new());
        let mut pending = VecDeque::from([PendingAppend {
            record,
            ack_tx,
            _byte_permit: permits.clone().try_acquire_owned().expect("byte permit"),
            _record_permit: permits.try_acquire_owned().expect("record permit"),
        }]);
        let ack = AppendAck {
            writer_seq_start: 7,
            writer_seq_end: 8,
            seq_start: 42,
            seq_end: 43,
        };

        assert!(matches!(
            dispatch_ack(ack, &mut pending),
            Err(TsfClientError::InvalidAppendAck(error_ack)) if error_ack == ack
        ));
        assert_eq!(pending.len(), 1);
    }

    #[test]
    fn dispatch_ack_validates_the_full_range_before_draining() {
        let permits = Arc::new(Semaphore::new(4));
        let mut pending = VecDeque::new();
        for writer_seq_num in [7, 9] {
            let (ack_tx, _ack_rx) = oneshot::channel();
            pending.push_back(PendingAppend {
                record: WriteRecord::new(
                    writer_seq_num,
                    PartHeader::unsplit(),
                    RecordFormat::Bytes,
                    Bytes::new(),
                ),
                ack_tx,
                _byte_permit: permits.clone().try_acquire_owned().expect("byte permit"),
                _record_permit: permits.clone().try_acquire_owned().expect("record permit"),
            });
        }
        let ack = AppendAck {
            writer_seq_start: 7,
            writer_seq_end: 8,
            seq_start: 42,
            seq_end: 43,
        };

        assert!(matches!(
            dispatch_ack(ack, &mut pending),
            Err(TsfClientError::InvalidAppendAck(error_ack)) if error_ack == ack
        ));
        assert_eq!(
            pending
                .iter()
                .map(|item| item.record.writer_seq_num)
                .collect::<Vec<_>>(),
            [7, 9]
        );
    }

    #[test]
    fn api_error_message_extracts_stable_code_and_message() {
        let body = r#"{"error":{"code":"forbidden","message":"owner link required"}}"#;

        assert_eq!(
            api_error_message(body).as_deref(),
            Some("forbidden: owner link required")
        );
    }

    #[test]
    fn api_error_message_leaves_non_standard_body_for_fallback() {
        assert_eq!(api_error_message("plain failure"), None);
    }

    #[test]
    fn websocket_retry_policy_distinguishes_transient_and_permanent_failures() {
        for code in [1000, 1001, 1005, 1006, 1011, 1012, 1013, 1014, 1015] {
            let error = TsfClientError::WebSocketClosedWithReason {
                code,
                reason: "transient".to_owned(),
            };
            assert!(error.is_retryable(), "close {code}");
            assert!(error.is_resumable_read_interruption(), "close {code}");
        }

        for code in [1002, 1003, 1007, 1008, 1009, 1010, 4000] {
            let error = TsfClientError::WebSocketClosedWithReason {
                code,
                reason: "permanent".to_owned(),
            };
            assert!(!error.is_retryable(), "close {code}");
            assert!(!error.is_resumable_read_interruption(), "close {code}");
        }

        assert!(
            TsfClientError::WebSocket(WebSocketError::Protocol(
                ProtocolError::ResetWithoutClosingHandshake,
            ))
            .is_retryable()
        );
        assert!(
            !TsfClientError::WebSocket(WebSocketError::Protocol(ProtocolError::InvalidOpcode(15),))
                .is_retryable()
        );
    }
}
