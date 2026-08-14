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
            AppendJsonRecord, AppendRecordsRequest, AppendRecordsResponse, CreateLinkRequest,
            CreateStreamRequest, CreateStreamResponse, ListLinksResponse, RecordData,
            RestRecordPart, SseCaughtUpEvent, SseReadRecord, SseRecordsEvent,
            SseSnapshotBoundaryEvent, StreamInfoResponse, StreamLinkCredential,
            UpdateStreamRequest,
        },
        ws::{
            DEFAULT_READ_TAIL_OFFSET, MAX_PLAYBACK_RATE_PERMILLE, MAX_READ_SELECTOR_VALUE,
            MIN_PLAYBACK_RATE_PERMILLE, ReadStart, ReadStreamOptions, WriteStreamOptions,
            frame::{
                AppendRecord, CaughtUpPosition, ClientFrame, FrameCodecError,
                MAX_APPEND_BATCH_RECORDS, MAX_BATCH_PAYLOAD_BYTES, MAX_RECORD_BYTES, PartHeader,
                ReadRecord, ReadStreamInfo, RecordFormat, ServerFrame, SnapshotBoundary,
                TSF_WEBSOCKET_PROTOCOL,
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
    pub api_origin: Url,
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
    pub fn new(api_origin: Url) -> Result<Self, TsfClientError> {
        validate_api_origin(&api_origin)?;
        Ok(Self {
            api_origin,
            rest_request_timeout: Duration::from_secs(10),
            websocket_connect_timeout: Duration::from_secs(10),
            websocket_operation_timeout: Duration::from_secs(30),
            websocket_read_idle_timeout: Some(Duration::from_secs(60)),
            retry_policy: RetryPolicy::default(),
        })
    }
}

impl Default for TsfClientConfig {
    fn default() -> Self {
        Self {
            api_origin: default_api_origin(),
            rest_request_timeout: Duration::from_secs(10),
            websocket_connect_timeout: Duration::from_secs(10),
            websocket_operation_timeout: Duration::from_secs(30),
            websocket_read_idle_timeout: Some(Duration::from_secs(60)),
            retry_policy: RetryPolicy::default(),
        }
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
/// REST operations preserve their retry identity and use [`RetryPolicy`]. Stateless append retries
/// can create physical duplicates, which logical transcript readers suppress. Durable WebSocket
/// writer recovery is owned by [`TsfWriter`].
#[derive(Clone)]
pub struct TsfClient {
    config: TsfClientConfig,
    http: reqwest::Client,
}

impl TsfClient {
    /// Creates a client for the default [tail.surf](https://tail.surf) API origin.
    pub fn new() -> Self {
        Self {
            config: TsfClientConfig::default(),
            http: reqwest::Client::new(),
        }
    }

    /// Creates a client for an explicit API origin with default timeouts.
    pub fn with_api_origin(api_origin: Url) -> Result<Self, TsfClientError> {
        Self::with_config(TsfClientConfig::new(api_origin)?)
    }

    /// Creates a client from a complete configuration.
    pub fn with_config(config: TsfClientConfig) -> Result<Self, TsfClientError> {
        validate_api_origin(&config.api_origin)?;
        Ok(Self {
            config,
            http: reqwest::Client::new(),
        })
    }

    /// Returns the configured API origin without the `/api/v1` namespace.
    pub fn api_origin(&self) -> &Url {
        &self.config.api_origin
    }

    /// Returns the complete immutable client configuration.
    pub fn config(&self) -> &TsfClientConfig {
        &self.config
    }

    /// Creates a stream and returns its metadata and newly created link credentials.
    ///
    /// Construct the request once so its prepared link secrets remain stable. The client generates
    /// one idempotency key for this logical call and reuses the complete request while retrying
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
    /// Recovery requires the same prepared request. The idempotency key alone cannot recover link
    /// credentials.
    pub async fn create_stream_with_idempotency_key(
        &self,
        request: &CreateStreamRequest,
        idempotency_key: &CreateStreamIdempotencyKey,
    ) -> Result<CreateStreamResponse, TsfClientError> {
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
    /// Transient failures are retried with the same absolute update values.
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
    /// Transient failures are retried. Deletion is idempotent.
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

    /// Creates a stream link idempotently.
    ///
    /// Transient failures are retried with the same client-generated Link ID and secret.
    pub async fn create_link(
        &self,
        stream_id: &StreamId,
        request: &CreateLinkRequest,
        owner_link_secret: &LinkSecret,
    ) -> Result<StreamLinkCredential, TsfClientError> {
        let link_id = request.link_id.clone();
        self.retry_transient(|| {
            self.send_json_with_bearer(
                self.http
                    .put(self.rest_url(&format!("/streams/{stream_id}/links/{link_id}")))
                    .json(request),
                "create link",
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

    /// Revokes a stream link by its non-secret identifier.
    ///
    /// Transient failures are retried. Revocation is idempotent.
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
        expected_end_seq_num: Option<u64>,
        write_link_secret: &LinkSecret,
    ) -> Result<AppendRecordsResponse, TsfClientError> {
        if records.is_empty() || records.len() > 128 {
            return Err(TsfClientError::InvalidStatelessAppend(
                "record count must be between 1 and 128",
            ));
        }
        let writer_start_seq_num = records[0].writer_seq_num;
        let mut json_records = Vec::with_capacity(records.len());
        for (index, record) in records.iter().enumerate() {
            record.validate()?;
            if record.writer_seq_num
                != writer_start_seq_num.checked_add(index as u64).ok_or(
                    TsfClientError::InvalidStatelessAppend("writer sequence overflow"),
                )?
            {
                return Err(TsfClientError::InvalidStatelessAppend(
                    "writer sequence numbers must be contiguous",
                ));
            }
            let data = if record.format == RecordFormat::Transcript {
                match std::str::from_utf8(&record.data) {
                    Ok(text) => RecordData::Utf8(text.to_owned()),
                    Err(_) => RecordData::Base64url(URL_SAFE_NO_PAD.encode(&record.data)),
                }
            } else {
                RecordData::Base64url(URL_SAFE_NO_PAD.encode(&record.data))
            };
            json_records.push(AppendJsonRecord {
                data,
                format: record.format,
                part: Some(RestRecordPart {
                    index: record.part.index(),
                    is_final: record.part.is_final(),
                }),
            });
        }
        let request = AppendRecordsRequest {
            writer_id: URL_SAFE_NO_PAD.encode(writer_id.as_bytes()),
            writer_start_seq_num,
            records: json_records,
            expected_end_seq_num,
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

    /// Connects the standard bounded, reconnecting durable writer.
    pub async fn connect_writer(
        &self,
        options: WriteStreamOptions,
    ) -> Result<TsfWriter, TsfClientError> {
        self.connect_writer_with_config(options, TsfWriterConfig::default())
            .await
    }

    /// Connects a durable writer with explicit in-flight and reconnect bounds.
    pub async fn connect_writer_with_config(
        &self,
        options: WriteStreamOptions,
        config: TsfWriterConfig,
    ) -> Result<TsfWriter, TsfClientError> {
        let session = self.connect_append_session(options.clone()).await?;
        TsfWriter::new(self.clone(), options, session, config)
    }

    /// Connects a low-level append session that sends records and receives ack ranges directly.
    ///
    /// Unlike [`TsfWriter`], this session does not retain or resend unacknowledged records.
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
    /// Private credentials stay in the bearer header. Reconnects reuse the original URL and send
    /// the latest versioned event cursor in `Last-Event-ID`.
    pub async fn connect_sse_reader(
        &self,
        mut options: ReadStreamOptions,
    ) -> Result<TsfSseReadSession, TsfClientError> {
        validate_read_options(&options)?;
        let request_options = options.clone();
        let connection = self
            .open_sse_connection(&request_options, None)
            .await?
            .ok_or(TsfClientError::InvalidSse(
                "initial read completed without stream_info",
            ))?;
        if let Some(boundary) = connection.snapshot_boundary {
            apply_snapshot_boundary(&mut options, Some(boundary));
        }
        Ok(TsfSseReadSession {
            client: self.clone(),
            options,
            request_options,
            body: connection.body,
            buffer: connection.buffer,
            queued_events: connection.queued_events,
            queued_records: VecDeque::new(),
            stream_info: connection.stream_info.expect("validated stream_info event"),
            last_caught_up: None,
            snapshot_boundary: connection.snapshot_boundary,
            reconnect_attempts: 0,
            last_event_id: connection.resume_event_id,
            finished: false,
        })
    }

    async fn open_sse_connection(
        &self,
        options: &ReadStreamOptions,
        last_event_id: Option<&str>,
    ) -> Result<Option<SseConnection>, TsfClientError> {
        self.retry_transient(|| async {
            let mut url = self.rest_url(&format!("/streams/{}/records", options.stream_id));
            append_sse_query(&mut url, options);
            let mut request = self.http.get(url).header("Accept", "text/event-stream");
            if let Some(secret) = options.link_secret.as_ref() {
                request = request.bearer_auth(secret.expose_secret());
            }
            if let Some(last_event_id) = last_event_id {
                request = request.header("Last-Event-ID", last_event_id);
            }
            let response = request.send().await?;
            if response.status() == StatusCode::NO_CONTENT {
                return Ok(None);
            }
            if !response.status().is_success() {
                return Err(http_status_error(response, "read SSE").await);
            }
            let mut connection = SseConnection {
                body: Box::pin(response.bytes_stream()),
                buffer: Vec::new(),
                queued_events: VecDeque::new(),
                stream_info: None,
                snapshot_boundary: None,
                resume_event_id: None,
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
            if event.id.is_some() {
                connection.resume_event_id = Some(sse_resume_event_id(&event)?.to_owned());
            }
            if options.snapshot {
                let event = next_sse_event(
                    &mut connection.body,
                    &mut connection.buffer,
                    &mut connection.queued_events,
                )
                .await?
                .ok_or(TsfClientError::InvalidSse(
                    "response ended before snapshot_boundary",
                ))?;
                if event.event != "snapshot_boundary" {
                    return Err(TsfClientError::InvalidSse(
                        "snapshot_boundary must follow stream_info",
                    ));
                }
                connection.resume_event_id = Some(sse_resume_event_id(&event)?.to_owned());
                let boundary: SseSnapshotBoundaryEvent = serde_json::from_str(&event.data)
                    .map_err(|_| TsfClientError::InvalidSse("invalid snapshot_boundary event"))?;
                connection.snapshot_boundary = Some(SnapshotBoundary {
                    end_seq_num: boundary.end_seq_num,
                    last_timestamp_ms: boundary.last_timestamp_ms,
                });
            }
            Ok(Some(connection))
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
            if options.end_seq_num.is_none() && !options.snapshot {
                return Err(TsfClientError::PlaybackRequiresEnd);
            }
        }
        if options.snapshot && options.end_seq_num.is_some() {
            return Err(TsfClientError::SnapshotWithEnd);
        }
        let opening_frame = ClientFrame::OpenRead {
            link_secret: options.link_secret.clone(),
            start: options
                .start
                .unwrap_or(ReadStart::TailOffset(DEFAULT_READ_TAIL_OFFSET)),
            count: options.count,
            end_seq_num: options.end_seq_num,
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
        let mut url = self.config.api_origin.clone();
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
pub fn default_api_origin() -> Url {
    Url::parse("https://tail.surf").expect("default tsf API base URL is valid")
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

/// Maximum payload bytes a writer may retain before acknowledgement.
///
/// This matches the TSF writer socket's hard queued-payload bound.
pub const MAX_WRITER_UNACKED_PAYLOAD_BYTES: usize = 5 * 1024 * 1024;
/// Maximum records a writer may retain before acknowledgement.
///
/// This matches the TSF writer socket's hard queued-message bound.
pub const MAX_WRITER_UNACKED_RECORDS: usize = 128;

/// Memory, concurrency, and reconnect bounds for [`TsfWriter`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TsfWriterConfig {
    /// Maximum total payload bytes retained until durability acknowledgement. Must not exceed
    /// [`MAX_WRITER_UNACKED_PAYLOAD_BYTES`].
    pub max_unacked_bytes: usize,
    /// Maximum number of records retained until durability acknowledgement. Must not exceed
    /// [`MAX_WRITER_UNACKED_RECORDS`].
    pub max_unacked_records: usize,
    /// Maximum consecutive writer reconnect attempts before failing pending records.
    pub max_reconnect_attempts: usize,
}

impl TsfWriterConfig {
    fn validate(self) -> Result<Self, TsfClientError> {
        if self.max_unacked_bytes == 0 {
            return Err(TsfClientError::InvalidWriterConfig(
                "max_unacked_bytes must be greater than zero".to_owned(),
            ));
        }
        if self.max_unacked_bytes > MAX_WRITER_UNACKED_PAYLOAD_BYTES {
            return Err(TsfClientError::InvalidWriterConfig(format!(
                "max_unacked_bytes must not exceed {}",
                MAX_WRITER_UNACKED_PAYLOAD_BYTES
            )));
        }
        if self.max_unacked_records == 0 {
            return Err(TsfClientError::InvalidWriterConfig(
                "max_unacked_records must be greater than zero".to_owned(),
            ));
        }
        if self.max_unacked_records > MAX_WRITER_UNACKED_RECORDS {
            return Err(TsfClientError::InvalidWriterConfig(format!(
                "max_unacked_records must not exceed {}",
                MAX_WRITER_UNACKED_RECORDS
            )));
        }
        Ok(self)
    }
}

impl Default for TsfWriterConfig {
    fn default() -> Self {
        Self {
            max_unacked_bytes: MAX_WRITER_UNACKED_PAYLOAD_BYTES,
            max_unacked_records: MAX_WRITER_UNACKED_RECORDS,
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
    pub writer_start_seq_num: u64,
    /// Exclusive writer-local sequence after the acknowledged range.
    pub writer_end_seq_num: u64,
    /// Durable sequence number assigned to the first acknowledged record.
    pub start_seq_num: u64,
    /// Exclusive durable sequence after the acknowledged range.
    pub end_seq_num: u64,
}

impl AppendAck {
    /// Returns whether the half-open writer range contains a sequence number.
    pub const fn contains_writer_seq(self, writer_seq_num: u64) -> bool {
        self.writer_start_seq_num <= writer_seq_num && writer_seq_num < self.writer_end_seq_num
    }

    /// Returns the number of records when writer and durable ranges are valid and equal in length.
    pub fn record_count(self) -> Result<u64, TsfClientError> {
        let writer_count = self
            .writer_end_seq_num
            .checked_sub(self.writer_start_seq_num)
            .ok_or(TsfClientError::InvalidAppendAck(self))?;
        let durable_count = self
            .end_seq_num
            .checked_sub(self.start_seq_num)
            .ok_or(TsfClientError::InvalidAppendAck(self))?;
        if writer_count == 0 {
            return Err(TsfClientError::InvalidAppendAck(self));
        }
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
    /// Append acknowledgement range that covered this record.
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
                Some(Err(TsfClientError::AppendWriterDropped))
            }
        }
    }
}

impl Future for AppendTicket {
    type Output = Result<AppendReceipt, TsfClientError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.rx).poll(cx) {
            Poll::Ready(Ok(result)) => Poll::Ready(result),
            Poll::Ready(Err(_)) => Poll::Ready(Err(TsfClientError::AppendWriterDropped)),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Bounded durable writer that retains unacknowledged records and resends them across transient
/// interruptions.
pub struct TsfWriter {
    cmd_tx: mpsc::Sender<WriterCommand>,
    byte_permits: Arc<Semaphore>,
    record_permits: Arc<Semaphore>,
    max_unacked_bytes: usize,
    task: Option<JoinHandle<()>>,
}

impl TsfWriter {
    fn new(
        client: TsfClient,
        options: WriteStreamOptions,
        session: TsfAppendSession,
        config: TsfWriterConfig,
    ) -> Result<Self, TsfClientError> {
        let config = config.validate()?;
        let command_capacity = config.max_unacked_records + 1;
        let (cmd_tx, cmd_rx) = mpsc::channel(command_capacity);
        let task = tokio::spawn(run_writer(client, options, session, cmd_rx, config));

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
            return Err(TsfClientError::AppendRecordExceedsWriterWindow {
                bytes,
                max_unacked_bytes: self.max_unacked_bytes,
            });
        }

        let record_permit = self
            .record_permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| TsfClientError::AppendWriterClosed)?;
        let byte_permit = self
            .byte_permits
            .clone()
            .acquire_many_owned(bytes as u32)
            .await
            .map_err(|_| TsfClientError::AppendWriterClosed)?;
        let cmd_tx_permit = self
            .cmd_tx
            .clone()
            .reserve_owned()
            .await
            .map_err(|_| TsfClientError::AppendWriterClosed)?;

        Ok(WritePermit {
            cmd_tx_permit,
            byte_permit,
            record_permit,
            reserved_bytes: bytes,
        })
    }

    /// Stops accepting records, waits for every pending durability acknowledgement, and joins the
    /// writer task.
    pub async fn close(mut self) -> Result<(), TsfClientError> {
        let (done_tx, done_rx) = oneshot::channel();
        self.cmd_tx
            .send(WriterCommand::Close { done_tx })
            .await
            .map_err(|_| TsfClientError::AppendWriterClosed)?;

        let result = done_rx
            .await
            .map_err(|_| TsfClientError::AppendWriterDropped)?;

        if let Some(task) = self.task.take() {
            task.await
                .map_err(|error| TsfClientError::AppendWriterFailed(error.to_string()))?;
        }

        result
    }
}

impl Drop for TsfWriter {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

/// Owned capacity in a writer's record and byte windows.
///
/// Dropping an unused permit releases its capacity.
pub struct WritePermit {
    cmd_tx_permit: mpsc::OwnedPermit<WriterCommand>,
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
        self.cmd_tx_permit.send(WriterCommand::Submit {
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
            Some(ServerFrame::AppendAck {
                writer_start_seq_num,
                writer_end_seq_num,
                start_seq_num,
                end_seq_num,
            }) => AppendAck {
                writer_start_seq_num,
                writer_end_seq_num,
                start_seq_num,
                end_seq_num,
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

enum WriterCommand {
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

async fn run_writer(
    client: TsfClient,
    options: WriteStreamOptions,
    mut session: TsfAppendSession,
    mut cmd_rx: mpsc::Receiver<WriterCommand>,
    config: TsfWriterConfig,
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
                            finish_writer_error(&mut pending, &mut close_tx, error);
                            return;
                        }
                    }
                    None => {
                        fail_pending(&mut pending, "append writer dropped");
                        return;
                    }
                }
            }

            ack = session.next_ack(), if !pending.is_empty() => {
                match ack {
                    Ok(Some(ack)) => {
                        if let Err(error) = dispatch_ack(ack, &mut pending) {
                            finish_writer_error(&mut pending, &mut close_tx, error);
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
                            finish_writer_error(&mut pending, &mut close_tx, error);
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
                            finish_writer_error(&mut pending, &mut close_tx, error);
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
    cmd_rx: &mut mpsc::Receiver<WriterCommand>,
    close_tx: &mut Option<oneshot::Sender<Result<(), TsfClientError>>>,
    first: WriterCommand,
) {
    let mut command = Some(first);

    while let Some(WriterCommand::Submit {
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

    if let Some(WriterCommand::Close { done_tx }) = command {
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
        .zip(ack.writer_start_seq_num..ack.writer_end_seq_num)
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
        .zip(ack.writer_start_seq_num..ack.writer_end_seq_num)
        .zip(ack.start_seq_num..ack.end_seq_num)
    {
        let _ = item.ack_tx.send(Ok(AppendReceipt {
            writer_seq_num,
            seq_num,
            ack,
        }));
    }

    Ok(())
}

fn finish_writer_error(
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
            .send(Err(TsfClientError::AppendWriterFailed(message.clone())));
    }
}

/// Resumable reader that advances its sequence position after every delivered record.
///
/// Transient transport and service interruptions reconnect from the next sequence number. Normal
/// completion and configured bounds return `None`; protocol and policy failures surface as errors.
type SseBody = Pin<Box<dyn futures_util::Stream<Item = Result<Bytes, reqwest::Error>> + Send>>;

struct ParsedSseEvent {
    event: String,
    data: String,
    id: Option<String>,
}

struct SseConnection {
    body: SseBody,
    buffer: Vec<u8>,
    queued_events: VecDeque<ParsedSseEvent>,
    stream_info: Option<ReadStreamInfo>,
    snapshot_boundary: Option<SnapshotBoundary>,
    resume_event_id: Option<String>,
}

/// Resumable HTTP event-stream reader.
pub struct TsfSseReadSession {
    client: TsfClient,
    options: ReadStreamOptions,
    request_options: ReadStreamOptions,
    body: SseBody,
    buffer: Vec<u8>,
    queued_events: VecDeque<ParsedSseEvent>,
    queued_records: VecDeque<ReadRecord>,
    stream_info: ReadStreamInfo,
    last_caught_up: Option<CaughtUpPosition>,
    snapshot_boundary: Option<SnapshotBoundary>,
    reconnect_attempts: usize,
    last_event_id: Option<String>,
    finished: bool,
}

impl TsfSseReadSession {
    /// Returns authorized stream metadata from the opening event.
    pub fn stream_info(&self) -> &ReadStreamInfo {
        &self.stream_info
    }

    /// Returns the fixed boundary captured for a snapshot read.
    pub fn snapshot_boundary(&self) -> Option<SnapshotBoundary> {
        self.snapshot_boundary
    }

    /// Returns the most recent reconnect-safe caught-up position.
    pub fn last_caught_up(&self) -> Option<CaughtUpPosition> {
        self.last_caught_up
    }

    /// Returns the next record, reconnecting from the last safe absolute cursor when needed.
    pub async fn next_record(&mut self) -> Result<Option<ReadRecord>, TsfClientError> {
        loop {
            if self.finished {
                return Ok(None);
            }
            if let Some(record) = self.queued_records.pop_front() {
                self.options.start = record.seq_num.checked_add(1).map(ReadStart::SeqNum);
                if let Some(count) = self.options.count.as_mut() {
                    *count = count.saturating_sub(1);
                }
                return Ok(Some(record));
            }
            if self.options.count == Some(0)
                || matches!((self.options.start, self.options.end_seq_num), (Some(ReadStart::SeqNum(next)), Some(end_seq_num)) if next >= end_seq_num)
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
                let Some(connection) = self
                    .client
                    .open_sse_connection(&self.request_options, self.last_event_id.as_deref())
                    .await?
                else {
                    self.finished = true;
                    return Ok(None);
                };
                if let Some(boundary) = connection.snapshot_boundary {
                    if self
                        .snapshot_boundary
                        .is_some_and(|previous| previous.end_seq_num != boundary.end_seq_num)
                    {
                        return Err(TsfClientError::InvalidSse(
                            "snapshot boundary changed during resume",
                        ));
                    }
                    self.snapshot_boundary = Some(boundary);
                }
                if connection.resume_event_id.is_some() {
                    self.last_event_id = connection.resume_event_id;
                }
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
                    let records = batch
                        .records
                        .into_iter()
                        .map(sse_read_record)
                        .collect::<Result<Vec<_>, _>>()?;
                    self.last_event_id = Some(sse_resume_event_id(&event)?.to_owned());
                    self.queued_records.extend(records);
                    self.reconnect_attempts = 0;
                }
                "caught_up" => {
                    let value: SseCaughtUpEvent = serde_json::from_str(&event.data)
                        .map_err(|_| TsfClientError::InvalidSse("invalid caught_up event"))?;
                    let caught_up = CaughtUpPosition {
                        next_seq_num: value.next_seq_num,
                        last_timestamp_ms: value.last_timestamp_ms,
                    };
                    self.last_event_id = Some(sse_resume_event_id(&event)?.to_owned());
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
    last_caught_up: Option<CaughtUpPosition>,
    snapshot_boundary: Option<SnapshotBoundary>,
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
        last_caught_up: Option<CaughtUpPosition>,
        snapshot_boundary: Option<SnapshotBoundary>,
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
    pub const fn last_caught_up(&self) -> Option<CaughtUpPosition> {
        self.last_caught_up
    }

    /// Returns the fixed exclusive end captured for a snapshot read.
    pub const fn snapshot_boundary(&self) -> Option<SnapshotBoundary> {
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

        if matches!(
            (self.options.start, self.options.end_seq_num),
            (Some(ReadStart::SeqNum(next)), Some(end_seq_num)) if next >= end_seq_num
        ) {
            self.finished = true;
        }
    }
}

fn read_options_exhausted(options: &ReadStreamOptions) -> bool {
    options.count == Some(0)
        || matches!(
            (options.start, options.end_seq_num),
            (Some(ReadStart::SeqNum(start)), Some(end_seq_num)) if start >= end_seq_num
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
    snapshot_boundary: Option<SnapshotBoundary>,
}

fn apply_snapshot_boundary(options: &mut ReadStreamOptions, boundary: Option<SnapshotBoundary>) {
    let Some(boundary) = boundary else {
        return;
    };
    options.snapshot = false;
    options.end_seq_num = Some(boundary.end_seq_num);
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
    CaughtUp(CaughtUpPosition),
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
        HeaderValue::from_static(TSF_WEBSOCKET_PROTOCOL),
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

    if selected_protocol.as_deref() != Some(TSF_WEBSOCKET_PROTOCOL) {
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
        if options.end_seq_num.is_none() && !options.snapshot {
            return Err(TsfClientError::PlaybackRequiresEnd);
        }
    }
    if options.snapshot && options.end_seq_num.is_some() {
        return Err(TsfClientError::SnapshotWithEnd);
    }
    Ok(())
}

fn append_sse_query(url: &mut Url, options: &ReadStreamOptions) {
    let mut query = url.query_pairs_mut();
    match options.start {
        Some(ReadStart::SeqNum(value)) => {
            query.append_pair("seq_num", &value.to_string());
        }
        Some(ReadStart::TimestampMs(value)) => {
            query.append_pair("timestamp_ms", &value.to_string());
        }
        Some(ReadStart::TailOffset(value)) => {
            query.append_pair("tail_offset", &value.to_string());
        }
        None => {}
    }
    if let Some(value) = options.count {
        query.append_pair("count", &value.to_string());
    }
    if let Some(value) = options.end_seq_num {
        query.append_pair("end_seq_num", &value.to_string());
    }
    if let Some(value) = options.playback_rate_permille {
        query.append_pair("playback_rate_permille", &value.to_string());
    }
    if options.snapshot {
        query.append_pair("snapshot", "true");
    }
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
    let mut id = None;
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
            "id" => id = Some(value.to_owned()),
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
            id,
        }))
    }
}

fn sse_resume_event_id(event: &ParsedSseEvent) -> Result<&str, TsfClientError> {
    let Some(id) = event.id.as_deref() else {
        return Err(invalid_sse_resume_cursor());
    };
    let mut fields = id.split(',');
    if fields.next() != Some("v1") {
        return Err(invalid_sse_resume_cursor());
    }
    let Some(next_seq_num) = fields.next().and_then(parse_sse_cursor_u64) else {
        return Err(invalid_sse_resume_cursor());
    };
    let Some(consumed_count) = fields.next().and_then(parse_sse_cursor_u64) else {
        return Err(invalid_sse_resume_cursor());
    };
    let snapshot = match (fields.next(), fields.next()) {
        (None, None) => None,
        (Some(next), Some(timestamp)) => Some((
            parse_sse_cursor_u64(next).ok_or_else(invalid_sse_resume_cursor)?,
            parse_sse_cursor_u64(timestamp).ok_or_else(invalid_sse_resume_cursor)?,
        )),
        _ => return Err(invalid_sse_resume_cursor()),
    };
    if fields.next().is_some()
        || next_seq_num > MAX_READ_SELECTOR_VALUE
        || consumed_count > next_seq_num
        || snapshot.is_some_and(|(snapshot_end_seq_num, snapshot_last_timestamp_ms)| {
            snapshot_end_seq_num > MAX_READ_SELECTOR_VALUE
                || next_seq_num > snapshot_end_seq_num
                || snapshot_last_timestamp_ms > MAX_READ_SELECTOR_VALUE
                || (snapshot_end_seq_num == 0 && snapshot_last_timestamp_ms != 0)
        })
    {
        return Err(invalid_sse_resume_cursor());
    }
    Ok(id)
}

fn parse_sse_cursor_u64(value: &str) -> Option<u64> {
    if value.is_empty()
        || (value != "0" && value.starts_with('0'))
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    value.parse().ok()
}

fn invalid_sse_resume_cursor() -> TsfClientError {
    TsfClientError::InvalidSse("SSE event does not carry a valid resume cursor")
}

fn sse_read_record(record: SseReadRecord) -> Result<ReadRecord, TsfClientError> {
    let writer = URL_SAFE_NO_PAD
        .decode(record.writer_id)
        .map_err(|_| TsfClientError::InvalidSse("invalid writer_id"))?;
    let writer: [u8; WriterId::BYTE_LEN] = writer
        .try_into()
        .map_err(|_| TsfClientError::InvalidSse("invalid writer_id length"))?;
    let data = match record.data {
        RecordData::Utf8(value) => Bytes::from(value),
        RecordData::Base64url(value) => {
            let data = URL_SAFE_NO_PAD
                .decode(value)
                .map_err(|_| TsfClientError::InvalidSse("invalid record base64url"))?;
            Bytes::from(data)
        }
    };
    let part = PartHeader::new(record.part.index, record.part.is_final)?;
    Ok(ReadRecord {
        seq_num: record.seq_num,
        timestamp_ms: record.timestamp_ms,
        writer_id: WriterId::from_bytes(writer),
        writer_seq_num: record.writer_seq_num,
        part,
        format: record.format,
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
    snapshot_boundary: Option<SnapshotBoundary>,
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
        ServerFrame::AppendAck { .. } => "append_ack",
        ServerFrame::ReadBatch(_) => "read batch",
        ServerFrame::Heartbeat => "heartbeat",
        ServerFrame::CaughtUp(_) => "caught up",
        ServerFrame::StreamInfo(_) => "stream info",
        ServerFrame::SnapshotBoundary(_) => "snapshot boundary",
    }
}

fn validate_api_origin(origin: &Url) -> Result<(), TsfClientError> {
    if !matches!(origin.scheme(), "http" | "https")
        || origin.host_str().is_none()
        || !origin.username().is_empty()
        || origin.password().is_some()
        || origin.path() != "/"
        || origin.query().is_some()
        || origin.fragment().is_some()
    {
        return Err(TsfClientError::InvalidApiOrigin(origin.clone()));
    }
    Ok(())
}

/// Error surfaced by REST operations, socket setup, reads, and durable writers.
#[derive(Debug, thiserror::Error)]
pub enum TsfClientError {
    /// The configured API origin is not a bare HTTP or HTTPS origin.
    #[error("API origin must be HTTP(S) without credentials, path, query, or fragment: {0}")]
    InvalidApiOrigin(Url),
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
    /// Writer bounds are zero or not representable by the semaphore implementation.
    #[error("invalid append writer config: {0}")]
    InvalidWriterConfig(String),
    /// A requested reservation is larger than the entire writer byte window.
    #[error("append record reserves {bytes} bytes, above writer window {max_unacked_bytes}")]
    AppendRecordExceedsWriterWindow {
        /// Requested reservation size.
        bytes: usize,
        /// Configured writer byte window.
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
    /// The writer command channel is closed.
    #[error("append writer is closed")]
    AppendWriterClosed,
    /// The writer task ended before resolving a pending ticket.
    #[error("append writer dropped with unacknowledged records")]
    AppendWriterDropped,
    /// The writer background task failed or could not be joined.
    #[error("append writer failed: {0}")]
    AppendWriterFailed(String),
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
    /// Timestamp playback needs a stable exclusive ending sequence.
    #[error("playback rate requires an exclusive end_seq_num sequence")]
    PlaybackRequiresEnd,
    /// A snapshot request also supplied an explicit ending sequence.
    #[error("snapshot and end_seq_num are mutually exclusive")]
    SnapshotWithEnd,
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
                    ServerFrame::CaughtUp(CaughtUpPosition {
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
            ReadSocketOutcome::CaughtUp(CaughtUpPosition {
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
    fn writer_window_cannot_exceed_server_queue_contract() {
        let default = TsfWriterConfig::default();
        assert_eq!(default.max_unacked_bytes, MAX_WRITER_UNACKED_PAYLOAD_BYTES);
        assert_eq!(default.max_unacked_records, MAX_WRITER_UNACKED_RECORDS);
        assert!(default.validate().is_ok());

        for config in [
            TsfWriterConfig {
                max_unacked_bytes: MAX_WRITER_UNACKED_PAYLOAD_BYTES + 1,
                ..TsfWriterConfig::default()
            },
            TsfWriterConfig {
                max_unacked_records: MAX_WRITER_UNACKED_RECORDS + 1,
                ..TsfWriterConfig::default()
            },
        ] {
            assert!(matches!(
                config.validate(),
                Err(TsfClientError::InvalidWriterConfig(_))
            ));
        }
    }

    #[test]
    fn builds_versioned_rest_urls_from_api_origin() {
        let client =
            TsfClient::with_api_origin(Url::parse("http://localhost:8787").expect("API origin"))
                .expect("valid API origin");

        assert_eq!(
            client.rest_url("/streams").as_str(),
            "http://localhost:8787/api/v1/streams"
        );
    }

    #[test]
    fn rejects_non_origin_api_urls() {
        for value in [
            "https://user@example.com",
            "https://example.com/api",
            "https://example.com?region=west",
            "https://example.com#api",
            "wss://example.com",
        ] {
            assert!(matches!(
                TsfClient::with_api_origin(Url::parse(value).expect("URL")),
                Err(TsfClientError::InvalidApiOrigin(_))
            ));
        }
    }

    #[test]
    fn builds_path_only_versioned_websocket_urls() {
        let client =
            TsfClient::with_api_origin(Url::parse("https://example.com").expect("API origin"))
                .expect("valid API origin");

        assert_eq!(
            client
                .websocket_url("/streams/0123456789abcdefghjkmnpqrstvwxyz/read")
                .expect("WebSocket URL")
                .as_str(),
            "wss://example.com/api/v1/streams/0123456789abcdefghjkmnpqrstvwxyz/read"
        );
    }

    #[test]
    fn sse_query_keeps_the_original_absolute_selector_and_limit() {
        let stream_id = "0123456789abcdefghjkmnpqrstvwxyz"
            .parse()
            .expect("stream ID");
        let mut options = ReadStreamOptions::new(stream_id);
        options.start = Some(ReadStart::SeqNum(42));
        options.count = Some(7);
        options.snapshot = true;
        let mut url = Url::parse("https://tail.surf/api/v1/streams/id/records").expect("SSE URL");

        append_sse_query(&mut url, &options);

        assert_eq!(url.query(), Some("seq_num=42&count=7&snapshot=true"));
    }

    #[test]
    fn sse_parser_retains_only_strict_versioned_resume_ids() {
        let cursor = "v1,4,0";
        let block = format!(
            "id: {cursor}\nevent: caught_up\ndata: {{\"next_seq_num\":\"4\",\"last_timestamp_ms\":\"0\"}}"
        );
        let event = parse_sse_block(block.as_bytes())
            .expect("parse SSE event")
            .expect("data event");

        assert_eq!(sse_resume_event_id(&event).expect("resume cursor"), cursor);

        let snapshot_cursor = "v1,4,0,5,0";
        let snapshot_event = ParsedSseEvent {
            event: "records".to_owned(),
            data: "{}".to_owned(),
            id: Some(snapshot_cursor.to_owned()),
        };
        assert_eq!(
            sse_resume_event_id(&snapshot_event).expect("snapshot resume cursor"),
            snapshot_cursor
        );

        for invalid in [
            "v2,4,0",
            "v1,04,0",
            "v1,4,5",
            "v1,4,0,5",
            "v1,4,0,3,6",
            "v1,4,0,5,6,7",
            "v1,0,0,0,1",
            "v1,1,0,1,9007199254740992",
            "v1,4, 0",
        ] {
            let event = ParsedSseEvent {
                event: "caught_up".to_owned(),
                data: "{}".to_owned(),
                id: Some(invalid.to_owned()),
            };
            assert!(matches!(
                sse_resume_event_id(&event),
                Err(TsfClientError::InvalidSse(_))
            ));
        }
    }

    #[tokio::test]
    async fn rejects_read_selectors_outside_the_adapter_range() {
        let client =
            TsfClient::with_api_origin(Url::parse("http://localhost").expect("API origin"))
                .expect("valid API origin");
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
    fn append_ack_counts_half_open_matching_ranges() {
        let ack = AppendAck {
            writer_start_seq_num: 7,
            writer_end_seq_num: 10,
            start_seq_num: 42,
            end_seq_num: 45,
        };

        assert_eq!(ack.record_count().expect("record count"), 3);
        assert_eq!(ack.validate().expect("valid ack"), ack);
    }

    #[test]
    fn append_ack_rejects_mismatched_range_lengths() {
        let ack = AppendAck {
            writer_start_seq_num: 7,
            writer_end_seq_num: 9,
            start_seq_num: 42,
            end_seq_num: 43,
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
            writer_start_seq_num: 7,
            writer_end_seq_num: 9,
            start_seq_num: 42,
            end_seq_num: 44,
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
            writer_start_seq_num: 7,
            writer_end_seq_num: 9,
            start_seq_num: 42,
            end_seq_num: 44,
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
