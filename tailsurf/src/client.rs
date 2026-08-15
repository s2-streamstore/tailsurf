//! Bounded REST, SSE, and WebSocket clients for the TSF service.

use std::{
    collections::{HashSet, VecDeque},
    fmt::Display,
    future::Future,
    pin::Pin,
    str::FromStr,
    sync::{Arc, OnceLock},
    task::{Context, Poll},
    time::Duration,
};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use rand::{Rng, RngExt};
use reqwest::StatusCode;
use secrecy::ExposeSecret;
use serde::de::DeserializeOwned;
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
    ClientWriterId, LinkId, LinkSecret, StreamId, WriterId,
    ids::{encode_base64url_32, is_canonical_base64url_32},
    protocol::{
        rest::{
            ApiErrorResponse, AppendJsonRecord, AppendRange, AppendRecordsRequest, CreateLinkInput,
            CreateStreamRequest, CreateStreamResponse, ListLinksResponse, MAX_LINK_PAGE_ITEMS,
            MAX_REST_ERROR_RESPONSE_BYTES, MAX_REST_RESPONSE_BYTES, MAX_SSE_EVENT_BYTES,
            MAX_SSE_READ_BATCH_PAYLOAD_BYTES, MAX_SSE_READ_BATCH_RECORDS,
            MAX_SSE_UNTERMINATED_EVENT_BYTES, MAX_STATELESS_APPEND_PAYLOAD_BYTES,
            MAX_STATELESS_APPEND_RECORDS, RecordData, RestRecordPart, SseCaughtUpData,
            SseReadBatchData, SseReadRecord, SseSnapshotBoundaryData, StreamLinkCredential,
            StreamMetadata, UpdateStreamRequest,
        },
        ws::{
            DEFAULT_READ_TAIL_OFFSET, MAX_PLAYBACK_RATE_PERMILLE, MAX_READ_SELECTOR_VALUE,
            MIN_PLAYBACK_RATE_PERMILLE, ReadStart, ReadStreamOptions, WriteStreamOptions,
            frame::{
                AppendRecord, CaughtUpPosition, ClientFrame, FrameCodecError,
                MAX_APPEND_BATCH_RECORDS, MAX_BATCH_PAYLOAD_BYTES, MAX_RECORD_BYTES, PartHeader,
                ReadRecord, RecordFormat, ServerFrame, SnapshotBoundary, TSF_WEBSOCKET_PROTOCOL,
            },
        },
    },
};

type ClientWebSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

const API_PREFIX: &str = "/api/v1";
const MAX_CLIENT_DELAY: Duration = Duration::from_millis(2_147_483_647);

/// Timeouts, retry behavior, and API origin for [`TsfClient`].
///
/// Configured durations cannot exceed 2,147,483,647 milliseconds. Required timeouts must be
/// greater than zero.
#[derive(Clone, Debug)]
pub struct TsfClientConfig {
    /// Service origin without the `/api/v1` namespace.
    pub api_origin: Url,
    /// Per-request timeout for REST operations and SSE opening handshakes.
    pub rest_request_timeout: Duration,
    /// Timeout for establishing and upgrading a WebSocket.
    pub websocket_connect_timeout: Duration,
    /// Timeout for authentication, frame sends, and append acknowledgements.
    pub websocket_operation_timeout: Duration,
    /// Optional idle timeout while waiting for a read frame. Protocol heartbeats reset the timer.
    /// `None` waits indefinitely.
    pub websocket_read_idle_timeout: Option<Duration>,
    /// Retry policy for anonymous stream creation, idempotent metadata reads, socket setup, and
    /// consecutive read connection failures.
    pub retry_policy: RetryPolicy,
}

impl TsfClientConfig {
    /// Creates a configuration with bounded defaults for the supplied API origin.
    pub fn new(api_origin: Url) -> Result<Self, TsfClientError> {
        validate_api_origin(&api_origin)?;
        Ok(Self {
            api_origin,
            ..Self::default()
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
    /// Total attempts including the initial request.
    pub max_attempts: usize,
    /// Base delay before the first retry. Client-controlled delays are jittered.
    pub initial_backoff: Duration,
    /// Maximum base delay and server retry hint honored by the client.
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

    fn next_backoff(self, current: Duration) -> Duration {
        current
            .checked_mul(2)
            .unwrap_or(self.max_backoff)
            .min(self.max_backoff)
    }

    fn reconnect_delay(self, retry: usize) -> Duration {
        let multiplier = 1_u32 << retry.min(30);
        let backoff = self
            .initial_backoff
            .checked_mul(multiplier)
            .unwrap_or(self.max_backoff)
            .min(self.max_backoff);
        jittered_backoff(backoff)
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

fn jittered_backoff(backoff: Duration) -> Duration {
    if backoff.is_zero() {
        Duration::ZERO
    } else {
        backoff
            .mul_f64(rand::rng().random_range(0.5_f64..=1.5_f64))
            .min(MAX_CLIENT_DELAY)
    }
}

/// Cloneable TSF REST, SSE, and v1 WebSocket client.
///
/// REST operations preserve their retry identity and use [`RetryPolicy`]. Stateless append retries
/// can create physical duplicates, which logical transcript readers suppress. Durable WebSocket
/// writer recovery is owned by [`TsfWriter`].
#[derive(Clone)]
pub struct TsfClient {
    config: TsfClientConfig,
    http: reqwest::Client,
}

/// Pagination controls for one link inventory request.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ListLinksOptions {
    /// Maximum number of links to return. The service accepts values from 1 through 100.
    pub limit: Option<u8>,
    /// Opaque cursor returned by the previous page.
    pub cursor: Option<String>,
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
        validate_client_config(&config)?;
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

    /// Creates a stream and returns its metadata and initial link credentials.
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
    /// An exact retry requires the same prepared request. The idempotency key alone cannot return
    /// the link credentials.
    pub async fn create_stream_with_idempotency_key(
        &self,
        request: &CreateStreamRequest,
        idempotency_key: &CreateStreamIdempotencyKey,
    ) -> Result<CreateStreamResponse, TsfClientError> {
        if request
            .links
            .iter()
            .any(|link| !is_canonical_base64url_32(link.secret.expose_secret()))
        {
            return Err(TsfClientError::InvalidLinkSecret);
        }
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
    ) -> Result<StreamMetadata, TsfClientError> {
        self.get_json_with_bearer(
            format_args!("/streams/{stream_id}"),
            "get stream",
            link_secret,
        )
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
    ) -> Result<StreamMetadata, TsfClientError> {
        self.retry_transient(|| {
            self.send_json_with_bearer(
                self.http
                    .patch(self.rest_url(format_args!("/streams/{stream_id}")))
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
                    .delete(self.rest_url(format_args!("/streams/{stream_id}"))),
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
        request: &CreateLinkInput,
        owner_link_secret: &LinkSecret,
    ) -> Result<StreamLinkCredential, TsfClientError> {
        if !is_canonical_base64url_32(request.secret.expose_secret()) {
            return Err(TsfClientError::InvalidLinkSecret);
        }
        let link_id = request.link_id.clone();
        self.retry_transient(|| {
            self.send_json_with_bearer(
                self.http
                    .put(self.rest_url(format_args!("/streams/{stream_id}/links/{link_id}")))
                    .json(request),
                "create link",
                Some(owner_link_secret),
            )
        })
        .await
    }

    /// Lists one page of retained, non-secret link metadata.
    pub async fn list_links(
        &self,
        stream_id: &StreamId,
        options: &ListLinksOptions,
        owner_link_secret: &LinkSecret,
    ) -> Result<ListLinksResponse, TsfClientError> {
        if options
            .limit
            .is_some_and(|limit| !(1..=MAX_LINK_PAGE_ITEMS as u8).contains(&limit))
        {
            return Err(TsfClientError::InvalidListLinksOptions(
                "limit must be between 1 and 100",
            ));
        }
        if options.cursor.as_deref() == Some("") {
            return Err(TsfClientError::InvalidListLinksOptions(
                "cursor must not be empty",
            ));
        }
        let mut url = self.rest_url(format_args!("/streams/{stream_id}/links"));
        {
            let mut query = url.query_pairs_mut();
            if let Some(limit) = options.limit {
                query.append_pair("limit", &limit.to_string());
            }
            if let Some(cursor) = &options.cursor {
                query.append_pair("cursor", cursor);
            }
        }
        let page = self
            .retry_transient(|| {
                self.send_json_with_bearer(
                    self.http.get(url.clone()),
                    "list links",
                    Some(owner_link_secret),
                )
            })
            .await?;
        validate_link_page(
            &page,
            options.limit.unwrap_or(MAX_LINK_PAGE_ITEMS as u8) as usize,
        )?;
        Ok(page)
    }

    /// Lists every retained link, following pagination until completion.
    pub async fn list_all_links(
        &self,
        stream_id: &StreamId,
        owner_link_secret: &LinkSecret,
    ) -> Result<ListLinksResponse, TsfClientError> {
        let mut links = Vec::new();
        let mut cursor: Option<String> = None;
        let mut authorizing_link_id = None;
        let mut seen_cursors = HashSet::new();
        loop {
            let page = self
                .list_links(
                    stream_id,
                    &ListLinksOptions {
                        limit: Some(MAX_LINK_PAGE_ITEMS as u8),
                        cursor,
                    },
                    owner_link_secret,
                )
                .await?;
            if authorizing_link_id
                .as_ref()
                .is_some_and(|expected| expected != &page.authorizing_link_id)
            {
                return Err(TsfClientError::InvalidLinkPage(
                    "authorizing link changed across pages",
                ));
            }
            authorizing_link_id.get_or_insert(page.authorizing_link_id);
            links.extend(page.links);
            match page.next_cursor {
                Some(next) if seen_cursors.insert(next.clone()) => cursor = Some(next),
                Some(_) => {
                    return Err(TsfClientError::InvalidLinkPage(
                        "link pagination cursor repeated",
                    ));
                }
                None => break,
            }
        }
        Ok(ListLinksResponse {
            authorizing_link_id: authorizing_link_id
                .expect("link inventory always contains an authorizing link ID"),
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
                    .delete(self.rest_url(format_args!("/streams/{stream_id}/links/{link_id}"))),
                "revoke link",
                Some(owner_link_secret),
            )
        })
        .await
    }

    /// Atomically appends one durable JSON batch without opening a WebSocket.
    ///
    /// A retry keeps client writer identity and writer sequence numbers stable. An ambiguous
    /// response may create physical duplicates. Logical readers suppress those duplicates.
    pub async fn append_records(
        &self,
        stream_id: &StreamId,
        client_writer_id: ClientWriterId,
        records: &[AppendRecord],
        expected_next_seq_num: Option<u64>,
        write_link_secret: &LinkSecret,
    ) -> Result<AppendRange, TsfClientError> {
        if records.is_empty() || records.len() > MAX_STATELESS_APPEND_RECORDS {
            return Err(TsfClientError::InvalidStatelessAppend(
                "record count must be between 1 and 128",
            ));
        }
        let writer_start_seq_num = records[0].writer_seq_num;
        let final_writer_seq_num = writer_start_seq_num
            .checked_add((records.len() - 1) as u64)
            .ok_or(TsfClientError::InvalidStatelessAppend(
                "writer sequence overflow",
            ))?;
        if final_writer_seq_num == u64::MAX {
            return Err(TsfClientError::InvalidStatelessAppend(
                "writer sequence range must end before u64::MAX",
            ));
        }
        if expected_next_seq_num.is_some_and(|value| value > MAX_READ_SELECTOR_VALUE) {
            return Err(TsfClientError::InvalidStatelessAppend(
                "expected next sequence exceeds the data adapter range",
            ));
        }
        let mut json_records = Vec::with_capacity(records.len());
        let mut payload_bytes = 0_usize;
        for (index, record) in records.iter().enumerate() {
            record.validate()?;
            payload_bytes = payload_bytes.checked_add(record.data.len()).ok_or(
                TsfClientError::InvalidStatelessAppend("payload size overflow"),
            )?;
            if record.writer_seq_num
                != writer_start_seq_num.checked_add(index as u64).ok_or(
                    TsfClientError::InvalidStatelessAppend("writer sequence overflow"),
                )?
            {
                return Err(TsfClientError::InvalidStatelessAppend(
                    "writer sequence numbers must be contiguous",
                ));
            }
            let data = compact_record_data(&record.data);
            json_records.push(AppendJsonRecord {
                data,
                format: record.format,
                part: Some(RestRecordPart {
                    index: record.part.index(),
                    is_final: record.part.is_final(),
                }),
            });
        }
        if payload_bytes > MAX_STATELESS_APPEND_PAYLOAD_BYTES {
            return Err(TsfClientError::InvalidStatelessAppend(
                "append payload must not exceed 900 KiB",
            ));
        }
        let request = AppendRecordsRequest {
            client_writer_id: URL_SAFE_NO_PAD.encode(client_writer_id.as_bytes()),
            writer_start_seq_num,
            records: json_records,
            expected_next_seq_num,
        };
        let range: AppendRange = self
            .retry_transient(|| {
                self.send_json_with_bearer(
                    self.http
                        .post(self.rest_url(format_args!("/streams/{stream_id}/records")))
                        .json(&request),
                    "append records",
                    Some(write_link_secret),
                )
            })
            .await?;
        validate_append_range(range, records.len())
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
        mut options: WriteStreamOptions,
        config: TsfWriterConfig,
    ) -> Result<TsfWriter, TsfClientError> {
        let session = self.open_write_session(&options).await?;
        options.expected_next_seq_num = None;
        TsfWriter::new(self.clone(), options, session, config)
    }

    /// Connects a low-level write session that sends records and receives ack ranges directly.
    ///
    /// Unlike [`TsfWriter`], this session does not retain or resend unacknowledged records.
    pub async fn connect_write_session(
        &self,
        options: WriteStreamOptions,
    ) -> Result<TsfWriteSession, TsfClientError> {
        self.open_write_session(&options).await
    }

    async fn open_write_session(
        &self,
        options: &WriteStreamOptions,
    ) -> Result<TsfWriteSession, TsfClientError> {
        self.retry_transient(|| self.connect_write_session_once(options))
            .await
    }

    async fn connect_write_session_once(
        &self,
        options: &WriteStreamOptions,
    ) -> Result<TsfWriteSession, TsfClientError> {
        let url = self.websocket_url(format_args!("/streams/{}/write", options.stream_id))?;
        let connect_timeout = self.config.websocket_connect_timeout;
        let operation_timeout = self.config.websocket_operation_timeout;
        let opening_frame = ClientFrame::OpenWrite {
            client_writer_id: options.client_writer_id,
            link_secret: options.link_secret.clone(),
            expected_next_seq_num: options.expected_next_seq_num,
        }
        .encode()?;

        let mut ws =
            connect_websocket(url, connect_timeout, operation_timeout, opening_frame).await?;
        with_timeout(operation_timeout, "writer ready", expect_ready(&mut ws)).await?;

        Ok(TsfWriteSession {
            ws,
            operation_timeout,
        })
    }

    /// Connects a resumable read session at the requested position and bounds.
    pub async fn connect_reader(
        &self,
        mut options: ReadStreamOptions,
    ) -> Result<TsfReadSession, TsfClientError> {
        let ConnectedReadSocket {
            socket,
            stream_metadata,
            snapshot_boundary,
        } = self.connect_read_socket(&options).await?;
        apply_snapshot_boundary(&mut options, snapshot_boundary);
        Ok(TsfReadSession::new(
            self.clone(),
            options,
            socket,
            stream_metadata,
            None,
            snapshot_boundary,
        ))
    }

    /// Connects a resumable SSE reader.
    ///
    /// Private credentials stay in the bearer header. Reconnects reuse the original URL and send
    /// the latest versioned event cursor in `Last-Event-ID`. The REST request timeout bounds each
    /// opening handshake but not the established event body.
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
                "initial read completed without stream_metadata",
            ))?;
        if let Some(boundary) = connection.snapshot_boundary {
            apply_snapshot_boundary(&mut options, Some(boundary));
        }
        Ok(TsfSseReadSession {
            client: self.clone(),
            options,
            request_options,
            body: connection.body,
            parser: connection.parser,
            queued_records: VecDeque::new(),
            stream_metadata: connection
                .stream_metadata
                .expect("validated stream_metadata event"),
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
        let handshake_timeout = self.config.rest_request_timeout;
        self.retry_transient(|| async {
            with_timeout(
                handshake_timeout,
                "SSE handshake",
                self.open_sse_connection_once(options, last_event_id),
            )
            .await
        })
        .await
    }

    async fn open_sse_connection_once(
        &self,
        options: &ReadStreamOptions,
        last_event_id: Option<&str>,
    ) -> Result<Option<SseConnection>, TsfClientError> {
        let mut url = self.rest_url(format_args!("/streams/{}/records", options.stream_id));
        append_sse_query(&mut url, options);
        let mut request = self.apply_rest_auth(
            self.http.get(url).header("Accept", "text/event-stream"),
            options.link_secret.as_ref(),
        )?;
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
            parser: SseParser::default(),
            stream_metadata: None,
            snapshot_boundary: None,
            resume_event_id: None,
        };
        let event = next_sse_event(&mut connection.body, &mut connection.parser)
            .await?
            .ok_or(TsfClientError::InvalidSse(
                "response ended before stream_metadata",
            ))?;
        if event.event != "stream_metadata" {
            return Err(TsfClientError::InvalidSse(
                "first event is not stream_metadata",
            ));
        }
        connection.stream_metadata = Some(
            serde_json::from_str(&event.data)
                .map_err(|_| TsfClientError::InvalidSse("invalid stream_metadata event"))?,
        );
        if event.id.is_some() {
            connection.resume_event_id = Some(sse_resume_event_id(&event)?.to_owned());
        }
        if options.snapshot {
            let event = next_sse_event(&mut connection.body, &mut connection.parser)
                .await?
                .ok_or(TsfClientError::InvalidSse(
                    "response ended before snapshot_boundary",
                ))?;
            if event.event != "snapshot_boundary" {
                return Err(TsfClientError::InvalidSse(
                    "snapshot_boundary must follow stream_metadata",
                ));
            }
            let (event_id, cursor) = sse_resume_cursor(&event)?;
            let boundary: SseSnapshotBoundaryData = serde_json::from_str(&event.data)
                .map_err(|_| TsfClientError::InvalidSse("invalid snapshot_boundary event"))?;
            let boundary = SnapshotBoundary {
                end_seq_num: boundary.end_seq_num,
                last_timestamp_ms: boundary.last_timestamp_ms,
            };
            let previous = last_event_id.map(parse_sse_resume_cursor).transpose()?;
            validate_sse_snapshot_cursor(boundary, cursor, previous)?;
            connection.resume_event_id = Some(event_id.to_owned());
            connection.snapshot_boundary = Some(boundary);
        }
        Ok(Some(connection))
    }

    async fn connect_read_socket(
        &self,
        options: &ReadStreamOptions,
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
            limit: options.limit,
            end_seq_num: options.end_seq_num,
            playback_rate_permille: options.playback_rate_permille,
            snapshot: options.snapshot,
        }
        .encode()?;
        let url = self.websocket_url(format_args!("/streams/{}/read", options.stream_id))?;
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
                    stream_metadata: handshake.stream_metadata,
                    snapshot_boundary: handshake.snapshot_boundary,
                })
            }
        })
        .await
    }

    fn rest_url(&self, path: impl Display) -> Url {
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
    ) -> Result<reqwest::RequestBuilder, TsfClientError> {
        if let Some(secret) = link_secret {
            if !is_canonical_base64url_32(secret.expose_secret()) {
                return Err(TsfClientError::InvalidLinkSecret);
            }
            Ok(request.bearer_auth(secret.expose_secret()))
        } else {
            Ok(request)
        }
    }

    fn websocket_url(&self, path: impl Display) -> Result<Url, TsfClientError> {
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
        path: impl Display,
        operation: &'static str,
        link_secret: Option<&LinkSecret>,
    ) -> Result<T, TsfClientError> {
        let url = self.rest_url(path);
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
            .apply_rest_auth(request, link_secret)?
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
            .apply_rest_auth(request, link_secret)?
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
        let attempts = retry_policy.max_attempts;
        let mut backoff = retry_policy.initial_backoff;

        for attempt in 1..=attempts {
            match run().await {
                Ok(value) => return Ok(value),
                Err(error) if attempt < attempts && should_retry(&error) => {
                    let delay = error
                        .retry_after()
                        .map(|delay| delay.min(retry_policy.max_backoff))
                        .unwrap_or_else(|| jittered_backoff(backoff));
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
pub struct TsfWriteSession {
    ws: ClientWebSocket,
    operation_timeout: Duration,
}

/// Maximum payload bytes a writer may retain before acknowledgement.
///
/// This matches the TSF writer socket's hard queued-payload bound.
pub const MAX_WRITER_UNACKED_PAYLOAD_BYTES: usize = 5 * 1024 * 1024;
/// Maximum records a writer may retain before acknowledgement.
///
/// This matches the TSF writer socket's hard queued-record bound.
pub const MAX_WRITER_UNACKED_RECORDS: usize = 128;

/// Memory and concurrency bounds for [`TsfWriter`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TsfWriterConfig {
    /// Maximum total payload bytes retained until durability acknowledgement. Must not exceed
    /// [`MAX_WRITER_UNACKED_PAYLOAD_BYTES`].
    pub max_unacked_bytes: usize,
    /// Maximum number of records retained until durability acknowledgement. Must not exceed
    /// [`MAX_WRITER_UNACKED_RECORDS`].
    pub max_unacked_records: usize,
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
        }
    }
}

impl AppendRecord {
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
    terminal_error: Arc<OnceLock<String>>,
}

impl AppendTicket {
    /// Polls for a completed receipt without registering an async wakeup.
    ///
    /// Returns `None` while the record remains pending.
    pub fn try_recv(&mut self) -> Option<Result<AppendReceipt, TsfClientError>> {
        match self.rx.try_recv() {
            Ok(result) => Some(result),
            Err(oneshot::error::TryRecvError::Empty) => {
                retained_terminal_error(&self.terminal_error).map(Err)
            }
            Err(oneshot::error::TryRecvError::Closed) => Some(Err(terminal_writer_error(
                &self.terminal_error,
                TsfClientError::AppendWriterDropped,
            ))),
        }
    }
}

impl Future for AppendTicket {
    type Output = Result<AppendReceipt, TsfClientError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.rx).poll(cx) {
            Poll::Ready(Ok(result)) => Poll::Ready(result),
            Poll::Ready(Err(_)) => Poll::Ready(Err(terminal_writer_error(
                &self.terminal_error,
                TsfClientError::AppendWriterDropped,
            ))),
            Poll::Pending => retained_terminal_error(&self.terminal_error)
                .map_or(Poll::Pending, |error| Poll::Ready(Err(error))),
        }
    }
}

/// Bounded durable writer that retains unacknowledged records and resends them across transient
/// interruptions.
pub struct TsfWriter {
    cmd_tx: mpsc::Sender<WriterCommand>,
    byte_permits: Arc<Semaphore>,
    record_permits: Arc<Semaphore>,
    terminal_error: Arc<OnceLock<String>>,
    max_unacked_bytes: usize,
    task: Option<JoinHandle<()>>,
}

impl TsfWriter {
    fn new(
        client: TsfClient,
        options: WriteStreamOptions,
        session: TsfWriteSession,
        config: TsfWriterConfig,
    ) -> Result<Self, TsfClientError> {
        let config = config.validate()?;
        let command_capacity = config.max_unacked_records + 1;
        let (cmd_tx, cmd_rx) = mpsc::channel(command_capacity);
        let terminal_error = Arc::new(OnceLock::new());
        let task = tokio::spawn(run_writer(
            client,
            options,
            session,
            cmd_rx,
            Arc::clone(&terminal_error),
        ));

        Ok(Self {
            cmd_tx,
            byte_permits: Arc::new(Semaphore::new(config.max_unacked_bytes)),
            record_permits: Arc::new(Semaphore::new(config.max_unacked_records)),
            terminal_error,
            max_unacked_bytes: config.max_unacked_bytes,
            task: Some(task),
        })
    }

    /// Waits for window capacity, submits a record, and returns its durability ticket.
    pub async fn submit(&self, record: AppendRecord) -> Result<AppendTicket, TsfClientError> {
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
            .map_err(|_| self.closed_error())?;
        let byte_permit = self
            .byte_permits
            .clone()
            .acquire_many_owned(bytes as u32)
            .await
            .map_err(|_| self.closed_error())?;
        let cmd_tx_permit = self
            .cmd_tx
            .clone()
            .reserve_owned()
            .await
            .map_err(|_| self.closed_error())?;

        Ok(WritePermit {
            cmd_tx_permit,
            byte_permit,
            record_permit,
            terminal_error: Arc::clone(&self.terminal_error),
            reserved_bytes: bytes,
        })
    }

    /// Stops accepting records, waits for every pending durability acknowledgement, and joins the
    /// writer task.
    pub async fn close(mut self) -> Result<(), TsfClientError> {
        let (done_tx, mut done_rx) = oneshot::channel();
        self.cmd_tx
            .send(WriterCommand::Close { done_tx })
            .await
            .map_err(|_| self.closed_error())?;

        if let Some(task) = self.task.take() {
            task.await
                .map_err(|error| TsfClientError::AppendWriterFailed(error.to_string()))?;
        }

        done_rx.try_recv().map_err(|_| self.dropped_error())?
    }

    fn closed_error(&self) -> TsfClientError {
        terminal_writer_error(&self.terminal_error, TsfClientError::AppendWriterClosed)
    }

    fn dropped_error(&self) -> TsfClientError {
        terminal_writer_error(&self.terminal_error, TsfClientError::AppendWriterDropped)
    }
}

fn terminal_writer_error(
    terminal_error: &OnceLock<String>,
    fallback: TsfClientError,
) -> TsfClientError {
    retained_terminal_error(terminal_error).unwrap_or(fallback)
}

fn retained_terminal_error(terminal_error: &OnceLock<String>) -> Option<TsfClientError> {
    terminal_error
        .get()
        .map(|message| TsfClientError::AppendWriterFailed(message.clone()))
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
    terminal_error: Arc<OnceLock<String>>,
    reserved_bytes: usize,
}

impl WritePermit {
    /// Submits a record no larger than the reserved capacity without awaiting another window slot.
    pub fn submit(self, record: AppendRecord) -> Result<AppendTicket, TsfClientError> {
        if let Some(error) = retained_terminal_error(&self.terminal_error) {
            return Err(error);
        }
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

        Ok(AppendTicket {
            rx: ack_rx,
            terminal_error: self.terminal_error,
        })
    }
}

/// Conversion into payload bytes accepted by [`AppendRecord::new`].
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

impl TsfWriteSession {
    /// Sends one physical record under the operation timeout.
    pub async fn send(&mut self, record: AppendRecord) -> Result<(), TsfClientError> {
        let operation_timeout = self.operation_timeout;

        with_timeout(operation_timeout, "send append frame", async move {
            self.buffer_batch(&[&record]).await?;
            self.flush().await
        })
        .await
    }

    /// Encodes one batch into the socket's write buffer, leaving the flush to the caller.
    async fn buffer_batch(&mut self, records: &[&AppendRecord]) -> Result<(), TsfClientError> {
        let frame = ClientFrame::encode_append_batch(records)?;
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
        let frame = with_timeout(
            self.operation_timeout,
            "append acknowledgement",
            next_server_frame(&mut self.ws),
        )
        .await
        .map_err(|error| match error {
            TsfClientError::WebSocketClosedWithReason { code: 1008, reason }
                if reason == "sequence_mismatch" =>
            {
                TsfClientError::SequenceMismatch
            }
            other => other,
        })?;
        match frame {
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
        record: AppendRecord,
        ack_tx: oneshot::Sender<Result<AppendReceipt, TsfClientError>>,
        byte_permit: OwnedSemaphorePermit,
        record_permit: OwnedSemaphorePermit,
    },
    Close {
        done_tx: oneshot::Sender<Result<(), TsfClientError>>,
    },
}

struct PendingAppend {
    record: AppendRecord,
    ack_tx: oneshot::Sender<Result<AppendReceipt, TsfClientError>>,
    _byte_permit: OwnedSemaphorePermit,
    _record_permit: OwnedSemaphorePermit,
}

async fn run_writer(
    client: TsfClient,
    options: WriteStreamOptions,
    mut session: TsfWriteSession,
    mut cmd_rx: mpsc::Receiver<WriterCommand>,
    terminal_error: Arc<OnceLock<String>>,
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
                                &mut reconnect_attempts,
                                error,
                            )
                            .await
                        {
                            finish_writer_error(
                                &mut pending,
                                &mut close_tx,
                                &terminal_error,
                                error,
                            );
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
                            finish_writer_error(
                                &mut pending,
                                &mut close_tx,
                                &terminal_error,
                                error,
                            );
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
                            &mut reconnect_attempts,
                            TsfClientError::WebSocketClosed,
                        )
                        .await
                        {
                            finish_writer_error(
                                &mut pending,
                                &mut close_tx,
                                &terminal_error,
                                error,
                            );
                            return;
                        }
                    }
                    Err(error) => {
                        if let Err(error) = recover_pending_appends(
                            &mut session,
                            &client,
                            &options,
                            &pending,
                            &mut reconnect_attempts,
                            error,
                        )
                        .await
                        {
                            finish_writer_error(
                                &mut pending,
                                &mut close_tx,
                                &terminal_error,
                                error,
                            );
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
    session: &mut TsfWriteSession,
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
            session.buffer_batch(&batch).await?;
        }
        session.flush().await
    })
    .await
}
async fn recover_pending_appends(
    session: &mut TsfWriteSession,
    client: &TsfClient,
    options: &WriteStreamOptions,
    pending: &VecDeque<PendingAppend>,
    reconnect_attempts: &mut usize,
    mut error: TsfClientError,
) -> Result<(), TsfClientError> {
    if !error.is_retryable() {
        return Err(error);
    }

    let retry_policy = client.config.retry_policy;
    let max_reconnects = retry_policy.max_attempts.saturating_sub(1);
    while *reconnect_attempts < max_reconnects {
        let delay = retry_policy.reconnect_delay(*reconnect_attempts);
        if !delay.is_zero() {
            sleep(delay).await;
        }
        *reconnect_attempts += 1;
        match client.connect_write_session_once(options).await {
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

fn validate_append_range(
    range: AppendRange,
    expected_records: usize,
) -> Result<AppendRange, TsfClientError> {
    if range.end_seq_num.checked_sub(range.start_seq_num) != Some(expected_records as u64) {
        return Err(TsfClientError::InvalidAppendRange(range));
    }
    Ok(range)
}

fn finish_writer_error(
    pending: &mut VecDeque<PendingAppend>,
    close_tx: &mut Option<oneshot::Sender<Result<(), TsfClientError>>>,
    terminal_error: &OnceLock<String>,
    error: TsfClientError,
) {
    let message = error.to_string();
    let _ = terminal_error.set(message.clone());
    fail_pending(pending, message);
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

/// Streaming body for one SSE connection.
type SseBody = Pin<Box<dyn futures_util::Stream<Item = Result<Bytes, reqwest::Error>> + Send>>;

struct ParsedSseEvent {
    event: String,
    data: String,
    id: Option<String>,
}

struct SseConnection {
    body: SseBody,
    parser: SseParser,
    stream_metadata: Option<StreamMetadata>,
    snapshot_boundary: Option<SnapshotBoundary>,
    resume_event_id: Option<String>,
}

/// Resumable HTTP event-stream reader.
///
/// Transient transport and service interruptions reconnect from the next sequence number. Normal
/// completion and configured bounds return `None`; protocol and policy failures surface as errors.
pub struct TsfSseReadSession {
    client: TsfClient,
    options: ReadStreamOptions,
    request_options: ReadStreamOptions,
    body: SseBody,
    parser: SseParser,
    queued_records: VecDeque<ReadRecord>,
    stream_metadata: StreamMetadata,
    last_caught_up: Option<CaughtUpPosition>,
    snapshot_boundary: Option<SnapshotBoundary>,
    reconnect_attempts: usize,
    last_event_id: Option<String>,
    finished: bool,
}

impl TsfSseReadSession {
    /// Returns authorized stream metadata from the opening event.
    pub fn stream_metadata(&self) -> &StreamMetadata {
        &self.stream_metadata
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
            if self.finished || read_options_exhausted(&self.options) {
                return Ok(None);
            }
            if let Some(record) = self.queued_records.pop_front() {
                self.finished = advance_read_options(&mut self.options, record.seq_num);
                return Ok(Some(record));
            }
            let event = match next_sse_event(&mut self.body, &mut self.parser).await {
                Ok(event) => event,
                Err(error) if error.is_resumable_sse_interruption() => None,
                Err(error) => return Err(error),
            };
            let Some(event) = event else {
                let retry_policy = self.client.config.retry_policy;
                let attempts = retry_policy.max_attempts;
                if self.reconnect_attempts + 1 >= attempts {
                    return Err(TsfClientError::ReadReconnectLimitExceeded {
                        max_connection_attempts: attempts,
                    });
                }
                let delay = retry_policy.reconnect_delay(self.reconnect_attempts);
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
                        .is_some_and(|previous| previous != boundary)
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
                self.parser = connection.parser;
                self.stream_metadata = connection
                    .stream_metadata
                    .expect("validated stream_metadata event");
                self.reconnect_attempts = 0;
                continue;
            };
            match event.event.as_str() {
                "read_batch" => {
                    let batch: SseReadBatchData = serde_json::from_str(&event.data)
                        .map_err(|_| TsfClientError::InvalidSse("invalid read_batch event"))?;
                    validate_sse_read_batch_count(batch.records.len())?;
                    let records = batch
                        .records
                        .into_iter()
                        .map(sse_read_record)
                        .collect::<Result<Vec<_>, _>>()?;
                    validate_sse_read_batch(&records, &self.options)?;
                    let (event_id, cursor) = sse_resume_cursor(&event)?;
                    let previous = self
                        .last_event_id
                        .as_deref()
                        .map(parse_sse_resume_cursor)
                        .transpose()?;
                    validate_sse_read_batch_cursor(
                        &records,
                        cursor,
                        previous,
                        &self.options,
                        self.snapshot_boundary,
                    )?;
                    self.last_event_id = Some(event_id.to_owned());
                    self.queued_records.extend(records);
                    self.reconnect_attempts = 0;
                }
                "caught_up" => {
                    let value: SseCaughtUpData = serde_json::from_str(&event.data)
                        .map_err(|_| TsfClientError::InvalidSse("invalid caught_up event"))?;
                    let caught_up = CaughtUpPosition {
                        next_seq_num: value.next_seq_num,
                        last_timestamp_ms: value.last_timestamp_ms,
                    };
                    let (event_id, cursor) = sse_resume_cursor(&event)?;
                    let previous = self
                        .last_event_id
                        .as_deref()
                        .map(parse_sse_resume_cursor)
                        .transpose()?;
                    validate_sse_caught_up_cursor(
                        caught_up,
                        cursor,
                        previous,
                        &self.options,
                        self.snapshot_boundary,
                    )?;
                    self.last_event_id = Some(event_id.to_owned());
                    self.options.start = Some(ReadStart::SeqNum(caught_up.next_seq_num));
                    self.last_caught_up = Some(caught_up);
                    self.reconnect_attempts = 0;
                }
                "error" => return Err(TsfClientError::SseTerminal(event.data)),
                "stream_metadata" => {
                    self.stream_metadata = serde_json::from_str(&event.data)
                        .map_err(|_| TsfClientError::InvalidSse("invalid stream_metadata event"))?
                }
                _ => {}
            }
        }
    }
}

/// Resumable WebSocket reader.
///
/// Transient transport and service interruptions reconnect from the next sequence number. Normal
/// completion and configured bounds return `None`; protocol and policy failures surface as errors.
pub struct TsfReadSession {
    client: TsfClient,
    options: ReadStreamOptions,
    socket: ReadSocket,
    stream_metadata: StreamMetadata,
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
        stream_metadata: StreamMetadata,
        last_caught_up: Option<CaughtUpPosition>,
        snapshot_boundary: Option<SnapshotBoundary>,
    ) -> Self {
        let reconnect_backoff = client.config.retry_policy.initial_backoff;
        Self {
            client,
            options,
            socket,
            stream_metadata,
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
    pub const fn stream_metadata(&self) -> &StreamMetadata {
        &self.stream_metadata
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
                Ok(ReadSocketOutcome::Records(records)) => {
                    validate_read_batch_for_request(&records, &self.options)?;
                    self.socket.pending_records.extend(records);
                }
                Ok(ReadSocketOutcome::CaughtUp(caught_up)) => {
                    validate_caught_up_for_request(caught_up, &self.options)?;
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
        let delay = jittered_backoff(self.pending_reconnect_backoff);
        if !delay.is_zero() {
            sleep(delay).await;
        }
        let ConnectedReadSocket {
            socket,
            stream_metadata,
            snapshot_boundary,
        } = self.client.connect_read_socket(&self.options).await?;
        self.socket = socket;
        self.stream_metadata = stream_metadata;
        apply_snapshot_boundary(&mut self.options, snapshot_boundary);
        if snapshot_boundary.is_some() {
            self.snapshot_boundary = snapshot_boundary;
        }
        self.no_progress_reconnects = 0;
        self.reconnect_backoff = self.client.config.retry_policy.initial_backoff;
        self.pending_reconnect_backoff = Duration::ZERO;
        self.reconnect_needed = false;
        Ok(())
    }

    fn require_reconnect(&mut self) -> Result<(), TsfClientError> {
        if self.reconnect_needed {
            return Ok(());
        }
        let retry_policy = self.client.config.retry_policy;
        let max_reconnects = retry_policy.max_attempts.saturating_sub(1);
        if self.no_progress_reconnects >= max_reconnects {
            return Err(TsfClientError::ReadReconnectLimitExceeded {
                max_connection_attempts: retry_policy.max_attempts,
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
        self.finished = advance_read_options(&mut self.options, seq_num);
    }
}

fn advance_read_options(options: &mut ReadStreamOptions, seq_num: u64) -> bool {
    let Some(next_seq_num) = seq_num.checked_add(1) else {
        return true;
    };
    options.start = Some(ReadStart::SeqNum(next_seq_num));
    if let Some(remaining) = options.limit.as_mut() {
        *remaining = remaining.saturating_sub(1);
    }
    read_options_exhausted(options)
}

fn read_options_exhausted(options: &ReadStreamOptions) -> bool {
    options.limit == Some(0)
        || matches!(
            (options.start, options.end_seq_num),
            (Some(ReadStart::SeqNum(start)), Some(end_seq_num)) if start >= end_seq_num
        )
}

fn validate_read_batch_for_request(
    records: &[ReadRecord],
    options: &ReadStreamOptions,
) -> Result<(), TsfClientError> {
    let Some(first) = records.first() else {
        return Err(TsfClientError::InvalidReadResponse("ReadBatch is empty"));
    };
    let wrong_start = match options.start {
        Some(ReadStart::SeqNum(start)) => first.seq_num != start,
        Some(ReadStart::TimestampMs(start)) => first.timestamp_ms < start,
        Some(ReadStart::TailOffset(_)) | None => false,
    };
    if wrong_start {
        return Err(TsfClientError::InvalidReadResponse(
            "ReadBatch does not begin at the requested position",
        ));
    }
    if options
        .limit
        .is_some_and(|remaining| records.len() as u64 > remaining)
    {
        return Err(TsfClientError::InvalidReadResponse(
            "ReadBatch exceeds the remaining record limit",
        ));
    }
    if options
        .end_seq_num
        .is_some_and(|end_seq_num| records.iter().any(|record| record.seq_num >= end_seq_num))
    {
        return Err(TsfClientError::InvalidReadResponse(
            "ReadBatch crosses the requested end sequence",
        ));
    }
    Ok(())
}

fn validate_caught_up_for_request(
    caught_up: CaughtUpPosition,
    options: &ReadStreamOptions,
) -> Result<(), TsfClientError> {
    if matches!(options.start, Some(ReadStart::SeqNum(next)) if caught_up.next_seq_num != next) {
        return Err(TsfClientError::InvalidReadResponse(
            "CaughtUp does not match the next requested sequence",
        ));
    }
    Ok(())
}

struct ReadSocket {
    ws: ClientWebSocket,
    read_idle_timeout: Option<Duration>,
    pending_records: VecDeque<ReadRecord>,
}

struct ConnectedReadSocket {
    socket: ReadSocket,
    stream_metadata: StreamMetadata,
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
            if let Some(outcome) = outcome {
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
        .map(|value| {
            value
                .to_str()
                .map_err(|_| TsfClientError::InvalidWebSocketProtocolHeader)
        })
        .transpose()?;

    if selected_protocol != Some(TSF_WEBSOCKET_PROTOCOL) {
        return Err(TsfClientError::UnexpectedWebSocketProtocol(
            selected_protocol.map(str::to_owned),
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
    if let Some(value) = options.limit {
        query.append_pair("limit", &value.to_string());
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

#[derive(Default)]
struct SseParser {
    buffer: Vec<u8>,
    offset: usize,
    /// Start of the not-yet-terminated event; only newly pushed bytes are validated.
    tail_start: usize,
}

impl SseParser {
    fn push(&mut self, chunk: &[u8]) -> Result<(), TsfClientError> {
        self.compact();
        self.buffer.extend_from_slice(chunk);
        self.validate_new_bytes(chunk.len())
    }

    fn next_event(&mut self) -> Result<Option<ParsedSseEvent>, TsfClientError> {
        loop {
            let Some((index, length)) = sse_boundary(&self.buffer[self.offset..]) else {
                return Ok(None);
            };
            let start = self.offset;
            self.offset += index + length;
            if let Some(event) = parse_sse_block(&self.buffer[start..start + index])? {
                return Ok(Some(event));
            }
        }
    }

    /// Validates only the bytes appended by the last push; earlier bytes were proven on arrival.
    /// The 3-byte overlap catches an event terminator straddling the previous chunk boundary.
    fn validate_new_bytes(&mut self, pushed: usize) -> Result<(), TsfClientError> {
        let new_start = self.buffer.len() - pushed;
        let mut pos = new_start.saturating_sub(3).max(self.tail_start);
        while let Some((index, length)) = sse_boundary(&self.buffer[pos..]) {
            let boundary_end = pos + index + length;
            if boundary_end - self.tail_start > MAX_SSE_EVENT_BYTES {
                return Err(TsfClientError::InvalidSse("event exceeds 2 MiB"));
            }
            self.tail_start = boundary_end;
            pos = boundary_end;
        }
        if self.buffer.len() - self.tail_start > MAX_SSE_UNTERMINATED_EVENT_BYTES {
            return Err(TsfClientError::InvalidSse(
                "unterminated event exceeds 2 MiB",
            ));
        }
        Ok(())
    }

    fn compact(&mut self) {
        if self.offset >= 64 * 1024 && self.offset >= self.buffer.len() / 2 {
            self.buffer.drain(..self.offset);
            self.tail_start -= self.offset;
            self.offset = 0;
        }
    }
}

async fn next_sse_event(
    body: &mut SseBody,
    parser: &mut SseParser,
) -> Result<Option<ParsedSseEvent>, TsfClientError> {
    loop {
        if let Some(event) = parser.next_event()? {
            return Ok(Some(event));
        }
        match body.next().await {
            Some(Ok(chunk)) => {
                parser.push(&chunk)?;
            }
            Some(Err(error)) => return Err(error.into()),
            None => return Ok(None),
        }
    }
}

fn sse_boundary(buffer: &[u8]) -> Option<(usize, usize)> {
    let mut from = 0;
    while let Some(index) = memchr::memchr(b'\n', &buffer[from..]) {
        let at = from + index;
        if at >= 3 && buffer[at - 3..=at] == *b"\r\n\r\n" {
            return Some((at - 3, 4));
        }
        if at >= 1 && buffer[at - 1] == b'\n' {
            return Some((at - 1, 2));
        }
        from = at + 1;
    }
    None
}

fn parse_sse_block(block: &[u8]) -> Result<Option<ParsedSseEvent>, TsfClientError> {
    let text =
        std::str::from_utf8(block).map_err(|_| TsfClientError::InvalidSse("event is not UTF-8"))?;
    // Borrow during the scan; allocate only for blocks that actually carry data.
    let mut event = None;
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
            "event" => event = Some(value),
            "id" => id = Some(value),
            "data" => data.push(value),
            _ => {}
        }
    }
    if data.is_empty() {
        return Ok(None);
    }
    // Single data lines dominate; join only multi-line payloads.
    let data = match data.as_slice() {
        [single] => (*single).to_owned(),
        lines => lines.join("\n"),
    };
    Ok(Some(ParsedSseEvent {
        event: event.unwrap_or("message").to_owned(),
        data,
        id: id.map(str::to_owned),
    }))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ParsedSseResumeCursor {
    next_seq_num: u64,
    consumed_records: u64,
    snapshot: Option<(u64, u64)>,
}

fn sse_resume_event_id(event: &ParsedSseEvent) -> Result<&str, TsfClientError> {
    Ok(sse_resume_cursor(event)?.0)
}

fn sse_resume_cursor(
    event: &ParsedSseEvent,
) -> Result<(&str, ParsedSseResumeCursor), TsfClientError> {
    let Some(id) = event.id.as_deref() else {
        return Err(invalid_sse_resume_cursor());
    };
    Ok((id, parse_sse_resume_cursor(id)?))
}

fn parse_sse_resume_cursor(id: &str) -> Result<ParsedSseResumeCursor, TsfClientError> {
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
    Ok(ParsedSseResumeCursor {
        next_seq_num,
        consumed_records: consumed_count,
        snapshot,
    })
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

fn compact_record_data(bytes: &[u8]) -> RecordData {
    let Ok(text) = std::str::from_utf8(bytes) else {
        return RecordData::Base64url(URL_SAFE_NO_PAD.encode(bytes));
    };
    let utf8 = RecordData::Utf8(text.to_owned());
    let utf8_len = serde_json::to_vec(&utf8)
        .expect("record data serialization is infallible")
        .len();
    let base64url_len =
        br#"{"encoding":"base64url","value":""}"#.len() + bytes.len().saturating_mul(4).div_ceil(3);
    if utf8_len <= base64url_len {
        utf8
    } else {
        RecordData::Base64url(URL_SAFE_NO_PAD.encode(bytes))
    }
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
    if data.len() > MAX_RECORD_BYTES {
        return Err(TsfClientError::InvalidSse(
            "read_batch contains an oversized record",
        ));
    }
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

fn validate_sse_read_batch(
    records: &[ReadRecord],
    options: &ReadStreamOptions,
) -> Result<(), TsfClientError> {
    let mut payload_bytes = 0_usize;
    let mut previous_seq_num = None;
    for record in records {
        payload_bytes = payload_bytes.saturating_add(record.data.len());
        if payload_bytes > MAX_SSE_READ_BATCH_PAYLOAD_BYTES {
            return Err(TsfClientError::InvalidSse(
                "read_batch exceeds the decoded payload limit",
            ));
        }
        if previous_seq_num
            .is_some_and(|previous: u64| previous.checked_add(1) != Some(record.seq_num))
        {
            return Err(TsfClientError::InvalidSse(
                "read_batch sequence numbers are not contiguous",
            ));
        }
        if options
            .end_seq_num
            .is_some_and(|end_seq_num| record.seq_num >= end_seq_num)
        {
            return Err(TsfClientError::InvalidSse(
                "read_batch crosses the requested end sequence",
            ));
        }
        previous_seq_num = Some(record.seq_num);
    }
    if options
        .limit
        .is_some_and(|remaining| records.len() as u64 > remaining)
    {
        return Err(TsfClientError::InvalidSse(
            "read_batch exceeds the remaining record limit",
        ));
    }
    Ok(())
}

fn validate_sse_read_batch_count(record_count: usize) -> Result<(), TsfClientError> {
    if record_count == 0 || record_count > MAX_SSE_READ_BATCH_RECORDS {
        return Err(TsfClientError::InvalidSse(
            "read_batch record count is outside the protocol limit",
        ));
    }
    Ok(())
}

fn validate_sse_read_batch_cursor(
    records: &[ReadRecord],
    cursor: ParsedSseResumeCursor,
    previous: Option<ParsedSseResumeCursor>,
    options: &ReadStreamOptions,
    snapshot_boundary: Option<SnapshotBoundary>,
) -> Result<(), TsfClientError> {
    let Some(first) = records.first() else {
        return Err(TsfClientError::InvalidSse("read_batch is empty"));
    };
    let Some(expected_next_seq_num) = records
        .last()
        .and_then(|record| record.seq_num.checked_add(1))
    else {
        return Err(TsfClientError::InvalidSse(
            "read_batch cursor cannot follow its records",
        ));
    };
    if cursor.next_seq_num != expected_next_seq_num {
        return Err(TsfClientError::InvalidSse(
            "read_batch cursor does not follow its records",
        ));
    }
    if previous.is_some_and(|value| first.seq_num != value.next_seq_num) {
        return Err(TsfClientError::InvalidSse(
            "read_batch does not resume at the previous cursor",
        ));
    }
    if previous.is_none()
        && matches!(options.start, Some(ReadStart::SeqNum(start)) if first.seq_num != start)
    {
        return Err(TsfClientError::InvalidSse(
            "read_batch does not begin at the requested sequence",
        ));
    }
    if previous.is_none()
        && matches!(options.start, Some(ReadStart::TimestampMs(start)) if first.timestamp_ms < start)
    {
        return Err(TsfClientError::InvalidSse(
            "read_batch begins before the requested timestamp",
        ));
    }
    let expected_consumed = previous
        .map_or(0, |value| value.consumed_records)
        .checked_add(records.len() as u64)
        .ok_or(TsfClientError::InvalidSse(
            "read_batch consumed count overflowed",
        ))?;
    if cursor.consumed_records != expected_consumed {
        return Err(TsfClientError::InvalidSse(
            "read_batch cursor has the wrong consumed count",
        ));
    }
    validate_sse_cursor_boundary(cursor, previous, snapshot_boundary)
}

fn validate_sse_caught_up_cursor(
    caught_up: CaughtUpPosition,
    cursor: ParsedSseResumeCursor,
    previous: Option<ParsedSseResumeCursor>,
    options: &ReadStreamOptions,
    snapshot_boundary: Option<SnapshotBoundary>,
) -> Result<(), TsfClientError> {
    if cursor.next_seq_num != caught_up.next_seq_num {
        return Err(TsfClientError::InvalidSse(
            "caught_up cursor does not match its position",
        ));
    }
    if let Some(previous) = previous {
        if cursor.next_seq_num != previous.next_seq_num
            || cursor.consumed_records != previous.consumed_records
        {
            return Err(TsfClientError::InvalidSse(
                "caught_up does not continue the previous cursor",
            ));
        }
    } else if cursor.consumed_records != 0 {
        return Err(TsfClientError::InvalidSse(
            "initial caught_up cursor has a consumed count",
        ));
    }
    if previous.is_none()
        && matches!(options.start, Some(ReadStart::SeqNum(start)) if cursor.next_seq_num != start)
    {
        return Err(TsfClientError::InvalidSse(
            "initial caught_up does not match the requested sequence",
        ));
    }
    validate_sse_cursor_boundary(cursor, previous, snapshot_boundary)
}

fn validate_sse_snapshot_cursor(
    boundary: SnapshotBoundary,
    cursor: ParsedSseResumeCursor,
    previous: Option<ParsedSseResumeCursor>,
) -> Result<(), TsfClientError> {
    if cursor.snapshot != Some((boundary.end_seq_num, boundary.last_timestamp_ms)) {
        return Err(TsfClientError::InvalidSse(
            "snapshot_boundary cursor does not match its boundary",
        ));
    }
    if let Some(previous) = previous {
        if cursor.next_seq_num != previous.next_seq_num
            || cursor.consumed_records != previous.consumed_records
        {
            return Err(TsfClientError::InvalidSse(
                "snapshot_boundary does not continue the previous cursor",
            ));
        }
    } else if cursor.consumed_records != 0 {
        return Err(TsfClientError::InvalidSse(
            "initial snapshot_boundary cursor has a consumed count",
        ));
    }
    validate_sse_cursor_boundary(cursor, previous, Some(boundary))
}

fn validate_sse_cursor_boundary(
    cursor: ParsedSseResumeCursor,
    previous: Option<ParsedSseResumeCursor>,
    boundary: Option<SnapshotBoundary>,
) -> Result<(), TsfClientError> {
    let expected_snapshot = boundary.map(|value| (value.end_seq_num, value.last_timestamp_ms));
    if cursor.snapshot != expected_snapshot
        || previous.is_some_and(|value| cursor.snapshot != value.snapshot)
    {
        return Err(TsfClientError::InvalidSse(
            "SSE resume cursor changed its snapshot boundary",
        ));
    }
    Ok(())
}

async fn json_response<T: DeserializeOwned>(
    response: reqwest::Response,
    operation: &'static str,
) -> Result<T, TsfClientError> {
    let status = response.status();
    if !status.is_success() {
        return Err(http_status_error(response, operation).await);
    }

    let body = bounded_response_body(response, operation, MAX_REST_RESPONSE_BYTES).await?;
    Ok(serde_json::from_slice(&body)?)
}

async fn http_status_error(response: reqwest::Response, operation: &'static str) -> TsfClientError {
    let status = response.status();
    let header_request_id = response
        .headers()
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    let header_retry_after = response
        .headers()
        .get("retry-after")
        .and_then(|value| value.to_str().ok())
        .and_then(parse_retry_after);
    let raw = bounded_response_body(response, operation, MAX_REST_ERROR_RESPONSE_BYTES)
        .await
        .unwrap_or_default();
    let parsed = serde_json::from_slice::<ApiErrorResponse>(&raw).ok();
    let request_id = header_request_id.or_else(|| {
        parsed
            .as_ref()
            .map(|response| response.error.request_id.as_str())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
    });
    let retry_after = header_retry_after.or_else(|| {
        parsed
            .as_ref()
            .and_then(|response| response.error.retry_after_ms)
            .map(Duration::from_millis)
    });
    let actual_next_seq_num = parsed
        .as_ref()
        .and_then(|response| response.error.actual_next_seq_num);
    let api_code = parsed
        .as_ref()
        .map(|response| response.error.code.clone())
        .filter(|value| !value.is_empty());
    let raw = String::from_utf8(raw).unwrap_or_default();
    let body = api_error_message(&raw).unwrap_or(raw);
    TsfClientError::HttpStatus {
        operation,
        status,
        body,
        api_code,
        request_id,
        retry_after,
        actual_next_seq_num,
    }
}

async fn bounded_response_body(
    response: reqwest::Response,
    operation: &'static str,
    maximum_bytes: usize,
) -> Result<Vec<u8>, TsfClientError> {
    if response
        .content_length()
        .is_some_and(|length| length > maximum_bytes as u64)
    {
        return Err(TsfClientError::ResponseTooLarge {
            operation,
            maximum_bytes,
        });
    }

    let mut body = Vec::new();
    let mut chunks = response.bytes_stream();
    while let Some(chunk) = chunks.next().await {
        let chunk = chunk?;
        if chunk.len() > maximum_bytes.saturating_sub(body.len()) {
            return Err(TsfClientError::ResponseTooLarge {
                operation,
                maximum_bytes,
            });
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn parse_retry_after(value: &str) -> Option<Duration> {
    value.trim().parse::<u64>().ok().map(Duration::from_secs)
}

fn api_error_message(body: &str) -> Option<String> {
    let response: serde_json::Value = serde_json::from_str(body).ok()?;
    let code = response["error"]["code"].as_str()?.trim();
    let message = response["error"]["message"].as_str()?.trim();

    match (code.is_empty(), message.is_empty()) {
        (true, true) => None,
        (true, false) => Some(message.to_owned()),
        (false, true) => Some(code.to_owned()),
        (false, false) => Some(format!("{code}: {message}")),
    }
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
    stream_metadata: StreamMetadata,
    snapshot_boundary: Option<SnapshotBoundary>,
}

async fn expect_read_handshake(
    ws: &mut ClientWebSocket,
    snapshot: bool,
) -> Result<ReadHandshake, TsfClientError> {
    expect_ready(ws).await?;
    let stream_metadata = match next_server_frame(ws).await? {
        Some(ServerFrame::StreamMetadata(stream_metadata)) => stream_metadata,
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
        stream_metadata,
        snapshot_boundary,
    })
}

fn server_frame_name(frame: &ServerFrame) -> &'static str {
    match frame {
        ServerFrame::Ready => "ready",
        ServerFrame::AppendAck { .. } => "append_ack",
        ServerFrame::ReadBatch(_) => "read_batch",
        ServerFrame::Heartbeat => "heartbeat",
        ServerFrame::CaughtUp(_) => "caught_up",
        ServerFrame::StreamMetadata(_) => "stream_metadata",
        ServerFrame::SnapshotBoundary(_) => "snapshot_boundary",
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

fn validate_link_page(
    page: &ListLinksResponse,
    maximum_links: usize,
) -> Result<(), TsfClientError> {
    if page.links.len() > maximum_links {
        return Err(TsfClientError::InvalidLinkPage(
            "page contains more links than requested",
        ));
    }
    if page.next_cursor.is_some() && page.links.is_empty() {
        return Err(TsfClientError::InvalidLinkPage(
            "empty page carries a next cursor",
        ));
    }
    let mut link_ids = HashSet::with_capacity(page.links.len());
    if page
        .links
        .iter()
        .any(|link| !link_ids.insert(&link.link_id))
    {
        return Err(TsfClientError::InvalidLinkPage(
            "page contains duplicate link IDs",
        ));
    }
    Ok(())
}

fn validate_client_config(config: &TsfClientConfig) -> Result<(), TsfClientError> {
    validate_api_origin(&config.api_origin)?;
    for (name, value) in [
        ("rest_request_timeout", config.rest_request_timeout),
        (
            "websocket_connect_timeout",
            config.websocket_connect_timeout,
        ),
        (
            "websocket_operation_timeout",
            config.websocket_operation_timeout,
        ),
    ] {
        if value.is_zero() || value > MAX_CLIENT_DELAY {
            return Err(TsfClientError::InvalidClientConfig(format!(
                "{name} must be greater than zero and at most {} milliseconds",
                MAX_CLIENT_DELAY.as_millis()
            )));
        }
    }
    if config
        .websocket_read_idle_timeout
        .is_some_and(|timeout| timeout.is_zero() || timeout > MAX_CLIENT_DELAY)
    {
        return Err(TsfClientError::InvalidClientConfig(format!(
            "websocket_read_idle_timeout must be greater than zero and at most {} milliseconds when set",
            MAX_CLIENT_DELAY.as_millis()
        )));
    }
    if config.retry_policy.max_attempts == 0 {
        return Err(TsfClientError::InvalidClientConfig(
            "retry_policy.max_attempts must be at least one".to_owned(),
        ));
    }
    if config.retry_policy.initial_backoff > config.retry_policy.max_backoff {
        return Err(TsfClientError::InvalidClientConfig(
            "retry_policy.initial_backoff must not exceed retry_policy.max_backoff".to_owned(),
        ));
    }
    if config.retry_policy.max_backoff > MAX_CLIENT_DELAY {
        return Err(TsfClientError::InvalidClientConfig(format!(
            "retry_policy delays must not exceed {} milliseconds",
            MAX_CLIENT_DELAY.as_millis()
        )));
    }
    Ok(())
}

/// Error surfaced by REST operations, socket setup, reads, and durable writers.
#[derive(Debug, thiserror::Error)]
pub enum TsfClientError {
    /// The configured API origin is not a bare HTTP or HTTPS origin.
    #[error("API origin must be HTTP(S) without credentials, path, query, or fragment: {0}")]
    InvalidApiOrigin(Url),
    /// Client timeout or retry settings are incoherent.
    #[error("invalid client config: {0}")]
    InvalidClientConfig(String),
    /// HTTP transport or response-decoding failure.
    #[error("HTTP client error: {0}")]
    Http(#[from] reqwest::Error),
    /// A REST response contained malformed JSON.
    #[error("invalid JSON in REST response: {0}")]
    Json(#[from] serde_json::Error),
    /// A REST response exceeded the SDK memory-safety bound.
    #[error("{operation} response exceeds {maximum_bytes} bytes")]
    ResponseTooLarge {
        /// Stable operation label.
        operation: &'static str,
        /// Maximum bytes buffered by the SDK.
        maximum_bytes: usize,
    },
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
        /// Actual stream next sequence for a failed sequence precondition.
        actual_next_seq_num: Option<u64>,
    },
    /// Stateless append input violates the local protocol contract.
    #[error("invalid stateless append: {0}")]
    InvalidStatelessAppend(&'static str),
    /// A link secret is not a canonical 32-byte unpadded base64url value.
    #[error("link secret must be canonical 43-character unpadded base64url")]
    InvalidLinkSecret,
    /// SSE response violated the public event contract.
    #[error("invalid SSE response: {0}")]
    InvalidSse(&'static str),
    /// WebSocket read response violated the requested stream contract.
    #[error("invalid WebSocket read response: {0}")]
    InvalidReadResponse(&'static str),
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
    /// The stream did not start the writer session at its requested sequence.
    #[error("stream next sequence did not match the writer session precondition")]
    SequenceMismatch,
    /// Link-list pagination controls are outside the supported range.
    #[error("invalid list links options: {0}")]
    InvalidListLinksOptions(&'static str),
    /// A link inventory page violated pagination invariants.
    #[error("invalid link page: {0}")]
    InvalidLinkPage(&'static str),
    /// The server returned an invalid or mismatched ack range.
    #[error("server sent invalid append acknowledgement {0:?}")]
    InvalidAppendAck(AppendAck),
    /// The server returned a stateless append range with the wrong length.
    #[error("server sent invalid stateless append range {0:?}")]
    InvalidAppendRange(AppendRange),
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

    /// Returns the actual stream next sequence attached to a failed sequence precondition.
    pub fn actual_next_seq_num(&self) -> Option<u64> {
        match self {
            Self::HttpStatus {
                actual_next_seq_num,
                ..
            } => *actual_next_seq_num,
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
            Self::Json(_) => true,
            Self::HttpStatus { status, .. } => is_retryable_http_status(status.as_u16()),
            _ => false,
        }
    }

    fn is_retryable(&self) -> bool {
        if self.is_resumable_read_interruption() {
            return true;
        }
        match self {
            Self::Http(error) => error.is_timeout() || error.is_connect(),
            Self::HttpStatus { status, .. } => is_retryable_http_status(status.as_u16()),
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

    fn is_resumable_sse_interruption(&self) -> bool {
        match self {
            Self::Http(error) => {
                error.is_timeout() || error.is_connect() || error.is_body() || error.is_decode()
            }
            Self::HttpStatus { status, .. } => is_retryable_http_status(status.as_u16()),
            Self::Timeout { .. } => true,
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
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_tungstenite::connect_async;

    use super::*;

    #[test]
    fn parses_structured_http_error_details() {
        let body = r#"{"error":{"code":"sequence_mismatch","message":"position changed","request_id":"request-42","retry_after_ms":125,"actual_next_seq_num":"42","future_field":true}}"#;
        let parsed: ApiErrorResponse = serde_json::from_str(body).expect("structured API error");

        assert_eq!(
            api_error_message(body).as_deref(),
            Some("sequence_mismatch: position changed")
        );
        assert_eq!(parsed.error.request_id, "request-42");
        assert_eq!(parsed.error.retry_after_ms, Some(125));
        assert_eq!(parsed.error.actual_next_seq_num, Some(42));
        for invalid in ["", "00", "01", "-1", "18446744073709551616"] {
            let body = format!(
                r#"{{"error":{{"code":"sequence_mismatch","message":"position changed","request_id":"request-42","actual_next_seq_num":"{invalid}"}}}}"#,
            );
            assert!(serde_json::from_str::<ApiErrorResponse>(&body).is_err());
        }
        assert_eq!(api_error_message("plain failure"), None);
    }

    #[tokio::test]
    async fn sse_handshake_uses_the_rest_request_timeout() {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind SSE listener");
        let address = listener.local_addr().expect("SSE listener address");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept SSE request");
            let mut request = [0_u8; 4096];
            let _ = stream.read(&mut request).await.expect("read SSE request");
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n",
                )
                .await
                .expect("write SSE headers");
            sleep(Duration::from_secs(1)).await;
        });
        let mut config =
            TsfClientConfig::new(Url::parse(&format!("http://{address}")).expect("SSE API origin"))
                .expect("valid client config");
        config.rest_request_timeout = Duration::from_millis(20);
        config.retry_policy = RetryPolicy::none();
        let client = TsfClient::with_config(config).expect("SSE client");
        let stream_id = "00000000000000000000000000000000"
            .parse()
            .expect("stream ID");

        let started_at = std::time::Instant::now();
        let result = client
            .connect_sse_reader(ReadStreamOptions::new(stream_id))
            .await;

        assert!(matches!(
            result,
            Err(TsfClientError::Timeout {
                operation: "SSE handshake"
            })
        ));
        assert!(started_at.elapsed() < Duration::from_millis(200));
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn rest_response_rejects_declared_body_above_memory_bound() {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind REST listener");
        let address = listener.local_addr().expect("REST listener address");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept REST request");
            let mut request = [0_u8; 4096];
            let _ = stream.read(&mut request).await.expect("read REST request");
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                MAX_REST_RESPONSE_BYTES + 1
            );
            stream
                .write_all(response.as_bytes())
                .await
                .expect("write REST headers");
        });
        let mut config = TsfClientConfig::new(
            Url::parse(&format!("http://{address}")).expect("REST API origin"),
        )
        .expect("valid client config");
        config.retry_policy = RetryPolicy::none();
        let client = TsfClient::with_config(config).expect("REST client");
        let stream_id = "00000000000000000000000000000000"
            .parse()
            .expect("stream ID");

        let result = client.get_stream(&stream_id, None).await;

        assert!(matches!(
            result,
            Err(TsfClientError::ResponseTooLarge {
                operation: "get stream",
                maximum_bytes: MAX_REST_RESPONSE_BYTES,
            })
        ));
        server.await.expect("join REST server");
    }

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
        let stream_metadata = StreamMetadata {
            stream_id: "00000000000000000000000000000000"
                .parse()
                .expect("stream ID"),
            title: None,
            visibility: crate::protocol::rest::Visibility::Private,
            created_at: "2026-08-13T00:00:00Z".to_owned(),
            expires_at: "2026-08-23T00:00:00Z".to_owned(),
        };
        let expected_stream_metadata = stream_metadata.clone();
        let sender = tokio::spawn(async move {
            for frame in [
                ServerFrame::Ready,
                ServerFrame::StreamMetadata(stream_metadata),
            ] {
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

        assert_eq!(handshake.stream_metadata, expected_stream_metadata);
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
    fn rejects_incoherent_client_config() {
        let mut config = TsfClientConfig::default();
        config.retry_policy.max_attempts = 0;
        assert!(matches!(
            TsfClient::with_config(config),
            Err(TsfClientError::InvalidClientConfig(_))
        ));

        let mut config = TsfClientConfig::default();
        config.retry_policy.initial_backoff = Duration::from_secs(2);
        config.retry_policy.max_backoff = Duration::from_secs(1);
        assert!(matches!(
            TsfClient::with_config(config),
            Err(TsfClientError::InvalidClientConfig(_))
        ));

        let config = TsfClientConfig {
            rest_request_timeout: Duration::ZERO,
            ..TsfClientConfig::default()
        };
        assert!(matches!(
            TsfClient::with_config(config),
            Err(TsfClientError::InvalidClientConfig(_))
        ));

        let config = TsfClientConfig {
            websocket_connect_timeout: MAX_CLIENT_DELAY + Duration::from_millis(1),
            ..TsfClientConfig::default()
        };
        assert!(matches!(
            TsfClient::with_config(config),
            Err(TsfClientError::InvalidClientConfig(_))
        ));

        let mut config = TsfClientConfig::default();
        config.retry_policy.max_backoff = MAX_CLIENT_DELAY + Duration::from_millis(1);
        assert!(matches!(
            TsfClient::with_config(config),
            Err(TsfClientError::InvalidClientConfig(_))
        ));
    }

    #[test]
    fn rejects_invalid_link_page_invariants() {
        let link = serde_json::json!({
            "link_id": "reader",
            "permissions": "r",
            "status": "active",
            "created_at": "2026-08-13T00:00:00Z",
            "expires_at": null,
            "revoked_at": null
        });
        let duplicate: ListLinksResponse = serde_json::from_value(serde_json::json!({
            "authorizing_link_id": "owner",
            "links": [link.clone(), link],
            "next_cursor": null
        }))
        .expect("decodable duplicate page");
        assert!(matches!(
            validate_link_page(&duplicate, 100),
            Err(TsfClientError::InvalidLinkPage(_))
        ));

        let empty_with_cursor: ListLinksResponse = serde_json::from_value(serde_json::json!({
            "authorizing_link_id": "owner",
            "links": [],
            "next_cursor": "next"
        }))
        .expect("decodable empty page");
        assert!(matches!(
            validate_link_page(&empty_with_cursor, 100),
            Err(TsfClientError::InvalidLinkPage(_))
        ));
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
    fn builds_versioned_rest_and_path_only_websocket_urls() {
        let client =
            TsfClient::with_api_origin(Url::parse("https://example.com").expect("API origin"))
                .expect("valid API origin");

        assert_eq!(
            client.rest_url("/streams").as_str(),
            "https://example.com/api/v1/streams"
        );
        assert_eq!(
            client
                .websocket_url("/streams/0123456789abcdefghjkmnpqrstvwxyz/read")
                .expect("WebSocket URL")
                .as_str(),
            "wss://example.com/api/v1/streams/0123456789abcdefghjkmnpqrstvwxyz/read"
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
    fn sse_query_keeps_the_original_absolute_selector_and_limit() {
        let stream_id = "0123456789abcdefghjkmnpqrstvwxyz"
            .parse()
            .expect("stream ID");
        let mut options = ReadStreamOptions::new(stream_id);
        options.start = Some(ReadStart::SeqNum(42));
        options.limit = Some(7);
        options.snapshot = true;
        let mut url = Url::parse("https://tail.surf/api/v1/streams/id/records").expect("SSE URL");

        append_sse_query(&mut url, &options);

        assert_eq!(url.query(), Some("seq_num=42&limit=7&snapshot=true"));
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
            event: "read_batch".to_owned(),
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

    #[tokio::test]
    async fn close_preserves_terminal_error_when_its_command_is_dropped() {
        let (cmd_tx, mut cmd_rx) = mpsc::channel::<WriterCommand>(1);
        let terminal_error = Arc::new(OnceLock::new());
        let task_terminal_error = Arc::clone(&terminal_error);
        let task = tokio::spawn(async move {
            let command = cmd_rx.recv().await.expect("close command");
            task_terminal_error
                .set("stream next sequence did not match".to_owned())
                .expect("set terminal error");
            drop(command);
        });
        let writer = TsfWriter {
            cmd_tx,
            byte_permits: Arc::new(Semaphore::new(1)),
            record_permits: Arc::new(Semaphore::new(1)),
            terminal_error,
            max_unacked_bytes: 1,
            task: Some(task),
        };

        let error = writer.close().await.expect_err("close must fail");
        assert!(
            matches!(&error, TsfClientError::AppendWriterFailed(message) if message.contains("stream next sequence did not match")),
            "error={error}"
        );
    }

    #[test]
    fn read_and_stateless_append_responses_match_the_requested_ranges() {
        assert!(matches!(
            validate_append_range(
                AppendRange {
                    start_seq_num: 4,
                    end_seq_num: 6,
                },
                1,
            ),
            Err(TsfClientError::InvalidAppendRange(_))
        ));

        let mut options = ReadStreamOptions::new(
            "00000000000000000000000000000000"
                .parse()
                .expect("stream ID"),
        );
        options.start = Some(ReadStart::SeqNum(2));
        assert!(validate_read_batch_for_request(&[sse_test_record(1, 0)], &options).is_err());
        assert!(
            validate_caught_up_for_request(
                CaughtUpPosition {
                    next_seq_num: 1,
                    last_timestamp_ms: 0,
                },
                &options,
            )
            .is_err()
        );
    }

    #[test]
    fn dispatch_ack_rejects_more_records_than_are_pending() {
        let permits = Arc::new(Semaphore::new(2));
        let (ack_tx, _ack_rx) = oneshot::channel();
        let record = AppendRecord::new(7, PartHeader::unsplit(), RecordFormat::Bytes, Bytes::new());
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
                record: AppendRecord::new(
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

    #[tokio::test]
    async fn sse_parser_accepts_multiple_complete_events_in_one_large_chunk() {
        let payload = format!(
            "event: first\ndata: {}\n\nevent: second\ndata: {}\n\n",
            "a".repeat(1_100_000),
            "b".repeat(1_100_000),
        );
        let mut body: SseBody =
            Box::pin(futures_util::stream::iter(vec![Ok::<_, reqwest::Error>(
                Bytes::from(payload),
            )]));
        let mut parser = SseParser::default();

        assert_eq!(
            next_sse_event(&mut body, &mut parser)
                .await
                .expect("first event")
                .expect("first event value")
                .event,
            "first"
        );
        assert_eq!(
            next_sse_event(&mut body, &mut parser)
                .await
                .expect("second event")
                .expect("second event value")
                .event,
            "second"
        );
    }

    #[tokio::test]
    async fn sse_parser_accepts_an_event_fragmented_across_chunks() {
        let payload = "event: read_batch\ndata: split 😀 payload\n\n".as_bytes();
        let chunks = payload
            .chunks(7)
            .map(|chunk| Ok::<_, reqwest::Error>(Bytes::copy_from_slice(chunk)))
            .collect::<Vec<_>>();
        let mut body: SseBody = Box::pin(futures_util::stream::iter(chunks));
        let mut parser = SseParser::default();

        let event = next_sse_event(&mut body, &mut parser)
            .await
            .expect("fragmented event")
            .expect("fragmented event value");
        assert_eq!(event.event, "read_batch");
        assert_eq!(event.data, "split 😀 payload");
    }

    #[tokio::test]
    async fn sse_parser_rejects_one_oversized_completed_event() {
        let payload = format!(
            "event: read_batch\ndata: {}\n\n",
            "a".repeat(MAX_SSE_EVENT_BYTES)
        );
        let mut body: SseBody =
            Box::pin(futures_util::stream::iter(vec![Ok::<_, reqwest::Error>(
                Bytes::from(payload),
            )]));
        let mut parser = SseParser::default();

        assert!(matches!(
            next_sse_event(&mut body, &mut parser).await,
            Err(TsfClientError::InvalidSse("event exceeds 2 MiB"))
        ));
    }

    #[tokio::test]
    async fn sse_parser_rejects_an_oversized_fragmented_event() {
        let first = format!(
            "event: read_batch\ndata: {}",
            "a".repeat(MAX_SSE_UNTERMINATED_EVENT_BYTES / 2)
        );
        let second = "a".repeat(MAX_SSE_UNTERMINATED_EVENT_BYTES / 2 + 1);
        let mut body: SseBody = Box::pin(futures_util::stream::iter(vec![
            Ok::<_, reqwest::Error>(Bytes::from(first)),
            Ok::<_, reqwest::Error>(Bytes::from(second)),
        ]));
        let mut parser = SseParser::default();

        assert!(matches!(
            next_sse_event(&mut body, &mut parser).await,
            Err(TsfClientError::InvalidSse(
                "unterminated event exceeds 2 MiB"
            ))
        ));
    }

    #[tokio::test]
    async fn sse_parser_rejects_an_event_oversized_across_fragments() {
        let first = format!("event: read_batch\ndata: {}", "a".repeat(1_500_000));
        let second = format!("{}\n\n", "b".repeat(700_000));
        let mut body: SseBody = Box::pin(futures_util::stream::iter(vec![
            Ok::<_, reqwest::Error>(Bytes::from(first)),
            Ok::<_, reqwest::Error>(Bytes::from(second)),
        ]));
        let mut parser = SseParser::default();

        assert!(matches!(
            next_sse_event(&mut body, &mut parser).await,
            Err(TsfClientError::InvalidSse("event exceeds 2 MiB"))
        ));
    }

    #[tokio::test]
    async fn sse_parser_accepts_a_terminator_split_across_chunks() {
        let chunks = ["data: hi\r\n\r", "\n"]
            .into_iter()
            .map(|chunk| Ok::<_, reqwest::Error>(Bytes::copy_from_slice(chunk.as_bytes())))
            .collect::<Vec<_>>();
        let mut body: SseBody = Box::pin(futures_util::stream::iter(chunks));
        let mut parser = SseParser::default();

        let event = next_sse_event(&mut body, &mut parser)
            .await
            .expect("straddled event")
            .expect("straddled event value");
        assert_eq!(event.data, "hi");
    }

    #[test]
    fn sse_batch_validation_enforces_decoded_bounds_and_read_limits() {
        assert!(validate_sse_read_batch_count(0).is_err());
        assert!(validate_sse_read_batch_count(MAX_SSE_READ_BATCH_RECORDS + 1).is_err());

        let mut options = ReadStreamOptions::new(
            "00000000000000000000000000000000"
                .parse()
                .expect("stream ID"),
        );
        let aggregate = [0, 1, 2].map(|seq_num| sse_test_record(seq_num, 400 * 1024));
        assert!(validate_sse_read_batch(&aggregate, &options).is_err());

        options.limit = Some(1);
        let two = [sse_test_record(0, 0), sse_test_record(1, 0)];
        assert!(validate_sse_read_batch(&two, &options).is_err());

        options.limit = None;
        options.end_seq_num = Some(1);
        assert!(validate_sse_read_batch(&two, &options).is_err());

        let non_contiguous = [sse_test_record(0, 0), sse_test_record(2, 0)];
        options.end_seq_num = None;
        assert!(validate_sse_read_batch(&non_contiguous, &options).is_err());

        let oversized = SseReadRecord {
            seq_num: 0,
            timestamp_ms: 0,
            writer_id: URL_SAFE_NO_PAD.encode([0_u8; 16]),
            writer_seq_num: 0,
            part: RestRecordPart {
                index: 0,
                is_final: true,
            },
            format: RecordFormat::Bytes,
            data: RecordData::Base64url(URL_SAFE_NO_PAD.encode(vec![0_u8; MAX_RECORD_BYTES + 1])),
        };
        assert!(sse_read_record(oversized).is_err());
    }

    #[test]
    fn sse_cursor_validation_binds_positions_counts_and_snapshot_timestamps() {
        let mut options = ReadStreamOptions::new(
            "00000000000000000000000000000000"
                .parse()
                .expect("stream ID"),
        );
        options.start = Some(ReadStart::SeqNum(0));
        let records = [sse_test_record(0, 0)];
        assert!(
            validate_sse_read_batch_cursor(
                &records,
                ParsedSseResumeCursor {
                    next_seq_num: 2,
                    consumed_records: 1,
                    snapshot: None,
                },
                None,
                &options,
                None,
            )
            .is_err()
        );
        let previous = ParsedSseResumeCursor {
            next_seq_num: 1,
            consumed_records: 1,
            snapshot: None,
        };
        assert!(
            validate_sse_caught_up_cursor(
                CaughtUpPosition {
                    next_seq_num: 2,
                    last_timestamp_ms: 0,
                },
                ParsedSseResumeCursor {
                    next_seq_num: 2,
                    consumed_records: 1,
                    snapshot: None,
                },
                Some(previous),
                &options,
                None,
            )
            .is_err()
        );

        let boundary = SnapshotBoundary {
            end_seq_num: 2,
            last_timestamp_ms: 10,
        };
        assert!(
            validate_sse_snapshot_cursor(
                boundary,
                ParsedSseResumeCursor {
                    next_seq_num: 0,
                    consumed_records: 0,
                    snapshot: Some((2, 11)),
                },
                None,
            )
            .is_err()
        );
    }

    fn sse_test_record(seq_num: u64, payload_bytes: usize) -> ReadRecord {
        ReadRecord {
            seq_num,
            timestamp_ms: seq_num,
            writer_id: WriterId::from_bytes([0_u8; 16]),
            writer_seq_num: seq_num,
            part: PartHeader::unsplit(),
            format: RecordFormat::Bytes,
            data: Bytes::from(vec![0_u8; payload_bytes]),
        }
    }

    #[test]
    fn stateless_append_compacts_an_escape_heavy_maximum_record() {
        let data = vec![0_u8; MAX_RECORD_BYTES];
        let encoded = compact_record_data(&data);
        assert!(matches!(encoded, RecordData::Base64url(_)));
        let request = AppendRecordsRequest {
            client_writer_id: URL_SAFE_NO_PAD.encode([0_u8; 16]),
            writer_start_seq_num: 0,
            records: vec![AppendJsonRecord {
                part: None,
                format: RecordFormat::Transcript,
                data: encoded,
            }],
            expected_next_seq_num: None,
        };
        let json = serde_json::to_vec(&request).expect("append request JSON");
        assert!(json.len() <= crate::protocol::rest::MAX_STATELESS_APPEND_JSON_BYTES);
    }

    #[tokio::test]
    async fn stateless_append_rejects_aggregate_payload_and_max_writer_sequence() {
        let client = TsfClient::new();
        let stream_id = "00000000000000000000000000000000"
            .parse()
            .expect("stream ID");
        let secret = LinkSecret::from("A".repeat(43));
        let large = vec![
            AppendRecord::new(
                0,
                PartHeader::unsplit(),
                RecordFormat::Bytes,
                Bytes::from(vec![0; 500 * 1024]),
            ),
            AppendRecord::new(
                1,
                PartHeader::unsplit(),
                RecordFormat::Bytes,
                Bytes::from(vec![0; 500 * 1024]),
            ),
        ];
        assert!(matches!(
            client
                .append_records(
                    &stream_id,
                    ClientWriterId::from_bytes([0; 16]),
                    &large,
                    None,
                    &secret
                )
                .await,
            Err(TsfClientError::InvalidStatelessAppend(
                "append payload must not exceed 900 KiB"
            ))
        ));

        let endpoint = [AppendRecord::new(
            u64::MAX,
            PartHeader::unsplit(),
            RecordFormat::Bytes,
            Bytes::new(),
        )];
        assert!(matches!(
            client
                .append_records(
                    &stream_id,
                    ClientWriterId::from_bytes([0; 16]),
                    &endpoint,
                    None,
                    &secret,
                )
                .await,
            Err(TsfClientError::InvalidStatelessAppend(
                "writer sequence range must end before u64::MAX"
            ))
        ));

        let valid = [AppendRecord::new(
            0,
            PartHeader::unsplit(),
            RecordFormat::Bytes,
            Bytes::new(),
        )];
        assert!(matches!(
            client
                .append_records(
                    &stream_id,
                    ClientWriterId::from_bytes([0; 16]),
                    &valid,
                    Some(MAX_READ_SELECTOR_VALUE + 1),
                    &secret,
                )
                .await,
            Err(TsfClientError::InvalidStatelessAppend(
                "expected next sequence exceeds the data adapter range"
            ))
        ));
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
