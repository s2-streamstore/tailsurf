//! Bounded REST, SSE, and WebSocket clients for the TSF service.

use std::{
    collections::{HashSet, VecDeque},
    fmt::{self, Display},
    future::Future,
    ops::Range,
    pin::Pin,
    str::FromStr,
    sync::{
        Arc, OnceLock, RwLock,
        atomic::{AtomicU8, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use rand::RngExt;
use reqwest::StatusCode;
use secrecy::ExposeSecret;
use serde::de::DeserializeOwned;
use tokio::{
    net::TcpStream,
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot},
    task::JoinHandle,
    time::{Instant, sleep, timeout, timeout_at},
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
    ids::{is_canonical_base64url_32, random_base64url_32},
    protocol::{
        MAX_SAFE_INTEGER_U64,
        read::{
            DEFAULT_READ_TAIL_OFFSET, MAX_PLAYBACK_RATE, MAX_READ_WAIT_SECONDS, MIN_PLAYBACK_RATE,
            ReadOptions, ReadStart,
        },
        rest::{
            ApiError, ApiErrorResponse, AppendJsonRecord, AppendRange, AppendRecordsRequest,
            CreateLinkInput, CreateStreamRequest, CreateStreamResponse, ListLinksResponse,
            MAX_LINK_PAGE_ITEMS, MAX_REST_ERROR_RESPONSE_BYTES, MAX_REST_RESPONSE_BYTES,
            MAX_SSE_EVENT_BYTES, MAX_SSE_UNTERMINATED_EVENT_BYTES,
            MAX_STATELESS_APPEND_PAYLOAD_BYTES, MAX_STATELESS_APPEND_RECORDS, RecordData,
            RestRecordPart, SseCaughtUpData, SseReadBatchData, StreamLinkCredential,
            StreamMetadata, UpdateStreamRequest, parse_canonical_decimal_u64,
        },
        ws::{
            WriteStreamOptions,
            frame::{
                AppendBatch, AppendRecord, CaughtUpPosition, ClientFrame, FrameCodecError,
                MAX_APPEND_FRAME_RECORDS, MAX_FRAME_PAYLOAD_BYTES, OwnedReadRecord, PartHeader,
                ReadBatch, RecordPayload, ServerFrame, TSF_WEBSOCKET_PROTOCOL,
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

    /// Returns the jittered delay before retry `retry` (0-indexed).
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
    /// The client generates one idempotency key for this logical call and reuses the complete
    /// request while retrying transient failures according to policy.
    pub async fn create_stream(
        &self,
        request: &CreateStreamRequest,
    ) -> Result<CreateStreamResponse, TsfClientError> {
        let idempotency_key = IdempotencyKey::new_random();
        self.create_stream_with_idempotency_key(request, &idempotency_key)
            .await
    }

    /// Creates a logical stream using a caller-owned idempotency key.
    ///
    /// An exact retry requires the same request and returns the same server-minted credentials.
    pub async fn create_stream_with_idempotency_key(
        &self,
        request: &CreateStreamRequest,
        idempotency_key: &IdempotencyKey,
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
    /// Transient failures are retried with one idempotency key.
    pub async fn create_link(
        &self,
        stream_id: &StreamId,
        request: &CreateLinkInput,
        owner_link_secret: &LinkSecret,
    ) -> Result<StreamLinkCredential, TsfClientError> {
        let idempotency_key = IdempotencyKey::new_random();
        self.create_link_with_idempotency_key(
            stream_id,
            request,
            &idempotency_key,
            owner_link_secret,
        )
        .await
    }

    /// Creates or retries one link with a caller-owned idempotency key.
    pub async fn create_link_with_idempotency_key(
        &self,
        stream_id: &StreamId,
        request: &CreateLinkInput,
        idempotency_key: &IdempotencyKey,
        owner_link_secret: &LinkSecret,
    ) -> Result<StreamLinkCredential, TsfClientError> {
        self.retry_when(
            || {
                self.send_json_with_bearer(
                    self.http
                        .put(self.rest_url(format_args!(
                            "/streams/{stream_id}/links/{}",
                            request.link_id
                        )))
                        .header("Idempotency-Key", idempotency_key.expose_secret())
                        .json(request),
                    "create link",
                    Some(owner_link_secret),
                )
            },
            TsfClientError::is_recoverable_create_failure,
        )
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
        let mut seen_link_ids = HashSet::new();
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
            // validate_link_page rejects duplicates within a page. Keep that invariant across
            // pages.
            for link in &page.links {
                if !seen_link_ids.insert(link.link_id.clone()) {
                    return Err(TsfClientError::InvalidLinkPage(
                        "link ID appears on multiple pages",
                    ));
                }
            }
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
        if expected_next_seq_num.is_some_and(|value| value > MAX_SAFE_INTEGER_U64) {
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
    ///
    /// The default retained backlog matches the server's per-socket in-flight window
    /// ([`MAX_WRITER_IN_FLIGHT_BYTES`] / [`MAX_WRITER_IN_FLIGHT_RECORDS`]), so a submitted
    /// batch larger than 5 MiB is rejected at submission. Use [`Self::connect_writer_with_config`]
    /// with a backlog sized to the largest submitted batch for bigger logical records.
    pub async fn connect_writer(
        &self,
        options: DurableWriterOptions,
    ) -> Result<TsfWriter, TsfClientError> {
        self.connect_writer_with_config(options, TsfWriterConfig::default())
            .await
    }

    /// Connects a durable writer with explicit retained-backlog bounds.
    pub async fn connect_writer_with_config(
        &self,
        options: DurableWriterOptions,
        config: TsfWriterConfig,
    ) -> Result<TsfWriter, TsfClientError> {
        let config = config.validate()?;
        let mut session_options = WriteStreamOptions::new(
            options.stream_id,
            ClientWriterId::new_random(),
            options.link_secret,
        );
        session_options.expected_next_seq_num = options.expected_next_seq_num;
        let session = self.open_write_session(&session_options).await?;
        session_options.expected_next_seq_num = None;
        Ok(TsfWriter::new(
            self.clone(),
            session_options,
            session,
            config,
        ))
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

    /// Connects a resumable read session at the requested position and stop conditions.
    pub async fn connect_reader(
        &self,
        options: ReadOptions,
    ) -> Result<TsfReadSession, TsfClientError> {
        let ConnectedReadSocket {
            socket,
            stream_metadata,
        } = self.connect_read_socket(&options).await?;
        Ok(TsfReadSession::new(
            self.clone(),
            options,
            socket,
            stream_metadata,
        ))
    }

    /// Connects a resumable SSE reader.
    ///
    /// Private credentials stay in the bearer header. Reconnects reuse the original URL and send
    /// the latest versioned event cursor in `Last-Event-ID`. The REST request timeout bounds each
    /// opening handshake but not the established event body.
    pub async fn connect_sse_reader(
        &self,
        options: ReadOptions,
    ) -> Result<TsfSseReadSession, TsfClientError> {
        validate_read_options(&options)?;
        let request = self.sse_read_request(&options);
        let connection =
            self.open_sse_connection(&request, None)
                .await?
                .ok_or(TsfClientError::InvalidSse(
                    "initial read completed without stream_metadata",
                ))?;
        Ok(TsfSseReadSession {
            client: self.clone(),
            options,
            request,
            body: connection.body,
            parser: connection.parser,
            stream_metadata: connection.stream_metadata,
            last_caught_up: None,
            reconnect_attempts: 0,
            last_event: connection.resume_event,
            finished: false,
        })
    }

    fn sse_read_request(&self, options: &ReadOptions) -> SseReadRequest {
        let mut url = self.rest_url(format_args!("/streams/{}/records", options.stream_id));
        append_read_query(&mut url, options);
        let finite = options.stop.is_some_and(|stop| {
            stop.count.is_some() || stop.until_timestamp_ms.is_some() || stop.wait_seconds.is_some()
        });
        SseReadRequest {
            url,
            link_secret: options.link_secret.clone(),
            finite,
        }
    }

    async fn open_sse_connection(
        &self,
        request: &SseReadRequest,
        last_event: Option<&SseResumeEvent>,
    ) -> Result<Option<SseConnection>, TsfClientError> {
        let handshake_timeout = self.config.rest_request_timeout;
        self.retry_transient(|| async {
            with_timeout(
                handshake_timeout,
                "SSE handshake",
                self.open_sse_connection_once(request, last_event),
            )
            .await
        })
        .await
    }

    async fn open_sse_connection_once(
        &self,
        sse_request: &SseReadRequest,
        last_event: Option<&SseResumeEvent>,
    ) -> Result<Option<SseConnection>, TsfClientError> {
        let mut request = self.apply_rest_auth(
            self.http
                .get(sse_request.url.clone())
                .header("Accept", "text/event-stream"),
            sse_request.link_secret.as_ref(),
        );
        if let Some((event_id, _)) = last_event {
            request = request.header("Last-Event-ID", event_id.as_str());
        }
        let response = request.send().await?;
        if response.status() == StatusCode::NO_CONTENT {
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err(http_status_error(response, "read SSE").await);
        }
        let mut body: SseBody = Box::pin(response.bytes_stream());
        let mut parser = SseParser::default();
        let event =
            next_sse_event(&mut body, &mut parser)
                .await?
                .ok_or(TsfClientError::InvalidSse(
                    "response ended before stream_metadata",
                ))?;
        if event.event != "stream_metadata" {
            return Err(TsfClientError::InvalidSse(
                "first event is not stream_metadata",
            ));
        }
        let stream_metadata = serde_json::from_str(&event.data)
            .map_err(|_| TsfClientError::InvalidSse("invalid stream_metadata event"))?;
        let mut resume_event = None;
        if event.id.is_some() {
            let (event_id, cursor) = sse_resume_cursor(&event)?;
            resume_event = Some((event_id.to_owned(), cursor));
        }
        Ok(Some(SseConnection {
            body,
            parser,
            stream_metadata,
            resume_event,
        }))
    }

    async fn connect_read_socket(
        &self,
        options: &ReadOptions,
    ) -> Result<ConnectedReadSocket, TsfClientError> {
        validate_read_options(options)?;
        let opening_frame = ClientFrame::OpenRead {
            link_secret: options.link_secret.clone(),
        }
        .encode()?;
        let mut url = self.websocket_url(format_args!("/streams/{}/read", options.stream_id))?;
        append_read_query(&mut url, options);
        let connect_timeout = self.config.websocket_connect_timeout;
        let operation_timeout = self.config.websocket_operation_timeout;
        let read_idle_timeout = self.config.websocket_read_idle_timeout;
        self.retry_transient(|| {
            let url = url.clone();
            let opening_frame = opening_frame.clone();

            async move {
                let mut ws =
                    connect_websocket(url, connect_timeout, operation_timeout, opening_frame)
                        .await?;
                let stream_metadata = with_timeout(
                    operation_timeout,
                    "reader handshake",
                    expect_read_handshake(&mut ws),
                )
                .await?;

                Ok(ConnectedReadSocket {
                    socket: ReadSocket {
                        ws,
                        read_idle_timeout,
                    },
                    stream_metadata,
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
    ) -> reqwest::RequestBuilder {
        match link_secret {
            Some(secret) => request.bearer_auth(secret.expose_secret()),
            None => request,
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
        let attempts = retry_policy.max_attempts;

        for attempt in 1..=attempts {
            match run().await {
                Ok(value) => return Ok(value),
                Err(error) if attempt < attempts && should_retry(&error) => {
                    let delay = error
                        .retry_after()
                        .map(|delay| delay.min(retry_policy.max_backoff))
                        .unwrap_or_else(|| retry_policy.reconnect_delay(attempt - 1));
                    if !delay.is_zero() {
                        sleep(delay).await;
                    }
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

/// Sensitive recovery key for one logical creation request.
#[derive(Clone, Debug)]
pub struct IdempotencyKey(secrecy::SecretString);

impl IdempotencyKey {
    /// Generates a cryptographically random canonical 256-bit key.
    pub fn new_random() -> Self {
        Self(random_base64url_32().into())
    }
}

impl FromStr for IdempotencyKey {
    type Err = InvalidIdempotencyKey;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if is_canonical_base64url_32(value) {
            Ok(Self(value.into()))
        } else {
            Err(InvalidIdempotencyKey)
        }
    }
}

impl ExposeSecret<str> for IdempotencyKey {
    fn expose_secret(&self) -> &str {
        self.0.expose_secret()
    }
}

/// Error returned for a malformed idempotency key.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("idempotency key must be canonical 43-character unpadded base64url")]
pub struct InvalidIdempotencyKey;

/// Low-level authenticated write socket without retained-record recovery.
///
/// Records are manually numbered: the caller owns `writer_seq_num` assignment and contiguous
/// split-part layout via [`AppendRecord`] and
/// [`split_logical_record`](crate::transcript::split_logical_record). [`TsfWriter`] is the
/// actor-sequenced alternative with reconnect resend.
pub struct TsfWriteSession {
    ws: ClientWebSocket,
    operation_timeout: Duration,
}

/// Maximum accounted bytes that one writer socket may keep in flight.
///
/// Empty payloads count as one byte. [`TsfWriter`] paces sends to stay within this server-enforced
/// bound regardless of the configured retained backlog.
pub const MAX_WRITER_IN_FLIGHT_BYTES: usize = 5 * 1024 * 1024;
/// Maximum physical records that one writer socket may keep in flight.
///
/// This is a server-enforced bound independent of the configured retained backlog.
pub const MAX_WRITER_IN_FLIGHT_RECORDS: usize = 128;
/// Default maximum accounted bytes retained until durability acknowledgement.
pub const DEFAULT_MAX_WRITER_RETAINED_BYTES: usize = MAX_WRITER_IN_FLIGHT_BYTES;
/// Default maximum physical records retained until durability acknowledgement.
pub const DEFAULT_MAX_WRITER_RETAINED_RECORDS: usize = MAX_WRITER_IN_FLIGHT_RECORDS;

/// Stream, credentials, and sequence precondition for one durable [`TsfWriter`].
///
/// The writer generates a fresh client writer identity at connect and reuses it across its own
/// reconnects. Identities cannot be shared between writers: since the actor numbers every writer
/// from sequence zero, a reused identity would republish old sequence numbers, which logical
/// readers suppress as duplicates. Callers needing a persisted writer identity use the manually
/// numbered [`TsfWriteSession`] instead.
#[derive(Clone, Debug)]
pub struct DurableWriterOptions {
    /// Stream to append to.
    pub stream_id: StreamId,
    /// Secret from a write-capable stream link.
    pub link_secret: LinkSecret,
    /// Initial stream sequence precondition for this writer session.
    pub expected_next_seq_num: Option<u64>,
}

impl DurableWriterOptions {
    /// Creates writer options from an owned stream link secret.
    pub fn new(stream_id: StreamId, link_secret: impl Into<LinkSecret>) -> Self {
        Self {
            stream_id,
            link_secret: link_secret.into(),
            expected_next_seq_num: None,
        }
    }

    /// Requires the stream to start this writer session at the supplied sequence.
    pub fn with_expected_next_seq_num(mut self, expected_next_seq_num: u64) -> Self {
        self.expected_next_seq_num = Some(expected_next_seq_num);
        self
    }
}

/// Memory and concurrency bounds for [`TsfWriter`]'s retained backlog.
///
/// Every submitted batch is retained until durability acknowledgement, so a batch larger than
/// these bounds can never be admitted and is rejected at submission; size the backlog at least to
/// the largest submitted batch. Capacity is accounted per batch and released when the whole batch
/// is acknowledged. The writer paces protocol frames within [`MAX_WRITER_IN_FLIGHT_BYTES`] and
/// [`MAX_WRITER_IN_FLIGHT_RECORDS`] independently of this backlog.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TsfWriterConfig {
    /// Maximum accounted bytes retained until durability acknowledgement.
    ///
    /// Empty payloads count as one byte.
    pub max_retained_bytes: usize,
    /// Maximum number of records retained until durability acknowledgement.
    pub max_retained_records: usize,
}

impl TsfWriterConfig {
    fn validate(self) -> Result<Self, TsfClientError> {
        if self.max_retained_bytes == 0 || self.max_retained_bytes > Semaphore::MAX_PERMITS {
            return Err(TsfClientError::InvalidWriterConfig(format!(
                "max_retained_bytes must be between 1 and {}",
                Semaphore::MAX_PERMITS
            )));
        }
        if self.max_retained_records == 0 || self.max_retained_records > Semaphore::MAX_PERMITS {
            return Err(TsfClientError::InvalidWriterConfig(format!(
                "max_retained_records must be between 1 and {}",
                Semaphore::MAX_PERMITS
            )));
        }
        Ok(self)
    }
}

impl Default for TsfWriterConfig {
    fn default() -> Self {
        Self {
            max_retained_bytes: DEFAULT_MAX_WRITER_RETAINED_BYTES,
            max_retained_records: DEFAULT_MAX_WRITER_RETAINED_RECORDS,
        }
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

/// Future that resolves when every record of one submitted batch is durable or the writer
/// permanently fails.
///
/// The resolved receipts match the submitted batch one-to-one in submission order; because one
/// batch may span several protocol frames, the receipts may reference multiple acknowledgement
/// ranges.
pub struct AppendTicket {
    rx: oneshot::Receiver<Result<Vec<AppendReceipt>, TsfClientError>>,
    terminal_error: Arc<OnceLock<Arc<TsfClientError>>>,
}

impl AppendTicket {
    /// Polls for completed receipts without registering an async wakeup.
    ///
    /// Returns `None` while any record of the batch remains pending.
    pub fn try_recv(&mut self) -> Option<Result<Vec<AppendReceipt>, TsfClientError>> {
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
    type Output = Result<Vec<AppendReceipt>, TsfClientError>;

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

const WRITER_OPEN: u8 = 0;
const WRITER_CLOSING: u8 = 1;
const WRITER_DONE: u8 = 2;

/// State shared by the writer controller, producer handles, and the writer actor.
struct WriterShared {
    byte_permits: Arc<Semaphore>,
    record_permits: Arc<Semaphore>,
    terminal_error: Arc<OnceLock<Arc<TsfClientError>>>,
    config: TsfWriterConfig,
    state: AtomicU8,
    /// Serializes a permit's open check plus command send against the close transition, making
    /// that transition the single linearization point: a permit either enqueues its submission
    /// before the close command or observes the writer closed. Held only across synchronous
    /// sections, so the lock never crosses an await.
    submit_lock: RwLock<()>,
}

impl WriterShared {
    /// Marks the writer finished and closes both backlog semaphores, waking producers blocked in a
    /// reservation on every actor exit path, including abort before the task's first poll.
    fn shutdown(&self) {
        self.state.store(WRITER_DONE, Ordering::SeqCst);
        self.byte_permits.close();
        self.record_permits.close();
    }

    fn is_open(&self) -> bool {
        self.state.load(Ordering::SeqCst) == WRITER_OPEN
    }
}

struct ShutdownGuard(Arc<WriterShared>);

impl Drop for ShutdownGuard {
    fn drop(&mut self) {
        self.0.shutdown();
    }
}

/// Cloneable submission handle for one durable writer.
///
/// The writer actor assigns writer sequence numbers in channel order, so cloned producers can
/// submit concurrently without ever interleaving out of sequence: each [`AppendBatch`] occupies
/// one contiguous writer-sequence range, keeping the split parts of a logical record adjacent.
#[derive(Clone)]
pub struct TsfProducer {
    cmd_tx: mpsc::UnboundedSender<WriterCommand>,
    shared: Arc<WriterShared>,
}

impl fmt::Debug for TsfProducer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TsfProducer")
            .field("config", &self.shared.config)
            .field("open", &self.shared.is_open())
            .finish()
    }
}

impl TsfProducer {
    /// Waits for backlog capacity, submits the batch as one command, and returns its durability
    /// ticket.
    pub async fn submit(&self, batch: AppendBatch) -> Result<AppendTicket, TsfClientError> {
        let permit = self.reserve(&batch).await?;
        permit.submit(batch)
    }

    /// Reserves the batch's record slots and payload bytes in the retained backlog.
    ///
    /// The returned permit owns capacity until it is dropped or submitted.
    pub async fn reserve(&self, batch: &AppendBatch) -> Result<WritePermit, TsfClientError> {
        let records = batch.record_count();
        let bytes = submission_retained_bytes(batch);
        if records > self.shared.config.max_retained_records
            || bytes > self.shared.config.max_retained_bytes
        {
            return Err(TsfClientError::AppendBatchExceedsRetainedBacklog {
                records,
                bytes,
                max_retained_records: self.shared.config.max_retained_records,
                max_retained_bytes: self.shared.config.max_retained_bytes,
            });
        }
        if !self.shared.is_open() {
            return Err(self.closed_error());
        }

        let shared = Arc::clone(&self.shared);
        let record_permit = shared
            .record_permits
            .clone()
            .acquire_many_owned(records as u32)
            .await
            .map_err(|_| self.closed_error())?;
        let byte_permit = shared
            .byte_permits
            .clone()
            .acquire_many_owned(bytes as u32)
            .await
            .map_err(|_| self.closed_error())?;
        Ok(WritePermit {
            cmd_tx: self.cmd_tx.clone(),
            byte_permit,
            record_permit,
            shared,
            reserved_records: records,
            reserved_bytes: bytes,
        })
    }

    fn closed_error(&self) -> TsfClientError {
        terminal_writer_error(
            &self.shared.terminal_error,
            TsfClientError::AppendWriterClosed,
        )
    }
}

/// The server's queued-payload bound counts every record as at least one byte.
fn accounted_record_bytes(data: &Bytes) -> usize {
    data.len().max(1)
}

fn submission_retained_bytes(batch: &AppendBatch) -> usize {
    batch
        .payloads()
        .iter()
        .map(|record| accounted_record_bytes(&record.data))
        .sum()
}

/// Bounded durable writer that retains unacknowledged records and resends them across transient
/// interruptions.
///
/// This controller owns the writer task and is the only handle that can close the writer. Clone
/// [`TsfProducer`] handles from [`TsfWriter::producer`] for concurrent submissions.
pub struct TsfWriter {
    producer: TsfProducer,
    task: Option<JoinHandle<()>>,
}

impl fmt::Debug for TsfWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TsfWriter")
            .field("producer", &self.producer)
            .finish_non_exhaustive()
    }
}

impl TsfWriter {
    fn new(
        client: TsfClient,
        options: WriteStreamOptions,
        session: TsfWriteSession,
        config: TsfWriterConfig,
    ) -> Self {
        // Submit commands remain bounded by the retained-record semaphore. The unbounded channel
        // avoids a third, redundant capacity reservation and leaves room for the Close command.
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let shared = Arc::new(WriterShared {
            byte_permits: Arc::new(Semaphore::new(config.max_retained_bytes)),
            record_permits: Arc::new(Semaphore::new(config.max_retained_records)),
            terminal_error: Arc::new(OnceLock::new()),
            config,
            state: AtomicU8::new(WRITER_OPEN),
            submit_lock: RwLock::new(()),
        });
        let task = tokio::spawn(run_writer(
            client,
            options,
            session,
            cmd_rx,
            Arc::clone(&shared),
        ));

        Self {
            producer: TsfProducer { cmd_tx, shared },
            task: Some(task),
        }
    }

    /// Returns a cloneable submission handle for this writer.
    pub fn producer(&self) -> TsfProducer {
        self.producer.clone()
    }

    /// Waits for backlog capacity, submits the batch, and returns its durability ticket.
    pub async fn submit(&self, batch: AppendBatch) -> Result<AppendTicket, TsfClientError> {
        self.producer.submit(batch).await
    }

    /// Reserves the batch's backlog capacity; see [`TsfProducer::reserve`].
    pub async fn reserve(&self, batch: &AppendBatch) -> Result<WritePermit, TsfClientError> {
        self.producer.reserve(batch).await
    }

    /// Stops accepting records, waits for every pending durability acknowledgement, and joins the
    /// writer task.
    pub async fn close(mut self) -> Result<(), TsfClientError> {
        // The write guard waits out every in-flight permit submission, so any permit that still
        // observes the writer open has already enqueued its command ahead of Close.
        {
            let _guard = self
                .producer
                .shared
                .submit_lock
                .write()
                .expect("writer submit lock poisoned");
            self.producer
                .shared
                .state
                .fetch_max(WRITER_CLOSING, Ordering::SeqCst);
        }
        let (done_tx, mut done_rx) = oneshot::channel();
        self.producer
            .cmd_tx
            .send(WriterCommand::Close { done_tx })
            .map_err(|_| self.closed_error())?;

        if let Some(task) = self.task.take() {
            // A detached actor would idle on the command channel forever while producer clones
            // keep it open, so a cancelled close still aborts the task.
            let mut abort_guard = AbortOnDrop(Some(task));
            let joined = abort_guard.0.as_mut().expect("writer task").await;
            abort_guard.0 = None;
            joined.map_err(|error| TsfClientError::AppendWriterTaskFailed(error.to_string()))?;
        }

        done_rx.try_recv().map_err(|_| self.dropped_error())?
    }

    fn closed_error(&self) -> TsfClientError {
        self.producer.closed_error()
    }

    fn dropped_error(&self) -> TsfClientError {
        terminal_writer_error(
            &self.producer.shared.terminal_error,
            TsfClientError::AppendWriterDropped,
        )
    }
}

/// Aborts the held task unless it was joined first.
struct AbortOnDrop(Option<JoinHandle<()>>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        if let Some(task) = &self.0 {
            task.abort();
        }
    }
}

fn terminal_writer_error(
    terminal_error: &OnceLock<Arc<TsfClientError>>,
    fallback: TsfClientError,
) -> TsfClientError {
    retained_terminal_error(terminal_error).unwrap_or(fallback)
}

fn retained_terminal_error(
    terminal_error: &OnceLock<Arc<TsfClientError>>,
) -> Option<TsfClientError> {
    terminal_error
        .get()
        .map(|error| TsfClientError::AppendDurabilityUnknown(Arc::clone(error)))
}

impl Drop for TsfWriter {
    fn drop(&mut self) {
        self.producer.shared.shutdown();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

/// Owned capacity in a writer's record and byte backlogs.
///
/// Dropping an unused permit releases its capacity.
pub struct WritePermit {
    cmd_tx: mpsc::UnboundedSender<WriterCommand>,
    byte_permit: OwnedSemaphorePermit,
    record_permit: OwnedSemaphorePermit,
    shared: Arc<WriterShared>,
    reserved_records: usize,
    reserved_bytes: usize,
}

impl WritePermit {
    /// Submits a batch no larger than the reserved capacity without awaiting another backlog
    /// slot. Reservation capacity beyond the batch's size is released immediately.
    pub fn submit(mut self, batch: AppendBatch) -> Result<AppendTicket, TsfClientError> {
        if let Some(error) = retained_terminal_error(&self.shared.terminal_error) {
            return Err(error);
        }
        let records = batch.record_count();
        let bytes = submission_retained_bytes(&batch);
        if records > self.reserved_records || bytes > self.reserved_bytes {
            return Err(TsfClientError::AppendBatchExceedsReservation {
                records,
                bytes,
                reserved_records: self.reserved_records,
                reserved_bytes: self.reserved_bytes,
            });
        }
        if self.reserved_bytes > bytes {
            // split leaves this permit holding exactly the batch's charge.
            drop(self.byte_permit.split(self.reserved_bytes - bytes));
        }
        if self.reserved_records > records {
            drop(self.record_permit.split(self.reserved_records - records));
        }

        let (ack_tx, ack_rx) = oneshot::channel();
        let terminal_error = Arc::clone(&self.shared.terminal_error);
        {
            let _guard = self
                .shared
                .submit_lock
                .read()
                .expect("writer submit lock poisoned");
            if !self.shared.is_open() {
                return Err(terminal_writer_error(
                    &self.shared.terminal_error,
                    TsfClientError::AppendWriterClosed,
                ));
            }
            self.cmd_tx
                .send(WriterCommand::Submit {
                    batch,
                    ack_tx,
                    byte_permit: self.byte_permit,
                    record_permit: self.record_permit,
                })
                .map_err(|_| {
                    terminal_writer_error(&terminal_error, TsfClientError::AppendWriterClosed)
                })?;
        }

        Ok(AppendTicket {
            rx: ack_rx,
            terminal_error,
        })
    }
}

impl TsfWriteSession {
    /// Sends one physical record under the operation timeout.
    pub async fn send(&mut self, record: AppendRecord) -> Result<(), TsfClientError> {
        let operation_timeout = self.operation_timeout;

        with_timeout(operation_timeout, "send append frame", async move {
            self.buffer_batch(std::slice::from_ref(&record)).await?;
            self.flush().await
        })
        .await
    }

    /// Encodes one batch into the socket's write buffer, leaving the flush to the caller.
    async fn buffer_batch(&mut self, records: &[AppendRecord]) -> Result<(), TsfClientError> {
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
        let operation_timeout = self.operation_timeout;
        with_timeout(operation_timeout, "append acknowledgement", self.recv_ack()).await
    }

    /// Receives the next acknowledgement untimed; the writer actor drives its own absolute
    /// deadline so submission traffic cannot postpone it.
    async fn recv_ack(&mut self) -> Result<Option<AppendAck>, TsfClientError> {
        let frame = next_server_frame(&mut self.ws).await?;
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
        batch: AppendBatch,
        ack_tx: oneshot::Sender<Result<Vec<AppendReceipt>, TsfClientError>>,
        byte_permit: OwnedSemaphorePermit,
        record_permit: OwnedSemaphorePermit,
    },
    Close {
        done_tx: oneshot::Sender<Result<(), TsfClientError>>,
    },
}

/// One actor-admitted batch: one contiguous writer-sequence range starting at `start_seq_num`,
/// receipts accumulated until the whole batch is acknowledged, and the backlog permits released
/// only at completion.
struct PendingSubmission {
    payloads: Vec<RecordPayload>,
    /// Writer sequence number of `payloads[0]`; part `index` carries `start_seq_num + index`.
    start_seq_num: u64,
    /// Leading records acknowledged on the current or a previous connection.
    acked: usize,
    /// Leading records sent on the current connection. `acked <= sent` always holds.
    sent: usize,
    receipts: Vec<AppendReceipt>,
    ack_tx: oneshot::Sender<Result<Vec<AppendReceipt>, TsfClientError>>,
    _byte_permit: OwnedSemaphorePermit,
    _record_permit: OwnedSemaphorePermit,
}

impl PendingSubmission {
    /// Writer sequence numbers still awaiting acknowledgement, in order.
    fn unacknowledged_range(&self) -> Range<u64> {
        self.start_seq_num + self.acked as u64..self.start_seq_num + self.payloads.len() as u64
    }

    /// Numbers the record at `index`. Payloads are reference-counted, so this clones a handle.
    fn record(&self, index: usize) -> AppendRecord {
        let payload = &self.payloads[index];
        AppendRecord {
            writer_seq_num: self.start_seq_num + index as u64,
            part: payload.part,
            format: payload.format,
            data: payload.data.clone(),
        }
    }
}

/// Sent-but-unacknowledged state for one connection: the window hard-bounded by the server
/// socket limits, plus the deadline for the acknowledgement that reopens it.
///
/// A reconnect replaces the whole value, so a deadline can never outlive the socket it was armed
/// for.
#[derive(Default)]
struct InFlightWindow {
    bytes: usize,
    records: usize,
    /// Absolute: submissions can fill the backlog but never postpone it, so it measures time
    /// without durability progress rather than command-channel inactivity.
    ack_deadline: Option<Instant>,
}

impl InFlightWindow {
    /// Arms the deadline for the first records to reach the wire; an armed deadline is never
    /// pushed back by later sends.
    fn arm(&mut self, operation_timeout: Duration) {
        self.ack_deadline
            .get_or_insert_with(|| Instant::now() + operation_timeout);
    }

    /// Restarts the deadline after durability progress, disarming once the window drains.
    fn restart(&mut self, operation_timeout: Duration) {
        self.ack_deadline = (self.records > 0).then(|| Instant::now() + operation_timeout);
    }
}

/// Actor-owned writer-sequence cursor; the single source of writer sequence numbers.
///
/// Numbering at admission keeps queue order and sequence order identical regardless of producer
/// concurrency.
#[derive(Default)]
struct WriterCursor {
    next_seq_num: u64,
}

impl WriterCursor {
    /// Reserves the next contiguous range of `count` sequence numbers and returns its start.
    fn reserve(&mut self, count: usize) -> Result<u64, TsfClientError> {
        // The exclusive end of the range is an ack boundary and must stay representable.
        let end = self
            .next_seq_num
            .checked_add(count as u64)
            .ok_or(FrameCodecError::WriterSequenceExhausted)?;
        Ok(std::mem::replace(&mut self.next_seq_num, end))
    }
}

async fn run_writer(
    client: TsfClient,
    options: WriteStreamOptions,
    mut session: TsfWriteSession,
    mut cmd_rx: mpsc::UnboundedReceiver<WriterCommand>,
    shared: Arc<WriterShared>,
) {
    // Wakes producers blocked on either backlog semaphore on every exit path, including abort.
    let _shutdown_guard = ShutdownGuard(Arc::clone(&shared));
    let mut pending: VecDeque<PendingSubmission> = VecDeque::new();
    let mut close_tx: Option<oneshot::Sender<Result<(), TsfClientError>>> = None;
    let mut reconnect_attempts = 0;
    let mut cursor = WriterCursor::default();
    let mut in_flight = InFlightWindow::default();
    // Reused across frames; one frame holds at most MAX_APPEND_FRAME_RECORDS records.
    let mut frame: Vec<AppendRecord> = Vec::new();

    loop {
        if let Err(error) =
            send_pending(&mut session, &mut pending, &mut in_flight, &mut frame).await
        {
            match recover_pending_appends(
                &mut session,
                &client,
                &options,
                &mut pending,
                &mut in_flight,
                &mut reconnect_attempts,
                error,
            )
            .await
            {
                // Resend the retained backlog on the fresh session at the loop top.
                Ok(()) => continue,
                Err(error) => {
                    finish_writer_error(&mut pending, &mut close_tx, &shared.terminal_error, error);
                    return;
                }
            }
        }

        if close_tx.is_some() && pending.is_empty() {
            if let Some(close_tx) = close_tx.take() {
                let _ = close_tx.send(Ok(()));
            }
            return;
        }

        tokio::select! {
            cmd = cmd_rx.recv(), if close_tx.is_none() => {
                match cmd {
                    Some(command) => {
                        if let Err(error) = drain_submissions(
                            &mut pending,
                            &mut cmd_rx,
                            &mut close_tx,
                            &mut cursor,
                            command,
                        ) {
                            finish_writer_error(
                                &mut pending,
                                &mut close_tx,
                                &shared.terminal_error,
                                error,
                            );
                            return;
                        }
                    }
                    None => {
                        fail_pending(
                            &mut pending,
                            &Arc::new(TsfClientError::AppendWriterDropped),
                        );
                        return;
                    }
                }
            }

            ack = next_ack(&mut session, in_flight.ack_deadline), if in_flight.records > 0 => {
                let recover_from = match ack {
                    Ok(Some(ack)) => {
                        if let Err(error) = dispatch_ack(ack, &mut pending, &mut in_flight) {
                            finish_writer_error(
                                &mut pending,
                                &mut close_tx,
                                &shared.terminal_error,
                                error,
                            );
                            return;
                        }
                        reconnect_attempts = 0;
                        in_flight.restart(session.operation_timeout);
                        None
                    }
                    Ok(None) => Some(TsfClientError::WebSocketClosed),
                    Err(error) => Some(error),
                };
                if let Some(error) = recover_from
                    && let Err(error) = recover_pending_appends(
                        &mut session,
                        &client,
                        &options,
                        &mut pending,
                        &mut in_flight,
                        &mut reconnect_attempts,
                        error,
                    )
                    .await
                {
                    finish_writer_error(&mut pending, &mut close_tx, &shared.terminal_error, error);
                    return;
                }
            }
        }
    }
}

/// Waits for the next acknowledgement, failing at the in-flight window's absolute deadline.
///
/// The actor loop drops this future whenever another `select!` branch wins; a `WebSocketStream`
/// buffers partial frames internally, so cancelling the read cannot lose an acknowledgement.
async fn next_ack(
    session: &mut TsfWriteSession,
    deadline: Option<Instant>,
) -> Result<Option<AppendAck>, TsfClientError> {
    let Some(deadline) = deadline else {
        return session.recv_ack().await;
    };
    timeout_at(deadline, session.recv_ack())
        .await
        .map_err(|_| TsfClientError::Timeout {
            operation: "append acknowledgement",
        })?
}

/// Moves the submitted batch and every already-queued submission into `pending`, numbering each
/// from the actor's cursor.
///
/// This never awaits, so a batch is fully retained before any I/O can fail: a failed or timed-out
/// write leaves every record in `pending` for reconnect resend.
fn drain_submissions(
    pending: &mut VecDeque<PendingSubmission>,
    cmd_rx: &mut mpsc::UnboundedReceiver<WriterCommand>,
    close_tx: &mut Option<oneshot::Sender<Result<(), TsfClientError>>>,
    cursor: &mut WriterCursor,
    first: WriterCommand,
) -> Result<(), TsfClientError> {
    let mut command = Some(first);

    while let Some(WriterCommand::Submit {
        batch,
        ack_tx,
        byte_permit,
        record_permit,
    }) = command
    {
        let payloads = batch.into_payloads();
        let start_seq_num = cursor.reserve(payloads.len())?;
        pending.push_back(PendingSubmission {
            receipts: Vec::with_capacity(payloads.len()),
            payloads,
            start_seq_num,
            acked: 0,
            sent: 0,
            ack_tx,
            _byte_permit: byte_permit,
            _record_permit: record_permit,
        });
        command = cmd_rx.try_recv().ok();
    }

    if let Some(WriterCommand::Close { done_tx }) = command {
        *close_tx = Some(done_tx);
    }
    Ok(())
}

/// Fills `frame` with the leading unsent records and charges the in-flight window for them, bounded by
/// that window and by the protocol-frame limits.
///
/// Payloads are reference-counted, so framing clones only handles. Charging before the write is
/// safe: a failed send is followed by a reconnect that resets every marker.
fn take_frame(
    pending: &mut VecDeque<PendingSubmission>,
    in_flight: &mut InFlightWindow,
    frame: &mut Vec<AppendRecord>,
) {
    frame.clear();
    let mut frame_bytes = 0;
    for submission in pending.iter_mut() {
        while let Some(payload) = submission.payloads.get(submission.sent) {
            let accounted = accounted_record_bytes(&payload.data);
            if in_flight.records == MAX_WRITER_IN_FLIGHT_RECORDS
                || in_flight.bytes + accounted > MAX_WRITER_IN_FLIGHT_BYTES
                || frame.len() == MAX_APPEND_FRAME_RECORDS
                || (!frame.is_empty() && payload.data.len() > MAX_FRAME_PAYLOAD_BYTES - frame_bytes)
            {
                return;
            }
            in_flight.bytes += accounted;
            in_flight.records += 1;
            frame_bytes += payload.data.len();
            frame.push(submission.record(submission.sent));
            submission.sent += 1;
        }
    }
}

/// Sends unsent retained records under the in-flight window, pacing around full windows; acks observed
/// by the actor loop reopen capacity.
async fn send_pending(
    session: &mut TsfWriteSession,
    pending: &mut VecDeque<PendingSubmission>,
    in_flight: &mut InFlightWindow,
    frame: &mut Vec<AppendRecord>,
) -> Result<(), TsfClientError> {
    let operation_timeout = session.operation_timeout;

    with_timeout(operation_timeout, "send append frames", async move {
        let mut fed = false;
        loop {
            take_frame(pending, in_flight, frame);
            if frame.is_empty() {
                break;
            }
            session.buffer_batch(frame).await?;
            fed = true;
        }
        if fed {
            session.flush().await?;
            in_flight.arm(operation_timeout);
        }
        Ok(())
    })
    .await
}

/// Reconnects the write session and marks every unacknowledged record for paced resend.
///
/// The fresh socket carries no in-flight records, so the whole in-flight window is replaced —
/// including the acknowledgement deadline armed for the dead socket — and every sent marker
/// rewinds to its acknowledged prefix; the loop top resends the backlog under the window.
async fn recover_pending_appends(
    session: &mut TsfWriteSession,
    client: &TsfClient,
    options: &WriteStreamOptions,
    pending: &mut VecDeque<PendingSubmission>,
    in_flight: &mut InFlightWindow,
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
            Ok(connected) => {
                *session = connected;
                for submission in pending.iter_mut() {
                    submission.sent = submission.acked;
                }
                *in_flight = InFlightWindow::default();
                return Ok(());
            }
            Err(next_error) if next_error.is_retryable() => error = next_error,
            Err(next_error) => return Err(next_error),
        }
    }

    Err(error)
}

fn dispatch_ack(
    ack: AppendAck,
    pending: &mut VecDeque<PendingSubmission>,
    in_flight: &mut InFlightWindow,
) -> Result<(), TsfClientError> {
    let record_count =
        usize::try_from(ack.record_count()?).map_err(|_| TsfClientError::InvalidAppendAck(ack))?;
    // The service can only acknowledge records this connection actually sent.
    if record_count > in_flight.records {
        return Err(TsfClientError::InvalidAppendAck(ack));
    }

    // Validate the whole range before mutating any submission.
    let mut expected_seq_num = ack.writer_start_seq_num;
    for submission in pending.iter() {
        let range = submission.unacknowledged_range();
        if range.is_empty() {
            continue;
        }
        if range.start < expected_seq_num {
            return Err(TsfClientError::AppendNotAcknowledged {
                writer_seq_num: range.start,
                ack,
            });
        }
        if range.start > expected_seq_num {
            return Err(TsfClientError::InvalidAppendAck(ack));
        }
        if ack.writer_end_seq_num <= range.end {
            expected_seq_num = ack.writer_end_seq_num;
            break;
        }
        expected_seq_num = range.end;
    }
    if expected_seq_num != ack.writer_end_seq_num {
        return Err(TsfClientError::InvalidAppendAck(ack));
    }

    let mut writer_seq_nums = ack.writer_start_seq_num..ack.writer_end_seq_num;
    let mut seq_nums = ack.start_seq_num..ack.end_seq_num;
    let mut remaining = record_count;
    while remaining > 0 {
        let submission = pending.front_mut().expect("ack validated against pending");
        while submission.acked < submission.payloads.len() && remaining > 0 {
            in_flight.bytes -= accounted_record_bytes(&submission.payloads[submission.acked].data);
            in_flight.records -= 1;
            submission.receipts.push(AppendReceipt {
                writer_seq_num: writer_seq_nums.next().expect("ack validated"),
                seq_num: seq_nums.next().expect("ack validated"),
                ack,
            });
            submission.acked += 1;
            remaining -= 1;
        }
        // The in-flight window bounds the ack to records this connection sent, so an acknowledged
        // prefix can never outrun the sent prefix.
        debug_assert!(submission.acked <= submission.sent);
        if submission.acked == submission.payloads.len() {
            let submission = pending.pop_front().expect("front submission");
            let _ = submission.ack_tx.send(Ok(submission.receipts));
        }
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
    pending: &mut VecDeque<PendingSubmission>,
    close_tx: &mut Option<oneshot::Sender<Result<(), TsfClientError>>>,
    terminal_error: &OnceLock<Arc<TsfClientError>>,
    error: TsfClientError,
) {
    let error = Arc::new(error);
    let _ = terminal_error.set(Arc::clone(&error));
    fail_pending(pending, &error);
    if let Some(close_tx) = close_tx.take() {
        let _ = close_tx.send(Err(TsfClientError::AppendDurabilityUnknown(error)));
    }
}

fn fail_pending(pending: &mut VecDeque<PendingSubmission>, error: &Arc<TsfClientError>) {
    while let Some(submission) = pending.pop_front() {
        let _ = submission
            .ack_tx
            .send(Err(TsfClientError::AppendDurabilityUnknown(Arc::clone(
                error,
            ))));
    }
}

/// Streaming body for one SSE connection.
type SseBody = Pin<Box<dyn futures_util::Stream<Item = Result<Bytes, reqwest::Error>> + Send>>;

struct ParsedSseEvent {
    event: String,
    data: String,
    id: Option<String>,
}

/// Versioned resume cursor as sent in `Last-Event-ID`, paired with its parsed form.
type SseResumeEvent = (String, ParsedSseResumeCursor);

struct SseConnection {
    body: SseBody,
    parser: SseParser,
    stream_metadata: StreamMetadata,
    resume_event: Option<SseResumeEvent>,
}

struct SseReadRequest {
    url: Url,
    link_secret: Option<LinkSecret>,
    finite: bool,
}

/// Resumable HTTP event-stream reader.
///
/// Transient transport and service interruptions reconnect from the next sequence number. Normal
/// completion and configured stop conditions return `None`; protocol and policy failures surface
/// as errors.
pub struct TsfSseReadSession {
    client: TsfClient,
    options: ReadOptions,
    request: SseReadRequest,
    body: SseBody,
    parser: SseParser,
    stream_metadata: StreamMetadata,
    last_caught_up: Option<CaughtUpPosition>,
    reconnect_attempts: usize,
    last_event: Option<SseResumeEvent>,
    finished: bool,
}

impl TsfSseReadSession {
    /// Returns authorized stream metadata from the opening event.
    pub fn stream_metadata(&self) -> &StreamMetadata {
        &self.stream_metadata
    }

    /// Returns the most recent reconnect-safe caught-up position.
    pub fn last_caught_up(&self) -> Option<CaughtUpPosition> {
        self.last_caught_up
    }

    fn resume_cursors(
        &self,
        event: &ParsedSseEvent,
    ) -> Result<(ParsedSseResumeCursor, Option<ParsedSseResumeCursor>), TsfClientError> {
        Ok((
            sse_resume_cursor(event)?.1,
            self.last_event.as_ref().map(|(_, cursor)| *cursor),
        ))
    }

    /// Returns the next record batch, reconnecting from the last safe absolute cursor when needed.
    ///
    /// The session advances past the whole batch on return: records the caller does not consume
    /// are not redelivered, including after a reconnect. Process or retain every needed record
    /// from each batch.
    pub async fn next_batch(&mut self) -> Result<Option<ReadBatch>, TsfClientError> {
        loop {
            if self.finished || read_options_exhausted(&self.options) {
                return Ok(None);
            }
            let event = match next_sse_event(&mut self.body, &mut self.parser).await {
                Ok(None) if self.request.finite => {
                    self.finished = true;
                    return Ok(None);
                }
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
                    .open_sse_connection(&self.request, self.last_event.as_ref())
                    .await?
                else {
                    self.finished = true;
                    return Ok(None);
                };
                if connection.resume_event.is_some() {
                    self.last_event = connection.resume_event;
                }
                self.body = connection.body;
                self.parser = connection.parser;
                self.stream_metadata = connection.stream_metadata;
                // A handshake alone is not progress. A read_batch or caught_up event resets the
                // counter below.
                continue;
            };
            match event.event.as_str() {
                "read_batch" => {
                    let batch: SseReadBatchData = serde_json::from_str(&event.data)
                        .map_err(|_| TsfClientError::InvalidSse("invalid read_batch event"))?;
                    let batch = sse_read_batch(batch)?;
                    validate_sse_read_batch(&batch, &self.options)?;
                    let (cursor, previous) = self.resume_cursors(&event)?;
                    validate_sse_read_batch_cursor(&batch, cursor, previous, &self.options)?;
                    self.last_event = event.id.map(|id| (id, cursor));
                    self.reconnect_attempts = 0;
                    self.finished = advance_read_options_for_batch(&mut self.options, &batch);
                    return Ok(Some(batch));
                }
                "caught_up" => {
                    let value: SseCaughtUpData = serde_json::from_str(&event.data)
                        .map_err(|_| TsfClientError::InvalidSse("invalid caught_up event"))?;
                    let caught_up = CaughtUpPosition {
                        next_seq_num: value.next_seq_num,
                        last_timestamp_ms: value.last_timestamp_ms,
                    };
                    let (cursor, previous) = self.resume_cursors(&event)?;
                    validate_sse_caught_up_cursor(caught_up, cursor, previous, &self.options)?;
                    self.last_event = event.id.map(|id| (id, cursor));
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
/// completion and configured stop conditions return `None`; protocol and policy failures surface
/// as errors.
pub struct TsfReadSession {
    client: TsfClient,
    options: ReadOptions,
    socket: ReadSocket,
    stream_metadata: StreamMetadata,
    finished: bool,
    last_caught_up: Option<CaughtUpPosition>,
    no_progress_reconnects: usize,
    reconnect_needed: bool,
}

impl TsfReadSession {
    fn new(
        client: TsfClient,
        options: ReadOptions,
        socket: ReadSocket,
        stream_metadata: StreamMetadata,
    ) -> Self {
        Self {
            client,
            options,
            socket,
            stream_metadata,
            finished: false,
            last_caught_up: None,
            no_progress_reconnects: 0,
            reconnect_needed: false,
        }
    }

    /// Returns the latest reconnect-safe position reported after preceding records were delivered.
    pub const fn last_caught_up(&self) -> Option<CaughtUpPosition> {
        self.last_caught_up
    }

    /// Returns metadata supplied by the latest successful read handshake.
    pub const fn stream_metadata(&self) -> &StreamMetadata {
        &self.stream_metadata
    }

    /// Waits for the next record batch using the configured idle timeout.
    ///
    /// The session advances past the whole batch on return: records the caller does not consume
    /// are not redelivered, including after a reconnect. Process or retain every needed record
    /// from each batch.
    pub async fn next_batch(&mut self) -> Result<Option<ReadBatch>, TsfClientError> {
        loop {
            if self.finished || read_options_exhausted(&self.options) {
                self.finished = true;
                return Ok(None);
            }
            if self.reconnect_needed {
                self.reconnect().await?;
            }

            match self.socket.next_outcome().await {
                Ok(ReadSocketOutcome::Records(batch)) => {
                    validate_read_batch_for_request(&batch, &self.options)?;
                    self.batch_delivered(&batch);
                    return Ok(Some(batch));
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

    /// Waits for the next record batch with a caller-supplied timeout for this operation.
    pub async fn next_batch_with_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<ReadBatch>, TsfClientError> {
        with_timeout(timeout, "read stream batch", self.next_batch()).await
    }

    async fn reconnect(&mut self) -> Result<(), TsfClientError> {
        debug_assert!(self.reconnect_needed);
        let delay = self
            .client
            .config
            .retry_policy
            .reconnect_delay(self.no_progress_reconnects.saturating_sub(1));
        if !delay.is_zero() {
            sleep(delay).await;
        }
        let ConnectedReadSocket {
            socket,
            stream_metadata,
        } = self.client.connect_read_socket(&self.options).await?;
        self.socket = socket;
        self.stream_metadata = stream_metadata;
        self.no_progress_reconnects = 0;
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
        self.reconnect_needed = true;
        Ok(())
    }

    fn batch_delivered(&mut self, batch: &ReadBatch) {
        self.no_progress_reconnects = 0;
        self.reconnect_needed = false;
        self.finished = advance_read_options_for_batch(&mut self.options, batch);
    }
}

fn advance_read_options_for_batch(options: &mut ReadOptions, batch: &ReadBatch) -> bool {
    let last = batch.last();
    advance_read_options(options, last.seq_num, batch.record_count())
}

fn advance_read_options(options: &mut ReadOptions, last_seq_num: u64, record_count: usize) -> bool {
    let Some(next_seq_num) = last_seq_num.checked_add(1) else {
        return true;
    };
    options.start = Some(ReadStart::SeqNum(next_seq_num));
    if let Some(remaining) = options.stop.as_mut().and_then(|stop| stop.count.as_mut()) {
        *remaining = remaining.saturating_sub(record_count as u64);
    }
    options.stop.is_some_and(|stop| stop.count == Some(0))
}

fn read_options_exhausted(options: &ReadOptions) -> bool {
    let stop = options.stop.unwrap_or_default();
    stop.count == Some(0)
        || matches!(
            (options.start, stop.until_timestamp_ms),
            (Some(ReadStart::TimestampMs(start)), Some(until)) if start >= until
        )
}

fn validate_read_batch_for_request(
    batch: &ReadBatch,
    options: &ReadOptions,
) -> Result<(), TsfClientError> {
    if let Some(message) = read_batch_start_violation(batch, options.start)
        .or_else(|| read_batch_stop_violation(batch, options))
    {
        return Err(TsfClientError::InvalidReadResponse(message));
    }
    Ok(())
}

fn read_batch_start_violation(batch: &ReadBatch, start: Option<ReadStart>) -> Option<&'static str> {
    let first = batch.first();
    match start {
        Some(ReadStart::SeqNum(seq_num)) if first.seq_num != seq_num => {
            Some("read batch does not begin at the requested sequence")
        }
        Some(ReadStart::TimestampMs(timestamp_ms)) if first.timestamp_ms < timestamp_ms => {
            Some("read batch begins before the requested timestamp")
        }
        _ => None,
    }
}

fn read_batch_stop_violation(batch: &ReadBatch, options: &ReadOptions) -> Option<&'static str> {
    let stop = options.stop.unwrap_or_default();
    if stop
        .count
        .is_some_and(|remaining| batch.record_count() as u64 > remaining)
    {
        return Some("read batch exceeds the remaining record count");
    }
    if stop
        .until_timestamp_ms
        .is_some_and(|until| batch.iter().any(|record| record.timestamp_ms >= until))
    {
        return Some("read batch reaches the exclusive until timestamp");
    }
    None
}

fn validate_caught_up_for_request(
    caught_up: CaughtUpPosition,
    options: &ReadOptions,
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
}

struct ConnectedReadSocket {
    socket: ReadSocket,
    stream_metadata: StreamMetadata,
}

impl ReadSocket {
    async fn next_outcome(&mut self) -> Result<ReadSocketOutcome, TsfClientError> {
        loop {
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
    Records(ReadBatch),
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

fn validate_read_options(options: &ReadOptions) -> Result<(), TsfClientError> {
    let selector = match options
        .start
        .unwrap_or(ReadStart::TailOffset(DEFAULT_READ_TAIL_OFFSET))
    {
        ReadStart::SeqNum(value) | ReadStart::TimestampMs(value) | ReadStart::TailOffset(value) => {
            value
        }
    };
    if selector > MAX_SAFE_INTEGER_U64 {
        return Err(TsfClientError::InvalidReadSelector {
            value: selector,
            maximum: MAX_SAFE_INTEGER_U64,
        });
    }
    let stop = options.stop.unwrap_or_default();
    if let Some(value) = stop.until_timestamp_ms
        && value > MAX_SAFE_INTEGER_U64
    {
        return Err(TsfClientError::InvalidReadSelector {
            value,
            maximum: MAX_SAFE_INTEGER_U64,
        });
    }
    if let Some(value) = stop.wait_seconds
        && value > MAX_READ_WAIT_SECONDS
    {
        return Err(TsfClientError::InvalidReadWait {
            value,
            maximum: MAX_READ_WAIT_SECONDS,
        });
    }
    if let Some(value) = options.rate {
        if !value.is_finite() || !(MIN_PLAYBACK_RATE..=MAX_PLAYBACK_RATE).contains(&value) {
            return Err(TsfClientError::InvalidPlaybackRate {
                value,
                minimum: MIN_PLAYBACK_RATE,
                maximum: MAX_PLAYBACK_RATE,
            });
        }
        if stop.count.is_none() && stop.until_timestamp_ms.is_none() && stop.wait_seconds != Some(0)
        {
            return Err(TsfClientError::PlaybackRequiresCountUntilOrWaitZero);
        }
    }
    Ok(())
}

fn append_read_query(url: &mut Url, options: &ReadOptions) {
    let mut query = url.query_pairs_mut();
    match options.start {
        Some(ReadStart::SeqNum(value)) => {
            query.append_pair("seq_num", &value.to_string());
        }
        Some(ReadStart::TimestampMs(value)) => {
            query.append_pair("timestamp", &value.to_string());
        }
        Some(ReadStart::TailOffset(value)) => {
            query.append_pair("tail_offset", &value.to_string());
        }
        None => {
            query.append_pair("tail_offset", &DEFAULT_READ_TAIL_OFFSET.to_string());
        }
    }
    let stop = options.stop.unwrap_or_default();
    if let Some(value) = stop.count {
        query.append_pair("count", &value.to_string());
    }
    if let Some(value) = stop.until_timestamp_ms {
        query.append_pair("until", &value.to_string());
    }
    if let Some(value) = options.rate {
        query.append_pair("rate", &value.to_string());
    }
    if let Some(value) = stop.wait_seconds {
        query.append_pair("wait", &value.to_string());
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
            // `validate_new_bytes` proved no terminator exists past `tail_start`, so scanning
            // stops there instead of re-walking the unterminated tail on every push.
            let Some((index, length)) = sse_boundary(&self.buffer[self.offset..self.tail_start])
            else {
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
    let Some(next_seq_num) = fields.next().and_then(parse_canonical_decimal_u64) else {
        return Err(invalid_sse_resume_cursor());
    };
    let Some(consumed_count) = fields.next().and_then(parse_canonical_decimal_u64) else {
        return Err(invalid_sse_resume_cursor());
    };
    if fields.next().is_some()
        || next_seq_num > MAX_SAFE_INTEGER_U64
        || consumed_count > next_seq_num
    {
        return Err(invalid_sse_resume_cursor());
    }
    Ok(ParsedSseResumeCursor {
        next_seq_num,
        consumed_records: consumed_count,
    })
}

fn invalid_sse_resume_cursor() -> TsfClientError {
    TsfClientError::InvalidSse("SSE event does not carry a valid resume cursor")
}

/// Per-byte JSON escaped length matching serde_json's ESCAPE table: '"', '\\', and the
/// five short-form controls take two bytes, the rest of C0 takes six (\u00XX), and every
/// other byte passes through unescaped.
const JSON_ESCAPED_LEN: [u8; 256] = {
    let mut table = [1u8; 256];
    let mut control = 0;
    while control < 0x20 {
        table[control] = 6;
        control += 1;
    }
    table[b'"' as usize] = 2;
    table[b'\\' as usize] = 2;
    table[b'\x08' as usize] = 2; // \b
    table[b'\t' as usize] = 2;
    table[b'\n' as usize] = 2;
    table[b'\x0C' as usize] = 2; // \f
    table[b'\r' as usize] = 2;
    table
};

fn compact_record_data(bytes: &[u8]) -> RecordData {
    let Ok(text) = std::str::from_utf8(bytes) else {
        return RecordData::Base64url(URL_SAFE_NO_PAD.encode(bytes));
    };
    let escaped_len = bytes.iter().fold(0usize, |total, byte| {
        total + JSON_ESCAPED_LEN[*byte as usize] as usize
    });
    let utf8_len = br#"{"encoding":"utf8","value":""}"#.len() + escaped_len;
    let base64url_len =
        br#"{"encoding":"base64url","value":""}"#.len() + bytes.len().saturating_mul(4).div_ceil(3);
    if utf8_len <= base64url_len {
        RecordData::Utf8(text.to_owned())
    } else {
        RecordData::Base64url(URL_SAFE_NO_PAD.encode(bytes))
    }
}

fn sse_read_batch(batch: SseReadBatchData) -> Result<ReadBatch, TsfClientError> {
    let mut records = Vec::with_capacity(batch.records.len());
    for record in batch.records {
        let mut writer = [0u8; WriterId::BYTE_LEN];
        let decoded_len = URL_SAFE_NO_PAD
            .decode_slice(record.writer_id, &mut writer)
            .map_err(|_| TsfClientError::InvalidSse("invalid writer_id"))?;
        if decoded_len != WriterId::BYTE_LEN {
            return Err(TsfClientError::InvalidSse("invalid writer_id length"));
        }
        let data = match record.data {
            RecordData::Utf8(value) => Bytes::from(value.into_bytes()),
            RecordData::Base64url(value) => Bytes::from(
                URL_SAFE_NO_PAD
                    .decode(value)
                    .map_err(|_| TsfClientError::InvalidSse("invalid record base64url"))?,
            ),
        };
        records.push(OwnedReadRecord {
            seq_num: record.seq_num,
            timestamp_ms: record.timestamp_ms,
            writer_id: WriterId::from_bytes(writer),
            writer_seq_num: record.writer_seq_num,
            part: PartHeader::new(record.part.index, record.part.is_final)?,
            format: record.format,
            data,
        });
    }
    // Construction enforces the shared bounded-batch invariant: record count, per-record and
    // aggregate payload sizes, and contiguous sequence numbers.
    ReadBatch::try_from_records(records).map_err(|error| match error {
        FrameCodecError::InvalidBatchRecordCount { .. } => {
            TsfClientError::InvalidSse("read_batch record count is outside the protocol limit")
        }
        FrameCodecError::RecordTooLarge { .. } => {
            TsfClientError::InvalidSse("read_batch contains an oversized record")
        }
        FrameCodecError::BatchPayloadTooLarge { .. } => {
            TsfClientError::InvalidSse("read_batch exceeds the decoded payload limit")
        }
        FrameCodecError::NonContiguousReadBatch => {
            TsfClientError::InvalidSse("read_batch sequence numbers are not contiguous")
        }
        other => TsfClientError::Frame(other),
    })
}

fn validate_sse_read_batch(batch: &ReadBatch, options: &ReadOptions) -> Result<(), TsfClientError> {
    // Construction upholds batch shape; only conformance to this request remains. The start
    // position is checked against the resume cursor in `validate_sse_read_batch_cursor`.
    match read_batch_stop_violation(batch, options) {
        Some(message) => Err(TsfClientError::InvalidSse(message)),
        None => Ok(()),
    }
}

fn validate_sse_read_batch_cursor(
    batch: &ReadBatch,
    cursor: ParsedSseResumeCursor,
    previous: Option<ParsedSseResumeCursor>,
    options: &ReadOptions,
) -> Result<(), TsfClientError> {
    let first = batch.first();
    let Some(expected_next_seq_num) = batch.last().seq_num.checked_add(1) else {
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
        && let Some(message) = read_batch_start_violation(batch, options.start)
    {
        return Err(TsfClientError::InvalidSse(message));
    }
    let expected_consumed = previous
        .map_or(0, |value| value.consumed_records)
        .checked_add(batch.record_count() as u64)
        .ok_or(TsfClientError::InvalidSse(
            "read_batch consumed count overflowed",
        ))?;
    if cursor.consumed_records != expected_consumed {
        return Err(TsfClientError::InvalidSse(
            "read_batch cursor has the wrong consumed count",
        ));
    }
    Ok(())
}

fn validate_sse_caught_up_cursor(
    caught_up: CaughtUpPosition,
    cursor: ParsedSseResumeCursor,
    previous: Option<ParsedSseResumeCursor>,
    options: &ReadOptions,
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
    // The same server condition surfaces as one typed error on every plane; the WebSocket
    // paths map the matching close reason in close_reason_error.
    if api_code.as_deref() == Some("sequence_mismatch") {
        return TsfClientError::SequenceMismatch {
            actual_next_seq_num,
            request_id,
            retry_after,
        };
    }
    let body = parsed
        .as_ref()
        .and_then(|response| api_error_message(&response.error))
        .unwrap_or_else(|| String::from_utf8(raw).unwrap_or_default());
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

    let mut body = Vec::with_capacity(
        response
            .content_length()
            .map_or(0, |length| length.min(maximum_bytes as u64)) as usize,
    );
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

fn api_error_message(error: &ApiError) -> Option<String> {
    let code = error.code.trim();
    let message = error.message.trim();

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
                return Err(close_reason_error(
                    u16::from(close.code),
                    close.reason.to_string(),
                ));
            }
            Message::Close(None) => return Ok(None),
            Message::Ping(_) | Message::Pong(_) => {}
            Message::Text(_) => return Err(TsfClientError::UnexpectedTextMessage),
            Message::Frame(_) => {}
        }
    }
}

/// Maps stable server close reasons to typed errors, once for every WebSocket path.
fn close_reason_error(code: u16, reason: String) -> TsfClientError {
    match (code, reason.as_str()) {
        (1008, "sequence_mismatch") => TsfClientError::SequenceMismatch {
            actual_next_seq_num: None,
            request_id: None,
            retry_after: None,
        },
        _ => TsfClientError::WebSocketClosedWithReason { code, reason },
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

/// Waits for the next server frame and extracts the expected handshake frame from it.
async fn expect_frame<T>(
    ws: &mut ClientWebSocket,
    extract: impl FnOnce(ServerFrame) -> Result<T, ServerFrame>,
) -> Result<T, TsfClientError> {
    match next_server_frame(ws).await? {
        Some(frame) => extract(frame)
            .map_err(|frame| TsfClientError::UnexpectedServerFrame(server_frame_name(&frame))),
        None => Err(TsfClientError::WebSocketClosed),
    }
}

async fn expect_ready(ws: &mut ClientWebSocket) -> Result<(), TsfClientError> {
    expect_frame(ws, |frame| match frame {
        ServerFrame::Ready => Ok(()),
        other => Err(other),
    })
    .await
}

async fn expect_read_handshake(ws: &mut ClientWebSocket) -> Result<StreamMetadata, TsfClientError> {
    expect_ready(ws).await?;
    expect_frame(ws, |frame| match frame {
        ServerFrame::StreamMetadata(stream_metadata) => Ok(stream_metadata),
        other => Err(other),
    })
    .await
}

fn server_frame_name(frame: &ServerFrame) -> &'static str {
    match frame {
        ServerFrame::Ready => "ready",
        ServerFrame::AppendAck { .. } => "append_ack",
        ServerFrame::ReadBatch(_) => "read_batch",
        ServerFrame::Heartbeat => "heartbeat",
        ServerFrame::CaughtUp(_) => "caught_up",
        ServerFrame::StreamMetadata(_) => "stream_metadata",
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
    /// A REST response contained malformed JSON or did not match the expected schema.
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
    /// The stream next sequence did not match an append or writer-session precondition.
    #[error("stream next sequence did not match the writer session precondition")]
    SequenceMismatch {
        /// Actual stream next sequence when the service reported it.
        actual_next_seq_num: Option<u64>,
        /// Server request ID used for support and tracing, when reported over REST.
        request_id: Option<String>,
        /// Server-requested retry delay, when reported over REST.
        retry_after: Option<Duration>,
    },
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
    /// A requested reservation is larger than the entire retained backlog.
    #[error(
        "append batch uses {records} records and {bytes} bytes, above retained backlog {max_retained_records} records and {max_retained_bytes} bytes"
    )]
    AppendBatchExceedsRetainedBacklog {
        /// Batch record count.
        records: usize,
        /// Batch backlog-accounted payload size.
        bytes: usize,
        /// Configured retained record limit.
        max_retained_records: usize,
        /// Configured retained byte limit.
        max_retained_bytes: usize,
    },
    /// A batch is larger than its previously acquired reservation.
    #[error(
        "append batch uses {records} records and {bytes} bytes, above reserved capacity {reserved_records} records and {reserved_bytes} bytes"
    )]
    AppendBatchExceedsReservation {
        /// Batch record count.
        records: usize,
        /// Batch backlog-accounted payload size.
        bytes: usize,
        /// Record capacity owned by the permit.
        reserved_records: usize,
        /// Byte capacity owned by the permit.
        reserved_bytes: usize,
    },
    /// The writer command channel is closed.
    #[error("append writer is closed")]
    AppendWriterClosed,
    /// The writer task ended before resolving a pending ticket.
    #[error("append writer dropped with unacknowledged records")]
    AppendWriterDropped,
    /// An append may be durable, but its acknowledgement could not be recovered.
    ///
    /// The writer is terminal. The typed first failure is retained for every observer.
    #[error("append durability is unknown; this writer cannot safely continue: {0}")]
    AppendDurabilityUnknown(Arc<TsfClientError>),
    /// The writer background task panicked or could not be joined.
    #[error("append writer task failed: {0}")]
    AppendWriterTaskFailed(String),
    /// Consecutive read connections ended without delivering a record batch or caught-up event.
    #[error(
        "read stream delivered no record batch or caught-up event across {max_connection_attempts} consecutive connection attempts"
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
    /// A read wait exceeds the supported long-poll duration.
    #[error("read wait {value} exceeds the supported maximum {maximum}")]
    InvalidReadWait {
        /// Requested wait in seconds.
        value: u32,
        /// Largest supported wait in seconds.
        maximum: u32,
    },
    /// A timestamp playback rate is outside the protocol range.
    #[error("playback rate {value} must be between {minimum} and {maximum}")]
    InvalidPlaybackRate {
        /// Requested playback rate.
        value: f64,
        /// Slowest accepted playback rate.
        minimum: f64,
        /// Fastest accepted playback rate.
        maximum: f64,
    },
    /// Timestamp playback needs `count`, `until`, or `wait=0`.
    #[error("playback rate requires count, until, or wait=0")]
    PlaybackRequiresCountUntilOrWaitZero,
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
            Self::HttpStatus { request_id, .. } | Self::SequenceMismatch { request_id, .. } => {
                request_id.as_deref()
            }
            Self::AppendDurabilityUnknown(inner) => inner.request_id(),
            _ => None,
        }
    }

    /// Returns the stable API code attached to an HTTP failure.
    pub fn api_code(&self) -> Option<&str> {
        match self {
            Self::HttpStatus { api_code, .. } => api_code.as_deref(),
            Self::AppendDurabilityUnknown(inner) => inner.api_code(),
            _ => None,
        }
    }

    /// Returns the server-requested retry delay.
    pub fn retry_after(&self) -> Option<Duration> {
        match self {
            Self::HttpStatus { retry_after, .. } | Self::SequenceMismatch { retry_after, .. } => {
                *retry_after
            }
            Self::AppendDurabilityUnknown(inner) => inner.retry_after(),
            _ => None,
        }
    }

    /// Returns the actual stream next sequence attached to a failed sequence precondition.
    pub fn actual_next_seq_num(&self) -> Option<u64> {
        match self {
            Self::HttpStatus {
                actual_next_seq_num,
                ..
            }
            | Self::SequenceMismatch {
                actual_next_seq_num,
                ..
            } => *actual_next_seq_num,
            Self::AppendDurabilityUnknown(inner) => inner.actual_next_seq_num(),
            _ => None,
        }
    }
    /// Returns whether retrying a failed create with the same idempotency key and request is safe
    /// and may succeed.
    pub fn is_recoverable_create_failure(&self) -> bool {
        match self {
            Self::Http(error) => is_transient_transport_error(error),
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
            Self::Json(_) => true,
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
            Self::Http(error) => is_transient_transport_error(error),
            Self::HttpStatus { status, .. } => is_retryable_http_status(status.as_u16()),
            Self::Timeout { .. } => true,
            _ => false,
        }
    }
}

fn is_transient_transport_error(error: &reqwest::Error) -> bool {
    error.is_timeout() || error.is_connect() || error.is_body() || error.is_decode()
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
    use crate::protocol::{
        read::ReadStop,
        rest::SseReadRecord,
        ws::frame::{MAX_RECORD_PAYLOAD_BYTES, OwnedReadRecord, RecordFormat},
    };

    #[test]
    fn parses_structured_http_error_details() {
        let body = r#"{"error":{"code":"sequence_mismatch","message":"position changed","request_id":"request-42","retry_after_ms":125,"actual_next_seq_num":"42","future_field":true}}"#;
        let parsed: ApiErrorResponse = serde_json::from_str(body).expect("structured API error");

        assert_eq!(
            api_error_message(&parsed.error).as_deref(),
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
        let blank: ApiErrorResponse =
            serde_json::from_str(r#"{"error":{"code":" ","message":" "}}"#).expect("blank error");
        assert_eq!(api_error_message(&blank.error), None);
    }

    #[tokio::test]
    async fn retries_invalid_rest_json() {
        let config = TsfClientConfig {
            retry_policy: RetryPolicy {
                max_attempts: 2,
                initial_backoff: Duration::ZERO,
                max_backoff: Duration::ZERO,
            },
            ..TsfClientConfig::default()
        };
        let client = TsfClient::with_config(config).expect("client");
        let attempts = std::cell::Cell::new(0);

        let result = client
            .retry_transient(|| {
                let attempt = attempts.get();
                attempts.set(attempt + 1);
                async move {
                    if attempt == 0 {
                        Err(TsfClientError::Json(
                            serde_json::from_str::<serde_json::Value>("{")
                                .expect_err("invalid JSON"),
                        ))
                    } else {
                        Ok(())
                    }
                }
            })
            .await;

        assert!(result.is_ok());
        assert_eq!(attempts.get(), 2);
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
        let result = client.connect_sse_reader(ReadOptions::new(stream_id)).await;

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
        let key = IdempotencyKey::new_random();
        let exposed = key.expose_secret().to_owned();

        assert!(is_canonical_base64url_32(&exposed));
        assert_eq!(
            exposed
                .parse::<IdempotencyKey>()
                .expect("canonical key")
                .expose_secret(),
            exposed
        );
        assert!(!format!("{key:?}").contains(&exposed));
        assert!(matches!(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".parse::<IdempotencyKey>(),
            Err(InvalidIdempotencyKey)
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

        let stream_metadata = expect_read_handshake(&mut client)
            .await
            .expect("read handshake");

        assert_eq!(stream_metadata, expected_stream_metadata);
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
    fn writer_backlog_defaults_match_the_server_window_and_reject_out_of_range_bounds() {
        let default = TsfWriterConfig::default();
        assert_eq!(
            default.max_retained_bytes,
            DEFAULT_MAX_WRITER_RETAINED_BYTES
        );
        assert_eq!(
            default.max_retained_records,
            DEFAULT_MAX_WRITER_RETAINED_RECORDS
        );
        assert!(default.validate().is_ok());

        // The retained backlog is client-local memory: batches larger than the in-flight window are
        // legitimate, so any semaphore-representable bound is accepted.
        assert!(
            TsfWriterConfig {
                max_retained_bytes: 4 * MAX_WRITER_IN_FLIGHT_BYTES,
                max_retained_records: 4 * MAX_WRITER_IN_FLIGHT_RECORDS,
            }
            .validate()
            .is_ok()
        );

        // Both backlog dimensions accept Tokio's exact semaphore boundary.
        assert!(
            TsfWriterConfig {
                max_retained_bytes: Semaphore::MAX_PERMITS,
                max_retained_records: Semaphore::MAX_PERMITS,
            }
            .validate()
            .is_ok()
        );

        for config in [
            TsfWriterConfig {
                max_retained_bytes: 0,
                ..TsfWriterConfig::default()
            },
            TsfWriterConfig {
                max_retained_records: 0,
                ..TsfWriterConfig::default()
            },
            TsfWriterConfig {
                max_retained_bytes: Semaphore::MAX_PERMITS + 1,
                ..TsfWriterConfig::default()
            },
            TsfWriterConfig {
                max_retained_records: usize::MAX,
                ..TsfWriterConfig::default()
            },
        ] {
            assert!(matches!(
                config.validate(),
                Err(TsfClientError::InvalidWriterConfig(_))
            ));
        }
    }

    #[tokio::test]
    async fn invalid_writer_config_is_rejected_before_connecting() {
        // Nothing answers on this port, so reaching the handshake would fail differently.
        let client =
            TsfClient::with_api_origin(Url::parse("http://127.0.0.1:1").expect("API origin"))
                .expect("valid API origin");
        let stream_id = "0123456789abcdefghjkmnpqrstvwxyz"
            .parse::<StreamId>()
            .expect("stream id");
        let options = DurableWriterOptions::new(
            stream_id,
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
                .parse::<LinkSecret>()
                .expect("canonical link secret"),
        );

        let error = client
            .connect_writer_with_config(
                options,
                TsfWriterConfig {
                    max_retained_records: 0,
                    ..TsfWriterConfig::default()
                },
            )
            .await
            .expect_err("invalid config must be rejected");

        assert!(matches!(error, TsfClientError::InvalidWriterConfig(_)));
    }

    #[test]
    fn builds_versioned_rest_and_websocket_base_urls() {
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
    fn read_query_keeps_the_original_absolute_selector_and_count() {
        let stream_id = "0123456789abcdefghjkmnpqrstvwxyz"
            .parse()
            .expect("stream ID");
        let mut options = ReadOptions::new(stream_id);
        options.start = Some(ReadStart::SeqNum(42));
        options.stop = Some(ReadStop {
            count: Some(7),
            until_timestamp_ms: Some(1_787_000_000_000),
            wait_seconds: Some(30),
        });
        options.rate = Some(0.5);
        let mut url = Url::parse("https://tail.surf/api/v1/streams/id/read").expect("read URL");

        append_read_query(&mut url, &options);

        assert_eq!(
            url.query(),
            Some("seq_num=42&count=7&until=1787000000000&rate=0.5&wait=30")
        );

        let mut default_url =
            Url::parse("https://tail.surf/api/v1/streams/id/read").expect("read URL");
        append_read_query(&mut default_url, &ReadOptions::new(stream_id));
        assert_eq!(default_url.query(), Some("tail_offset=0"));
    }

    #[test]
    fn read_rate_rejects_non_finite_and_out_of_range_values() {
        let stream_id = "0123456789abcdefghjkmnpqrstvwxyz"
            .parse()
            .expect("stream ID");
        for rate in [f64::NAN, f64::INFINITY, 0.09, 101.0] {
            let mut options = ReadOptions::new(stream_id);
            options.stop = Some(ReadStop {
                wait_seconds: Some(0),
                ..ReadStop::default()
            });
            options.rate = Some(rate);
            assert!(matches!(
                validate_read_options(&options),
                Err(TsfClientError::InvalidPlaybackRate { .. })
            ));
        }
    }

    #[test]
    fn read_wait_rejects_values_above_the_supported_maximum() {
        let stream_id = "0123456789abcdefghjkmnpqrstvwxyz"
            .parse()
            .expect("stream ID");
        let mut options = ReadOptions::new(stream_id);
        options.stop = Some(ReadStop {
            wait_seconds: Some(MAX_READ_WAIT_SECONDS + 1),
            ..ReadStop::default()
        });

        assert!(matches!(
            validate_read_options(&options),
            Err(TsfClientError::InvalidReadWait { .. })
        ));
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

        assert_eq!(sse_resume_cursor(&event).expect("resume cursor").0, cursor);

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
                sse_resume_cursor(&event),
                Err(TsfClientError::InvalidSse(_))
            ));
        }
    }

    #[tokio::test]
    async fn rejects_read_selectors_outside_the_adapter_range() {
        let client =
            TsfClient::with_api_origin(Url::parse("http://localhost").expect("API origin"))
                .expect("valid API origin");
        let mut options = ReadOptions::new(
            "0123456789abcdefghjkmnpqrstvwxyz"
                .parse()
                .expect("stream ID"),
        );
        options.start = Some(ReadStart::TailOffset(MAX_SAFE_INTEGER_U64 + 1));

        assert!(matches!(
            client.connect_reader(options).await,
            Err(TsfClientError::InvalidReadSelector {
                value,
                maximum: MAX_SAFE_INTEGER_U64,
            }) if value == MAX_SAFE_INTEGER_U64 + 1
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
        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<WriterCommand>();
        let shared = Arc::new(WriterShared {
            byte_permits: Arc::new(Semaphore::new(1)),
            record_permits: Arc::new(Semaphore::new(1)),
            terminal_error: Arc::new(OnceLock::new()),
            config: TsfWriterConfig {
                max_retained_bytes: 1,
                max_retained_records: 1,
            },
            state: AtomicU8::new(WRITER_OPEN),
            submit_lock: RwLock::new(()),
        });
        let task_terminal_error = Arc::clone(&shared.terminal_error);
        let task = tokio::spawn(async move {
            let command = cmd_rx.recv().await.expect("close command");
            task_terminal_error
                .set(Arc::new(TsfClientError::SequenceMismatch {
                    actual_next_seq_num: Some(7),
                    request_id: Some("request-42".to_owned()),
                    retry_after: None,
                }))
                .expect("set terminal error");
            drop(command);
        });
        let writer = TsfWriter {
            producer: TsfProducer { cmd_tx, shared },
            task: Some(task),
        };

        let error = writer.close().await.expect_err("close must fail");
        assert!(
            matches!(
                &error,
                TsfClientError::AppendDurabilityUnknown(inner)
                    if matches!(**inner, TsfClientError::SequenceMismatch { .. })
            ),
            "error={error}"
        );
        assert_eq!(error.actual_next_seq_num(), Some(7));
        assert_eq!(error.request_id(), Some("request-42"));
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

        let mut options = ReadOptions::new(
            "00000000000000000000000000000000"
                .parse()
                .expect("stream ID"),
        );
        options.start = Some(ReadStart::SeqNum(2));
        assert!(
            validate_read_batch_for_request(
                &ReadBatch::try_from_records(vec![sse_test_record(1, 0)]).expect("valid batch"),
                &options,
            )
            .is_err()
        );
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

    fn pending_submission(
        start_seq_num: u64,
        count: usize,
        permits: &Arc<Semaphore>,
    ) -> (
        PendingSubmission,
        oneshot::Receiver<Result<Vec<AppendReceipt>, TsfClientError>>,
    ) {
        let (ack_tx, ack_rx) = oneshot::channel();
        let payloads = (0..count)
            .map(|_| RecordPayload::new(PartHeader::unsplit(), RecordFormat::Bytes, Bytes::new()))
            .collect::<Vec<_>>();
        let submission = PendingSubmission {
            start_seq_num,
            acked: 0,
            sent: payloads.len(),
            receipts: Vec::with_capacity(payloads.len()),
            payloads,
            ack_tx,
            _byte_permit: permits
                .clone()
                .try_acquire_many_owned(count as u32)
                .expect("byte permits"),
            _record_permit: permits
                .clone()
                .try_acquire_many_owned(count as u32)
                .expect("record permits"),
        };
        (submission, ack_rx)
    }

    /// In-flight window state matching fully sent submissions of empty records.
    fn in_flight_window(pending: &VecDeque<PendingSubmission>) -> InFlightWindow {
        InFlightWindow {
            records: pending.iter().map(|s| s.payloads.len()).sum(),
            bytes: pending.iter().map(|s| s.payloads.len()).sum(),
            ack_deadline: None,
        }
    }

    #[tokio::test]
    async fn permit_submit_releases_over_reserved_capacity() {
        let shared = Arc::new(WriterShared {
            byte_permits: Arc::new(Semaphore::new(8)),
            record_permits: Arc::new(Semaphore::new(8)),
            terminal_error: Arc::new(OnceLock::new()),
            config: TsfWriterConfig {
                max_retained_bytes: 8,
                max_retained_records: 8,
            },
            state: AtomicU8::new(WRITER_OPEN),
            submit_lock: RwLock::new(()),
        });
        let (cmd_tx, _cmd_rx) = mpsc::unbounded_channel();
        let producer = TsfProducer {
            cmd_tx,
            shared: Arc::clone(&shared),
        };
        let big = AppendBatch::from_records(
            (0..4)
                .map(|_| {
                    RecordPayload::new(PartHeader::unsplit(), RecordFormat::Bytes, Bytes::new())
                })
                .collect(),
        )
        .expect("four-record batch");
        let permit = producer.reserve(&big).await.expect("reserve");
        assert_eq!(shared.byte_permits.available_permits(), 4);
        assert_eq!(shared.record_permits.available_permits(), 4);

        let small = AppendBatch::single(PartHeader::unsplit(), RecordFormat::Bytes, Bytes::new())
            .expect("one-record batch");
        let _ticket = permit.submit(small).expect("submit smaller batch");

        // The unused remainder returns to the windows instead of waiting for the ack.
        assert_eq!(shared.byte_permits.available_permits(), 7);
        assert_eq!(shared.record_permits.available_permits(), 7);
    }

    #[test]
    fn writer_cursor_rejects_sequence_exhaustion() {
        let mut cursor = WriterCursor {
            next_seq_num: u64::MAX - 1,
        };

        assert_eq!(
            cursor.reserve(1).expect("last usable sequence"),
            u64::MAX - 1
        );
        assert!(matches!(
            cursor.reserve(1),
            Err(TsfClientError::Frame(
                FrameCodecError::WriterSequenceExhausted
            ))
        ));
    }

    #[test]
    fn drain_submissions_numbers_in_channel_order() {
        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<WriterCommand>();
        let permits = Arc::new(Semaphore::new(8));
        let batch = |count: usize| {
            AppendBatch::from_records(
                (0..count)
                    .map(|_| {
                        RecordPayload::new(PartHeader::unsplit(), RecordFormat::Bytes, Bytes::new())
                    })
                    .collect(),
            )
            .expect("batch")
        };
        for count in [2, 1] {
            let (ack_tx, _ack_rx) = oneshot::channel();
            cmd_tx
                .send(WriterCommand::Submit {
                    batch: batch(count),
                    ack_tx,
                    byte_permit: permits
                        .clone()
                        .try_acquire_many_owned(count as u32)
                        .expect("byte permits"),
                    record_permit: permits
                        .clone()
                        .try_acquire_many_owned(count as u32)
                        .expect("record permits"),
                })
                .expect("submit command");
        }

        let mut pending = VecDeque::new();
        let mut close_tx = None;
        let mut cursor = WriterCursor::default();
        let first = cmd_rx.try_recv().expect("first command");
        drain_submissions(&mut pending, &mut cmd_rx, &mut close_tx, &mut cursor, first)
            .expect("drain submissions");

        assert_eq!(
            pending
                .iter()
                .map(|submission| (submission.start_seq_num, submission.payloads.len()))
                .collect::<Vec<_>>(),
            [(0, 2), (2, 1)]
        );
        assert!(pending.iter().all(|submission| submission.sent == 0));
    }

    #[test]
    fn take_frame_batches_across_submissions_within_frame_bounds() {
        let permits = Arc::new(Semaphore::new(12));
        let (first, _first_rx) = pending_submission(0, 3, &permits);
        let (second, _second_rx) = pending_submission(3, 3, &permits);
        let mut pending = VecDeque::from([first, second]);
        for submission in &mut pending {
            submission.sent = 0;
            for payload in &mut submission.payloads {
                payload.data = Bytes::from(vec![0_u8; MAX_RECORD_PAYLOAD_BYTES]);
            }
        }
        let mut in_flight = InFlightWindow::default();
        let mut frame = Vec::new();

        // Two 512 KiB records per 1 MiB frame; six records fit the 5 MiB window.
        let mut planned = Vec::new();
        loop {
            take_frame(&mut pending, &mut in_flight, &mut frame);
            if frame.is_empty() {
                break;
            }
            planned.push(
                frame
                    .iter()
                    .map(|record| record.writer_seq_num)
                    .collect::<Vec<_>>(),
            );
        }

        assert_eq!(planned, [vec![0, 1], vec![2, 3], vec![4, 5]]);
        assert_eq!(in_flight.records, 6);
        assert_eq!(in_flight.bytes, 6 * MAX_RECORD_PAYLOAD_BYTES);
        assert!(
            pending
                .iter()
                .all(|submission| submission.sent == submission.payloads.len())
        );
    }

    #[test]
    fn take_frame_stops_at_the_in_flight_window() {
        let permits = Arc::new(Semaphore::new(4));
        let (submission, _ack_rx) = pending_submission(0, 2, &permits);
        let mut pending = VecDeque::from([submission]);
        pending[0].sent = 0;
        for payload in &mut pending[0].payloads {
            payload.data = Bytes::from(vec![0_u8; MAX_RECORD_PAYLOAD_BYTES]);
        }
        // One record fits the remaining window exactly; the second must wait for acks.
        let mut in_flight = InFlightWindow {
            bytes: MAX_WRITER_IN_FLIGHT_BYTES - MAX_RECORD_PAYLOAD_BYTES,
            records: MAX_WRITER_IN_FLIGHT_RECORDS - 2,
            ack_deadline: None,
        };
        let mut frame = Vec::new();

        take_frame(&mut pending, &mut in_flight, &mut frame);
        assert_eq!(frame.len(), 1);
        assert_eq!(frame[0].writer_seq_num, 0);
        assert_eq!(in_flight.bytes, MAX_WRITER_IN_FLIGHT_BYTES);
        assert_eq!(pending[0].sent, 1);
        take_frame(&mut pending, &mut in_flight, &mut frame);
        assert!(frame.is_empty());

        // The record-count bound stops framing even with byte capacity left.
        let mut record_full = InFlightWindow {
            bytes: 0,
            records: MAX_WRITER_IN_FLIGHT_RECORDS,
            ack_deadline: None,
        };
        let other_permits = Arc::new(Semaphore::new(2));
        let (mut unsent, _rx) = pending_submission(7, 1, &other_permits);
        unsent.sent = 0;
        let mut pending2 = VecDeque::from([unsent]);
        take_frame(&mut pending2, &mut record_full, &mut frame);
        assert!(frame.is_empty());
    }

    #[test]
    fn dispatch_ack_rejects_more_records_than_are_pending() {
        let permits = Arc::new(Semaphore::new(2));
        let (submission, _ack_rx) = pending_submission(7, 1, &permits);
        let mut pending = VecDeque::from([submission]);
        let mut in_flight = in_flight_window(&pending);
        let ack = AppendAck {
            writer_start_seq_num: 7,
            writer_end_seq_num: 9,
            start_seq_num: 42,
            end_seq_num: 44,
        };

        assert!(matches!(
            dispatch_ack(ack, &mut pending, &mut in_flight),
            Err(TsfClientError::InvalidAppendAck(error_ack)) if error_ack == ack
        ));
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].acked, 0);
    }

    #[test]
    fn dispatch_ack_validates_the_full_range_before_draining() {
        let permits = Arc::new(Semaphore::new(4));
        let (first, _first_rx) = pending_submission(7, 1, &permits);
        let (second, _second_rx) = pending_submission(9, 1, &permits);
        let mut pending = VecDeque::from([first, second]);
        let mut in_flight = in_flight_window(&pending);
        let ack = AppendAck {
            writer_start_seq_num: 7,
            writer_end_seq_num: 9,
            start_seq_num: 42,
            end_seq_num: 44,
        };

        assert!(matches!(
            dispatch_ack(ack, &mut pending, &mut in_flight),
            Err(TsfClientError::InvalidAppendAck(error_ack)) if error_ack == ack
        ));
        assert_eq!(
            pending
                .iter()
                .map(|submission| submission.start_seq_num)
                .collect::<Vec<_>>(),
            [7, 9]
        );
        assert!(pending.iter().all(|submission| submission.acked == 0));
    }

    #[test]
    fn dispatch_ack_resolves_completed_submissions_across_boundaries() {
        let permits = Arc::new(Semaphore::new(6));
        let (first, mut first_rx) = pending_submission(7, 2, &permits);
        let (second, mut second_rx) = pending_submission(9, 1, &permits);
        let mut pending = VecDeque::from([first, second]);
        let mut in_flight = in_flight_window(&pending);
        let first_ack = AppendAck {
            writer_start_seq_num: 7,
            writer_end_seq_num: 8,
            start_seq_num: 42,
            end_seq_num: 43,
        };
        let second_ack = AppendAck {
            writer_start_seq_num: 8,
            writer_end_seq_num: 10,
            start_seq_num: 43,
            end_seq_num: 45,
        };

        dispatch_ack(first_ack, &mut pending, &mut in_flight).expect("partial ack");
        assert!(matches!(
            first_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert_eq!(pending[0].acked, 1);
        assert_eq!(in_flight.records, 2);

        dispatch_ack(second_ack, &mut pending, &mut in_flight).expect("spanning ack");
        assert!(pending.is_empty());
        assert_eq!(in_flight.records, 0);
        assert_eq!(in_flight.bytes, 0);
        for (ack_rx, expected) in [
            (&mut first_rx, vec![(7, 42, first_ack), (8, 43, second_ack)]),
            (&mut second_rx, vec![(9, 44, second_ack)]),
        ] {
            let receipts = ack_rx
                .try_recv()
                .expect("resolved ticket")
                .expect("receipts");
            assert_eq!(
                receipts
                    .iter()
                    .map(|receipt| (receipt.writer_seq_num, receipt.seq_num, receipt.ack))
                    .collect::<Vec<_>>(),
                expected
            );
        }
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

    #[tokio::test]
    async fn sse_reader_bounds_consecutive_reconnects_without_delivered_records() {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind SSE listener");
        let address = listener.local_addr().expect("SSE listener address");
        let server = tokio::spawn(async move {
            // Every handshake completes with valid metadata; no body ever delivers a record.
            for _ in 0..8 {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let mut request = [0_u8; 4096];
                let _ = stream.read(&mut request).await;
                let response = "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n\
                    event: stream_metadata\n\
                    data: {\"stream_id\":\"00000000000000000000000000000000\",\"visibility\":\"private\",\"created_at\":\"2026-08-13T00:00:00Z\",\"expires_at\":\"2026-08-23T00:00:00Z\"}\n\n";
                let _ = stream.write_all(response.as_bytes()).await;
            }
        });
        let mut config =
            TsfClientConfig::new(Url::parse(&format!("http://{address}")).expect("SSE API origin"))
                .expect("valid client config");
        config.retry_policy = RetryPolicy {
            max_attempts: 3,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(2),
        };
        let client = TsfClient::with_config(config).expect("SSE client");
        let stream_id = "00000000000000000000000000000000"
            .parse()
            .expect("stream ID");
        let mut reader = client
            .connect_sse_reader(ReadOptions::new(stream_id))
            .await
            .expect("initial SSE connect");

        let result = tokio::time::timeout(Duration::from_secs(5), reader.next_batch()).await;

        assert!(matches!(
            result,
            Ok(Err(TsfClientError::ReadReconnectLimitExceeded {
                max_connection_attempts: 3,
            }))
        ));
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn list_all_links_rejects_link_ids_repeated_across_pages() {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind REST listener");
        let address = listener.local_addr().expect("REST listener address");
        let server = tokio::spawn(async move {
            let link = r#"{"link_id":"reader","permissions":"r","status":"active","created_at":"2026-08-13T00:00:00Z","expires_at":null,"revoked_at":null}"#;
            for page in [
                format!(
                    r#"{{"authorizing_link_id":"owner","links":[{link}],"next_cursor":"next"}}"#
                ),
                format!(r#"{{"authorizing_link_id":"owner","links":[{link}],"next_cursor":null}}"#),
            ] {
                let (mut stream, _) = listener.accept().await.expect("accept REST request");
                let mut request = [0_u8; 4096];
                let _ = stream.read(&mut request).await;
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{page}",
                    page.len()
                );
                let _ = stream.write_all(response.as_bytes()).await;
            }
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
        let owner: LinkSecret = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
            .parse()
            .expect("canonical secret");

        let result = client.list_all_links(&stream_id, &owner).await;

        assert!(matches!(
            result,
            Err(TsfClientError::InvalidLinkPage(
                "link ID appears on multiple pages"
            ))
        ));
        server.await.expect("join REST server");
    }

    #[test]
    fn compact_record_data_choice_matches_serialized_lengths() {
        let cases: Vec<Vec<u8>> = vec![
            b"plain text".to_vec(),
            "unicode 😀 text".as_bytes().to_vec(),
            b"\"\\escape\theavy\n".to_vec(),
            vec![0x00; 64],
            vec![0x7f; 128],
            "😀".repeat(1024).into_bytes(),
            vec![0xff; 32],
        ];
        for bytes in cases {
            let chosen = compact_record_data(&bytes);
            if let Ok(text) = std::str::from_utf8(&bytes) {
                let utf8_len = serde_json::to_vec(&RecordData::Utf8(text.to_owned()))
                    .expect("measure utf8")
                    .len();
                let base64url_len = br#"{"encoding":"base64url","value":""}"#.len()
                    + bytes.len().saturating_mul(4).div_ceil(3);
                assert_eq!(
                    matches!(chosen, RecordData::Utf8(_)),
                    utf8_len <= base64url_len,
                    "bytes={bytes:?}"
                );
            } else {
                assert!(matches!(chosen, RecordData::Base64url(_)));
            }
            let round_tripped = match &chosen {
                RecordData::Utf8(value) => value.as_bytes().to_vec(),
                RecordData::Base64url(value) => URL_SAFE_NO_PAD.decode(value).expect("decode"),
            };
            assert_eq!(round_tripped, bytes);
        }
    }

    #[test]
    fn sse_batch_validation_enforces_decoded_bounds_and_read_count() {
        assert!(sse_read_batch(SseReadBatchData { records: vec![] }).is_err());

        let mut options = ReadOptions::new(
            "00000000000000000000000000000000"
                .parse()
                .expect("stream ID"),
        );
        assert!(matches!(
            sse_read_batch(SseReadBatchData {
                records: [0, 1, 2]
                    .map(|seq_num| sse_wire_record(seq_num, 400 * 1024))
                    .to_vec(),
            }),
            Err(TsfClientError::InvalidSse(
                "read_batch exceeds the decoded payload limit"
            ))
        ));

        options.stop = Some(ReadStop {
            count: Some(1),
            ..ReadStop::default()
        });
        let two = ReadBatch::try_from_records(vec![sse_test_record(0, 0), sse_test_record(1, 0)])
            .expect("valid batch");
        assert!(validate_sse_read_batch(&two, &options).is_err());

        options.stop = Some(ReadStop {
            until_timestamp_ms: Some(1),
            ..ReadStop::default()
        });
        assert!(validate_sse_read_batch(&two, &options).is_err());

        assert!(matches!(
            sse_read_batch(SseReadBatchData {
                records: vec![sse_wire_record(0, 0), sse_wire_record(2, 0)],
            }),
            Err(TsfClientError::InvalidSse(
                "read_batch sequence numbers are not contiguous"
            ))
        ));

        let mut wire_record = SseReadRecord {
            seq_num: 0,
            timestamp_ms: 0,
            writer_id: URL_SAFE_NO_PAD.encode([0_u8; WriterId::BYTE_LEN - 1]),
            writer_seq_num: 0,
            part: RestRecordPart {
                index: 0,
                is_final: true,
            },
            format: RecordFormat::Bytes,
            data: RecordData::Utf8(String::new()),
        };
        assert!(matches!(
            sse_read_batch(SseReadBatchData {
                records: vec![wire_record.clone()],
            }),
            Err(TsfClientError::InvalidSse("invalid writer_id length"))
        ));

        wire_record.writer_id = URL_SAFE_NO_PAD.encode([0_u8; WriterId::BYTE_LEN]);
        wire_record.data =
            RecordData::Base64url(URL_SAFE_NO_PAD.encode(vec![0_u8; MAX_RECORD_PAYLOAD_BYTES + 1]));
        assert!(
            sse_read_batch(SseReadBatchData {
                records: vec![wire_record],
            })
            .is_err()
        );
    }

    #[test]
    fn sse_cursor_validation_binds_positions_and_counts() {
        let mut options = ReadOptions::new(
            "00000000000000000000000000000000"
                .parse()
                .expect("stream ID"),
        );
        options.start = Some(ReadStart::SeqNum(0));
        let records =
            ReadBatch::try_from_records(vec![sse_test_record(0, 0)]).expect("valid batch");
        assert!(
            validate_sse_read_batch_cursor(
                &records,
                ParsedSseResumeCursor {
                    next_seq_num: 2,
                    consumed_records: 1,
                },
                None,
                &options,
            )
            .is_err()
        );
        let previous = ParsedSseResumeCursor {
            next_seq_num: 1,
            consumed_records: 1,
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
                },
                Some(previous),
                &options,
            )
            .is_err()
        );
    }

    fn sse_test_record(seq_num: u64, payload_bytes: usize) -> OwnedReadRecord {
        OwnedReadRecord {
            seq_num,
            timestamp_ms: seq_num,
            writer_id: WriterId::from_bytes([0_u8; 16]),
            writer_seq_num: seq_num,
            part: PartHeader::unsplit(),
            format: RecordFormat::Bytes,
            data: Bytes::from(vec![0_u8; payload_bytes]),
        }
    }

    fn sse_wire_record(seq_num: u64, payload_bytes: usize) -> SseReadRecord {
        SseReadRecord {
            seq_num,
            timestamp_ms: seq_num,
            writer_id: URL_SAFE_NO_PAD.encode([0_u8; WriterId::BYTE_LEN]),
            writer_seq_num: seq_num,
            part: RestRecordPart {
                index: 0,
                is_final: true,
            },
            format: RecordFormat::Bytes,
            data: RecordData::Base64url(URL_SAFE_NO_PAD.encode(vec![0_u8; payload_bytes])),
        }
    }

    #[test]
    fn sse_read_batch_decodes_mixed_utf8_and_base64url_payloads() {
        let text = SseReadRecord {
            seq_num: 3,
            timestamp_ms: 300,
            writer_id: URL_SAFE_NO_PAD.encode([7_u8; WriterId::BYTE_LEN]),
            writer_seq_num: 30,
            part: RestRecordPart {
                index: 0,
                is_final: true,
            },
            format: RecordFormat::Transcript,
            data: RecordData::Utf8("héllo".to_owned()),
        };
        let binary = SseReadRecord {
            seq_num: 4,
            timestamp_ms: 301,
            writer_id: URL_SAFE_NO_PAD.encode([8_u8; WriterId::BYTE_LEN]),
            writer_seq_num: 40,
            part: RestRecordPart {
                index: 0,
                is_final: true,
            },
            format: RecordFormat::Bytes,
            data: RecordData::Base64url(URL_SAFE_NO_PAD.encode([0_u8, 159, 146, 150])),
        };
        let batch = sse_read_batch(SseReadBatchData {
            records: vec![text, binary],
        })
        .expect("valid read_batch event");

        assert_eq!(batch.record_count(), 2);
        let first = batch.first();
        assert_eq!(first.data, "héllo".as_bytes());
        assert_eq!(first.format, RecordFormat::Transcript);
        assert_eq!(first.writer_id, WriterId::from_bytes([7_u8; 16]));
        assert_eq!(first.writer_seq_num, 30);
        let last = batch.last();
        assert_eq!(last.data, &[0_u8, 159, 146, 150]);
        assert_eq!(last.seq_num, 4);

        let options = ReadOptions::new(
            "00000000000000000000000000000000"
                .parse()
                .expect("stream ID"),
        );
        assert!(validate_sse_read_batch(&batch, &options).is_ok());
    }

    #[test]
    fn stateless_append_compacts_an_escape_heavy_maximum_record() {
        let data = vec![0_u8; MAX_RECORD_PAYLOAD_BYTES];
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
        let secret: LinkSecret = "A".repeat(32).parse().expect("canonical secret");
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
                    Some(MAX_SAFE_INTEGER_U64 + 1),
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
