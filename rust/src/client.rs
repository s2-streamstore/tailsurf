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
    sync::{mpsc, oneshot},
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
            AppendWriter, CreateLinkInput, CreateLinkResponse, CreateStreamRequest,
            CreateStreamResponse, JsonRecordPayload, ListLinksResponse, MAX_LINK_PAGE_ITEMS,
            MAX_REST_ERROR_RESPONSE_BYTES, MAX_REST_RESPONSE_BYTES, MAX_SSE_EVENT_BYTES,
            MAX_SSE_UNTERMINATED_EVENT_BYTES, MAX_STATELESS_APPEND_PAYLOAD_BYTES,
            MAX_STATELESS_APPEND_RECORDS, RestRecordPart, SseCaughtUpData, SseReadBatchData,
            StreamKind, StreamMetadata, UpdateStreamRequest, parse_canonical_decimal_u64,
        },
        ws::{
            MAX_WRITER_IN_FLIGHT_PAYLOAD_BYTES, MAX_WRITER_IN_FLIGHT_RECORDS,
            WEBSOCKET_HEARTBEAT_INTERVAL_MS, WriteSessionOptions,
            frame::{
                AppendBatch, AppendRecord, CaughtUpPosition, ClientFrame, FrameCodecError,
                MAX_APPEND_FRAME_RECORDS, MAX_FRAME_PAYLOAD_BYTES, PartHeader, ReadBatch,
                RecordMeta, RecordPayload, ServerFrame, TSF_WEBSOCKET_PROTOCOL,
            },
        },
    },
};

type ClientWebSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

const API_PREFIX: &str = "/api/v1";
const MAX_CLIENT_DELAY: Duration = Duration::from_millis(2_147_483_647);
const INITIAL_RETRY_BACKOFF: Duration = Duration::from_millis(200);
const MAX_RETRY_BACKOFF: Duration = Duration::from_secs(2);
const WEBSOCKET_READ_IDLE_TIMEOUT: Duration =
    Duration::from_millis(WEBSOCKET_HEARTBEAT_INTERVAL_MS * 3);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DataPlaneRoute {
    Records,
    TerminalInput,
    TerminalOutput,
}

impl DataPlaneRoute {
    fn path(self, stream_id: &StreamId, operation: &str) -> String {
        match self {
            Self::Records => format!("/streams/{stream_id}/{operation}"),
            Self::TerminalInput => {
                format!("/streams/{stream_id}/terminal/input/{operation}")
            }
            Self::TerminalOutput => {
                format!("/streams/{stream_id}/terminal/output/{operation}")
            }
        }
    }
}

/// Timeouts, retry behavior, and API origin for [`TsfClient`].
///
/// Configured durations cannot exceed 2,147,483,647 milliseconds. Required timeouts must be
/// greater than zero.
#[derive(Clone, Debug)]
pub struct TsfClientConfig {
    /// Service origin without the `/api/v1` namespace.
    pub api_origin: Url,
    /// Per-request timeout for HTTP operations and SSE opening handshakes.
    pub http_request_timeout: Duration,
    /// Timeout for establishing and upgrading a WebSocket.
    pub websocket_connect_timeout: Duration,
    /// Progress timeout for authentication, frame sends, and append acknowledgements.
    pub websocket_progress_timeout: Duration,
    /// Total attempts for bounded operations, including the initial attempt.
    ///
    /// This bounds anonymous stream creation, idempotent metadata reads, socket setup, and
    /// consecutive read connection failures. Established durable writers keep recovering until
    /// acknowledged or cancelled.
    pub bounded_operation_attempts: usize,
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
            http_request_timeout: Duration::from_secs(10),
            websocket_connect_timeout: Duration::from_secs(10),
            websocket_progress_timeout: Duration::from_secs(30),
            bounded_operation_attempts: 3,
        }
    }
}

/// Returns the jittered delay before retry `retry` (0-indexed).
fn reconnect_delay(retry: usize) -> Duration {
    let multiplier = 1_u32 << retry.min(30);
    let backoff = INITIAL_RETRY_BACKOFF
        .checked_mul(multiplier)
        .unwrap_or(MAX_RETRY_BACKOFF)
        .min(MAX_RETRY_BACKOFF);
    jittered_backoff(backoff)
}

fn jittered_backoff(backoff: Duration) -> Duration {
    if backoff.is_zero() {
        Duration::ZERO
    } else {
        backoff
            .mul_f64(rand::rng().random_range(0.5_f64..=1.5_f64))
            .min(MAX_RETRY_BACKOFF)
    }
}

/// Cloneable TSF REST, SSE, and v1 WebSocket client.
///
/// REST operations preserve their retry identity. Stateless append retries can create physical
/// duplicates, which logical-record readers suppress. Durable WebSocket writer recovery is
/// owned by [`TsfWriter`].
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
        let url = self.rest_url(format_args!("/streams/{stream_id}"));
        self.retry_transient(|| {
            self.send_json_with_bearer(self.http.get(url.clone()), "get stream", link_secret)
        })
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
    ) -> Result<CreateLinkResponse, TsfClientError> {
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
    ) -> Result<CreateLinkResponse, TsfClientError> {
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
            json_records.push(AppendJsonRecord {
                payload: compact_record_payload(&record.data),
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
            writer: Some(AppendWriter {
                id: URL_SAFE_NO_PAD.encode(client_writer_id.as_bytes()),
                seq_num: writer_start_seq_num,
            }),
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

    /// Connects the reconnecting durable writer.
    ///
    /// Submitted records are queued in memory. The writer sends them through the fixed
    /// [`MAX_WRITER_IN_FLIGHT_PAYLOAD_BYTES`] and [`MAX_WRITER_IN_FLIGHT_RECORDS`] socket window.
    pub async fn connect_writer(
        &self,
        options: DurableWriterOptions,
    ) -> Result<TsfWriter, TsfClientError> {
        self.connect_writer_route(options, DataPlaneRoute::Records)
            .await
    }

    /// Connects a controller writer to a terminal session's input channel.
    pub async fn connect_terminal_input_writer(
        &self,
        options: DurableWriterOptions,
    ) -> Result<TsfWriter, TsfClientError> {
        self.connect_writer_route(options, DataPlaneRoute::TerminalInput)
            .await
    }

    /// Connects the terminal host writer to the output channel. The link must be an owner.
    pub async fn connect_terminal_output_writer(
        &self,
        options: DurableWriterOptions,
    ) -> Result<TsfWriter, TsfClientError> {
        self.connect_writer_route(options, DataPlaneRoute::TerminalOutput)
            .await
    }

    async fn connect_writer_route(
        &self,
        options: DurableWriterOptions,
        route: DataPlaneRoute,
    ) -> Result<TsfWriter, TsfClientError> {
        let mut session_options = WriteSessionOptions::new(
            options.stream_id,
            ClientWriterId::new_random(),
            options.link_secret,
        );
        session_options.expected_next_seq_num = options.expected_next_seq_num;
        let session = self.open_write_session(&session_options, route).await?;
        session_options.expected_next_seq_num = None;
        Ok(TsfWriter::new(
            self.clone(),
            session_options,
            route,
            session,
        ))
    }

    /// Connects a low-level write session that sends records and receives ack ranges directly.
    ///
    /// Unlike [`TsfWriter`], this session does not retain or resend unacknowledged records.
    pub async fn connect_write_session(
        &self,
        options: WriteSessionOptions,
    ) -> Result<TsfWriteSession, TsfClientError> {
        self.open_write_session(&options, DataPlaneRoute::Records)
            .await
    }

    async fn open_write_session(
        &self,
        options: &WriteSessionOptions,
        route: DataPlaneRoute,
    ) -> Result<TsfWriteSession, TsfClientError> {
        self.retry_transient(|| self.connect_write_session_once(options, route))
            .await
    }

    async fn connect_write_session_once(
        &self,
        options: &WriteSessionOptions,
        route: DataPlaneRoute,
    ) -> Result<TsfWriteSession, TsfClientError> {
        let url = self.websocket_url(route.path(&options.stream_id, "write"))?;
        let connect_timeout = self.config.websocket_connect_timeout;
        let progress_timeout = self.config.websocket_progress_timeout;
        let opening_frame = ClientFrame::OpenWrite {
            client_writer_id: options.client_writer_id,
            link_secret: options.link_secret.clone(),
            expected_next_seq_num: options.expected_next_seq_num,
        }
        .encode()?;

        let mut ws =
            connect_websocket(url, connect_timeout, progress_timeout, opening_frame).await?;
        let stream_kind =
            with_timeout(progress_timeout, "writer ready", expect_ready(&mut ws)).await?;

        Ok(TsfWriteSession {
            ws,
            progress_timeout,
            stream_kind,
        })
    }

    /// Connects a resumable read session at the requested position and stop conditions.
    pub async fn connect_reader(
        &self,
        options: ReadOptions,
    ) -> Result<TsfReadSession, TsfClientError> {
        self.connect_reader_route(options, DataPlaneRoute::Records)
            .await
    }

    /// Connects an observer reader to a terminal session's output channel.
    pub async fn connect_terminal_output_reader(
        &self,
        options: ReadOptions,
    ) -> Result<TsfReadSession, TsfClientError> {
        self.connect_reader_route(options, DataPlaneRoute::TerminalOutput)
            .await
    }

    /// Connects the terminal host to its input channel. The link must be an owner.
    pub async fn connect_terminal_input_reader(
        &self,
        options: ReadOptions,
    ) -> Result<TsfReadSession, TsfClientError> {
        self.connect_reader_route(options, DataPlaneRoute::TerminalInput)
            .await
    }

    async fn connect_reader_route(
        &self,
        options: ReadOptions,
        route: DataPlaneRoute,
    ) -> Result<TsfReadSession, TsfClientError> {
        let ConnectedReadSocket {
            socket,
            stream_metadata,
        } = self.connect_read_socket(&options, route).await?;
        Ok(TsfReadSession::new(
            self.clone(),
            options,
            route,
            socket,
            stream_metadata,
        ))
    }

    /// Connects a resumable SSE reader.
    ///
    /// Private credentials stay in the bearer header. Reconnects reuse the original URL and send
    /// the latest versioned event cursor in `Last-Event-ID`. The HTTP request timeout bounds each
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
        validate_read_stream_metadata(&options.stream_id, None, &connection.stream_metadata)?;
        let reconnects = BoundedReadReconnects::new(self.config.bounded_operation_attempts);
        Ok(TsfSseReadSession {
            client: self.clone(),
            options,
            request,
            body: connection.body,
            parser: connection.parser,
            stream_metadata: connection.stream_metadata,
            last_caught_up: None,
            reconnects,
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
        let handshake_timeout = self.config.http_request_timeout;
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
        let mut request = Self::apply_rest_auth(
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
        route: DataPlaneRoute,
    ) -> Result<ConnectedReadSocket, TsfClientError> {
        validate_read_options(options)?;
        let opening_frame = ClientFrame::OpenRead {
            link_secret: options.link_secret.clone(),
        }
        .encode()?;
        let mut url = self.websocket_url(route.path(&options.stream_id, "read"))?;
        append_read_query(&mut url, options);
        let connect_timeout = self.config.websocket_connect_timeout;
        let progress_timeout = self.config.websocket_progress_timeout;
        self.retry_transient(|| {
            let url = url.clone();
            let opening_frame = opening_frame.clone();

            async move {
                let mut ws =
                    connect_websocket(url, connect_timeout, progress_timeout, opening_frame)
                        .await?;
                let stream_metadata = with_timeout(
                    progress_timeout,
                    "reader handshake",
                    expect_read_handshake(&mut ws),
                )
                .await?;
                validate_read_stream_metadata(&options.stream_id, None, &stream_metadata)?;

                Ok(ConnectedReadSocket {
                    socket: ReadSocket {
                        ws,
                        read_idle_timeout: WEBSOCKET_READ_IDLE_TIMEOUT,
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

    async fn send_json_with_bearer<T: DeserializeOwned>(
        &self,
        request: reqwest::RequestBuilder,
        operation: &'static str,
        link_secret: Option<&LinkSecret>,
    ) -> Result<T, TsfClientError> {
        let response = self.send_request(request, link_secret).await?;
        json_response(response, operation).await
    }

    async fn send_empty(
        &self,
        request: reqwest::RequestBuilder,
        operation: &'static str,
        link_secret: Option<&LinkSecret>,
    ) -> Result<(), TsfClientError> {
        let response = self.send_request(request, link_secret).await?;
        let status = response.status();
        if status == StatusCode::NO_CONTENT {
            return Ok(());
        }
        Err(http_status_error(response, operation).await)
    }

    async fn send_request(
        &self,
        request: reqwest::RequestBuilder,
        link_secret: Option<&LinkSecret>,
    ) -> Result<reqwest::Response, TsfClientError> {
        Self::apply_rest_auth(request, link_secret)
            .timeout(self.config.http_request_timeout)
            .send()
            .await
            .map_err(Into::into)
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
        let attempts = self.config.bounded_operation_attempts;

        for attempt in 1..=attempts {
            match run().await {
                Ok(value) => return Ok(value),
                Err(error) if attempt < attempts && should_retry(&error) => {
                    let delay = error.retry_after().map_or_else(
                        || reconnect_delay(attempt - 1),
                        |delay| delay.min(MAX_RETRY_BACKOFF),
                    );
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

/// Default Tailsurf service origin.
pub const DEFAULT_API_ORIGIN: &str = "https://tail.surf";

/// Returns the default [`DEFAULT_API_ORIGIN`] API origin.
pub fn default_api_origin() -> Url {
    Url::parse(DEFAULT_API_ORIGIN).expect("default tsf API base URL is valid")
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
/// [`split_logical_record`](crate::logical_records::split_logical_record). [`TsfWriter`] is the
/// actor-sequenced alternative with reconnect resend.
pub struct TsfWriteSession {
    ws: ClientWebSocket,
    progress_timeout: Duration,
    stream_kind: StreamKind,
}

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

/// Future that resolves when every record of one submitted batch is durable or the writer reaches
/// a non-retryable failure or is cancelled.
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
    terminal_error: Arc<OnceLock<Arc<TsfClientError>>>,
    state: AtomicU8,
    /// Serializes a submission's open check plus command send against the close transition, making
    /// that transition the single linearization point: a submission either enters the queue
    /// before the close command or observes the writer closed. Held only across synchronous
    /// sections, so the lock never crosses an await.
    submit_lock: RwLock<()>,
}

impl WriterShared {
    /// Marks the writer finished on every actor exit path, including abort before the task's first
    /// poll.
    fn shutdown(&self) {
        self.state.store(WRITER_DONE, Ordering::SeqCst);
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
            .field("open", &self.shared.is_open())
            .finish()
    }
}

impl TsfProducer {
    /// Queues the batch as one contiguous writer-sequence range and returns its durability ticket.
    pub fn submit(&self, batch: AppendBatch) -> Result<AppendTicket, TsfClientError> {
        let (ack_tx, ack_rx) = oneshot::channel();
        let terminal_error = Arc::clone(&self.shared.terminal_error);
        {
            let _guard = self
                .shared
                .submit_lock
                .read()
                .expect("writer submit lock poisoned");
            if !self.shared.is_open() {
                return Err(self.closed_error());
            }
            self.cmd_tx
                .send(WriterCommand::Submit { batch, ack_tx })
                .map_err(|_| self.closed_error())?;
        }
        Ok(AppendTicket {
            rx: ack_rx,
            terminal_error,
        })
    }

    fn closed_error(&self) -> TsfClientError {
        terminal_writer_error(
            &self.shared.terminal_error,
            TsfClientError::AppendWriterClosed,
        )
    }
}

/// Durable writer that retains unacknowledged records and resends them across transient
/// interruptions.
///
/// This controller owns the writer task and is the only handle that can close the writer. Clone
/// [`TsfProducer`] handles from [`TsfWriter::producer`] for concurrent submissions. Retryable
/// interruptions keep the writer alive with the same identity, sequence numbers, and payloads
/// until acknowledgement, [`TsfWriter::abort`], or drop.
pub struct TsfWriter {
    producer: TsfProducer,
    task: Option<JoinHandle<()>>,
    stream_kind: StreamKind,
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
        options: WriteSessionOptions,
        route: DataPlaneRoute,
        session: TsfWriteSession,
    ) -> Self {
        let stream_kind = session.stream_kind;
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let shared = Arc::new(WriterShared {
            terminal_error: Arc::new(OnceLock::new()),
            state: AtomicU8::new(WRITER_OPEN),
            submit_lock: RwLock::new(()),
        });
        let task = tokio::spawn(run_writer(
            client,
            options,
            route,
            session,
            cmd_rx,
            Arc::clone(&shared),
        ));

        Self {
            producer: TsfProducer { cmd_tx, shared },
            task: Some(task),
            stream_kind,
        }
    }

    /// Returns a cloneable submission handle for this writer.
    pub fn producer(&self) -> TsfProducer {
        self.producer.clone()
    }

    /// Returns the immutable kind reported by the stream.
    pub const fn stream_kind(&self) -> StreamKind {
        self.stream_kind
    }

    /// Queues the batch and returns its durability ticket.
    pub fn submit(&self, batch: AppendBatch) -> Result<AppendTicket, TsfClientError> {
        self.producer.submit(batch)
    }

    /// Stops accepting records, waits for every pending durability acknowledgement, and joins the
    /// writer task.
    ///
    /// Retryable interruptions do not make this return early. Use [`TsfWriter::abort`] instead
    /// when the caller does not want to wait for the service. Dropping the close future also
    /// cancels recovery.
    pub async fn close(mut self) -> Result<(), TsfClientError> {
        // The write guard waits out every concurrent submission, so any producer that still
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
            let mut abort_guard = AbortOnDrop {
                task: Some(task),
                shared: Arc::clone(&self.producer.shared),
            };
            let joined = abort_guard.task.as_mut().expect("writer task").await;
            abort_guard.task = None;
            joined.map_err(|error| TsfClientError::AppendWriterTaskFailed(error.to_string()))?;
        }

        done_rx.try_recv().map_err(|_| self.dropped_error())?
    }

    /// Immediately stops recovery and rejects pending tickets.
    ///
    /// Accepted records may already be durable. Their tickets report
    /// [`TsfClientError::AppendDurabilityUnknown`].
    pub fn abort(mut self) {
        self.abort_task(TsfClientError::AppendWriterAborted);
    }

    fn abort_task(&mut self, cause: TsfClientError) {
        let Some(task) = self.task.take() else {
            return;
        };
        let _ = self.producer.shared.terminal_error.set(Arc::new(cause));
        self.producer.shared.shutdown();
        task.abort();
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
struct AbortOnDrop {
    task: Option<JoinHandle<()>>,
    shared: Arc<WriterShared>,
}

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            let _ = self
                .shared
                .terminal_error
                .set(Arc::new(TsfClientError::AppendWriterDropped));
            self.shared.shutdown();
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
        self.abort_task(TsfClientError::AppendWriterDropped);
    }
}

impl TsfWriteSession {
    /// Returns the immutable kind reported by the stream.
    pub const fn stream_kind(&self) -> StreamKind {
        self.stream_kind
    }

    /// Sends one physical record under the progress timeout.
    pub async fn send(&mut self, record: AppendRecord) -> Result<(), TsfClientError> {
        let progress_timeout = self.progress_timeout;

        with_timeout(progress_timeout, "send append frame", async move {
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
        let progress_timeout = self.progress_timeout;
        with_timeout(progress_timeout, "append acknowledgement", self.recv_ack()).await
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
    },
    Close {
        done_tx: oneshot::Sender<Result<(), TsfClientError>>,
    },
}

/// One actor-admitted batch: one contiguous writer-sequence range starting at `start_seq_num`,
/// receipts accumulated until the whole batch is acknowledged.
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
    payload_bytes: usize,
    records: usize,
    /// Absolute: submissions can fill the queue but never postpone it, so it measures time
    /// without durability progress rather than command-channel inactivity.
    ack_deadline: Option<Instant>,
}

impl InFlightWindow {
    /// Arms the deadline for the first records to reach the wire; an armed deadline is never
    /// pushed back by later sends.
    fn arm(&mut self, progress_timeout: Duration) {
        self.ack_deadline
            .get_or_insert_with(|| Instant::now() + progress_timeout);
    }

    /// Restarts the deadline after durability progress, disarming once the window drains.
    fn restart(&mut self, progress_timeout: Duration) {
        self.ack_deadline = (self.records > 0).then(|| Instant::now() + progress_timeout);
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
    options: WriteSessionOptions,
    route: DataPlaneRoute,
    mut session: TsfWriteSession,
    mut cmd_rx: mpsc::UnboundedReceiver<WriterCommand>,
    shared: Arc<WriterShared>,
) {
    let _shutdown_guard = ShutdownGuard(Arc::clone(&shared));
    let connection = WriterReconnectContext {
        client: &client,
        options: &options,
        route,
    };
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
            if let Err(error) = recover_pending_appends(
                &mut session,
                &connection,
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
            // Resend the unacknowledged queue on the fresh session at the loop top.
            continue;
        }

        if pending.is_empty()
            && let Some(close_tx) = close_tx.take()
        {
            let _ = close_tx.send(Ok(()));
            return;
        }

        tokio::select! {
            cmd = cmd_rx.recv(), if close_tx.is_none() => {
                let Some(command) = cmd else {
                    fail_pending(
                        &mut pending,
                        &Arc::new(TsfClientError::AppendWriterDropped),
                    );
                    return;
                };
                if let Err(error) = drain_queued_commands(
                    &mut pending,
                    &mut close_tx,
                    &mut cursor,
                    &mut cmd_rx,
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
                        in_flight.restart(session.progress_timeout);
                        None
                    }
                    Ok(None) => Some(TsfClientError::WebSocketClosed),
                    Err(error) => Some(error),
                };
                if let Some(error) = recover_from
                    && let Err(error) = recover_pending_appends(
                        &mut session,
                        &connection,
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

/// Moves one command plus queued commands into `pending`, numbering each submitted batch from the
/// actor's cursor.
///
/// This adds no deliberate batching delay. A drain stops after its submissions reach either
/// dimension of the socket window, so a continuously filled channel cannot starve
/// acknowledgements. A submission is never split at this boundary. The send path remains the sole
/// owner of the actual in-flight limits and may encode the drained records in several frames.
/// Nothing awaits here, so each batch is retained for reconnect resend before any I/O can fail.
fn drain_queued_commands(
    pending: &mut VecDeque<PendingSubmission>,
    close_tx: &mut Option<oneshot::Sender<Result<(), TsfClientError>>>,
    cursor: &mut WriterCursor,
    cmd_rx: &mut mpsc::UnboundedReceiver<WriterCommand>,
    mut command: WriterCommand,
) -> Result<(), TsfClientError> {
    let mut drained_records = 0_usize;
    let mut drained_payload_bytes = 0_usize;
    loop {
        match command {
            WriterCommand::Submit { batch, ack_tx } => {
                let payloads = batch.into_payloads();
                let record_count = payloads.len();
                let payload_bytes = payloads.iter().fold(0_usize, |total, payload| {
                    total.saturating_add(payload.data.len())
                });
                let start_seq_num = cursor.reserve(record_count)?;
                pending.push_back(PendingSubmission {
                    receipts: Vec::with_capacity(record_count),
                    payloads,
                    start_seq_num,
                    acked: 0,
                    sent: 0,
                    ack_tx,
                });
                drained_records = drained_records.saturating_add(record_count);
                drained_payload_bytes = drained_payload_bytes.saturating_add(payload_bytes);
            }
            WriterCommand::Close { done_tx } => {
                *close_tx = Some(done_tx);
                return Ok(());
            }
        }
        if drained_records >= MAX_WRITER_IN_FLIGHT_RECORDS
            || drained_payload_bytes >= MAX_WRITER_IN_FLIGHT_PAYLOAD_BYTES
        {
            return Ok(());
        }
        let Ok(next) = cmd_rx.try_recv() else {
            return Ok(());
        };
        command = next;
    }
}

/// Fills `frame` with the leading unsent records and charges the in-flight window for them, bounded
/// by that window and by the protocol-frame limits.
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
            let payload_bytes = payload.data.len();
            if in_flight.records == MAX_WRITER_IN_FLIGHT_RECORDS
                || in_flight.payload_bytes + payload_bytes > MAX_WRITER_IN_FLIGHT_PAYLOAD_BYTES
                || frame.len() == MAX_APPEND_FRAME_RECORDS
                || (!frame.is_empty() && payload.data.len() > MAX_FRAME_PAYLOAD_BYTES - frame_bytes)
            {
                return;
            }
            in_flight.payload_bytes += payload_bytes;
            in_flight.records += 1;
            frame_bytes += payload.data.len();
            frame.push(submission.record(submission.sent));
            submission.sent += 1;
        }
    }
}

/// Sends unsent retained records under the in-flight window, pacing around full windows; acks
/// observed by the actor loop reopen capacity.
async fn send_pending(
    session: &mut TsfWriteSession,
    pending: &mut VecDeque<PendingSubmission>,
    in_flight: &mut InFlightWindow,
    frame: &mut Vec<AppendRecord>,
) -> Result<(), TsfClientError> {
    let progress_timeout = session.progress_timeout;

    with_timeout(progress_timeout, "send append frames", async move {
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
            in_flight.arm(progress_timeout);
        }
        Ok(())
    })
    .await
}

struct WriterReconnectContext<'a> {
    client: &'a TsfClient,
    options: &'a WriteSessionOptions,
    route: DataPlaneRoute,
}

/// Reconnects the write session and marks every unacknowledged record for paced resend.
///
/// The fresh socket carries no in-flight records, so the whole in-flight window is replaced —
/// including the acknowledgement deadline armed for the dead socket — and every sent marker
/// rewinds to its acknowledged prefix; the loop top resends the backlog under the window.
/// Retryable failures keep the actor here until recovery or task cancellation, preserving the
/// exact writer identity, sequence ranges, and payloads.
async fn recover_pending_appends(
    session: &mut TsfWriteSession,
    connection: &WriterReconnectContext<'_>,
    pending: &mut VecDeque<PendingSubmission>,
    in_flight: &mut InFlightWindow,
    reconnect_attempts: &mut usize,
    error: TsfClientError,
) -> Result<(), TsfClientError> {
    if !error.is_retryable() {
        return Err(error);
    }

    loop {
        let delay = reconnect_delay(*reconnect_attempts);
        sleep(delay).await;
        *reconnect_attempts = (*reconnect_attempts).saturating_add(1);
        match connection
            .client
            .connect_write_session_once(connection.options, connection.route)
            .await
        {
            Ok(connected) => {
                if connected.stream_kind != session.stream_kind {
                    return Err(TsfClientError::StreamKindChanged);
                }
                *session = connected;
                for submission in pending.iter_mut() {
                    submission.sent = submission.acked;
                }
                *in_flight = InFlightWindow::default();
                return Ok(());
            }
            Err(next_error) if next_error.is_retryable() => {}
            Err(next_error) => return Err(next_error),
        }
    }
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
            in_flight.payload_bytes -= submission.payloads[submission.acked].data.len();
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
    reconnects: BoundedReadReconnects,
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
                let delay = self.reconnects.next_delay()?;
                if !delay.is_zero() {
                    sleep(delay).await;
                }
                let Some(connection) = self
                    .client
                    .open_sse_connection(&self.request, self.last_event.as_ref())
                    .await?
                else {
                    self.finished = true;
                    return Ok(None);
                };
                validate_read_stream_metadata(
                    &self.options.stream_id,
                    Some(self.stream_metadata.kind),
                    &connection.stream_metadata,
                )?;
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
                    self.reconnects.reset();
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
                    self.reconnects.reset();
                }
                "error" => return Err(TsfClientError::SseTerminal(event.data)),
                "stream_metadata" => {
                    let stream_metadata = serde_json::from_str(&event.data)
                        .map_err(|_| TsfClientError::InvalidSse("invalid stream_metadata event"))?;
                    validate_read_stream_metadata(
                        &self.options.stream_id,
                        Some(self.stream_metadata.kind),
                        &stream_metadata,
                    )?;
                    self.stream_metadata = stream_metadata;
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
    route: DataPlaneRoute,
    socket: ReadSocket,
    stream_metadata: StreamMetadata,
    finished: bool,
    last_caught_up: Option<CaughtUpPosition>,
    reconnects: BoundedReadReconnects,
    reconnect_delay: Option<Duration>,
}

impl TsfReadSession {
    fn new(
        client: TsfClient,
        options: ReadOptions,
        route: DataPlaneRoute,
        socket: ReadSocket,
        stream_metadata: StreamMetadata,
    ) -> Self {
        let reconnects = BoundedReadReconnects::new(client.config.bounded_operation_attempts);
        Self {
            client,
            options,
            route,
            socket,
            stream_metadata,
            finished: false,
            last_caught_up: None,
            reconnects,
            reconnect_delay: None,
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
            if self.reconnect_delay.is_some() {
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
        let delay = self
            .reconnect_delay
            .expect("reconnect is called only when one is pending");
        if !delay.is_zero() {
            sleep(delay).await;
        }
        let ConnectedReadSocket {
            socket,
            stream_metadata,
        } = self
            .client
            .connect_read_socket(&self.options, self.route)
            .await?;
        validate_read_stream_metadata(
            &self.options.stream_id,
            Some(self.stream_metadata.kind),
            &stream_metadata,
        )?;
        self.socket = socket;
        self.stream_metadata = stream_metadata;
        self.reconnects.reset();
        self.reconnect_delay = None;
        Ok(())
    }

    fn require_reconnect(&mut self) -> Result<(), TsfClientError> {
        if self.reconnect_delay.is_none() {
            self.reconnect_delay = Some(self.reconnects.next_delay()?);
        }
        Ok(())
    }

    fn batch_delivered(&mut self, batch: &ReadBatch) {
        self.reconnects.reset();
        self.reconnect_delay = None;
        self.finished = advance_read_options_for_batch(&mut self.options, batch);
    }
}

struct BoundedReadReconnects {
    retries: usize,
    max_connection_attempts: usize,
}

impl BoundedReadReconnects {
    fn new(max_connection_attempts: usize) -> Self {
        Self {
            retries: 0,
            max_connection_attempts,
        }
    }

    fn next_delay(&mut self) -> Result<Duration, TsfClientError> {
        if self.retries >= self.max_connection_attempts.saturating_sub(1) {
            return Err(TsfClientError::ReadReconnectLimitExceeded {
                max_connection_attempts: self.max_connection_attempts,
            });
        }
        let delay = reconnect_delay(self.retries);
        self.retries += 1;
        Ok(delay)
    }

    fn reset(&mut self) {
        self.retries = 0;
    }
}

fn advance_read_options_for_batch(options: &mut ReadOptions, batch: &ReadBatch) -> bool {
    let Some(next_seq_num) = batch.last().seq_num.checked_add(1) else {
        return true;
    };
    options.start = Some(ReadStart::SeqNum(next_seq_num));
    if let Some(remaining) = options.stop.as_mut().and_then(|stop| stop.count.as_mut()) {
        *remaining = remaining.saturating_sub(batch.record_count() as u64);
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
    read_idle_timeout: Duration,
}

struct ConnectedReadSocket {
    socket: ReadSocket,
    stream_metadata: StreamMetadata,
}

impl ReadSocket {
    async fn next_outcome(&mut self) -> Result<ReadSocketOutcome, TsfClientError> {
        loop {
            let outcome = with_timeout(
                self.read_idle_timeout,
                "read stream record",
                next_read_socket_frame(&mut self.ws),
            )
            .await?;
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
    progress_timeout: Duration,
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

    timeout(progress_timeout, ws.send(Message::Binary(opening_frame)))
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

/// Chooses the smaller JSON payload key while preserving the exact record bytes.
fn compact_record_payload(bytes: &[u8]) -> JsonRecordPayload {
    let Ok(text) = std::str::from_utf8(bytes) else {
        return JsonRecordPayload::Bytes(URL_SAFE_NO_PAD.encode(bytes));
    };
    let escaped_len = bytes.iter().fold(0usize, |total, byte| {
        total + JSON_ESCAPED_LEN[*byte as usize] as usize
    });
    let text_len = br#""text":""#.len() + escaped_len;
    let bytes_len = br#""bytes":""#.len() + bytes.len().saturating_mul(4).div_ceil(3);
    if text_len <= bytes_len {
        JsonRecordPayload::Text(text.to_owned())
    } else {
        JsonRecordPayload::Bytes(URL_SAFE_NO_PAD.encode(bytes))
    }
}

/// Decodes one SSE `read_batch` event, writing each payload straight into the batch's backing
/// buffer rather than materializing it per record and concatenating afterwards.
fn sse_read_batch(batch: SseReadBatchData) -> Result<ReadBatch, TsfClientError> {
    // Only a reservation: base64 estimates round up, so `try_from_parts` owns the real bounds.
    let capacity: usize = batch
        .records
        .iter()
        .map(|record| match &record.payload {
            JsonRecordPayload::Text(text) => text.len(),
            JsonRecordPayload::Bytes(bytes) => base64::decoded_len_estimate(bytes.len()),
        })
        .sum();
    let mut payload = Vec::with_capacity(capacity);
    let mut records = Vec::with_capacity(batch.records.len());
    for record in batch.records {
        let mut writer = [0u8; WriterId::BYTE_LEN];
        let decoded_len = URL_SAFE_NO_PAD
            .decode_slice(&record.writer.id, &mut writer)
            .map_err(|_| TsfClientError::InvalidSse("invalid writer id"))?;
        if decoded_len != WriterId::BYTE_LEN {
            return Err(TsfClientError::InvalidSse("invalid writer id length"));
        }
        let data_start = payload.len();
        match record.payload {
            JsonRecordPayload::Text(text) => payload.extend_from_slice(text.as_bytes()),
            JsonRecordPayload::Bytes(bytes) => URL_SAFE_NO_PAD
                .decode_vec(&bytes, &mut payload)
                .map_err(|_| TsfClientError::InvalidSse("invalid record base64url"))?,
        }
        // An omitted part header is an unsplit record. The SSE event bound
        // caps the buffer well inside u32, so these narrows cannot truncate.
        let part = record.part.map_or_else(
            || Ok(PartHeader::unsplit()),
            |part| PartHeader::new(part.index, part.is_final),
        )?;
        records.push(RecordMeta {
            seq_num: record.seq_num,
            timestamp_ms: record.timestamp_ms,
            writer_id: WriterId::from_bytes(writer),
            writer_seq_num: record.writer.seq_num,
            part,
            data_start: data_start as u32,
            data_len: (payload.len() - data_start) as u32,
        });
    }
    ReadBatch::try_from_parts(Bytes::from(payload), records).map_err(|error| match error {
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
            Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {}
            Message::Text(_) => return Err(TsfClientError::UnexpectedTextMessage),
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

async fn expect_ready(ws: &mut ClientWebSocket) -> Result<StreamKind, TsfClientError> {
    expect_frame(ws, |frame| match frame {
        ServerFrame::Ready(kind) => Ok(kind),
        other => Err(other),
    })
    .await
}

async fn expect_read_handshake(ws: &mut ClientWebSocket) -> Result<StreamMetadata, TsfClientError> {
    let kind = expect_ready(ws).await?;
    let metadata = expect_frame(ws, |frame| match frame {
        ServerFrame::StreamMetadata(stream_metadata) => Ok(stream_metadata),
        other => Err(other),
    })
    .await?;
    if metadata.kind != kind {
        return Err(TsfClientError::StreamKindChanged);
    }
    Ok(metadata)
}

fn validate_read_stream_metadata(
    expected_stream_id: &StreamId,
    expected_kind: Option<StreamKind>,
    metadata: &StreamMetadata,
) -> Result<(), TsfClientError> {
    if metadata.stream_id != *expected_stream_id {
        return Err(TsfClientError::StreamIdChanged);
    }
    if expected_kind.is_some_and(|kind| metadata.kind != kind) {
        return Err(TsfClientError::StreamKindChanged);
    }
    Ok(())
}

fn server_frame_name(frame: &ServerFrame) -> &'static str {
    match frame {
        ServerFrame::Ready(_) => "ready",
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
        ("http_request_timeout", config.http_request_timeout),
        (
            "websocket_connect_timeout",
            config.websocket_connect_timeout,
        ),
        (
            "websocket_progress_timeout",
            config.websocket_progress_timeout,
        ),
    ] {
        if value.is_zero() || value > MAX_CLIENT_DELAY {
            return Err(TsfClientError::InvalidClientConfig(format!(
                "{name} must be greater than zero and at most {} milliseconds",
                MAX_CLIENT_DELAY.as_millis()
            )));
        }
    }
    if config.bounded_operation_attempts == 0 {
        return Err(TsfClientError::InvalidClientConfig(
            "bounded_operation_attempts must be at least one".to_owned(),
        ));
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
    /// A reconnect or metadata frame reported a different immutable stream kind.
    #[error("server reported inconsistent stream kinds")]
    StreamKindChanged,
    /// A reader handshake or metadata frame reported a different stream identity.
    #[error("server reported a different stream ID")]
    StreamIdChanged,
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
    /// The writer command channel is closed.
    #[error("append writer is closed")]
    AppendWriterClosed,
    /// The writer task ended before resolving a pending ticket.
    #[error("append writer dropped with unacknowledged records")]
    AppendWriterDropped,
    /// The writer was explicitly aborted before resolving a pending ticket.
    #[error("append writer aborted with unacknowledged records")]
    AppendWriterAborted,
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
            Self::Timeout { .. } | Self::WebSocketClosed => true,
            Self::WebSocket(error) => is_retryable_websocket_error(error),
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
        | WebSocketError::WriteBufferFull(_)
        | WebSocketError::Protocol(ProtocolError::ResetWithoutClosingHandshake) => true,
        WebSocketError::Http(response) => is_retryable_http_status(response.status().as_u16()),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_tungstenite::connect_async;

    use super::*;
    use crate::protocol::ws::frame::MAX_RECORD_PAYLOAD_BYTES;

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
        config.bounded_operation_attempts = 1;
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
            read_idle_timeout: Duration::from_millis(100),
        };

        assert!(matches!(
            socket.next_outcome().await.expect("caught-up outcome"),
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
                if server
                    .send(Message::Binary(
                        ServerFrame::Heartbeat.encode().expect("encode heartbeat"),
                    ))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });
        let mut socket = ReadSocket {
            ws: client,
            read_idle_timeout: Duration::from_secs(1),
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
    async fn sse_parser_handles_fragmentation_and_enforces_memory_bounds() {
        let payload = "event: read_batch\ndata: split 😀 payload\n\n".as_bytes();
        let event = parse_sse_chunks(payload.chunks(7).map(Bytes::copy_from_slice).collect())
            .await
            .expect("fragmented event")
            .expect("fragmented event value");
        assert_eq!(event.event, "read_batch");
        assert_eq!(event.data, "split 😀 payload");

        let oversized = vec![Bytes::from(format!(
            "event: read_batch\ndata: {}\n\n",
            "a".repeat(MAX_SSE_EVENT_BYTES)
        ))];
        assert!(matches!(
            parse_sse_chunks(oversized).await,
            Err(TsfClientError::InvalidSse("event exceeds 2 MiB"))
        ));

        let unterminated = vec![
            Bytes::from(format!(
                "event: read_batch\ndata: {}",
                "a".repeat(MAX_SSE_UNTERMINATED_EVENT_BYTES / 2)
            )),
            Bytes::from("a".repeat(MAX_SSE_UNTERMINATED_EVENT_BYTES / 2 + 1)),
        ];
        assert!(matches!(
            parse_sse_chunks(unterminated).await,
            Err(TsfClientError::InvalidSse(
                "unterminated event exceeds 2 MiB"
            ))
        ));
    }

    async fn parse_sse_chunks(
        chunks: Vec<Bytes>,
    ) -> Result<Option<ParsedSseEvent>, TsfClientError> {
        let chunks = chunks.into_iter().map(Ok::<_, reqwest::Error>);
        let mut body: SseBody = Box::pin(futures_util::stream::iter(chunks));
        next_sse_event(&mut body, &mut SseParser::default()).await
    }

    #[test]
    fn maximum_stateless_record_stays_within_the_json_bound() {
        let request = AppendRecordsRequest {
            writer: Some(AppendWriter {
                id: URL_SAFE_NO_PAD.encode([0_u8; 16]),
                seq_num: 0,
            }),
            records: vec![AppendJsonRecord {
                part: None,
                payload: compact_record_payload(&vec![0_u8; MAX_RECORD_PAYLOAD_BYTES]),
            }],
            expected_next_seq_num: None,
        };

        assert!(
            serde_json::to_vec(&request)
                .expect("append request JSON")
                .len()
                <= crate::protocol::rest::MAX_STATELESS_APPEND_JSON_BYTES
        );
    }
}
