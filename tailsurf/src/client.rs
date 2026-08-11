//! Bounded REST and WebSocket clients for the TSF service.

use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use reqwest::StatusCode;
use serde::{Deserialize, de::DeserializeOwned};
use tokio::{
    net::TcpStream,
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot},
    task::JoinHandle,
    time::{sleep, timeout},
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{
        Error as WebSocketError, Message,
        client::IntoClientRequest,
        error::ProtocolError,
        http::{HeaderValue, header::SEC_WEBSOCKET_PROTOCOL},
    },
};
use url::Url;

use crate::{
    BearerToken, StreamId, TokenId,
    protocol::{
        rest::{
            CreateStreamRequest, CreateStreamResponse, IssueTokenRequest, IssueTokenResponse,
            ListTokensResponse, RevokeTokenRequest, StreamInfoResponse, StreamRangeResponse,
            StreamTailResponse, UpdateStreamRequest,
        },
        ws::{
            ReadStart, ReadStreamOptions, WriteStreamOptions,
            frame::{
                ClientFrame, FrameCodecError, MAX_RECORD_BYTES, PartHeader, ReadRecord, ReadTail,
                RecordFormat, ServerFrame, TSF_V3, TSF_WS_PROTOCOL,
            },
        },
    },
};
use secrecy::ExposeSecret;

type ClientWebSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

const API_PREFIX: &str = "/api/v1";

/// Timeouts, retry behavior, API origin, and optional account authorization for [`TsfClient`].
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
    /// Optional idle timeout while waiting for a read frame. Protocol heartbeats reset the timer. `None` waits indefinitely.
    pub websocket_read_idle_timeout: Option<Duration>,
    /// Retry policy for idempotent metadata reads and initial socket setup.
    pub retry_policy: RetryPolicy,
    /// Optional account bearer token sent on REST requests.
    pub rest_bearer_token: Option<BearerToken>,
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
            rest_bearer_token: None,
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

/// Cloneable TSF control-plane and v3 data-plane client.
///
/// Mutating REST operations are not retried because a timeout may occur after the service applies the mutation. Metadata reads and initial socket setup use [`RetryPolicy`]. Durable writer recovery is owned by [`TsfProducer`].
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

    /// Creates a client with an explicit API origin and account bearer token for REST requests.
    pub fn with_api_base_url_and_rest_bearer_token(
        api_base_url: Url,
        bearer_token: impl Into<BearerToken>,
    ) -> Self {
        let mut config = TsfClientConfig::new(api_base_url);
        config.rest_bearer_token = Some(bearer_token.into());
        Self::with_config(config)
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

    /// Creates a stream and returns its metadata and newly issued secret tokens.
    ///
    /// This mutation is attempted once and is not transparently retried.
    pub async fn create_stream(
        &self,
        request: &CreateStreamRequest,
    ) -> Result<CreateStreamResponse, TsfClientError> {
        self.send_json(
            self.http.post(self.rest_url("/streams")).json(request),
            "create stream",
        )
        .await
    }

    /// Retrieves current stream metadata, retrying transient failures according to policy.
    pub async fn get_stream(
        &self,
        stream_id: &StreamId,
    ) -> Result<StreamInfoResponse, TsfClientError> {
        self.get_json(format!("/streams/{stream_id}"), "get stream")
            .await
    }

    /// Retrieves the current durable stream tail, retrying transient failures according to policy.
    pub async fn get_stream_tail(
        &self,
        stream_id: &StreamId,
    ) -> Result<StreamTailResponse, TsfClientError> {
        self.get_json(format!("/streams/{stream_id}/tail"), "check stream tail")
            .await
    }

    /// Retrieves retained stream bounds, retrying transient failures according to policy.
    pub async fn get_stream_range(
        &self,
        stream_id: &StreamId,
    ) -> Result<StreamRangeResponse, TsfClientError> {
        self.get_json(format!("/streams/{stream_id}/range"), "check stream range")
            .await
    }

    /// Updates owner-controlled stream settings.
    ///
    /// This mutation is attempted once and is not transparently retried.
    pub async fn update_stream(
        &self,
        stream_id: &StreamId,
        request: &UpdateStreamRequest,
    ) -> Result<StreamInfoResponse, TsfClientError> {
        self.send_json(
            self.http
                .patch(self.rest_url(&format!("/streams/{stream_id}")))
                .json(request),
            "update stream",
        )
        .await
    }

    /// Permanently deletes a stream.
    ///
    /// This mutation is attempted once and is not transparently retried.
    pub async fn delete_stream(&self, stream_id: &StreamId) -> Result<(), TsfClientError> {
        self.send_empty(
            self.http
                .delete(self.rest_url(&format!("/streams/{stream_id}"))),
            "delete stream",
        )
        .await
    }

    /// Issues a new secret stream token.
    ///
    /// This mutation is attempted once and is not transparently retried.
    pub async fn issue_token(
        &self,
        stream_id: &StreamId,
        request: &IssueTokenRequest,
    ) -> Result<IssueTokenResponse, TsfClientError> {
        self.send_json(
            self.http
                .post(self.rest_url(&format!("/streams/{stream_id}/tokens")))
                .json(request),
            "issue token",
        )
        .await
    }

    /// Lists retained, non-secret token metadata, retrying transient failures according to policy.
    pub async fn list_tokens(
        &self,
        stream_id: &StreamId,
    ) -> Result<ListTokensResponse, TsfClientError> {
        self.get_json(format!("/streams/{stream_id}/tokens"), "list tokens")
            .await
    }

    /// Revokes a stream token by its non-secret identifier.
    ///
    /// This mutation is attempted once and is not transparently retried.
    pub async fn revoke_token(
        &self,
        stream_id: &StreamId,
        token_id: &TokenId,
    ) -> Result<(), TsfClientError> {
        let request = RevokeTokenRequest {
            token_id: *token_id,
        };
        self.send_empty(
            self.http
                .delete(self.rest_url(&format!("/streams/{stream_id}/tokens")))
                .json(&request),
            "revoke token",
        )
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
        let url = self.websocket_url(&format!("/streams/{}/write", options.stream_id), &[])?;
        let connect_timeout = self.config.websocket_connect_timeout;
        let operation_timeout = self.config.websocket_operation_timeout;

        self.retry_transient(|| {
            let url = url.clone();
            let options = options.clone();

            async move {
                let mut ws = connect_websocket(url, connect_timeout).await?;
                with_timeout(
                    operation_timeout,
                    "authenticate writer",
                    send_client_frame(
                        &mut ws,
                        ClientFrame::AuthWrite {
                            writer_id: options.writer_id,
                            bearer_token: options.bearer_token,
                        },
                    ),
                )
                .await?;
                with_timeout(operation_timeout, "writer hello", expect_hello(&mut ws)).await?;

                Ok(TsfAppendSession {
                    ws,
                    operation_timeout,
                })
            }
        })
        .await
    }

    /// Connects a resumable read session at the requested position and bounds.
    ///
    /// A relative tail offset is resolved to an absolute S2 sequence number before the first socket is opened. Reconnects therefore resume from the same position even if the stream advances before a record arrives.
    pub async fn connect_reader(
        &self,
        mut options: ReadStreamOptions,
    ) -> Result<TsfReadSession, TsfClientError> {
        if let Some(ReadStart::TailOffset(offset)) = options.start {
            let tail = self
                .get_stream_tail_with_bearer(&options.stream_id, options.bearer_token.as_ref())
                .await?;
            options.start = Some(ReadStart::SeqNum(
                tail.next_s2_seq_num.saturating_sub(offset),
            ));
        }
        let socket = self.connect_read_socket(options.clone()).await?;
        Ok(TsfReadSession::new(self.clone(), options, socket))
    }

    async fn connect_read_socket(
        &self,
        options: ReadStreamOptions,
    ) -> Result<ReadSocket, TsfClientError> {
        let query = options.query_pairs();
        let url = self.websocket_url(&format!("/streams/{}/read", options.stream_id), &query)?;
        let connect_timeout = self.config.websocket_connect_timeout;
        let operation_timeout = self.config.websocket_operation_timeout;
        let read_idle_timeout = self.config.websocket_read_idle_timeout;

        self.retry_transient(|| {
            let url = url.clone();
            let bearer_token = options.bearer_token.clone();

            async move {
                let mut ws = connect_websocket(url, connect_timeout).await?;

                match with_timeout(
                    operation_timeout,
                    "reader hello",
                    next_server_frame(&mut ws),
                )
                .await?
                {
                    Some(ServerFrame::Hello { version }) => ensure_protocol_version(version)?,
                    Some(ServerFrame::AuthRequired) => {
                        let bearer_token = bearer_token.ok_or(TsfClientError::MissingReadToken)?;
                        with_timeout(
                            operation_timeout,
                            "authenticate reader",
                            send_client_frame(&mut ws, ClientFrame::AuthRead { bearer_token }),
                        )
                        .await?;
                        with_timeout(operation_timeout, "reader hello", expect_hello(&mut ws))
                            .await?;
                    }
                    Some(frame) => {
                        return Err(TsfClientError::UnexpectedServerFrame(server_frame_name(
                            &frame,
                        )));
                    }
                    None => return Err(TsfClientError::WebSocketClosed),
                }

                Ok(ReadSocket {
                    ws,
                    read_idle_timeout,
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
        bearer_token: Option<&BearerToken>,
    ) -> reqwest::RequestBuilder {
        if let Some(token) = bearer_token.or(self.config.rest_bearer_token.as_ref()) {
            request.bearer_auth(token.expose_secret())
        } else {
            request
        }
    }

    fn websocket_url(
        &self,
        path: &str,
        query: &[(&'static str, String)],
    ) -> Result<Url, TsfClientError> {
        let mut url = self.rest_url(path);
        let scheme = match url.scheme() {
            "http" => "ws",
            "https" => "wss",
            other => return Err(TsfClientError::InvalidWebSocketScheme(other.to_owned())),
        };
        url.set_scheme(scheme)
            .map_err(|_| TsfClientError::InvalidWebSocketScheme(url.scheme().to_owned()))?;

        if !query.is_empty() {
            url.query_pairs_mut()
                .extend_pairs(query.iter().map(|(key, value)| (*key, value.as_str())));
        }

        Ok(url)
    }

    async fn get_json<T: DeserializeOwned>(
        &self,
        path: String,
        operation: &'static str,
    ) -> Result<T, TsfClientError> {
        self.get_json_with_bearer(path, operation, None).await
    }

    async fn get_stream_tail_with_bearer(
        &self,
        stream_id: &StreamId,
        bearer_token: Option<&BearerToken>,
    ) -> Result<StreamTailResponse, TsfClientError> {
        self.get_json_with_bearer(
            format!("/streams/{stream_id}/tail"),
            "check stream tail",
            bearer_token,
        )
        .await
    }

    async fn get_json_with_bearer<T: DeserializeOwned>(
        &self,
        path: String,
        operation: &'static str,
        bearer_token: Option<&BearerToken>,
    ) -> Result<T, TsfClientError> {
        let url = self.rest_url(&path);
        self.retry_transient(|| {
            self.send_json_with_bearer(self.http.get(url.clone()), operation, bearer_token)
        })
        .await
    }

    async fn send_json<T: DeserializeOwned>(
        &self,
        request: reqwest::RequestBuilder,
        operation: &'static str,
    ) -> Result<T, TsfClientError> {
        self.send_json_with_bearer(request, operation, None).await
    }

    async fn send_json_with_bearer<T: DeserializeOwned>(
        &self,
        request: reqwest::RequestBuilder,
        operation: &'static str,
        bearer_token: Option<&BearerToken>,
    ) -> Result<T, TsfClientError> {
        let response = self
            .apply_rest_auth(request, bearer_token)
            .timeout(self.config.rest_request_timeout)
            .send()
            .await?;
        json_response(response, operation).await
    }

    async fn send_empty(
        &self,
        request: reqwest::RequestBuilder,
        operation: &'static str,
    ) -> Result<(), TsfClientError> {
        let response = self
            .apply_rest_auth(request, None)
            .timeout(self.config.rest_request_timeout)
            .send()
            .await?;
        let status = response.status();
        if status == StatusCode::NO_CONTENT {
            return Ok(());
        }
        Err(TsfClientError::HttpStatus {
            operation,
            status,
            body: http_status_body(response).await,
        })
    }

    async fn retry_transient<T, Fut>(
        &self,
        mut run: impl FnMut() -> Fut,
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
                Err(error) if attempt < attempts && error.is_retryable() => {
                    if !backoff.is_zero() {
                        sleep(backoff).await;
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

/// Low-level authenticated write socket without retained-record recovery.
pub struct TsfAppendSession {
    ws: ClientWebSocket,
    operation_timeout: Duration,
}

/// Memory, concurrency, and reconnect bounds for [`TsfProducer`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TsfProducerConfig {
    /// Maximum total payload bytes retained until durability acknowledgement.
    pub max_unacked_bytes: usize,
    /// Maximum number of records retained until durability acknowledgement.
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
        if self.max_unacked_bytes > u32::MAX as usize {
            return Err(TsfClientError::InvalidProducerConfig(format!(
                "max_unacked_bytes must not exceed {}",
                u32::MAX
            )));
        }
        if self.max_unacked_records == 0 {
            return Err(TsfClientError::InvalidProducerConfig(
                "max_unacked_records must be greater than zero".to_owned(),
            ));
        }
        if self.max_unacked_records >= Semaphore::MAX_PERMITS {
            return Err(TsfClientError::InvalidProducerConfig(format!(
                "max_unacked_records must be less than {}",
                Semaphore::MAX_PERMITS
            )));
        }
        Ok(self)
    }
}

impl Default for TsfProducerConfig {
    fn default() -> Self {
        Self {
            max_unacked_bytes: 5 * 1024 * 1024,
            max_unacked_records: 128,
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

/// Server acknowledgement mapping a contiguous writer range to durable S2 sequence numbers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AppendAck {
    /// First acknowledged writer-local sequence number.
    pub writer_seq_start: u64,
    /// Last acknowledged writer-local sequence number, inclusive.
    pub writer_seq_end: u64,
    /// S2 sequence number assigned to the first acknowledged record.
    pub s2_seq_start: u64,
    /// S2 sequence number assigned to the last acknowledged record, inclusive.
    pub s2_seq_end: u64,
}

impl AppendAck {
    /// Returns whether the inclusive writer range contains a sequence number.
    pub const fn contains_writer_seq(self, writer_seq_num: u64) -> bool {
        self.writer_seq_start <= writer_seq_num && writer_seq_num <= self.writer_seq_end
    }

    /// Returns the number of records when writer and S2 ranges are valid and equal in length.
    pub fn record_count(self) -> Result<u64, TsfClientError> {
        let writer_count = inclusive_range_len(self.writer_seq_start, self.writer_seq_end)
            .ok_or(TsfClientError::InvalidAppendAck(self))?;
        let s2_count = inclusive_range_len(self.s2_seq_start, self.s2_seq_end)
            .ok_or(TsfClientError::InvalidAppendAck(self))?;
        if writer_count != s2_count {
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
    /// Durable S2 sequence number assigned by the service.
    pub s2_seq_num: u64,
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

/// Bounded durable producer that retains unacknowledged records and resends them across transient interruptions.
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

    /// Stops accepting records, waits for every pending durability acknowledgement, and joins the producer task.
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

impl IntoRecordData for Bytes {
    fn into_record_data(self) -> Bytes {
        self
    }
}

impl IntoRecordData for &Bytes {
    fn into_record_data(self) -> Bytes {
        self.clone()
    }
}

impl IntoRecordData for Vec<u8> {
    fn into_record_data(self) -> Bytes {
        Bytes::from(self)
    }
}

impl IntoRecordData for Box<[u8]> {
    fn into_record_data(self) -> Bytes {
        Bytes::from(self)
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

impl IntoRecordData for String {
    fn into_record_data(self) -> Bytes {
        Bytes::from(self)
    }
}

impl IntoRecordData for &str {
    fn into_record_data(self) -> Bytes {
        Bytes::copy_from_slice(self.as_bytes())
    }
}

impl TsfAppendSession {
    /// Sends one physical append frame after validating its size.
    pub async fn send(&mut self, record: WriteRecord) -> Result<(), TsfClientError> {
        record.validate()?;
        with_timeout(
            self.operation_timeout,
            "send append frame",
            send_client_frame(
                &mut self.ws,
                ClientFrame::AppendRecord {
                    writer_seq_num: record.writer_seq_num,
                    part: record.part,
                    format: record.format,
                    data: record.data,
                },
            ),
        )
        .await
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
                s2_seq_start,
                s2_seq_end,
            }) => AppendAck {
                writer_seq_start,
                writer_seq_end,
                s2_seq_start,
                s2_seq_end,
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
                    Some(ProducerCommand::Submit {
                        record,
                        ack_tx,
                        byte_permit,
                        record_permit,
                    }) => {
                        let record_to_send = record.clone();
                        pending.push_back(PendingAppend {
                            record,
                            ack_tx,
                            _byte_permit: byte_permit,
                            _record_permit: record_permit,
                        });
                        if let Err(error) = session.send(record_to_send).await
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
                    Some(ProducerCommand::Close { done_tx }) => {
                        close_tx = Some(done_tx);
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
            Ok(mut connected) => match resend_pending(&mut connected, pending).await {
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

async fn resend_pending(
    session: &mut TsfAppendSession,
    pending: &VecDeque<PendingAppend>,
) -> Result<(), TsfClientError> {
    for pending in pending {
        session.send(pending.record.clone()).await?;
    }
    Ok(())
}

fn dispatch_ack(
    ack: AppendAck,
    pending: &mut VecDeque<PendingAppend>,
) -> Result<(), TsfClientError> {
    let record_count =
        usize::try_from(ack.record_count()?).map_err(|_| TsfClientError::InvalidAppendAck(ack))?;

    for offset in 0..record_count {
        let offset = u64::try_from(offset).map_err(|_| TsfClientError::InvalidAppendAck(ack))?;
        let writer_seq_num = ack
            .writer_seq_start
            .checked_add(offset)
            .ok_or(TsfClientError::InvalidAppendAck(ack))?;
        let s2_seq_num = ack
            .s2_seq_start
            .checked_add(offset)
            .ok_or(TsfClientError::InvalidAppendAck(ack))?;
        let Some(front) = pending.front() else {
            return Err(TsfClientError::InvalidAppendAck(ack));
        };
        if front.record.writer_seq_num < writer_seq_num {
            return Err(TsfClientError::AppendNotAcknowledged {
                writer_seq_num: front.record.writer_seq_num,
                ack,
            });
        }
        if front.record.writer_seq_num > writer_seq_num {
            return Err(TsfClientError::InvalidAppendAck(ack));
        }

        let pending = pending
            .pop_front()
            .expect("pending front should exist after checking it");
        let _ = pending.ack_tx.send(Ok(AppendReceipt {
            writer_seq_num,
            s2_seq_num,
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
/// Transient transport and service interruptions reconnect from the next S2 sequence number. Normal completion and configured bounds return `None`; protocol and policy failures surface as errors.
pub struct TsfReadSession {
    client: TsfClient,
    options: ReadStreamOptions,
    socket: ReadSocket,
    finished: bool,
    last_observed_tail: Option<ReadTail>,
}

impl TsfReadSession {
    fn new(client: TsfClient, options: ReadStreamOptions, socket: ReadSocket) -> Self {
        Self {
            client,
            options,
            socket,
            finished: false,
            last_observed_tail: None,
        }
    }

    /// Returns the latest tail reported by the active read session.
    pub const fn last_observed_tail(&self) -> Option<ReadTail> {
        self.last_observed_tail
    }

    /// Waits for the next physical record using the configured idle timeout.
    pub async fn next_record(&mut self) -> Result<Option<ReadRecord>, TsfClientError> {
        self.next_record_inner(None).await
    }

    /// Waits for the next physical record with a caller-supplied timeout for this operation.
    pub async fn next_record_with_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<ReadRecord>, TsfClientError> {
        self.next_record_inner(Some(timeout)).await
    }

    async fn next_record_inner(
        &mut self,
        timeout: Option<Duration>,
    ) -> Result<Option<ReadRecord>, TsfClientError> {
        loop {
            if self.finished || read_options_exhausted(&self.options) {
                self.finished = true;
                return Ok(None);
            }

            match self.next_socket_outcome(timeout).await {
                Ok(ReadSocketOutcome::Record(record)) => {
                    self.record_delivered(record.s2_seq_num);
                    return Ok(Some(record));
                }
                Ok(ReadSocketOutcome::Tail(tail)) => {
                    self.last_observed_tail = Some(tail);
                }
                Ok(ReadSocketOutcome::ReconnectAdvised) => {
                    self.reconnect().await?;
                }
                Ok(ReadSocketOutcome::Closed) => {
                    self.finished = true;
                    return Ok(None);
                }
                Err(error) if error.is_resumable_read_interruption() => {
                    self.reconnect().await?;
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn next_socket_outcome(
        &mut self,
        timeout: Option<Duration>,
    ) -> Result<ReadSocketOutcome, TsfClientError> {
        if let Some(timeout) = timeout {
            self.socket.next_outcome_with_timeout(timeout).await
        } else {
            self.socket.next_outcome().await
        }
    }

    async fn reconnect(&mut self) -> Result<(), TsfClientError> {
        self.socket = self
            .client
            .connect_read_socket(self.options.clone())
            .await?;
        Ok(())
    }

    fn record_delivered(&mut self, s2_seq_num: u64) {
        match s2_seq_num.checked_add(1) {
            Some(next_seq_num) => self.options.start = Some(ReadStart::SeqNum(next_seq_num)),
            None => self.finished = true,
        }

        if let Some(count) = self.options.count.as_mut() {
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.finished = true;
            }
        }

        if self.options.until.is_some_and(|until| s2_seq_num >= until) {
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

    async fn next_outcome_with_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<ReadSocketOutcome, TsfClientError> {
        with_timeout(
            timeout,
            "read stream record",
            next_read_socket_outcome(&mut self.ws),
        )
        .await
    }
}

enum ReadSocketOutcome {
    Record(ReadRecord),
    Tail(ReadTail),
    ReconnectAdvised,
    Closed,
}

async fn connect_websocket(
    url: Url,
    connect_timeout: Duration,
) -> Result<ClientWebSocket, TsfClientError> {
    let mut request = url.as_str().into_client_request()?;
    request.headers_mut().insert(
        SEC_WEBSOCKET_PROTOCOL,
        HeaderValue::from_static(TSF_WS_PROTOCOL),
    );

    let (ws, response) = timeout(connect_timeout, connect_async(request))
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

async fn json_response<T: DeserializeOwned>(
    response: reqwest::Response,
    operation: &'static str,
) -> Result<T, TsfClientError> {
    let status = response.status();
    if !status.is_success() {
        let body = http_status_body(response).await;
        return Err(TsfClientError::HttpStatus {
            operation,
            status,
            body,
        });
    }

    Ok(response.json().await?)
}

async fn http_status_body(response: reqwest::Response) -> String {
    let body = response.text().await.unwrap_or_default();
    api_error_message(&body).unwrap_or(body)
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

async fn send_client_frame(
    ws: &mut ClientWebSocket,
    frame: ClientFrame,
) -> Result<(), TsfClientError> {
    ws.send(Message::Binary(frame.encode()?)).await?;
    Ok(())
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

async fn next_read_socket_outcome(
    ws: &mut ClientWebSocket,
) -> Result<ReadSocketOutcome, TsfClientError> {
    loop {
        if let Some(outcome) = next_read_socket_frame(ws).await? {
            return Ok(outcome);
        }
    }
}

async fn next_read_socket_frame(
    ws: &mut ClientWebSocket,
) -> Result<Option<ReadSocketOutcome>, TsfClientError> {
    match next_server_frame(ws).await? {
        Some(ServerFrame::ReadRecord(record)) => Ok(Some(ReadSocketOutcome::Record(record))),
        Some(ServerFrame::ReadTail(tail)) => Ok(Some(ReadSocketOutcome::Tail(tail))),
        Some(ServerFrame::Heartbeat) => Ok(None),
        Some(ServerFrame::ReconnectAdvised { .. }) => Ok(Some(ReadSocketOutcome::ReconnectAdvised)),
        Some(frame) => Err(TsfClientError::UnexpectedServerFrame(server_frame_name(
            &frame,
        ))),
        None => Ok(Some(ReadSocketOutcome::Closed)),
    }
}

async fn expect_hello(ws: &mut ClientWebSocket) -> Result<(), TsfClientError> {
    match next_server_frame(ws).await? {
        Some(ServerFrame::Hello { version }) => ensure_protocol_version(version),
        Some(frame) => Err(TsfClientError::UnexpectedServerFrame(server_frame_name(
            &frame,
        ))),
        None => Err(TsfClientError::WebSocketClosed),
    }
}

fn ensure_protocol_version(version: u16) -> Result<(), TsfClientError> {
    if version == TSF_V3 {
        Ok(())
    } else {
        Err(TsfClientError::UnsupportedProtocolVersion(version))
    }
}

fn server_frame_name(frame: &ServerFrame) -> &'static str {
    match frame {
        ServerFrame::Hello { .. } => "hello",
        ServerFrame::AuthRequired => "auth required",
        ServerFrame::Ack { .. } => "ack",
        ServerFrame::ReadRecord(_) => "read record",
        ServerFrame::Heartbeat => "heartbeat",
        ServerFrame::ReconnectAdvised { .. } => "reconnect advised",
        ServerFrame::ReadTail(_) => "read tail",
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
    },
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
    /// The server did not select `tsf.v3` during upgrade.
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
    /// A private read requested authentication but no token was configured.
    #[error("private stream read requires a bearer token")]
    MissingReadToken,
    /// The service sent a valid TSF frame that is not allowed at this protocol state.
    #[error("server sent unexpected {0} frame")]
    UnexpectedServerFrame(&'static str),
    /// The server selected a TSF protocol version unsupported by this client.
    #[error("server sent unsupported protocol version {0}")]
    UnsupportedProtocolVersion(u16),
    /// The server sent a text WebSocket message instead of one binary TSF frame.
    #[error("server sent an unexpected text WebSocket message")]
    UnexpectedTextMessage,
}

impl TsfClientError {
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
    fn default_config_uses_api_origin() {
        let config = TsfClientConfig::default();

        assert_eq!(config.api_base_url, default_api_base_url());
        assert_eq!(config.rest_request_timeout, Duration::from_secs(10));
        assert_eq!(config.websocket_connect_timeout, Duration::from_secs(10));
        assert_eq!(config.websocket_operation_timeout, Duration::from_secs(30));
        assert_eq!(
            config.websocket_read_idle_timeout,
            Some(Duration::from_secs(60))
        );
        assert_eq!(config.retry_policy, RetryPolicy::default());
        assert!(config.rest_bearer_token.is_none());
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
                    ServerFrame::ReadTail(ReadTail {
                        next_s2_seq_num: 42,
                        timestamp_ms: 1_786_377_600_000,
                    })
                    .encode()
                    .expect("encode read tail"),
                ))
                .await
                .expect("send read tail");
        });
        let mut socket = ReadSocket {
            ws: client,
            read_idle_timeout: Some(Duration::from_millis(100)),
        };

        let outcome = socket.next_outcome().await.expect("read tail outcome");

        assert!(matches!(
            outcome,
            ReadSocketOutcome::Tail(ReadTail {
                next_s2_seq_num: 42,
                timestamp_ms: 1_786_377_600_000,
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

        let result = socket
            .next_outcome_with_timeout(Duration::from_millis(100))
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
    fn retry_policy_always_attempts_at_least_once() {
        let retry_policy = RetryPolicy {
            max_attempts: 0,
            initial_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
        };

        assert_eq!(retry_policy.attempt_count(), 1);
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
    fn builds_versioned_websocket_urls_with_read_query() {
        let client =
            TsfClient::with_api_base_url(Url::parse("https://example.com").expect("API origin"));

        assert_eq!(
            client
                .websocket_url(
                    "/streams/0123456789abcdefghjkmnpqrstvwxyz/read",
                    &[("seq_num", "42".to_owned()), ("count", "3".to_owned())],
                )
                .expect("WebSocket URL")
                .as_str(),
            "wss://example.com/api/v1/streams/0123456789abcdefghjkmnpqrstvwxyz/read?seq_num=42&count=3"
        );
    }

    #[test]
    fn append_ack_counts_inclusive_matching_ranges() {
        let ack = AppendAck {
            writer_seq_start: 7,
            writer_seq_end: 9,
            s2_seq_start: 42,
            s2_seq_end: 44,
        };

        assert_eq!(ack.record_count().expect("record count"), 3);
        assert_eq!(ack.validate().expect("valid ack"), ack);
    }

    #[test]
    fn append_ack_rejects_mismatched_range_lengths() {
        let ack = AppendAck {
            writer_seq_start: 7,
            writer_seq_end: 9,
            s2_seq_start: 42,
            s2_seq_end: 43,
        };

        assert!(matches!(
            ack.record_count(),
            Err(TsfClientError::InvalidAppendAck(error_ack)) if error_ack == ack
        ));
    }

    #[test]
    fn api_error_message_extracts_stable_code_and_message() {
        let body = r#"{"error":{"code":"forbidden","message":"owner token required"}}"#;

        assert_eq!(
            api_error_message(body).as_deref(),
            Some("forbidden: owner token required")
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
