//! `tsf` command-line client for creating, writing, replaying, tailing, and managing Tailsurf streams.

use std::{
    collections::{BTreeMap, VecDeque},
    fs::OpenOptions,
    path::{Path, PathBuf},
    process::{ExitStatus, Stdio},
    str::FromStr,
};

use bytes::{Buf, Bytes, BytesMut};
use clap::{Args, Parser, Subcommand, ValueEnum};
use eyre::{Context, ContextCompat, bail};
use secrecy::ExposeSecret;
use serde::Serialize;
use tailsurf::{
    AppendTicket, BearerToken, StreamId, TokenId, TokenPermissions, TsfClient, TsfProducer,
    WriteRecord, WriterId,
    protocol::{
        rest::{
            CreateStreamRequest, CreateStreamResponse, IssueTokenRequest, IssueTokenResponse,
            IssuedStreamToken, RequestedRetention, StreamInfoResponse, StreamTokenStatus,
            UpdateStreamRequest, Visibility,
        },
        ws::{
            ReadStart, ReadStreamOptions, WriteStreamOptions,
            frame::{MAX_RECORD_BYTES, PartHeader, RecordFormat},
        },
    },
    stream_url::{StreamLocator, stream_url},
    transcript::{DEFAULT_MAX_LOGICAL_RECORD_BYTES, LogicalTranscript, TranscriptRecord},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    process::Command as TokioCommand,
    sync::mpsc,
    time::{Duration, Instant, sleep_until},
};
use url::Url;

const INTERRUPT_EXIT_CODE: i32 = 130;
const RAW_LINGER: Duration = Duration::from_millis(10);

#[derive(Debug, Parser)]
#[command(name = "tsf")]
#[command(about = "tail.surf command line client")]
struct Cli {
    #[arg(
        long = "api-url",
        env = "TSF_API_URL",
        default_value = "https://tail.surf"
    )]
    api_url: Url,
    #[arg(
        long = "web-url",
        env = "TSF_WEB_URL",
        default_value = "https://tail.surf"
    )]
    web_url: Url,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    New(NewArgs),
    Write(WriteArgs),
    Tail(TailArgs),
    Replay(ReplayArgs),
    Delete(OwnerUrlArgs),
    Visibility(VisibilityArgs),
    Token(TokenArgs),
    ParseUrl { url: String },
}

#[derive(Debug, Args)]
struct NewArgs {
    #[arg(long, conflicts_with = "private")]
    public: bool,
    #[arg(long, conflicts_with = "public")]
    private: bool,
    #[arg(long = "token", value_name = "PERMISSIONS")]
    tokens: Vec<TokenPermissions>,
    #[arg(
        long,
        value_name = "DURATION",
        help = "Record retention, such as 6h, 7d, or infinite"
    )]
    retention: Option<RetentionArg>,
    #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
    format: OutputFormat,
    #[arg(long = "owner-token-file", value_name = "PATH")]
    owner_token_file: Option<PathBuf>,
    #[arg(long = "read-token-file", value_name = "PATH")]
    read_token_file: Option<PathBuf>,
    #[arg(long = "write-token-file", value_name = "PATH")]
    write_token_file: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct WriteArgs {
    url: Option<String>,
    #[arg(long)]
    new: bool,
    #[arg(long, conflicts_with = "private")]
    public: bool,
    #[arg(long, conflicts_with = "public")]
    private: bool,
    #[arg(
        long,
        value_name = "DURATION",
        requires = "new",
        help = "New-stream record retention, such as 6h, 7d, or infinite"
    )]
    retention: Option<RetentionArg>,
    #[arg(long)]
    raw: bool,
    #[arg(last = true, value_name = "COMMAND")]
    command: Vec<String>,
}

#[derive(Debug, Args)]
struct TailArgs {
    url: String,
    #[arg(short = 'n', long, conflicts_with_all = ["seq_num", "timestamp"])]
    tail_offset: Option<u64>,
    #[arg(long, conflicts_with = "timestamp")]
    seq_num: Option<u64>,
    #[arg(long, conflicts_with = "seq_num")]
    timestamp: Option<u64>,
    #[arg(long)]
    count: Option<u64>,
    #[arg(long, value_name = "BYTES", default_value_t = DEFAULT_MAX_LOGICAL_RECORD_BYTES)]
    max_logical_record_bytes: usize,
}

#[derive(Debug, Args)]
struct ReplayArgs {
    url: String,
    #[arg(long, conflicts_with = "timestamp")]
    seq_num: Option<u64>,
    #[arg(long, conflicts_with = "seq_num")]
    timestamp: Option<u64>,
    #[arg(long)]
    count: Option<u64>,
    #[arg(long, value_name = "BYTES", default_value_t = DEFAULT_MAX_LOGICAL_RECORD_BYTES)]
    max_logical_record_bytes: usize,
}

#[derive(Debug, Args)]
struct OwnerUrlArgs {
    url: String,
}

#[derive(Debug, Args)]
struct VisibilityArgs {
    url: String,
    visibility: VisibilityArg,
    #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
    format: OutputFormat,
}

#[derive(Debug, Args)]
struct TokenArgs {
    #[command(subcommand)]
    command: TokenCommand,
}

#[derive(Debug, Subcommand)]
enum TokenCommand {
    List(ListTokenArgs),
    Issue(IssueTokenArgs),
    Revoke(RevokeTokenArgs),
}

#[derive(Debug, Args)]
struct ListTokenArgs {
    url: String,
    #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
    format: OutputFormat,
}

#[derive(Debug, Args)]
struct IssueTokenArgs {
    url: String,
    #[arg(long = "token", value_name = "PERMISSIONS")]
    permissions: TokenPermissions,
    #[arg(long)]
    expires_at: Option<String>,
    #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
    format: OutputFormat,
    #[arg(long = "token-file", value_name = "PATH")]
    token_file: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct RevokeTokenArgs {
    url: String,
    token_id: TokenId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum VisibilityArg {
    Private,
    Public,
}

impl From<VisibilityArg> for Visibility {
    fn from(value: VisibilityArg) -> Self {
        match value {
            VisibilityArg::Private => Self::Private,
            VisibilityArg::Public => Self::Public,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum OutputFormat {
    Text,
    Json,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RetentionArg {
    Seconds(u64),
    Infinite,
}

impl FromStr for RetentionArg {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.eq_ignore_ascii_case("infinite") {
            return Ok(Self::Infinite);
        }
        let duration = humantime::parse_duration(value)
            .map_err(|error| format!("invalid retention duration: {error}"))?;
        if duration.is_zero() {
            return Err("retention must be at least one second".to_owned());
        }
        if duration.subsec_nanos() != 0 {
            return Err("retention must be a whole number of seconds".to_owned());
        }
        Ok(Self::Seconds(duration.as_secs()))
    }
}

impl From<RetentionArg> for RequestedRetention {
    fn from(value: RetentionArg) -> Self {
        match value {
            RetentionArg::Seconds(seconds) => Self::Seconds(seconds),
            RetentionArg::Infinite => Self::Infinite,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WriteBuffering {
    Raw,
    Lines,
}

#[derive(Debug)]
struct WriterState {
    writer_id: WriterId,
    next_writer_seq: BTreeMap<StreamId, u64>,
}

impl WriterState {
    fn new_random() -> Self {
        Self {
            writer_id: WriterId::new_random(),
            next_writer_seq: BTreeMap::new(),
        }
    }

    fn writer_id(&self) -> WriterId {
        self.writer_id
    }

    fn reserve_writer_seq(&mut self, stream_id: &StreamId) -> eyre::Result<u64> {
        let next = self.next_writer_seq.entry(*stream_id).or_default();
        let reserved = *next;
        *next = next
            .checked_add(1)
            .with_context(|| format!("writer sequence for stream {stream_id} overflowed"))?;
        Ok(reserved)
    }
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::New(args) => new_stream(cli.api_url, cli.web_url, args).await,
        Command::Write(args) => write_stream(cli.api_url, cli.web_url, args).await,
        Command::Tail(args) => tail_stream(cli.api_url, args).await,
        Command::Replay(args) => replay_stream(cli.api_url, args).await,
        Command::Delete(args) => delete_stream(cli.api_url, args).await,
        Command::Visibility(args) => update_visibility(cli.api_url, args).await,
        Command::Token(args) => token_command(cli.api_url, cli.web_url, args).await,
        Command::ParseUrl { url } => parse_url(&url),
    }
}

async fn new_stream(api_url: Url, web_url: Url, args: NewArgs) -> eyre::Result<()> {
    let visibility = visibility_from_flags(args.public);
    let issue_tokens = if args.tokens.is_empty() {
        Some(default_cli_tokens(visibility))
    } else {
        Some(args.tokens.clone())
    };

    let created = TsfClient::with_api_base_url(api_url)
        .create_stream(&CreateStreamRequest {
            visibility,
            retention_secs: args.retention.map(Into::into),
            issue_tokens,
        })
        .await
        .context("failed to create stream")?;
    write_token_files(&created.tokens, &args)?;
    print_created_stream(&web_url, &created, args.format, OutputTarget::Stdout)?;

    Ok(())
}

async fn write_stream(api_url: Url, web_url: Url, args: WriteArgs) -> eyre::Result<()> {
    let buffering = if args.raw {
        WriteBuffering::Raw
    } else {
        WriteBuffering::Lines
    };
    let command = args.command;
    let (stream_id, token) = if args.new {
        let visibility = visibility_from_flags(args.public);
        let created = TsfClient::with_api_base_url(api_url.clone())
            .create_stream(&CreateStreamRequest {
                visibility,
                retention_secs: args.retention.map(Into::into),
                issue_tokens: Some(default_cli_tokens(visibility)),
            })
            .await
            .context("failed to create stream")?;
        print_created_stream(&web_url, &created, OutputFormat::Text, OutputTarget::Stderr)?;
        let token = created
            .tokens
            .iter()
            .find(|token| token.permissions.allows_write())
            .context("created stream did not include a write token")?
            .token
            .clone();
        (created.stream_id, token)
    } else {
        let url = args
            .url
            .context("write requires a stream URL unless --new is set")?;
        let locator = StreamLocator::parse(&url).context("invalid stream URL")?;
        let token = locator
            .token_with(TokenPermissions::allows_write)
            .context("stream URL does not contain a write token")?
            .clone();
        (locator.stream_id, token)
    };

    if command.is_empty() {
        stream_stdin_to_writer(api_url, stream_id, token, buffering).await
    } else {
        stream_command_to_writer(api_url, stream_id, token, buffering, command).await
    }
}

async fn stream_stdin_to_writer(
    api_url: Url,
    stream_id: StreamId,
    token: BearerToken,
    buffering: WriteBuffering,
) -> eyre::Result<()> {
    let client = TsfClient::with_api_base_url(api_url);
    let mut state = WriterState::new_random();
    let writer = client
        .connect_producer(WriteStreamOptions::with_stream_token(
            stream_id,
            state.writer_id(),
            &token,
        ))
        .await
        .context("failed to connect writer")?;

    match buffering {
        WriteBuffering::Raw => stream_raw_stdin_to_writer(&writer, &mut state, &stream_id).await,
        WriteBuffering::Lines => stream_lines_to_writer(&writer, &mut state, &stream_id).await,
    }?;
    writer.close().await.context("failed to close writer")
}

async fn stream_raw_stdin_to_writer(
    writer: &TsfProducer,
    state: &mut WriterState,
    stream_id: &StreamId,
) -> eyre::Result<()> {
    let mut stdin = tokio::io::stdin();
    let mut buffer = vec![0_u8; 16 * 1024];
    let mut appender = RawRecordAppender::new(RAW_LINGER);
    let mut session = WriterSession {
        writer,
        state,
        stream_id,
        pending_tickets: VecDeque::new(),
    };
    loop {
        if let Some(deadline) = appender.deadline() {
            tokio::select! {
                byte_count = stdin.read(&mut buffer) => {
                    let byte_count = byte_count.context("failed to read stdin")?;
                    if byte_count == 0 {
                        break;
                    }
                    appender.push_bytes(&mut session, &buffer[..byte_count]).await?;
                }
                _ = sleep_until(deadline) => {
                    appender.flush(&mut session).await?;
                }
                interrupt = tokio::signal::ctrl_c() => {
                    interrupt.context("failed to listen for interrupt signal")?;
                    exit_interrupted();
                }
            }
        } else {
            let byte_count = tokio::select! {
                byte_count = stdin.read(&mut buffer) => byte_count.context("failed to read stdin")?,
                interrupt = tokio::signal::ctrl_c() => {
                    interrupt.context("failed to listen for interrupt signal")?;
                    exit_interrupted();
                }
            };
            if byte_count == 0 {
                break;
            }
            appender
                .push_bytes(&mut session, &buffer[..byte_count])
                .await?;
        };
    }
    appender.finish(&mut session).await?;
    session.finish().await?;

    Ok(())
}

async fn stream_lines_to_writer(
    writer: &TsfProducer,
    state: &mut WriterState,
    stream_id: &StreamId,
) -> eyre::Result<()> {
    let mut stdin = tokio::io::stdin();
    let mut read_buffer = vec![0_u8; 16 * 1024];
    let mut line_appender = LineRecordAppender::new();
    let mut session = WriterSession {
        writer,
        state,
        stream_id,
        pending_tickets: VecDeque::new(),
    };

    loop {
        let byte_count = tokio::select! {
            byte_count = stdin.read(&mut read_buffer) => byte_count.context("failed to read stdin")?,
            interrupt = tokio::signal::ctrl_c() => {
                interrupt.context("failed to listen for interrupt signal")?;
                exit_interrupted();
            }
        };
        if byte_count == 0 {
            break;
        }

        line_appender
            .push_bytes(&mut session, &read_buffer[..byte_count])
            .await?;
    }

    line_appender.finish(&mut session).await?;
    session.finish().await?;

    Ok(())
}

async fn stream_command_to_writer(
    api_url: Url,
    stream_id: StreamId,
    token: BearerToken,
    buffering: WriteBuffering,
    command: Vec<String>,
) -> eyre::Result<()> {
    let client = TsfClient::with_api_base_url(api_url);
    let mut state = WriterState::new_random();
    let writer = client
        .connect_producer(WriteStreamOptions::with_stream_token(
            stream_id,
            state.writer_id(),
            &token,
        ))
        .await
        .context("failed to connect writer")?;
    let status = {
        let mut session = WriterSession {
            writer: &writer,
            state: &mut state,
            stream_id: &stream_id,
            pending_tickets: VecDeque::new(),
        };
        let status = stream_child_command_output(&mut session, buffering, command).await?;
        session.finish().await?;
        status
    };
    writer.close().await.context("failed to close writer")?;
    if status.success() {
        Ok(())
    } else {
        exit_with_status(status)
    }
}

async fn stream_child_command_output(
    session: &mut WriterSession<'_>,
    buffering: WriteBuffering,
    command: Vec<String>,
) -> eyre::Result<ExitStatus> {
    let program = command
        .first()
        .context("command mode requires a program after --")?;
    let mut child = TokioCommand::new(program)
        .args(&command[1..])
        .stdin(Stdio::inherit())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("failed to spawn command {program:?}"))?;
    let stdout = child
        .stdout
        .take()
        .context("failed to capture child stdout")?;
    let stderr = child
        .stderr
        .take()
        .context("failed to capture child stderr")?;
    let (chunk_tx, mut chunk_rx) = mpsc::channel::<eyre::Result<Bytes>>(16);
    let stdout_task = tokio::spawn(read_child_pipe(stdout, chunk_tx.clone()));
    let stderr_task = tokio::spawn(read_child_pipe(stderr, chunk_tx));

    let stream_result = tokio::select! {
        result = async {
            match buffering {
                WriteBuffering::Raw => stream_raw_chunks_to_writer(session, &mut chunk_rx).await?,
                WriteBuffering::Lines => {
                    let mut line_appender = LineRecordAppender::new();
                    while let Some(chunk) = chunk_rx.recv().await {
                        line_appender.push_bytes(session, &chunk?).await?;
                    }
                    line_appender.finish(session).await?;
                }
            }
            eyre::Result::<()>::Ok(())
        } => result,
        interrupt = tokio::signal::ctrl_c() => {
            interrupt.context("failed to listen for interrupt signal")?;
            let _ = child.kill().await;
            exit_interrupted();
        }
    };

    if let Err(error) = stream_result {
        let _ = child.kill().await;
        return Err(error);
    }

    stdout_task.await.context("stdout reader task panicked")??;
    stderr_task.await.context("stderr reader task panicked")??;
    child.wait().await.context("failed to wait for command")
}

async fn read_child_pipe<R>(
    mut pipe: R,
    chunk_tx: mpsc::Sender<eyre::Result<Bytes>>,
) -> eyre::Result<()>
where
    R: AsyncRead + Unpin,
{
    let mut buffer = vec![0_u8; MAX_RECORD_BYTES];
    loop {
        let byte_count = pipe
            .read(&mut buffer)
            .await
            .context("failed to read command output")?;
        if byte_count == 0 {
            return Ok(());
        }
        if chunk_tx
            .send(Ok(Bytes::copy_from_slice(&buffer[..byte_count])))
            .await
            .is_err()
        {
            return Ok(());
        }
    }
}

struct RawRecordAppender {
    pending: BytesMut,
    deadline: Option<Instant>,
    linger: Duration,
}

impl RawRecordAppender {
    fn new(linger: Duration) -> Self {
        Self {
            pending: BytesMut::with_capacity(MAX_RECORD_BYTES),
            deadline: None,
            linger,
        }
    }

    fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    async fn push_bytes(
        &mut self,
        session: &mut WriterSession<'_>,
        mut bytes: &[u8],
    ) -> eyre::Result<()> {
        while !bytes.is_empty() {
            if self.pending.is_empty() {
                self.deadline = Some(Instant::now() + self.linger);
            }
            let available = MAX_RECORD_BYTES - self.pending.len();
            let take = available.min(bytes.len());
            self.pending.extend_from_slice(&bytes[..take]);
            bytes = &bytes[take..];
            if self.pending.len() == MAX_RECORD_BYTES {
                self.flush(session).await?;
            }
        }
        Ok(())
    }

    async fn flush(&mut self, session: &mut WriterSession<'_>) -> eyre::Result<()> {
        if self.pending.is_empty() {
            self.deadline = None;
            return Ok(());
        }
        let data = self.pending.split().freeze();
        self.deadline = None;
        session
            .append_physical_record(PartHeader::unsplit(), RecordFormat::Bytes, data)
            .await
    }

    async fn finish(&mut self, session: &mut WriterSession<'_>) -> eyre::Result<()> {
        self.flush(session).await
    }
}

async fn stream_raw_chunks_to_writer(
    session: &mut WriterSession<'_>,
    chunk_rx: &mut mpsc::Receiver<eyre::Result<Bytes>>,
) -> eyre::Result<()> {
    let mut appender = RawRecordAppender::new(RAW_LINGER);
    loop {
        if let Some(deadline) = appender.deadline() {
            tokio::select! {
                biased;
                chunk = chunk_rx.recv() => {
                    let Some(chunk) = chunk else {
                        break;
                    };
                    appender.push_bytes(session, &chunk?).await?;
                }
                _ = sleep_until(deadline) => {
                    appender.flush(session).await?;
                }
            }
        } else {
            let Some(chunk) = chunk_rx.recv().await else {
                break;
            };
            appender.push_bytes(session, &chunk?).await?;
        }
    }
    appender.finish(session).await
}

struct LineRecordAppender {
    pending: BytesMut,
    split_part_index: u32,
}

impl LineRecordAppender {
    fn new() -> Self {
        Self {
            pending: BytesMut::with_capacity(MAX_RECORD_BYTES),
            split_part_index: 0,
        }
    }

    async fn push_bytes(
        &mut self,
        session: &mut WriterSession<'_>,
        bytes: &[u8],
    ) -> eyre::Result<()> {
        for byte in bytes {
            if self.pending.len() == MAX_RECORD_BYTES {
                let data = self.pending.split().freeze();
                session
                    .append_line_part(self.split_part_index, false, data)
                    .await?;
                self.split_part_index = self
                    .split_part_index
                    .checked_add(1)
                    .context("line split part index overflowed")?;
            }

            self.pending.extend_from_slice(&[*byte]);
            if *byte == b'\n' {
                let data = self.pending.split().freeze();
                session
                    .append_line_part(self.split_part_index, true, data)
                    .await?;
                self.split_part_index = 0;
            }
        }
        Ok(())
    }

    async fn finish(&mut self, session: &mut WriterSession<'_>) -> eyre::Result<()> {
        if !self.pending.is_empty() {
            let data = self.pending.split().freeze();
            session
                .append_line_part(self.split_part_index, true, data)
                .await?;
            self.split_part_index = 0;
        }
        Ok(())
    }
}

struct WriterSession<'a> {
    writer: &'a TsfProducer,
    state: &'a mut WriterState,
    stream_id: &'a StreamId,
    pending_tickets: VecDeque<AppendTicket>,
}

impl WriterSession<'_> {
    async fn append_line_part(
        &mut self,
        part_index: u32,
        is_final: bool,
        data: Bytes,
    ) -> eyre::Result<()> {
        let part = if part_index == 0 && is_final {
            PartHeader::unsplit()
        } else {
            PartHeader::new(part_index, is_final).context("failed to encode split part")?
        };
        self.append_physical_record(part, RecordFormat::Transcript, data)
            .await
    }

    async fn append_physical_record(
        &mut self,
        part: PartHeader,
        format: RecordFormat,
        data: Bytes,
    ) -> eyre::Result<()> {
        let writer_seq_num = self
            .state
            .reserve_writer_seq(self.stream_id)
            .context("failed to reserve writer sequence")?;
        let record = WriteRecord::new(writer_seq_num, part, format, data);
        let ticket = self
            .writer
            .submit(record)
            .await
            .context("failed to submit record")?;
        self.pending_tickets.push_back(ticket);
        self.drain_ready_tickets()
    }

    async fn finish(&mut self) -> eyre::Result<()> {
        while let Some(ticket) = self.pending_tickets.pop_front() {
            ticket.await.context("failed to append record")?;
        }
        Ok(())
    }

    fn drain_ready_tickets(&mut self) -> eyre::Result<()> {
        loop {
            let Some(result) = self
                .pending_tickets
                .front_mut()
                .and_then(AppendTicket::try_recv)
            else {
                return Ok(());
            };
            result.context("failed to append record")?;
            self.pending_tickets.pop_front();
        }
    }
}

async fn tail_stream(api_url: Url, args: TailArgs) -> eyre::Result<()> {
    ensure_single_selector(args.seq_num, args.timestamp)?;
    let locator = StreamLocator::parse(&args.url).context("invalid stream URL")?;
    let mut request = ReadStreamOptions::new(locator.stream_id);
    request.start = Some(if let Some(seq_num) = args.seq_num {
        ReadStart::SeqNum(seq_num)
    } else if let Some(timestamp) = args.timestamp {
        ReadStart::TimestampMs(timestamp)
    } else {
        ReadStart::TailOffset(args.tail_offset.unwrap_or_default())
    });
    request.count = args.count;
    if let Some(token) = locator.token_with(TokenPermissions::allows_read) {
        request = request.with_stream_token(token);
    }

    read_transcript_loop(api_url, request, true, args.max_logical_record_bytes).await
}

async fn replay_stream(api_url: Url, args: ReplayArgs) -> eyre::Result<()> {
    ensure_single_selector(args.seq_num, args.timestamp)?;
    let locator = StreamLocator::parse(&args.url).context("invalid stream URL")?;
    let read_token = locator.token_with(TokenPermissions::allows_read);
    let read_client = if let Some(token) = read_token {
        TsfClient::with_api_base_url_and_rest_bearer_token(api_url.clone(), token.expose_secret())
    } else {
        TsfClient::with_api_base_url(api_url.clone())
    };
    let tail = read_client
        .get_stream_tail(&locator.stream_id)
        .await
        .context("failed to check stream tail")?;
    if tail.next_s2_seq_num == 0 {
        return Ok(());
    }

    let mut request = ReadStreamOptions::new(locator.stream_id);
    request.start = Some(if let Some(seq_num) = args.seq_num {
        ReadStart::SeqNum(seq_num)
    } else if let Some(timestamp) = args.timestamp {
        ReadStart::TimestampMs(timestamp)
    } else {
        ReadStart::SeqNum(0)
    });
    request.until = Some(tail.next_s2_seq_num - 1);
    request.count = args
        .count
        .or_else(|| replay_count_from_tail(&request, tail.next_s2_seq_num));
    if let Some(token) = read_token {
        request = request.with_stream_token(token);
    }

    read_transcript_loop(api_url, request, false, args.max_logical_record_bytes).await
}

async fn delete_stream(api_url: Url, args: OwnerUrlArgs) -> eyre::Result<()> {
    let (client, locator) = owner_client_from_url(api_url, &args.url)?;
    client
        .delete_stream(&locator.stream_id)
        .await
        .context("failed to delete stream")?;
    Ok(())
}

async fn update_visibility(api_url: Url, args: VisibilityArgs) -> eyre::Result<()> {
    let (client, locator) = owner_client_from_url(api_url, &args.url)?;
    let stream = client
        .update_stream(
            &locator.stream_id,
            &UpdateStreamRequest {
                visibility: Some(args.visibility.into()),
            },
        )
        .await
        .context("failed to update stream visibility")?;
    print_stream_info(&stream, args.format)?;
    Ok(())
}

async fn token_command(api_url: Url, web_url: Url, args: TokenArgs) -> eyre::Result<()> {
    match args.command {
        TokenCommand::List(args) => list_tokens(api_url, args).await,
        TokenCommand::Issue(args) => issue_token(api_url, web_url, args).await,
        TokenCommand::Revoke(args) => revoke_token(api_url, args).await,
    }
}

async fn list_tokens(api_url: Url, args: ListTokenArgs) -> eyre::Result<()> {
    let (client, locator) = owner_client_from_url(api_url, &args.url)?;
    let response = client
        .list_tokens(&locator.stream_id)
        .await
        .context("failed to list tokens")?;
    match args.format {
        OutputFormat::Text => {
            for token in response.tokens {
                println!(
                    "{}\t{}\t{}\t{}\t{}{}",
                    token.token_id,
                    token.permissions,
                    token_status_label(token.status),
                    token.issued_at,
                    token.expires_at.as_deref().unwrap_or("never"),
                    if token.is_current { "\tcurrent" } else { "" }
                );
            }
        }
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&response)?),
    }
    Ok(())
}

fn token_status_label(status: StreamTokenStatus) -> &'static str {
    match status {
        StreamTokenStatus::Active => "active",
        StreamTokenStatus::Expired => "expired",
        StreamTokenStatus::Revoked => "revoked",
    }
}

async fn issue_token(api_url: Url, web_url: Url, args: IssueTokenArgs) -> eyre::Result<()> {
    let (client, locator) = owner_client_from_url(api_url, &args.url)?;
    let issued = client
        .issue_token(
            &locator.stream_id,
            &IssueTokenRequest {
                permissions: args.permissions,
                expires_at: args.expires_at,
            },
        )
        .await
        .context("failed to issue token")?;
    if let Some(path) = &args.token_file {
        write_secret_file(path, issued.token.expose_secret())
            .with_context(|| format!("failed to write token file {}", path.display()))?;
    }
    print_issued_token(&web_url, &locator.stream_id, &issued, args.format)?;
    Ok(())
}

async fn revoke_token(api_url: Url, args: RevokeTokenArgs) -> eyre::Result<()> {
    let (client, locator) = owner_client_from_url(api_url, &args.url)?;
    client
        .revoke_token(&locator.stream_id, &args.token_id)
        .await
        .context("failed to revoke token")?;
    Ok(())
}

async fn read_transcript_loop(
    api_url: Url,
    mut options: ReadStreamOptions,
    follow: bool,
    max_logical_record_bytes: usize,
) -> eyre::Result<()> {
    let client = TsfClient::with_api_base_url(api_url);
    let mut transcript = LogicalTranscript::with_max_logical_record_bytes(max_logical_record_bytes);
    let mut stdout = tokio::io::stdout();
    let mut last_s2_seq_num = None;

    loop {
        let mut reader = client
            .connect_reader(options.clone())
            .await
            .context("failed to connect reader")?;

        while let Some(record) = tokio::select! {
            record = reader.next_record() => record.context("failed to read stream")?,
            interrupt = tokio::signal::ctrl_c() => {
                interrupt.context("failed to listen for interrupt signal")?;
                exit_interrupted();
            }
        } {
            last_s2_seq_num = Some(record.s2_seq_num);
            let record = transcript
                .push_record(record)
                .context("failed to assemble transcript record")?;
            write_transcript_record(&mut stdout, record).await?;
        }

        if !follow {
            return Ok(());
        }
        if let Some(last_s2_seq_num) = last_s2_seq_num {
            options.start = Some(ReadStart::SeqNum(last_s2_seq_num.saturating_add(1)));
        }
    }
}

async fn write_transcript_record(
    stdout: &mut tokio::io::Stdout,
    record: Option<TranscriptRecord>,
) -> eyre::Result<()> {
    if let Some(record) = record {
        write_transcript_data(stdout, record.data).await?;
    }
    Ok(())
}

async fn write_transcript_data(
    stdout: &mut tokio::io::Stdout,
    mut data: impl Buf,
) -> eyre::Result<()> {
    while data.has_remaining() {
        let chunk = data.chunk();
        let chunk_len = chunk.len();
        if chunk_len == 0 {
            bail!("transcript data returned an empty chunk before EOF");
        }
        stdout
            .write_all(chunk)
            .await
            .context("failed to write stdout")?;
        data.advance(chunk_len);
    }
    stdout.flush().await.context("failed to flush stdout")?;
    Ok(())
}

fn parse_url(url: &str) -> eyre::Result<()> {
    let locator = StreamLocator::parse(url).context("invalid stream URL")?;
    let output = ParsedUrlOutput {
        stream_id: locator.stream_id.to_string(),
        tokens: locator
            .token
            .iter()
            .map(|token| ParsedTokenOutput {
                permissions: token.permissions.to_string(),
                token_present: true,
            })
            .collect(),
    };

    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

fn owner_client_from_url(api_url: Url, url: &str) -> eyre::Result<(TsfClient, StreamLocator)> {
    let locator = StreamLocator::parse(url).context("invalid stream URL")?;
    let owner_token = locator
        .token_with(TokenPermissions::allows_owner)
        .context("stream URL does not contain an owner token")?;
    let client =
        TsfClient::with_api_base_url_and_rest_bearer_token(api_url, owner_token.expose_secret());
    Ok((client, locator))
}

fn print_created_stream(
    web_url: &Url,
    created: &CreateStreamResponse,
    format: OutputFormat,
    target: OutputTarget,
) -> eyre::Result<()> {
    match format {
        OutputFormat::Text => {
            target.print_line(&format!("stream_id={}", created.stream_id));
            target.print_line(&format!("retention_secs={}", created.retention_secs));
            for issued in &created.tokens {
                let url = stream_url(
                    web_url,
                    &created.stream_id,
                    issued.permissions,
                    &issued.token,
                );
                target.print_line(&format!("{}={url}", issued.permissions));
            }
        }
        OutputFormat::Json => {
            let output = CreatedStreamOutput {
                stream_id: created.stream_id.to_string(),
                retention_secs: created.retention_secs,
                urls: created
                    .tokens
                    .iter()
                    .map(|issued| {
                        (
                            issued.permissions.to_string(),
                            stream_url(
                                web_url,
                                &created.stream_id,
                                issued.permissions,
                                &issued.token,
                            )
                            .to_string(),
                        )
                    })
                    .collect(),
            };
            target.print_line(&serde_json::to_string_pretty(&output)?);
        }
    }
    Ok(())
}

fn print_stream_info(stream: &StreamInfoResponse, format: OutputFormat) -> eyre::Result<()> {
    match format {
        OutputFormat::Text => {
            println!("stream_id={}", stream.stream_id);
            println!("visibility={}", visibility_label(stream.visibility));
            println!("state={}", stream.state);
            println!("retention_secs={}", stream.retention_secs);
            println!("active_token_count={}", stream.active_token_count);
        }
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(stream)?);
        }
    }
    Ok(())
}

fn print_issued_token(
    web_url: &Url,
    stream_id: &StreamId,
    issued: &IssueTokenResponse,
    format: OutputFormat,
) -> eyre::Result<()> {
    let url = stream_url(web_url, stream_id, issued.permissions, &issued.token);
    match format {
        OutputFormat::Text => {
            println!("token_id={}", issued.token_id);
            println!("permissions={}", issued.permissions);
            println!("token={}", issued.token.expose_secret());
            println!("url={url}");
        }
        OutputFormat::Json => {
            let output = IssuedTokenOutput {
                token_id: issued.token_id.to_string(),
                permissions: issued.permissions.to_string(),
                token: issued.token.expose_secret().to_owned(),
                url: url.to_string(),
            };
            println!("{}", serde_json::to_string_pretty(&output)?);
        }
    }
    Ok(())
}

fn write_token_files(tokens: &[IssuedStreamToken], args: &NewArgs) -> eyre::Result<()> {
    write_token_file(
        &args.owner_token_file,
        tokens,
        TokenPermissions::allows_owner,
        "owner",
    )?;
    write_token_file(
        &args.read_token_file,
        tokens,
        TokenPermissions::allows_read,
        "read",
    )?;
    write_token_file(
        &args.write_token_file,
        tokens,
        TokenPermissions::allows_write,
        "write",
    )?;
    Ok(())
}

fn write_token_file(
    path: &Option<PathBuf>,
    tokens: &[IssuedStreamToken],
    allows: impl Fn(TokenPermissions) -> bool,
    label: &str,
) -> eyre::Result<()> {
    let Some(path) = path else {
        return Ok(());
    };
    let token = tokens
        .iter()
        .find(|token| allows(token.permissions))
        .with_context(|| format!("created stream did not include a {label} token"))?;
    write_secret_file(path, token.token.expose_secret())
        .with_context(|| format!("failed to write {label} token file {}", path.display()))?;
    Ok(())
}

fn write_secret_file(path: &Path, secret: &str) -> std::io::Result<()> {
    let mut options = OpenOptions::new();
    options.write(true).create(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;

        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;

        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    }
    file.set_len(0)?;
    std::io::Write::write_all(&mut file, secret.as_bytes())
}

fn default_cli_tokens(visibility: Visibility) -> Vec<TokenPermissions> {
    match visibility {
        Visibility::Private => vec![
            TokenPermissions::owner(),
            TokenPermissions::write(),
            TokenPermissions::read(),
        ],
        Visibility::Public => vec![TokenPermissions::owner(), TokenPermissions::write()],
    }
}

fn visibility_from_flags(public: bool) -> Visibility {
    if public {
        Visibility::Public
    } else {
        Visibility::Private
    }
}

fn visibility_label(visibility: Visibility) -> &'static str {
    match visibility {
        Visibility::Private => "private",
        Visibility::Public => "public",
    }
}

fn ensure_single_selector(seq_num: Option<u64>, timestamp: Option<u64>) -> eyre::Result<()> {
    if seq_num.is_some() && timestamp.is_some() {
        bail!("only one of --seq-num or --timestamp can be set");
    }
    Ok(())
}

fn replay_count_from_tail(options: &ReadStreamOptions, next_s2_seq_num: u64) -> Option<u64> {
    match options.start {
        Some(ReadStart::SeqNum(seq_num)) => Some(next_s2_seq_num.saturating_sub(seq_num)),
        None => Some(next_s2_seq_num),
        Some(ReadStart::TimestampMs(_) | ReadStart::TailOffset(_)) => None,
    }
}

fn exit_with_status(status: ExitStatus) -> ! {
    std::process::exit(exit_code_from_status(status));
}

fn exit_interrupted() -> ! {
    std::process::exit(INTERRUPT_EXIT_CODE);
}

fn exit_code_from_status(status: ExitStatus) -> i32 {
    status.code().unwrap_or_else(|| {
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            status.signal().map(|signal| 128 + signal).unwrap_or(1)
        }
        #[cfg(not(unix))]
        {
            1
        }
    })
}

#[derive(Clone, Copy)]
enum OutputTarget {
    Stdout,
    Stderr,
}

impl OutputTarget {
    fn print_line(self, line: &str) {
        match self {
            Self::Stdout => println!("{line}"),
            Self::Stderr => eprintln!("{line}"),
        }
    }
}

#[derive(Serialize)]
struct CreatedStreamOutput {
    stream_id: String,
    retention_secs: u64,
    urls: BTreeMap<String, String>,
}

#[derive(Serialize)]
struct IssuedTokenOutput {
    token_id: String,
    permissions: String,
    token: String,
    url: String,
}

#[derive(Serialize)]
struct ParsedUrlOutput {
    stream_id: String,
    tokens: Vec<ParsedTokenOutput>,
}

#[derive(Serialize)]
struct ParsedTokenOutput {
    permissions: String,
    token_present: bool,
}
