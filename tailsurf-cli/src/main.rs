//! `tsf` command-line client for creating, writing, replaying, tailing, and managing Tailsurf
//! streams.

use std::{
    collections::{BTreeMap, VecDeque},
    fs::{self, OpenOptions},
    io::{ErrorKind, IsTerminal},
    path::{Path, PathBuf},
    process::{ExitCode, ExitStatus, Stdio},
    str::FromStr,
    time::{SystemTime, UNIX_EPOCH},
};

use axoupdater::AxoUpdater;
use bytes::{Buf, Bytes, BytesMut};
use clap::{Args, Parser, Subcommand, ValueEnum};
use eyre::{Context, ContextCompat, bail};
use memchr::memchr;
use secrecy::ExposeSecret;
use serde::Serialize;
use tailsurf::{
    AppendTicket, BearerToken, CreateStreamIdempotencyKey, StreamId, TokenId, TokenPermissions,
    TsfClient, TsfProducer, TsfReadSession, WriteRecord, WriterId,
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
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufWriter},
    process::Command as TokioCommand,
    sync::mpsc,
    time::{Duration, Instant, sleep_until, timeout},
};
use url::Url;

const INTERRUPT_EXIT_CODE: i32 = 130;
const RAW_LINGER: Duration = Duration::from_millis(10);
/// Stdout batching window for `tail` and `replay`.
const TRANSCRIPT_OUTPUT_BUFFER_BYTES: usize = 64 * 1024;
/// Decoded transcript records held while stdout drains.
const TRANSCRIPT_RECORD_QUEUE: usize = 8;
/// Stdin read block size for line-framed and raw writes.
const STDIN_READ_BYTES: usize = 16 * 1024;
const MAX_INITIAL_TOKENS: usize = 3;
const UPDATE_HINT_CACHE_FILE: &str = ".tailsurf-cli-update-check";
const UPDATE_HINT_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);
const UPDATE_HINT_TIMEOUT: Duration = Duration::from_millis(500);

#[derive(Debug, Parser)]
#[command(name = "tsf")]
#[command(version, about = "Create, write, and read tail.surf streams")]
#[command(
    after_help = "With piped input and no subcommand, tsf behaves like tsf write:\n  anything | tsf"
)]
struct Cli {
    /// Tailsurf API origin.
    #[arg(
        long = "api-url",
        env = "TSF_API_URL",
        default_value = "https://tail.surf",
        global = true
    )]
    api_url: Url,
    /// Origin used when printing share URLs.
    #[arg(
        long = "web-url",
        env = "TSF_WEB_URL",
        default_value = "https://tail.surf",
        global = true
    )]
    web_url: Url,
    /// Optional so piped input can default to `write`; resolved before dispatch.
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Create a stream and print its share links.
    New(NewArgs),
    /// Stream stdin or a command's output to a stream.
    Write(WriteArgs),
    /// Follow a stream, optionally starting from retained records.
    Tail(TailArgs),
    /// Print a bounded snapshot of retained records.
    Replay(ReplayArgs),
    /// Show current stream metadata.
    Info(InfoArgs),
    /// Permanently delete a stream.
    Delete(OwnerUrlArgs),
    /// Change stream visibility.
    Visibility(VisibilityArgs),
    /// Manage share links.
    Link(LinkArgs),
    /// Update an installation managed by the tail.surf installer.
    Update(UpdateArgs),
}

#[derive(Debug, Args)]
struct UpdateArgs {
    /// Check whether an update is available without installing it.
    #[arg(long)]
    check: bool,
}

#[derive(Debug, Args)]
struct NewArgs {
    /// Allow anonymous reads.
    #[arg(long)]
    public: bool,
    /// Issue an additional link at creation. The owner link is always issued. May be repeated.
    #[arg(long = "link", value_name = "ACCESS")]
    links: Vec<AccessArg>,
    #[arg(
        long,
        value_name = "DURATION",
        help = "Record retention, such as 6h, 7d, or infinite"
    )]
    retention: Option<RetentionArg>,
    /// Owner-equivalent recovery key for resuming this exact create request.
    #[arg(long, env = "TSF_CREATE_IDEMPOTENCY_KEY", value_name = "KEY")]
    create_idempotency_key: Option<CreateStreamIdempotencyKey>,
    /// Output format.
    #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
    format: OutputFormat,
    /// Write the owner token secret to this file.
    #[arg(long = "owner-token-file", value_name = "PATH")]
    owner_token_file: Option<PathBuf>,
    /// Write the exact view-only token secret to this file. Requires `--link view`.
    #[arg(long = "view-token-file", value_name = "PATH")]
    view_token_file: Option<PathBuf>,
    /// Write the exact write-only token secret to this file. Requires `--link write`.
    #[arg(long = "write-token-file", value_name = "PATH")]
    write_token_file: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct WriteArgs {
    /// Write-capable stream share URL. Creates a stream when omitted.
    #[arg(value_name = "STREAM_URL")]
    url: Option<String>,
    /// Make a newly created stream publicly readable.
    #[arg(long)]
    public: bool,
    #[arg(
        long,
        value_name = "DURATION",
        help = "New-stream record retention, such as 6h, 7d, or infinite"
    )]
    retention: Option<RetentionArg>,
    /// Owner-equivalent recovery key for an implicitly created stream.
    #[arg(long, env = "TSF_CREATE_IDEMPOTENCY_KEY", value_name = "KEY")]
    create_idempotency_key: Option<CreateStreamIdempotencyKey>,
    /// Preserve input as arbitrary byte records instead of newline-delimited transcript records.
    #[arg(long)]
    raw: bool,
    /// Maximum logical line size. Readers use the same default.
    #[arg(long, value_name = "BYTES", default_value_t = DEFAULT_MAX_LOGICAL_RECORD_BYTES)]
    max_logical_record_bytes: usize,
    /// Command to run. Its stdout and stderr are written to the stream.
    #[arg(last = true, value_name = "COMMAND")]
    command: Vec<String>,
}

#[derive(Debug, Args)]
struct TailArgs {
    /// Read-capable or public stream share URL.
    #[arg(value_name = "STREAM_URL")]
    url: String,
    /// Start this many retained records before the live tail.
    #[arg(short = 'n', long, conflicts_with_all = ["seq_num", "timestamp"])]
    tail_offset: Option<u64>,
    #[command(flatten)]
    read: ReadArgs,
}

#[derive(Debug, Args)]
struct ReadArgs {
    /// Start at this S2 sequence number.
    #[arg(long, conflicts_with = "timestamp")]
    seq_num: Option<u64>,
    /// Start at this Unix timestamp in milliseconds.
    #[arg(long, conflicts_with = "seq_num")]
    timestamp: Option<u64>,
    /// Read at most this many stored records.
    #[arg(long)]
    count: Option<u64>,
    /// Maximum assembled transcript record size.
    #[arg(long, value_name = "BYTES", default_value_t = DEFAULT_MAX_LOGICAL_RECORD_BYTES)]
    max_logical_record_bytes: usize,
}

#[derive(Debug, Args)]
struct ReplayArgs {
    /// Read-capable or public stream share URL.
    #[arg(value_name = "STREAM_URL")]
    url: String,
    #[command(flatten)]
    read: ReadArgs,
}

#[derive(Debug, Args)]
struct InfoArgs {
    /// Read-capable or public stream share URL.
    #[arg(value_name = "STREAM_URL")]
    url: String,
    /// Output format.
    #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
    format: OutputFormat,
}

#[derive(Debug, Args)]
struct OwnerUrlArgs {
    /// Owner stream share URL.
    #[arg(value_name = "OWNER_URL")]
    url: String,
}

#[derive(Debug, Args)]
struct VisibilityArgs {
    /// Owner stream share URL.
    #[arg(value_name = "OWNER_URL")]
    url: String,
    /// New visibility.
    visibility: VisibilityArg,
    /// Output format.
    #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
    format: OutputFormat,
}

#[derive(Debug, Args)]
struct LinkArgs {
    #[command(subcommand)]
    command: LinkCommand,
}

#[derive(Debug, Subcommand)]
enum LinkCommand {
    /// List link metadata without secrets.
    List(ListLinkArgs),
    /// Issue a share link and print it once.
    Issue(IssueLinkArgs),
    /// Revoke a link by its ID.
    Revoke(RevokeLinkArgs),
}

#[derive(Debug, Args)]
struct ListLinkArgs {
    /// Owner stream share URL.
    #[arg(value_name = "OWNER_URL")]
    url: String,
    /// Output format.
    #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
    format: OutputFormat,
}

#[derive(Debug, Args)]
struct IssueLinkArgs {
    /// Owner stream share URL.
    #[arg(value_name = "OWNER_URL")]
    url: String,
    /// Access level: view, write, view+write, or owner.
    #[arg(long = "access", value_name = "ACCESS")]
    access: AccessArg,
    /// Expiry such as 1h, 7d, or never.
    #[arg(long, value_name = "EXPIRY", default_value = "never")]
    expires: ExpiresArg,
    /// Output format.
    #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
    format: OutputFormat,
    /// Write the new token secret to this file.
    #[arg(long = "token-file", value_name = "PATH")]
    token_file: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct RevokeLinkArgs {
    /// Owner stream share URL.
    #[arg(value_name = "OWNER_URL")]
    url: String,
    /// Link ID from `tsf link list`.
    #[arg(value_name = "LINK_ID")]
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

#[derive(Clone, Copy, Debug)]
struct AccessArg(TokenPermissions);

impl FromStr for AccessArg {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let permissions = match value.to_ascii_lowercase().as_str() {
            "view" | "r" => TokenPermissions::read(),
            "write" | "w" => TokenPermissions::write(),
            "view+write" | "view-write" | "rw" => TokenPermissions::read_write(),
            "owner" | "o" => TokenPermissions::owner(),
            other => {
                return Err(format!(
                    "unknown access level {other:?}; use view, write, view+write, or owner"
                ));
            }
        };
        Ok(Self(permissions))
    }
}

#[derive(Clone, Copy, Debug)]
enum ExpiresArg {
    Never,
    In(Duration),
}

impl ExpiresArg {
    fn rfc3339(self) -> Option<String> {
        match self {
            Self::Never => None,
            Self::In(duration) => Some(
                humantime::format_rfc3339_seconds(std::time::SystemTime::now() + duration)
                    .to_string(),
            ),
        }
    }
}

impl FromStr for ExpiresArg {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.eq_ignore_ascii_case("never") {
            return Ok(Self::Never);
        }
        let duration = humantime::parse_duration(value)
            .map_err(|error| format!("invalid expiry duration: {error}"))?;
        if duration.is_zero() {
            return Err("expiry must be at least one second".to_owned());
        }
        Ok(Self::In(duration))
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
    next_writer_seq: u64,
}

impl WriterState {
    fn new_random() -> Self {
        Self {
            writer_id: WriterId::new_random(),
            next_writer_seq: 0,
        }
    }

    fn reserve_writer_seq(&mut self) -> eyre::Result<u64> {
        let reserved = self.next_writer_seq;
        self.next_writer_seq = self
            .next_writer_seq
            .checked_add(1)
            .context("writer sequence overflowed")?;
        Ok(reserved)
    }
}

// One socket, one stdin, one stdout: worker threads only add wakeup and handoff cost.
#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let Some(Cli {
        api_url,
        web_url,
        command: Some(command),
    }) = resolve_cli()
    else {
        return ExitCode::from(2);
    };
    let check_for_update = should_check_for_update_hint(
        &command,
        &api_url,
        std::io::stderr().is_terminal(),
        automatic_update_checks_disabled(),
    );
    match run(api_url, web_url, command).await {
        Ok(()) => {
            if check_for_update {
                maybe_print_update_hint().await;
            }
            ExitCode::SUCCESS
        }
        Err(error) if is_broken_pipe(&error) => ExitCode::SUCCESS,
        Err(error) => {
            print_error(&error);
            ExitCode::FAILURE
        }
    }
}

/// Parses the command line, defaulting piped input without a subcommand to `write`.
///
/// Returns `None` when there is no subcommand and stdin is a terminal; the caller
/// exits with a usage error after help has been printed to stderr.
fn resolve_cli() -> Option<Cli> {
    let cli = Cli::parse();
    if cli.command.is_some() {
        return Some(cli);
    }
    if std::io::stdin().is_terminal() {
        let help = <Cli as clap::CommandFactory>::command().render_help();
        eprint!("{help}");
        return None;
    }
    // Reparse with an implicit `write` so clap applies its env vars and defaults.
    let mut argv: Vec<std::ffi::OsString> = std::env::args_os().collect();
    argv.push("write".into());
    Some(Cli::parse_from(argv))
}

async fn run(api_url: Url, web_url: Url, command: Command) -> eyre::Result<()> {
    match command {
        Command::New(args) => new_stream(api_url, web_url, args).await,
        Command::Write(args) => write_stream(api_url, web_url, args).await,
        Command::Tail(args) => tail_stream(api_url, args).await,
        Command::Replay(args) => replay_stream(api_url, args).await,
        Command::Info(args) => stream_info(api_url, args).await,
        Command::Delete(args) => delete_stream(api_url, args).await,
        Command::Visibility(args) => update_visibility(api_url, args).await,
        Command::Link(args) => link_command(api_url, web_url, args).await,
        Command::Update(args) => update_cli(args).await,
    }
}

async fn update_cli(args: UpdateArgs) -> eyre::Result<()> {
    let mut updater = managed_updater()?;

    if args.check {
        if updater
            .is_update_needed()
            .await
            .context("failed to check for a tsf update")?
        {
            println!("An update is available. Run `tsf update` to install it.");
        } else {
            println!("tsf is up to date.");
        }
        return Ok(());
    }

    updater.enable_installer_output();
    match updater.run().await.context("failed to update tsf")? {
        Some(_) => eprintln!("Updated tsf."),
        None => eprintln!("tsf is already up to date."),
    }
    Ok(())
}

fn managed_updater() -> eyre::Result<AxoUpdater> {
    const OWNERSHIP_ERROR: &str = "this tsf installation is not managed by the tail.surf installer; update it with the package manager that installed it (Cargo: cargo install tailsurf-cli --locked)";

    let mut updater = AxoUpdater::new_for("tailsurf-cli");
    updater
        .load_receipt()
        .map_err(|_| eyre::eyre!(OWNERSHIP_ERROR))?;
    let owns_executable = updater
        .check_receipt_is_for_this_executable()
        .map_err(|_| eyre::eyre!(OWNERSHIP_ERROR))?;
    if !owns_executable {
        bail!(OWNERSHIP_ERROR);
    }
    Ok(updater)
}

fn should_check_for_update_hint(
    command: &Command,
    api_url: &Url,
    stderr_is_terminal: bool,
    disabled: bool,
) -> bool {
    stderr_is_terminal
        && !disabled
        && !matches!(command, Command::Update(_))
        && api_url.as_str() == "https://tail.surf/"
}

fn automatic_update_checks_disabled() -> bool {
    ["CI", "TSF_NO_UPDATE_CHECK", "DO_NOT_TRACK"]
        .into_iter()
        .any(|name| std::env::var_os(name).is_some())
}

async fn maybe_print_update_hint() {
    let Ok(mut updater) = managed_updater() else {
        return;
    };
    let Ok(install_root) = updater.install_prefix_root() else {
        return;
    };
    let Ok(now) = SystemTime::now().duration_since(UNIX_EPOCH) else {
        return;
    };
    let cache_path = install_root.join(UPDATE_HINT_CACHE_FILE);
    if !claim_update_hint_check(cache_path.as_std_path(), now.as_secs()) {
        return;
    }

    if matches!(
        timeout(UPDATE_HINT_TIMEOUT, updater.is_update_needed()).await,
        Ok(Ok(true))
    ) {
        eprintln!("A tsf update is available. Run `tsf update` to install it.");
    }
}

fn claim_update_hint_check(cache_path: &Path, now: u64) -> bool {
    let last_check = fs::read_to_string(cache_path)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok());
    if !update_hint_check_is_due(last_check, now) {
        return false;
    }
    fs::write(cache_path, format!("{now}\n")).is_ok()
}

fn update_hint_check_is_due(last_check: Option<u64>, now: u64) -> bool {
    last_check.is_none_or(|last_check| last_check.abs_diff(now) >= UPDATE_HINT_INTERVAL.as_secs())
}

fn is_broken_pipe(error: &eyre::Report) -> bool {
    error.chain().any(|source| {
        source
            .downcast_ref::<std::io::Error>()
            .is_some_and(|error| error.kind() == ErrorKind::BrokenPipe)
    })
}

fn print_error(error: &eyre::Report) {
    let mut chain = error.chain();
    if let Some(message) = chain.next() {
        eprintln!("error: {message}");
    }
    for cause in chain {
        eprintln!("  caused by: {cause}");
    }
}

async fn new_stream(api_url: Url, web_url: Url, args: NewArgs) -> eyre::Result<()> {
    let visibility = visibility_from_flags(args.public);
    let issue_tokens = new_stream_tokens(&args)?;

    let created = create_stream(
        api_url,
        visibility,
        args.retention.map(Into::into),
        issue_tokens,
        args.create_idempotency_key.as_ref(),
    )
    .await?;
    print_created_stream(&web_url, &created, args.format, OutputTarget::Stdout)?;
    write_token_files(&created.tokens, &args)?;

    Ok(())
}

fn new_stream_tokens(args: &NewArgs) -> eyre::Result<Vec<TokenPermissions>> {
    let mut issue_tokens = vec![TokenPermissions::owner()];
    for access in &args.links {
        if !issue_tokens.contains(&access.0) {
            issue_tokens.push(access.0);
        }
    }
    if issue_tokens.len() > MAX_INITIAL_TOKENS {
        bail!(
            "at most {MAX_INITIAL_TOKENS} initial links may be issued, including the mandatory owner link"
        );
    }
    if args.view_token_file.is_some() && !issue_tokens.contains(&TokenPermissions::read()) {
        bail!("--view-token-file requires --link view");
    }
    if args.write_token_file.is_some() && !issue_tokens.contains(&TokenPermissions::write()) {
        bail!("--write-token-file requires --link write");
    }
    Ok(issue_tokens)
}

async fn write_stream(api_url: Url, web_url: Url, args: WriteArgs) -> eyre::Result<()> {
    validate_write_args(&args)?;
    let buffering = if args.raw {
        WriteBuffering::Raw
    } else {
        WriteBuffering::Lines
    };
    let command = args.command;
    let (stream_id, token, view_link) = if let Some(url) = args.url {
        let locator = StreamLocator::parse(&url).context("invalid stream URL")?;
        let token = locator
            .token_with(TokenPermissions::allows_write)
            .context("URL does not grant write access")?
            .clone();
        (locator.stream_id, token, None)
    } else {
        let visibility = visibility_from_flags(args.public);
        let created = create_stream(
            api_url.clone(),
            visibility,
            args.retention.map(Into::into),
            write_new_default_links(visibility),
            args.create_idempotency_key.as_ref(),
        )
        .await?;
        print_created_stream(&web_url, &created, OutputFormat::Text, OutputTarget::Stderr)?;
        let token = created
            .tokens
            .iter()
            .find(|token| token.permissions.allows_write())
            .context("created stream did not include a write-capable link")?
            .token
            .clone();
        let view_link = created_view_link(&web_url, &created)?
            .context("created stream did not include a view link")?;
        println!("{view_link}");
        (created.stream_id, token, Some(view_link))
    };

    if command.is_empty() {
        stream_stdin_to_writer(
            api_url,
            stream_id,
            token,
            buffering,
            args.max_logical_record_bytes,
            view_link,
        )
        .await
    } else {
        stream_command_to_writer(
            api_url,
            stream_id,
            token,
            buffering,
            args.max_logical_record_bytes,
            command,
            view_link,
        )
        .await
    }
}

fn print_write_summary(records: u64, view_link: Option<&Url>) {
    let noun = if records == 1 { "record" } else { "records" };
    match view_link {
        Some(url) => eprintln!("{records} {noun} durable · view {url}"),
        None => eprintln!("{records} {noun} durable"),
    }
}

fn validate_write_args(args: &WriteArgs) -> eyre::Result<()> {
    if args.url.is_none() {
        return Ok(());
    }
    if args.public {
        bail!("--public cannot be used when writing to an existing stream");
    }
    if args.retention.is_some() {
        bail!("--retention cannot be used when writing to an existing stream");
    }
    if args.create_idempotency_key.is_some() {
        bail!("--create-idempotency-key cannot be used when writing to an existing stream");
    }
    Ok(())
}

async fn create_stream(
    api_url: Url,
    visibility: Visibility,
    retention_secs: Option<RequestedRetention>,
    issue_tokens: Vec<TokenPermissions>,
    supplied_key: Option<&CreateStreamIdempotencyKey>,
) -> eyre::Result<CreateStreamResponse> {
    let generated_key;
    let idempotency_key = match supplied_key {
        Some(key) => key,
        None => {
            generated_key = CreateStreamIdempotencyKey::new_random();
            &generated_key
        }
    };
    let result = TsfClient::with_api_base_url(api_url)
        .create_stream_with_idempotency_key(
            &CreateStreamRequest {
                visibility,
                retention_secs,
                issue_tokens: Some(issue_tokens),
            },
            idempotency_key,
        )
        .await;
    match result {
        Ok(created) => Ok(created),
        Err(error) if error.is_recoverable_create_failure() => Err(error).wrap_err(format!(
            "stream creation did not complete; recover this exact request by setting TSF_CREATE_IDEMPOTENCY_KEY to this owner-equivalent recovery key (keep it secret):\n{}",
            idempotency_key.expose_secret()
        )),
        Err(error) => Err(error).context("failed to create stream"),
    }
}

fn created_view_link(web_url: &Url, created: &CreateStreamResponse) -> eyre::Result<Option<Url>> {
    if matches!(created.visibility, Visibility::Public) {
        return Ok(Some(bare_stream_url(web_url, &created.stream_id)));
    }

    created
        .tokens
        .iter()
        .find(|issued| issued.permissions == TokenPermissions::read())
        .map(|issued| {
            stream_url(
                web_url,
                &created.stream_id,
                issued.permissions,
                &issued.token,
            )
        })
        .transpose()
        .map_err(Into::into)
}

async fn stream_stdin_to_writer(
    api_url: Url,
    stream_id: StreamId,
    token: BearerToken,
    buffering: WriteBuffering,
    max_logical_record_bytes: usize,
    view_link: Option<Url>,
) -> eyre::Result<()> {
    let client = TsfClient::with_api_base_url(api_url);
    let mut state = WriterState::new_random();
    let writer = client
        .connect_producer(WriteStreamOptions::with_stream_token(
            stream_id,
            state.writer_id,
            &token,
        ))
        .await
        .context("failed to connect writer")?;

    let interrupted = match buffering {
        WriteBuffering::Raw => stream_raw_stdin_to_writer(&writer, &mut state).await,
        WriteBuffering::Lines => {
            stream_lines_to_writer(&writer, &mut state, max_logical_record_bytes).await
        }
    }?;
    writer.close().await.context("failed to close writer")?;
    print_write_summary(state.next_writer_seq, view_link.as_ref());
    if interrupted {
        exit_interrupted();
    }
    Ok(())
}

async fn stream_raw_stdin_to_writer(
    writer: &TsfProducer,
    state: &mut WriterState,
) -> eyre::Result<bool> {
    let mut stdin = tokio::io::stdin();
    let mut buffer = vec![0_u8; STDIN_READ_BYTES];
    let mut appender = RawRecordAppender::new(RAW_LINGER);
    let mut session = WriterSession {
        writer,
        state,
        pending_tickets: VecDeque::new(),
    };
    let interrupted = loop {
        if let Some(deadline) = appender.deadline() {
            tokio::select! {
                byte_count = stdin.read(&mut buffer) => {
                    let byte_count = byte_count.context("failed to read stdin")?;
                    if byte_count == 0 {
                        break false;
                    }
                    appender.push_bytes(&mut session, &buffer[..byte_count]).await?;
                }
                _ = sleep_until(deadline) => {
                    appender.flush(&mut session).await?;
                }
                interrupt = tokio::signal::ctrl_c() => {
                    interrupt.context("failed to listen for interrupt signal")?;
                    break true;
                }
            }
        } else {
            let byte_count = tokio::select! {
                byte_count = stdin.read(&mut buffer) => byte_count.context("failed to read stdin")?,
                interrupt = tokio::signal::ctrl_c() => {
                    interrupt.context("failed to listen for interrupt signal")?;
                    break true;
                }
            };
            if byte_count == 0 {
                break false;
            }
            appender
                .push_bytes(&mut session, &buffer[..byte_count])
                .await?;
        };
    };
    appender.flush(&mut session).await?;
    session.finish().await?;

    Ok(interrupted)
}

async fn stream_lines_to_writer(
    writer: &TsfProducer,
    state: &mut WriterState,
    max_logical_record_bytes: usize,
) -> eyre::Result<bool> {
    let mut stdin = tokio::io::stdin();
    let mut read_buffer = BytesMut::with_capacity(STDIN_READ_BYTES);
    let mut line_appender = LineRecordAppender::new(max_logical_record_bytes);
    let mut session = WriterSession {
        writer,
        state,
        pending_tickets: VecDeque::new(),
    };

    let interrupted = loop {
        read_buffer.reserve(STDIN_READ_BYTES);
        let byte_count = tokio::select! {
            byte_count = stdin.read_buf(&mut read_buffer) => byte_count.context("failed to read stdin")?,
            interrupt = tokio::signal::ctrl_c() => {
                interrupt.context("failed to listen for interrupt signal")?;
                break true;
            }
        };
        if byte_count == 0 {
            break false;
        }

        line_appender
            .push_bytes(&mut session, read_buffer.split().freeze())
            .await?;
    };

    line_appender.finish(&mut session).await?;
    session.finish().await?;

    Ok(interrupted)
}

async fn stream_command_to_writer(
    api_url: Url,
    stream_id: StreamId,
    token: BearerToken,
    buffering: WriteBuffering,
    max_logical_record_bytes: usize,
    command: Vec<String>,
    view_link: Option<Url>,
) -> eyre::Result<()> {
    let client = TsfClient::with_api_base_url(api_url);
    let mut state = WriterState::new_random();
    let writer = client
        .connect_producer(WriteStreamOptions::with_stream_token(
            stream_id,
            state.writer_id,
            &token,
        ))
        .await
        .context("failed to connect writer")?;
    let outcome = {
        let mut session = WriterSession {
            writer: &writer,
            state: &mut state,
            pending_tickets: VecDeque::new(),
        };
        let outcome =
            stream_child_command_output(&mut session, buffering, max_logical_record_bytes, command)
                .await?;
        session.finish().await?;
        outcome
    };
    writer.close().await.context("failed to close writer")?;
    print_write_summary(state.next_writer_seq, view_link.as_ref());
    if outcome.interrupted {
        exit_interrupted();
    }
    if outcome.status.success() {
        Ok(())
    } else {
        std::process::exit(exit_code_from_status(outcome.status))
    }
}

struct ChildCommandOutcome {
    status: ExitStatus,
    interrupted: bool,
}

async fn stream_child_command_output(
    session: &mut WriterSession<'_>,
    buffering: WriteBuffering,
    max_logical_record_bytes: usize,
    command: Vec<String>,
) -> eyre::Result<ChildCommandOutcome> {
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

    let stream_output = async {
        match buffering {
            WriteBuffering::Raw => stream_raw_chunks_to_writer(session, &mut chunk_rx).await?,
            WriteBuffering::Lines => {
                let mut line_appender = LineRecordAppender::new(max_logical_record_bytes);
                while let Some(chunk) = chunk_rx.recv().await {
                    line_appender.push_bytes(session, chunk?).await?;
                }
                line_appender.finish(session).await?;
            }
        }
        eyre::Result::<()>::Ok(())
    };
    tokio::pin!(stream_output);

    let (stream_result, interrupted) = tokio::select! {
        result = &mut stream_output => (result, false),
        interrupt = tokio::signal::ctrl_c() => {
            interrupt.context("failed to listen for interrupt signal")?;
            let _ = child.kill().await;
            (stream_output.await, true)
        }
    };

    if let Err(error) = stream_result {
        let _ = child.kill().await;
        stdout_task.abort();
        stderr_task.abort();
        let _ = stdout_task.await;
        let _ = stderr_task.await;
        return Err(error);
    }

    stdout_task.await.context("stdout reader task panicked")??;
    stderr_task.await.context("stderr reader task panicked")??;
    let status = child.wait().await.context("failed to wait for command")?;
    Ok(ChildCommandOutcome {
        status,
        interrupted,
    })
}

async fn read_child_pipe<R>(
    mut pipe: R,
    chunk_tx: mpsc::Sender<eyre::Result<Bytes>>,
) -> eyre::Result<()>
where
    R: AsyncRead + Unpin,
{
    let mut buffer = BytesMut::with_capacity(MAX_RECORD_BYTES);
    loop {
        buffer.reserve(MAX_RECORD_BYTES);
        let byte_count = pipe
            .read_buf(&mut buffer)
            .await
            .context("failed to read command output")?;
        if byte_count == 0 {
            return Ok(());
        }
        if chunk_tx.send(Ok(buffer.split().freeze())).await.is_err() {
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
    appender.flush(session).await
}

struct LineRecordAppender {
    pending: BytesMut,
    pending_parts: Vec<Bytes>,
    logical_record_bytes: usize,
    max_logical_record_bytes: usize,
}

impl LineRecordAppender {
    fn new(max_logical_record_bytes: usize) -> Self {
        Self {
            pending: BytesMut::with_capacity(MAX_RECORD_BYTES),
            pending_parts: Vec::new(),
            logical_record_bytes: 0,
            max_logical_record_bytes,
        }
    }

    async fn push_bytes(
        &mut self,
        session: &mut WriterSession<'_>,
        mut bytes: Bytes,
    ) -> eyre::Result<()> {
        while !bytes.is_empty() {
            let newline = memchr(b'\n', &bytes);
            let take = newline.map_or(bytes.len(), |index| index + 1);
            let logical_record_bytes = self
                .logical_record_bytes
                .checked_add(take)
                .context("input line length overflowed while enforcing the logical record limit")?;
            if logical_record_bytes > self.max_logical_record_bytes {
                bail!(
                    "input line exceeds the configured {}-byte logical record limit; raise --max-logical-record-bytes only when readers use the same limit",
                    self.max_logical_record_bytes
                );
            }
            self.logical_record_bytes = logical_record_bytes;
            self.buffer(&bytes[..take]);
            bytes.advance(take);
            if newline.is_some() {
                self.send_line(session).await?;
            }
        }
        Ok(())
    }

    async fn finish(&mut self, session: &mut WriterSession<'_>) -> eyre::Result<()> {
        if self.logical_record_bytes > 0 {
            self.send_line(session).await?;
        }
        Ok(())
    }

    fn buffer(&mut self, mut bytes: &[u8]) {
        while !bytes.is_empty() {
            let available = MAX_RECORD_BYTES - self.pending.len();
            let take = available.min(bytes.len());
            self.pending.extend_from_slice(&bytes[..take]);
            bytes = &bytes[take..];
            if self.pending.len() == MAX_RECORD_BYTES {
                self.pending_parts.push(self.pending.split().freeze());
            }
        }
    }

    async fn send_line(&mut self, session: &mut WriterSession<'_>) -> eyre::Result<()> {
        if !self.pending.is_empty() {
            self.pending_parts.push(self.pending.split().freeze());
        }
        let parts = std::mem::take(&mut self.pending_parts);
        self.logical_record_bytes = 0;
        let last_part = parts
            .len()
            .checked_sub(1)
            .context("logical line is empty")?;
        for (index, part) in parts.into_iter().enumerate() {
            let part_index = u32::try_from(index).context("line split part index overflowed")?;
            session
                .append_line_part(part_index, index == last_part, part)
                .await?;
        }
        Ok(())
    }
}

struct WriterSession<'a> {
    writer: &'a TsfProducer,
    state: &'a mut WriterState,
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
            .reserve_writer_seq()
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
    let locator = StreamLocator::parse(&args.url).context("invalid stream URL")?;
    let mut request = ReadStreamOptions::new(locator.stream_id);
    request.start = Some(selected_read_start(
        args.read.seq_num,
        args.read.timestamp,
        ReadStart::TailOffset(args.tail_offset.unwrap_or_default()),
    ));
    request.count = args.read.count;
    if let Some(token) = locator.token_with(TokenPermissions::allows_read) {
        request = request.with_stream_token(token);
    }

    read_transcript(api_url, request, args.read.max_logical_record_bytes).await
}

async fn replay_stream(api_url: Url, args: ReplayArgs) -> eyre::Result<()> {
    let locator = StreamLocator::parse(&args.url).context("invalid stream URL")?;
    if args.read.count == Some(0) {
        return Ok(());
    }
    let read_token = locator.token_with(TokenPermissions::allows_read);
    let read_client = TsfClient::with_api_base_url(api_url.clone());
    let tail = read_client
        .get_stream_tail(&locator.stream_id, read_token)
        .await
        .context("failed to check stream tail")?;
    if tail.next_s2_seq_num == 0 {
        return Ok(());
    }

    let mut request = ReadStreamOptions::new(locator.stream_id);
    request.start = Some(selected_read_start(
        args.read.seq_num,
        args.read.timestamp,
        ReadStart::SeqNum(0),
    ));
    request.until = Some(tail.next_s2_seq_num - 1);
    request.count = args
        .read
        .count
        .or_else(|| replay_count_from_tail(&request, tail.next_s2_seq_num));
    if let Some(token) = read_token {
        request = request.with_stream_token(token);
    }

    read_transcript(api_url, request, args.read.max_logical_record_bytes).await
}

async fn stream_info(api_url: Url, args: InfoArgs) -> eyre::Result<()> {
    let locator = StreamLocator::parse(&args.url).context("invalid stream URL")?;
    let client = TsfClient::with_api_base_url(api_url);
    let stream = client
        .get_stream(
            &locator.stream_id,
            locator.token.as_ref().map(|token| &token.token),
        )
        .await
        .context("failed to get stream")?;
    print_stream_info(&stream, args.format)
}

async fn delete_stream(api_url: Url, args: OwnerUrlArgs) -> eyre::Result<()> {
    let (client, locator, owner_token) = owner_client_from_url(api_url, &args.url)?;
    client
        .delete_stream(&locator.stream_id, &owner_token)
        .await
        .context("failed to delete stream")?;
    Ok(())
}

async fn update_visibility(api_url: Url, args: VisibilityArgs) -> eyre::Result<()> {
    let (client, locator, owner_token) = owner_client_from_url(api_url, &args.url)?;
    let stream = client
        .update_stream(
            &locator.stream_id,
            &UpdateStreamRequest {
                visibility: Some(args.visibility.into()),
            },
            &owner_token,
        )
        .await
        .context("failed to update stream visibility")?;
    print_stream_info(&stream, args.format)?;
    Ok(())
}

async fn link_command(api_url: Url, web_url: Url, args: LinkArgs) -> eyre::Result<()> {
    match args.command {
        LinkCommand::List(args) => list_links(api_url, args).await,
        LinkCommand::Issue(args) => issue_link(api_url, web_url, args).await,
        LinkCommand::Revoke(args) => revoke_link(api_url, args).await,
    }
}

async fn list_links(api_url: Url, args: ListLinkArgs) -> eyre::Result<()> {
    let (client, locator, owner_token) = owner_client_from_url(api_url, &args.url)?;
    let response = client
        .list_tokens(&locator.stream_id, &owner_token)
        .await
        .context("failed to list links")?;
    match args.format {
        OutputFormat::Text => {
            for token in response.tokens {
                println!(
                    "{:<10}  {:<7}  expires {:<24}  id {}{}",
                    link_label(token.permissions),
                    token_status_label(token.status),
                    token.expires_at.as_deref().unwrap_or("never"),
                    token.token_id,
                    if token.is_current { "  (current)" } else { "" }
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

async fn issue_link(api_url: Url, web_url: Url, args: IssueLinkArgs) -> eyre::Result<()> {
    let (client, locator, owner_token) = owner_client_from_url(api_url, &args.url)?;
    let issued = client
        .issue_token(
            &locator.stream_id,
            &IssueTokenRequest {
                permissions: args.access.0,
                expires_at: args.expires.rfc3339(),
            },
            &owner_token,
        )
        .await
        .context("failed to issue link")?;
    if let Some(path) = &args.token_file {
        write_secret_file(path, issued.token.expose_secret())
            .with_context(|| format!("failed to write token file {}", path.display()))?;
    }
    print_issued_token(&web_url, &locator.stream_id, &issued, args.format)?;
    Ok(())
}

async fn revoke_link(api_url: Url, args: RevokeLinkArgs) -> eyre::Result<()> {
    let (client, locator, owner_token) = owner_client_from_url(api_url, &args.url)?;
    client
        .revoke_token(&locator.stream_id, &args.token_id, &owner_token)
        .await
        .context("failed to revoke link")?;
    Ok(())
}

async fn read_transcript(
    api_url: Url,
    options: ReadStreamOptions,
    max_logical_record_bytes: usize,
) -> eyre::Result<()> {
    if options.count == Some(0) {
        return Ok(());
    }

    let client = TsfClient::with_api_base_url(api_url);
    let reader = client
        .connect_reader(options)
        .await
        .context("failed to connect reader")?;
    let (record_tx, mut record_rx) = mpsc::channel(TRANSCRIPT_RECORD_QUEUE);
    let reader_task = tokio::spawn(assemble_transcript_records(
        reader,
        max_logical_record_bytes,
        record_tx,
    ));

    let mut stdout = BufWriter::with_capacity(TRANSCRIPT_OUTPUT_BUFFER_BYTES, tokio::io::stdout());
    let result = write_transcript_records(&mut record_rx, &mut stdout).await;
    stdout.flush().await.context("failed to flush stdout")?;
    result?;

    reader_task.await.context("transcript reader task panicked")
}

/// Writes decoded records until the reader finishes, flushing whenever none is already waiting.
async fn write_transcript_records(
    record_rx: &mut mpsc::Receiver<eyre::Result<TranscriptRecord>>,
    stdout: &mut BufWriter<tokio::io::Stdout>,
) -> eyre::Result<()> {
    loop {
        let record = tokio::select! {
            record = record_rx.recv() => record,
            interrupt = tokio::signal::ctrl_c() => {
                interrupt.context("failed to listen for interrupt signal")?;
                stdout.flush().await.context("failed to flush stdout")?;
                exit_interrupted();
            }
        };
        let Some(record) = record else {
            return Ok(());
        };

        write_transcript_data(stdout, record?.data).await?;
        // Batching must never hold output back, so flush as soon as nothing is already decoded.
        if record_rx.is_empty() {
            stdout.flush().await.context("failed to flush stdout")?;
        }
    }
}

/// Reassembles logical records off the output path so socket reads overlap stdout writes.
async fn assemble_transcript_records(
    mut reader: TsfReadSession,
    max_logical_record_bytes: usize,
    record_tx: mpsc::Sender<eyre::Result<TranscriptRecord>>,
) {
    // Failures belong in stream order behind the records already sent, not in the join result.
    if let Err(error) =
        forward_transcript_records(&mut reader, max_logical_record_bytes, &record_tx).await
    {
        let _ = record_tx.send(Err(error)).await;
    }
}

async fn forward_transcript_records(
    reader: &mut TsfReadSession,
    max_logical_record_bytes: usize,
    record_tx: &mpsc::Sender<eyre::Result<TranscriptRecord>>,
) -> eyre::Result<()> {
    let mut transcript = LogicalTranscript::with_max_logical_record_bytes(max_logical_record_bytes);

    while let Some(record) = reader
        .next_record()
        .await
        .context("failed to read stream")?
    {
        let Some(record) = transcript
            .push_record(record)
            .context("failed to assemble transcript record")?
        else {
            continue;
        };
        if record_tx.send(Ok(record)).await.is_err() {
            return Ok(());
        }
    }

    Ok(())
}
async fn write_transcript_data(
    stdout: &mut (impl AsyncWrite + Unpin),
    mut data: impl Buf,
) -> eyre::Result<()> {
    // Chunk-at-a-time beats `write_all_buf` here: stdout is not vectored, so the vectored path
    // only adds per-poll `IoSlice` setup for the common single-chunk record.
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
    Ok(())
}

fn owner_client_from_url(
    api_url: Url,
    url: &str,
) -> eyre::Result<(TsfClient, StreamLocator, BearerToken)> {
    let locator = StreamLocator::parse(url).context("invalid stream URL")?;
    let owner_token = locator
        .token_with(TokenPermissions::allows_owner)
        .context("URL does not grant owner access")?
        .clone();
    Ok((TsfClient::with_api_base_url(api_url), locator, owner_token))
}

fn print_created_stream(
    web_url: &Url,
    created: &CreateStreamResponse,
    format: OutputFormat,
    target: OutputTarget,
) -> eyre::Result<()> {
    match format {
        OutputFormat::Text => {
            target.print_line(&format!(
                "Created {} stream {}",
                visibility_label(created.visibility),
                created.stream_id
            ));
            target.print_line(&format!(
                "Retention: {}",
                humanize_retention(created.retention_secs)
            ));
            let mut links = created
                .tokens
                .iter()
                .map(|issued| {
                    Ok((
                        link_label(issued.permissions),
                        stream_url(
                            web_url,
                            &created.stream_id,
                            issued.permissions,
                            &issued.token,
                        )?,
                        if issued.permissions.allows_owner() {
                            "  (keep private)"
                        } else {
                            ""
                        },
                    ))
                })
                .collect::<Result<Vec<_>, tailsurf::stream_url::StreamUrlError>>()?;
            if matches!(created.visibility, Visibility::Public) {
                links.push((
                    "view",
                    bare_stream_url(web_url, &created.stream_id),
                    "  (public)",
                ));
            }
            if !links.is_empty() {
                target.print_line("");
                links.sort_by_key(|(label, _, _)| link_rank(label));
                let width = links
                    .iter()
                    .map(|(label, _, _)| label.len())
                    .max()
                    .unwrap_or(0);
                for (label, url, suffix) in &links {
                    target.print_line(&format!("  {label:<width$}  {url}{suffix}"));
                }
                target.print_line("");
                target.print_line("Links are shown once.");
            }
        }
        OutputFormat::Json => {
            let output = CreatedStreamOutput {
                stream_id: created.stream_id.to_string(),
                visibility: visibility_label(created.visibility),
                retention_secs: created.retention_secs,
                urls: created
                    .tokens
                    .iter()
                    .map(|issued| {
                        Ok((
                            issued.permissions.to_string(),
                            stream_url(
                                web_url,
                                &created.stream_id,
                                issued.permissions,
                                &issued.token,
                            )?
                            .to_string(),
                        ))
                    })
                    .collect::<Result<BTreeMap<_, _>, tailsurf::stream_url::StreamUrlError>>()?,
            };
            target.print_line(&serde_json::to_string_pretty(&output)?);
        }
    }
    Ok(())
}

fn print_stream_info(stream: &StreamInfoResponse, format: OutputFormat) -> eyre::Result<()> {
    match format {
        OutputFormat::Text => {
            println!("Stream {}", stream.stream_id);
            println!("Visibility: {}", visibility_label(stream.visibility));
            println!("State: {}", stream.state);
            println!("Retention: {}", humanize_retention(stream.retention_secs));
            println!("Active links: {}", stream.active_token_count);
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
    let url = stream_url(web_url, stream_id, issued.permissions, &issued.token)?;
    match format {
        OutputFormat::Text => {
            println!("Issued {} link", link_label(issued.permissions));
            println!("  url  {url}");
            println!("  id   {}", issued.token_id);
            println!("Link is shown once. Revoke it with the id above.");
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
        TokenPermissions::owner(),
        "owner",
    )?;
    write_token_file(
        &args.view_token_file,
        tokens,
        TokenPermissions::read(),
        "view",
    )?;
    write_token_file(
        &args.write_token_file,
        tokens,
        TokenPermissions::write(),
        "write",
    )?;
    Ok(())
}

fn write_token_file(
    path: &Option<PathBuf>,
    tokens: &[IssuedStreamToken],
    permissions: TokenPermissions,
    label: &str,
) -> eyre::Result<()> {
    let Some(path) = path else {
        return Ok(());
    };
    let token = tokens
        .iter()
        .find(|token| token.permissions == permissions)
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

fn write_new_default_links(visibility: Visibility) -> Vec<TokenPermissions> {
    match visibility {
        Visibility::Private => vec![TokenPermissions::owner(), TokenPermissions::read()],
        Visibility::Public => vec![TokenPermissions::owner()],
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

fn bare_stream_url(base_url: &Url, stream_id: &StreamId) -> Url {
    let mut url = base_url.clone();
    url.set_path(&format!("/s/{stream_id}"));
    url.set_query(None);
    url.set_fragment(None);
    url
}

fn link_label(permissions: TokenPermissions) -> &'static str {
    match permissions.to_string().as_str() {
        "o" => "owner",
        "r" => "view",
        "w" => "write",
        "rw" => "view+write",
        _ => "link",
    }
}

fn link_rank(label: &str) -> usize {
    match label {
        "view" => 0,
        "write" => 1,
        "view+write" => 2,
        "owner" => 3,
        _ => 4,
    }
}

fn humanize_retention(secs: u64) -> String {
    if secs == u64::MAX {
        return "infinite".to_owned();
    }
    const MINUTE: u64 = 60;
    const HOUR: u64 = 60 * MINUTE;
    const DAY: u64 = 24 * HOUR;
    let unit =
        |count: u64, name: &str| format!("{count} {name}{}", if count == 1 { "" } else { "s" });
    if secs >= DAY && secs.is_multiple_of(DAY) {
        return unit(secs / DAY, "day");
    }
    if secs >= HOUR && secs.is_multiple_of(HOUR) {
        return unit(secs / HOUR, "hour");
    }
    if secs >= MINUTE && secs.is_multiple_of(MINUTE) {
        return unit(secs / MINUTE, "minute");
    }
    unit(secs, "second")
}

fn selected_read_start(
    seq_num: Option<u64>,
    timestamp: Option<u64>,
    default: ReadStart,
) -> ReadStart {
    seq_num
        .map(ReadStart::SeqNum)
        .or_else(|| timestamp.map(ReadStart::TimestampMs))
        .unwrap_or(default)
}

fn replay_count_from_tail(options: &ReadStreamOptions, next_s2_seq_num: u64) -> Option<u64> {
    match options.start {
        Some(ReadStart::SeqNum(seq_num)) => Some(next_s2_seq_num.saturating_sub(seq_num)),
        None => Some(next_s2_seq_num),
        Some(ReadStart::TimestampMs(_) | ReadStart::TailOffset(_)) => None,
    }
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
    visibility: &'static str,
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

#[cfg(test)]
mod tests {
    use super::*;

    const VIEW_URL: &str = "https://tail.surf/s/0123456789abcdefghjkmnpqrstvwxyz#r=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

    #[test]
    fn parses_update_and_check_modes() {
        for (arguments, expected_check) in [
            (&["tsf", "update"][..], false),
            (&["tsf", "update", "--check"][..], true),
        ] {
            let cli = Cli::try_parse_from(arguments).expect("valid update command");
            let Some(Command::Update(args)) = cli.command else {
                panic!("expected update command");
            };
            assert_eq!(args.check, expected_check);
        }
    }

    #[test]
    fn update_hints_require_a_successful_interactive_production_command() {
        let production = Url::parse("https://tail.surf").expect("production URL");
        let non_production = Url::parse("https://api.example").expect("custom URL");
        let service = Cli::try_parse_from(["tsf", "tail", VIEW_URL])
            .expect("valid streaming service command")
            .command
            .expect("tail subcommand");
        let check = |command, api_url, terminal, disabled| {
            should_check_for_update_hint(command, api_url, terminal, disabled)
        };
        assert!(check(&service, &production, true, false));
        assert!(!check(&service, &production, false, false));
        assert!(!check(&service, &production, true, true));
        assert!(!check(&service, &non_production, true, false));

        let update = Cli::try_parse_from(["tsf", "update"])
            .expect("valid update command")
            .command
            .expect("update subcommand");
        assert!(!check(&update, &production, true, false));
    }

    #[test]
    fn update_hint_cache_has_a_bounded_daily_interval() {
        const NOW: u64 = 2_000_000;
        let interval = UPDATE_HINT_INTERVAL.as_secs();

        assert!(update_hint_check_is_due(None, NOW));
        assert!(!update_hint_check_is_due(Some(NOW), NOW));
        assert!(!update_hint_check_is_due(Some(NOW - interval + 1), NOW));
        assert!(update_hint_check_is_due(Some(NOW - interval), NOW));
        assert!(!update_hint_check_is_due(Some(NOW + interval - 1), NOW));
        assert!(update_hint_check_is_due(Some(NOW + interval), NOW));
        assert!(!claim_update_hint_check(Path::new("."), NOW));
    }

    #[test]
    fn classifies_wrapped_broken_pipes_only() {
        let broken_pipe: eyre::Report =
            std::io::Error::new(ErrorKind::BrokenPipe, "closed consumer").into();
        assert!(is_broken_pipe(
            &broken_pipe.wrap_err("failed to write stdout")
        ));

        let connection_reset: eyre::Report =
            std::io::Error::new(ErrorKind::ConnectionReset, "reset consumer").into();
        assert!(!is_broken_pipe(&connection_reset));
    }
}
