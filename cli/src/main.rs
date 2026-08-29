//! `tsf` command-line client for creating, writing, replaying, tailing, and managing Tailsurf
//! streams.

use std::{
    collections::{HashSet, VecDeque},
    fmt,
    fs::{self, OpenOptions},
    io::{ErrorKind, IsTerminal, Write as _},
    path::{Path, PathBuf},
    process::{ExitCode, ExitStatus, Stdio},
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use axoupdater::AxoUpdater;
use bytes::{Buf, Bytes, BytesMut};
use clap::{Args, Parser, Subcommand, ValueEnum};
use eyre::{Context, ContextCompat, bail, eyre};
use memchr::memchr;
use serde::{Deserialize, Serialize};
use tailsurf::{
    AppendBatch, AppendTicket, DEFAULT_API_ORIGIN, DurableWriterOptions, LinkId, LinkPermissions,
    LinkSecret, MAX_WRITER_IN_FLIGHT_PAYLOAD_BYTES, MAX_WRITER_IN_FLIGHT_RECORDS, ReadOptions,
    ReadStart, ReadStop, StreamId, StreamTitle, TsfClient, TsfProducer, TsfReadSession,
    TsfSseReadSession, TsfWriter, default_api_origin,
    protocol::{
        rest::{
            CreateLinkInput, CreateStreamRequest, CreateStreamResponse, InitialStreamLink,
            MAX_INITIAL_STREAM_LINKS, StreamLinkCredential, StreamMetadata, StreamTitleUpdate,
            UpdateStreamRequest, Visibility,
        },
        ws::frame::{MAX_RECORD_PAYLOAD_BYTES, PartHeader, RecordFormat},
    },
    stream_url::{StreamLocator, public_stream_url, stream_link},
    transcript::{DEFAULT_MAX_TRANSCRIPT_REASSEMBLY_BYTES, LogicalTranscript},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufWriter},
    process::Command as TokioCommand,
    sync::{Notify, mpsc},
    time::{Duration, Instant, sleep_until, timeout},
};
use url::Url;

const INTERRUPT_EXIT_CODE: i32 = 130;
const BYTE_RECORD_LINGER: Duration = Duration::from_millis(10);
/// Stdout batching window for `tail` and `replay`.
const TRANSCRIPT_OUTPUT_BUFFER_BYTES: usize = 64 * 1024;
/// Read batches held while stdout drains. Each frame carries at most MAX_READ_FRAME_RECORDS
/// records and about 1 MiB of payload backing, so the queue bounds in-flight output to roughly
/// 8 MiB plus the batch being printed and transcript split-part pending state.
const TRANSCRIPT_BATCH_QUEUE: usize = 8;
/// Stdin read block size for line-framed and byte-record writes.
const STDIN_READ_BYTES: usize = 16 * 1024;
const UPDATE_HINT_CACHE_FILE: &str = ".tailsurf-cli-update-check";
const UPDATE_HINT_RETRY_INTERVAL: Duration = Duration::from_secs(60 * 60);
const UPDATE_HINT_SUCCESS_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);
const UPDATE_HINT_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct UpdateHintCheckCache {
    last_attempt_at: u64,
    last_success_at: Option<u64>,
}

#[derive(Debug, Parser)]
#[command(name = "tsf")]
#[command(version, about = "Create, write, and read tail.surf streams")]
#[command(
    after_help = "Create a stream from piped input:\n  anything | tsf\n  anything | tsf new\n\nCapture a program in a new stream:\n  tsf new -- program\n\nWrite to an existing stream:\n  anything | tsf write WRITE_LINK"
)]
struct Cli {
    /// Tailsurf service origin.
    #[arg(
        long,
        env = "TSF_ORIGIN",
        default_value = DEFAULT_API_ORIGIN,
        global = true,
        help_heading = "Connection"
    )]
    origin: Url,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Create a stream and print its links.
    New(NewArgs),
    /// Write piped input or a program's output to an existing stream.
    Write(WriteArgs),
    /// Follow a stream, optionally starting from existing records.
    Tail(TailArgs),
    /// Print existing records and stop at the current tail.
    Replay(ReplayArgs),
    /// Show current stream metadata.
    Info(InfoArgs),
    /// Permanently delete a stream.
    Delete(DeleteArgs),
    /// Change stream visibility.
    Visibility(VisibilityArgs),
    /// Set or clear a stream title.
    Title(TitleArgs),
    /// Extend a stream's expiration.
    Renew(RenewArgs),
    /// Manage links.
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
    /// Human-facing stream title.
    #[arg(long, value_name = "TITLE")]
    title: Option<StreamTitle>,
    /// Allow anonymous reads.
    #[arg(long)]
    public: bool,
    /// Create an additional link with the stream, as LINK_ID=PERMISSION. May be repeated.
    #[arg(long = "link", value_name = "LINK_ID=PERMISSION")]
    links: Vec<InitialLinkArg>,
    #[arg(
        long,
        value_name = "DURATION",
        help = "Stream lifetime, such as 6h or 7d"
    )]
    expires: Option<StreamExpiryArg>,
    /// Print one JSON object instead of human-readable output.
    #[arg(long)]
    json: bool,
    /// Write the complete owner link to this file.
    #[arg(long = "owner-link-file", value_name = "PATH")]
    owner_link_file: Option<PathBuf>,
    /// Write the complete read-only link to this file.
    #[arg(long = "read-link-file", value_name = "PATH")]
    read_link_file: Option<PathBuf>,
    /// Write the complete write-only link to this file. Requires a write link.
    #[arg(long = "write-link-file", value_name = "PATH")]
    write_link_file: Option<PathBuf>,
    #[command(flatten)]
    input: InputArgs,
}

#[derive(Debug, Args)]
struct WriteArgs {
    /// Write-capable link or @path containing one.
    #[arg(value_name = "WRITE_LINK")]
    link: LinkInput,
    /// Require the stream to begin this writer session at this sequence.
    #[arg(long, value_name = "SEQ_NUM", help_heading = "Advanced")]
    expected_next_seq_num: Option<u64>,
    #[command(flatten)]
    input: InputArgs,
}

#[derive(Debug, Args)]
struct InputArgs {
    /// Preserve input as arbitrary byte records instead of newline-delimited transcript records.
    #[arg(long)]
    bytes: bool,
    /// Program to run. Its stdout and stderr are written to the stream.
    #[arg(last = true, value_name = "PROGRAM")]
    program: Vec<String>,
}

impl InputArgs {
    fn piped_defaults() -> Self {
        Self {
            bytes: false,
            program: Vec::new(),
        }
    }
}

impl NewArgs {
    fn piped_defaults() -> Self {
        Self {
            title: None,
            public: false,
            links: Vec::new(),
            expires: None,
            json: false,
            owner_link_file: None,
            read_link_file: None,
            write_link_file: None,
            input: InputArgs::piped_defaults(),
        }
    }
}

#[derive(Debug, Args)]
struct TailArgs {
    /// Read-capable link, public stream URL, or @path containing one.
    #[arg(value_name = "STREAM_LINK_OR_URL")]
    link: LinkInput,
    #[command(flatten)]
    read: ReadArgs,
}

#[derive(Debug, Args)]
struct ReadArgs {
    /// Use resumable HTTP event streaming instead of the binary WebSocket transport.
    #[arg(long)]
    sse: bool,
    /// Start this many records before the current tail.
    #[arg(short = 'n', long, conflicts_with_all = ["seq", "since"])]
    last: Option<u64>,
    /// Start at this absolute sequence number.
    #[arg(long, conflicts_with_all = ["last", "since"])]
    seq: Option<u64>,
    /// Start at an RFC 3339 time or this duration ago, such as 15m.
    #[arg(long, conflicts_with_all = ["last", "seq"])]
    since: Option<SinceArg>,
    /// Read at most this many records.
    #[arg(long)]
    count: Option<u64>,
    /// Maximum bytes used to reassemble split transcript records.
    #[arg(
        long,
        value_name = "BYTES",
        default_value_t = DEFAULT_MAX_TRANSCRIPT_REASSEMBLY_BYTES,
        help_heading = "Advanced"
    )]
    max_reassembly_bytes: usize,
}

#[derive(Debug, Args)]
struct ReplayArgs {
    /// Read-capable link, public stream URL, or @path containing one.
    #[arg(value_name = "STREAM_LINK_OR_URL")]
    link: LinkInput,
    #[command(flatten)]
    read: ReadArgs,
}

#[derive(Debug, Args)]
struct InfoArgs {
    /// Read-capable link, public stream URL, or @path containing one.
    #[arg(value_name = "STREAM_LINK_OR_URL")]
    link: LinkInput,
    /// Print one JSON object instead of human-readable output.
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
struct DeleteArgs {
    /// Owner link or @path containing one.
    #[arg(value_name = "OWNER_LINK")]
    owner_link: LinkInput,
    /// Skip the interactive confirmation.
    #[arg(long)]
    yes: bool,
    /// Print one JSON object instead of human-readable output.
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
struct VisibilityArgs {
    /// Owner link or @path containing one.
    #[arg(value_name = "OWNER_LINK")]
    owner_link: LinkInput,
    /// New visibility.
    visibility: VisibilityArg,
    /// Print one JSON object instead of human-readable output.
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
struct TitleArgs {
    #[command(subcommand)]
    command: TitleCommand,
}

#[derive(Debug, Subcommand)]
enum TitleCommand {
    /// Set the stream title.
    Set(SetTitleArgs),
    /// Remove the stream title.
    Clear(ClearTitleArgs),
}

#[derive(Debug, Args)]
struct SetTitleArgs {
    /// Owner link or @path containing one.
    #[arg(value_name = "OWNER_LINK")]
    owner_link: LinkInput,
    /// New stream title.
    #[arg(value_name = "TITLE")]
    title: StreamTitle,
    /// Print one JSON object instead of human-readable output.
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
struct ClearTitleArgs {
    /// Owner link or @path containing one.
    #[arg(value_name = "OWNER_LINK")]
    owner_link: LinkInput,
    /// Print one JSON object instead of human-readable output.
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
struct RenewArgs {
    /// Owner link or @path containing one.
    #[arg(value_name = "OWNER_LINK")]
    owner_link: LinkInput,
    /// New lifetime from now, such as 6h or 7d.
    #[arg(value_name = "DURATION")]
    expires: StreamExpiryArg,
    /// Print one JSON object instead of human-readable output.
    #[arg(long)]
    json: bool,
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
    /// Create a link and print it once.
    Create(CreateLinkArgs),
    /// Revoke a link by its ID.
    Revoke(RevokeLinkArgs),
}

#[derive(Debug, Args)]
struct ListLinkArgs {
    /// Owner link or @path containing one.
    #[arg(value_name = "OWNER_LINK")]
    owner_link: LinkInput,
    /// Print one JSON object instead of human-readable output.
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
struct CreateLinkArgs {
    /// Owner link or @path containing one.
    #[arg(value_name = "OWNER_LINK")]
    owner_link: LinkInput,
    /// Immutable Link ID and permission, as LINK_ID=PERMISSION.
    #[arg(value_name = "LINK_ID=PERMISSION")]
    link: InitialLinkArg,
    /// Expiry such as 1h, 7d, or never.
    #[arg(long, value_name = "EXPIRY", default_value = "never")]
    expires: ExpiresArg,
    /// Print one JSON object instead of human-readable output.
    #[arg(long)]
    json: bool,
    /// Write the complete new link to this file.
    #[arg(long = "link-file", value_name = "PATH")]
    link_file: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct RevokeLinkArgs {
    /// Owner link or @path containing one.
    #[arg(value_name = "OWNER_LINK")]
    owner_link: LinkInput,
    /// Link ID to revoke.
    #[arg(value_name = "LINK_ID")]
    link_id: LinkId,
    /// Print one JSON object instead of human-readable output.
    #[arg(long)]
    json: bool,
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

#[derive(Clone)]
struct LinkInput(String);

impl LinkInput {
    fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for LinkInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("LinkInput(<redacted>)")
    }
}

impl FromStr for LinkInput {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let Some(path) = value.strip_prefix('@') else {
            return Ok(Self(value.to_owned()));
        };
        if path.is_empty() {
            return Err("link file path after @ must not be empty".to_owned());
        }
        let link = fs::read_to_string(path)
            .map_err(|error| format!("failed to read link file {path}: {error}"))?;
        let link = link.trim();
        if link.is_empty() {
            return Err(format!("link file {path} is empty"));
        }
        if link.lines().count() != 1 {
            return Err(format!("link file {path} must contain exactly one link"));
        }
        Ok(Self(link.to_owned()))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SinceArg(u64);

impl FromStr for SinceArg {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let timestamp = match humantime::parse_duration(value) {
            Ok(duration) => SystemTime::now()
                .checked_sub(duration)
                .ok_or_else(|| "relative start time is before the Unix epoch".to_owned())?,
            Err(_) => humantime::parse_rfc3339(value).map_err(|_| {
                "start time must be a duration such as 15m or an RFC 3339 timestamp".to_owned()
            })?,
        };
        let millis = timestamp
            .duration_since(UNIX_EPOCH)
            .map_err(|_| "start time must not be before the Unix epoch".to_owned())?
            .as_millis();
        let millis = u64::try_from(millis).map_err(|_| "start time is too large".to_owned())?;
        Ok(Self(millis))
    }
}

fn parse_duration_arg(value: &str, what: &str) -> Result<Duration, String> {
    let duration = humantime::parse_duration(value)
        .map_err(|error| format!("invalid {what} duration: {error}"))?;
    if duration.is_zero() {
        return Err(format!("{what} must be at least one second"));
    }
    Ok(duration)
}

fn rfc3339_from_now(duration: Duration, what: &str) -> eyre::Result<String> {
    let expires_at = SystemTime::now()
        .checked_add(duration)
        .ok_or_else(|| eyre!("{what} is too large"))?;
    // humantime only formats years 0000..=9999 and its Display-to-String panics outside that.
    let unix_secs = expires_at
        .duration_since(UNIX_EPOCH)
        .map_err(|_| eyre!("{what} is too large"))?;
    if unix_secs.as_secs() > 253_402_300_799 {
        return Err(eyre!("{what} is too large"));
    }
    Ok(humantime::format_rfc3339_seconds(expires_at).to_string())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct StreamExpiryArg(Duration);

impl FromStr for StreamExpiryArg {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let duration = parse_duration_arg(value, "stream expiry")?;
        if duration.subsec_nanos() != 0 {
            return Err("stream expiry must be a whole number of seconds".to_owned());
        }
        Ok(Self(duration))
    }
}

impl StreamExpiryArg {
    fn seconds(self) -> u64 {
        self.0.as_secs()
    }

    fn rfc3339(self) -> eyre::Result<String> {
        rfc3339_from_now(self.0, "stream expiry")
    }
}

#[derive(Clone, Copy, Debug)]
struct PermissionArg(LinkPermissions);

impl FromStr for PermissionArg {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value = value.to_ascii_lowercase();
        let short = match value.as_str() {
            "read" => "r",
            "write" => "w",
            "read-write" => "rw",
            "owner" => "o",
            other => other,
        };
        short.parse().map(Self).map_err(|_| {
            format!("unknown permission {value:?}; use read, write, read-write, or owner")
        })
    }
}

#[derive(Clone, Debug)]
struct InitialLinkArg(InitialStreamLink);

impl FromStr for InitialLinkArg {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (link_id, permission) = value
            .split_once('=')
            .ok_or_else(|| "link must use LINK_ID=PERMISSION".to_owned())?;
        Ok(Self(InitialStreamLink::new(
            link_id
                .parse()
                .map_err(|error| format!("invalid Link ID: {error}"))?,
            permission.parse::<PermissionArg>()?.0,
        )))
    }
}

#[derive(Clone, Copy, Debug)]
enum ExpiresArg {
    Never,
    In(Duration),
}

impl ExpiresArg {
    fn rfc3339(self) -> eyre::Result<Option<String>> {
        match self {
            Self::Never => Ok(None),
            Self::In(duration) => rfc3339_from_now(duration, "link expiry").map(Some),
        }
    }
}

impl FromStr for ExpiresArg {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.eq_ignore_ascii_case("never") {
            return Ok(Self::Never);
        }
        parse_duration_arg(value, "expiry").map(Self::In)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WriteBuffering {
    Bytes,
    Lines,
}

// One socket, one stdin, one stdout: worker threads only add wakeup and handoff cost.
#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let Cli { origin, command } = Cli::parse();
    let stdin_is_terminal = std::io::stdin().is_terminal();
    if command.is_none() && stdin_is_terminal {
        let help = <Cli as clap::CommandFactory>::command().render_help();
        eprint!("{help}");
        return ExitCode::from(2);
    }
    let command = command.unwrap_or_else(|| Command::New(NewArgs::piped_defaults()));
    let check_for_update = should_check_for_update_hint(
        matches!(command, Command::Update(_)),
        &origin,
        std::io::stderr().is_terminal(),
        automatic_update_checks_disabled(),
    );
    let result = run(origin, command).await;
    match result {
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

async fn run(origin: Url, command: Command) -> eyre::Result<()> {
    match command {
        Command::New(args) => new_stream(origin, args).await,
        Command::Write(args) => write_stream(origin, args).await,
        Command::Tail(args) => tail_stream(origin, args).await,
        Command::Replay(args) => replay_stream(origin, args).await,
        Command::Info(args) => stream_metadata(origin, args).await,
        Command::Delete(args) => delete_stream(origin, args).await,
        Command::Visibility(args) => update_visibility(origin, args).await,
        Command::Title(args) => update_title(origin, args).await,
        Command::Renew(args) => renew_stream(origin, args).await,
        Command::Link(args) => link_command(origin, args).await,
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
    is_update_command: bool,
    origin: &Url,
    stderr_is_terminal: bool,
    disabled: bool,
) -> bool {
    stderr_is_terminal && !disabled && !is_update_command && *origin == default_api_origin()
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
    let Some(mut cache) = claim_update_hint_check(cache_path.as_std_path(), now.as_secs()) else {
        return;
    };

    let Ok(Ok(update_needed)) = timeout(UPDATE_HINT_TIMEOUT, updater.is_update_needed()).await
    else {
        return;
    };
    cache.last_success_at = Some(now.as_secs());
    let _ = write_update_hint_check_cache(cache_path.as_std_path(), cache);

    if update_needed {
        eprintln!("A tsf update is available. Run `tsf update` to install it.");
    }
}

fn claim_update_hint_check(cache_path: &Path, now: u64) -> Option<UpdateHintCheckCache> {
    let previous = fs::read(cache_path)
        .ok()
        .and_then(|value| serde_json::from_slice::<UpdateHintCheckCache>(&value).ok());
    if !update_hint_check_is_due(previous.as_ref(), now) {
        return None;
    }

    let cache = UpdateHintCheckCache {
        last_attempt_at: now,
        last_success_at: previous.and_then(|cache| cache.last_success_at),
    };
    write_update_hint_check_cache(cache_path, cache).then_some(cache)
}

fn write_update_hint_check_cache(cache_path: &Path, cache: UpdateHintCheckCache) -> bool {
    let Ok(mut value) = serde_json::to_vec(&cache) else {
        return false;
    };
    value.push(b'\n');
    fs::write(cache_path, value).is_ok()
}

fn update_hint_check_is_due(cache: Option<&UpdateHintCheckCache>, now: u64) -> bool {
    let Some(cache) = cache else {
        return true;
    };
    let success_is_current = cache.last_success_at.is_some_and(|last_success| {
        last_success.abs_diff(now) < UPDATE_HINT_SUCCESS_INTERVAL.as_secs()
    });
    !success_is_current
        && cache.last_attempt_at.abs_diff(now) >= UPDATE_HINT_RETRY_INTERVAL.as_secs()
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

async fn new_stream(origin: Url, args: NewArgs) -> eyre::Result<()> {
    let visibility = if args.public {
        Visibility::Public
    } else {
        Visibility::Private
    };
    let links = new_stream_links(&args, visibility)?;

    let created = TsfClient::with_api_origin(origin.clone())?
        .create_stream(&CreateStreamRequest {
            title: args.title.clone(),
            visibility,
            expires_in_seconds: args.expires.map(StreamExpiryArg::seconds),
            links,
        })
        .await
        .context("failed to create stream")?;
    print_created_stream(&created, args.json)?;
    write_link_files(&created, &args)?;

    if args.input.program.is_empty() && std::io::stdin().is_terminal() {
        return Ok(());
    }

    let owner_link_secret = created
        .links
        .iter()
        .find(|link| link.permissions == LinkPermissions::owner())
        .context("created stream did not include an owner link")?
        .secret
        .clone();
    write_input(
        origin,
        created.stream_id,
        owner_link_secret,
        None,
        args.input,
    )
    .await
}

async fn write_stream(origin: Url, args: WriteArgs) -> eyre::Result<()> {
    let locator = StreamLocator::parse(args.link.as_str()).context("invalid stream link")?;
    let link = locator
        .link_declaring(LinkPermissions::allows_write)
        .context("link does not declare write permission")?
        .clone();
    write_input(
        origin,
        locator.stream_id,
        link,
        args.expected_next_seq_num,
        args.input,
    )
    .await
}

fn new_stream_links(
    args: &NewArgs,
    visibility: Visibility,
) -> eyre::Result<Vec<InitialStreamLink>> {
    let mut links = args
        .links
        .iter()
        .map(|link| link.0.clone())
        .collect::<Vec<_>>();
    if !links.iter().any(|link| link.permissions.allows_owner()) {
        if links.iter().any(|link| link.link_id.as_str() == "owner") {
            bail!(
                "Link ID \"owner\" is reserved for the default owner link; choose another ID or give that link owner permission"
            );
        }
        links.insert(0, initial_link("owner", LinkPermissions::owner())?);
    }
    if matches!(visibility, Visibility::Private)
        && !links
            .iter()
            .any(|link| link.permissions == LinkPermissions::read())
    {
        if links.iter().any(|link| link.link_id.as_str() == "reader") {
            bail!(
                "Link ID \"reader\" is reserved for the default read link on private streams; choose another ID or give that link read-only permission"
            );
        }
        links.push(initial_link("reader", LinkPermissions::read())?);
    }
    if links.len() > MAX_INITIAL_STREAM_LINKS {
        bail!(
            "at most {MAX_INITIAL_STREAM_LINKS} initial links may be created, including the default owner and private reader links"
        );
    }
    let mut link_ids = HashSet::with_capacity(links.len());
    if links
        .iter()
        .any(|link| !link_ids.insert(link.link_id.as_str()))
    {
        bail!("initial Link IDs must be unique");
    }
    if args.read_link_file.is_some()
        && !links
            .iter()
            .any(|link| link.permissions == LinkPermissions::read())
    {
        bail!("--read-link-file requires a link with read permission");
    }
    if args.write_link_file.is_some()
        && !links
            .iter()
            .any(|link| link.permissions == LinkPermissions::write())
    {
        bail!("--write-link-file requires a link with write permission");
    }
    Ok(links)
}

async fn write_input(
    origin: Url,
    stream_id: StreamId,
    link: LinkSecret,
    expected_next_seq_num: Option<u64>,
    input: InputArgs,
) -> eyre::Result<()> {
    let buffering = if input.bytes {
        WriteBuffering::Bytes
    } else {
        WriteBuffering::Lines
    };
    if input.program.is_empty() {
        stream_stdin_to_writer(origin, stream_id, link, expected_next_seq_num, buffering).await
    } else {
        stream_command_to_writer(
            origin,
            stream_id,
            link,
            expected_next_seq_num,
            buffering,
            input.program,
        )
        .await
    }
}

fn print_write_summary(records: u64) {
    let noun = if records == 1 { "record" } else { "records" };
    eprintln!("{records} {noun} durable");
}

async fn finish_and_close_writer(
    session: WriterSession,
    writer: TsfWriter,
    interrupted: bool,
) -> eyre::Result<u64> {
    if interrupted {
        eprintln!(
            "Interrupted. Input stopped. Waiting for accepted records to become durable; press Ctrl-C again to stop immediately."
        );
    }

    let shutdown_interrupt = tokio::signal::ctrl_c();
    tokio::pin!(shutdown_interrupt);
    let records = tokio::select! {
        result = session.finish() => result?,
        interrupt = &mut shutdown_interrupt => {
            interrupt.context("failed to listen for interrupt signal")?;
            exit_interrupted();
        }
    };
    tokio::select! {
        result = writer.close() => result.context("failed to close writer")?,
        interrupt = &mut shutdown_interrupt => {
            interrupt.context("failed to listen for interrupt signal")?;
            exit_interrupted();
        }
    }
    Ok(records)
}

async fn connect_session_writer(
    origin: Url,
    stream_id: StreamId,
    link: LinkSecret,
    expected_next_seq_num: Option<u64>,
) -> eyre::Result<TsfWriter> {
    let client = TsfClient::with_api_origin(origin)?;
    let mut options = DurableWriterOptions::new(stream_id, link);
    options.expected_next_seq_num = expected_next_seq_num;
    client
        .connect_writer(options)
        .await
        .context("failed to connect writer")
}

async fn stream_stdin_to_writer(
    origin: Url,
    stream_id: StreamId,
    link: LinkSecret,
    expected_next_seq_num: Option<u64>,
    buffering: WriteBuffering,
) -> eyre::Result<()> {
    let writer = connect_session_writer(origin, stream_id, link, expected_next_seq_num).await?;
    let interrupt = Arc::new(WriteInterrupt::default());

    let (chunk_tx, mut chunk_rx) = mpsc::channel::<eyre::Result<Bytes>>(16);
    // The first Ctrl-C stops the consumer before its next chunk and drops the sender. The
    // consumer flushes its current chunk and finishes the session before the interrupted exit.
    let stdin_interrupt = Arc::clone(&interrupt);
    let stdin_task = tokio::spawn(async move {
        tokio::select! {
            biased;
            interrupt = tokio::signal::ctrl_c() => {
                interrupt.context("failed to listen for interrupt signal")?;
                stdin_interrupt.trigger();
                Ok(true)
            }
            result = read_pipe_chunks(tokio::io::stdin(), chunk_tx, "failed to read stdin") => {
                result.map(|()| false)
            }
        }
    });
    let mut session = WriterSession::new(&writer, interrupt);
    match buffering {
        WriteBuffering::Bytes => stream_byte_chunks_to_writer(&mut session, &mut chunk_rx).await?,
        WriteBuffering::Lines => stream_line_chunks_to_writer(&mut session, &mut chunk_rx).await?,
    }
    let interrupted = stdin_task.await.context("stdin reader task panicked")??;
    let records = finish_and_close_writer(session, writer, interrupted).await?;
    print_write_summary(records);
    if interrupted {
        exit_interrupted();
    }
    Ok(())
}

async fn stream_command_to_writer(
    origin: Url,
    stream_id: StreamId,
    link: LinkSecret,
    expected_next_seq_num: Option<u64>,
    buffering: WriteBuffering,
    command: Vec<String>,
) -> eyre::Result<()> {
    let writer = connect_session_writer(origin, stream_id, link, expected_next_seq_num).await?;
    let interrupt = Arc::new(WriteInterrupt::default());
    let mut session = WriterSession::new(&writer, Arc::clone(&interrupt));
    let outcome = stream_child_command_output(&mut session, interrupt, buffering, command).await?;
    let records = finish_and_close_writer(session, writer, outcome.interrupted).await?;
    print_write_summary(records);
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
    session: &mut WriterSession,
    write_interrupt: Arc<WriteInterrupt>,
    buffering: WriteBuffering,
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
    let stdout_task = tokio::spawn(read_pipe_chunks(
        stdout,
        chunk_tx.clone(),
        "failed to read command output",
    ));
    let stderr_task = tokio::spawn(read_pipe_chunks(
        stderr,
        chunk_tx,
        "failed to read command output",
    ));

    let (stream_result, interrupted) = {
        let stream_output = async {
            match buffering {
                WriteBuffering::Bytes => {
                    stream_byte_chunks_to_writer(session, &mut chunk_rx).await?
                }
                WriteBuffering::Lines => {
                    stream_line_chunks_to_writer(session, &mut chunk_rx).await?
                }
            }
            eyre::Result::<()>::Ok(())
        };
        tokio::pin!(stream_output);

        tokio::select! {
            result = &mut stream_output => (result, false),
            interrupt = tokio::signal::ctrl_c() => {
                interrupt.context("failed to listen for interrupt signal")?;
                write_interrupt.trigger();
                let _ = child.kill().await;
                (stream_output.await, true)
            }
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

    if interrupted {
        chunk_rx.close();
    }
    stdout_task.await.context("stdout reader task panicked")??;
    stderr_task.await.context("stderr reader task panicked")??;
    let status = child.wait().await.context("failed to wait for command")?;
    Ok(ChildCommandOutcome {
        status,
        interrupted,
    })
}

async fn read_pipe_chunks<R>(
    mut pipe: R,
    chunk_tx: mpsc::Sender<eyre::Result<Bytes>>,
    read_error_context: &'static str,
) -> eyre::Result<()>
where
    R: AsyncRead + Unpin,
{
    let mut buffer = BytesMut::with_capacity(STDIN_READ_BYTES);
    loop {
        buffer.reserve(STDIN_READ_BYTES);
        let byte_count = pipe
            .read_buf(&mut buffer)
            .await
            .context(read_error_context)?;
        if byte_count == 0 {
            return Ok(());
        }
        // split_to keeps spare capacity local; a full split would give the whole allocation away.
        if chunk_tx
            .send(Ok(buffer.split_to(byte_count).freeze()))
            .await
            .is_err()
        {
            return Ok(());
        }
    }
}

struct ByteRecordAppender {
    pending: BytesMut,
    deadline: Option<Instant>,
    linger: Duration,
}

impl ByteRecordAppender {
    fn new(linger: Duration) -> Self {
        Self {
            pending: BytesMut::with_capacity(MAX_RECORD_PAYLOAD_BYTES),
            deadline: None,
            linger,
        }
    }

    fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    async fn push_bytes(
        &mut self,
        session: &mut WriterSession,
        mut bytes: &[u8],
    ) -> eyre::Result<()> {
        while !bytes.is_empty() {
            if self.pending.is_empty() {
                self.deadline = Some(Instant::now() + self.linger);
            }
            let take = (MAX_RECORD_PAYLOAD_BYTES - self.pending.len()).min(bytes.len());
            self.pending.extend_from_slice(&bytes[..take]);
            bytes = &bytes[take..];
            if self.pending.len() == MAX_RECORD_PAYLOAD_BYTES {
                self.flush(session).await?;
            }
        }
        Ok(())
    }

    async fn flush(&mut self, session: &mut WriterSession) -> eyre::Result<()> {
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

async fn stream_byte_chunks_to_writer(
    session: &mut WriterSession,
    chunk_rx: &mut mpsc::Receiver<eyre::Result<Bytes>>,
) -> eyre::Result<()> {
    let mut appender = ByteRecordAppender::new(BYTE_RECORD_LINGER);
    loop {
        if let Some(deadline) = appender.deadline() {
            tokio::select! {
                biased;
                _ = session.interrupted() => break,
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
            let chunk = tokio::select! {
                biased;
                _ = session.interrupted() => break,
                chunk = chunk_rx.recv() => chunk,
            };
            let Some(chunk) = chunk else {
                break;
            };
            appender.push_bytes(session, &chunk?).await?;
        }
    }
    appender.flush(session).await
}

async fn stream_line_chunks_to_writer(
    session: &mut WriterSession,
    chunk_rx: &mut mpsc::Receiver<eyre::Result<Bytes>>,
) -> eyre::Result<()> {
    let mut line_appender = LineRecordAppender::new();
    loop {
        let chunk = tokio::select! {
            biased;
            _ = session.interrupted() => break,
            chunk = chunk_rx.recv() => chunk,
        };
        let Some(chunk) = chunk else {
            break;
        };
        line_appender.push_bytes(session, &chunk?).await?;
    }
    line_appender.finish(session).await
}

/// Splits input into transcript records on `\n`. The delimiter is consumed, not stored;
/// every other byte, including `\r`, is record content.
struct LineRecordAppender {
    pending: BytesMut,
}

impl LineRecordAppender {
    fn new() -> Self {
        Self {
            pending: BytesMut::new(),
        }
    }

    async fn push_bytes(
        &mut self,
        session: &mut WriterSession,
        mut bytes: &[u8],
    ) -> eyre::Result<()> {
        while !bytes.is_empty() {
            match memchr(b'\n', bytes) {
                Some(index) => {
                    self.pending.extend_from_slice(&bytes[..index]);
                    bytes = &bytes[index + 1..];
                    self.send_line(session).await?;
                }
                None => {
                    self.pending.extend_from_slice(bytes);
                    break;
                }
            }
        }
        Ok(())
    }

    async fn finish(&mut self, session: &mut WriterSession) -> eyre::Result<()> {
        if !self.pending.is_empty() {
            self.send_line(session).await?;
        }
        Ok(())
    }

    async fn send_line(&mut self, session: &mut WriterSession) -> eyre::Result<()> {
        session
            .append_logical_record(RecordFormat::Transcript, self.pending.split().freeze())
            .await
    }
}

#[derive(Default)]
struct WriteInterrupt {
    triggered: AtomicBool,
    notify: Notify,
}

impl WriteInterrupt {
    fn trigger(&self) {
        self.triggered.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    fn is_triggered(&self) -> bool {
        self.triggered.load(Ordering::Acquire)
    }

    async fn triggered(&self) {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.is_triggered() {
                return;
            }
            notified.await;
        }
    }
}

struct PendingAppend {
    ticket: AppendTicket,
    record_count: usize,
    payload_bytes: usize,
}

struct WriterSession {
    producer: TsfProducer,
    interrupt: Arc<WriteInterrupt>,
    pending: VecDeque<PendingAppend>,
    pending_records: usize,
    pending_payload_bytes: usize,
    records: u64,
}

impl WriterSession {
    fn new(writer: &TsfWriter, interrupt: Arc<WriteInterrupt>) -> Self {
        Self {
            producer: writer.producer(),
            interrupt,
            pending: VecDeque::new(),
            pending_records: 0,
            pending_payload_bytes: 0,
            records: 0,
        }
    }

    async fn interrupted(&self) {
        self.interrupt.triggered().await;
    }

    async fn append_logical_record(
        &mut self,
        format: RecordFormat,
        data: Bytes,
    ) -> eyre::Result<()> {
        let payload_bytes = data.len();
        let batch =
            AppendBatch::split_logical(format, data).context("failed to split logical record")?;
        self.submit_batch(batch, payload_bytes).await
    }

    async fn append_physical_record(
        &mut self,
        part: PartHeader,
        format: RecordFormat,
        data: Bytes,
    ) -> eyre::Result<()> {
        let payload_bytes = data.len();
        let batch = AppendBatch::single(part, format, data).context("failed to build record")?;
        self.submit_batch(batch, payload_bytes).await
    }

    async fn submit_batch(&mut self, batch: AppendBatch, payload_bytes: usize) -> eyre::Result<()> {
        let record_count = batch.record_count();
        self.wait_for_capacity(record_count, payload_bytes).await?;
        let ticket = self
            .producer
            .submit(batch)
            .context("failed to submit record")?;
        self.records += record_count as u64;
        self.pending_records += record_count;
        self.pending_payload_bytes += payload_bytes;
        self.pending.push_back(PendingAppend {
            ticket,
            record_count,
            payload_bytes,
        });
        self.drain_ready_tickets()
    }

    async fn finish(mut self) -> eyre::Result<u64> {
        while !self.pending.is_empty() {
            self.wait_for_oldest().await?;
        }
        Ok(self.records)
    }

    async fn wait_for_capacity(
        &mut self,
        record_count: usize,
        payload_bytes: usize,
    ) -> eyre::Result<()> {
        // Keep ordinary accepted input within one socket window. An empty queue admits one
        // larger logical record intact. An interrupt admits the rest of the current input chunk;
        // the input loops stop before receiving another chunk.
        self.drain_ready_tickets()?;
        while !self.pending.is_empty()
            && !self.interrupt.is_triggered()
            && (self.pending_records.saturating_add(record_count) > MAX_WRITER_IN_FLIGHT_RECORDS
                || self.pending_payload_bytes.saturating_add(payload_bytes)
                    > MAX_WRITER_IN_FLIGHT_PAYLOAD_BYTES)
        {
            let interrupt = Arc::clone(&self.interrupt);
            tokio::select! {
                biased;
                _ = interrupt.triggered() => return Ok(()),
                result = self.wait_for_oldest() => result?,
            }
        }
        Ok(())
    }

    async fn wait_for_oldest(&mut self) -> eyre::Result<()> {
        let pending = self.pending.front_mut().expect("pending append");
        (&mut pending.ticket)
            .await
            .context("failed to append record")?;
        self.remove_oldest();
        Ok(())
    }

    fn drain_ready_tickets(&mut self) -> eyre::Result<()> {
        loop {
            let Some(result) = self
                .pending
                .front_mut()
                .and_then(|pending| pending.ticket.try_recv())
            else {
                return Ok(());
            };
            result.context("failed to append record")?;
            self.remove_oldest();
        }
    }

    fn remove_oldest(&mut self) {
        let pending = self.pending.pop_front().expect("pending append");
        self.pending_records -= pending.record_count;
        self.pending_payload_bytes -= pending.payload_bytes;
    }
}

fn read_options(locator: &StreamLocator, read: &ReadArgs, default_start: ReadStart) -> ReadOptions {
    let mut options = ReadOptions::new(locator.stream_id);
    options.start = Some(selected_read_start(
        read.last,
        read.seq,
        read.since,
        default_start,
    ));
    options.stop = read.count.map(|count| ReadStop {
        count: Some(count),
        ..ReadStop::default()
    });
    if let Some(link) = locator.link_declaring(LinkPermissions::allows_read) {
        options = options.with_link_secret(link.clone());
    }
    options
}

async fn tail_stream(origin: Url, args: TailArgs) -> eyre::Result<()> {
    let locator = StreamLocator::parse(args.link.as_str()).context("invalid stream URL")?;
    let request = read_options(&locator, &args.read, ReadStart::TailOffset(0));

    read_transcript(
        origin,
        request,
        args.read.max_reassembly_bytes,
        args.read.sse,
    )
    .await
}

async fn replay_stream(origin: Url, args: ReplayArgs) -> eyre::Result<()> {
    let locator = StreamLocator::parse(args.link.as_str()).context("invalid stream URL")?;
    let mut request = read_options(&locator, &args.read, ReadStart::SeqNum(0));
    request.stop.get_or_insert(ReadStop {
        wait_seconds: Some(0),
        ..ReadStop::default()
    });

    read_transcript(
        origin,
        request,
        args.read.max_reassembly_bytes,
        args.read.sse,
    )
    .await
}

async fn stream_metadata(origin: Url, args: InfoArgs) -> eyre::Result<()> {
    let locator = StreamLocator::parse(args.link.as_str()).context("invalid stream URL")?;
    let client = TsfClient::with_api_origin(origin)?;
    let stream = client
        .get_stream(
            &locator.stream_id,
            locator.link.as_ref().map(|link| &link.secret),
        )
        .await
        .context("failed to get stream")?;
    print_stream_metadata(&stream, args.json)
}

async fn delete_stream(origin: Url, args: DeleteArgs) -> eyre::Result<()> {
    let (client, locator, owner_link_secret) =
        owner_client_from_link(origin, args.owner_link.as_str())?;
    if !confirm_delete(&locator.stream_id, args.yes)? {
        eprintln!("Deletion cancelled.");
        return Ok(());
    }
    client
        .delete_stream(&locator.stream_id, &owner_link_secret)
        .await
        .context("failed to delete stream")?;
    if args.json {
        print_json(&DeleteOutput {
            stream_id: locator.stream_id.to_string(),
            status: "deleted",
        })?;
    } else {
        println!("Deleted stream {}", locator.stream_id);
    }
    Ok(())
}

async fn update_and_print(
    origin: Url,
    owner_link: &LinkInput,
    request: &UpdateStreamRequest,
    json: bool,
    context: &'static str,
) -> eyre::Result<()> {
    let (client, locator, owner_link_secret) = owner_client_from_link(origin, owner_link.as_str())?;
    let stream = client
        .update_stream(&locator.stream_id, request, &owner_link_secret)
        .await
        .context(context)?;
    print_stream_metadata(&stream, json)
}

async fn update_visibility(origin: Url, args: VisibilityArgs) -> eyre::Result<()> {
    update_and_print(
        origin,
        &args.owner_link,
        &UpdateStreamRequest {
            title: StreamTitleUpdate::Unchanged,
            visibility: Some(args.visibility.into()),
            expires_at: None,
        },
        args.json,
        "failed to update stream visibility",
    )
    .await
}

async fn update_title(origin: Url, args: TitleArgs) -> eyre::Result<()> {
    let (owner_link, title, json) = match args.command {
        TitleCommand::Set(args) => (
            args.owner_link,
            StreamTitleUpdate::Set(args.title),
            args.json,
        ),
        TitleCommand::Clear(args) => (args.owner_link, StreamTitleUpdate::Clear, args.json),
    };
    update_and_print(
        origin,
        &owner_link,
        &UpdateStreamRequest {
            title,
            visibility: None,
            expires_at: None,
        },
        json,
        "failed to update stream title",
    )
    .await
}

async fn renew_stream(origin: Url, args: RenewArgs) -> eyre::Result<()> {
    update_and_print(
        origin,
        &args.owner_link,
        &UpdateStreamRequest {
            title: StreamTitleUpdate::Unchanged,
            visibility: None,
            expires_at: Some(args.expires.rfc3339()?),
        },
        args.json,
        "failed to renew stream",
    )
    .await
}

async fn link_command(origin: Url, args: LinkArgs) -> eyre::Result<()> {
    match args.command {
        LinkCommand::List(args) => list_links(origin, args).await,
        LinkCommand::Create(args) => create_link(origin, args).await,
        LinkCommand::Revoke(args) => revoke_link(origin, args).await,
    }
}

async fn list_links(origin: Url, args: ListLinkArgs) -> eyre::Result<()> {
    let (client, locator, owner_link_secret) =
        owner_client_from_link(origin, args.owner_link.as_str())?;
    let inventory = client
        .list_all_links(&locator.stream_id, &owner_link_secret)
        .await
        .context("failed to list links")?;
    if args.json {
        print_json(&inventory)?;
    } else {
        for link in &inventory.links {
            println!(
                "{:<24}  {:<10}  {:<7}  expires {}{}",
                link.link_id,
                permission_label(link.permissions),
                link.status.as_str(),
                link.expires_at.as_deref().unwrap_or("never"),
                if link.link_id == inventory.authorizing_link_id {
                    "  (current)"
                } else {
                    ""
                }
            );
        }
    }
    Ok(())
}

async fn create_link(origin: Url, args: CreateLinkArgs) -> eyre::Result<()> {
    let (client, locator, owner_link_secret) =
        owner_client_from_link(origin, args.owner_link.as_str())?;
    let InitialStreamLink {
        link_id,
        permissions,
    } = args.link.0;
    let expires_at = args.expires.rfc3339()?;
    let created = client
        .create_link(
            &locator.stream_id,
            &CreateLinkInput {
                link_id,
                permissions,
                expires_at,
            },
            &owner_link_secret,
        )
        .await
        .context("failed to create link")?;
    let url = stream_link(
        &created.web_origin,
        &locator.stream_id,
        created.credential.permissions,
        &created.credential.secret,
    )?;
    if let Some(path) = &args.link_file {
        write_private_file(path, url.as_str())
            .with_context(|| format!("failed to write link file {}", path.display()))?;
    }
    print_created_link(&url, &created.credential, args.json)?;
    Ok(())
}

async fn revoke_link(origin: Url, args: RevokeLinkArgs) -> eyre::Result<()> {
    let (client, locator, owner_link_secret) =
        owner_client_from_link(origin, args.owner_link.as_str())?;
    client
        .revoke_link(&locator.stream_id, &args.link_id, &owner_link_secret)
        .await
        .context("failed to revoke link")?;
    print_link_revoked(&args.link_id, args.json)
}

async fn read_transcript(
    origin: Url,
    options: ReadOptions,
    max_reassembly_bytes: usize,
    sse: bool,
) -> eyre::Result<()> {
    if options.stop.is_some_and(|stop| stop.count == Some(0)) {
        return Ok(());
    }

    let client = TsfClient::with_api_origin(origin)?;
    let reader = if sse {
        TranscriptReader::Sse(Box::new(
            client
                .connect_sse_reader(options)
                .await
                .context("failed to connect SSE reader")?,
        ))
    } else {
        TranscriptReader::WebSocket(Box::new(
            client
                .connect_reader(options)
                .await
                .context("failed to connect reader")?,
        ))
    };
    let (batch_tx, mut batch_rx) = mpsc::channel(TRANSCRIPT_BATCH_QUEUE);
    let reader_task = tokio::spawn(forward_read_batches(reader, batch_tx));

    let mut stdout = BufWriter::with_capacity(TRANSCRIPT_OUTPUT_BUFFER_BYTES, tokio::io::stdout());
    let mut transcript = LogicalTranscript::with_max_reassembly_bytes(max_reassembly_bytes);
    let result = write_transcript_batches(&mut batch_rx, &mut stdout, &mut transcript).await;
    stdout.flush().await.context("failed to flush stdout")?;
    result?;

    reader_task.await.context("transcript reader task panicked")
}

/// Writes decoded batches until the reader finishes, flushing whenever none is already waiting.
///
/// Assembly happens here so transient output borrows payloads straight from each batch instead
/// of copying every record into an owned transcript record.
async fn write_transcript_batches(
    batch_rx: &mut mpsc::Receiver<eyre::Result<tailsurf::ReadBatch>>,
    stdout: &mut BufWriter<tokio::io::Stdout>,
    transcript: &mut LogicalTranscript,
) -> eyre::Result<()> {
    loop {
        let batch = tokio::select! {
            batch = batch_rx.recv() => batch,
            interrupt = tokio::signal::ctrl_c() => {
                interrupt.context("failed to listen for interrupt signal")?;
                stdout.flush().await.context("failed to flush stdout")?;
                exit_interrupted();
            }
        };
        let Some(batch) = batch else {
            return Ok(());
        };

        let batch = batch?;
        for record in &batch {
            let Some(record) = transcript
                .push_record(record)
                .context("failed to assemble transcript record")?
            else {
                continue;
            };
            let format = record.format;
            write_transcript_data(stdout, record.data).await?;
            // Transcript records carry no delimiter; the terminator is presentation framing.
            if format == RecordFormat::Transcript {
                stdout
                    .write_all(b"\n")
                    .await
                    .context("failed to write stdout")?;
            }
        }
        // Batching must never hold output back, so flush as soon as nothing is already decoded.
        if batch_rx.is_empty() {
            stdout.flush().await.context("failed to flush stdout")?;
        }
    }
}

enum TranscriptReader {
    WebSocket(Box<TsfReadSession>),
    Sse(Box<TsfSseReadSession>),
}

impl TranscriptReader {
    async fn next_batch(&mut self) -> eyre::Result<Option<tailsurf::ReadBatch>> {
        match self {
            Self::WebSocket(reader) => reader.next_batch().await.context("failed to read stream"),
            Self::Sse(reader) => reader
                .next_batch()
                .await
                .context("failed to read SSE stream"),
        }
    }
}

/// Forwards read batches off the output path so reads overlap stdout writes.
async fn forward_read_batches(
    mut reader: TranscriptReader,
    batch_tx: mpsc::Sender<eyre::Result<tailsurf::ReadBatch>>,
) {
    let result = async {
        while let Some(batch) = reader.next_batch().await? {
            if batch_tx.send(Ok(batch)).await.is_err() {
                return eyre::Result::<()>::Ok(());
            }
        }
        Ok(())
    }
    .await;
    if let Err(error) = result {
        // Failures belong in stream order behind batches already sent, not in the join result.
        let _ = batch_tx.send(Err(error)).await;
    }
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

fn owner_client_from_link(
    origin: Url,
    link: &str,
) -> eyre::Result<(TsfClient, StreamLocator, LinkSecret)> {
    let locator = StreamLocator::parse(link).context("invalid owner link")?;
    let owner_link_secret = locator
        .link_declaring(LinkPermissions::allows_owner)
        .context("link does not declare owner permission")?
        .clone();
    Ok((
        TsfClient::with_api_origin(origin)?,
        locator,
        owner_link_secret,
    ))
}

fn confirm_delete(stream_id: &StreamId, yes: bool) -> eyre::Result<bool> {
    if yes {
        return Ok(true);
    }
    if !std::io::stdin().is_terminal() {
        bail!("--yes is required to delete a stream when stdin is not a terminal");
    }
    eprint!("Permanently delete stream {stream_id}? [y/N] ");
    {
        use std::io::Write as _;
        std::io::stderr()
            .flush()
            .context("failed to flush deletion prompt")?;
    }
    let mut response = String::new();
    std::io::stdin()
        .read_line(&mut response)
        .context("failed to read deletion confirmation")?;
    Ok(matches!(
        response.trim().to_ascii_lowercase().as_str(),
        "y" | "yes"
    ))
}

fn print_link_revoked(link_id: &LinkId, json: bool) -> eyre::Result<()> {
    if json {
        print_json(&LinkMutationOutput {
            link_id: link_id.to_string(),
            status: "revoked",
        })?;
    } else {
        println!("Revoked link {link_id}");
    }
    Ok(())
}

fn print_json(value: &impl Serialize) -> eyre::Result<()> {
    write_json(std::io::stdout().lock(), value)
}

fn write_json(writer: impl std::io::Write, value: &impl Serialize) -> eyre::Result<()> {
    let mut writer = std::io::BufWriter::new(writer);
    // serde_json::Error hides the io::Error from chain walking; unwrap it so broken-pipe
    // classification and error reports see the real cause.
    serde_json::to_writer_pretty(&mut writer, value).map_err(std::io::Error::from)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    Ok(())
}

fn print_created_stream(created: &CreateStreamResponse, json: bool) -> eyre::Result<()> {
    let web_origin = &created.web_origin;
    if !json {
        println!(
            "Created {} stream {}",
            created.visibility, created.stream_id
        );
        println!(
            "Title: {}",
            created
                .title
                .as_ref()
                .map_or("Untitled stream", StreamTitle::as_str)
        );
        println!("Expires: {}", created.expires_at);
        let mut links = created
            .links
            .iter()
            .map(|credential| {
                Ok((
                    credential.link_id.as_str(),
                    credential.permissions,
                    stream_link(
                        web_origin,
                        &created.stream_id,
                        credential.permissions,
                        &credential.secret,
                    )?,
                    if credential.permissions.allows_owner() {
                        "  (keep private)"
                    } else {
                        ""
                    },
                ))
            })
            .collect::<Result<Vec<_>, tailsurf::stream_url::StreamLinkError>>()?;
        if matches!(created.visibility, Visibility::Public) {
            links.push((
                "Public",
                LinkPermissions::read(),
                public_stream_url(web_origin, &created.stream_id)?,
                "  (public)",
            ));
        }
        if !links.is_empty() {
            println!();
            links.sort_by_key(|(_, permissions, _, _)| permission_rank(*permissions));
            let width = links
                .iter()
                .map(|(label, _, _, _)| label.len())
                .max()
                .unwrap_or(0);
            for (label, permissions, url, suffix) in &links {
                let permission = permission_label(*permissions);
                println!("  {label:<width$}  {permission:<10}  {url}{suffix}");
            }
            println!();
            println!("Links are shown once.");
        }
    } else {
        let output = CreatedStreamOutput {
            stream_id: created.stream_id.to_string(),
            title: created
                .title
                .as_ref()
                .map(|title| title.as_str().to_owned()),
            visibility: created.visibility.as_str(),
            expires_at: created.expires_at.clone(),
            links: created
                .links
                .iter()
                .map(|credential| {
                    Ok(CreatedLinkOutput {
                        link_id: credential.link_id.to_string(),
                        permissions: permission_label(credential.permissions),
                        url: stream_link(
                            web_origin,
                            &created.stream_id,
                            credential.permissions,
                            &credential.secret,
                        )?
                        .to_string(),
                    })
                })
                .collect::<Result<Vec<_>, tailsurf::stream_url::StreamLinkError>>()?,
            public_url: match created.visibility {
                Visibility::Public => {
                    Some(public_stream_url(web_origin, &created.stream_id)?.to_string())
                }
                Visibility::Private => None,
            },
        };
        print_json(&output)?;
    }
    Ok(())
}

fn print_stream_metadata(stream: &StreamMetadata, json: bool) -> eyre::Result<()> {
    if !json {
        println!("Stream {}", stream.stream_id);
        println!(
            "Title: {}",
            stream
                .title
                .as_ref()
                .map_or("Untitled stream", StreamTitle::as_str)
        );
        println!("Visibility: {}", stream.visibility);
        println!("Created: {}", stream.created_at);
        println!("Expires: {}", stream.expires_at);
    } else {
        print_json(stream)?;
    }
    Ok(())
}

fn print_created_link(
    url: &Url,
    credential: &StreamLinkCredential,
    json: bool,
) -> eyre::Result<()> {
    if !json {
        println!(
            "Created {} ({})",
            credential.link_id,
            permission_label(credential.permissions)
        );
        println!("  Link     {url}");
        println!("  Link ID  {}", credential.link_id);
        println!("Link is shown once. Revoke it with the id above.");
    } else {
        let output = CreatedLinkOutput {
            link_id: credential.link_id.to_string(),
            permissions: permission_label(credential.permissions),
            url: url.to_string(),
        };
        print_json(&output)?;
    }
    Ok(())
}

fn write_link_files(created: &CreateStreamResponse, args: &NewArgs) -> eyre::Result<()> {
    write_link_file(
        &args.owner_link_file,
        created,
        LinkPermissions::owner(),
        "owner",
    )?;
    write_link_file(
        &args.read_link_file,
        created,
        LinkPermissions::read(),
        "read",
    )?;
    write_link_file(
        &args.write_link_file,
        created,
        LinkPermissions::write(),
        "write",
    )?;
    Ok(())
}

fn write_link_file(
    path: &Option<PathBuf>,
    created: &CreateStreamResponse,
    permissions: LinkPermissions,
    kind: &str,
) -> eyre::Result<()> {
    let Some(path) = path else {
        return Ok(());
    };
    let link = created
        .links
        .iter()
        .find(|link| link.permissions == permissions)
        .with_context(|| format!("created stream did not include a {kind} link"))?;
    let url = stream_link(
        &created.web_origin,
        &created.stream_id,
        link.permissions,
        &link.secret,
    )?;
    write_private_file(path, url.as_str())
        .with_context(|| format!("failed to write {kind} link file {}", path.display()))?;
    Ok(())
}

fn write_private_file(path: &Path, value: &str) -> std::io::Result<()> {
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
    std::io::Write::write_all(&mut file, value.as_bytes())
}

fn initial_link(link_id: &str, permissions: LinkPermissions) -> eyre::Result<InitialStreamLink> {
    Ok(InitialStreamLink::new(
        link_id
            .parse()
            .map_err(|error| eyre!("invalid Link ID: {error}"))?,
        permissions,
    ))
}

fn permission_label(permissions: LinkPermissions) -> &'static str {
    match permissions {
        p if p == LinkPermissions::owner() => "owner",
        p if p == LinkPermissions::read() => "read",
        p if p == LinkPermissions::write() => "write",
        // Constructors validate bits into {o, r, w, rw}; only read-write remains.
        _ => "read-write",
    }
}

fn permission_rank(permissions: LinkPermissions) -> usize {
    match permissions {
        p if p == LinkPermissions::read() => 0,
        p if p == LinkPermissions::write() => 1,
        p if p == LinkPermissions::read_write() => 2,
        _ => 3,
    }
}

fn selected_read_start(
    last: Option<u64>,
    seq: Option<u64>,
    since: Option<SinceArg>,
    default: ReadStart,
) -> ReadStart {
    last.map(ReadStart::TailOffset)
        .or_else(|| seq.map(ReadStart::SeqNum))
        .or_else(|| since.map(|since| ReadStart::TimestampMs(since.0)))
        .unwrap_or(default)
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

#[derive(Serialize)]
struct CreatedStreamOutput {
    stream_id: String,
    title: Option<String>,
    visibility: &'static str,
    expires_at: String,
    links: Vec<CreatedLinkOutput>,
    #[serde(skip_serializing_if = "Option::is_none")]
    public_url: Option<String>,
}

#[derive(Serialize)]
struct CreatedLinkOutput {
    link_id: String,
    permissions: &'static str,
    url: String,
}

#[derive(Serialize)]
struct DeleteOutput {
    stream_id: String,
    status: &'static str,
}

#[derive(Serialize)]
struct LinkMutationOutput {
    link_id: String,
    status: &'static str,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_links_accept_semantic_ids_and_short_permissions() {
        let parsed = "deploy-bot=read"
            .parse::<InitialLinkArg>()
            .expect("valid initial link");

        assert_eq!(parsed.0.link_id.as_str(), "deploy-bot");
        assert_eq!(parsed.0.permissions, LinkPermissions::read());

        for (value, expected) in [
            ("reader=r", LinkPermissions::read()),
            ("writer=w", LinkPermissions::write()),
            ("operator=rw", LinkPermissions::read_write()),
            ("owner=o", LinkPermissions::owner()),
        ] {
            let parsed = value
                .parse::<InitialLinkArg>()
                .expect("valid short permission");
            assert_eq!(parsed.0.permissions, expected);
        }
    }

    #[test]
    fn reserved_default_link_ids_report_the_collision() {
        let new_args = |links: Vec<InitialLinkArg>| NewArgs {
            title: None,
            public: false,
            links,
            expires: None,
            json: false,
            owner_link_file: None,
            read_link_file: None,
            write_link_file: None,
            input: InputArgs {
                bytes: false,
                program: Vec::new(),
            },
        };

        let args = new_args(vec!["owner=w".parse().expect("valid link")]);
        let error = new_stream_links(&args, Visibility::Private).expect_err("reserved owner ID");
        assert_eq!(
            error.to_string(),
            "Link ID \"owner\" is reserved for the default owner link; choose another ID or give that link owner permission"
        );

        let args = new_args(vec!["reader=w".parse().expect("valid link")]);
        let error = new_stream_links(&args, Visibility::Private).expect_err("reserved reader ID");
        assert_eq!(
            error.to_string(),
            "Link ID \"reader\" is reserved for the default read link on private streams; choose another ID or give that link read-only permission"
        );

        let args = new_args(vec!["reader=rw".parse().expect("valid link")]);
        let error = new_stream_links(&args, Visibility::Private).expect_err("reserved reader ID");
        assert_eq!(
            error.to_string(),
            "Link ID \"reader\" is reserved for the default read link on private streams; choose another ID or give that link read-only permission"
        );

        // User-vs-user duplicates still hit the generic uniqueness error.
        let args = new_args(vec![
            "bot=r".parse().expect("valid link"),
            "bot=w".parse().expect("valid link"),
        ]);
        let error = new_stream_links(&args, Visibility::Public).expect_err("duplicate IDs");
        assert_eq!(error.to_string(), "initial Link IDs must be unique");

        // A reserved ID carrying the required permission needs no default injection.
        let args = new_args(vec!["owner=o".parse().expect("valid link")]);
        assert!(new_stream_links(&args, Visibility::Private).is_ok());
    }

    #[test]
    fn update_hints_require_a_successful_interactive_production_command() {
        let production = Url::parse("https://tail.surf").expect("production URL");
        let non_production = Url::parse("https://api.example").expect("custom URL");
        let check = |is_update, origin, terminal, disabled| {
            should_check_for_update_hint(is_update, origin, terminal, disabled)
        };
        assert!(check(false, &production, true, false));
        assert!(!check(false, &production, false, false));
        assert!(!check(false, &production, true, true));
        assert!(!check(false, &non_production, true, false));
        assert!(!check(true, &production, true, false));
    }

    #[test]
    fn update_hint_cache_retries_failures_before_successes() {
        const NOW: u64 = 2_000_000;
        let retry = UPDATE_HINT_RETRY_INTERVAL.as_secs();
        let success = UPDATE_HINT_SUCCESS_INTERVAL.as_secs();
        let cache = |last_attempt_at, last_success_at| UpdateHintCheckCache {
            last_attempt_at,
            last_success_at,
        };

        assert!(update_hint_check_is_due(None, NOW));
        assert!(!update_hint_check_is_due(Some(&cache(NOW, None)), NOW));
        assert!(!update_hint_check_is_due(
            Some(&cache(NOW - retry + 1, None)),
            NOW
        ));
        assert!(update_hint_check_is_due(
            Some(&cache(NOW - retry, None)),
            NOW
        ));
        assert!(!update_hint_check_is_due(
            Some(&cache(NOW - success + 1, Some(NOW - success + 1))),
            NOW
        ));
        assert!(update_hint_check_is_due(
            Some(&cache(NOW - success, Some(NOW - success))),
            NOW
        ));
        assert!(!update_hint_check_is_due(
            Some(&cache(NOW + retry - 1, None)),
            NOW
        ));
        assert!(update_hint_check_is_due(
            Some(&cache(NOW + retry, None)),
            NOW
        ));
        assert!(claim_update_hint_check(Path::new("."), NOW).is_none());
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

    #[test]
    fn json_output_surfaces_io_errors_for_broken_pipe_classification() {
        struct BrokenPipeWriter;
        impl std::io::Write for BrokenPipeWriter {
            fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
                Err(std::io::Error::new(
                    ErrorKind::BrokenPipe,
                    "closed consumer",
                ))
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Err(std::io::Error::new(
                    ErrorKind::BrokenPipe,
                    "closed consumer",
                ))
            }
        }

        // Larger than the BufWriter capacity: fails inside serde_json, exercising the
        // io::Error unwrap; without it the chain ends at serde_json::Error.
        let large = serde_json::json!({ "data": "x".repeat(16 * 1024) });
        let error = write_json(BrokenPipeWriter, &large).expect_err("broken pipe");
        assert!(is_broken_pipe(&error));

        // Smaller than the buffer: fails at the final flush instead.
        let small = serde_json::json!({ "a": 1 });
        let error = write_json(BrokenPipeWriter, &small).expect_err("broken pipe");
        assert!(is_broken_pipe(&error));
    }

    #[test]
    fn expiry_beyond_rfc3339_range_errors_instead_of_panicking() {
        let huge = Duration::from_secs(u64::MAX / 4);
        assert!(rfc3339_from_now(huge, "link expiry").is_err());

        // One second before the last representable instant still formats.
        let to_end = Duration::from_secs(
            253_402_300_799
                - SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("post-epoch")
                    .as_secs()
                - 1,
        );
        let formatted = rfc3339_from_now(to_end, "link expiry").expect("in-range expiry");
        assert!(formatted.starts_with("9999-12-31T23:59:5"));
    }
}
