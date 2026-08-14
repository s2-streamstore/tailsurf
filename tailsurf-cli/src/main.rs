//! `tsf` command-line client for creating, writing, replaying, tailing, and managing Tailsurf
//! streams.

use std::{
    collections::{HashSet, VecDeque},
    fmt,
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
use eyre::{Context, ContextCompat, bail, eyre};
use memchr::memchr;
use serde::Serialize;
use tailsurf::{
    AppendTicket, LinkId, LinkPermissions, LinkSecret, StreamId, StreamTitle, TsfClient,
    TsfReadSession, TsfSseReadSession, TsfWriter, WriteRecord, WriterId,
    protocol::{
        rest::{
            CreateLinkRequest, CreateStreamRequest, CreateStreamResponse, InitialStreamLink,
            StreamInfoResponse, StreamLinkCredential, StreamLinkStatus, StreamTitleUpdate,
            UpdateStreamRequest, Visibility,
        },
        ws::{
            ReadStart, ReadStreamOptions, WriteStreamOptions,
            frame::{MAX_RECORD_BYTES, PartHeader, RecordFormat},
        },
    },
    stream_url::{StreamLocator, stream_link},
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
const MAX_INITIAL_LINKS: usize = 3;
const UPDATE_HINT_CACHE_FILE: &str = ".tailsurf-cli-update-check";
const UPDATE_HINT_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);
const UPDATE_HINT_TIMEOUT: Duration = Duration::from_millis(500);

#[derive(Debug, Parser)]
#[command(name = "tsf")]
#[command(version, about = "Create, write, and read tail.surf streams")]
#[command(
    after_help = "Create a stream from piped input:\n  anything | tsf\n  anything | tsf new\n\nCapture a program in a new stream:\n  tsf new -- program\n\nWrite to an existing stream:\n  anything | tsf write WRITE_LINK"
)]
struct Cli {
    /// Tailsurf API origin.
    #[arg(
        long = "api-url",
        env = "TSF_API_URL",
        default_value = "https://tail.surf",
        global = true,
        help_heading = "Connection"
    )]
    api_url: Url,
    /// Origin used when printing stream links.
    #[arg(
        long = "web-url",
        env = "TSF_WEB_URL",
        default_value = "https://tail.surf",
        global = true,
        help_heading = "Connection"
    )]
    web_url: Url,
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
    /// Print a bounded snapshot of existing records.
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
    #[command(flatten)]
    input: InputArgs,
}

#[derive(Debug, Args)]
struct InputArgs {
    /// Preserve input as arbitrary byte records instead of newline-delimited transcript records.
    #[arg(long)]
    raw: bool,
    /// Maximum logical line size. Readers use the same default.
    #[arg(
        long,
        value_name = "BYTES",
        default_value_t = DEFAULT_MAX_LOGICAL_RECORD_BYTES,
        help_heading = "Advanced"
    )]
    max_logical_record_bytes: usize,
    /// Program to run. Its stdout and stderr are written to the stream.
    #[arg(last = true, value_name = "PROGRAM")]
    program: Vec<String>,
}

impl InputArgs {
    fn piped_defaults() -> Self {
        Self {
            raw: false,
            max_logical_record_bytes: DEFAULT_MAX_LOGICAL_RECORD_BYTES,
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
    limit: Option<u64>,
    /// Maximum assembled transcript record size.
    #[arg(
        long,
        value_name = "BYTES",
        default_value_t = DEFAULT_MAX_LOGICAL_RECORD_BYTES,
        help_heading = "Advanced"
    )]
    max_logical_record_bytes: usize,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct StreamExpiryArg(Duration);

impl FromStr for StreamExpiryArg {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let duration = humantime::parse_duration(value)
            .map_err(|error| format!("invalid stream expiry duration: {error}"))?;
        if duration.is_zero() {
            return Err("stream expiry must be at least one second".to_owned());
        }
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
        let expires_at = SystemTime::now()
            .checked_add(self.0)
            .ok_or_else(|| eyre!("stream expiry is too large"))?;
        Ok(humantime::format_rfc3339_seconds(expires_at).to_string())
    }
}

#[derive(Clone, Copy, Debug)]
struct PermissionArg(LinkPermissions);

impl FromStr for PermissionArg {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let permissions = match value.to_ascii_lowercase().as_str() {
            "read" | "r" => LinkPermissions::read(),
            "write" | "w" => LinkPermissions::write(),
            "read-write" | "rw" => LinkPermissions::read_write(),
            "owner" | "o" => LinkPermissions::owner(),
            other => {
                return Err(format!(
                    "unknown permission {other:?}; use read, write, read-write, or owner"
                ));
            }
        };
        Ok(Self(permissions))
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
    let Cli {
        api_url,
        web_url,
        command,
    } = Cli::parse();
    let stdin_is_terminal = std::io::stdin().is_terminal();
    if command.is_none() && stdin_is_terminal {
        let help = <Cli as clap::CommandFactory>::command().render_help();
        eprint!("{help}");
        return ExitCode::from(2);
    }
    let command = command.unwrap_or_else(|| Command::New(NewArgs::piped_defaults()));
    let check_for_update = should_check_for_update_hint(
        matches!(command, Command::Update(_)),
        &api_url,
        std::io::stderr().is_terminal(),
        automatic_update_checks_disabled(),
    );
    let result = run(api_url, web_url, command).await;
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

async fn run(api_url: Url, web_url: Url, command: Command) -> eyre::Result<()> {
    match command {
        Command::New(args) => new_stream(api_url, web_url, args).await,
        Command::Write(args) => write_stream(api_url, args).await,
        Command::Tail(args) => tail_stream(api_url, args).await,
        Command::Replay(args) => replay_stream(api_url, args).await,
        Command::Info(args) => stream_info(api_url, args).await,
        Command::Delete(args) => delete_stream(api_url, args).await,
        Command::Visibility(args) => update_visibility(api_url, args).await,
        Command::Title(args) => update_title(api_url, args).await,
        Command::Renew(args) => renew_stream(api_url, args).await,
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
    is_update_command: bool,
    api_url: &Url,
    stderr_is_terminal: bool,
    disabled: bool,
) -> bool {
    stderr_is_terminal
        && !disabled
        && !is_update_command
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
    let links = new_stream_links(&args, visibility)?;

    let created = create_stream(
        api_url.clone(),
        args.title.clone(),
        visibility,
        args.expires.map(StreamExpiryArg::seconds),
        links,
    )
    .await?;
    print_created_stream(&web_url, &created, args.json)?;
    write_link_files(&web_url, &created, &args)?;

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
    write_input(api_url, created.stream_id, owner_link_secret, args.input).await
}

async fn write_stream(api_url: Url, args: WriteArgs) -> eyre::Result<()> {
    let locator = StreamLocator::parse(args.link.as_str()).context("invalid stream link")?;
    let link = locator
        .link_declaring(LinkPermissions::allows_write)
        .context("link does not declare write permission")?
        .clone();
    write_input(api_url, locator.stream_id, link, args.input).await
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
        links.insert(0, initial_link("owner", LinkPermissions::owner())?);
    }
    if matches!(visibility, Visibility::Private)
        && !links
            .iter()
            .any(|link| link.permissions == LinkPermissions::read())
    {
        links.push(initial_link("reader", LinkPermissions::read())?);
    }
    if links.len() > MAX_INITIAL_LINKS {
        bail!(
            "at most {MAX_INITIAL_LINKS} initial links may be created, including the default owner and private reader links"
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
    api_url: Url,
    stream_id: StreamId,
    link: LinkSecret,
    input: InputArgs,
) -> eyre::Result<()> {
    let buffering = if input.raw {
        WriteBuffering::Raw
    } else {
        WriteBuffering::Lines
    };
    if input.program.is_empty() {
        stream_stdin_to_writer(
            api_url,
            stream_id,
            link,
            buffering,
            input.max_logical_record_bytes,
        )
        .await
    } else {
        stream_command_to_writer(
            api_url,
            stream_id,
            link,
            buffering,
            input.max_logical_record_bytes,
            input.program,
        )
        .await
    }
}

fn print_write_summary(records: u64) {
    let noun = if records == 1 { "record" } else { "records" };
    eprintln!("{records} {noun} durable");
}

async fn create_stream(
    api_url: Url,
    title: Option<StreamTitle>,
    visibility: Visibility,
    expires_in_seconds: Option<u64>,
    links: Vec<InitialStreamLink>,
) -> eyre::Result<CreateStreamResponse> {
    let result = TsfClient::with_api_origin(api_url)?
        .create_stream(&CreateStreamRequest {
            title,
            visibility,
            expires_in_seconds,
            links,
        })
        .await;
    match result {
        Ok(created) => Ok(created),
        Err(error) => Err(error).context("failed to create stream"),
    }
}

async fn stream_stdin_to_writer(
    api_url: Url,
    stream_id: StreamId,
    link: LinkSecret,
    buffering: WriteBuffering,
    max_logical_record_bytes: usize,
) -> eyre::Result<()> {
    let client = TsfClient::with_api_origin(api_url)?;
    let mut state = WriterState::new_random();
    let writer = client
        .connect_writer(WriteStreamOptions::new(stream_id, state.writer_id, link))
        .await
        .context("failed to connect writer")?;

    let interrupted = match buffering {
        WriteBuffering::Raw => stream_raw_stdin_to_writer(&writer, &mut state).await,
        WriteBuffering::Lines => {
            stream_lines_to_writer(&writer, &mut state, max_logical_record_bytes).await
        }
    }?;
    writer.close().await.context("failed to close writer")?;
    print_write_summary(state.next_writer_seq);
    if interrupted {
        exit_interrupted();
    }
    Ok(())
}

async fn stream_raw_stdin_to_writer(
    writer: &TsfWriter,
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
    writer: &TsfWriter,
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
    link: LinkSecret,
    buffering: WriteBuffering,
    max_logical_record_bytes: usize,
    command: Vec<String>,
) -> eyre::Result<()> {
    let client = TsfClient::with_api_origin(api_url)?;
    let mut state = WriterState::new_random();
    let writer = client
        .connect_writer(WriteStreamOptions::new(stream_id, state.writer_id, link))
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
    print_write_summary(state.next_writer_seq);
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
    writer: &'a TsfWriter,
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
    let locator = StreamLocator::parse(args.link.as_str()).context("invalid stream URL")?;
    let mut request = ReadStreamOptions::new(locator.stream_id);
    request.start = Some(selected_read_start(
        args.read.last,
        args.read.seq,
        args.read.since,
        ReadStart::TailOffset(0),
    ));
    request.count = args.read.limit;
    if let Some(link) = locator.link_declaring(LinkPermissions::allows_read) {
        request = request.with_link_secret(link.clone());
    }

    if args.read.sse {
        read_transcript_sse(api_url, request, args.read.max_logical_record_bytes).await
    } else {
        read_transcript(api_url, request, args.read.max_logical_record_bytes).await
    }
}

async fn replay_stream(api_url: Url, args: ReplayArgs) -> eyre::Result<()> {
    let locator = StreamLocator::parse(args.link.as_str()).context("invalid stream URL")?;
    if args.read.limit == Some(0) {
        return Ok(());
    }
    let mut request = ReadStreamOptions::new(locator.stream_id);
    request.start = Some(selected_read_start(
        args.read.last,
        args.read.seq,
        args.read.since,
        ReadStart::SeqNum(0),
    ));
    request.snapshot = true;
    request.count = args.read.limit;
    let read_link = locator.link_declaring(LinkPermissions::allows_read);
    if let Some(link) = read_link {
        request = request.with_link_secret(link.clone());
    }

    if args.read.sse {
        read_transcript_sse(api_url, request, args.read.max_logical_record_bytes).await
    } else {
        read_transcript(api_url, request, args.read.max_logical_record_bytes).await
    }
}

async fn stream_info(api_url: Url, args: InfoArgs) -> eyre::Result<()> {
    let locator = StreamLocator::parse(args.link.as_str()).context("invalid stream URL")?;
    let client = TsfClient::with_api_origin(api_url)?;
    let stream = client
        .get_stream(
            &locator.stream_id,
            locator.link.as_ref().map(|link| &link.secret),
        )
        .await
        .context("failed to get stream")?;
    print_stream_info(&stream, args.json)
}

async fn delete_stream(api_url: Url, args: DeleteArgs) -> eyre::Result<()> {
    let (client, locator, owner_link_secret) =
        owner_client_from_link(api_url, args.owner_link.as_str())?;
    if !confirm_delete(&locator.stream_id, args.yes)? {
        eprintln!("Deletion cancelled.");
        return Ok(());
    }
    client
        .delete_stream(&locator.stream_id, &owner_link_secret)
        .await
        .context("failed to delete stream")?;
    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&DeleteOutput {
                stream_id: locator.stream_id.to_string(),
                status: "deleted",
            })?
        );
    } else {
        println!("Deleted stream {}", locator.stream_id);
    }
    Ok(())
}

async fn update_visibility(api_url: Url, args: VisibilityArgs) -> eyre::Result<()> {
    let (client, locator, owner_link_secret) =
        owner_client_from_link(api_url, args.owner_link.as_str())?;
    let stream = client
        .update_stream(
            &locator.stream_id,
            &UpdateStreamRequest {
                title: StreamTitleUpdate::Unchanged,
                visibility: Some(args.visibility.into()),
                expires_at: None,
            },
            &owner_link_secret,
        )
        .await
        .context("failed to update stream visibility")?;
    print_stream_info(&stream, args.json)?;
    Ok(())
}

async fn update_title(api_url: Url, args: TitleArgs) -> eyre::Result<()> {
    let (owner_link, title, json) = match args.command {
        TitleCommand::Set(args) => (
            args.owner_link,
            StreamTitleUpdate::Set(args.title),
            args.json,
        ),
        TitleCommand::Clear(args) => (args.owner_link, StreamTitleUpdate::Clear, args.json),
    };
    let (client, locator, owner_link_secret) =
        owner_client_from_link(api_url, owner_link.as_str())?;
    let stream = client
        .update_stream(
            &locator.stream_id,
            &UpdateStreamRequest {
                title,
                visibility: None,
                expires_at: None,
            },
            &owner_link_secret,
        )
        .await
        .context("failed to update stream title")?;
    print_stream_info(&stream, json)?;
    Ok(())
}

async fn renew_stream(api_url: Url, args: RenewArgs) -> eyre::Result<()> {
    let (client, locator, owner_link_secret) =
        owner_client_from_link(api_url, args.owner_link.as_str())?;
    let stream = client
        .update_stream(
            &locator.stream_id,
            &UpdateStreamRequest {
                title: StreamTitleUpdate::Unchanged,
                visibility: None,
                expires_at: Some(args.expires.rfc3339()?),
            },
            &owner_link_secret,
        )
        .await
        .context("failed to renew stream")?;
    print_stream_info(&stream, args.json)?;
    Ok(())
}

async fn link_command(api_url: Url, web_url: Url, args: LinkArgs) -> eyre::Result<()> {
    match args.command {
        LinkCommand::List(args) => list_links(api_url, args).await,
        LinkCommand::Create(args) => create_link(api_url, web_url, args).await,
        LinkCommand::Revoke(args) => revoke_link(api_url, args).await,
    }
}

async fn list_links(api_url: Url, args: ListLinkArgs) -> eyre::Result<()> {
    let (client, locator, owner_link_secret) =
        owner_client_from_link(api_url, args.owner_link.as_str())?;
    let response = client
        .list_links(&locator.stream_id, &owner_link_secret)
        .await
        .context("failed to list links")?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&response)?);
    } else {
        for link in response.links {
            println!(
                "{:<24}  {:<10}  {:<7}  expires {}{}",
                link.link_id,
                permission_label(link.permissions),
                link_status_label(link.status),
                link.expires_at.as_deref().unwrap_or("never"),
                if link.is_authorizing {
                    "  (current)"
                } else {
                    ""
                }
            );
        }
    }
    Ok(())
}

fn link_status_label(status: StreamLinkStatus) -> &'static str {
    match status {
        StreamLinkStatus::Active => "active",
        StreamLinkStatus::Expired => "expired",
        StreamLinkStatus::Revoked => "revoked",
    }
}

async fn create_link(api_url: Url, web_url: Url, args: CreateLinkArgs) -> eyre::Result<()> {
    let (client, locator, owner_link_secret) =
        owner_client_from_link(api_url, args.owner_link.as_str())?;
    let InitialStreamLink {
        link_id,
        permissions,
        secret,
    } = args.link.0;
    let credential = client
        .create_link(
            &locator.stream_id,
            &CreateLinkRequest {
                link_id,
                secret,
                permissions,
                expires_at: args.expires.rfc3339(),
            },
            &owner_link_secret,
        )
        .await
        .context("failed to create link")?;
    let url = stream_link(
        &web_url,
        &locator.stream_id,
        credential.permissions,
        &credential.secret,
    )?;
    if let Some(path) = &args.link_file {
        write_private_file(path, url.as_str())
            .with_context(|| format!("failed to write link file {}", path.display()))?;
    }
    print_created_link(&url, &credential, args.json)?;
    Ok(())
}

async fn revoke_link(api_url: Url, args: RevokeLinkArgs) -> eyre::Result<()> {
    let (client, locator, owner_link_secret) =
        owner_client_from_link(api_url, args.owner_link.as_str())?;
    client
        .revoke_link(&locator.stream_id, &args.link_id, &owner_link_secret)
        .await
        .context("failed to revoke link")?;
    print_link_revoked(&args.link_id, args.json)
}

async fn read_transcript(
    api_url: Url,
    options: ReadStreamOptions,
    max_logical_record_bytes: usize,
) -> eyre::Result<()> {
    if options.count == Some(0) {
        return Ok(());
    }

    let client = TsfClient::with_api_origin(api_url)?;
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

async fn read_transcript_sse(
    api_url: Url,
    options: ReadStreamOptions,
    max_logical_record_bytes: usize,
) -> eyre::Result<()> {
    if options.count == Some(0) {
        return Ok(());
    }
    let client = TsfClient::with_api_origin(api_url)?;
    let reader = client
        .connect_sse_reader(options)
        .await
        .context("failed to connect SSE reader")?;
    let (record_tx, mut record_rx) = mpsc::channel(TRANSCRIPT_RECORD_QUEUE);
    let reader_task = tokio::spawn(assemble_sse_transcript_records(
        reader,
        max_logical_record_bytes,
        record_tx,
    ));
    let mut stdout = BufWriter::with_capacity(TRANSCRIPT_OUTPUT_BUFFER_BYTES, tokio::io::stdout());
    let result = write_transcript_records(&mut record_rx, &mut stdout).await;
    stdout.flush().await.context("failed to flush stdout")?;
    result?;
    reader_task
        .await
        .context("SSE transcript reader task panicked")
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

async fn assemble_sse_transcript_records(
    mut reader: TsfSseReadSession,
    max_logical_record_bytes: usize,
    record_tx: mpsc::Sender<eyre::Result<TranscriptRecord>>,
) {
    let mut transcript = LogicalTranscript::with_max_logical_record_bytes(max_logical_record_bytes);
    let result = async {
        while let Some(record) = reader
            .next_record()
            .await
            .context("failed to read SSE stream")?
        {
            let Some(record) = transcript
                .push_record(record)
                .context("failed to assemble transcript record")?
            else {
                continue;
            };
            if record_tx.send(Ok(record)).await.is_err() {
                return eyre::Result::<()>::Ok(());
            }
        }
        Ok(())
    }
    .await;
    if let Err(error) = result {
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

fn owner_client_from_link(
    api_url: Url,
    link: &str,
) -> eyre::Result<(TsfClient, StreamLocator, LinkSecret)> {
    let locator = StreamLocator::parse(link).context("invalid owner link")?;
    let owner_link_secret = locator
        .link_declaring(LinkPermissions::allows_owner)
        .context("link does not declare owner permission")?
        .clone();
    Ok((
        TsfClient::with_api_origin(api_url)?,
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
        println!(
            "{}",
            serde_json::to_string_pretty(&LinkMutationOutput {
                link_id: link_id.to_string(),
                status: "revoked",
            })?
        );
    } else {
        println!("Revoked link {link_id}");
    }
    Ok(())
}

fn print_created_stream(
    web_url: &Url,
    created: &CreateStreamResponse,
    json: bool,
) -> eyre::Result<()> {
    if !json {
        println!(
            "Created {} stream {}",
            visibility_label(created.visibility),
            created.stream_id
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
                    permission_label(credential.permissions),
                    stream_link(
                        web_url,
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
                "read",
                bare_stream_url(web_url, &created.stream_id),
                "  (public)",
            ));
        }
        if !links.is_empty() {
            println!();
            links.sort_by_key(|(_, permission, _, _)| permission_rank(permission));
            let width = links
                .iter()
                .map(|(label, _, _, _)| label.len())
                .max()
                .unwrap_or(0);
            for (label, permission, url, suffix) in &links {
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
            visibility: visibility_label(created.visibility),
            expires_at: created.expires_at.clone(),
            links: created
                .links
                .iter()
                .map(|credential| {
                    Ok(CreatedLinkOutput {
                        link_id: credential.link_id.to_string(),
                        permissions: permission_label(credential.permissions),
                        url: stream_link(
                            web_url,
                            &created.stream_id,
                            credential.permissions,
                            &credential.secret,
                        )?
                        .to_string(),
                    })
                })
                .collect::<Result<Vec<_>, tailsurf::stream_url::StreamLinkError>>()?,
            public_url: matches!(created.visibility, Visibility::Public)
                .then(|| bare_stream_url(web_url, &created.stream_id).to_string()),
        };
        println!("{}", serde_json::to_string_pretty(&output)?);
    }
    Ok(())
}

fn print_stream_info(stream: &StreamInfoResponse, json: bool) -> eyre::Result<()> {
    if !json {
        println!("Stream {}", stream.stream_id);
        println!(
            "Title: {}",
            stream
                .title
                .as_ref()
                .map_or("Untitled stream", StreamTitle::as_str)
        );
        println!("Visibility: {}", visibility_label(stream.visibility));
        println!("Created: {}", stream.created_at);
        println!("Expires: {}", stream.expires_at);
    } else {
        println!("{}", serde_json::to_string_pretty(stream)?);
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
        println!("{}", serde_json::to_string_pretty(&output)?);
    }
    Ok(())
}

fn write_link_files(
    web_url: &Url,
    created: &CreateStreamResponse,
    args: &NewArgs,
) -> eyre::Result<()> {
    write_link_file(
        &args.owner_link_file,
        web_url,
        created,
        LinkPermissions::owner(),
        "owner",
    )?;
    write_link_file(
        &args.read_link_file,
        web_url,
        created,
        LinkPermissions::read(),
        "read",
    )?;
    write_link_file(
        &args.write_link_file,
        web_url,
        created,
        LinkPermissions::write(),
        "write",
    )?;
    Ok(())
}

fn write_link_file(
    path: &Option<PathBuf>,
    web_url: &Url,
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
    let url = stream_link(web_url, &created.stream_id, link.permissions, &link.secret)?;
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

fn permission_label(permissions: LinkPermissions) -> &'static str {
    match permissions.to_string().as_str() {
        "o" => "owner",
        "r" => "read",
        "w" => "write",
        "rw" => "read-write",
        _ => "link",
    }
}

fn permission_rank(label: &str) -> usize {
    match label {
        "read" => 0,
        "write" => 1,
        "read-write" => 2,
        "owner" => 3,
        _ => 4,
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

    const WRITE_LINK: &str = "https://tail.surf/s/0123456789abcdefghjkmnpqrstvwxyz#w=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

    #[test]
    fn parses_explicit_new_and_write_modes() {
        let created =
            Cli::try_parse_from(["tsf", "new", "--title", "Build log", "--", "make", "test"])
                .expect("new-stream capture");
        let Some(Command::New(created)) = created.command else {
            panic!("expected new command");
        };
        assert_eq!(
            created.title.as_ref().map(StreamTitle::as_str),
            Some("Build log")
        );
        assert_eq!(created.input.program, ["make", "test"]);

        let existing = Cli::try_parse_from(["tsf", "write", WRITE_LINK, "--", "make", "test"])
            .expect("existing-stream write");
        let Some(Command::Write(existing)) = existing.command else {
            panic!("expected write command");
        };
        assert_eq!(existing.link.as_str(), WRITE_LINK);
        assert_eq!(existing.input.program, ["make", "test"]);

        assert!(Cli::try_parse_from(["tsf", "--title", "Build log"]).is_err());
    }

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
        let check = |is_update, api_url, terminal, disabled| {
            should_check_for_update_hint(is_update, api_url, terminal, disabled)
        };
        assert!(check(false, &production, true, false));
        assert!(!check(false, &production, false, false));
        assert!(!check(false, &production, true, true));
        assert!(!check(false, &non_production, true, false));
        assert!(!check(true, &production, true, false));
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
