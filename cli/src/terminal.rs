use std::{
    collections::{HashMap, VecDeque},
    fmt,
    future::pending,
    io::{Read as _, Write as _},
    sync::{Arc, Condvar, Mutex},
};

use clap::Args;
use eyre::{Context, ContextCompat, bail, eyre};
use portable_pty::{ChildKiller, CommandBuilder, PtySize, native_pty_system};
use tailsurf::{
    AppendBatch, AppendTicket, DurableWriterOptions, LinkPermissions, LinkSecret, ReadOptions,
    ReadStart, StreamId, StreamKind, StreamTitle, TerminalInputEvent, TerminalOutputEvent,
    TsfClient, TsfWriter, WriterId, decode_terminal_input, encode_terminal_output,
    protocol::{
        rest::{CreateStreamRequest, Visibility},
        ws::frame::PartHeader,
    },
    validate_terminal_size,
};
use tokio::{
    sync::{Notify, mpsc, oneshot},
    time::{Duration, Instant, sleep_until},
};
use url::Url;

use super::{
    INTERRUPT_EXIT_CODE, STDIN_READ_BYTES, StreamExpiryArg, exit_interrupted, initial_link,
    print_created_stream, resource_link,
};

mod checkpoint;

use checkpoint::{TerminalCheckpointEmitter, resembles_checkpoint};

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
const MAX_INPUT_WRITERS: usize = 4_096;
const MAX_PENDING_OUTPUT_RECORDS: usize = 32;
const PTY_EXIT_DRAIN_IDLE_TIMEOUT: Duration = Duration::from_millis(100);
const PTY_EXIT_DRAIN_MAX_TIMEOUT: Duration = Duration::from_secs(2);
const PTY_INPUT_QUEUE: usize = 16;
const PTY_OUTPUT_QUEUE: usize = 16;

#[derive(Debug, Args)]
pub(super) struct TerminalArgs {
    /// Human-facing terminal title.
    #[arg(long, value_name = "TITLE")]
    title: Option<StreamTitle>,
    /// Allow anonymous observers.
    #[arg(long)]
    public: bool,
    /// Print the owner link for later administration. Keep it private.
    #[arg(long)]
    show_owner_link: bool,
    #[arg(
        long,
        value_name = "DURATION",
        help = "Session lifetime, such as 6h or 7d"
    )]
    expires: Option<StreamExpiryArg>,
    /// Initial terminal columns.
    #[arg(long, default_value_t = 80)]
    columns: u16,
    /// Initial terminal rows.
    #[arg(long, default_value_t = 24)]
    rows: u16,
    /// Command to host. The user's shell is used when omitted.
    #[arg(last = true, value_name = "COMMAND")]
    command: Vec<String>,
}

pub(super) async fn run(origin: Url, args: TerminalArgs) -> eyre::Result<()> {
    validate_terminal_size(args.columns, args.rows).context("invalid initial terminal size")?;
    let visibility = if args.public {
        Visibility::Public
    } else {
        Visibility::Private
    };
    let mut links = vec![
        initial_link("owner", LinkPermissions::owner())?,
        initial_link("controller", LinkPermissions::read_write())?,
    ];
    if matches!(visibility, Visibility::Private) {
        links.push(initial_link("observer", LinkPermissions::read())?);
    }

    let client = TsfClient::with_api_origin(origin)?;
    let created = client
        .create_stream(&CreateStreamRequest {
            kind: StreamKind::Terminal,
            title: args.title,
            visibility,
            expires_in_seconds: args.expires.map(StreamExpiryArg::seconds),
            links,
        })
        .await
        .context("failed to create terminal session")?;
    let owner = created
        .links
        .iter()
        .find(|link| link.permissions.allows_owner())
        .context("created terminal did not include an owner link")?;
    let owner_link = resource_link(
        &created.web_origin,
        &created.stream_id,
        created.kind,
        owner.permissions,
        &owner.secret,
    )
    .context("failed to build terminal owner link")?;
    let owner_secret = owner.secret.clone();
    let stream_id = created.stream_id;
    let cleanup_client = client.clone();
    let cleanup_owner_secret = owner_secret.clone();
    if let Err(error) = print_created_stream(&created, false, args.show_owner_link) {
        cleanup_failed_start(
            &cleanup_client,
            &stream_id,
            &cleanup_owner_secret,
            &owner_link,
        )
        .await;
        return Err(error);
    }
    let mut started = false;
    let result = host(
        client,
        stream_id,
        owner_secret,
        args.columns,
        args.rows,
        args.command,
        &mut started,
    )
    .await;
    if result.is_err() && !started {
        cleanup_failed_start(
            &cleanup_client,
            &stream_id,
            &cleanup_owner_secret,
            &owner_link,
        )
        .await;
    }
    if result
        .as_ref()
        .is_err_and(|error| error.downcast_ref::<TerminalStartupInterrupted>().is_some())
    {
        exit_interrupted();
    }
    result
}

#[derive(Debug)]
struct TerminalStartupInterrupted;

impl fmt::Display for TerminalStartupInterrupted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("terminal startup interrupted")
    }
}

impl std::error::Error for TerminalStartupInterrupted {}

async fn cleanup_failed_start(
    client: &TsfClient,
    stream_id: &StreamId,
    owner_secret: &LinkSecret,
    owner_link: &Url,
) {
    match client.delete_stream(stream_id, owner_secret).await {
        Ok(()) => {
            eprintln!("Removed terminal {stream_id} after startup failed. It cannot be recovered.");
        }
        Err(error) => {
            eprintln!(
                "Terminal {stream_id} may still be active because startup cleanup failed: {error}"
            );
            eprintln!("Delete it with this private owner link: {owner_link}");
        }
    }
}

struct PtyResize {
    columns: u16,
    rows: u16,
    applied: oneshot::Sender<Result<(), String>>,
}

type PtyOutput = std::io::Result<Vec<u8>>;

#[derive(Default)]
struct PtyOutputHandoffState {
    closing: bool,
    resizing: bool,
    sending: bool,
}

#[derive(Default)]
struct PtyOutputHandoff {
    state: Mutex<PtyOutputHandoffState>,
    changed: Condvar,
    send_finished: Notify,
}

struct PtyWrite {
    data: Vec<u8>,
    applied: oneshot::Sender<Result<(), String>>,
}

enum OwnedTerminalOutput {
    Started { columns: u16, rows: u16 },
    Data(Vec<u8>),
    Resize { columns: u16, rows: u16 },
    Exited { status: i32, output_truncated: bool },
    Heartbeat,
}

impl OwnedTerminalOutput {
    fn as_event(&self) -> TerminalOutputEvent<'_> {
        match self {
            Self::Started { columns, rows } => TerminalOutputEvent::Started {
                columns: *columns,
                rows: *rows,
            },
            Self::Data(data) => TerminalOutputEvent::Data(data),
            Self::Resize { columns, rows } => TerminalOutputEvent::Resize {
                columns: *columns,
                rows: *rows,
            },
            Self::Exited {
                status,
                output_truncated,
            } => TerminalOutputEvent::Exited {
                status: *status,
                output_truncated: *output_truncated,
            },
            Self::Heartbeat => TerminalOutputEvent::Heartbeat,
        }
    }
}

struct OutputCommand {
    event: OwnedTerminalOutput,
    durable: Option<oneshot::Sender<()>>,
}

struct AbortOnDropTask<T> {
    handle: Option<tokio::task::JoinHandle<T>>,
}

impl<T> AbortOnDropTask<T> {
    fn new(handle: tokio::task::JoinHandle<T>) -> Self {
        Self {
            handle: Some(handle),
        }
    }

    fn handle(&mut self) -> &mut tokio::task::JoinHandle<T> {
        self.handle.as_mut().expect("task handle")
    }

    fn abort(&mut self) {
        if let Some(handle) = &self.handle {
            handle.abort();
        }
    }

    async fn join(mut self) -> Result<T, tokio::task::JoinError> {
        self.handle.take().expect("task handle").await
    }
}

impl<T> Drop for AbortOnDropTask<T> {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

struct ChildGuard {
    killer: Option<Box<dyn ChildKiller + Send + Sync>>,
}

impl ChildGuard {
    fn new(killer: Box<dyn ChildKiller + Send + Sync>) -> Self {
        Self {
            killer: Some(killer),
        }
    }

    fn take(&mut self) -> Option<Box<dyn ChildKiller + Send + Sync>> {
        self.killer.take()
    }

    fn disarm(&mut self) {
        self.killer = None;
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(killer) = &mut self.killer {
            let _ = killer.kill();
        }
    }
}

struct OutputPublisher {
    writer: TsfWriter,
    pending: VecDeque<AppendTicket>,
}

impl OutputPublisher {
    fn new(writer: TsfWriter) -> Self {
        Self {
            writer,
            pending: VecDeque::new(),
        }
    }

    async fn publish(&mut self, event: TerminalOutputEvent<'_>) -> eyre::Result<()> {
        if self.pending.len() >= MAX_PENDING_OUTPUT_RECORDS {
            self.wait_for_oldest().await?;
        }
        let payload = encode_terminal_output(event).context("failed to encode terminal output")?;
        self.pending.push_back(
            self.writer
                .submit(AppendBatch::single(PartHeader::unsplit(), payload)?)?,
        );
        Ok(())
    }

    async fn flush(&mut self) -> eyre::Result<()> {
        while !self.pending.is_empty() {
            self.wait_for_oldest().await?;
        }
        Ok(())
    }

    async fn close(mut self) -> eyre::Result<()> {
        self.flush().await?;
        self.writer
            .close()
            .await
            .context("failed to close terminal output")
    }

    async fn wait_for_oldest(&mut self) -> eyre::Result<()> {
        self.pending
            .pop_front()
            .context("terminal output queue is empty")?
            .await
            .context("failed to publish terminal output")?;
        Ok(())
    }
}

async fn run_output_publisher(
    writer: TsfWriter,
    mut commands: mpsc::Receiver<OutputCommand>,
) -> eyre::Result<()> {
    let mut output = OutputPublisher::new(writer);
    while let Some(command) = commands.recv().await {
        output.publish(command.event.as_event()).await?;
        if let Some(durable) = command.durable {
            output.flush().await?;
            let _ = durable.send(());
        }
    }
    output.close().await
}

async fn forward_terminal_input(
    mut input_reader: tailsurf::TsfReadSession,
    pty_input: mpsc::Sender<PtyWrite>,
    pty_resizes: mpsc::Sender<PtyResize>,
) -> eyre::Result<()> {
    let mut writer_positions = HashMap::<WriterId, u64>::new();
    loop {
        let Some(batch) = input_reader
            .next_batch()
            .await
            .context("failed to read terminal input")?
        else {
            bail!("terminal input stream ended while the PTY was running");
        };
        for record in &batch {
            if !writer_positions.contains_key(&record.writer_id)
                && writer_positions.len() >= MAX_INPUT_WRITERS
            {
                bail!("terminal input exceeded the writer identity limit");
            }
            if writer_positions
                .get(&record.writer_id)
                .is_some_and(|position| *position >= record.writer_seq_num)
            {
                continue;
            }
            writer_positions.insert(record.writer_id, record.writer_seq_num);
            if record.part != PartHeader::unsplit() {
                bail!("terminal input must use unsplit records");
            }
            match decode_terminal_input(record.data).context("invalid terminal input event")? {
                TerminalInputEvent::Data(data) => {
                    let (applied, confirmation) = oneshot::channel();
                    pty_input
                        .send(PtyWrite {
                            data: data.to_vec(),
                            applied,
                        })
                        .await
                        .map_err(|_| eyre!("PTY input worker stopped"))?;
                    confirmation
                        .await
                        .map_err(|_| eyre!("PTY input worker stopped"))?
                        .map_err(|error| eyre!("failed to write PTY input: {error}"))?;
                }
                TerminalInputEvent::Resize { columns, rows } => {
                    let (applied, confirmation) = oneshot::channel();
                    pty_resizes
                        .send(PtyResize {
                            columns,
                            rows,
                            applied,
                        })
                        .await
                        .map_err(|_| eyre!("terminal host stopped"))?;
                    confirmation
                        .await
                        .map_err(|_| eyre!("terminal host stopped"))?
                        .map_err(|error| eyre!("failed to resize PTY: {error}"))?;
                }
            }
        }
    }
}

async fn stop_child(child_guard: &mut ChildGuard) -> eyre::Result<()> {
    let Some(mut child_killer) = child_guard.take() else {
        return Ok(());
    };
    let (child_killer, result) = tokio::task::spawn_blocking(move || {
        let result = child_killer.kill();
        (child_killer, result)
    })
    .await
    .context("terminal kill task panicked")?;
    if let Err(error) = result {
        child_guard.killer = Some(child_killer);
        return Err(error).context("failed to stop terminal command");
    }
    Ok(())
}

fn finish_output_task(
    result: Result<eyre::Result<()>, tokio::task::JoinError>,
) -> eyre::Result<()> {
    result.context("terminal output publisher panicked")?
}

async fn enqueue_final_output(
    output: &mpsc::Sender<OutputCommand>,
    output_task: &mut AbortOnDropTask<eyre::Result<()>>,
    event: OwnedTerminalOutput,
) -> eyre::Result<()> {
    tokio::select! {
        result = output.send(OutputCommand { event, durable: None }) => {
            result.map_err(|_| eyre!("terminal output publisher stopped"))
        }
        result = output_task.handle() => {
            finish_output_task(result)?;
            bail!("terminal output publisher stopped before terminal exit was durable");
        }
        interrupt = tokio::signal::ctrl_c() => {
            interrupt.context("failed to listen for interrupt signal")?;
            output_task.abort();
            exit_interrupted();
        }
    }
}

async fn host(
    client: TsfClient,
    stream_id: StreamId,
    owner_secret: LinkSecret,
    columns: u16,
    rows: u16,
    command: Vec<String>,
    started: &mut bool,
) -> eyre::Result<()> {
    let mut input_options = ReadOptions::new(stream_id).with_link_secret(owner_secret.clone());
    input_options.start = Some(ReadStart::SeqNum(0));
    let input_reader = client
        .connect_terminal_input_reader(input_options)
        .await
        .context("failed to connect terminal input")?;
    let output_writer = client
        .connect_terminal_output_writer(
            DurableWriterOptions::new(stream_id, owner_secret).with_expected_next_seq_num(0),
        )
        .await
        .context("failed to connect terminal output")?;

    let pair = native_pty_system()
        .openpty(PtySize {
            rows,
            cols: columns,
            pixel_width: 0,
            pixel_height: 0,
        })
        .map_err(|error| eyre!("failed to open PTY: {error:#}"))?;
    let mut command_builder = if let Some(program) = command.first() {
        let mut builder = CommandBuilder::new(program);
        builder.args(&command[1..]);
        builder
    } else {
        CommandBuilder::new_default_prog()
    };
    command_builder.env("TERM", "xterm-256color");
    let mut pty_reader = pair
        .master
        .try_clone_reader()
        .map_err(|error| eyre!("failed to open PTY output: {error:#}"))?;
    let mut pty_writer = pair
        .master
        .take_writer()
        .map_err(|error| eyre!("failed to open PTY input: {error:#}"))?;
    let master = pair.master;
    let mut child = pair
        .slave
        .spawn_command(command_builder)
        .map_err(|error| eyre!("failed to spawn terminal command: {error:#}"))?;
    drop(pair.slave);
    let mut child_guard = ChildGuard::new(child.clone_killer());
    let (output_command_tx, output_command_rx) = mpsc::channel(PTY_OUTPUT_QUEUE);
    let mut output_task = AbortOnDropTask::new(tokio::spawn(run_output_publisher(
        output_writer,
        output_command_rx,
    )));
    let (started_tx, mut started_rx) = oneshot::channel();
    output_command_tx
        .send(OutputCommand {
            event: OwnedTerminalOutput::Started { columns, rows },
            durable: Some(started_tx),
        })
        .await
        .map_err(|_| eyre!("terminal output publisher stopped"))?;
    tokio::select! {
        confirmation = &mut started_rx => {
            confirmation.context("terminal output publisher stopped before startup was durable")?;
            *started = true;
        }
        result = output_task.handle() => {
            finish_output_task(result)?;
            bail!("terminal output publisher stopped before startup was durable");
        }
        interrupt = tokio::signal::ctrl_c() => {
            interrupt.context("failed to listen for interrupt signal")?;
            output_task.abort();
            stop_child(&mut child_guard).await?;
            return Err(TerminalStartupInterrupted.into());
        }
    }

    // A stalled PTY write must not block the async runtime. The native thread may be detached on
    // shutdown, while the bounded channel prevents unbounded input retention.
    let (pty_input_tx, mut pty_input_rx) = mpsc::channel::<PtyWrite>(PTY_INPUT_QUEUE);
    let (input_worker_done_tx, mut input_worker_done_rx) = oneshot::channel();
    let input_thread = std::thread::spawn(move || {
        let result = (|| -> eyre::Result<()> {
            while let Some(write) = pty_input_rx.blocking_recv() {
                let result = (|| -> eyre::Result<()> {
                    pty_writer
                        .write_all(&write.data)
                        .context("failed to write terminal input to PTY")?;
                    pty_writer.flush().context("failed to flush PTY input")
                })();
                match result {
                    Ok(()) => {
                        let _ = write.applied.send(Ok(()));
                    }
                    Err(error) => {
                        let error = format!("{error:#}");
                        let _ = write.applied.send(Err(error.clone()));
                        bail!(error);
                    }
                }
            }
            Ok(())
        })();
        let _ = input_worker_done_tx.send(result);
    });
    let (pty_resize_tx, mut pty_resize_rx) = mpsc::channel(PTY_INPUT_QUEUE);
    let mut input_task = AbortOnDropTask::new(tokio::spawn(forward_terminal_input(
        input_reader,
        pty_input_tx,
        pty_resize_tx,
    )));

    let (output_tx, mut output_rx) = mpsc::channel::<PtyOutput>(PTY_OUTPUT_QUEUE);
    // The cutoff handshake recovers the one chunk that may already be blocked in
    // `blocking_send` when the receiver closes.
    let output_handoff_state = Arc::new(PtyOutputHandoff::default());
    let thread_handoff_state = Arc::clone(&output_handoff_state);
    let (output_close_tx, mut output_close_rx) = oneshot::channel();
    // A descendant may retain the slave after the direct child exits. A native thread can be
    // detached after the bounded drain without delaying Tokio runtime shutdown.
    let output_thread = std::thread::spawn(move || {
        let mut output_close_tx = Some(output_close_tx);
        let mut buffer = vec![0_u8; STDIN_READ_BYTES];
        loop {
            match pty_reader.read(&mut buffer) {
                Ok(0) => break,
                Ok(count) => {
                    if !send_pty_output(
                        Ok(buffer[..count].to_vec()),
                        &output_tx,
                        &thread_handoff_state,
                        &mut output_close_tx,
                    ) {
                        break;
                    }
                }
                Err(error) => {
                    let _ = send_pty_output(
                        Err(error),
                        &output_tx,
                        &thread_handoff_state,
                        &mut output_close_tx,
                    );
                    break;
                }
            }
        }
    });
    let mut wait_task = tokio::task::spawn_blocking(move || child.wait());
    let mut heartbeat = tokio::time::interval(HEARTBEAT_INTERVAL);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    heartbeat.tick().await;
    let mut output_open = true;
    let mut output_drain_deadline = None;
    let mut output_drain_max_deadline = None;
    let mut exit_status = None;
    let mut output_truncated = false;
    let mut interrupted = false;
    let mut pending_output = VecDeque::new();
    let mut checkpoints = TerminalCheckpointEmitter::new(columns, rows);

    while exit_status.is_none() || output_open {
        tokio::select! {
            permit = output_command_tx.reserve(), if !pending_output.is_empty() => {
                let permit = permit.map_err(|_| eyre!("terminal output publisher stopped"))?;
                permit.send(OutputCommand {
                    event: pending_output.pop_front().expect("pending output"),
                    durable: None,
                });
            }
            chunk = output_rx.recv(), if output_open && pending_output.is_empty() => {
                match chunk {
                    Some(Ok(data)) => {
                        let checkpoint = checkpoints.process(&data);
                        queue_pty_data(&mut pending_output, data);
                        if let Some(checkpoint) = checkpoint {
                            pending_output.push_back(OwnedTerminalOutput::Data(checkpoint));
                        }
                        if let Some(max_deadline) = output_drain_max_deadline {
                            output_drain_deadline = Some(std::cmp::min(
                                Instant::now() + PTY_EXIT_DRAIN_IDLE_TIMEOUT,
                                max_deadline,
                            ));
                        }
                    }
                    Some(Err(error)) => return Err(error).context("failed to read PTY output"),
                    None => output_open = false,
                }
            }
            resize = pty_resize_rx.recv(), if exit_status.is_none() && pending_output.is_empty() => {
                let resize = resize.context("terminal input forwarder stopped")?;
                let pending_start = pending_output.len();
                output_open = pause_and_queue_pty_output(
                    &mut output_rx,
                    &output_handoff_state,
                    &mut pending_output,
                ).await?;
                ingest_queued_pty_output(
                    &mut checkpoints,
                    &pending_output,
                    pending_start,
                );
                let result = master
                    .resize(PtySize {
                        rows: resize.rows,
                        cols: resize.columns,
                        pixel_width: 0,
                        pixel_height: 0,
                    })
                    .map_err(|error| format!("{error:#}"));
                if result.is_ok() {
                    pending_output.push_back(OwnedTerminalOutput::Resize {
                        columns: resize.columns,
                        rows: resize.rows,
                    });
                    if let Some(checkpoint) = checkpoints.resize(
                        resize.columns,
                        resize.rows,
                    ) {
                        pending_output.push_back(OwnedTerminalOutput::Data(checkpoint));
                    }
                }
                resume_pty_output(&output_handoff_state);
                let _ = resize.applied.send(result);
            }
            result = input_task.handle(), if exit_status.is_none() => {
                result.context("terminal input forwarder panicked")??;
                bail!("terminal input forwarder stopped while the PTY was running");
            }
            result = &mut input_worker_done_rx, if exit_status.is_none() => {
                result.context("PTY input worker stopped")??;
                bail!("PTY input worker stopped while the PTY was running");
            }
            result = output_task.handle() => {
                finish_output_task(result)?;
                bail!("terminal output publisher stopped while the PTY was running");
            }
            status = &mut wait_task, if exit_status.is_none() => {
                let status = status
                    .context("terminal wait task panicked")?
                    .context("failed to wait for terminal command")?;
                exit_status = Some(i32::try_from(status.exit_code()).unwrap_or(i32::MAX));
                child_guard.disarm();
                input_task.abort();
                let now = Instant::now();
                output_drain_deadline = Some(now + PTY_EXIT_DRAIN_IDLE_TIMEOUT);
                output_drain_max_deadline = Some(now + PTY_EXIT_DRAIN_MAX_TIMEOUT);
            }
            _ = wait_for_deadline(output_drain_deadline), if output_open && exit_status.is_some() && pty_output_is_idle(&pending_output, &output_rx) => {
                let pending_start = pending_output.len();
                close_and_queue_pty_output(
                    &mut output_rx,
                    &output_handoff_state,
                    &mut output_close_rx,
                    &mut pending_output,
                ).await?;
                ingest_queued_pty_output(
                    &mut checkpoints,
                    &pending_output,
                    pending_start,
                );
                output_open = false;
                output_truncated = true;
            }
            _ = wait_for_deadline(output_drain_max_deadline), if output_open && exit_status.is_some() => {
                let pending_start = pending_output.len();
                close_and_queue_pty_output(
                    &mut output_rx,
                    &output_handoff_state,
                    &mut output_close_rx,
                    &mut pending_output,
                ).await?;
                ingest_queued_pty_output(
                    &mut checkpoints,
                    &pending_output,
                    pending_start,
                );
                output_open = false;
                output_truncated = true;
            }
            _ = heartbeat.tick(), if exit_status.is_none() && pending_output.is_empty() => {
                pending_output.push_back(OwnedTerminalOutput::Heartbeat);
                if let Some(checkpoint) = checkpoints.heartbeat() {
                    pending_output.push_back(OwnedTerminalOutput::Data(checkpoint));
                }
            }
            interrupt = tokio::signal::ctrl_c(), if exit_status.is_none() => {
                interrupt.context("failed to listen for interrupt signal")?;
                if interrupted {
                    output_task.abort();
                    exit_interrupted();
                }
                interrupted = true;
                stop_child(&mut child_guard).await?;
            }
        }
    }

    let status = exit_status.context("terminal command ended without an exit status")?;
    drop(master);
    drop(output_rx);
    while let Some(event) = pending_output.pop_front() {
        enqueue_final_output(&output_command_tx, &mut output_task, event).await?;
    }
    if let Some(checkpoint) = checkpoints.flush() {
        enqueue_final_output(
            &output_command_tx,
            &mut output_task,
            OwnedTerminalOutput::Data(checkpoint),
        )
        .await?;
    }
    enqueue_final_output(
        &output_command_tx,
        &mut output_task,
        OwnedTerminalOutput::Exited {
            status: if interrupted {
                INTERRUPT_EXIT_CODE
            } else {
                status
            },
            output_truncated,
        },
    )
    .await?;
    drop(output_command_tx);
    tokio::select! {
        result = output_task.join() => finish_output_task(result)?,
        interrupt = tokio::signal::ctrl_c() => {
            interrupt.context("failed to listen for interrupt signal")?;
            exit_interrupted();
        }
    }
    input_task.abort();
    let _ = input_task.join().await;
    if input_thread.is_finished() {
        input_thread
            .join()
            .map_err(|_| eyre!("PTY input thread panicked"))?;
    }
    if output_thread.is_finished() {
        output_thread
            .join()
            .map_err(|_| eyre!("PTY output thread panicked"))?;
    }

    if interrupted {
        exit_interrupted();
    }
    if status == 0 {
        Ok(())
    } else {
        std::process::exit(status);
    }
}

async fn wait_for_deadline(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => sleep_until(deadline).await,
        None => pending().await,
    }
}

fn ingest_queued_pty_output(
    checkpoints: &mut TerminalCheckpointEmitter,
    pending: &VecDeque<OwnedTerminalOutput>,
    start: usize,
) {
    for event in pending.iter().skip(start) {
        if let OwnedTerminalOutput::Data(data) = event {
            checkpoints.ingest(data);
        }
    }
}

fn queue_pty_data(pending: &mut VecDeque<OwnedTerminalOutput>, mut data: Vec<u8>) {
    if resembles_checkpoint(&data) {
        let remainder = data.split_off(1);
        pending.push_back(OwnedTerminalOutput::Data(data));
        pending.push_back(OwnedTerminalOutput::Data(remainder));
    } else {
        pending.push_back(OwnedTerminalOutput::Data(data));
    }
}

async fn pause_and_queue_pty_output(
    output: &mut mpsc::Receiver<PtyOutput>,
    handoff: &Arc<PtyOutputHandoff>,
    pending: &mut VecDeque<OwnedTerminalOutput>,
) -> eyre::Result<bool> {
    {
        let mut state = handoff
            .state
            .lock()
            .expect("PTY output handoff lock poisoned");
        state.resizing = true;
    }
    loop {
        let send_finished = handoff.send_finished.notified();
        tokio::pin!(send_finished);
        send_finished.as_mut().enable();
        let sending = handoff
            .state
            .lock()
            .expect("PTY output handoff lock poisoned")
            .sending;
        if !sending {
            break;
        }
        tokio::select! {
            result = output.recv() => match result {
                Some(Ok(data)) => queue_pty_data(pending, data),
                Some(Err(error)) => return Err(error).context("failed to read PTY output"),
                None => return Ok(false),
            },
            () = &mut send_finished => {}
        }
    }
    loop {
        match output.try_recv() {
            Ok(Ok(data)) => queue_pty_data(pending, data),
            Ok(Err(error)) => return Err(error).context("failed to read PTY output"),
            Err(mpsc::error::TryRecvError::Empty) => return Ok(true),
            Err(mpsc::error::TryRecvError::Disconnected) => return Ok(false),
        }
    }
}

fn resume_pty_output(handoff: &Arc<PtyOutputHandoff>) {
    let mut state = handoff
        .state
        .lock()
        .expect("PTY output handoff lock poisoned");
    state.resizing = false;
    handoff.changed.notify_all();
}

fn send_pty_output(
    output: PtyOutput,
    sender: &mpsc::Sender<PtyOutput>,
    handoff: &Arc<PtyOutputHandoff>,
    close_ack: &mut Option<oneshot::Sender<Option<PtyOutput>>>,
) -> bool {
    let mut state = handoff
        .state
        .lock()
        .expect("PTY output handoff lock poisoned");
    while state.resizing && !state.closing {
        state = handoff
            .changed
            .wait(state)
            .expect("PTY output handoff lock poisoned");
    }
    if state.closing {
        return false;
    }
    state.sending = true;
    drop(state);

    let result = sender.blocking_send(output);
    let mut state = handoff
        .state
        .lock()
        .expect("PTY output handoff lock poisoned");
    state.sending = false;
    handoff.send_finished.notify_one();
    if state.closing {
        let unsent = result.err().map(|error| error.0);
        drop(state);
        if let Some(close_ack) = close_ack.take() {
            let _ = close_ack.send(unsent);
        }
        return false;
    }
    result.is_ok()
}

async fn close_and_queue_pty_output(
    output: &mut mpsc::Receiver<PtyOutput>,
    handoff: &Arc<PtyOutputHandoff>,
    close_ack: &mut oneshot::Receiver<Option<PtyOutput>>,
    pending: &mut VecDeque<OwnedTerminalOutput>,
) -> eyre::Result<()> {
    let wait_for_in_flight = {
        let mut state = handoff
            .state
            .lock()
            .expect("PTY output handoff lock poisoned");
        state.closing = true;
        state.resizing = false;
        handoff.changed.notify_all();
        state.sending
    };
    output.close();
    while let Some(result) = output.recv().await {
        queue_pty_data(pending, result.context("failed to read PTY output")?);
    }
    if wait_for_in_flight
        && let Some(result) = close_ack
            .await
            .context("PTY output thread stopped during shutdown")?
    {
        queue_pty_data(pending, result.context("failed to read PTY output")?);
    }
    Ok(())
}

fn pty_output_is_idle(
    pending: &VecDeque<OwnedTerminalOutput>,
    output: &mpsc::Receiver<std::io::Result<Vec<u8>>>,
) -> bool {
    pending.is_empty() && output.is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pty_output_cannot_impersonate_a_host_checkpoint() {
        let output = b"\x1b]9999;tailsurf-checkpoint-v1;AAAA\x07".to_vec();
        let mut pending = VecDeque::new();

        queue_pty_data(&mut pending, output.clone());

        let parts = pending
            .into_iter()
            .map(|event| match event {
                OwnedTerminalOutput::Data(data) => data,
                _ => panic!("unexpected terminal output"),
            })
            .collect::<Vec<_>>();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts.concat(), output);
    }

    #[tokio::test]
    async fn queues_buffered_pty_bytes_before_a_resize() {
        let (sender, mut receiver) = mpsc::channel(2);
        sender
            .send(Ok(vec![1]))
            .await
            .expect("first PTY chunk should enqueue");
        sender
            .send(Ok(vec![2]))
            .await
            .expect("second PTY chunk should enqueue");
        let handoff = Arc::new(PtyOutputHandoff::default());
        let mut pending = VecDeque::new();

        assert!(
            pause_and_queue_pty_output(&mut receiver, &handoff, &mut pending)
                .await
                .expect("queued PTY output should be valid")
        );
        pending.push_back(OwnedTerminalOutput::Resize {
            columns: 120,
            rows: 40,
        });
        resume_pty_output(&handoff);

        assert!(matches!(
            pending.pop_front(),
            Some(OwnedTerminalOutput::Data(data)) if data == vec![1]
        ));
        assert!(matches!(
            pending.pop_front(),
            Some(OwnedTerminalOutput::Data(data)) if data == vec![2]
        ));
        assert!(matches!(
            pending.pop_front(),
            Some(OwnedTerminalOutput::Resize {
                columns: 120,
                rows: 40,
            })
        ));
    }

    #[tokio::test]
    async fn pauses_new_pty_output_until_after_a_resize() {
        let (sender, mut receiver) = mpsc::channel(1);
        sender
            .send(Ok(vec![1]))
            .await
            .expect("first PTY chunk should enqueue");
        let handoff = Arc::new(PtyOutputHandoff::default());
        let blocked_handoff = Arc::clone(&handoff);
        let blocked_sender = sender.clone();
        let (close_tx, _close_rx) = oneshot::channel();
        let blocked_send = std::thread::spawn(move || {
            let mut close_tx = Some(close_tx);
            send_pty_output(
                Ok(vec![2]),
                &blocked_sender,
                &blocked_handoff,
                &mut close_tx,
            )
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while !handoff
                .state
                .lock()
                .expect("PTY output handoff lock should not be poisoned")
                .sending
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("PTY output send should block on the full queue");

        let mut pending = VecDeque::new();
        assert!(
            tokio::time::timeout(
                Duration::from_secs(1),
                pause_and_queue_pty_output(&mut receiver, &handoff, &mut pending),
            )
            .await
            .expect("PTY output pause should not stall after a send finishes")
            .expect("PTY output should pause")
        );
        assert!(blocked_send.join().expect("blocked send should finish"));
        pending.push_back(OwnedTerminalOutput::Resize {
            columns: 120,
            rows: 40,
        });

        let paused_handoff = Arc::clone(&handoff);
        let (attempted_tx, attempted_rx) = std::sync::mpsc::channel();
        let (close_tx, _close_rx) = oneshot::channel();
        let paused_send = std::thread::spawn(move || {
            let mut close_tx = Some(close_tx);
            attempted_tx
                .send(())
                .expect("send attempt should be observed");
            send_pty_output(Ok(vec![3]), &sender, &paused_handoff, &mut close_tx)
        });
        attempted_rx
            .recv()
            .expect("paused output thread should start sending");
        assert!(matches!(
            receiver.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));

        resume_pty_output(&handoff);
        assert_eq!(
            receiver
                .recv()
                .await
                .expect("resumed output should enqueue")
                .expect("resumed output should be valid"),
            vec![3],
        );
        assert!(paused_send.join().expect("paused send should finish"));
        assert!(matches!(
            pending.pop_front(),
            Some(OwnedTerminalOutput::Data(data)) if data == vec![1]
        ));
        assert!(matches!(
            pending.pop_front(),
            Some(OwnedTerminalOutput::Data(data)) if data == vec![2]
        ));
        assert!(matches!(
            pending.pop_front(),
            Some(OwnedTerminalOutput::Resize {
                columns: 120,
                rows: 40,
            })
        ));
    }

    #[tokio::test]
    async fn queues_in_flight_pty_bytes_when_closing_output() {
        let (sender, mut receiver) = mpsc::channel(2);
        sender
            .send(Ok(vec![1]))
            .await
            .expect("first PTY chunk should enqueue");
        sender
            .send(Ok(vec![2]))
            .await
            .expect("second PTY chunk should enqueue");
        let state = Arc::new(PtyOutputHandoff::default());
        let thread_state = Arc::clone(&state);
        let (close_tx, mut close_rx) = oneshot::channel();
        let blocked_send = std::thread::spawn(move || {
            let mut close_tx = Some(close_tx);
            send_pty_output(Ok(vec![3]), &sender, &thread_state, &mut close_tx)
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while !state
                .state
                .lock()
                .expect("PTY output handoff lock should not be poisoned")
                .sending
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("PTY output send should block on the full queue");

        let mut pending = VecDeque::new();
        close_and_queue_pty_output(&mut receiver, &state, &mut close_rx, &mut pending)
            .await
            .expect("queued and in-flight PTY output should drain");

        assert!(matches!(
            pending.pop_front(),
            Some(OwnedTerminalOutput::Data(data)) if data == [1]
        ));
        assert!(matches!(
            pending.pop_front(),
            Some(OwnedTerminalOutput::Data(data)) if data == [2]
        ));
        assert!(matches!(
            pending.pop_front(),
            Some(OwnedTerminalOutput::Data(data)) if data == [3]
        ));
        assert!(pending.is_empty());
        assert!(!blocked_send.join().expect("send thread should finish"));
    }

    #[tokio::test]
    async fn closing_output_does_not_wait_for_a_stalled_read() {
        let (_sender, mut receiver) = mpsc::channel(1);
        let state = Arc::new(PtyOutputHandoff::default());
        let (_close_tx, mut close_rx) = oneshot::channel();
        let mut pending = VecDeque::new();

        tokio::time::timeout(
            Duration::from_secs(1),
            close_and_queue_pty_output(&mut receiver, &state, &mut close_rx, &mut pending),
        )
        .await
        .expect("PTY output close should not wait for a blocked read")
        .expect("PTY output close should succeed");
        assert!(pending.is_empty());
    }

    #[tokio::test]
    async fn queued_pty_bytes_are_not_idle() {
        let (sender, mut receiver) = mpsc::channel(1);
        let mut pending = VecDeque::new();

        sender
            .send(Ok(vec![1]))
            .await
            .expect("PTY chunk should enqueue");
        assert!(!pty_output_is_idle(&pending, &receiver));
        receiver
            .recv()
            .await
            .expect("PTY channel should remain open")
            .expect("PTY chunk should be valid");
        assert!(pty_output_is_idle(&pending, &receiver));

        pending.push_back(OwnedTerminalOutput::Heartbeat);
        assert!(!pty_output_is_idle(&pending, &receiver));
    }
}
