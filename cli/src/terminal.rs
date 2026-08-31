use std::{
    collections::{HashMap, VecDeque},
    future::pending,
    io::{Read as _, Write as _},
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
        ws::frame::{PartHeader, RecordFormat},
    },
};
use tokio::{
    sync::{mpsc, oneshot},
    time::{Duration, Instant, sleep_until},
};
use url::Url;

use super::{
    INTERRUPT_EXIT_CODE, STDIN_READ_BYTES, StreamExpiryArg, exit_interrupted, initial_link,
    print_created_stream,
};

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
    if args.columns == 0 || args.rows == 0 {
        bail!("terminal columns and rows must be positive");
    }
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
    print_created_stream(&created, false)?;

    let owner_secret = created
        .links
        .iter()
        .find(|link| link.permissions.allows_owner())
        .context("created terminal did not include an owner link")?
        .secret
        .clone();
    host(
        client,
        created.stream_id,
        owner_secret,
        args.columns,
        args.rows,
        args.command,
    )
    .await
}

enum PtyInput {
    Data(Vec<u8>),
    Resize {
        columns: u16,
        rows: u16,
        applied: oneshot::Sender<Result<(), String>>,
    },
}

enum OwnedTerminalOutput {
    Started { columns: u16, rows: u16 },
    Data(Vec<u8>),
    Resize { columns: u16, rows: u16 },
    Exited { status: i32 },
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
            Self::Exited { status } => TerminalOutputEvent::Exited { status: *status },
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
        self.pending
            .push_back(self.writer.submit(AppendBatch::single(
                PartHeader::unsplit(),
                RecordFormat::Bytes,
                payload,
            )?)?);
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
    pty_input: mpsc::Sender<PtyInput>,
    applied_resizes: mpsc::Sender<(u16, u16)>,
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
            if record.format != RecordFormat::Bytes || record.part != PartHeader::unsplit() {
                bail!("terminal input must use unsplit byte records");
            }
            match decode_terminal_input(record.data).context("invalid terminal input event")? {
                TerminalInputEvent::Data(data) => pty_input
                    .send(PtyInput::Data(data.to_vec()))
                    .await
                    .map_err(|_| eyre!("PTY input worker stopped"))?,
                TerminalInputEvent::Resize { columns, rows } => {
                    let (applied, confirmation) = oneshot::channel();
                    pty_input
                        .send(PtyInput::Resize {
                            columns,
                            rows,
                            applied,
                        })
                        .await
                        .map_err(|_| eyre!("PTY input worker stopped"))?;
                    confirmation
                        .await
                        .map_err(|_| eyre!("PTY input worker stopped"))?
                        .map_err(|error| eyre!("failed to resize PTY: {error}"))?;
                    applied_resizes
                        .send((columns, rows))
                        .await
                        .map_err(|_| eyre!("terminal host stopped"))?;
                }
            }
        }
    }
}

async fn stop_child(child_guard: &mut ChildGuard) -> eyre::Result<()> {
    if let Some(mut child_killer) = child_guard.take() {
        tokio::task::spawn_blocking(move || child_killer.kill())
            .await
            .context("terminal kill task panicked")?
            .context("failed to stop terminal command")?;
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
        }
        result = output_task.handle() => {
            finish_output_task(result)?;
            bail!("terminal output publisher stopped before startup was durable");
        }
        interrupt = tokio::signal::ctrl_c() => {
            interrupt.context("failed to listen for interrupt signal")?;
            output_task.abort();
            stop_child(&mut child_guard).await?;
            exit_interrupted();
        }
    }

    // A stalled PTY write must not block the async runtime. The native thread may be detached on
    // shutdown, while the bounded channel prevents unbounded input retention.
    let (pty_input_tx, mut pty_input_rx) = mpsc::channel::<PtyInput>(PTY_INPUT_QUEUE);
    let (input_worker_done_tx, mut input_worker_done_rx) = oneshot::channel();
    let input_thread = std::thread::spawn(move || {
        let result = (|| -> eyre::Result<()> {
            while let Some(event) = pty_input_rx.blocking_recv() {
                match event {
                    PtyInput::Data(data) => {
                        pty_writer
                            .write_all(&data)
                            .context("failed to write terminal input to PTY")?;
                        pty_writer.flush().context("failed to flush PTY input")?;
                    }
                    PtyInput::Resize {
                        columns,
                        rows,
                        applied,
                    } => {
                        let result = master
                            .resize(PtySize {
                                rows,
                                cols: columns,
                                pixel_width: 0,
                                pixel_height: 0,
                            })
                            .map_err(|error| format!("{error:#}"));
                        let _ = applied.send(result);
                    }
                }
            }
            Ok(())
        })();
        let _ = input_worker_done_tx.send(result);
    });
    let (applied_resize_tx, mut applied_resize_rx) = mpsc::channel(PTY_INPUT_QUEUE);
    let mut input_task = AbortOnDropTask::new(tokio::spawn(forward_terminal_input(
        input_reader,
        pty_input_tx,
        applied_resize_tx,
    )));

    let (output_tx, mut output_rx) = mpsc::channel::<std::io::Result<Vec<u8>>>(PTY_OUTPUT_QUEUE);
    // A descendant may retain the slave after the direct child exits. A native thread can be
    // detached after the bounded drain without delaying Tokio runtime shutdown.
    let output_thread = std::thread::spawn(move || {
        let mut buffer = vec![0_u8; STDIN_READ_BYTES];
        loop {
            match pty_reader.read(&mut buffer) {
                Ok(0) => break,
                Ok(count) => {
                    if output_tx
                        .blocking_send(Ok(buffer[..count].to_vec()))
                        .is_err()
                    {
                        break;
                    }
                }
                Err(error) => {
                    let _ = output_tx.blocking_send(Err(error));
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
    let mut interrupted = false;
    let mut pending_output = None;

    while exit_status.is_none() || output_open {
        tokio::select! {
            permit = output_command_tx.reserve(), if pending_output.is_some() => {
                let permit = permit.map_err(|_| eyre!("terminal output publisher stopped"))?;
                permit.send(OutputCommand {
                    event: pending_output.take().expect("pending output"),
                    durable: None,
                });
            }
            chunk = output_rx.recv(), if output_open && pending_output.is_none() => {
                match chunk {
                    Some(Ok(data)) => {
                        pending_output = Some(OwnedTerminalOutput::Data(data));
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
            resize = applied_resize_rx.recv(), if exit_status.is_none() && pending_output.is_none() => {
                let (columns, rows) = resize.context("PTY input worker stopped")?;
                pending_output = Some(OwnedTerminalOutput::Resize { columns, rows });
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
            _ = wait_for_deadline(output_drain_deadline), if output_open && exit_status.is_some() => {
                output_open = false;
            }
            _ = heartbeat.tick(), if exit_status.is_none() && pending_output.is_none() => {
                pending_output = Some(OwnedTerminalOutput::Heartbeat);
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
    drop(output_rx);
    if let Some(event) = pending_output.take() {
        enqueue_final_output(&output_command_tx, &mut output_task, event).await?;
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
