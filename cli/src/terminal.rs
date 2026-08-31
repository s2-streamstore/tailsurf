use std::{
    collections::{HashMap, VecDeque},
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
    time::Duration,
};
use url::Url;

use super::{
    INTERRUPT_EXIT_CODE, STDIN_READ_BYTES, StreamExpiryArg, exit_interrupted, initial_link,
    print_created_stream,
};

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
const MAX_INPUT_WRITERS: usize = 4_096;
const MAX_PENDING_OUTPUT_RECORDS: usize = 32;
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
    let mut input_reader = client
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
    let mut output = OutputPublisher::new(output_writer);
    output
        .publish(TerminalOutputEvent::Started { columns, rows })
        .await?;
    output.flush().await?;

    let (pty_input_tx, pty_input_rx) = std::sync::mpsc::channel::<PtyInput>();
    let input_task = tokio::task::spawn_blocking(move || -> eyre::Result<()> {
        while let Ok(event) = pty_input_rx.recv() {
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
    });

    let (output_tx, mut output_rx) = mpsc::channel::<std::io::Result<Vec<u8>>>(PTY_OUTPUT_QUEUE);
    let output_task = tokio::task::spawn_blocking(move || {
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
    let mut exit_status = None;
    let mut interrupted = false;
    let mut input_writer_positions = HashMap::<WriterId, u64>::new();

    while exit_status.is_none() || output_open {
        tokio::select! {
            batch = input_reader.next_batch(), if exit_status.is_none() => {
                let Some(batch) = batch.context("failed to read terminal input")? else {
                    bail!("terminal input stream ended while the PTY was running");
                };
                for record in &batch {
                    if !input_writer_positions.contains_key(&record.writer_id)
                        && input_writer_positions.len() >= MAX_INPUT_WRITERS
                    {
                        bail!("terminal input exceeded the writer identity limit");
                    }
                    if input_writer_positions
                        .get(&record.writer_id)
                        .is_some_and(|position| *position >= record.writer_seq_num)
                    {
                        continue;
                    }
                    input_writer_positions.insert(record.writer_id, record.writer_seq_num);
                    if record.format != RecordFormat::Bytes || record.part != PartHeader::unsplit() {
                        bail!("terminal input must use unsplit byte records");
                    }
                    match decode_terminal_input(record.data).context("invalid terminal input event")? {
                        TerminalInputEvent::Data(data) => pty_input_tx
                            .send(PtyInput::Data(data.to_vec()))
                            .map_err(|_| eyre!("PTY input worker stopped"))?,
                        TerminalInputEvent::Resize { columns, rows } => {
                            let (applied, confirmation) = oneshot::channel();
                            pty_input_tx
                                .send(PtyInput::Resize {
                                    columns,
                                    rows,
                                    applied,
                                })
                                .map_err(|_| eyre!("PTY input worker stopped"))?;
                            confirmation
                                .await
                                .map_err(|_| eyre!("PTY input worker stopped"))?
                                .map_err(|error| eyre!("failed to resize PTY: {error}"))?;
                            output
                                .publish(TerminalOutputEvent::Resize { columns, rows })
                                .await?;
                        }
                    }
                }
            }
            chunk = output_rx.recv(), if output_open => {
                match chunk {
                    Some(Ok(data)) => output.publish(TerminalOutputEvent::Data(&data)).await?,
                    Some(Err(error)) => return Err(error).context("failed to read PTY output"),
                    None => output_open = false,
                }
            }
            status = &mut wait_task, if exit_status.is_none() => {
                let status = status
                    .context("terminal wait task panicked")?
                    .context("failed to wait for terminal command")?;
                exit_status = Some(i32::try_from(status.exit_code()).unwrap_or(i32::MAX));
                child_guard.disarm();
            }
            _ = heartbeat.tick(), if exit_status.is_none() => {
                output.publish(TerminalOutputEvent::Heartbeat).await?;
            }
            interrupt = tokio::signal::ctrl_c(), if !interrupted && exit_status.is_none() => {
                interrupt.context("failed to listen for interrupt signal")?;
                interrupted = true;
                if let Some(mut child_killer) = child_guard.take() {
                    tokio::task::spawn_blocking(move || child_killer.kill())
                        .await
                        .context("terminal kill task panicked")?
                        .context("failed to stop terminal command")?;
                }
            }
        }
    }

    let status = exit_status.context("terminal command ended without an exit status")?;
    output
        .publish(TerminalOutputEvent::Exited {
            status: if interrupted {
                INTERRUPT_EXIT_CODE
            } else {
                status
            },
        })
        .await?;
    output.close().await?;
    drop(pty_input_tx);
    input_task.await.context("PTY input task panicked")??;
    output_task.await.context("PTY output task panicked")?;

    if interrupted {
        exit_interrupted();
    }
    if status == 0 {
        Ok(())
    } else {
        std::process::exit(status);
    }
}
