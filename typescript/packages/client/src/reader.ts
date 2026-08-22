import {
  DEFAULT_READ_TAIL_OFFSET,
  MAX_SAFE_INTEGER_U64,
  MAX_U64,
  MAX_PLAYBACK_RATE,
  MAX_READ_WAIT_SECONDS,
  MIN_PLAYBACK_RATE,
  parseStreamId,
  type ClientFrame,
  type ReadRequest,
  type ReadStart as ProtocolReadStart,
  type ReadStop as ProtocolReadStop,
  type ReadRecord,
  type CaughtUpPosition,
  type ServerFrame,
  type StreamId,
} from "@tailsurf/protocol";

export type ReadStart = ProtocolReadStart;
export type ReadStop = ProtocolReadStop;

import { streamMetadataFromWire, type StreamMetadata } from "./models.js";

import {
  TsfClientError,
  TsfWebSocketClosedError,
} from "./errors.js";
import {
  type FrameSocket,
  isRetryableSocketError,
  NORMAL_CLOSE_CODE,
  reconnectSocket,
  requireLinkSecret,
  type SocketPolicy,
  u64,
  unexpectedFrame,
  WEBSOCKET_READ_IDLE_TIMEOUT_MS,
  withTimeout,
} from "./socket.js";
import { INITIAL_RETRY_BACKOFF_MS } from "./retry.js";

export interface ReadOptions {
  readonly streamId: StreamId;
  /** Cancels connection establishment. Close the returned session to stop reading. */
  readonly signal?: AbortSignal;
  readonly start?: ReadStart;
  readonly stop?: ReadStop;
  readonly rate?: number;
  readonly linkSecret?: string;
  readonly onCaughtUp?: (caughtUp: CaughtUpPosition) => void;
  readonly onStreamMetadata?: (stream: StreamMetadata) => void;
}

export interface TsfReadSession extends AsyncIterable<ReadRecord> {
  nextRecord(): Promise<ReadRecord | undefined>;
  lastCaughtUp(): CaughtUpPosition | undefined;
  streamMetadata(): StreamMetadata;
  /** Next sequence number the session will read, once known. */
  position(): bigint | undefined;
  /** Remaining record count, or undefined when unbounded. */
  remainingCount(): bigint | undefined;
  close(): void;
}

export interface NormalizedReadOptions {
  readonly streamId: StreamId;
  start: ReadStart;
  stop?: NormalizedReadStop;
  readonly rate?: number;
  readonly linkSecret?: string;
  readonly onCaughtUp?: (caughtUp: CaughtUpPosition) => void;
  readonly onStreamMetadata?: (stream: StreamMetadata) => void;
  streamMetadata?: StreamMetadata;
  lastCaughtUp?: CaughtUpPosition;
}

interface NormalizedReadStop {
  count?: bigint;
  readonly untilTimestampMs?: bigint;
  readonly waitSeconds?: number;
}

export function normalizeReadOptions(
  options: ReadOptions,
): NormalizedReadOptions {
  const start = normalizeReadStart(options.start ?? {
    type: "tailOffset",
    tailOffset: DEFAULT_READ_TAIL_OFFSET,
  });
  const rate = options.rate;
  const stop = normalizeReadStop(options.stop);
  if (
    rate !== undefined &&
    (!Number.isFinite(rate) || rate < MIN_PLAYBACK_RATE || rate > MAX_PLAYBACK_RATE)
  ) {
    throw new TsfClientError(
      "invalid_read_parameter",
      `rate must be between ${MIN_PLAYBACK_RATE} and ${MAX_PLAYBACK_RATE}`,
    );
  }
  if (
    rate !== undefined &&
    stop?.count === undefined &&
    stop?.untilTimestampMs === undefined &&
    stop?.waitSeconds !== 0
  ) {
    throw new TsfClientError(
      "invalid_read_parameter",
      "rate requires stop.count, stop.untilTimestampMs, or stop.waitSeconds=0",
    );
  }
  return {
    streamId: parseStreamId(options.streamId),
    start,
    ...(stop === undefined ? {} : { stop }),
    ...(rate === undefined ? {} : { rate }),
    ...(options.linkSecret === undefined
      ? {}
      : { linkSecret: requireLinkSecret(options.linkSecret) }),
    ...(options.onCaughtUp === undefined ? {} : { onCaughtUp: options.onCaughtUp }),
    ...(options.onStreamMetadata === undefined
      ? {}
      : { onStreamMetadata: options.onStreamMetadata }),
  };
}

export function openReadFrame(
  linkSecret: string | undefined,
): Extract<ClientFrame, { readonly type: "openRead" }> {
  return {
    type: "openRead",
    ...(linkSecret === undefined
      ? {}
      : { linkSecret }),
  };
}

export abstract class BaseTsfReadSession implements TsfReadSession {
  protected pendingRecords: readonly ReadRecord[] = [];
  protected pendingRecordIndex = 0;
  protected finished = false;
  protected readonly controller = new AbortController();
  #reading = false;

  protected constructor(protected readonly options: NormalizedReadOptions) {}

  public async nextRecord(): Promise<ReadRecord | undefined> {
    if (this.#reading) {
      throw new TsfClientError(
        "concurrent_read",
        "only one read may be pending on a TSF read session",
      );
    }
    this.#reading = true;
    try {
      return await this.pump();
    } finally {
      this.#reading = false;
    }
  }

  public close(): void {
    this.finished = true;
    this.controller.abort();
    this.closeTransport();
  }

  public async *[Symbol.asyncIterator](): AsyncIterableIterator<ReadRecord> {
    try {
      for (;;) {
        const record = await this.nextRecord();
        if (record === undefined) {
          return;
        }
        yield record;
      }
    } finally {
      this.close();
    }
  }

  public lastCaughtUp(): CaughtUpPosition | undefined {
    return this.options.lastCaughtUp;
  }

  public streamMetadata(): StreamMetadata {
    const stream = this.options.streamMetadata;
    if (stream === undefined) {
      throw new TsfClientError(
        "missing_stream_metadata",
        "reader handshake did not provide stream metadata",
      );
    }
    return stream;
  }

  public position(): bigint | undefined {
    return this.options.start.type === "seqNum"
      ? this.options.start.seqNum
      : undefined;
  }

  public remainingCount(): bigint | undefined {
    return this.options.stop?.count;
  }

  protected nextPendingRecord(): ReadRecord | undefined {
    const record = this.pendingRecords[this.pendingRecordIndex];
    if (record === undefined) {
      return undefined;
    }
    this.pendingRecordIndex += 1;
    if (this.pendingRecordIndex === this.pendingRecords.length) {
      this.pendingRecords = [];
      this.pendingRecordIndex = 0;
    }
    this.finished = advanceReadOptions(this.options, record.seqNum);
    return record;
  }

  protected notify<T>(observer: ((value: T) => void) | undefined, value: T): void {
    try {
      observer?.(value);
    } catch (error) {
      this.close();
      throw error;
    }
  }

  protected abstract pump(): Promise<ReadRecord | undefined>;

  protected abstract closeTransport(): void;
}

export class DefaultTsfReadSession extends BaseTsfReadSession {
  #socket: FrameSocket;

  public constructor(
    options: NormalizedReadOptions,
    socket: FrameSocket,
    private readonly connect: (signal: AbortSignal) => Promise<FrameSocket>,
    private readonly policy: SocketPolicy,
  ) {
    super(options);
    this.#socket = socket;
    if (options.streamMetadata === undefined) {
      throw new TsfClientError(
        "missing_stream_metadata",
        "reader handshake did not provide stream metadata",
      );
    }
  }

  protected closeTransport(): void {
    this.#socket.close();
  }

  protected async pump(): Promise<ReadRecord | undefined> {
    let reconnectAttempts = 0;
    let reconnectDelay = INITIAL_RETRY_BACKOFF_MS;
    while (!this.finished && !readExhausted(this.options)) {
      const pending = this.nextPendingRecord();
      if (pending !== undefined) {
        return pending;
      }
      let frame: ServerFrame;
      try {
        frame = await withTimeout(
          this.#socket.nextFrame(),
          WEBSOCKET_READ_IDLE_TIMEOUT_MS,
          "read stream record",
          this.controller.signal,
        );
      } catch (error) {
        if (this.finished) {
          return undefined;
        }
        if (
          error instanceof TsfWebSocketClosedError &&
          error.closeCode === NORMAL_CLOSE_CODE
        ) {
          this.finished = true;
          return undefined;
        }
        if (!isRetryableSocketError(error)) {
          throw error;
        }
        this.#socket.close();
        let recovered: Awaited<ReturnType<typeof reconnectSocket>>;
        try {
          recovered = await reconnectSocket(
            () => this.connect(this.controller.signal),
            this.policy,
            reconnectAttempts,
            reconnectDelay,
            error,
            this.controller.signal,
          );
        } catch (reconnectError) {
          if (this.finished) {
            return undefined;
          }
          throw reconnectError;
        }
        this.#socket = recovered.socket;
        // A completed handshake starts a fresh retry burst. The retry budget bounds
        // consecutive connection failures, not established connections that later close.
        reconnectAttempts = 0;
        reconnectDelay = INITIAL_RETRY_BACKOFF_MS;
        continue;
      }

      if (frame.type === "readBatch") {
        validateReadBatchForRequest(frame.records, this.options);
        this.pendingRecords = frame.records;
        this.pendingRecordIndex = 0;
        continue;
      }
      if (frame.type === "heartbeat") {
        continue;
      }
      if (frame.type === "caughtUp") {
        validateCaughtUpForRequest(frame.nextSeqNum, this.options);
        const caughtUp: CaughtUpPosition = {
          nextSeqNum: frame.nextSeqNum,
          lastTimestampMs: frame.lastTimestampMs,
        };
        this.options.lastCaughtUp = caughtUp;
        this.options.start = { type: "seqNum", seqNum: caughtUp.nextSeqNum };
        this.notify(this.options.onCaughtUp, caughtUp);
        continue;
      }
      if (frame.type === "streamMetadata") {
        const stream = streamMetadataFromWire(frame.stream);
        this.options.streamMetadata = stream;
        this.notify(this.options.onStreamMetadata, stream);
        continue;
      }
      throw unexpectedFrame(frame);
    }
    this.finished = true;
    this.#socket.close();
    return undefined;
  }
}

function readSelector(value: bigint, name: string): bigint {
  const parsed = u64(value, name);
  if (parsed > MAX_SAFE_INTEGER_U64) {
    throw new TsfClientError(
      "invalid_read_parameter",
      `${name} cannot exceed ${MAX_SAFE_INTEGER_U64}`,
    );
  }
  return parsed;
}

function readWaitSeconds(value: number): number {
  if (!Number.isInteger(value) || value < 0 || value > MAX_READ_WAIT_SECONDS) {
    throw new TsfClientError(
      "invalid_read_parameter",
      `waitSeconds must be an integer from 0 through ${MAX_READ_WAIT_SECONDS}`,
    );
  }
  return value;
}

function normalizeReadStop(stop: ReadStop | undefined): NormalizedReadStop | undefined {
  if (stop === undefined) {
    return undefined;
  }
  const count = stop.count === undefined ? undefined : u64(stop.count, "stop.count");
  const untilTimestampMs = stop.untilTimestampMs === undefined
    ? undefined
    : readSelector(stop.untilTimestampMs, "stop.untilTimestampMs");
  const waitSeconds = stop.waitSeconds === undefined
    ? undefined
    : readWaitSeconds(stop.waitSeconds);
  return count === undefined &&
      untilTimestampMs === undefined &&
      waitSeconds === undefined
    ? undefined
    : {
        ...(count === undefined ? {} : { count }),
        ...(untilTimestampMs === undefined ? {} : { untilTimestampMs }),
        ...(waitSeconds === undefined ? {} : { waitSeconds }),
      };
}

function normalizeReadStart(start: ReadStart): ReadStart {
  switch (start.type) {
    case "seqNum":
      return { type: "seqNum", seqNum: readSelector(start.seqNum, "start.seqNum") };
    case "timestampMs":
      return {
        type: "timestampMs",
        timestampMs: readSelector(start.timestampMs, "start.timestampMs"),
      };
    case "tailOffset":
      return {
        type: "tailOffset",
        tailOffset: readSelector(start.tailOffset, "start.tailOffset"),
      };
    default:
      throw new TsfClientError("invalid_read_parameter", "start has an unknown selector type");
  }
}

export function readExhausted(options: NormalizedReadOptions): boolean {
  return (
    options.stop?.count === 0n ||
    (options.start.type === "timestampMs" &&
      options.stop?.untilTimestampMs !== undefined &&
      options.start.timestampMs >= options.stop.untilTimestampMs)
  );
}

export function readRequestForConnection(
  options: NormalizedReadOptions,
): ReadRequest {
  return {
    start: options.start,
    ...(options.stop === undefined ? {} : { stop: options.stop }),
    ...(options.rate === undefined ? {} : { rate: options.rate }),
  };
}

export function advanceReadOptions(
  options: NormalizedReadOptions,
  seqNum: bigint,
): boolean {
  if (seqNum === MAX_U64) {
    return true;
  }
  options.start = { type: "seqNum", seqNum: seqNum + 1n };
  if (options.stop?.count !== undefined) {
    options.stop.count -= 1n;
  }
  return readExhausted(options);
}

function validateReadBatchForRequest(
  records: readonly ReadRecord[],
  options: NormalizedReadOptions,
): void {
  const first = records[0];
  if (first === undefined) {
    throw invalidServerRead("ReadBatch is empty");
  }
  const start = options.start;
  if (
    (start.type === "seqNum" && first.seqNum !== start.seqNum) ||
    (start.type === "timestampMs" && first.timestampMs < start.timestampMs)
  ) {
    throw invalidServerRead("ReadBatch does not begin at the requested position");
  }
  if (
    options.stop?.count !== undefined &&
    BigInt(records.length) > options.stop.count
  ) {
    throw invalidServerRead("ReadBatch exceeds the remaining record count");
  }
  const untilTimestampMs = options.stop?.untilTimestampMs;
  if (untilTimestampMs !== undefined && records.some(
    (record) => record.timestampMs >= untilTimestampMs,
  )) {
    throw invalidServerRead("ReadBatch reaches the exclusive until timestamp");
  }
}

function validateCaughtUpForRequest(
  nextSeqNum: bigint,
  options: NormalizedReadOptions,
): void {
  if (options.start.type === "seqNum" && nextSeqNum !== options.start.seqNum) {
    throw invalidServerRead("CaughtUp does not match the next requested sequence");
  }
}

function invalidServerRead(message: string): TsfClientError {
  return new TsfClientError("invalid_server_read", message);
}
