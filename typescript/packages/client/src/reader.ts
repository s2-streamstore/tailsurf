import {
  DEFAULT_READ_TAIL_OFFSET,
  MAX_U64,
  encodeReadQuery,
  parseStreamId,
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
  unexpectedFrame,
  WEBSOCKET_READ_IDLE_TIMEOUT_MS,
} from "./socket.js";
import { INITIAL_RETRY_BACKOFF_MS, withTimeout } from "./retry.js";

export interface ReadOptions {
  readonly streamId: StreamId;
  /** Cancels connection establishment. Close the returned session to stop reading. */
  readonly signal?: AbortSignal | undefined;
  readonly start?: ReadStart | undefined;
  readonly stop?: ReadStop | undefined;
  readonly rate?: number | undefined;
  readonly linkSecret?: string | undefined;
  readonly onCaughtUp?: ((caughtUp: CaughtUpPosition) => void) | undefined;
  readonly onStreamMetadata?: ((stream: StreamMetadata) => void) | undefined;
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
  stop?: NormalizedReadStop | undefined;
  readonly rate?: number | undefined;
  readonly linkSecret?: string | undefined;
  readonly onCaughtUp?: ((caughtUp: CaughtUpPosition) => void) | undefined;
  readonly onStreamMetadata?: ((stream: StreamMetadata) => void) | undefined;
  streamMetadata?: StreamMetadata | undefined;
  lastCaughtUp?: CaughtUpPosition | undefined;
}

interface NormalizedReadStop {
  count?: bigint | undefined;
  readonly untilTimestampMs?: bigint | undefined;
  readonly waitSeconds?: number | undefined;
}

export function normalizeReadOptions(
  options: ReadOptions,
): NormalizedReadOptions {
  const start = options.start ?? {
    type: "tailOffset",
    tailOffset: DEFAULT_READ_TAIL_OFFSET,
  };
  const requestedStop = options.stop;
  const stop = requestedStop === undefined ||
      (requestedStop.count === undefined &&
        requestedStop.untilTimestampMs === undefined &&
        requestedStop.waitSeconds === undefined)
    ? undefined
    : { ...requestedStop };
  const rate = options.rate;
  try {
    encodeReadQuery({ start, stop, rate });
  } catch (cause) {
    throw new TsfClientError(
      "invalid_read_parameter",
      cause instanceof Error ? cause.message : "invalid read parameters",
      { cause },
    );
  }
  return {
    streamId: parseStreamId(options.streamId),
    start,
    stop,
    rate,
    linkSecret: options.linkSecret === undefined
      ? undefined
      : requireLinkSecret(options.linkSecret),
    onCaughtUp: options.onCaughtUp,
    onStreamMetadata: options.onStreamMetadata,
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
    if (record.seqNum === MAX_U64) {
      this.finished = true;
    } else {
      this.options.start = { type: "seqNum", seqNum: record.seqNum + 1n };
      if (this.options.stop?.count !== undefined) {
        this.options.stop.count -= 1n;
      }
      this.finished = readExhausted(this.options);
    }
    return record;
  }

  protected recordCaughtUp(caughtUp: CaughtUpPosition): void {
    this.options.lastCaughtUp = caughtUp;
    this.options.start = { type: "seqNum", seqNum: caughtUp.nextSeqNum };
    this.notify(this.options.onCaughtUp, caughtUp);
  }

  protected recordStreamMetadata(stream: StreamMetadata): void {
    try {
      validateReadStreamMetadata(this.options, stream);
    } catch (error) {
      this.close();
      throw error;
    }
    this.options.streamMetadata = stream;
    this.notify(this.options.onStreamMetadata, stream);
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
        this.recordCaughtUp(caughtUp);
        continue;
      }
      if (frame.type === "streamMetadata") {
        this.recordStreamMetadata(streamMetadataFromWire(frame.stream));
        continue;
      }
      throw unexpectedFrame(frame);
    }
    this.finished = true;
    this.#socket.close();
    return undefined;
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

export function validateReadStreamMetadata(
  options: NormalizedReadOptions,
  stream: StreamMetadata,
): void {
  if (stream.streamId !== options.streamId) {
    throw new TsfClientError(
      "invalid_api_response",
      "reader handshake returned a different stream ID",
    );
  }
  if (
    options.streamMetadata !== undefined &&
    stream.kind !== options.streamMetadata.kind
  ) {
    throw new TsfClientError(
      "invalid_api_response",
      "stream kind changed while reconnecting the reader",
    );
  }
}

export function validateReadBatchForRequest(
  records: readonly ReadRecord[],
  options: NormalizedReadOptions,
  invalid: (message: string) => TsfClientError = invalidServerRead,
): void {
  const first = records[0];
  if (first === undefined) {
    throw invalid("ReadBatch is empty");
  }
  const start = options.start;
  if (
    (start.type === "seqNum" && first.seqNum !== start.seqNum) ||
    (start.type === "timestampMs" && first.timestampMs < start.timestampMs)
  ) {
    throw invalid("ReadBatch does not begin at the requested position");
  }
  if (
    options.stop?.count !== undefined &&
    BigInt(records.length) > options.stop.count
  ) {
    throw invalid("ReadBatch exceeds the remaining record count");
  }
  const untilTimestampMs = options.stop?.untilTimestampMs;
  if (untilTimestampMs !== undefined && records.some(
    (record) => record.timestampMs >= untilTimestampMs,
  )) {
    throw invalid("ReadBatch reaches the exclusive until timestamp");
  }
}

export function validateCaughtUpForRequest(
  nextSeqNum: bigint,
  options: NormalizedReadOptions,
  invalid: (message: string) => TsfClientError = invalidServerRead,
): void {
  if (options.start.type === "seqNum" && nextSeqNum !== options.start.seqNum) {
    throw invalid("CaughtUp does not match the next requested sequence");
  }
}

function invalidServerRead(message: string): TsfClientError {
  return new TsfClientError("invalid_server_read", message);
}
