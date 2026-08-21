import {
  MAX_APPEND_FRAME_RECORDS,
  MAX_FRAME_PAYLOAD_BYTES,
  MAX_RECORD_PAYLOAD_BYTES,
  MAX_U64,
  partHeader,
  RecordFormat,
  UNSPLIT_PART,
  type PartHeader,
} from "@s2-dev/tailsurf-protocol";

import { TsfClientError, TsfWebSocketClosedError } from "./errors.js";
import {
  type FrameSocket,
  isRetryableSocketError,
  reconnectSocket,
  type SocketPolicy,
  unexpectedFrame,
  withTimeout,
} from "./socket.js";

const textEncoder = new TextEncoder();

export interface AppendInput {
  readonly data: string | Uint8Array;
  readonly format?: RecordFormat;
  readonly part?: PartHeader;
}

export interface LogicalAppendInput {
  readonly data: string | Uint8Array;
  readonly format?: RecordFormat;
}

export interface AppendReceipt {
  readonly writerSeqNum: bigint;
  readonly seqNum: bigint;
}

export interface TsfWriter {
  append(input: AppendInput): Promise<AppendReceipt>;
  appendBatch(inputs: readonly AppendInput[]): Promise<readonly AppendReceipt[]>;
  appendLogical(input: LogicalAppendInput): Promise<readonly AppendReceipt[]>;
  /** Immediately stops recovery. Accepted records may already be durable. */
  abort(): void;
  close(): Promise<void>;
}

/** Maximum physical records that one writer socket may keep in flight. */
export const MAX_WRITER_IN_FLIGHT_RECORDS = 128;
/** Maximum accounted bytes that one writer socket may keep in flight. Empty payloads count as one byte. */
export const MAX_WRITER_IN_FLIGHT_BYTES = 5 * 1024 * 1024;
/** Default maximum physical records retained until durability acknowledgement. */
export const DEFAULT_MAX_WRITER_RETAINED_RECORDS = MAX_WRITER_IN_FLIGHT_RECORDS;
/** Default maximum accounted bytes retained until durability acknowledgement. */
export const DEFAULT_MAX_WRITER_RETAINED_BYTES = MAX_WRITER_IN_FLIGHT_BYTES;
/** Maximum physical records accepted as one writer submission. */
export const MAX_APPEND_SUBMISSION_RECORDS = MAX_APPEND_FRAME_RECORDS;
/** Structural payload maximum for one writer submission. */
export const MAX_APPEND_SUBMISSION_PAYLOAD_BYTES =
  MAX_APPEND_SUBMISSION_RECORDS * MAX_RECORD_PAYLOAD_BYTES;

export interface TsfWriterConfig {
  /** Maximum physical records retained until durability acknowledgement. */
  readonly maxRetainedRecords?: number;
  /** Maximum accounted bytes retained until durability acknowledgement. Empty payloads count as one byte. */
  readonly maxRetainedBytes?: number;
}

export type NormalizedTsfWriterConfig = Required<TsfWriterConfig>;

export function normalizeWriterConfig(
  config: TsfWriterConfig = {},
): NormalizedTsfWriterConfig {
  return {
    maxRetainedRecords: positiveSafeInteger(
      config.maxRetainedRecords ?? DEFAULT_MAX_WRITER_RETAINED_RECORDS,
      "maxRetainedRecords",
    ),
    maxRetainedBytes: positiveSafeInteger(
      config.maxRetainedBytes ?? DEFAULT_MAX_WRITER_RETAINED_BYTES,
      "maxRetainedBytes",
    ),
  };
}

export class DefaultTsfWriter implements TsfWriter {
  #socket: FrameSocket;
  #nextWriterSeqNum = 0n;
  readonly #queue: PendingAppendCall[] = [];
  #drain: Promise<void> | undefined;
  #retainedRecords = 0;
  #retainedBytes = 0;
  #inFlightRecords = 0;
  #inFlightBytes = 0;
  #ambiguousWriterEndSeqNum: bigint | undefined;
  #closing = false;
  #closed = false;
  #terminalError: TsfClientError | undefined;
  readonly #controller = new AbortController();

  public constructor(
    socket: FrameSocket,
    private readonly connect: () => Promise<FrameSocket>,
    private readonly policy: SocketPolicy,
    private readonly config: NormalizedTsfWriterConfig,
  ) {
    this.#socket = socket;
  }

  public append(input: AppendInput): Promise<AppendReceipt> {
    return this.appendBatch([input]).then((receipts) => {
      const receipt = receipts[0];
      if (receipt === undefined) {
        throw new TsfClientError("invalid_append_batch", "append batch was empty");
      }
      return receipt;
    });
  }

  public appendBatch(
    inputs: readonly AppendInput[],
  ): Promise<readonly AppendReceipt[]> {
    const unavailable = this.#submissionError();
    if (unavailable !== undefined) {
      return Promise.reject(unavailable);
    }
    if (inputs.length === 0 || inputs.length > MAX_APPEND_SUBMISSION_RECORDS) {
      return Promise.reject(
        new TsfClientError(
          "invalid_append_batch",
          `append batch must contain 1 to ${MAX_APPEND_SUBMISSION_RECORDS} records`,
        ),
      );
    }

    let records: PendingAppend[];
    try {
      records = inputs.map(prepareAppend);
    } catch (error) {
      return Promise.reject(asAppendInputError(error));
    }
    const retainedBytes = records.reduce(
      (total, record) => total + record.retainedBytes,
      0,
    );
    if (
      records.length > this.config.maxRetainedRecords - this.#retainedRecords ||
      retainedBytes > this.config.maxRetainedBytes - this.#retainedBytes
    ) {
      return Promise.reject(writerAdmissionError(
        records.length,
        retainedBytes,
        this.config,
      ));
    }

    const writerEndSeqNum = this.#nextWriterSeqNum + BigInt(records.length);
    if (writerEndSeqNum > MAX_U64) {
      return Promise.reject(new TsfClientError(
        "writer_sequence_exhausted",
        "append batch exceeds the writer sequence range",
      ));
    }
    const writerStartSeqNum = this.#nextWriterSeqNum;
    this.#nextWriterSeqNum = writerEndSeqNum;
    this.#retainedRecords += records.length;
    this.#retainedBytes += retainedBytes;

    const result = new Promise<readonly AppendReceipt[]>((resolve, reject) => {
      this.#queue.push({
        writerStartSeqNum,
        records,
        retainedBytes,
        acknowledged: 0,
        sent: 0,
        receipts: [],
        resolve,
        reject,
      });
    });
    this.#scheduleDrain();
    return result;
  }

  public appendLogical(
    input: LogicalAppendInput,
  ): Promise<readonly AppendReceipt[]> {
    const unavailable = this.#submissionError();
    if (unavailable !== undefined) {
      return Promise.reject(unavailable);
    }
    const maximumBytes = MAX_APPEND_SUBMISSION_PAYLOAD_BYTES;
    if (
      (typeof input.data === "string" && input.data.length > maximumBytes) ||
      (typeof input.data !== "string" && input.data.byteLength > maximumBytes)
    ) {
      return Promise.reject(logicalRecordTooLarge());
    }
    let data: Uint8Array;
    try {
      data = typeof input.data === "string"
        ? textEncoder.encode(input.data)
        : new Uint8Array(input.data);
    } catch (error) {
      return Promise.reject(asAppendInputError(error));
    }
    const partCount = Math.max(1, Math.ceil(data.byteLength / MAX_RECORD_PAYLOAD_BYTES));
    if (partCount > MAX_APPEND_SUBMISSION_RECORDS) {
      return Promise.reject(logicalRecordTooLarge());
    }
    const format = input.format ??
      (typeof input.data === "string" ? RecordFormat.Transcript : RecordFormat.Bytes);
    if (partCount === 1) {
      return this.appendBatch([{ data, format }]);
    }
    return this.appendBatch(Array.from({ length: partCount }, (_, index) => ({
      data: data.subarray(
        index * MAX_RECORD_PAYLOAD_BYTES,
        Math.min((index + 1) * MAX_RECORD_PAYLOAD_BYTES, data.byteLength),
      ),
      format,
      part: partHeader(index, index + 1 === partCount),
    })));
  }

  public async close(): Promise<void> {
    if (this.#closed) {
      if (this.#terminalError !== undefined) {
        throw this.#terminalError;
      }
      return;
    }
    this.#closing = true;
    while (this.#drain !== undefined) {
      await this.#drain;
    }
    this.#closed = true;
    this.#socket.close();
    if (this.#terminalError !== undefined) {
      throw this.#terminalError;
    }
  }

  public abort(): void {
    if (this.#closed) {
      return;
    }
    const cause = new TsfClientError(
      "writer_aborted",
      "TSF writer recovery was aborted",
    );
    const durabilityAmbiguous =
      this.#ambiguousWriterEndSeqNum !== undefined || this.#inFlightRecords > 0;
    const terminal = durabilityAmbiguous
      ? this.#terminateAmbiguous(cause)
      : this.#terminate(cause);
    this.#finish(terminal);
  }

  #scheduleDrain(): void {
    if (this.#drain !== undefined) {
      return;
    }
    const run = Promise.resolve().then(() => this.#drainQueue());
    this.#drain = run.finally(() => {
      this.#drain = undefined;
      if (this.#queue.length > 0 && this.#terminalError === undefined) {
        this.#scheduleDrain();
      }
    });
  }

  async #drainQueue(): Promise<void> {
    let reconnectAttempts = 0;
    let reconnectDelay = this.policy.initialBackoffMs;
    while (this.#queue.length > 0) {
      try {
        this.#fillSocketWindow();
        if (this.#inFlightRecords === 0) {
          throw new TsfClientError(
            "invalid_writer_state",
            "writer could not send a retained record",
          );
        }
        const response = await withTimeout(
          this.#socket.nextFrame(),
          this.policy.webSocketOperationTimeoutMs,
          "append acknowledgement",
        );
        if (response.type !== "appendAck") {
          throw unexpectedFrame(response);
        }
        this.#dispatchAck(response);
        reconnectAttempts = 0;
        reconnectDelay = this.policy.initialBackoffMs;
      } catch (error) {
        const sentRange = this.#sentRange();
        if (
          sentRange !== undefined &&
          (this.#ambiguousWriterEndSeqNum === undefined ||
            sentRange.end > this.#ambiguousWriterEndSeqNum)
        ) {
          this.#ambiguousWriterEndSeqNum = sentRange.end;
        }
        const durabilityAmbiguous = this.#ambiguousWriterEndSeqNum !== undefined;
        if (isSequenceMismatch(error)) {
          this.#finish(this.#terminateSequenceMismatch(error));
          return;
        }
        if (!isRetryableSocketError(error)) {
          this.#finish(
            durabilityAmbiguous
              ? this.#terminateAmbiguous(error)
              : this.#terminate(error),
          );
          return;
        }

        this.#socket.close();
        this.#resetSentToAcknowledged();
        let recovered: Awaited<ReturnType<typeof reconnectSocket>>;
        try {
          recovered = await reconnectSocket(
            this.connect,
            this.policy,
            reconnectAttempts,
            reconnectDelay,
            error,
            this.#controller.signal,
            Number.POSITIVE_INFINITY,
          );
        } catch (reconnectError) {
          const terminal = isSequenceMismatch(reconnectError)
            ? this.#terminateSequenceMismatch(reconnectError)
            : durabilityAmbiguous
            ? this.#terminateAmbiguous(reconnectError)
            : this.#terminate(reconnectError);
          this.#finish(terminal);
          return;
        }
        this.#socket = recovered.socket;
        reconnectAttempts = recovered.attempts;
        reconnectDelay = recovered.nextDelayMs;
      }
    }
  }

  #fillSocketWindow(): void {
    while (this.#sendNextFrame()) {
      // Fill the fixed socket window before waiting for acknowledgement progress.
    }
  }

  #sendNextFrame(): boolean {
    const selected: SelectedAppend[] = [];
    let payloadBytes = 0;
    let chargedBytes = 0;
    const availableRecords = MAX_WRITER_IN_FLIGHT_RECORDS - this.#inFlightRecords;
    const availableBytes =
      MAX_WRITER_IN_FLIGHT_BYTES - this.#inFlightBytes;
    if (availableRecords === 0 || availableBytes === 0) {
      return false;
    }
    outer: for (const call of this.#queue) {
      for (let index = call.sent; index < call.records.length; index += 1) {
        const record = requiredRecord(call, index);
        if (
          selected.length >= Math.min(MAX_APPEND_FRAME_RECORDS, availableRecords) ||
          record.retainedBytes > availableBytes - chargedBytes ||
          (selected.length > 0 &&
            record.data.byteLength > MAX_FRAME_PAYLOAD_BYTES - payloadBytes)
        ) {
          break outer;
        }
        selected.push({ call, index, record });
        payloadBytes += record.data.byteLength;
        chargedBytes += record.retainedBytes;
      }
    }
    if (selected.length === 0) {
      return false;
    }

    this.#socket.send({
      type: "appendBatch",
      records: selected.map(({ call, index, record }) => ({
        writerSeqNum: call.writerStartSeqNum + BigInt(index),
        part: record.part,
        format: record.format,
        data: record.data,
      })),
    });
    for (const { call } of selected) {
      call.sent += 1;
    }
    this.#inFlightRecords += selected.length;
    this.#inFlightBytes += chargedBytes;
    return true;
  }

  #dispatchAck(response: AppendAckFrame): void {
    const sentRange = this.#sentRange();
    const writerCount = response.writerEndSeqNum - response.writerStartSeqNum;
    if (
      sentRange === undefined ||
      response.writerStartSeqNum !== sentRange.start ||
      response.writerEndSeqNum > sentRange.end ||
      writerCount <= 0n ||
      response.endSeqNum - response.startSeqNum !== writerCount
    ) {
      throw new TsfClientError(
        "invalid_append_ack",
        "append acknowledgement does not match the sent writer range",
      );
    }

    let remaining = Number(writerCount);
    let streamSeqNum = response.startSeqNum;
    while (remaining > 0) {
      const call = this.#queue[0];
      if (call === undefined || call.acknowledged >= call.sent) {
        throw new TsfClientError(
          "invalid_append_ack",
          "append acknowledgement exceeds the sent writer range",
        );
      }
      const acknowledged = Math.min(remaining, call.sent - call.acknowledged);
      for (let offset = 0; offset < acknowledged; offset += 1) {
        const record = requiredRecord(call, call.acknowledged + offset);
        call.receipts.push({
          writerSeqNum: call.writerStartSeqNum + BigInt(call.acknowledged + offset),
          seqNum: streamSeqNum + BigInt(offset),
        });
        this.#inFlightBytes -= record.retainedBytes;
      }
      this.#inFlightRecords -= acknowledged;
      call.acknowledged += acknowledged;
      remaining -= acknowledged;
      streamSeqNum += BigInt(acknowledged);
      if (call.acknowledged === call.records.length) {
        this.#queue.shift();
        this.#releaseCall(call);
        call.resolve(call.receipts);
      }
    }
    const firstPending = this.#queue[0];
    if (
      firstPending === undefined ||
      (this.#ambiguousWriterEndSeqNum !== undefined &&
        firstPending.writerStartSeqNum + BigInt(firstPending.acknowledged) >=
          this.#ambiguousWriterEndSeqNum)
    ) {
      this.#ambiguousWriterEndSeqNum = undefined;
    }
  }

  #sentRange(): { readonly start: bigint; readonly end: bigint } | undefined {
    let start: bigint | undefined;
    let end: bigint | undefined;
    for (const call of this.#queue) {
      if (call.acknowledged < call.sent) {
        start ??= call.writerStartSeqNum + BigInt(call.acknowledged);
        end = call.writerStartSeqNum + BigInt(call.sent);
      } else if (start !== undefined) {
        break;
      }
    }
    return start === undefined || end === undefined ? undefined : { start, end };
  }

  #resetSentToAcknowledged(): void {
    for (const call of this.#queue) {
      call.sent = call.acknowledged;
    }
    this.#inFlightRecords = 0;
    this.#inFlightBytes = 0;
  }

  #finish(error: TsfClientError): void {
    for (const call of this.#queue.splice(0)) {
      this.#releaseCall(call);
      call.reject(error);
    }
  }

  #releaseCall(call: PendingAppendCall): void {
    this.#retainedRecords -= call.records.length;
    this.#retainedBytes -= call.retainedBytes;
  }

  #submissionError(): TsfClientError | undefined {
    const unavailable = this.#terminalOrClosedError();
    if (unavailable !== undefined) {
      return unavailable;
    }
    return this.#closing
      ? new TsfClientError("writer_closed", "TSF writer is closing")
      : undefined;
  }

  #terminalOrClosedError(): TsfClientError | undefined {
    if (this.#terminalError !== undefined) {
      return this.#terminalError;
    }
    return this.#closed
      ? new TsfClientError("writer_closed", "TSF writer is closed")
      : undefined;
  }

  #terminate(error: unknown): TsfClientError {
    const terminal = error instanceof TsfClientError
      ? error
      : new TsfClientError("writer_failed", "TSF writer failed", {
        cause: error,
      });
    return this.#setTerminal(terminal);
  }

  #terminateAmbiguous(cause: unknown): TsfClientError {
    return this.#setTerminal(
      new TsfClientError(
        "writer_durability_unknown",
        "append durability is unknown; this writer cannot safely continue",
        { cause },
      ),
    );
  }

  #terminateSequenceMismatch(cause: unknown): TsfClientError {
    return this.#setTerminal(
      new TsfClientError(
        "sequence_mismatch",
        "stream next sequence did not match the writer session precondition",
        { cause },
      ),
    );
  }

  #setTerminal(error: TsfClientError): TsfClientError {
    this.#terminalError ??= error;
    this.#closed = true;
    this.#controller.abort(this.#terminalError);
    this.#socket.close();
    return this.#terminalError;
  }
}

interface PendingAppend {
  readonly data: Uint8Array;
  readonly format: RecordFormat;
  readonly part: PartHeader;
  readonly retainedBytes: number;
}

interface PendingAppendCall {
  readonly writerStartSeqNum: bigint;
  readonly records: readonly PendingAppend[];
  readonly retainedBytes: number;
  acknowledged: number;
  sent: number;
  readonly receipts: AppendReceipt[];
  readonly resolve: (receipts: readonly AppendReceipt[]) => void;
  readonly reject: (reason: unknown) => void;
}

interface SelectedAppend {
  readonly call: PendingAppendCall;
  readonly index: number;
  readonly record: PendingAppend;
}

interface AppendAckFrame {
  readonly writerStartSeqNum: bigint;
  readonly writerEndSeqNum: bigint;
  readonly startSeqNum: bigint;
  readonly endSeqNum: bigint;
}

function prepareAppend(input: AppendInput): PendingAppend {
  if (
    (typeof input.data === "string" && input.data.length > MAX_RECORD_PAYLOAD_BYTES) ||
    (typeof input.data !== "string" && input.data.byteLength > MAX_RECORD_PAYLOAD_BYTES)
  ) {
    throw recordTooLarge();
  }
  const data = typeof input.data === "string"
    ? textEncoder.encode(input.data)
    : new Uint8Array(input.data);
  if (data.byteLength > MAX_RECORD_PAYLOAD_BYTES) {
    throw recordTooLarge();
  }
  return {
    data,
    format: input.format ??
      (typeof input.data === "string" ? RecordFormat.Transcript : RecordFormat.Bytes),
    part: input.part === undefined
      ? UNSPLIT_PART
      : partHeader(input.part.index, input.part.isFinal),
    retainedBytes: Math.max(data.byteLength, 1),
  };
}

function requiredRecord(call: PendingAppendCall, index: number): PendingAppend {
  const record = call.records[index];
  if (record === undefined) {
    throw new TsfClientError("invalid_writer_state", "writer record is missing");
  }
  return record;
}

function writerAdmissionError(
  records: number,
  bytes: number,
  config: NormalizedTsfWriterConfig,
): TsfClientError {
  return new TsfClientError(
    "client_write_overload",
    `append requires ${records} records and ${bytes} bytes; retained backlog is limited to ${config.maxRetainedRecords} records and ${config.maxRetainedBytes} bytes`,
  );
}

function positiveSafeInteger(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new TsfClientError(
      "invalid_writer_config",
      `${name} must be a positive safe integer`,
    );
  }
  return value;
}

function logicalRecordTooLarge(): TsfClientError {
  return new TsfClientError(
    "logical_record_too_large",
    `logical record exceeds the ${MAX_APPEND_SUBMISSION_PAYLOAD_BYTES}-byte maximum`,
  );
}

function recordTooLarge(): TsfClientError {
  return new TsfClientError(
    "record_too_large",
    `record exceeds the ${MAX_RECORD_PAYLOAD_BYTES}-byte maximum`,
  );
}

function asAppendInputError(error: unknown): Error {
  return error instanceof Error
    ? error
    : new TsfClientError(
        "invalid_append_input",
        "could not prepare append input",
        { cause: error },
      );
}

function isSequenceMismatch(error: unknown): boolean {
  return error instanceof TsfWebSocketClosedError &&
    error.closeCode === 1008 &&
    error.reason === "sequence_mismatch";
}
