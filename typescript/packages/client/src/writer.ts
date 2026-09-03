import {
  MAX_APPEND_FRAME_RECORDS,
  MAX_FRAME_PAYLOAD_BYTES,
  MAX_RECORD_PAYLOAD_BYTES,
  MAX_WRITER_IN_FLIGHT_PAYLOAD_BYTES,
  MAX_WRITER_IN_FLIGHT_RECORDS,
  MAX_U64,
  partHeader,
  UNSPLIT_PART,
  type PartHeader,
  type ServerFrame,
  type StreamKind,
} from "@tailsurf/protocol";

import { TsfClientError, TsfWebSocketClosedError } from "./errors.js";
import { INITIAL_RETRY_BACKOFF_MS } from "./retry.js";
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
  readonly part?: PartHeader;
}

export interface LogicalAppendInput {
  readonly data: string | Uint8Array;
}

export interface AppendReceipt {
  readonly writerSeqNum: bigint;
  readonly seqNum: bigint;
}

export interface TsfWriter {
  readonly streamKind: StreamKind;
  append(input: AppendInput): Promise<AppendReceipt>;
  appendBatch(inputs: readonly AppendInput[]): Promise<readonly AppendReceipt[]>;
  appendLogical(input: LogicalAppendInput): Promise<readonly AppendReceipt[]>;
  /** Immediately stops recovery. Accepted records may already be durable. */
  abort(): void;
  close(): Promise<void>;
}

export class DefaultTsfWriter implements TsfWriter {
  #socket: FrameSocket;
  #nextWriterSeqNum = 0n;
  readonly #queue: PendingAppendCall[] = [];
  #drain: Promise<void> | undefined;
  #inFlightRecords = 0;
  #inFlightPayloadBytes = 0;
  #ambiguousWriterEndSeqNum: bigint | undefined;
  #closing = false;
  #closed = false;
  #terminalError: TsfClientError | undefined;
  readonly #controller = new AbortController();

  public constructor(
    socket: FrameSocket,
    private readonly connect: () => Promise<FrameSocket>,
    private readonly policy: SocketPolicy,
    public readonly streamKind: StreamKind,
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
    if (inputs.length === 0) {
      return Promise.reject(
        new TsfClientError(
          "invalid_append_batch",
          "append batch must not be empty",
        ),
      );
    }

    let records: PendingAppend[];
    try {
      records = inputs.map(prepareAppend);
    } catch (error) {
      return Promise.reject(asAppendInputError(error));
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

    const result = new Promise<readonly AppendReceipt[]>((resolve, reject) => {
      this.#queue.push({
        writerStartSeqNum,
        records,
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
    let data: Uint8Array;
    try {
      data = typeof input.data === "string"
        ? textEncoder.encode(input.data)
        : new Uint8Array(input.data);
    } catch (error) {
      return Promise.reject(asAppendInputError(error));
    }
    const partCount = Math.max(1, Math.ceil(data.byteLength / MAX_RECORD_PAYLOAD_BYTES));
    if (partCount === 1) {
      return this.appendBatch([{ data }]);
    }
    return this.appendBatch(Array.from({ length: partCount }, (_, index) => ({
      data: data.subarray(
        index * MAX_RECORD_PAYLOAD_BYTES,
        Math.min((index + 1) * MAX_RECORD_PAYLOAD_BYTES, data.byteLength),
      ),
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
    let reconnectDelay = INITIAL_RETRY_BACKOFF_MS;
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
          this.policy.webSocketProgressTimeoutMs,
          "append acknowledgement",
        );
        if (response.type !== "appendAck") {
          throw unexpectedFrame(response);
        }
        this.#dispatchAck(response);
        reconnectAttempts = 0;
        reconnectDelay = INITIAL_RETRY_BACKOFF_MS;
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
    const availableRecords = MAX_WRITER_IN_FLIGHT_RECORDS - this.#inFlightRecords;
    const availablePayloadBytes =
      MAX_WRITER_IN_FLIGHT_PAYLOAD_BYTES - this.#inFlightPayloadBytes;
    if (availableRecords === 0) {
      return false;
    }
    outer: for (const call of this.#queue) {
      for (let index = call.sent; index < call.records.length; index += 1) {
        const record = requiredRecord(call, index);
        if (
          selected.length >= Math.min(MAX_APPEND_FRAME_RECORDS, availableRecords) ||
          record.data.byteLength > availablePayloadBytes - payloadBytes ||
          (selected.length > 0 &&
            record.data.byteLength > MAX_FRAME_PAYLOAD_BYTES - payloadBytes)
        ) {
          break outer;
        }
        selected.push({ call, index, record });
        payloadBytes += record.data.byteLength;
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
        data: record.data,
      })),
    });
    for (const { call } of selected) {
      call.sent += 1;
    }
    this.#inFlightRecords += selected.length;
    this.#inFlightPayloadBytes += payloadBytes;
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
        this.#inFlightPayloadBytes -= record.data.byteLength;
      }
      this.#inFlightRecords -= acknowledged;
      call.acknowledged += acknowledged;
      remaining -= acknowledged;
      streamSeqNum += BigInt(acknowledged);
      if (call.acknowledged === call.records.length) {
        this.#queue.shift();
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
    this.#inFlightPayloadBytes = 0;
  }

  #finish(error: TsfClientError): void {
    for (const call of this.#queue.splice(0)) {
      call.reject(error);
    }
  }

  #submissionError(): TsfClientError | undefined {
    if (this.#terminalError !== undefined) {
      return this.#terminalError;
    }
    if (this.#closed) {
      return new TsfClientError("writer_closed", "TSF writer is closed");
    }
    return this.#closing
      ? new TsfClientError("writer_closed", "TSF writer is closing")
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
  readonly part: PartHeader;
}

interface PendingAppendCall {
  readonly writerStartSeqNum: bigint;
  readonly records: readonly PendingAppend[];
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

type AppendAckFrame = Extract<ServerFrame, { readonly type: "appendAck" }>;

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
    part: input.part === undefined
      ? UNSPLIT_PART
      : partHeader(input.part.index, input.part.isFinal),
  };
}

function requiredRecord(call: PendingAppendCall, index: number): PendingAppend {
  const record = call.records[index];
  if (record === undefined) {
    throw new TsfClientError("invalid_writer_state", "writer record is missing");
  }
  return record;
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
