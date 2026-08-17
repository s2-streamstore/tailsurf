import {
  MAX_BATCH_PAYLOAD_BYTES,
  MAX_APPEND_BATCH_RECORDS,
  MAX_RECORD_BYTES,
  RecordFormat,
  UNSPLIT_PART,
  type PartHeader,
} from "@tailsurf/protocol";

import { TsfClientError, TsfWebSocketClosedError } from "./errors.js";
import { MAX_U64 } from "@tailsurf/protocol";

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

export interface AppendReceipt {
  readonly writerSeqNum: bigint;
  readonly seqNum: bigint;
}

export interface TsfWriter {
  append(input: AppendInput): Promise<AppendReceipt>;
  appendBatch(inputs: readonly AppendInput[]): Promise<readonly AppendReceipt[]>;
  close(): Promise<void>;
}

/** Maximum append calls retained by one writer until they settle. */
export const MAX_PENDING_WRITER_RECORDS = 128;
/** Maximum append payload retained by one writer until calls settle. */
export const MAX_PENDING_WRITER_PAYLOAD_BYTES = 5 * 1024 * 1024;

export class DefaultTsfWriter implements TsfWriter {
  #socket: FrameSocket;
  #nextWriterSeqNum: bigint;
  readonly #queue: PendingAppendCall[] = [];
  #drain: Promise<void> | undefined;
  #pendingRecords = 0;
  #pendingPayloadBytes = 0;
  #closing = false;
  #closed = false;
  #terminalError: TsfClientError | undefined;

  public constructor(
    writerStartSeqNum: bigint,
    socket: FrameSocket,
    private readonly connect: () => Promise<FrameSocket>,
    private readonly policy: SocketPolicy,
  ) {
    this.#nextWriterSeqNum = writerStartSeqNum;
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

    if (inputs.length === 0 || inputs.length > MAX_APPEND_BATCH_RECORDS) {
      return Promise.reject(
        new TsfClientError(
          "invalid_append_batch",
          `append batch must contain 1 to ${MAX_APPEND_BATCH_RECORDS} records`,
        ),
      );
    }
    if (inputs.length > MAX_PENDING_WRITER_RECORDS - this.#pendingRecords) {
      return Promise.reject(writerAdmissionError("record"));
    }
    let availablePayloadBytes = Math.min(
      MAX_BATCH_PAYLOAD_BYTES,
      MAX_PENDING_WRITER_PAYLOAD_BYTES - this.#pendingPayloadBytes,
    );
    const pending: PendingAppend[] = [];
    try {
      for (const input of inputs) {
        const record = prepareAppend(input, availablePayloadBytes);
        pending.push(record);
        availablePayloadBytes -= record.reservedPayloadBytes;
      }
    } catch (error) {
      return Promise.reject(
        error instanceof Error
          ? error
          : new TsfClientError(
              "invalid_append_input",
              "could not prepare append input",
              { cause: error },
            ),
      );
    }
    const reservedPayloadBytes = pending.reduce(
      (total, record) => total + record.reservedPayloadBytes,
      0,
    );
    this.#pendingRecords += pending.length;
    this.#pendingPayloadBytes += reservedPayloadBytes;

    const result = new Promise<readonly AppendReceipt[]>((resolve, reject) => {
      this.#queue.push({
        records: pending,
        reservedPayloadBytes,
        resolve,
        reject,
      });
    });
    this.#scheduleDrain();
    return result;
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

  #scheduleDrain(): void {
    if (this.#drain !== undefined) {
      return;
    }
    const run = Promise.resolve().then(() => this.#drainQueue());
    this.#drain = run.finally(() => {
      this.#drain = undefined;
      if (this.#queue.length > 0) {
        this.#scheduleDrain();
      }
    });
  }

  async #drainQueue(): Promise<void> {
    while (this.#queue.length > 0) {
      const calls = this.#takeQueuedBatch();
      const unavailable = this.#terminalOrClosedError();
      if (unavailable !== undefined) {
        this.#rejectCalls(calls, unavailable);
        continue;
      }
      const records = calls.flatMap((call) => call.records);
      try {
        const receipts = await this.#appendBatch(records);
        this.#resolveCalls(calls, receipts);
      } catch (error) {
        this.#rejectCalls(calls, error);
      }
    }
  }

  #takeQueuedBatch(): PendingAppendCall[] {
    const calls: PendingAppendCall[] = [];
    let recordCount = 0;
    let payloadBytes = 0;
    for (const call of this.#queue) {
      if (
        recordCount + call.records.length > MAX_APPEND_BATCH_RECORDS ||
        payloadBytes + call.reservedPayloadBytes > MAX_BATCH_PAYLOAD_BYTES
      ) {
        break;
      }
      calls.push(call);
      recordCount += call.records.length;
      payloadBytes += call.reservedPayloadBytes;
    }
    this.#queue.splice(0, calls.length);
    return calls;
  }

  #resolveCalls(
    calls: readonly PendingAppendCall[],
    receipts: readonly AppendReceipt[],
  ): void {
    let receiptOffset = 0;
    for (const call of calls) {
      const nextOffset = receiptOffset + call.records.length;
      call.resolve(receipts.slice(receiptOffset, nextOffset));
      receiptOffset = nextOffset;
      this.#releaseCall(call);
    }
  }

  #rejectCalls(calls: readonly PendingAppendCall[], reason: unknown): void {
    for (const call of calls) {
      call.reject(reason);
      this.#releaseCall(call);
    }
  }

  #releaseCall(call: PendingAppendCall): void {
    this.#pendingRecords -= call.records.length;
    this.#pendingPayloadBytes -= call.reservedPayloadBytes;
  }

  async #appendBatch(
    inputs: readonly PendingAppend[],
  ): Promise<readonly AppendReceipt[]> {
    const writerStartSeqNum = this.#nextWriterSeqNum;
    const writerEndSeqNum = writerStartSeqNum + BigInt(inputs.length);
    if (writerEndSeqNum > MAX_U64) {
      throw new TsfClientError(
        "writer_sequence_exhausted",
        "append batch exceeds the writer sequence range",
      );
    }
    const frame = {
      type: "appendBatch" as const,
      records: inputs.map((input, index) => ({
        writerSeqNum: writerStartSeqNum + BigInt(index),
        part: input.part,
        format: input.format,
        data: input.data,
      })),
    };

    let reconnectAttempts = 0;
    let reconnectDelay = this.policy.initialBackoffMs;
    let durabilityAmbiguous = false;
    for (;;) {
      try {
        this.#socket.send(frame);
        durabilityAmbiguous = true;
        const receipts: AppendReceipt[] = [];
        let nextWriterSeqNum = writerStartSeqNum;
        while (nextWriterSeqNum < writerEndSeqNum) {
          const response = await withTimeout(
            this.#socket.nextFrame(),
            this.policy.webSocketOperationTimeoutMs,
            "append acknowledgement",
          );
          if (response.type !== "appendAck") {
            throw unexpectedFrame(response);
          }
          const records = response.writerEndSeqNum - response.writerStartSeqNum;
          if (
            response.writerStartSeqNum !== nextWriterSeqNum ||
            response.writerEndSeqNum > writerEndSeqNum ||
            response.writerEndSeqNum <= response.writerStartSeqNum ||
            response.endSeqNum <= response.startSeqNum ||
            response.endSeqNum - response.startSeqNum !== records
          ) {
            throw new TsfClientError(
              "invalid_append_ack",
              "append acknowledgement does not match the submitted batch",
            );
          }
          for (let offset = 0n; offset < records; offset += 1n) {
            receipts.push({
              writerSeqNum: response.writerStartSeqNum + offset,
              seqNum: response.startSeqNum + offset,
            });
          }
          nextWriterSeqNum = response.writerEndSeqNum;
        }
        if (writerEndSeqNum === MAX_U64) {
          this.#closed = true;
        } else {
          this.#nextWriterSeqNum = writerEndSeqNum;
        }
        return receipts;
      } catch (error) {
        if (
          error instanceof TsfWebSocketClosedError &&
          error.closeCode === 1008 &&
          error.reason === "sequence_mismatch"
        ) {
          throw this.#terminateSequenceMismatch(error);
        }
        if (!isRetryableSocketError(error)) {
          throw durabilityAmbiguous ? this.#terminateAmbiguous(error) : error;
        }
        this.#socket.close();
        let recovered: Awaited<ReturnType<typeof reconnectSocket>>;
        try {
          recovered = await reconnectSocket(
            this.connect,
            this.policy,
            reconnectAttempts,
            reconnectDelay,
            error,
          );
        } catch (reconnectError) {
          throw durabilityAmbiguous
            ? this.#terminateAmbiguous(reconnectError)
            : reconnectError;
        }
        this.#socket = recovered.socket;
        reconnectAttempts = recovered.attempts;
        reconnectDelay = recovered.nextDelayMs;
      }
    }
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

  #terminateAmbiguous(cause: unknown): TsfClientError {
    this.#terminalError ??= new TsfClientError(
      "writer_durability_unknown",
      "append durability is unknown; this writer cannot safely continue",
      { cause },
    );
    this.#closed = true;
    this.#socket.close();
    return this.#terminalError;
  }

  #terminateSequenceMismatch(cause: unknown): TsfClientError {
    this.#terminalError ??= new TsfClientError(
      "sequence_mismatch",
      "stream next sequence did not match the writer session precondition",
      { cause },
    );
    this.#closed = true;
    this.#socket.close();
    return this.#terminalError;
  }
}

interface PendingAppend {
  readonly data: Uint8Array;
  readonly format: RecordFormat;
  readonly part: PartHeader;
  readonly reservedPayloadBytes: number;
}

interface PendingAppendCall {
  readonly records: readonly PendingAppend[];
  readonly reservedPayloadBytes: number;
  readonly resolve: (receipts: readonly AppendReceipt[]) => void;
  readonly reject: (reason: unknown) => void;
}

function prepareAppend(
  input: AppendInput,
  availablePayloadBytes: number,
): PendingAppend {
  if (availablePayloadBytes <= 0) {
    throw writerAdmissionError("payload");
  }
  let data: Uint8Array;
  if (typeof input.data === "string") {
    if (input.data.length > MAX_RECORD_BYTES) {
      throw recordTooLarge();
    }
    if (input.data.length > availablePayloadBytes) {
      throw writerAdmissionError("payload");
    }
    data = textEncoder.encode(input.data);
    if (data.byteLength > MAX_RECORD_BYTES) {
      throw recordTooLarge();
    }
    if (Math.max(data.byteLength, 1) > availablePayloadBytes) {
      throw writerAdmissionError("payload");
    }
  } else {
    if (input.data.byteLength > MAX_RECORD_BYTES) {
      throw recordTooLarge();
    }
    if (Math.max(input.data.byteLength, 1) > availablePayloadBytes) {
      throw writerAdmissionError("payload");
    }
    data = new Uint8Array(input.data);
  }
  const part = input.part ?? UNSPLIT_PART;
  return {
    data,
    format: input.format ??
      (typeof input.data === "string"
        ? RecordFormat.Transcript
        : RecordFormat.Bytes),
    part: part === UNSPLIT_PART
      ? UNSPLIT_PART
      : { index: part.index, isFinal: part.isFinal },
    reservedPayloadBytes: Math.max(data.byteLength, 1),
  };
}

function writerAdmissionError(limit: "payload" | "record"): TsfClientError {
  const message = limit === "record"
    ? `writer has reached its ${MAX_PENDING_WRITER_RECORDS}-record pending limit`
    : `writer has reached its ${MAX_PENDING_WRITER_PAYLOAD_BYTES}-byte pending payload limit`;
  return new TsfClientError("client_write_overload", message);
}

function recordTooLarge(): TsfClientError {
  return new TsfClientError(
    "record_too_large",
    `record exceeds the ${MAX_RECORD_BYTES}-byte maximum`,
  );
}
