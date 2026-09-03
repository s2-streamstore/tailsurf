import { ProtocolError } from "./errors.js";
import { writerIdKey } from "./ids.js";
import {
  isUnsplitPart,
  MAX_RECORD_PAYLOAD_BYTES,
  type ReadRecord,
} from "./ws.js";

/** Default maximum bytes used for split-record reassembly. */
export const DEFAULT_MAX_RECORD_REASSEMBLY_BYTES = MAX_RECORD_PAYLOAD_BYTES * 32;
const MAX_RECORD_WRITER_STATES = 4_096;
const MAX_RECORD_TOTAL_PENDING_PARTS = 16_384;

export interface LogicalRecordAssemblerOptions {
  /** Maximum bytes retained across unfinished split records or assembled for one completed split record. */
  readonly maxReassemblyBytes?: number;
}

export interface LogicalRecord {
  readonly data: Uint8Array;
}

interface PendingRecord {
  readonly startSeqNum: bigint;
  nextPartIndex: number;
  length: number;
  partCount: number;
  readonly chunks: Uint8Array[];
}

interface WriterState {
  highestSeq?: bigint;
  pending?: PendingRecord;
}

export class LogicalRecordAssembler {
  readonly #writers = new Map<string, WriterState>();
  #totalPendingBytes = 0;
  #totalPendingParts = 0;
  public readonly maxReassemblyBytes: number;

  public constructor(options: LogicalRecordAssemblerOptions = {}) {
    const maximum = options.maxReassemblyBytes ?? DEFAULT_MAX_RECORD_REASSEMBLY_BYTES;
    if (!Number.isSafeInteger(maximum) || maximum < 0) {
      throw new ProtocolError(
        "invalid_record_limit",
        "reassembly bytes limit must be a non-negative safe integer",
      );
    }
    this.maxReassemblyBytes = maximum;
  }

  public pushRecord(record: ReadRecord): LogicalRecord | undefined {
    const key = writerIdKey(record.writerId);
    let writer = this.#writers.get(key);
    if (writer === undefined) {
      if (this.#writers.size >= MAX_RECORD_WRITER_STATES) {
        throw new ProtocolError(
          "record_writer_limit",
          `record assembler has reached its ${MAX_RECORD_WRITER_STATES} writer-state limit`,
        );
      }
      writer = {};
      this.#writers.set(key, writer);
    }

    if (writer.highestSeq !== undefined && record.writerSeqNum <= writer.highestSeq) {
      return undefined;
    }
    writer.highestSeq = record.writerSeqNum;

    if (isUnsplitPart(record.part)) {
      this.#clearPending(writer);
      return { data: record.data };
    }

    const startSeqNum = record.writerSeqNum - BigInt(record.part.index);
    if (startSeqNum < 0n) {
      this.#clearPending(writer);
      return undefined;
    }

    if (record.part.index === 0) {
      this.#clearPending(writer);
      this.#setPending(writer, {
        startSeqNum,
        nextPartIndex: 1,
        length: record.data.byteLength,
        partCount: 1,
        chunks: record.data.byteLength === 0 ? [] : [record.data],
      });
      return undefined;
    }

    const pending = this.#takePending(writer);
    if (
      pending === undefined ||
      pending.startSeqNum !== startSeqNum ||
      pending.nextPartIndex !== record.part.index
    ) {
      return undefined;
    }

    const length = pending.length + record.data.byteLength;
    this.#checkReassemblyBytes(length);
    pending.length = length;
    pending.partCount += 1;
    if (record.data.byteLength > 0) {
      pending.chunks.push(record.data);
    }
    if (record.part.isFinal) {
      return { data: concatenate(pending.chunks, pending.length) };
    }

    pending.nextPartIndex = record.part.index + 1;
    this.#setPending(writer, pending);
    return undefined;
  }

  #checkReassemblyBytes(length: number): void {
    if (length > this.maxReassemblyBytes) {
      throw new ProtocolError(
        "record_reassembly_limit",
        `record reassembly would use ${length} bytes; maximum is ${this.maxReassemblyBytes}`,
      );
    }
  }

  #setPending(writer: WriterState, pending: PendingRecord): void {
    const reassemblyBytes = this.#totalPendingBytes + pending.length;
    if (reassemblyBytes > this.maxReassemblyBytes) {
      throw new ProtocolError(
        "record_reassembly_limit",
        `record reassembly would use ${reassemblyBytes} bytes; maximum is ${this.maxReassemblyBytes}`,
      );
    }
    if (pending.partCount > MAX_RECORD_TOTAL_PENDING_PARTS - this.#totalPendingParts) {
      throw new ProtocolError(
        "record_total_pending_parts_limit",
        `record assembler has reached its ${MAX_RECORD_TOTAL_PENDING_PARTS} total pending-part limit`,
      );
    }
    writer.pending = pending;
    this.#totalPendingBytes += pending.length;
    this.#totalPendingParts += pending.partCount;
  }

  #clearPending(writer: WriterState): void {
    this.#takePending(writer);
  }

  #takePending(writer: WriterState): PendingRecord | undefined {
    const pending = writer.pending;
    if (pending === undefined) {
      return undefined;
    }
    delete writer.pending;
    this.#totalPendingBytes -= pending.length;
    this.#totalPendingParts -= pending.partCount;
    return pending;
  }
}

function concatenate(chunks: readonly Uint8Array[], length: number): Uint8Array {
  if (chunks.length === 0) {
    return new Uint8Array();
  }
  if (chunks.length === 1) {
    return chunks[0] ?? new Uint8Array();
  }
  const output = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) {
    output.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return output;
}
