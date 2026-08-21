import { ProtocolError } from "./errors.js";
import { writerIdKey } from "./ids.js";
import {
  isUnsplitPart,
  MAX_RECORD_PAYLOAD_BYTES,
  type ReadRecord,
  type RecordFormat,
} from "./ws.js";

/** Default maximum bytes used for split-record reassembly. */
export const DEFAULT_MAX_TRANSCRIPT_REASSEMBLY_BYTES = MAX_RECORD_PAYLOAD_BYTES * 32;
const MAX_TRANSCRIPT_WRITER_STATES = 4_096;
const MAX_TRANSCRIPT_TOTAL_PENDING_PARTS = 16_384;

export interface LogicalTranscriptOptions {
  /** Maximum bytes retained across unfinished split records or assembled for one completed split record. */
  readonly maxReassemblyBytes?: number;
}

export interface TranscriptRecord {
  readonly format: RecordFormat;
  readonly data: Uint8Array;
}

interface PendingRecord {
  readonly startSeqNum: bigint;
  nextPartIndex: number;
  readonly format: RecordFormat;
  length: number;
  partCount: number;
  readonly chunks: Uint8Array[];
}

interface WriterState {
  highestSeq?: bigint;
  pending?: PendingRecord;
}

export class LogicalTranscript {
  readonly #writers = new Map<string, WriterState>();
  #totalPendingBytes = 0;
  #totalPendingParts = 0;
  public readonly maxReassemblyBytes: number;

  public constructor(options: LogicalTranscriptOptions = {}) {
    this.maxReassemblyBytes = transcriptLimit(
      options.maxReassemblyBytes ?? DEFAULT_MAX_TRANSCRIPT_REASSEMBLY_BYTES,
      "reassembly bytes",
    );
  }

  public pushRecord(record: ReadRecord): TranscriptRecord | undefined {
    const key = writerIdKey(record.writerId);
    let writer = this.#writers.get(key);
    if (writer === undefined) {
      if (this.#writers.size >= MAX_TRANSCRIPT_WRITER_STATES) {
        throw new ProtocolError(
          "transcript_writer_limit",
          `transcript has reached its ${MAX_TRANSCRIPT_WRITER_STATES} writer-state limit`,
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
      return { format: record.format, data: record.data };
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
        format: record.format,
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
      pending.nextPartIndex !== record.part.index ||
      pending.format !== record.format
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
      return {
        format: pending.format,
        data: concatenate(pending.chunks, pending.length),
      };
    }

    pending.nextPartIndex = record.part.index + 1;
    this.#setPending(writer, pending);
    return undefined;
  }

  #checkReassemblyBytes(length: number): void {
    if (length > this.maxReassemblyBytes) {
      throw new ProtocolError(
        "transcript_reassembly_limit",
        `transcript reassembly would use ${length} bytes; maximum is ${this.maxReassemblyBytes}`,
      );
    }
  }

  #setPending(writer: WriterState, pending: PendingRecord): void {
    const reassemblyBytes = this.#totalPendingBytes + pending.length;
    if (reassemblyBytes > this.maxReassemblyBytes) {
      throw new ProtocolError(
        "transcript_reassembly_limit",
        `transcript reassembly would use ${reassemblyBytes} bytes; maximum is ${this.maxReassemblyBytes}`,
      );
    }
    if (pending.partCount > MAX_TRANSCRIPT_TOTAL_PENDING_PARTS - this.#totalPendingParts) {
      throw new ProtocolError(
        "transcript_total_pending_parts_limit",
        `transcript has reached its ${MAX_TRANSCRIPT_TOTAL_PENDING_PARTS} total pending-part limit`,
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

function transcriptLimit(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new ProtocolError(
      "invalid_transcript_limit",
      `${name} limit must be a non-negative safe integer`,
    );
  }
  return value;
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
