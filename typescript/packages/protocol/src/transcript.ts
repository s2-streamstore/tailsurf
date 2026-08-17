import { ProtocolError } from "./errors.js";
import { writerIdKey } from "./ids.js";
import {
  isUnsplitPart,
  MAX_RECORD_BYTES,
  type ReadRecord,
  type RecordFormat,
} from "./ws.js";

export const DEFAULT_MAX_LOGICAL_RECORD_BYTES = MAX_RECORD_BYTES * 32;
export const DEFAULT_MAX_TRANSCRIPT_WRITERS = 4_096;
/** SDK memory-safety limit across all unfinished split records. */
export const DEFAULT_MAX_TRANSCRIPT_PENDING_BYTES =
  DEFAULT_MAX_LOGICAL_RECORD_BYTES;
export const DEFAULT_MAX_TRANSCRIPT_PENDING_PARTS = 16_384;

export interface LogicalTranscriptOptions {
  readonly maxLogicalRecordBytes?: number;
  readonly maxWriters?: number;
  readonly maxPendingBytes?: number;
  readonly maxPendingParts?: number;
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
  #pendingBytes = 0;
  #pendingParts = 0;
  public readonly maxLogicalRecordBytes: number;
  public readonly maxWriters: number;
  public readonly maxPendingBytes: number;
  public readonly maxPendingParts: number;

  public constructor(options: LogicalTranscriptOptions = {}) {
    this.maxLogicalRecordBytes = transcriptLimit(
      options.maxLogicalRecordBytes ?? DEFAULT_MAX_LOGICAL_RECORD_BYTES,
      "logical record bytes",
    );
    this.maxWriters = transcriptLimit(
      options.maxWriters ?? DEFAULT_MAX_TRANSCRIPT_WRITERS,
      "writers",
    );
    this.maxPendingBytes = transcriptLimit(
      options.maxPendingBytes ?? DEFAULT_MAX_TRANSCRIPT_PENDING_BYTES,
      "pending bytes",
    );
    this.maxPendingParts = transcriptLimit(
      options.maxPendingParts ?? DEFAULT_MAX_TRANSCRIPT_PENDING_PARTS,
      "pending parts",
    );
  }

  public pushRecord(record: ReadRecord): TranscriptRecord | undefined {
    const key = writerIdKey(record.writerId);
    let writer = this.#writers.get(key);
    if (writer === undefined) {
      if (this.#writers.size >= this.maxWriters) {
        throw new ProtocolError(
          "transcript_writer_limit",
          `transcript has reached its ${this.maxWriters} writer limit`,
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
      this.#checkLength(record.data.byteLength);
      return { format: record.format, data: record.data };
    }

    const startSeqNum = record.writerSeqNum - BigInt(record.part.index);
    if (startSeqNum < 0n) {
      this.#clearPending(writer);
      return undefined;
    }

    if (record.part.index === 0) {
      this.#clearPending(writer);
      this.#checkLength(record.data.byteLength);
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

    const pending = writer.pending;
    this.#clearPending(writer);
    if (
      pending === undefined ||
      pending.startSeqNum !== startSeqNum ||
      pending.nextPartIndex !== record.part.index ||
      pending.format !== record.format
    ) {
      return undefined;
    }

    const length = pending.length + record.data.byteLength;
    this.#checkLength(length);
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

  #checkLength(length: number): void {
    if (length > this.maxLogicalRecordBytes) {
      throw new ProtocolError(
        "logical_record_too_large",
        `logical record is ${length} bytes; maximum is ${this.maxLogicalRecordBytes}`,
      );
    }
  }

  #setPending(writer: WriterState, pending: PendingRecord): void {
    if (pending.length > this.maxPendingBytes - this.#pendingBytes) {
      throw new ProtocolError(
        "transcript_pending_bytes_limit",
        `transcript has reached its ${this.maxPendingBytes} pending-byte limit`,
      );
    }
    if (pending.partCount > this.maxPendingParts - this.#pendingParts) {
      throw new ProtocolError(
        "transcript_pending_parts_limit",
        `transcript has reached its ${this.maxPendingParts} pending-part limit`,
      );
    }
    writer.pending = pending;
    this.#pendingBytes += pending.length;
    this.#pendingParts += pending.partCount;
  }

  #clearPending(writer: WriterState): void {
    const pending = writer.pending;
    if (pending === undefined) {
      return;
    }
    delete writer.pending;
    this.#pendingBytes -= pending.length;
    this.#pendingParts -= pending.partCount;
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
