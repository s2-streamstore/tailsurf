import { ProtocolError } from "./errors.js";
import { writerIdKey } from "./ids.js";
import {
  isUnsplitPart,
  MAX_RECORD_PAYLOAD_BYTES,
  type ReadRecord,
  type RecordFormat,
} from "./ws.js";

/** Default maximum size of one reassembled logical record. */
export const DEFAULT_MAX_TRANSCRIPT_LOGICAL_RECORD_BYTES = MAX_RECORD_PAYLOAD_BYTES * 32;
/** Default maximum writer identities retained for deduplication and reassembly. */
export const DEFAULT_MAX_TRANSCRIPT_WRITER_STATES = 4_096;
/** Default maximum bytes retained across all unfinished split records. */
export const DEFAULT_MAX_TRANSCRIPT_TOTAL_PENDING_BYTES =
  DEFAULT_MAX_TRANSCRIPT_LOGICAL_RECORD_BYTES;
/** Default maximum physical parts retained across all unfinished split records. */
export const DEFAULT_MAX_TRANSCRIPT_TOTAL_PENDING_PARTS = 16_384;

export interface LogicalTranscriptOptions {
  /** Maximum size of one reassembled logical record. */
  readonly maxLogicalRecordBytes?: number;
  /** Maximum writer identities retained for deduplication and reassembly. */
  readonly maxWriterStates?: number;
  /** Maximum bytes retained across all unfinished split records. */
  readonly maxTotalPendingBytes?: number;
  /** Maximum physical parts retained across all unfinished split records. */
  readonly maxTotalPendingParts?: number;
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
  public readonly maxLogicalRecordBytes: number;
  public readonly maxWriterStates: number;
  public readonly maxTotalPendingBytes: number;
  public readonly maxTotalPendingParts: number;

  public constructor(options: LogicalTranscriptOptions = {}) {
    const maxLogicalRecordBytes = transcriptLimit(
      options.maxLogicalRecordBytes ?? DEFAULT_MAX_TRANSCRIPT_LOGICAL_RECORD_BYTES,
      "logical record bytes",
    );
    const maxTotalPendingBytes = transcriptLimit(
      options.maxTotalPendingBytes ?? Math.max(
        DEFAULT_MAX_TRANSCRIPT_TOTAL_PENDING_BYTES,
        maxLogicalRecordBytes,
      ),
      "total pending bytes",
    );
    if (maxTotalPendingBytes < maxLogicalRecordBytes) {
      throw new ProtocolError(
        "invalid_transcript_limit",
        `total pending bytes limit (${maxTotalPendingBytes}) must be at least logical record bytes limit (${maxLogicalRecordBytes})`,
      );
    }
    this.maxLogicalRecordBytes = maxLogicalRecordBytes;
    this.maxWriterStates = transcriptLimit(
      options.maxWriterStates ?? DEFAULT_MAX_TRANSCRIPT_WRITER_STATES,
      "writer states",
    );
    this.maxTotalPendingBytes = maxTotalPendingBytes;
    this.maxTotalPendingParts = transcriptLimit(
      options.maxTotalPendingParts ?? DEFAULT_MAX_TRANSCRIPT_TOTAL_PENDING_PARTS,
      "total pending parts",
    );
  }

  public pushRecord(record: ReadRecord): TranscriptRecord | undefined {
    const key = writerIdKey(record.writerId);
    let writer = this.#writers.get(key);
    if (writer === undefined) {
      if (this.#writers.size >= this.maxWriterStates) {
        throw new ProtocolError(
          "transcript_writer_limit",
          `transcript has reached its ${this.maxWriterStates} writer-state limit`,
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
    if (pending.length > this.maxTotalPendingBytes - this.#totalPendingBytes) {
      throw new ProtocolError(
        "transcript_total_pending_bytes_limit",
        `transcript has reached its ${this.maxTotalPendingBytes} total pending-byte limit`,
      );
    }
    if (pending.partCount > this.maxTotalPendingParts - this.#totalPendingParts) {
      throw new ProtocolError(
        "transcript_total_pending_parts_limit",
        `transcript has reached its ${this.maxTotalPendingParts} total pending-part limit`,
      );
    }
    writer.pending = pending;
    this.#totalPendingBytes += pending.length;
    this.#totalPendingParts += pending.partCount;
  }

  #clearPending(writer: WriterState): void {
    const pending = writer.pending;
    if (pending === undefined) {
      return;
    }
    delete writer.pending;
    this.#totalPendingBytes -= pending.length;
    this.#totalPendingParts -= pending.partCount;
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
