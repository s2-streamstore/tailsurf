import { describe, expect, it } from "vitest";

import {
  DEFAULT_MAX_TRANSCRIPT_LOGICAL_RECORD_BYTES,
  LogicalTranscript,
  partHeader,
  parseWriterId,
  ProtocolError,
  RecordFormat,
  UNSPLIT_PART,
  type ReadRecord,
} from "../src/index.js";

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

describe("logical transcript", () => {
  it("suppresses duplicate writer sequence numbers", () => {
    const transcript = new LogicalTranscript();

    expect(text(transcript.pushRecord(record(0n, UNSPLIT_PART, "hello")))).toBe(
      "hello",
    );
    expect(transcript.pushRecord(record(0n, UNSPLIT_PART, "HELLO"))).toBeUndefined();
  });

  it("reassembles contiguous split records", () => {
    const transcript = new LogicalTranscript();

    expect(transcript.pushRecord(record(7n, partHeader(0, false), "hel"))).toBeUndefined();
    expect(text(transcript.pushRecord(record(8n, partHeader(1, true), "lo")))).toBe(
      "hello",
    );
  });

  it("drops split records after gaps and resumes on an unsplit record", () => {
    const transcript = new LogicalTranscript();

    expect(transcript.pushRecord(record(7n, partHeader(0, false), "hel"))).toBeUndefined();
    expect(transcript.pushRecord(record(9n, partHeader(2, true), "lo"))).toBeUndefined();
    expect(text(transcript.pushRecord(record(10n, UNSPLIT_PART, "next")))).toBe(
      "next",
    );
  });

  it("tracks writers independently", () => {
    const transcript = new LogicalTranscript();

    expect(text(transcript.pushRecord(record(0n, UNSPLIT_PART, "first", 1)))).toBe(
      "first",
    );
    expect(text(transcript.pushRecord(record(0n, UNSPLIT_PART, "second", 2)))).toBe(
      "second",
    );
  });

  it("enforces the logical record size limit and resynchronizes", () => {
    const transcript = new LogicalTranscript({ maxLogicalRecordBytes: 4 });

    expect(transcript.pushRecord(record(7n, partHeader(0, false), "hel"))).toBeUndefined();
    expect(() =>
      transcript.pushRecord(record(8n, partHeader(1, true), "lo")),
    ).toThrow(ProtocolError);
    expect(text(transcript.pushRecord(record(9n, UNSPLIT_PART, "next")))).toBe(
      "next",
    );
  });

  it("bounds writer cardinality without disturbing known writers", () => {
    const transcript = new LogicalTranscript({ maxWriterStates: 2 });

    expect(text(transcript.pushRecord(record(0n, UNSPLIT_PART, "one", 1)))).toBe(
      "one",
    );
    expect(text(transcript.pushRecord(record(0n, UNSPLIT_PART, "two", 2)))).toBe(
      "two",
    );
    expect(() => transcript.pushRecord(record(0n, UNSPLIT_PART, "three", 3)))
      .toThrowError(expect.objectContaining({ code: "transcript_writer_limit" }));
    expect(text(transcript.pushRecord(record(1n, UNSPLIT_PART, "next", 1)))).toBe(
      "next",
    );
  });

  it("bounds total pending bytes and releases them on resynchronization", () => {
    const transcript = new LogicalTranscript({
      maxLogicalRecordBytes: 4,
      maxTotalPendingBytes: 4,
    });

    expect(
      transcript.pushRecord(record(0n, partHeader(0, false), "abc", 1)),
    ).toBeUndefined();
    expect(() =>
      transcript.pushRecord(record(0n, partHeader(0, false), "de", 2))
    ).toThrowError(
      expect.objectContaining({ code: "transcript_total_pending_bytes_limit" }),
    );
    expect(text(transcript.pushRecord(record(1n, UNSPLIT_PART, "done", 1)))).toBe(
      "done",
    );
    expect(
      transcript.pushRecord(record(1n, partHeader(0, false), "wxyz", 2)),
    ).toBeUndefined();
  });

  it("bounds total pending parts independently of payload bytes", () => {
    const transcript = new LogicalTranscript({
      maxLogicalRecordBytes: 10,
      maxTotalPendingBytes: 10,
      maxTotalPendingParts: 2,
    });

    expect(
      transcript.pushRecord(record(0n, partHeader(0, false), "", 1)),
    ).toBeUndefined();
    expect(
      transcript.pushRecord(record(1n, partHeader(1, false), "", 1)),
    ).toBeUndefined();
    expect(() =>
      transcript.pushRecord(record(2n, partHeader(2, false), "", 1))
    ).toThrowError(
      expect.objectContaining({ code: "transcript_total_pending_parts_limit" }),
    );
  });

  it("raises the implicit pending budget with the logical record limit", () => {
    const transcript = new LogicalTranscript({
      maxLogicalRecordBytes: DEFAULT_MAX_TRANSCRIPT_LOGICAL_RECORD_BYTES + 1,
    });

    expect(transcript.maxTotalPendingBytes).toBe(DEFAULT_MAX_TRANSCRIPT_LOGICAL_RECORD_BYTES + 1);
  });

  it("rejects a pending budget below the logical record limit", () => {
    expect(() => new LogicalTranscript({
      maxLogicalRecordBytes: 5,
      maxTotalPendingBytes: 4,
    })).toThrowError(expect.objectContaining({ code: "invalid_transcript_limit" }));
  });

  it("accepts a pending budget above the logical record limit", () => {
    const transcript = new LogicalTranscript({
      maxLogicalRecordBytes: 4,
      maxTotalPendingBytes: 8,
    });

    expect(transcript.maxTotalPendingBytes).toBe(8);
  });
});

function record(
  writerSeqNum: bigint,
  part: ReadRecord["part"],
  data: string,
  writer = 1,
): ReadRecord {
  return {
    seqNum: writerSeqNum,
    timestampMs: writerSeqNum,
    writerId: parseWriterId(new Uint8Array(16).fill(writer)),
    writerSeqNum,
    part,
    format: RecordFormat.Transcript,
    data: textEncoder.encode(data),
  };
}

function text(record: ReturnType<LogicalTranscript["pushRecord"]>): string | undefined {
  return record === undefined ? undefined : textDecoder.decode(record.data);
}
