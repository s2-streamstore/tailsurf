import { describe, expect, it } from "vitest";

import {
  DEFAULT_MAX_TRANSCRIPT_REASSEMBLY_BYTES,
  LogicalTranscript,
  partHeader,
  parseWriterId,
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

  it("enforces the reassembly byte limit and resynchronizes", () => {
    const transcript = new LogicalTranscript({ maxReassemblyBytes: 4 });

    expect(transcript.pushRecord(record(7n, partHeader(0, false), "hel"))).toBeUndefined();
    expect(() =>
      transcript.pushRecord(record(8n, partHeader(1, true), "lo")),
    ).toThrowError(expect.objectContaining({ code: "transcript_reassembly_limit" }));
    expect(text(transcript.pushRecord(record(9n, UNSPLIT_PART, "next")))).toBe(
      "next",
    );
  });

  it("does not charge borrowed unsplit records to reassembly", () => {
    const transcript = new LogicalTranscript({ maxReassemblyBytes: 4 });

    expect(text(transcript.pushRecord(record(0n, UNSPLIT_PART, "hello")))).toBe(
      "hello",
    );
  });

  it("bounds writer cardinality internally without disturbing known writers", () => {
    const transcript = new LogicalTranscript();

    for (let writer = 0; writer < 4_096; writer += 1) {
      expect(text(transcript.pushRecord(record(0n, UNSPLIT_PART, "value", writer))))
        .toBe("value");
    }
    expect(() => transcript.pushRecord(record(0n, UNSPLIT_PART, "overflow", 4_096)))
      .toThrowError(expect.objectContaining({ code: "transcript_writer_limit" }));
    expect(text(transcript.pushRecord(record(1n, UNSPLIT_PART, "next", 0)))).toBe(
      "next",
    );
  });

  it("bounds reassembly bytes across writers and releases them", () => {
    const transcript = new LogicalTranscript({ maxReassemblyBytes: 4 });

    expect(
      transcript.pushRecord(record(0n, partHeader(0, false), "abc", 1)),
    ).toBeUndefined();
    expect(() =>
      transcript.pushRecord(record(0n, partHeader(0, false), "de", 2))
    ).toThrowError(
      expect.objectContaining({ code: "transcript_reassembly_limit" }),
    );
    expect(text(transcript.pushRecord(record(1n, UNSPLIT_PART, "done", 1)))).toBe(
      "done",
    );
    expect(
      transcript.pushRecord(record(1n, partHeader(0, false), "wxyz", 2)),
    ).toBeUndefined();
  });

  it("bounds total pending parts independently of payload bytes", () => {
    const transcript = new LogicalTranscript();

    for (let index = 0; index < 16_384; index += 1) {
      expect(
        transcript.pushRecord(record(BigInt(index), partHeader(index, false), "", 1)),
      ).toBeUndefined();
    }
    expect(() =>
      transcript.pushRecord(record(16_384n, partHeader(16_384, false), "", 1))
    ).toThrowError(
      expect.objectContaining({ code: "transcript_total_pending_parts_limit" }),
    );
  });

  it("uses one shared default reassembly byte limit", () => {
    const transcript = new LogicalTranscript();

    expect(DEFAULT_MAX_TRANSCRIPT_REASSEMBLY_BYTES).toBe(16 * 1024 * 1024);
    expect(transcript.maxReassemblyBytes).toBe(DEFAULT_MAX_TRANSCRIPT_REASSEMBLY_BYTES);
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
    writerId: writerId(writer),
    writerSeqNum,
    part,
    format: RecordFormat.Transcript,
    data: textEncoder.encode(data),
  };
}

function writerId(index: number): ReadRecord["writerId"] {
  const bytes = new Uint8Array(16);
  new DataView(bytes.buffer).setUint32(12, index);
  return parseWriterId(bytes);
}

function text(record: ReturnType<LogicalTranscript["pushRecord"]>): string | undefined {
  return record === undefined ? undefined : textDecoder.decode(record.data);
}
