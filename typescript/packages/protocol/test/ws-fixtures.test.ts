import { readFileSync } from "node:fs";

import { describe, expect, it } from "vitest";

import {
  decodeClientFrame,
  decodeServerFrame,
  encodeClientFrame,
  encodeServerFrame,
  MAX_APPEND_FRAME_RECORDS,
  MAX_ENCODED_FRAME_BYTES,
  MAX_FRAME_PAYLOAD_BYTES,
  MAX_READ_FRAME_RECORDS,
  MAX_RECORD_PAYLOAD_BYTES,
  MAX_WRITER_IN_FLIGHT_PAYLOAD_BYTES,
  MAX_WRITER_IN_FLIGHT_RECORDS,
  partHeaderFromRaw,
  parseClientWriterId,
  parseWriterId,
  ProtocolError,
  streamMetadataSchema,
  TSF_WEBSOCKET_PROTOCOL,
  WEBSOCKET_HEARTBEAT_INTERVAL_MS,
  type ClientFrame,
  type RecordFormat,
  type ServerFrame,
} from "../src/index.js";

interface FrameFixture {
  readonly name: string;
  readonly frame: Readonly<Record<string, string | number | boolean | null>>;
  readonly hex: string;
}

interface Fixtures {
  readonly websocket_protocol: string;
  readonly websocket_heartbeat_interval_ms: number;
  readonly max_record_payload_bytes: number;
  readonly max_append_frame_records: number;
  readonly max_read_frame_records: number;
  readonly max_frame_payload_bytes: number;
  readonly max_encoded_frame_bytes: number;
  readonly max_writer_in_flight_records: number;
  readonly max_writer_in_flight_payload_bytes: number;
  readonly client_frames: readonly FrameFixture[];
  readonly server_frames: readonly FrameFixture[];
}

const fixtures = JSON.parse(
  readFileSync(new URL("../fixtures/v1.json", import.meta.url), "utf8"),
) as Fixtures;

describe("TSF v1 wire fixtures", () => {
  it("pins protocol constants", () => {
    expect(TSF_WEBSOCKET_PROTOCOL).toBe(fixtures.websocket_protocol);
    expect(WEBSOCKET_HEARTBEAT_INTERVAL_MS).toBe(
      fixtures.websocket_heartbeat_interval_ms,
    );
    expect(MAX_RECORD_PAYLOAD_BYTES).toBe(fixtures.max_record_payload_bytes);
    expect(MAX_APPEND_FRAME_RECORDS).toBe(fixtures.max_append_frame_records);
    expect(MAX_READ_FRAME_RECORDS).toBe(fixtures.max_read_frame_records);
    expect(MAX_FRAME_PAYLOAD_BYTES).toBe(fixtures.max_frame_payload_bytes);
    expect(MAX_ENCODED_FRAME_BYTES).toBe(fixtures.max_encoded_frame_bytes);
    expect(MAX_WRITER_IN_FLIGHT_RECORDS).toBe(
      fixtures.max_writer_in_flight_records,
    );
    expect(MAX_WRITER_IN_FLIGHT_PAYLOAD_BYTES).toBe(
      fixtures.max_writer_in_flight_payload_bytes,
    );
  });

  it.each(fixtures.client_frames)("encodes and decodes client $name", (fixture) => {
    const expected = fromHex(fixture.hex);
    expect(toHex(encodeClientFrame(clientFrame(fixture.frame)))).toBe(fixture.hex);
    expect(encodeClientFrame(decodeClientFrame(expected))).toEqual(expected);
  });

  it.each(fixtures.server_frames)("encodes and decodes server $name", (fixture) => {
    const expected = fromHex(fixture.hex);
    expect(toHex(encodeServerFrame(serverFrame(fixture.frame)))).toBe(fixture.hex);
    expect(encodeServerFrame(decodeServerFrame(expected))).toEqual(expected);
  });

  it("ignores unknown socket metadata fields", () => {
    const fixture = fixtures.server_frames.find(
      ({ frame }) => frame.type === "stream_metadata",
    );
    if (fixture === undefined) {
      throw new Error("missing stream metadata fixture");
    }
    const encoded = fromHex(fixture.hex);
    const metadata = JSON.parse(new TextDecoder().decode(encoded.subarray(1))) as
      Record<string, unknown>;
    metadata.future_field = { enabled: true };
    const payload = new TextEncoder().encode(JSON.stringify(metadata));
    const extended = new Uint8Array(payload.byteLength + 1);
    extended[0] = encoded[0] ?? 0;
    extended.set(payload, 1);

    expect(decodeServerFrame(extended)).toEqual(serverFrame(fixture.frame));
  });

  it("round-trips multi-record append and read batches", () => {
    const writerId = parseWriterId(
      fromHex("000102030405060708090a0b0c0d0e0f"),
    );
    const append: ClientFrame = {
      type: "appendBatch",
      records: [0n, 1n].map((writerSeqNum) => ({
        writerSeqNum,
        part: partHeaderFromRaw(0x8000_0000),
        format: 0,
        data: Uint8Array.of(Number(writerSeqNum)),
      })),
    };
    const read: ServerFrame = {
      type: "readBatch",
      records: append.records.map((record, index) => ({
        ...record,
        seqNum: BigInt(10 + index),
        timestampMs: BigInt(100 + index),
        writerId,
      })),
    };

    expect(decodeClientFrame(encodeClientFrame(append))).toEqual(append);
    expect(decodeServerFrame(encodeServerFrame(read))).toEqual(read);
  });

  it("uses separate append and read record-count limits", () => {
    const writerId = parseWriterId(
      fromHex("000102030405060708090a0b0c0d0e0f"),
    );
    const appendRecord = {
      writerSeqNum: 0n,
      part: partHeaderFromRaw(0x8000_0000),
      format: 0 as RecordFormat,
      data: new Uint8Array(),
    };
    const readRecord = {
      ...appendRecord,
      seqNum: 0n,
      timestampMs: 0n,
      writerId,
    };
    const maximumRead: ServerFrame = {
      type: "readBatch",
      records: Array.from(
        { length: MAX_READ_FRAME_RECORDS },
        (_, index) => ({ ...readRecord, seqNum: BigInt(index) }),
      ),
    };

    expect(
      decodeServerFrame(encodeServerFrame(maximumRead)),
    ).toEqual(maximumRead);
    expect(() =>
      encodeClientFrame({
        type: "appendBatch",
        records: Array.from(
          { length: MAX_APPEND_FRAME_RECORDS + 1 },
          () => appendRecord,
        ),
      })
    ).toThrow(ProtocolError);
    expect(() =>
      encodeServerFrame({
        type: "readBatch",
        records: Array.from(
          { length: MAX_READ_FRAME_RECORDS + 1 },
          () => readRecord,
        ),
      })
    ).toThrow(ProtocolError);
    expect(() =>
      encodeServerFrame({
        type: "readBatch",
        records: [readRecord, readRecord],
      })
    ).toThrow(ProtocolError);
  });

  it("rejects malformed, trailing, and oversized frames", () => {
    expect(() => decodeClientFrame(new Uint8Array())).toThrow(ProtocolError);
    expect(() => decodeClientFrame(Uint8Array.of(0x03))).toThrow(ProtocolError);
    expect(() => decodeClientFrame(Uint8Array.of(0x03, 0, 0, 0, 0))).toThrow(
      ProtocolError,
    );
    expect(() => decodeServerFrame(Uint8Array.of(0x80, 0))).toThrow(ProtocolError);
    expect(() =>
      decodeServerFrame(Uint8Array.of(0x84, ...new Uint8Array(8)))
    ).toThrow(ProtocolError);
    expect(() =>
      decodeServerFrame(Uint8Array.of(0x84, ...new Uint8Array(17)))
    ).toThrow(ProtocolError);
    expect(() =>
      encodeClientFrame({
        type: "appendBatch",
        records: [{
          writerSeqNum: 0n,
          part: partHeaderFromRaw(0x8000_0000),
          format: 0,
          data: new Uint8Array(MAX_RECORD_PAYLOAD_BYTES + 1),
        }],
      }),
    ).toThrow(ProtocolError);
    expect(() =>
      encodeClientFrame({
        type: "openWrite",
        clientWriterId: parseClientWriterId(new Uint8Array(16)),
        linkSecret: "B".repeat(33),
      })
    ).toThrow(ProtocolError);
  });

  it("strictly validates OpenRead flags, credentials, and lengths", () => {
    const valid = Uint8Array.of(0x01, 0x00);
    const unknownFlags = valid.slice();
    unknownFlags[1] = 0x02;
    const malformedUtf8 = Uint8Array.of(
      0x01,
      0x01,
      ...new Uint8Array(31),
      0xff,
    );
    const emptySecret = Uint8Array.of(0x01, 0x01);

    for (const invalid of [
      valid.subarray(0, 1),
      Uint8Array.from([...valid, 0x00]),
      unknownFlags,
      malformedUtf8,
      emptySecret,
    ]) {
      expect(() => decodeClientFrame(invalid)).toThrow(ProtocolError);
    }
  });

  it("strictly validates OpenWrite flags, preconditions, and lengths", () => {
    const valid = encodeClientFrame({
      type: "openWrite",
      clientWriterId: parseClientWriterId(new Uint8Array(16)),
      linkSecret: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
      expectedNextSeqNum: 7n,
    });
    const unknownFlags = valid.slice();
    unknownFlags[1] = 0x02;
    expect(() => decodeClientFrame(unknownFlags)).toThrow(ProtocolError);
    expect(() => decodeClientFrame(valid.subarray(0, valid.byteLength - 1)))
      .toThrow(ProtocolError);
    expect(() => encodeClientFrame({
      type: "openWrite",
      clientWriterId: parseClientWriterId(new Uint8Array(16)),
      linkSecret: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
      expectedNextSeqNum: BigInt(Number.MAX_SAFE_INTEGER) + 1n,
    })).toThrow(ProtocolError);
  });
});

function clientFrame(frame: FrameFixture["frame"]): ClientFrame {
  switch (frame.type) {
    case "open_read":
      return {
        type: "openRead",
        ...optionalString(frame, "link_secret", "linkSecret"),
      };
    case "open_write":
      return {
        type: "openWrite",
        clientWriterId: parseClientWriterId(
          fromHex(requiredString(frame, "client_writer_id_hex")),
        ),
        linkSecret: requiredString(frame, "link_secret"),
        ...optionalBigInt(frame, "expected_next_seq_num", "expectedNextSeqNum"),
      };
    case "append_batch":
      return {
        type: "appendBatch",
        records: [{
          writerSeqNum: BigInt(requiredString(frame, "writer_seq_num")),
          part: partHeaderFromRaw(Number.parseInt(requiredString(frame, "part_raw"), 16)),
          format: requiredNumber(frame, "format") as RecordFormat,
          data: fromHex(requiredString(frame, "data_hex")),
        }],
      };
    default:
      throw new Error(`unknown client fixture ${String(frame.type)}`);
  }
}

function optionalBigInt(
  frame: FrameFixture["frame"],
  fixtureKey: string,
  frameKey = fixtureKey,
): Record<string, bigint> {
  const value = frame[fixtureKey];
  return value === undefined ? {} : { [frameKey]: BigInt(String(value)) };
}

function optionalString(
  frame: FrameFixture["frame"],
  fixtureKey: string,
  frameKey: string,
): Record<string, string> {
  const value = frame[fixtureKey];
  return value === undefined ? {} : { [frameKey]: String(value) };
}

function serverFrame(frame: FrameFixture["frame"]): ServerFrame {
  switch (frame.type) {
    case "ready":
      return { type: "ready" };
    case "append_ack":
      return {
        type: "appendAck",
        writerStartSeqNum: BigInt(requiredString(frame, "writer_start_seq_num")),
        writerEndSeqNum: BigInt(requiredString(frame, "writer_end_seq_num")),
        startSeqNum: BigInt(requiredString(frame, "start_seq_num")),
        endSeqNum: BigInt(requiredString(frame, "end_seq_num")),
      };
    case "read_batch":
      return {
        type: "readBatch",
        records: [{
          seqNum: BigInt(requiredString(frame, "seq_num")),
          timestampMs: BigInt(requiredString(frame, "timestamp_ms")),
          writerId: parseWriterId(
            fromHex(requiredString(frame, "writer_id_hex")),
          ),
          writerSeqNum: BigInt(requiredString(frame, "writer_seq_num")),
          part: partHeaderFromRaw(Number.parseInt(requiredString(frame, "part_raw"), 16)),
          format: requiredNumber(frame, "format") as RecordFormat,
          data: fromHex(requiredString(frame, "data_hex")),
        }],
      };
    case "heartbeat":
      return { type: "heartbeat" };
    case "caught_up":
      return {
        type: "caughtUp",
        nextSeqNum: BigInt(requiredString(frame, "next_seq_num")),
        lastTimestampMs: BigInt(requiredString(frame, "last_timestamp_ms")),
      };
    case "stream_metadata":
      return {
        type: "streamMetadata",
        stream: streamMetadataSchema.parse({
          stream_id: requiredString(frame, "stream_id"),
          title: requiredString(frame, "title"),
          visibility: requiredString(frame, "visibility"),
          created_at: requiredString(frame, "created_at"),
          expires_at: requiredString(frame, "expires_at"),
        }),
      };
    default:
      throw new Error(`unknown server fixture ${String(frame.type)}`);
  }
}

function requiredString(
  value: FrameFixture["frame"],
  key: string,
): string {
  const field = value[key];
  if (typeof field !== "string") {
    throw new Error(`fixture field ${key} must be a string`);
  }
  return field;
}

function requiredNumber(
  value: FrameFixture["frame"],
  key: string,
): number {
  const field = value[key];
  if (typeof field !== "number") {
    throw new Error(`fixture field ${key} must be a number`);
  }
  return field;
}

function fromHex(hex: string): Uint8Array {
  if (hex.length % 2 !== 0 || !/^[0-9a-f]*$/.test(hex)) {
    throw new Error("invalid fixture hex");
  }
  return Uint8Array.from(
    { length: hex.length / 2 },
    (_, index) => Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16),
  );
}

function toHex(bytes: Uint8Array): string {
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
}
