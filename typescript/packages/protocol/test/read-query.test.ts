import { describe, expect, it } from "vitest";

import {
  encodeReadQuery,
  parseReadQuery,
  ProtocolError,
} from "../src/index.js";

describe("read query", () => {
  it("round-trips every read option", () => {
    const request = {
      start: { type: "timestampMs" as const, timestampMs: 1_000n },
      stop: {
        count: 20n,
        untilTimestampMs: 2_001n,
        waitSeconds: 30,
      },
      rate: 2,
    };
    const parameters = encodeReadQuery(request);

    expect(parameters.toString()).toBe(
      "timestamp=1000&count=20&until=2001&rate=2&wait=30",
    );
    expect(parseReadQuery(parameters)).toEqual(request);
  });

  it("defaults to the S2 tail position", () => {
    expect(parseReadQuery(new URLSearchParams())).toEqual({
      start: { type: "tailOffset", tailOffset: 0n },
    });
  });

  it("round-trips finite reads and full u64 bounds", () => {
    const request = {
      start: { type: "seqNum" as const, seqNum: 0n },
      stop: {
        count: 0xffff_ffff_ffff_ffffn,
      },
    };
    expect(parseReadQuery(encodeReadQuery(request))).toEqual(request);
  });

  it.each([
    ["0.1", 0.1],
    ["0.125", 0.125],
    ["0.5", 0.5],
    ["1", 1],
    ["2", 2],
    ["100", 100],
  ])("parses floating-point rate %s", (rate, expected) => {
    expect(parseReadQuery(new URLSearchParams(
      `until=1&rate=${rate}`,
    ))).toMatchObject({ rate: expected });
  });

  it.each(["count=1&rate=1", "until=1&rate=1", "wait=0&rate=1"])(
    "accepts paced read stop condition %s",
    (query) => {
      expect(parseReadQuery(new URLSearchParams(query))).toMatchObject({ rate: 1 });
    },
  );

  it.each([
    "unknown=1",
    "count=1&count=2",
    "seq_num=1&tail_offset=2",
    "seq_num=01",
    "seq_num=9007199254740992",
    "count=18446744073709551616",
    "until=9007199254740992",
    "wait=61",
    "rate=1",
    "until=1&rate=0.099",
    "until=1&rate=.5",
    "until=1&rate=1e0",
  ])("rejects invalid query %s", (query) => {
    expect(() => parseReadQuery(new URLSearchParams(query))).toThrow(
      ProtocolError,
    );
  });

  it("rejects invalid typed requests before encoding", () => {
    expect(() => encodeReadQuery({
      start: { type: "tailOffset", tailOffset: -1n },
    })).toThrow(ProtocolError);
    for (const rate of [Number.NaN, Number.POSITIVE_INFINITY, 0.09, 101]) {
      expect(() => encodeReadQuery({
        start: { type: "seqNum", seqNum: 0n },
        stop: { count: 1n },
        rate,
      })).toThrow(ProtocolError);
    }
  });
});
