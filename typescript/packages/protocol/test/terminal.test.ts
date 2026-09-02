import { readFileSync } from "node:fs";

import { describe, expect, it } from "vitest";

import {
  decodeTerminalInputEvent,
  decodeTerminalOutputEvent,
  encodeTerminalInputEvent,
  encodeTerminalOutputEvent,
  MAX_TERMINAL_CELLS,
  MAX_TERMINAL_COLUMNS,
  MAX_TERMINAL_ROWS,
  MAX_RECORD_PAYLOAD_BYTES,
  ProtocolError,
  type TerminalInputEvent,
  type TerminalOutputEvent,
} from "../src/index.js";

describe("terminal event protocol", () => {
  it("matches the shared Rust and TypeScript vectors", () => {
    const vectors = JSON.parse(readFileSync(
      new URL("../../../../rust/testdata/terminal-events.json", import.meta.url),
      "utf8",
    )) as readonly { readonly name: string; readonly bytes: readonly number[] }[];
    for (const vector of vectors) {
      const event = vectorEvent(vector.name);
      const encoded = "input" in event
        ? encodeTerminalInputEvent(event.input)
        : encodeTerminalOutputEvent(event.output);
      expect(Array.from(encoded), vector.name).toEqual(vector.bytes);
      expect(
        "input" in event
          ? decodeTerminalInputEvent(encoded)
          : decodeTerminalOutputEvent(encoded),
        vector.name,
      ).toEqual("input" in event ? event.input : event.output);
    }
  });

  it("decodes data as a zero-copy view", () => {
    const payload = Uint8Array.of(1, 1, 0x61);
    const event = decodeTerminalOutputEvent(payload);
    expect(event.type).toBe("data");
    if (event.type !== "data") {
      throw new Error("expected data event");
    }
    payload[2] = 0x62;
    expect(event.data).toEqual(Uint8Array.of(0x62));
  });

  it("rejects invalid events", () => {
    expect(() => decodeTerminalInputEvent(Uint8Array.of(1))).toThrow(ProtocolError);
    expect(() => decodeTerminalOutputEvent(Uint8Array.of(2, 1))).toThrow(
      /unknown terminal event version/,
    );
    expect(() => decodeTerminalInputEvent(Uint8Array.of(1, 2, 0, 80, 0)))
      .toThrow(/expected 6/);
    expect(() => decodeTerminalOutputEvent(
      Uint8Array.of(1, 4, 0, 0, 0, 0, 2),
    )).toThrow(/invalid terminal exited flags/);
    expect(() => decodeTerminalOutputEvent(
      Uint8Array.of(1, 6, 0, 80, 0),
    )).toThrow(/expected at least 6/);
    expect(() => encodeTerminalInputEvent({
      type: "resize",
      columns: 0,
      rows: 24,
    })).toThrow(/columns/);
    expect(() => encodeTerminalInputEvent({
      type: "resize",
      columns: MAX_TERMINAL_COLUMNS + 1,
      rows: 24,
    })).toThrow(/columns/);
    expect(() => encodeTerminalOutputEvent({
      type: "started",
      columns: MAX_TERMINAL_COLUMNS,
      rows: MAX_TERMINAL_ROWS,
    })).toThrow(new RegExp(`${MAX_TERMINAL_CELLS} cells`));
    expect(() => encodeTerminalOutputEvent({
      type: "checkpoint",
      columns: 80,
      rows: 24,
      state: new Uint8Array(MAX_RECORD_PAYLOAD_BYTES),
    })).toThrow(/maximum/);
  });
});

type TerminalEventVector =
  | { readonly input: TerminalInputEvent }
  | { readonly output: TerminalOutputEvent };

const VECTOR_EVENTS: Readonly<Record<string, TerminalEventVector>> = {
  "input-data": {
    input: { type: "data", data: Uint8Array.of(0, 1, 255) },
  },
  "input-resize": {
    input: { type: "resize", columns: 132, rows: 43 },
  },
  "output-data": {
    output: { type: "data", data: Uint8Array.of(27, 91, 109) },
  },
  "output-resize": {
    output: { type: "resize", columns: 80, rows: 24 },
  },
  "output-started": {
    output: { type: "started", columns: 120, rows: 40 },
  },
  "output-exited": {
    output: { type: "exited", status: -1, outputTruncated: false },
  },
  "output-exited-truncated": {
    output: { type: "exited", status: 0, outputTruncated: true },
  },
  "output-heartbeat": { output: { type: "heartbeat" } },
  "output-checkpoint": {
    output: {
      type: "checkpoint",
      columns: 80,
      rows: 24,
      state: Uint8Array.of(27, 91, 109),
    },
  },
};

function vectorEvent(name: string): TerminalEventVector {
  const event = VECTOR_EVENTS[name];
  if (event === undefined) {
    throw new Error(`unknown terminal test vector ${name}`);
  }
  return event;
}
