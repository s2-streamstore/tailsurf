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
  ProtocolError,
} from "../src/index.js";

describe("terminal event protocol", () => {
  it("matches the shared Rust and TypeScript vectors", () => {
    const vectors = JSON.parse(readFileSync(
      new URL("../../../../rust/testdata/terminal-events.json", import.meta.url),
      "utf8",
    )) as readonly { readonly name: string; readonly bytes: readonly number[] }[];
    for (const vector of vectors) {
      const encoded = encodeVector(vector.name);
      expect(Array.from(encoded), vector.name).toEqual(vector.bytes);
    }
  });

  it("round trips input events", () => {
    expect(decodeTerminalInputEvent(encodeTerminalInputEvent({
      type: "data",
      data: Uint8Array.of(0, 1, 255),
    }))).toEqual({ type: "data", data: Uint8Array.of(0, 1, 255) });
    expect(decodeTerminalInputEvent(encodeTerminalInputEvent({
      type: "resize",
      columns: 132,
      rows: 43,
    }))).toEqual({ type: "resize", columns: 132, rows: 43 });
  });

  it("round trips output events", () => {
    const events = [
      { type: "data", data: Uint8Array.of(27, 91, 109) },
      { type: "resize", columns: 80, rows: 24 },
      { type: "started", columns: 120, rows: 40 },
      { type: "exited", status: -1, outputTruncated: false },
      { type: "exited", status: 0, outputTruncated: true },
      { type: "heartbeat" },
      {
        type: "checkpoint",
        columns: 80,
        rows: 24,
        state: Uint8Array.of(27, 91, 109),
      },
    ] as const;
    for (const event of events) {
      expect(decodeTerminalOutputEvent(encodeTerminalOutputEvent(event)))
        .toEqual(event);
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
  });
});

function encodeVector(name: string): Uint8Array {
  switch (name) {
    case "input-data":
      return encodeTerminalInputEvent({
        type: "data",
        data: Uint8Array.of(0, 1, 255),
      });
    case "input-resize":
      return encodeTerminalInputEvent({ type: "resize", columns: 132, rows: 43 });
    case "output-data":
      return encodeTerminalOutputEvent({
        type: "data",
        data: Uint8Array.of(27, 91, 109),
      });
    case "output-resize":
      return encodeTerminalOutputEvent({ type: "resize", columns: 80, rows: 24 });
    case "output-started":
      return encodeTerminalOutputEvent({ type: "started", columns: 120, rows: 40 });
    case "output-exited":
      return encodeTerminalOutputEvent({
        type: "exited",
        status: -1,
        outputTruncated: false,
      });
    case "output-exited-truncated":
      return encodeTerminalOutputEvent({
        type: "exited",
        status: 0,
        outputTruncated: true,
      });
    case "output-heartbeat":
      return encodeTerminalOutputEvent({ type: "heartbeat" });
    case "output-checkpoint":
      return encodeTerminalOutputEvent({
        type: "checkpoint",
        columns: 80,
        rows: 24,
        state: Uint8Array.of(27, 91, 109),
      });
    default:
      throw new Error(`unknown terminal test vector ${name}`);
  }
}
