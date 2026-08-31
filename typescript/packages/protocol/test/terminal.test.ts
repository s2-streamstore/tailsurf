import { describe, expect, it } from "vitest";

import {
  decodeTerminalInputEvent,
  decodeTerminalOutputEvent,
  encodeTerminalInputEvent,
  encodeTerminalOutputEvent,
  ProtocolError,
} from "../src/index.js";

describe("terminal event protocol", () => {
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
      { type: "exited", status: -1 },
      { type: "heartbeat" },
    ] as const;
    for (const event of events) {
      expect(decodeTerminalOutputEvent(encodeTerminalOutputEvent(event)))
        .toEqual(event);
    }
  });

  it("rejects invalid events", () => {
    expect(() => decodeTerminalInputEvent(Uint8Array.of(1))).toThrow(ProtocolError);
    expect(() => decodeTerminalOutputEvent(Uint8Array.of(2, 1))).toThrow(
      /unknown terminal event version/,
    );
    expect(() => decodeTerminalInputEvent(Uint8Array.of(1, 2, 0, 80, 0)))
      .toThrow(/expected 6/);
    expect(() => encodeTerminalInputEvent({
      type: "resize",
      columns: 0,
      rows: 24,
    })).toThrow(/columns/);
  });
});
