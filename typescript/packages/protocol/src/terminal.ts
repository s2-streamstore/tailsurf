import { ProtocolError } from "./errors.js";

export const TERMINAL_EVENT_VERSION = 0x01;
export const MAX_TERMINAL_DIMENSION = 0xffff;

const HEADER_BYTES = 2;
const FIXED_EVENT_BYTES = HEADER_BYTES + 4;

const DATA = 0x01;
const RESIZE = 0x02;
const STARTED = 0x03;
const EXITED = 0x04;
const HEARTBEAT = 0x05;

export type TerminalInputEvent =
  | { readonly type: "data"; readonly data: Uint8Array }
  | { readonly type: "resize"; readonly columns: number; readonly rows: number };

export type TerminalOutputEvent =
  | { readonly type: "data"; readonly data: Uint8Array }
  | { readonly type: "resize"; readonly columns: number; readonly rows: number }
  | { readonly type: "started"; readonly columns: number; readonly rows: number }
  | { readonly type: "exited"; readonly status: number }
  | { readonly type: "heartbeat" };

export function encodeTerminalInputEvent(event: TerminalInputEvent): Uint8Array {
  switch (event.type) {
    case "data":
      return encodeData(DATA, event.data);
    case "resize":
      return encodeSize(RESIZE, event.columns, event.rows);
  }
}

export function decodeTerminalInputEvent(payload: Uint8Array): TerminalInputEvent {
  const type = terminalEventType(payload);
  switch (type) {
    case DATA:
      return { type: "data", data: payload.slice(HEADER_BYTES) };
    case RESIZE:
      return { type: "resize", ...decodeSize(payload, "resize") };
    default:
      throw unknownType("input", type);
  }
}

export function encodeTerminalOutputEvent(event: TerminalOutputEvent): Uint8Array {
  switch (event.type) {
    case "data":
      return encodeData(DATA, event.data);
    case "resize":
      return encodeSize(RESIZE, event.columns, event.rows);
    case "started":
      return encodeSize(STARTED, event.columns, event.rows);
    case "exited": {
      requireInt32(event.status, "terminal exit status");
      const payload = eventHeader(EXITED, FIXED_EVENT_BYTES);
      new DataView(payload.buffer).setInt32(HEADER_BYTES, event.status);
      return payload;
    }
    case "heartbeat":
      return eventHeader(HEARTBEAT, HEADER_BYTES);
  }
}

export function decodeTerminalOutputEvent(payload: Uint8Array): TerminalOutputEvent {
  const type = terminalEventType(payload);
  switch (type) {
    case DATA:
      return { type: "data", data: payload.slice(HEADER_BYTES) };
    case RESIZE:
      return { type: "resize", ...decodeSize(payload, "resize") };
    case STARTED:
      return { type: "started", ...decodeSize(payload, "started") };
    case EXITED:
      requireLength(payload, FIXED_EVENT_BYTES, "exited");
      return {
        type: "exited",
        status: new DataView(
          payload.buffer,
          payload.byteOffset,
          payload.byteLength,
        ).getInt32(HEADER_BYTES),
      };
    case HEARTBEAT:
      requireLength(payload, HEADER_BYTES, "heartbeat");
      return { type: "heartbeat" };
    default:
      throw unknownType("output", type);
  }
}

function encodeData(type: number, data: Uint8Array): Uint8Array {
  const payload = eventHeader(type, HEADER_BYTES + data.byteLength);
  payload.set(data, HEADER_BYTES);
  return payload;
}

function encodeSize(
  type: number,
  columns: number,
  rows: number,
): Uint8Array {
  requireDimension(columns, "terminal columns");
  requireDimension(rows, "terminal rows");
  const payload = eventHeader(type, FIXED_EVENT_BYTES);
  const view = new DataView(payload.buffer);
  view.setUint16(HEADER_BYTES, columns);
  view.setUint16(HEADER_BYTES + 2, rows);
  return payload;
}

function decodeSize(
  payload: Uint8Array,
  name: string,
): { readonly columns: number; readonly rows: number } {
  requireLength(payload, FIXED_EVENT_BYTES, name);
  const view = new DataView(
    payload.buffer,
    payload.byteOffset,
    payload.byteLength,
  );
  const columns = view.getUint16(HEADER_BYTES);
  const rows = view.getUint16(HEADER_BYTES + 2);
  requireDimension(columns, "terminal columns");
  requireDimension(rows, "terminal rows");
  return { columns, rows };
}

function eventHeader(type: number, length: number): Uint8Array {
  const payload = new Uint8Array(length);
  payload[0] = TERMINAL_EVENT_VERSION;
  payload[1] = type;
  return payload;
}

function terminalEventType(payload: Uint8Array): number {
  if (payload.byteLength < HEADER_BYTES) {
    throw new ProtocolError(
      "terminal_event_truncated",
      "terminal event must contain a version and type",
    );
  }
  const version = payload[0];
  if (version !== TERMINAL_EVENT_VERSION) {
    throw new ProtocolError(
      "unknown_terminal_event_version",
      `unknown terminal event version 0x${hex(version)}`,
    );
  }
  return payload[1]!;
}

function requireLength(payload: Uint8Array, expected: number, name: string): void {
  if (payload.byteLength !== expected) {
    throw new ProtocolError(
      "invalid_terminal_event_length",
      `${name} terminal event is ${payload.byteLength} bytes; expected ${expected}`,
    );
  }
}

function requireDimension(value: number, name: string): void {
  if (!Number.isInteger(value) || value < 1 || value > MAX_TERMINAL_DIMENSION) {
    throw new ProtocolError(
      "invalid_terminal_dimension",
      `${name} must be an integer between 1 and ${MAX_TERMINAL_DIMENSION}`,
    );
  }
}

function requireInt32(value: number, name: string): void {
  if (!Number.isInteger(value) || value < -0x8000_0000 || value > 0x7fff_ffff) {
    throw new ProtocolError(
      "invalid_terminal_exit_status",
      `${name} must fit in a signed 32-bit integer`,
    );
  }
}

function unknownType(direction: string, type: number): ProtocolError {
  return new ProtocolError(
    "unknown_terminal_event_type",
    `unknown terminal ${direction} event type 0x${hex(type)}`,
  );
}

function hex(value: number | undefined): string {
  return (value ?? 0).toString(16).padStart(2, "0");
}
