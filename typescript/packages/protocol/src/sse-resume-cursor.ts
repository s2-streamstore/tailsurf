import { MAX_SAFE_INTEGER_U64, MAX_U64, tryParseDecimalU64 } from "./primitives.js";

export interface SseResumeCursor {
  readonly nextSeqNum: bigint;
  readonly consumedRecords: bigint;
}

export function encodeSseResumeCursor(cursor: SseResumeCursor): string {
  validateSseResumeCursor(cursor);
  return [
    "v1",
    cursor.nextSeqNum,
    cursor.consumedRecords,
  ].join(",");
}

export function decodeSseResumeCursor(value: string): SseResumeCursor {
  const fields = value.split(",");
  if (fields[0] !== "v1" || fields.length !== 3) {
    throw new RangeError("invalid SSE resume cursor");
  }
  const values = fields.slice(1).map(parseDecimalU64);
  const cursor: SseResumeCursor = {
    nextSeqNum: values[0]!,
    consumedRecords: values[1]!,
  };
  validateSseResumeCursor(cursor);
  return cursor;
}

function parseDecimalU64(value: string): bigint {
  const parsed = tryParseDecimalU64(value);
  if (parsed === undefined) {
    throw new RangeError("invalid SSE resume cursor");
  }
  return parsed;
}

function validateSseResumeCursor(cursor: SseResumeCursor): void {
  if (
    cursor.nextSeqNum < 0n ||
    cursor.nextSeqNum > MAX_SAFE_INTEGER_U64 ||
    cursor.consumedRecords < 0n ||
    cursor.consumedRecords > MAX_U64 ||
    cursor.consumedRecords > cursor.nextSeqNum
  ) {
    throw new RangeError("invalid SSE resume cursor");
  }
}
