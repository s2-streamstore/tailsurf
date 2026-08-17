import { ProtocolError } from "./errors.js";
import { MAX_SAFE_INTEGER_U64, MAX_U64, U64_PATTERN } from "./primitives.js";

export const DEFAULT_READ_TAIL_OFFSET = 0n;
export const MIN_PLAYBACK_RATE = 0.1;
export const MAX_PLAYBACK_RATE = 100;
export const MAX_READ_WAIT_SECONDS = 60;

export type ReadStart =
  /** Start at this absolute sequence number. */
  | { readonly type: "seqNum"; readonly seqNum: bigint }
  /** Start at the first record at or after this Unix epoch millisecond. */
  | { readonly type: "timestampMs"; readonly timestampMs: bigint }
  /** Start this many records before the current tail. */
  | { readonly type: "tailOffset"; readonly tailOffset: bigint };

/** Conditions that make a stream read finite. */
export interface ReadStop {
  /** Maximum number of physical records to deliver. */
  readonly count?: bigint;
  /** Exclusive ending timestamp in Unix epoch milliseconds. */
  readonly untilTimestampMs?: bigint;
  /** Seconds to wait at the tail before ending this connection. */
  readonly waitSeconds?: number;
}

export interface ReadRequest {
  readonly start: ReadStart;
  readonly stop?: ReadStop;
  readonly rate?: number;
}

const READ_START_PARAMETERS = [
  "seq_num",
  "timestamp",
  "tail_offset",
] as const;

const READ_PARAMETERS: ReadonlySet<string> = new Set([
  ...READ_START_PARAMETERS,
  "count",
  "until",
  "rate",
  "wait",
]);

const RATE_PATTERN = /^(0|[1-9][0-9]*)(?:\.[0-9]+)?$/;

export function encodeReadQuery(request: ReadRequest): URLSearchParams {
  validateReadRequest(request);
  const parameters = new URLSearchParams();
  switch (request.start.type) {
    case "seqNum":
      parameters.set("seq_num", request.start.seqNum.toString());
      break;
    case "timestampMs":
      parameters.set("timestamp", request.start.timestampMs.toString());
      break;
    case "tailOffset":
      parameters.set("tail_offset", request.start.tailOffset.toString());
      break;
  }
  if (request.stop?.count !== undefined) {
    parameters.set("count", request.stop.count.toString());
  }
  if (request.stop?.untilTimestampMs !== undefined) {
    parameters.set("until", request.stop.untilTimestampMs.toString());
  }
  if (request.rate !== undefined) {
    parameters.set("rate", request.rate.toString());
  }
  if (request.stop?.waitSeconds !== undefined) {
    parameters.set("wait", request.stop.waitSeconds.toString());
  }
  return parameters;
}

export function parseReadQuery(parameters: URLSearchParams): ReadRequest {
  rejectUnknownOrDuplicateParameters(parameters);
  const selectors = READ_START_PARAMETERS.filter((name) =>
    parameters.has(name)
  );
  if (selectors.length > 1) {
    throw new ProtocolError(
      "ambiguous_read_start",
      "read start selectors are mutually exclusive",
    );
  }
  const start = parseReadStart(parameters);
  const count = parameters.has("count")
    ? requiredU64(parameters, "count")
    : undefined;
  const untilTimestampMs = parameters.has("until")
    ? requiredU64(parameters, "until", MAX_SAFE_INTEGER_U64)
    : undefined;
  const rate = optionalRate(parameters);
  const waitSeconds = parameters.has("wait")
    ? Number(requiredU64(parameters, "wait", BigInt(MAX_READ_WAIT_SECONDS)))
    : undefined;
  const stop = count === undefined &&
      untilTimestampMs === undefined &&
      waitSeconds === undefined
    ? undefined
    : {
        ...(count === undefined ? {} : { count }),
        ...(untilTimestampMs === undefined ? {} : { untilTimestampMs }),
        ...(waitSeconds === undefined ? {} : { waitSeconds }),
      };
  const request: ReadRequest = {
    start,
    ...(stop === undefined ? {} : { stop }),
    ...(rate === undefined ? {} : { rate }),
  };
  validateReadRequest(request);
  return request;
}

function parseReadStart(parameters: URLSearchParams): ReadStart {
  if (parameters.has("seq_num")) {
    return {
      type: "seqNum",
      seqNum: requiredU64(parameters, "seq_num", MAX_SAFE_INTEGER_U64),
    };
  }
  if (parameters.has("timestamp")) {
    return {
      type: "timestampMs",
      timestampMs: requiredU64(
        parameters,
        "timestamp",
        MAX_SAFE_INTEGER_U64,
      ),
    };
  }
  return {
    type: "tailOffset",
    tailOffset: parameters.has("tail_offset")
      ? requiredU64(parameters, "tail_offset", MAX_SAFE_INTEGER_U64)
      : 0n,
  };
}

function rejectUnknownOrDuplicateParameters(parameters: URLSearchParams): void {
  const seen = new Set<string>();
  for (const [name] of parameters) {
    if (!READ_PARAMETERS.has(name)) {
      throw new ProtocolError(
        "unknown_read_parameter",
        `unknown read query parameter: ${name}`,
      );
    }
    if (seen.has(name)) {
      throw new ProtocolError(
        "duplicate_read_parameter",
        `duplicate read query parameter: ${name}`,
      );
    }
    seen.add(name);
  }
}

function optionalRate(parameters: URLSearchParams): number | undefined {
  const value = parameters.get("rate");
  if (value === null) {
    return undefined;
  }
  if (!RATE_PATTERN.test(value)) {
    throw invalidParameter(
      "rate",
      "must be a decimal multiplier",
    );
  }
  return Number(value);
}

function requiredU64(
  parameters: URLSearchParams,
  name: string,
  maximum = MAX_U64,
): bigint {
  const value = parameters.get(name);
  if (value === null || !U64_PATTERN.test(value)) {
    throw invalidParameter(name, "must be a canonical decimal u64");
  }
  const parsed = BigInt(value);
  if (parsed > maximum) {
    throw invalidParameter(name, `must not exceed ${maximum}`);
  }
  return parsed;
}

function validateReadRequest(request: ReadRequest): void {
  const selector = readSelectorValue(request.start);
  const stop = request.stop;
  if (selector < 0n || selector > MAX_SAFE_INTEGER_U64) {
    throw invalidParameter(
      readStartParameter(request.start),
      `must not exceed ${MAX_SAFE_INTEGER_U64}`,
    );
  }
  if (stop?.count !== undefined &&
    (stop.count < 0n || stop.count > MAX_U64)) {
    throw invalidParameter("count", "must fit in an unsigned 64-bit integer");
  }
  if (stop?.untilTimestampMs !== undefined &&
    (stop.untilTimestampMs < 0n ||
      stop.untilTimestampMs > MAX_SAFE_INTEGER_U64)) {
    throw invalidParameter(
      "until",
      `must not exceed ${MAX_SAFE_INTEGER_U64}`,
    );
  }
  if (stop?.waitSeconds !== undefined &&
    (!Number.isInteger(stop.waitSeconds) ||
      stop.waitSeconds < 0 ||
      stop.waitSeconds > MAX_READ_WAIT_SECONDS)) {
    throw invalidParameter(
      "wait",
      `must be an integer from 0 through ${MAX_READ_WAIT_SECONDS}`,
    );
  }
  if (request.rate !== undefined) {
    if (
      !Number.isFinite(request.rate) ||
      request.rate < MIN_PLAYBACK_RATE ||
      request.rate > MAX_PLAYBACK_RATE
    ) {
      throw invalidParameter(
        "rate",
        "must be between 0.1 and 100",
      );
    }
    if (stop?.count === undefined &&
      stop?.untilTimestampMs === undefined &&
      stop?.waitSeconds !== 0) {
      throw invalidParameter("rate", "requires count, until, or wait=0");
    }
  }
}

function readSelectorValue(start: ReadStart): bigint {
  switch (start.type) {
    case "seqNum":
      return start.seqNum;
    case "timestampMs":
      return start.timestampMs;
    case "tailOffset":
      return start.tailOffset;
  }
}

function readStartParameter(start: ReadStart): string {
  switch (start.type) {
    case "seqNum":
      return "seq_num";
    case "timestampMs":
      return "timestamp";
    case "tailOffset":
      return "tail_offset";
  }
}

function invalidParameter(name: string, requirement: string): ProtocolError {
  return new ProtocolError(
    "invalid_read_parameter",
    `read query parameter ${name} ${requirement}`,
  );
}
