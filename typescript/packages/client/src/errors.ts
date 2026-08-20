import {
  decimalSafeU64Schema,
  MAX_REST_ERROR_RESPONSE_BYTES,
} from "@s2-dev/tailsurf-protocol";

export class TsfClientError extends Error {
  public constructor(
    public readonly code: string,
    message: string,
    options?: ErrorOptions,
  ) {
    super(message, options);
    this.name = "TsfClientError";
  }
}

export interface TsfHttpErrorDetails {
  readonly operation: string;
  readonly status: number;
  readonly apiCode?: string;
  readonly message: string;
  readonly requestId?: string;
  readonly retryAfterMs?: number;
  readonly actualNextSeqNum?: bigint;
  /** Raw server-provided message, without the API code prefix. */
  readonly apiMessage?: string;
}

export class TsfHttpError extends TsfClientError {
  public readonly operation: string;
  public readonly status: number;
  public readonly apiCode: string | undefined;
  public readonly requestId: string | undefined;
  public readonly retryAfterMs: number | undefined;
  public readonly actualNextSeqNum: bigint | undefined;
  public readonly apiMessage: string | undefined;

  public constructor(details: TsfHttpErrorDetails) {
    super("http_status", details.message);
    this.name = "TsfHttpError";
    this.operation = details.operation;
    this.status = details.status;
    this.apiCode = details.apiCode;
    this.requestId = details.requestId;
    this.retryAfterMs = details.retryAfterMs;
    this.actualNextSeqNum = details.actualNextSeqNum;
    this.apiMessage = details.apiMessage;
  }
}

export class TsfWebSocketClosedError extends TsfClientError {
  public constructor(
    public readonly closeCode: number,
    public readonly reason: string,
    public readonly wasClean: boolean,
  ) {
    super(
      "websocket_closed",
      reason.length === 0
        ? `WebSocket closed with code ${closeCode}`
        : `WebSocket closed with code ${closeCode}: ${reason}`,
    );
    this.name = "TsfWebSocketClosedError";
  }
}

export async function httpStatusError(
  response: Response,
  operation: string,
): Promise<TsfHttpError> {
  let retryAfterMs = retryAfterDelay(response.headers.get("retry-after"));
  let requestId = response.headers.get("x-request-id")?.trim() || undefined;
  let message = `${operation} failed with ${response.status} ${response.statusText}`.trim();
  let apiCode: string | undefined;
  let rawApiMessage: string | undefined;
  let actualNextSeqNum: bigint | undefined;
  try {
    const body = await readJsonResponse(
      response,
      MAX_REST_ERROR_RESPONSE_BYTES,
      operation,
    );
    if (isRecord(body) && isRecord(body.error)) {
      const code = body.error.code;
      const apiMessage = body.error.message;
      const bodyRequestId = body.error.request_id;
      const bodyRetryAfterMs = body.error.retry_after_ms;
      if (typeof code === "string" && code.length > 0) {
        apiCode = code;
      }
      if (typeof apiMessage === "string" && apiMessage.length > 0) {
        rawApiMessage = apiMessage;
        message = apiCode === undefined ? apiMessage : `${apiCode}: ${apiMessage}`;
      }
      if (
        requestId === undefined &&
        typeof bodyRequestId === "string" &&
        bodyRequestId.trim().length > 0
      ) {
        requestId = bodyRequestId.trim();
      }
      if (
        retryAfterMs === undefined &&
        typeof bodyRetryAfterMs === "number" &&
        Number.isSafeInteger(bodyRetryAfterMs) &&
        bodyRetryAfterMs >= 0
      ) {
        retryAfterMs = bodyRetryAfterMs;
      }
      try {
        actualNextSeqNum = BigInt(decimalSafeU64Schema.parse(
          body.error.actual_next_seq_num,
        ));
      } catch {
        // An optional malformed detail must not hide the primary HTTP error.
      }
    }
  } catch {
    // Preserve the status-only fallback without exposing arbitrary response bodies.
  }
  return new TsfHttpError({
    operation,
    status: response.status,
    ...(apiCode === undefined ? {} : { apiCode }),
    message,
    ...(requestId === undefined ? {} : { requestId }),
    ...(retryAfterMs === undefined ? {} : { retryAfterMs }),
    ...(actualNextSeqNum === undefined ? {} : { actualNextSeqNum }),
    ...(rawApiMessage === undefined ? {} : { apiMessage: rawApiMessage }),
  });
}

export async function readJsonResponse(
  response: Response,
  maximumBytes: number,
  operation: string,
): Promise<unknown> {
  const declaredLength = response.headers.get("content-length");
  if (
    declaredLength !== null &&
    /^\d+$/.test(declaredLength) &&
    Number(declaredLength) > maximumBytes
  ) {
    await response.body?.cancel().catch(() => undefined);
    throw responseTooLarge(operation, maximumBytes);
  }
  if (response.body === null) {
    return JSON.parse("") as unknown;
  }

  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let length = 0;
  try {
    for (;;) {
      const next = await reader.read();
      if (next.done) {
        break;
      }
      length += next.value.byteLength;
      if (length > maximumBytes) {
        await reader.cancel().catch(() => undefined);
        throw responseTooLarge(operation, maximumBytes);
      }
      chunks.push(next.value);
    }
  } finally {
    reader.releaseLock();
  }

  const bytes = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return JSON.parse(
    new TextDecoder("utf-8", { fatal: true }).decode(bytes),
  ) as unknown;
}

function responseTooLarge(
  operation: string,
  maximumBytes: number,
): TsfClientError {
  return new TsfClientError(
    "response_too_large",
    `${operation} response exceeds ${maximumBytes} bytes`,
  );
}

function retryAfterDelay(value: string | null): number | undefined {
  if (value === null) {
    return undefined;
  }
  const trimmed = value.trim();
  let delayMs: number;
  if (/^\d+$/.test(trimmed)) {
    delayMs = Number(trimmed) * 1_000;
  } else {
    const at = Date.parse(trimmed);
    if (!Number.isFinite(at)) {
      return undefined;
    }
    delayMs = Math.max(0, at - Date.now());
  }
  return Number.isSafeInteger(delayMs) && delayMs >= 0 ? delayMs : undefined;
}

export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}
