import {
  decodeServerFrame,
  encodeClientFrame,
  MAX_U64,
  parseLinkSecret,
  TSF_WEBSOCKET_PROTOCOL,
  WEBSOCKET_HEARTBEAT_INTERVAL_MS,
  type ClientFrame,
  type ServerFrame,
  type StreamId,
  type StreamMetadata,
} from "@tailsurf/protocol";

import {
  TsfClientError,
  TsfWebSocketClosedError,
} from "./errors.js";
import {
  INITIAL_RETRY_BACKOFF_MS,
  jitteredBackoffMs,
  MAX_RETRY_BACKOFF_MS,
} from "./retry.js";

export const NORMAL_CLOSE_CODE = 1000;
const CONNECTING_READY_STATE = 0;
const OPEN_READY_STATE = 1;
const DEFAULT_MAX_RECEIVE_QUEUE_UNITS = 1_024;
const DEFAULT_MAX_RECEIVE_QUEUE_BYTES = 16 * 1024 * 1024;
export const WEBSOCKET_READ_IDLE_TIMEOUT_MS =
  WEBSOCKET_HEARTBEAT_INTERVAL_MS * 3;
const RETRYABLE_CLOSE_CODES = new Set([
  NORMAL_CLOSE_CODE,
  1001,
  1005,
  1006,
  1011,
  1012,
  1013,
  1014,
  1015,
]);

/** The browser-compatible WebSocket surface used by the client. */
export interface WebSocketLike {
  binaryType: string;
  readonly protocol: string;
  readonly readyState: number;
  addEventListener(type: string, listener: (event: unknown) => void): void;
  removeEventListener(type: string, listener: (event: unknown) => void): void;
  send(data: Uint8Array<ArrayBuffer>): void;
  close(code?: number, reason?: string): void;
}

export type WebSocketFactory = (
  url: string,
  protocol: string,
) => WebSocketLike;

interface ReceiveQueueLimits {
  /** Maximum queued physical records. A control frame consumes one unit. */
  readonly maxUnits: number;
  /** Maximum estimated encoded bytes across queued server frames. */
  readonly maxBytes: number;
}

interface QueuedServerFrame {
  readonly frame: ServerFrame;
  readonly bytes: number;
  readonly units: number;
}

export interface SocketPolicy {
  readonly webSocketFactory: WebSocketFactory;
  readonly webSocketConnectTimeoutMs: number;
  readonly webSocketProgressTimeoutMs: number;
  readonly boundedOperationAttempts: number;
}

export async function connectSocket(
  url: string,
  policy: SocketPolicy,
  signal?: AbortSignal,
  openingFrame?: ClientFrame,
): Promise<FrameSocket> {
  const openingMessage = openingFrame === undefined
    ? undefined
    : webSocketBytes(encodeClientFrame(openingFrame));
  let websocket: WebSocketLike;
  try {
    websocket = policy.webSocketFactory(url, TSF_WEBSOCKET_PROTOCOL);
  } catch (cause) {
    throw new TsfClientError("websocket_connect", "could not create WebSocket", {
      cause,
    });
  }
  const socket = new FrameSocket(websocket, undefined, openingMessage);
  try {
    await withTimeout(
      socket.opened,
      policy.webSocketConnectTimeoutMs,
      "connect WebSocket",
      signal,
    );
    if (websocket.protocol !== TSF_WEBSOCKET_PROTOCOL) {
      throw new TsfClientError(
        "unexpected_websocket_protocol",
        `server selected WebSocket protocol ${JSON.stringify(websocket.protocol)}`,
      );
    }
    signal?.throwIfAborted();
    return socket;
  } catch (error) {
    socket.close();
    throw error;
  }
}

export class FrameSocket {
  public readonly opened: Promise<void>;
  readonly #queue: QueuedServerFrame[] = [];
  readonly #limits: ReceiveQueueLimits;
  #queuedBytes = 0;
  #queuedUnits = 0;
  #waiting:
    | {
        readonly resolve: (frame: ServerFrame) => void;
        readonly reject: (error: unknown) => void;
      }
    | undefined;
  #terminalError: Error | undefined;
  #messagePipeline = Promise.resolve();

  public constructor(
    private readonly websocket: WebSocketLike,
    limits: ReceiveQueueLimits = {
      maxUnits: DEFAULT_MAX_RECEIVE_QUEUE_UNITS,
      maxBytes: DEFAULT_MAX_RECEIVE_QUEUE_BYTES,
    },
    openingMessage?: Uint8Array<ArrayBuffer>,
  ) {
    if (
      !Number.isSafeInteger(limits.maxUnits) ||
      limits.maxUnits <= 0 ||
      !Number.isSafeInteger(limits.maxBytes) ||
      limits.maxBytes <= 0
    ) {
      throw new TsfClientError(
        "invalid_client_option",
        "WebSocket receive queue limits must be positive integers",
      );
    }
    this.#limits = limits;
    websocket.binaryType = "arraybuffer";
    websocket.addEventListener("message", (event) => {
      this.#messagePipeline = this.#messagePipeline
        .then(async () => this.#push(decodeServerFrame(
          await messageBytes(messageEventData(event)),
        )))
        .catch((error: unknown) => {
          this.#finish(error);
          try {
            websocket.close(1002, "invalid server frame");
          } catch {
            // The decode error remains the useful terminal error.
          }
        });
    });
    websocket.addEventListener("close", (event) => {
      void this.#messagePipeline.finally(() => this.#finish(closeError(event)));
    });
    this.opened = opened(websocket, openingMessage);
  }

  public send(frame: ClientFrame): void {
    if (this.websocket.readyState !== OPEN_READY_STATE) {
      throw new TsfWebSocketClosedError(1006, "socket is not open", false);
    }
    try {
      this.websocket.send(webSocketBytes(encodeClientFrame(frame)));
    } catch (cause) {
      throw new TsfClientError("websocket_send", "WebSocket send failed", {
        cause,
      });
    }
  }

  public nextFrame(): Promise<ServerFrame> {
    const queued = this.#queue.shift();
    if (queued !== undefined) {
      this.#queuedBytes -= queued.bytes;
      this.#queuedUnits -= queued.units;
      return Promise.resolve(queued.frame);
    }
    if (this.#terminalError !== undefined) {
      return Promise.reject(this.#terminalError);
    }
    if (this.#waiting !== undefined) {
      return Promise.reject(
        new TsfClientError(
          "concurrent_socket_read",
          "only one WebSocket frame read may be pending",
        ),
      );
    }
    return new Promise<ServerFrame>((resolve, reject) => {
      this.#waiting = { resolve, reject };
    });
  }

  public close(): void {
    this.#finish(
      new TsfWebSocketClosedError(NORMAL_CLOSE_CODE, "client closing", true),
    );
    try {
      if (this.websocket.readyState === CONNECTING_READY_STATE) {
        this.websocket.close();
      } else if (this.websocket.readyState === OPEN_READY_STATE) {
        this.websocket.close(NORMAL_CLOSE_CODE, "client closing");
      }
    } catch {
      // The local terminal state already settles pending work.
    }
  }

  #push(frame: ServerFrame): void {
    if (this.#terminalError !== undefined) {
      return;
    }
    if (this.#waiting !== undefined) {
      const waiting = this.#waiting;
      this.#waiting = undefined;
      waiting.resolve(frame);
    } else {
      const bytes = serverFrameBytes(frame);
      const units = serverFrameQueueUnits(frame);
      if (
        units > this.#limits.maxUnits - this.#queuedUnits ||
        bytes > this.#limits.maxBytes - this.#queuedBytes
      ) {
        this.#finish(
          new TsfClientError(
            "client_receive_overload",
            "WebSocket receive buffer reached its bounded limit",
          ),
        );
        try {
          this.websocket.close(1013, "client receive buffer full");
        } catch {
          // The bounded client error remains the actionable failure.
        }
        return;
      }
      this.#queue.push({ frame, bytes, units });
      this.#queuedBytes += bytes;
      this.#queuedUnits += units;
    }
  }

  #finish(error: unknown): void {
    if (this.#terminalError !== undefined) {
      return;
    }
    const terminalError = asError(error);
    this.#terminalError = terminalError;
    if (this.#waiting !== undefined && this.#queue.length === 0) {
      const waiting = this.#waiting;
      this.#waiting = undefined;
      waiting.reject(terminalError);
    }
  }
}

export async function expectReady(socket: FrameSocket): Promise<void> {
  const frame = await socket.nextFrame();
  if (frame.type !== "ready") {
    throw unexpectedFrame(frame);
  }
}

export async function expectReadHandshake(
  socket: FrameSocket,
): Promise<StreamMetadata> {
  await expectReady(socket);
  const info = await socket.nextFrame();
  if (info.type !== "streamMetadata") {
    throw unexpectedFrame(info);
  }
  return info.stream;
}

export function unexpectedFrame(frame: ServerFrame): TsfClientError {
  return new TsfClientError(
    "unexpected_server_frame",
    `server sent unexpected ${frame.type} frame`,
  );
}

export function dataPlaneUrl(
  apiOrigin: string,
  streamId: StreamId,
  operation:
    | "read"
    | "write"
    | "terminal/input/read"
    | "terminal/input/write"
    | "terminal/output/read"
    | "terminal/output/write",
): string {
  const url = new URL(
    `/api/v1/streams/${streamId}/${operation}`,
    apiOrigin,
  );
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  return url.href;
}

export function isRetryableSocketError(error: unknown): boolean {
  return (
    (error instanceof TsfWebSocketClosedError &&
      RETRYABLE_CLOSE_CODES.has(error.closeCode)) ||
    (error instanceof TsfClientError &&
      [
        "websocket_connect",
        "websocket_send",
        "operation_timeout",
        "client_receive_overload",
      ].includes(error.code))
  );
}

export async function reconnectSocket(
  connect: () => Promise<FrameSocket>,
  policy: SocketPolicy,
  attempts: number,
  delayMs: number,
  initialError: unknown,
  signal?: AbortSignal,
  attemptLimit = policy.boundedOperationAttempts,
): Promise<{
  readonly socket: FrameSocket;
  readonly attempts: number;
  readonly nextDelayMs: number;
}> {
  let lastError = initialError;
  let nextDelayMs = delayMs;
  signal?.throwIfAborted();
  while (attempts + 1 < attemptLimit) {
    attempts += 1;
    await sleep(jitteredBackoffMs(nextDelayMs), signal);
    nextDelayMs = Math.min(nextDelayMs * 2, MAX_RETRY_BACKOFF_MS);
    try {
      const socket = await connect();
      if (signal?.aborted) {
        socket.close();
        signal.throwIfAborted();
      }
      return { socket, attempts, nextDelayMs };
    } catch (error) {
      signal?.throwIfAborted();
      if (!isRetryableSocketError(error)) {
        throw error;
      }
      lastError = error;
    }
  }
  throw asError(lastError);
}

export async function connectInitialSocket(
  connect: () => Promise<FrameSocket>,
  policy: SocketPolicy,
  signal?: AbortSignal,
): Promise<FrameSocket> {
  signal?.throwIfAborted();
  try {
    const socket = await connect();
    if (signal?.aborted) {
      socket.close();
      signal.throwIfAborted();
    }
    return socket;
  } catch (error) {
    signal?.throwIfAborted();
    if (!isRetryableSocketError(error)) {
      throw error;
    }
    return (
      await reconnectSocket(
        connect,
        policy,
        0,
        INITIAL_RETRY_BACKOFF_MS,
        error,
        signal,
      )
    ).socket;
  }
}

export async function withTimeout<T>(
  promise: Promise<T>,
  timeoutMs: number,
  operation: string,
  signal?: AbortSignal,
  options?: {
    readonly error?: Error;
    readonly onTimeout?: () => void;
  },
): Promise<T> {
  signal?.throwIfAborted();
  let timer: ReturnType<typeof setTimeout> | undefined;
  let abort: (() => void) | undefined;
  const deadline = new Promise<never>((_, reject) => {
    timer = setTimeout(() => {
      options?.onTimeout?.();
      reject(
        options?.error ??
          new TsfClientError(
            "operation_timeout",
            `${operation} timed out after ${timeoutMs}ms`,
          ),
      );
    }, timeoutMs);
    if (signal !== undefined) {
      abort = () => reject(asError(signal.reason));
      signal.addEventListener("abort", abort, { once: true });
    }
  });
  try {
    return await Promise.race([promise, deadline]);
  } finally {
    if (timer !== undefined) {
      clearTimeout(timer);
    }
    if (abort !== undefined) {
      signal?.removeEventListener("abort", abort);
    }
  }
}

export function requireLinkSecret(secret: string): string {
  try {
    return parseLinkSecret(secret);
  } catch (cause) {
    throw new TsfClientError(
      "invalid_link_secret",
      "link secret must be a canonical 24-byte unpadded base64url secret",
      { cause },
    );
  }
}

export function u64(value: bigint, name: string): bigint {
  if (value < 0n || value > MAX_U64) {
    throw new TsfClientError(
      "invalid_u64",
      `${name} must be an unsigned 64-bit integer`,
    );
  }
  return value;
}

function opened(
  websocket: WebSocketLike,
  openingMessage?: Uint8Array<ArrayBuffer>,
): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    const sendOpeningMessage = () => {
      if (openingMessage !== undefined) {
        if (websocket.protocol !== TSF_WEBSOCKET_PROTOCOL) {
          throw new TsfClientError(
            "unexpected_websocket_protocol",
            `server selected WebSocket protocol ${JSON.stringify(websocket.protocol)}`,
          );
        }
        websocket.send(openingMessage);
      }
    };
    if (websocket.readyState === OPEN_READY_STATE) {
      try {
        sendOpeningMessage();
        resolve();
      } catch (cause) {
        reject(openingSendError(cause));
      }
      return;
    }
    const succeeded = () => {
      cleanup();
      try {
        sendOpeningMessage();
        resolve();
      } catch (cause) {
        reject(openingSendError(cause));
      }
    };
    const failed = () => {
      cleanup();
      reject(new TsfClientError("websocket_connect", "WebSocket connection failed"));
    };
    const closed = (event: unknown) => {
      cleanup();
      reject(closeError(event));
    };
    const cleanup = () => {
      websocket.removeEventListener("open", succeeded);
      websocket.removeEventListener("error", failed);
      websocket.removeEventListener("close", closed);
    };
    websocket.addEventListener("open", succeeded);
    websocket.addEventListener("error", failed);
    websocket.addEventListener("close", closed);
  });
}

function openingSendError(cause: unknown): TsfClientError {
  return cause instanceof TsfClientError
    ? cause
    : new TsfClientError("websocket_send", "WebSocket send failed", { cause });
}

function closeError(event: unknown): TsfWebSocketClosedError {
  if (
    typeof event === "object" &&
    event !== null &&
    "code" in event &&
    typeof event.code === "number" &&
    "reason" in event &&
    typeof event.reason === "string" &&
    "wasClean" in event &&
    typeof event.wasClean === "boolean"
  ) {
    return new TsfWebSocketClosedError(event.code, event.reason, event.wasClean);
  }
  return new TsfWebSocketClosedError(1006, "invalid WebSocket close event", false);
}

function messageEventData(event: unknown): unknown {
  return typeof event === "object" && event !== null && "data" in event
    ? event.data
    : undefined;
}

async function messageBytes(data: unknown): Promise<Uint8Array> {
  if (data instanceof ArrayBuffer) {
    return new Uint8Array(data);
  }
  if (ArrayBuffer.isView(data)) {
    return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
  }
  if (typeof Blob !== "undefined" && data instanceof Blob) {
    return new Uint8Array(await data.arrayBuffer());
  }
  if (typeof data === "string") {
    throw new TsfClientError(
      "unexpected_text_message",
      "server sent a text WebSocket message",
    );
  }
  throw new TsfClientError(
    "invalid_websocket_message",
    "server sent an unsupported WebSocket message",
  );
}

function webSocketBytes(bytes: Uint8Array): Uint8Array<ArrayBuffer> {
  if (!(bytes.buffer instanceof ArrayBuffer)) {
    return bytes.slice();
  }
  return new Uint8Array(bytes.buffer, bytes.byteOffset, bytes.byteLength);
}

function serverFrameBytes(frame: ServerFrame): number {
  if (frame.type === "readBatch") {
    return frame.records.reduce(
      (bytes, record) => bytes + record.data.byteLength + 49,
      1,
    );
  }
  if (frame.type === "streamMetadata") {
    return JSON.stringify(frame.stream).length + 64;
  }
  return 64;
}

function serverFrameQueueUnits(frame: ServerFrame): number {
  return frame.type === "readBatch" ? frame.records.length : 1;
}

export function sleep(durationMs: number, signal?: AbortSignal): Promise<void> {
  if (signal === undefined) {
    return new Promise((resolve) => setTimeout(resolve, durationMs));
  }
  signal.throwIfAborted();
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      signal.removeEventListener("abort", aborted);
      resolve();
    }, durationMs);
    const aborted = () => {
      clearTimeout(timer);
      reject(asError(signal.reason));
    };
    signal.addEventListener("abort", aborted, { once: true });
  });
}

function asError(error: unknown): Error {
  return error instanceof Error
    ? error
    : new TsfClientError("websocket_error", "WebSocket failed", {
        cause: error,
      });
}
