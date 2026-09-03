import { TsfClientError } from "./errors.js";

export const MAX_TIMER_DELAY_MS = 2_147_483_647;
export const INITIAL_RETRY_BACKOFF_MS = 200;
export const MAX_RETRY_BACKOFF_MS = 2_000;

export function integerOption(
  value: number,
  name: string,
  minimum = 0,
  maximum = Number.MAX_SAFE_INTEGER,
): number {
  if (!Number.isSafeInteger(value) || value < minimum || value > maximum) {
    throw new TsfClientError(
      "invalid_client_option",
      `${name} must be an integer between ${minimum} and ${maximum}`,
    );
  }
  return value;
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

export function jitteredBackoffMs(backoffMs: number): number {
  if (backoffMs === 0) {
    return 0;
  }
  return Math.min(
    MAX_RETRY_BACKOFF_MS,
    Math.floor(backoffMs * (0.5 + Math.random())),
  );
}

export function isRetryableHttpStatus(status: number): boolean {
  return [408, 425, 429, 500, 502, 503, 504].includes(status);
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

export function asError(error: unknown): Error {
  return error instanceof Error
    ? error
    : new TsfClientError("websocket_error", "WebSocket failed", {
        cause: error,
      });
}
