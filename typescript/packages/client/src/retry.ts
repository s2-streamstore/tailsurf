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
