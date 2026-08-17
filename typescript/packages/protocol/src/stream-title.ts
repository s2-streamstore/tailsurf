import { ProtocolError } from "./errors.js";

export const MAX_STREAM_TITLE_CODE_POINTS = 120;

declare const streamTitleBrand: unique symbol;

export type StreamTitle = string & { readonly [streamTitleBrand]: true };

export function parseStreamTitle(input: string): StreamTitle {
  if (!input.isWellFormed()) {
    throw new ProtocolError(
      "invalid_stream_title_character",
      "stream title must contain well-formed Unicode",
    );
  }
  const length = Array.from(input).length;
  if (length === 0 || length > MAX_STREAM_TITLE_CODE_POINTS) {
    throw new ProtocolError(
      "invalid_stream_title_length",
      `stream title must contain 1 to ${MAX_STREAM_TITLE_CODE_POINTS} Unicode code points`,
    );
  }
  if (input.trim() !== input) {
    throw new ProtocolError(
      "invalid_stream_title_whitespace",
      "stream title must not have leading or trailing whitespace",
    );
  }
  if (/\p{Cc}|\p{Zl}|\p{Zp}/u.test(input)) {
    throw new ProtocolError(
      "invalid_stream_title_character",
      "stream title must not contain control characters or line breaks",
    );
  }
  return input as StreamTitle;
}
