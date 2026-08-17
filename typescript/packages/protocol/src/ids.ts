import { ProtocolError } from "./errors.js";

const CROCKFORD_ALPHABET = "0123456789abcdefghjkmnpqrstvwxyz";
const CROCKFORD_ALIASES: Readonly<Record<string, string>> = {
  i: "1",
  l: "1",
  o: "0",
};

export const STREAM_ID_BYTE_LENGTH = 20;
export const WRITER_ID_BYTE_LENGTH = 16;
export const MAX_LINK_ID_LENGTH = 64;

declare const streamIdBrand: unique symbol;
declare const linkIdBrand: unique symbol;
declare const clientWriterIdBrand: unique symbol;
declare const writerIdBrand: unique symbol;

export type StreamId = string & { readonly [streamIdBrand]: true };
export type LinkId = string & { readonly [linkIdBrand]: true };
/** Client-chosen identity reused by one writer across reconnects. */
export type ClientWriterId = Uint8Array & {
  readonly [clientWriterIdBrand]: true;
};
/** Server-derived identity attached to delivered records. */
export type WriterId = Uint8Array & { readonly [writerIdBrand]: true };

export function parseStreamId(input: string): StreamId {
  return canonicalizeUbid(input) as StreamId;
}

export function parseLinkId(input: string): LinkId {
  if (
    input.length > MAX_LINK_ID_LENGTH ||
    !/^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$/.test(input)
  ) {
    throw new ProtocolError(
      "invalid_link_id",
      "link ID must be 1 to 64 lowercase letters, digits, or hyphens, without a leading or trailing hyphen",
    );
  }
  return input as LinkId;
}

export function generateStreamId(): StreamId {
  return streamIdFromBytes(randomBytes(STREAM_ID_BYTE_LENGTH));
}

export function streamIdFromBytes(bytes: Uint8Array): StreamId {
  if (bytes.byteLength !== STREAM_ID_BYTE_LENGTH) {
    throw new ProtocolError(
      "invalid_stream_id_bytes",
      `stream ID must be ${STREAM_ID_BYTE_LENGTH} bytes`,
    );
  }
  return encodeUbid(bytes) as StreamId;
}

export function generateClientWriterId(): ClientWriterId {
  return randomBytes(WRITER_ID_BYTE_LENGTH) as ClientWriterId;
}

export function parseClientWriterId(input: Uint8Array): ClientWriterId {
  return parseExactWriterId(
    input,
    "client writer ID",
    "invalid_client_writer_id",
  ) as ClientWriterId;
}

export function parseWriterId(input: Uint8Array): WriterId {
  return parseExactWriterId(input, "writer ID", "invalid_writer_id") as WriterId;
}

function parseExactWriterId(
  input: Uint8Array,
  name: string,
  code: string,
): Uint8Array {
  validateWriterIdLength(input, name, code);
  return input.slice();
}

export function validateWriterIdLength(
  input: Uint8Array,
  name: string,
  code: string,
): void {
  if (input.byteLength !== WRITER_ID_BYTE_LENGTH) {
    throw new ProtocolError(
      code,
      `${name} must be ${WRITER_ID_BYTE_LENGTH} bytes`,
    );
  }
}

export function writerIdKey(writerId: WriterId): string {
  validateWriterIdLength(writerId, "writer ID", "invalid_writer_id");
  return Array.from(writerId, (byte) =>
    byte.toString(16).padStart(2, "0"),
  ).join("");
}

function canonicalizeUbid(input: string): string {
  const encodedLength = (STREAM_ID_BYTE_LENGTH / 5) * 8;
  if (input.length !== encodedLength) {
    throw new ProtocolError(
      "invalid_ubid_length",
      `UBID must be ${encodedLength} characters`,
    );
  }

  let canonical = "";
  for (const original of input) {
    const lower = original.toLowerCase();
    const normalized = CROCKFORD_ALIASES[lower] ?? lower;
    if (!CROCKFORD_ALPHABET.includes(normalized)) {
      throw new ProtocolError(
        "invalid_ubid_character",
        `invalid UBID character ${JSON.stringify(original)}`,
      );
    }
    canonical += normalized;
  }
  return canonical;
}

function encodeUbid(bytes: Uint8Array): string {
  let output = "";
  for (let offset = 0; offset < bytes.byteLength; offset += 5) {
    let value = 0n;
    for (const byte of bytes.subarray(offset, offset + 5)) {
      value = (value << 8n) | BigInt(byte);
    }
    for (let shift = 35n; shift >= 0n; shift -= 5n) {
      output += CROCKFORD_ALPHABET[Number((value >> shift) & 0x1fn)];
    }
  }
  return output;
}

export function randomBytes(length: number): Uint8Array {
  const bytes = new Uint8Array(length);
  crypto.getRandomValues(bytes);
  return bytes;
}
