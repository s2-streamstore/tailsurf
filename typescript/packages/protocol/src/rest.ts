// zod/mini keeps the tree-shaken server bundle ~400KB smaller than classic zod.
import * as z from "zod/mini";

import { parseLinkId, parseStreamId } from "./ids.js";
import { parseLinkPermissions } from "./permissions.js";
import {
  canonicalBase64url,
  decodeBase64url,
  encodeBase64url,
  MAX_SAFE_INTEGER_U64,
  MAX_U64,
  U64_PATTERN,
} from "./primitives.js";
import { parseLinkSecret } from "./stream-url.js";
import { parseStreamTitle } from "./stream-title.js";

const BASE64URL_PATTERN = /^[A-Za-z0-9_-]*$/;
const IDEMPOTENCY_KEY_PATTERN = /^[A-Za-z0-9_-]{43}$/;
const WRITER_ID_BASE64URL_PATTERN = /^[A-Za-z0-9_-]{22}$/;
const TEXT_PAYLOAD_JSON_OVERHEAD = '"text":""'.length;
const BYTES_PAYLOAD_JSON_OVERHEAD = '"bytes":""'.length;

export const MAX_STATELESS_APPEND_RECORDS = 128;
export const MAX_STATELESS_APPEND_PAYLOAD_BYTES = 900 * 1024;
export const MAX_STATELESS_APPEND_JSON_BYTES = 1_300_000;
export const MAX_REST_RESPONSE_BYTES = 2 * 1024 * 1024;
export const MAX_REST_ERROR_RESPONSE_BYTES = 64 * 1024;
export const MAX_LINK_PAGE_ITEMS = 100;
export const MAX_INITIAL_STREAM_LINKS = 3;
export const IDEMPOTENCY_KEY_BYTES = 32;
export const MAX_SSE_READ_BATCH_RECORDS = 1_000;
export const MAX_SSE_READ_BATCH_PAYLOAD_BYTES = 1024 * 1024;
export const MAX_SSE_EVENT_BYTES = 2 * 1024 * 1024;
export const MAX_SSE_UNTERMINATED_EVENT_BYTES = 2 * 1024 * 1024;

function transformedString<T>(parser: (input: string) => T) {
  return z.pipe(
    z.string(),
    z.transform((input, payload) => {
      try {
        return parser(input);
      } catch (error) {
        payload.issues.push({
          code: "custom",
          message: error instanceof Error ? error.message : "invalid value",
          input,
        });
        return z.NEVER;
      }
    }),
  );
}

export const streamIdSchema = transformedString(parseStreamId);
export const linkIdSchema = transformedString(parseLinkId);
export const streamTitleSchema = transformedString(parseStreamTitle);
export const linkPermissionsSchema = transformedString(parseLinkPermissions);
export const visibilitySchema = z.enum(["private", "public"]);
export const streamKindSchema = z.enum(["transcript", "bytes", "terminal"]);
export const jsonU64Schema = z.number().check(
  z.int(),
  z.nonnegative(),
  z.maximum(Number.MAX_SAFE_INTEGER),
);
export const streamTimestampSchema = z.iso.datetime({ offset: true });
export const decimalU64Schema = z.string().check(
  z.regex(U64_PATTERN),
  z.refine(
    (value) => BigInt(value) <= MAX_U64,
    "value must fit in an unsigned 64-bit integer",
  ),
);
export const decimalSafeU64Schema = decimalU64Schema.check(z.refine(
  (value) => BigInt(value) <= MAX_SAFE_INTEGER_U64,
  `value must not exceed ${MAX_SAFE_INTEGER_U64}`,
));
export const clientWriterIdBase64urlSchema = z.string().check(
  z.regex(WRITER_ID_BASE64URL_PATTERN),
  z.refine(
    (value) => canonicalBase64url(value, 16),
    "writer id must be 16 bytes encoded as unpadded base64url",
  ),
);
export const writerIdBase64urlSchema = z.string().check(
  z.regex(WRITER_ID_BASE64URL_PATTERN),
  z.refine(
    (value) => canonicalBase64url(value, 16),
    "writer_id must be 16 bytes encoded as unpadded base64url",
  ),
);
export const bytesBase64urlSchema = z.string().check(
  z.regex(BASE64URL_PATTERN),
  z.refine(
    (value) => canonicalBase64url(value),
    "value must be canonical unpadded base64url",
  ),
);

export const initialStreamLinkSchema = z.strictObject({
  link_id: linkIdSchema,
  permissions: linkPermissionsSchema,
});

export const createStreamRequestSchema = z.strictObject({
  kind: z.optional(streamKindSchema),
  title: z.optional(streamTitleSchema),
  visibility: z._default(visibilitySchema, "private"),
  expires_in_seconds: z.optional(z.number().check(
    z.int(),
    z.positive(),
    z.maximum(Number.MAX_SAFE_INTEGER),
  )),
  links: z.array(initialStreamLinkSchema).check(
    z.minLength(1),
    z.maxLength(MAX_INITIAL_STREAM_LINKS),
  ),
}).check(z.superRefine((request, context) => {
  if (!request.links.some((link) => link.permissions === "o")) {
    context.addIssue({
      code: "custom",
      path: ["links"],
      message: "links must contain an owner link",
    });
  }
  const linkIds = new Set(request.links.map((link) => link.link_id));
  if (linkIds.size !== request.links.length) {
    context.addIssue({
      code: "custom",
      path: ["links"],
      message: "links must contain unique link IDs",
    });
  }
}));

export const streamLinkCredentialSchema = z.object({
  link_id: linkIdSchema,
  permissions: linkPermissionsSchema,
  secret: transformedString(parseLinkSecret),
});

export const streamMetadataSchema = z.object({
  stream_id: streamIdSchema,
  kind: streamKindSchema,
  title: z.nullable(streamTitleSchema),
  visibility: visibilitySchema,
  created_at: streamTimestampSchema,
  expires_at: streamTimestampSchema,
});

export const webOriginSchema = z.url();

export const createStreamResponseSchema = z.extend(streamMetadataSchema, {
  web_origin: webOriginSchema,
  links: z.array(streamLinkCredentialSchema),
});

export const createLinkResponseSchema = z.extend(streamLinkCredentialSchema, {
  web_origin: webOriginSchema,
});

export const createLinkRequestSchema = z.strictObject({
  permissions: linkPermissionsSchema,
  expires_at: z.optional(z.iso.datetime({ offset: true })),
});

export const streamLinkStatusSchema = z.enum(["active", "expired", "revoked"]);

export const streamLinkSummarySchema = z.object({
  link_id: linkIdSchema,
  permissions: linkPermissionsSchema,
  status: streamLinkStatusSchema,
  created_at: z.iso.datetime({ offset: true }),
  expires_at: z.nullable(z.iso.datetime({ offset: true })),
  revoked_at: z.nullable(z.iso.datetime({ offset: true })),
});

export const listLinksResponseSchema = z.object({
  authorizing_link_id: linkIdSchema,
  links: z.array(streamLinkSummarySchema),
  next_cursor: z.nullable(z.string().check(z.minLength(1))),
});

export const updateStreamRequestSchema = z.strictObject({
  title: z.optional(z.nullable(streamTitleSchema)),
  visibility: z.optional(visibilitySchema),
  expires_at: z.optional(streamTimestampSchema),
}).check(z.refine((request) => Object.keys(request).length !== 0, {
  message: "stream patch must contain at least one field",
}));

export const appendPartSchema = z.strictObject({
  index: z.number().check(z.int(), z.nonnegative(), z.maximum(0x7fff_ffff)),
  is_final: z.boolean(),
});

const ssePartSchema = z.object({
  index: z.number().check(z.int(), z.nonnegative(), z.maximum(0x7fff_ffff)),
  is_final: z.boolean(),
});

// A record's payload key is only its JSON representation: `text` carries UTF-8
// directly and `bytes` carries canonical base64url. The stream kind defines
// how consumers interpret the decoded bytes.
export const appendJsonRecordSchema = z.strictObject({
  part: z.optional(appendPartSchema),
  text: z.optional(z.string()),
  bytes: z.optional(bytesBase64urlSchema),
}).check(z.refine(
  (record) => (record.text === undefined) !== (record.bytes === undefined),
  "a record carries exactly one of text or bytes",
));

// Writer identity is one optional value: an id and the writer-local
// sequence assigned to the first record travel together or not at all.
export const appendWriterSchema = z.strictObject({
  id: clientWriterIdBase64urlSchema,
  seq_num: decimalU64Schema,
});

export const appendRecordsRequestSchema = z.strictObject({
  writer: z.optional(appendWriterSchema),
  records: z.array(appendJsonRecordSchema).check(
    z.minLength(1),
    z.maxLength(MAX_STATELESS_APPEND_RECORDS),
  ),
  expected_next_seq_num: z.optional(decimalSafeU64Schema),
});

export const appendRangeSchema = z.object({
  start_seq_num: decimalU64Schema,
  end_seq_num: decimalU64Schema,
});

export const apiErrorSchema = z.object({
  code: z.string(),
  message: z.string(),
  request_id: z.string(),
  retry_after_ms: z.optional(z.number().check(z.int(), z.nonnegative())),
  actual_next_seq_num: z.optional(decimalSafeU64Schema),
});

export const apiErrorResponseSchema = z.object({
  error: apiErrorSchema,
});

export const sseReadRecordSchema = z.object({
  seq_num: decimalU64Schema,
  timestamp_ms: decimalU64Schema,
  writer: z.object({
    id: writerIdBase64urlSchema,
    seq_num: decimalU64Schema,
  }),
  part: z.optional(ssePartSchema),
  text: z.optional(z.string()),
  bytes: z.optional(bytesBase64urlSchema),
}).check(z.refine(
  (record) => (record.text === undefined) !== (record.bytes === undefined),
  "a record carries exactly one of text or bytes",
));

export const sseReadBatchDataSchema = z.object({
  records: z.array(sseReadRecordSchema).check(
    z.minLength(1),
    z.maxLength(MAX_SSE_READ_BATCH_RECORDS),
  ),
});

export const sseCaughtUpDataSchema = z.object({
  next_seq_num: decimalU64Schema,
  last_timestamp_ms: decimalU64Schema,
});

export type Visibility = z.infer<typeof visibilitySchema>;
export type StreamKind = z.infer<typeof streamKindSchema>;
export type InitialStreamLink = z.infer<typeof initialStreamLinkSchema>;
export type CreateStreamRequest = z.infer<typeof createStreamRequestSchema>;
export type StreamLinkCredential = z.infer<typeof streamLinkCredentialSchema>;
export type CreateStreamResponse = z.infer<typeof createStreamResponseSchema>;
export type CreateLinkResponse = z.infer<typeof createLinkResponseSchema>;
export type CreateLinkRequestInput = z.input<typeof createLinkRequestSchema>;
export type CreateLinkRequest = z.infer<typeof createLinkRequestSchema>;
export type StreamLinkSummary = z.infer<typeof streamLinkSummarySchema>;
export type ListLinksResponse = z.infer<typeof listLinksResponseSchema>;
export type StreamMetadata = z.infer<typeof streamMetadataSchema>;
export type UpdateStreamRequest = z.infer<typeof updateStreamRequestSchema>;
export type RecordPayload =
  | { readonly text: string; readonly bytes?: undefined }
  | { readonly bytes: string; readonly text?: undefined };
export type AppendRange = z.infer<typeof appendRangeSchema>;
export type ApiError = z.infer<typeof apiErrorSchema>;
export type SseReadRecord = z.infer<typeof sseReadRecordSchema>;

const utf8Decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
const utf8Encoder = new TextEncoder();

/** Chooses the smaller JSON payload key while preserving the exact record bytes. */
export function compactRecordPayload(bytes: Uint8Array): RecordPayload {
  let text: string;
  try {
    text = utf8Decoder.decode(bytes);
  } catch {
    return { bytes: encodeBase64url(bytes) };
  }
  const textByteLength = TEXT_PAYLOAD_JSON_OVERHEAD +
    jsonByteLength(text) - '""'.length;
  const bytesByteLength = BYTES_PAYLOAD_JSON_OVERHEAD +
    Math.ceil(bytes.byteLength * 4 / 3);
  return textByteLength <= bytesByteLength
    ? { text }
    : { bytes: encodeBase64url(bytes) };
}

/** The exact payload bytes named by a record's `text` or `bytes` key. */
export function recordPayloadBytes(record: {
  readonly text?: string | undefined;
  readonly bytes?: string | undefined;
}): Uint8Array {
  return record.text === undefined
    ? decodeBase64url(record.bytes ?? "")
    : utf8Encoder.encode(record.text);
}

function jsonByteLength(value: unknown): number {
  return utf8Encoder.encode(JSON.stringify(value)).byteLength;
}

export function isCanonicalIdempotencyKey(value: string): boolean {
  return IDEMPOTENCY_KEY_PATTERN.test(value) &&
    canonicalBase64url(value, IDEMPOTENCY_KEY_BYTES);
}
