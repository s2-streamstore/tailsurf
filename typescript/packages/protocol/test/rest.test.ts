import { readFileSync } from "node:fs";

import { describe, expect, it } from "vitest";

import {
  createStreamRequestSchema,
  createStreamResponseSchema,
  decodeSseResumeCursor,
  encodeSseResumeCursor,
  createLinkRequestSchema,
  createLinkResponseSchema,
  listLinksResponseSchema,
  streamTitleSchema,
  updateStreamRequestSchema,
  streamMetadataSchema,
  appendRecordsRequestSchema,
  appendRangeSchema,
  apiErrorResponseSchema,
  compactRecordPayload,
  recordPayloadBytes,
  resolvedRecordFormat,
  sseReadBatchDataSchema,
  sseCaughtUpDataSchema,
  MAX_SSE_EVENT_BYTES,
  MAX_REST_ERROR_RESPONSE_BYTES,
  MAX_REST_RESPONSE_BYTES,
  MAX_INITIAL_STREAM_LINKS,
  MAX_LINK_PAGE_ITEMS,
  MAX_SSE_UNTERMINATED_EVENT_BYTES,
  MAX_SSE_READ_BATCH_PAYLOAD_BYTES,
  MAX_SSE_READ_BATCH_RECORDS,
  MAX_STATELESS_APPEND_JSON_BYTES,
  MAX_STATELESS_APPEND_PAYLOAD_BYTES,
  MAX_STATELESS_APPEND_RECORDS,
} from "../src/index.js";

const fixtures = JSON.parse(readFileSync(
  new URL("../fixtures/rest-v1.json", import.meta.url),
  "utf8",
)) as Record<string, unknown>;

describe("REST schemas", () => {
  it("pins REST and SSE transport limits", () => {
    expect(fixtures).toMatchObject({
      max_stateless_append_records: MAX_STATELESS_APPEND_RECORDS,
      max_stateless_append_payload_bytes: MAX_STATELESS_APPEND_PAYLOAD_BYTES,
      max_stateless_append_json_bytes: MAX_STATELESS_APPEND_JSON_BYTES,
      max_rest_response_bytes: MAX_REST_RESPONSE_BYTES,
      max_rest_error_response_bytes: MAX_REST_ERROR_RESPONSE_BYTES,
      max_link_page_items: MAX_LINK_PAGE_ITEMS,
      max_initial_stream_links: MAX_INITIAL_STREAM_LINKS,
      max_sse_read_batch_records: MAX_SSE_READ_BATCH_RECORDS,
      max_sse_read_batch_payload_bytes: MAX_SSE_READ_BATCH_PAYLOAD_BYTES,
      max_sse_event_bytes: MAX_SSE_EVENT_BYTES,
      max_sse_unterminated_event_bytes: MAX_SSE_UNTERMINATED_EVENT_BYTES,
    });
  });

  it("chooses the smaller exact JSON payload key", () => {
    const encoder = new TextEncoder();
    const inputs = [
      "plain deployment output\n",
      'quotes " and slashes \\\\ and tabs\t',
      "\u0000\u0001\b\f\n\r\t",
      "κόσμε 😀 \u2028 \u2029",
    ];
    expect(inputs.map((value) => compactRecordPayload(encoder.encode(value))))
      .toEqual([
        { text: inputs[0] },
        { text: inputs[1] },
        { bytes: "AAEIDAoNCQ" },
        { text: inputs[3] },
      ]);
    for (const value of inputs) {
      expect(recordPayloadBytes(compactRecordPayload(encoder.encode(value))))
        .toEqual(encoder.encode(value));
    }
  });

  it("implies the format from the payload key", () => {
    expect(resolvedRecordFormat({ text: "hello\n" })).toBe("transcript");
    expect(resolvedRecordFormat({ bytes: "AP8" } as never)).toBe("bytes");
    expect(resolvedRecordFormat({ format: "transcript", bytes: "AP8" } as never))
      .toBe("transcript");
  });

  it("decodes shared additive response fixtures", () => {
    expect(createStreamRequestSchema.parse(fixtures.create_request).links)
      .toHaveLength(2);
    expect(streamMetadataSchema.parse(fixtures.stream_resource)).not
      .toHaveProperty("future_field");
    const createdStream = createStreamResponseSchema.parse(fixtures.create_response);
    expect(createdStream.web_origin).toBe("https://tail.surf");
    expect(createdStream.links).toHaveLength(1);
    expect(createdStream).not.toHaveProperty("future_field");
    const createdLink = createLinkResponseSchema.parse(fixtures.create_link_response);
    expect(createdLink.web_origin).toBe("https://tail.surf");
    expect(createdLink.link_id).toBe("deploy-bot");
    expect(createdLink).not.toHaveProperty("future_field");
    const append = appendRecordsRequestSchema.parse(fixtures.append_request);
    expect(append.records).toHaveLength(2);
    expect(append.writer).toEqual({
      id: "AAECAwQFBgcICQoLDA0ODw",
      seq_num: "41",
    });
    expect(appendRecordsRequestSchema.parse({
      records: [{ text: "one-shot\n" }],
    }).writer).toBeUndefined();
    expect(() =>
      appendRecordsRequestSchema.parse({
        records: [{ text: "both", bytes: "AP8" }],
      })
    ).toThrow();
    expect(() =>
      appendRecordsRequestSchema.parse({ records: [{ format: "bytes" }] })
    ).toThrow();
    expect(appendRangeSchema.parse(fixtures.append_response)).not
      .toHaveProperty("future_field");
    expect(apiErrorResponseSchema.parse(fixtures.error_response)).toEqual({
      error: {
        code: "sequence_mismatch",
        message: "append sequence precondition failed",
        request_id: "request-42",
        actual_next_seq_num: "9",
      },
    });
    const batch = sseReadBatchDataSchema.parse(fixtures.sse_read_batch);
    expect(batch.records).toHaveLength(2);
    expect(batch.records[0]?.part).toBeUndefined();
    expect(resolvedRecordFormat(batch.records[0]!)).toBe("transcript");
    expect(resolvedRecordFormat(batch.records[1]!)).toBe("bytes");
    expect(sseCaughtUpDataSchema.parse(fixtures.sse_caught_up).next_seq_num).toBe("8");
    expect(decodeSseResumeCursor(String(fixtures.sse_resume_cursor))).toEqual({
      nextSeqNum: 2n,
      consumedRecords: 2n,
    });
  });

  it("round-trips canonical SSE resume cursors and rejects invalid states", () => {
    const cursor = {
      nextSeqNum: 42n,
      consumedRecords: 7n,
    };
    expect(encodeSseResumeCursor(cursor)).toBe("v1,42,7");
    expect(decodeSseResumeCursor(encodeSseResumeCursor(cursor))).toEqual(cursor);

    for (const invalid of [
      "",
      "42",
      "v2,1,0",
      "v1,1",
      "v1,1,0,2",
      "v1,1,0,2,3,4",
      "v1,,0",
      "v1,01,0",
      "v1,1,00",
      "v1, 1,0",
      "v1,1,0 ",
      "v1,18446744073709551616,0",
      "v1,9007199254740992,0",
      "v1,8,1,7,9",
      "v1,1,2",
      "v1,0,0,0,1",
      "v1,1,0,1,9007199254740992",
    ]) {
      expect(() => decodeSseResumeCursor(invalid)).toThrow(RangeError);
    }
  });
  it("defaults creation to a private stream and canonicalizes permissions", () => {
    expect(
      createStreamRequestSchema.parse({
        title: "Deploy log",
        links: [
          { link_id: "team", permissions: "wr" },
          { link_id: "owner", permissions: "o" },
        ],
      }),
    ).toEqual({
      title: "Deploy log",
      visibility: "private",
      links: [
        { link_id: "team", permissions: "rw" },
        { link_id: "owner", permissions: "o" },
      ],
    });
  });

  it("keeps immutable link creation bodies strict", () => {
    expect(
      createLinkRequestSchema.parse({ permissions: "w" }),
    ).toEqual({ permissions: "w" });
    expect(() =>
      createLinkRequestSchema.parse({
        permissions: "w",
        label: "Deploy bot",
      }),
    ).toThrow();
  });

  it("validates optional mutable stream titles by Unicode code point", () => {
    const title = "😀".repeat(120);
    expect(streamTitleSchema.parse(title)).toBe(title);
    expect(updateStreamRequestSchema.parse({ title: null })).toEqual({
      title: null,
    });
    for (const invalid of [
      "",
      " padded",
      "padded ",
      "padded\u00a0",
      "tab\tbreak",
      "nul\u0000break",
      "line\nbreak",
      "line\u2028break",
      "unpaired high surrogate \ud800",
      "unpaired low surrogate \udc00",
      "😀".repeat(121),
    ]) {
      expect(() => streamTitleSchema.parse(invalid)).toThrow();
    }
  });

  it("validates link inventory without accepting secrets", () => {
    const link = {
      link_id: "release-dashboard",
      permissions: "rw",
      status: "active",
      created_at: "2026-08-07T12:00:00.000Z",
      expires_at: null,
      revoked_at: null,
    };
    expect(listLinksResponseSchema.parse({
      authorizing_link_id: "release-dashboard",
      links: [{ ...link, future_field: true }],
      next_cursor: null,
      future_field: true,
    })).toEqual({
      authorizing_link_id: "release-dashboard",
      links: [link],
      next_cursor: null,
    });
  });

  it("keeps management stream metadata lean and forward-compatible", () => {
    const stream = {
      stream_id: "0123456789abcdefghjkmnpqrstvwxyz",
      kind: "records" as const,
      title: null,
      visibility: "private",
      created_at: "2026-08-13T00:00:00Z",
      expires_at: "2026-08-23T00:00:00Z",
    };
    expect(streamMetadataSchema.parse(stream)).toEqual(stream);
    expect(streamMetadataSchema.parse({ ...stream, future_field: true }))
      .toEqual(stream);
    const { kind: _kind, ...legacyStream } = stream;
    expect(streamMetadataSchema.parse(legacyStream)).toEqual(stream);
    expect(() => updateStreamRequestSchema.parse({})).toThrow();
  });
});
