import {
  appendRecordsRequestSchema,
  generateStreamId,
  MAX_RECORD_PAYLOAD_BYTES,
  MAX_REST_ERROR_RESPONSE_BYTES,
  MAX_REST_RESPONSE_BYTES,
  MAX_STATELESS_APPEND_JSON_BYTES,
  parseClientWriterId,
  parseLinkId,
} from "@tailsurf/protocol";
import { describe, expect, it, vi } from "vitest";

import {
  generateIdempotencyKey,
  parseIdempotencyKey,
  TsfClient,
  TsfHttpError,
} from "../src/index.js";

const LINK_SECRET = "A".repeat(32);

describe("TsfClient REST API", () => {
  it("bounds success and error response bodies", async () => {
    for (const response of [
      new Response("{}", {
        headers: { "content-length": String(MAX_REST_RESPONSE_BYTES + 1) },
      }),
      new Response(new Uint8Array(MAX_REST_RESPONSE_BYTES + 1)),
    ]) {
      const client = new TsfClient({
        fetch: vi.fn<typeof fetch>(async () => response),
        boundedOperationAttempts: 1,
      });
      await expect(client.getStream(generateStreamId())).rejects.toMatchObject({
        code: "response_too_large",
      });
    }

    let cancelled = false;
    const oversizedError = new Response(
      new ReadableStream({ cancel: () => { cancelled = true; } }),
      {
        status: 503,
        headers: {
          "content-length": String(MAX_REST_ERROR_RESPONSE_BYTES + 1),
        },
      },
    );
    const client = new TsfClient({
      fetch: vi.fn<typeof fetch>(async () => oversizedError),
      boundedOperationAttempts: 1,
    });
    await expect(client.getStream(generateStreamId())).rejects.toMatchObject({
      status: 503,
      apiCode: undefined,
    });
    expect(cancelled).toBe(true);
  });

  it("validates stateless append bounds and response ranges", async () => {
    const request = vi.fn<typeof fetch>();
    const client = new TsfClient({ fetch: request });
    const writer = {
      clientWriterId: parseClientWriterId(new Uint8Array(16)),
      writerStartSeqNum: 0n,
    };

    expect(() => client.appendRecords(generateStreamId(), {
      ...writer,
      records: [{ data: new Uint8Array(MAX_RECORD_PAYLOAD_BYTES + 1) }],
    }, { linkSecret: LINK_SECRET })).toThrow(expect.objectContaining({
      code: "invalid_client_option",
    }));
    expect(() => client.appendRecords(generateStreamId(), {
      ...writer,
      expectedNextSeqNum: BigInt(Number.MAX_SAFE_INTEGER) + 1n,
      records: [{ data: new Uint8Array() }],
    }, { linkSecret: LINK_SECRET })).toThrow(expect.objectContaining({
      code: "invalid_client_option",
    }));
    expect(request).not.toHaveBeenCalled();

    const invalidResponse = new TsfClient({
      fetch: vi.fn<typeof fetch>(async () =>
        Response.json({ start_seq_num: "4", end_seq_num: "6" })
      ),
    });
    await expect(invalidResponse.appendRecords(generateStreamId(), {
      ...writer,
      records: [{ data: new Uint8Array() }],
    }, { linkSecret: LINK_SECRET })).rejects.toMatchObject({
      code: "invalid_api_response",
    });
  });

  it("uses compact JSON for a maximum-size binary record", async () => {
    const request = vi.fn<typeof fetch>(async () =>
      Response.json({ start_seq_num: "0", end_seq_num: "1" })
    );
    const client = new TsfClient({ fetch: request });

    await client.appendRecords(generateStreamId(), {
      clientWriterId: parseClientWriterId(new Uint8Array(16)),
      writerStartSeqNum: 0n,
      records: [{ data: new Uint8Array(MAX_RECORD_PAYLOAD_BYTES) }],
    }, { linkSecret: LINK_SECRET });

    const body = request.mock.calls[0]?.[1]?.body;
    expect(typeof body).toBe("string");
    expect(new TextEncoder().encode(body as string).byteLength).toBeLessThanOrEqual(
      MAX_STATELESS_APPEND_JSON_BYTES,
    );
    const parsed = appendRecordsRequestSchema.parse(JSON.parse(body as string));
    expect(parsed.records[0]?.text).toBeUndefined();
    expect(Buffer.from(parsed.records[0]!.bytes!, "base64url")).toHaveLength(
      MAX_RECORD_PAYLOAD_BYTES,
    );
  });

  it("retries creation without changing its recovery material", async () => {
    const streamId = generateStreamId();
    let attempt = 0;
    const request = vi.fn<typeof fetch>(async () => {
      attempt += 1;
      if (attempt === 1) {
        throw new Error("connection reset");
      }
      return attempt === 2
        ? new Response(null, {
            status: 503,
            headers: { "retry-after": "0" },
          })
        : createResponse(streamId);
    });
    const client = new TsfClient({ fetch: request });

    await expect(client.createStream({ visibility: "public" })).resolves
      .toMatchObject({ streamId });

    expect(request).toHaveBeenCalledTimes(3);
    const keys = request.mock.calls.map(([, init]) =>
      new Headers(init?.headers).get("idempotency-key")
    );
    const bodies = request.mock.calls.map(([, init]) => init?.body);
    expect(new Set(keys).size).toBe(1);
    expect(keys[0]).toMatch(/^[A-Za-z0-9_-]{43}$/);
    expect(new Set(bodies).size).toBe(1);
  });

  it("returns bounded structured API errors", async () => {
    const client = new TsfClient({
      fetch: vi.fn<typeof fetch>(async () =>
        Response.json({
          error: {
            code: "forbidden",
            message: "owner link required",
            request_id: "request-id",
            retry_after_ms: 125,
            actual_next_seq_num: "42",
          },
        }, { status: 403 })
      ),
    });

    const error = await client.revokeLink(
      generateStreamId(),
      parseLinkId("reader"),
      { linkSecret: LINK_SECRET },
    ).catch((caught: unknown) => caught);
    expect(error).toBeInstanceOf(TsfHttpError);
    expect(error).toMatchObject({
      status: 403,
      apiCode: "forbidden",
      message: "forbidden: owner link required",
      requestId: "request-id",
      retryAfterMs: 125,
      actualNextSeqNum: 42n,
    });
  });

  it("collects validated link pages", async () => {
    const request = vi.fn<typeof fetch>(async (input) => {
      const cursor = new URL(String(input)).searchParams.get("cursor");
      return Response.json({
        authorizing_link_id: "owner",
        links: [linkSummary(cursor === null ? "reader" : "writer")],
        next_cursor: cursor === null ? "second-page" : null,
      });
    });
    const client = new TsfClient({ fetch: request });

    await expect(client.listAllLinks(generateStreamId(), {
      linkSecret: LINK_SECRET,
    })).resolves.toMatchObject({
      authorizingLinkId: "owner",
      links: [{ linkId: "reader" }, { linkId: "writer" }],
      nextCursor: null,
    });
    expect(request).toHaveBeenCalledTimes(2);
  });

  it("aborts REST work at the configured deadline", async () => {
    let aborted = false;
    const client = new TsfClient({
      httpRequestTimeoutMs: 5,
      fetch: vi.fn<typeof fetch>((_input, init) =>
        new Promise<Response>((_resolve, reject) => {
          init?.signal?.addEventListener("abort", () => {
            aborted = true;
            reject(new Error("request aborted"));
          });
        })
      ),
    });

    await expect(client.getStream(generateStreamId())).rejects.toMatchObject({
      code: "http_timeout",
    });
    expect(aborted).toBe(true);
  });

  it("accepts only canonical caller-owned idempotency keys", () => {
    const key = generateIdempotencyKey();
    expect(parseIdempotencyKey(key)).toBe(key);
    expect(() => parseIdempotencyKey(`${key}!`)).toThrow();
  });
});

function linkSummary(linkId: string) {
  return {
    link_id: linkId,
    permissions: "r",
    status: "active",
    created_at: "2026-08-13T00:00:00Z",
    expires_at: null,
    revoked_at: null,
  };
}

function createResponse(streamId: string): Response {
  return Response.json({
    stream_id: streamId,
    kind: "transcript",
    title: null,
    visibility: "public",
    created_at: "2026-08-11T00:00:00Z",
    expires_at: "2026-08-21T00:00:00Z",
    web_origin: "https://tail.surf",
    links: [],
  });
}
