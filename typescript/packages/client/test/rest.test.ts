import {
  appendRecordsRequestSchema,
  createStreamRequestSchema,
  generateStreamId,
  MAX_RECORD_PAYLOAD_BYTES,
  MAX_REST_ERROR_RESPONSE_BYTES,
  MAX_REST_RESPONSE_BYTES,
  MAX_STATELESS_APPEND_JSON_BYTES,
  parseLinkId,
  parseClientWriterId,
  RecordFormat,
} from "@tailsurf/protocol";
import { describe, expect, it, vi } from "vitest";

import {
  generateIdempotencyKey,
  parseIdempotencyKey,
  prepareCreateStreamRequest,
  TsfClient,
  TsfHttpError,
} from "../src/index.js";

const LINK_SECRET = "A".repeat(32);

describe("TsfClient REST API", () => {
  it("rejects an invalid bounded operation attempt count", () => {
    expect(() => new TsfClient({ boundedOperationAttempts: 0 })).toThrow(
      expect.objectContaining({ code: "invalid_client_option" }),
    );
  });

  it("rejects declared and streamed REST success bodies above the memory bound", async () => {
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
  });

  it("bounds and discards oversized REST error bodies", async () => {
    let cancelled = false;
    const response = new Response(new ReadableStream({
      cancel() {
        cancelled = true;
      },
    }), {
      status: 503,
      headers: {
        "content-length": String(MAX_REST_ERROR_RESPONSE_BYTES + 1),
      },
    });
    const client = new TsfClient({
      fetch: vi.fn<typeof fetch>(async () => response),
      boundedOperationAttempts: 1,
    });

    await expect(client.getStream(generateStreamId())).rejects.toMatchObject({
      status: 503,
      apiCode: undefined,
    });
    expect(cancelled).toBe(true);
  });

  it("replays link creation with the same caller-owned idempotency key", async () => {
    const streamId = generateStreamId();
    const link = {
      linkId: "deploy-bot",
      permissions: "w",
    } as const;
    const idempotencyKey = generateIdempotencyKey();
    const request = vi.fn<typeof fetch>(async () => Response.json({
      link_id: link.linkId,
      permissions: link.permissions,
      secret: LINK_SECRET,
    }));
    const client = new TsfClient({ fetch: request });

    await client.createLink(
      streamId,
      link,
      { idempotencyKey, linkSecret: LINK_SECRET },
    );
    await client.createLink(
      streamId,
      link,
      { idempotencyKey, linkSecret: LINK_SECRET },
    );

    expect(request).toHaveBeenCalledTimes(2);
    expect(request.mock.calls[0]?.[1]?.body).toBe(request.mock.calls[1]?.[1]?.body);
    expect(JSON.parse(request.mock.calls[0]?.[1]?.body as string)).toEqual({
      permissions: "w",
    });
    expect(new Headers(request.mock.calls[0]?.[1]?.headers).get("idempotency-key"))
      .toBe(idempotencyKey);
  });

  it("generates an idempotency key for link creation", async () => {
    const request = vi.fn<typeof fetch>(async () => Response.json({
      link_id: "deploy-bot",
      permissions: "w",
      secret: LINK_SECRET,
    }));
    const client = new TsfClient({ fetch: request });

    await client.createLink(
      generateStreamId(),
      { linkId: "deploy-bot", permissions: "w" },
      { linkSecret: LINK_SECRET },
    );

    const [, init] = request.mock.calls[0] ?? [];
    const idempotencyKey = new Headers(init?.headers).get("idempotency-key");
    expect(idempotencyKey).toMatch(/^[A-Za-z0-9_-]{43}$/);
  });

  it("rejects stateless append bounds before making a request", async () => {
    const request = vi.fn<typeof fetch>();
    const client = new TsfClient({ fetch: request });
    const base = {
      clientWriterId: parseClientWriterId(new Uint8Array(16)),
      writerStartSeqNum: 0n,
    };
    expect(() => client.appendRecords(generateStreamId(), {
      ...base,
      records: [{
        format: RecordFormat.Bytes,
        data: new Uint8Array(512 * 1024 + 1),
      }],
    }, { linkSecret: LINK_SECRET })).toThrow(expect.objectContaining({
      code: "invalid_client_option",
    }));
    expect(() => client.appendRecords(generateStreamId(), {
      ...base,
      records: [500 * 1024, 500 * 1024].map((size) => ({
        format: RecordFormat.Bytes,
        data: new Uint8Array(size),
      })),
    }, { linkSecret: LINK_SECRET })).toThrow(expect.objectContaining({
      code: "invalid_client_option",
    }));
    expect(() => client.appendRecords(generateStreamId(), {
      ...base,
      expectedNextSeqNum: BigInt(Number.MAX_SAFE_INTEGER) + 1n,
      records: [{ format: RecordFormat.Bytes, data: new Uint8Array() }],
    }, { linkSecret: LINK_SECRET })).toThrow(expect.objectContaining({
      code: "invalid_client_option",
    }));
    expect(request).not.toHaveBeenCalled();
  });

  it("rejects an append response range with the wrong record count", async () => {
    const client = new TsfClient({
      fetch: vi.fn<typeof fetch>(async () =>
        Response.json({ start_seq_num: "4", end_seq_num: "6" })
      ),
    });

    await expect(client.appendRecords(generateStreamId(), {
      clientWriterId: parseClientWriterId(new Uint8Array(16)),
      writerStartSeqNum: 0n,
      records: [{ format: RecordFormat.Bytes, data: new Uint8Array() }],
    }, { linkSecret: LINK_SECRET })).rejects.toMatchObject({
      code: "invalid_api_response",
    });
  });

  it("uses compact JSON for an escape-heavy maximum-size record", async () => {
    const request = vi.fn<typeof fetch>(async () =>
      Response.json({ start_seq_num: "0", end_seq_num: "1" })
    );
    const client = new TsfClient({ fetch: request });

    await client.appendRecords(generateStreamId(), {
      clientWriterId: parseClientWriterId(new Uint8Array(16)),
      writerStartSeqNum: 0n,
      records: [{
        format: RecordFormat.Transcript,
        data: new Uint8Array(MAX_RECORD_PAYLOAD_BYTES),
      }],
    }, { linkSecret: LINK_SECRET });

    const body = request.mock.calls[0]?.[1]?.body;
    expect(typeof body).toBe("string");
    const encoded = new TextEncoder().encode(body as string);
    expect(encoded.byteLength).toBeLessThanOrEqual(MAX_STATELESS_APPEND_JSON_BYTES);
    const parsed = appendRecordsRequestSchema.parse(JSON.parse(body as string));
    expect(parsed.records[0]!.data.encoding).toBe("base64url");
    expect(Buffer.from(parsed.records[0]!.data.value, "base64url")).toHaveLength(
      MAX_RECORD_PAYLOAD_BYTES,
    );
  });

  it("uses an anonymous idempotent request for stream creation", async () => {
    const streamId = generateStreamId();
    const request = vi.fn<typeof fetch>(async () => createResponse(streamId));
    const client = new TsfClient({
      apiOrigin: "http://localhost:8787",
      fetch: request,
    });

    await expect(client.createStream()).resolves.toMatchObject({ streamId });
    expect(request).toHaveBeenCalledOnce();
    const [input, init] = request.mock.calls[0] ?? [];
    expect(input).toBe("http://localhost:8787/api/v1/streams");
    expect(init?.method).toBe("POST");
    const createBody = jsonRequestBody(init?.body);
    expect(createBody.visibility).toBe("private");
    expect(createBody.links).toHaveLength(1);
    expect(createBody.links[0]).toMatchObject({
      link_id: "owner",
      permissions: "o",
    });
    expect(createBody.links[0]).not.toHaveProperty("secret");
    const headers = new Headers(init?.headers);
    expect(headers.get("authorization")).toBeNull();
    expect(headers.get("content-type")).toBe(
      "application/json",
    );
    const idempotencyKey = headers.get("idempotency-key");
    expect(idempotencyKey).toMatch(/^[A-Za-z0-9_-]{43}$/);
    if (idempotencyKey === null) {
      throw new TypeError("expected an idempotency key");
    }
    const idempotencyBytes = Buffer.from(idempotencyKey, "base64url");
    expect(idempotencyBytes).toHaveLength(32);
    expect(idempotencyBytes.toString("base64url")).toBe(idempotencyKey);
  });

  it("accepts a canonical caller-owned creation idempotency key", async () => {
    const idempotencyKey = generateIdempotencyKey();
    const request = vi.fn<typeof fetch>(async () =>
      createResponse(generateStreamId())
    );
    const client = new TsfClient({ fetch: request });

    await client.createStream(
      prepareCreateStreamRequest({ title: "Deploy log", visibility: "public" }),
      { idempotencyKey },
    );

    const [, init] = request.mock.calls[0] ?? [];
    expect(new Headers(init?.headers).get("idempotency-key")).toBe(
      idempotencyKey,
    );
    const createBody = jsonRequestBody(init?.body);
    expect(createBody.title).toBe("Deploy log");
    expect(createBody.visibility).toBe("public");
    expect(createBody.links).toHaveLength(1);
  });

  it.each([
    "A".repeat(42),
    `${"A".repeat(42)}!`,
    `${"A".repeat(42)}B`,
  ])("rejects non-canonical caller-owned key %s", (idempotencyKey) => {
    const request = vi.fn<typeof fetch>();
    const client = new TsfClient({ fetch: request });
    const prepared = prepareCreateStreamRequest();

    expect(() => client.createStream(prepared, { idempotencyKey }))
      .toThrowError(
      expect.objectContaining({ code: "invalid_idempotency_key" }),
    );
    expect(() => parseIdempotencyKey(idempotencyKey)).toThrow();
    expect(request).not.toHaveBeenCalled();
  });

  it.each([429, 503])(
    "retries create status %i with the same idempotency key",
    async (status) => {
      const streamId = generateStreamId();
      let attempt = 0;
      const request = vi.fn<typeof fetch>(async () => {
        attempt += 1;
        return attempt === 1
          ? Response.json(
              { error: { code: "retry", message: "try again" } },
              { status, headers: { "retry-after": "0" } },
            )
          : createResponse(streamId, "public");
      });
      const client = new TsfClient({
        fetch: request,
      });

      await expect(client.createStream({ visibility: "public" })).resolves
        .toMatchObject({ streamId });

      expect(request).toHaveBeenCalledTimes(2);
      const keys = request.mock.calls.map(([, init]) =>
        new Headers(init?.headers).get("idempotency-key")
      );
      expect(new Set(keys)).toEqual(new Set([keys[0]]));
      expect(keys[0]).toMatch(/^[A-Za-z0-9_-]{43}$/);
      expect(request.mock.calls.every(([, init]) =>
        !new Headers(init?.headers).has("authorization")
      )).toBe(true);
      const bodies = request.mock.calls.map(([, init]) => {
        if (typeof init?.body !== "string") {
          throw new TypeError("expected a JSON request body");
        }
        return init.body;
      });
      expect(new Set(bodies).size).toBe(1);
      const createBody = jsonRequestBody(bodies[0]);
      expect(createBody.visibility).toBe("public");
      expect(createBody.links).toHaveLength(1);
    },
  );

  it.each([
    ["invalid JSON", () => new Response("{", {
      status: 200,
      headers: { "content-type": "application/json" },
    })],
    ["an invalid API response", () => Response.json({ ok: true })],
  ] as const)(
    "retries a committed create that returns %s",
    async (_label, failedResponse) => {
      const streamId = generateStreamId();
      let attempt = 0;
      const request = vi.fn<typeof fetch>(async () => {
        attempt += 1;
        return attempt === 1 ? failedResponse() : createResponse(streamId);
      });
      const client = new TsfClient({ fetch: request });

      await expect(client.createStream()).resolves.toMatchObject({
        streamId,
      });

      expect(request).toHaveBeenCalledTimes(2);
      const keys = request.mock.calls.map(([, init]) =>
        new Headers(init?.headers).get("idempotency-key")
      );
      expect(new Set(keys).size).toBe(1);
    },
  );

  it("retries create transport failures and timeouts", async () => {
    const streamId = generateStreamId();
    let attempt = 0;
    const request = vi.fn<typeof fetch>(async (_input, init) => {
      attempt += 1;
      if (attempt === 1) {
        throw new Error("connection reset");
      }
      if (attempt === 2) {
        return await new Promise<Response>((_resolve, reject) => {
          init?.signal?.addEventListener("abort", () => {
            reject(new Error("request aborted"));
          });
        });
      }
      return createResponse(streamId);
    });
    const client = new TsfClient({
      fetch: request,
      httpRequestTimeoutMs: 5,
    });

    await expect(client.createStream()).resolves.toMatchObject({
      streamId,
    });
    expect(request).toHaveBeenCalledTimes(3);
    const keys = request.mock.calls.map(([, init]) =>
      new Headers(init?.headers).get("idempotency-key")
    );
    expect(new Set(keys).size).toBe(1);
  });

  it("uses a fresh idempotency key for each create invocation", async () => {
    const request = vi.fn<typeof fetch>(async () =>
      createResponse(generateStreamId())
    );
    const client = new TsfClient({ fetch: request });

    await client.createStream();
    await client.createStream();

    const keys = request.mock.calls.map(([, init]) =>
      new Headers(init?.headers).get("idempotency-key")
    );
    expect(keys).toHaveLength(2);
    expect(new Set(keys).size).toBe(2);
  });

  it("waits for a reasonable Retry-After delay", async () => {
    vi.useFakeTimers();
    try {
      const streamId = generateStreamId();
      let attempt = 0;
      const request = vi.fn<typeof fetch>(async () => {
        attempt += 1;
        return attempt === 1
          ? new Response(null, {
              status: 429,
              headers: { "retry-after": "1" },
            })
          : createResponse(streamId);
      });
      const client = new TsfClient({ fetch: request });

      const created = client.createStream();
      await vi.advanceTimersByTimeAsync(999);
      expect(request).toHaveBeenCalledOnce();
      await vi.advanceTimersByTimeAsync(1);
      await expect(created).resolves.toMatchObject({ streamId });
      expect(request).toHaveBeenCalledTimes(2);
    } finally {
      vi.useRealTimers();
    }
  });

  it("caps automatic Retry-After waits without discarding the server hint", async () => {
    vi.useFakeTimers();
    try {
      const streamId = generateStreamId();
      let attempt = 0;
      const request = vi.fn<typeof fetch>(async () => {
        attempt += 1;
        return attempt === 1
          ? new Response(null, {
              status: 429,
              headers: { "retry-after": "3600" },
            })
          : createResponse(streamId);
      });
      const client = new TsfClient({ fetch: request });

      const created = client.createStream();
      await vi.advanceTimersByTimeAsync(1_999);
      expect(request).toHaveBeenCalledOnce();
      await vi.advanceTimersByTimeAsync(1);
      await expect(created).resolves.toMatchObject({ streamId });
      expect(request).toHaveBeenCalledTimes(2);
    } finally {
      vi.useRealTimers();
    }
  });

  it("bounds create retries", async () => {
    const request = vi.fn<typeof fetch>(async () =>
      new Response(null, {
        status: 503,
        headers: { "retry-after": "0" },
      })
    );
    const client = new TsfClient({ fetch: request });

    await expect(client.createStream()).rejects.toMatchObject({ status: 503 });
    expect(request).toHaveBeenCalledTimes(3);
  });

  it("returns stable API errors without exposing arbitrary response bodies", async () => {
    const client = new TsfClient({
      fetch: vi.fn<typeof fetch>(async () =>
        Response.json(
          {
            error: {
              code: "forbidden",
              message: "owner link required",
              request_id: "request-from-body",
              retry_after_ms: 125,
              actual_next_seq_num: "42",
            },
          },
          { status: 403 },
        ),
      ),
    });

    const error = await client
      .revokeLink(
        generateStreamId(),
        parseLinkId("reader"),
        { linkSecret: LINK_SECRET },
      )
      .catch((caught: unknown) => caught);
    expect(error).toBeInstanceOf(TsfHttpError);
    expect(error).toMatchObject({
      status: 403,
      apiCode: "forbidden",
      message: "forbidden: owner link required",
      requestId: "request-from-body",
      retryAfterMs: 125,
      actualNextSeqNum: 42n,
    });
  });

  it("prefers HTTP headers over duplicate structured error hints", async () => {
    const client = new TsfClient({
      fetch: vi.fn<typeof fetch>(async () =>
        Response.json(
          {
            error: {
              code: "rate_limited",
              message: "try later",
              request_id: "request-from-body",
              retry_after_ms: 125,
            },
          },
          {
            status: 429,
            headers: {
              "retry-after": "0",
              "x-request-id": "request-from-header",
            },
          },
        )
      ),
    });

    const error = await client
      .getStream(generateStreamId())
      .catch((caught: unknown) => caught);
    expect(error).toMatchObject({
      requestId: "request-from-header",
      retryAfterMs: 0,
    });
  });

  it("targets a link resource when revoking", async () => {
    const request = vi.fn<typeof fetch>(async () =>
      new Response(null, { status: 204 }),
    );
    const client = new TsfClient({
      apiOrigin: "https://example.tsf",
      fetch: request,
    });
    const streamId = generateStreamId();
    const linkId = parseLinkId("deploy-bot");

    await expect(
      client.revokeLink(streamId, linkId, { linkSecret: LINK_SECRET }),
    ).resolves.toBeUndefined();

    const [input, init] = request.mock.calls[0] ?? [];
    expect(input).toBe(
      `https://example.tsf/api/v1/streams/${streamId}/links/${linkId}`,
    );
    expect(init?.method).toBe("DELETE");
    expect(init?.body).toBeUndefined();
    expect(new Headers(init?.headers).get("authorization")).toBe(
      `Bearer ${LINK_SECRET}`,
    );
  });

  it("requires an explicit owner link secret for every management operation", () => {
    const request = vi.fn<typeof fetch>();
    const client = new TsfClient({ fetch: request });

    expect(() =>
      client.deleteStream(
        generateStreamId(),
        undefined as never,
      )
    ).toThrowError(expect.objectContaining({ code: "invalid_client_option" }));
    expect(request).not.toHaveBeenCalled();
  });

  it.each([
    [
      "more links than requested",
      {
        links: [linkSummary("reader"), linkSummary("writer")],
        next_cursor: null,
      },
      1,
    ],
    [
      "duplicate link IDs",
      {
        links: [linkSummary("reader"), linkSummary("reader")],
        next_cursor: null,
      },
      100,
    ],
    [
      "a cursor on an empty page",
      { links: [], next_cursor: "next" },
      100,
    ],
  ] as const)("rejects link pages with %s", async (_label, page, limit) => {
    const client = new TsfClient({
      fetch: vi.fn<typeof fetch>(async () => Response.json({
        authorizing_link_id: "owner",
        ...page,
      })),
      boundedOperationAttempts: 1,
    });

    await expect(client.listLinks(generateStreamId(), {
      linkSecret: LINK_SECRET,
      limit,
    })).rejects.toMatchObject({ code: "invalid_api_response" });
  });

  it("lists all links across pages", async () => {
    const request = vi.fn<typeof fetch>(
      async (input) => {
        const cursor = new URL(String(input)).searchParams.get("cursor");
        return Response.json(cursor === null
          ? {
              authorizing_link_id: "owner",
              links: [linkSummary("reader")],
              next_cursor: "second-page",
            }
          : {
              authorizing_link_id: "owner",
              links: [linkSummary("writer")],
              next_cursor: null,
            });
      },
    );
    const client = new TsfClient({ fetch: request });

    await expect(client.listAllLinks(generateStreamId(), {
      linkSecret: LINK_SECRET,
    })).resolves.toMatchObject({
      authorizingLinkId: "owner",
      links: [{ linkId: "reader" }, { linkId: "writer" }],
      nextCursor: null,
    });
    expect(request).toHaveBeenCalledTimes(2);
    expect(new URL(String(request.mock.calls[0]?.[0])).searchParams.get("limit"))
      .toBe("100");
    expect(new URL(String(request.mock.calls[1]?.[0])).searchParams.get("cursor"))
      .toBe("second-page");
  });

  it.each([
    [
      "a changed authorizing link",
      {
        authorizing_link_id: "other-owner",
        links: [linkSummary("writer")],
        next_cursor: null,
      },
    ],
    [
      "a link ID repeated across pages",
      {
        authorizing_link_id: "owner",
        links: [linkSummary("reader")],
        next_cursor: null,
      },
    ],
    [
      "a repeated cursor",
      {
        authorizing_link_id: "owner",
        links: [linkSummary("writer")],
        next_cursor: "next",
      },
    ],
  ] as const)("rejects link pagination with %s", async (_label, secondPage) => {
    let page = 0;
    const client = new TsfClient({
      fetch: vi.fn<typeof fetch>(async () => {
        page += 1;
        return Response.json(page === 1
          ? {
              authorizing_link_id: "owner",
              links: [linkSummary("reader")],
              next_cursor: "next",
            }
          : secondPage);
      }),
      boundedOperationAttempts: 1,
    });

    await expect(client.listAllLinks(generateStreamId(), {
      linkSecret: LINK_SECRET,
    })).rejects.toMatchObject({ code: "invalid_api_response" });
  });

  it.each([
    "https://user@tail.surf",
    "https://tail.surf/api",
    "https://tail.surf?region=west",
    "https://tail.surf#api",
    "wss://tail.surf",
  ])("rejects non-origin API URL %s", (apiOrigin) => {
    expect(() => new TsfClient({ apiOrigin })).toThrow(
      "API origin must be an HTTP origin",
    );
  });

  it("aborts REST requests at the configured timeout", async () => {
    let aborted = false;
    const client = new TsfClient({
      httpRequestTimeoutMs: 5,
      fetch: vi.fn<typeof fetch>(
        (_input, init) =>
          new Promise<Response>((_resolve, reject) => {
            init?.signal?.addEventListener("abort", () => {
              aborted = true;
              reject(new Error("request aborted"));
            });
          }),
      ),
    });

    await expect(client.getStream(generateStreamId())).rejects.toMatchObject({
      code: "http_timeout",
      message: "get stream timed out after 5ms",
    });
    expect(aborted).toBe(true);
  });

  it.each([200, 503])("times out while consuming a %i response body", async (status) => {
    const streamId = generateStreamId();
    const body = status === 200
      ? {
          stream_id: streamId,
          title: null,
          visibility: "private",
          created_at: "2026-08-11T00:00:00Z",
          expires_at: "2026-08-21T00:00:00Z",
        }
      : { error: { code: "unavailable", message: "try later" } };
    const response = new Response(new ReadableStream({
      start(controller) {
        setTimeout(() => {
          controller.enqueue(new TextEncoder().encode(JSON.stringify(body)));
          controller.close();
        }, 50);
      },
    }), { status });
    const client = new TsfClient({
      httpRequestTimeoutMs: 5,
      boundedOperationAttempts: 1,
      fetch: vi.fn<typeof fetch>(async () => response),
    });

    await expect(client.getStream(streamId)).rejects.toMatchObject({
      code: "http_timeout",
      message: "get stream timed out after 5ms",
    });
  });

  it("binds the default fetch transport to the global receiver", async () => {
    const original = globalThis.fetch;
    const receiver = globalThis;
    globalThis.fetch = vi.fn(function (this: typeof globalThis) {
      if (this !== receiver) {
        throw new TypeError("illegal invocation");
      }
      return Promise.resolve(Response.json({ ok: true }));
    });
    try {
      const client = new TsfClient();
      await expect(client.createStream()).rejects.toMatchObject({
        code: "invalid_api_response",
      });
    } finally {
      globalThis.fetch = original;
    }
  });
});

function jsonRequestBody(body: BodyInit | null | undefined) {
  if (typeof body !== "string") {
    throw new TypeError("expected a JSON request body");
  }
  const parsed = JSON.parse(body) as unknown;
  return createStreamRequestSchema.parse(parsed);
}

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

function createResponse(
  streamId: string,
  visibility: "private" | "public" = "private",
): Response {
  return Response.json({
    stream_id: streamId,
    title: null,
    visibility,
    created_at: "2026-08-11T00:00:00Z",
    expires_at: "2026-08-21T00:00:00Z",
    links: [],
  });
}
