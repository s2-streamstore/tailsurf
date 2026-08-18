import {
  generateStreamId,
  MAX_RECORD_BYTES,
  MAX_SSE_EVENT_BYTES,
  MAX_SSE_UNTERMINATED_EVENT_BYTES,
} from "@tailsurf/protocol";
import { describe, expect, it, vi } from "vitest";

import { TsfClient } from "../src/index.js";

const CURSOR_ONE = "v1,1,1";
const CURSOR_TWO = "v1,2,2";
const CAUGHT_UP_CURSOR = "v1,4,0";

describe("SSE reader resume", () => {
  it("accepts several complete events delivered in one large transport chunk", async () => {
    const streamId = generateStreamId();
    const padding = `:${"a".repeat(1_100_000)}\n\n:${"b".repeat(1_100_000)}\n\n`;
    const fetch = vi.fn<typeof globalThis.fetch>().mockResolvedValue(
      sseResponse(streamId, `${padding}${recordsEvent("v1,1,1", 0)}`),
    );
    const session = await new TsfClient({ fetch }).connectSseReader({
      streamId,
      start: { type: "seqNum", seqNum: 0n },
      stop: { count: 1n },
    });

    expect((await session.nextRecord())?.seqNum).toBe(0n);
    session.close();
  });

  it("accepts a valid event fragmented across transport chunks", async () => {
    const streamId = generateStreamId();
    const encoded = new TextEncoder().encode(
      sseResponseText(streamId, readBatchEvent("v1,1,1", {
        encoding: "utf8",
        value: "split 😀 payload",
      })),
    );
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        for (let offset = 0; offset < encoded.byteLength; offset += 7) {
          controller.enqueue(encoded.subarray(offset, offset + 7));
        }
        controller.close();
      },
    });
    const fetch = vi.fn<typeof globalThis.fetch>().mockResolvedValue(
      new Response(body, { headers: { "content-type": "text/event-stream" } }),
    );
    const session = await new TsfClient({ fetch }).connectSseReader({
      streamId,
      start: { type: "seqNum", seqNum: 0n },
      stop: { count: 1n },
    });

    expect(new TextDecoder().decode((await session.nextRecord())?.data)).toBe(
      "split 😀 payload",
    );
    session.close();
  });

  it("accepts a CRLF event boundary split across transport chunks", async () => {
    const streamId = generateStreamId();
    const text = sseResponseText(
      streamId,
      recordsEvent("v1,1,1", 0),
    ).replaceAll("\n", "\r\n");
    const boundary = text.indexOf("\r\n\r\n");
    const encoder = new TextEncoder();
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(encoder.encode(text.slice(0, boundary + 3)));
        controller.enqueue(encoder.encode(text.slice(boundary + 3)));
        controller.close();
      },
    });
    const fetch = vi.fn<typeof globalThis.fetch>().mockResolvedValue(
      new Response(body, { headers: { "content-type": "text/event-stream" } }),
    );
    const session = await new TsfClient({ fetch }).connectSseReader({
      streamId,
      start: { type: "seqNum", seqNum: 0n },
      stop: { count: 1n },
    });

    expect((await session.nextRecord())?.seqNum).toBe(0n);
    session.close();
  });

  it("rejects invalid UTF-8 after an earlier event in the same chunk", async () => {
    const streamId = generateStreamId();
    const prefix = new TextEncoder().encode(sseResponseText(streamId, ""));
    const body = new Uint8Array(prefix.byteLength + 1);
    body.set(prefix);
    body[body.byteLength - 1] = 0xff;
    const fetch = vi.fn<typeof globalThis.fetch>().mockResolvedValue(
      new Response(body, { headers: { "content-type": "text/event-stream" } }),
    );

    await expect(new TsfClient({ fetch }).connectSseReader({
      streamId,
      start: { type: "seqNum", seqNum: 0n },
    })).rejects.toMatchObject({
      code: "invalid_api_response",
    });
  });

  it("drains one maximum-size read batch in sequence", async () => {
    const streamId = generateStreamId();
    const count = 1_000;
    const records = Array.from({ length: count }, (_unused, seqNum) =>
      readRecordWire(seqNum, { encoding: "utf8", value: `${seqNum}\n` })
    );
    const fetch = vi.fn<typeof globalThis.fetch>().mockResolvedValue(
      sseResponse(
        streamId,
        readBatchRecordsEvent(`v1,${count},${count}`, records),
      ),
    );
    const session = await new TsfClient({ fetch }).connectSseReader({
      streamId,
      start: { type: "seqNum", seqNum: 0n },
      stop: { count: BigInt(count) },
    });

    for (let seqNum = 0; seqNum < count; seqNum += 1) {
      expect((await session.nextRecord())?.seqNum).toBe(BigInt(seqNum));
    }
    expect(await session.nextRecord()).toBeUndefined();
    expect(fetch).toHaveBeenCalledOnce();
  });

  it("accepts an escape-heavy maximum-size record in its compact encoding", async () => {
    const streamId = generateStreamId();
    const value = Buffer.alloc(MAX_RECORD_BYTES).toString("base64url");
    const fetch = vi.fn<typeof globalThis.fetch>().mockResolvedValue(
      sseResponse(streamId, readBatchEvent("v1,1,1", {
        encoding: "base64url",
        value,
      })),
    );
    const session = await new TsfClient({ fetch }).connectSseReader({
      streamId,
      start: { type: "seqNum", seqNum: 0n },
      stop: { count: 1n },
    });

    expect((await session.nextRecord())?.data).toHaveLength(MAX_RECORD_BYTES);
    session.close();
  });

  it.each([
    {
      name: "an oversized decoded record",
      count: undefined,
      event: () => readBatchEvent("v1,1,1", {
        encoding: "base64url",
        value: Buffer.alloc(MAX_RECORD_BYTES + 1).toString("base64url"),
      }),
    },
    {
      name: "an oversized aggregate decoded payload",
      count: undefined,
      event: () => readBatchRecordsEvent("v1,3,3", [0, 1, 2].map((seqNum) =>
        readRecordWire(seqNum, {
          encoding: "base64url",
          value: Buffer.alloc(400 * 1024).toString("base64url"),
        })
      )),
    },
    {
      name: "a cursor that does not follow its records",
      count: undefined,
      event: () => recordsEvent("v1,2,1", 0),
    },
    {
      name: "more records than the remaining count",
      count: 1n,
      event: () => readBatchRecordsEvent("v1,2,2", [
        readRecordWire(0, { encoding: "utf8", value: "zero" }),
        readRecordWire(1, { encoding: "utf8", value: "one" }),
      ]),
    },
  ])("rejects read_batch with $name", async ({ event, count }) => {
    const streamId = generateStreamId();
    const fetch = vi.fn<typeof globalThis.fetch>().mockResolvedValue(
      sseResponse(streamId, event()),
    );
    const session = await new TsfClient({ fetch }).connectSseReader({
      streamId,
      start: { type: "seqNum", seqNum: 0n },
      ...(count === undefined ? {} : { stop: { count } }),
    });

    await expect(session.nextRecord()).rejects.toMatchObject({
      code: "invalid_api_response",
    });
  });

  it("rejects caught_up that skips past the previous cursor", async () => {
    const streamId = generateStreamId();
    const fetch = vi.fn<typeof globalThis.fetch>().mockResolvedValue(
      sseResponse(
        streamId,
        `${recordsEvent("v1,1,1", 0)}id: v1,2,1\nevent: caught_up\ndata: {"next_seq_num":"2","last_timestamp_ms":"0"}\n\n`,
      ),
    );
    const session = await new TsfClient({ fetch }).connectSseReader({
      streamId,
      start: { type: "seqNum", seqNum: 0n },
    });

    expect((await session.nextRecord())?.seqNum).toBe(0n);
    await expect(session.nextRecord()).rejects.toMatchObject({
      code: "invalid_api_response",
    });
  });

  it("rejects one oversized completed SSE event", async () => {
    const streamId = generateStreamId();
    const oversized = `event: read_batch\ndata: ${"a".repeat(MAX_SSE_EVENT_BYTES)}\n\n`;
    const fetch = vi.fn<typeof globalThis.fetch>().mockResolvedValue(
      sseResponse(streamId, oversized),
    );
    await expect(new TsfClient({ fetch }).connectSseReader({
      streamId,
      start: { type: "seqNum", seqNum: 0n },
    })).rejects.toMatchObject({
      code: "invalid_api_response",
    });
  });

  it("rejects an oversized completed SSE event across chunks", async () => {
    const streamId = generateStreamId();
    const encoder = new TextEncoder();
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(encoder.encode(
          `${sseResponseText(streamId, "")}event: read_batch\ndata: ${"a".repeat(
            MAX_SSE_EVENT_BYTES / 2,
          )}`,
        ));
        controller.enqueue(encoder.encode(
          `${"a".repeat(MAX_SSE_EVENT_BYTES / 2)}\n\n`,
        ));
        controller.close();
      },
    });
    const fetch = vi.fn<typeof globalThis.fetch>().mockResolvedValue(
      new Response(body, { headers: { "content-type": "text/event-stream" } }),
    );
    const session = await new TsfClient({ fetch }).connectSseReader({
      streamId,
      start: { type: "seqNum", seqNum: 0n },
    });

    await expect(session.nextRecord()).rejects.toMatchObject({
      code: "invalid_api_response",
    });
  });

  it("rejects an oversized unterminated SSE event across chunks", async () => {
    const streamId = generateStreamId();
    const prefix = sseResponseText(streamId, "");
    const encoder = new TextEncoder();
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(encoder.encode(prefix));
        controller.enqueue(encoder.encode(`event: read_batch\ndata: ${"a".repeat(
          MAX_SSE_UNTERMINATED_EVENT_BYTES / 2,
        )}`));
        controller.enqueue(encoder.encode("a".repeat(
          MAX_SSE_UNTERMINATED_EVENT_BYTES / 2 + 1,
        )));
        controller.close();
      },
    });
    const fetch = vi.fn<typeof globalThis.fetch>().mockResolvedValue(
      new Response(body, { headers: { "content-type": "text/event-stream" } }),
    );
    const session = await new TsfClient({ fetch }).connectSseReader({
      streamId,
      start: { type: "seqNum", seqNum: 0n },
    });

    await expect(session.nextRecord()).rejects.toMatchObject({
      code: "invalid_api_response",
    });
  });

  it("retries a transient resume request and keeps its cursor", async () => {
    const streamId = generateStreamId();
    const fetch = vi.fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(interruptedSseResponseAfter(
        streamId,
        recordsEvent("v1,1,1", 0),
      ))
      .mockResolvedValueOnce(new Response(null, { status: 503 }))
      .mockResolvedValueOnce(sseResponse(streamId, recordsEvent("v1,2,2", 1)));
    const session = await new TsfClient({
      fetch,
      retryPolicy: { initialBackoffMs: 0, maxBackoffMs: 0, maxAttempts: 3 },
    }).connectSseReader({
      streamId,
      start: { type: "seqNum", seqNum: 0n },
      stop: { count: 2n },
    });

    expect((await session.nextRecord())?.seqNum).toBe(0n);
    expect((await session.nextRecord())?.seqNum).toBe(1n);
    expect(fetch).toHaveBeenCalledTimes(3);
    expect(new Headers(fetch.mock.calls[1]?.[1]?.headers).get("last-event-id"))
      .toBe("v1,1,1");
    expect(new Headers(fetch.mock.calls[2]?.[1]?.headers).get("last-event-id"))
      .toBe("v1,1,1");
  });

  it("honors a bounded structured retry hint during SSE setup", async () => {
    vi.useFakeTimers();
    try {
      const streamId = generateStreamId();
      const fetch = vi.fn<typeof globalThis.fetch>()
        .mockResolvedValueOnce(new Response(JSON.stringify({
          error: {
            code: "rate_limited",
            message: "slow down",
            request_id: "request-sse",
            retry_after_ms: 60_000,
          },
        }), {
          status: 429,
          headers: { "content-type": "application/json" },
        }))
        .mockResolvedValueOnce(sseResponse(
          streamId,
          recordsEvent("v1,1,1", 0),
        ));
      const opening = new TsfClient({
        fetch,
        retryPolicy: { initialBackoffMs: 1, maxBackoffMs: 50, maxAttempts: 2 },
      }).connectSseReader({
        streamId,
        start: { type: "seqNum", seqNum: 0n },
        stop: { count: 1n },
      });

      await vi.advanceTimersByTimeAsync(49);
      expect(fetch).toHaveBeenCalledOnce();
      await vi.advanceTimersByTimeAsync(1);
      const session = await opening;
      expect((await session.nextRecord())?.seqNum).toBe(0n);
      expect(fetch).toHaveBeenCalledTimes(2);
      session.close();
    } finally {
      vi.useRealTimers();
    }
  });

  it("preserves structured HTTP details when SSE setup retries are exhausted", async () => {
    const streamId = generateStreamId();
    const fetch = vi.fn<typeof globalThis.fetch>().mockResolvedValue(
      new Response(JSON.stringify({
        error: {
          code: "rate_limited",
          message: "slow down",
          request_id: "request-sse",
          retry_after_ms: 125,
        },
      }), { status: 429 }),
    );

    await expect(new TsfClient({
      fetch,
      retryPolicy: { maxAttempts: 1 },
    }).connectSseReader({ streamId })).rejects.toMatchObject({
      apiCode: "rate_limited",
      requestId: "request-sse",
      retryAfterMs: 125,
    });
  });

  it("bounds an SSE handshake with the REST request timeout", async () => {
    vi.useFakeTimers();
    try {
      const streamId = generateStreamId();
      const fetch = vi.fn<typeof globalThis.fetch>((_input, init) =>
        new Promise((_resolve, reject) => {
          init?.signal?.addEventListener("abort", () => {
            reject(new Error("request aborted", { cause: init.signal?.reason }));
          }, { once: true });
        })
      );
      const opening = new TsfClient({
        fetch,
        restRequestTimeoutMs: 5,
        retryPolicy: { maxAttempts: 1 },
      }).connectSseReader({ streamId });
      const rejected = expect(opening).rejects.toMatchObject({
        code: "http_timeout",
      });

      await vi.advanceTimersByTimeAsync(5);
      await rejected;
      expect(fetch).toHaveBeenCalledOnce();
    } finally {
      vi.useRealTimers();
    }
  });

  it("resumes after the response body fails mid-stream", async () => {
    const streamId = generateStreamId();
    const fetch = vi.fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(interruptedSseResponse(streamId))
      .mockResolvedValueOnce(sseResponse(streamId, recordsEvent("v1,1,1", 0)));
    const session = await new TsfClient({
      fetch,
      retryPolicy: { initialBackoffMs: 0, maxBackoffMs: 0, maxAttempts: 2 },
    }).connectSseReader({
      streamId,
      start: { type: "seqNum", seqNum: 0n },
      stop: { count: 1n },
    });

    expect((await session.nextRecord())?.seqNum).toBe(0n);
    expect(fetch).toHaveBeenCalledTimes(2);
  });

  it("reconnects with the versioned event ID and the unchanged request URL", async () => {
    const streamId = generateStreamId();
    const calls: Array<{ readonly url: string; readonly headers: Headers }> = [];
    const responses = [
      interruptedSseResponseAfter(
        streamId,
        recordsEvent(CURSOR_ONE, 0),
      ),
      sseResponse(
        streamId,
        recordsEvent(CURSOR_TWO, 1),
      ),
    ];
    const fetch = vi.fn<typeof globalThis.fetch>(async (input, init) => {
      const url = input instanceof Request
        ? input.url
        : input instanceof URL
          ? input.href
          : input;
      calls.push({ url, headers: new Headers(init?.headers) });
      const response = responses.shift();
      if (response === undefined) {
        throw new Error("unexpected SSE request");
      }
      return response;
    });
    const session = await new TsfClient({
      apiOrigin: "http://localhost:8787",
      fetch,
    }).connectSseReader({
      streamId,
      start: { type: "seqNum", seqNum: 0n },
      stop: { count: 2n },
    });

    expect((await session.nextRecord())?.seqNum).toBe(0n);
    expect((await session.nextRecord())?.seqNum).toBe(1n);
    expect(await session.nextRecord()).toBeUndefined();
    expect(calls).toHaveLength(2);
    expect(calls[1]?.url).toBe(calls[0]?.url);
    const url = new URL(calls[0]!.url);
    expect(Object.fromEntries(url.searchParams)).toEqual({
      seq_num: "0",
      count: "2",
    });
    expect(calls[0]?.headers.get("last-event-id")).toBeNull();
    expect(calls[1]?.headers.get("last-event-id")).toBe(CURSOR_ONE);
  });

  it("treats clean finite completion as terminal", async () => {
    const streamId = generateStreamId();
    const calls: Headers[] = [];
    const responses = [sseResponse(streamId, recordsEvent(CURSOR_ONE, 0))];
    const fetch = vi.fn<typeof globalThis.fetch>(async (_input, init) => {
      calls.push(new Headers(init?.headers));
      const response = responses.shift();
      if (response === undefined) {
        throw new Error("unexpected SSE request");
      }
      return response;
    });
    const session = await new TsfClient({ fetch }).connectSseReader({
      streamId,
      start: { type: "seqNum", seqNum: 0n },
      stop: { waitSeconds: 0 },
    });

    expect((await session.nextRecord())?.seqNum).toBe(0n);
    expect(await session.nextRecord()).toBeUndefined();
    expect(calls).toHaveLength(1);
  });

  it("treats HTTP 204 as terminal after an established resume cursor", async () => {
    const streamId = generateStreamId();
    const fetch = vi.fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(sseResponse(
        streamId,
        `id: ${CAUGHT_UP_CURSOR}\nevent: caught_up\ndata: {"next_seq_num":"4","last_timestamp_ms":"0"}\n\n`,
      ))
      .mockResolvedValueOnce(new Response(null, { status: 204 }));
    const session = await new TsfClient({ fetch }).connectSseReader({
      streamId,
      start: { type: "tailOffset", tailOffset: 80n },
    });

    expect(await session.nextRecord()).toBeUndefined();
    expect(fetch).toHaveBeenCalledTimes(2);
    const [, secondInit] = fetch.mock.calls[1] ?? [];
    expect(new Headers(secondInit?.headers).get("last-event-id")).toBe(
      CAUGHT_UP_CURSOR,
    );
  });

  it("accepts a terminal count-zero cursor on stream metadata", async () => {
    const streamId = generateStreamId();
    const fetch = vi.fn<typeof globalThis.fetch>().mockResolvedValue(
      sseResponse(streamId, "", { streamMetadataCursor: "v1,0,0" }),
    );
    const session = await new TsfClient({ fetch }).connectSseReader({
      streamId,
      stop: { count: 0n },
    });

    expect(await session.nextRecord()).toBeUndefined();
    expect(session.streamMetadata().streamId).toBe(streamId);
    expect(fetch).toHaveBeenCalledOnce();
  });

  it("rejects record and caught-up events without a resume cursor", async () => {
    const streamId = generateStreamId();
    const fetch = vi.fn<typeof globalThis.fetch>().mockResolvedValue(
      sseResponse(streamId, recordsEvent(undefined, 0)),
    );
    const session = await new TsfClient({ fetch }).connectSseReader({
      streamId,
      start: { type: "seqNum", seqNum: 0n },
    });

    await expect(session.nextRecord()).rejects.toMatchObject({
      code: "invalid_api_response",
    });
  });

  it("rejects malformed and unsupported resume cursor IDs", async () => {
    const streamId = generateStreamId();
    const fetch = vi.fn<typeof globalThis.fetch>().mockResolvedValue(
      sseResponse(streamId, recordsEvent("v2,1,1", 0)),
    );
    const session = await new TsfClient({ fetch }).connectSseReader({
      streamId,
      start: { type: "seqNum", seqNum: 0n },
    });

    await expect(session.nextRecord()).rejects.toMatchObject({
      code: "invalid_api_response",
    });
  });

  it("bounds reconnects that repeatedly make no read progress", async () => {
    const streamId = generateStreamId();
    const fetch = vi.fn<typeof globalThis.fetch>().mockImplementation(async () =>
      sseResponse(streamId, "")
    );
    const session = await new TsfClient({
      fetch,
      retryPolicy: { maxAttempts: 3, initialBackoffMs: 0, maxBackoffMs: 0 },
    }).connectSseReader({ streamId });

    await expect(session.nextRecord()).rejects.toMatchObject({
      code: "read_reconnect_limit_exceeded",
    });
    expect(fetch).toHaveBeenCalledTimes(3);
  });
});

function sseResponse(
  streamId: string,
  events: string,
  options: {
    readonly streamMetadataCursor?: string;
  } = {},
): Response {
  return new Response(sseResponseText(streamId, events, options), {
    headers: { "content-type": "text/event-stream" },
  });
}

function sseResponseText(
  streamId: string,
  events: string,
  options: {
    readonly streamMetadataCursor?: string;
  } = {},
): string {
  return `${options.streamMetadataCursor === undefined ? "" : `id: ${options.streamMetadataCursor}\n`}event: stream_metadata\ndata: ${JSON.stringify({
      stream_id: streamId,
      title: null,
      visibility: "public",
      created_at: "2026-08-13T00:00:00Z",
      expires_at: "2026-08-23T00:00:00Z",
    })}\n\n${events}`;
}

function interruptedSseResponse(streamId: string): Response {
  return interruptedSseResponseAfter(streamId, "");
}

function interruptedSseResponseAfter(streamId: string, events: string): Response {
  const encoder = new TextEncoder();
  const body = new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(encoder.encode(
        `event: stream_metadata\ndata: ${JSON.stringify({
          stream_id: streamId,
          title: null,
          visibility: "public",
          created_at: "2026-08-13T00:00:00Z",
          expires_at: "2026-08-23T00:00:00Z",
        })}\n\n${events}`,
      ));
      setTimeout(() => controller.error(new TypeError("connection reset")), 0);
    },
  });
  return new Response(body, {
    headers: { "content-type": "text/event-stream" },
  });
}

function recordsEvent(id: string | undefined, seqNum: number): string {
  return readBatchEvent(id, {
    encoding: "utf8",
    value: `record ${seqNum.toString()}\n`,
  }, seqNum);
}

function readBatchEvent(
  id: string | undefined,
  data: { readonly encoding: "utf8" | "base64url"; readonly value: string },
  seqNum = 0,
): string {
  return readBatchRecordsEvent(id, [readRecordWire(seqNum, data)]);
}

function readBatchRecordsEvent(
  id: string | undefined,
  records: readonly ReturnType<typeof readRecordWire>[],
): string {
  return `${id === undefined ? "" : `id: ${id}\n`}event: read_batch\ndata: ${JSON.stringify({
    records,
  })}\n\n`;
}

function readRecordWire(
  seqNum: number,
  data: { readonly encoding: "utf8" | "base64url"; readonly value: string },
) {
  return {
    seq_num: seqNum.toString(),
    timestamp_ms: (1_786_579_200_000 + seqNum).toString(),
    writer_id: "AAAAAAAAAAAAAAAAAAAAAA",
    writer_seq_num: seqNum.toString(),
    part: { index: 0, is_final: true },
    format: "transcript",
    data,
  } as const;
}
