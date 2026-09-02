import {
  generateStreamId,
  MAX_SSE_EVENT_BYTES,
  type StreamKind,
} from "@tailsurf/protocol";
import { describe, expect, it, vi } from "vitest";

import { TsfClient } from "../src/index.js";

const CURSOR_ONE = "v1,1,1";
const CURSOR_TWO = "v1,2,2";

describe("SSE reader", () => {
  it("parses UTF-8 and CRLF boundaries split across chunks", async () => {
    const streamId = generateStreamId();
    const source = sseText(
      streamId,
      recordEvent(CURSOR_ONE, 0, "split 😀 payload"),
    ).replaceAll("\n", "\r\n");
    const encoded = new TextEncoder().encode(source);
    const chunks = Array.from(
      { length: Math.ceil(encoded.byteLength / 7) },
      (_unused, index) => encoded.subarray(index * 7, index * 7 + 7),
    );
    const session = await new TsfClient({
      fetch: vi.fn<typeof fetch>(async () => chunkedResponse(chunks)),
    }).connectSseReader({
      streamId,
      start: { type: "seqNum", seqNum: 0n },
      stop: { count: 1n },
    });

    expect(new TextDecoder().decode((await session.nextRecord())?.data)).toBe(
      "split 😀 payload",
    );
  });

  it("rejects invalid UTF-8 and oversized events", async () => {
    const streamId = generateStreamId();
    const prefix = new TextEncoder().encode(sseText(streamId, ""));
    const invalid = new Uint8Array(prefix.byteLength + 1);
    invalid.set(prefix);
    invalid[invalid.byteLength - 1] = 255;
    await expect(new TsfClient({
      fetch: vi.fn<typeof fetch>(async () => chunkedResponse([invalid])),
    }).connectSseReader({ streamId })).rejects.toMatchObject({
      code: "invalid_api_response",
    });

    const oversized = `${sseText(streamId, "")}event: read_batch\ndata: ${"a".repeat(MAX_SSE_EVENT_BYTES)}\n\n`;
    const reading = new TsfClient({
      fetch: vi.fn<typeof fetch>(async () => sseResponse(oversized)),
    }).connectSseReader({ streamId }).then((session) => session.nextRecord());
    await expect(reading).rejects.toMatchObject({ code: "invalid_api_response" });
  });

  it("resumes with the last event ID through transient setup failures", async () => {
    const streamId = generateStreamId();
    const fetch = vi.fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(interruptedAfter(
        streamId,
        recordEvent(CURSOR_ONE, 0),
      ))
      .mockResolvedValueOnce(new Response(null, { status: 503 }))
      .mockResolvedValueOnce(sseResponse(
        sseText(streamId, recordEvent(CURSOR_TWO, 1)),
      ));
    const session = await new TsfClient({
      fetch,
      boundedOperationAttempts: 3,
    }).connectSseReader({
      streamId,
      start: { type: "seqNum", seqNum: 0n },
      stop: { count: 2n },
    });

    expect((await session.nextRecord())?.seqNum).toBe(0n);
    expect((await session.nextRecord())?.seqNum).toBe(1n);
    expect(fetch).toHaveBeenCalledTimes(3);
    for (const call of fetch.mock.calls.slice(1)) {
      expect(new Headers(call[1]?.headers).get("last-event-id")).toBe(CURSOR_ONE);
    }
  });

  it.each([undefined, "v2,1,1"])(
    "rejects an invalid resume cursor %s",
    async (cursor) => {
      const streamId = generateStreamId();
      const session = await new TsfClient({
        fetch: vi.fn<typeof fetch>(async () =>
          sseResponse(sseText(streamId, recordEvent(cursor, 0)))
        ),
      }).connectSseReader({
        streamId,
        start: { type: "seqNum", seqNum: 0n },
      });

      await expect(session.nextRecord()).rejects.toMatchObject({
        code: "invalid_api_response",
      });
    },
  );

  it("rejects a stream kind change while reconnecting", async () => {
    const streamId = generateStreamId();
    const fetch = vi.fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(interruptedAfter(
        streamId,
        recordEvent(CURSOR_ONE, 0),
      ))
      .mockResolvedValueOnce(sseResponse(
        sseText(streamId, recordEvent(CURSOR_TWO, 1), "bytes"),
      ));
    const session = await new TsfClient({ fetch }).connectSseReader({
      streamId,
      start: { type: "seqNum", seqNum: 0n },
    });

    expect((await session.nextRecord())?.seqNum).toBe(0n);
    await expect(session.nextRecord()).rejects.toMatchObject({
      code: "invalid_api_response",
      message: "stream kind changed while reconnecting the reader",
    });
  });

  it("treats HTTP 204 as terminal after receiving a cursor", async () => {
    const streamId = generateStreamId();
    const cursor = "v1,4,0";
    const fetch = vi.fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(sseResponse(sseText(
        streamId,
        `id: ${cursor}\nevent: caught_up\ndata: {"next_seq_num":"4","last_timestamp_ms":"0"}\n\n`,
      )))
      .mockResolvedValueOnce(new Response(null, { status: 204 }));
    const session = await new TsfClient({ fetch }).connectSseReader({
      streamId,
      start: { type: "tailOffset", tailOffset: 80n },
    });

    expect(await session.nextRecord()).toBeUndefined();
    expect(new Headers(fetch.mock.calls[1]?.[1]?.headers).get("last-event-id"))
      .toBe(cursor);
  });

  it("bounds reconnects that make no progress", async () => {
    const streamId = generateStreamId();
    const fetch = vi.fn<typeof globalThis.fetch>(async () =>
      sseResponse(sseText(streamId, ""))
    );
    const session = await new TsfClient({
      fetch,
      boundedOperationAttempts: 3,
    }).connectSseReader({ streamId });

    await expect(session.nextRecord()).rejects.toMatchObject({
      code: "read_reconnect_limit_exceeded",
    });
    expect(fetch).toHaveBeenCalledTimes(3);
  });
});

function sseText(
  streamId: string,
  events: string,
  kind: StreamKind = "transcript",
): string {
  return `event: stream_metadata\ndata: ${JSON.stringify({
    stream_id: streamId,
    kind,
    title: null,
    visibility: "public",
    created_at: "2026-08-13T00:00:00Z",
    expires_at: "2026-08-23T00:00:00Z",
  })}\n\n${events}`;
}

function recordEvent(
  cursor: string | undefined,
  seqNum: number,
  text = `record ${seqNum.toString()}\n`,
): string {
  return `${cursor === undefined ? "" : `id: ${cursor}\n`}event: read_batch\ndata: ${JSON.stringify({
    records: [{
      seq_num: seqNum.toString(),
      timestamp_ms: (1_786_579_200_000 + seqNum).toString(),
      writer: {
        id: "AAAAAAAAAAAAAAAAAAAAAA",
        seq_num: seqNum.toString(),
      },
      text,
    }],
  })}\n\n`;
}

function sseResponse(body: BodyInit): Response {
  return new Response(body, {
    headers: { "content-type": "text/event-stream" },
  });
}

function chunkedResponse(chunks: readonly Uint8Array[]): Response {
  return sseResponse(new ReadableStream<Uint8Array>({
    start(controller) {
      for (const chunk of chunks) {
        controller.enqueue(chunk);
      }
      controller.close();
    },
  }));
}

function interruptedAfter(streamId: string, events: string): Response {
  const bytes = new TextEncoder().encode(sseText(streamId, events));
  return sseResponse(new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(bytes);
      setTimeout(() => controller.error(new TypeError("connection reset")), 0);
    },
  }));
}
