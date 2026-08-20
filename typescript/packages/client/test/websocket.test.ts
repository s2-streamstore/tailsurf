import {
  decodeClientFrame,
  encodeServerFrame,
  generateStreamId,
  parseWriterId,
  MAX_BATCH_PAYLOAD_BYTES,
  MAX_READ_BATCH_RECORDS,
  MAX_RECORD_BYTES,
  RecordFormat,
  TSF_WEBSOCKET_PROTOCOL,
  UNSPLIT_PART,
  type CaughtUpPosition,
  type ReadRecord,
  type ClientFrame,
  type ServerFrame,
  type StreamId,
} from "@tailsurf/protocol";
import { describe, expect, it, vi } from "vitest";

import {
  DEFAULT_WRITER_RETAINED_BYTES,
  DEFAULT_WRITER_RETAINED_RECORDS,
  TsfClient,
  type WebSocketFactory,
} from "../src/index.js";
import { FrameSocket } from "../src/socket.js";

describe("FrameSocket", () => {
  it("closes the transport after an invalid server frame", async () => {
    const transport = new HangingWebSocket(true, [], false);
    const socket = new FrameSocket(transport);
    await socket.opened;

    transport.dispatchEvent(new MessageEvent("message", {
      data: new Uint8Array([255]),
    }));

    await expect(socket.nextFrame()).rejects.toBeInstanceOf(Error);
    expect(transport.closed).toBe(true);
  });

  it("bounds queued physical record slots and bytes", async () => {
    const frameBounded = new FrameSocket(
      new ScriptedWebSocket(
        [
          { type: "ready" },
          { type: "heartbeat" },
          { type: "heartbeat" },
        ],
        1000,
      ),
      { maxPhysicalRecordSlots: 2, maxBytes: 1_024 },
    );
    await frameBounded.opened;
    await new Promise((resolve) => setTimeout(resolve, 0));
    await expect(frameBounded.nextFrame()).resolves.toMatchObject({ type: "ready" });
    await expect(frameBounded.nextFrame()).resolves.toMatchObject({ type: "heartbeat" });
    await expect(frameBounded.nextFrame()).rejects.toMatchObject({
      code: "client_receive_overload",
    });

    const byteBounded = new FrameSocket(
      new ScriptedWebSocket(
        [
          { type: "ready" },
          readBatch(record(0n, "too large")),
        ],
        1000,
      ),
      { maxPhysicalRecordSlots: 10, maxBytes: 100 },
    );
    await byteBounded.opened;
    await new Promise((resolve) => setTimeout(resolve, 0));
    await expect(byteBounded.nextFrame()).resolves.toMatchObject({ type: "ready" });
    await expect(byteBounded.nextFrame()).rejects.toMatchObject({
      code: "client_receive_overload",
    });
  });

  it("accounts for every physical record in a queued read batch", async () => {
    const records = Array.from(
      { length: MAX_READ_BATCH_RECORDS },
      (_unused, index) => record(BigInt(index), ""),
    );
    const socket = new FrameSocket(
      new ScriptedWebSocket(
        [
          { type: "ready" },
          { type: "readBatch", records },
          {
            type: "readBatch",
            records: Array.from(
              { length: 25 },
              (_unused, index) =>
                record(BigInt(MAX_READ_BATCH_RECORDS + index), ""),
            ),
          },
        ],
        1000,
      ),
      { maxPhysicalRecordSlots: 1_024, maxBytes: 16 * 1024 * 1024 },
    );
    await socket.opened;
    await new Promise((resolve) => setTimeout(resolve, 0));

    await expect(socket.nextFrame()).resolves.toMatchObject({ type: "ready" });
    await expect(socket.nextFrame()).resolves.toMatchObject({
      type: "readBatch",
      records: { length: MAX_READ_BATCH_RECORDS },
    });
    await expect(socket.nextFrame()).rejects.toMatchObject({
      code: "client_receive_overload",
    });
  });
});

describe("TsfClient configuration", () => {
  it("rejects timeouts that cannot be represented by JavaScript timers", () => {
    expect(() => new TsfClient({
      webSocketConnectTimeoutMs: 2_147_483_648,
    })).toThrow(expect.objectContaining({ code: "invalid_client_option" }));
  });
});

describe("TsfReadSession", () => {
  it("drains a maximum read batch in order", async () => {
    const streamId = generateStreamId();
    const records = Array.from(
      { length: MAX_READ_BATCH_RECORDS },
      (_unused, index) => record(BigInt(index), ""),
    );
    const client = new TsfClient({
      webSocketFactory: () =>
        new ScriptedWebSocket(
          [
            { type: "ready" },
            streamMetadataFrame(streamId),
            { type: "readBatch", records },
          ],
          1000,
        ),
    });
    const reader = await client.connectReader({
      streamId,
      start: { type: "seqNum", seqNum: 0n },
      stop: { count: BigInt(MAX_READ_BATCH_RECORDS) },
    });

    for (let index = 0; index < MAX_READ_BATCH_RECORDS; index += 1) {
      await expect(reader.nextRecord()).resolves.toMatchObject({
        seqNum: BigInt(index),
      });
    }
    await expect(reader.nextRecord()).resolves.toBeUndefined();
  });

  it("is async iterable and closes when iteration stops early", async () => {
    const streamId = generateStreamId();
    const socket = new HangingWebSocket(true, [
      { type: "ready" },
      streamMetadataFrame(streamId),
      { type: "readBatch", records: [record(0n, "first"), record(1n, "second")] },
    ], false);
    const client = new TsfClient({ webSocketFactory: () => socket });
    const session = await client.connectReader({
      streamId,
      start: { type: "seqNum", seqNum: 0n },
    });

    const seen: bigint[] = [];
    for await (const next of session) {
      seen.push(next.seqNum);
      break;
    }

    expect(seen).toEqual([0n]);
    expect(socket.closed).toBe(true);
  });

  it("cancels an in-flight connection without retrying", async () => {
    const stalled = new HangingWebSocket(true);
    const controller = new AbortController();
    let connectionCount = 0;
    const client = new TsfClient({
      webSocketFactory: () => {
        connectionCount += 1;
        return stalled;
      },
    });

    const connecting = client.connectReader({
      streamId: generateStreamId(),
      start: { type: "seqNum", seqNum: 0n },
      signal: controller.signal,
    });
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(stalled.readyState).toBe(1);
    const reason = new Error("connection cancelled");
    controller.abort(reason);

    await expect(connecting).rejects.toBe(reason);
    expect(stalled.closed).toBe(true);
    expect(connectionCount).toBe(1);
  });

  it("stops reconnecting when a session closes", async () => {
    const streamId = generateStreamId();
    let connectionCount = 0;
    const client = new TsfClient({
      webSocketFactory: () => {
        connectionCount += 1;
        return new ScriptedWebSocket(
          [
            { type: "ready" },
            streamMetadataFrame(streamId),
          ],
          1006,
        );
      },
      retryPolicy: { initialBackoffMs: 20, maxBackoffMs: 20 },
    });

    const reader = await client.connectReader({
      streamId,
      start: { type: "seqNum", seqNum: 0n },
    });
    const pending = reader.nextRecord();
    await new Promise((resolve) => setTimeout(resolve, 0));
    reader.close();

    await expect(pending).resolves.toBeUndefined();
    await new Promise((resolve) => setTimeout(resolve, 30));
    expect(connectionCount).toBe(1);
  });

  it("cancels an active record wait when a session closes", async () => {
    const streamId = generateStreamId();
    const socket = new HangingWebSocket(
      true,
      [
        { type: "ready" },
        streamMetadataFrame(streamId),
      ],
      false,
    );
    const client = new TsfClient({
      webSocketFactory: () => socket,
    });
    const reader = await client.connectReader({
      streamId,
      start: { type: "seqNum", seqNum: 0n },
    });

    const pending = reader.nextRecord();
    reader.close();

    await expect(Promise.race([
      pending,
      new Promise((resolve) => setTimeout(() => resolve("still pending"), 10)),
    ])).resolves.toBeUndefined();
  });

  it("uses an independent read-idle timeout", async () => {
    const streamId = generateStreamId();
    const socket = new HangingWebSocket(
      true,
      [{ type: "ready" }, streamMetadataFrame(streamId)],
      false,
    );
    const client = new TsfClient({
      webSocketFactory: () => socket,
      webSocketReadIdleTimeoutMs: 5,
      retryPolicy: { maxAttempts: 1 },
    });
    const reader = await client.connectReader({ streamId });

    await expect(reader.nextRecord()).rejects.toMatchObject({
      code: "operation_timeout",
      message: "read stream record timed out after 5ms",
    });
  });

  it("can disable the read-idle timeout", async () => {
    const streamId = generateStreamId();
    const socket = new HangingWebSocket(
      true,
      [{ type: "ready" }, streamMetadataFrame(streamId)],
      false,
    );
    const client = new TsfClient({
      webSocketFactory: () => socket,
      webSocketReadIdleTimeoutMs: null,
    });
    const reader = await client.connectReader({ streamId });
    const pending = reader.nextRecord();

    await expect(Promise.race([
      pending,
      new Promise((resolve) => setTimeout(() => resolve("still pending"), 10)),
    ])).resolves.toBe("still pending");
    reader.close();
    await expect(pending).resolves.toBeUndefined();
  });

  it("retries a timed-out initial handshake and closes the abandoned socket", async () => {
    const streamId = generateStreamId();
    const abandoned = new HangingWebSocket(true);
    let connectionCount = 0;
    const client = new TsfClient({
      webSocketFactory: () => {
        connectionCount += 1;
        return (connectionCount === 1
          ? abandoned
          : new ScriptedWebSocket(
              [
                { type: "ready" },
                streamMetadataFrame(streamId),
                readBatch(record(0n, "recovered")),
              ],
              1000,
            ));
      },
      webSocketOperationTimeoutMs: 5,
      retryPolicy: { maxAttempts: 2, initialBackoffMs: 0, maxBackoffMs: 0 },
    });

    const reader = await client.connectReader({
      streamId,
      start: { type: "seqNum", seqNum: 0n },
      stop: { count: 1n },
    });
    await expect(reader.nextRecord()).resolves.toMatchObject({ seqNum: 0n });
    expect(abandoned.closed).toBe(true);
    expect(connectionCount).toBe(2);
  });

  it("bounds consecutive failed reconnect handshakes", async () => {
    const streamId = generateStreamId();
    let connections = 0;
    const client = new TsfClient({
      webSocketFactory: () => {
        connections += 1;
        return connections === 1
          ? new ScriptedWebSocket(
              [{ type: "ready" }, streamMetadataFrame(streamId)],
              1006,
            )
          : new HangingWebSocket(false);
      },
      webSocketConnectTimeoutMs: 5,
      retryPolicy: { maxAttempts: 3, initialBackoffMs: 0, maxBackoffMs: 0 },
    });
    const session = await client.connectReader({ streamId });

    await expect(session.nextRecord()).rejects.toMatchObject({
      code: "operation_timeout",
    });
    expect(connections).toBe(3);
  });

  it("closes the read transport when an observer throws", async () => {
    const streamId = generateStreamId();
    const socket = new HangingWebSocket(true, [
      { type: "ready" },
      streamMetadataFrame(streamId),
      { type: "caughtUp", nextSeqNum: 0n, lastTimestampMs: 0n },
    ], false);
    const client = new TsfClient({ webSocketFactory: () => socket });
    const failure = new Error("observer failed");
    const session = await client.connectReader({
      streamId,
      onCaughtUp: () => {
        throw failure;
      },
    });

    await expect(session.nextRecord()).rejects.toBe(failure);
    expect(socket.closed).toBe(true);
  });

  it("reports caught-up positions without interrupting record delivery", async () => {
    const streamId = generateStreamId();
    const caughtUpPositions: CaughtUpPosition[] = [];
    const streams: string[] = [];
    const client = new TsfClient({
      webSocketFactory: () =>
        new ScriptedWebSocket(
          [
            { type: "ready" },
            streamMetadataFrame(streamId),
            {
              type: "caughtUp",
              nextSeqNum: 1n,
              lastTimestampMs: 1_000n,
            },
            readBatch(record(1n, "next")),
          ],
          1000,
        ),
    });

    const reader = await client.connectReader({
      streamId,
      start: { type: "seqNum", seqNum: 1n },
      stop: { count: 1n },
      onCaughtUp: (caughtUp) => caughtUpPositions.push(caughtUp),
      onStreamMetadata: (stream) => streams.push(stream.streamId),
    });
    await expect(reader.nextRecord()).resolves.toMatchObject({ seqNum: 1n });
    expect(caughtUpPositions).toEqual([{
      nextSeqNum: 1n,
      lastTimestampMs: 1_000n,
    }]);
    expect(reader.lastCaughtUp()).toEqual(caughtUpPositions[0]);
    expect(streams).toEqual([streamId]);
    expect(reader.streamMetadata().streamId).toBe(streamId);
  });

  it("rejects a caught-up position that rewinds an absolute read", async () => {
    const streamId = generateStreamId();
    const client = new TsfClient({
      webSocketFactory: () =>
        new ScriptedWebSocket(
          [
            { type: "ready" },
            streamMetadataFrame(streamId),
            {
              type: "caughtUp",
              nextSeqNum: 1n,
              lastTimestampMs: 1_000n,
            },
          ],
          1000,
        ),
    });

    const reader = await client.connectReader({
      streamId,
      start: { type: "seqNum", seqNum: 2n },
    });
    await expect(reader.nextRecord()).rejects.toMatchObject({
      code: "invalid_server_read",
    });
  });

  it("reconnects a finite read without retaining a tail boundary", async () => {
    const streamId = generateStreamId();
    const urls: string[] = [];
    const clientFrames: ClientFrame[][] = [];
    let connection = 0;
    const client = new TsfClient({
      webSocketFactory: (url) => {
        urls.push(url);
        const index = connection;
        connection += 1;
        return new ScriptedWebSocket(
          index === 0
            ? [
                { type: "ready" },
                streamMetadataFrame(streamId),
                readBatch(record(0n, "first")),
              ]
            : [
                { type: "ready" },
                streamMetadataFrame(streamId),
                {
                  type: "readBatch",
                  records: [record(1n, "second"), record(2n, "third")],
                },
              ],
          index === 0 ? 1013 : 1000,
          clientFrames[index] = [],
        );
      },
      retryPolicy: { initialBackoffMs: 0, maxBackoffMs: 0 },
    });
    const reader = await client.connectReader({
      streamId,
      start: { type: "seqNum", seqNum: 0n },
      stop: { waitSeconds: 0 },
    });

    await expect(reader.nextRecord()).resolves.toMatchObject({ seqNum: 0n });
    await expect(reader.nextRecord()).resolves.toMatchObject({ seqNum: 1n });
    await expect(reader.nextRecord()).resolves.toMatchObject({ seqNum: 2n });
    await expect(reader.nextRecord()).resolves.toBeUndefined();
    expect(urls.map(readQuery)).toEqual([
      "seq_num=0&wait=0",
      "seq_num=1&wait=0",
    ]);
    expect(clientFrames).toEqual([
      [{ type: "openRead" }],
      [{ type: "openRead" }],
    ]);
  });

  it("opens one bounded paced read", async () => {
    const streamId = generateStreamId();
    const urls: string[] = [];
    const clientFrames: ClientFrame[] = [];
    const client = new TsfClient({
      apiOrigin: "http://localhost:8787",
      webSocketFactory: (url) => {
        urls.push(url);
        return new ScriptedWebSocket(
          [
            { type: "ready" },
            streamMetadataFrame(streamId),
            readBatch(record(9n, "paced")),
          ],
          1000,
          clientFrames,
        );
      },
    });

    const reader = await client.connectReader({
      streamId,
      start: { type: "timestampMs", timestampMs: 1_000n },
      stop: { untilTimestampMs: 1_787_000_000_000n },
      rate: 2,
    });
    await expect(reader.nextRecord()).resolves.toMatchObject({ seqNum: 9n });

    expect(urls).toHaveLength(1);
    expect(urls.map(readQuery)).toEqual([
      "timestamp=1000&until=1787000000000&rate=2",
    ]);
    expect(clientFrames).toEqual([{ type: "openRead" }]);
  });

  it.each([
    { closeCode: 1006, description: "an unclean disconnect" },
    { closeCode: 1001, description: "server shutdown" },
  ])("resumes from the next sequence after $description", async ({ closeCode }) => {
    const streamId = generateStreamId();
    const urls: string[] = [];
    const clientFrames: ClientFrame[][] = [];
    const scripts: readonly (readonly ServerFrame[])[] = [
      [
        { type: "ready" },
        streamMetadataFrame(streamId),
        readBatch(record(5n, "first")),
      ],
      [
        { type: "ready" },
        streamMetadataFrame(streamId),
        readBatch(record(6n, "second")),
      ],
    ];
    const webSocketFactory: WebSocketFactory = (url) => {
      const index = urls.length;
      urls.push(url);
      const script = scripts[index];
      if (script === undefined) {
        throw new Error("unexpected reconnect");
      }
      return new ScriptedWebSocket(
        script,
        index === 0 ? closeCode : 1000,
        clientFrames[index] = [],
      );
    };
    const client = new TsfClient({
      apiOrigin: "http://localhost:8787",
      webSocketFactory,
      retryPolicy: { initialBackoffMs: 0, maxBackoffMs: 0 },
    });

    const reader = await client.connectReader({
      streamId,
      start: { type: "seqNum", seqNum: 5n },
      stop: { count: 2n },
    });
    await expect(reader.nextRecord()).resolves.toMatchObject({ seqNum: 5n });
    await expect(reader.nextRecord()).resolves.toMatchObject({ seqNum: 6n });
    await expect(reader.nextRecord()).resolves.toBeUndefined();

    expect(urls.map(readQuery)).toEqual([
      "seq_num=5&count=2",
      "seq_num=6&count=1",
    ]);
    expect(clientFrames).toEqual([
      [{ type: "openRead" }],
      [{ type: "openRead" }],
    ]);
  });

  it("reconnects a tail-relative read after the last received record", async () => {
    const streamId = generateStreamId();
    const urls: string[] = [];
    const clientFrames: ClientFrame[][] = [];
    const client = new TsfClient({
      apiOrigin: "http://localhost:8787",
      webSocketFactory: (url) => {
        const connection = urls.length;
        urls.push(url);
        return new ScriptedWebSocket(
          [
            { type: "ready" },
            streamMetadataFrame(streamId),
            readBatch(record(connection === 0 ? 8n : 9n, "record")),
          ],
          connection === 0 ? 1013 : 1000,
          clientFrames[connection] = [],
        );
      },
      retryPolicy: { initialBackoffMs: 0, maxBackoffMs: 0 },
    });

    const reader = await client.connectReader({
      streamId,
      start: { type: "tailOffset", tailOffset: 2n },
      stop: { count: 2n },
    });
    await expect(reader.nextRecord()).resolves.toMatchObject({ seqNum: 8n });
    await expect(reader.nextRecord()).resolves.toMatchObject({ seqNum: 9n });

    expect(urls.map(readQuery)).toEqual([
      "tail_offset=2&count=2",
      "seq_num=9&count=1",
    ]);
    expect(clientFrames).toEqual([
      [{ type: "openRead" }],
      [{ type: "openRead" }],
    ]);
  });

  it("uses an empty caught-up position as the reconnect position", async () => {
    const streamId = generateStreamId();
    const urls: string[] = [];
    const clientFrames: ClientFrame[] = [];
    const request = vi.fn<typeof fetch>();
    const client = new TsfClient({
      apiOrigin: "http://localhost:8787",
      fetch: request,
      webSocketFactory: (url) => {
        const connection = urls.length;
        urls.push(url);
        return new ScriptedWebSocket(
          connection === 0
            ? [
                { type: "ready" },
                streamMetadataFrame(streamId),
                {
                  type: "caughtUp",
                  nextSeqNum: 10n,
                  lastTimestampMs: 1_000n,
                },
              ]
            : [
                { type: "ready" },
                streamMetadataFrame(streamId),
                readBatch(record(10n, "stable")),
              ],
          connection === 0 ? 1006 : 1000,
          clientFrames,
        );
      },
      retryPolicy: { initialBackoffMs: 0, maxBackoffMs: 0 },
    });

    const reader = await client.connectReader({
      streamId,
      start: { type: "tailOffset", tailOffset: 2n },
      stop: { count: 1n },
      linkSecret: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    });
    await expect(reader.nextRecord()).resolves.toMatchObject({ seqNum: 10n });

    expect(request).not.toHaveBeenCalled();
    expect(urls).toHaveLength(2);
    expect(urls.map(readQuery)).toEqual([
      "tail_offset=2&count=1",
      "seq_num=10&count=1",
    ]);
    expect(clientFrames).toEqual([
      {
        type: "openRead",
        linkSecret: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
      },
      {
        type: "openRead",
        linkSecret: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
      },
    ]);
  });

  it("retries the original tail selector before receiving session state", async () => {
    const streamId = generateStreamId();
    const urls: string[] = [];
    const clientFrames: ClientFrame[] = [];
    const client = new TsfClient({
      apiOrigin: "http://localhost:8787",
      webSocketFactory: (url) => {
        const connection = urls.length;
        urls.push(url);
        return new ScriptedWebSocket(
          [
            { type: "ready" },
            streamMetadataFrame(streamId),
            ...(connection === 0
              ? []
              : [readBatch(record(8n, "stable"))]),
          ],
          connection === 0 ? 1006 : 1000,
          clientFrames,
        );
      },
      retryPolicy: { initialBackoffMs: 0, maxBackoffMs: 0 },
    });

    const reader = await client.connectReader({
      streamId,
      start: { type: "tailOffset", tailOffset: 2n },
      stop: { count: 1n },
    });
    await expect(reader.nextRecord()).resolves.toMatchObject({ seqNum: 8n });

    expect(urls).toHaveLength(2);
    expect(urls.map(readQuery)).toEqual(Array.from(
      { length: 2 },
      () => "tail_offset=2&count=1",
    ));
    expect(clientFrames).toEqual(Array.from({ length: 2 }, () => ({
      type: "openRead",
    })));
  });

  it("uses the implicit default tail offset until the first record", async () => {
    const streamId = generateStreamId();
    let socketUrl = "";
    const clientFrames: ClientFrame[] = [];
    const request = vi.fn<typeof fetch>();
    const client = new TsfClient({
      fetch: request,
      webSocketFactory: (url) => {
        socketUrl = url;
        return new ScriptedWebSocket(
          [
            { type: "ready" },
            streamMetadataFrame(streamId),
            readBatch(record(20n, "default")),
          ],
          1000,
          clientFrames,
        );
      },
    });

    const reader = await client.connectReader({
      streamId,
      stop: { count: 1n },
    });
    await expect(reader.nextRecord()).resolves.toMatchObject({ seqNum: 20n });
    expect(readQuery(socketUrl)).toBe("tail_offset=0&count=1");
    expect(clientFrames).toEqual([{ type: "openRead" }]);
    expect(request).not.toHaveBeenCalled();
  });

  it.each([1006, 1013])(
    "starts a fresh retry burst after an idle reconnect on close %i",
    async (closeCode) => {
      const streamId = generateStreamId();
      let connectionCount = 0;
      const client = new TsfClient({
        webSocketFactory: () => {
          const connection = connectionCount;
          connectionCount += 1;
          return new ScriptedWebSocket(
            connection < 2
              ? [
                  { type: "ready" },
                  streamMetadataFrame(streamId),
                ]
              : [
                  { type: "ready" },
                  streamMetadataFrame(streamId),
                  readBatch(record(0n, "recovered")),
                ],
            connection < 2 ? closeCode : 1000,
          );
        },
        retryPolicy: { maxAttempts: 2, initialBackoffMs: 0, maxBackoffMs: 0 },
      });

      const reader = await client.connectReader({
        streamId,
        start: { type: "seqNum", seqNum: 0n },
      });
      await expect(reader.nextRecord()).resolves.toMatchObject({
        seqNum: 0n,
        data: new TextEncoder().encode("recovered"),
      });
      expect(connectionCount).toBe(3);
    },
  );

  it.each([1002, 1008])(
    "surfaces permanent close %i without reconnecting",
    async (closeCode) => {
      const streamId = generateStreamId();
      let connectionCount = 0;
      const client = new TsfClient({
        webSocketFactory: () => {
          connectionCount += 1;
          return new ScriptedWebSocket(
            [
              { type: "ready" },
              streamMetadataFrame(streamId),
            ],
            closeCode,
          );
        },
        retryPolicy: { initialBackoffMs: 0, maxBackoffMs: 0 },
      });

      const reader = await client.connectReader({
        streamId,
        start: { type: "seqNum", seqNum: 0n },
      });
      await expect(reader.nextRecord()).rejects.toMatchObject({
        code: "websocket_closed",
        closeCode,
      });
      expect(connectionCount).toBe(1);
    },
  );

  it("rejects invalid start selectors before opening a socket", async () => {
    const client = new TsfClient({
      webSocketFactory: () => {
        throw new Error("should not connect");
      },
    });

    await expect(
      client.connectReader({
        streamId: generateStreamId(),
        start: { type: "unknown", value: 1n },
      } as never),
    ).rejects.toMatchObject({ code: "invalid_read_parameter" });
    await expect(
      client.connectReader({
        streamId: generateStreamId(),
        start: { type: "timestampMs", timestampMs: 1n },
        rate: 1,
      }),
    ).rejects.toMatchObject({ code: "invalid_read_parameter" });
    await expect(
      client.connectReader({
        streamId: generateStreamId(),
        start: { type: "tailOffset", tailOffset: BigInt(Number.MAX_SAFE_INTEGER) + 1n },
      }),
    ).rejects.toMatchObject({ code: "invalid_read_parameter" });
  });
});

describe("TsfWriter", () => {
  it("rejects malformed credentials before opening a socket", async () => {
    const webSocketFactory = vi.fn<WebSocketFactory>();
    const client = new TsfClient({ webSocketFactory });

    await expect(client.connectWriter({
      streamId: generateStreamId(),
      linkSecret: "not-a-secret",
    })).rejects.toMatchObject({ code: "invalid_link_secret" });
    expect(webSocketFactory).not.toHaveBeenCalled();
  });

  it("coalesces queued append calls without delaying the first drain", async () => {
    const socket = new ControlledWriterWebSocket();
    const client = new TsfClient({
      webSocketFactory: () => socket,
    });
    const writer = await client.connectWriter({
      streamId: generateStreamId(),
      linkSecret: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    });

    const first = writer.append({ data: "first" });
    await vi.waitFor(() => expect(socket.pendingRecordCount).toBe(1));
    const second = writer.append({ data: "second" });
    const third = writer.append({ data: "third" });

    socket.ackCurrent();
    await expect(first).resolves.toEqual({ writerSeqNum: 0n, seqNum: 0n });
    await vi.waitFor(() => expect(socket.pendingRecordCount).toBe(2));
    expect(socket.appendCount).toBe(2);

    socket.ackCurrent();
    await expect(Promise.all([second, third])).resolves.toEqual([
      { writerSeqNum: 1n, seqNum: 1n },
      { writerSeqNum: 2n, seqNum: 2n },
    ]);
    await writer.close();
  });

  it("coalesces append calls made in one turn into one wire batch", async () => {
    const socket = new ControlledWriterWebSocket();
    const client = new TsfClient({
      webSocketFactory: () => socket,
    });
    const writer = await client.connectWriter({
      streamId: generateStreamId(),
      linkSecret: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    });

    const appends = [
      writer.append({ data: "first" }),
      writer.append({ data: "second" }),
      writer.append({ data: "third" }),
    ];
    await vi.waitFor(() => expect(socket.pendingRecordCount).toBe(3));
    expect(socket.appendCount).toBe(1);

    socket.ackCurrent();
    await expect(Promise.all(appends)).resolves.toEqual([
      { writerSeqNum: 0n, seqNum: 0n },
      { writerSeqNum: 1n, seqNum: 1n },
      { writerSeqNum: 2n, seqNum: 2n },
    ]);
    await writer.close();
  });

  it("flushes accepted appends before closing", async () => {
    const socket = new ControlledWriterWebSocket();
    const client = new TsfClient({ webSocketFactory: () => socket });
    const writer = await client.connectWriter({
      streamId: generateStreamId(),
      linkSecret: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    });
    const appends = [
      writer.append({ data: "first" }),
      writer.append({ data: "second" }),
    ];

    const closing = writer.close();
    await expect(writer.append({ data: "late" })).rejects.toMatchObject({
      code: "writer_closed",
    });
    await vi.waitFor(() => expect(socket.pendingRecordCount).toBe(2));
    socket.ackCurrent();

    await expect(Promise.all(appends)).resolves.toHaveLength(2);
    await expect(closing).resolves.toBeUndefined();
  });

  it("bounds pending records and payload while releasing settled capacity", async () => {
    const socket = new ControlledWriterWebSocket();
    const client = new TsfClient({
      webSocketFactory: () => socket,
    });
    const writer = await client.connectWriter({
      streamId: generateStreamId(),
      linkSecret: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    });

    const recordCalls = Array.from(
      { length: DEFAULT_WRITER_RETAINED_RECORDS },
      () => writer.append({ data: new Uint8Array() }),
    );
    await expect(writer.append({ data: new Uint8Array() })).rejects.toMatchObject({
      code: "client_write_overload",
    });
    await vi.waitFor(() => expect(socket.hasPendingAppend).toBe(true));
    socket.ackCurrent();
    await expect(recordCalls[0]).resolves.toMatchObject({ writerSeqNum: 0n });
    const recordReplacement = writer.append({ data: new Uint8Array() });
    socket.setAutoAck(true);
    await Promise.all([...recordCalls.slice(1), recordReplacement]);

    socket.setAutoAck(false);
    const payloadRecordCount =
      DEFAULT_WRITER_RETAINED_BYTES / MAX_RECORD_BYTES;
    if (!Number.isInteger(payloadRecordCount)) {
      throw new TypeError("writer payload limit must compose from whole records");
    }
    const maxRecord = new Uint8Array(MAX_RECORD_BYTES);
    const payloadCalls = Array.from(
      { length: payloadRecordCount },
      () => writer.append({ data: maxRecord }),
    );
    await expect(writer.append({ data: Uint8Array.of(1) })).rejects.toMatchObject({
      code: "client_write_overload",
    });
    await vi.waitFor(() => expect(socket.hasPendingAppend).toBe(true));
    socket.ackCurrent();
    await expect(payloadCalls[0]).resolves.toBeDefined();
    const payloadReplacement = writer.append({ data: Uint8Array.of(1) });
    socket.setAutoAck(true);
    await Promise.all([...payloadCalls.slice(1), payloadReplacement]);

    expect(socket.appendCount).toBe(
      2 +
        Math.ceil(DEFAULT_WRITER_RETAINED_BYTES / MAX_BATCH_PAYLOAD_BYTES) +
        1,
    );
    await writer.close();
  });

  it("bounds writer authentication and closes the stalled socket", async () => {
    const stalled = new HangingWebSocket(true);
    const client = new TsfClient({
      webSocketFactory: () => stalled,
      webSocketOperationTimeoutMs: 5,
      retryPolicy: { maxAttempts: 1 },
    });

    await expect(
      client.connectWriter({
        streamId: generateStreamId(),
        linkSecret: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
      }),
    ).rejects.toMatchObject({ code: "operation_timeout" });
    expect(stalled.closed).toBe(true);
  });

  it("closes a socket whose opening handshake times out", async () => {
    const stalled = new HangingWebSocket(false);
    const client = new TsfClient({
      webSocketFactory: () => stalled,
      webSocketConnectTimeoutMs: 5,
      retryPolicy: { maxAttempts: 1 },
    });

    await expect(
      client.connectWriter({
        streamId: generateStreamId(),
        linkSecret: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
      }),
    ).rejects.toMatchObject({ code: "operation_timeout" });
    expect(stalled.closed).toBe(true);
  });

  it("rejects invalid retained-backlog limits before opening a socket", async () => {
    const webSocketFactory = vi.fn<WebSocketFactory>();
    const client = new TsfClient({ webSocketFactory });

    await expect(client.connectWriter({
      streamId: generateStreamId(),
      linkSecret: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    }, { maxRetainedBytes: 0 })).rejects.toMatchObject({
      code: "invalid_writer_config",
    });
    expect(webSocketFactory).not.toHaveBeenCalled();
  });

  it("creates a fresh identity for each durable writer", async () => {
    const authFrames: WriterOpenFrame[] = [];
    const client = new TsfClient({
      webSocketFactory: () => new WriterWebSocket(true, authFrames, []),
    });
    const options = {
      streamId: generateStreamId(),
      linkSecret: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    } as const;

    const first = await client.connectWriter(options);
    await first.close();
    const second = await client.connectWriter(options);
    await second.close();

    expect(authFrames).toHaveLength(2);
    expect(authFrames[0]?.clientWriterId).not.toEqual(
      authFrames[1]?.clientWriterId,
    );
  });

  it("reuses its writer identity and sequence when resending after disconnect", async () => {
    const appends: AppendRecord[] = [];
    const authFrames: WriterOpenFrame[] = [];
    let connectionCount = 0;
    const client = new TsfClient({
      apiOrigin: "http://localhost:8787",
      webSocketFactory: () => {
        const shouldAck = connectionCount > 0;
        connectionCount += 1;
        return new WriterWebSocket(
          shouldAck,
          authFrames,
          appends,
          1013,
        );
      },
      retryPolicy: { initialBackoffMs: 0, maxBackoffMs: 0 },
    });
    const writer = await client.connectWriter({
      streamId: generateStreamId(),
      linkSecret: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
      expectedNextSeqNum: 42n,
    });

    await expect(writer.append({ data: "durable\n" })).resolves.toEqual({
      writerSeqNum: 0n,
      seqNum: 42n,
    });
    await writer.close();

    expect(connectionCount).toBe(2);
    expect(authFrames).toHaveLength(2);
    expect(authFrames[0]?.clientWriterId).toEqual(authFrames[1]?.clientWriterId);
    expect(authFrames.map((frame) => frame.expectedNextSeqNum)).toEqual([
      42n,
      undefined,
    ]);
    expect(appends.map((frame) => frame.writerSeqNum)).toEqual([0n, 0n]);
    expect(appends[0]?.data).toEqual(appends[1]?.data);
  });

  it("sends an explicit append batch in one message", async () => {
    const appends: AppendRecord[] = [];
    const client = new TsfClient({
      webSocketFactory: () =>
        new WriterWebSocket(true, [], appends),
    });
    const writer = await client.connectWriter({
      streamId: generateStreamId(),
      linkSecret: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    });

    await expect(writer.appendBatch([
      { data: "first" },
      { data: "second" },
    ])).resolves.toEqual([
      { writerSeqNum: 0n, seqNum: 42n },
      { writerSeqNum: 1n, seqNum: 43n },
    ]);
    expect(appends.map(({ writerSeqNum }) => writerSeqNum)).toEqual([0n, 1n]);
    await writer.close();
  });

  it("splits one submission across bounded wire frames", async () => {
    const socket = new ControlledWriterWebSocket();
    const client = new TsfClient({ webSocketFactory: () => socket });
    const writer = await client.connectWriter({
      streamId: generateStreamId(),
      linkSecret: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    });

    const submission = writer.appendBatch(Array.from(
      { length: 3 },
      () => ({ data: new Uint8Array(MAX_RECORD_BYTES) }),
    ));
    await vi.waitFor(() => expect(socket.pendingRecordCount).toBe(3));
    expect(socket.appendCount).toBe(2);
    socket.ackCurrent();
    await vi.waitFor(() => expect(socket.pendingRecordCount).toBe(1));
    socket.ackCurrent();

    await expect(submission).resolves.toHaveLength(3);
    expect(socket.appendCount).toBe(2);
    await writer.close();
  });

  it("paces a retained backlog larger than the socket window", async () => {
    const socket = new ControlledWriterWebSocket();
    const client = new TsfClient({ webSocketFactory: () => socket });
    const writer = await client.connectWriter({
      streamId: generateStreamId(),
      linkSecret: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    }, {
      maxRetainedRecords: 16,
      maxRetainedBytes: 8 * 1024 * 1024,
    });

    const submission = writer.appendBatch(Array.from(
      { length: 16 },
      () => ({ data: new Uint8Array(MAX_RECORD_BYTES) }),
    ));
    await vi.waitFor(() => expect(socket.pendingRecordCount).toBe(10));
    expect(socket.appendCount).toBe(5);

    socket.setAutoAck(true);
    await expect(submission).resolves.toHaveLength(16);
    await writer.close();
  });

  it("splits a logical record into contiguous physical parts", async () => {
    const appends: AppendRecord[] = [];
    const client = new TsfClient({
      webSocketFactory: () => new WriterWebSocket(true, [], appends),
    });
    const writer = await client.connectWriter({
      streamId: generateStreamId(),
      linkSecret: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    });

    await expect(writer.appendLogical({
      data: new Uint8Array(MAX_RECORD_BYTES + 1),
    })).resolves.toHaveLength(2);
    expect(appends.map(({ writerSeqNum, part, data }) => ({
      writerSeqNum,
      part,
      bytes: data.byteLength,
    }))).toEqual([
      {
        writerSeqNum: 0n,
        part: { index: 0, isFinal: false },
        bytes: MAX_RECORD_BYTES,
      },
      {
        writerSeqNum: 1n,
        part: { index: 1, isFinal: true },
        bytes: 1,
      },
    ]);
    await writer.close();
  });

  it("retains acknowledged progress and resends only the unacknowledged suffix", async () => {
    const appends: AppendRecord[] = [];
    const authFrames: WriterOpenFrame[] = [];
    let connection = 0;
    const client = new TsfClient({
      webSocketFactory: () => {
        const current = connection;
        connection += 1;
        return new WriterWebSocket(
          true,
          authFrames,
          appends,
          1013,
          "interrupted",
          current === 0 ? 1 : undefined,
          current === 0 ? 42n : 43n,
        );
      },
      retryPolicy: { initialBackoffMs: 0, maxBackoffMs: 0 },
    });
    const writer = await client.connectWriter({
      streamId: generateStreamId(),
      linkSecret: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    });

    await expect(writer.appendBatch([
      { data: "first" },
      { data: "second" },
      { data: "third" },
    ])).resolves.toEqual([
      { writerSeqNum: 0n, seqNum: 42n },
      { writerSeqNum: 1n, seqNum: 43n },
      { writerSeqNum: 2n, seqNum: 44n },
    ]);
    expect(appends.map(({ writerSeqNum }) => writerSeqNum)).toEqual([
      0n,
      1n,
      2n,
      1n,
      2n,
    ]);
    expect(authFrames[0]?.clientWriterId).toEqual(authFrames[1]?.clientWriterId);
    await writer.close();
  });

  it("surfaces policy closes without resending", async () => {
    const appends: AppendRecord[] = [];
    const authFrames: WriterOpenFrame[] = [];
    let connectionCount = 0;
    const client = new TsfClient({
      webSocketFactory: () => {
        connectionCount += 1;
        return new WriterWebSocket(
          false,
          authFrames,
          appends,
          1008,
        );
      },
      retryPolicy: { initialBackoffMs: 0, maxBackoffMs: 0 },
    });
    const writer = await client.connectWriter({
      streamId: generateStreamId(),
      linkSecret: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    });

    const error = await writer.append({ data: "rejected\n" }).catch(
      (caught: unknown) => caught,
    );
    expect(error).toMatchObject({ code: "writer_durability_unknown" });
    expect((error as Error).cause).toMatchObject({
      code: "websocket_closed",
      closeCode: 1008,
    });
    expect(connectionCount).toBe(1);
    expect(appends).toHaveLength(1);
  });

  it("surfaces an opening sequence mismatch without retrying", async () => {
    const appends: AppendRecord[] = [];
    let connectionCount = 0;
    const client = new TsfClient({
      webSocketFactory: () => {
        connectionCount += 1;
        return new WriterWebSocket(
          false,
          [],
          appends,
          1008,
          "sequence_mismatch",
        );
      },
      retryPolicy: { initialBackoffMs: 0, maxBackoffMs: 0 },
    });
    const writer = await client.connectWriter({
      streamId: generateStreamId(),
      linkSecret: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
      expectedNextSeqNum: 0n,
    });

    await expect(writer.append({ data: "rejected\n" })).rejects.toMatchObject({
      code: "sequence_mismatch",
    });
    expect(connectionCount).toBe(1);
    expect(appends).toHaveLength(1);
  });

  it("becomes terminal when an acknowledgement is lost and reconnects exhaust", async () => {
    const appends: AppendRecord[] = [];
    const authFrames: WriterOpenFrame[] = [];
    let connectionCount = 0;
    const client = new TsfClient({
      webSocketFactory: () => {
        connectionCount += 1;
        return (connectionCount === 1
          ? new WriterWebSocket(false, authFrames, appends)
          : new HangingWebSocket(true));
      },
      webSocketOperationTimeoutMs: 5,
      retryPolicy: { maxAttempts: 2, initialBackoffMs: 0, maxBackoffMs: 0 },
    });
    const writer = await client.connectWriter({
      streamId: generateStreamId(),
      linkSecret: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    });

    const uncertain = writer.append({ data: "possibly durable\n" });
    const queued = writer.append({ data: "must not reuse sequence\n" });

    await expect(uncertain).rejects.toMatchObject({
      code: "writer_durability_unknown",
    });
    await expect(queued).rejects.toMatchObject({
      code: "writer_durability_unknown",
    });
    await expect(writer.append({ data: "still terminal\n" })).rejects.toMatchObject({
      code: "writer_durability_unknown",
    });
    expect(connectionCount).toBe(2);
    expect(appends.map((frame) => frame.writerSeqNum)).toEqual([0n, 1n]);
  });
});

class HangingWebSocket extends EventTarget {
  public readonly protocol = TSF_WEBSOCKET_PROTOCOL;
  public readyState = 0;
  public binaryType: BinaryType = "blob";
  public closed = false;

  public constructor(
    open: boolean,
    private readonly frames: readonly ServerFrame[] = [],
    private readonly reportsClose = true,
  ) {
    super();
    if (open) {
      queueMicrotask(() => {
        if (!this.closed) {
          this.readyState = 1;
          this.dispatchEvent(new Event("open"));
          for (const frame of this.frames) {
            this.dispatchEvent(
              new MessageEvent("message", {
                data: arrayBuffer(encodeServerFrame(frame)),
              }),
            );
          }
        }
      });
    }
  }

  public send(): void {}

  public close(code = 1000, reason = ""): void {
    if (this.closed) {
      return;
    }
    this.closed = true;
    this.readyState = 3;
    if (!this.reportsClose) {
      return;
    }
    const event = new Event("close") as CloseEvent;
    Object.defineProperties(event, {
      code: { value: code },
      reason: { value: reason },
      wasClean: { value: true },
    });
    this.dispatchEvent(event);
  }
}

class ScriptedWebSocket extends EventTarget {
  public readonly protocol = TSF_WEBSOCKET_PROTOCOL;
  public readyState = 0;
  public binaryType: BinaryType = "blob";
  #closed = false;

  public constructor(
    private readonly frames: readonly ServerFrame[],
    private readonly closeCode: number,
    private readonly clientFrames: ClientFrame[] = [],
  ) {
    super();
    queueMicrotask(() => this.#open());
  }

  public send(data: Uint8Array<ArrayBuffer>): void {
    this.clientFrames.push(decodeClientFrame(data));
  }

  public close(code = 1000, reason = ""): void {
    this.#dispatchClose(code, reason, true);
  }

  #open(): void {
    this.readyState = 1;
    this.dispatchEvent(new Event("open"));
    for (const frame of this.frames) {
      this.dispatchEvent(
        new MessageEvent("message", {
          data: arrayBuffer(encodeServerFrame(frame)),
        }),
      );
    }
    setTimeout(
      () =>
        this.#dispatchClose(
          this.closeCode,
          "script complete",
          this.closeCode === 1000,
        ),
      0,
    );
  }

  #dispatchClose(code: number, reason: string, wasClean: boolean): void {
    if (this.#closed) {
      return;
    }
    this.#closed = true;
    this.readyState = 3;
    const event = new Event("close") as CloseEvent;
    Object.defineProperties(event, {
      code: { value: code },
      reason: { value: reason },
      wasClean: { value: wasClean },
    });
    this.dispatchEvent(event);
  }
}

type WriterOpenFrame = Extract<
  ClientFrame,
  { type: "openWrite" }
>;
type AppendRecord = Extract<
  ClientFrame,
  { type: "appendBatch" }
>["records"][number];

class WriterWebSocket extends EventTarget {
  public readonly protocol = TSF_WEBSOCKET_PROTOCOL;
  public readyState = 0;
  public binaryType: BinaryType = "blob";
  #closed = false;

  public constructor(
    private readonly shouldAck: boolean,
    private readonly openFrames: WriterOpenFrame[],
    private readonly appends: AppendRecord[],
    private readonly disconnectCode = 1006,
    private readonly disconnectReason = "interrupted",
    private readonly acknowledgedRecords?: number,
    private nextStreamSeqNum = 42n,
  ) {
    super();
    queueMicrotask(() => {
      this.readyState = 1;
      this.dispatchEvent(new Event("open"));
    });
  }

  public send(data: Uint8Array<ArrayBuffer>): void {
    const frame = decodeClientFrame(data);
    if (frame.type === "openWrite") {
      this.openFrames.push(frame);
      this.#emit({ type: "ready" });
      return;
    }
    if (frame.type !== "appendBatch") {
      throw new Error(`unexpected ${frame.type} frame`);
    }
    const first = frame.records[0];
    const last = frame.records.at(-1);
    if (first === undefined || last === undefined) {
      throw new Error("empty append batch");
    }
    this.appends.push(...frame.records);
    if (this.shouldAck) {
      const acknowledgedRecords = Math.min(
        this.acknowledgedRecords ?? frame.records.length,
        frame.records.length,
      );
      const lastAcknowledged = frame.records[acknowledgedRecords - 1];
      if (lastAcknowledged === undefined) {
        throw new Error("writer acknowledgement was empty");
      }
      this.#emit({
        type: "appendAck",
        writerStartSeqNum: first.writerSeqNum,
        writerEndSeqNum: lastAcknowledged.writerSeqNum + 1n,
        startSeqNum: this.nextStreamSeqNum,
        endSeqNum: this.nextStreamSeqNum + BigInt(acknowledgedRecords),
      });
      this.nextStreamSeqNum += BigInt(acknowledgedRecords);
      if (acknowledgedRecords < frame.records.length) {
        this.#dispatchClose(
          this.disconnectCode,
          this.disconnectReason,
          this.disconnectCode === 1000,
        );
      }
    } else {
      this.#dispatchClose(
        this.disconnectCode,
        this.disconnectReason,
        this.disconnectCode === 1000,
      );
    }
  }

  public close(code = 1000, reason = ""): void {
    this.#dispatchClose(code, reason, true);
  }

  #emit(frame: ServerFrame): void {
    this.dispatchEvent(
      new MessageEvent("message", {
        data: arrayBuffer(encodeServerFrame(frame)),
      }),
    );
  }

  #dispatchClose(code: number, reason: string, wasClean: boolean): void {
    if (this.#closed) {
      return;
    }
    this.#closed = true;
    this.readyState = 3;
    const event = new Event("close") as CloseEvent;
    Object.defineProperties(event, {
      code: { value: code },
      reason: { value: reason },
      wasClean: { value: wasClean },
    });
    this.dispatchEvent(event);
  }
}

class ControlledWriterWebSocket extends EventTarget {
  public readonly protocol = TSF_WEBSOCKET_PROTOCOL;
  public readyState = 0;
  public binaryType: BinaryType = "blob";
  public appendCount = 0;
  #autoAck = false;
  #closed = false;
  readonly #pendingAppends: (readonly AppendRecord[])[] = [];

  public constructor() {
    super();
    queueMicrotask(() => {
      this.readyState = 1;
      this.dispatchEvent(new Event("open"));
    });
  }

  public get hasPendingAppend(): boolean {
    return this.#pendingAppends.length > 0;
  }

  public get pendingRecordCount(): number {
    return this.#pendingAppends.reduce(
      (total, records) => total + records.length,
      0,
    );
  }

  public send(data: Uint8Array<ArrayBuffer>): void {
    const frame = decodeClientFrame(data);
    if (frame.type === "openWrite") {
      this.#emit({ type: "ready" });
      return;
    }
    if (frame.type !== "appendBatch" || frame.records.length === 0) {
      throw new Error(`unexpected ${frame.type} frame`);
    }
    this.appendCount += 1;
    this.#pendingAppends.push(frame.records);
    if (this.#autoAck && this.#pendingAppends.length === 1) {
      queueMicrotask(() => this.ackCurrent());
    }
  }

  public ackCurrent(): void {
    const records = this.#pendingAppends.shift();
    const first = records?.[0];
    const last = records?.at(-1);
    if (first === undefined || last === undefined) {
      throw new Error("writer has no append awaiting acknowledgement");
    }
    this.#emit({
      type: "appendAck",
      writerStartSeqNum: first.writerSeqNum,
      writerEndSeqNum: last.writerSeqNum + 1n,
      startSeqNum: first.writerSeqNum,
      endSeqNum: last.writerSeqNum + 1n,
    });
    if (this.#autoAck && this.#pendingAppends.length > 0) {
      queueMicrotask(() => this.ackCurrent());
    }
  }

  public setAutoAck(enabled: boolean): void {
    this.#autoAck = enabled;
    if (enabled && this.#pendingAppends.length > 0) {
      queueMicrotask(() => this.ackCurrent());
    }
  }

  public close(code = 1000, reason = ""): void {
    if (this.#closed) {
      return;
    }
    this.#closed = true;
    this.readyState = 3;
    const event = new Event("close") as CloseEvent;
    Object.defineProperties(event, {
      code: { value: code },
      reason: { value: reason },
      wasClean: { value: true },
    });
    this.dispatchEvent(event);
  }

  #emit(frame: ServerFrame): void {
    this.dispatchEvent(
      new MessageEvent("message", {
        data: arrayBuffer(encodeServerFrame(frame)),
      }),
    );
  }
}

function record(seqNum: bigint, text: string): ReadRecord {
  return {
    seqNum,
    timestampMs: 1_786_000_000_000n + seqNum,
    writerId: parseWriterId(new Uint8Array(16)),
    writerSeqNum: seqNum,
    part: UNSPLIT_PART,
    format: RecordFormat.Transcript,
    data: new TextEncoder().encode(text),
  };
}

function readBatch(recordValue: ReadRecord): ServerFrame {
  return { type: "readBatch", records: [recordValue] };
}

function readQuery(url: string): string {
  return new URL(url).searchParams.toString();
}

function streamMetadataFrame(
  streamId: StreamId,
): Extract<ServerFrame, { readonly type: "streamMetadata" }> {
  return {
    type: "streamMetadata",
    stream: {
      stream_id: streamId,
      title: null,
      visibility: "public",
      created_at: "2026-08-13T00:00:00Z",
      expires_at: "2026-08-21T00:00:00Z",
    },
  };
}

function arrayBuffer(bytes: Uint8Array): ArrayBuffer {
  const output = new ArrayBuffer(bytes.byteLength);
  new Uint8Array(output).set(bytes);
  return output;
}
