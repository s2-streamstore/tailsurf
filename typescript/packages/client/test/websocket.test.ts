import {
  decodeClientFrame,
  encodeServerFrame,
  generateStreamId,
  parseWriterId,
  TSF_WEBSOCKET_PROTOCOL,
  UNSPLIT_PART,
  WRITER_ID_BYTE_LENGTH,
  type ClientFrame,
  type ReadRecord,
  type ServerFrame,
  type StreamId,
} from "@tailsurf/protocol";
import { describe, expect, it, vi } from "vitest";

import { TsfClient, type WebSocketFactory } from "../src/index.js";
import { FrameSocket } from "../src/socket.js";

const LINK_SECRET = "A".repeat(32);

describe("FrameSocket", () => {
  it("closes after invalid input", async () => {
    const transport = new TestWebSocket();
    const socket = new FrameSocket(transport);
    await socket.opened;

    transport.serverBytes(Uint8Array.of(255));

    await expect(socket.nextFrame()).rejects.toBeInstanceOf(Error);
    expect(transport.closed).toBe(true);
  });

});

describe("TsfReadSession", () => {
  it("cancels an in-flight connection without retrying", async () => {
    const transport = new TestWebSocket(undefined, false);
    const controller = new AbortController();
    const factory = vi.fn<WebSocketFactory>(() => transport);
    const client = new TsfClient({ webSocketFactory: factory });
    const connecting = client.connectReader({
      streamId: generateStreamId(),
      start: { type: "seqNum", seqNum: 0n },
      signal: controller.signal,
    });
    const reason = new Error("connection cancelled");

    controller.abort(reason);

    await expect(connecting).rejects.toBe(reason);
    expect(transport.closed).toBe(true);
    expect(factory).toHaveBeenCalledOnce();
  });

  it.each([1006, 1013])(
    "resumes a finite read after close %i",
    async (closeCode) => {
      const streamId = generateStreamId();
      const urls: string[] = [];
      const opens: ClientFrame[] = [];
      const client = new TsfClient({
        webSocketFactory: (url) => {
          const connection = urls.length;
          urls.push(url);
          return readerSocket(
            streamId,
            [record(BigInt(5 + connection), connection === 0 ? "first" : "second")],
            connection === 0 ? closeCode : 1000,
            opens,
          );
        },
      });
      const reader = await client.connectReader({
        streamId,
        start: { type: "seqNum", seqNum: 5n },
        stop: { count: 2n },
      });

      await expect(reader.nextRecord()).resolves.toMatchObject({ seqNum: 5n });
      await expect(reader.nextRecord()).resolves.toMatchObject({ seqNum: 6n });
      await expect(reader.nextRecord()).resolves.toBeUndefined();
      expect(urls.map((url) => new URL(url).searchParams.toString())).toEqual([
        "seq_num=5&count=2",
        "seq_num=6&count=1",
      ]);
      expect(opens).toEqual([{ type: "openRead" }, { type: "openRead" }]);
    },
  );
});

describe("TsfWriter", () => {
  it("retains acknowledged progress and resends only the pending suffix", async () => {
    const appends: AppendRecord[] = [];
    const opens: WriterOpenFrame[] = [];
    let connection = 0;
    const client = new TsfClient({
      webSocketFactory: () => {
        const current = connection++;
        return writerSocket({
          opens,
          appends,
          ...(current === 0 ? { acknowledgedRecords: 1 } : {}),
          nextSeqNum: current === 0 ? 42n : 43n,
          closeAfterAck: current === 0,
        });
      },
    });
    const writer = await client.connectWriter({
      streamId: generateStreamId(),
      linkSecret: LINK_SECRET,
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
    expect(opens[0]?.clientWriterId).toEqual(opens[1]?.clientWriterId);
    await writer.close();
  });

  it("can abort ambiguous recovery", async () => {
    let connection = 0;
    const client = new TsfClient({
      webSocketFactory: () => {
        connection += 1;
        return connection === 1
          ? writerSocket({ closeWithoutAck: true })
          : new TestWebSocket(undefined, false);
      },
      webSocketConnectTimeoutMs: 5,
      boundedOperationAttempts: 1,
    });
    const writer = await client.connectWriter({
      streamId: generateStreamId(),
      linkSecret: LINK_SECRET,
    });
    const append = writer.append({ data: "possibly durable" });
    const rejection = expect(append).rejects.toMatchObject({
      code: "writer_durability_unknown",
      cause: { code: "writer_aborted" },
    });
    await vi.waitFor(() => expect(connection).toBeGreaterThan(1));

    writer.abort();

    await rejection;
  });
});

type WriterOpenFrame = Extract<ClientFrame, { type: "openWrite" }>;
type AppendRecord = Extract<ClientFrame, { type: "appendBatch" }>["records"][number];

interface WriterSocketOptions {
  readonly opens?: WriterOpenFrame[];
  readonly appends?: AppendRecord[];
  readonly acknowledgedRecords?: number;
  readonly nextSeqNum?: bigint;
  readonly closeAfterAck?: boolean;
  readonly closeWithoutAck?: boolean;
}

function writerSocket(options: WriterSocketOptions): TestWebSocket {
  return new TestWebSocket((frame, socket) => {
    if (frame.type === "openWrite") {
      options.opens?.push(frame);
      socket.serverFrame({ type: "ready", kind: "transcript" });
      return;
    }
    if (frame.type !== "appendBatch" || frame.records.length === 0) {
      throw new Error(`unexpected ${frame.type} frame`);
    }
    options.appends?.push(...frame.records);
    if (options.closeWithoutAck) {
      socket.serverClose(1006, "interrupted");
      return;
    }
    const count = Math.min(
      options.acknowledgedRecords ?? frame.records.length,
      frame.records.length,
    );
    const first = frame.records[0];
    const last = frame.records[count - 1];
    if (first === undefined || last === undefined) {
      throw new Error("empty acknowledgement");
    }
    const nextSeqNum = options.nextSeqNum ?? 0n;
    socket.serverFrame({
      type: "appendAck",
      writerStartSeqNum: first.writerSeqNum,
      writerEndSeqNum: last.writerSeqNum + 1n,
      startSeqNum: nextSeqNum,
      endSeqNum: nextSeqNum + BigInt(count),
    });
    if (options.closeAfterAck) {
      socket.serverClose(1013, "interrupted");
    }
  });
}

function readerSocket(
  streamId: StreamId,
  records: readonly ReadRecord[],
  closeCode: number,
  opens: ClientFrame[],
): TestWebSocket {
  return new TestWebSocket((frame, socket) => {
    if (frame.type !== "openRead") {
      throw new Error(`unexpected ${frame.type} frame`);
    }
    opens.push(frame);
    socket.serverFrame({ type: "ready", kind: "transcript" });
    socket.serverFrame(streamMetadataFrame(streamId));
    socket.serverFrame({ type: "readBatch", records });
    setTimeout(() => socket.serverClose(closeCode, "script complete"), 0);
  });
}

class TestWebSocket extends EventTarget {
  public readonly protocol = TSF_WEBSOCKET_PROTOCOL;
  public readyState = 0;
  public binaryType = "blob";
  public closed = false;

  public constructor(
    private readonly onClientFrame?: (
      frame: ClientFrame,
      socket: TestWebSocket,
    ) => void,
    open = true,
  ) {
    super();
    if (open) {
      queueMicrotask(() => {
        if (!this.closed) {
          this.readyState = 1;
          this.dispatchEvent(new Event("open"));
        }
      });
    }
  }

  public send(data: Uint8Array<ArrayBuffer>): void {
    this.onClientFrame?.(decodeClientFrame(data), this);
  }

  public close(code = 1000, reason = ""): void {
    this.serverClose(code, reason, true);
  }

  public serverFrame(frame: ServerFrame): void {
    this.serverBytes(encodeServerFrame(frame));
  }

  public serverBytes(data: Uint8Array): void {
    this.dispatchEvent(new MessageEvent("message", { data: arrayBuffer(data) }));
  }

  public serverClose(code: number, reason: string, wasClean = false): void {
    if (this.closed) {
      return;
    }
    this.closed = true;
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

function record(seqNum: bigint, text: string): ReadRecord {
  return {
    seqNum,
    timestampMs: 1_786_000_000_000n + seqNum,
    writerId: parseWriterId(new Uint8Array(WRITER_ID_BYTE_LENGTH)),
    writerSeqNum: seqNum,
    part: UNSPLIT_PART,
    data: new TextEncoder().encode(text),
  };
}

function streamMetadataFrame(
  streamId: StreamId,
): Extract<ServerFrame, { type: "streamMetadata" }> {
  return {
    type: "streamMetadata",
    stream: {
      stream_id: streamId,
      kind: "transcript",
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
