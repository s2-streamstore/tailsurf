import {
  encodeReadQuery,
  generateClientWriterId,
  parseStreamId,
  type ClientWriterId,
  type StreamKind,
  type StreamId,
} from "@tailsurf/protocol";

import { TsfClientError } from "./errors.js";
import {
  DefaultTsfReadSession,
  normalizeReadOptions,
  validateReadStreamMetadata,
  type NormalizedReadOptions,
  type ReadOptions,
  type TsfReadSession,
} from "./reader.js";
import { BaseTsfClient, type HttpClientOptions } from "./rest.js";
import {
  connectInitialSocket,
  connectSocket,
  dataPlaneUrl,
  expectReady,
  expectReadHandshake,
  type FrameSocket,
  requireLinkSecret,
  type SocketPolicy,
  u64,
  type WebSocketFactory,
  type WebSocketLike,
} from "./socket.js";
import {
  DefaultTsfWriter,
  type TsfWriter,
} from "./writer.js";
import { streamMetadataFromWire } from "./models.js";
import { integerOption, MAX_TIMER_DELAY_MS, withTimeout } from "./retry.js";

export interface TsfClientOptions extends HttpClientOptions {
  readonly webSocketFactory?: WebSocketFactory;
  readonly webSocketConnectTimeoutMs?: number;
  readonly webSocketProgressTimeoutMs?: number;
}

export interface DurableWriterOptions {
  readonly streamId: StreamId;
  readonly linkSecret: string;
  readonly expectedNextSeqNum?: bigint;
}

interface NormalizedWriteOptions {
  readonly streamId: StreamId;
  readonly linkSecret: string;
  readonly clientWriterId: ClientWriterId;
  expectedNextSeqNum?: bigint;
  streamKind?: StreamKind;
}

type ReadOperation = "read" | "terminal/input/read" | "terminal/output/read";
type WriteOperation = "write" | "terminal/input/write" | "terminal/output/write";

export class TsfClient extends BaseTsfClient {
  readonly #socketPolicy: SocketPolicy;

  public constructor(options: TsfClientOptions = {}) {
    super(options);
    this.#socketPolicy = {
      webSocketFactory: options.webSocketFactory ?? defaultWebSocketFactory,
      webSocketConnectTimeoutMs: integerOption(
        options.webSocketConnectTimeoutMs ?? 10_000,
        "webSocketConnectTimeoutMs",
        1,
        MAX_TIMER_DELAY_MS,
      ),
      webSocketProgressTimeoutMs: integerOption(
        options.webSocketProgressTimeoutMs ?? 30_000,
        "webSocketProgressTimeoutMs",
        1,
        MAX_TIMER_DELAY_MS,
      ),
      boundedOperationAttempts: this.boundedOperationAttempts,
    };
  }

  public async connectReader(
    options: ReadOptions,
  ): Promise<TsfReadSession> {
    return this.#connectReader(options, "read");
  }

  /** Reads the browser-visible output of a terminal session. */
  public async connectTerminalOutputReader(
    options: ReadOptions,
  ): Promise<TsfReadSession> {
    return this.#connectReader(options, "terminal/output/read");
  }

  /** Reads controller input for a terminal host. Requires an owner link. */
  public async connectTerminalInputReader(
    options: ReadOptions,
  ): Promise<TsfReadSession> {
    return this.#connectReader(options, "terminal/input/read");
  }

  async #connectReader(
    options: ReadOptions,
    operation: ReadOperation,
  ): Promise<TsfReadSession> {
    const normalized = normalizeReadOptions(options);
    const connect = (signal: AbortSignal) =>
      this.#connectReadSocket(normalized, operation, signal);
    return new DefaultTsfReadSession(
      normalized,
      await connectInitialSocket(
        () => this.#connectReadSocket(normalized, operation, options.signal),
        this.#socketPolicy,
        options.signal,
      ),
      connect,
      this.#socketPolicy,
    );
  }

  public async connectWriter(
    options: DurableWriterOptions,
  ): Promise<TsfWriter> {
    return this.#connectWriter(options, "write");
  }

  /** Sends input and resize events from a terminal controller. */
  public async connectTerminalInputWriter(
    options: DurableWriterOptions,
  ): Promise<TsfWriter> {
    return this.#connectWriter(options, "terminal/input/write");
  }

  /** Publishes PTY output from a terminal host. Requires an owner link. */
  public async connectTerminalOutputWriter(
    options: DurableWriterOptions,
  ): Promise<TsfWriter> {
    return this.#connectWriter(options, "terminal/output/write");
  }

  async #connectWriter(
    options: DurableWriterOptions,
    operation: WriteOperation,
  ): Promise<TsfWriter> {
    const normalized: NormalizedWriteOptions = {
      streamId: parseStreamId(options.streamId),
      linkSecret: requireLinkSecret(options.linkSecret),
      clientWriterId: generateClientWriterId(),
      ...(options.expectedNextSeqNum === undefined
        ? {}
        : {
          expectedNextSeqNum: u64(
            options.expectedNextSeqNum,
            "expectedNextSeqNum",
          ),
        }),
    };
    const connect = () => this.#connectAppendSocket(normalized, operation);
    const socket = await connectInitialSocket(connect, this.#socketPolicy);
    const streamKind = normalized.streamKind;
    if (streamKind === undefined) {
      socket.close();
      throw new TsfClientError("invalid_api_response", "writer handshake omitted stream kind");
    }
    return new DefaultTsfWriter(
      socket,
      connect,
      this.#socketPolicy,
      streamKind,
    );
  }

  async #connectReadSocket(
    options: NormalizedReadOptions,
    operation: ReadOperation,
    signal?: AbortSignal,
  ): Promise<FrameSocket> {
    const url = new URL(dataPlaneUrl(this.apiOrigin, options.streamId, operation));
    url.search = encodeReadQuery({
      start: options.start,
      stop: options.stop,
      rate: options.rate,
    }).toString();
    const socket = await connectSocket(
      url.href,
      this.#socketPolicy,
      signal,
      { type: "openRead", linkSecret: options.linkSecret },
    );
    try {
      const metadata = await withTimeout(
        expectReadHandshake(socket),
        this.#socketPolicy.webSocketProgressTimeoutMs,
        "reader handshake",
        signal,
      );
      const stream = streamMetadataFromWire(metadata);
      validateReadStreamMetadata(options, stream);
      options.streamMetadata = stream;
      options.onStreamMetadata?.(stream);
      signal?.throwIfAborted();
      return socket;
    } catch (error) {
      socket.close();
      throw error;
    }
  }

  async #connectAppendSocket(
    options: NormalizedWriteOptions,
    operation: WriteOperation,
  ): Promise<FrameSocket> {
    const socket = await connectSocket(
      dataPlaneUrl(this.apiOrigin, options.streamId, operation),
      this.#socketPolicy,
      undefined,
      {
        type: "openWrite",
        clientWriterId: options.clientWriterId,
        linkSecret: options.linkSecret,
        ...(options.expectedNextSeqNum === undefined
          ? {}
          : { expectedNextSeqNum: options.expectedNextSeqNum }),
      },
    );
    try {
      const kind = await withTimeout(
        expectReady(socket),
        this.#socketPolicy.webSocketProgressTimeoutMs,
        "writer authentication",
      );
      if (options.streamKind !== undefined && options.streamKind !== kind) {
        throw new TsfClientError(
          "invalid_api_response",
          "stream kind changed while reconnecting the writer",
        );
      }
      options.streamKind = kind;
      delete options.expectedNextSeqNum;
      return socket;
    } catch (error) {
      socket.close();
      throw error;
    }
  }
}

function defaultWebSocketFactory(url: string, protocol: string): WebSocketLike {
  if (globalThis.WebSocket === undefined) {
    throw new TsfClientError(
      "websocket_unavailable",
      "WebSocket is unavailable; provide a webSocketFactory",
    );
  }
  return new globalThis.WebSocket(url, protocol);
}
