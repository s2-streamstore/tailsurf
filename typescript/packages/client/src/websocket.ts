import {
  encodeReadQuery,
  generateClientWriterId,
  parseStreamId,
  type ClientWriterId,
  type StreamId,
} from "@tailsurf/protocol";

import { TsfClientError } from "./errors.js";
import {
  DefaultTsfReadSession,
  normalizeReadOptions,
  openReadFrame,
  readRequestForConnection,
  type NormalizedReadOptions,
  type ReadOptions,
  type TsfReadSession,
} from "./reader.js";
import { BaseTsfClient, type RestClientOptions } from "./rest.js";
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
  withTimeout,
  type WebSocketFactory,
  type WebSocketLike,
} from "./socket.js";
import {
  DefaultTsfWriter,
  type TsfWriter,
} from "./writer.js";
import { streamMetadataFromWire } from "./models.js";
import { integerOption, MAX_TIMER_DELAY_MS } from "./retry.js";

export interface TsfClientOptions extends RestClientOptions {
  readonly webSocketFactory?: WebSocketFactory;
  readonly webSocketConnectTimeoutMs?: number;
  readonly webSocketProgressTimeoutMs?: number;
}

export interface WriteStreamOptions {
  readonly streamId: StreamId;
  readonly linkSecret: string;
  readonly expectedNextSeqNum?: bigint;
}

interface NormalizedWriteOptions {
  readonly streamId: StreamId;
  readonly linkSecret: string;
  readonly clientWriterId: ClientWriterId;
  expectedNextSeqNum?: bigint;
}

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
    const normalized = normalizeReadOptions(options);
    const connect = (signal: AbortSignal) =>
      this.#connectReadSocket(normalized, signal);
    return new DefaultTsfReadSession(
      normalized,
      await connectInitialSocket(
        () => this.#connectReadSocket(normalized, options.signal),
        this.#socketPolicy,
        options.signal,
      ),
      connect,
      this.#socketPolicy,
    );
  }

  public async connectWriter(
    options: WriteStreamOptions,
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
    const connect = () => this.#connectAppendSocket(normalized);
    return new DefaultTsfWriter(
      await connectInitialSocket(connect, this.#socketPolicy),
      connect,
      this.#socketPolicy,
    );
  }

  async #connectReadSocket(
    options: NormalizedReadOptions,
    signal?: AbortSignal,
  ): Promise<FrameSocket> {
    const url = new URL(dataPlaneUrl(this.apiOrigin, options.streamId, "read"));
    const request = readRequestForConnection(options);
    url.search = encodeReadQuery(request).toString();
    const socket = await connectSocket(
      url.href,
      this.#socketPolicy,
      signal,
      openReadFrame(options.linkSecret),
    );
    try {
      const metadata = await withTimeout(
        expectReadHandshake(socket),
        this.#socketPolicy.webSocketProgressTimeoutMs,
        "reader handshake",
        signal,
      );
      const stream = streamMetadataFromWire(metadata);
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
  ): Promise<FrameSocket> {
    const socket = await connectSocket(
      dataPlaneUrl(this.apiOrigin, options.streamId, "write"),
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
      await withTimeout(
        expectReady(socket),
        this.#socketPolicy.webSocketProgressTimeoutMs,
        "writer authentication",
      );
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
