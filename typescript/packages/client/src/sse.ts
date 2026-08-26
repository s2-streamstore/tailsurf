import {
  encodeReadQuery,
  streamMetadataSchema,
  sseCaughtUpDataSchema,
  sseReadBatchDataSchema,
  MAX_SSE_EVENT_BYTES,
  MAX_SSE_UNTERMINATED_EVENT_BYTES,
  MAX_SSE_READ_BATCH_PAYLOAD_BYTES,
  MAX_RECORD_PAYLOAD_BYTES,
  RecordFormat,
  decodeBase64url,
  parseWriterId,
  recordPayloadBytes,
  resolvedRecordFormat,
  decodeSseResumeCursor,
  type CaughtUpPosition,
  type ReadRecord,
  type SseResumeCursor,
} from "@tailsurf/protocol";

import {
  httpStatusError,
  TsfClientError,
  TsfHttpError,
} from "./errors.js";
import { streamMetadataFromWire, type StreamMetadata } from "./models.js";
import {
  BaseTsfReadSession,
  normalizeReadOptions,
  readExhausted,
  type NormalizedReadOptions,
  type ReadOptions,
  type TsfReadSession,
} from "./reader.js";
import {
  INITIAL_RETRY_BACKOFF_MS,
  isRetryableHttpStatus,
  jitteredBackoffMs,
  MAX_RETRY_BACKOFF_MS,
} from "./retry.js";
import { sleep, withTimeout } from "./socket.js";

const API_PREFIX = "/api/v1";
const CARRIAGE_RETURN = 0x0d;
const LINE_FEED = 0x0a;
const MAX_RETAINED_SSE_BUFFER_BYTES = 64 * 1024;
interface SseConnectOptions {
  readonly fetch: typeof globalThis.fetch;
  readonly apiOrigin: string;
  readonly httpRequestTimeoutMs: number;
  readonly boundedOperationAttempts: number;
}

interface SseConnection {
  readonly events: AsyncIterator<ParsedSseEvent>;
  readonly stream: StreamMetadata;
  readonly resumeEventId?: string;
  close(): void;
}

interface SseRequest {
  readonly url: string;
  readonly linkSecret?: string;
  readonly finite: boolean;
}

interface ParsedSseEvent {
  readonly event: string;
  readonly data: string;
  readonly id?: string;
}

interface ParsedSseResumeCursor extends SseResumeCursor {
  readonly value: string;
}

export async function connectSseReader(
  options: ReadOptions,
  connectionOptions: SseConnectOptions,
): Promise<TsfReadSession> {
  const normalized = normalizeReadOptions(options);
  const request = sseRequest(connectionOptions.apiOrigin, normalized);
  const connection = await openConnectionWithRetry(
    request,
    connectionOptions,
    options.signal,
  );
  if (connection === undefined) {
    throw new TsfClientError(
      "invalid_api_response",
      "initial SSE read completed without stream metadata",
    );
  }
  normalized.streamMetadata = connection.stream;
  try {
    normalized.onStreamMetadata?.(connection.stream);
  } catch (error) {
    connection.close();
    throw error;
  }
  return new SseReadSession(
    normalized,
    request,
    connection,
    connectionOptions,
  );
}

class SseReadSession extends BaseTsfReadSession {
  #connection: SseConnection;
  #lastEventId: string | undefined;
  #noProgressReconnects = 0;

  public constructor(
    options: NormalizedReadOptions,
    private readonly request: SseRequest,
    connection: SseConnection,
    private readonly connectionOptions: SseConnectOptions,
  ) {
    super(options);
    this.#connection = connection;
    this.#lastEventId = connection.resumeEventId;
  }

  protected closeTransport(): void {
    this.#connection.close();
  }

  protected async pump(): Promise<ReadRecord | undefined> {
    while (!this.finished && !readExhausted(this.options)) {
      const record = this.nextPendingRecord();
      if (record !== undefined) {
        return record;
      }
      let result: IteratorResult<ParsedSseEvent>;
      let interrupted = false;
      try {
        result = await this.#connection.events.next();
      } catch (cause) {
        if (this.finished || this.controller.signal.aborted) {
          return undefined;
        }
        if (!isRetryableSseError(cause)) {
          throw cause;
        }
        interrupted = true;
        result = { done: true, value: undefined };
      }
      if (result.done) {
        if (readExhausted(this.options) ||
          (!interrupted && this.request.finite)) {
          this.finished = true;
          return undefined;
        }
        this.#connection.close();
        this.#noProgressReconnects += 1;
        if (
          this.#noProgressReconnects >=
            this.connectionOptions.boundedOperationAttempts
        ) {
          throw new TsfClientError(
            "read_reconnect_limit_exceeded",
            "SSE read repeatedly reconnected without receiving a record or caught-up position",
          );
        }
        const connection = await openConnectionWithRetry(
          this.request,
          this.connectionOptions,
          this.controller.signal,
          this.#lastEventId,
          true,
        );
        if (connection === undefined) {
          this.finished = true;
          return undefined;
        }
        this.#connection = connection;
        if (connection.resumeEventId !== undefined) {
          this.#lastEventId = connection.resumeEventId;
        }
        this.options.streamMetadata = this.#connection.stream;
        this.notify(this.options.onStreamMetadata, this.#connection.stream);
        continue;
      }
      const event = result.value;
      if (event.event === "read_batch") {
        const decoded = parseJsonEvent(event, sseReadBatchDataSchema);
        const cursor = resumeCursor(event);
        const records = decoded.records.map(readRecord);
        validateReadBatch(records, cursor, this.#lastEventId, this.options);
        this.#noProgressReconnects = 0;
        this.#lastEventId = cursor.value;
        this.pendingRecords = records;
        this.pendingRecordIndex = 0;
        continue;
      }
      if (event.event === "caught_up") {
        const decoded = parseJsonEvent(event, sseCaughtUpDataSchema);
        const caughtUp = {
          nextSeqNum: BigInt(decoded.next_seq_num),
          lastTimestampMs: BigInt(decoded.last_timestamp_ms),
        };
        const cursor = resumeCursor(event);
        validateCaughtUp(caughtUp, cursor, this.#lastEventId, this.options);
        this.#noProgressReconnects = 0;
        this.#lastEventId = cursor.value;
        this.options.lastCaughtUp = caughtUp;
        this.options.start = { type: "seqNum", seqNum: caughtUp.nextSeqNum };
        this.notify(this.options.onCaughtUp, caughtUp);
        continue;
      }
      if (event.event === "stream_metadata") {
        const stream = streamMetadataFromWire(parseJsonEvent(event, streamMetadataSchema));
        this.options.streamMetadata = stream;
        this.notify(this.options.onStreamMetadata, stream);
        continue;
      }
      if (event.event === "error") {
        throw terminalSseError(event);
      }
    }
    this.finished = true;
    this.#connection.close();
    return undefined;
  }
}

async function openConnection(
  request: SseRequest,
  connectionOptions: SseConnectOptions,
  signal?: AbortSignal,
  lastEventId?: string,
): Promise<SseConnection | undefined> {
  const controller = new AbortController();
  const abort = () => controller.abort(signal?.reason);
  signal?.addEventListener("abort", abort, { once: true });
  const timeoutMs = connectionOptions.httpRequestTimeoutMs;
  const timeoutError = new TsfClientError(
    "http_timeout",
    `SSE handshake timed out after ${timeoutMs}ms`,
  );
  try {
    return await withTimeout(
      openConnectionResponse(
        request,
        connectionOptions,
        controller,
        signal,
        abort,
        lastEventId,
      ),
      timeoutMs,
      "SSE handshake",
      undefined,
      {
        error: timeoutError,
        onTimeout: () => controller.abort(timeoutError),
      },
    );
  } catch (error) {
    signal?.removeEventListener("abort", abort);
    controller.abort();
    throw error;
  }
}

async function openConnectionResponse(
  request: SseRequest,
  connectionOptions: SseConnectOptions,
  controller: AbortController,
  signal: AbortSignal | undefined,
  abort: () => void,
  lastEventId: string | undefined,
): Promise<SseConnection | undefined> {
  const headers = new Headers({ accept: "text/event-stream" });
  if (request.linkSecret !== undefined) {
    headers.set("authorization", `Bearer ${request.linkSecret}`);
  }
  if (lastEventId !== undefined) {
    headers.set("last-event-id", lastEventId);
  }
  let response: Response;
  try {
    response = await connectionOptions.fetch(
      request.url,
      { headers, signal: controller.signal },
    );
  } catch (cause) {
    signal?.removeEventListener("abort", abort);
    throw new TsfClientError("http_transport", "SSE read request failed", { cause });
  }
  if (response.status === 204) {
    signal?.removeEventListener("abort", abort);
    controller.abort();
    return undefined;
  }
  if (!response.ok) {
    throw await httpStatusError(response, "SSE read");
  }
  if (response.body === null) {
    controller.abort();
    throw new TsfClientError("invalid_api_response", "SSE response has no body");
  }
  const events = parseSse(response.body, controller);
  const first = await events.next();
  if (first.done || first.value.event !== "stream_metadata") {
    controller.abort();
    throw new TsfClientError(
      "invalid_api_response",
      "SSE response must begin with stream_metadata",
    );
  }
  const stream = streamMetadataFromWire(parseJsonEvent(first.value, streamMetadataSchema));
  const resumeId = first.value.id === undefined
    ? undefined
    : resumeCursor(first.value).value;
  return {
    events,
    stream,
    ...(resumeId === undefined ? {} : { resumeEventId: resumeId }),
    close: () => {
      signal?.removeEventListener("abort", abort);
      controller.abort();
    },
  };
}

async function openConnectionWithRetry(
  request: SseRequest,
  connectionOptions: SseConnectOptions,
  signal?: AbortSignal,
  lastEventId?: string,
  delayBeforeFirst = false,
): Promise<SseConnection | undefined> {
  const maximumAttempts = connectionOptions.boundedOperationAttempts;
  let reconnectDelay = INITIAL_RETRY_BACKOFF_MS;
  let retryAfterMs: number | undefined;
  for (let attempt = 0; ; attempt += 1) {
    if (delayBeforeFirst || attempt > 0) {
      await sleep(
        retryAfterMs ?? jitteredBackoffMs(reconnectDelay),
        signal,
      );
      retryAfterMs = undefined;
      reconnectDelay = Math.min(MAX_RETRY_BACKOFF_MS, reconnectDelay * 2);
    }
    try {
      return await openConnection(request, connectionOptions, signal, lastEventId);
    } catch (error) {
      if (signal?.aborted === true || !isRetryableSseError(error)) {
        throw error;
      }
      if (error instanceof TsfHttpError && error.retryAfterMs !== undefined) {
        retryAfterMs = Math.min(error.retryAfterMs, MAX_RETRY_BACKOFF_MS);
      }
      if (attempt + 1 >= maximumAttempts) {
        throw error;
      }
    }
  }
}

function isRetryableSseError(error: unknown): boolean {
  if (error instanceof TsfHttpError) {
    return isRetryableHttpStatus(error.status);
  }
  return error instanceof TsfClientError &&
    (error.code === "http_transport" || error.code === "http_timeout");
}

function sseRequest(
  apiOrigin: string,
  options: NormalizedReadOptions,
): SseRequest {
  const url = new URL(
    `${API_PREFIX}/streams/${options.streamId}/records`,
    `${apiOrigin}/`,
  );
  url.search = encodeReadQuery(options).toString();
  return {
    url: url.toString(),
    ...(options.linkSecret === undefined ? {} : { linkSecret: options.linkSecret }),
    finite: options.stop !== undefined,
  };
}

async function* parseSse(
  body: ReadableStream<Uint8Array>,
  controller: AbortController,
): AsyncIterator<ParsedSseEvent> {
  const reader = body.getReader();
  const parser = new SseParser();
  try {
    for (;;) {
      let result: ReadableStreamReadResult<Uint8Array>;
      try {
        result = await reader.read();
      } catch (cause) {
        throw new TsfClientError("http_transport", "SSE response body failed", {
          cause,
        });
      }
      if (result.done) {
        parser.finish();
        break;
      }
      for (const event of parser.push(result.value)) {
        yield event;
      }
    }
  } finally {
    reader.releaseLock();
    controller.abort();
  }
}

class SseParser {
  readonly #decoder = new TextDecoder("utf-8", { fatal: true });
  readonly #validator = new TextDecoder("utf-8", { fatal: true });
  #buffer = new Uint8Array(0);
  #byteLength = 0;

  public push(chunk: Uint8Array): ParsedSseEvent[] {
    this.#validateUtf8(chunk);
    const events: ParsedSseEvent[] = [];
    let segmentStart = 0;
    for (
      let lineFeed = chunk.indexOf(LINE_FEED);
      lineFeed !== -1;
      lineFeed = chunk.indexOf(LINE_FEED, lineFeed + 1)
    ) {
      const boundaryLength = this.#boundaryLength(chunk, segmentStart, lineFeed);
      if (boundaryLength === undefined) {
        continue;
      }
      this.#append(
        chunk.subarray(segmentStart, lineFeed + 1),
        MAX_SSE_EVENT_BYTES,
        "SSE event is too large",
      );
      const event = parseEventBlock(this.#decodeBlock(boundaryLength));
      segmentStart = lineFeed + 1;
      if (event !== undefined) {
        events.push(event);
      }
    }
    this.#append(
      chunk.subarray(segmentStart),
      MAX_SSE_UNTERMINATED_EVENT_BYTES,
      "unterminated SSE event is too large",
    );
    return events;
  }

  public finish(): void {
    this.#finishUtf8Validation();
    if (this.#byteLength > MAX_SSE_UNTERMINATED_EVENT_BYTES) {
      throw new TsfClientError("invalid_api_response", "unterminated SSE event is too large");
    }
  }

  #append(bytes: Uint8Array, maximumBytes: number, errorMessage: string): void {
    if (bytes.byteLength === 0) {
      return;
    }
    if (bytes.byteLength > maximumBytes - this.#byteLength) {
      throw new TsfClientError("invalid_api_response", errorMessage);
    }
    const required = this.#byteLength + bytes.byteLength;
    if (required > this.#buffer.byteLength) {
      const capacity = Math.min(
        maximumBytes,
        Math.max(required, this.#buffer.byteLength * 2, 4096),
      );
      const expanded = new Uint8Array(capacity);
      expanded.set(this.#buffer.subarray(0, this.#byteLength));
      this.#buffer = expanded;
    }
    this.#buffer.set(bytes, this.#byteLength);
    this.#byteLength = required;
  }

  #boundaryLength(
    chunk: Uint8Array,
    segmentStart: number,
    lineFeed: number,
  ): 2 | 4 | undefined {
    const before = (distance: number) => {
      const localIndex = lineFeed - distance;
      if (localIndex >= segmentStart) {
        return chunk[localIndex];
      }
      return this.#buffer[this.#byteLength + localIndex - segmentStart];
    };
    if (
      before(3) === CARRIAGE_RETURN &&
      before(2) === LINE_FEED &&
      before(1) === CARRIAGE_RETURN
    ) {
      return 4;
    }
    return before(1) === LINE_FEED ? 2 : undefined;
  }

  #takeBlock(boundaryLength: number): Uint8Array {
    const block = this.#buffer.subarray(0, this.#byteLength - boundaryLength);
    this.#byteLength = 0;
    return block;
  }

  #decodeBlock(boundaryLength: number): string {
    const decoded = this.#decode(this.#takeBlock(boundaryLength));
    if (this.#buffer.byteLength > MAX_RETAINED_SSE_BUFFER_BYTES) {
      this.#buffer = new Uint8Array(0);
    }
    return decoded;
  }

  #decode(bytes: Uint8Array): string {
    try {
      return this.#decoder.decode(bytes);
    } catch (cause) {
      throw new TsfClientError(
        "invalid_api_response",
        "SSE response is not valid UTF-8",
        { cause },
      );
    }
  }

  #validateUtf8(bytes: Uint8Array): void {
    try {
      this.#validator.decode(bytes, { stream: true });
    } catch (cause) {
      throw new TsfClientError(
        "invalid_api_response",
        "SSE response is not valid UTF-8",
        { cause },
      );
    }
  }

  #finishUtf8Validation(): void {
    try {
      this.#validator.decode();
    } catch (cause) {
      throw new TsfClientError(
        "invalid_api_response",
        "SSE response is not valid UTF-8",
        { cause },
      );
    }
  }
}

function parseEventBlock(block: string): ParsedSseEvent | undefined {
  let event = "message";
  let id: string | undefined;
  const data: string[] = [];
  for (const line of block.split(/\r?\n/)) {
    if (line === "" || line.startsWith(":")) {
      continue;
    }
    const colon = line.indexOf(":");
    const name = colon === -1 ? line : line.slice(0, colon);
    const value = colon === -1
      ? ""
      : line.slice(colon + 1).replace(/^ /, "");
    if (name === "event") {
      event = value;
    } else if (name === "id") {
      id = value;
    } else if (name === "data") {
      data.push(value);
    }
  }
  return data.length === 0 ? undefined : {
    event,
    data: data.join("\n"),
    ...(id === undefined ? {} : { id }),
  };
}

function parseJsonEvent<T>(
  event: ParsedSseEvent,
  schema: { parse(input: unknown): T },
): T {
  try {
    return schema.parse(JSON.parse(event.data));
  } catch (cause) {
    throw new TsfClientError(
      "invalid_api_response",
      `invalid ${event.event} SSE event`,
      { cause },
    );
  }
}

function resumeCursor(event: ParsedSseEvent): ParsedSseResumeCursor {
  const id = event.id;
  if (id === undefined) {
    throw invalidResumeEventId(event.event);
  }
  return parseResumeCursor(id, event.event);
}

function parseResumeCursor(id: string, event: string): ParsedSseResumeCursor {
  try {
    return { value: id, ...decodeSseResumeCursor(id) };
  } catch {
    throw invalidResumeEventId(event);
  }
}

function validateReadBatch(
  records: readonly ReadRecord[],
  cursor: ParsedSseResumeCursor,
  previousId: string | undefined,
  options: NormalizedReadOptions,
): void {
  let payloadBytes = 0;
  let previousSeqNum: bigint | undefined;
  for (const record of records) {
    if (record.data.byteLength > MAX_RECORD_PAYLOAD_BYTES) {
      throw invalidSseContract("read_batch contains an oversized record");
    }
    payloadBytes += record.data.byteLength;
    if (payloadBytes > MAX_SSE_READ_BATCH_PAYLOAD_BYTES) {
      throw invalidSseContract("read_batch exceeds the decoded payload limit");
    }
    if (previousSeqNum !== undefined && record.seqNum !== previousSeqNum + 1n) {
      throw invalidSseContract("read_batch sequence numbers are not contiguous");
    }
    if (options.stop?.untilTimestampMs !== undefined &&
      record.timestampMs >= options.stop.untilTimestampMs) {
      throw invalidSseContract("read_batch reaches the exclusive until timestamp");
    }
    previousSeqNum = record.seqNum;
  }
  if (options.stop?.count !== undefined &&
    BigInt(records.length) > options.stop.count) {
    throw invalidSseContract("read_batch exceeds the remaining record count");
  }
  const first = records[0];
  const last = records.at(-1);
  if (first === undefined || last === undefined || cursor.nextSeqNum !== last.seqNum + 1n) {
    throw invalidSseContract("read_batch cursor does not follow its records");
  }
  const previous = previousId === undefined
    ? undefined
    : parseResumeCursor(previousId, "previous");
  if (previous !== undefined && first.seqNum !== previous.nextSeqNum) {
    throw invalidSseContract("read_batch does not resume at the previous cursor");
  }
  if (
    previous === undefined &&
    options.start.type === "seqNum" &&
    first.seqNum !== options.start.seqNum
  ) {
    throw invalidSseContract("read_batch does not begin at the requested sequence");
  }
  if (
    previous === undefined &&
    options.start.type === "timestampMs" &&
    first.timestampMs < options.start.timestampMs
  ) {
    throw invalidSseContract("read_batch begins before the requested timestamp");
  }
  const consumedBefore = previous?.consumedRecords ?? 0n;
  if (cursor.consumedRecords !== consumedBefore + BigInt(records.length)) {
    throw invalidSseContract("read_batch cursor has the wrong consumed count");
  }
}

function validateCaughtUp(
  caughtUp: CaughtUpPosition,
  cursor: ParsedSseResumeCursor,
  previousId: string | undefined,
  options: NormalizedReadOptions,
): void {
  if (cursor.nextSeqNum !== caughtUp.nextSeqNum) {
    throw invalidSseContract("caught_up cursor does not match its position");
  }
  const previous = previousId === undefined
    ? undefined
    : parseResumeCursor(previousId, "previous");
  if (
    previous !== undefined &&
    (cursor.nextSeqNum !== previous.nextSeqNum ||
      cursor.consumedRecords !== previous.consumedRecords)
  ) {
    throw invalidSseContract("caught_up does not continue the previous cursor");
  }
  if (previous === undefined && cursor.consumedRecords !== 0n) {
    throw invalidSseContract("initial caught_up cursor has a consumed count");
  }
  if (
    previous === undefined &&
    options.start.type === "seqNum" &&
    cursor.nextSeqNum !== options.start.seqNum
  ) {
    throw invalidSseContract("initial caught_up does not match the requested sequence");
  }
}

function invalidSseContract(message: string): TsfClientError {
  return new TsfClientError("invalid_api_response", message);
}

function invalidResumeEventId(event: string): TsfClientError {
  return new TsfClientError(
    "invalid_api_response",
    `${event} SSE event has no valid resume cursor`,
  );
}

function readRecord(record: ReturnType<typeof sseReadBatchDataSchema.parse>["records"][number]): ReadRecord {
  const format = resolvedRecordFormat(record) === "transcript"
    ? RecordFormat.Transcript
    : RecordFormat.Bytes;
  return {
    seqNum: BigInt(record.seq_num),
    timestampMs: BigInt(record.timestamp_ms),
    writerId: parseWriterId(decodeBase64url(record.writer.id)),
    writerSeqNum: BigInt(record.writer.seq_num),
    // An omitted part header is an unsplit record.
    part: record.part === undefined
      ? { index: 0, isFinal: true }
      : { index: record.part.index, isFinal: record.part.is_final },
    format,
    data: recordPayloadBytes(record),
  };
}

function terminalSseError(event: ParsedSseEvent): TsfClientError {
  try {
    const body: unknown = JSON.parse(event.data);
    if (typeof body === "object" && body !== null && "error" in body &&
      typeof body.error === "object" && body.error !== null &&
      "code" in body.error && typeof body.error.code === "string" &&
      "message" in body.error && typeof body.error.message === "string") {
      return new TsfClientError(body.error.code, body.error.message);
    }
  } catch {
    // Fall through to the stable invalid-response error.
  }
  return new TsfClientError("invalid_api_response", "invalid terminal SSE event");
}
