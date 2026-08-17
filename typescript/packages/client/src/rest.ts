import {
  createStreamRequestSchema,
  createStreamResponseSchema,
  createLinkRequestSchema,
  streamLinkCredentialSchema,
  listLinksResponseSchema,
  parseStreamId,
  parseLinkId,
  parseStreamTitle,
  streamMetadataSchema,
  updateStreamRequestSchema,
  appendRecordsRequestSchema,
  appendRangeSchema,
  compactRecordData,
  encodeBase64url,
  isCanonicalIdempotencyKey,
  IDEMPOTENCY_KEY_BYTES,
  MAX_SAFE_INTEGER_U64,
  MAX_LINK_PAGE_ITEMS,
  MAX_RECORD_BYTES,
  MAX_REST_RESPONSE_BYTES,
  MAX_STATELESS_APPEND_JSON_BYTES,
  MAX_STATELESS_APPEND_PAYLOAD_BYTES,
  MAX_STATELESS_APPEND_RECORDS,
  parseLinkSecret,
  parseClientWriterId,
  partHeader,
  randomBytes,
  RecordFormat,
  type PartHeader,
  type ClientWriterId,
  type CreateStreamRequest as WireCreateStreamRequest,
  type CreateLinkRequestInput as WireCreateLinkRequestInput,
  type StreamId,
  type LinkId,
  type LinkPermissions,
  type Visibility,
} from "@tailsurf/protocol";

import {
  httpStatusError,
  isRecord,
  readJsonResponse,
  TsfClientError,
  TsfHttpError,
} from "./errors.js";
import {
  createStreamResponseFromWire,
  streamLinkCredentialFromWire,
  listLinksResponseFromWire,
  streamMetadataFromWire,
  type CreateStreamResponse,
  type StreamLinkCredential,
  type ListLinksResponse,
  type StreamMetadata,
} from "./models.js";
import { connectSseReader as openSseReader } from "./sse.js";
import type { ReadOptions, TsfReadSession } from "./reader.js";
import {
  integerOption,
  isRetryableHttpStatus,
  jitteredBackoffMs,
  MAX_TIMER_DELAY_MS,
} from "./retry.js";
import { sleep, withTimeout } from "./socket.js";

export const DEFAULT_API_ORIGIN = "https://tail.surf";
const API_PREFIX = "/api/v1";
const DEFAULT_REQUEST_TIMEOUT_MS = 10_000;
const textEncoder = new TextEncoder();

interface Schema<T> {
  parse(input: unknown): T;
}

export interface RestClientOptions {
  readonly apiOrigin?: string | URL;
  readonly fetch?: typeof globalThis.fetch;
  /** Bounds REST requests and SSE opening handshakes. It does not time out an established SSE body. */
  readonly restRequestTimeoutMs?: number;
  /** Retry policy shared by REST requests and SSE or WebSocket connections. */
  readonly retryPolicy?: RetryPolicy;
}

export interface RetryPolicy {
  /** Total attempts including the initial attempt. */
  readonly maxAttempts?: number;
  /** Base delay before the first retry. Client-controlled delays are jittered. */
  readonly initialBackoffMs?: number;
  /** Maximum base delay and server retry hint honored by the client. */
  readonly maxBackoffMs?: number;
}

interface NormalizedRetryPolicy {
  readonly maxAttempts: number;
  readonly initialBackoffMs: number;
  readonly maxBackoffMs: number;
}

export interface IdempotencyOptions {
  /** Sensitive recovery key retained across every attempt of one logical mutation. */
  readonly idempotencyKey?: string;
}

export interface InitialStreamLinkOptions {
  readonly linkId: string;
  readonly permissions: LinkPermissions;
}

export interface CreateStreamInput {
  readonly title?: string;
  readonly visibility?: Visibility;
  readonly expiresInSeconds?: number;
  readonly links?: readonly InitialStreamLinkOptions[];
}

export interface PreparedCreateStreamRequest {
  readonly title?: string;
  readonly visibility: Visibility;
  readonly expiresInSeconds?: number;
  readonly links: readonly InitialStreamLinkOptions[];
}

/** Authorization for metadata reads. Public streams may omit the link secret. */
export interface ReadAuthOptions {
  readonly linkSecret?: string;
}

/** Owner authorization for one stream-management request. */
export interface OwnerAuthOptions {
  readonly linkSecret: string;
}

/** Write authorization for one stateless append request. */
export interface WriteAuthOptions {
  readonly linkSecret: string;
}

export interface ListLinksOptions extends OwnerAuthOptions {
  readonly limit?: number;
  readonly cursor?: string;
}

export interface CreateLinkInput {
  readonly linkId: string;
  readonly permissions: LinkPermissions;
  readonly expiresAt?: string;
}

export interface CreateLinkOptions extends OwnerAuthOptions, IdempotencyOptions {}

export interface UpdateStreamInput {
  readonly title?: string | null;
  readonly visibility?: Visibility;
  readonly expiresAt?: string;
}

export interface AppendRange {
  readonly startSeqNum: bigint;
  readonly endSeqNum: bigint;
}

export interface StatelessAppendRecord {
  readonly part?: PartHeader;
  readonly format: RecordFormat;
  readonly data: Uint8Array | string;
}

export interface StatelessAppendRequest {
  readonly clientWriterId: ClientWriterId;
  readonly writerStartSeqNum: bigint;
  readonly records: readonly StatelessAppendRecord[];
  readonly expectedNextSeqNum?: bigint;
}

export class BaseTsfClient {
  public readonly apiOrigin: string;
  protected readonly retryPolicy: NormalizedRetryPolicy;
  readonly #fetch: typeof globalThis.fetch;
  readonly #restRequestTimeoutMs: number;

  public constructor(options: RestClientOptions = {}) {
    this.apiOrigin = parseApiOrigin(options.apiOrigin ?? DEFAULT_API_ORIGIN);
    const fetchImplementation = options.fetch ?? globalThis.fetch?.bind(globalThis);
    if (fetchImplementation === undefined) {
      throw new TsfClientError(
        "fetch_unavailable",
        "fetch is unavailable; provide a fetch implementation",
      );
    }
    this.#fetch = fetchImplementation;
    this.#restRequestTimeoutMs = integerOption(
      options.restRequestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS,
      "restRequestTimeoutMs",
      1,
      MAX_TIMER_DELAY_MS,
    );
    const initialBackoffMs = integerOption(
      options.retryPolicy?.initialBackoffMs ?? 200,
      "retryPolicy.initialBackoffMs",
      0,
      MAX_TIMER_DELAY_MS,
    );
    const maxBackoffMs = integerOption(
      options.retryPolicy?.maxBackoffMs ?? 2_000,
      "retryPolicy.maxBackoffMs",
      0,
      MAX_TIMER_DELAY_MS,
    );
    if (initialBackoffMs > maxBackoffMs) {
      throw new TsfClientError(
        "invalid_client_option",
        "retryPolicy.initialBackoffMs must not exceed retryPolicy.maxBackoffMs",
      );
    }
    this.retryPolicy = {
      maxAttempts: integerOption(
        options.retryPolicy?.maxAttempts ?? 3,
        "retryPolicy.maxAttempts",
        1,
      ),
      initialBackoffMs,
      maxBackoffMs,
    };
  }

  public connectSseReader(options: ReadOptions): Promise<TsfReadSession> {
    return openSseReader(options, {
      fetch: this.#fetch,
      apiOrigin: this.apiOrigin,
      restRequestTimeoutMs: this.#restRequestTimeoutMs,
      retryPolicy: this.retryPolicy,
    });
  }

  public createStream(
    request: CreateStreamInput = {},
    options: IdempotencyOptions = {},
  ): Promise<CreateStreamResponse> {
    const normalized = prepareCreateStreamRequest(request);
    const body = JSON.stringify(
      createStreamRequestSchema.parse(createStreamRequestToWire(normalized)),
    );
    const idempotencyKey = parseIdempotencyKey(
      options.idempotencyKey ?? generateIdempotencyKey(),
    );
    return this.#json(
      "create stream",
      "/streams",
      createStreamResponseSchema,
      {
        method: "POST",
        headers: { "idempotency-key": idempotencyKey },
        body,
      },
    ).then(createStreamResponseFromWire);
  }

  public getStream(
    streamId: StreamId,
    options: ReadAuthOptions = {},
  ): Promise<StreamMetadata> {
    return this.#json(
      "get stream",
      `/streams/${parseStreamId(streamId)}`,
      streamMetadataSchema,
      undefined,
      optionalLinkSecret(options.linkSecret),
    ).then(streamMetadataFromWire);
  }

  public updateStream(
    streamId: StreamId,
    request: UpdateStreamInput,
    options: OwnerAuthOptions,
  ): Promise<StreamMetadata> {
    return this.#json(
      "update stream",
      `/streams/${parseStreamId(streamId)}`,
      streamMetadataSchema,
      {
        method: "PATCH",
        body: JSON.stringify(updateStreamRequestSchema.parse({
          ...(request.title === undefined ? {} : { title: request.title }),
          ...(request.visibility === undefined ? {} : { visibility: request.visibility }),
          ...(request.expiresAt === undefined ? {} : { expires_at: request.expiresAt }),
        })),
      },
      requiredLinkSecret(options, "update stream", "owner"),
    ).then(streamMetadataFromWire);
  }

  public deleteStream(
    streamId: StreamId,
    options: OwnerAuthOptions,
  ): Promise<void> {
    return this.#request(
      "delete stream",
      `/streams/${parseStreamId(streamId)}`,
      async (response) => {
        if (response.status !== 204) {
          throw await httpStatusError(response, "delete stream");
        }
      },
      { method: "DELETE" },
      requiredLinkSecret(options, "delete stream", "owner"),
    );
  }

  public createLink(
    streamId: StreamId,
    request: CreateLinkInput,
    options: CreateLinkOptions,
  ): Promise<StreamLinkCredential> {
    const linkId = parseLinkId(request.linkId);
    const idempotencyKey = parseIdempotencyKey(
      options.idempotencyKey ?? generateIdempotencyKey(),
    );
    const createRequest: WireCreateLinkRequestInput = {
      permissions: request.permissions,
      ...(request.expiresAt === undefined ? {} : { expires_at: request.expiresAt }),
    };
    return this.#json(
      "create link",
      `/streams/${parseStreamId(streamId)}/links/${linkId}`,
      streamLinkCredentialSchema,
      {
        method: "PUT",
        headers: { "idempotency-key": idempotencyKey },
        body: JSON.stringify(createLinkRequestSchema.parse(createRequest)),
      },
      requiredLinkSecret(options, "create link", "owner"),
    ).then(streamLinkCredentialFromWire);
  }

  public listLinks(
    streamId: StreamId,
    options: ListLinksOptions,
  ): Promise<ListLinksResponse> {
    const maximumLinks = options.limit ?? MAX_LINK_PAGE_ITEMS;
    return this.#json(
      "list links",
      linkListPath(parseStreamId(streamId), options),
      listLinksResponseSchema,
      undefined,
      requiredLinkSecret(options, "list links", "owner"),
    ).then(listLinksResponseFromWire).then((page) => {
      if (page.links.length > maximumLinks) {
        throw new TsfClientError(
          "invalid_api_response",
          "link page contains more entries than requested",
        );
      }
      if (page.nextCursor !== null && page.links.length === 0) {
        throw new TsfClientError(
          "invalid_api_response",
          "empty link page must not carry a next cursor",
        );
      }
      const linkIds = new Set(page.links.map((link) => link.linkId));
      if (linkIds.size !== page.links.length) {
        throw new TsfClientError(
          "invalid_api_response",
          "link page contains duplicate link IDs",
        );
      }
      return page;
    });
  }

  public revokeLink(
    streamId: StreamId,
    linkId: LinkId,
    options: OwnerAuthOptions,
  ): Promise<void> {
    return this.#request(
      "revoke link",
      `/streams/${parseStreamId(streamId)}/links/${parseLinkId(linkId)}`,
      async (response) => {
        if (response.status !== 204) {
          throw await httpStatusError(response, "revoke link");
        }
      },
      { method: "DELETE" },
      requiredLinkSecret(options, "revoke link", "owner"),
    );
  }

  public appendRecords(
    streamId: StreamId,
    request: StatelessAppendRequest,
    options: WriteAuthOptions,
  ): Promise<AppendRange> {
    if (request.records.length === 0 || request.records.length > MAX_STATELESS_APPEND_RECORDS) {
      throw new TsfClientError(
        "invalid_client_option",
        `append batch must contain between 1 and ${MAX_STATELESS_APPEND_RECORDS} records`,
      );
    }
    let payloadBytes = 0;
    const records = request.records.map((record) => {
      const part = record.part === undefined
        ? undefined
        : partHeader(record.part.index, record.part.isFinal);
      const bytes = typeof record.data === "string"
        ? textEncoder.encode(record.data)
        : record.data;
      validateStatelessRecordBytes(bytes.byteLength);
      payloadBytes += bytes.byteLength;
      return {
        format: record.format === RecordFormat.Transcript
          ? "transcript" as const
          : "bytes" as const,
        data: compactRecordData(bytes),
        ...(part === undefined
          ? {}
          : { part: { index: part.index, is_final: part.isFinal } }),
      };
    });
    if (payloadBytes > MAX_STATELESS_APPEND_PAYLOAD_BYTES) {
      throw new TsfClientError(
        "invalid_client_option",
        `append payload must not exceed ${MAX_STATELESS_APPEND_PAYLOAD_BYTES} bytes`,
      );
    }
    const finalWriterSeqNum = request.writerStartSeqNum + BigInt(request.records.length - 1);
    if (request.writerStartSeqNum < 0n || finalWriterSeqNum >= 0xffff_ffff_ffff_ffffn) {
      throw new TsfClientError(
        "invalid_client_option",
        "writer sequence range must end before u64::MAX",
      );
    }
    if (
      request.expectedNextSeqNum !== undefined &&
      (request.expectedNextSeqNum < 0n ||
        request.expectedNextSeqNum > MAX_SAFE_INTEGER_U64)
    ) {
      throw new TsfClientError(
        "invalid_client_option",
        `expectedNextSeqNum must be between 0 and ${MAX_SAFE_INTEGER_U64}`,
      );
    }
    const body = appendRecordsRequestSchema.parse({
      client_writer_id: encodeBase64url(
        parseClientWriterId(request.clientWriterId),
      ),
      writer_start_seq_num: request.writerStartSeqNum.toString(),
      records,
      ...(request.expectedNextSeqNum === undefined
        ? {}
        : { expected_next_seq_num: request.expectedNextSeqNum.toString() }),
    });
    const encodedBody = JSON.stringify(body);
    if (textEncoder.encode(encodedBody).byteLength > MAX_STATELESS_APPEND_JSON_BYTES) {
      throw new TsfClientError(
        "invalid_client_option",
        `encoded append body must not exceed ${MAX_STATELESS_APPEND_JSON_BYTES} bytes`,
      );
    }
    return this.#json(
      "append records",
      `/streams/${parseStreamId(streamId)}/records`,
      appendRangeSchema,
      { method: "POST", body: encodedBody },
      requiredLinkSecret(options, "append records", "write-capable"),
    ).then((range) => {
      const result = {
        startSeqNum: BigInt(range.start_seq_num),
        endSeqNum: BigInt(range.end_seq_num),
      };
      if (
        result.endSeqNum - result.startSeqNum !== BigInt(request.records.length)
      ) {
        throw new TsfClientError(
          "invalid_api_response",
          "append response range does not match the submitted record count",
        );
      }
      return result;
    });
  }

  async #json<T>(
    operation: string,
    path: string,
    schema: Schema<T>,
    init?: RequestInit,
    linkSecret?: string,
  ): Promise<T> {
    return this.#request(
      operation,
      path,
      async (response) => {
        if (!response.ok) {
          throw await httpStatusError(response, operation);
        }
        let body: unknown;
        try {
          body = await readJsonResponse(
            response,
            MAX_REST_RESPONSE_BYTES,
            operation,
          );
        } catch (cause) {
          if (
            cause instanceof TsfClientError &&
            cause.code === "response_too_large"
          ) {
            throw cause;
          }
          throw new TsfClientError(
            "invalid_json_response",
            `${operation} returned invalid JSON`,
            { cause },
          );
        }
        try {
          return schema.parse(body);
        } catch (cause) {
          throw new TsfClientError(
            "invalid_api_response",
            `${operation} returned a response that does not match the TSF API`,
            { cause },
          );
        }
      },
      init,
      linkSecret,
    );
  }

  async #request<T>(
    operation: string,
    path: string,
    consume: (response: Response) => Promise<T>,
    init: RequestInit = {},
    linkSecret?: string,
  ): Promise<T> {
    return retryRest(
      () => this.#requestOnce(operation, path, consume, init, linkSecret),
      this.retryPolicy,
    );
  }

  async #requestOnce<T>(
    operation: string,
    path: string,
    consume: (response: Response) => Promise<T>,
    init: RequestInit = {},
    linkSecret?: string,
  ): Promise<T> {
    const headers = new Headers(init.headers);
    if (init.body !== undefined && !headers.has("content-type")) {
      headers.set("content-type", "application/json");
    }
    if (linkSecret !== undefined) {
      headers.set("authorization", `Bearer ${linkSecret}`);
    }
    const controller = new AbortController();
    const timeoutError = new TsfClientError(
      "http_timeout",
      `${operation} timed out after ${this.#restRequestTimeoutMs}ms`,
    );
    let timedOut = false;
    const request = (async () => {
      let receivedResponse = false;
      try {
        const response = await this.#fetch(
          `${this.apiOrigin}${API_PREFIX}${path}`,
          {
            ...init,
            headers,
            signal: controller.signal,
          },
        );
        receivedResponse = true;
        return await consume(response);
      } catch (cause) {
        if (timedOut) {
          throw timeoutError;
        }
        if (receivedResponse) {
          throw cause;
        }
        throw new TsfClientError(
          "http_transport",
          `${operation} request failed`,
          { cause },
        );
      }
    })();
    return withTimeout(request, this.#restRequestTimeoutMs, operation, undefined, {
      error: timeoutError,
      onTimeout: () => {
        timedOut = true;
        controller.abort();
      },
    });
  }
}

function optionalLinkSecret(value: string | undefined): string | undefined {
  if (value === undefined) {
    return undefined;
  }
  try {
    return parseLinkSecret(value);
  } catch (cause) {
    throw new TsfClientError(
      "invalid_client_option",
      "linkSecret must be a canonical 24-byte unpadded base64url secret",
      { cause },
    );
  }
}

function validateStatelessRecordBytes(bytes: number): void {
  if (bytes > MAX_RECORD_BYTES) {
    throw new TsfClientError(
      "invalid_client_option",
      `each append record must not exceed ${MAX_RECORD_BYTES} bytes`,
    );
  }
}

function requiredLinkSecret(
  options: OwnerAuthOptions | WriteAuthOptions | undefined,
  operation: string,
  permission: "owner" | "write-capable",
): string {
  const secret = optionalLinkSecret(options?.linkSecret);
  if (secret === undefined) {
    throw new TsfClientError(
      "invalid_client_option",
      `${operation} requires an explicit ${permission} linkSecret`,
    );
  }
  return secret;
}

export function parseApiOrigin(input: string | URL): string {
  let url: URL;
  try {
    url = new URL(input);
  } catch (cause) {
    throw new TsfClientError("invalid_api_origin", "API origin is not a valid URL", {
      cause,
    });
  }
  if (
    (url.protocol !== "http:" && url.protocol !== "https:") ||
    url.username !== "" ||
    url.password !== "" ||
    url.pathname !== "/" ||
    url.search !== "" ||
    url.hash !== ""
  ) {
    throw new TsfClientError(
      "invalid_api_origin",
      "API origin must be an HTTP origin without credentials, path, query, or fragment",
    );
  }
  return url.origin;
}

async function retryRest<T>(
  attempt: () => Promise<T>,
  policy: NormalizedRetryPolicy,
): Promise<T> {
  let retryDelayMs = policy.initialBackoffMs;
  for (let attemptIndex = 0; attemptIndex < policy.maxAttempts; attemptIndex += 1) {
    try {
      return await attempt();
    } catch (error) {
      if (attemptIndex + 1 === policy.maxAttempts || !isRetryableRestError(error)) {
        throw error;
      }
      const delayMs = error instanceof TsfHttpError && error.retryAfterMs !== undefined
        ? Math.min(error.retryAfterMs, policy.maxBackoffMs)
        : jitteredBackoffMs(retryDelayMs);
      await sleep(delayMs);
      retryDelayMs = Math.min(policy.maxBackoffMs, Math.max(1, retryDelayMs * 2));
    }
  }
  throw new Error("REST retry loop exhausted without returning");
}

function isRetryableRestError(error: unknown): boolean {
  if (error instanceof TsfHttpError) {
    return isRetryableHttpStatus(error.status);
  }
  return error instanceof TsfClientError &&
    (
      error.code === "http_timeout" ||
      error.code === "http_transport" ||
      error.code === "invalid_json_response" ||
      error.code === "invalid_api_response"
    );
}

export function generateIdempotencyKey(): string {
  return encodeBase64url(randomBytes(IDEMPOTENCY_KEY_BYTES));
}

export function prepareCreateStreamRequest(
  request: CreateStreamInput = {},
): PreparedCreateStreamRequest {
  const requestedLinks = request.links ?? [];
  const links = requestedLinks.some((link) => link.permissions === "o")
    ? requestedLinks
    : [{ linkId: parseLinkId("owner"), permissions: "o" as const }, ...requestedLinks];
  return parsePreparedCreateStreamRequest({
    ...(request.title === undefined ? {} : { title: request.title }),
    visibility: request.visibility ?? "private",
    ...(request.expiresInSeconds === undefined
      ? {}
      : { expiresInSeconds: request.expiresInSeconds }),
    links,
  });
}

export function parsePreparedCreateStreamRequest(
  input: unknown,
): PreparedCreateStreamRequest {
  if (!isRecord(input) || !Array.isArray(input.links)) {
    throw new TsfClientError(
      "invalid_client_option",
      "normalized stream request is invalid",
    );
  }
  const wire = createStreamRequestSchema.parse({
    ...(input.title === undefined ? {} : { title: input.title }),
    ...(input.visibility === undefined ? {} : { visibility: input.visibility }),
    ...(input.expiresInSeconds === undefined
      ? {}
      : { expires_in_seconds: input.expiresInSeconds }),
    links: input.links.map((link) => {
      if (!isRecord(link)) {
        throw new TsfClientError(
          "invalid_client_option",
          "normalized stream link is invalid",
        );
      }
      return {
        link_id: link.linkId,
        permissions: link.permissions,
      };
    }),
  });
  return {
    ...(wire.title === undefined ? {} : { title: wire.title }),
    visibility: wire.visibility,
    ...(wire.expires_in_seconds === undefined
      ? {}
      : { expiresInSeconds: wire.expires_in_seconds }),
    links: wire.links.map((link) => ({
      linkId: link.link_id,
      permissions: link.permissions,
    })),
  };
}

function createStreamRequestToWire(
  request: PreparedCreateStreamRequest,
): WireCreateStreamRequest {
  return {
    ...(request.title === undefined
      ? {}
      : { title: parseStreamTitle(request.title) }),
    visibility: request.visibility,
    ...(request.expiresInSeconds === undefined
      ? {}
      : { expires_in_seconds: request.expiresInSeconds }),
    links: request.links.map((link) => ({
      link_id: parseLinkId(link.linkId),
      permissions: link.permissions,
    })),
  };
}

function linkListPath(streamId: StreamId, options: ListLinksOptions): string {
  const query = new URLSearchParams();
  if (options.limit !== undefined) {
    if (
      !Number.isSafeInteger(options.limit) ||
      options.limit < 1 ||
      options.limit > MAX_LINK_PAGE_ITEMS
    ) {
      throw new TsfClientError(
        "invalid_client_option",
        `link list limit must be an integer between 1 and ${MAX_LINK_PAGE_ITEMS}`,
      );
    }
    query.set("limit", String(options.limit));
  }
  if (options.cursor !== undefined) {
    if (options.cursor.length === 0) {
      throw new TsfClientError("invalid_client_option", "link list cursor must not be empty");
    }
    query.set("cursor", options.cursor);
  }
  const encoded = query.toString();
  return `/streams/${streamId}/links${encoded === "" ? "" : `?${encoded}`}`;
}

export function parseIdempotencyKey(input: string): string {
  if (!isCanonicalIdempotencyKey(input)) {
    throw new TsfClientError(
      "invalid_idempotency_key",
      "idempotency key must be 32 random bytes encoded as unpadded base64url",
    );
  }
  return input;
}
