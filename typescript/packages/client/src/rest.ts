import {
  createStreamRequestSchema,
  createStreamResponseSchema,
  createLinkRequestSchema,
  createLinkResponseSchema,
  listLinksResponseSchema,
  parseStreamId,
  parseLinkId,
  streamMetadataSchema,
  updateStreamRequestSchema,
  appendRecordsRequestSchema,
  appendRangeSchema,
  compactRecordPayload,
  encodeBase64url,
  isCanonicalIdempotencyKey,
  IDEMPOTENCY_KEY_BYTES,
  MAX_SAFE_INTEGER_U64,
  MAX_LINK_PAGE_ITEMS,
  MAX_RECORD_PAYLOAD_BYTES,
  MAX_REST_RESPONSE_BYTES,
  MAX_STATELESS_APPEND_JSON_BYTES,
  MAX_STATELESS_APPEND_PAYLOAD_BYTES,
  MAX_STATELESS_APPEND_RECORDS,
  MAX_U64,
  parseLinkSecret,
  parseClientWriterId,
  partHeader,
  randomBytes,
  type PartHeader,
  type ClientWriterId,
  type CreateStreamRequest as WireCreateStreamRequest,
  type CreateLinkRequestInput as WireCreateLinkRequestInput,
  type StreamId,
  type LinkId,
  type LinkPermissions,
  type Visibility,
  type StreamKind,
  type StreamTitle,
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
  createLinkResponseFromWire,
  listLinksResponseFromWire,
  streamMetadataFromWire,
  type CreateStreamResponse,
  type CreateLinkResponse,
  type ListLinksResponse,
  type StreamMetadata,
} from "./models.js";
import { connectSseReader as openSseReader } from "./sse.js";
import type { ReadOptions, TsfReadSession } from "./reader.js";
import {
  integerOption,
  isRetryableHttpStatus,
  MAX_TIMER_DELAY_MS,
  retryOperation,
  withTimeout,
} from "./retry.js";

export const DEFAULT_API_ORIGIN = "https://tail.surf";
const API_PREFIX = "/api/v1";
const DEFAULT_HTTP_REQUEST_TIMEOUT_MS = 10_000;
const textEncoder = new TextEncoder();

interface Schema<T> {
  parse(input: unknown): T;
}

export interface HttpClientOptions {
  readonly apiOrigin?: string | URL | undefined;
  readonly fetch?: typeof globalThis.fetch | undefined;
  /** Bounds REST requests and SSE opening handshakes. It does not time out an established SSE body. */
  readonly httpRequestTimeoutMs?: number | undefined;
  /** Total attempts for bounded operations, including the initial attempt. */
  readonly boundedOperationAttempts?: number | undefined;
}

export interface IdempotencyOptions {
  /** Sensitive recovery key retained across every attempt of one logical mutation. */
  readonly idempotencyKey?: string | undefined;
}

export interface InitialStreamLinkOptions {
  readonly linkId: string;
  readonly permissions: LinkPermissions;
}

export interface CreateStreamInput {
  readonly kind?: StreamKind | undefined;
  readonly title?: string | undefined;
  readonly visibility?: Visibility | undefined;
  readonly expiresInSeconds?: number | undefined;
  readonly links?: readonly InitialStreamLinkOptions[] | undefined;
}

export interface PreparedCreateStreamRequest {
  readonly kind: StreamKind;
  readonly title?: StreamTitle | undefined;
  readonly visibility: Visibility;
  readonly expiresInSeconds?: number | undefined;
  readonly links: readonly {
    readonly linkId: LinkId;
    readonly permissions: LinkPermissions;
  }[];
}

/** Authorization for metadata reads. Public streams may omit the link secret. */
export interface ReadAuthOptions {
  readonly linkSecret?: string | undefined;
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
  readonly limit?: number | undefined;
  readonly cursor?: string | undefined;
}

export interface CreateLinkInput {
  readonly linkId: string;
  readonly permissions: LinkPermissions;
  readonly expiresAt?: string | undefined;
}

export interface CreateLinkOptions extends OwnerAuthOptions, IdempotencyOptions {}

export interface UpdateStreamInput {
  readonly title?: string | null | undefined;
  readonly visibility?: Visibility | undefined;
  readonly expiresAt?: string | undefined;
}

export interface AppendRange {
  readonly startSeqNum: bigint;
  readonly endSeqNum: bigint;
}

export interface StatelessAppendRecord {
  readonly part?: PartHeader | undefined;
  readonly data: Uint8Array | string;
}

export interface StatelessAppendRequest {
  readonly clientWriterId: ClientWriterId;
  readonly writerStartSeqNum: bigint;
  readonly records: readonly StatelessAppendRecord[];
  readonly expectedNextSeqNum?: bigint | undefined;
}

export class BaseTsfClient {
  public readonly apiOrigin: string;
  protected readonly boundedOperationAttempts: number;
  readonly #fetch: typeof globalThis.fetch;
  readonly #httpRequestTimeoutMs: number;

  public constructor(options: HttpClientOptions = {}) {
    this.apiOrigin = parseApiOrigin(options.apiOrigin ?? DEFAULT_API_ORIGIN);
    const fetchImplementation = options.fetch ?? globalThis.fetch?.bind(globalThis);
    if (fetchImplementation === undefined) {
      throw new TsfClientError(
        "fetch_unavailable",
        "fetch is unavailable; provide a fetch implementation",
      );
    }
    this.#fetch = fetchImplementation;
    this.#httpRequestTimeoutMs = integerOption(
      options.httpRequestTimeoutMs ?? DEFAULT_HTTP_REQUEST_TIMEOUT_MS,
      "httpRequestTimeoutMs",
      1,
      MAX_TIMER_DELAY_MS,
    );
    this.boundedOperationAttempts = integerOption(
      options.boundedOperationAttempts ?? 3,
      "boundedOperationAttempts",
      1,
    );
  }

  public connectSseReader(options: ReadOptions): Promise<TsfReadSession> {
    return openSseReader(options, {
      fetch: this.#fetch,
      apiOrigin: this.apiOrigin,
      httpRequestTimeoutMs: this.#httpRequestTimeoutMs,
      boundedOperationAttempts: this.boundedOperationAttempts,
    });
  }

  public createStream(
    request: CreateStreamInput = {},
    options: IdempotencyOptions = {},
  ): Promise<CreateStreamResponse> {
    const normalized = prepareCreateStreamRequest(request);
    const body = JSON.stringify(createStreamRequestToWire(normalized));
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
          title: request.title,
          visibility: request.visibility,
          expires_at: request.expiresAt,
        })),
      },
      requiredLinkSecret(options, "update stream", "owner"),
    ).then(streamMetadataFromWire);
  }

  public deleteStream(
    streamId: StreamId,
    options: OwnerAuthOptions,
  ): Promise<void> {
    return this.#delete(
      "delete stream",
      `/streams/${parseStreamId(streamId)}`,
      requiredLinkSecret(options, "delete stream", "owner"),
    );
  }

  public createLink(
    streamId: StreamId,
    request: CreateLinkInput,
    options: CreateLinkOptions,
  ): Promise<CreateLinkResponse> {
    const linkId = parseLinkId(request.linkId);
    const idempotencyKey = parseIdempotencyKey(
      options.idempotencyKey ?? generateIdempotencyKey(),
    );
    const createRequest: WireCreateLinkRequestInput = {
      permissions: request.permissions,
      expires_at: request.expiresAt,
    };
    return this.#json(
      "create link",
      `/streams/${parseStreamId(streamId)}/links/${linkId}`,
      createLinkResponseSchema,
      {
        method: "PUT",
        headers: { "idempotency-key": idempotencyKey },
        body: JSON.stringify(createLinkRequestSchema.parse(createRequest)),
      },
      requiredLinkSecret(options, "create link", "owner"),
    ).then(createLinkResponseFromWire);
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

  /** Lists every retained link, following pagination until completion. */
  public async listAllLinks(
    streamId: StreamId,
    options: OwnerAuthOptions,
  ): Promise<ListLinksResponse> {
    const links: ListLinksResponse["links"][number][] = [];
    const seenCursors = new Set<string>();
    const seenLinkIds = new Set<LinkId>();
    let authorizingLinkId: LinkId | undefined;
    let cursor: string | undefined;

    for (;;) {
      const page = await this.listLinks(streamId, {
        ...options,
        limit: MAX_LINK_PAGE_ITEMS,
        cursor,
      });
      if (
        authorizingLinkId !== undefined &&
        page.authorizingLinkId !== authorizingLinkId
      ) {
        throw new TsfClientError(
          "invalid_api_response",
          "authorizing link changed across link pages",
        );
      }
      authorizingLinkId ??= page.authorizingLinkId;
      for (const link of page.links) {
        if (seenLinkIds.has(link.linkId)) {
          throw new TsfClientError(
            "invalid_api_response",
            "link ID appears on multiple pages",
          );
        }
        seenLinkIds.add(link.linkId);
        links.push(link);
      }
      if (page.nextCursor === null) {
        return {
          authorizingLinkId,
          links,
          nextCursor: null,
        };
      }
      if (seenCursors.has(page.nextCursor)) {
        throw new TsfClientError(
          "invalid_api_response",
          "link pagination cursor repeated",
        );
      }
      seenCursors.add(page.nextCursor);
      cursor = page.nextCursor;
    }
  }

  public revokeLink(
    streamId: StreamId,
    linkId: LinkId,
    options: OwnerAuthOptions,
  ): Promise<void> {
    return this.#delete(
      "revoke link",
      `/streams/${parseStreamId(streamId)}/links/${parseLinkId(linkId)}`,
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
        ...compactRecordPayload(bytes),
        part: part === undefined
          ? undefined
          : { index: part.index, is_final: part.isFinal },
      };
    });
    if (payloadBytes > MAX_STATELESS_APPEND_PAYLOAD_BYTES) {
      throw new TsfClientError(
        "invalid_client_option",
        `append payload must not exceed ${MAX_STATELESS_APPEND_PAYLOAD_BYTES} bytes`,
      );
    }
    const finalWriterSeqNum = request.writerStartSeqNum + BigInt(request.records.length - 1);
    if (request.writerStartSeqNum < 0n || finalWriterSeqNum >= MAX_U64) {
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
      writer: {
        id: encodeBase64url(parseClientWriterId(request.clientWriterId)),
        seq_num: request.writerStartSeqNum.toString(),
      },
      records,
      expected_next_seq_num: request.expectedNextSeqNum?.toString(),
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

  #json<T>(
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

  #delete(
    operation: string,
    path: string,
    linkSecret: string,
  ): Promise<void> {
    return this.#request(operation, path, async (response) => {
      if (response.status !== 204) {
        throw await httpStatusError(response, operation);
      }
    }, { method: "DELETE" }, linkSecret);
  }

  async #request<T>(
    operation: string,
    path: string,
    consume: (response: Response) => Promise<T>,
    init: RequestInit = {},
    linkSecret?: string,
  ): Promise<T> {
    return retryOperation(
      () => this.#requestOnce(operation, path, consume, init, linkSecret),
      {
        attempts: this.boundedOperationAttempts,
        shouldRetry: isRetryableRestError,
        retryAfterMs: (error) =>
          error instanceof TsfHttpError ? error.retryAfterMs : undefined,
      },
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
      `${operation} timed out after ${this.#httpRequestTimeoutMs}ms`,
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
    return withTimeout(request, this.#httpRequestTimeoutMs, operation, undefined, {
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
  if (bytes > MAX_RECORD_PAYLOAD_BYTES) {
    throw new TsfClientError(
      "invalid_client_option",
      `each append record must not exceed ${MAX_RECORD_PAYLOAD_BYTES} bytes`,
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
    : [{ linkId: "owner", permissions: "o" as const }, ...requestedLinks];
  return parsePreparedCreateStreamRequest({
    kind: request.kind ?? "transcript",
    title: request.title,
    visibility: request.visibility ?? "private",
    expiresInSeconds: request.expiresInSeconds,
    links,
  });
}

export function parsePreparedCreateStreamRequest(
  input: unknown,
): PreparedCreateStreamRequest {
  if (
    !isRecord(input) ||
    input.kind === undefined ||
    !Array.isArray(input.links)
  ) {
    throw new TsfClientError(
      "invalid_client_option",
      "normalized stream request is invalid",
    );
  }
  const wire = createStreamRequestSchema.parse({
    kind: input.kind,
    title: input.title,
    visibility: input.visibility,
    expires_in_seconds: input.expiresInSeconds,
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
    kind: wire.kind ?? "transcript",
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
    ...(request.kind === "transcript" ? {} : { kind: request.kind }),
    title: request.title,
    visibility: request.visibility,
    expires_in_seconds: request.expiresInSeconds,
    links: request.links.map((link) => ({
      link_id: link.linkId,
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
