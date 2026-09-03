import type { StreamId } from "./ids.js";
import { parseStreamId } from "./ids.js";
import type { LinkPermissions } from "./permissions.js";
import { parseLinkPermissions } from "./permissions.js";
import { ProtocolError } from "./errors.js";
import { canonicalBase64url, MAX_SAFE_INTEGER_U64, U64_PATTERN } from "./primitives.js";

export const LINK_SECRET_BYTES = 24;
export const LINK_SECRET_ENCODED_LENGTH = Math.ceil(
  LINK_SECRET_BYTES * 8 / 6,
);

const LINK_SECRET_PATTERN = /^[A-Za-z0-9_-]{32}$/;

export interface StreamLinkParam {
  readonly declaredPermissions: LinkPermissions;
  readonly secret: string;
}

export interface StreamAnchor {
  readonly seqNum: bigint;
}

export type StreamRoute = "stream" | "terminal";

export interface StreamLocator {
  readonly streamId: StreamId;
  readonly route: StreamRoute;
  readonly link?: StreamLinkParam;
  readonly anchor?: StreamAnchor;
}

export function parseStreamUrl(input: string): StreamLocator {
  let url: URL;
  try {
    url = new URL(input);
  } catch (cause) {
    throw new ProtocolError("invalid_stream_url", "invalid stream URL", { cause });
  }
  requireWebUrl(url);
  const fragmentIndex = url.href.indexOf("#");
  const queryIndex = url.href.indexOf("?");
  if (
    queryIndex !== -1 &&
    (fragmentIndex === -1 || queryIndex < fragmentIndex)
  ) {
    throw new ProtocolError(
      "invalid_stream_query",
      "stream URLs do not accept query parameters",
    );
  }

  const path = /^\/([st])\/([^/]+)$/.exec(url.pathname);
  const prefix = path?.[1];
  const rawStreamId = path?.[2];
  if (prefix === undefined || rawStreamId === undefined) {
    throw new ProtocolError(
      "invalid_stream_path",
      "stream URL path must be /s/{stream_id} or /t/{stream_id}",
    );
  }

  const streamId = parseStreamId(rawStreamId);
  const route: StreamRoute = prefix === "t" ? "terminal" : "stream";
  if (fragmentIndex === -1) {
    return { streamId, route };
  }
  const fragment = url.hash.slice(1);
  const parameters = Array.from(new URLSearchParams(fragment));
  if (
    fragment.length === 0 ||
    fragment.split("&").some((parameter) => parameter.length === 0)
  ) {
    throw new ProtocolError(
      "invalid_stream_fragment",
      "stream URL fragment must contain a credential or at",
    );
  }
  let link: StreamLinkParam | undefined;
  let anchor: StreamAnchor | undefined;
  for (const [key, value] of parameters) {
    if (key === "at") {
      if (anchor !== undefined) {
        throw new ProtocolError(
          "multiple_stream_anchors",
          "stream URL fragment contains multiple at parameters",
        );
      }
      anchor = parseStreamAnchor(value);
      continue;
    }
    if (link !== undefined) {
      throw new ProtocolError(
        "multiple_stream_links",
        "stream URL fragment contains multiple links",
      );
    }
    link = {
      declaredPermissions: parseLinkPermissions(key),
      secret: parseLinkSecret(value),
    };
  }
  if (route === "terminal" && anchor !== undefined) {
    throw new ProtocolError(
      "terminal_anchor_not_allowed",
      "terminal URLs do not accept record anchors",
    );
  }
  return {
    streamId,
    route,
    ...(link === undefined ? {} : { link }),
    ...(anchor === undefined ? {} : { anchor }),
  };
}

function parseStreamAnchor(raw: string): StreamAnchor {
  if (!U64_PATTERN.test(raw)) {
    throw new ProtocolError(
      "invalid_stream_anchor",
      "at must be a decimal sequence number",
    );
  }
  const seqNum = BigInt(raw);
  if (seqNum > MAX_SAFE_INTEGER_U64) {
    throw new ProtocolError(
      "invalid_stream_anchor",
      "at exceeds the maximum supported sequence number",
    );
  }
  return { seqNum };
}

export function buildStreamLink(
  baseUrl: string | URL,
  streamId: StreamId,
  permissions: LinkPermissions,
  secret: string,
  anchor?: StreamAnchor,
): URL {
  return buildLink(baseUrl, "s", streamId, permissions, secret, anchor);
}

export function buildTerminalLink(
  baseUrl: string | URL,
  streamId: StreamId,
  permissions: LinkPermissions,
  secret: string,
): URL {
  return buildLink(baseUrl, "t", streamId, permissions, secret);
}

export function buildPublicStreamUrl(
  baseUrl: string | URL,
  streamId: StreamId,
): URL {
  const url = normalizedStreamUrl(baseUrl, "s", streamId);
  url.hash = "";
  return url;
}

export function buildPublicTerminalUrl(
  baseUrl: string | URL,
  streamId: StreamId,
): URL {
  const url = normalizedStreamUrl(baseUrl, "t", streamId);
  url.hash = "";
  return url;
}

function buildLink(
  baseUrl: string | URL,
  prefix: "s" | "t",
  streamId: StreamId,
  permissions: LinkPermissions,
  secret: string,
  anchor?: StreamAnchor,
): URL {
  const url = normalizedStreamUrl(baseUrl, prefix, streamId);
  const fragment = new URLSearchParams([
    [parseLinkPermissions(permissions), parseLinkSecret(secret)],
  ]);
  if (anchor !== undefined) {
    fragment.set("at", parseStreamAnchor(anchor.seqNum.toString()).seqNum.toString());
  }
  url.hash = fragment.toString();
  return url;
}

function normalizedStreamUrl(
  baseUrl: string | URL,
  prefix: "s" | "t",
  streamId: StreamId,
): URL {
  const url = new URL(baseUrl);
  requireWebUrl(url);
  url.username = "";
  url.password = "";
  url.pathname = `/${prefix}/${parseStreamId(streamId)}`;
  url.search = "";
  url.hash = "";
  return url;
}

export function parseLinkSecret(input: string): string {
  if (
    input.length !== LINK_SECRET_ENCODED_LENGTH ||
    !LINK_SECRET_PATTERN.test(input) ||
    !canonicalBase64url(input, LINK_SECRET_BYTES)
  ) {
    throw new ProtocolError(
      "invalid_link_secret",
      `link secret must be ${LINK_SECRET_ENCODED_LENGTH} base64url characters`,
    );
  }
  return input;
}

function requireWebUrl(url: URL): void {
  if (url.protocol !== "http:" && url.protocol !== "https:") {
    throw new ProtocolError(
      "invalid_stream_url",
      "stream URL must use HTTP or HTTPS",
    );
  }
}
