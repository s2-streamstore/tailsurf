import type {
  CreateLinkResponse as WireCreateLinkResponse,
  CreateStreamResponse as WireCreateStreamResponse,
  StreamLinkCredential as WireStreamLinkCredential,
  LinkId,
  LinkPermissions,
  ListLinksResponse as WireListLinksResponse,
  StreamId,
  StreamMetadata as WireStreamMetadata,
  StreamLinkSummary as WireStreamLinkSummary,
  StreamTitle,
  StreamKind,
  Visibility,
} from "@tailsurf/protocol";

export interface StreamMetadata {
  readonly streamId: StreamId;
  readonly kind: StreamKind;
  readonly title: StreamTitle | null;
  readonly visibility: Visibility;
  readonly createdAt: string;
  readonly expiresAt: string;
}

export interface StreamLinkCredential {
  readonly linkId: LinkId;
  readonly permissions: LinkPermissions;
  readonly secret: string;
}

export interface CreateStreamResponse extends StreamMetadata {
  readonly webOrigin: string;
  readonly links: readonly StreamLinkCredential[];
}

export interface CreateLinkResponse extends StreamLinkCredential {
  readonly webOrigin: string;
}

export interface StreamLinkSummary {
  readonly linkId: LinkId;
  readonly permissions: LinkPermissions;
  readonly status: "active" | "expired" | "revoked";
  readonly createdAt: string;
  readonly expiresAt: string | null;
  readonly revokedAt: string | null;
}

export interface ListLinksResponse {
  readonly authorizingLinkId: LinkId;
  readonly links: readonly StreamLinkSummary[];
  readonly nextCursor: string | null;
}

export function streamMetadataFromWire(
  stream: WireStreamMetadata,
): StreamMetadata {
  return {
    streamId: stream.stream_id,
    kind: stream.kind,
    title: stream.title,
    visibility: stream.visibility,
    createdAt: stream.created_at,
    expiresAt: stream.expires_at,
  };
}

export function streamLinkCredentialFromWire(
  link: WireStreamLinkCredential,
): StreamLinkCredential {
  return {
    linkId: link.link_id,
    permissions: link.permissions,
    secret: link.secret,
  };
}

export function createStreamResponseFromWire(
  stream: WireCreateStreamResponse,
): CreateStreamResponse {
  return {
    ...streamMetadataFromWire(stream),
    webOrigin: stream.web_origin,
    links: stream.links.map(streamLinkCredentialFromWire),
  };
}

export function createLinkResponseFromWire(
  link: WireCreateLinkResponse,
): CreateLinkResponse {
  return {
    ...streamLinkCredentialFromWire(link),
    webOrigin: link.web_origin,
  };
}

function streamLinkSummaryFromWire(
  link: WireStreamLinkSummary,
): StreamLinkSummary {
  return {
    linkId: link.link_id,
    permissions: link.permissions,
    status: link.status,
    createdAt: link.created_at,
    expiresAt: link.expires_at,
    revokedAt: link.revoked_at,
  };
}

export function listLinksResponseFromWire(
  page: WireListLinksResponse,
): ListLinksResponse {
  return {
    authorizingLinkId: page.authorizing_link_id,
    links: page.links.map(streamLinkSummaryFromWire),
    nextCursor: page.next_cursor,
  };
}
