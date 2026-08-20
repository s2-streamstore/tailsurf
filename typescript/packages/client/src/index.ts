export { TsfClientError, TsfHttpError, TsfWebSocketClosedError } from "./errors.js";
export type { TsfHttpErrorDetails } from "./errors.js";
export type {
  CreateStreamResponse,
  ListLinksResponse,
  StreamMetadata,
  StreamLinkCredential,
  StreamLinkSummary,
} from "./models.js";
export {
  DEFAULT_API_ORIGIN,
  generateIdempotencyKey,
  parseApiOrigin,
  parseIdempotencyKey,
  parsePreparedCreateStreamRequest,
  prepareCreateStreamRequest,
} from "./rest.js";
export type {
  AppendRange,
  CreateLinkInput,
  CreateLinkOptions,
  CreateStreamInput,
  InitialStreamLinkOptions,
  ListLinksOptions,
  OwnerAuthOptions,
  IdempotencyOptions,
  PreparedCreateStreamRequest,
  ReadAuthOptions,
  RetryPolicy,
  RestClientOptions,
  StatelessAppendRecord,
  StatelessAppendRequest,
  UpdateStreamInput,
  WriteAuthOptions,
} from "./rest.js";
export { TsfClient } from "./websocket.js";
export type { TsfClientOptions, WriteStreamOptions } from "./websocket.js";
export type {
  ReadStart,
  ReadStop,
  ReadOptions,
  TsfReadSession,
} from "./reader.js";
export { isRetryableSocketError } from "./socket.js";
export type { WebSocketFactory, WebSocketLike } from "./socket.js";
export {
  DEFAULT_WRITER_RETAINED_BYTES,
  DEFAULT_WRITER_RETAINED_RECORDS,
  MAX_WRITER_UNACKED_PAYLOAD_BYTES,
  MAX_WRITER_UNACKED_RECORDS,
} from "./writer.js";
export type {
  AppendInput,
  AppendReceipt,
  LogicalAppendInput,
  TsfWriter,
  TsfWriterConfig,
} from "./writer.js";
export {
  DEFAULT_MAX_LOGICAL_RECORD_BYTES,
  DEFAULT_MAX_TRANSCRIPT_PENDING_BYTES,
  DEFAULT_MAX_TRANSCRIPT_PENDING_PARTS,
  DEFAULT_MAX_TRANSCRIPT_WRITERS,
  LINK_SECRET_BYTES,
  LINK_SECRET_ENCODED_LENGTH,
  LogicalTranscript,
  MAX_APPEND_BATCH_RECORDS,
  MAX_BATCH_PAYLOAD_BYTES,
  MAX_PART_INDEX,
  MAX_READ_BATCH_RECORDS,
  MAX_RECORD_BYTES,
  MAX_STREAM_TITLE_CODE_POINTS,
  ProtocolError,
  RecordFormat,
  STREAM_ID_BYTE_LENGTH,
  UNSPLIT_PART,
  WRITER_ID_BYTE_LENGTH,
  buildStreamLink,
  generateClientWriterId,
  generateStreamId,
  isUnsplitPart,
  parseClientWriterId,
  parseLinkId,
  parseLinkPermissions,
  parseLinkSecret,
  parseStreamId,
  parseStreamTitle,
  parseStreamUrl,
  parseWriterId,
  partHeader,
  permissionsAllowOwner,
  permissionsAllowRead,
  permissionsAllowWrite,
  writerIdKey,
} from "@tailsurf/protocol";
export type {
  CaughtUpPosition,
  ClientWriterId,
  LinkId,
  LinkPermissions,
  LogicalTranscriptOptions,
  PartHeader,
  ReadRecord,
  StreamAnchor,
  StreamId,
  StreamLinkParam,
  StreamLocator,
  StreamTitle,
  TranscriptRecord,
  Visibility,
  WriterId,
} from "@tailsurf/protocol";
