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
  DEFAULT_MAX_WRITER_RETAINED_BYTES,
  DEFAULT_MAX_WRITER_RETAINED_RECORDS,
  MAX_APPEND_SUBMISSION_PAYLOAD_BYTES,
  MAX_APPEND_SUBMISSION_RECORDS,
  MAX_WRITER_IN_FLIGHT_BYTES,
  MAX_WRITER_IN_FLIGHT_RECORDS,
} from "./writer.js";
export type {
  AppendInput,
  AppendReceipt,
  LogicalAppendInput,
  TsfWriter,
  TsfWriterConfig,
} from "./writer.js";
export {
  DEFAULT_MAX_TRANSCRIPT_REASSEMBLY_BYTES,
  DEFAULT_MAX_TRANSCRIPT_TOTAL_PENDING_PARTS,
  DEFAULT_MAX_TRANSCRIPT_WRITER_STATES,
  LINK_SECRET_BYTES,
  LINK_SECRET_ENCODED_LENGTH,
  LogicalTranscript,
  MAX_APPEND_FRAME_RECORDS,
  MAX_ENCODED_FRAME_BYTES,
  MAX_FRAME_PAYLOAD_BYTES,
  MAX_PART_INDEX,
  MAX_READ_FRAME_RECORDS,
  MAX_RECORD_PAYLOAD_BYTES,
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
} from "@s2-dev/tailsurf-protocol";
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
} from "@s2-dev/tailsurf-protocol";
