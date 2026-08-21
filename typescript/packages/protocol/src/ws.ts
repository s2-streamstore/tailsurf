import { ProtocolError } from "./errors.js";
import type { ClientWriterId, WriterId } from "./ids.js";
import {
  parseClientWriterId,
  parseWriterId,
  validateWriterIdLength,
  WRITER_ID_BYTE_LENGTH,
} from "./ids.js";
import {
  MAX_SAFE_INTEGER_U64,
  MAX_U64,
} from "./primitives.js";
import { streamMetadataSchema, type StreamMetadata } from "./rest.js";
import {
  LINK_SECRET_ENCODED_LENGTH,
  parseLinkSecret,
} from "./stream-url.js";

export const TSF_WEBSOCKET_PROTOCOL = "tsf.v1";
/** Maximum data payload in one physical record. */
export const MAX_RECORD_PAYLOAD_BYTES = 512 * 1024;
/** Maximum physical records in one append protocol frame. */
export const MAX_APPEND_FRAME_RECORDS = 128;
/** Maximum physical records in one read protocol frame. */
export const MAX_READ_FRAME_RECORDS = 1_000;
/** Maximum aggregate record payload in one append or read protocol frame. */
export const MAX_FRAME_PAYLOAD_BYTES = 1024 * 1024;
/** Maximum encoded size of any TSF protocol frame. */
export const MAX_ENCODED_FRAME_BYTES =
  1 + MAX_READ_FRAME_RECORDS * (4 + 45) + MAX_FRAME_PAYLOAD_BYTES;
export const PART_FINAL_BIT = 0x8000_0000;
export const MAX_PART_INDEX = 0x7fff_ffff;

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder("utf-8", { fatal: true });

const ClientOp = {
  OpenRead: 0x01,
  OpenWrite: 0x02,
  AppendBatch: 0x03,
} as const;

const OpenReadFlag = {
  LinkSecret: 0x01,
} as const;

const OpenWriteFlag = {
  ExpectedNextSeqNum: 0x01,
} as const;

const OPEN_WRITE_FLAGS = OpenWriteFlag.ExpectedNextSeqNum;

const OPEN_READ_FLAGS = OpenReadFlag.LinkSecret;

const ServerOp = {
  Ready: 0x80,
  AppendAck: 0x81,
  ReadBatch: 0x82,
  Heartbeat: 0x83,
  CaughtUp: 0x84,
  StreamMetadata: 0x85,
} as const;

export const RecordFormat = {
  Bytes: 0x00,
  Transcript: 0x01,
} as const;

export type RecordFormat = (typeof RecordFormat)[keyof typeof RecordFormat];

export interface PartHeader {
  readonly index: number;
  readonly isFinal: boolean;
}

export const UNSPLIT_PART: PartHeader = Object.freeze({ index: 0, isFinal: true });

export interface AppendRecord {
  readonly writerSeqNum: bigint;
  readonly part: PartHeader;
  readonly format: RecordFormat;
  readonly data: Uint8Array;
}

export type ClientFrame =
  | {
      readonly type: "openRead";
      readonly linkSecret?: string;
    }
  | {
      readonly type: "openWrite";
      readonly clientWriterId: ClientWriterId;
      readonly linkSecret: string;
      readonly expectedNextSeqNum?: bigint;
    }
  | { readonly type: "appendBatch"; readonly records: readonly AppendRecord[] };

export interface ReadRecord {
  readonly seqNum: bigint;
  readonly timestampMs: bigint;
  readonly writerId: WriterId;
  readonly writerSeqNum: bigint;
  readonly part: PartHeader;
  readonly format: RecordFormat;
  readonly data: Uint8Array;
}

export interface CaughtUpPosition {
  readonly nextSeqNum: bigint;
  readonly lastTimestampMs: bigint;
}

export type ServerFrame =
  | { readonly type: "ready" }
  | {
      readonly type: "appendAck";
      readonly writerStartSeqNum: bigint;
      readonly writerEndSeqNum: bigint;
      readonly startSeqNum: bigint;
      readonly endSeqNum: bigint;
    }
  | { readonly type: "readBatch"; readonly records: readonly ReadRecord[] }
  | { readonly type: "heartbeat" }
  | ({ readonly type: "caughtUp" } & CaughtUpPosition)
  | { readonly type: "streamMetadata"; readonly stream: StreamMetadata };

export function partHeader(index: number, isFinal: boolean): PartHeader {
  if (!Number.isInteger(index) || index < 0 || index > MAX_PART_INDEX) {
    throw new ProtocolError(
      "part_index_too_large",
      `part index must be an integer between 0 and ${MAX_PART_INDEX}`,
    );
  }
  return { index, isFinal };
}

export function partHeaderFromRaw(raw: number): PartHeader {
  if (!Number.isInteger(raw) || raw < 0 || raw > 0xffff_ffff) {
    throw new ProtocolError("invalid_part_header", "part header must be a u32");
  }
  return {
    index: raw & MAX_PART_INDEX,
    isFinal: (raw & PART_FINAL_BIT) !== 0,
  };
}

export function partHeaderRaw(part: PartHeader): number {
  const validated = partHeader(part.index, part.isFinal);
  return (validated.index | (validated.isFinal ? PART_FINAL_BIT : 0)) >>> 0;
}

export function isUnsplitPart(part: PartHeader): boolean {
  return part.index === 0 && part.isFinal;
}

export function encodeClientFrame(frame: ClientFrame): Uint8Array {
  switch (frame.type) {
    case "openRead":
      return encodeOpenRead(frame);
    case "openWrite": {
      const clientWriterId = parseClientWriterId(frame.clientWriterId);
      const secret = textEncoder.encode(parseLinkSecret(frame.linkSecret));
      const expected = frame.expectedNextSeqNum;
      if (expected !== undefined) {
        validateExpectedNextSeqNum(expected);
      }
      const output = new Uint8Array(
        2 + WRITER_ID_BYTE_LENGTH +
          (expected === undefined ? 0 : 8) +
          LINK_SECRET_ENCODED_LENGTH,
      );
      const view = new DataView(output.buffer);
      output[0] = ClientOp.OpenWrite;
      output[1] = expected === undefined ? 0 : OpenWriteFlag.ExpectedNextSeqNum;
      output.set(clientWriterId, 2);
      let offset = 2 + WRITER_ID_BYTE_LENGTH;
      if (expected !== undefined) {
        writeU64(view, offset, expected);
        offset += 8;
      }
      output.set(secret, offset);
      return output;
    }
    case "appendBatch": {
      const output = new Uint8Array(
        batchFrameLength(frame.records, 13, MAX_APPEND_FRAME_RECORDS),
      );
      const view = new DataView(output.buffer);
      output[0] = ClientOp.AppendBatch;
      let offset = 1;
      for (const record of frame.records) {
        validateAppendWriterSeqNum(record.writerSeqNum);
        view.setUint32(offset, 13 + record.data.byteLength);
        writeU64(view, offset + 4, record.writerSeqNum);
        view.setUint32(offset + 12, partHeaderRaw(record.part));
        output[offset + 16] = validateRecordFormat(record.format);
        output.set(record.data, offset + 17);
        offset += 17 + record.data.byteLength;
      }
      return output;
    }
  }
}

export function decodeClientFrame(input: Uint8Array | ArrayBuffer): ClientFrame {
  const bytes = toBytes(input);
  const op = requireOperation(bytes);
  switch (op) {
    case ClientOp.OpenRead:
      return decodeOpenRead(bytes);
    case ClientOp.OpenWrite: {
      requireLength(bytes, 2 + WRITER_ID_BYTE_LENGTH);
      const flags = requireByte(bytes, 1);
      if ((flags & ~OPEN_WRITE_FLAGS) !== 0) {
        throw new ProtocolError(
          "unknown_open_write_flags",
          `OpenWrite has unknown flags 0x${(flags & ~OPEN_WRITE_FLAGS).toString(16).padStart(2, "0")}`,
        );
      }
      const hasExpected = (flags & OpenWriteFlag.ExpectedNextSeqNum) !== 0;
      const expectedOffset = 2 + WRITER_ID_BYTE_LENGTH;
      const secretOffset = expectedOffset + (hasExpected ? 8 : 0);
      requireExactLength(
        bytes,
        secretOffset + LINK_SECRET_ENCODED_LENGTH,
        ClientOp.OpenWrite,
      );
      const expectedNextSeqNum = hasExpected
        ? dataView(bytes).getBigUint64(expectedOffset)
        : undefined;
      if (expectedNextSeqNum !== undefined) {
        validateExpectedNextSeqNum(expectedNextSeqNum);
      }
      return {
        type: "openWrite",
        clientWriterId: parseClientWriterId(bytes.subarray(2, expectedOffset)),
        linkSecret: parseLinkSecret(
          decodeUtf8(bytes.subarray(secretOffset)),
        ),
        ...(expectedNextSeqNum === undefined ? {} : { expectedNextSeqNum }),
      };
    }
    case ClientOp.AppendBatch: {
      const view = dataView(bytes);
      const records: AppendRecord[] = [];
      let payloadBytes = 0;
      for (const body of recordBodies(bytes, view, MAX_APPEND_FRAME_RECORDS)) {
        requireLength(body, 13);
        const bodyView = dataView(body);
        const data = body.slice(13);
        const writerSeqNum = bodyView.getBigUint64(0);
        validateAppendWriterSeqNum(writerSeqNum);
        validateRecordLength(data.byteLength);
        payloadBytes += data.byteLength;
        records.push({
          writerSeqNum,
          part: partHeaderFromRaw(bodyView.getUint32(8)),
          format: validateRecordFormat(body[12]),
          data,
        });
      }
      validateBatchBounds(
        records.length,
        payloadBytes,
        MAX_APPEND_FRAME_RECORDS,
      );
      return { type: "appendBatch", records };
    }
    default:
      throw unknownOperation(op);
  }
}

function encodeOpenRead(
  frame: Extract<ClientFrame, { readonly type: "openRead" }>,
): Uint8Array {
  const secret = frame.linkSecret === undefined
    ? undefined
    : textEncoder.encode(parseLinkSecret(frame.linkSecret));
  const flags = secret === undefined ? 0 : OpenReadFlag.LinkSecret;
  const output = new Uint8Array(
    2 + (secret === undefined ? 0 : LINK_SECRET_ENCODED_LENGTH),
  );
  output[0] = ClientOp.OpenRead;
  output[1] = flags;
  if (secret !== undefined) {
    output.set(secret, 2);
  }
  return output;
}

function decodeOpenRead(bytes: Uint8Array): Extract<
  ClientFrame,
  { readonly type: "openRead" }
> {
  requireLength(bytes, 2);
  const flags = requireByte(bytes, 1);
  if ((flags & ~OPEN_READ_FLAGS) !== 0) {
    throw new ProtocolError(
      "unknown_open_read_flags",
      `OpenRead has unknown flags 0x${(flags & ~OPEN_READ_FLAGS).toString(16).padStart(2, "0")}`,
    );
  }
  let linkSecret: string | undefined;
  if ((flags & OpenReadFlag.LinkSecret) !== 0) {
    requireExactLength(
      bytes,
      2 + LINK_SECRET_ENCODED_LENGTH,
      ClientOp.OpenRead,
    );
    linkSecret = parseLinkSecret(decodeUtf8(bytes.subarray(2)));
  } else {
    requireExactLength(bytes, 2, ClientOp.OpenRead);
  }
  return {
    type: "openRead" as const,
    ...(linkSecret === undefined ? {} : { linkSecret }),
  };
}

export function encodeServerFrame(frame: ServerFrame): Uint8Array {
  switch (frame.type) {
    case "ready":
      return Uint8Array.of(ServerOp.Ready);
    case "appendAck": {
      const output = new Uint8Array(33);
      const view = new DataView(output.buffer);
      output[0] = ServerOp.AppendAck;
      writeU64(view, 1, frame.writerStartSeqNum);
      writeU64(view, 9, frame.writerEndSeqNum);
      writeU64(view, 17, frame.startSeqNum);
      writeU64(view, 25, frame.endSeqNum);
      return output;
    }
    case "readBatch": {
      const output = new Uint8Array(
        batchFrameLength(frame.records, 45, MAX_READ_FRAME_RECORDS),
      );
      validateReadBatchSequence(frame.records);
      const view = new DataView(output.buffer);
      output[0] = ServerOp.ReadBatch;
      let offset = 1;
      for (const record of frame.records) {
        validateWriterIdLength(record.writerId, "writer ID", "invalid_writer_id");
        view.setUint32(offset, 45 + record.data.byteLength);
        writeU64(view, offset + 4, record.seqNum);
        writeU64(view, offset + 12, record.timestampMs);
        output.set(record.writerId, offset + 20);
        writeU64(view, offset + 36, record.writerSeqNum);
        view.setUint32(offset + 44, partHeaderRaw(record.part));
        output[offset + 48] = validateRecordFormat(record.format);
        output.set(record.data, offset + 49);
        offset += 49 + record.data.byteLength;
      }
      return output;
    }
    case "heartbeat":
      return Uint8Array.of(ServerOp.Heartbeat);
    case "caughtUp": {
      const output = new Uint8Array(17);
      const view = new DataView(output.buffer);
      output[0] = ServerOp.CaughtUp;
      writeU64(view, 1, frame.nextSeqNum);
      writeU64(view, 9, frame.lastTimestampMs);
      return output;
    }
    case "streamMetadata": {
      const stream = streamMetadataSchema.parse(frame.stream);
      const payload = textEncoder.encode(JSON.stringify(stream));
      const output = new Uint8Array(1 + payload.byteLength);
      output[0] = ServerOp.StreamMetadata;
      output.set(payload, 1);
      return output;
    }
  }
}

export function decodeServerFrame(input: Uint8Array | ArrayBuffer): ServerFrame {
  const bytes = toBytes(input);
  const op = requireOperation(bytes);
  const view = dataView(bytes);
  switch (op) {
    case ServerOp.Ready:
      requireExactLength(bytes, 1, op);
      return { type: "ready" };
    case ServerOp.AppendAck:
      requireExactLength(bytes, 33, op);
      return {
        type: "appendAck",
        writerStartSeqNum: view.getBigUint64(1),
        writerEndSeqNum: view.getBigUint64(9),
        startSeqNum: view.getBigUint64(17),
        endSeqNum: view.getBigUint64(25),
      };
    case ServerOp.ReadBatch: {
      const records: ReadRecord[] = [];
      let payloadBytes = 0;
      for (const body of recordBodies(bytes, view, MAX_READ_FRAME_RECORDS)) {
        requireLength(body, 45);
        const bodyView = dataView(body);
        const data = body.slice(45);
        validateRecordLength(data.byteLength);
        payloadBytes += data.byteLength;
        records.push({
          seqNum: bodyView.getBigUint64(0),
          timestampMs: bodyView.getBigUint64(8),
          writerId: parseWriterId(body.subarray(16, 32)),
          writerSeqNum: bodyView.getBigUint64(32),
          part: partHeaderFromRaw(bodyView.getUint32(40)),
          format: validateRecordFormat(body[44]),
          data,
        });
      }
      validateBatchBounds(records.length, payloadBytes, MAX_READ_FRAME_RECORDS);
      validateReadBatchSequence(records);
      return { type: "readBatch", records };
    }
    case ServerOp.Heartbeat:
      requireExactLength(bytes, 1, op);
      return { type: "heartbeat" };
    case ServerOp.CaughtUp:
      requireExactLength(bytes, 17, op);
      return {
        type: "caughtUp",
        nextSeqNum: view.getBigUint64(1),
        lastTimestampMs: view.getBigUint64(9),
      };
    case ServerOp.StreamMetadata: {
      let body: unknown;
      try {
        body = JSON.parse(decodeUtf8(bytes.subarray(1)));
      } catch (cause) {
        throw new ProtocolError(
          "invalid_stream_metadata",
          "stream metadata frame must contain valid JSON",
          { cause },
        );
      }
      const parsed = streamMetadataSchema.safeParse(body);
      if (!parsed.success) {
        throw new ProtocolError(
          "invalid_stream_metadata",
          "stream metadata frame does not match the stream metadata schema",
          { cause: parsed.error },
        );
      }
      return { type: "streamMetadata", stream: parsed.data };
    }
    default:
      throw unknownOperation(op);
  }
}

function toBytes(input: Uint8Array | ArrayBuffer): Uint8Array {
  return input instanceof Uint8Array ? input : new Uint8Array(input);
}

function dataView(bytes: Uint8Array): DataView {
  return new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
}

function requireOperation(bytes: Uint8Array): number {
  if (bytes.byteLength === 0) {
    throw new ProtocolError("empty_frame", "frame cannot be empty");
  }
  return requireByte(bytes, 0);
}

function requireByte(bytes: Uint8Array, offset: number): number {
  const value = bytes[offset];
  if (value === undefined) {
    throw new ProtocolError("truncated_frame", "frame is truncated");
  }
  return value;
}

function requireLength(bytes: Uint8Array, minimum: number): void {
  if (bytes.byteLength < minimum) {
    throw new ProtocolError(
      "truncated_frame",
      `frame is ${bytes.byteLength} bytes; expected at least ${minimum}`,
    );
  }
}

function batchFrameLength(
  records: readonly { readonly data: Uint8Array }[],
  recordHeaderBytes: number,
  maximumRecords: number,
): number {
  let payloadBytes = 0;
  for (const record of records) {
    validateRecordLength(record.data.byteLength);
    payloadBytes += record.data.byteLength;
  }
  validateBatchBounds(records.length, payloadBytes, maximumRecords);
  return 1 + records.length * (4 + recordHeaderBytes) + payloadBytes;
}

function validateBatchBounds(
  recordCount: number,
  payloadBytes: number,
  maximumRecords: number,
): void {
  if (recordCount === 0 || recordCount > maximumRecords) {
    throw new ProtocolError(
      "invalid_batch_record_count",
      `batch must contain 1 to ${maximumRecords} records`,
    );
  }
  if (payloadBytes > MAX_FRAME_PAYLOAD_BYTES) {
    throw new ProtocolError(
      "batch_payload_too_large",
      `batch payload exceeds ${MAX_FRAME_PAYLOAD_BYTES} bytes`,
    );
  }
}

function validateReadBatchSequence(
  records: readonly Pick<ReadRecord, "seqNum">[],
): void {
  for (let index = 1; index < records.length; index += 1) {
    const previous = records[index - 1]!.seqNum;
    if (previous === MAX_U64 || records[index]!.seqNum !== previous + 1n) {
      throw new ProtocolError(
        "non_contiguous_read_batch",
        "ReadBatch sequence numbers must be contiguous",
      );
    }
  }
}

function recordBodies(
  bytes: Uint8Array,
  view: DataView,
  maximumRecords: number,
): readonly Uint8Array[] {
  const records: Uint8Array[] = [];
  let offset = 1;
  while (offset < bytes.byteLength) {
    if (records.length === maximumRecords) {
      throw new ProtocolError(
        "invalid_batch_record_count",
        `batch must contain at most ${maximumRecords} records`,
      );
    }
    if (bytes.byteLength - offset < 4) {
      throw new ProtocolError("truncated_frame", "record length is truncated");
    }
    const length = view.getUint32(offset);
    offset += 4;
    if (length === 0 || length > bytes.byteLength - offset) {
      throw new ProtocolError("invalid_record_length", "record length is invalid");
    }
    records.push(bytes.subarray(offset, offset + length));
    offset += length;
  }
  if (records.length === 0) {
    throw new ProtocolError("invalid_batch_record_count", "batch cannot be empty");
  }
  return records;
}

function requireExactLength(bytes: Uint8Array, expected: number, op: number): void {
  requireLength(bytes, expected);
  if (bytes.byteLength > expected) {
    throw new ProtocolError(
      "trailing_frame_bytes",
      `frame 0x${op.toString(16).padStart(2, "0")} has ${bytes.byteLength - expected} trailing bytes`,
    );
  }
}

function validateRecordLength(length: number): void {
  if (length > MAX_RECORD_PAYLOAD_BYTES) {
    throw new ProtocolError(
      "record_too_large",
      `record is ${length} bytes; maximum is ${MAX_RECORD_PAYLOAD_BYTES}`,
    );
  }
}

function validateRecordFormat(value: number | undefined): RecordFormat {
  if (value !== RecordFormat.Bytes && value !== RecordFormat.Transcript) {
    throw new ProtocolError(
      "unknown_record_format",
      `unknown record format ${String(value)}`,
    );
  }
  return value;
}

function validateAppendWriterSeqNum(value: bigint): void {
  validateU64(value);
  if (value === MAX_U64) {
    throw new ProtocolError(
      "writer_sequence_exhausted",
      "writer sequence must leave room for an exclusive acknowledgement boundary",
    );
  }
}

function validateExpectedNextSeqNum(value: bigint): void {
  validateU64(value);
  if (value > MAX_SAFE_INTEGER_U64) {
    throw new ProtocolError(
      "expected_next_seq_num_out_of_range",
      `expected next sequence cannot exceed ${MAX_SAFE_INTEGER_U64}`,
    );
  }
}

function writeU64(view: DataView, offset: number, value: bigint): void {
  validateU64(value);
  view.setBigUint64(offset, value);
}

function validateU64(value: bigint): void {
  if (value < 0n || value > MAX_U64) {
    throw new ProtocolError("integer_out_of_range", "value must be a u64");
  }
}

function decodeUtf8(bytes: Uint8Array): string {
  try {
    return textDecoder.decode(bytes);
  } catch (cause) {
    throw new ProtocolError("invalid_utf8", "link secret is not valid UTF-8", { cause });
  }
}

function unknownOperation(op: number): ProtocolError {
  return new ProtocolError(
    "unknown_operation",
    `unknown operation id 0x${op.toString(16).padStart(2, "0")}`,
  );
}
