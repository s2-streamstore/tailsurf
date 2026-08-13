//! Exact binary codec for `tsf.v1` WebSocket traffic.

use bytes::{BufMut, Bytes, BytesMut};
use secrecy::ExposeSecret;
use serde::{Deserialize, Serialize};

use crate::{LinkSecret, StreamId, StreamTitle, WriterId, protocol::rest::Visibility};
/// WebSocket subprotocol offered and selected for TSF v1 connections.
pub const TSF_WS_PROTOCOL: &str = "tsf.v1";
/// Maximum data payload in one physical record.
pub const MAX_RECORD_BYTES: usize = 512 * 1024;
/// Maximum physical records carried by one append batch.
pub const MAX_APPEND_BATCH_RECORDS: usize = 128;
/// Maximum physical records carried by one read batch.
pub const MAX_READ_BATCH_RECORDS: usize = 1_000;
/// Maximum aggregate record payload carried by one batch frame.
pub const MAX_BATCH_PAYLOAD_BYTES: usize = 1024 * 1024;

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ClientOp {
    AuthRead = 0x01,
    AuthWrite = 0x02,
    AppendBatch = 0x03,
}

impl ClientOp {
    const fn byte(self) -> u8 {
        self as u8
    }
}

impl TryFrom<u8> for ClientOp {
    type Error = FrameCodecError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            value if value == Self::AuthRead.byte() => Ok(Self::AuthRead),
            value if value == Self::AuthWrite.byte() => Ok(Self::AuthWrite),
            value if value == Self::AppendBatch.byte() => Ok(Self::AppendBatch),
            other => Err(FrameCodecError::UnknownOperation(other)),
        }
    }
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ServerOp {
    Ready = 0x80,
    Ack = 0x81,
    ReadBatch = 0x82,
    Heartbeat = 0x83,
    CaughtUp = 0x84,
    StreamInfo = 0x85,
}

impl ServerOp {
    const fn byte(self) -> u8 {
        self as u8
    }
}

impl TryFrom<u8> for ServerOp {
    type Error = FrameCodecError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            value if value == Self::Ready.byte() => Ok(Self::Ready),
            value if value == Self::Ack.byte() => Ok(Self::Ack),
            value if value == Self::ReadBatch.byte() => Ok(Self::ReadBatch),
            value if value == Self::Heartbeat.byte() => Ok(Self::Heartbeat),
            value if value == Self::CaughtUp.byte() => Ok(Self::CaughtUp),
            value if value == Self::StreamInfo.byte() => Ok(Self::StreamInfo),
            other => Err(FrameCodecError::UnknownOperation(other)),
        }
    }
}

/// Packed split-record part index and final-part marker.
///
/// The high bit marks the final part and the low 31 bits contain the zero-based part index. An
/// unsplit record is encoded as index zero with the final bit set.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PartHeader(u32);

impl PartHeader {
    /// Bit used to mark the final physical part of a logical record.
    pub const FINAL_BIT: u32 = 0x8000_0000;
    /// Largest part index representable in the remaining 31 bits.
    pub const MAX_INDEX: u32 = 0x7fff_ffff;

    /// Packs a validated zero-based index and final marker.
    pub fn new(index: u32, is_final: bool) -> Result<Self, FrameCodecError> {
        if index > Self::MAX_INDEX {
            return Err(FrameCodecError::PartIndexTooLarge(index));
        }

        let final_bit = if is_final { Self::FINAL_BIT } else { 0 };
        Ok(Self(final_bit | index))
    }

    /// Returns the canonical header for a complete unsplit logical record.
    pub const fn unsplit() -> Self {
        Self(Self::FINAL_BIT)
    }

    /// Creates a header from its exact wire representation.
    pub const fn from_raw(raw: u32) -> Self {
        Self(raw)
    }

    /// Returns the exact packed wire representation.
    pub const fn raw(self) -> u32 {
        self.0
    }

    /// Returns the zero-based part index.
    pub const fn index(self) -> u32 {
        self.0 & Self::MAX_INDEX
    }

    /// Returns whether this physical record is the final logical part.
    pub const fn is_final(self) -> bool {
        self.0 & Self::FINAL_BIT != 0
    }
}

/// Presentation hint carried with each record without transforming its bytes.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum RecordFormat {
    /// Opaque binary bytes.
    #[default]
    Bytes = 0x00,
    /// Transcript-oriented bytes suitable for text-first presentation.
    Transcript = 0x01,
}

impl RecordFormat {
    /// Returns the one-byte wire representation.
    pub const fn byte(self) -> u8 {
        self as u8
    }
}

impl TryFrom<u8> for RecordFormat {
    type Error = FrameCodecError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            value if value == Self::Bytes.byte() => Ok(Self::Bytes),
            value if value == Self::Transcript.byte() => Ok(Self::Transcript),
            other => Err(FrameCodecError::UnknownRecordFormat(other)),
        }
    }
}

/// One physical stream record delivered by the read data plane.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadRecord {
    /// Durable absolute sequence number.
    pub seq_num: u64,
    /// Record timestamp as Unix epoch milliseconds.
    pub timestamp_ms: u64,
    /// Stable identity of the producer that wrote this record.
    pub writer_id: WriterId,
    /// Writer-local sequence number reused if this record is retransmitted.
    pub writer_seq_num: u64,
    /// Logical split-part metadata.
    pub part: PartHeader,
    /// Presentation hint for the payload.
    pub format: RecordFormat,
    /// Exact record payload bytes.
    pub data: Bytes,
}

/// One physical record submitted by a writer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppendRecord {
    /// Writer-local sequence number reused on retransmission.
    pub writer_seq_num: u64,
    /// Logical split-part metadata.
    pub part: PartHeader,
    /// Presentation hint for the payload.
    pub format: RecordFormat,
    /// Exact record payload bytes.
    pub data: Bytes,
}

/// Reconnect-safe position emitted after all preceding records have been delivered.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReadCaughtUp {
    /// Sequence number assigned to the next appended record.
    pub next_seq_num: u64,
    /// Timestamp of the last record, or zero for an empty stream.
    pub last_timestamp_ms: u64,
}

/// Stream metadata supplied by an authorized read handshake.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReadStreamInfo {
    /// Stable stream identifier.
    pub stream_id: StreamId,
    /// Human-facing title when one has been set.
    pub title: Option<StreamTitle>,
    /// Current visibility.
    pub visibility: Visibility,
    /// Absolute RFC 3339 stream creation timestamp.
    pub created_at: String,
    /// Absolute RFC 3339 stream expiration timestamp.
    pub expires_at: String,
}

/// Frame sent from a reader or writer to the service.
#[derive(Clone, Debug)]
pub enum ClientFrame {
    /// Authenticates a private read connection.
    AuthRead {
        /// Secret from a read-capable stream link.
        link_secret: LinkSecret,
    },
    /// Authenticates a write connection and establishes writer identity.
    AuthWrite {
        /// Stable identity reused across reconnects.
        writer_id: WriterId,
        /// Secret from a write-capable stream link.
        link_secret: LinkSecret,
    },
    /// Submits a bounded batch of physical records for durable append.
    AppendBatch(Vec<AppendRecord>),
}

/// Frame sent from the service to a reader or writer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ServerFrame {
    /// Confirms successful authorization and socket readiness.
    Ready,
    /// Confirms a contiguous range of writer records is durable.
    Ack {
        /// First acknowledged writer-local sequence number.
        writer_seq_start: u64,
        /// Last acknowledged writer-local sequence number, inclusive.
        writer_seq_end: u64,
        /// Durable sequence number assigned to the first acknowledged record.
        seq_start: u64,
        /// Durable sequence number assigned to the last acknowledged record, inclusive.
        seq_end: u64,
    },
    /// Delivers a bounded batch of physical stream records.
    ReadBatch(Vec<ReadRecord>),
    /// Keeps an otherwise idle unbounded read connection active.
    Heartbeat,
    /// Confirms that every record preceding the captured position was delivered.
    CaughtUp(ReadCaughtUp),
    /// Supplies stream metadata from the read authorization result.
    StreamInfo(ReadStreamInfo),
}

impl ClientFrame {
    const APPEND_BODY_HEADER_LEN: usize = 8 + 4 + 1;

    /// Returns the exact wire length of this frame, validating the payload size for records.
    fn encoded_len(&self) -> Result<usize, FrameCodecError> {
        match self {
            Self::AuthRead { link_secret } => Ok(1 + link_secret.expose_secret().len()),
            Self::AuthWrite { link_secret, .. } => {
                Ok(1 + WriterId::BYTE_LEN + link_secret.expose_secret().len())
            }
            Self::AppendBatch(records) => batch_encoded_len(
                records.iter().map(|record| &record.data),
                Self::APPEND_BODY_HEADER_LEN,
                MAX_APPEND_BATCH_RECORDS,
            ),
        }
    }

    /// Writes this frame into `output`, which must have at least [`Self::encoded_len`] capacity.
    fn encode_into(&self, output: &mut BytesMut) {
        match self {
            Self::AuthRead { link_secret } => {
                output.put_u8(ClientOp::AuthRead.byte());
                output.put_slice(link_secret.expose_secret().as_bytes());
            }
            Self::AuthWrite {
                writer_id,
                link_secret,
            } => {
                output.put_u8(ClientOp::AuthWrite.byte());
                output.put_slice(writer_id.as_bytes());
                output.put_slice(link_secret.expose_secret().as_bytes());
            }
            Self::AppendBatch(records) => {
                output.put_u8(ClientOp::AppendBatch.byte());
                for record in records {
                    output.put_u32((Self::APPEND_BODY_HEADER_LEN + record.data.len()) as u32);
                    output.put_u64(record.writer_seq_num);
                    output.put_u32(record.part.raw());
                    output.put_u8(record.format.byte());
                    output.put_slice(&record.data);
                }
            }
        }
    }

    /// Encodes one client frame into a complete WebSocket binary message.
    pub fn encode(&self) -> Result<Bytes, FrameCodecError> {
        let mut output = BytesMut::with_capacity(self.encoded_len()?);
        self.encode_into(&mut output);
        Ok(output.freeze())
    }

    /// Decodes one client frame, copying any record payload into owned bytes.
    pub fn decode(input: &[u8]) -> Result<Self, FrameCodecError> {
        decode_client_frame(input)
    }

    /// Decodes one client frame while retaining a zero-copy slice for record payload data.
    pub fn decode_bytes(input: Bytes) -> Result<Self, FrameCodecError> {
        decode_client_frame(input)
    }
}

impl ServerFrame {
    const READ_BODY_HEADER_LEN: usize = 8 + 8 + WriterId::BYTE_LEN + 8 + 4 + 1;
    /// Largest encoded size among the fixed-width frames, set by [`ServerFrame::Ack`].
    const MAX_FIXED_FRAME_LEN: usize = 1 + 4 * 8;

    /// Returns the exact wire length of this frame, validating the payload size for records.
    fn encoded_len(&self) -> Result<usize, FrameCodecError> {
        match self {
            Self::ReadBatch(records) => batch_encoded_len(
                records.iter().map(|record| &record.data),
                Self::READ_BODY_HEADER_LEN,
                MAX_READ_BATCH_RECORDS,
            ),
            Self::StreamInfo(_) => Ok(1),
            _ => Ok(Self::MAX_FIXED_FRAME_LEN),
        }
    }

    /// Writes this frame into `output`, which must have at least [`Self::encoded_len`] capacity.
    fn encode_into(&self, output: &mut BytesMut) -> Result<(), FrameCodecError> {
        match self {
            Self::Ready => output.put_u8(ServerOp::Ready.byte()),
            Self::Ack {
                writer_seq_start,
                writer_seq_end,
                seq_start,
                seq_end,
            } => {
                output.put_u8(ServerOp::Ack.byte());
                output.put_u64(*writer_seq_start);
                output.put_u64(*writer_seq_end);
                output.put_u64(*seq_start);
                output.put_u64(*seq_end);
            }
            Self::ReadBatch(records) => {
                output.put_u8(ServerOp::ReadBatch.byte());
                for record in records {
                    output.put_u32((Self::READ_BODY_HEADER_LEN + record.data.len()) as u32);
                    output.put_u64(record.seq_num);
                    output.put_u64(record.timestamp_ms);
                    output.put_slice(record.writer_id.as_bytes());
                    output.put_u64(record.writer_seq_num);
                    output.put_u32(record.part.raw());
                    output.put_u8(record.format.byte());
                    output.put_slice(&record.data);
                }
            }
            Self::Heartbeat => output.put_u8(ServerOp::Heartbeat.byte()),
            Self::CaughtUp(caught_up) => {
                output.put_u8(ServerOp::CaughtUp.byte());
                output.put_u64(caught_up.next_seq_num);
                output.put_u64(caught_up.last_timestamp_ms);
            }
            Self::StreamInfo(stream) => {
                let payload =
                    serde_json::to_vec(stream).map_err(FrameCodecError::InvalidStreamInfo)?;
                output.put_u8(ServerOp::StreamInfo.byte());
                output.put_slice(&payload);
            }
        }
        Ok(())
    }

    /// Encodes one server frame into a complete WebSocket binary message.
    pub fn encode(&self) -> Result<Bytes, FrameCodecError> {
        let mut output = BytesMut::with_capacity(self.encoded_len()?);
        self.encode_into(&mut output)?;
        Ok(output.freeze())
    }

    /// Decodes one server frame, copying any record payload into owned bytes.
    pub fn decode(input: &[u8]) -> Result<Self, FrameCodecError> {
        decode_server_frame(input)
    }

    /// Decodes one server frame while retaining a zero-copy slice for record payload data.
    pub fn decode_bytes(input: Bytes) -> Result<Self, FrameCodecError> {
        decode_server_frame(input)
    }
}

trait FrameInput {
    fn into_bytes(self) -> Bytes;
}

impl FrameInput for &[u8] {
    fn into_bytes(self) -> Bytes {
        Bytes::copy_from_slice(self)
    }
}

impl FrameInput for Bytes {
    fn into_bytes(self) -> Bytes {
        self
    }
}

fn decode_client_frame(input: impl FrameInput) -> Result<ClientFrame, FrameCodecError> {
    let input = input.into_bytes();
    let bytes = input.as_ref();
    let Some((&op_byte, body)) = bytes.split_first() else {
        return Err(FrameCodecError::EmptyFrame);
    };

    match ClientOp::try_from(op_byte)? {
        ClientOp::AuthRead => Ok(ClientFrame::AuthRead {
            link_secret: LinkSecret::from(utf8_tail(body)?),
        }),
        ClientOp::AuthWrite => {
            let (writer_id, secret_bytes) = take::<{ WriterId::BYTE_LEN }>(body)?;
            Ok(ClientFrame::AuthWrite {
                writer_id: WriterId::from_bytes(writer_id),
                link_secret: LinkSecret::from(utf8_tail(secret_bytes)?),
            })
        }
        ClientOp::AppendBatch => {
            let mut records = Vec::new();
            let mut payload_bytes = 0;
            for (start, end) in record_body_ranges(bytes, MAX_APPEND_BATCH_RECORDS)? {
                let record_body = &bytes[start..end];
                let (writer_seq_num, body) = read_u64(record_body)?;
                let (part_raw, body) = read_u32(body)?;
                let (format, data) = read_record_format(body)?;
                validate_record_len(data.len())?;
                payload_bytes += data.len();
                let data_start = end - data.len();
                records.push(AppendRecord {
                    writer_seq_num,
                    part: PartHeader::from_raw(part_raw),
                    format,
                    data: input.slice(data_start..end),
                });
            }
            validate_batch(records.len(), payload_bytes, MAX_APPEND_BATCH_RECORDS)?;
            Ok(ClientFrame::AppendBatch(records))
        }
    }
}

fn decode_server_frame(input: impl FrameInput) -> Result<ServerFrame, FrameCodecError> {
    let input = input.into_bytes();
    let bytes = input.as_ref();
    let Some((&op_byte, body)) = bytes.split_first() else {
        return Err(FrameCodecError::EmptyFrame);
    };

    match ServerOp::try_from(op_byte)? {
        ServerOp::Ready => {
            ensure_empty(op_byte, body)?;
            Ok(ServerFrame::Ready)
        }
        ServerOp::Ack => {
            let (writer_seq_start, body) = read_u64(body)?;
            let (writer_seq_end, body) = read_u64(body)?;
            let (seq_start, body) = read_u64(body)?;
            let (seq_end, body) = read_u64(body)?;
            ensure_empty(op_byte, body)?;
            Ok(ServerFrame::Ack {
                writer_seq_start,
                writer_seq_end,
                seq_start,
                seq_end,
            })
        }
        ServerOp::ReadBatch => {
            let mut records = Vec::new();
            let mut payload_bytes = 0;
            for (start, end) in record_body_ranges(bytes, MAX_READ_BATCH_RECORDS)? {
                let record_body = &bytes[start..end];
                let (seq_num, body) = read_u64(record_body)?;
                let (timestamp_ms, body) = read_u64(body)?;
                let (writer_id, body) = take::<{ WriterId::BYTE_LEN }>(body)?;
                let (writer_seq_num, body) = read_u64(body)?;
                let (part_raw, body) = read_u32(body)?;
                let (format, data) = read_record_format(body)?;
                validate_record_len(data.len())?;
                payload_bytes += data.len();
                let data_start = end - data.len();
                records.push(ReadRecord {
                    seq_num,
                    timestamp_ms,
                    writer_id: WriterId::from_bytes(writer_id),
                    writer_seq_num,
                    part: PartHeader::from_raw(part_raw),
                    format,
                    data: input.slice(data_start..end),
                });
            }
            validate_batch(records.len(), payload_bytes, MAX_READ_BATCH_RECORDS)?;
            Ok(ServerFrame::ReadBatch(records))
        }
        ServerOp::Heartbeat => {
            ensure_empty(op_byte, body)?;
            Ok(ServerFrame::Heartbeat)
        }
        ServerOp::CaughtUp => {
            let (next_seq_num, body) = read_u64(body)?;
            let (last_timestamp_ms, body) = read_u64(body)?;
            ensure_empty(op_byte, body)?;
            Ok(ServerFrame::CaughtUp(ReadCaughtUp {
                next_seq_num,
                last_timestamp_ms,
            }))
        }
        ServerOp::StreamInfo => serde_json::from_slice(body)
            .map(ServerFrame::StreamInfo)
            .map_err(FrameCodecError::InvalidStreamInfo),
    }
}

fn validate_record_len(len: usize) -> Result<(), FrameCodecError> {
    if len > MAX_RECORD_BYTES {
        return Err(FrameCodecError::RecordTooLarge {
            actual: len,
            max: MAX_RECORD_BYTES,
        });
    }
    Ok(())
}

fn batch_encoded_len<'a>(
    records: impl ExactSizeIterator<Item = &'a Bytes>,
    record_header_len: usize,
    maximum_records: usize,
) -> Result<usize, FrameCodecError> {
    let record_count = records.len();
    let mut payload_bytes = 0;
    for data in records {
        validate_record_len(data.len())?;
        payload_bytes += data.len();
    }
    validate_batch(record_count, payload_bytes, maximum_records)?;
    Ok(1 + record_count * (4 + record_header_len) + payload_bytes)
}

fn validate_batch(
    record_count: usize,
    payload_bytes: usize,
    maximum_records: usize,
) -> Result<(), FrameCodecError> {
    if record_count == 0 || record_count > maximum_records {
        return Err(FrameCodecError::InvalidBatchRecordCount {
            actual: record_count,
            max: maximum_records,
        });
    }
    if payload_bytes > MAX_BATCH_PAYLOAD_BYTES {
        return Err(FrameCodecError::BatchPayloadTooLarge {
            actual: payload_bytes,
            max: MAX_BATCH_PAYLOAD_BYTES,
        });
    }
    Ok(())
}

fn record_body_ranges(
    input: &[u8],
    maximum_records: usize,
) -> Result<Vec<(usize, usize)>, FrameCodecError> {
    let mut ranges = Vec::new();
    let mut offset = 1;
    while offset < input.len() {
        if ranges.len() == maximum_records {
            return Err(FrameCodecError::InvalidBatchRecordCount {
                actual: maximum_records + 1,
                max: maximum_records,
            });
        }
        let (length, _) = read_u32(&input[offset..])?;
        offset += 4;
        let length = length as usize;
        let Some(end) = offset.checked_add(length).filter(|end| *end <= input.len()) else {
            return Err(FrameCodecError::InvalidRecordLength);
        };
        if length == 0 {
            return Err(FrameCodecError::InvalidRecordLength);
        }
        ranges.push((offset, end));
        offset = end;
    }
    if ranges.is_empty() {
        return Err(FrameCodecError::InvalidBatchRecordCount {
            actual: 0,
            max: maximum_records,
        });
    }
    Ok(ranges)
}

fn take<const N: usize>(input: &[u8]) -> Result<([u8; N], &[u8]), FrameCodecError> {
    let Some((head, tail)) = input.split_at_checked(N) else {
        return Err(FrameCodecError::TruncatedFrame { op: 0, needed: N });
    };

    let mut bytes = [0_u8; N];
    bytes.copy_from_slice(head);
    Ok((bytes, tail))
}

fn read_u32(input: &[u8]) -> Result<(u32, &[u8]), FrameCodecError> {
    let (bytes, tail) = take::<4>(input)?;
    Ok((u32::from_be_bytes(bytes), tail))
}

fn read_u64(input: &[u8]) -> Result<(u64, &[u8]), FrameCodecError> {
    let (bytes, tail) = take::<8>(input)?;
    Ok((u64::from_be_bytes(bytes), tail))
}

fn read_record_format(input: &[u8]) -> Result<(RecordFormat, &[u8]), FrameCodecError> {
    let (&raw, tail) = input
        .split_first()
        .ok_or(FrameCodecError::TruncatedFrame { op: 0, needed: 1 })?;
    Ok((RecordFormat::try_from(raw)?, tail))
}

fn utf8_tail(input: &[u8]) -> Result<&str, FrameCodecError> {
    std::str::from_utf8(input).map_err(FrameCodecError::InvalidUtf8)
}

fn ensure_empty(op: u8, body: &[u8]) -> Result<(), FrameCodecError> {
    if body.is_empty() {
        Ok(())
    } else {
        Err(FrameCodecError::TrailingBytes {
            op,
            count: body.len(),
        })
    }
}

/// Error returned when encoding or decoding a TSF v1 binary frame.
#[derive(Debug, thiserror::Error)]
pub enum FrameCodecError {
    /// A message did not contain an operation byte.
    #[error("frame cannot be empty")]
    EmptyFrame,
    /// The operation byte is not defined by TSF v1.
    #[error("unknown operation id 0x{0:02x}")]
    UnknownOperation(u8),
    /// A record used an undefined presentation format byte.
    #[error("unknown record format 0x{0:02x}")]
    UnknownRecordFormat(u8),
    /// A batch record length was zero or extended beyond the message.
    #[error("batch record length is invalid")]
    InvalidRecordLength,
    /// A batch had no records or exceeded its direction-specific record limit.
    #[error("batch has {actual} records; expected 1 to {max}")]
    InvalidBatchRecordCount {
        /// Actual record count.
        actual: usize,
        /// Maximum accepted record count.
        max: usize,
    },
    /// Aggregate record payload exceeded [`MAX_BATCH_PAYLOAD_BYTES`].
    #[error("batch payload is {actual} bytes; maximum is {max}")]
    BatchPayloadTooLarge {
        /// Actual aggregate payload length.
        actual: usize,
        /// Maximum accepted aggregate payload length.
        max: usize,
    },
    /// A fixed-width frame ended before all required bytes were present.
    #[error("frame 0x{op:02x} is truncated; needed {needed} more bytes")]
    TruncatedFrame {
        /// Operation byte, or zero when truncation occurred in a shared field decoder.
        op: u8,
        /// Minimum number of additional bytes required.
        needed: usize,
    },
    /// A fixed-width frame contained bytes after its defined body.
    #[error("frame 0x{op:02x} has {count} trailing bytes")]
    TrailingBytes {
        /// Operation byte for the decoded frame.
        op: u8,
        /// Number of undefined trailing bytes.
        count: usize,
    },
    /// A link-secret tail was not valid UTF-8.
    #[error("link secret is not valid UTF-8: {0}")]
    InvalidUtf8(#[source] std::str::Utf8Error),
    /// A stream metadata frame did not contain the expected JSON object.
    #[error("stream info frame is invalid: {0}")]
    InvalidStreamInfo(#[source] serde_json::Error),
    /// A physical record payload exceeded [`MAX_RECORD_BYTES`].
    #[error("record is {actual} bytes; maximum is {max}")]
    RecordTooLarge {
        /// Actual payload length.
        actual: usize,
        /// Maximum accepted payload length.
        max: usize,
    },
    /// A split-record part index exceeded [`PartHeader::MAX_INDEX`].
    #[error("part index {0} is larger than the 31-bit part index range")]
    PartIndexTooLarge(u32),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn part_header_packs_final_bit_and_index() {
        let part = PartHeader::new(42, true).expect("part header");

        assert_eq!(part.index(), 42);
        assert!(part.is_final());
        assert_eq!(PartHeader::from_raw(part.raw()), part);
    }

    #[test]
    fn record_byte_limit_is_enforced_at_the_shared_boundary() {
        let max_data = Bytes::from(vec![0; MAX_RECORD_BYTES]);
        let oversized_data = Bytes::from(vec![0; MAX_RECORD_BYTES + 1]);

        ClientFrame::AppendBatch(vec![AppendRecord {
            writer_seq_num: 0,
            part: PartHeader::unsplit(),
            format: RecordFormat::Bytes,
            data: max_data.clone(),
        }])
        .encode()
        .expect("client max record encodes");
        assert!(matches!(
            ClientFrame::AppendBatch(vec![AppendRecord {
                writer_seq_num: 0,
                part: PartHeader::unsplit(),
                format: RecordFormat::Bytes,
                data: oversized_data.clone(),
            }])
            .encode(),
            Err(FrameCodecError::RecordTooLarge {
                actual,
                max: MAX_RECORD_BYTES
            }) if actual == MAX_RECORD_BYTES + 1
        ));

        ServerFrame::ReadBatch(vec![ReadRecord {
            seq_num: 0,
            timestamp_ms: 0,
            writer_id: WriterId::from_bytes([1; WriterId::BYTE_LEN]),
            writer_seq_num: 0,
            part: PartHeader::unsplit(),
            format: RecordFormat::Bytes,
            data: max_data,
        }])
        .encode()
        .expect("server max record encodes");
        assert!(matches!(
            ServerFrame::ReadBatch(vec![ReadRecord {
                seq_num: 0,
                timestamp_ms: 0,
                writer_id: WriterId::from_bytes([1; WriterId::BYTE_LEN]),
                writer_seq_num: 0,
                part: PartHeader::unsplit(),
                format: RecordFormat::Bytes,
                data: oversized_data,
            }])
            .encode(),
            Err(FrameCodecError::RecordTooLarge {
                actual,
                max: MAX_RECORD_BYTES
            }) if actual == MAX_RECORD_BYTES + 1
        ));

        let oversized_client_frame = encoded_append_data_with_len(MAX_RECORD_BYTES + 1);
        assert!(matches!(
            ClientFrame::decode(&oversized_client_frame),
            Err(FrameCodecError::RecordTooLarge {
                actual,
                max: MAX_RECORD_BYTES
            }) if actual == MAX_RECORD_BYTES + 1
        ));
    }

    #[test]
    fn part_header_rejects_indexes_above_the_31_bit_range() {
        let max = PartHeader::new(PartHeader::MAX_INDEX, true).expect("max part index");

        assert_eq!(max.index(), PartHeader::MAX_INDEX);
        assert!(max.is_final());
        assert!(matches!(
            PartHeader::new(PartHeader::MAX_INDEX + 1, false),
            Err(FrameCodecError::PartIndexTooLarge(value)) if value == PartHeader::MAX_INDEX + 1
        ));
    }

    #[test]
    fn frame_decoders_reject_unknown_empty_and_truncated_frames() {
        assert!(matches!(
            ClientFrame::decode(&[]),
            Err(FrameCodecError::EmptyFrame)
        ));
        assert!(matches!(
            ServerFrame::decode(&[]),
            Err(FrameCodecError::EmptyFrame)
        ));
        assert!(matches!(
            ClientFrame::decode(&[0x7f]),
            Err(FrameCodecError::UnknownOperation(0x7f))
        ));
        assert!(matches!(
            ServerFrame::decode(&[0x7f]),
            Err(FrameCodecError::UnknownOperation(0x7f))
        ));
        assert!(matches!(
            ClientFrame::decode(&[ClientOp::AppendBatch.byte(), 0]),
            Err(FrameCodecError::TruncatedFrame { .. })
        ));
        assert!(matches!(
            ServerFrame::decode(&[ServerOp::Ack.byte(), 0]),
            Err(FrameCodecError::TruncatedFrame { .. })
        ));
    }

    #[test]
    fn stream_info_ignores_unknown_json_fields() {
        let mut encoded = BytesMut::from(&[ServerOp::StreamInfo.byte()][..]);
        encoded.extend_from_slice(br#"{"stream_id":"00000000000000000000000000000000","title":null,"visibility":"private","created_at":"2026-08-13T00:00:00Z","expires_at":"2026-08-23T00:00:00Z","future_field":{"enabled":true}}"#);

        assert_eq!(
            ServerFrame::decode(&encoded).expect("decode stream info"),
            ServerFrame::StreamInfo(ReadStreamInfo {
                stream_id: "00000000000000000000000000000000"
                    .parse()
                    .expect("stream ID"),
                title: None,
                visibility: Visibility::Private,
                created_at: "2026-08-13T00:00:00Z".to_owned(),
                expires_at: "2026-08-23T00:00:00Z".to_owned(),
            })
        );
    }

    #[test]
    fn frame_decoders_reject_unknown_record_formats() {
        let mut client = encoded_append_data_with_len(0).to_vec();
        client[1 + size_of::<u32>() + size_of::<u64>() + size_of::<u32>()] = 0x7f;
        assert!(matches!(
            ClientFrame::decode(&client),
            Err(FrameCodecError::UnknownRecordFormat(0x7f))
        ));

        let writer_id = WriterId::from_bytes([1; WriterId::BYTE_LEN]);
        let mut server = ServerFrame::ReadBatch(vec![ReadRecord {
            seq_num: 0,
            timestamp_ms: 0,
            writer_id,
            writer_seq_num: 0,
            part: PartHeader::unsplit(),
            format: RecordFormat::Bytes,
            data: Bytes::new(),
        }])
        .encode()
        .expect("server record")
        .to_vec();
        let format_offset = 1
            + size_of::<u32>()
            + size_of::<u64>()
            + size_of::<u64>()
            + WriterId::BYTE_LEN
            + size_of::<u64>()
            + size_of::<u32>();
        server[format_offset] = 0x7f;
        assert!(matches!(
            ServerFrame::decode(&server),
            Err(FrameCodecError::UnknownRecordFormat(0x7f))
        ));
    }

    #[test]
    fn frame_decoders_reject_invalid_utf8_and_trailing_bytes() {
        assert!(matches!(
            ClientFrame::decode(&[ClientOp::AuthRead.byte(), 0xff]),
            Err(FrameCodecError::InvalidUtf8(_))
        ));
        assert!(matches!(
            ServerFrame::decode(&[ServerOp::Ready.byte(), 0]),
            Err(FrameCodecError::TrailingBytes { op, count: 1 }) if op == ServerOp::Ready.byte()
        ));
    }

    #[test]
    fn multi_record_batches_round_trip_and_enforce_bounds() {
        let append = ClientFrame::AppendBatch(
            (0..2)
                .map(|writer_seq_num| AppendRecord {
                    writer_seq_num,
                    part: PartHeader::unsplit(),
                    format: RecordFormat::Bytes,
                    data: Bytes::from(vec![writer_seq_num as u8]),
                })
                .collect(),
        );
        let encoded = append.encode().expect("encode append batch");
        let ClientFrame::AppendBatch(decoded) = ClientFrame::decode_bytes(encoded).expect("decode")
        else {
            panic!("expected append batch");
        };
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[1].writer_seq_num, 1);

        assert!(matches!(
            ClientFrame::AppendBatch(Vec::new()).encode(),
            Err(FrameCodecError::InvalidBatchRecordCount { actual: 0, .. })
        ));
        assert!(matches!(
            ClientFrame::decode(&[ClientOp::AppendBatch.byte()]),
            Err(FrameCodecError::InvalidBatchRecordCount { actual: 0, .. })
        ));
        assert!(matches!(
            ClientFrame::decode(&[ClientOp::AppendBatch.byte(), 0, 0, 0, 0]),
            Err(FrameCodecError::InvalidRecordLength)
        ));

        let read_record = || ReadRecord {
            seq_num: 0,
            timestamp_ms: 0,
            writer_id: WriterId::from_bytes([1; WriterId::BYTE_LEN]),
            writer_seq_num: 0,
            part: PartHeader::unsplit(),
            format: RecordFormat::Bytes,
            data: Bytes::new(),
        };
        let maximum_read = ServerFrame::ReadBatch(
            std::iter::repeat_with(read_record)
                .take(MAX_READ_BATCH_RECORDS)
                .collect(),
        );
        let encoded = maximum_read.encode().expect("encode maximum read batch");
        assert_eq!(
            ServerFrame::decode_bytes(encoded).expect("decode maximum read batch"),
            maximum_read
        );

        let append_record = || AppendRecord {
            writer_seq_num: 0,
            part: PartHeader::unsplit(),
            format: RecordFormat::Bytes,
            data: Bytes::new(),
        };
        assert!(matches!(
            ClientFrame::AppendBatch(
                std::iter::repeat_with(append_record)
                    .take(MAX_APPEND_BATCH_RECORDS + 1)
                    .collect()
            )
            .encode(),
            Err(FrameCodecError::InvalidBatchRecordCount {
                max: MAX_APPEND_BATCH_RECORDS,
                ..
            })
        ));
        assert!(matches!(
            ServerFrame::ReadBatch(
                std::iter::repeat_with(read_record)
                    .take(MAX_READ_BATCH_RECORDS + 1)
                    .collect()
            )
            .encode(),
            Err(FrameCodecError::InvalidBatchRecordCount {
                max: MAX_READ_BATCH_RECORDS,
                ..
            })
        ));
    }

    fn encoded_append_data_with_len(data_len: usize) -> Bytes {
        let mut frame = BytesMut::new();
        frame.extend_from_slice(&[ClientOp::AppendBatch.byte()]);
        frame.extend_from_slice(&((13 + data_len) as u32).to_be_bytes());
        frame.extend_from_slice(&0_u64.to_be_bytes());
        frame.extend_from_slice(&PartHeader::unsplit().raw().to_be_bytes());
        frame.extend_from_slice(&[RecordFormat::Bytes.byte()]);
        frame.extend(std::iter::repeat_n(0, data_len));
        frame.freeze()
    }
}
