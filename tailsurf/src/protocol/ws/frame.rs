//! Exact binary codec for `tsf.v1` WebSocket traffic.

use std::borrow::Borrow;

use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};

use crate::{
    ClientWriterId, LinkSecret, WriterId,
    protocol::{
        rest::StreamMetadata,
        ws::{
            MAX_PLAYBACK_RATE_PERMILLE, MAX_READ_SELECTOR_VALUE, MIN_PLAYBACK_RATE_PERMILLE,
            ReadStart,
        },
    },
    stream_url::LINK_SECRET_ENCODED_LENGTH,
};
/// WebSocket subprotocol offered and selected for TSF v1 connections.
pub const TSF_WEBSOCKET_PROTOCOL: &str = "tsf.v1";
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
    OpenRead = 0x01,
    OpenWrite = 0x02,
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
            value if value == Self::OpenRead.byte() => Ok(Self::OpenRead),
            value if value == Self::OpenWrite.byte() => Ok(Self::OpenWrite),
            value if value == Self::AppendBatch.byte() => Ok(Self::AppendBatch),
            other => Err(FrameCodecError::UnknownOperation(other)),
        }
    }
}

const OPEN_READ_LINK_SECRET: u8 = 0x01;
const OPEN_READ_LIMIT: u8 = 0x02;
const OPEN_READ_END_SEQ_NUM: u8 = 0x04;
const OPEN_READ_PLAYBACK_RATE: u8 = 0x08;
const OPEN_READ_SNAPSHOT: u8 = 0x10;
const OPEN_READ_FLAGS: u8 = OPEN_READ_LINK_SECRET
    | OPEN_READ_LIMIT
    | OPEN_READ_END_SEQ_NUM
    | OPEN_READ_PLAYBACK_RATE
    | OPEN_READ_SNAPSHOT;

const OPEN_WRITE_EXPECTED_NEXT_SEQ_NUM: u8 = 0x01;
const OPEN_WRITE_FLAGS: u8 = OPEN_WRITE_EXPECTED_NEXT_SEQ_NUM;

const READ_START_SEQ_NUM: u8 = 0x01;
const READ_START_TIMESTAMP_MS: u8 = 0x02;
const READ_START_TAIL_OFFSET: u8 = 0x03;

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ServerOp {
    Ready = 0x80,
    AppendAck = 0x81,
    ReadBatch = 0x82,
    Heartbeat = 0x83,
    CaughtUp = 0x84,
    StreamMetadata = 0x85,
    SnapshotBoundary = 0x86,
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
            value if value == Self::AppendAck.byte() => Ok(Self::AppendAck),
            value if value == Self::ReadBatch.byte() => Ok(Self::ReadBatch),
            value if value == Self::Heartbeat.byte() => Ok(Self::Heartbeat),
            value if value == Self::CaughtUp.byte() => Ok(Self::CaughtUp),
            value if value == Self::StreamMetadata.byte() => Ok(Self::StreamMetadata),
            value if value == Self::SnapshotBoundary.byte() => Ok(Self::SnapshotBoundary),
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
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
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

/// One physical stream record borrowed from a [`ReadBatch`].
///
/// The payload borrows from the batch's shared backing buffer; use [`ReadRecord::into_owned`] to
/// retain a record beyond the batch's lifetime.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReadRecord<'a> {
    /// Durable absolute sequence number.
    pub seq_num: u64,
    /// Record timestamp as Unix epoch milliseconds.
    pub timestamp_ms: u64,
    /// Stable identity of the writer that wrote this record.
    pub writer_id: WriterId,
    /// Writer-local sequence number reused if this record is retransmitted.
    pub writer_seq_num: u64,
    /// Logical split-part metadata.
    pub part: PartHeader,
    /// Presentation hint for the payload.
    pub format: RecordFormat,
    /// Exact record payload bytes.
    pub data: &'a [u8],
}

impl ReadRecord<'_> {
    /// Copies this record into an independently owned value.
    ///
    /// Named `into_owned` because `ReadRecord` is `Clone`: an inherent `to_owned` would collide
    /// with the blanket [`ToOwned`] impl, whose `Owned = Self` would silently hand generic code
    /// a borrowed view instead of an owned copy.
    pub fn into_owned(self) -> OwnedReadRecord {
        self.into()
    }
}

impl From<ReadRecord<'_>> for OwnedReadRecord {
    fn from(record: ReadRecord<'_>) -> Self {
        Self {
            seq_num: record.seq_num,
            timestamp_ms: record.timestamp_ms,
            writer_id: record.writer_id,
            writer_seq_num: record.writer_seq_num,
            part: record.part,
            format: record.format,
            data: Bytes::copy_from_slice(record.data),
        }
    }
}

/// One physical stream record with independently owned payload bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OwnedReadRecord {
    /// Durable absolute sequence number.
    pub seq_num: u64,
    /// Record timestamp as Unix epoch milliseconds.
    pub timestamp_ms: u64,
    /// Stable identity of the writer that wrote this record.
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

impl OwnedReadRecord {
    /// Borrows this record as a [`ReadRecord`] view.
    pub fn as_record(&self) -> ReadRecord<'_> {
        ReadRecord {
            seq_num: self.seq_num,
            timestamp_ms: self.timestamp_ms,
            writer_id: self.writer_id,
            writer_seq_num: self.writer_seq_num,
            part: self.part,
            format: self.format,
            data: &self.data,
        }
    }
}

/// Fixed record metadata with the payload location inside the batch backing buffer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RecordMeta {
    pub(crate) seq_num: u64,
    pub(crate) timestamp_ms: u64,
    pub(crate) writer_id: WriterId,
    pub(crate) writer_seq_num: u64,
    pub(crate) part: PartHeader,
    pub(crate) format: RecordFormat,
    pub(crate) data_start: u32,
    pub(crate) data_len: u32,
}

/// A bounded batch of physical stream records sharing one backing payload buffer.
///
/// Decoding keeps the received frame as one buffer instead of refcounting one [`Bytes`] handle
/// per record; iterating borrows payloads directly from that buffer.
#[derive(Clone, Debug)]
pub struct ReadBatch {
    payload: Bytes,
    records: Vec<RecordMeta>,
}

impl ReadBatch {
    /// Builds a batch from owned records, validating the same bounds the wire codec enforces
    /// and concatenating payloads into one buffer.
    ///
    /// Rejects empty or over-count batches, oversized records or aggregate payloads, and
    /// non-contiguous sequences; construction is the parse step that upholds the bounded-batch
    /// invariant.
    pub fn try_from_records(records: Vec<OwnedReadRecord>) -> Result<Self, FrameCodecError> {
        // Count first: an over-count input could otherwise wrap the payload sum on 32-bit
        // targets before the aggregate bound rejects it.
        validate_batch_count(records.len(), MAX_READ_BATCH_RECORDS)?;
        validate_sequence_contiguous(records.iter().map(|record| record.seq_num))?;
        let mut payload_bytes = 0_usize;
        for record in &records {
            validate_record_len(record.data.len())?;
            // MAX_READ_BATCH_RECORDS records of MAX_RECORD_BYTES each cannot overflow.
            payload_bytes += record.data.len();
        }
        if payload_bytes > MAX_BATCH_PAYLOAD_BYTES {
            return Err(FrameCodecError::BatchPayloadTooLarge {
                actual: payload_bytes,
                max: MAX_BATCH_PAYLOAD_BYTES,
            });
        }

        let mut payload = BytesMut::with_capacity(payload_bytes);
        let mut metas = Vec::with_capacity(records.len());
        for record in records {
            // payload_bytes is capped at MAX_BATCH_PAYLOAD_BYTES, so these narrows cannot
            // truncate.
            let data_start = payload.len() as u32;
            payload.extend_from_slice(&record.data);
            metas.push(RecordMeta {
                seq_num: record.seq_num,
                timestamp_ms: record.timestamp_ms,
                writer_id: record.writer_id,
                writer_seq_num: record.writer_seq_num,
                part: record.part,
                format: record.format,
                data_start,
                data_len: record.data.len() as u32,
            });
        }
        Ok(Self::from_parts(payload.freeze(), metas))
    }

    pub(crate) fn from_parts(payload: Bytes, records: Vec<RecordMeta>) -> Self {
        debug_assert!(!records.is_empty());
        Self { payload, records }
    }

    /// Returns the number of records in this non-empty batch.
    pub fn record_count(&self) -> usize {
        self.records.len()
    }

    /// Returns the first record.
    pub fn first(&self) -> ReadRecord<'_> {
        self.record(&self.records[0])
    }

    /// Returns the last record.
    pub fn last(&self) -> ReadRecord<'_> {
        self.record(&self.records[self.records.len() - 1])
    }

    /// Iterates the records, borrowing payloads from the shared buffer.
    pub fn iter(&self) -> Iter<'_> {
        Iter {
            batch: self,
            records: self.records.iter(),
        }
    }

    fn record(&self, meta: &RecordMeta) -> ReadRecord<'_> {
        let data_start = meta.data_start as usize;
        ReadRecord {
            seq_num: meta.seq_num,
            timestamp_ms: meta.timestamp_ms,
            writer_id: meta.writer_id,
            writer_seq_num: meta.writer_seq_num,
            part: meta.part,
            format: meta.format,
            data: &self.payload[data_start..data_start + meta.data_len as usize],
        }
    }
}

/// Iterates the records of a [`ReadBatch`], borrowing payloads from the shared buffer.
///
/// Returned by [`ReadBatch::iter`]; `&ReadBatch` also iterates directly via its
/// [`IntoIterator`] impl.
#[derive(Clone, Debug)]
pub struct Iter<'a> {
    batch: &'a ReadBatch,
    records: std::slice::Iter<'a, RecordMeta>,
}

impl<'a> Iterator for Iter<'a> {
    type Item = ReadRecord<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.records.next().map(|meta| self.batch.record(meta))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.records.size_hint()
    }
}

impl DoubleEndedIterator for Iter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.records.next_back().map(|meta| self.batch.record(meta))
    }
}

impl ExactSizeIterator for Iter<'_> {}

impl std::iter::FusedIterator for Iter<'_> {}

impl<'a> IntoIterator for &'a ReadBatch {
    type Item = ReadRecord<'a>;
    type IntoIter = Iter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl PartialEq for ReadBatch {
    // Decode backs the batch with the whole frame while try_from_records stores concatenated
    // payloads, so equality compares records rather than backing buffers.
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}

impl Eq for ReadBatch {}

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
pub struct CaughtUpPosition {
    /// Sequence number assigned to the next appended record.
    pub next_seq_num: u64,
    /// Timestamp of the last record. This is zero when `next_seq_num` is zero.
    pub last_timestamp_ms: u64,
}

/// Fixed exclusive ending position captured when a snapshot read opens.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnapshotBoundary {
    /// Exclusive end of the fixed snapshot.
    pub end_seq_num: u64,
    /// Timestamp of the last record. This is zero when `end_seq_num` is zero.
    pub last_timestamp_ms: u64,
}

/// Frame sent from a reader or writer to the service.
#[derive(Clone, Debug)]
pub enum ClientFrame {
    /// Opens a read connection with its complete request and optional authentication.
    OpenRead {
        /// Secret from a read-capable stream link for a private stream.
        link_secret: Option<LinkSecret>,
        /// Initial absolute, timestamp, or tail-relative read position.
        start: ReadStart,
        /// Maximum number of physical records to deliver.
        limit: Option<u64>,
        /// Exclusive ending sequence number.
        end_seq_num: Option<u64>,
        /// Timestamp playback rate in thousandths.
        playback_rate_permille: Option<u64>,
        /// Whether the server captures a fixed ending position.
        snapshot: bool,
    },
    /// Opens a write connection and establishes authorization and client writer identity.
    OpenWrite {
        /// Stable identity reused across reconnects.
        client_writer_id: ClientWriterId,
        /// Secret from a write-capable stream link.
        link_secret: LinkSecret,
        /// Initial stream sequence precondition for this writer session.
        expected_next_seq_num: Option<u64>,
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
    AppendAck {
        /// First acknowledged writer-local sequence number.
        writer_start_seq_num: u64,
        /// Exclusive writer-local sequence after the acknowledged range.
        writer_end_seq_num: u64,
        /// Durable sequence number assigned to the first acknowledged record.
        start_seq_num: u64,
        /// Exclusive durable sequence after the acknowledged range.
        end_seq_num: u64,
    },
    /// Delivers a bounded batch of physical stream records.
    ReadBatch(ReadBatch),
    /// Keeps an otherwise idle unbounded read connection active.
    Heartbeat,
    /// Confirms that every record preceding the captured position was delivered.
    CaughtUp(CaughtUpPosition),
    /// Supplies stream metadata from the read authorization result.
    StreamMetadata(StreamMetadata),
    /// Supplies the fixed exclusive end for a snapshot read.
    SnapshotBoundary(SnapshotBoundary),
}

impl ClientFrame {
    const APPEND_BODY_HEADER_LEN: usize = 8 + 4 + 1;
    const OPEN_READ_FIXED_LEN: usize = 1 + 1 + 1 + 8;

    /// Returns the exact wire length of this frame, validating the payload size for records.
    fn encoded_len(&self) -> Result<usize, FrameCodecError> {
        match self {
            Self::OpenRead {
                link_secret,
                start,
                limit,
                end_seq_num,
                playback_rate_permille,
                snapshot,
            } => {
                validate_open_read(*start, *end_seq_num, *playback_rate_permille, *snapshot)?;
                Ok(Self::OPEN_READ_FIXED_LEN
                    + limit.map_or(0, |_| 8)
                    + end_seq_num.map_or(0, |_| 8)
                    + playback_rate_permille.map_or(0, |_| 8)
                    + link_secret
                        .as_ref()
                        .map_or(0, |_| LINK_SECRET_ENCODED_LENGTH))
            }
            Self::OpenWrite {
                expected_next_seq_num,
                ..
            } => {
                if let Some(value) = expected_next_seq_num {
                    validate_expected_next_seq_num(*value)?;
                }
                Ok(2 + ClientWriterId::BYTE_LEN
                    + expected_next_seq_num.map_or(0, |_| 8)
                    + LINK_SECRET_ENCODED_LENGTH)
            }
            Self::AppendBatch(records) => Self::append_batch_encoded_len(records),
        }
    }

    fn append_batch_encoded_len<R: Borrow<AppendRecord>>(
        records: &[R],
    ) -> Result<usize, FrameCodecError> {
        for record in records {
            validate_writer_seq_num(record.borrow().writer_seq_num)?;
        }
        batch_encoded_len(
            records.iter().map(|record| record.borrow().data.len()),
            Self::APPEND_BODY_HEADER_LEN,
            MAX_APPEND_BATCH_RECORDS,
        )
    }

    fn put_append_batch<R: Borrow<AppendRecord>>(output: &mut BytesMut, records: &[R]) {
        output.put_u8(ClientOp::AppendBatch.byte());
        for record in records {
            let record = record.borrow();
            output.put_u32((Self::APPEND_BODY_HEADER_LEN + record.data.len()) as u32);
            output.put_u64(record.writer_seq_num);
            output.put_u32(record.part.raw());
            output.put_u8(record.format.byte());
            output.put_slice(&record.data);
        }
    }

    /// Writes this frame into `output`, which must have at least [`Self::encoded_len`] capacity.
    fn encode_into(&self, output: &mut BytesMut) {
        match self {
            Self::OpenRead {
                link_secret,
                start,
                limit,
                end_seq_num,
                playback_rate_permille,
                snapshot,
            } => {
                output.put_u8(ClientOp::OpenRead.byte());
                let flags = link_secret.as_ref().map_or(0, |_| OPEN_READ_LINK_SECRET)
                    | limit.map_or(0, |_| OPEN_READ_LIMIT)
                    | end_seq_num.map_or(0, |_| OPEN_READ_END_SEQ_NUM)
                    | playback_rate_permille.map_or(0, |_| OPEN_READ_PLAYBACK_RATE)
                    | if *snapshot { OPEN_READ_SNAPSHOT } else { 0 };
                output.put_u8(flags);
                let (tag, value) = read_start_wire(*start);
                output.put_u8(tag);
                output.put_u64(value);
                if let Some(value) = limit {
                    output.put_u64(*value);
                }
                if let Some(value) = end_seq_num {
                    output.put_u64(*value);
                }
                if let Some(value) = playback_rate_permille {
                    output.put_u64(*value);
                }
                if let Some(secret) = link_secret {
                    output.put_slice(secret.expose_secret().as_bytes());
                }
            }
            Self::OpenWrite {
                client_writer_id,
                link_secret,
                expected_next_seq_num,
            } => {
                output.put_u8(ClientOp::OpenWrite.byte());
                output
                    .put_u8(expected_next_seq_num.map_or(0, |_| OPEN_WRITE_EXPECTED_NEXT_SEQ_NUM));
                output.put_slice(client_writer_id.as_bytes());
                if let Some(value) = expected_next_seq_num {
                    output.put_u64(*value);
                }
                output.put_slice(link_secret.expose_secret().as_bytes());
            }
            Self::AppendBatch(records) => Self::put_append_batch(output, records),
        }
    }

    /// Encodes one append batch borrowed from retained records into a complete WebSocket message.
    ///
    /// Uses the same wire encoding as [`ClientFrame::AppendBatch`] without taking ownership.
    pub(crate) fn encode_append_batch<R: Borrow<AppendRecord>>(
        records: &[R],
    ) -> Result<Bytes, FrameCodecError> {
        let mut output = BytesMut::with_capacity(Self::append_batch_encoded_len(records)?);
        Self::put_append_batch(&mut output, records);
        Ok(output.freeze())
    }

    /// Encodes one client frame into a complete WebSocket binary message.
    pub fn encode(&self) -> Result<Bytes, FrameCodecError> {
        let mut output = BytesMut::with_capacity(self.encoded_len()?);
        self.encode_into(&mut output);
        Ok(output.freeze())
    }

    /// Decodes one client frame, copying any record payload into owned bytes.
    pub fn decode(input: &[u8]) -> Result<Self, FrameCodecError> {
        decode_client_frame(Bytes::copy_from_slice(input))
    }

    /// Decodes one client frame while retaining a zero-copy slice for record payload data.
    pub fn decode_bytes(input: Bytes) -> Result<Self, FrameCodecError> {
        decode_client_frame(input)
    }
}

impl ServerFrame {
    const READ_BODY_HEADER_LEN: usize = 8 + 8 + WriterId::BYTE_LEN + 8 + 4 + 1;
    /// Largest encoded size among the fixed-width frames, set by [`ServerFrame::AppendAck`].
    const MAX_FIXED_FRAME_LEN: usize = 1 + 4 * 8;

    /// Returns the exact wire length of this frame, validating the payload size for records.
    ///
    /// Covers only fixed-size and batch frames; [`ServerFrame::StreamMetadata`] carries
    /// variable-length JSON and is serialized directly by [`ServerFrame::encode`]. ReadBatch
    /// validity is a construction invariant ([`ReadBatch::try_from_records`], decode), so only
    /// the length is computed here.
    fn encoded_len(&self) -> Result<usize, FrameCodecError> {
        match self {
            Self::ReadBatch(batch) => Ok(1
                + batch.records.len() * (4 + Self::READ_BODY_HEADER_LEN)
                + batch
                    .records
                    .iter()
                    .map(|record| record.data_len as usize)
                    .sum::<usize>()),
            Self::StreamMetadata(_) => {
                unreachable!("StreamMetadata is serialized directly by encode()")
            }
            _ => Ok(Self::MAX_FIXED_FRAME_LEN),
        }
    }

    /// Writes this frame into `output`, which must have at least [`Self::encoded_len`] capacity.
    fn encode_into(&self, output: &mut BytesMut) {
        match self {
            Self::Ready => output.put_u8(ServerOp::Ready.byte()),
            Self::AppendAck {
                writer_start_seq_num,
                writer_end_seq_num,
                start_seq_num,
                end_seq_num,
            } => {
                output.put_u8(ServerOp::AppendAck.byte());
                output.put_u64(*writer_start_seq_num);
                output.put_u64(*writer_end_seq_num);
                output.put_u64(*start_seq_num);
                output.put_u64(*end_seq_num);
            }
            Self::ReadBatch(batch) => {
                output.put_u8(ServerOp::ReadBatch.byte());
                for record in &batch.records {
                    let data_start = record.data_start as usize;
                    let data_len = record.data_len as usize;
                    output.put_u32((Self::READ_BODY_HEADER_LEN + data_len) as u32);
                    output.put_u64(record.seq_num);
                    output.put_u64(record.timestamp_ms);
                    output.put_slice(record.writer_id.as_bytes());
                    output.put_u64(record.writer_seq_num);
                    output.put_u32(record.part.raw());
                    output.put_u8(record.format.byte());
                    output.put_slice(&batch.payload[data_start..data_start + data_len]);
                }
            }
            Self::Heartbeat => output.put_u8(ServerOp::Heartbeat.byte()),
            Self::CaughtUp(caught_up) => {
                output.put_u8(ServerOp::CaughtUp.byte());
                output.put_u64(caught_up.next_seq_num);
                output.put_u64(caught_up.last_timestamp_ms);
            }
            Self::StreamMetadata(_) => {
                unreachable!("StreamMetadata is serialized directly by encode()")
            }
            Self::SnapshotBoundary(boundary) => {
                output.put_u8(ServerOp::SnapshotBoundary.byte());
                output.put_u64(boundary.end_seq_num);
                output.put_u64(boundary.last_timestamp_ms);
            }
        }
    }

    /// Encodes one server frame into a complete WebSocket binary message.
    pub fn encode(&self) -> Result<Bytes, FrameCodecError> {
        // StreamMetadata carries variable-length JSON; serialize straight into the output.
        if let Self::StreamMetadata(stream) = self {
            let mut output = BytesMut::new();
            output.put_u8(ServerOp::StreamMetadata.byte());
            serde_json::to_writer((&mut output).writer(), stream)
                .map_err(FrameCodecError::InvalidStreamMetadata)?;
            return Ok(output.freeze());
        }
        let mut output = BytesMut::with_capacity(self.encoded_len()?);
        self.encode_into(&mut output);
        Ok(output.freeze())
    }

    /// Decodes one server frame, copying any record payload into owned bytes.
    pub fn decode(input: &[u8]) -> Result<Self, FrameCodecError> {
        decode_server_frame(Bytes::copy_from_slice(input))
    }

    /// Decodes one server frame while retaining a zero-copy slice for record payload data.
    pub fn decode_bytes(input: Bytes) -> Result<Self, FrameCodecError> {
        decode_server_frame(input)
    }
}

fn decode_client_frame(input: Bytes) -> Result<ClientFrame, FrameCodecError> {
    let bytes = input.as_ref();
    let Some((&op_byte, body)) = bytes.split_first() else {
        return Err(FrameCodecError::EmptyFrame);
    };

    match ClientOp::try_from(op_byte)? {
        ClientOp::OpenRead => decode_open_read(op_byte, body),
        ClientOp::OpenWrite => {
            let (&flags, body) = body.split_first().ok_or(FrameCodecError::TruncatedFrame {
                op: op_byte,
                needed: 1,
            })?;
            if flags & !OPEN_WRITE_FLAGS != 0 {
                return Err(FrameCodecError::UnknownOpenWriteFlags(
                    flags & !OPEN_WRITE_FLAGS,
                ));
            }
            let (client_writer_id, body) = take::<{ ClientWriterId::BYTE_LEN }>(body)?;
            let (expected_next_seq_num, secret_bytes) =
                if flags & OPEN_WRITE_EXPECTED_NEXT_SEQ_NUM == 0 {
                    (None, body)
                } else {
                    let (value, body) = read_u64(body)?;
                    validate_expected_next_seq_num(value)?;
                    (Some(value), body)
                };
            if secret_bytes.len() != LINK_SECRET_ENCODED_LENGTH {
                return Err(FrameCodecError::InvalidLinkSecret);
            }
            let link_secret = parse_link_secret(utf8_tail(secret_bytes)?)?;
            Ok(ClientFrame::OpenWrite {
                client_writer_id: ClientWriterId::from_bytes(client_writer_id),
                link_secret,
                expected_next_seq_num,
            })
        }
        ClientOp::AppendBatch => {
            let mut records = Vec::new();
            let mut payload_bytes = 0;
            for range in record_body_ranges(bytes, MAX_APPEND_BATCH_RECORDS) {
                let (start, end) = range?;
                let record_body = &bytes[start..end];
                let (writer_seq_num, body) = read_u64(record_body)?;
                validate_writer_seq_num(writer_seq_num)?;
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

fn decode_open_read(op: u8, body: &[u8]) -> Result<ClientFrame, FrameCodecError> {
    let (&flags, body) = body
        .split_first()
        .ok_or(FrameCodecError::TruncatedFrame { op, needed: 1 })?;
    if flags & !OPEN_READ_FLAGS != 0 {
        return Err(FrameCodecError::UnknownOpenReadFlags(
            flags & !OPEN_READ_FLAGS,
        ));
    }
    let (&start_tag, body) = body
        .split_first()
        .ok_or(FrameCodecError::TruncatedFrame { op, needed: 1 })?;
    let (start_value, mut body) = read_u64(body)?;
    let start = read_start_from_wire(start_tag, start_value)?;
    let limit = read_flagged_u64(flags, OPEN_READ_LIMIT, &mut body)?;
    let end_seq_num = read_flagged_u64(flags, OPEN_READ_END_SEQ_NUM, &mut body)?;
    let playback_rate_permille = read_flagged_u64(flags, OPEN_READ_PLAYBACK_RATE, &mut body)?;
    let snapshot = flags & OPEN_READ_SNAPSHOT != 0;
    let link_secret = if flags & OPEN_READ_LINK_SECRET == 0 {
        ensure_empty(op, body)?;
        None
    } else {
        let Some((secret, trailing)) = body.split_at_checked(LINK_SECRET_ENCODED_LENGTH) else {
            return Err(FrameCodecError::TruncatedFrame {
                op,
                needed: LINK_SECRET_ENCODED_LENGTH.saturating_sub(body.len()),
            });
        };
        ensure_empty(op, trailing)?;
        Some(parse_link_secret(utf8_tail(secret)?)?)
    };
    validate_open_read(start, end_seq_num, playback_rate_permille, snapshot)?;
    Ok(ClientFrame::OpenRead {
        link_secret,
        start,
        limit,
        end_seq_num,
        playback_rate_permille,
        snapshot,
    })
}

fn read_flagged_u64(flags: u8, bit: u8, body: &mut &[u8]) -> Result<Option<u64>, FrameCodecError> {
    if flags & bit == 0 {
        return Ok(None);
    }
    let (value, tail) = read_u64(body)?;
    *body = tail;
    Ok(Some(value))
}

pub(crate) fn validate_open_read(
    start: ReadStart,
    end_seq_num: Option<u64>,
    playback_rate_permille: Option<u64>,
    snapshot: bool,
) -> Result<(), FrameCodecError> {
    let (_, selector) = read_start_wire(start);
    if selector > MAX_READ_SELECTOR_VALUE {
        return Err(FrameCodecError::ReadSelectorOutOfRange(selector));
    }
    if snapshot && end_seq_num.is_some() {
        return Err(FrameCodecError::SnapshotWithEnd);
    }
    if let Some(rate) = playback_rate_permille {
        if !(MIN_PLAYBACK_RATE_PERMILLE..=MAX_PLAYBACK_RATE_PERMILLE).contains(&rate) {
            return Err(FrameCodecError::PlaybackRateOutOfRange(rate));
        }
        if end_seq_num.is_none() && !snapshot {
            return Err(FrameCodecError::PlaybackRequiresEnd);
        }
    }
    Ok(())
}

const fn read_start_wire(start: ReadStart) -> (u8, u64) {
    match start {
        ReadStart::SeqNum(value) => (READ_START_SEQ_NUM, value),
        ReadStart::TimestampMs(value) => (READ_START_TIMESTAMP_MS, value),
        ReadStart::TailOffset(value) => (READ_START_TAIL_OFFSET, value),
    }
}

fn read_start_from_wire(tag: u8, value: u64) -> Result<ReadStart, FrameCodecError> {
    match tag {
        READ_START_SEQ_NUM => Ok(ReadStart::SeqNum(value)),
        READ_START_TIMESTAMP_MS => Ok(ReadStart::TimestampMs(value)),
        READ_START_TAIL_OFFSET => Ok(ReadStart::TailOffset(value)),
        other => Err(FrameCodecError::UnknownReadStartTag(other)),
    }
}

fn decode_server_frame(input: Bytes) -> Result<ServerFrame, FrameCodecError> {
    let bytes = input.as_ref();
    let Some((&op_byte, body)) = bytes.split_first() else {
        return Err(FrameCodecError::EmptyFrame);
    };

    match ServerOp::try_from(op_byte)? {
        ServerOp::Ready => {
            ensure_empty(op_byte, body)?;
            Ok(ServerFrame::Ready)
        }
        ServerOp::AppendAck => {
            let (writer_start_seq_num, body) = read_u64(body)?;
            let (writer_end_seq_num, body) = read_u64(body)?;
            let (start_seq_num, body) = read_u64(body)?;
            let (end_seq_num, body) = read_u64(body)?;
            ensure_empty(op_byte, body)?;
            Ok(ServerFrame::AppendAck {
                writer_start_seq_num,
                writer_end_seq_num,
                start_seq_num,
                end_seq_num,
            })
        }
        ServerOp::ReadBatch => {
            // Every record costs a 4-byte length prefix plus the fixed header on the wire, so
            // the frame length bounds the record count.
            let max_records =
                (body.len() / (4 + ServerFrame::READ_BODY_HEADER_LEN)).min(MAX_READ_BATCH_RECORDS);
            let mut records = Vec::with_capacity(max_records);
            let mut payload_bytes = 0;
            for range in record_body_ranges(bytes, MAX_READ_BATCH_RECORDS) {
                let (start, end) = range?;
                let record_body = &bytes[start..end];
                let (seq_num, body) = read_u64(record_body)?;
                let (timestamp_ms, body) = read_u64(body)?;
                let (writer_id, body) = take::<{ WriterId::BYTE_LEN }>(body)?;
                let (writer_seq_num, body) = read_u64(body)?;
                let (part_raw, body) = read_u32(body)?;
                let (format, data) = read_record_format(body)?;
                validate_record_len(data.len())?;
                payload_bytes += data.len();
                records.push(RecordMeta {
                    seq_num,
                    timestamp_ms,
                    writer_id: WriterId::from_bytes(writer_id),
                    writer_seq_num,
                    part: PartHeader::from_raw(part_raw),
                    format,
                    data_start: (end - data.len()) as u32,
                    data_len: data.len() as u32,
                });
            }
            validate_batch(records.len(), payload_bytes, MAX_READ_BATCH_RECORDS)?;
            validate_sequence_contiguous(records.iter().map(|record| record.seq_num))?;
            Ok(ServerFrame::ReadBatch(ReadBatch::from_parts(
                input, records,
            )))
        }
        ServerOp::Heartbeat => {
            ensure_empty(op_byte, body)?;
            Ok(ServerFrame::Heartbeat)
        }
        ServerOp::CaughtUp => decode_position(op_byte, body, |next_seq_num, last_timestamp_ms| {
            ServerFrame::CaughtUp(CaughtUpPosition {
                next_seq_num,
                last_timestamp_ms,
            })
        }),
        ServerOp::StreamMetadata => serde_json::from_slice(body)
            .map(ServerFrame::StreamMetadata)
            .map_err(FrameCodecError::InvalidStreamMetadata),
        ServerOp::SnapshotBoundary => {
            decode_position(op_byte, body, |end_seq_num, last_timestamp_ms| {
                ServerFrame::SnapshotBoundary(SnapshotBoundary {
                    end_seq_num,
                    last_timestamp_ms,
                })
            })
        }
    }
}

fn decode_position(
    op: u8,
    body: &[u8],
    frame: impl FnOnce(u64, u64) -> ServerFrame,
) -> Result<ServerFrame, FrameCodecError> {
    let (seq_num, body) = read_u64(body)?;
    let (last_timestamp_ms, body) = read_u64(body)?;
    ensure_empty(op, body)?;
    Ok(frame(seq_num, last_timestamp_ms))
}

fn parse_link_secret(text: &str) -> Result<LinkSecret, FrameCodecError> {
    text.parse().map_err(|_| FrameCodecError::InvalidLinkSecret)
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

fn validate_writer_seq_num(value: u64) -> Result<(), FrameCodecError> {
    if value == u64::MAX {
        Err(FrameCodecError::WriterSequenceExhausted)
    } else {
        Ok(())
    }
}

fn validate_expected_next_seq_num(value: u64) -> Result<(), FrameCodecError> {
    if value > MAX_READ_SELECTOR_VALUE {
        Err(FrameCodecError::ExpectedNextSeqNumOutOfRange(value))
    } else {
        Ok(())
    }
}

fn batch_encoded_len(
    record_lens: impl ExactSizeIterator<Item = usize>,
    record_header_len: usize,
    maximum_records: usize,
) -> Result<usize, FrameCodecError> {
    let record_count = record_lens.len();
    let mut payload_bytes = 0;
    for len in record_lens {
        validate_record_len(len)?;
        payload_bytes += len;
    }
    validate_batch(record_count, payload_bytes, maximum_records)?;
    Ok(1 + record_count * (4 + record_header_len) + payload_bytes)
}

fn validate_batch_count(
    record_count: usize,
    maximum_records: usize,
) -> Result<(), FrameCodecError> {
    if record_count == 0 || record_count > maximum_records {
        return Err(FrameCodecError::InvalidBatchRecordCount {
            actual: record_count,
            max: maximum_records,
        });
    }
    Ok(())
}

fn validate_batch(
    record_count: usize,
    payload_bytes: usize,
    maximum_records: usize,
) -> Result<(), FrameCodecError> {
    validate_batch_count(record_count, maximum_records)?;
    if payload_bytes > MAX_BATCH_PAYLOAD_BYTES {
        return Err(FrameCodecError::BatchPayloadTooLarge {
            actual: payload_bytes,
            max: MAX_BATCH_PAYLOAD_BYTES,
        });
    }
    Ok(())
}

fn validate_sequence_contiguous(
    seq_nums: impl IntoIterator<Item = u64>,
) -> Result<(), FrameCodecError> {
    let mut previous = None;
    for seq_num in seq_nums {
        if previous.is_some_and(|previous: u64| previous.checked_add(1) != Some(seq_num)) {
            return Err(FrameCodecError::NonContiguousReadBatch);
        }
        previous = Some(seq_num);
    }
    Ok(())
}

/// Lazily walks length-prefixed record bodies so decoding parses each record in one pass.
fn record_body_ranges(input: &[u8], maximum_records: usize) -> RecordBodyRanges<'_> {
    RecordBodyRanges {
        input,
        offset: 1,
        count: 0,
        maximum_records,
    }
}

struct RecordBodyRanges<'a> {
    input: &'a [u8],
    offset: usize,
    count: usize,
    maximum_records: usize,
}

impl Iterator for RecordBodyRanges<'_> {
    type Item = Result<(usize, usize), FrameCodecError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= self.input.len() {
            return None;
        }
        let result = self.advance();
        if result.is_err() {
            // Fuse: callers stop on the first error; never yield twice.
            self.offset = self.input.len();
        }
        Some(result)
    }
}

impl RecordBodyRanges<'_> {
    fn advance(&mut self) -> Result<(usize, usize), FrameCodecError> {
        if self.count == self.maximum_records {
            return Err(FrameCodecError::InvalidBatchRecordCount {
                actual: self.maximum_records + 1,
                max: self.maximum_records,
            });
        }
        let (length, _) = read_u32(&self.input[self.offset..])?;
        self.offset += 4;
        let length = length as usize;
        let Some(end) = self
            .offset
            .checked_add(length)
            .filter(|end| *end <= self.input.len())
        else {
            return Err(FrameCodecError::InvalidRecordLength);
        };
        if length == 0 {
            return Err(FrameCodecError::InvalidRecordLength);
        }
        let start = self.offset;
        self.offset = end;
        self.count += 1;
        Ok((start, end))
    }
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
    /// An `OpenRead` flag bit is not defined by TSF v1.
    #[error("OpenRead has unknown flags 0x{0:02x}")]
    UnknownOpenReadFlags(u8),
    /// An `OpenWrite` flag bit is not defined by TSF v1.
    #[error("OpenWrite has unknown flags 0x{0:02x}")]
    UnknownOpenWriteFlags(u8),
    /// An `OpenRead` selector tag is not defined by TSF v1.
    #[error("OpenRead has unknown start tag 0x{0:02x}")]
    UnknownReadStartTag(u8),
    /// A selector exceeds the exact integer range of the current data adapter.
    #[error("read selector {0} exceeds {MAX_READ_SELECTOR_VALUE}")]
    ReadSelectorOutOfRange(u64),
    /// A write precondition exceeds the exact integer range of the current data adapter.
    #[error("expected next sequence {0} exceeds {MAX_READ_SELECTOR_VALUE}")]
    ExpectedNextSeqNumOutOfRange(u64),
    /// Timestamp playback was outside the accepted rate range.
    #[error(
        "playback rate {0} must be between {MIN_PLAYBACK_RATE_PERMILLE} and {MAX_PLAYBACK_RATE_PERMILLE} permille"
    )]
    PlaybackRateOutOfRange(u64),
    /// A snapshot request also supplied an explicit ending sequence.
    #[error("snapshot and end_seq_num are mutually exclusive")]
    SnapshotWithEnd,
    /// Timestamp playback did not include a fixed ending sequence.
    #[error("playback rate requires an exclusive end_seq_num sequence or snapshot")]
    PlaybackRequiresEnd,
    /// An opening credential is not canonical 256-bit unpadded base64url.
    #[error("opening link secret must be canonical 43-character unpadded base64url")]
    InvalidLinkSecret,
    /// A writer sequence left no representable exclusive acknowledgement boundary.
    #[error("writer sequence must leave room for an exclusive acknowledgement boundary")]
    WriterSequenceExhausted,
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
    /// A read batch skipped or repeated a physical sequence number.
    #[error("ReadBatch sequence numbers must be contiguous")]
    NonContiguousReadBatch,
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
    #[error("stream metadata frame is invalid: {0}")]
    InvalidStreamMetadata(#[source] serde_json::Error),
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
    use crate::protocol::rest::Visibility;

    fn owned_read_record(seq_num: u64, data: Bytes) -> OwnedReadRecord {
        OwnedReadRecord {
            seq_num,
            timestamp_ms: 0,
            writer_id: WriterId::from_bytes([1; WriterId::BYTE_LEN]),
            writer_seq_num: 0,
            part: PartHeader::unsplit(),
            format: RecordFormat::Bytes,
            data,
        }
    }

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

        ServerFrame::ReadBatch(
            ReadBatch::try_from_records(vec![owned_read_record(0, max_data)])
                .expect("max record batch"),
        )
        .encode()
        .expect("server max record encodes");

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
            ServerFrame::decode(&[ServerOp::AppendAck.byte(), 0]),
            Err(FrameCodecError::TruncatedFrame { .. })
        ));
    }

    #[test]
    fn stream_metadata_ignores_unknown_json_fields() {
        let mut encoded = BytesMut::from(&[ServerOp::StreamMetadata.byte()][..]);
        encoded.extend_from_slice(br#"{"stream_id":"00000000000000000000000000000000","title":null,"visibility":"private","created_at":"2026-08-13T00:00:00Z","expires_at":"2026-08-23T00:00:00Z","future_field":{"enabled":true}}"#);

        assert_eq!(
            ServerFrame::decode(&encoded).expect("decode stream metadata"),
            ServerFrame::StreamMetadata(StreamMetadata {
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
    fn stream_metadata_tolerates_absent_title_and_requires_valid_timestamps() {
        let mut missing_title = BytesMut::from(&[ServerOp::StreamMetadata.byte()][..]);
        missing_title.extend_from_slice(br#"{"stream_id":"00000000000000000000000000000000","visibility":"private","created_at":"2026-08-13T00:00:00Z","expires_at":"2026-08-23T00:00:00Z"}"#);
        assert!(matches!(
            ServerFrame::decode(&missing_title),
            Ok(ServerFrame::StreamMetadata(StreamMetadata {
                title: None,
                ..
            }))
        ));

        let mut invalid_time = BytesMut::from(&[ServerOp::StreamMetadata.byte()][..]);
        invalid_time.extend_from_slice(br#"{"stream_id":"00000000000000000000000000000000","title":null,"visibility":"private","created_at":"not-a-time","expires_at":"2026-08-23T00:00:00Z"}"#);
        assert!(matches!(
            ServerFrame::decode(&invalid_time),
            Err(FrameCodecError::InvalidStreamMetadata(_))
        ));
    }

    #[test]
    fn frame_decoders_reject_unknown_record_formats() {
        let mut client = encoded_append_data_with_len(0).to_vec();
        client[1 + size_of::<u32>() + size_of::<u64>() + size_of::<u32>()] = 0x7f;
        assert!(matches!(
            ClientFrame::decode(&client),
            Err(FrameCodecError::UnknownRecordFormat(0x7f))
        ));

        let mut server = ServerFrame::ReadBatch(
            ReadBatch::try_from_records(vec![owned_read_record(0, Bytes::new())])
                .expect("valid batch"),
        )
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
        let mut invalid_utf8 = vec![
            ClientOp::OpenRead.byte(),
            OPEN_READ_LINK_SECRET,
            READ_START_SEQ_NUM,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
        ];
        invalid_utf8.extend_from_slice(&[b'A'; LINK_SECRET_ENCODED_LENGTH]);
        *invalid_utf8.last_mut().expect("secret byte") = 0xff;
        assert!(matches!(
            ClientFrame::decode(&invalid_utf8),
            Err(FrameCodecError::InvalidUtf8(_))
        ));
        assert!(matches!(
            ServerFrame::decode(&[ServerOp::Ready.byte(), 0]),
            Err(FrameCodecError::TrailingBytes { op, count: 1 }) if op == ServerOp::Ready.byte()
        ));
        let mut malformed_open_write = vec![ClientOp::OpenWrite.byte()];
        malformed_open_write.extend_from_slice(&[0; ClientWriterId::BYTE_LEN]);
        malformed_open_write.extend_from_slice("B".repeat(LINK_SECRET_ENCODED_LENGTH).as_bytes());
        assert!(matches!(
            ClientFrame::decode(&malformed_open_write),
            Err(FrameCodecError::InvalidLinkSecret)
        ));
        let mut missing_timestamp = vec![ServerOp::CaughtUp.byte()];
        missing_timestamp.extend_from_slice(&[0; 8]);
        assert!(matches!(
            ServerFrame::decode(&missing_timestamp),
            Err(FrameCodecError::TruncatedFrame { .. })
        ));
        let mut trailing_position = vec![ServerOp::CaughtUp.byte()];
        trailing_position.extend_from_slice(&[0; 17]);
        assert!(matches!(
            ServerFrame::decode(&trailing_position),
            Err(FrameCodecError::TrailingBytes { count: 1, .. })
        ));
    }

    #[test]
    fn open_read_strictly_validates_tags_flags_fields_and_bounds() {
        let valid = ClientFrame::OpenRead {
            link_secret: None,
            start: ReadStart::TailOffset(80),
            limit: None,
            end_seq_num: None,
            playback_rate_permille: None,
            snapshot: false,
        }
        .encode()
        .expect("valid OpenRead");
        assert_eq!(valid.len(), ClientFrame::OPEN_READ_FIXED_LEN);

        let mut unknown_flags = valid.to_vec();
        unknown_flags[1] = 0x20;
        assert!(matches!(
            ClientFrame::decode(&unknown_flags),
            Err(FrameCodecError::UnknownOpenReadFlags(0x20))
        ));

        let mut unknown_tag = valid.to_vec();
        unknown_tag[2] = 0xff;
        assert!(matches!(
            ClientFrame::decode(&unknown_tag),
            Err(FrameCodecError::UnknownReadStartTag(0xff))
        ));

        let mut oversized_selector = valid.to_vec();
        oversized_selector[3..11].copy_from_slice(&(MAX_READ_SELECTOR_VALUE + 1).to_be_bytes());
        assert!(matches!(
            ClientFrame::decode(&oversized_selector),
            Err(FrameCodecError::ReadSelectorOutOfRange(value))
                if value == MAX_READ_SELECTOR_VALUE + 1
        ));

        for (limit, end_seq_num) in [(Some(u64::MAX), None), (None, Some(u64::MAX))] {
            let frame = ClientFrame::OpenRead {
                link_secret: None,
                start: ReadStart::SeqNum(0),
                limit,
                end_seq_num,
                playback_rate_permille: None,
                snapshot: false,
            };
            let encoded = frame.encode().expect("OpenRead with u64 bound");
            let ClientFrame::OpenRead {
                limit: decoded_limit,
                end_seq_num: decoded_end_seq_num,
                ..
            } = ClientFrame::decode(&encoded).expect("decode OpenRead with u64 bound")
            else {
                panic!("decoded a different client frame");
            };
            assert_eq!(decoded_limit, limit);
            assert_eq!(decoded_end_seq_num, end_seq_num);
        }

        let empty_secret = [
            ClientOp::OpenRead.byte(),
            OPEN_READ_LINK_SECRET,
            READ_START_SEQ_NUM,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
        ];
        assert!(matches!(
            ClientFrame::decode(&empty_secret),
            Err(FrameCodecError::TruncatedFrame { .. })
        ));
        let mut malformed_secret = valid.to_vec();
        malformed_secret[1] = OPEN_READ_LINK_SECRET;
        malformed_secret.extend_from_slice("B".repeat(43).as_bytes());
        assert!(matches!(
            ClientFrame::decode(&malformed_secret),
            Err(FrameCodecError::InvalidLinkSecret)
        ));
        assert!(matches!(
            ClientFrame::decode(&valid[..valid.len() - 1]),
            Err(FrameCodecError::TruncatedFrame { .. })
        ));
        let mut trailing = valid.to_vec();
        trailing.push(0);
        assert!(matches!(
            ClientFrame::decode(&trailing),
            Err(FrameCodecError::TrailingBytes { count: 1, .. })
        ));
        assert!(matches!(
            ClientFrame::OpenRead {
                link_secret: None,
                start: ReadStart::SeqNum(0),
                limit: None,
                end_seq_num: None,
                playback_rate_permille: Some(1_000),
                snapshot: false,
            }
            .encode(),
            Err(FrameCodecError::PlaybackRequiresEnd)
        ));
        assert!(matches!(
            ClientFrame::OpenRead {
                link_secret: None,
                start: ReadStart::SeqNum(0),
                limit: None,
                end_seq_num: Some(1),
                playback_rate_permille: Some(MAX_PLAYBACK_RATE_PERMILLE + 1),
                snapshot: false,
            }
            .encode(),
            Err(FrameCodecError::PlaybackRateOutOfRange(_))
        ));
    }

    #[test]
    fn open_write_strictly_validates_flags_preconditions_and_lengths() {
        let valid = ClientFrame::OpenWrite {
            client_writer_id: ClientWriterId::from_bytes([0; ClientWriterId::BYTE_LEN]),
            link_secret: "A"
                .repeat(LINK_SECRET_ENCODED_LENGTH)
                .parse()
                .expect("canonical secret"),
            expected_next_seq_num: Some(7),
        }
        .encode()
        .expect("valid OpenWrite");

        let mut unknown_flags = valid.to_vec();
        unknown_flags[1] = 0x02;
        assert!(matches!(
            ClientFrame::decode(&unknown_flags),
            Err(FrameCodecError::UnknownOpenWriteFlags(0x02))
        ));
        assert!(ClientFrame::decode(&valid[..valid.len() - 1]).is_err());
        assert!(matches!(
            ClientFrame::OpenWrite {
                client_writer_id: ClientWriterId::from_bytes([0; ClientWriterId::BYTE_LEN]),
                link_secret: "A".repeat(LINK_SECRET_ENCODED_LENGTH).parse().expect("canonical secret"),
                expected_next_seq_num: Some(MAX_READ_SELECTOR_VALUE + 1),
            }
            .encode(),
            Err(FrameCodecError::ExpectedNextSeqNumOutOfRange(value))
                if value == MAX_READ_SELECTOR_VALUE + 1
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

        let maximum_read = ServerFrame::ReadBatch(
            ReadBatch::try_from_records(
                (0..MAX_READ_BATCH_RECORDS as u64)
                    .map(|seq_num| owned_read_record(seq_num, Bytes::new()))
                    .collect(),
            )
            .expect("maximum read batch"),
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
    }

    #[test]
    fn try_from_records_enforces_wire_batch_bounds() {
        assert!(matches!(
            ReadBatch::try_from_records(vec![]),
            Err(FrameCodecError::InvalidBatchRecordCount { actual: 0, .. })
        ));
        assert!(matches!(
            ReadBatch::try_from_records(
                (0..=MAX_READ_BATCH_RECORDS as u64)
                    .map(|seq_num| owned_read_record(seq_num, Bytes::new()))
                    .collect()
            ),
            Err(FrameCodecError::InvalidBatchRecordCount {
                max: MAX_READ_BATCH_RECORDS,
                ..
            })
        ));
        assert!(matches!(
            ReadBatch::try_from_records(vec![owned_read_record(
                0,
                Bytes::from(vec![0; MAX_RECORD_BYTES + 1])
            )]),
            Err(FrameCodecError::RecordTooLarge {
                max: MAX_RECORD_BYTES,
                ..
            })
        ));
        assert!(matches!(
            ReadBatch::try_from_records(
                [0, 1, 2]
                    .map(|seq_num| owned_read_record(seq_num, Bytes::from(vec![0; 400 * 1024])))
                    .to_vec()
            ),
            Err(FrameCodecError::BatchPayloadTooLarge {
                max: MAX_BATCH_PAYLOAD_BYTES,
                ..
            })
        ));
        assert!(matches!(
            ReadBatch::try_from_records(vec![
                owned_read_record(0, Bytes::new()),
                owned_read_record(2, Bytes::new())
            ]),
            Err(FrameCodecError::NonContiguousReadBatch)
        ));
        assert!(ReadBatch::try_from_records(vec![owned_read_record(0, Bytes::new())]).is_ok());
    }

    #[test]
    fn read_batch_views_preserve_payload_boundaries() {
        let alpha = OwnedReadRecord {
            seq_num: 7,
            timestamp_ms: 100,
            writer_id: WriterId::from_bytes([1; WriterId::BYTE_LEN]),
            writer_seq_num: 3,
            part: PartHeader::unsplit(),
            format: RecordFormat::Bytes,
            data: Bytes::from_static(b"alpha"),
        };
        let beta = OwnedReadRecord {
            seq_num: 8,
            timestamp_ms: 101,
            writer_id: WriterId::from_bytes([2; WriterId::BYTE_LEN]),
            writer_seq_num: 4,
            part: PartHeader::unsplit(),
            format: RecordFormat::Transcript,
            data: Bytes::from_static(b"beta-longer"),
        };
        let batch =
            ReadBatch::try_from_records(vec![alpha.clone(), beta.clone()]).expect("valid batch");

        assert_eq!(batch.record_count(), 2);
        assert_eq!(batch.first(), alpha.as_record());
        assert_eq!(batch.last(), beta.as_record());

        let viewed: Vec<ReadRecord<'_>> = batch.iter().collect();
        assert_eq!(viewed, [alpha.as_record(), beta.as_record()]);
        assert_eq!(viewed[0].data, b"alpha");
        assert_eq!(viewed[1].data, b"beta-longer");
        assert_eq!(batch.iter().len(), 2);

        // Owned conversion copies payload bytes out of the shared buffer.
        let owned = viewed[1].into_owned();
        assert_eq!(owned, beta);
        assert_eq!(owned.data.as_ref(), b"beta-longer");

        // `&batch` iterates directly, and the iterator is double-ended and fused.
        let mut total = 0_usize;
        for record in &batch {
            total += record.data.len();
        }
        assert_eq!(total, b"alpha".len() + b"beta-longer".len());
        let mut reversed = batch.iter().rev().map(|record| record.seq_num);
        assert_eq!(reversed.next(), Some(8));
        assert_eq!(reversed.next(), Some(7));
        assert_eq!(reversed.next(), None);
        assert_eq!(reversed.next(), None);

        // A wire round trip preserves the same record views and payload boundaries.
        let frame = ServerFrame::ReadBatch(batch);
        let decoded = ServerFrame::decode_bytes(frame.encode().expect("encode")).expect("decode");
        assert_eq!(decoded, frame);
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
