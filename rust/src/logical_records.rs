//! Logical records over physical TSF records:
//! [`split_logical_record`](crate::logical_records::split_logical_record) produces the split-part
//! layout on the write side that
//! [`LogicalRecordAssembler`](crate::logical_records::LogicalRecordAssembler) reassembles
//! on the read side.

use std::collections::{HashMap, hash_map::Entry};

use bytes::{Buf, Bytes, BytesMut};

use crate::{
    WriterId,
    protocol::ws::frame::{
        AppendRecord, FrameCodecError, MAX_RECORD_PAYLOAD_BYTES, PartHeader, ReadRecord,
        split_record_payloads,
    },
};

/// Default maximum bytes used for split-record reassembly: 16 MiB.
pub const DEFAULT_MAX_RECORD_REASSEMBLY_BYTES: usize = MAX_RECORD_PAYLOAD_BYTES * 32;
const MAX_RECORD_WRITER_STATES: usize = 4_096;
const MAX_RECORD_TOTAL_PENDING_PARTS: usize = 16_384;

/// Per-writer duplicate suppression and split-record reassembly state.
///
/// Records are processed in delivery order. Reused or decreasing writer sequence numbers are
/// suppressed, malformed partial sequences are dropped, and a read beginning mid-split waits for
/// the next complete logical record. [`split_logical_record`] is the write-side counterpart that
/// produces the part layout reassembly expects.
pub struct LogicalRecordAssembler {
    max_reassembly_bytes: usize,
    writers: HashMap<WriterId, WriterState>,
    pending_totals: PendingTotals,
}

impl LogicalRecordAssembler {
    /// Creates record assembly state with [`DEFAULT_MAX_RECORD_REASSEMBLY_BYTES`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates record assembly state with an explicit split-record reassembly byte limit.
    pub fn with_max_reassembly_bytes(max_reassembly_bytes: usize) -> Self {
        Self {
            max_reassembly_bytes,
            writers: HashMap::new(),
            pending_totals: PendingTotals::default(),
        }
    }

    /// Processes one physical record.
    ///
    /// Returns a complete logical record when one becomes available, or `None` when the input was a
    /// duplicate, an incomplete split part, or a malformed partial sequence. Unsplit records lend
    /// their payload from the source batch; use [`LogicalRecordData::into_bytes`] to retain one.
    pub fn push_record<'a>(
        &mut self,
        record: ReadRecord<'a>,
    ) -> Result<Option<LogicalRecord<'a>>, LogicalRecordError> {
        let writer_count = self.writers.len();
        let writer = match self.writers.entry(record.writer_id) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                if writer_count >= MAX_RECORD_WRITER_STATES {
                    return Err(LogicalRecordError::WriterStateLimitExceeded {
                        actual: writer_count.saturating_add(1),
                        max: MAX_RECORD_WRITER_STATES,
                    });
                }
                entry.insert(WriterState::default())
            }
        };
        if writer
            .highest_seq
            .is_some_and(|highest| record.writer_seq_num <= highest)
        {
            return Ok(None);
        }
        writer.highest_seq = Some(record.writer_seq_num);

        if record.part == PartHeader::unsplit() {
            clear_pending(writer, &mut self.pending_totals);
            // Unsplit payloads borrow from the source batch; only split parts are copied at
            // ingest, because they must outlive their batch.
            return Ok(Some(LogicalRecord {
                data: LogicalRecordData::Borrowed(record.data),
            }));
        }

        let Some(start_seq_num) = record
            .writer_seq_num
            .checked_sub(u64::from(record.part.index()))
        else {
            clear_pending(writer, &mut self.pending_totals);
            return Ok(None);
        };

        let part_index = record.part.index();
        if part_index == 0 {
            clear_pending(writer, &mut self.pending_totals);
            let mut pending = PendingRecord {
                start_seq_num,
                next_part_index: 1,
                len: record.data.len(),
                part_count: 1,
                chunks: Vec::new(),
            };
            if !record.data.is_empty() {
                pending.chunks.push(Bytes::copy_from_slice(record.data));
            }
            self.pending_totals = self
                .pending_totals
                .with_added(&pending, self.max_reassembly_bytes)?;
            writer.pending = Some(pending);
            return Ok(None);
        }

        let Some(mut pending) = take_pending(writer, &mut self.pending_totals) else {
            return Ok(None);
        };
        if pending.start_seq_num != start_seq_num || pending.next_part_index != part_index {
            return Ok(None);
        }

        let logical_record_len = pending.len.checked_add(record.data.len()).ok_or(
            LogicalRecordError::ReassemblyLimitExceeded {
                actual: usize::MAX,
                max: self.max_reassembly_bytes,
            },
        )?;
        check_reassembly_len(logical_record_len, self.max_reassembly_bytes)?;
        let part_count = pending.part_count.saturating_add(1);
        pending.len = logical_record_len;
        pending.part_count = part_count;
        if !record.data.is_empty() {
            pending.chunks.push(Bytes::copy_from_slice(record.data));
        }
        if record.part.is_final() {
            return Ok(Some(LogicalRecord {
                data: LogicalRecordData::from_ordered_chunks(pending.chunks, pending.len),
            }));
        }
        let Some(next_part_index) = part_index.checked_add(1) else {
            return Ok(None);
        };
        pending.next_part_index = next_part_index;
        self.pending_totals = self
            .pending_totals
            .with_added(&pending, self.max_reassembly_bytes)?;
        writer.pending = Some(pending);
        Ok(None)
    }
}

impl Default for LogicalRecordAssembler {
    fn default() -> Self {
        Self::with_max_reassembly_bytes(DEFAULT_MAX_RECORD_REASSEMBLY_BYTES)
    }
}

/// Splits one logical record into the physical parts [`LogicalRecordAssembler`] reassembles.
///
/// Reassembly requires split parts to occupy consecutive writer sequence numbers matching their
/// part index; the returned records have both baked in, starting at `writer_start_seq_num`, so
/// submitting them in order upholds the invariant. A record that fits in
/// [`MAX_RECORD_PAYLOAD_BYTES`] is returned unsplit; larger payloads are sliced without copying.
/// Readers reject split records above their configured reassembly-byte limit.
///
/// This numbers parts for manually sequenced sinks (stateless appends and
/// [`TsfWriteSession`](crate::TsfWriteSession)); durable writers assign sequences themselves and
/// take [`AppendBatch::split_logical`](crate::protocol::ws::frame::AppendBatch::split_logical)
/// instead.
pub fn split_logical_record(
    writer_start_seq_num: u64,
    data: Bytes,
) -> Result<Vec<AppendRecord>, FrameCodecError> {
    let payloads = split_record_payloads(data)?;
    // The exclusive end of the range is an ack boundary and must stay representable.
    writer_start_seq_num
        .checked_add(payloads.len() as u64)
        .ok_or(FrameCodecError::WriterSequenceExhausted)?;
    Ok(payloads
        .into_iter()
        .enumerate()
        .map(|(index, payload)| AppendRecord {
            writer_seq_num: writer_start_seq_num + index as u64,
            part: payload.part,
            data: payload.data,
        })
        .collect())
}

/// One complete logical record after deduplication and reassembly.
///
/// Unsplit records borrow their payload from the source batch; split-record completions own
/// their assembled chunks. Retain past the batch with [`LogicalRecord::into_owned`] (keeps
/// chunks uncoalesced) or [`LogicalRecordData::into_bytes`] (explicitly contiguous).
#[derive(Clone, Debug)]
pub struct LogicalRecord<'a> {
    /// Exact logical payload, borrowed when possible and otherwise retained as owned chunks.
    pub data: LogicalRecordData<'a>,
}

impl LogicalRecord<'_> {
    /// Retains this record independently of the source batch, copying only a borrowed payload.
    pub fn into_owned(self) -> LogicalRecord<'static> {
        LogicalRecord {
            data: self.data.into_owned(),
        }
    }
}

/// Logical payload: a borrow from the source batch, one owned value, or multiple owned chunks.
#[derive(Clone, Debug)]
pub enum LogicalRecordData<'a> {
    /// Payload borrowed from the batch the record arrived in.
    Borrowed(&'a [u8]),
    /// Contiguous owned payload bytes.
    Owned(Bytes),
    /// Ordered non-empty physical chunks.
    Chunked(ChunkedBytes),
}

impl LogicalRecordData<'_> {
    /// Returns the number of bytes not consumed through the [`Buf`] implementation.
    pub fn len(&self) -> usize {
        self.remaining()
    }

    /// Returns whether no unconsumed bytes remain.
    pub fn is_empty(&self) -> bool {
        !self.has_remaining()
    }

    /// Retains the payload independently of the source batch without coalescing chunks: only a
    /// borrowed payload is copied.
    pub fn into_owned(self) -> LogicalRecordData<'static> {
        match self {
            Self::Borrowed(data) => LogicalRecordData::Owned(Bytes::copy_from_slice(data)),
            Self::Owned(data) => LogicalRecordData::Owned(data),
            Self::Chunked(data) => LogicalRecordData::Chunked(data),
        }
    }

    /// Coalesces the remaining payload into owned contiguous bytes, copying when borrowed or
    /// chunked. Use [`LogicalRecordData::into_owned`] to retain without forcing contiguity.
    pub fn into_bytes(self) -> Bytes {
        match self {
            Self::Borrowed(slice) => Bytes::copy_from_slice(slice),
            Self::Owned(bytes) => bytes,
            Self::Chunked(chunked) => chunked.into_bytes(),
        }
    }

    fn from_ordered_chunks(chunks: Vec<Bytes>, len: usize) -> Self {
        match chunks.len() {
            0 => Self::Owned(Bytes::new()),
            1 => Self::Owned(chunks.into_iter().next().expect("single chunk")),
            _ => Self::Chunked(ChunkedBytes::new(chunks, len)),
        }
    }
}

impl From<Bytes> for LogicalRecordData<'_> {
    fn from(bytes: Bytes) -> Self {
        Self::Owned(bytes)
    }
}

/// Storage shape does not change payload identity: compare contents, not variants.
impl<'b> PartialEq<LogicalRecordData<'b>> for LogicalRecordData<'_> {
    fn eq(&self, other: &LogicalRecordData<'b>) -> bool {
        let mut this = self.clone();
        let mut other = other.clone();
        if this.remaining() != other.remaining() {
            return false;
        }
        loop {
            let left = this.chunk();
            let right = other.chunk();
            if left.is_empty() && right.is_empty() {
                return true;
            }
            let shared = left.len().min(right.len());
            if shared == 0 || left[..shared] != right[..shared] {
                return false;
            }
            this.advance(shared);
            other.advance(shared);
        }
    }
}

impl Eq for LogicalRecordData<'_> {}

impl<'b> PartialEq<LogicalRecord<'b>> for LogicalRecord<'_> {
    fn eq(&self, other: &LogicalRecord<'b>) -> bool {
        self.data == other.data
    }
}

impl Eq for LogicalRecord<'_> {}

impl Buf for LogicalRecordData<'_> {
    fn remaining(&self) -> usize {
        match self {
            Self::Borrowed(slice) => slice.len(),
            Self::Owned(bytes) => bytes.len(),
            Self::Chunked(chunked) => chunked.remaining(),
        }
    }

    fn chunk(&self) -> &[u8] {
        match self {
            Self::Borrowed(slice) => slice,
            Self::Owned(bytes) => bytes.as_ref(),
            Self::Chunked(chunked) => chunked.chunk(),
        }
    }

    fn advance(&mut self, cnt: usize) {
        match self {
            Self::Borrowed(slice) => slice.advance(cnt),
            Self::Owned(bytes) => bytes.advance(cnt),
            Self::Chunked(chunked) => chunked.advance(cnt),
        }
    }
}

/// Ordered zero-copy payload chunks implementing [`Buf`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChunkedBytes {
    chunks: Vec<Bytes>,
    index: usize,
    offset: usize,
    remaining: usize,
}

impl ChunkedBytes {
    fn new(chunks: Vec<Bytes>, remaining: usize) -> Self {
        debug_assert!(chunks.iter().all(|chunk| !chunk.is_empty()));
        debug_assert_eq!(remaining, chunks.iter().map(Bytes::len).sum::<usize>());
        Self {
            chunks,
            index: 0,
            offset: 0,
            remaining,
        }
    }

    /// Coalesces the remaining chunks into one contiguous byte value.
    pub fn into_bytes(self) -> Bytes {
        // A fully consumed prefix leaves the payload contiguous, so the tail chunk can be shared.
        if let [chunk] = &self.chunks[self.index..] {
            return chunk.slice(self.offset..);
        }

        let mut data = BytesMut::with_capacity(self.remaining);
        for chunk in self.chunks.into_iter().skip(self.index) {
            let bytes = if data.is_empty() && self.offset > 0 {
                &chunk[self.offset..]
            } else {
                chunk.as_ref()
            };
            data.extend_from_slice(bytes);
        }
        data.freeze()
    }
}

impl Buf for ChunkedBytes {
    fn remaining(&self) -> usize {
        self.remaining
    }

    fn chunk(&self) -> &[u8] {
        if self.remaining == 0 {
            return &[];
        }
        &self.chunks[self.index][self.offset..]
    }

    fn advance(&mut self, mut cnt: usize) {
        assert!(
            cnt <= self.remaining,
            "cannot advance past remaining record data"
        );
        self.remaining -= cnt;

        while cnt > 0 {
            let current_remaining = self.chunks[self.index].len() - self.offset;
            if cnt < current_remaining {
                self.offset += cnt;
                return;
            }

            cnt -= current_remaining;
            self.index += 1;
            self.offset = 0;
        }
    }
}

/// Error returned while reconstructing logical records.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum LogicalRecordError {
    /// Split-record reassembly exceeded the configured byte limit.
    #[error("record reassembly would use {actual} bytes; maximum is {max}")]
    ReassemblyLimitExceeded {
        /// Required or overflow-saturated reassembly bytes.
        actual: usize,
        /// Configured maximum reassembly bytes.
        max: usize,
    },
    /// Retaining a new writer identity would exceed the internal cardinality guard.
    #[error("record assembly has {actual} writer states; maximum is {max}")]
    WriterStateLimitExceeded {
        /// Writer-state count after the attempted insertion.
        actual: usize,
        /// Internal writer-state guard.
        max: usize,
    },
    /// Retaining another split part would exceed the internal pending-part guard.
    #[error("record assembly would retain {actual} total pending parts; maximum is {max}")]
    TotalPendingPartsLimitExceeded {
        /// Total pending physical parts after the attempted update.
        actual: usize,
        /// Internal total pending-part guard.
        max: usize,
    },
}

#[derive(Default)]
struct WriterState {
    highest_seq: Option<u64>,
    pending: Option<PendingRecord>,
}

struct PendingRecord {
    start_seq_num: u64,
    next_part_index: u32,
    len: usize,
    part_count: usize,
    chunks: Vec<Bytes>,
}

#[derive(Clone, Copy, Default)]
struct PendingTotals {
    bytes: usize,
    parts: usize,
}

impl PendingTotals {
    fn with_added(
        self,
        pending: &PendingRecord,
        max_reassembly_bytes: usize,
    ) -> Result<Self, LogicalRecordError> {
        let bytes = self.bytes.saturating_add(pending.len);
        if bytes > max_reassembly_bytes {
            return Err(LogicalRecordError::ReassemblyLimitExceeded {
                actual: bytes,
                max: max_reassembly_bytes,
            });
        }
        let parts = self.parts.saturating_add(pending.part_count);
        if parts > MAX_RECORD_TOTAL_PENDING_PARTS {
            return Err(LogicalRecordError::TotalPendingPartsLimitExceeded {
                actual: parts,
                max: MAX_RECORD_TOTAL_PENDING_PARTS,
            });
        }
        Ok(Self { bytes, parts })
    }

    fn remove(&mut self, pending: &PendingRecord) {
        debug_assert!(self.bytes >= pending.len);
        debug_assert!(self.parts >= pending.part_count);
        self.bytes = self.bytes.saturating_sub(pending.len);
        self.parts = self.parts.saturating_sub(pending.part_count);
    }
}

fn check_reassembly_len(len: usize, max: usize) -> Result<(), LogicalRecordError> {
    if len > max {
        Err(LogicalRecordError::ReassemblyLimitExceeded { actual: len, max })
    } else {
        Ok(())
    }
}

fn take_pending(writer: &mut WriterState, totals: &mut PendingTotals) -> Option<PendingRecord> {
    let pending = writer.pending.take()?;
    totals.remove(&pending);
    Some(pending)
}

fn clear_pending(writer: &mut WriterState, totals: &mut PendingTotals) {
    drop(take_pending(writer, totals));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_records_round_trip_through_reassembly() {
        let len = MAX_RECORD_PAYLOAD_BYTES * 2 + MAX_RECORD_PAYLOAD_BYTES / 2;
        let data = Bytes::from((0..len).map(|i| i as u8).collect::<Vec<u8>>());
        let records = split_logical_record(7, data.clone()).expect("split");

        assert_eq!(records.len(), 3);
        for (index, part) in records.iter().enumerate() {
            assert_eq!(part.writer_seq_num, 7 + index as u64);
            assert_eq!(part.part.index(), index as u32);
            assert_eq!(part.part.is_final(), index == records.len() - 1);
        }
        assert!(
            records[..records.len() - 1]
                .iter()
                .all(|part| part.data.len() == MAX_RECORD_PAYLOAD_BYTES)
        );

        let mut transcript = LogicalRecordAssembler::new();
        let mut reassembled = None;
        for part in &records {
            let read = ReadRecord {
                seq_num: part.writer_seq_num,
                timestamp_ms: 0,
                writer_id: WriterId::from_bytes([1; WriterId::BYTE_LEN]),
                writer_seq_num: part.writer_seq_num,
                part: part.part,
                data: &part.data,
            };
            reassembled = transcript.push_record(read).expect("push part");
        }
        let reassembled = reassembled.expect("final part completes the logical record");
        assert_eq!(reassembled.data.into_bytes(), data);
    }

    #[test]
    fn small_and_empty_logical_records_stay_unsplit_without_copying() {
        let data = Bytes::from_static(b"fits");
        let records = split_logical_record(3, data.clone()).expect("split");
        let [record] = records.as_slice() else {
            panic!("expected one unsplit record");
        };
        assert_eq!(record.writer_seq_num, 3);
        assert_eq!(record.part, PartHeader::unsplit());
        // Same backing storage, not a copy.
        assert!(std::ptr::eq(record.data.as_ptr(), data.as_ptr()));

        let empty = split_logical_record(0, Bytes::new()).expect("split");
        assert_eq!(empty.len(), 1);
        assert!(empty[0].data.is_empty());
        assert_eq!(empty[0].part, PartHeader::unsplit());
    }

    #[test]
    fn split_rejects_writer_sequence_exhaustion() {
        assert!(matches!(
            split_logical_record(u64::MAX, Bytes::from_static(b"x")),
            Err(FrameCodecError::WriterSequenceExhausted)
        ));
        assert!(matches!(
            split_logical_record(
                u64::MAX - 1,
                Bytes::from(vec![0_u8; MAX_RECORD_PAYLOAD_BYTES + 1]),
            ),
            Err(FrameCodecError::WriterSequenceExhausted)
        ));
    }

    fn record(seq: u64, part: PartHeader, data: &[u8]) -> ReadRecord<'_> {
        record_with_writer(
            WriterId::from_bytes([1; WriterId::BYTE_LEN]),
            seq,
            part,
            data,
        )
    }

    fn record_with_writer(
        writer_id: WriterId,
        seq: u64,
        part: PartHeader,
        data: &[u8],
    ) -> ReadRecord<'_> {
        ReadRecord {
            seq_num: seq,
            timestamp_ms: seq,
            writer_id,
            writer_seq_num: seq,
            part,
            data,
        }
    }

    fn writer_id(index: usize) -> WriterId {
        let mut bytes = [0; WriterId::BYTE_LEN];
        bytes[WriterId::BYTE_LEN - size_of::<usize>()..].copy_from_slice(&index.to_be_bytes());
        WriterId::from_bytes(bytes)
    }

    fn push<'a>(
        transcript: &mut LogicalRecordAssembler,
        record: ReadRecord<'a>,
    ) -> Option<LogicalRecord<'a>> {
        transcript.push_record(record).expect("push record")
    }

    fn assert_chunked_record(record: Option<LogicalRecord>, expected: &'static [u8]) {
        let Some(record) = record else {
            panic!("expected logical record");
        };
        assert!(matches!(record.data, LogicalRecordData::Chunked(_)));
        assert_eq!(record.data.into_bytes(), Bytes::from_static(expected));
    }

    #[test]
    fn suppresses_reused_writer_sequences() {
        let mut transcript = LogicalRecordAssembler::new();

        assert_eq!(
            push(&mut transcript, record(0, PartHeader::unsplit(), b"hello")),
            Some(LogicalRecord {
                data: LogicalRecordData::Borrowed(b"hello")
            })
        );
        assert_eq!(
            push(&mut transcript, record(0, PartHeader::unsplit(), b"hello")),
            None
        );
        assert_eq!(
            push(&mut transcript, record(0, PartHeader::unsplit(), b"HELLO")),
            None
        );
    }

    #[test]
    fn drops_split_records_after_gap() {
        let mut transcript = LogicalRecordAssembler::new();

        assert_eq!(
            push(
                &mut transcript,
                record(7, PartHeader::new(0, false).expect("part"), b"hel"),
            ),
            None
        );
        assert_eq!(
            push(
                &mut transcript,
                record(9, PartHeader::new(2, true).expect("part"), b"lo"),
            ),
            None
        );
        assert_eq!(
            push(&mut transcript, record(10, PartHeader::unsplit(), b"next")),
            Some(LogicalRecord {
                data: LogicalRecordData::Borrowed(b"next")
            })
        );
    }

    #[test]
    fn tracks_writer_sequences_independently() {
        let mut transcript = LogicalRecordAssembler::new();
        let first_writer = WriterId::from_bytes([1; WriterId::BYTE_LEN]);
        let second_writer = WriterId::from_bytes([2; WriterId::BYTE_LEN]);

        assert_eq!(
            push(
                &mut transcript,
                record_with_writer(first_writer, 0, PartHeader::unsplit(), b"first"),
            ),
            Some(LogicalRecord {
                data: LogicalRecordData::Borrowed(b"first")
            })
        );
        assert_eq!(
            push(
                &mut transcript,
                record_with_writer(second_writer, 0, PartHeader::unsplit(), b"second"),
            ),
            Some(LogicalRecord {
                data: LogicalRecordData::Borrowed(b"second")
            })
        );
    }

    #[test]
    fn rejects_split_records_above_the_reassembly_limit_and_resyncs() {
        let mut transcript = LogicalRecordAssembler::with_max_reassembly_bytes(4);

        assert_eq!(
            push(
                &mut transcript,
                record(7, PartHeader::new(0, false).expect("part"), b"hel"),
            ),
            None
        );
        let error = transcript
            .push_record(record(8, PartHeader::new(1, true).expect("part"), b"lo"))
            .expect_err("reassembly limit");
        assert_eq!(
            error,
            LogicalRecordError::ReassemblyLimitExceeded { actual: 5, max: 4 }
        );
        assert_eq!(
            push(&mut transcript, record(9, PartHeader::unsplit(), b"next")),
            Some(LogicalRecord {
                data: LogicalRecordData::Borrowed(b"next")
            })
        );
    }

    #[test]
    fn rejects_new_writer_identities_above_the_internal_limit() {
        let mut transcript = LogicalRecordAssembler::with_max_reassembly_bytes(16);

        for index in 0..MAX_RECORD_WRITER_STATES {
            assert!(
                push(
                    &mut transcript,
                    record_with_writer(writer_id(index), 0, PartHeader::unsplit(), b"ok",),
                )
                .is_some()
            );
        }

        let error = transcript
            .push_record(record_with_writer(
                writer_id(MAX_RECORD_WRITER_STATES),
                0,
                PartHeader::unsplit(),
                b"rejected",
            ))
            .expect_err("writer-state limit");
        assert_eq!(
            error,
            LogicalRecordError::WriterStateLimitExceeded {
                actual: MAX_RECORD_WRITER_STATES + 1,
                max: MAX_RECORD_WRITER_STATES,
            }
        );
        assert_eq!(transcript.writers.len(), MAX_RECORD_WRITER_STATES);
    }

    #[test]
    fn bounds_reassembly_bytes_across_writers_and_releases_completed_state() {
        let mut transcript = LogicalRecordAssembler::with_max_reassembly_bytes(4);
        let first_writer = WriterId::from_bytes([1; WriterId::BYTE_LEN]);
        let second_writer = WriterId::from_bytes([2; WriterId::BYTE_LEN]);

        assert_eq!(
            push(
                &mut transcript,
                record_with_writer(
                    first_writer,
                    0,
                    PartHeader::new(0, false).expect("part"),
                    b"abc",
                ),
            ),
            None
        );
        assert_eq!(transcript.pending_totals.bytes, 3);
        assert_eq!(transcript.pending_totals.parts, 1);

        let error = transcript
            .push_record(record_with_writer(
                second_writer,
                0,
                PartHeader::new(0, false).expect("part"),
                b"de",
            ))
            .expect_err("reassembly-byte limit");
        assert_eq!(
            error,
            LogicalRecordError::ReassemblyLimitExceeded { actual: 5, max: 4 }
        );
        assert_eq!(transcript.pending_totals.bytes, 3);

        assert_chunked_record(
            push(
                &mut transcript,
                record_with_writer(
                    first_writer,
                    1,
                    PartHeader::new(1, true).expect("part"),
                    b"d",
                ),
            ),
            b"abcd",
        );
        assert_eq!(transcript.pending_totals.bytes, 0);
        assert_eq!(transcript.pending_totals.parts, 0);

        assert_eq!(
            push(
                &mut transcript,
                record_with_writer(
                    second_writer,
                    1,
                    PartHeader::new(0, false).expect("part"),
                    b"wxyz",
                ),
            ),
            None
        );
        assert_eq!(transcript.pending_totals.bytes, 4);
        assert_eq!(transcript.pending_totals.parts, 1);
    }

    #[test]
    fn bounds_total_pending_parts_including_empty_parts() {
        let mut transcript = LogicalRecordAssembler::with_max_reassembly_bytes(16);

        for index in 0..MAX_RECORD_TOTAL_PENDING_PARTS {
            assert_eq!(
                push(
                    &mut transcript,
                    record(
                        index as u64,
                        PartHeader::new(index as u32, false).expect("part"),
                        b"",
                    ),
                ),
                None
            );
        }
        assert_eq!(transcript.pending_totals.bytes, 0);
        assert_eq!(
            transcript.pending_totals.parts,
            MAX_RECORD_TOTAL_PENDING_PARTS
        );

        let error = transcript
            .push_record(record(
                MAX_RECORD_TOTAL_PENDING_PARTS as u64,
                PartHeader::new(MAX_RECORD_TOTAL_PENDING_PARTS as u32, false).expect("part"),
                b"",
            ))
            .expect_err("total pending-part limit");
        assert_eq!(
            error,
            LogicalRecordError::TotalPendingPartsLimitExceeded {
                actual: MAX_RECORD_TOTAL_PENDING_PARTS + 1,
                max: MAX_RECORD_TOTAL_PENDING_PARTS,
            }
        );
        assert_eq!(transcript.pending_totals.bytes, 0);
        assert_eq!(transcript.pending_totals.parts, 0);
    }

    #[test]
    fn total_pending_part_limit_does_not_prevent_completion_at_the_boundary() {
        let mut transcript = LogicalRecordAssembler::with_max_reassembly_bytes(16);

        for index in 0..MAX_RECORD_TOTAL_PENDING_PARTS - 1 {
            assert_eq!(
                push(
                    &mut transcript,
                    record(
                        index as u64,
                        PartHeader::new(index as u32, false).expect("part"),
                        b"",
                    ),
                ),
                None
            );
        }
        assert_eq!(
            push(
                &mut transcript,
                record(
                    (MAX_RECORD_TOTAL_PENDING_PARTS - 1) as u64,
                    PartHeader::new((MAX_RECORD_TOTAL_PENDING_PARTS - 1) as u32, true)
                        .expect("part"),
                    b"",
                ),
            ),
            Some(LogicalRecord {
                data: LogicalRecordData::Borrowed(b""),
            }),
        );
        assert_eq!(transcript.pending_totals.parts, 0);
    }
}
