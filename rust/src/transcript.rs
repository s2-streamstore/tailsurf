//! Logical records over physical TSF records:
//! [`split_logical_record`](crate::transcript::split_logical_record) produces the split-part layout
//! on the write side that [`LogicalTranscript`](crate::transcript::LogicalTranscript) reassembles
//! on the read side.

use std::collections::{HashMap, hash_map::Entry};

use bytes::{Buf, Bytes, BytesMut};

use crate::{
    WriterId,
    protocol::ws::frame::{
        AppendRecord, FrameCodecError, MAX_RECORD_BYTES, PartHeader, ReadRecord, RecordFormat,
        split_record_payloads,
    },
};

/// Default maximum reassembled logical-record size: 16 MiB.
pub const DEFAULT_MAX_LOGICAL_RECORD_BYTES: usize = MAX_RECORD_BYTES * 32;
/// Default maximum writer identities retained for deduplication and reassembly.
pub const DEFAULT_MAX_WRITER_STATES: usize = 4_096;
/// Default SDK memory-safety limit across all unfinished split records: 16 MiB.
pub const DEFAULT_MAX_PENDING_BYTES: usize = 16 * 1024 * 1024;
/// Default maximum physical parts retained across all unfinished split records.
pub const DEFAULT_MAX_PENDING_PARTS: usize = 16_384;

/// Memory and cardinality limits for one logical transcript.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TranscriptLimits {
    /// Maximum size of one reassembled logical record.
    pub max_logical_record_bytes: usize,
    /// Maximum writer identities retained for deduplication and reassembly.
    pub max_writer_states: usize,
    /// Maximum payload bytes retained across all unfinished split records.
    pub max_pending_bytes: usize,
    /// Maximum physical parts retained across all unfinished split records.
    pub max_pending_parts: usize,
}

impl TranscriptLimits {
    /// Creates explicit transcript limits.
    pub const fn new(
        max_logical_record_bytes: usize,
        max_writer_states: usize,
        max_pending_bytes: usize,
        max_pending_parts: usize,
    ) -> Self {
        Self {
            max_logical_record_bytes,
            max_writer_states,
            max_pending_bytes,
            max_pending_parts,
        }
    }
}

impl Default for TranscriptLimits {
    fn default() -> Self {
        Self::new(
            DEFAULT_MAX_LOGICAL_RECORD_BYTES,
            DEFAULT_MAX_WRITER_STATES,
            DEFAULT_MAX_PENDING_BYTES,
            DEFAULT_MAX_PENDING_PARTS,
        )
    }
}

/// Per-writer duplicate suppression and split-record reassembly state.
///
/// Records are processed in delivery order. Reused or decreasing writer sequence numbers are
/// suppressed, malformed partial sequences are dropped, and a read beginning mid-split waits for
/// the next complete logical record. [`split_logical_record`] is the write-side counterpart that
/// produces the part layout reassembly expects.
pub struct LogicalTranscript {
    limits: TranscriptLimits,
    writers: HashMap<WriterId, WriterState>,
    pending_bytes: usize,
    pending_parts: usize,
}

impl LogicalTranscript {
    /// Creates transcript state with [`DEFAULT_MAX_LOGICAL_RECORD_BYTES`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates transcript state with an explicit logical-record byte limit.
    ///
    /// The aggregate pending-byte limit is raised as needed to hold one record at this limit.
    pub fn with_max_logical_record_bytes(max_logical_record_bytes: usize) -> Self {
        Self::with_limits(TranscriptLimits {
            max_logical_record_bytes,
            max_pending_bytes: DEFAULT_MAX_PENDING_BYTES.max(max_logical_record_bytes),
            ..TranscriptLimits::default()
        })
    }

    /// Creates transcript state with explicit record, writer, pending-byte, and pending-part
    /// limits.
    pub fn with_limits(limits: TranscriptLimits) -> Self {
        Self {
            limits,
            writers: HashMap::new(),
            pending_bytes: 0,
            pending_parts: 0,
        }
    }

    /// Processes one physical record.
    ///
    /// Returns a complete logical record when one becomes available, or `None` when the input was a
    /// duplicate, an incomplete split part, or a malformed partial sequence. Unsplit records lend
    /// their payload from the source batch; use [`TranscriptData::into_bytes`] to retain one.
    pub fn push_record<'a>(
        &mut self,
        record: ReadRecord<'a>,
    ) -> Result<Option<TranscriptRecord<'a>>, TranscriptError> {
        let limits = self.limits;
        let writer_count = self.writers.len();
        let writer = match self.writers.entry(record.writer_id) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                if writer_count >= limits.max_writer_states {
                    return Err(TranscriptError::WriterStateLimitExceeded {
                        actual: writer_count.saturating_add(1),
                        max: limits.max_writer_states,
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
            clear_pending(writer, &mut self.pending_bytes, &mut self.pending_parts);
            check_logical_record_len(record.data.len(), limits.max_logical_record_bytes)?;
            // Unsplit payloads borrow from the source batch; only split parts are copied at
            // ingest, because they must outlive their batch.
            return Ok(Some(TranscriptRecord {
                format: record.format,
                data: TranscriptData::Borrowed(record.data),
            }));
        }

        let Some(start_seq_num) = record
            .writer_seq_num
            .checked_sub(u64::from(record.part.index()))
        else {
            clear_pending(writer, &mut self.pending_bytes, &mut self.pending_parts);
            return Ok(None);
        };

        let part_index = record.part.index();
        if part_index == 0 {
            clear_pending(writer, &mut self.pending_bytes, &mut self.pending_parts);
            check_logical_record_len(record.data.len(), limits.max_logical_record_bytes)?;
            let pending_bytes = checked_pending_bytes(
                self.pending_bytes,
                record.data.len(),
                limits.max_pending_bytes,
            )?;
            let pending_parts =
                checked_pending_parts(self.pending_parts, 1, limits.max_pending_parts)?;
            let mut pending = PendingRecord {
                start_seq_num,
                next_part_index: 1,
                format: record.format,
                len: record.data.len(),
                part_count: 1,
                chunks: Vec::new(),
            };
            if !record.data.is_empty() {
                pending.chunks.push(Bytes::copy_from_slice(record.data));
            }
            writer.pending = Some(pending);
            self.pending_bytes = pending_bytes;
            self.pending_parts = pending_parts;
            return Ok(None);
        }

        let Some(mut pending) =
            take_pending(writer, &mut self.pending_bytes, &mut self.pending_parts)
        else {
            return Ok(None);
        };
        if pending.start_seq_num != start_seq_num
            || pending.next_part_index != part_index
            || pending.format != record.format
        {
            return Ok(None);
        }

        let logical_record_len = pending.len.checked_add(record.data.len()).ok_or(
            TranscriptError::LogicalRecordTooLarge {
                len: usize::MAX,
                max: limits.max_logical_record_bytes,
            },
        )?;
        check_logical_record_len(logical_record_len, limits.max_logical_record_bytes)?;
        let part_count = pending.part_count.saturating_add(1);
        pending.len = logical_record_len;
        pending.part_count = part_count;
        if !record.data.is_empty() {
            pending.chunks.push(Bytes::copy_from_slice(record.data));
        }
        if record.part.is_final() {
            return Ok(Some(TranscriptRecord {
                format: pending.format,
                data: TranscriptData::from_ordered_chunks(pending.chunks, pending.len),
            }));
        }
        let pending_parts =
            checked_pending_parts(self.pending_parts, part_count, limits.max_pending_parts)?;

        let Some(next_part_index) = part_index.checked_add(1) else {
            return Ok(None);
        };
        pending.next_part_index = next_part_index;
        self.pending_bytes =
            checked_pending_bytes(self.pending_bytes, pending.len, limits.max_pending_bytes)?;
        self.pending_parts = pending_parts;
        writer.pending = Some(pending);
        Ok(None)
    }
}

impl Default for LogicalTranscript {
    fn default() -> Self {
        Self::with_limits(TranscriptLimits::default())
    }
}

/// Splits one logical record into the physical parts [`LogicalTranscript`] reassembles.
///
/// Reassembly requires split parts to occupy consecutive writer sequence numbers matching their
/// part index; the returned records have both baked in, starting at `writer_start_seq_num`, so
/// submitting them in order upholds the invariant. A record that fits in [`MAX_RECORD_BYTES`]
/// is returned unsplit; larger payloads are sliced without copying. Readers drop logical
/// records above their configured [`TranscriptLimits::max_logical_record_bytes`].
///
/// This numbers parts for manually sequenced sinks (stateless appends and
/// [`TsfWriteSession`](crate::TsfWriteSession)); durable writers assign sequences themselves and
/// take [`AppendBatch::split_logical`](crate::protocol::ws::frame::AppendBatch::split_logical)
/// instead.
pub fn split_logical_record(
    writer_start_seq_num: u64,
    format: RecordFormat,
    data: Bytes,
) -> Result<Vec<AppendRecord>, FrameCodecError> {
    let payloads = split_record_payloads(format, data)?;
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
            format: payload.format,
            data: payload.data,
        })
        .collect())
}

/// One complete logical transcript record after deduplication and reassembly.
///
/// Unsplit records borrow their payload from the source batch; split-record completions own
/// their assembled chunks. Retain past the batch with [`TranscriptRecord::into_owned`] (keeps
/// chunks uncoalesced) or [`TranscriptData::into_bytes`] (explicitly contiguous).
#[derive(Clone, Debug)]
pub struct TranscriptRecord<'a> {
    /// Presentation hint shared by every physical part.
    pub format: RecordFormat,
    /// Exact logical payload, borrowed when possible and otherwise retained as owned chunks.
    pub data: TranscriptData<'a>,
}

impl TranscriptRecord<'_> {
    /// Retains this record independently of the source batch, copying only a borrowed payload.
    pub fn into_owned(self) -> TranscriptRecord<'static> {
        TranscriptRecord {
            format: self.format,
            data: self.data.into_owned(),
        }
    }
}

/// Logical payload: a borrow from the source batch, one owned value, or multiple owned chunks.
#[derive(Clone, Debug)]
pub enum TranscriptData<'a> {
    /// Payload borrowed from the batch the record arrived in.
    Borrowed(&'a [u8]),
    /// Contiguous owned payload bytes.
    Owned(Bytes),
    /// Ordered non-empty physical chunks.
    Chunked(ChunkedBytes),
}

impl TranscriptData<'_> {
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
    pub fn into_owned(self) -> TranscriptData<'static> {
        match self {
            Self::Borrowed(data) => TranscriptData::Owned(Bytes::copy_from_slice(data)),
            Self::Owned(data) => TranscriptData::Owned(data),
            Self::Chunked(data) => TranscriptData::Chunked(data),
        }
    }

    /// Coalesces the remaining payload into owned contiguous bytes, copying when borrowed or
    /// chunked. Use [`TranscriptData::into_owned`] to retain without forcing contiguity.
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

impl From<Bytes> for TranscriptData<'_> {
    fn from(bytes: Bytes) -> Self {
        Self::Owned(bytes)
    }
}

/// Storage shape does not change payload identity: compare contents, not variants.
impl<'a, 'b> PartialEq<TranscriptData<'b>> for TranscriptData<'a> {
    fn eq(&self, other: &TranscriptData<'b>) -> bool {
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

impl Eq for TranscriptData<'_> {}

impl<'a, 'b> PartialEq<TranscriptRecord<'b>> for TranscriptRecord<'a> {
    fn eq(&self, other: &TranscriptRecord<'b>) -> bool {
        self.format == other.format && self.data == other.data
    }
}

impl Eq for TranscriptRecord<'_> {}

impl Buf for TranscriptData<'_> {
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
            "cannot advance past remaining transcript data"
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

/// Error returned while reconstructing a logical transcript.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum TranscriptError {
    /// A complete or partial logical record exceeded the configured limit.
    #[error("logical record is {len} bytes; maximum is {max}")]
    LogicalRecordTooLarge {
        /// Actual or overflow-saturated logical length.
        len: usize,
        /// Configured maximum logical length.
        max: usize,
    },
    /// Retaining a new writer identity would exceed the configured cardinality limit.
    #[error("transcript has {actual} writer states; maximum is {max}")]
    WriterStateLimitExceeded {
        /// Writer-state count after the attempted insertion.
        actual: usize,
        /// Configured writer-state limit.
        max: usize,
    },
    /// Retaining an unfinished split record would exceed the aggregate pending-byte limit.
    #[error("transcript would retain {actual} pending bytes; maximum is {max}")]
    PendingBytesLimitExceeded {
        /// Aggregate pending payload bytes after the attempted update.
        actual: usize,
        /// Configured aggregate pending-byte limit.
        max: usize,
    },
    /// Retaining another split part would exceed the aggregate pending-part limit.
    #[error("transcript would retain {actual} pending parts; maximum is {max}")]
    PendingPartsLimitExceeded {
        /// Aggregate pending physical parts after the attempted update.
        actual: usize,
        /// Configured aggregate pending-part limit.
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
    format: RecordFormat,
    len: usize,
    part_count: usize,
    chunks: Vec<Bytes>,
}

fn check_logical_record_len(len: usize, max: usize) -> Result<(), TranscriptError> {
    if len > max {
        Err(TranscriptError::LogicalRecordTooLarge { len, max })
    } else {
        Ok(())
    }
}

fn checked_pending_bytes(
    current: usize,
    added: usize,
    max: usize,
) -> Result<usize, TranscriptError> {
    let actual = current.saturating_add(added);
    if actual > max {
        Err(TranscriptError::PendingBytesLimitExceeded { actual, max })
    } else {
        Ok(actual)
    }
}

fn checked_pending_parts(
    current: usize,
    added: usize,
    max: usize,
) -> Result<usize, TranscriptError> {
    let actual = current.saturating_add(added);
    if actual > max {
        Err(TranscriptError::PendingPartsLimitExceeded { actual, max })
    } else {
        Ok(actual)
    }
}

fn take_pending(
    writer: &mut WriterState,
    pending_bytes: &mut usize,
    pending_parts: &mut usize,
) -> Option<PendingRecord> {
    let pending = writer.pending.take()?;
    debug_assert!(*pending_bytes >= pending.len);
    debug_assert!(*pending_parts >= pending.part_count);
    *pending_bytes = pending_bytes.saturating_sub(pending.len);
    *pending_parts = pending_parts.saturating_sub(pending.part_count);
    Some(pending)
}

fn clear_pending(writer: &mut WriterState, pending_bytes: &mut usize, pending_parts: &mut usize) {
    drop(take_pending(writer, pending_bytes, pending_parts));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::ws::frame::{OwnedReadRecord, ReadBatch};

    fn owned_batch_record(seq: u64, part: PartHeader, data: &'static [u8]) -> OwnedReadRecord {
        OwnedReadRecord {
            seq_num: seq,
            timestamp_ms: seq,
            writer_id: WriterId::from_bytes([1; WriterId::BYTE_LEN]),
            writer_seq_num: seq,
            part,
            format: RecordFormat::Transcript,
            data: Bytes::from_static(data),
        }
    }

    #[test]
    fn unsplit_records_lend_the_source_payload() {
        let mut transcript = LogicalTranscript::new();
        let data = b"lent";
        let pushed =
            push(&mut transcript, record(0, PartHeader::unsplit(), data)).expect("unsplit record");

        // Content equality alone cannot catch an accidental copy regression; require the exact
        // source slice.
        let TranscriptData::Borrowed(slice) = &pushed.data else {
            panic!("unsplit record must lend the source payload");
        };
        assert!(std::ptr::eq(*slice, data.as_slice()));
    }

    #[test]
    fn into_owned_retains_records_beyond_the_source_batch() {
        let mut transcript = LogicalTranscript::new();
        let retained: TranscriptRecord<'static> = {
            let batch = ReadBatch::try_from_records(vec![owned_batch_record(
                0,
                PartHeader::unsplit(),
                b"kept",
            )])
            .expect("batch");
            push(&mut transcript, batch.first())
                .expect("record")
                .into_owned()
        };
        // The batch is dropped; the retained record must own its payload.
        assert!(matches!(retained.data, TranscriptData::Owned(_)));
        assert_eq!(retained.data.into_bytes(), Bytes::from_static(b"kept"));
    }

    #[test]
    fn split_completion_across_batches_retains_without_coalescing() {
        let mut transcript = LogicalTranscript::new();
        {
            let first_batch = ReadBatch::try_from_records(vec![owned_batch_record(
                0,
                PartHeader::new(0, false).expect("part"),
                b"hel",
            )])
            .expect("batch");
            assert!(push(&mut transcript, first_batch.first()).is_none());
        }
        // The first batch is dropped; the pending part was copied at ingest.
        let second_batch = ReadBatch::try_from_records(vec![owned_batch_record(
            1,
            PartHeader::new(1, true).expect("part"),
            b"lo",
        )])
        .expect("batch");
        let completed = push(&mut transcript, second_batch.first()).expect("split completion");
        assert!(matches!(completed.data, TranscriptData::Chunked(_)));

        let retained = completed.into_owned();
        assert!(
            matches!(retained.data, TranscriptData::Chunked(_)),
            "retention must not coalesce chunks"
        );
        assert_eq!(retained.data.into_bytes(), Bytes::from_static(b"hello"));
    }

    #[test]
    fn split_records_round_trip_through_reassembly() {
        let len = MAX_RECORD_BYTES * 2 + MAX_RECORD_BYTES / 2;
        let data = Bytes::from((0..len).map(|i| i as u8).collect::<Vec<u8>>());
        let records =
            split_logical_record(7, RecordFormat::Transcript, data.clone()).expect("split");

        assert_eq!(records.len(), 3);
        for (index, part) in records.iter().enumerate() {
            assert_eq!(part.writer_seq_num, 7 + index as u64);
            assert_eq!(part.part.index(), index as u32);
            assert_eq!(part.part.is_final(), index == records.len() - 1);
            assert_eq!(part.format, RecordFormat::Transcript);
        }
        assert!(
            records[..records.len() - 1]
                .iter()
                .all(|part| part.data.len() == MAX_RECORD_BYTES)
        );

        let mut transcript = LogicalTranscript::new();
        let mut reassembled = None;
        for part in &records {
            let read = ReadRecord {
                seq_num: part.writer_seq_num,
                timestamp_ms: 0,
                writer_id: WriterId::from_bytes([1; WriterId::BYTE_LEN]),
                writer_seq_num: part.writer_seq_num,
                part: part.part,
                format: part.format,
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
        let records = split_logical_record(3, RecordFormat::Bytes, data.clone()).expect("split");
        let [record] = records.as_slice() else {
            panic!("expected one unsplit record");
        };
        assert_eq!(record.writer_seq_num, 3);
        assert_eq!(record.part, PartHeader::unsplit());
        // Same backing storage, not a copy.
        assert!(std::ptr::eq(record.data.as_ptr(), data.as_ptr()));

        let empty = split_logical_record(0, RecordFormat::Bytes, Bytes::new()).expect("split");
        assert_eq!(empty.len(), 1);
        assert!(empty[0].data.is_empty());
        assert_eq!(empty[0].part, PartHeader::unsplit());
    }

    #[test]
    fn exact_multiples_split_into_full_parts() {
        let data = Bytes::from(vec![0_u8; MAX_RECORD_BYTES * 2]);
        let records = split_logical_record(0, RecordFormat::Bytes, data).expect("split");
        assert_eq!(records.len(), 2);
        assert!(
            records
                .iter()
                .all(|part| part.data.len() == MAX_RECORD_BYTES)
        );
        assert!(!records[0].part.is_final());
        assert!(records[1].part.is_final());
    }

    #[test]
    fn split_rejects_writer_sequence_exhaustion() {
        assert!(matches!(
            split_logical_record(u64::MAX, RecordFormat::Bytes, Bytes::from_static(b"x")),
            Err(FrameCodecError::WriterSequenceExhausted)
        ));
        assert!(matches!(
            split_logical_record(
                u64::MAX - 1,
                RecordFormat::Bytes,
                Bytes::from(vec![0_u8; MAX_RECORD_BYTES + 1]),
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
            format: RecordFormat::Transcript,
            data,
        }
    }

    fn push<'a>(
        transcript: &mut LogicalTranscript,
        record: ReadRecord<'a>,
    ) -> Option<TranscriptRecord<'a>> {
        transcript.push_record(record).expect("push record")
    }

    fn assert_chunked_record(record: Option<TranscriptRecord>, expected: &'static [u8]) {
        let Some(record) = record else {
            panic!("expected transcript record");
        };
        assert_eq!(record.format, RecordFormat::Transcript);
        assert!(matches!(record.data, TranscriptData::Chunked(_)));
        assert_eq!(record.data.into_bytes(), Bytes::from_static(expected));
    }

    #[test]
    fn suppresses_reused_writer_sequences() {
        let mut transcript = LogicalTranscript::new();

        assert_eq!(
            push(&mut transcript, record(0, PartHeader::unsplit(), b"hello")),
            Some(TranscriptRecord {
                format: RecordFormat::Transcript,
                data: TranscriptData::Borrowed(b"hello")
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
    fn reassembles_split_records() {
        let mut transcript = LogicalTranscript::new();

        assert_eq!(
            push(
                &mut transcript,
                record(7, PartHeader::new(0, false).expect("part"), b"hel"),
            ),
            None
        );
        assert_chunked_record(
            push(
                &mut transcript,
                record(8, PartHeader::new(1, true).expect("part"), b"lo"),
            ),
            b"hello",
        );
    }

    #[test]
    fn chunked_transcript_data_advances_across_parts() {
        let mut data = TranscriptData::from_ordered_chunks(
            vec![Bytes::from_static(b"hel"), Bytes::from_static(b"lo")],
            5,
        );

        assert_eq!(data.remaining(), 5);
        assert_eq!(data.chunk(), b"hel");
        data.advance(2);
        assert_eq!(data.chunk(), b"l");
        data.advance(1);
        assert_eq!(data.chunk(), b"lo");
        data.advance(2);
        assert_eq!(data.remaining(), 0);
        assert_eq!(data.chunk(), b"");
    }

    #[test]
    fn partially_consumed_chunks_coalesce_remaining_bytes() {
        let mut data = TranscriptData::from_ordered_chunks(
            vec![
                Bytes::from_static(b"hel"),
                Bytes::from_static(b"lo "),
                Bytes::from_static(b"world"),
            ],
            11,
        );

        // Several chunks remain: the payload has to be copied out from the consumed offset.
        data.advance(4);
        assert_eq!(data.clone().into_bytes(), Bytes::from_static(b"o world"));

        // One chunk remains at offset zero, so it is shared as-is.
        data.advance(2);
        assert_eq!(data.clone().into_bytes(), Bytes::from_static(b"world"));

        // One chunk remains mid-way through, so the shared slice must start at the offset.
        data.advance(1);
        assert_eq!(data.clone().into_bytes(), Bytes::from_static(b"orld"));

        data.advance(4);
        assert_eq!(data.into_bytes(), Bytes::new());
    }

    #[test]
    fn drops_split_records_without_prefix() {
        let mut transcript = LogicalTranscript::new();

        assert_eq!(
            push(
                &mut transcript,
                record(8, PartHeader::new(1, true).expect("part"), b"lo"),
            ),
            None
        );
        assert_eq!(
            push(&mut transcript, record(9, PartHeader::unsplit(), b"next")),
            Some(TranscriptRecord {
                format: RecordFormat::Transcript,
                data: TranscriptData::Borrowed(b"next")
            })
        );
    }

    #[test]
    fn drops_split_records_after_gap() {
        let mut transcript = LogicalTranscript::new();

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
            Some(TranscriptRecord {
                format: RecordFormat::Transcript,
                data: TranscriptData::Borrowed(b"next")
            })
        );
    }

    #[test]
    fn tracks_writer_sequences_independently() {
        let mut transcript = LogicalTranscript::new();
        let first_writer = WriterId::from_bytes([1; WriterId::BYTE_LEN]);
        let second_writer = WriterId::from_bytes([2; WriterId::BYTE_LEN]);

        assert_eq!(
            push(
                &mut transcript,
                record_with_writer(first_writer, 0, PartHeader::unsplit(), b"first"),
            ),
            Some(TranscriptRecord {
                format: RecordFormat::Transcript,
                data: TranscriptData::Borrowed(b"first")
            })
        );
        assert_eq!(
            push(
                &mut transcript,
                record_with_writer(second_writer, 0, PartHeader::unsplit(), b"second"),
            ),
            Some(TranscriptRecord {
                format: RecordFormat::Transcript,
                data: TranscriptData::Borrowed(b"second")
            })
        );
    }

    #[test]
    fn rejects_unsplit_records_above_the_logical_limit() {
        let mut transcript = LogicalTranscript::with_max_logical_record_bytes(4);
        let error = transcript
            .push_record(record(0, PartHeader::unsplit(), b"hello"))
            .expect_err("logical record limit");

        assert_eq!(
            error,
            TranscriptError::LogicalRecordTooLarge { len: 5, max: 4 }
        );
    }

    #[test]
    fn explicit_logical_limit_can_hold_one_record_at_that_limit() {
        let raised_limit = DEFAULT_MAX_PENDING_BYTES + 1;
        let raised = LogicalTranscript::with_max_logical_record_bytes(raised_limit);
        assert_eq!(raised.limits.max_pending_bytes, raised_limit);

        let lowered = LogicalTranscript::with_max_logical_record_bytes(4);
        assert_eq!(lowered.limits.max_pending_bytes, DEFAULT_MAX_PENDING_BYTES);
    }

    #[test]
    fn rejects_split_records_above_the_logical_limit_and_resyncs() {
        let mut transcript = LogicalTranscript::with_max_logical_record_bytes(4);

        assert_eq!(
            push(
                &mut transcript,
                record(7, PartHeader::new(0, false).expect("part"), b"hel"),
            ),
            None
        );
        let error = transcript
            .push_record(record(8, PartHeader::new(1, true).expect("part"), b"lo"))
            .expect_err("logical record limit");
        assert_eq!(
            error,
            TranscriptError::LogicalRecordTooLarge { len: 5, max: 4 }
        );
        assert_eq!(
            push(&mut transcript, record(9, PartHeader::unsplit(), b"next")),
            Some(TranscriptRecord {
                format: RecordFormat::Transcript,
                data: TranscriptData::Borrowed(b"next")
            })
        );
    }

    #[test]
    fn rejects_new_writer_identities_above_the_configured_limit() {
        let mut transcript = LogicalTranscript::with_limits(TranscriptLimits::new(16, 2, 16, 16));

        for writer_byte in [1, 2] {
            assert!(
                push(
                    &mut transcript,
                    record_with_writer(
                        WriterId::from_bytes([writer_byte; WriterId::BYTE_LEN]),
                        0,
                        PartHeader::unsplit(),
                        b"ok",
                    ),
                )
                .is_some()
            );
        }

        let error = transcript
            .push_record(record_with_writer(
                WriterId::from_bytes([3; WriterId::BYTE_LEN]),
                0,
                PartHeader::unsplit(),
                b"rejected",
            ))
            .expect_err("writer-state limit");
        assert_eq!(
            error,
            TranscriptError::WriterStateLimitExceeded { actual: 3, max: 2 }
        );
        assert_eq!(transcript.writers.len(), 2);
    }

    #[test]
    fn bounds_aggregate_pending_bytes_across_writers_and_releases_completed_state() {
        let mut transcript = LogicalTranscript::with_limits(TranscriptLimits::new(16, 4, 4, 16));
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
        assert_eq!(transcript.pending_bytes, 3);
        assert_eq!(transcript.pending_parts, 1);

        let error = transcript
            .push_record(record_with_writer(
                second_writer,
                0,
                PartHeader::new(0, false).expect("part"),
                b"de",
            ))
            .expect_err("aggregate pending-byte limit");
        assert_eq!(
            error,
            TranscriptError::PendingBytesLimitExceeded { actual: 5, max: 4 }
        );
        assert_eq!(transcript.pending_bytes, 3);

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
        assert_eq!(transcript.pending_bytes, 0);
        assert_eq!(transcript.pending_parts, 0);

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
        assert_eq!(transcript.pending_bytes, 4);
        assert_eq!(transcript.pending_parts, 1);
    }

    #[test]
    fn bounds_aggregate_pending_parts_including_empty_parts() {
        let mut transcript = LogicalTranscript::with_limits(TranscriptLimits::new(16, 2, 16, 2));
        let first_writer = WriterId::from_bytes([1; WriterId::BYTE_LEN]);
        let second_writer = WriterId::from_bytes([2; WriterId::BYTE_LEN]);

        assert_eq!(
            push(
                &mut transcript,
                record_with_writer(
                    first_writer,
                    0,
                    PartHeader::new(0, false).expect("part"),
                    b"",
                ),
            ),
            None
        );
        assert_eq!(
            push(
                &mut transcript,
                record_with_writer(
                    second_writer,
                    0,
                    PartHeader::new(0, false).expect("part"),
                    b"",
                ),
            ),
            None
        );
        assert_eq!(transcript.pending_bytes, 0);
        assert_eq!(transcript.pending_parts, 2);

        let error = transcript
            .push_record(record_with_writer(
                first_writer,
                1,
                PartHeader::new(1, false).expect("part"),
                b"",
            ))
            .expect_err("aggregate pending-part limit");
        assert_eq!(
            error,
            TranscriptError::PendingPartsLimitExceeded { actual: 3, max: 2 }
        );
        assert_eq!(transcript.pending_bytes, 0);
        assert_eq!(transcript.pending_parts, 1);

        assert_eq!(
            push(
                &mut transcript,
                record_with_writer(
                    second_writer,
                    1,
                    PartHeader::new(1, true).expect("part"),
                    b"",
                ),
            ),
            Some(TranscriptRecord {
                format: RecordFormat::Transcript,
                data: TranscriptData::Borrowed(b""),
            })
        );
        assert_eq!(transcript.pending_parts, 0);
    }

    #[test]
    fn pending_part_limit_does_not_prevent_completion_at_the_boundary() {
        let mut transcript = LogicalTranscript::with_limits(TranscriptLimits::new(16, 1, 16, 1));

        assert_eq!(
            push(
                &mut transcript,
                record(0, PartHeader::new(0, false).expect("part"), b"a"),
            ),
            None
        );
        assert_eq!(transcript.pending_parts, 1);
        assert_chunked_record(
            push(
                &mut transcript,
                record(1, PartHeader::new(1, true).expect("part"), b"b"),
            ),
            b"ab",
        );
        assert_eq!(transcript.pending_parts, 0);
    }

    #[test]
    fn malformed_sequence_releases_its_aggregate_pending_bytes() {
        let mut transcript = LogicalTranscript::with_limits(TranscriptLimits::new(16, 2, 4, 16));

        assert_eq!(
            push(
                &mut transcript,
                record(0, PartHeader::new(0, false).expect("part"), b"abc"),
            ),
            None
        );
        assert_eq!(transcript.pending_bytes, 3);
        assert_eq!(transcript.pending_parts, 1);
        assert_eq!(
            push(
                &mut transcript,
                record(2, PartHeader::new(2, true).expect("part"), b"d"),
            ),
            None
        );
        assert_eq!(transcript.pending_bytes, 0);
        assert_eq!(transcript.pending_parts, 0);
    }
}
