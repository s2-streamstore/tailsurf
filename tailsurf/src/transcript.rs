//! Logical transcript reconstruction from physical TSF read records.

use std::collections::HashMap;

use bytes::{Buf, Bytes, BytesMut};

use crate::{
    WriterId,
    protocol::ws::frame::{MAX_RECORD_BYTES, PartHeader, ReadRecord, RecordFormat},
};

/// Default maximum reassembled logical-record size: 16 MiB.
pub const DEFAULT_MAX_LOGICAL_RECORD_BYTES: usize = MAX_RECORD_BYTES * 32;
/// Default maximum writer identities retained for deduplication and reassembly.
pub const DEFAULT_MAX_WRITER_STATES: usize = 4_096;
/// Default maximum payload bytes retained across all unfinished split records: 64 MiB.
pub const DEFAULT_MAX_PENDING_BYTES: usize = 64 * 1024 * 1024;
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
/// the next complete logical record.
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
    pub fn with_max_logical_record_bytes(max_logical_record_bytes: usize) -> Self {
        Self::with_limits(TranscriptLimits {
            max_logical_record_bytes,
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
    /// duplicate, an incomplete split part, or a malformed partial sequence.
    pub fn push_record(
        &mut self,
        record: ReadRecord,
    ) -> Result<Option<TranscriptRecord>, TranscriptError> {
        let limits = self.limits;
        if !self.writers.contains_key(&record.writer_id)
            && self.writers.len() >= limits.max_writer_states
        {
            return Err(TranscriptError::WriterStateLimitExceeded {
                actual: self.writers.len().saturating_add(1),
                max: limits.max_writer_states,
            });
        }

        let writer = self.writers.entry(record.writer_id).or_default();
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
            return Ok(Some(TranscriptRecord {
                format: record.format,
                data: TranscriptData::from(record.data),
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
                pending.chunks.push(record.data);
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
            pending.chunks.push(record.data);
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

/// One complete logical transcript record after deduplication and reassembly.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TranscriptRecord {
    /// Presentation hint shared by every physical part.
    pub format: RecordFormat,
    /// Exact logical payload, possibly retained as zero-copy chunks.
    pub data: TranscriptData,
}

/// Logical payload represented as one contiguous value or multiple zero-copy chunks.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TranscriptData {
    /// Contiguous payload bytes.
    Single(Bytes),
    /// Ordered non-empty physical chunks.
    Chunked(ChunkedBytes),
}

impl TranscriptData {
    /// Creates contiguous transcript data from a static byte slice.
    pub fn from_static(data: &'static [u8]) -> Self {
        Self::Single(Bytes::from_static(data))
    }

    /// Returns the number of bytes not consumed through the [`Buf`] implementation.
    pub fn len(&self) -> usize {
        self.remaining()
    }

    /// Returns whether no unconsumed bytes remain.
    pub fn is_empty(&self) -> bool {
        !self.has_remaining()
    }

    /// Coalesces the remaining payload into contiguous bytes when necessary.
    pub fn into_bytes(self) -> Bytes {
        match self {
            Self::Single(bytes) => bytes,
            Self::Chunked(chunked) => chunked.into_bytes(),
        }
    }

    fn from_ordered_chunks(chunks: Vec<Bytes>, len: usize) -> Self {
        match chunks.len() {
            0 => Self::Single(Bytes::new()),
            1 => Self::Single(chunks.into_iter().next().expect("single chunk")),
            _ => Self::Chunked(ChunkedBytes::new(chunks, len)),
        }
    }
}

impl From<Bytes> for TranscriptData {
    fn from(bytes: Bytes) -> Self {
        Self::Single(bytes)
    }
}

impl Buf for TranscriptData {
    fn remaining(&self) -> usize {
        match self {
            Self::Single(bytes) => bytes.len(),
            Self::Chunked(chunked) => chunked.remaining(),
        }
    }

    fn chunk(&self) -> &[u8] {
        match self {
            Self::Single(bytes) => bytes.as_ref(),
            Self::Chunked(chunked) => chunked.chunk(),
        }
    }

    fn advance(&mut self, cnt: usize) {
        match self {
            Self::Single(bytes) => bytes.advance(cnt),
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

    fn record(seq: u64, part: PartHeader, data: &[u8]) -> ReadRecord {
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
    ) -> ReadRecord {
        ReadRecord {
            seq_num: seq,
            timestamp_ms: seq,
            writer_id,
            writer_seq_num: seq,
            part,
            format: RecordFormat::Transcript,
            data: Bytes::copy_from_slice(data),
        }
    }

    fn push(transcript: &mut LogicalTranscript, record: ReadRecord) -> Option<TranscriptRecord> {
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
                data: TranscriptData::from_static(b"hello")
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
                data: TranscriptData::from_static(b"next")
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
                data: TranscriptData::from_static(b"next")
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
                data: TranscriptData::from_static(b"first")
            })
        );
        assert_eq!(
            push(
                &mut transcript,
                record_with_writer(second_writer, 0, PartHeader::unsplit(), b"second"),
            ),
            Some(TranscriptRecord {
                format: RecordFormat::Transcript,
                data: TranscriptData::from_static(b"second")
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
                data: TranscriptData::from_static(b"next")
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
                data: TranscriptData::from_static(b""),
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
