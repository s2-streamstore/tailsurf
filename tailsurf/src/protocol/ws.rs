//! WebSocket connection options for TSF v1 readers and writers.

/// Binary TSF v1 frames and their codec.
pub mod frame;

use crate::{ClientWriterId, LinkSecret, StreamId};

/// Largest read selector accepted by the current TypeScript data adapter.
pub const MAX_READ_SELECTOR_VALUE: u64 = 9_007_199_254_740_991;
/// Tail-relative start used when a reader does not select a position.
pub const DEFAULT_READ_TAIL_OFFSET: u64 = 80;
/// Slowest accepted timestamp playback rate.
pub const MIN_PLAYBACK_RATE_PERMILLE: u64 = 100;
/// Fastest accepted timestamp playback rate.
pub const MAX_PLAYBACK_RATE_PERMILLE: u64 = 100_000;

/// Position, bounds, and credentials for one read WebSocket.
#[derive(Clone, Debug)]
pub struct ReadStreamOptions {
    /// Stream to read.
    pub stream_id: StreamId,
    /// Optional initial read position. No value sends a tail offset of 80.
    pub start: Option<ReadStart>,
    /// Optional maximum number of physical records to deliver.
    pub limit: Option<u64>,
    /// Optional exclusive ending sequence number.
    pub end_seq_num: Option<u64>,
    /// Optional timestamp playback rate in thousandths. `1000` is recorded speed.
    pub playback_rate_permille: Option<u64>,
    /// Captures a fixed ending position when the socket opens.
    pub snapshot: bool,
    /// Secret from a read-capable stream link for private streams.
    pub link_secret: Option<LinkSecret>,
}

impl ReadStreamOptions {
    /// Creates unbounded read options using the service's default start position.
    pub fn new(stream_id: StreamId) -> Self {
        Self {
            stream_id,
            start: None,
            limit: None,
            end_seq_num: None,
            playback_rate_permille: None,
            snapshot: false,
            link_secret: None,
        }
    }

    /// Sets an owned stream link secret.
    pub fn with_link_secret(mut self, link_secret: impl Into<LinkSecret>) -> Self {
        self.link_secret = Some(link_secret.into());
        self
    }
}

/// Initial read position. At most one selector can be sent per connection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadStart {
    /// First record whose absolute sequence number is at least this value.
    SeqNum(u64),
    /// First record whose timestamp is at least this Unix epoch millisecond value.
    TimestampMs(u64),
    /// Start this many records before the current tail, saturating at the stream start.
    TailOffset(u64),
}

/// Stream, client writer identity, and credentials for one write WebSocket.
#[derive(Clone, Debug)]
pub struct WriteStreamOptions {
    /// Stream to append to.
    pub stream_id: StreamId,
    /// Stable client writer identity reused with sequence numbers across reconnects.
    pub client_writer_id: ClientWriterId,
    /// Secret from a write-capable stream link.
    pub link_secret: LinkSecret,
    /// Initial stream sequence precondition for this writer session.
    pub expected_next_seq_num: Option<u64>,
}

impl WriteStreamOptions {
    /// Creates write options from an owned stream link secret.
    pub fn new(
        stream_id: StreamId,
        client_writer_id: ClientWriterId,
        link_secret: impl Into<LinkSecret>,
    ) -> Self {
        Self {
            stream_id,
            client_writer_id,
            link_secret: link_secret.into(),
            expected_next_seq_num: None,
        }
    }

    /// Requires the stream to start this writer session at the supplied sequence.
    pub fn with_expected_next_seq_num(mut self, expected_next_seq_num: u64) -> Self {
        self.expected_next_seq_num = Some(expected_next_seq_num);
        self
    }
}
