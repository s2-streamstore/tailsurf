//! Transport-neutral options for TSF stream reads.

use crate::{LinkSecret, StreamId};

/// Tail-relative start used when a reader does not select a position.
pub const DEFAULT_READ_TAIL_OFFSET: u64 = 0;
/// Slowest accepted timestamp playback rate.
pub const MIN_PLAYBACK_RATE: f64 = 0.1;
/// Fastest accepted timestamp playback rate.
pub const MAX_PLAYBACK_RATE: f64 = 100.0;
/// Longest explicit S2 tail wait accepted by TSF.
pub const MAX_READ_WAIT_SECONDS: u32 = 60;

/// Position, bounds, and credentials for a stream read.
#[derive(Clone, Debug)]
pub struct ReadOptions {
    /// Stream to read.
    pub stream_id: StreamId,
    /// Optional initial read position. No value sends a tail offset of zero.
    pub start: Option<ReadStart>,
    /// Optional maximum number of physical records to deliver.
    pub count: Option<u64>,
    /// Optional exclusive ending timestamp in Unix epoch milliseconds.
    pub until_timestamp_ms: Option<u64>,
    /// Optional timestamp playback multiplier. `1.0` is recorded speed.
    pub rate: Option<f64>,
    /// Optional seconds to wait at the tail before ending this connection.
    pub wait_seconds: Option<u32>,
    /// Secret from a read-capable stream link for private streams.
    pub link_secret: Option<LinkSecret>,
}

impl ReadOptions {
    /// Creates unbounded read options using the service's default start position.
    pub fn new(stream_id: StreamId) -> Self {
        Self {
            stream_id,
            start: None,
            count: None,
            until_timestamp_ms: None,
            rate: None,
            wait_seconds: None,
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
