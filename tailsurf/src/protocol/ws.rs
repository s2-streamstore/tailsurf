//! WebSocket connection options for TSF v3 readers and writers.

/// Binary TSF v3 frames and their codec.
pub mod frame;

use crate::{BearerToken, StreamId, WriterId};

/// Position, bounds, and authorization for one read WebSocket.
#[derive(Clone, Debug)]
pub struct ReadStreamOptions {
    /// Stream to read.
    pub stream_id: StreamId,
    /// Optional initial read position. No value uses the service default tail offset.
    pub start: Option<ReadStart>,
    /// Optional maximum number of physical records to deliver.
    pub count: Option<u64>,
    /// Optional inclusive ending S2 sequence number.
    pub until: Option<u64>,
    /// Account or read-capable stream bearer token for private streams.
    pub bearer_token: Option<BearerToken>,
}

impl ReadStreamOptions {
    /// Creates unbounded read options using the service's default start position.
    pub fn new(stream_id: StreamId) -> Self {
        Self {
            stream_id,
            start: None,
            count: None,
            until: None,
            bearer_token: None,
        }
    }

    /// Sets an owned account or stream bearer token.
    pub fn with_bearer_token(mut self, bearer_token: impl Into<BearerToken>) -> Self {
        self.bearer_token = Some(bearer_token.into());
        self
    }

    /// Sets a cloned stream token without exposing its secret value.
    pub fn with_stream_token(self, token: &BearerToken) -> Self {
        self.with_bearer_token(token.clone())
    }

    pub(crate) fn query_pairs(&self) -> Vec<(&'static str, String)> {
        let mut pairs = Vec::new();
        match self.start {
            None => {}
            Some(ReadStart::SeqNum(seq_num)) => pairs.push(("seq_num", seq_num.to_string())),
            Some(ReadStart::TimestampMs(timestamp)) => {
                pairs.push(("timestamp", timestamp.to_string()));
            }
            Some(ReadStart::TailOffset(tail_offset)) => {
                pairs.push(("tail_offset", tail_offset.to_string()));
            }
        }
        if let Some(count) = self.count {
            pairs.push(("count", count.to_string()));
        }
        if let Some(until) = self.until {
            pairs.push(("until", until.to_string()));
        }
        pairs
    }
}

/// Initial read position. At most one selector can be sent per connection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadStart {
    /// First record whose S2 sequence number is at least this value.
    SeqNum(u64),
    /// First record whose timestamp is at least this Unix epoch millisecond value.
    TimestampMs(u64),
    /// Start this many records before the current tail, saturating at retained history.
    TailOffset(u64),
}

/// Stream, writer identity, and authorization for one write WebSocket.
#[derive(Clone, Debug)]
pub struct WriteStreamOptions {
    /// Stream to append to.
    pub stream_id: StreamId,
    /// Stable writer identity reused with sequence numbers across reconnects.
    pub writer_id: WriterId,
    /// Account or write-capable stream bearer token.
    pub bearer_token: BearerToken,
}

impl WriteStreamOptions {
    /// Creates write options from an owned account or stream bearer token.
    pub fn new(
        stream_id: StreamId,
        writer_id: WriterId,
        bearer_token: impl Into<BearerToken>,
    ) -> Self {
        Self {
            stream_id,
            writer_id,
            bearer_token: bearer_token.into(),
        }
    }

    /// Creates write options by cloning a stream token without exposing it.
    pub fn with_stream_token(
        stream_id: StreamId,
        writer_id: WriterId,
        token: &BearerToken,
    ) -> Self {
        Self::new(stream_id, writer_id, token.clone())
    }
}
