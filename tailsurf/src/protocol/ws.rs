//! WebSocket connection options for TSF v3 readers and writers.

/// Binary TSF v3 frames and their codec.
pub mod frame;

use crate::{LinkSecret, StreamId, WriterId};

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
    /// Secret from a read-capable stream link for private streams.
    pub link_secret: Option<LinkSecret>,
}

impl ReadStreamOptions {
    /// Creates unbounded read options using the service's default start position.
    pub fn new(stream_id: StreamId) -> Self {
        Self {
            stream_id,
            start: None,
            count: None,
            until: None,
            link_secret: None,
        }
    }

    /// Sets an owned stream link secret.
    pub fn with_link_secret(mut self, link_secret: impl Into<LinkSecret>) -> Self {
        self.link_secret = Some(link_secret.into());
        self
    }

    /// Sets a cloned stream link without exposing its secret value.
    pub fn with_stream_link(self, link: &LinkSecret) -> Self {
        self.with_link_secret(link.clone())
    }

    pub(crate) fn query_pairs(&self) -> Vec<(&'static str, String)> {
        let mut pairs = Vec::new();
        if self.link_secret.is_some() {
            pairs.push(("auth", "link".to_owned()));
        }
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
    /// Start this many records before the current tail, saturating at the stream start.
    TailOffset(u64),
}

/// Stream, writer identity, and authorization for one write WebSocket.
#[derive(Clone, Debug)]
pub struct WriteStreamOptions {
    /// Stream to append to.
    pub stream_id: StreamId,
    /// Stable writer identity reused with sequence numbers across reconnects.
    pub writer_id: WriterId,
    /// Secret from a write-capable stream link.
    pub link_secret: LinkSecret,
}

impl WriteStreamOptions {
    /// Creates write options from an owned stream link secret.
    pub fn new(
        stream_id: StreamId,
        writer_id: WriterId,
        link_secret: impl Into<LinkSecret>,
    ) -> Self {
        Self {
            stream_id,
            writer_id,
            link_secret: link_secret.into(),
        }
    }

    /// Creates write options by cloning a stream link without exposing it.
    pub fn with_stream_link(stream_id: StreamId, writer_id: WriterId, link: &LinkSecret) -> Self {
        Self::new(stream_id, writer_id, link.clone())
    }
}
