//! WebSocket write options and TSF v1 binary frames.

/// Binary TSF v1 frames and their codec.
pub mod frame;

/// Interval between WebSocket heartbeats while a reader is otherwise idle.
pub const WEBSOCKET_HEARTBEAT_INTERVAL_MS: u64 = 20_000;
/// Maximum accounted payload bytes an SDK durable writer keeps sent but unacknowledged.
///
/// Empty payloads count as one byte.
pub const MAX_WRITER_IN_FLIGHT_BYTES: usize = 5 * 1024 * 1024;
/// Maximum physical records an SDK durable writer keeps sent but unacknowledged.
pub const MAX_WRITER_IN_FLIGHT_RECORDS: usize = 1_024;

use crate::{ClientWriterId, LinkSecret, StreamId};

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
