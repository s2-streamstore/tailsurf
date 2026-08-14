//! Async Rust SDK and shared TSF v1 protocol types for [tail.surf](https://tail.surf).
//!
//! Use [`TsfClient`] for control-plane requests, [`TsfWriter`] for durable reconnecting writes,
//! and [`TsfReadSession`] for resumable reads. Stream links and transcript reconstruction are
//! available in [`stream_url`] and [`transcript`].

/// HTTP and WebSocket clients, retry policy, and durable writer types.
pub mod client;
/// Stream and link IDs, link secrets, and writer identities.
pub mod ids;
/// Stream-link permission parsing and validation.
pub mod permissions;
/// TSF REST models and v1 binary WebSocket protocol types.
pub mod protocol;
/// User-provided stream titles.
pub mod stream_title;
/// Human-facing stream link parsing and construction.
pub mod stream_url;
/// Duplicate suppression and split-record transcript reconstruction.
pub mod transcript;

pub use client::{
    AppendAck, AppendReceipt, AppendTicket, CreateStreamIdempotencyKey, IntoRecordData,
    InvalidCreateStreamIdempotencyKey, ListLinksOptions, MAX_WRITER_UNACKED_PAYLOAD_BYTES,
    MAX_WRITER_UNACKED_RECORDS, RetryPolicy, TsfClient, TsfClientConfig, TsfClientError,
    TsfReadSession, TsfSseReadSession, TsfWriteSession, TsfWriter, TsfWriterConfig, WritePermit,
    default_api_origin,
};
pub use ids::{
    ClientWriterId, LinkId, LinkIdError, LinkSecret, MAX_LINK_ID_LEN, StreamId, WriterId,
};
pub use permissions::{LinkPermissions, PermissionsError};
pub use protocol::{
    rest::StreamMetadata,
    ws::frame::{AppendRecord, CaughtUpPosition, ReadRecord, SnapshotBoundary},
};
pub use stream_title::{MAX_STREAM_TITLE_CODE_POINTS, StreamTitle, StreamTitleError};
