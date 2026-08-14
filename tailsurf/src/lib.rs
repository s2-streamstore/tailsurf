//! Async Rust SDK and shared TSF v1 protocol types for [tail.surf](https://tail.surf).
//!
//! Use [`TsfClient`] for control-plane requests, [`TsfProducer`] for durable reconnecting writes,
//! and [`TsfReadSession`] for resumable reads. Stream links and transcript reconstruction are
//! available in [`stream_url`] and [`transcript`].

/// HTTP and WebSocket clients, retry policy, and durable producer types.
pub mod client;
/// Stream and link IDs, link secrets, and writer identities.
pub mod ids;
/// User-provided stream link labels.
pub mod link_label;
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
    InvalidCreateStreamIdempotencyKey, MAX_PRODUCER_UNACKED_PAYLOAD_BYTES,
    MAX_PRODUCER_UNACKED_RECORDS, RetryPolicy, TsfAppendSession, TsfClient, TsfClientConfig,
    TsfClientError, TsfProducer, TsfProducerConfig, TsfReadSession, TsfSseReadSession, WritePermit,
    WriteRecord, default_api_base_url,
};
pub use ids::{LinkId, LinkSecret, StreamId, WriterId};
pub use link_label::{LinkLabel, LinkLabelError, MAX_LINK_LABEL_CODE_POINTS};
pub use permissions::{LinkPermissions, PermissionsError};
pub use protocol::{
    rest::StreamInfoResponse,
    ws::frame::{AppendRecord, ReadCaughtUp, ReadRecord, ReadSnapshotBoundary, ReadStreamInfo},
};
pub use stream_title::{MAX_STREAM_TITLE_CODE_POINTS, StreamTitle, StreamTitleError};
