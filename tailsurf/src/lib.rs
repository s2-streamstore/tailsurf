//! Async Rust SDK and shared TSF v3 protocol types for [tail.surf](https://tail.surf).
//!
//! Use [`TsfClient`] for control-plane requests, [`TsfProducer`] for durable reconnecting writes, and [`TsfReadSession`] for resumable reads. Share URLs and transcript reconstruction are available in [`stream_url`] and [`transcript`].

/// HTTP and WebSocket clients, retry policy, and durable producer types.
pub mod client;
/// Stream, token, bearer-token, and writer identifiers.
pub mod ids;
/// Stream-token permission parsing and validation.
pub mod permissions;
/// TSF REST models and v3 binary WebSocket protocol types.
pub mod protocol;
/// Human-facing stream share URL parsing and construction.
pub mod stream_url;
/// Duplicate suppression and split-record transcript reconstruction.
pub mod transcript;

pub use client::{
    AppendAck, AppendReceipt, AppendTicket, CreateStreamIdempotencyKey, IntoRecordData,
    InvalidCreateStreamIdempotencyKey, RetryPolicy, TsfAppendSession, TsfClient, TsfClientConfig,
    TsfClientError, TsfProducer, TsfProducerConfig, TsfReadSession, WritePermit, WriteRecord,
    default_api_base_url,
};
pub use ids::{BearerToken, StreamId, TokenId, WriterId};
pub use permissions::{PermissionsError, TokenPermissions};
pub use protocol::ws::frame::{ReadRecord, ReadTail};
