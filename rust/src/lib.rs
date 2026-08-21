#![doc = include_str!("../README.md")]

/// REST, SSE, and WebSocket clients and durable writer types.
pub mod client;
/// Stream and link IDs, link secrets, and writer identities.
pub mod ids;
/// Stream-link permission parsing and validation.
pub mod permissions;
/// TSF read options, REST models, and v1 binary WebSocket protocol types.
pub mod protocol;
/// User-provided stream titles.
pub mod stream_title;
/// Human-facing stream link parsing and construction.
pub mod stream_url;
/// Duplicate suppression and split-record transcript reconstruction.
pub mod transcript;

pub use client::{
    AppendAck, AppendReceipt, AppendTicket, DurableWriterOptions, IdempotencyKey,
    InvalidIdempotencyKey, ListLinksOptions, TsfClient, TsfClientConfig, TsfClientError,
    TsfProducer, TsfReadSession, TsfSseReadSession, TsfWriteSession, TsfWriter, default_api_origin,
};
pub use ids::{
    ClientWriterId, LinkId, LinkIdError, LinkSecret, LinkSecretError, MAX_LINK_ID_LEN, StreamId,
    WriterId,
};
pub use permissions::{LinkPermissions, PermissionsError};
pub use protocol::{
    read::{ReadOptions, ReadStart, ReadStop},
    rest::{
        AppendRange, CreateLinkInput, CreateStreamRequest, CreateStreamResponse, InitialStreamLink,
        ListLinksResponse, MAX_INITIAL_STREAM_LINKS, StreamLinkCredential, StreamLinkSummary,
        StreamMetadata, UpdateStreamRequest, Visibility,
    },
    ws::{
        MAX_WRITER_IN_FLIGHT_ACCOUNTED_BYTES, MAX_WRITER_IN_FLIGHT_RECORDS, WriteStreamOptions,
        frame::{
            AppendBatch, AppendRecord, CaughtUpPosition, IntoRecordData, OwnedReadRecord,
            PartHeader, ReadBatch, ReadRecord, RecordFormat, RecordPayload,
        },
    },
};
pub use stream_title::{MAX_STREAM_TITLE_CODE_POINTS, StreamTitle, StreamTitleError};
