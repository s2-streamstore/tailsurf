//! Language-neutral TSF REST, SSE, and v1 WebSocket models.

/// Largest integer TSF accepts where JavaScript interoperability requires exact values.
pub const MAX_SAFE_INTEGER_U64: u64 = 9_007_199_254_740_991;

/// Transport-neutral stream read options.
pub mod read;
/// JSON request and response models for `/api/v1` REST endpoints.
pub mod rest;
/// Versioned terminal input and output event payloads.
pub mod terminal;
/// WebSocket write options and binary frame codec.
pub mod ws;
