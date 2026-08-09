//! Exact binary codec for one-frame-per-message `tsf.v3` WebSocket traffic.

use bytes::{Bytes, BytesMut};

use crate::{BearerToken, WriterId};
use secrecy::ExposeSecret;

/// Numeric protocol version sent in [`ServerFrame::Hello`].
pub type ProtocolVersion = u16;

/// Protocol version implemented by this crate.
pub const TSF_V3: ProtocolVersion = 3;
/// WebSocket subprotocol offered and selected for TSF v3 connections.
pub const TSF_WS_PROTOCOL: &str = "tsf.v3";
/// Maximum data payload in one physical record.
pub const MAX_RECORD_BYTES: usize = 512 * 1024;

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ClientOp {
    AuthRead = 0x01,
    AuthWrite = 0x02,
    AppendRecord = 0x03,
}

impl ClientOp {
    const fn byte(self) -> u8 {
        self as u8
    }
}

impl TryFrom<u8> for ClientOp {
    type Error = FrameCodecError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            value if value == Self::AuthRead.byte() => Ok(Self::AuthRead),
            value if value == Self::AuthWrite.byte() => Ok(Self::AuthWrite),
            value if value == Self::AppendRecord.byte() => Ok(Self::AppendRecord),
            other => Err(FrameCodecError::UnknownOperation(other)),
        }
    }
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ServerOp {
    Hello = 0x80,
    AuthRequired = 0x81,
    Ack = 0x82,
    ReadRecord = 0x83,
    Heartbeat = 0x84,
    ReconnectAdvised = 0x85,
    ReadTail = 0x86,
}

impl ServerOp {
    const fn byte(self) -> u8 {
        self as u8
    }
}

impl TryFrom<u8> for ServerOp {
    type Error = FrameCodecError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            value if value == Self::Hello.byte() => Ok(Self::Hello),
            value if value == Self::AuthRequired.byte() => Ok(Self::AuthRequired),
            value if value == Self::Ack.byte() => Ok(Self::Ack),
            value if value == Self::ReadRecord.byte() => Ok(Self::ReadRecord),
            value if value == Self::Heartbeat.byte() => Ok(Self::Heartbeat),
            value if value == Self::ReconnectAdvised.byte() => Ok(Self::ReconnectAdvised),
            value if value == Self::ReadTail.byte() => Ok(Self::ReadTail),
            other => Err(FrameCodecError::UnknownOperation(other)),
        }
    }
}

/// Packed split-record part index and final-part marker.
///
/// The high bit marks the final part and the low 31 bits contain the zero-based part index. An unsplit record is encoded as index zero with the final bit set.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PartHeader(u32);

impl PartHeader {
    /// Bit used to mark the final physical part of a logical record.
    pub const FINAL_BIT: u32 = 0x8000_0000;
    /// Largest part index representable in the remaining 31 bits.
    pub const MAX_INDEX: u32 = 0x7fff_ffff;

    /// Packs a validated zero-based index and final marker.
    pub fn new(index: u32, is_final: bool) -> Result<Self, FrameCodecError> {
        if index > Self::MAX_INDEX {
            return Err(FrameCodecError::PartIndexTooLarge(index));
        }

        let final_bit = if is_final { Self::FINAL_BIT } else { 0 };
        Ok(Self(final_bit | index))
    }

    /// Returns the canonical header for a complete unsplit logical record.
    pub const fn unsplit() -> Self {
        Self(Self::FINAL_BIT)
    }

    /// Creates a header from its exact wire representation.
    pub const fn from_raw(raw: u32) -> Self {
        Self(raw)
    }

    /// Returns the exact packed wire representation.
    pub const fn raw(self) -> u32 {
        self.0
    }

    /// Returns the zero-based part index.
    pub const fn index(self) -> u32 {
        self.0 & Self::MAX_INDEX
    }

    /// Returns whether this physical record is the final logical part.
    pub const fn is_final(self) -> bool {
        self.0 & Self::FINAL_BIT != 0
    }
}

/// Presentation hint carried with each record without transforming its bytes.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum RecordFormat {
    /// Opaque binary bytes.
    #[default]
    Bytes = 0x00,
    /// Transcript-oriented bytes suitable for text-first presentation.
    Transcript = 0x01,
}

impl RecordFormat {
    /// Returns the one-byte wire representation.
    pub const fn byte(self) -> u8 {
        self as u8
    }
}

impl TryFrom<u8> for RecordFormat {
    type Error = FrameCodecError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            value if value == Self::Bytes.byte() => Ok(Self::Bytes),
            value if value == Self::Transcript.byte() => Ok(Self::Transcript),
            other => Err(FrameCodecError::UnknownRecordFormat(other)),
        }
    }
}

/// One physical stream record delivered by the read data plane.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadRecord {
    /// Durable sequence number assigned by S2.
    pub s2_seq_num: u64,
    /// S2 record timestamp as Unix epoch milliseconds.
    pub timestamp_ms: u64,
    /// Stable identity of the producer that wrote this record.
    pub writer_id: WriterId,
    /// Writer-local sequence number reused if this record is retransmitted.
    pub writer_seq_num: u64,
    /// Logical split-part metadata.
    pub part: PartHeader,
    /// Presentation hint for the payload.
    pub format: RecordFormat,
    /// Exact record payload bytes.
    pub data: Bytes,
}

/// Tail position observed by a read session after it catches up.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReadTail {
    /// Sequence number assigned to the next appended record.
    pub next_s2_seq_num: u64,
    /// Timestamp of the last record, or zero for an empty stream.
    pub timestamp_ms: u64,
}

/// Frame sent from a reader or writer to the service.
#[derive(Clone, Debug)]
pub enum ClientFrame {
    /// Authenticates a private read connection.
    AuthRead {
        /// Account or read-capable stream bearer token.
        bearer_token: BearerToken,
    },
    /// Authenticates a write connection and establishes writer identity.
    AuthWrite {
        /// Stable identity reused across reconnects.
        writer_id: WriterId,
        /// Account or write-capable stream bearer token.
        bearer_token: BearerToken,
    },
    /// Submits one physical record for durable append.
    AppendRecord {
        /// Writer-local sequence number reused on retransmission.
        writer_seq_num: u64,
        /// Logical split-part metadata.
        part: PartHeader,
        /// Presentation hint for the payload.
        format: RecordFormat,
        /// Exact record payload bytes.
        data: Bytes,
    },
}

/// Frame sent from the service to a reader or writer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ServerFrame {
    /// Confirms successful authorization and selected protocol version.
    Hello {
        /// Selected TSF protocol version.
        version: ProtocolVersion,
    },
    /// Requests a reader authentication frame before streaming records.
    AuthRequired,
    /// Confirms a contiguous range of writer records is durable.
    Ack {
        /// First acknowledged writer-local sequence number.
        writer_seq_start: u64,
        /// Last acknowledged writer-local sequence number, inclusive.
        writer_seq_end: u64,
        /// S2 sequence number assigned to the first acknowledged record.
        s2_seq_start: u64,
        /// S2 sequence number assigned to the last acknowledged record, inclusive.
        s2_seq_end: u64,
    },
    /// Delivers one physical stream record.
    ReadRecord(ReadRecord),
    /// Keeps an otherwise idle unbounded read connection active.
    Heartbeat,
    /// Requests a resumable reader reconnect before the deadline.
    ReconnectAdvised {
        /// Advisory reconnect deadline in seconds.
        deadline_secs: u8,
    },
    /// Reports the latest tail observed by the underlying read session.
    ReadTail(ReadTail),
}

impl ClientFrame {
    /// Encodes one client frame into a complete WebSocket binary message.
    pub fn encode(&self) -> Result<Bytes, FrameCodecError> {
        let mut output = BytesMut::new();

        match self {
            Self::AuthRead { bearer_token } => {
                output.extend_from_slice(&[ClientOp::AuthRead.byte()]);
                output.extend_from_slice(bearer_token.expose_secret().as_bytes());
            }
            Self::AuthWrite {
                writer_id,
                bearer_token,
            } => {
                output.extend_from_slice(&[ClientOp::AuthWrite.byte()]);
                output.extend_from_slice(writer_id.as_bytes());
                output.extend_from_slice(bearer_token.expose_secret().as_bytes());
            }
            Self::AppendRecord {
                writer_seq_num,
                part,
                format,
                data,
            } => {
                validate_record_len(data.len())?;
                output.extend_from_slice(&[ClientOp::AppendRecord.byte()]);
                output.extend_from_slice(&writer_seq_num.to_be_bytes());
                output.extend_from_slice(&part.raw().to_be_bytes());
                output.extend_from_slice(&[format.byte()]);
                output.extend_from_slice(data);
            }
        }

        Ok(output.freeze())
    }

    /// Decodes one client frame, copying any record payload into owned bytes.
    pub fn decode(input: &[u8]) -> Result<Self, FrameCodecError> {
        let (&op_byte, body) = input.split_first().ok_or(FrameCodecError::EmptyFrame)?;

        match ClientOp::try_from(op_byte)? {
            ClientOp::AuthRead => Ok(Self::AuthRead {
                bearer_token: BearerToken::from(utf8_tail(body)?),
            }),
            ClientOp::AuthWrite => {
                let (writer_id, token_bytes) = take::<{ WriterId::BYTE_LEN }>(body)?;
                Ok(Self::AuthWrite {
                    writer_id: WriterId::from_bytes(writer_id),
                    bearer_token: BearerToken::from(utf8_tail(token_bytes)?),
                })
            }
            ClientOp::AppendRecord => {
                let (writer_seq_num, body) = read_u64(body)?;
                let (part_raw, body) = read_u32(body)?;
                let (format, body) = read_record_format(body)?;
                let data = body;
                validate_record_len(data.len())?;
                Ok(Self::AppendRecord {
                    writer_seq_num,
                    part: PartHeader::from_raw(part_raw),
                    format,
                    data: Bytes::copy_from_slice(data),
                })
            }
        }
    }

    /// Decodes one client frame while retaining a zero-copy slice for record payload data.
    pub fn decode_bytes(input: Bytes) -> Result<Self, FrameCodecError> {
        let Some(&op_byte) = input.first() else {
            return Err(FrameCodecError::EmptyFrame);
        };
        let body = &input[1..];

        match ClientOp::try_from(op_byte)? {
            ClientOp::AuthRead => Ok(Self::AuthRead {
                bearer_token: BearerToken::from(utf8_tail(body)?),
            }),
            ClientOp::AuthWrite => {
                let (writer_id, token_bytes) = take::<{ WriterId::BYTE_LEN }>(body)?;
                Ok(Self::AuthWrite {
                    writer_id: WriterId::from_bytes(writer_id),
                    bearer_token: BearerToken::from(utf8_tail(token_bytes)?),
                })
            }
            ClientOp::AppendRecord => {
                let (writer_seq_num, body) = read_u64(body)?;
                let (part_raw, body) = read_u32(body)?;
                let (format, body) = read_record_format(body)?;
                let data = body;
                validate_record_len(data.len())?;
                let data_start = input.len() - data.len();
                Ok(Self::AppendRecord {
                    writer_seq_num,
                    part: PartHeader::from_raw(part_raw),
                    format,
                    data: input.slice(data_start..),
                })
            }
        }
    }
}

impl ServerFrame {
    /// Encodes one server frame into a complete WebSocket binary message.
    pub fn encode(&self) -> Result<Bytes, FrameCodecError> {
        let mut output = BytesMut::new();

        match self {
            Self::Hello { version } => {
                output.extend_from_slice(&[ServerOp::Hello.byte()]);
                output.extend_from_slice(&version.to_be_bytes());
            }
            Self::AuthRequired => output.extend_from_slice(&[ServerOp::AuthRequired.byte()]),
            Self::Ack {
                writer_seq_start,
                writer_seq_end,
                s2_seq_start,
                s2_seq_end,
            } => {
                output.extend_from_slice(&[ServerOp::Ack.byte()]);
                output.extend_from_slice(&writer_seq_start.to_be_bytes());
                output.extend_from_slice(&writer_seq_end.to_be_bytes());
                output.extend_from_slice(&s2_seq_start.to_be_bytes());
                output.extend_from_slice(&s2_seq_end.to_be_bytes());
            }
            Self::ReadRecord(record) => {
                validate_record_len(record.data.len())?;
                output.extend_from_slice(&[ServerOp::ReadRecord.byte()]);
                output.extend_from_slice(&record.s2_seq_num.to_be_bytes());
                output.extend_from_slice(&record.timestamp_ms.to_be_bytes());
                output.extend_from_slice(record.writer_id.as_bytes());
                output.extend_from_slice(&record.writer_seq_num.to_be_bytes());
                output.extend_from_slice(&record.part.raw().to_be_bytes());
                output.extend_from_slice(&[record.format.byte()]);
                output.extend_from_slice(&record.data);
            }
            Self::Heartbeat => output.extend_from_slice(&[ServerOp::Heartbeat.byte()]),
            Self::ReconnectAdvised { deadline_secs } => {
                output.extend_from_slice(&[ServerOp::ReconnectAdvised.byte()]);
                output.extend_from_slice(&[*deadline_secs]);
            }
            Self::ReadTail(tail) => {
                output.extend_from_slice(&[ServerOp::ReadTail.byte()]);
                output.extend_from_slice(&tail.next_s2_seq_num.to_be_bytes());
                output.extend_from_slice(&tail.timestamp_ms.to_be_bytes());
            }
        }

        Ok(output.freeze())
    }

    /// Decodes one server frame, copying any record payload into owned bytes.
    pub fn decode(input: &[u8]) -> Result<Self, FrameCodecError> {
        let (&op_byte, body) = input.split_first().ok_or(FrameCodecError::EmptyFrame)?;

        match ServerOp::try_from(op_byte)? {
            ServerOp::Hello => {
                let (version, body) = read_u16(body)?;
                ensure_empty(op_byte, body)?;
                Ok(Self::Hello { version })
            }
            ServerOp::AuthRequired => {
                ensure_empty(op_byte, body)?;
                Ok(Self::AuthRequired)
            }
            ServerOp::Ack => {
                let (writer_seq_start, body) = read_u64(body)?;
                let (writer_seq_end, body) = read_u64(body)?;
                let (s2_seq_start, body) = read_u64(body)?;
                let (s2_seq_end, body) = read_u64(body)?;
                ensure_empty(op_byte, body)?;
                Ok(Self::Ack {
                    writer_seq_start,
                    writer_seq_end,
                    s2_seq_start,
                    s2_seq_end,
                })
            }
            ServerOp::ReadRecord => {
                let (s2_seq_num, body) = read_u64(body)?;
                let (timestamp_ms, body) = read_u64(body)?;
                let (writer_id, body) = take::<{ WriterId::BYTE_LEN }>(body)?;
                let (writer_seq_num, body) = read_u64(body)?;
                let (part_raw, body) = read_u32(body)?;
                let (format, body) = read_record_format(body)?;
                let data = body;
                validate_record_len(data.len())?;
                Ok(Self::ReadRecord(ReadRecord {
                    s2_seq_num,
                    timestamp_ms,
                    writer_id: WriterId::from_bytes(writer_id),
                    writer_seq_num,
                    part: PartHeader::from_raw(part_raw),
                    format,
                    data: Bytes::copy_from_slice(data),
                }))
            }
            ServerOp::Heartbeat => {
                ensure_empty(op_byte, body)?;
                Ok(Self::Heartbeat)
            }
            ServerOp::ReconnectAdvised => {
                let (&deadline_secs, body) =
                    body.split_first().ok_or(FrameCodecError::TruncatedFrame {
                        op: op_byte,
                        needed: 1,
                    })?;
                ensure_empty(op_byte, body)?;
                Ok(Self::ReconnectAdvised { deadline_secs })
            }
            ServerOp::ReadTail => {
                let (next_s2_seq_num, body) = read_u64(body)?;
                let (timestamp_ms, body) = read_u64(body)?;
                ensure_empty(op_byte, body)?;
                Ok(Self::ReadTail(ReadTail {
                    next_s2_seq_num,
                    timestamp_ms,
                }))
            }
        }
    }

    /// Decodes one server frame while retaining a zero-copy slice for record payload data.
    pub fn decode_bytes(input: Bytes) -> Result<Self, FrameCodecError> {
        let Some(&op_byte) = input.first() else {
            return Err(FrameCodecError::EmptyFrame);
        };
        let body = &input[1..];

        match ServerOp::try_from(op_byte)? {
            ServerOp::Hello => {
                let (version, body) = read_u16(body)?;
                ensure_empty(op_byte, body)?;
                Ok(Self::Hello { version })
            }
            ServerOp::AuthRequired => {
                ensure_empty(op_byte, body)?;
                Ok(Self::AuthRequired)
            }
            ServerOp::Ack => {
                let (writer_seq_start, body) = read_u64(body)?;
                let (writer_seq_end, body) = read_u64(body)?;
                let (s2_seq_start, body) = read_u64(body)?;
                let (s2_seq_end, body) = read_u64(body)?;
                ensure_empty(op_byte, body)?;
                Ok(Self::Ack {
                    writer_seq_start,
                    writer_seq_end,
                    s2_seq_start,
                    s2_seq_end,
                })
            }
            ServerOp::ReadRecord => {
                let (s2_seq_num, body) = read_u64(body)?;
                let (timestamp_ms, body) = read_u64(body)?;
                let (writer_id, body) = take::<{ WriterId::BYTE_LEN }>(body)?;
                let (writer_seq_num, body) = read_u64(body)?;
                let (part_raw, body) = read_u32(body)?;
                let (format, body) = read_record_format(body)?;
                let data = body;
                validate_record_len(data.len())?;
                let data_start = input.len() - data.len();
                Ok(Self::ReadRecord(ReadRecord {
                    s2_seq_num,
                    timestamp_ms,
                    writer_id: WriterId::from_bytes(writer_id),
                    writer_seq_num,
                    part: PartHeader::from_raw(part_raw),
                    format,
                    data: input.slice(data_start..),
                }))
            }
            ServerOp::Heartbeat => {
                ensure_empty(op_byte, body)?;
                Ok(Self::Heartbeat)
            }
            ServerOp::ReconnectAdvised => {
                let (&deadline_secs, body) =
                    body.split_first().ok_or(FrameCodecError::TruncatedFrame {
                        op: op_byte,
                        needed: 1,
                    })?;
                ensure_empty(op_byte, body)?;
                Ok(Self::ReconnectAdvised { deadline_secs })
            }
            ServerOp::ReadTail => {
                let (next_s2_seq_num, body) = read_u64(body)?;
                let (timestamp_ms, body) = read_u64(body)?;
                ensure_empty(op_byte, body)?;
                Ok(Self::ReadTail(ReadTail {
                    next_s2_seq_num,
                    timestamp_ms,
                }))
            }
        }
    }
}

fn validate_record_len(len: usize) -> Result<(), FrameCodecError> {
    if len > MAX_RECORD_BYTES {
        return Err(FrameCodecError::RecordTooLarge {
            actual: len,
            max: MAX_RECORD_BYTES,
        });
    }
    Ok(())
}

fn take<const N: usize>(input: &[u8]) -> Result<([u8; N], &[u8]), FrameCodecError> {
    let Some((head, tail)) = input.split_at_checked(N) else {
        return Err(FrameCodecError::TruncatedFrame { op: 0, needed: N });
    };

    let mut bytes = [0_u8; N];
    bytes.copy_from_slice(head);
    Ok((bytes, tail))
}

fn read_u16(input: &[u8]) -> Result<(u16, &[u8]), FrameCodecError> {
    let (bytes, tail) = take::<2>(input)?;
    Ok((u16::from_be_bytes(bytes), tail))
}

fn read_u32(input: &[u8]) -> Result<(u32, &[u8]), FrameCodecError> {
    let (bytes, tail) = take::<4>(input)?;
    Ok((u32::from_be_bytes(bytes), tail))
}

fn read_u64(input: &[u8]) -> Result<(u64, &[u8]), FrameCodecError> {
    let (bytes, tail) = take::<8>(input)?;
    Ok((u64::from_be_bytes(bytes), tail))
}

fn read_record_format(input: &[u8]) -> Result<(RecordFormat, &[u8]), FrameCodecError> {
    let (&raw, tail) = input
        .split_first()
        .ok_or(FrameCodecError::TruncatedFrame { op: 0, needed: 1 })?;
    Ok((RecordFormat::try_from(raw)?, tail))
}

fn utf8_tail(input: &[u8]) -> Result<&str, FrameCodecError> {
    std::str::from_utf8(input).map_err(FrameCodecError::InvalidUtf8)
}

fn ensure_empty(op: u8, body: &[u8]) -> Result<(), FrameCodecError> {
    if body.is_empty() {
        Ok(())
    } else {
        Err(FrameCodecError::TrailingBytes {
            op,
            count: body.len(),
        })
    }
}

/// Error returned when encoding or decoding a TSF v3 binary frame.
#[derive(Debug, thiserror::Error)]
pub enum FrameCodecError {
    /// A message did not contain an operation byte.
    #[error("frame cannot be empty")]
    EmptyFrame,
    /// The operation byte is not defined by TSF v3.
    #[error("unknown operation id 0x{0:02x}")]
    UnknownOperation(u8),
    /// A record used an undefined presentation format byte.
    #[error("unknown record format 0x{0:02x}")]
    UnknownRecordFormat(u8),
    /// A fixed-width frame ended before all required bytes were present.
    #[error("frame 0x{op:02x} is truncated; needed {needed} more bytes")]
    TruncatedFrame {
        /// Operation byte, or zero when truncation occurred in a shared field decoder.
        op: u8,
        /// Minimum number of additional bytes required.
        needed: usize,
    },
    /// A fixed-width frame contained bytes after its defined body.
    #[error("frame 0x{op:02x} has {count} trailing bytes")]
    TrailingBytes {
        /// Operation byte for the decoded frame.
        op: u8,
        /// Number of undefined trailing bytes.
        count: usize,
    },
    /// A bearer-token tail was not valid UTF-8.
    #[error("token is not valid UTF-8: {0}")]
    InvalidUtf8(#[source] std::str::Utf8Error),
    /// A physical record payload exceeded [`MAX_RECORD_BYTES`].
    #[error("record is {actual} bytes; maximum is {max}")]
    RecordTooLarge {
        /// Actual payload length.
        actual: usize,
        /// Maximum accepted payload length.
        max: usize,
    },
    /// A split-record part index exceeded [`PartHeader::MAX_INDEX`].
    #[error("part index {0} is larger than the 31-bit part index range")]
    PartIndexTooLarge(u32),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn part_header_packs_final_bit_and_index() {
        let part = PartHeader::new(42, true).expect("part header");

        assert_eq!(part.index(), 42);
        assert!(part.is_final());
        assert_eq!(PartHeader::from_raw(part.raw()), part);
    }

    #[test]
    fn client_append_round_trips() {
        let frame = ClientFrame::AppendRecord {
            writer_seq_num: 7,
            part: PartHeader::unsplit(),
            format: RecordFormat::Transcript,
            data: Bytes::from_static(b"hello"),
        };

        let encoded = frame.encode().expect("encode frame");
        assert_client_frame_eq(ClientFrame::decode(&encoded).expect("decode frame"), &frame);
        assert_client_frame_eq(
            ClientFrame::decode_bytes(encoded).expect("decode bytes frame"),
            &frame,
        );
    }

    #[test]
    fn client_auth_frames_round_trip() {
        let read = ClientFrame::AuthRead {
            bearer_token: BearerToken::from("read-token"),
        };
        let write = ClientFrame::AuthWrite {
            writer_id: WriterId::from_bytes([3; WriterId::BYTE_LEN]),
            bearer_token: BearerToken::from("write-token"),
        };

        assert_client_frame_eq(
            ClientFrame::decode(&read.encode().expect("encode read auth"))
                .expect("decode read auth"),
            &read,
        );
        assert_client_frame_eq(
            ClientFrame::decode(&write.encode().expect("encode write auth"))
                .expect("decode write auth"),
            &write,
        );
    }

    #[test]
    fn server_read_record_round_trips() {
        let writer_id = WriterId::from_bytes([9; WriterId::BYTE_LEN]);
        let frame = ServerFrame::ReadRecord(ReadRecord {
            s2_seq_num: 11,
            timestamp_ms: 1_786_000_000_123,
            writer_id,
            writer_seq_num: 12,
            part: PartHeader::unsplit(),
            format: RecordFormat::Transcript,
            data: Bytes::from_static(b"line\n"),
        });

        let encoded = frame.encode().expect("encode frame");
        assert_eq!(ServerFrame::decode(&encoded).expect("decode frame"), frame);
        assert_eq!(
            ServerFrame::decode_bytes(encoded).expect("decode bytes frame"),
            frame
        );
    }

    #[test]
    fn server_control_frames_round_trip() {
        for frame in [
            ServerFrame::Hello { version: TSF_V3 },
            ServerFrame::AuthRequired,
            ServerFrame::Ack {
                writer_seq_start: 1,
                writer_seq_end: 2,
                s2_seq_start: 3,
                s2_seq_end: 4,
            },
            ServerFrame::Heartbeat,
            ServerFrame::ReconnectAdvised { deadline_secs: 5 },
            ServerFrame::ReadTail(ReadTail {
                next_s2_seq_num: 6,
                timestamp_ms: 7,
            }),
        ] {
            let encoded = frame.encode().expect("encode frame");
            assert_eq!(ServerFrame::decode(&encoded).expect("decode frame"), frame);
        }
    }

    #[test]
    fn record_byte_limit_is_enforced_at_the_shared_boundary() {
        let max_data = Bytes::from(vec![0; MAX_RECORD_BYTES]);
        let oversized_data = Bytes::from(vec![0; MAX_RECORD_BYTES + 1]);

        ClientFrame::AppendRecord {
            writer_seq_num: 0,
            part: PartHeader::unsplit(),
            format: RecordFormat::Bytes,
            data: max_data.clone(),
        }
        .encode()
        .expect("client max record encodes");
        assert!(matches!(
            ClientFrame::AppendRecord {
                writer_seq_num: 0,
                part: PartHeader::unsplit(),
                format: RecordFormat::Bytes,
                data: oversized_data.clone(),
            }
            .encode(),
            Err(FrameCodecError::RecordTooLarge {
                actual,
                max: MAX_RECORD_BYTES
            }) if actual == MAX_RECORD_BYTES + 1
        ));

        ServerFrame::ReadRecord(ReadRecord {
            s2_seq_num: 0,
            timestamp_ms: 0,
            writer_id: WriterId::from_bytes([1; WriterId::BYTE_LEN]),
            writer_seq_num: 0,
            part: PartHeader::unsplit(),
            format: RecordFormat::Bytes,
            data: max_data,
        })
        .encode()
        .expect("server max record encodes");
        assert!(matches!(
            ServerFrame::ReadRecord(ReadRecord {
                s2_seq_num: 0,
                timestamp_ms: 0,
                writer_id: WriterId::from_bytes([1; WriterId::BYTE_LEN]),
                writer_seq_num: 0,
                part: PartHeader::unsplit(),
                format: RecordFormat::Bytes,
                data: oversized_data,
            })
            .encode(),
            Err(FrameCodecError::RecordTooLarge {
                actual,
                max: MAX_RECORD_BYTES
            }) if actual == MAX_RECORD_BYTES + 1
        ));

        let oversized_client_frame = encoded_append_data_with_len(MAX_RECORD_BYTES + 1);
        assert!(matches!(
            ClientFrame::decode(&oversized_client_frame),
            Err(FrameCodecError::RecordTooLarge {
                actual,
                max: MAX_RECORD_BYTES
            }) if actual == MAX_RECORD_BYTES + 1
        ));
    }

    #[test]
    fn part_header_rejects_indexes_above_the_31_bit_range() {
        let max = PartHeader::new(PartHeader::MAX_INDEX, true).expect("max part index");

        assert_eq!(max.index(), PartHeader::MAX_INDEX);
        assert!(max.is_final());
        assert!(matches!(
            PartHeader::new(PartHeader::MAX_INDEX + 1, false),
            Err(FrameCodecError::PartIndexTooLarge(value)) if value == PartHeader::MAX_INDEX + 1
        ));
    }

    #[test]
    fn frame_decoders_reject_unknown_empty_and_truncated_frames() {
        assert!(matches!(
            ClientFrame::decode(&[]),
            Err(FrameCodecError::EmptyFrame)
        ));
        assert!(matches!(
            ServerFrame::decode(&[]),
            Err(FrameCodecError::EmptyFrame)
        ));
        assert!(matches!(
            ClientFrame::decode(&[0x7f]),
            Err(FrameCodecError::UnknownOperation(0x7f))
        ));
        assert!(matches!(
            ServerFrame::decode(&[0x7f]),
            Err(FrameCodecError::UnknownOperation(0x7f))
        ));
        assert!(matches!(
            ClientFrame::decode(&[ClientOp::AppendRecord.byte(), 0]),
            Err(FrameCodecError::TruncatedFrame { .. })
        ));
        assert!(matches!(
            ServerFrame::decode(&[ServerOp::Ack.byte(), 0]),
            Err(FrameCodecError::TruncatedFrame { .. })
        ));
    }

    #[test]
    fn frame_decoders_reject_unknown_record_formats() {
        let mut client = encoded_append_data_with_len(0).to_vec();
        client[1 + size_of::<u64>() + size_of::<u32>()] = 0x7f;
        assert!(matches!(
            ClientFrame::decode(&client),
            Err(FrameCodecError::UnknownRecordFormat(0x7f))
        ));

        let writer_id = WriterId::from_bytes([1; WriterId::BYTE_LEN]);
        let mut server = ServerFrame::ReadRecord(ReadRecord {
            s2_seq_num: 0,
            timestamp_ms: 0,
            writer_id,
            writer_seq_num: 0,
            part: PartHeader::unsplit(),
            format: RecordFormat::Bytes,
            data: Bytes::new(),
        })
        .encode()
        .expect("server record")
        .to_vec();
        let format_offset = 1
            + size_of::<u64>()
            + size_of::<u64>()
            + WriterId::BYTE_LEN
            + size_of::<u64>()
            + size_of::<u32>();
        server[format_offset] = 0x7f;
        assert!(matches!(
            ServerFrame::decode(&server),
            Err(FrameCodecError::UnknownRecordFormat(0x7f))
        ));
    }

    #[test]
    fn frame_decoders_reject_invalid_utf8_and_trailing_bytes() {
        assert!(matches!(
            ClientFrame::decode(&[ClientOp::AuthRead.byte(), 0xff]),
            Err(FrameCodecError::InvalidUtf8(_))
        ));
        assert!(matches!(
            ServerFrame::decode(&[ServerOp::Hello.byte(), 0, TSF_V3 as u8, 0]),
            Err(FrameCodecError::TrailingBytes { op, count: 1 }) if op == ServerOp::Hello.byte()
        ));
    }

    fn encoded_append_data_with_len(data_len: usize) -> Bytes {
        let mut frame = BytesMut::new();
        frame.extend_from_slice(&[ClientOp::AppendRecord.byte()]);
        frame.extend_from_slice(&0_u64.to_be_bytes());
        frame.extend_from_slice(&PartHeader::unsplit().raw().to_be_bytes());
        frame.extend_from_slice(&[RecordFormat::Bytes.byte()]);
        frame.extend(std::iter::repeat_n(0, data_len));
        frame.freeze()
    }

    fn assert_client_frame_eq(actual: ClientFrame, expected: &ClientFrame) {
        match (actual, expected) {
            (
                ClientFrame::AuthRead {
                    bearer_token: actual,
                },
                ClientFrame::AuthRead {
                    bearer_token: expected,
                },
            ) => assert_eq!(actual.expose_secret(), expected.expose_secret()),
            (
                ClientFrame::AuthWrite {
                    writer_id: actual_writer_id,
                    bearer_token: actual,
                },
                ClientFrame::AuthWrite {
                    writer_id: expected_writer_id,
                    bearer_token: expected,
                },
            ) => {
                assert_eq!(actual_writer_id, *expected_writer_id);
                assert_eq!(actual.expose_secret(), expected.expose_secret());
            }
            (
                ClientFrame::AppendRecord {
                    writer_seq_num: actual_writer_seq_num,
                    part: actual_part,
                    format: actual_format,
                    data: actual_data,
                },
                ClientFrame::AppendRecord {
                    writer_seq_num: expected_writer_seq_num,
                    part: expected_part,
                    format: expected_format,
                    data: expected_data,
                },
            ) => {
                assert_eq!(actual_writer_seq_num, *expected_writer_seq_num);
                assert_eq!(actual_part, *expected_part);
                assert_eq!(actual_format, *expected_format);
                assert_eq!(actual_data, *expected_data);
            }
            (actual, expected) => panic!("client frame mismatch: {actual:?} != {expected:?}"),
        }
    }
}
