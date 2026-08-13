//! Cross-language TSF v1 frame conformance tests driven by packaged JSON fixtures.

use bytes::Bytes;
use serde::Deserialize;
use tailsurf::{
    LinkSecret, WriterId,
    protocol::{
        rest::Visibility,
        ws::{
            ReadStart,
            frame::{
                AppendRecord, ClientFrame, MAX_APPEND_BATCH_RECORDS, MAX_BATCH_PAYLOAD_BYTES,
                MAX_READ_BATCH_RECORDS, MAX_RECORD_BYTES, PartHeader, ReadCaughtUp, ReadRecord,
                ReadStreamInfo, RecordFormat, ServerFrame, TSF_WS_PROTOCOL,
            },
        },
    },
};

const FIXTURES_JSON: &str = include_str!("../fixtures/v1.json");

#[derive(Deserialize)]
struct Fixtures {
    websocket_protocol: String,
    max_record_bytes: usize,
    max_append_batch_records: usize,
    max_read_batch_records: usize,
    max_batch_payload_bytes: usize,
    client_frames: Vec<FrameFixture<ClientFixture>>,
    server_frames: Vec<FrameFixture<ServerFixture>>,
}

#[derive(Deserialize)]
struct FrameFixture<T> {
    name: String,
    frame: T,
    hex: String,
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ClientFixture {
    OpenRead {
        start_type: String,
        start_value: String,
        count: Option<String>,
        until: Option<String>,
        playback_rate_permille: Option<String>,
        link_secret: Option<String>,
    },
    OpenWrite {
        writer_id_hex: String,
        link_secret: String,
    },
    AppendBatch {
        writer_seq_num: String,
        part_raw: String,
        format: u8,
        data_hex: String,
    },
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ServerFixture {
    Ready,
    Ack {
        writer_seq_start: String,
        writer_seq_end: String,
        seq_start: String,
        seq_end: String,
    },
    ReadBatch {
        seq_num: String,
        timestamp_ms: String,
        writer_id_hex: String,
        writer_seq_num: String,
        part_raw: String,
        format: u8,
        data_hex: String,
    },
    Heartbeat,
    CaughtUp {
        next_seq_num: String,
        last_timestamp_ms: String,
    },
    StreamInfo {
        stream_id: String,
        title: Option<String>,
        visibility: Visibility,
        created_at: String,
        expires_at: String,
    },
}

#[test]
fn protocol_constants_match_v1_fixtures() {
    let fixtures = fixtures();

    assert_eq!(fixtures.websocket_protocol, TSF_WS_PROTOCOL);
    assert_eq!(fixtures.max_record_bytes, MAX_RECORD_BYTES);
    assert_eq!(fixtures.max_append_batch_records, MAX_APPEND_BATCH_RECORDS);
    assert_eq!(fixtures.max_read_batch_records, MAX_READ_BATCH_RECORDS);
    assert_eq!(fixtures.max_batch_payload_bytes, MAX_BATCH_PAYLOAD_BYTES);
}

#[test]
fn client_frames_match_v1_fixtures() {
    let fixtures = fixtures();

    for fixture in fixtures.client_frames {
        let expected = decode_hex(&fixture.hex);
        let encoded = client_frame(fixture.frame)
            .encode()
            .unwrap_or_else(|error| panic!("{} fixture failed to encode: {error}", fixture.name));
        assert_eq!(encoded.as_ref(), expected, "{} fixture", fixture.name);

        let decoded = ClientFrame::decode(&expected)
            .unwrap_or_else(|error| panic!("{} fixture failed to decode: {error}", fixture.name));
        let reencoded = decoded.encode().unwrap_or_else(|error| {
            panic!("{} fixture failed to re-encode: {error}", fixture.name)
        });
        assert_eq!(reencoded.as_ref(), expected, "{} fixture", fixture.name);
    }
}

#[test]
fn server_frames_match_v1_fixtures() {
    let fixtures = fixtures();

    for fixture in fixtures.server_frames {
        let expected = decode_hex(&fixture.hex);
        let frame = server_frame(fixture.frame);
        let encoded = frame
            .encode()
            .unwrap_or_else(|error| panic!("{} fixture failed to encode: {error}", fixture.name));
        assert_eq!(encoded.as_ref(), expected, "{} fixture", fixture.name);

        let decoded = ServerFrame::decode(&expected)
            .unwrap_or_else(|error| panic!("{} fixture failed to decode: {error}", fixture.name));
        assert_eq!(decoded, frame, "{} fixture", fixture.name);
    }
}

fn fixtures() -> Fixtures {
    serde_json::from_str(FIXTURES_JSON).expect("v1 protocol fixtures are valid JSON")
}

fn client_frame(fixture: ClientFixture) -> ClientFrame {
    match fixture {
        ClientFixture::OpenRead {
            start_type,
            start_value,
            count,
            until,
            playback_rate_permille,
            link_secret,
        } => ClientFrame::OpenRead {
            link_secret: link_secret.map(LinkSecret::from),
            start: read_start(&start_type, parse_u64(&start_value)),
            count: count.as_deref().map(parse_u64),
            until: until.as_deref().map(parse_u64),
            playback_rate_permille: playback_rate_permille.as_deref().map(parse_u64),
        },
        ClientFixture::OpenWrite {
            writer_id_hex,
            link_secret,
        } => ClientFrame::OpenWrite {
            writer_id: decode_writer_id(&writer_id_hex),
            link_secret: LinkSecret::from(link_secret),
        },
        ClientFixture::AppendBatch {
            writer_seq_num,
            part_raw,
            format,
            data_hex,
        } => ClientFrame::AppendBatch(vec![AppendRecord {
            writer_seq_num: parse_u64(&writer_seq_num),
            part: PartHeader::from_raw(parse_hex_u32(&part_raw)),
            format: parse_format(format),
            data: Bytes::from(decode_hex(&data_hex)),
        }]),
    }
}

fn read_start(kind: &str, value: u64) -> ReadStart {
    match kind {
        "seq_num" => ReadStart::SeqNum(value),
        "timestamp_ms" => ReadStart::TimestampMs(value),
        "tail_offset" => ReadStart::TailOffset(value),
        other => panic!("unknown fixture read start {other}"),
    }
}

fn server_frame(fixture: ServerFixture) -> ServerFrame {
    match fixture {
        ServerFixture::Ready => ServerFrame::Ready,
        ServerFixture::Ack {
            writer_seq_start,
            writer_seq_end,
            seq_start,
            seq_end,
        } => ServerFrame::Ack {
            writer_seq_start: parse_u64(&writer_seq_start),
            writer_seq_end: parse_u64(&writer_seq_end),
            seq_start: parse_u64(&seq_start),
            seq_end: parse_u64(&seq_end),
        },
        ServerFixture::ReadBatch {
            seq_num,
            timestamp_ms,
            writer_id_hex,
            writer_seq_num,
            part_raw,
            format,
            data_hex,
        } => ServerFrame::ReadBatch(vec![ReadRecord {
            seq_num: parse_u64(&seq_num),
            timestamp_ms: parse_u64(&timestamp_ms),
            writer_id: decode_writer_id(&writer_id_hex),
            writer_seq_num: parse_u64(&writer_seq_num),
            part: PartHeader::from_raw(parse_hex_u32(&part_raw)),
            format: parse_format(format),
            data: Bytes::from(decode_hex(&data_hex)),
        }]),
        ServerFixture::Heartbeat => ServerFrame::Heartbeat,
        ServerFixture::CaughtUp {
            next_seq_num,
            last_timestamp_ms,
        } => ServerFrame::CaughtUp(ReadCaughtUp {
            next_seq_num: parse_u64(&next_seq_num),
            last_timestamp_ms: parse_u64(&last_timestamp_ms),
        }),
        ServerFixture::StreamInfo {
            stream_id,
            title,
            visibility,
            created_at,
            expires_at,
        } => ServerFrame::StreamInfo(ReadStreamInfo {
            stream_id: stream_id.parse().expect("fixture stream ID"),
            title: title.map(|title| title.parse().expect("fixture stream title")),
            visibility,
            created_at,
            expires_at,
        }),
    }
}

fn parse_u64(value: &str) -> u64 {
    value.parse().expect("fixture value is a u64")
}

fn parse_hex_u32(value: &str) -> u32 {
    u32::from_str_radix(value, 16).expect("fixture value is a hexadecimal u32")
}

fn parse_format(value: u8) -> RecordFormat {
    RecordFormat::try_from(value).expect("fixture record format is valid")
}

fn decode_writer_id(value: &str) -> WriterId {
    let bytes: [u8; WriterId::BYTE_LEN] = decode_hex(value)
        .try_into()
        .expect("fixture writer ID has the correct length");
    WriterId::from_bytes(bytes)
}

fn decode_hex(value: &str) -> Vec<u8> {
    assert!(
        value.len().is_multiple_of(2),
        "fixture hex has an even length"
    );
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|digits| {
            let digits = std::str::from_utf8(digits).expect("fixture hex is ASCII");
            u8::from_str_radix(digits, 16).expect("fixture hex contains hexadecimal bytes")
        })
        .collect()
}
