//! Cross-language TSF v3 frame conformance tests driven by packaged JSON fixtures.

use bytes::Bytes;
use serde::Deserialize;
use tailsurf::{
    LinkSecret, WriterId,
    protocol::ws::frame::{
        ClientFrame, MAX_LINK_AUTHORIZATION_BYTES, MAX_RECORD_BYTES, PartHeader, ReadRecord,
        ReadTail, RecordFormat, ServerFrame, TSF_V3, TSF_WS_PROTOCOL,
    },
};

const FIXTURES_JSON: &str = include_str!("../fixtures/v3.json");

#[derive(Deserialize)]
struct Fixtures {
    version: u16,
    websocket_protocol: String,
    max_record_bytes: usize,
    max_link_authorization_bytes: usize,
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
    AuthRead {
        link_secret: String,
    },
    AuthReadGrant {
        authorization: String,
        link_secret: String,
    },
    AuthWrite {
        writer_id_hex: String,
        link_secret: String,
    },
    AuthWriteGrant {
        writer_id_hex: String,
        authorization: String,
        link_secret: String,
    },
    AppendRecord {
        writer_seq_num: String,
        part_raw: String,
        format: u8,
        data_hex: String,
    },
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ServerFixture {
    Hello {
        version: u16,
    },
    Ack {
        writer_seq_start: String,
        writer_seq_end: String,
        s2_seq_start: String,
        s2_seq_end: String,
    },
    ReadRecord {
        s2_seq_num: String,
        timestamp_ms: String,
        writer_id_hex: String,
        writer_seq_num: String,
        part_raw: String,
        format: u8,
        data_hex: String,
    },
    Heartbeat,
    ReconnectAdvised {
        deadline_secs: u8,
    },
    ReadTail {
        next_s2_seq_num: String,
        timestamp_ms: String,
    },
}

#[test]
fn protocol_constants_match_v3_fixtures() {
    let fixtures = fixtures();

    assert_eq!(fixtures.version, TSF_V3);
    assert_eq!(fixtures.websocket_protocol, TSF_WS_PROTOCOL);
    assert_eq!(fixtures.max_record_bytes, MAX_RECORD_BYTES);
    assert_eq!(
        fixtures.max_link_authorization_bytes,
        MAX_LINK_AUTHORIZATION_BYTES
    );
}

#[test]
fn client_frames_match_v3_fixtures() {
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
fn server_frames_match_v3_fixtures() {
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
    serde_json::from_str(FIXTURES_JSON).expect("v3 protocol fixtures are valid JSON")
}

fn client_frame(fixture: ClientFixture) -> ClientFrame {
    match fixture {
        ClientFixture::AuthRead { link_secret } => ClientFrame::AuthRead {
            link_secret: LinkSecret::from(link_secret),
        },
        ClientFixture::AuthReadGrant {
            authorization,
            link_secret,
        } => ClientFrame::AuthReadGrant {
            authorization,
            link_secret: LinkSecret::from(link_secret),
        },
        ClientFixture::AuthWrite {
            writer_id_hex,
            link_secret,
        } => ClientFrame::AuthWrite {
            writer_id: decode_writer_id(&writer_id_hex),
            link_secret: LinkSecret::from(link_secret),
        },
        ClientFixture::AuthWriteGrant {
            writer_id_hex,
            authorization,
            link_secret,
        } => ClientFrame::AuthWriteGrant {
            writer_id: decode_writer_id(&writer_id_hex),
            authorization,
            link_secret: LinkSecret::from(link_secret),
        },
        ClientFixture::AppendRecord {
            writer_seq_num,
            part_raw,
            format,
            data_hex,
        } => ClientFrame::AppendRecord {
            writer_seq_num: parse_u64(&writer_seq_num),
            part: PartHeader::from_raw(parse_hex_u32(&part_raw)),
            format: parse_format(format),
            data: Bytes::from(decode_hex(&data_hex)),
        },
    }
}

fn server_frame(fixture: ServerFixture) -> ServerFrame {
    match fixture {
        ServerFixture::Hello { version } => ServerFrame::Hello { version },
        ServerFixture::Ack {
            writer_seq_start,
            writer_seq_end,
            s2_seq_start,
            s2_seq_end,
        } => ServerFrame::Ack {
            writer_seq_start: parse_u64(&writer_seq_start),
            writer_seq_end: parse_u64(&writer_seq_end),
            s2_seq_start: parse_u64(&s2_seq_start),
            s2_seq_end: parse_u64(&s2_seq_end),
        },
        ServerFixture::ReadRecord {
            s2_seq_num,
            timestamp_ms,
            writer_id_hex,
            writer_seq_num,
            part_raw,
            format,
            data_hex,
        } => ServerFrame::ReadRecord(ReadRecord {
            s2_seq_num: parse_u64(&s2_seq_num),
            timestamp_ms: parse_u64(&timestamp_ms),
            writer_id: decode_writer_id(&writer_id_hex),
            writer_seq_num: parse_u64(&writer_seq_num),
            part: PartHeader::from_raw(parse_hex_u32(&part_raw)),
            format: parse_format(format),
            data: Bytes::from(decode_hex(&data_hex)),
        }),
        ServerFixture::Heartbeat => ServerFrame::Heartbeat,
        ServerFixture::ReconnectAdvised { deadline_secs } => {
            ServerFrame::ReconnectAdvised { deadline_secs }
        }
        ServerFixture::ReadTail {
            next_s2_seq_num,
            timestamp_ms,
        } => ServerFrame::ReadTail(ReadTail {
            next_s2_seq_num: parse_u64(&next_s2_seq_num),
            timestamp_ms: parse_u64(&timestamp_ms),
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
