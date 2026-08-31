//! Versioned terminal input and output events carried by byte records.

use thiserror::Error;

/// Current terminal event payload version.
pub const TERMINAL_EVENT_VERSION: u8 = 0x01;
/// Largest supported terminal width.
pub const MAX_TERMINAL_COLUMNS: u16 = 1_000;
/// Largest supported terminal height.
pub const MAX_TERMINAL_ROWS: u16 = 500;
/// Largest supported terminal viewport area.
pub const MAX_TERMINAL_CELLS: u32 = 131_072;

const HEADER_LEN: usize = 2;
const FIXED_EVENT_LEN: usize = HEADER_LEN + 4;
const DATA: u8 = 0x01;
const RESIZE: u8 = 0x02;
const STARTED: u8 = 0x03;
const EXITED: u8 = 0x04;
const HEARTBEAT: u8 = 0x05;

/// One event sent from a terminal controller to its PTY host.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TerminalInputEvent<'a> {
    /// Bytes to write to the PTY.
    Data(&'a [u8]),
    /// Requested PTY dimensions.
    Resize {
        /// Terminal columns.
        columns: u16,
        /// Terminal rows.
        rows: u16,
    },
}

/// One event sent from a PTY host to terminal observers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TerminalOutputEvent<'a> {
    /// Bytes emitted by the PTY.
    Data(&'a [u8]),
    /// PTY dimensions accepted from a controller.
    Resize {
        /// Terminal columns.
        columns: u16,
        /// Terminal rows.
        rows: u16,
    },
    /// The PTY child started with these dimensions.
    Started {
        /// Terminal columns.
        columns: u16,
        /// Terminal rows.
        rows: u16,
    },
    /// The PTY child exited with this status.
    Exited {
        /// Signed process status selected by the host.
        status: i32,
    },
    /// The PTY host is still connected.
    Heartbeat,
}

/// Encodes one terminal input event.
pub fn encode_terminal_input(
    event: TerminalInputEvent<'_>,
) -> Result<Vec<u8>, TerminalProtocolError> {
    match event {
        TerminalInputEvent::Data(data) => Ok(encode_data(DATA, data)),
        TerminalInputEvent::Resize { columns, rows } => encode_size(RESIZE, columns, rows),
    }
}

/// Decodes one terminal input event.
pub fn decode_terminal_input(
    payload: &[u8],
) -> Result<TerminalInputEvent<'_>, TerminalProtocolError> {
    match event_type(payload)? {
        DATA => Ok(TerminalInputEvent::Data(&payload[HEADER_LEN..])),
        RESIZE => {
            let (columns, rows) = decode_size(payload, "resize")?;
            Ok(TerminalInputEvent::Resize { columns, rows })
        }
        event_type => Err(TerminalProtocolError::UnknownType {
            direction: "input",
            event_type,
        }),
    }
}

/// Encodes one terminal output event.
pub fn encode_terminal_output(
    event: TerminalOutputEvent<'_>,
) -> Result<Vec<u8>, TerminalProtocolError> {
    match event {
        TerminalOutputEvent::Data(data) => Ok(encode_data(DATA, data)),
        TerminalOutputEvent::Resize { columns, rows } => encode_size(RESIZE, columns, rows),
        TerminalOutputEvent::Started { columns, rows } => encode_size(STARTED, columns, rows),
        TerminalOutputEvent::Exited { status } => {
            let mut payload = event_header(EXITED, FIXED_EVENT_LEN);
            payload[HEADER_LEN..].copy_from_slice(&status.to_be_bytes());
            Ok(payload)
        }
        TerminalOutputEvent::Heartbeat => Ok(event_header(HEARTBEAT, HEADER_LEN)),
    }
}

/// Decodes one terminal output event.
pub fn decode_terminal_output(
    payload: &[u8],
) -> Result<TerminalOutputEvent<'_>, TerminalProtocolError> {
    match event_type(payload)? {
        DATA => Ok(TerminalOutputEvent::Data(&payload[HEADER_LEN..])),
        RESIZE => {
            let (columns, rows) = decode_size(payload, "resize")?;
            Ok(TerminalOutputEvent::Resize { columns, rows })
        }
        STARTED => {
            let (columns, rows) = decode_size(payload, "started")?;
            Ok(TerminalOutputEvent::Started { columns, rows })
        }
        EXITED => {
            require_length(payload, FIXED_EVENT_LEN, "exited")?;
            let status = i32::from_be_bytes([
                payload[HEADER_LEN],
                payload[HEADER_LEN + 1],
                payload[HEADER_LEN + 2],
                payload[HEADER_LEN + 3],
            ]);
            Ok(TerminalOutputEvent::Exited { status })
        }
        HEARTBEAT => {
            require_length(payload, HEADER_LEN, "heartbeat")?;
            Ok(TerminalOutputEvent::Heartbeat)
        }
        event_type => Err(TerminalProtocolError::UnknownType {
            direction: "output",
            event_type,
        }),
    }
}

fn encode_data(event_type: u8, data: &[u8]) -> Vec<u8> {
    let mut payload = event_header(event_type, HEADER_LEN + data.len());
    payload[HEADER_LEN..].copy_from_slice(data);
    payload
}

fn encode_size(event_type: u8, columns: u16, rows: u16) -> Result<Vec<u8>, TerminalProtocolError> {
    validate_terminal_size(columns, rows)?;
    let mut payload = event_header(event_type, FIXED_EVENT_LEN);
    payload[HEADER_LEN..HEADER_LEN + 2].copy_from_slice(&columns.to_be_bytes());
    payload[HEADER_LEN + 2..].copy_from_slice(&rows.to_be_bytes());
    Ok(payload)
}

fn decode_size(payload: &[u8], name: &'static str) -> Result<(u16, u16), TerminalProtocolError> {
    require_length(payload, FIXED_EVENT_LEN, name)?;
    let columns = u16::from_be_bytes([payload[HEADER_LEN], payload[HEADER_LEN + 1]]);
    let rows = u16::from_be_bytes([payload[HEADER_LEN + 2], payload[HEADER_LEN + 3]]);
    validate_terminal_size(columns, rows)?;
    Ok((columns, rows))
}

fn event_header(event_type: u8, len: usize) -> Vec<u8> {
    let mut payload = vec![0; len];
    payload[0] = TERMINAL_EVENT_VERSION;
    payload[1] = event_type;
    payload
}

fn event_type(payload: &[u8]) -> Result<u8, TerminalProtocolError> {
    if payload.len() < HEADER_LEN {
        return Err(TerminalProtocolError::Truncated);
    }
    if payload[0] != TERMINAL_EVENT_VERSION {
        return Err(TerminalProtocolError::UnknownVersion(payload[0]));
    }
    Ok(payload[1])
}

fn require_length(
    payload: &[u8],
    expected: usize,
    name: &'static str,
) -> Result<(), TerminalProtocolError> {
    if payload.len() != expected {
        return Err(TerminalProtocolError::InvalidLength {
            name,
            actual: payload.len(),
            expected,
        });
    }
    Ok(())
}

/// Validates terminal dimensions accepted by every TSF implementation.
pub fn validate_terminal_size(columns: u16, rows: u16) -> Result<(), TerminalProtocolError> {
    if columns == 0 || columns > MAX_TERMINAL_COLUMNS {
        return Err(TerminalProtocolError::InvalidDimension {
            name: "columns",
            maximum: MAX_TERMINAL_COLUMNS,
        });
    }
    if rows == 0 || rows > MAX_TERMINAL_ROWS {
        return Err(TerminalProtocolError::InvalidDimension {
            name: "rows",
            maximum: MAX_TERMINAL_ROWS,
        });
    }
    if u32::from(columns) * u32::from(rows) > MAX_TERMINAL_CELLS {
        return Err(TerminalProtocolError::TooManyCells {
            maximum: MAX_TERMINAL_CELLS,
        });
    }
    Ok(())
}

/// A terminal event payload is malformed or unsupported.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum TerminalProtocolError {
    /// The payload does not contain its version and type.
    #[error("terminal event must contain a version and type")]
    Truncated,
    /// The payload uses an unknown version.
    #[error("unknown terminal event version 0x{0:02x}")]
    UnknownVersion(u8),
    /// The payload uses an unknown direction-specific event type.
    #[error("unknown terminal {direction} event type 0x{event_type:02x}")]
    UnknownType {
        /// Event direction.
        direction: &'static str,
        /// Unknown type byte.
        event_type: u8,
    },
    /// A fixed-width event has the wrong length.
    #[error("{name} terminal event is {actual} bytes; expected {expected}")]
    InvalidLength {
        /// Event name.
        name: &'static str,
        /// Actual encoded length.
        actual: usize,
        /// Required encoded length.
        expected: usize,
    },
    /// A terminal dimension is outside its supported range.
    #[error("terminal {name} must be between 1 and {maximum}")]
    InvalidDimension {
        /// Dimension name.
        name: &'static str,
        /// Inclusive maximum.
        maximum: u16,
    },
    /// A terminal viewport has too many cells.
    #[error("terminal viewport must not exceed {maximum} cells")]
    TooManyCells {
        /// Inclusive maximum.
        maximum: u32,
    },
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::*;

    #[derive(Deserialize)]
    struct TestVector {
        name: String,
        bytes: Vec<u8>,
    }

    #[test]
    fn matches_shared_test_vectors() {
        let vectors: Vec<TestVector> =
            serde_json::from_str(include_str!("../../testdata/terminal-events.json"))
                .expect("terminal event vectors");
        for vector in vectors {
            let encoded = match vector.name.as_str() {
                "input-data" => encode_terminal_input(TerminalInputEvent::Data(&[0, 1, 255])),
                "input-resize" => encode_terminal_input(TerminalInputEvent::Resize {
                    columns: 132,
                    rows: 43,
                }),
                "output-data" => encode_terminal_output(TerminalOutputEvent::Data(&[27, 91, 109])),
                "output-resize" => encode_terminal_output(TerminalOutputEvent::Resize {
                    columns: 80,
                    rows: 24,
                }),
                "output-started" => encode_terminal_output(TerminalOutputEvent::Started {
                    columns: 120,
                    rows: 40,
                }),
                "output-exited" => {
                    encode_terminal_output(TerminalOutputEvent::Exited { status: -1 })
                }
                "output-heartbeat" => encode_terminal_output(TerminalOutputEvent::Heartbeat),
                name => panic!("unknown terminal test vector {name}"),
            }
            .expect("encode terminal event");
            assert_eq!(encoded, vector.bytes, "{}", vector.name);
        }
    }

    #[test]
    fn round_trips_input_events() {
        for event in [
            TerminalInputEvent::Data(&[0, 1, 255]),
            TerminalInputEvent::Resize {
                columns: 132,
                rows: 43,
            },
        ] {
            let encoded = encode_terminal_input(event).expect("encode input event");
            assert_eq!(decode_terminal_input(&encoded), Ok(event));
        }
    }

    #[test]
    fn round_trips_output_events() {
        for event in [
            TerminalOutputEvent::Data(&[27, 91, 109]),
            TerminalOutputEvent::Resize {
                columns: 80,
                rows: 24,
            },
            TerminalOutputEvent::Started {
                columns: 120,
                rows: 40,
            },
            TerminalOutputEvent::Exited { status: -1 },
            TerminalOutputEvent::Heartbeat,
        ] {
            let encoded = encode_terminal_output(event).expect("encode output event");
            assert_eq!(decode_terminal_output(&encoded), Ok(event));
        }
    }

    #[test]
    fn rejects_invalid_events() {
        assert_eq!(
            decode_terminal_input(&[TERMINAL_EVENT_VERSION]),
            Err(TerminalProtocolError::Truncated)
        );
        assert_eq!(
            decode_terminal_output(&[2, DATA]),
            Err(TerminalProtocolError::UnknownVersion(2))
        );
        assert!(matches!(
            decode_terminal_input(&[TERMINAL_EVENT_VERSION, RESIZE, 0, 80, 0]),
            Err(TerminalProtocolError::InvalidLength { .. })
        ));
        assert_eq!(
            encode_terminal_input(TerminalInputEvent::Resize {
                columns: 0,
                rows: 24
            }),
            Err(TerminalProtocolError::InvalidDimension {
                name: "columns",
                maximum: MAX_TERMINAL_COLUMNS,
            })
        );
        assert_eq!(
            encode_terminal_output(TerminalOutputEvent::Started {
                columns: MAX_TERMINAL_COLUMNS,
                rows: MAX_TERMINAL_ROWS,
            }),
            Err(TerminalProtocolError::TooManyCells {
                maximum: MAX_TERMINAL_CELLS,
            })
        );
    }
}
