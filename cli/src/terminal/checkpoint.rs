use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use tailsurf::protocol::ws::frame::MAX_RECORD_PAYLOAD_BYTES;

const CHECKPOINT_BYTE_INTERVAL: usize = 4 * 1024 * 1024;
const CHECKPOINT_RECORD_INTERVAL: usize = 512;
const CHECKPOINT_PREFIX: &[u8] = b"\x1b]9999;tailsurf-checkpoint-v1;";
const CHECKPOINT_SUFFIX: u8 = 0x07;
const TERMINAL_DATA_EVENT_HEADER_BYTES: usize = 2;

pub(super) fn resembles_checkpoint(data: &[u8]) -> bool {
    data.starts_with(CHECKPOINT_PREFIX) && data.ends_with(&[CHECKPOINT_SUFFIX])
}

pub(super) struct TerminalCheckpointEmitter {
    parser: vt100::Parser<CheckpointCallbacks>,
    compatibility_parser: vte::Parser,
    compatibility: CheckpointCompatibility,
    pending_bytes: usize,
    pending_records: usize,
}

impl TerminalCheckpointEmitter {
    pub(super) fn new(columns: u16, rows: u16) -> Self {
        Self {
            parser: vt100::Parser::new_with_callbacks(
                rows,
                columns,
                0,
                CheckpointCallbacks::default(),
            ),
            compatibility_parser: vte::Parser::new(),
            compatibility: CheckpointCompatibility::default(),
            pending_bytes: 0,
            pending_records: 0,
        }
    }

    pub(super) fn process(&mut self, data: &[u8]) -> Option<Vec<u8>> {
        self.ingest(data);
        if self.pending_bytes >= CHECKPOINT_BYTE_INTERVAL
            || self.pending_records >= CHECKPOINT_RECORD_INTERVAL
        {
            return self.flush();
        }
        None
    }

    pub(super) fn ingest(&mut self, data: &[u8]) {
        self.parser.process(data);
        self.compatibility_parser
            .advance(&mut self.compatibility, data);
        self.pending_bytes = self.pending_bytes.saturating_add(data.len());
        self.pending_records = self.pending_records.saturating_add(1);
    }

    pub(super) fn resize(&mut self, columns: u16, rows: u16) -> Option<Vec<u8>> {
        self.parser.screen_mut().set_size(rows, columns);
        self.pending_records = self.pending_records.saturating_add(1);
        self.flush()
    }

    pub(super) fn heartbeat(&mut self) -> Option<Vec<u8>> {
        self.pending_records = self.pending_records.saturating_add(1);
        if self.pending_records >= CHECKPOINT_RECORD_INTERVAL {
            return self.flush();
        }
        None
    }

    pub(super) fn flush(&mut self) -> Option<Vec<u8>> {
        if self.pending_records == 0
            || self.parser.screen().alternate_screen()
            || !self.parser.callbacks().compatible
            || !self.compatibility.compatible
        {
            return None;
        }
        self.pending_bytes = 0;
        self.pending_records = 0;
        encode_checkpoint(self.parser.screen())
    }
}

struct CheckpointCallbacks {
    compatible: bool,
}

impl Default for CheckpointCallbacks {
    fn default() -> Self {
        Self { compatible: true }
    }
}

impl CheckpointCallbacks {
    fn reject(&mut self) {
        self.compatible = false;
    }
}

impl vt100::Callbacks for CheckpointCallbacks {
    fn unhandled_char(&mut self, _: &mut vt100::Screen, _: char) {
        self.reject();
    }

    fn unhandled_control(&mut self, _: &mut vt100::Screen, _: u8) {
        self.reject();
    }

    fn unhandled_escape(&mut self, _: &mut vt100::Screen, _: Option<u8>, _: Option<u8>, _: u8) {
        self.reject();
    }

    fn unhandled_csi(
        &mut self,
        _: &mut vt100::Screen,
        _: Option<u8>,
        _: Option<u8>,
        _: &[&[u16]],
        _: char,
    ) {
        self.reject();
    }

    fn unhandled_osc(&mut self, _: &mut vt100::Screen, _: &[&[u8]]) {
        self.reject();
    }
}

struct CheckpointCompatibility {
    compatible: bool,
}

impl Default for CheckpointCompatibility {
    fn default() -> Self {
        Self { compatible: true }
    }
}

impl vte::Perform for CheckpointCompatibility {
    fn execute(&mut self, byte: u8) {
        if !matches!(byte, 7..=13) {
            self.compatible = false;
        }
    }

    fn hook(&mut self, _: &vte::Params, _: &[u8], _: bool, _: char) {
        self.compatible = false;
    }

    fn esc_dispatch(&mut self, intermediates: &[u8], ignore: bool, byte: u8) {
        if ignore || !intermediates.is_empty() || matches!(byte, b'7' | b'8') {
            self.compatible = false;
        }
    }

    fn csi_dispatch(
        &mut self,
        params: &vte::Params,
        intermediates: &[u8],
        ignore: bool,
        action: char,
    ) {
        if ignore {
            self.compatible = false;
            return;
        }
        match (intermediates, action) {
            ([], 'r') => self.compatible = false,
            ([b'?'], 'h' | 'l')
                if params
                    .iter()
                    .any(|param| matches!(param, [6] | [47] | [1049])) =>
            {
                self.compatible = false;
            }
            _ => {}
        }
    }
}

fn encode_checkpoint(screen: &vt100::Screen) -> Option<Vec<u8>> {
    let (rows, columns) = screen.size();
    let state = screen.state_formatted();
    let mut payload = Vec::with_capacity(4 + state.len());
    payload.extend_from_slice(&columns.to_be_bytes());
    payload.extend_from_slice(&rows.to_be_bytes());
    payload.extend_from_slice(&state);

    let encoded_len = payload.len().div_ceil(3) * 4;
    let checkpoint_len = CHECKPOINT_PREFIX.len() + encoded_len + 1;
    if checkpoint_len + TERMINAL_DATA_EVENT_HEADER_BYTES > MAX_RECORD_PAYLOAD_BYTES {
        return None;
    }

    let encoded = URL_SAFE_NO_PAD.encode(payload);
    let mut checkpoint = Vec::with_capacity(checkpoint_len);
    checkpoint.extend_from_slice(CHECKPOINT_PREFIX);
    checkpoint.extend_from_slice(encoded.as_bytes());
    checkpoint.push(CHECKPOINT_SUFFIX);
    Some(checkpoint)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checkpoint_reconstructs_terminal_state() {
        let mut emitter = TerminalCheckpointEmitter::new(80, 24);
        assert!(
            emitter
                .process(b"hello \x1b[31mred\x1b[0m\x1b[4;8Hcursor")
                .is_none()
        );
        assert!(emitter.process(b"\x1b[?1h\x1b[?2004h").is_none());
        let checkpoint = emitter.flush().expect("checkpoint");
        let encoded = checkpoint
            .strip_prefix(CHECKPOINT_PREFIX)
            .and_then(|value| value.strip_suffix(&[CHECKPOINT_SUFFIX]))
            .expect("checkpoint envelope");
        let payload = URL_SAFE_NO_PAD.decode(encoded).expect("checkpoint payload");
        let columns = u16::from_be_bytes([payload[0], payload[1]]);
        let rows = u16::from_be_bytes([payload[2], payload[3]]);
        let mut restored = vt100::Parser::new(rows, columns, 0);
        restored.process(&payload[4..]);

        assert_eq!(restored.screen().size(), emitter.parser.screen().size());
        assert_eq!(
            restored.screen().state_formatted(),
            emitter.parser.screen().state_formatted()
        );
    }

    #[test]
    fn checkpoint_rejects_state_that_cannot_be_reproduced_exactly() {
        let mut emitter = TerminalCheckpointEmitter::new(80, 24);
        assert!(emitter.process(b"shell\x1b[?1049hfull screen").is_none());
        assert!(emitter.flush().is_none());
        emitter.process(b"\x1b[?1049l");

        assert!(emitter.flush().is_none());

        let mut emitter = TerminalCheckpointEmitter::new(80, 24);
        emitter.process(b"\x1b[5;20rscroll region");
        assert!(emitter.flush().is_none());

        let mut emitter = TerminalCheckpointEmitter::new(80, 24);
        emitter.process(b"\x1b[9munsupported attribute");
        assert!(emitter.flush().is_none());
    }

    #[test]
    fn idle_heartbeats_cannot_push_the_checkpoint_out_of_the_record_window() {
        let mut emitter = TerminalCheckpointEmitter::new(80, 24);
        for _ in 1..CHECKPOINT_RECORD_INTERVAL {
            assert!(emitter.heartbeat().is_none());
        }

        assert!(emitter.heartbeat().is_some());
    }
}
