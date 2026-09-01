use tailsurf::{
    TERMINAL_CHECKPOINT_EVENT_HEADER_BYTES, TERMINAL_CHECKPOINT_RECORD_INTERVAL,
    protocol::ws::frame::MAX_RECORD_PAYLOAD_BYTES,
};

const CHECKPOINT_BYTE_INTERVAL: usize = 4 * 1024 * 1024;
const MAX_CHECKPOINT_STATE_BYTES: usize =
    MAX_RECORD_PAYLOAD_BYTES - TERMINAL_CHECKPOINT_EVENT_HEADER_BYTES;

pub(super) struct TerminalStateCheckpoint {
    pub(super) columns: u16,
    pub(super) rows: u16,
    pub(super) state: Vec<u8>,
}

pub(super) struct TerminalCheckpointEmitter {
    parser: vt100::Parser<CheckpointCallbacks>,
    compatibility_parser: vte::Parser,
    compatibility: CheckpointCompatibility,
    restore_columns: u16,
    restore_rows: u16,
    restore_state: Option<Vec<u8>>,
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
            restore_columns: columns,
            restore_rows: rows,
            restore_state: Some(Vec::new()),
            pending_bytes: 0,
            pending_records: 0,
        }
    }

    pub(super) fn process(&mut self, data: &[u8]) -> Option<TerminalStateCheckpoint> {
        self.parser.process(data);
        self.compatibility_parser
            .advance(&mut self.compatibility, data);
        self.append_restore_data(data);
        self.pending_bytes = self.pending_bytes.saturating_add(data.len());
        self.pending_records = self.pending_records.saturating_add(1);
        if self.pending_bytes >= CHECKPOINT_BYTE_INTERVAL
            || self.pending_records >= TERMINAL_CHECKPOINT_RECORD_INTERVAL
        {
            return self.flush();
        }
        None
    }

    pub(super) fn resize(&mut self, columns: u16, rows: u16) -> Option<TerminalStateCheckpoint> {
        if self.parser.screen().size() != (rows, columns) {
            self.restore_state = None;
        }
        self.parser.screen_mut().set_size(rows, columns);
        self.pending_records = self.pending_records.saturating_add(1);
        self.flush()
    }

    pub(super) fn heartbeat(&mut self) -> Option<TerminalStateCheckpoint> {
        self.pending_records = self.pending_records.saturating_add(1);
        if self.pending_records >= TERMINAL_CHECKPOINT_RECORD_INTERVAL {
            return self.flush();
        }
        None
    }

    pub(super) fn flush(&mut self) -> Option<TerminalStateCheckpoint> {
        if self.pending_records == 0 {
            return None;
        }
        self.pending_bytes = 0;
        self.pending_records = 0;

        if !self.parser.screen().alternate_screen()
            && self.parser.callbacks().compatible
            && self.compatibility.compatible
        {
            let (rows, columns) = self.parser.screen().size();
            let state = self.parser.screen().state_formatted();
            if state.len() > MAX_CHECKPOINT_STATE_BYTES {
                self.restore_state = None;
                return None;
            }
            self.restore_columns = columns;
            self.restore_rows = rows;
            self.restore_state = Some(state.clone());
            return Some(TerminalStateCheckpoint {
                columns,
                rows,
                state,
            });
        }

        Some(TerminalStateCheckpoint {
            columns: self.restore_columns,
            rows: self.restore_rows,
            state: self.restore_state.clone()?,
        })
    }

    fn append_restore_data(&mut self, data: &[u8]) {
        let Some(state) = &mut self.restore_state else {
            return;
        };
        if data.len() > MAX_CHECKPOINT_STATE_BYTES.saturating_sub(state.len()) {
            self.restore_state = None;
            return;
        }
        state.extend_from_slice(data);
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
        let mut restored = vt100::Parser::new(checkpoint.rows, checkpoint.columns, 0);
        restored.process(&checkpoint.state);

        assert_eq!(restored.screen().size(), emitter.parser.screen().size());
        assert_eq!(
            restored.screen().state_formatted(),
            emitter.parser.screen().state_formatted()
        );
    }

    #[test]
    fn checkpoint_replays_state_that_cannot_be_compacted() {
        let mut emitter = TerminalCheckpointEmitter::new(80, 24);
        emitter.process(b"shell\x1b[7m\x1b7\x1b[?1049hfull screen");
        let checkpoint = emitter.flush().expect("fallback checkpoint");
        let mut restored = vt100::Parser::new(checkpoint.rows, checkpoint.columns, 0);
        restored.process(&checkpoint.state);

        assert!(restored.screen().alternate_screen());
        assert_eq!(
            restored.screen().state_formatted(),
            emitter.parser.screen().state_formatted()
        );

        emitter.process(b"\x1b[?1049l\x1b8restored");
        restored.process(b"\x1b[?1049l\x1b8restored");
        assert_eq!(
            restored.screen().state_formatted(),
            emitter.parser.screen().state_formatted()
        );
    }

    #[test]
    fn resize_discards_an_unsafe_replay_program() {
        let mut emitter = TerminalCheckpointEmitter::new(80, 24);
        emitter.process(b"shell\x1b[?1049hfull screen");

        assert!(emitter.resize(120, 40).is_none());
    }

    #[test]
    fn idle_heartbeats_cannot_push_the_checkpoint_out_of_the_record_window() {
        let mut emitter = TerminalCheckpointEmitter::new(80, 24);
        for _ in 1..TERMINAL_CHECKPOINT_RECORD_INTERVAL {
            assert!(emitter.heartbeat().is_none());
        }

        assert!(emitter.heartbeat().is_some());
    }
}
