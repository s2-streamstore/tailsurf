use std::collections::BTreeMap;

use tailsurf::{
    TERMINAL_CHECKPOINT_EVENT_HEADER_BYTES, TERMINAL_CHECKPOINT_RECORD_INTERVAL,
    protocol::ws::frame::MAX_RECORD_PAYLOAD_BYTES,
};

const CHECKPOINT_BYTE_INTERVAL: usize = 256 * 1024;
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
    alternate_restore: Option<AlternateRestore>,
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
            alternate_restore: None,
            pending_bytes: 0,
            pending_records: 0,
        }
    }

    pub(super) fn process(&mut self, data: &[u8]) -> Option<TerminalStateCheckpoint> {
        // Alternate-screen mode changes end in `h` or `l`. Stop at those boundaries so the
        // primary screen can be serialized immediately before the parser switches buffers.
        let mut compatibility_start = 0;
        let mut terminal_start = 0;
        for (index, byte) in data.iter().enumerate() {
            if !matches!(byte, b'h' | b'l') {
                continue;
            }

            self.compatibility.transition = None;
            self.compatibility_parser
                .advance(&mut self.compatibility, &data[compatibility_start..=index]);
            compatibility_start = index + 1;

            let Some(transition) = self.compatibility.transition.take() else {
                continue;
            };
            self.process_segment(&data[terminal_start..index]);
            self.process_alternate_transition(transition, *byte);
            terminal_start = index + 1;
        }
        self.compatibility.transition = None;
        self.compatibility_parser
            .advance(&mut self.compatibility, &data[compatibility_start..]);
        self.process_segment(&data[terminal_start..]);
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
            let alternate_screen = self.parser.screen().alternate_screen();
            if alternate_screen && self.can_compact() {
                if let Some(restore) = &mut self.alternate_restore {
                    if !restore.resize(columns, rows) {
                        self.restore_state = None;
                        self.compatibility.reject();
                    }
                } else {
                    self.restore_state = None;
                }
            } else if alternate_screen || !self.can_compact() {
                self.restore_state = None;
            }
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

        if self.can_compact() {
            let Some((columns, rows, state)) = self.compact_state() else {
                self.restore_state = None;
                return None;
            };
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

    fn process_segment(&mut self, data: &[u8]) {
        self.parser.process(data);
        self.append_restore_data(data);
    }

    fn process_alternate_transition(&mut self, transition: AlternateTransition, byte: u8) {
        let can_compact = self.can_compact();
        match transition {
            AlternateTransition::Enter(mode) => {
                let primary_state = if can_compact && !self.parser.screen().alternate_screen() {
                    Some(self.parser.screen().state_formatted())
                } else {
                    self.compatibility.reject();
                    None
                };

                self.process_segment(&[byte]);
                self.translate_unsupported_alternate_mode(mode, true);
                if !self.parser.screen().alternate_screen() {
                    self.compatibility.reject();
                }
                self.alternate_restore = primary_state.and_then(|primary_state| {
                    self.parser.screen().alternate_screen().then(|| {
                        let (rows, columns) = self.parser.screen().size();
                        AlternateRestore {
                            primary_state,
                            columns,
                            rows,
                            mode,
                        }
                    })
                });
                self.rebase_restore_state();
            }
            AlternateTransition::Exit(mode) => {
                let matching_restore = can_compact
                    && self.parser.screen().alternate_screen()
                    && self
                        .alternate_restore
                        .as_ref()
                        .is_some_and(|restore| restore.mode == mode);
                if !matching_restore {
                    self.compatibility.reject();
                }

                self.process_segment(&[byte]);
                self.translate_unsupported_alternate_mode(mode, false);
                self.alternate_restore = None;
                if self.parser.screen().alternate_screen() {
                    self.compatibility.reject();
                }
                self.rebase_restore_state();
            }
        }
    }

    fn compact_state(&self) -> Option<(u16, u16, Vec<u8>)> {
        if !self.can_compact() {
            return None;
        }

        let (rows, columns) = self.parser.screen().size();
        let mut state = if self.parser.screen().alternate_screen() {
            let restore = self.alternate_restore.as_ref()?;
            let active_state = self.parser.screen().state_formatted();
            let mut state = Vec::with_capacity(
                restore.primary_state.len()
                    + restore.mode.enter_sequence().len()
                    + active_state.len(),
            );
            state.extend_from_slice(&restore.primary_state);
            state.extend_from_slice(restore.mode.enter_sequence());
            state.extend_from_slice(&active_state);
            state
        } else {
            self.parser.screen().state_formatted()
        };
        self.compatibility.append_passthrough_modes(&mut state);

        (state.len() <= MAX_CHECKPOINT_STATE_BYTES).then_some((columns, rows, state))
    }

    fn rebase_restore_state(&mut self) {
        let Some((columns, rows, state)) = self.compact_state() else {
            if self.can_compact() {
                self.restore_state = None;
            }
            return;
        };
        self.restore_columns = columns;
        self.restore_rows = rows;
        self.restore_state = Some(state);
    }

    fn can_compact(&self) -> bool {
        self.parser.callbacks().compatible && self.compatibility.compatible
    }

    fn translate_unsupported_alternate_mode(&mut self, mode: AlternateScreenMode, enter: bool) {
        if mode != AlternateScreenMode::Switch1047 {
            return;
        }
        self.parser
            .process(if enter { b"\x1b[?47h" } else { b"\x1b[?47l" });
        self.parser.callbacks_mut().compatible = true;
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum AlternateScreenMode {
    Switch47,
    Switch1047,
    SaveCursor,
}

impl AlternateScreenMode {
    fn enter_sequence(self) -> &'static [u8] {
        match self {
            Self::Switch47 | Self::Switch1047 => b"\x1b[?47h",
            Self::SaveCursor => b"\x1b[?1049h",
        }
    }
}

#[derive(Clone, Copy)]
enum AlternateTransition {
    Enter(AlternateScreenMode),
    Exit(AlternateScreenMode),
}

struct AlternateRestore {
    primary_state: Vec<u8>,
    columns: u16,
    rows: u16,
    mode: AlternateScreenMode,
}

impl AlternateRestore {
    fn resize(&mut self, columns: u16, rows: u16) -> bool {
        let mut parser = vt100::Parser::new(self.rows, self.columns, 0);
        parser.process(&self.primary_state);
        parser.screen_mut().set_size(rows, columns);
        let state = parser.screen().state_formatted();
        if state.len() > MAX_CHECKPOINT_STATE_BYTES {
            return false;
        }
        self.primary_state = state;
        self.columns = columns;
        self.rows = rows;
        true
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
        first_intermediate: Option<u8>,
        second_intermediate: Option<u8>,
        params: &[&[u16]],
        action: char,
    ) {
        let serializable_dec_modes = first_intermediate == Some(b'?')
            && second_intermediate.is_none()
            && matches!(action, 'h' | 'l')
            && params.iter().all(|param| {
                let [mode] = **param else {
                    return false;
                };
                vt100_handles_dec_mode(mode) || mode == 1047 || is_passthrough_dec_mode(mode)
            });
        if !serializable_dec_modes {
            self.reject();
        }
    }

    fn unhandled_osc(&mut self, _: &mut vt100::Screen, _: &[&[u8]]) {
        self.reject();
    }
}

struct CheckpointCompatibility {
    compatible: bool,
    transition: Option<AlternateTransition>,
    passthrough_modes: BTreeMap<u16, bool>,
}

impl Default for CheckpointCompatibility {
    fn default() -> Self {
        Self {
            compatible: true,
            transition: None,
            passthrough_modes: BTreeMap::new(),
        }
    }
}

impl CheckpointCompatibility {
    fn reject(&mut self) {
        self.compatible = false;
        self.transition = None;
    }

    fn append_passthrough_modes(&self, state: &mut Vec<u8>) {
        for (mode, enabled) in &self.passthrough_modes {
            state.extend_from_slice(b"\x1b[?");
            state.extend_from_slice(mode.to_string().as_bytes());
            state.push(if *enabled { b'h' } else { b'l' });
        }
    }
}

fn vt100_handles_dec_mode(mode: u16) -> bool {
    matches!(
        mode,
        1 | 6 | 9 | 25 | 47 | 1000 | 1002 | 1003 | 1005 | 1006 | 1049 | 2004
    )
}

// These modes affect presentation or terminal-to-application input. They do not change how
// output bytes update the grid, so their latest value can follow the serialized screen state.
fn is_passthrough_dec_mode(mode: u16) -> bool {
    matches!(
        mode,
        4 | 5
            | 8
            | 12
            | 66
            | 67
            | 1001
            | 1004
            | 1007
            | 1010
            | 1011
            | 1014
            | 1015
            | 1016
            | 1034
            | 1035
            | 1036
            | 1037
            | 1039
            | 1040
            | 1041
            | 1042
            | 1043
            | 1044
            | 1050
            | 1051
            | 1052
            | 1053
            | 1060
            | 1061
            | 2001
            | 2002
            | 2003
            | 2005
            | 2006
            | 2026
            | 2031
            | 2048
            | 5522
            | 7727
            | 9001
    )
}

impl vte::Perform for CheckpointCompatibility {
    fn execute(&mut self, byte: u8) {
        if !matches!(byte, 7..=13) {
            self.reject();
        }
    }

    fn hook(&mut self, _: &vte::Params, _: &[u8], _: bool, _: char) {
        self.reject();
    }

    fn esc_dispatch(&mut self, intermediates: &[u8], ignore: bool, byte: u8) {
        if ignore || !intermediates.is_empty() || matches!(byte, b'7' | b'8') {
            self.reject();
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
            self.reject();
            return;
        }
        match (intermediates, action) {
            ([], 'r') => self.reject(),
            ([b'?'], 'h' | 'l') => {
                let mut mode = None;
                let mut count = 0;
                for param in params.iter() {
                    count += 1;
                    match param {
                        [6] => self.reject(),
                        [47] => mode = Some(AlternateScreenMode::Switch47),
                        [1047] => mode = Some(AlternateScreenMode::Switch1047),
                        [1049] => mode = Some(AlternateScreenMode::SaveCursor),
                        [value] if is_passthrough_dec_mode(*value) => {
                            self.passthrough_modes.insert(*value, action == 'h');
                        }
                        [value] if vt100_handles_dec_mode(*value) => {}
                        _ => self.reject(),
                    }
                }
                if let Some(mode) = mode {
                    if count == 1 && self.compatible {
                        self.transition = Some(if action == 'h' {
                            AlternateTransition::Enter(mode)
                        } else {
                            AlternateTransition::Exit(mode)
                        });
                    } else {
                        self.reject();
                    }
                }
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

        assert!(emitter.resize(120, 40).is_none());
    }

    #[test]
    fn checkpoint_preserves_both_screens_across_a_resize() {
        let mut emitter = TerminalCheckpointEmitter::new(80, 24);
        emitter.process(b"shell\x1b[?1049hfull screen");
        let checkpoint = emitter.resize(120, 40).expect("resized checkpoint");
        let mut restored = vt100::Parser::new(checkpoint.rows, checkpoint.columns, 0);
        restored.process(&checkpoint.state);

        assert!(restored.screen().alternate_screen());
        assert_eq!(
            restored.screen().state_formatted(),
            emitter.parser.screen().state_formatted()
        );

        emitter.process(b"\x1b[?1049l");
        restored.process(b"\x1b[?1049l");
        assert_eq!(
            restored.screen().state_formatted(),
            emitter.parser.screen().state_formatted()
        );
    }

    #[test]
    fn btop_synchronized_output_stays_checkpointable_across_a_resize() {
        let mut emitter = TerminalCheckpointEmitter::new(80, 24);
        emitter.process(b"shell\x1b[?1049h\x1b[?1006h\x1b[?2026hframe");
        emitter.process(b" updated\x1b[?2026l");
        let checkpoint = emitter.resize(120, 40).expect("resized checkpoint");
        let mut restored = vt100::Parser::new(checkpoint.rows, checkpoint.columns, 0);
        restored.process(&checkpoint.state);

        assert!(restored.screen().alternate_screen());
        assert_eq!(
            restored.screen().state_formatted(),
            emitter.parser.screen().state_formatted()
        );
    }

    #[test]
    fn checkpoint_preserves_modern_input_and_reporting_modes() {
        let mut emitter = TerminalCheckpointEmitter::new(80, 24);
        emitter.process(b"screen\x1b[?1004;1016;2031;2048;5522;7727;9001h");
        emitter.process(b"\x1b[?2031;5522l");
        let checkpoint = emitter.flush().expect("checkpoint");

        assert!(checkpoint.state.ends_with(
            b"\x1b[?1004h\x1b[?1016h\x1b[?2031l\x1b[?2048h\x1b[?5522l\x1b[?7727h\x1b[?9001h"
        ));
    }

    #[test]
    fn alternate_screen_checkpoints_stay_compact() {
        let mut emitter = TerminalCheckpointEmitter::new(80, 24);
        emitter.process(b"shell\x1b[?10");
        emitter.process(b"49h");
        let output = vec![b'x'; 8 * 1024];
        let mut checkpoints = Vec::new();
        for _ in 0..(3 * CHECKPOINT_BYTE_INTERVAL / output.len()) {
            if let Some(checkpoint) = emitter.process(&output) {
                checkpoints.push(checkpoint);
            }
        }

        assert_eq!(checkpoints.len(), 3);
        let checkpoint = checkpoints.pop().expect("checkpoint");
        assert!(checkpoint.state.len() < MAX_CHECKPOINT_STATE_BYTES);
        let mut restored = vt100::Parser::new(checkpoint.rows, checkpoint.columns, 0);
        restored.process(&checkpoint.state);

        assert!(restored.screen().alternate_screen());
        assert_eq!(
            restored.screen().state_formatted(),
            emitter.parser.screen().state_formatted()
        );

        emitter.process(b"\x1b[?104");
        emitter.process(b"9l");
        restored.process(b"\x1b[?1049l");
        assert_eq!(
            restored.screen().state_formatted(),
            emitter.parser.screen().state_formatted()
        );
    }

    #[test]
    fn checkpoint_normalizes_alternate_screen_mode_1047() {
        let mut emitter = TerminalCheckpointEmitter::new(80, 24);
        emitter.process(b"shell\x1b[?1047hfull screen");
        let checkpoint = emitter.flush().expect("checkpoint");
        let mut restored = vt100::Parser::new(checkpoint.rows, checkpoint.columns, 0);
        restored.process(&checkpoint.state);

        assert!(restored.screen().alternate_screen());
        assert_eq!(
            restored.screen().state_formatted(),
            emitter.parser.screen().state_formatted()
        );

        emitter.process(b"\x1b[?1047l");
        restored.process(b"\x1b[?47l");
        assert_eq!(
            restored.screen().state_formatted(),
            emitter.parser.screen().state_formatted()
        );
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
