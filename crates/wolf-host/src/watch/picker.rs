//! The `mrwolf watch` setup screen (P1-9), shown when `watch` gets no pids: pick the .NET
//! processes to attach and the physical shard count before the graph is built. Process
//! command lines come from `ProcessInfo` over the diagnostics endpoint; the scan runs on
//! a throwaway tokio runtime because `run()` builds the real one later.
//!
//! Keys: `↑`/`↓` move, `Space` toggle, `←`/`→` shards, `r` rescan, `Enter` start,
//! `q`/`Esc` cancel.

use std::collections::BTreeSet;

use crossterm::event::{self, KeyCode, KeyEventKind};
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, List, ListItem, Paragraph};
use wolf_ipc::{DiagnosticsClient, discover};

/// One row of the picker: a discoverable .NET process.
#[derive(Debug)]
struct ProcessRow {
    pid: u32,
    /// Command line, or why it is missing (stale endpoint, permission).
    desc: String,
}

/// What the picker returns: the selected pids and the shard count.
#[derive(Debug)]
pub struct PickerResult {
    pub pids: Vec<u32>,
    pub shards: u16,
}

const MAX_SHARDS: u16 = 64;
const DESC_CAP: usize = 80;

/// Runs the setup screen. `None` means the user cancelled (or no .NET process exists).
pub fn pick_targets(initial_shards: u16) -> Option<PickerResult> {
    let mut picker = Picker {
        processes: scan(),
        cursor: 0,
        selected: BTreeSet::new(),
        shards: initial_shards.max(1),
    };
    if picker.processes.is_empty() {
        eprintln!(
            "mrwolf: no .NET processes found (nothing is listening on a diagnostics endpoint)"
        );
        return None;
    }
    let (_guard, mut terminal) = super::view::terminal().ok()?;
    loop {
        terminal
            .draw(|frame| picker.draw(frame))
            .expect("picker draw");
        if let event::Event::Key(key) = event::read().expect("key event") {
            if key.kind != KeyEventKind::Press {
                continue;
            }
            match key.code {
                KeyCode::Char('q') | KeyCode::Esc => return None,
                KeyCode::Enter => {
                    if let Some(result) = picker.result() {
                        return Some(result);
                    }
                }
                KeyCode::Up | KeyCode::Char('k') => picker.move_cursor(-1),
                KeyCode::Down | KeyCode::Char('j') => picker.move_cursor(1),
                KeyCode::Char(' ') => picker.toggle(),
                KeyCode::Left | KeyCode::Char('-') => {
                    picker.shards = picker.shards.saturating_sub(1).max(1);
                }
                KeyCode::Right | KeyCode::Char('+') => {
                    picker.shards = (picker.shards + 1).min(MAX_SHARDS);
                }
                KeyCode::Char('r') => {
                    picker.processes = scan();
                    picker.cursor = 0;
                }
                _ => {}
            }
        }
    }
}

#[derive(Debug)]
struct Picker {
    processes: Vec<ProcessRow>,
    cursor: usize,
    /// Selected pids; a set keeps toggle idempotent and the result sorted.
    selected: BTreeSet<u32>,
    shards: u16,
}

impl Picker {
    fn move_cursor(&mut self, delta: i64) {
        let len = self.processes.len() as i64;
        self.cursor = ((self.cursor as i64 + delta).rem_euclid(len)) as usize;
    }

    fn toggle(&mut self) {
        if let Some(row) = self.processes.get(self.cursor) {
            if !self.selected.insert(row.pid) {
                self.selected.remove(&row.pid);
            }
        }
    }

    /// The selection on Enter; starting needs at least one pid.
    fn result(&self) -> Option<PickerResult> {
        if self.selected.is_empty() {
            return None;
        }
        Some(PickerResult {
            pids: self.selected.iter().copied().collect(),
            shards: self.shards,
        })
    }

    fn draw(&self, frame: &mut Frame) {
        let label = Style::default().add_modifier(Modifier::DIM);
        let chunks = Layout::vertical([
            Constraint::Length(1), // title
            Constraint::Length(1), // blank
            Constraint::Min(3),    // process list
            Constraint::Length(2), // shards + blank
            Constraint::Length(1), // help
        ])
        .split(frame.area());

        frame.render_widget(
            Paragraph::new(Span::styled(
                "mrwolf watch — pick targets",
                Style::default().add_modifier(Modifier::BOLD),
            )),
            chunks[0],
        );

        let items: Vec<ListItem> = self
            .processes
            .iter()
            .enumerate()
            .map(|(index, row)| {
                let marked = if self.selected.contains(&row.pid) {
                    "[x]"
                } else {
                    "[ ]"
                };
                let line = Line::from(vec![
                    Span::raw(format!(" {marked} {:>6}  ", row.pid)),
                    Span::styled(&row.desc, label),
                ]);
                let item = ListItem::new(line);
                if index == self.cursor {
                    item.style(Style::default().bg(Color::DarkGray))
                } else {
                    item
                }
            })
            .collect();
        frame.render_widget(
            List::new(items).block(Block::bordered().title(Span::styled(
                format!(" .NET processes ({}) ", self.processes.len()),
                label,
            ))),
            chunks[2],
        );

        frame.render_widget(
            Paragraph::new(Line::from(vec![
                Span::styled("physical shards ", label),
                Span::raw(format!("◀ {:>2} ▶", self.shards)),
                Span::styled(
                    "   (one dotnet-gc + process → store block group each)",
                    label,
                ),
            ])),
            chunks[3],
        );

        frame.render_widget(
            Paragraph::new(Span::styled(
                "↑/↓ move · Space toggle · ←/→ shards · r rescan · Enter start · q cancel",
                label,
            )),
            chunks[4],
        );
    }
}

/// Every diagnostics endpoint with its process's command line. Unreadable endpoints
/// (stale, no permission) stay in the list with the reason — a stale endpoint fails
/// attach with a clear error, and the user can see it was there.
fn scan() -> Vec<ProcessRow> {
    let Ok(endpoints) = discover() else {
        return Vec::new();
    };
    let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    else {
        return endpoints
            .into_iter()
            .map(|endpoint| ProcessRow {
                pid: endpoint.pid,
                desc: "(no runtime to query process info)".to_owned(),
            })
            .collect();
    };
    endpoints
        .into_iter()
        .map(|endpoint| {
            let pid = endpoint.pid;
            let client = DiagnosticsClient::new(endpoint);
            let desc = match runtime.block_on(client.process_info()) {
                Ok(info) => cap(&info.command_line),
                Err(error) => format!("(no process info: {error})"),
            };
            ProcessRow { pid, desc }
        })
        .collect()
}

fn cap(desc: &str) -> String {
    if desc.chars().count() > DESC_CAP {
        let capped: String = desc.chars().take(DESC_CAP - 1).collect();
        format!("{capped}…")
    } else {
        desc.to_owned()
    }
}
