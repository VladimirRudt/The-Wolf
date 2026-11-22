//! The live view: ratatui charts and panels over the tapped [`Watch`] state, key handling,
//! and the storage panel whose queries go through the store blocks ([`StoreOp::Query`]).
//!
//! Keys: `Tab` cycles the focused target, `t`/`e`/`p` run storage queries (targets,
//! events, points), `↑`/`↓` scroll the panel, `q`/`Esc` stop. Rendering is a full redraw
//! of a fixed layout; ratatui diffs it onto the terminal.

use std::io::{self, Stdout};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver};
use std::time::Duration;

use crossterm::event::{self, KeyCode, KeyEventKind};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::symbols;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Axis, Block, Chart, Dataset, GraphType, List, ListItem, Paragraph};
use ratatui::{Frame, Terminal};
use wolf_contract::{SeriesDescriptor, TargetId, Timestamp, Unit};

use super::{
    ALLOC_RATE, COMMITTED, FRAGMENTATION, GENERATIONS, HEAP_SIZE, HEAP_TOTAL, HISTORY, PRIVATE,
    RSS, THREADS, TIME_IN_GC, TargetState, Watch, bytes, clock, uptime,
};
use crate::blocks::store::{Query, QueryOp, QueryOutcome, QueryReply, StoreOp};
use crate::dataflow::Target;

const MIB: f64 = 1024.0 * 1024.0;
/// How far back the storage panel's point/event queries reach.
const QUERY_RANGE_NS: i64 = 3_600_000_000_000;
/// Panel line length cap.
const LINE_CAP: usize = 100;

/// Raw mode + alternate screen while a watch screen (view or picker) runs; drop restores.
#[derive(Debug)]
pub(crate) struct TerminalGuard {
    _private: (),
}

impl TerminalGuard {
    fn enter() -> io::Result<Self> {
        crossterm::terminal::enable_raw_mode()?;
        crossterm::execute!(
            io::stdout(),
            crossterm::terminal::EnterAlternateScreen,
            crossterm::cursor::Hide
        )?;
        Ok(Self { _private: () })
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = crossterm::terminal::disable_raw_mode();
        let _ = crossterm::execute!(
            io::stdout(),
            crossterm::cursor::Show,
            crossterm::terminal::LeaveAlternateScreen
        );
    }
}

/// A prepared terminal for any watch screen.
pub(crate) fn terminal() -> io::Result<(TerminalGuard, Terminal<CrosstermBackend<Stdout>>)> {
    let guard = TerminalGuard::enter()?;
    let mut terminal = Terminal::new(CrosstermBackend::new(io::stdout()))?;
    terminal.clear()?;
    Ok((guard, terminal))
}

/// The live view: owns the terminal, draws the tapped state.
#[derive(Debug)]
pub(crate) struct View {
    _guard: TerminalGuard,
    terminal: Terminal<CrosstermBackend<Stdout>>,
}

impl View {
    pub(crate) fn enter() -> io::Result<Self> {
        let (guard, terminal) = terminal()?;
        Ok(Self {
            _guard: guard,
            terminal,
        })
    }

    pub(crate) fn draw(&mut self, watch: &Watch, view: &mut ViewState) {
        drain_panel(&mut view.panel);
        let _ = self.terminal.draw(|frame| draw_frame(frame, watch, view));
    }
}

/// Everything the view needs that is not tapped telemetry: focus, scroll, storage panel.
#[derive(Debug)]
pub(crate) struct ViewState {
    /// Index into `Watch`'s target list; `Tab` cycles it.
    focus: usize,
    /// Storage panel scroll offset in lines.
    scroll: u16,
    panel: Panel,
}

impl Default for ViewState {
    fn default() -> Self {
        Self {
            focus: 0,
            scroll: 0,
            panel: Panel::idle(),
        }
    }
}

impl ViewState {
    /// Drains the pending key events and applies them.
    pub(crate) fn handle_input(
        &mut self,
        watch: &Watch,
        store_inputs: &[Target<StoreOp>],
        shutdown: &Arc<AtomicBool>,
    ) {
        while event::poll(Duration::ZERO).unwrap_or(false) {
            let Ok(event::Event::Key(key)) = event::read() else {
                continue;
            };
            if key.kind != KeyEventKind::Press {
                continue;
            }
            match key.code {
                KeyCode::Char('q') | KeyCode::Esc => shutdown.store(true, Ordering::Relaxed),
                KeyCode::Tab => {
                    let count = watch.lock().targets.len();
                    if count > 0 {
                        self.focus = (self.focus + 1) % count;
                    }
                }
                KeyCode::Up => self.scroll = self.scroll.saturating_sub(1),
                KeyCode::Down => self.scroll = self.scroll.saturating_add(1),
                KeyCode::PageUp => self.scroll = self.scroll.saturating_sub(10),
                KeyCode::PageDown => self.scroll = self.scroll.saturating_add(10),
                KeyCode::Char('t') => {
                    self.start_query(QueryOp::Targets, "targets".to_owned(), store_inputs);
                }
                KeyCode::Char('e') => {
                    if let Some(target) = self.focused_target(watch) {
                        let op = QueryOp::Events {
                            target: target.clone(),
                            range: last_hour(),
                        };
                        self.start_query(op, format!("events of {target} (1 h)"), store_inputs);
                    }
                }
                KeyCode::Char('p') => {
                    if let Some(target) = self.focused_target(watch) {
                        let series = SeriesDescriptor::new(
                            target.clone(),
                            HEAP_TOTAL,
                            Default::default(),
                            Unit::Bytes,
                        )
                        .id();
                        let op = QueryOp::Points {
                            series,
                            range: last_hour(),
                        };
                        self.start_query(
                            op,
                            format!("points {HEAP_TOTAL} of {target} (1 h)"),
                            store_inputs,
                        );
                    }
                }
                _ => {}
            }
        }
    }

    fn focused_target(&self, watch: &Watch) -> Option<TargetId> {
        watch.lock().targets.get(self.focus).cloned()
    }

    /// Posts one query to every shard's store block; the panel collects the replies.
    fn start_query(&mut self, op: QueryOp, title: String, store_inputs: &[Target<StoreOp>]) {
        let (tx, rx) = mpsc::channel();
        let mut sent = 0;
        for input in store_inputs {
            let query = Query {
                op: op.clone(),
                reply: tx.clone(),
            };
            if input.post(StoreOp::Query(query)).is_ok() {
                sent += 1;
            }
        }
        self.scroll = 0;
        self.panel = Panel {
            title,
            op,
            expected: sent,
            received: 0,
            results: Vec::new(),
            errors: Vec::new(),
            rx: (sent > 0).then_some(rx),
            lines: vec!["querying…".to_owned()],
        };
    }
}

/// One in-flight or finished storage query.
#[derive(Debug)]
struct Panel {
    title: String,
    op: QueryOp,
    /// Replies expected (one per store block that accepted the query).
    expected: usize,
    received: usize,
    results: Vec<QueryReply>,
    errors: Vec<String>,
    rx: Option<Receiver<QueryOutcome>>,
    /// Rendered result lines.
    lines: Vec<String>,
}

impl Panel {
    fn idle() -> Self {
        Self {
            title: "storage".to_owned(),
            op: QueryOp::Targets,
            expected: 0,
            received: 0,
            results: Vec::new(),
            errors: Vec::new(),
            rx: None,
            lines: vec![
                "run a query against the live store:".to_owned(),
                "  t — list targets".to_owned(),
                "  e — events of the focused target (last hour)".to_owned(),
                "  p — dotnet.heap.total points of the focused target (last hour)".to_owned(),
                "queries run inside the store blocks; they see flushed batches only".to_owned(),
            ],
        }
    }
}

/// The range of the storage panel's queries: the last hour up to now.
fn last_hour() -> std::ops::Range<Timestamp> {
    let end = Timestamp::now();
    Timestamp::from_nanos(end.as_nanos().saturating_sub(QUERY_RANGE_NS))..end
}

/// Collects shard replies; merges them into the panel's lines when all are in.
fn drain_panel(panel: &mut Panel) {
    let Some(rx) = &panel.rx else { return };
    while let Ok(outcome) = rx.try_recv() {
        panel.received += 1;
        match outcome {
            Ok(reply) => panel.results.push(reply),
            Err(error) => panel.errors.push(error),
        }
    }
    if panel.received < panel.expected {
        panel.lines = vec![format!("querying… ({}/{})", panel.received, panel.expected)];
        return;
    }
    panel.rx = None;
    let mut lines = finalize(&panel.op, &panel.results);
    for error in &panel.errors {
        lines.push(cap(&format!("shard error: {error}")));
    }
    if lines.is_empty() {
        lines.push("(nothing stored yet)".to_owned());
    }
    panel.lines = lines;
}

/// Merges every shard's reply into display lines.
fn finalize(op: &QueryOp, results: &[QueryReply]) -> Vec<String> {
    match op {
        QueryOp::Targets => {
            let mut seen = std::collections::BTreeMap::new();
            for reply in results {
                let QueryReply::Targets(targets) = reply else {
                    continue;
                };
                for target in targets {
                    seen.entry(target.id.to_string())
                        .or_insert(target.first_seen);
                }
            }
            let mut lines = vec![format!("{} target(s)", seen.len())];
            for (id, first_seen) in seen {
                lines.push(cap(&format!("{id}  first seen {}", clock(first_seen))));
            }
            lines
        }
        QueryOp::Events { .. } => {
            let mut events: Vec<_> = results
                .iter()
                .filter_map(|reply| match reply {
                    QueryReply::Events(events) => Some(events.iter()),
                    _ => None,
                })
                .flatten()
                .collect();
            events.sort_by_key(|event| event.ts);
            let mut lines = vec![format!("{} event(s) in range", events.len())];
            for event in events.iter().rev().take(20).rev() {
                let data = serde_json::to_string(&event.data).unwrap_or_default();
                lines.push(cap(&format!(
                    "{}  {}  {}",
                    clock(event.ts),
                    event.kind,
                    data
                )));
            }
            lines
        }
        QueryOp::Points { .. } => {
            let mut points: Vec<_> = results
                .iter()
                .filter_map(|reply| match reply {
                    QueryReply::Points(points) => Some(points.iter()),
                    _ => None,
                })
                .flatten()
                .collect();
            points.sort_by_key(|point| point.ts);
            if points.is_empty() {
                return Vec::new();
            }
            let min = points.iter().map(|p| p.value).fold(f64::INFINITY, f64::min);
            let max = points
                .iter()
                .map(|p| p.value)
                .fold(f64::NEG_INFINITY, f64::max);
            let last = points[points.len() - 1];
            let mut lines = vec![
                format!("{} point(s) in range", points.len()),
                cap(&format!(
                    "min {}  max {}  last {} @ {}",
                    bytes(min),
                    bytes(max),
                    bytes(last.value),
                    clock(last.ts)
                )),
                "latest:".to_owned(),
            ];
            for point in points.iter().rev().take(5).rev() {
                lines.push(cap(&format!(
                    "  {}  {}",
                    clock(point.ts),
                    bytes(point.value)
                )));
            }
            lines
        }
    }
}

fn cap(line: &str) -> String {
    if line.chars().count() > LINE_CAP {
        let capped: String = line.chars().take(LINE_CAP - 1).collect();
        format!("{capped}…")
    } else {
        line.to_owned()
    }
}

/// One full redraw.
fn draw_frame(frame: &mut Frame, watch: &Watch, view: &mut ViewState) {
    let label = Style::default().add_modifier(Modifier::DIM);
    let chunks = Layout::vertical([
        Constraint::Length(1), // header
        Constraint::Min(8),    // charts
        Constraint::Length(3), // values
        Constraint::Min(6),    // gc log | storage panel
        Constraint::Length(1), // key help
    ])
    .split(frame.area());

    let state = watch.lock();
    let target = state.targets.get(view.focus).cloned();
    let focused = target.as_ref().and_then(|t| state.current.get(t));

    let header = match &target {
        Some(target) => format!(
            "mrwolf watch — {target} (target {}/{}) — up {} — ctrl-c/q to stop",
            view.focus + 1,
            state.targets.len(),
            uptime(watch.started().elapsed())
        ),
        None => format!(
            "mrwolf watch — waiting for targets… — up {}",
            uptime(watch.started().elapsed())
        ),
    };
    frame.render_widget(
        Paragraph::new(Span::styled(
            header,
            Style::default().add_modifier(Modifier::BOLD),
        )),
        chunks[0],
    );

    let Some(focused) = focused else {
        frame.render_widget(
            Paragraph::new(Span::styled("no telemetry yet", label)),
            chunks[1],
        );
        return;
    };

    let charts = Layout::horizontal([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(chunks[1]);
    let now_ns = Timestamp::now().as_nanos();
    // Chart data borrows from these vectors; they must outlive the render calls.
    let heap_data = chart_data(focused, now_ns, &HEAP_SERIES);
    let process_data = chart_data(focused, now_ns, &PROCESS_SERIES);
    frame.render_widget(
        chart(" GC heap (MiB) ", &HEAP_SERIES, &heap_data),
        charts[0],
    );
    frame.render_widget(
        chart(" process memory (MiB) ", &PROCESS_SERIES, &process_data),
        charts[1],
    );

    frame.render_widget(Paragraph::new(value_lines(focused, label)), chunks[2]);

    let bottom = Layout::horizontal([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(chunks[3]);
    let visible_gc = bottom[0].height.saturating_sub(2) as usize;
    let gc_items: Vec<ListItem> = focused
        .gc_log
        .iter()
        .rev()
        .take(visible_gc)
        .rev()
        .map(|entry| ListItem::new(entry.as_str()))
        .collect();
    let gc_block = Block::bordered().title(Span::styled(" recent GCs (UTC) ", label));
    if gc_items.is_empty() {
        frame.render_widget(
            Paragraph::new(Span::styled("  (none yet)", label)).block(gc_block),
            bottom[0],
        );
    } else {
        frame.render_widget(List::new(gc_items).block(gc_block), bottom[0]);
    }

    let panel_height = bottom[1].height.saturating_sub(2) as usize;
    let max_scroll = view.panel.lines.len().saturating_sub(panel_height) as u16;
    view.scroll = view.scroll.min(max_scroll);
    let panel_lines: Vec<Line> = view
        .panel
        .lines
        .iter()
        .map(|line| Line::from(line.as_str()))
        .collect();
    frame.render_widget(
        Paragraph::new(panel_lines)
            .block(Block::bordered().title(Span::styled(
                format!(" storage — {} ", view.panel.title),
                label,
            )))
            .scroll((view.scroll, 0)),
        bottom[1],
    );

    frame.render_widget(
        Paragraph::new(Span::styled(
            "Tab target · t targets · e events · p points · ↑/↓ scroll · q quit",
            label,
        )),
        chunks[4],
    );
}

/// One chart series: legend name, color, metric, optional generation label.
type ChartSeries = (&'static str, Color, &'static str, Option<&'static str>);

/// The GC heap chart: `dotnet.gc.heap.size` per generation plus the heap-total counter.
const HEAP_SERIES: [ChartSeries; 6] = [
    ("gen0", Color::Gray, HEAP_SIZE, Some("gen0")),
    ("gen1", Color::Cyan, HEAP_SIZE, Some("gen1")),
    ("gen2", Color::Red, HEAP_SIZE, Some("gen2")),
    ("loh", Color::Yellow, HEAP_SIZE, Some("loh")),
    ("poh", Color::Magenta, HEAP_SIZE, Some("poh")),
    ("total", Color::White, HEAP_TOTAL, None),
];

/// The process memory chart.
const PROCESS_SERIES: [ChartSeries; 2] = [
    ("rss", Color::Green, RSS, None),
    ("private", Color::Blue, PRIVATE, None),
];

/// (seconds ago, MiB) points for every series of a chart, in `series` order.
fn chart_data(target: &TargetState, now_ns: i64, series: &[ChartSeries]) -> Vec<Vec<(f64, f64)>> {
    series
        .iter()
        .map(|(.., metric, generation)| {
            target
                .history(metric, *generation)
                .map(|history| {
                    history
                        .iter()
                        .map(|(ts, value)| ((*ts - now_ns) as f64 / 1e9, value / MIB))
                        .collect()
                })
                .unwrap_or_default()
        })
        .collect()
}

fn chart<'a>(
    title: &'static str,
    series: &[ChartSeries],
    data: &'a [Vec<(f64, f64)>],
) -> Chart<'a> {
    let y_max = data
        .iter()
        .flatten()
        .map(|(_, value)| *value)
        .fold(1.0_f64, f64::max)
        * 1.1;
    let datasets: Vec<Dataset> = series
        .iter()
        .zip(data)
        .filter(|(_, points)| !points.is_empty())
        .map(|((name, color, ..), points)| {
            Dataset::default()
                .name(*name)
                .marker(symbols::Marker::Braille)
                .graph_type(GraphType::Line)
                .style(Style::default().fg(*color))
                .data(points)
        })
        .collect();
    let axis_style = Style::default().fg(Color::DarkGray);
    Chart::new(datasets)
        .block(Block::bordered().title(title))
        .x_axis(
            Axis::default()
                .style(axis_style)
                .bounds([-(HISTORY as f64), 0.0])
                .labels(vec![Span::raw("-5m"), Span::raw("now")]),
        )
        .y_axis(
            Axis::default()
                .style(axis_style)
                .bounds([0.0, y_max])
                .labels(vec![Span::raw("0"), Span::raw(format!("{y_max:.0}"))]),
        )
}

/// The latest-values block: generation sizes, then the counter and process lines.
fn value_lines(target: &TargetState, label: Style) -> Vec<Line<'static>> {
    let mut gen_spans = vec![Span::styled("GC heap    ", label)];
    for generation in GENERATIONS {
        let value = target
            .latest(HEAP_SIZE, Some(generation))
            .map_or("—".to_owned(), bytes);
        gen_spans.push(Span::raw(format!("{generation} {value}   ")));
    }
    let committed = target.latest(COMMITTED, None).map_or("—".into(), bytes);
    let fragmentation = target
        .latest(FRAGMENTATION, None)
        .map_or("—".into(), |v| format!("{v:.0} %"));
    let alloc = target
        .latest(ALLOC_RATE, None)
        .map_or("—".into(), |v| format!("{:.1} MB/s", v / 1e6));
    let in_gc = target
        .latest(TIME_IN_GC, None)
        .map_or("—".into(), |v| format!("{v:.1} %"));
    let rss = target.latest(RSS, None).map_or("—".into(), bytes);
    let private = target.latest(PRIVATE, None).map_or("—".into(), bytes);
    let threads = target
        .latest(THREADS, None)
        .map_or("—".into(), |v| format!("{v:.0}"));
    vec![
        Line::from(gen_spans),
        Line::from(vec![
            Span::styled("committed ", label),
            Span::raw(format!("  {committed:>10}   fragmentation {fragmentation}")),
            Span::raw("   "),
            Span::styled("alloc rate", label),
            Span::raw(format!("  {alloc:>10}   time in GC {in_gc}")),
        ]),
        Line::from(vec![
            Span::styled("process   ", label),
            Span::raw(format!(
                "  rss {rss:>10}   private {private}   threads {threads}"
            )),
        ]),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::backend::TestBackend;
    use std::collections::BTreeMap;
    use wolf_contract::{Event, Point};
    use wolf_storage::TargetInfo;

    fn target() -> TargetId {
        "pid:4812.1760843645@srv-a".parse().expect("valid target")
    }

    fn register_heap_total(watch: &Watch, target: &TargetId) {
        watch.tap(&StoreOp::Register {
            target: target.clone(),
            first_seen: Timestamp::from_nanos(1),
            descriptors: vec![SeriesDescriptor::new(
                target.clone(),
                HEAP_TOTAL,
                BTreeMap::new(),
                Unit::Bytes,
            )]
            .into_boxed_slice(),
        });
    }

    fn heap_total_point(target: &TargetId, ts: i64, value: f64) -> StoreOp {
        let descriptor =
            SeriesDescriptor::new(target.clone(), HEAP_TOTAL, BTreeMap::new(), Unit::Bytes);
        StoreOp::Point(Point {
            series: descriptor.id(),
            ts: Timestamp::from_nanos(ts),
            value,
        })
    }

    #[test]
    fn draw_shows_charts_and_panels() {
        let watch = Watch::new();
        register_heap_total(&watch, &target());
        for i in 0..10 {
            watch.tap(&heap_total_point(
                &target(),
                Timestamp::now().as_nanos() - (10 - i) * 1_000_000_000,
                (100 + i) as f64 * MIB,
            ));
        }
        let mut terminal = Terminal::new(TestBackend::new(120, 30)).expect("test backend");
        let mut view = ViewState::default();
        terminal
            .draw(|frame| draw_frame(frame, &watch, &mut view))
            .expect("draw");
        let buffer = terminal.backend().buffer();
        let mut text = String::new();
        for y in 0..buffer.area.height {
            for x in 0..buffer.area.width {
                text.push_str(buffer[(x, y)].symbol());
            }
            text.push('\n');
        }
        assert!(text.contains("mrwolf watch — pid:4812.1760843645@srv-a (target 1/1)"));
        assert!(text.contains("GC heap (MiB)"));
        assert!(text.contains("process memory (MiB)"));
        assert!(text.contains("recent GCs (UTC)"));
        assert!(text.contains("storage"));
    }

    #[test]
    fn targets_finalize_dedups_across_shards() {
        let info = |id: &str, ts| TargetInfo {
            id: id.parse().expect("target"),
            first_seen: Timestamp::from_nanos(ts),
        };
        let results = vec![
            QueryReply::Targets(vec![info("pid:1.1@h", 10), info("pid:2.2@h", 20)]),
            QueryReply::Targets(vec![info("pid:2.2@h", 20), info("pid:3.3@h", 30)]),
        ];
        let lines = finalize(&QueryOp::Targets, &results);
        assert_eq!(lines[0], "3 target(s)");
        assert!(lines.iter().any(|l| l.starts_with("pid:3.3@h")));
    }

    #[test]
    fn events_finalize_sorts_and_limits() {
        let event = |ts| Event {
            ts: Timestamp::from_nanos(ts),
            target: target(),
            kind: "dotnet.gc".to_owned(),
            data: serde_json::json!({"pause_ms": 1.0})
                .as_object()
                .unwrap()
                .clone(),
        };
        let results = vec![
            QueryReply::Events(vec![event(30_000_000_000), event(10_000_000_000)]),
            QueryReply::Events(vec![event(20_000_000_000)]),
        ];
        let op = QueryOp::Events {
            target: target(),
            range: Timestamp::from_nanos(0)..Timestamp::from_nanos(100_000_000_000),
        };
        let lines = finalize(&op, &results);
        assert_eq!(lines[0], "3 event(s) in range");
        assert_eq!(lines.len(), 4);
        assert!(lines[1].contains("dotnet.gc"));
        // Ascending order after the merge: clock times increase.
        assert!(lines[1] < lines[2] && lines[2] < lines[3]);
    }

    #[test]
    fn points_finalize_summarizes() {
        let point = |ts, value| Point {
            series: wolf_contract::SeriesId(1),
            ts: Timestamp::from_nanos(ts),
            value,
        };
        let results = vec![
            QueryReply::Points(vec![point(10, 2.0 * MIB), point(30, 4.0 * MIB)]),
            QueryReply::Points(vec![point(20, 3.0 * MIB)]),
        ];
        let op = QueryOp::Points {
            series: wolf_contract::SeriesId(1),
            range: Timestamp::from_nanos(0)..Timestamp::from_nanos(100),
        };
        let lines = finalize(&op, &results);
        assert_eq!(lines[0], "3 point(s) in range");
        assert!(lines[1].contains("min 2.0 MiB"));
        assert!(lines[1].contains("max 4.0 MiB"));
    }

    #[test]
    fn empty_results_say_so() {
        let op = QueryOp::Points {
            series: wolf_contract::SeriesId(1),
            range: Timestamp::from_nanos(0)..Timestamp::from_nanos(100),
        };
        assert!(finalize(&op, &[]).is_empty());
    }
}
