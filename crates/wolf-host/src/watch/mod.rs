//! The `mrwolf watch` live terminal view (P1-9). A store cannot be read while the host
//! writes it (DuckDB allows one writer or many readers, never both), so the view lives
//! inside the collector process:
//!
//! - [`Watch`] is the tap: every shard's store block — the graph's fan-in point — feeds
//!   it each [`StoreOp`] it handles, so the view sees every target's points and events
//!   without touching the store files.
//! - [`view`] renders the tapped state with ratatui: line charts, latest values, the GC
//!   log, and a storage panel whose queries go back through the store blocks
//!   ([`StoreOp::Query`]) — the blocks own the shard files, so they are the only safe
//!   readers while the host runs.
//! - [`picker`] is the setup screen shown when `watch` gets no pids: pick targets and
//!   the shard count before the graph is built.
//!
//! The tap is cheap and never grows: a map insert per registered series, two map writes
//! per point, a capped ring for GC events, fixed-size histories for the charted series.

pub(crate) mod picker;
pub(crate) mod view;

pub use picker::{PickerResult, pick_targets};

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Instant;

use wolf_contract::{Event, SeriesId, TargetId, Timestamp};

use crate::blocks::store::StoreOp;

/// Catalogue names the view reads (`docs/collector-dotnet.md` §4 and §5).
pub(crate) const HEAP_SIZE: &str = "dotnet.gc.heap.size";
pub(crate) const HEAP_TOTAL: &str = "dotnet.heap.total";
pub(crate) const COMMITTED: &str = "dotnet.gc.committed";
pub(crate) const FRAGMENTATION: &str = "dotnet.gc.fragmentation";
pub(crate) const ALLOC_RATE: &str = "dotnet.alloc.rate";
pub(crate) const TIME_IN_GC: &str = "dotnet.gc.time_in_gc";
pub(crate) const RSS: &str = "process.memory.rss";
pub(crate) const PRIVATE: &str = "process.memory.private";
pub(crate) const THREADS: &str = "process.threads";

/// Display order of the `dotnet.gc.heap.size` generations.
pub(crate) const GENERATIONS: [&str; 5] = ["gen0", "gen1", "gen2", "loh", "poh"];

/// GC log lines kept for display, per target.
const GC_LOG_LINES: usize = 20;
/// History kept per charted series; at the 1 s counter cadence this is five minutes.
pub(crate) const HISTORY: usize = 300;

/// The metrics with a history worth charting.
fn charted(metric: &str) -> bool {
    matches!(metric, HEAP_SIZE | HEAP_TOTAL | RSS | PRIVATE)
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// What the tap knows about one series: enough to route its points to a display slot.
type SeriesMeta = (String, BTreeMap<String, String>);

/// Per-target tapped state.
#[derive(Debug, Default)]
pub(crate) struct TargetState {
    /// SeriesId → (metric, labels) of this target, learned from `Register` ops.
    series: HashMap<SeriesId, SeriesMeta>,
    /// Latest value per series.
    latest: HashMap<SeriesId, f64>,
    /// (ts ns, value) of the charted series only, capped at [`HISTORY`].
    history: HashMap<SeriesId, VecDeque<(i64, f64)>>,
    /// Most recent GC events, oldest first, preformatted with their UTC clock time.
    gc_log: VecDeque<String>,
}

impl TargetState {
    /// The id of this target's series for a metric and optional generation label.
    pub(crate) fn series_id(&self, metric: &str, generation: Option<&str>) -> Option<SeriesId> {
        self.series
            .iter()
            .find(|(_, (m, labels))| {
                m == metric
                    && generation.is_none_or(|g| labels.get("generation").is_some_and(|l| l == g))
            })
            .map(|(id, _)| *id)
    }

    pub(crate) fn latest(&self, metric: &str, generation: Option<&str>) -> Option<f64> {
        self.series_id(metric, generation)
            .and_then(|id| self.latest.get(&id).copied())
    }

    /// The (ts, value) history of a charted series.
    pub(crate) fn history(
        &self,
        metric: &str,
        generation: Option<&str>,
    ) -> Option<&VecDeque<(i64, f64)>> {
        self.series_id(metric, generation)
            .and_then(|id| self.history.get(&id))
    }
}

/// All tapped state: every attached target, in first-register order.
#[derive(Debug, Default)]
pub(crate) struct State {
    /// Attach order; the view's focus index cycles this.
    pub(crate) targets: Vec<TargetId>,
    pub(crate) current: HashMap<TargetId, TargetState>,
    /// SeriesId → owning target, so points route to their target's state.
    owners: HashMap<SeriesId, TargetId>,
}

/// Shared view state: tapped by the store blocks, rendered by the main thread.
#[derive(Debug)]
pub struct Watch {
    /// Targets the view never shows (the host's own self-telemetry, D11).
    excluded: Vec<TargetId>,
    started: Instant,
    state: Mutex<State>,
}

impl Default for Watch {
    fn default() -> Self {
        Self::new()
    }
}

impl Watch {
    pub fn new() -> Self {
        Self {
            excluded: Vec::new(),
            started: Instant::now(),
            state: Mutex::new(State::default()),
        }
    }

    /// Never show this target (registered anyway: it shares the store blocks).
    pub fn excluding(mut self, target: TargetId) -> Self {
        self.excluded.push(target);
        self
    }

    pub(crate) fn started(&self) -> Instant {
        self.started
    }

    pub(crate) fn lock(&self) -> MutexGuard<'_, State> {
        lock(&self.state)
    }

    /// Called by a store block with every op it handles, on that block's thread.
    pub fn tap(&self, op: &StoreOp) {
        match op {
            StoreOp::Register {
                target,
                descriptors,
                ..
            } => {
                if self.excluded.contains(target) {
                    return;
                }
                let mut guard = lock(&self.state);
                let state = &mut *guard;
                let entry = state.current.entry(target.clone()).or_insert_with(|| {
                    state.targets.push(target.clone());
                    TargetState::default()
                });
                for descriptor in descriptors {
                    entry.series.insert(
                        descriptor.id(),
                        (descriptor.metric().to_owned(), descriptor.labels().clone()),
                    );
                    state.owners.insert(descriptor.id(), target.clone());
                }
            }
            StoreOp::Point(point) => {
                let mut guard = lock(&self.state);
                let state = &mut *guard;
                let Some(owner) = state.owners.get(&point.series) else {
                    return;
                };
                let charted = state
                    .current
                    .get(owner)
                    .and_then(|ts| ts.series.get(&point.series))
                    .is_some_and(|(metric, _)| charted(metric));
                let entry = state
                    .current
                    .get_mut(owner)
                    .expect("registered before its points");
                entry.latest.insert(point.series, point.value);
                if charted {
                    let history = entry.history.entry(point.series).or_default();
                    if history.len() == HISTORY {
                        history.pop_front();
                    }
                    history.push_back((point.ts.as_nanos(), point.value));
                }
            }
            StoreOp::Event(event) => {
                let line = match event.kind.as_str() {
                    "dotnet.gc" => format_gc(event),
                    "dotnet.gc.gen0.summary" => format_gen0(event),
                    _ => None,
                };
                if let Some(line) = line {
                    let mut state = lock(&self.state);
                    if let Some(entry) = state.current.get_mut(&event.target) {
                        if entry.gc_log.len() == GC_LOG_LINES {
                            entry.gc_log.pop_front();
                        }
                        entry.gc_log.push_back(line);
                    }
                }
            }
            StoreOp::Query(_) => {}
        }
    }
}

/// One `dotnet.gc` event for the log: `12:01:09  gen2 background AllocSmall  pause 7.9 ms`.
/// Fields are read defensively — a malformed event shows what it has (ground rule 4).
fn format_gc(event: &Event) -> Option<String> {
    let data = &event.data;
    let generation = data.get("generation")?.as_u64()?;
    let kind = data.get("type").and_then(|v| v.as_str()).unwrap_or("?");
    let reason = data.get("reason").and_then(|v| v.as_str()).unwrap_or("?");
    let pause = data.get("pause_ms").and_then(|v| v.as_f64()).unwrap_or(0.0);
    Some(format!(
        "{}  gen{generation} {kind} {reason}  pause {pause:.1} ms",
        clock(event.ts)
    ))
}

/// One `dotnet.gc.gen0.summary`: `12:01:09  gen0 ×42  pause 31.0 ms (max 2.1 ms)`.
fn format_gen0(event: &Event) -> Option<String> {
    let data = &event.data;
    let count = data.get("count")?.as_u64()?;
    let total = data.get("total_pause_ms")?.as_f64()?;
    let max = data.get("max_pause_ms")?.as_f64()?;
    Some(format!(
        "{}  gen0 ×{count}  pause {total:.1} ms (max {max:.1} ms)",
        clock(event.ts)
    ))
}

/// UTC wall clock of a timestamp, `HH:MM:SS`.
pub(crate) fn clock(ts: Timestamp) -> String {
    let secs = ts.as_nanos().div_euclid(1_000_000_000).rem_euclid(86_400);
    format!(
        "{:02}:{:02}:{:02}",
        secs / 3600,
        (secs / 60) % 60,
        secs % 60
    )
}

/// `01:23:45`-style elapsed time.
pub(crate) fn uptime(elapsed: std::time::Duration) -> String {
    let secs = elapsed.as_secs();
    format!(
        "{:02}:{:02}:{:02}",
        secs / 3600,
        (secs / 60) % 60,
        secs % 60
    )
}

/// Human bytes: `512 B`, `12.0 KiB`, `48.3 MiB`, `1.5 GiB`.
pub(crate) fn bytes(value: f64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    const GIB: f64 = MIB * 1024.0;
    if value.abs() >= GIB {
        format!("{:.1} GiB", value / GIB)
    } else if value.abs() >= MIB {
        format!("{:.1} MiB", value / MIB)
    } else if value.abs() >= KIB {
        format!("{:.1} KiB", value / KIB)
    } else {
        format!("{value:.0} B")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wolf_contract::{Point, SeriesDescriptor, Unit};

    fn target() -> TargetId {
        "pid:4812.1760843645@srv-a".parse().expect("valid target")
    }

    fn other_target() -> TargetId {
        "pid:9999.1760843645@srv-a".parse().expect("valid target")
    }

    fn register(target: &TargetId, metric: &str, generation: Option<&str>) -> StoreOp {
        let labels = generation
            .map(|g| BTreeMap::from([("generation".to_owned(), g.to_owned())]))
            .unwrap_or_default();
        StoreOp::Register {
            target: target.clone(),
            first_seen: Timestamp::from_nanos(1),
            descriptors: vec![SeriesDescriptor::new(
                target.clone(),
                metric,
                labels,
                Unit::Bytes,
            )]
            .into_boxed_slice(),
        }
    }

    fn point(target: &TargetId, metric: &str, generation: Option<&str>, value: f64) -> StoreOp {
        let labels = generation
            .map(|g| BTreeMap::from([("generation".to_owned(), g.to_owned())]))
            .unwrap_or_default();
        let descriptor = SeriesDescriptor::new(target.clone(), metric, labels, Unit::Bytes);
        StoreOp::Point(Point {
            series: descriptor.id(),
            ts: Timestamp::from_nanos(42),
            value,
        })
    }

    #[test]
    fn tap_routes_series_to_their_target() {
        let watch = Watch::new();
        watch.tap(&register(&target(), HEAP_TOTAL, None));
        watch.tap(&register(&target(), HEAP_SIZE, Some("gen2")));
        watch.tap(&register(&other_target(), HEAP_TOTAL, None));
        watch.tap(&point(&target(), HEAP_TOTAL, None, 100.0 * 1024.0 * 1024.0));
        watch.tap(&point(
            &other_target(),
            HEAP_TOTAL,
            None,
            5.0 * 1024.0 * 1024.0,
        ));

        let state = watch.lock();
        assert_eq!(state.targets, vec![target(), other_target()]);
        let first = &state.current[&target()];
        assert_eq!(
            first.latest(HEAP_TOTAL, None),
            Some(100.0 * 1024.0 * 1024.0)
        );
        // Registered but no points yet: known series, no value.
        assert_eq!(first.latest(HEAP_SIZE, Some("gen2")), None);
        let second = &state.current[&other_target()];
        assert_eq!(second.latest(HEAP_TOTAL, None), Some(5.0 * 1024.0 * 1024.0));
        // Charted metrics keep a history.
        assert_eq!(first.history(HEAP_TOTAL, None).expect("history").len(), 1);
    }

    #[test]
    fn re_registering_a_target_keeps_its_place_and_series() {
        let watch = Watch::new();
        watch.tap(&register(&target(), HEAP_TOTAL, None));
        watch.tap(&register(&other_target(), HEAP_TOTAL, None));
        watch.tap(&register(&target(), HEAP_SIZE, Some("gen0")));
        let state = watch.lock();
        assert_eq!(state.targets, vec![target(), other_target()]);
        assert_eq!(state.current[&target()].series.len(), 2);
    }

    #[test]
    fn points_of_unknown_series_are_ignored() {
        let watch = Watch::new();
        watch.tap(&point(&target(), HEAP_TOTAL, None, 1.0));
        assert!(watch.lock().targets.is_empty());
    }

    #[test]
    fn gc_events_fill_a_capped_log() {
        let watch = Watch::new();
        watch.tap(&register(&target(), HEAP_TOTAL, None));
        for n in 0..GC_LOG_LINES + 3 {
            watch.tap(&StoreOp::Event(Event {
                ts: Timestamp::from_nanos(1_760_843_645_000_000_000),
                target: target(),
                kind: "dotnet.gc".to_owned(),
                data: serde_json::json!({
                    "gc_number": n, "generation": 2, "reason": "AllocSmall",
                    "type": "background", "pause_ms": 7.9
                })
                .as_object()
                .expect("object")
                .clone(),
            }));
        }
        let state = watch.lock();
        let log = &state.current[&target()].gc_log;
        assert_eq!(log.len(), GC_LOG_LINES);
        assert!(log[0].contains("gen2 background AllocSmall"));
        assert!(log[0].contains("pause 7.9 ms"));
        assert!(log[0].starts_with("03:14:05")); // 1760843645 s UTC
    }

    #[test]
    fn malformed_events_are_skipped() {
        let watch = Watch::new();
        watch.tap(&register(&target(), HEAP_TOTAL, None));
        watch.tap(&StoreOp::Event(Event {
            ts: Timestamp::from_nanos(1),
            target: target(),
            kind: "dotnet.gc".to_owned(),
            data: serde_json::json!({ "unexpected": true })
                .as_object()
                .expect("object")
                .clone(),
        }));
        assert!(watch.lock().current[&target()].gc_log.is_empty());
    }

    #[test]
    fn excluded_targets_are_never_shown() {
        let watch = Watch::new().excluding(other_target());
        watch.tap(&register(&other_target(), HEAP_TOTAL, None));
        watch.tap(&register(&target(), HEAP_TOTAL, None));
        watch.tap(&StoreOp::Event(Event {
            ts: Timestamp::from_nanos(1_760_843_645_000_000_000),
            target: other_target(),
            kind: "dotnet.gc".to_owned(),
            data: serde_json::json!({"generation": 2, "pause_ms": 1.0})
                .as_object()
                .expect("object")
                .clone(),
        }));
        let state = watch.lock();
        assert_eq!(state.targets, vec![target()]);
    }

    #[test]
    fn bytes_picks_a_sensible_unit() {
        assert_eq!(bytes(512.0), "512 B");
        assert_eq!(bytes(12.0 * 1024.0), "12.0 KiB");
        assert_eq!(bytes(48.3 * 1024.0 * 1024.0), "48.3 MiB");
        assert_eq!(bytes(1.5 * 1024.0 * 1024.0 * 1024.0), "1.5 GiB");
    }
}
