//! Groups GC events into one [`GcCompleted`] per GC without sorting
//! (`docs/collector-dotnet.md` §3.2).
//!
//! Events are only in order within one thread. `GCStart`, `GCEnd` and a GCPrep suspension
//! carry the GC number and go to `gcs` directly; `GCRestartEEEnd`, `GCGlobalHeapHistory` and
//! `GCHeapStats` carry none and find their GC through the thread that wrote them (`threads`).
//! Pause attribution follows TraceEvent (PerfView GCStats), checked by the golden test.

use std::collections::HashMap;

use wolf_nettrace::{Event, TraceInfo};

use crate::gc::{GcCompleted, GcKind, GcReason, HeapStats};
use crate::gc_events::{self, Decoded, GcEvent, SUSPEND_FOR_GC_PREP};

/// An entry still incomplete this long after it was created (by the newest GC event's
/// timestamp) lost its other events (drops, attach mid-GC) and is removed. Far beyond the
/// reordering between threads, which is bounded by how often the runtime writes its buffers.
/// Not a GC count: with frequent gen0 GCs, many can complete inside one reordered stretch.
const STALE_AFTER_S: i64 = 60;
/// A background GC can run for a long time on a large heap.
const STALE_BACKGROUND_AFTER_S: i64 = 600;

/// Counters for self-telemetry.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct CorrelatorStats {
    /// GCs emitted.
    pub gcs: u64,
    /// GC events in a version or size we can't read.
    pub unreadable_events: u64,
    /// Incomplete GCs and pauses removed because their other events never came.
    pub stale_entries: u64,
}

/// The GC being assembled.
#[derive(Debug, Default)]
struct GcRecord {
    start: Option<Start>,
    end_qpc: Option<i64>,
    compacting: Option<bool>,
    heap: Option<HeapStats>,
    pause_ticks: i64,
    pauses: u8,
    /// A GCPrep suspension was seen for this number: it is a background GC even before its
    /// `GCStart` arrives.
    background: bool,
    /// Newest GC event timestamp when this entry was created (thread entries: last touched).
    created_at: i64,
}

#[derive(Debug, Clone, Copy)]
struct Start {
    qpc: i64,
    generation: u32,
    reason: GcReason,
    kind: GcKind,
}

/// What one thread is doing for a GC right now.
#[derive(Debug, Default)]
struct ThreadState {
    pause: Option<Pause>,
    /// From `GCGlobalHeapHistory`, for the next `GCEnd` on this thread.
    compacting: Option<bool>,
    /// The GC whose `GCEnd` this thread wrote; its `GCHeapStats` follows on the same thread.
    last_ended: Option<u32>,
    created_at: i64,
}

/// From `GCSuspendEEBegin` to `GCRestartEEEnd` on one thread.
#[derive(Debug)]
struct Pause {
    start_qpc: i64,
    suspended_qpc: Option<i64>,
    /// Set for a background GC's second pause (GCPrep), whose suspension names the GC.
    prep_for: Option<u32>,
    /// GCs started inside this pause: one, or a background GC plus an ephemeral GC.
    started: [Option<u32>; 2],
}

/// Turns runtime GC events into completed GCs.
#[derive(Debug, Default)]
pub struct GcCorrelator {
    gcs: HashMap<u32, GcRecord>,
    threads: HashMap<u64, ThreadState>,
    /// Newest timestamp of any GC event, for staleness.
    latest_qpc: i64,
    stats: CorrelatorStats,
}

impl GcCorrelator {
    pub fn new() -> Self {
        Self::default()
    }

    /// Feeds one parsed event; non-GC events are ignored. Calls `on_gc` for each GC this event
    /// completes.
    pub fn on_event(&mut self, event: &Event<'_>, on_gc: &mut impl FnMut(GcCompleted)) {
        match gc_events::decode_gc_event(event) {
            Decoded::Other => {}
            Decoded::Unreadable => self.stats.unreadable_events += 1,
            Decoded::Gc(gc_event) => {
                self.on_gc_event(
                    event.thread_id,
                    event.timestamp,
                    gc_event,
                    event.trace,
                    on_gc,
                );
            }
        }
    }

    pub fn stats(&self) -> CorrelatorStats {
        self.stats
    }

    fn on_gc_event(
        &mut self,
        thread: u64,
        qpc: i64,
        event: GcEvent,
        trace: &TraceInfo,
        on_gc: &mut impl FnMut(GcCompleted),
    ) {
        self.latest_qpc = self.latest_qpc.max(qpc);
        let touched = match event {
            GcEvent::SuspendBegin { reason, count } => {
                let prep_for = (reason == SUSPEND_FOR_GC_PREP).then_some(count);
                if let Some(number) = prep_for {
                    self.record(number).background = true;
                }
                let state = self.thread(thread);
                if state.pause.is_some() {
                    self.stats.stale_entries += 1;
                }
                let state = self.thread(thread);
                state.pause = Some(Pause {
                    start_qpc: qpc,
                    suspended_qpc: None,
                    prep_for,
                    started: [None; 2],
                });
                None
            }
            GcEvent::SuspendEnd => {
                if let Some(pause) = self.pause_mut(thread) {
                    pause.suspended_qpc = Some(qpc);
                }
                None
            }
            GcEvent::Start {
                number,
                generation,
                reason,
                kind,
            } => {
                self.record(number).start = Some(Start {
                    qpc,
                    generation,
                    reason,
                    kind,
                });
                if let Some(pause) = self.pause_mut(thread)
                    && let Some(slot) = pause.started.iter_mut().find(|slot| slot.is_none())
                {
                    *slot = Some(number);
                }
                Some(number)
            }
            GcEvent::GlobalHeapHistory { compacting } => {
                self.thread(thread).compacting = Some(compacting);
                None
            }
            GcEvent::End { number } => {
                let state = self.thread(thread);
                state.last_ended = Some(number);
                let compacting = state.compacting.take();
                let record = self.record(number);
                record.end_qpc = Some(qpc);
                record.compacting = compacting;
                Some(number)
            }
            GcEvent::HeapStats(heap) => {
                let number = self
                    .threads
                    .get_mut(&thread)
                    .and_then(|s| s.last_ended.take());
                if let Some(number) = number {
                    self.record(number).heap = Some(heap);
                }
                self.forget_thread_if_idle(thread);
                number
            }
            GcEvent::RestartEnd => {
                let pause = self.threads.get_mut(&thread).and_then(|s| s.pause.take());
                self.forget_thread_if_idle(thread);
                // No open pause: attached mid-pause, or its suspension was dropped.
                if let Some(pause) = pause {
                    for number in self.close_pause(pause, qpc) {
                        self.emit_if_complete(number, trace, on_gc);
                    }
                }
                None
            }
        };
        if let Some(number) = touched {
            self.emit_if_complete(number, trace, on_gc);
        }
    }

    /// Adds a finished pause to the GCs it belongs to; returns them.
    fn close_pause(&mut self, pause: Pause, restart_qpc: i64) -> Vec<u32> {
        if let Some(number) = pause.prep_for {
            self.add_pause(number, restart_qpc - pause.start_qpc);
            return vec![number];
        }
        match pause.started {
            [Some(first), Some(second)] => {
                // A background GC and an ephemeral GC in one pause (Maoni, steps 3–4). As in
                // TraceEvent: the background GC gets the suspension, the ephemeral GC the time
                // from its own GCStart to the restart.
                let (background, ephemeral) = if self.is_background(first) {
                    (first, second)
                } else {
                    (second, first)
                };
                let suspended = pause.suspended_qpc.unwrap_or(pause.start_qpc);
                self.add_pause(background, suspended - pause.start_qpc);
                let ephemeral_start = self
                    .gcs
                    .get(&ephemeral)
                    .and_then(|r| r.start)
                    .map_or(pause.start_qpc, |s| s.qpc);
                self.add_pause(ephemeral, restart_qpc - ephemeral_start);
                vec![background, ephemeral]
            }
            [Some(number), None] => {
                self.add_pause(number, restart_qpc - pause.start_qpc);
                vec![number]
            }
            // Not a GC pause (debugger, shutdown) or its GCStart was lost.
            _ => Vec::new(),
        }
    }

    fn add_pause(&mut self, number: u32, ticks: i64) {
        let record = self.record(number);
        record.pause_ticks += ticks;
        record.pauses += 1;
    }

    fn is_background(&self, number: u32) -> bool {
        self.gcs
            .get(&number)
            .is_some_and(|r| r.background || r.start.is_some_and(|s| s.kind == GcKind::Background))
    }

    fn emit_if_complete(
        &mut self,
        number: u32,
        trace: &TraceInfo,
        on_gc: &mut impl FnMut(GcCompleted),
    ) {
        let Some(record) = self.gcs.get(&number) else {
            return;
        };
        let (Some(start), Some(end_qpc), Some(heap)) = (record.start, record.end_qpc, record.heap)
        else {
            return;
        };
        let pauses_needed = if start.kind == GcKind::Background {
            2
        } else {
            1
        };
        if record.pauses < pauses_needed {
            return;
        }
        let gc = GcCompleted {
            number,
            generation: start.generation,
            kind: start.kind,
            reason: start.reason,
            start_utc_ns: trace.to_utc_ns(start.qpc),
            end_utc_ns: trace.to_utc_ns(end_qpc),
            pause_ns: ticks_to_ns(record.pause_ticks, trace),
            duration_ns: ticks_to_ns(end_qpc - start.qpc, trace),
            compacting: record.compacting,
            heap,
        };
        self.gcs.remove(&number);
        self.stats.gcs += 1;
        self.drop_stale(trace);
        on_gc(gc);
    }

    /// Removes entries whose other events never came: older than the limit, measured against
    /// the newest GC event seen.
    fn drop_stale(&mut self, trace: &TraceInfo) {
        let ticks = |seconds: i64| seconds.saturating_mul(trace.qpc_frequency);
        let (limit, background_limit) = (ticks(STALE_AFTER_S), ticks(STALE_BACKGROUND_AFTER_S));
        let latest = self.latest_qpc;
        let background: Vec<u32> = self
            .gcs
            .keys()
            .copied()
            .filter(|&n| self.is_background(n))
            .collect();
        let before = self.gcs.len() + self.threads.len();
        self.gcs.retain(|number, record| {
            let limit = if background.contains(number) {
                background_limit
            } else {
                limit
            };
            latest.saturating_sub(record.created_at) <= limit
        });
        self.threads
            .retain(|_, state| latest.saturating_sub(state.created_at) <= limit);
        self.stats.stale_entries += (before - self.gcs.len() - self.threads.len()) as u64;
    }

    fn record(&mut self, number: u32) -> &mut GcRecord {
        let created_at = self.latest_qpc;
        self.gcs.entry(number).or_insert_with(|| GcRecord {
            created_at,
            ..GcRecord::default()
        })
    }

    /// The thread's entry, created if needed; touching it renews its age.
    fn thread(&mut self, thread: u64) -> &mut ThreadState {
        let latest = self.latest_qpc;
        let state = self.threads.entry(thread).or_default();
        state.created_at = latest;
        state
    }

    fn pause_mut(&mut self, thread: u64) -> Option<&mut Pause> {
        self.threads.get_mut(&thread)?.pause.as_mut()
    }

    fn forget_thread_if_idle(&mut self, thread: u64) {
        if self
            .threads
            .get(&thread)
            .is_some_and(|s| s.pause.is_none() && s.last_ended.is_none() && s.compacting.is_none())
        {
            self.threads.remove(&thread);
        }
    }
}

fn ticks_to_ns(ticks: i64, trace: &TraceInfo) -> i64 {
    let ns = i128::from(ticks) * 1_000_000_000 / i128::from(trace.qpc_frequency);
    i64::try_from(ns).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 1 tick = 1 µs; sync at QPC 0 = Unix epoch.
    const TRACE: TraceInfo = TraceInfo {
        sync_time_utc_ns: 0,
        sync_qpc: 0,
        qpc_frequency: 1_000_000,
        pointer_size: 8,
    };
    const APP: u64 = 1;
    const BGC: u64 = 2;
    const OTHER: u64 = 3;

    fn start(number: u32, generation: u32, kind: GcKind) -> GcEvent {
        GcEvent::Start {
            number,
            generation,
            reason: GcReason(0),
            kind,
        }
    }

    fn suspend(reason: u32, count: u32) -> GcEvent {
        GcEvent::SuspendBegin { reason, count }
    }

    fn run(correlator: &mut GcCorrelator, events: &[(u64, i64, GcEvent)]) -> Vec<GcCompleted> {
        let mut gcs = Vec::new();
        for &(thread, qpc, event) in events {
            correlator.on_gc_event(thread, qpc, event, &TRACE, &mut |gc| gcs.push(gc));
        }
        gcs
    }

    fn blocking_gc(thread: u64, at: i64, number: u32) -> Vec<(u64, i64, GcEvent)> {
        vec![
            (thread, at, suspend(1, number - 1)),
            (thread, at + 1, start(number, 0, GcKind::Foreground)),
            (
                thread,
                at + 5,
                GcEvent::GlobalHeapHistory { compacting: true },
            ),
            (thread, at + 5, GcEvent::End { number }),
            (thread, at + 5, GcEvent::HeapStats(HeapStats::default())),
            (thread, at + 6, GcEvent::RestartEnd),
        ]
    }

    #[test]
    fn foreground_gc_during_a_background_gc_stays_separate() {
        let mut events = vec![
            (APP, 0, suspend(1, 9)),
            (APP, 1, start(10, 2, GcKind::Background)),
            (APP, 3, GcEvent::RestartEnd), // 1st pause: 3 µs
        ];
        events.extend(blocking_gc(OTHER, 10, 11)); // foreground gen0 while the BGC runs
        events.extend([
            (BGC, 20, suspend(SUSPEND_FOR_GC_PREP, 10)),
            (BGC, 22, GcEvent::RestartEnd), // 2nd pause: 2 µs
            (BGC, 30, GcEvent::GlobalHeapHistory { compacting: false }),
            (BGC, 30, GcEvent::End { number: 10 }),
            (BGC, 30, GcEvent::HeapStats(HeapStats::default())),
        ]);
        let mut correlator = GcCorrelator::new();
        let gcs = run(&mut correlator, &events);

        let summary: Vec<_> = gcs
            .iter()
            .map(|gc| {
                (
                    gc.number,
                    gc.kind,
                    gc.pause_ns,
                    gc.duration_ns,
                    gc.compacting,
                )
            })
            .collect();
        assert_eq!(
            summary,
            [
                (11, GcKind::Foreground, 6_000, 4_000, Some(true)),
                (10, GcKind::Background, 5_000, 29_000, Some(false)),
            ]
        );
        assert!(correlator.gcs.is_empty() && correlator.threads.is_empty());
    }

    #[test]
    fn restart_without_a_pause_and_other_suspensions_are_ignored() {
        let mut correlator = GcCorrelator::new();
        let events = [
            (APP, 0, GcEvent::RestartEnd), // attached mid-pause
            (APP, 1, suspend(5, 0)),       // debugger
            (APP, 2, GcEvent::RestartEnd),
        ];
        assert!(run(&mut correlator, &events).is_empty());
        assert!(correlator.threads.is_empty());
        assert_eq!(correlator.stats(), CorrelatorStats::default());
    }

    #[test]
    fn a_gc_with_lost_events_is_removed_later_and_counted() {
        let mut correlator = GcCorrelator::new();
        // GC 5 never gets its RestartEnd: its pause stays open on APP.
        let mut events = blocking_gc(APP, 0, 5);
        events.pop();
        // A minute later other GCs complete; the stale entries go.
        events.extend(blocking_gc(OTHER, 61_000_000, 6));
        let gcs = run(&mut correlator, &events);

        assert_eq!(gcs.iter().map(|gc| gc.number).collect::<Vec<_>>(), [6]);
        assert!(correlator.gcs.is_empty() && correlator.threads.is_empty());
        assert_eq!(correlator.stats().stale_entries, 2); // record 5 and APP's open pause
    }
}
