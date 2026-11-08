//! The GC part of a telemetry session: correlation, then volume control
//! (`docs/collector-dotnet.md` §3.2 "Aggregation"). Runs on the session's reader thread.

use crate::gc::{GcCompleted, HeapStats};

/// Gen0 GCs are summarized over at least this long (by their own timestamps).
const GEN0_SUMMARY_NS: i64 = 1_000_000_000;

/// Gen0 GCs over at least one second: up to 100+ per second on a busy server, too many to
/// store one by one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Gen0Summary {
    /// `GCStart` of the first GC, UTC ns.
    pub start_utc_ns: i64,
    /// `GCEnd` of the last GC, UTC ns.
    pub end_utc_ns: i64,
    pub count: u32,
    pub total_pause_ns: i64,
    pub max_pause_ns: i64,
    /// Heap after the last GC.
    pub heap: HeapStats,
}

/// GC-driven, not clock-driven: the summary is emitted by the first gen0 GC that ends at
/// least a second after the summary started. No timer, no sorting.
pub fn add_gen0(
    pending: &mut Option<Gen0Summary>,
    gc: GcCompleted,
    on_summary: &mut impl FnMut(Gen0Summary)) {
    let summary = pending.get_or_insert(Gen0Summary
    {
        start_utc_ns: gc.start_utc_ns,
        end_utc_ns: gc.end_utc_ns,
        count: 0,
        total_pause_ns: 0,
        max_pause_ns: 0,
        heap: gc.heap,
    });
    summary.start_utc_ns = summary.start_utc_ns.min(gc.start_utc_ns);
    if gc.end_utc_ns >= summary.end_utc_ns {
        summary.end_utc_ns = gc.end_utc_ns;
        summary.heap = gc.heap;
    }
    summary.count += 1;
    summary.total_pause_ns += gc.pause_ns;
    summary.max_pause_ns = summary.max_pause_ns.max(gc.pause_ns);

    if summary.end_utc_ns - summary.start_utc_ns >= GEN0_SUMMARY_NS
       && let Some(some_summary) = pending.take()  {
        on_summary(some_summary)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gc::{GcKind, GcReason};

    fn gen0(start_ms: i64, pause_ms: i64, gen0_size: u64) -> GcCompleted {
        GcCompleted {
            number: 0,
            generation: 0,
            kind: GcKind::Blocking,
            reason: GcReason(0),
            start_utc_ns: start_ms * 1_000_000,
            end_utc_ns: (start_ms + pause_ms) * 1_000_000,
            pause_ns: pause_ms * 1_000_000,
            duration_ns: pause_ms * 1_000_000,
            compacting: Some(true),
            heap: HeapStats {
                size: [gen0_size, 0, 0, 0, 0],
                ..HeapStats::default()
            },
        }
    }

    #[test]
    fn summarizes_gen0_gcs_per_second_of_gc_time() {
        let mut pending = None;
        let mut out = Vec::new();
        for gc in [
            gen0(0, 1, 10),
            gen0(400, 3, 20),
            gen0(999, 2, 30),
            gen0(1500, 1, 40),
        ] {
            add_gen0(&mut pending, gc, &mut |item| out.push(item));
        }
        // The GC ending at 1001 ms closes the first summary; the one at 1500 ms starts the next.
        assert_eq!(
            out,
            [Gen0Summary {
                start_utc_ns: 0,
                end_utc_ns: 1_001_000_000,
                count: 3,
                total_pause_ns: 6_000_000,
                max_pause_ns: 3_000_000,
                heap: gen0(999, 2, 30).heap,
            }]
        );
        assert_eq!(pending.map(|s| s.count), Some(1));
    }
}
