//! Sources: the ingress stage of every pipeline (Vector-style naming). A source owns no
//! contract or storage knowledge — it produces [`crate::Record`]s into a bounded channel
//! and the pipeline's transform maps them.
//!
//! - [`session`]: one blocking thread per attached target, reading its EventPipe stream.
//! - [`process`]: one thread sampling OS memory of every live target on a fixed cadence.

pub mod process;
pub mod session;

use std::sync::atomic::{AtomicU64, Ordering};

/// Cumulative source-side counters, shared with the pipeline (self-telemetry, D11).
/// Written by the session source thread only; read once a minute.
#[derive(Debug, Default)]
pub struct SourceStats {
    pub bytes: AtomicU64,
    /// Parser counters (`wolf_nettrace::Stats`), copied after every commit.
    pub events: AtomicU64,
    /// Events the runtime dropped, from sequence-number gaps.
    pub dropped: AtomicU64,
    pub unknown: AtomicU64,
    pub skipped_blocks: AtomicU64,
    /// Correlator counters (`wolf_dotnet::CorrelatorStats`).
    pub gcs: AtomicU64,
    pub unreadable_gc: AtomicU64,
    pub stale: AtomicU64,
    /// EventCounters payloads that matched neither layout.
    pub unreadable_counters: AtomicU64,
}

impl SourceStats {
    pub(crate) fn set(slot: &AtomicU64, value: u64) {
        slot.store(value, Ordering::Relaxed);
    }

    /// A consistent-enough copy for reporting (Relaxed; the source updates in bursts).
    pub fn snapshot(&self) -> SourceStatsSnapshot {
        let get = |slot: &AtomicU64| slot.load(Ordering::Relaxed);
        SourceStatsSnapshot {
            bytes: get(&self.bytes),
            events: get(&self.events),
            dropped: get(&self.dropped),
            unknown: get(&self.unknown),
            skipped_blocks: get(&self.skipped_blocks),
            gcs: get(&self.gcs),
            unreadable_gc: get(&self.unreadable_gc),
            stale: get(&self.stale),
            unreadable_counters: get(&self.unreadable_counters),
        }
    }
}

/// Plain copy of [`SourceStats`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SourceStatsSnapshot {
    pub bytes: u64,
    pub events: u64,
    pub dropped: u64,
    pub unknown: u64,
    pub skipped_blocks: u64,
    pub gcs: u64,
    pub unreadable_gc: u64,
    pub stale: u64,
    pub unreadable_counters: u64,
}

impl SourceStatsSnapshot {
    /// Field-by-field sum over any number of sources.
    pub fn total(snapshots: impl Iterator<Item = Self>) -> Self {
        let mut total = Self::default();
        for s in snapshots {
            total.bytes += s.bytes;
            total.events += s.events;
            total.dropped += s.dropped;
            total.unknown += s.unknown;
            total.skipped_blocks += s.skipped_blocks;
            total.gcs += s.gcs;
            total.unreadable_gc += s.unreadable_gc;
            total.stale += s.stale;
            total.unreadable_counters += s.unreadable_counters;
        }
        total
    }
}
