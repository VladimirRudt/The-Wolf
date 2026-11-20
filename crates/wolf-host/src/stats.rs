//! Per-shard counters (self-telemetry, D11), shared between the blocks of one shard and
//! with the host thread.

use std::sync::atomic::{AtomicU64, Ordering};

/// Shard-side counters, updated on the shard's threads, read by the host for reports.
#[derive(Debug, Default)]
pub struct ShardStats {
    pub points_written: AtomicU64,
    pub events_written: AtomicU64,
    pub batches: AtomicU64,
    /// Counter samples the catalogue doesn't track, or with broken layout/units.
    pub counters_skipped: AtomicU64,
    /// Dead or unreadable processes found by the process block.
    pub sample_errors: AtomicU64,
}

impl ShardStats {
    /// A consistent-enough copy for reporting (Relaxed; updated once per flush).
    pub fn snapshot(&self) -> ShardStatsSnapshot {
        ShardStatsSnapshot {
            points_written: self.points_written.load(Ordering::Relaxed),
            events_written: self.events_written.load(Ordering::Relaxed),
            batches: self.batches.load(Ordering::Relaxed),
            counters_skipped: self.counters_skipped.load(Ordering::Relaxed),
            sample_errors: self.sample_errors.load(Ordering::Relaxed),
        }
    }
}

/// Plain copy of [`ShardStats`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ShardStatsSnapshot {
    pub points_written: u64,
    pub events_written: u64,
    pub batches: u64,
    pub counters_skipped: u64,
    pub sample_errors: u64,
}

impl ShardStatsSnapshot {
    /// Field-by-field sum over any number of shards.
    pub fn total(snapshots: impl Iterator<Item = Self>) -> Self {
        let mut total = Self::default();
        for s in snapshots {
            total.points_written += s.points_written;
            total.events_written += s.events_written;
            total.batches += s.batches;
            total.counters_skipped += s.counters_skipped;
            total.sample_errors += s.sample_errors;
        }
        total
    }
}
