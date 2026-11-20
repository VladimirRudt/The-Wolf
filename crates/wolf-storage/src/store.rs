//! The storage trait: the single seam between collectors/host and persistence
//! (`docs/architecture.md` §4). Collectors never persist long-term; the host routes
//! everything through here.

use std::ops::Range;

use wolf_contract::{Event, Point, SeriesDescriptor, SeriesId, TargetId, Timestamp};

use crate::{Error, Result};

/// A target the store knows about, with when it was first seen.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetInfo {
    pub id: TargetId,
    pub first_seen: Timestamp,
}

/// What one [`Storage::retain`] run did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Retention {
    /// Raw points dropped with their retired segments.
    pub points_deleted: usize,
    /// Minute buckets written to the downsampled table (skipped buckets already present
    /// from an earlier run are not counted).
    pub minutes_downsampled: usize,
    /// The day-aligned cutoff actually used: raw points at or after it were kept.
    pub cutoff: Timestamp,
}

/// How much of the host machine the embedded engine may use. DuckDB's own defaults are
/// one thread per core and ~80% of RAM — right for an analytics workstation, wrong for a
/// collector that must not disturb the processes it watches. The defaults are small on
/// purpose; raise them only on a dedicated box.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Config {
    /// DuckDB worker threads.
    pub threads: u32,
    /// Engine memory limit, in MiB. Insert buffering, checkpoints and retention scans
    /// share it; exceeding it spills to disk, it does not fail.
    pub memory_limit_mib: u32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            threads: 2,
            memory_limit_mib: 512,
        }
    }
}

/// Durable store for points, events and target metadata. All methods are synchronous
/// (D13); one shard's block is the single writer of its store (D14).
pub trait Storage: Send {
    /// Registers a target, ignoring re-registration (first seen wins).
    fn register_target(&mut self, target: TargetId, first_seen: Timestamp) -> Result<()>;

    /// All registered targets, oldest first.
    fn targets(&self) -> Result<Vec<TargetInfo>>;

    /// Registers series descriptors, ignoring ones already present. The descriptor's
    /// target must be registered first — `series.target` references `targets.id`.
    fn register_series(&mut self, descriptors: &[SeriesDescriptor]) -> Result<()>;

    /// Appends a batch of points. The hot path: no per-point allocation in the store —
    /// the DuckDB backend uses the appender API. Batching is the caller's job: one call
    /// is one flush, so hosts should accumulate points from all sources and append them
    /// together rather than trickle single rows.
    fn insert_points(&mut self, points: &[Point]) -> Result<()>;

    /// Appends a batch of events; same batching rules as [`Storage::insert_points`].
    fn insert_events(&mut self, events: &[Event]) -> Result<()>;

    /// Points of one series with `range.start <= ts < range.end`, time-ordered. Reads the
    /// full-resolution table only; downsampled history is exposed separately (P4 tools).
    fn points(&self, series: SeriesId, range: Range<Timestamp>) -> Result<Vec<Point>>;

    /// Events of one target with `range.start <= ts < range.end`, time-ordered.
    fn events(&self, target: &TargetId, range: Range<Timestamp>) -> Result<Vec<Event>>;

    /// Retires raw points older than `before`: downsamples them into `points_1m`
    /// (`min` keeps sawtooth bottoms, `max` keeps spikes, `last` suits step series,
    /// §4.1) and drops their storage. Re-running is safe: buckets already downsampled
    /// are neither duplicated nor rewritten. The granularity is the backend's choice —
    /// the sharded layout retires whole day segments, so the partially covered current
    /// day stays raw.
    fn retain(&mut self, before: Timestamp) -> Result<Retention>;
}

/// Parses a stored target id; a corrupt row fails the query instead of being silently
/// dropped.
pub(crate) fn parse_target(text: &str) -> Result<TargetId> {
    text.parse()
        .map_err(|e| Error::Contract(format!("stored target id {text:?}: {e}")))
}
