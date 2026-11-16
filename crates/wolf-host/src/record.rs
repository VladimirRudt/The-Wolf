//! The record type every source sends to its pipeline, tagged with the target slot.
//!
//! Slots are per-pipeline: a record only means something to the pipeline that owns the
//! channel it arrived on (D14 — a shard owns its targets exclusively).

use wolf_contract::Timestamp;
use wolf_process::ProcessSample;

use crate::sources::SourceStatsSnapshot;
use crate::sources::session::SessionItem;

/// One channel record: everything a pipeline consumes.
#[derive(Debug)]
pub enum Record {
    /// From a session source.
    Session(SessionItem),
    /// From the process source: one OS memory reading of a target.
    Sample {
        ts: Timestamp,
        sample: ProcessSample,
    },
    /// From the host itself: one self-telemetry tick (D11). The slot is unused — the
    /// pipeline maps this to its registered `wolf.*` series.
    SelfReport {
        ts: Timestamp,
        totals: SourceStatsSnapshot,
        rss: u64,
    },
}

/// Channel payload: the target slot and the record. Moved by value through the ring —
/// no allocation (`EcEvent`'s strings are pool-rented and return on drop).
pub type Message = (u32, Record);
