//! Collector self-telemetry (D11, `docs/collector-dotnet.md` §4, `wolf.` prefix): the
//! host reports its own overhead as ordinary series under its own target id, so the same
//! retention and tooling apply. `wolf.collector.cpu` and `wolf.collector.lag_ms` from the
//! catalogue are not wired yet — no cheap CPU% source, and lag needs an arrival watermark
//! in the reader.

use std::collections::BTreeMap;

use wolf_contract::{Point, SeriesDescriptor, SeriesId, TargetId, Timestamp, Unit};

use crate::session::ReaderStatsSnapshot;

const EVENTS_RECEIVED: &str = "wolf.collector.events_received";
const EVENTS_DROPPED: &str = "wolf.collector.events_dropped";
const BYTES_RECEIVED: &str = "wolf.collector.bytes_received";
const MEMORY: &str = "wolf.collector.memory";

/// The `wolf.*` series of the host process itself, created once at start.
#[derive(Debug, Clone)]
pub struct SelfTelemetry {
    target: TargetId,
    descriptors: Vec<SeriesDescriptor>,
    ids: [SeriesId; 4],
}

impl SelfTelemetry {
    pub fn new(target: TargetId) -> Self {
        let descriptor = |metric: &str, unit: Unit| {
            SeriesDescriptor::new(target.clone(), metric, BTreeMap::new(), unit)
        };
        let descriptors = [
            descriptor(EVENTS_RECEIVED, Unit::Count),
            descriptor(EVENTS_DROPPED, Unit::Count),
            descriptor(BYTES_RECEIVED, Unit::Bytes),
            descriptor(MEMORY, Unit::Bytes),
        ];
        let ids = [
            descriptors[0].id(),
            descriptors[1].id(),
            descriptors[2].id(),
            descriptors[3].id(),
        ];
        Self {
            target,
            descriptors: descriptors.into(),
            ids,
        }
    }

    pub fn target(&self) -> &TargetId {
        &self.target
    }

    /// Descriptors to register with storage before the first report.
    pub fn descriptors(&self) -> &[SeriesDescriptor] {
        &self.descriptors
    }

    /// One report tick: cumulative reader totals plus the host's own RSS.
    pub fn points(
        &self,
        ts: Timestamp,
        totals: &ReaderStatsSnapshot,
        memory_rss: u64,
    ) -> [Point; 4] {
        let point = |series, value| Point {
            series,
            ts,
            value: value as f64,
        };
        [
            point(self.ids[0], totals.events),
            point(self.ids[1], totals.dropped),
            point(self.ids[2], totals.bytes),
            point(self.ids[3], memory_rss),
        ]
    }
}
