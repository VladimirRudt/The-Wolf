//! Maps [`DotnetTelemetry`] to contract events and points, with the names of the metric
//! catalogue (`docs/collector-dotnet.md` §4). The only place those names are written.
//! Runs on the host side of the session channel, which knows the target.

use std::collections::BTreeMap;

use serde_json::{Map, Value, json};
use wolf_contract::{Event, Point, SeriesDescriptor, SeriesId, TargetId, Timestamp, Unit};

use crate::gc::{GENERATIONS, GcCompleted, HeapStats};
use crate::gen_zero_summary::{Gen0Summary};

pub const GC_EVENT: &str = "dotnet.gc";
pub const GEN0_SUMMARY_EVENT: &str = "dotnet.gc.gen0.summary";
const HEAP_SIZE: &str = "dotnet.gc.heap.size";
const PROMOTED: &str = "dotnet.gc.promoted";
const PINNED_OBJECTS: &str = "dotnet.gc.pinned_objects";
const FINALIZATION_PENDING: &str = "dotnet.gc.finalization_pending";
const HANDLES: &str = "dotnet.gc.handles";

/// The per-GC series of one target, created once at attach.
#[derive(Debug, Clone)]
pub struct GcSeries {
    descriptors: Vec<SeriesDescriptor>,
    ids: GcSeriesIds,
}

#[derive(Debug, Clone)]
pub struct GcSeriesIds {
    heap_size: [SeriesId; 5],
    promoted: [SeriesId; 5],
    pinned_objects: SeriesId,
    finalization_pending: SeriesId,
    handles: SeriesId
}

#[derive(Debug, Clone)]
pub enum DotnetTelemetry {
    Gc(GcCompleted),
    Gen0Summary(Gen0Summary)
}

pub trait ContractMapper {
    fn to_events(&self, target: &TargetId, consume: &mut impl FnMut(Event));

    fn to_points(&self, series: &GcSeriesIds, consume: &mut impl FnMut(Point));
}

impl GcSeries {
    pub fn new(target: TargetId) -> Self {
        let mut descriptors = Vec::new();
        let heap_size = GENERATIONS
            .map(|g| Self::gen_series(&target, HEAP_SIZE, g, Unit::Bytes));
        let promoted = GENERATIONS
            .map(|g| Self::gen_series(&target, PROMOTED, g, Unit::Bytes));
        let pinned_objects = Self::series(&target, PINNED_OBJECTS, Unit::Count);
        let finalization_pending = Self::series(&target, FINALIZATION_PENDING, Unit::Count);
        let handles = Self::series(&target, HANDLES, Unit::Count);

        descriptors.extend(heap_size.to_owned());
        descriptors.extend(promoted.to_owned());
        descriptors.push(pinned_objects.to_owned());
        descriptors.push(finalization_pending.to_owned());
        descriptors.push(handles.to_owned());

        Self {
            descriptors,
            ids: GcSeriesIds {
                heap_size: heap_size.map(| h | h.id()),
                promoted: promoted.map(| h | h.id()),
                pinned_objects: pinned_objects.id(),
                finalization_pending: finalization_pending.id(),
                handles: handles.id()
            }
        }
    }

    fn gen_series(target_id: &TargetId, metric: &str, generation: &str, unit: Unit) -> SeriesDescriptor {
        let labels = BTreeMap::from([("generation".to_owned(), generation.to_owned())]);
        let descriptor = SeriesDescriptor::new(target_id.clone(), metric, labels, unit);
        descriptor
    }

    fn series(target_id: &TargetId, metric: &str, unit: Unit) -> SeriesDescriptor {
        let labels = BTreeMap::new();
        let descriptor = SeriesDescriptor::new(target_id.clone(), metric, labels, unit);
        descriptor
    }

    /// Descriptors to register with storage before the first point.
    pub fn descriptors(&self) -> &[SeriesDescriptor] {
        &self.descriptors
    }
}

impl ContractMapper for DotnetTelemetry {
    fn to_events(
        &self,
        target: &TargetId,
        consume: &mut impl FnMut(Event)) {

        let event = match self {
            DotnetTelemetry::Gc(gc) => Event {
                ts: Timestamp::from_nanos(gc.start_utc_ns),
                target: target.clone(),
                kind: GC_EVENT.to_owned(),
                data: gc_data(gc)
            },

            DotnetTelemetry::Gen0Summary(summary) => Event {
                ts: Timestamp::from_nanos(summary.start_utc_ns),
                target: target.clone(),
                kind: GEN0_SUMMARY_EVENT.to_owned(),
                data: summary_data(summary)
            }
        };

        consume(event)
    }

    fn to_points(
        &self,
        series_ids: &GcSeriesIds,
        consume: &mut impl FnMut(Point)) {

        let (heap, end_utc_ns)  = match self {
            DotnetTelemetry::Gc(gc) => (gc.heap, gc.end_utc_ns),
            DotnetTelemetry::Gen0Summary(summary) => (summary.heap, summary.end_utc_ns)
        };

        consume_points(
            series_ids,
            Timestamp::from_nanos(end_utc_ns),
            &heap,
            consume);
    }
}

fn gc_data(gc: &GcCompleted) -> Map<String, Value> {
    let mut data = Map::new();
    data.insert("gc_number".into(), gc.number.into());
    data.insert("generation".into(), gc.generation.into());
    data.insert("reason".into(), named(gc.reason.name(), gc.reason.0));
    data.insert("type".into(), named(gc.kind.name(), gc.kind.raw()));
    data.insert("pause_ms".into(), ms(gc.pause_ns));
    data.insert("duration_ms".into(), ms(gc.duration_ns));
    if let Some(compacting) = gc.compacting {
        data.insert("compacting".into(), compacting.into());
    }
    data
}

impl ContractMapper for Gen0Summary {
    fn to_events(
        &self,
        target: &TargetId,
        consume: &mut impl FnMut(Event)) {
        consume(Event {
            ts: Timestamp::from_nanos(self.start_utc_ns),
            target: target.clone(),
            kind: GEN0_SUMMARY_EVENT.to_owned(),
            data: summary_data(self)
        })
    }

    fn to_points(
        &self,
        series_ids: &GcSeriesIds,
        consume: &mut impl FnMut(Point)) {
        consume_points(
            series_ids,
            Timestamp::from_nanos(self.end_utc_ns),
            &self.heap,
            consume);
    }
}

fn summary_data(summary: &Gen0Summary) -> Map<String, Value> {
    let mut data = Map::new();
    data.insert("count".into(), summary.count.into());
    data.insert("total_pause_ms".into(), ms(summary.total_pause_ns));
    data.insert("max_pause_ms".into(), ms(summary.max_pause_ns));
    data
}

fn consume_points(
    series_ids: &GcSeriesIds,
    ts: Timestamp,
    heap: &HeapStats,
    consume: &mut impl FnMut(Point)) {
    let mut create_point = |series_id, value: f64| consume(Point { series: series_id, ts, value });

    for (series, size) in series_ids.heap_size.iter().zip(heap.size) {
        create_point(*series, size as f64);
    }
    for (series, promoted) in series_ids.promoted.iter().zip(heap.promoted) {
        create_point(*series, promoted as f64);
    }
    create_point(series_ids.pinned_objects, f64::from(heap.pinned_objects));
    create_point(
        series_ids.finalization_pending,
        heap.finalization_promoted_count as f64,
    );
    create_point(series_ids.handles, f64::from(heap.gc_handles));
}

/// The name, or the raw runtime value for one newer than this collector.
fn named(name: Option<&str>, raw: u32) -> Value {
    name.map_or_else(|| json!(raw), |name| json!(name))
}

fn ms(ns: i64) -> Value {
    json!(ns as f64 / 1e6)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gc::{GcKind, GcReason};

    #[test]
    fn gc_becomes_an_event_and_thirteen_points() {
        let target: TargetId = "pid:4812.1760843645@srv-a".parse().expect("target");
        let series = GcSeries::new(target.clone());
        assert_eq!(series.descriptors().len(), 13);
        let gc = DotnetTelemetry::Gc(GcCompleted {
            number: 103,
            generation: 2,
            kind: GcKind::Background,
            reason: GcReason(4),
            start_utc_ns: 1_000,
            end_utc_ns: 2_000,
            pause_ns: 254_100,
            duration_ns: 1_595_700,
            compacting: Some(false),
            heap: HeapStats {
                size: [1, 2, 3, 4, 5],
                ..HeapStats::default()
            },
        });
        let (mut events, mut points) = (Vec::new(), Vec::new());

        gc.to_events(&target, &mut | event | events.push(event));

        gc.to_points(&series.ids, &mut | point | points.push(point));

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].target, target);
        assert_eq!(events[0].kind, "dotnet.gc");
        assert_eq!(
            Value::Object(events[0].data.clone()),
            json!({ "gc_number": 103, "generation": 2, "reason": "AllocLarge",
                    "type": "background", "pause_ms": 0.2541, "duration_ms": 1.5957,
                    "compacting": false })
        );
        assert_eq!(points.len(), 13);
        let gen2 = &series.descriptors()[2];
        assert_eq!(
            (gen2.metric(), gen2.labels()["generation"].as_str()),
            (HEAP_SIZE, "gen2")
        );
        assert_eq!(
            points[2],
            Point {
                series: gen2.id(),
                ts: Timestamp::from_nanos(2_000),
                value: 3.0
            }
        );
    }

    #[test]
    fn unknown_reason_keeps_the_raw_value() {
        assert_eq!(named(GcReason(42).name(), 42), json!(42));
    }
}
