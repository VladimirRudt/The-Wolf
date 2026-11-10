//! Maps [`DotnetTelemetry`] to contract events and points, with the names of the metric
//! catalogue (`docs/collector-dotnet.md` §4). The only place those names are written.
//! Runs on the host side of the session channel, which knows the target.

use std::collections::BTreeMap;

use serde_json::{Map, Value, json};
use wolf_contract::{Event, Point, SeriesDescriptor, SeriesId, TargetId, Timestamp, Unit};

use crate::ec_events::{CounterType, EcEvent};
use crate::gc::{GENERATIONS, GcCompleted, HeapStats};
use crate::gen_zero_summary::Gen0Summary;

pub const GC_EVENT: &str = "dotnet.gc";
pub const GEN0_SUMMARY_EVENT: &str = "dotnet.gc.gen0.summary";
const HEAP_SIZE: &str = "dotnet.gc.heap.size";
const PROMOTED: &str = "dotnet.gc.promoted";
const PINNED_OBJECTS: &str = "dotnet.gc.pinned_objects";
const FINALIZATION_PENDING: &str = "dotnet.gc.finalization_pending";
const HANDLES: &str = "dotnet.gc.handles";
const HEAP_TOTAL: &str = "dotnet.heap.total";
const GC_COMMITTED: &str = "dotnet.gc.committed";
const GC_FRAGMENTATION: &str = "dotnet.gc.fragmentation";
const ALLOC_RATE: &str = "dotnet.alloc.rate";
const GC_TIME_IN_GC: &str = "dotnet.gc.time_in_gc";

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
    handles: SeriesId,
}

#[derive(Debug, Clone)]
pub enum DotnetTelemetry {
    Gc(GcCompleted),
    Gen0Summary(Gen0Summary),
}

pub trait ContractMapper {
    fn to_events(&self, target: &TargetId, consume: &mut impl FnMut(Event));

    fn to_points(&self, series: &GcSeriesIds, consume: &mut impl FnMut(Point));
}

impl GcSeries {
    pub fn new(target: TargetId) -> Self {
        let mut descriptors = Vec::new();
        let heap_size = GENERATIONS.map(|g| Self::gen_series(&target, HEAP_SIZE, g, Unit::Bytes));
        let promoted = GENERATIONS.map(|g| Self::gen_series(&target, PROMOTED, g, Unit::Bytes));
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
                heap_size: heap_size.map(|h| h.id()),
                promoted: promoted.map(|h| h.id()),
                pinned_objects: pinned_objects.id(),
                finalization_pending: finalization_pending.id(),
                handles: handles.id(),
            },
        }
    }

    fn gen_series(
        target_id: &TargetId,
        metric: &str,
        generation: &str,
        unit: Unit,
    ) -> SeriesDescriptor {
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

/// The per-target timer series fed by EventCounters (`docs/collector-dotnet.md` §3.3 → §4),
/// created once at attach. One counter sample maps to at most one point.
#[derive(Debug, Clone)]
pub struct EcSeries {
    descriptors: Vec<SeriesDescriptor>,
    ids: EcSeriesIds,
}

#[derive(Debug, Clone, Copy)]
struct EcSeriesIds {
    heap_total: SeriesId,
    committed: SeriesId,
    fragmentation: SeriesId,
    alloc_rate: SeriesId,
    time_in_gc: SeriesId,
}

/// `System.Runtime` counter names, verified against `RuntimeEventSource.cs` (.NET 6–10).
const COUNTER_HEAP_SIZE: &str = "gc-heap-size";
const COUNTER_COMMITTED: &str = "gc-committed";
const COUNTER_FRAGMENTATION: &str = "gc-fragmentation";
const COUNTER_ALLOC_RATE: &str = "alloc-rate";
const COUNTER_TIME_IN_GC: &str = "time-in-gc";

impl EcSeries {
    /// The five counter-driven timer series of one target, created once at attach.
    pub fn new(target: TargetId) -> Self {
        let descriptors = [
            SeriesDescriptor::new(target.clone(), HEAP_TOTAL, BTreeMap::new(), Unit::Bytes),
            SeriesDescriptor::new(target.clone(), GC_COMMITTED, BTreeMap::new(), Unit::Bytes),
            SeriesDescriptor::new(
                target.clone(),
                GC_FRAGMENTATION,
                BTreeMap::new(),
                Unit::Percent,
            ),
            SeriesDescriptor::new(
                target.clone(),
                ALLOC_RATE,
                BTreeMap::new(),
                Unit::BytesPerSecond,
            ),
            SeriesDescriptor::new(
                target.clone(),
                GC_TIME_IN_GC,
                BTreeMap::new(),
                Unit::Percent,
            ),
        ];
        let ids = EcSeriesIds {
            heap_total: descriptors[0].id(),
            committed: descriptors[1].id(),
            fragmentation: descriptors[2].id(),
            alloc_rate: descriptors[3].id(),
            time_in_gc: descriptors[4].id(),
        };
        Self {
            descriptors: descriptors.into(),
            ids,
        }
    }

    /// Descriptors to register with storage before the first point.
    pub fn descriptors(&self) -> &[SeriesDescriptor] {
        &self.descriptors
    }

    /// The catalogue point for one counter sample, or `None` for a counter the catalogue
    /// doesn't track (skipped and counted in self-telemetry by the caller).
    pub fn point(&self, ts: Timestamp, counter: &EcEvent) -> Option<Point> {
        let (series, value) = match (counter.name.as_str(), counter.counter_type) {
            (COUNTER_HEAP_SIZE, CounterType::Mean) => (
                self.ids.heap_total,
                counter.mean? * bytes_per_unit(counter.display_units.as_str())?,
            ),
            (COUNTER_COMMITTED, CounterType::Mean) => (
                self.ids.committed,
                counter.mean? * bytes_per_unit(counter.display_units.as_str())?,
            ),
            (COUNTER_FRAGMENTATION, CounterType::Mean) => (self.ids.fragmentation, counter.mean?),
            (COUNTER_ALLOC_RATE, CounterType::Sum) => {
                let interval = counter.interval_sec;
                if interval <= 0.0 {
                    return None;
                }
                (
                    self.ids.alloc_rate,
                    counter.increment? / f64::from(interval),
                )
            }
            (COUNTER_TIME_IN_GC, CounterType::Mean) => (self.ids.time_in_gc, counter.mean?),
            _ => return None,
        };
        Some(Point { series, ts, value })
    }
}

/// Scale of a published counter value to bytes by its declared display units: the runtime
/// publishes `gc-heap-size`/`gc-committed` as bytes / 1_000_000 with units "MB".
fn bytes_per_unit(display_units: &str) -> Option<f64> {
    match display_units {
        "B" => Some(1.0),
        "MB" => Some(1_000_000.0),
        _ => None,
    }
}

impl ContractMapper for DotnetTelemetry {
    fn to_events(&self, target: &TargetId, consume: &mut impl FnMut(Event)) {
        let event = match self {
            DotnetTelemetry::Gc(gc) => Event {
                ts: Timestamp::from_nanos(gc.start_utc_ns),
                target: target.clone(),
                kind: GC_EVENT.to_owned(),
                data: gc_data(gc),
            },

            DotnetTelemetry::Gen0Summary(summary) => Event {
                ts: Timestamp::from_nanos(summary.start_utc_ns),
                target: target.clone(),
                kind: GEN0_SUMMARY_EVENT.to_owned(),
                data: summary_data(summary),
            },
        };

        consume(event)
    }

    fn to_points(&self, series_ids: &GcSeriesIds, consume: &mut impl FnMut(Point)) {
        let (heap, end_utc_ns) = match self {
            DotnetTelemetry::Gc(gc) => (gc.heap, gc.end_utc_ns),
            DotnetTelemetry::Gen0Summary(summary) => (summary.heap, summary.end_utc_ns),
        };

        consume_points(
            series_ids,
            Timestamp::from_nanos(end_utc_ns),
            &heap,
            consume,
        );
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
    fn to_events(&self, target: &TargetId, consume: &mut impl FnMut(Event)) {
        consume(Event {
            ts: Timestamp::from_nanos(self.start_utc_ns),
            target: target.clone(),
            kind: GEN0_SUMMARY_EVENT.to_owned(),
            data: summary_data(self),
        })
    }

    fn to_points(&self, series_ids: &GcSeriesIds, consume: &mut impl FnMut(Point)) {
        consume_points(
            series_ids,
            Timestamp::from_nanos(self.end_utc_ns),
            &self.heap,
            consume,
        );
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
    consume: &mut impl FnMut(Point),
) {
    let mut create_point = |series_id, value: f64| {
        consume(Point {
            series: series_id,
            ts,
            value,
        })
    };

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
    use crate::ec_events::{EcString, EcStringPool};
    use crate::gc::{GcKind, GcReason};

    fn pooled(pool: &EcStringPool, text: &str) -> EcString {
        let mut s = pool.rent();
        s.set_str(text);
        s
    }

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

        gc.to_events(&target, &mut |event| events.push(event));

        gc.to_points(&series.ids, &mut |point| points.push(point));

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

    #[test]
    fn counter_samples_become_catalogue_points() {
        let target: TargetId = "pid:4812.1760843645@srv-a".parse().expect("target");
        let series = EcSeries::new(target);
        let pool = EcStringPool::new();
        assert_eq!(series.descriptors().len(), 5);
        let ts = Timestamp::from_nanos(1_307_360_320_270_000_000);
        let metric_of = |point: &Point| {
            series
                .descriptors()
                .iter()
                .find(|d| d.id() == point.series)
                .expect("series")
                .metric()
                .to_owned()
        };

        // gc-heap-size, published in MB (1,000,000 bytes), becomes bytes on dotnet.heap.total.
        let heap = EcEvent {
            name: pooled(&pool, "gc-heap-size"),
            counter_type: CounterType::Mean,
            interval_sec: 1.0119938,
            mean: Some(71.753_704),
            increment: None,
            display_units: pooled(&pool, "MB"),
        };
        let point = series.point(ts, &heap).expect("heap point");
        assert_eq!(point.value, 71_753_704.0);
        assert_eq!(metric_of(&point), HEAP_TOTAL);

        // alloc-rate is a Sum counter: the point is bytes/s (increment / interval).
        let alloc = EcEvent {
            name: pooled(&pool, "alloc-rate"),
            counter_type: CounterType::Sum,
            interval_sec: 1.0119938,
            mean: None,
            increment: Some(57_133_600.0),
            display_units: pooled(&pool, "B"),
        };
        let point = series.point(ts, &alloc).expect("alloc point");
        assert_eq!(metric_of(&point), ALLOC_RATE);
        assert!((point.value - 57_133_600.0 / f64::from(1.0119938_f32)).abs() < 1.0);

        // Counters the catalogue doesn't track, wrong types and broken intervals are skipped.
        let untracked = EcEvent {
            name: pooled(&pool, "gen-0-gc-count"),
            counter_type: CounterType::Sum,
            interval_sec: 1.0119938,
            mean: None,
            increment: Some(4.0),
            display_units: pooled(&pool, ""),
        };
        assert_eq!(series.point(ts, &untracked), None);
        let wrong_type = EcEvent {
            counter_type: CounterType::Sum,
            ..heap.clone()
        };
        assert_eq!(series.point(ts, &wrong_type), None);
        let no_interval = EcEvent {
            interval_sec: 0.0,
            ..alloc
        };
        assert_eq!(series.point(ts, &no_interval), None);
    }

    #[test]
    fn heap_total_bytes_use_the_declared_unit() {
        let target: TargetId = "pid:4812.1760843645@srv-a".parse().expect("target");
        let series = EcSeries::new(target);
        let pool = EcStringPool::new();
        let ts = Timestamp::from_nanos(0);
        for (units, factor) in [("B", 1.0), ("MB", 1_000_000.0)] {
            let counter = EcEvent {
                name: pooled(&pool, "gc-committed"),
                counter_type: CounterType::Mean,
                interval_sec: 1.0,
                mean: Some(88.981_504),
                increment: None,
                display_units: pooled(&pool, units),
            };
            let point = series.point(ts, &counter).expect("point");
            assert_eq!(point.value, 88.981_504 * factor, "{units}");
        }
        let unknown_units = EcEvent {
            display_units: pooled(&pool, "MiB"),
            ..EcEvent {
                name: pooled(&pool, "gc-heap-size"),
                counter_type: CounterType::Mean,
                interval_sec: 1.0,
                mean: Some(1.0),
                increment: None,
                display_units: pooled(&pool, ""),
            }
        };
        assert_eq!(series.point(ts, &unknown_units), None);
    }
}
