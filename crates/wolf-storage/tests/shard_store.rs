//! The sharded store end to end: day segmentation on disk, unioned reads, drop-file
//! retention, persistence across reopen, and the cross-shard read-only query layer.

use std::collections::BTreeMap;
use std::path::Path;

use wolf_contract::{Point, SeriesDescriptor, SeriesId, TargetId, Timestamp, Unit};
use wolf_storage::{QueryStore, ShardStore, Storage, shard_dir};

const DAY_NS: i64 = 86_400_000_000_000;

/// Day 20559 since the epoch (2026-04-16) 00:00 UTC in nanos, plus `days` and a bit, so
/// tests control the segment a point lands in.
fn day_ns(days: i64) -> i64 {
    (20_559 + days) * DAY_NS + 3_600_000_000_000
}

fn ts(nanos: i64) -> Timestamp {
    Timestamp::from_nanos(nanos)
}

fn target(n: u32) -> TargetId {
    TargetId::new(4812 + n, 1_760_843_645, "srv-a").expect("target")
}

fn series(target: &TargetId, metric: &str) -> SeriesDescriptor {
    SeriesDescriptor::new(target.clone(), metric, BTreeMap::new(), Unit::Bytes)
}

fn point(id: SeriesId, nanos: i64, value: f64) -> Point {
    Point {
        series: id,
        ts: ts(nanos),
        value,
    }
}

fn open_shard(root: &Path, shard: u16) -> ShardStore {
    ShardStore::open(shard_dir(root, shard)).expect("shard store")
}

#[test]
fn points_segment_by_utc_day_and_read_back_unioned() {
    let root = tempfile::tempdir().expect("tempdir");
    let target = target(1);
    let series = series(&target, "dotnet.gc.heap.size");
    let mut store = open_shard(root.path(), 0);
    store
        .register_target(target.clone(), ts(day_ns(0)))
        .expect("register target");
    store
        .register_series(std::slice::from_ref(&series))
        .expect("register series");

    store
        .insert_points(&[
            point(series.id(), day_ns(1), 10.0),
            point(series.id(), day_ns(0), 20.0),
            point(series.id(), day_ns(1) + 60_000_000_000, 30.0),
        ])
        .expect("insert");

    // Two segment files on disk, named by their day.
    let dir = shard_dir(root.path(), 0);
    let mut segments: Vec<String> = std::fs::read_dir(&dir)
        .expect("shard dir")
        .map(|entry| {
            entry
                .expect("entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .filter(|name| name.starts_with("seg-") && name.ends_with(".duckdb"))
        .collect();
    segments.sort();
    assert_eq!(segments, ["seg-2026-04-16.duckdb", "seg-2026-04-17.duckdb"]);

    // Reads union across segments in time order.
    let points = store
        .points(series.id(), ts(0)..ts(i64::MAX))
        .expect("points");
    let values: Vec<f64> = points.iter().map(|point| point.value).collect();
    assert_eq!(values, [20.0, 10.0, 30.0]);
}

#[test]
fn retention_downsamples_and_drops_sealed_segments() {
    let root = tempfile::tempdir().expect("tempdir");
    let target = target(1);
    let series = series(&target, "dotnet.gc.heap.size");
    let mut store = open_shard(root.path(), 0);
    store
        .register_target(target.clone(), ts(day_ns(0)))
        .expect("register target");
    store
        .register_series(std::slice::from_ref(&series))
        .expect("register series");
    store
        .insert_points(&[
            point(series.id(), day_ns(0), 10.0),
            point(series.id(), day_ns(0) + 60_000_000_000, 20.0),
            point(series.id(), day_ns(1), 30.0),
            point(series.id(), day_ns(2), 40.0),
        ])
        .expect("insert");

    // Retire everything before day 2: days 0 and 1 are downsampled and dropped.
    let done = store.retain(ts(day_ns(2))).expect("retain");
    assert_eq!(done.points_deleted, 3);
    assert_eq!(done.minutes_downsampled, 3);
    assert_eq!(done.cutoff, ts(day_ns(2) - 3_600_000_000_000));

    let dir = shard_dir(root.path(), 0);
    let remaining: Vec<_> = std::fs::read_dir(&dir)
        .expect("shard dir")
        .map(|entry| {
            entry
                .expect("entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .filter(|name| name.starts_with("seg-") && name.ends_with(".duckdb"))
        .collect();
    assert_eq!(remaining, ["seg-2026-04-18.duckdb"]);

    // Only day 2 remains raw.
    let points = store
        .points(series.id(), ts(0)..ts(i64::MAX))
        .expect("points");
    assert_eq!(points.len(), 1);
    assert_eq!(points[0].value, 40.0);

    // Re-running is a no-op: the segments are gone, nothing is counted again.
    let again = store.retain(ts(day_ns(2))).expect("retain again");
    assert_eq!(again.points_deleted, 0);
    assert_eq!(again.minutes_downsampled, 0);
}

#[test]
fn a_reopened_shard_keeps_its_segments_and_metadata() {
    let root = tempfile::tempdir().expect("tempdir");
    let target = target(1);
    let series = series(&target, "dotnet.gc.heap.size");
    {
        let mut store = open_shard(root.path(), 0);
        store
            .register_target(target.clone(), ts(day_ns(0)))
            .expect("register target");
        store
            .register_series(std::slice::from_ref(&series))
            .expect("register series");
        store
            .insert_points(&[point(series.id(), day_ns(0), 10.0)])
            .expect("insert");
    }
    let mut store = open_shard(root.path(), 0);
    assert_eq!(
        store.targets().expect("targets").len(),
        1,
        "target survives reopen"
    );
    let points = store
        .points(series.id(), ts(0)..ts(i64::MAX))
        .expect("points after reopen");
    assert_eq!(points.len(), 1);
    // Writing a new day after reopen adds a segment without touching the old one.
    store
        .insert_points(&[point(series.id(), day_ns(3), 99.0)])
        .expect("insert day 3");
    let points = store
        .points(series.id(), ts(0)..ts(i64::MAX))
        .expect("points");
    assert_eq!(points.len(), 2);
}

#[test]
fn events_live_in_meta_and_survive_segment_retention() {
    let root = tempfile::tempdir().expect("tempdir");
    let target = target(1);
    let series = series(&target, "dotnet.gc.heap.size");
    let mut store = open_shard(root.path(), 0);
    store
        .register_target(target.clone(), ts(day_ns(0)))
        .expect("register target");
    store
        .register_series(std::slice::from_ref(&series))
        .expect("register series");
    store
        .insert_points(&[point(series.id(), day_ns(0), 10.0)])
        .expect("insert");
    store
        .insert_events(&[wolf_contract::Event {
            ts: ts(day_ns(0)),
            target: target.clone(),
            kind: "target.attached".to_owned(),
            data: serde_json::Map::new(),
        }])
        .expect("event");

    store.retain(ts(day_ns(2))).expect("retain");
    let events = store.events(&target, ts(0)..ts(i64::MAX)).expect("events");
    assert_eq!(events.len(), 1, "events are not day-segmented");
    assert_eq!(events[0].kind, "target.attached");
}

#[test]
fn query_store_unions_all_shards_read_only() {
    let root = tempfile::tempdir().expect("tempdir");
    let target_a = target(1);
    let target_b = target(2);
    let series_a = series(&target_a, "dotnet.gc.heap.size");
    let series_b = series(&target_b, "dotnet.gc.heap.size");
    {
        let mut shard0 = open_shard(root.path(), 0);
        shard0
            .register_target(target_a.clone(), ts(day_ns(0)))
            .expect("register");
        shard0
            .register_series(std::slice::from_ref(&series_a))
            .expect("series");
        shard0
            .insert_points(&[point(series_a.id(), day_ns(0), 1.0)])
            .expect("insert");
        shard0
            .insert_events(&[wolf_contract::Event {
                ts: ts(day_ns(0)),
                target: target_a.clone(),
                kind: "target.attached".to_owned(),
                data: serde_json::Map::new(),
            }])
            .expect("event");

        let mut shard1 = open_shard(root.path(), 1);
        shard1
            .register_target(target_b.clone(), ts(day_ns(1)))
            .expect("register");
        shard1
            .register_series(std::slice::from_ref(&series_b))
            .expect("series");
        shard1
            .insert_points(&[point(series_b.id(), day_ns(1), 2.0)])
            .expect("insert");
    }

    let query = QueryStore::open(root.path()).expect("query store");
    assert_eq!(query.targets().expect("targets").len(), 2);
    let points = query
        .points(series_a.id(), ts(0)..ts(i64::MAX))
        .expect("points a");
    assert_eq!(points.len(), 1);
    assert_eq!(points[0].value, 1.0);
    let points = query
        .points(series_b.id(), ts(0)..ts(i64::MAX))
        .expect("points b");
    assert_eq!(points.len(), 1);
    let events = query
        .events(&target_a, ts(0)..ts(i64::MAX))
        .expect("events");
    assert_eq!(events.len(), 1);

    // An empty root is an empty store, not an error.
    let empty = tempfile::tempdir().expect("tempdir");
    let query = QueryStore::open(empty.path()).expect("empty store");
    assert!(query.targets().expect("targets").is_empty());
    assert!(
        query
            .points(series_a.id(), ts(0)..ts(i64::MAX))
            .expect("points")
            .is_empty()
    );
}
