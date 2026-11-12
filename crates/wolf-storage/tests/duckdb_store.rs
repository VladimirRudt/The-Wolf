//! Integration tests for `wolf-storage`: batch insert performance (P1-7 "Done when"),
//! range queries, retention downsampling, schema versioning.
//!
//! The store trait is the production seam; a few tests also read the internal tables
//! (`points_1m`, `series`) over a raw `duckdb` connection — the schema of those tables is
//! part of the design (`docs/architecture.md` §4.1) and needs direct verification.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use serde_json::{Map, Value, json};
use wolf_contract::{Event, Point, SeriesDescriptor, SeriesId, TargetId, Timestamp, Unit};
use wolf_storage::{DuckDbStore, Error, Retention, Storage, TargetInfo};

const BASE_NS: i64 = 1_760_843_645_000_000_000; // 2026-10-19T03:14:05Z
const MINUTE_NS: i64 = 60_000_000_000;

fn ts(nanos: i64) -> Timestamp {
    Timestamp::from_nanos(nanos)
}

fn target(n: u64) -> TargetId {
    TargetId::new(4_812 + n as u32, 1_760_843_645 + n, "srv-a").expect("valid target")
}

fn descriptor(target: &TargetId, metric: &str, generation: &str) -> SeriesDescriptor {
    SeriesDescriptor::new(
        target.clone(),
        metric,
        BTreeMap::from([("generation".to_owned(), generation.to_owned())]),
        Unit::Bytes,
    )
}

fn point(series: SeriesId, at: i64, value: f64) -> Point {
    Point {
        series,
        ts: ts(at),
        value,
    }
}

/// Points of the series covering `range`, conveniently built by the caller.
fn inserted_points(
    store: &mut impl Storage,
    series: &SeriesDescriptor,
    stamps_values: &[(i64, f64)],
) -> Vec<Point> {
    let points: Vec<Point> = stamps_values
        .iter()
        .map(|(at, value)| point(series.id(), *at, *value))
        .collect();
    store.insert_points(&points).expect("insert points");
    points
}

#[test]
fn registers_and_lists_targets_first_seen_wins() {
    let mut store = DuckDbStore::open_in_memory().expect("open");
    let a = target(0);
    let b = target(1);
    store
        .register_target(a.clone(), ts(BASE_NS))
        .expect("register a");
    store
        .register_target(b.clone(), ts(BASE_NS + 1_000))
        .expect("register b");
    // Re-registration does not fail and does not move the first-seen time.
    store
        .register_target(a.clone(), ts(BASE_NS + 999_000_000))
        .expect("re-register a");

    assert_eq!(
        store.targets().expect("targets"),
        [
            TargetInfo {
                id: a,
                first_seen: ts(BASE_NS)
            },
            TargetInfo {
                id: b,
                first_seen: ts(BASE_NS + 1_000)
            },
        ]
    );
}

#[test]
fn points_roundtrip_in_time_order_and_range_is_half_open() {
    let mut store = DuckDbStore::open_in_memory().expect("open");
    let series = descriptor(&target(0), "dotnet.gc.heap.size", "gen2");
    let other = descriptor(&target(0), "dotnet.gc.heap.size", "gen0");
    store
        .register_target(target(0), ts(BASE_NS))
        .expect("target");
    store
        .register_series(&[series.clone(), other.clone()])
        .expect("series");

    // Insert out of order on purpose; reads must be time-ordered.
    inserted_points(
        &mut store,
        &other,
        &[(BASE_NS + 30, 5.0), (BASE_NS + 10, 7.0)],
    );
    let expected = inserted_points(
        &mut store,
        &series,
        &[(BASE_NS + 20, 2.0), (BASE_NS, 1.0), (BASE_NS + 40, 3.0)],
    );

    assert_eq!(
        store
            .points(series.id(), ts(BASE_NS - 1)..ts(BASE_NS + 100))
            .expect("range query"),
        [expected[1], expected[0], expected[2]],
        "ordered by ts, other series excluded"
    );
    // Half-open: the end bound is excluded, the start bound included.
    assert_eq!(
        store
            .points(series.id(), ts(BASE_NS + 20)..ts(BASE_NS + 40))
            .expect("sub range"),
        [expected[0]]
    );
    assert!(
        store
            .points(series.id(), ts(BASE_NS)..ts(BASE_NS))
            .expect("empty range")
            .is_empty()
    );
    assert!(
        store
            .points(SeriesId(u64::MAX), ts(BASE_NS)..ts(BASE_NS + 100))
            .expect("unknown series")
            .is_empty()
    );
}

#[test]
fn events_roundtrip_with_data_and_per_target_ranges() {
    let mut store = DuckDbStore::open_in_memory().expect("open");
    let a = target(0);
    let b = target(1);
    let event = |at: i64, target: &TargetId, kind: &str, data: Value| Event {
        ts: ts(at),
        target: target.clone(),
        kind: kind.to_owned(),
        data: data.as_object().expect("object").clone(),
    };
    let events = [
        event(
            BASE_NS,
            &a,
            "dotnet.gc",
            json!({"generation": 2, "pause_ms": 7.9}),
        ),
        event(BASE_NS + 10, &a, "target.attached", json!({})),
        event(BASE_NS + 5, &b, "dotnet.gc", json!({"generation": 0})),
    ];
    store.insert_events(&events).expect("insert events");

    assert_eq!(
        store
            .events(&a, ts(BASE_NS - 1)..ts(BASE_NS + 100))
            .expect("a"),
        [events[0].clone(), events[1].clone()],
        "time-ordered, b excluded"
    );
    assert_eq!(
        store
            .events(&a, ts(BASE_NS)..ts(BASE_NS + 10))
            .expect("half"),
        [events[0].clone()]
    );
    assert!(
        store
            .events(&target(9), ts(BASE_NS)..ts(BASE_NS + 100))
            .expect("unknown")
            .is_empty()
    );
}

#[test]
fn opens_with_explicit_resource_limits() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("wolf.duckdb");
    let config = wolf_storage::Config {
        threads: 1,
        memory_limit_mib: 128,
    };
    let mut store = DuckDbStore::open_with(&path, config).expect("open with config");
    let series = descriptor(&target(0), "process.threads", "ignored");
    store
        .register_target(target(0), ts(BASE_NS))
        .expect("target");
    store
        .register_series(std::slice::from_ref(&series))
        .expect("series");
    inserted_points(&mut store, &series, &[(BASE_NS, 1.0)]);
    assert_eq!(
        store
            .points(series.id(), ts(BASE_NS)..ts(BASE_NS + 1))
            .expect("points"),
        [point(series.id(), BASE_NS, 1.0)]
    );
}

#[test]
fn a_huge_payload_does_not_break_later_batches() {
    let mut store = DuckDbStore::open_in_memory().expect("open");
    let target = target(0);
    // Larger than the scratch burst cap: the buffer is handed back to the allocator
    // after the batch instead of pinning its capacity.
    let big = "x".repeat(2 << 20);
    let events = [
        Event {
            ts: ts(BASE_NS),
            target: target.clone(),
            kind: "heap_snapshot".to_owned(),
            data: Map::from_iter([("path".to_owned(), Value::from(big))]),
        },
        Event {
            ts: ts(BASE_NS + 1),
            target: target.clone(),
            kind: "dotnet.gc".to_owned(),
            data: Map::from_iter([("generation".to_owned(), Value::from(2))]),
        },
    ];
    store.insert_events(&events).expect("big batch");
    let after = Event {
        ts: ts(BASE_NS + 2),
        target: target.clone(),
        kind: "dotnet.gc".to_owned(),
        data: Map::from_iter([("generation".to_owned(), Value::from(1))]),
    };
    store
        .insert_events(std::slice::from_ref(&after))
        .expect("batch after the burst");

    assert_eq!(
        store
            .events(&target, ts(BASE_NS)..ts(BASE_NS + 100))
            .expect("events"),
        [events[0].clone(), events[1].clone(), after]
    );
}

#[test]
fn inserts_one_million_points_in_a_few_seconds() {
    let mut store = DuckDbStore::open_in_memory().expect("open");
    let target = target(0);
    let descriptors: Vec<_> = (0..10)
        .map(|n| descriptor(&target, "dotnet.gc.heap.size", &format!("gen{n}")))
        .collect();
    store
        .register_target(target.clone(), ts(BASE_NS))
        .expect("target");
    store.register_series(&descriptors).expect("series");

    let batches = 20;
    let batch_size = 50_000; // 1M points total
    let mut points = Vec::with_capacity(batch_size);
    let started = Instant::now();
    for batch in 0..batches {
        points.clear();
        for i in 0..batch_size {
            let n = batch * batch_size + i;
            points.push(point(
                descriptors[n % descriptors.len()].id(),
                BASE_NS + n as i64 * 1_000_000, // 1 ms steps
                n as f64,
            ));
        }
        store.insert_points(&points).expect("batch insert");
    }
    let elapsed = started.elapsed();
    println!("1M points inserted in {elapsed:.2?}");
    assert!(
        elapsed < Duration::from_secs(10),
        "1M points took {elapsed:.2?}; the few-seconds budget is blown"
    );

    // The data actually landed, in order.
    let first = descriptors[0].id();
    let read = store
        .points(first, ts(BASE_NS)..ts(BASE_NS + 1_000_000_000_000))
        .expect("range query");
    assert_eq!(read.len(), batches * batch_size / descriptors.len());
    assert!(
        read.windows(2).all(|pair| pair[0].ts < pair[1].ts),
        "time-ordered"
    );
}

#[test]
fn retention_downsamples_old_minutes_and_deletes_raw_points() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("wolf.duckdb");
    let minute = BASE_NS / MINUTE_NS * MINUTE_NS;
    let series = descriptor(&target(0), "dotnet.gc.heap.size", "gen2");

    let mut store = DuckDbStore::open(&path).expect("open");
    store
        .register_target(target(0), ts(BASE_NS))
        .expect("target");
    store
        .register_series(std::slice::from_ref(&series))
        .expect("series");
    // Three minutes of data; the third minute also has a point after the cutoff.
    inserted_points(
        &mut store,
        &series,
        &[
            (minute, 1.0),
            (minute + 10_000_000_000, 2.0),
            (minute + 20_000_000_000, 3.0),
            (minute + MINUTE_NS, 10.0),
            (minute + MINUTE_NS + 10_000_000_000, 20.0),
            (minute + 2 * MINUTE_NS, 100.0),
        ],
    );

    // Cutoff request lands mid-minute; the job must floor it to the minute boundary.
    let requested = ts(minute + 2 * MINUTE_NS + 30_000_000_000);
    let retention: Retention = store.retain(requested).expect("retain");
    assert_eq!(retention.cutoff, ts(minute + 2 * MINUTE_NS));
    assert_eq!(retention.minutes_downsampled, 2);
    assert_eq!(retention.points_deleted, 5);

    // Raw side: old points gone, the newest kept.
    assert!(
        store
            .points(series.id(), ts(minute)..ts(minute + 2 * MINUTE_NS))
            .expect("old range")
            .is_empty()
    );
    assert_eq!(
        store
            .points(
                series.id(),
                ts(minute + 2 * MINUTE_NS)..ts(minute + 3 * MINUTE_NS)
            )
            .expect("kept range"),
        [point(series.id(), minute + 2 * MINUTE_NS, 100.0)]
    );
    // DuckDB allows one read-write handle per file; the raw inspection below happens
    // after the store closes.
    drop(store);

    // Downsampled side: min keeps bottoms, max keeps spikes, last the end-of-minute value.
    // The whole raw block is scoped: statement/row handles keep the connection alive, and
    // the file lock would block the reopen below.
    let buckets: Vec<(i64, f64, f64, f64, f64)> = {
        let conn = duckdb::Connection::open(&path).expect("raw connection");
        let mut stmt = conn
            .prepare(
                "SELECT minute_ns, min_value, max_value, avg_value, last_value
                 FROM points_1m ORDER BY minute_ns",
            )
            .expect("prepare");
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, f64>(1)?,
                    row.get::<_, f64>(2)?,
                    row.get::<_, f64>(3)?,
                    row.get::<_, f64>(4)?,
                ))
            })
            .expect("query");
        rows.map(|row| row.expect("row")).collect()
    };
    assert_eq!(
        buckets,
        [
            (minute, 1.0, 3.0, 2.0, 3.0),
            (minute + MINUTE_NS, 10.0, 20.0, 15.0, 20.0),
        ],
        "avg compares exactly: the test values average without error"
    );

    // Re-running is a safe no-op: nothing new to downsample, nothing deleted.
    let mut reopened = DuckDbStore::open(&path).expect("reopen");
    let again = reopened.retain(requested).expect("retain again");
    assert_eq!(again.minutes_downsampled, 0);
    assert_eq!(again.points_deleted, 0);
}

#[test]
fn file_store_persists_across_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("wolf.duckdb");
    let target = target(0);
    let series = descriptor(&target, "process.threads", "ignored");

    {
        let mut store = DuckDbStore::open(&path).expect("open");
        store
            .register_target(target.clone(), ts(BASE_NS))
            .expect("target");
        store
            .register_series(std::slice::from_ref(&series))
            .expect("series");
        inserted_points(&mut store, &series, &[(BASE_NS, 42.0)]);
    }

    let reopened = DuckDbStore::open(&path).expect("reopen");
    assert_eq!(reopened.targets().expect("targets").len(), 1);
    assert_eq!(
        reopened
            .points(series.id(), ts(BASE_NS - 1)..ts(BASE_NS + 1))
            .expect("points"),
        [point(series.id(), BASE_NS, 42.0)]
    );
}

#[test]
fn series_rows_keep_catalogue_names_and_labels() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("wolf.duckdb");
    let series = descriptor(&target(0), "dotnet.gc.heap.size", "gen2");

    {
        let mut store = DuckDbStore::open(&path).expect("open");
        store
            .register_target(target(0), ts(BASE_NS))
            .expect("target");
        store
            .register_series(std::slice::from_ref(&series))
            .expect("series");
        store.register_series(&[series]).expect("idempotent");
    }

    let conn = duckdb::Connection::open(&path).expect("raw connection");
    let count: i64 = conn
        .query_row("SELECT count(*) FROM series", [], |row| row.get(0))
        .expect("count");
    assert_eq!(count, 1, "re-registration must not duplicate");
    let (metric, labels, unit): (String, String, String) = conn
        .query_row("SELECT metric, labels, unit FROM series", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .expect("row");
    assert_eq!(metric, "dotnet.gc.heap.size");
    assert_eq!(
        serde_json::from_str::<Map<String, Value>>(&labels).expect("labels json"),
        Map::from_iter([("generation".to_owned(), Value::from("gen2"))])
    );
    assert_eq!(unit, "bytes");
}

#[test]
fn rejects_a_store_written_by_a_newer_schema() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("wolf.duckdb");
    let target = target(0);
    {
        let mut store = DuckDbStore::open(&path).expect("open");
        store.register_target(target, ts(BASE_NS)).expect("target");
    }

    let conn = duckdb::Connection::open(&path).expect("raw connection");
    conn.execute(
        "UPDATE wolf_meta SET value = '99' WHERE key = 'schema_version'",
        [],
    )
    .expect("bump version");
    drop(conn);

    match DuckDbStore::open(&path) {
        Err(Error::SchemaTooNew(99, 2)) => {}
        other => panic!("expected SchemaTooNew(99, 2), got {other:?}"),
    }
}

#[test]
fn rejects_a_store_with_an_unmigratable_older_schema() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("wolf.duckdb");
    {
        let _store = DuckDbStore::open(&path).expect("open");
    }
    let conn = duckdb::Connection::open(&path).expect("raw connection");
    conn.execute(
        "UPDATE wolf_meta SET value = '1' WHERE key = 'schema_version'",
        [],
    )
    .expect("downgrade version");
    drop(conn);

    match DuckDbStore::open(&path) {
        Err(Error::SchemaTooOld(1, 2)) => {}
        other => panic!("expected SchemaTooOld(1, 2), got {other:?}"),
    }
}

#[test]
fn series_for_an_unknown_target_is_rejected() {
    let mut store = DuckDbStore::open_in_memory().expect("open");
    let orphan = descriptor(&target(0), "dotnet.gc.heap.size", "gen2");
    assert!(
        store
            .register_series(std::slice::from_ref(&orphan))
            .is_err(),
        "series.target references targets.id"
    );
    store
        .register_target(target(0), ts(BASE_NS))
        .expect("target");
    store
        .register_series(&[orphan])
        .expect("registered once the target exists");
}

#[test]
fn non_finite_points_are_skipped_not_stored() {
    let mut store = DuckDbStore::open_in_memory().expect("open");
    let series = descriptor(&target(0), "dotnet.gc.heap.size", "gen2");
    store
        .register_target(target(0), ts(BASE_NS))
        .expect("target");
    store
        .register_series(std::slice::from_ref(&series))
        .expect("series");
    // NaN and inf would poison the minute aggregates at retention time.
    inserted_points(
        &mut store,
        &series,
        &[
            (BASE_NS, f64::NAN),
            (BASE_NS + 1, f64::INFINITY),
            (BASE_NS + 2, 1.0),
        ],
    );
    assert_eq!(
        store
            .points(series.id(), ts(BASE_NS)..ts(BASE_NS + 100))
            .expect("points"),
        [point(series.id(), BASE_NS + 2, 1.0)],
        "only the finite point lands"
    );
}
