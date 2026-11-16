//! Sharded, time-segmented DuckDB store: the scalable layout (`docs/architecture.md`
//! §4.1, D14).
//!
//! One [`ShardStore`] is one shard of one host; the host runs a handful and each is the
//! single writer of its own files (D13 — one writer per partition, never many writers to
//! one file). Layout:
//!
//! ```text
//! <root>/shard-00/
//!   meta.duckdb            — targets, series, events, wolf_meta (small, kept forever)
//!   longterm.duckdb        — points_1m (downsampled history)
//!   seg-YYYY-MM-DD.duckdb  — raw points of one UTC day
//! ```
//!
//! Everything hangs off one DuckDB connection whose primary catalog is `meta`; segments
//! and `longterm` are ATTACHed. Raw points land in the segment of their UTC day, so
//! retention never deletes rows: it downsamples a sealed segment into `longterm.points_1m`
//! and removes the file (§4.1). A crash between the downsample and the removal is
//! idempotent — the anti-join against `points_1m` skips buckets already downsampled.
//!
//! Cross-shard reads are [`crate::QueryStore`]; this type is the write path and answers
//! reads for its own shard only.

use std::collections::BTreeMap;
use std::ops::Range;
use std::path::{Path, PathBuf};

use duckdb::{Connection, params, params_from_iter};
use wolf_contract::{Event, Point, SeriesDescriptor, SeriesId, TargetId, Timestamp};

use crate::date::Date;
use crate::duckdb::{Scratch, unit_text};
use crate::store::parse_target;
use crate::{Error, Result, Retention, Storage, TargetInfo};

/// Version of the sharded on-disk layout, recorded in `wolf_meta` of each `meta.duckdb`.
/// Independent of the single-file [`crate::DuckDbStore`] schema version.
const SHARD_SCHEMA_VERSION: u32 = 1;

/// Segment file naming: `seg-YYYY-MM-DD.duckdb`.
pub(crate) const SEGMENT_PREFIX: &str = "seg-";
pub(crate) const SEGMENT_SUFFIX: &str = ".duckdb";

/// Shard directory naming: `shard-00`, `shard-01`, …
pub(crate) const SHARD_PREFIX: &str = "shard-";

/// Catalog aliases inside the shard's connection.
const LONGTERM_CATALOG: &str = "longterm";

/// The primary catalog (`meta.duckdb`): metadata and events. `points` and `points_1m`
/// deliberately live elsewhere — segments and `longterm` — so retention is a file drop.
const META_DDL: &str = "
CREATE TABLE IF NOT EXISTS wolf_meta (
    key   VARCHAR PRIMARY KEY,
    value VARCHAR NOT NULL
);
CREATE TABLE IF NOT EXISTS targets (
    id            VARCHAR PRIMARY KEY,
    first_seen_ns BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS series (
    id     BIGINT PRIMARY KEY,
    target VARCHAR NOT NULL REFERENCES targets (id),
    metric VARCHAR NOT NULL,
    labels VARCHAR NOT NULL,
    unit   VARCHAR NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
    ts_utc_ns BIGINT  NOT NULL,
    target    VARCHAR NOT NULL,
    kind      VARCHAR NOT NULL,
    data      VARCHAR NOT NULL
);
";

/// The directory one shard writes into (`<root>/shard-NN`).
pub fn shard_dir(root: &Path, shard: u16) -> PathBuf {
    root.join(format!("{SHARD_PREFIX}{shard:02}"))
}

/// A DuckDB store covering one shard: one connection, one primary catalog for metadata,
/// one attached catalog per day-segment of raw points, one for downsampled history.
///
/// Resource budget: shards multiply DuckDB connections, so the default [`Config`] here is
/// smaller than the single-file store's — one engine thread per shard.
#[derive(Debug)]
pub struct ShardStore {
    conn: Connection,
    /// `None` in the in-memory mode used by tests: segment files are never removed.
    dir: Option<PathBuf>,
    /// Attached segments, day → catalog alias.
    segments: BTreeMap<Date, String>,
    scratch: Scratch,
}

impl ShardStore {
    /// Opens (creating if needed) a shard directory. Small engine footprint: one DuckDB
    /// thread and 128 MiB per shard; raise via [`Self::open_with`] on a dedicated box.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        Self::open_with(
            dir,
            crate::Config {
                threads: 1,
                memory_limit_mib: 128,
            },
        )
    }

    /// Opens with explicit resource limits.
    pub fn open_with(dir: impl AsRef<Path>, config: crate::Config) -> Result<Self> {
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir)?;
        let conn = Connection::open(dir.join("meta.duckdb"))?;
        let mut store = Self::init(conn, Some(dir.to_path_buf()), config)?;
        for (date, path) in segment_files(dir)? {
            store.attach_segment(date, &path)?;
        }
        Ok(store)
    }

    /// In-memory shard store: unit tests and ephemeral runs.
    pub fn open_in_memory() -> Result<Self> {
        Self::open_in_memory_with(crate::Config::default())
    }

    /// In-memory shard store with explicit resource limits.
    pub fn open_in_memory_with(config: crate::Config) -> Result<Self> {
        Self::init(Connection::open_in_memory()?, None, config)
    }

    fn init(conn: Connection, dir: Option<PathBuf>, config: crate::Config) -> Result<Self> {
        conn.execute_batch(&format!(
            "SET preserve_insertion_order = false;
             SET threads = {};
             SET memory_limit = '{}MiB';",
            config.threads, config.memory_limit_mib
        ))?;
        // The primary catalog is the meta database, so metadata DDL and inserts are
        // unqualified — same names as the single-file layout.
        conn.execute_batch(META_DDL)?;
        check_schema_version(&conn)?;
        let store = Self {
            conn,
            dir,
            segments: BTreeMap::new(),
            scratch: Scratch::default(),
        };
        store.attach_catalog(LONGTERM_CATALOG, "longterm.duckdb")?;
        store.conn.execute_batch(&format!(
            "CREATE TABLE IF NOT EXISTS {LONGTERM_CATALOG}.points_1m (
                 series_id BIGINT NOT NULL,
                 minute_ns BIGINT NOT NULL,
                 min_value  DOUBLE NOT NULL,
                 max_value  DOUBLE NOT NULL,
                 avg_value  DOUBLE NOT NULL,
                 last_value DOUBLE NOT NULL,
                 PRIMARY KEY (series_id, minute_ns)
             );"
        ))?;
        Ok(store)
    }

    /// Attaches one database file (or `:memory:`) under `catalog`, creating it if needed.
    fn attach_catalog(&self, catalog: &str, file: &str) -> Result<()> {
        let path = match &self.dir {
            Some(dir) => dir.join(file).to_string_lossy().into_owned(),
            None => ":memory:".to_owned(),
        };
        self.conn.execute_batch(&format!(
            "ATTACH '{}' AS {catalog}",
            path.replace('\'', "''")
        ))?;
        Ok(())
    }

    /// Attaches an existing segment file found on disk at open time.
    fn attach_segment(&mut self, date: Date, path: &Path) -> Result<()> {
        let catalog = segment_catalog(date);
        self.conn.execute_batch(&format!(
            "ATTACH '{}' AS {catalog}",
            path.to_string_lossy().replace('\'', "''")
        ))?;
        self.segments.insert(date, catalog);
        Ok(())
    }

    /// The catalog holding `day`'s raw points, attaching and creating it on first use.
    /// Returns an owned name: a borrow would keep `self` frozen for the caller's appender.
    fn segment(&mut self, day: Date) -> Result<String> {
        if !self.segments.contains_key(&day) {
            let catalog = segment_catalog(day);
            self.attach_catalog(&catalog, &segment_file_name(day))?;
            self.conn.execute_batch(&format!(
                "CREATE TABLE IF NOT EXISTS {catalog}.points (
                     series_id BIGINT NOT NULL,
                     ts_utc_ns BIGINT NOT NULL,
                     value     DOUBLE  NOT NULL
                 );"
            ))?;
            self.segments.insert(day, catalog);
        }
        Ok(self.segments[&day].clone())
    }
}

impl Storage for ShardStore {
    fn register_target(&mut self, target: TargetId, first_seen: Timestamp) -> Result<()> {
        self.conn.execute(
            "INSERT INTO targets (id, first_seen_ns) VALUES (?, ?) ON CONFLICT DO NOTHING",
            params![target.to_string(), first_seen.as_nanos()],
        )?;
        Ok(())
    }

    fn targets(&self) -> Result<Vec<TargetInfo>> {
        let mut stmt = self
            .conn
            .prepare("SELECT id, first_seen_ns FROM targets ORDER BY first_seen_ns")?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?;
        let mut targets = Vec::new();
        for row in rows {
            let (id, first_seen_ns) = row?;
            targets.push(TargetInfo {
                id: parse_target(&id)?,
                first_seen: Timestamp::from_nanos(first_seen_ns),
            });
        }
        Ok(targets)
    }

    fn register_series(&mut self, descriptors: &[SeriesDescriptor]) -> Result<()> {
        if descriptors.is_empty() {
            return Ok(());
        }
        let mut stmt = self.conn.prepare(
            "INSERT INTO series (id, target, metric, labels, unit)
             VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
        )?;
        for descriptor in descriptors {
            let (target, labels) = self.scratch.row(descriptor.target(), descriptor.labels())?;
            stmt.execute(params![
                descriptor.id().0 as i64,
                target,
                descriptor.metric(),
                labels,
                unit_text(descriptor.unit())?,
            ])?;
        }
        self.scratch.release_burst();
        Ok(())
    }

    /// Points are grouped by UTC day and appended to that day's segment. A flush
    /// straddling midnight lands in two segments; order within each is preserved.
    fn insert_points(&mut self, points: &[Point]) -> Result<()> {
        if points.is_empty() {
            return Ok(());
        }
        let mut by_day: BTreeMap<Date, Vec<&Point>> = BTreeMap::new();
        for point in points {
            // Same NaN/inf rule as the single-file store: never poison an aggregate.
            if point.value.is_finite() {
                by_day
                    .entry(Date::from_nanos(point.ts.as_nanos()))
                    .or_default()
                    .push(point);
            }
        }
        for (day, points) in by_day {
            let catalog = self.segment(day)?;
            let mut appender = self
                .conn
                .appender_to_catalog_and_db("points", &catalog, "main")?;
            for point in points {
                appender.append_row(params![
                    point.series.0 as i64,
                    point.ts.as_nanos(),
                    point.value
                ])?;
            }
            appender.flush()?;
        }
        Ok(())
    }

    fn insert_events(&mut self, events: &[Event]) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        let mut appender = self.conn.appender("events")?;
        for event in events {
            let (target, data) = self.scratch.row(&event.target, &event.data)?;
            appender.append_row(params![
                event.ts.as_nanos(),
                target,
                event.kind.as_str(),
                data,
            ])?;
        }
        appender.flush()?;
        self.scratch.release_burst();
        Ok(())
    }

    /// Raw points of one series, unioned over this shard's day segments.
    fn points(&self, series: SeriesId, range: Range<Timestamp>) -> Result<Vec<Point>> {
        if self.segments.is_empty() {
            return Ok(Vec::new());
        }
        let mut sql = String::new();
        for (i, catalog) in self.segments.values().enumerate() {
            if i > 0 {
                sql.push_str(" UNION ALL ");
            }
            sql.push_str(&format!(
                "SELECT ts_utc_ns, value FROM {catalog}.points \
                 WHERE series_id = ? AND ts_utc_ns >= ? AND ts_utc_ns < ?"
            ));
        }
        sql.push_str(" ORDER BY ts_utc_ns");
        let bindings = self.segments.values().flat_map(|_| {
            [
                series.0 as i64,
                range.start.as_nanos(),
                range.end.as_nanos(),
            ]
        });
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map(params_from_iter(bindings), |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, f64>(1)?))
        })?;
        let mut points = Vec::new();
        for row in rows {
            let (ts, value) = row?;
            points.push(Point {
                series,
                ts: Timestamp::from_nanos(ts),
                value,
            });
        }
        Ok(points)
    }

    fn events(&self, target: &TargetId, range: Range<Timestamp>) -> Result<Vec<Event>> {
        let mut stmt = self.conn.prepare(
            "SELECT ts_utc_ns, kind, data FROM events
             WHERE target = ? AND ts_utc_ns >= ? AND ts_utc_ns < ?
             ORDER BY ts_utc_ns",
        )?;
        let rows = stmt.query_map(
            params![
                target.to_string(),
                range.start.as_nanos(),
                range.end.as_nanos()
            ],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )?;
        let mut events = Vec::new();
        for row in rows {
            let (ts, kind, data) = row?;
            events.push(Event {
                ts: Timestamp::from_nanos(ts),
                target: target.clone(),
                kind,
                data: serde_json::from_str(&data)?,
            });
        }
        Ok(events)
    }

    /// Retires every segment whose whole day is older than `before`: downsamples it into
    /// `longterm.points_1m` (idempotently — re-running skips buckets already present) and
    /// removes the file. No `DELETE` ever runs against the raw table. The day containing
    /// `before` stays raw, so the effective granularity is one UTC day, not one minute
    /// like the single-file store.
    fn retain(&mut self, before: Timestamp) -> Result<Retention> {
        let cutoff_day = Date::from_nanos(before.as_nanos());
        let retiring: Vec<(Date, String)> = self
            .segments
            .range(..cutoff_day)
            .map(|(date, catalog)| (*date, catalog.clone()))
            .collect();

        let mut points_deleted = 0;
        let mut minutes_downsampled = 0;
        for (date, catalog) in retiring {
            // `//`, not `/`: in DuckDB `/` is floating-point division even on BIGINT.
            let new_buckets: i64 = self.conn.query_row(
                &format!(
                    "SELECT count(*) FROM (
                         SELECT DISTINCT series_id,
                                ts_utc_ns // 60000000000 * 60000000000 AS minute_ns
                         FROM {catalog}.points
                     ) a
                     LEFT JOIN {LONGTERM_CATALOG}.points_1m m
                       ON m.series_id = a.series_id AND m.minute_ns = a.minute_ns
                     WHERE m.series_id IS NULL"
                ),
                [],
                |row| row.get(0),
            )?;
            minutes_downsampled += usize::try_from(new_buckets).unwrap_or(usize::MAX);
            self.conn.execute_batch(&format!(
                "INSERT INTO {LONGTERM_CATALOG}.points_1m
                     (series_id, minute_ns, min_value, max_value, avg_value, last_value)
                 SELECT a.series_id, a.minute_ns, a.min_value, a.max_value,
                        a.avg_value, a.last_value
                 FROM (
                     SELECT series_id,
                            ts_utc_ns // 60000000000 * 60000000000 AS minute_ns,
                            min(value)  AS min_value,
                            max(value)  AS max_value,
                            avg(value)  AS avg_value,
                            arg_max(value, ts_utc_ns) AS last_value
                     FROM {catalog}.points
                     GROUP BY series_id, minute_ns
                 ) a
                 LEFT JOIN {LONGTERM_CATALOG}.points_1m m
                   ON m.series_id = a.series_id AND m.minute_ns = a.minute_ns
                 WHERE m.series_id IS NULL
                 ORDER BY a.series_id, a.minute_ns"
            ))?;
            let count: i64 = self.conn.query_row(
                &format!("SELECT count(*) FROM {catalog}.points"),
                [],
                |row| row.get(0),
            )?;
            points_deleted += usize::try_from(count).unwrap_or(usize::MAX);

            self.conn.execute_batch(&format!("DETACH {catalog}"))?;
            self.segments.remove(&date);
            if let Some(dir) = &self.dir {
                std::fs::remove_file(dir.join(segment_file_name(date)))?;
            }
        }

        Ok(Retention {
            points_deleted,
            minutes_downsampled,
            cutoff: Timestamp::from_nanos(cutoff_day.start_nanos()),
        })
    }
}

/// `seg-YYYY-MM-DD.duckdb`.
pub(crate) fn segment_file_name(date: Date) -> String {
    format!("{SEGMENT_PREFIX}{date}{SEGMENT_SUFFIX}")
}

/// The catalog alias of a day segment: `seg_2026_05_20` (identifiers cannot contain `-`).
pub(crate) fn segment_catalog(date: Date) -> String {
    format!("seg_{}", date.to_string().replace('-', "_"))
}

/// Lists the segment files of one shard directory, oldest first. Foreign files (a WAL,
/// a stray lock file) are skipped, not an error.
pub(crate) fn segment_files(dir: &Path) -> Result<Vec<(Date, PathBuf)>> {
    let mut segments = Vec::new();
    if !dir.exists() {
        return Ok(segments);
    }
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let Some(body) = name
            .strip_prefix(SEGMENT_PREFIX)
            .and_then(|body| body.strip_suffix(SEGMENT_SUFFIX))
        else {
            continue;
        };
        if let Some(date) = Date::parse(body) {
            segments.push((date, path));
        }
    }
    segments.sort();
    Ok(segments)
}

fn check_schema_version(conn: &Connection) -> Result<()> {
    let stored = conn.query_row(
        "SELECT value FROM wolf_meta WHERE key = 'schema_version'",
        [],
        |row| row.get::<_, String>(0),
    );
    match stored {
        Ok(text) => {
            let version: u32 = text.parse().map_err(|_| {
                Error::Contract(format!(
                    "wolf_meta.schema_version is not a number: {text:?}"
                ))
            })?;
            if version > SHARD_SCHEMA_VERSION {
                return Err(Error::SchemaTooNew(version, SHARD_SCHEMA_VERSION));
            }
            if version < SHARD_SCHEMA_VERSION {
                return Err(Error::SchemaTooOld(version, SHARD_SCHEMA_VERSION));
            }
            Ok(())
        }
        Err(duckdb::Error::QueryReturnedNoRows) => {
            conn.execute(
                "INSERT INTO wolf_meta (key, value) VALUES ('schema_version', ?)",
                params![SHARD_SCHEMA_VERSION.to_string()],
            )?;
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wolf_contract::{SeriesDescriptor, Unit};

    const DAY_NS: i64 = 86_400_000_000_000;

    fn day_ns(days: i64) -> i64 {
        (20_559 + days) * DAY_NS + 3_600_000_000_000
    }

    /// Retention writes the minute aggregates into `longterm.duckdb` before dropping the
    /// segment. Verified by opening the longterm file directly — the writer is dropped
    /// first, so the file lock is free.
    #[test]
    fn retention_writes_minute_aggregates() {
        let root = tempfile::tempdir().expect("tempdir");
        let dir = shard_dir(root.path(), 0);
        let target = TargetId::new(4812, 1_760_843_645, "srv-a").expect("target");
        let series = SeriesDescriptor::new(
            target.clone(),
            "dotnet.gc.heap.size",
            BTreeMap::new(),
            Unit::Bytes,
        );
        {
            let mut store = ShardStore::open(&dir).expect("store");
            store
                .register_target(target, Timestamp::from_nanos(day_ns(0)))
                .expect("register");
            store
                .register_series(std::slice::from_ref(&series))
                .expect("series");
            store
                .insert_points(&[
                    Point {
                        series: series.id(),
                        ts: Timestamp::from_nanos(day_ns(0)),
                        value: 10.0,
                    },
                    Point {
                        series: series.id(),
                        ts: Timestamp::from_nanos(day_ns(0) + 30_000_000_000),
                        value: 20.0,
                    },
                ])
                .expect("insert");
            let done = store
                .retain(Timestamp::from_nanos(day_ns(1)))
                .expect("retain");
            assert_eq!(done.points_deleted, 2);
            assert_eq!(done.minutes_downsampled, 1);
        }

        let conn = Connection::open(dir.join("longterm.duckdb")).expect("longterm");
        let mut stmt = conn
            .prepare(
                "SELECT series_id, minute_ns, min_value, max_value, avg_value, last_value
                 FROM points_1m",
            )
            .expect("prepare");
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, f64>(2)?,
                    row.get::<_, f64>(3)?,
                    row.get::<_, f64>(4)?,
                    row.get::<_, f64>(5)?,
                ))
            })
            .expect("query")
            .collect::<std::result::Result<Vec<_>, _>>()
            .expect("rows");
        assert_eq!(rows.len(), 1);
        let (series_id, minute_ns, min, max, avg, last) = rows[0];
        assert_eq!(series_id, series.id().0 as i64);
        assert_eq!(minute_ns, day_ns(0));
        assert_eq!((min, max, avg, last), (10.0, 20.0, 15.0, 20.0));
    }
}
