//! DuckDB implementation of [`Storage`] (`docs/architecture.md` §4.1, D7).
//!
//! Layout follows §4.1 exactly: `series` (one row per descriptor), `points` with no
//! index — points arrive in time order and row-group zone maps skip by `ts` — and the
//! retention output `points_1m` (`min`/`max`/`avg`/`last` per series per minute, written
//! ordered by `(series_id, minute)`). The DB file is versioned in `wolf_meta` (D9) and a
//! file from a newer build is rejected on open.
//!
//! [`SeriesId`] is a full-range FNV-1a `u64` stored bit-cast as `BIGINT`: equality —
//! the only thing storage relies on — is unaffected, and unsigned order within one series
//! is preserved. Hot path: batch appends go through DuckDB's appender API with no per-row
//! SQL and no per-row allocation (serialized strings reuse buffers across rows). `labels`
//! and event `data` are stored as JSON text (`VARCHAR`); DuckDB reads them with `::JSON`
//! casts where querying inside the store ever needs to.
//!
//! Two connection settings keep inserts cheap (`init`): `preserve_insertion_order = off`
//! drops the order tracking DuckDB otherwise maintains on every write (all reads here
//! `ORDER BY` explicitly), and [`Config`] caps threads and memory so the embedded engine
//! stays a sidecar on the monitored host instead of taking a core and most of the RAM.
//!
//! Serialized forms (target id, JSON payload) are written into [`Scratch`] owned by the
//! store, so repeated batches allocate nothing after warm-up. The EC decoder uses a
//! shared rent-on-drop pool (`wolf-dotnet`'s `EcStringPool`) because its buffers outlive
//! the decode and cross threads; here the buffers never escape one `append_row` and the
//! single writer (D13) is the only user, so plain struct fields do the same job without
//! the pool machinery.

use std::fmt::Write as _;
use std::ops::Range;
use std::path::Path;
use std::str;

use duckdb::{Connection, params};
use wolf_contract::{Event, Point, SeriesDescriptor, SeriesId, TargetId, Timestamp, Unit};

use crate::store::parse_target;
use crate::{Error, Result, Retention, Storage, TargetInfo};

/// Version of the on-disk schema; bump on any breaking DDL change. Older files are
/// migrated forward where a migration exists; newer files are rejected. v1 → v2 has none:
/// it renamed the `points_1m` aggregates and added constraints, and DuckDB cannot add a
/// PRIMARY KEY or FOREIGN KEY to an existing table — recreate v1 files (pre-release only).
const SCHEMA_VERSION: u32 = 2;

/// Nanoseconds per minute: `points_1m` bucket width and the retention cutoff granularity.
const MINUTE_NS: i64 = 60_000_000_000;

/// Scratch capacity beyond this is handed back to the allocator after a batch: one rogue
/// payload (a huge `data` document) must not pin its allocation for the life of the store.
const MAX_SCRATCH_BYTES: usize = 1 << 20;

// Constraints live where they are cheap. DuckDB maintains an ART index per PRIMARY KEY /
// FOREIGN KEY and taxes every inserted row for it, so the append-hot tables (`points`,
// `events`) stay keyless: the single writer keeps their references valid by construction
// and zone maps already skip scans by ts (§4.1). The small, slowly-changing tables carry
// the integrity instead: `series.target` references `targets.id`, and `points_1m` is
// keyed by its grain `(series_id, minute_ns)` so a buggy retention run cannot duplicate
// a bucket.
const DDL: &str = "
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
CREATE TABLE IF NOT EXISTS points (
    series_id BIGINT NOT NULL,
    ts_utc_ns BIGINT NOT NULL,
    value     DOUBLE  NOT NULL
);
CREATE TABLE IF NOT EXISTS points_1m (
    series_id BIGINT NOT NULL,
    minute_ns BIGINT NOT NULL,
    min_value  DOUBLE NOT NULL,
    max_value  DOUBLE NOT NULL,
    avg_value  DOUBLE NOT NULL,
    last_value DOUBLE NOT NULL,
    PRIMARY KEY (series_id, minute_ns)
);
CREATE TABLE IF NOT EXISTS events (
    ts_utc_ns BIGINT  NOT NULL,
    target    VARCHAR NOT NULL,
    kind      VARCHAR NOT NULL,
    data      VARCHAR NOT NULL
);
";

/// How much of the host machine the embedded engine may use. DuckDB's own defaults are
/// one thread per core and ~80% of RAM — right for an analytics workstation, wrong for a
/// collector that must not disturb the processes it watches. The defaults here are small
/// on purpose; raise them only on a dedicated box.
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

/// Serialized forms for the insert path: target id text and JSON payload bytes. Owned by
/// the store, cleared per row, reused across every batch — after the first batches these
/// hold steady-state capacity and inserts allocate nothing.
#[derive(Debug, Default)]
struct Scratch {
    target: String,
    json: Vec<u8>,
}

impl Scratch {
    /// Re-serialize `target` and `payload`, then hand out the borrowed pair for one
    /// appender row. The borrows end with the row; the capacity stays.
    fn row<'a>(
        &'a mut self,
        target: &TargetId,
        payload: &impl serde::Serialize,
    ) -> Result<(&'a str, &'a str)> {
        self.target.clear();
        write!(self.target, "{target}").expect("writing to a String is infallible");
        self.json.clear();
        serde_json::to_writer(&mut self.json, payload)?;
        Ok((
            self.target.as_str(),
            str::from_utf8(&self.json).expect("serde_json emits UTF-8"),
        ))
    }

    /// Drop excess capacity after a batch (see [`MAX_SCRATCH_BYTES`]).
    fn release_burst(&mut self) {
        if self.json.capacity() > MAX_SCRATCH_BYTES {
            self.json = Vec::new();
        }
    }
}

/// DuckDB-backed [`Storage`]. One connection is the single writer; the host owns it.
#[derive(Debug)]
pub struct DuckDbStore {
    conn: Connection,
    scratch: Scratch,
}

impl DuckDbStore {
    /// Opens (creating if needed) a store file with the default resource [`Config`].
    /// Fails if the file was written by a newer schema version.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with(path, Config::default())
    }

    /// Opens with explicit resource limits.
    pub fn open_with(path: impl AsRef<Path>, config: Config) -> Result<Self> {
        Self::init(Connection::open(path)?, config)
    }

    /// In-memory store: unit tests and ephemeral runs.
    pub fn open_in_memory() -> Result<Self> {
        Self::open_in_memory_with(Config::default())
    }

    /// In-memory store with explicit resource limits.
    pub fn open_in_memory_with(config: Config) -> Result<Self> {
        Self::init(Connection::open_in_memory()?, config)
    }

    fn init(conn: Connection, config: Config) -> Result<Self> {
        // Connection-scoped, set before any data arrives. preserve_insertion_order = off
        // skips the per-insert order bookkeeping; nothing here relies on physical row
        // order. The numbers come from `Config` (u32), so no escaping is needed.
        conn.execute_batch(&format!(
            "SET preserve_insertion_order = false;
             SET threads = {};
             SET memory_limit = '{}MiB';",
            config.threads, config.memory_limit_mib
        ))?;
        conn.execute_batch(DDL)?;
        Self::check_schema_version(&conn)?;
        Ok(Self {
            conn,
            scratch: Scratch::default(),
        })
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
                if version > SCHEMA_VERSION {
                    return Err(Error::SchemaTooNew(version, SCHEMA_VERSION));
                }
                if version < SCHEMA_VERSION {
                    return Err(Error::SchemaTooOld(version, SCHEMA_VERSION));
                }
                Ok(())
            }
            // No version recorded yet: this build writes the first version of the format.
            Err(duckdb::Error::QueryReturnedNoRows) => {
                conn.execute(
                    "INSERT INTO wolf_meta (key, value) VALUES ('schema_version', ?)",
                    params![SCHEMA_VERSION.to_string()],
                )?;
                Ok(())
            }
            Err(error) => Err(error.into()),
        }
    }
}

impl Storage for DuckDbStore {
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
        // One statement parse for the whole batch; serialized forms reuse scratch buffers.
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

    fn insert_points(&mut self, points: &[Point]) -> Result<()> {
        if points.is_empty() {
            return Ok(());
        }
        let mut appender = self.conn.appender("points")?;
        for point in points {
            // NaN/inf would poison the retention aggregates of a whole minute bucket
            // (max and avg become NaN). Skip such points rather than fail the batch —
            // never crash on unknown input; a dropped-point counter belongs to the
            // host's self-telemetry (P1-8).
            if !point.value.is_finite() {
                continue;
            }
            appender.append_row(params![
                point.series.0 as i64,
                point.ts.as_nanos(),
                point.value
            ])?;
        }
        appender.flush()?;
        Ok(())
    }

    fn insert_events(&mut self, events: &[Event]) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        let mut appender = self.conn.appender("events")?;
        // No allocation per event and none per batch: target id and JSON payload go into
        // the store's scratch space, the kind is passed by reference.
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

    fn points(&self, series: SeriesId, range: Range<Timestamp>) -> Result<Vec<Point>> {
        let mut stmt = self.conn.prepare(
            "SELECT ts_utc_ns, value FROM points
             WHERE series_id = ? AND ts_utc_ns >= ? AND ts_utc_ns < ?
             ORDER BY ts_utc_ns",
        )?;
        let rows = stmt.query_map(
            params![
                series.0 as i64,
                range.start.as_nanos(),
                range.end.as_nanos()
            ],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, f64>(1)?)),
        )?;
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

    fn retain(&mut self, before: Timestamp) -> Result<Retention> {
        // Floor to the minute: the partially covered trailing minute stays raw, and every
        // bucket is either fully before the cutoff or fully after it.
        let cutoff = Timestamp::from_nanos(before.as_nanos() / MINUTE_NS * MINUTE_NS);
        let tx = self.conn.transaction()?;

        // Aggregate the retiring points once (per §4.1: min keeps sawtooth bottoms, max
        // keeps spikes, avg the level, last the end-of-minute step value).
        tx.execute_batch("DROP TABLE IF EXISTS _retention_agg")?;
        // `//`, not `/`: in DuckDB `/` is floating-point division even on BIGINT
        // (`5 / 2 = 2.5`), which buckets every point separately; `//` is integer division.
        // The literal is a crate constant — no injection surface.
        tx.execute(
            "CREATE TEMP TABLE _retention_agg AS
             SELECT series_id,
                    ts_utc_ns // 60000000000 * 60000000000 AS minute_ns,
                    min(value)  AS min_value,
                    max(value)  AS max_value,
                    avg(value)  AS avg_value,
                    arg_max(value, ts_utc_ns) AS last_value
             FROM points
             WHERE ts_utc_ns < ?
             GROUP BY series_id, minute_ns",
            params![cutoff.as_nanos()],
        )?;
        let minutes_downsampled: i64 = tx.query_row(
            "SELECT count(*) FROM _retention_agg a
             LEFT JOIN points_1m m
               ON m.series_id = a.series_id AND m.minute_ns = a.minute_ns
             WHERE m.series_id IS NULL",
            [],
            |row| row.get(0),
        )?;
        // The anti-join makes the job idempotent: a bucket already downsampled (e.g. from
        // a late out-of-order point) is left alone instead of duplicated — and the
        // PRIMARY KEY on (series_id, minute_ns) is the backstop if this logic ever errs.
        // ORDER BY writes points_1m grouped by series so long-term reads skip by
        // series_id too.
        tx.execute_batch(
            "INSERT INTO points_1m (series_id, minute_ns, min_value, max_value,
                                    avg_value, last_value)
             SELECT a.series_id, a.minute_ns, a.min_value, a.max_value,
                    a.avg_value, a.last_value
             FROM _retention_agg a
             LEFT JOIN points_1m m
               ON m.series_id = a.series_id AND m.minute_ns = a.minute_ns
             WHERE m.series_id IS NULL
             ORDER BY a.series_id, a.minute_ns",
        )?;
        let deleted = tx.execute(
            "DELETE FROM points WHERE ts_utc_ns < ?",
            params![cutoff.as_nanos()],
        )?;
        tx.execute_batch("DROP TABLE _retention_agg")?;
        tx.commit()?;

        Ok(Retention {
            points_deleted: deleted,
            minutes_downsampled: usize::try_from(minutes_downsampled).unwrap_or(usize::MAX),
            cutoff,
        })
    }
}

/// Serializes a unit to its catalogue string (`"bytes"`, `"count"`, …; unknown units as
/// their raw text). Reuses the contract's serde rules so storage and wire agree.
fn unit_text(unit: &Unit) -> Result<String> {
    let value = serde_json::to_value(unit)?;
    value
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| Error::Contract(format!("unit does not serialize to a string: {value}")))
}
