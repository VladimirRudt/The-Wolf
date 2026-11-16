//! Read-only view over a whole sharded store (`docs/architecture.md` §4.1, D14): one
//! in-memory DuckDB connection that ATTACHes every shard's `meta` and segment files
//! read-only and answers queries over their union. This is the query surface future
//! consumers use (CLI, MCP server, TUI) — they never open shard files for writing.
//!
//! DuckDB file locks allow either one read-write connection or many read-only ones, so a
//! `QueryStore` and a running host cannot share the files. Use it after the host stops,
//! or against a copied snapshot of the store directory.

use std::ops::Range;
use std::path::Path;

use duckdb::types::Value;
use duckdb::{Connection, params_from_iter};
use wolf_contract::{Event, Point, SeriesId, TargetId, Timestamp};

use crate::shard::{SHARD_PREFIX, segment_files};
use crate::store::parse_target;
use crate::{Result, TargetInfo};

/// Read-only union over every shard file under a store root.
#[derive(Debug)]
pub struct QueryStore {
    conn: Connection,
    /// Aliases of every attached `meta` catalog, one per shard.
    metas: Vec<String>,
    /// Aliases of every attached day-segment catalog.
    segments: Vec<String>,
}

impl QueryStore {
    /// Attaches every shard under `root` read-only. A missing or empty root gives an
    /// empty store, not an error.
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch("SET preserve_insertion_order = false")?;
        let mut store = Self {
            conn,
            metas: Vec::new(),
            segments: Vec::new(),
        };
        let mut shards: Vec<_> = match std::fs::read_dir(root) {
            Ok(entries) => entries.collect::<std::result::Result<_, _>>()?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(store),
            Err(error) => return Err(error.into()),
        };
        shards.sort_by_key(|entry| entry.file_name());
        for entry in shards {
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if !name.starts_with(SHARD_PREFIX) || !entry.path().is_dir() {
                continue;
            }
            store.attach_shard(name, &entry.path())?;
        }
        Ok(store)
    }

    /// Attaches one shard directory's meta and segment files. Catalog aliases derive from
    /// the directory name (`shard-00` → `shard_00_meta`, …).
    fn attach_shard(&mut self, shard: &str, dir: &Path) -> Result<()> {
        let prefix = shard.replace('-', "_");
        let meta = dir.join("meta.duckdb");
        if meta.exists() {
            let alias = format!("{prefix}_meta");
            self.conn.execute_batch(&format!(
                "ATTACH '{}' AS {alias} (READ_ONLY)",
                meta.to_string_lossy().replace('\'', "''")
            ))?;
            self.metas.push(alias);
        }
        for (date, path) in segment_files(dir)? {
            let alias = format!("{prefix}_seg_{date}").replace('-', "_");
            self.conn.execute_batch(&format!(
                "ATTACH '{}' AS {alias} (READ_ONLY)",
                path.to_string_lossy().replace('\'', "''")
            ))?;
            self.segments.push(alias);
        }
        Ok(())
    }

    /// All registered targets across all shards, oldest first. A target lives in exactly
    /// one shard's meta, but deduplicating covers a target that changed shards when the
    /// shard count changed between runs.
    pub fn targets(&self) -> Result<Vec<TargetInfo>> {
        let rows = self.union_query(
            &self.metas,
            "SELECT id, first_seen_ns FROM {cat}.targets",
            "UNION",
            " ORDER BY first_seen_ns",
            Vec::new(),
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
        )?;
        let mut targets = Vec::new();
        for (id, first_seen_ns) in rows {
            targets.push(TargetInfo {
                id: parse_target(&id)?,
                first_seen: Timestamp::from_nanos(first_seen_ns),
            });
        }
        Ok(targets)
    }

    /// Raw points of one series across every segment of every shard, time-ordered.
    pub fn points(&self, series: SeriesId, range: Range<Timestamp>) -> Result<Vec<Point>> {
        // DuckDB numbers `?` placeholders across the whole statement, so each union leg
        // contributes its own triple of bindings, concatenated in leg order.
        let bindings = self
            .segments
            .iter()
            .flat_map(|_| {
                [
                    series.0 as i64,
                    range.start.as_nanos(),
                    range.end.as_nanos(),
                ]
            })
            .map(Value::from)
            .collect();
        let rows = self.union_query(
            &self.segments,
            "SELECT ts_utc_ns, value FROM {cat}.points
             WHERE series_id = ? AND ts_utc_ns >= ? AND ts_utc_ns < ?",
            "UNION ALL",
            " ORDER BY ts_utc_ns",
            bindings,
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, f64>(1)?)),
        )?;
        Ok(rows
            .into_iter()
            .map(|(ts, value)| Point {
                series,
                ts: Timestamp::from_nanos(ts),
                value,
            })
            .collect())
    }

    /// Events of one target across all shards, time-ordered.
    pub fn events(&self, target: &TargetId, range: Range<Timestamp>) -> Result<Vec<Event>> {
        let bindings = self
            .metas
            .iter()
            .flat_map(|_| {
                [
                    Value::from(target.to_string()),
                    Value::from(range.start.as_nanos()),
                    Value::from(range.end.as_nanos()),
                ]
            })
            .collect();
        let rows = self.union_query(
            &self.metas,
            "SELECT ts_utc_ns, kind, data FROM {cat}.events
             WHERE target = ? AND ts_utc_ns >= ? AND ts_utc_ns < ?",
            "UNION ALL",
            " ORDER BY ts_utc_ns",
            bindings,
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )?;
        let mut events = Vec::new();
        for (ts, kind, data) in rows {
            events.push(Event {
                ts: Timestamp::from_nanos(ts),
                target: target.clone(),
                kind,
                data: serde_json::from_str(&data)?,
            });
        }
        Ok(events)
    }

    /// Runs `select` once per catalog (`{cat}` replaced by the alias), joined with
    /// `join` (`UNION` / `UNION ALL`), plus a trailing clause such as `ORDER BY`.
    fn union_query<T>(
        &self,
        catalogs: &[String],
        select: &str,
        join: &str,
        suffix: &str,
        bindings: Vec<Value>,
        map: impl FnMut(&duckdb::Row<'_>) -> duckdb::Result<T>,
    ) -> Result<Vec<T>> {
        if catalogs.is_empty() {
            return Ok(Vec::new());
        }
        let sql = catalogs
            .iter()
            .map(|catalog| select.replace("{cat}", catalog))
            .collect::<Vec<_>>()
            .join(&format!(" {join} "));
        let mut stmt = self.conn.prepare(&(sql + suffix))?;
        let rows = stmt.query_map(params_from_iter(bindings), map)?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }
}
