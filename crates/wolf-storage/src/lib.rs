//! Storage for telemetry, events and targets: one trait, a DuckDB implementation behind it
//! (`docs/architecture.md` §4, D7). The DB file is a versioned format of its own
//! (`docs/architecture.md` D9): the schema version lives in `wolf_meta` and a file written
//! by a newer build is rejected on open.
//!
//! Layout (`docs/architecture.md` §4.1):
//! - `series` — one row per [`SeriesDescriptor`];
//! - `points (series_id, ts_utc_ns, value)` — no index: points arrive in time order and
//!   row-group zone maps skip by `ts`;
//! - `points_1m` — retention output: `min`/`max`/`avg`/`last` per series per minute,
//!   written ordered by `(series_id, minute)`;
//! - `events`, `targets`, `wolf_meta`.
//!
//! Synchronous like the rest of the data path (D13): the host owns the single writer and
//! calls it from its own thread; `Storage: Send` so it can be moved there.
//!
//! Inserts are the hot path (many sources × many series): they go through DuckDB's
//! appender in caller-sized batches with no per-row allocation, and the engine is capped
//! ([`Config`]) so it cannot crowd out the processes being watched.

mod duckdb;
mod error;
mod store;

pub use duckdb::{Config, DuckDbStore};
pub use error::{Error, Result};
pub use store::{Retention, Storage, TargetInfo};
