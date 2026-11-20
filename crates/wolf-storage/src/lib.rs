//! Storage for telemetry, events and targets: one trait, a sharded DuckDB layout behind
//! it (`docs/architecture.md` §4, D7, D14).
//!
//! - [`ShardStore`] — the layout the host writes: one directory per shard with a small
//!   `meta.duckdb` (targets, series, events), one `seg-YYYY-MM-DD.duckdb` of raw points
//!   per UTC day, and `longterm.duckdb` for downsampled history. Retention downsamples a
//!   sealed day into `longterm` and drops the file — no `DELETE` ever runs.
//! - [`QueryStore`] — the read side: attaches every shard's files read-only and answers
//!   over their union.
//!
//! Each store file is versioned in `wolf_meta` (D9); a file written by a newer build is
//! rejected on open. The trait stays backend-agnostic so DuckDB can be swapped
//! (ClickHouse, VictoriaMetrics) later.
//!
//! Synchronous like the rest of the data path (D13): one shard's writer block owns one
//! [`ShardStore`] and is the single writer of its files; `Storage: Send` so stores can be
//! moved to their threads.
//!
//! Inserts are the hot path (many sources × many series): they go through DuckDB's
//! appender in caller-sized batches with no per-row allocation, and the engine is capped
//! ([`Config`]) so it cannot crowd out the processes being watched.

mod date;
mod error;
mod query;
mod shard;
mod store;

pub use error::{Error, Result};
pub use query::QueryStore;
pub use shard::{ShardStore, shard_dir};
pub use store::{Config, Retention, Storage, TargetInfo};
