//! Storage errors. Backend-specific failures flow through [`Error::Backend`]; the trait
//! stays backend-agnostic so DuckDB can be swapped (`docs/architecture.md` §4, D7).

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    /// The embedded database reported a failure.
    #[error("duckdb: {0}")]
    DuckDb(#[from] duckdb::Error),
    /// Data read back violates the contract (e.g. a stored target id does not parse).
    /// Only reachable with a corrupted database; callers treat it as fatal for the query.
    #[error("stored data violates the contract: {0}")]
    Contract(String),
    /// JSON (de)serialization of a stored document failed; the database is corrupt.
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    /// The store file was written by a newer schema version than this build understands.
    #[error("storage schema v{0} is newer than supported v{1}")]
    SchemaTooNew(u32, u32),
    /// The store file predates the oldest schema this build can still read, and no
    /// migration exists for it.
    #[error(
        "storage schema v{0} is older than supported v{1} and has no migration; recreate the store file"
    )]
    SchemaTooOld(u32, u32),
}

pub type Result<T> = std::result::Result<T, Error>;
