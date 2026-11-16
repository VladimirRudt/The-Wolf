//! The collector host (P1-8): attaches to .NET processes, runs their telemetry sessions,
//! and routes points and events to storage (`docs/collector-dotnet.md` §2,
//! `docs/architecture.md` §4).
//!
//! Streaming pipeline — framework-free per D13 (hand-written, plain threads, blocking
//! std I/O in the data path; tokio only for request/response commands). Vector-style
//! stage names: **sources** produce records, bounded **channels** carry them, and one
//! **pipeline** per shard runs drain → **transform** → **sink**:
//!
//! ```text
//! per target: session source (EventSession → SessionDecoder)
//!       │  ring channel: a preallocated circular buffer (crossbeam ArrayQueue),
//!       │  no allocation per record; a full ring is the backpressure point —
//!       │  the pipe fills, the runtime drops, the parser counts the sequence gaps
//!       ▼
//! pipeline actor, one per shard (D14): drain → transform (pure mapping) → sink
//!       │  the sink is the single writer of its shard's store files
//!       ▲
//! process source: OS memory of every target on a 1 s cadence (thread count every 60th)
//!
//! main: attach, place targets by stable hash (256 logical shards), tokio driver
//!       (ctrl-c → flag), graceful detach:
//!       stop_session → runtime flushes → sources see end of stream → drain, flush, join
//! ```
//!
//! Resource accounting (bounded): per target a 256 KiB parser buffer plus one ring
//! (`session_ring` × ~150 B ≈ 300 KiB at the default); counter strings are rented from the
//! EC pool (`wolf-dotnet`); batch and staging buffers keep steady-state capacity. Nothing
//! in the data path grows with uptime.

mod attach;
mod channel;
mod error;
mod host;
mod pipeline;
mod placement;
mod record;
mod sink;
mod sources;
mod telemetry;
mod transform;

pub use attach::{Attached, attach, host_name, target_id, telemetry_session_config};
pub use channel::{Consumer, Producer, Wakeup, channel};
pub use error::{Error, Result};
pub use host::{HostConfig, HostReport, run};
pub use pipeline::{
    Pipeline, PipelineConfig, PipelineHandle, PipelineStats, PipelineStatsSnapshot, RetentionConfig,
};
pub use placement::{LOGICAL_SHARDS, Placement};
pub use record::{Message, Record};
pub use sources::process::{ProcessSource, ProcessTarget};
pub use sources::session::{SessionDecoder, SessionEnd, SessionItem, run_session_source};
pub use sources::{SourceStats, SourceStatsSnapshot};
pub use telemetry::SelfTelemetry;
pub use transform::TargetFlags;
