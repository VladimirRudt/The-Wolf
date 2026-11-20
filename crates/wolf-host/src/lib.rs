//! The collector host (P1-8): attaches to .NET processes, runs their telemetry sessions,
//! and routes points and events to storage (`docs/collector-dotnet.md` §2,
//! `docs/architecture.md` §4).
//!
//! The workflow is a dataflow graph (D15), TPL-Dataflow style: blocks with bounded input
//! buffers, wired with `link_to`, completed by dropping their producers. There is no
//! actor framework — blocks are built from `dataflow.rs`, one block per subsystem
//! (`blocks/`), and the wiring in `host.rs` is the only place that knows the graph's
//! shape:
//!
//! ```text
//! per target: session producer (EventSession → SessionDecoder)
//!       │  post into the shard's dotnet-gc block input (bounded ring; a full buffer is
//!       │  the backpressure point — the pipe fills, the runtime drops, the parser
//!       │  counts the sequence gaps)
//!       ▼
//! dotnet-gc block, one per shard (D14): session telemetry/counters → points,
//!       target and session lifecycle → events, attach → registration
//!       │  link_to (completion propagates) ──┐
//!                                          ▼
//! process block, one per shard ───►  store block, one per shard: batching + the
//!       │                            single writer of the shard's store files
//!       └ samples OS memory of its targets on its own tick (thread count every 60th)
//!
//! main: attach, place targets by stable hash (256 logical shards), tokio driver
//!       (ctrl-c → flag), graceful detach:
//!       stop_session → runtime flushes → producers end → targets drop → cascade
//!       completes → store blocks flush and close
//! ```
//!
//! Resource accounting (bounded): per target a 256 KiB parser buffer; per shard the two
//! block buffers (`gc_ring`, `store_ring`); counter strings are rented from the EC
//! pool (`wolf-dotnet`); batch buffers keep steady-state capacity. Nothing in the data
//! path grows with uptime.

mod attach;
mod blocks;
mod dataflow;
mod error;
mod host;
mod placement;
mod producers;
mod stats;
mod telemetry;

pub use attach::{Attached, attach, host_name, target_id, telemetry_session_config};
pub use blocks::dotnet_gc::{DotnetGcBlock, GcAttach, GcIn, TargetFlags};
pub use blocks::process::{ProcessBlock, ProcessIn, ProcessTarget};
pub use blocks::store::{RetentionConfig, StoreBlock, StoreConfig, StoreOp};
pub use dataflow::{
    Block, BlockHandle, BlockOptions, Handler, Target, action_block, transform_block,
};
pub use error::{Error, Result};
pub use host::{HostConfig, HostReport, run};
pub use placement::{LOGICAL_SHARDS, Placement};
pub use producers::session::{SessionDecoder, SessionEnd, SessionItem, run_session_producer};
pub use producers::{SessionStats, SessionStatsSnapshot};
pub use stats::{ShardStats, ShardStatsSnapshot};
pub use telemetry::SelfTelemetry;
