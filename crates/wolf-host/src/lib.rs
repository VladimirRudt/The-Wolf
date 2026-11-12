//! The collector host (P1-8): attaches to .NET processes, runs their telemetry sessions,
//! and routes points and events to storage (`docs/collector-dotnet.md` §2, `docs/architecture.md` §4).
//!
//! Actor model — framework-free per D13 (hand-written, plain threads, blocking std I/O in
//! the data path; tokio only for request/response commands):
//!
//! ```text
//! per target: reader actor (EventSession → SessionDecoder)
//!       │  ring mailbox: a preallocated circular buffer (crossbeam ArrayQueue),
//!       │  no allocation per message; a full ring is the backpressure point —
//!       │  the pipe fills, the runtime drops, the parser counts the sequence gaps
//!       ▼
//! router actor (the single writer): batches in reused Vecs → wolf_storage once a second
//!       ▲
//! sampler actor: OS memory of every target on a 1 s cadence (threads count every 60th)
//!
//! main: attach, tokio driver (ctrl-c → flag), graceful detach:
//!       stop_session → runtime flushes → readers see end of stream → drain, flush, join
//! ```
//!
//! Resource accounting (bounded): per target a 256 KiB parser buffer plus one ring
//! (`reader_ring` × ~150 B ≈ 300 KiB at the default); counter strings are rented from the
//! EC pool (`wolf-dotnet`); batch and staging buffers keep steady-state capacity. Nothing
//! in the data path grows with uptime.

mod attach;
mod error;
mod host;
mod mailbox;
mod messages;
mod router;
mod sampler;
mod session;
mod telemetry;

pub use attach::{Attached, attach, host_name, target_id, telemetry_session_config};
pub use error::{Error, Result};
pub use host::{HostConfig, HostReport, RetentionConfig, run};
pub use mailbox::{Consumer, Producer, Wakeup, mailbox};
pub use messages::{Input, Message};
pub use router::{Router, RouterStats};
pub use sampler::{Sampler, SamplerTarget};
pub use session::{
    ReaderStats, ReaderStatsSnapshot, SessionDecoder, SessionEnd, SessionItem, run_reader,
};
pub use telemetry::SelfTelemetry;
