//! .NET collector: discovery, EventPipe sessions, GC telemetry, heap snapshot capture.
//!
//! GC telemetry (`docs/collector-dotnet.md` §3.2), on the session's reader thread:
//!
//! ```text
//! wolf_nettrace::Event → GcTracker → DotnetTelemetry → (session channel) → to_contract → storage
//! ```
//!
//! [`GcCorrelator`] groups the runtime's GC events into one [`GcCompleted`] per GC;
//! [`GcTracker`] adds the gen0 summary; [`to_contract`] maps the result to the metric and
//! event names of the catalogue (§4).

mod contract;
mod correlator;
mod gc;
mod gc_events;
mod gen_zero_summary;

pub use contract::{GcSeries, ContractMapper};
pub use correlator::{CorrelatorStats, GcCorrelator};
pub use gc::{GENERATIONS, GcCompleted, GcKind, GcReason, HeapStats};
pub use gen_zero_summary::{Gen0Summary};
