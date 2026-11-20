//! The domain blocks (D15): one block per subsystem, each with its own input message
//! type, wired per shard in `host.rs`. There is no general router — a block does one
//! subsystem's work and nothing else:
//!
//! - [`dotnet_gc`]: the .NET diagnostic session — GC telemetry, EventCounters, target
//!   and session lifecycle.
//! - [`process`]: OS process memory — watches its attached targets and samples them on
//!   its tick, no separate producer thread.
//! - [`store`]: the shard's single writer — batches points and events, flushes and
//!   retains on cadence.
//!
//! The dotnet-gc and process blocks both `link_to` the shard's store block; the host
//! posts its own `wolf.*` telemetry into the store block directly.

pub mod dotnet_gc;
pub mod process;
pub mod store;
