//! Mr. Wolf collector contract: series, points, events, commands, artifacts.
//!
//! The contract is collector-agnostic (`docs/architecture.md` §3): it defines generic
//! concepts only. Collector-specific names — metrics, event kinds, command names such as
//! `heap_snapshot` — are string constants owned by each collector crate.
//!
//! Hot path: a [`Point`] is three numbers. Strings live in the [`SeriesDescriptor`],
//! sent once per series.
//!
//! Forward compatibility: unknown fields are ignored and unknown units deserialize
//! into [`Unit::Other`] instead of failing.

mod command;
mod fnv;
mod target;
mod telemetry;

pub use command::{ArtifactRef, CollectorInfo, Command, CommandId, CommandResult, CommandStatus};
pub use target::{ParseTargetIdError, TargetId};
pub use telemetry::{Event, Point, SeriesDescriptor, SeriesId, Timestamp, Unit};

/// Version of the schema defined by this crate. Bump on any breaking change; any future
/// wire format or stored copy of these types must carry it.
pub const SCHEMA_VERSION: u32 = 1;
