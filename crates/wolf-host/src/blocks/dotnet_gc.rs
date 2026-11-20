//! The dotnet-gc block's handler: everything that belongs to a target's .NET diagnostic
//! session — GC telemetry and EventCounters become points, session and target lifecycle
//! become events, attaching a target registers its series. Everything mutable about a
//! target's *routing* lives here; persistence lives in the store block downstream. The
//! block thread, channels and completion come from `dataflow` — this struct is workflow
//! only.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use serde_json::{Map, Value, json};
use wolf_contract::{TargetId, Timestamp};
use wolf_dotnet::{ContractMapper as _, EcSeries, GcSeries};
use wolf_ipc::{ProcessInfo, SessionId};

use crate::blocks::store::StoreOp;
use crate::dataflow::Handler;
use crate::producers::session::{SessionEnd, SessionItem};
use crate::stats::ShardStats;

const TARGET_ATTACHED: &str = "target.attached";
const TARGET_DETACHED: &str = "target.detached";
const SESSION_STARTED: &str = "session.started";
const SESSION_LOST: &str = "session.lost";

/// The block's input: the control plane rides the same buffer as the data (TPL Dataflow
/// style) — attaching a target is a message, not a method call. Slots are assigned in
/// message order; the host posts every `Attach` for a shard itself, so it knows the slot
/// a target will get before any of that target's producers start (they are spawned after
/// the `Attach` post, and per-producer posts are FIFO).
#[derive(Debug)]
pub enum GcIn {
    /// Register and start routing a new target.
    Attach(Box<GcAttach>),
    /// Attach-time failure after the `Attach` message (e.g. the session failed to
    /// start): record `target.detached` and stop routing the slot.
    AttachFailed { slot: u32, reason: String },
    /// `session.started`.
    Started { slot: u32, session_id: SessionId },
    /// From a session producer.
    Item { slot: u32, item: SessionItem },
}

/// Everything needed to register and route a new target.
#[derive(Debug)]
pub struct GcAttach {
    pub id: TargetId,
    pub first_seen: Timestamp,
    pub info: ProcessInfo,
    pub gc: GcSeries,
    pub ec: EcSeries,
    /// Lifecycle flags created by the host and shared with the block, so stop and
    /// liveness signals work without reaching into a running block.
    pub flags: TargetFlags,
}

/// Lifecycle flags shared between the host, the dotnet-gc block and the process block.
/// Created by the host, carried in the [`GcAttach`], so stop and liveness signals work
/// without reaching into a running block.
#[derive(Debug, Clone)]
pub struct TargetFlags {
    /// Process liveness: false stops the process block's sampling of this target.
    pub active: Arc<AtomicBool>,
    /// Set by the host when it asks the runtime to stop the session: a clean end is
    /// expected ("stopped", not "exited").
    pub detaching: Arc<AtomicBool>,
    /// Set by the dotnet-gc block when the session stream ends.
    pub detached: Arc<AtomicBool>,
}

impl TargetFlags {
    pub fn new() -> Self {
        Self {
            active: Arc::new(AtomicBool::new(true)),
            detaching: Arc::new(AtomicBool::new(false)),
            detached: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl Default for TargetFlags {
    fn default() -> Self {
        Self::new()
    }
}

/// Everything the block knows about one target.
#[derive(Debug)]
struct GcTarget {
    id: TargetId,
    gc: GcSeries,
    ec: EcSeries,
    flags: TargetFlags,
}

/// The .NET session block of one shard. See the module docs.
#[derive(Debug)]
pub struct DotnetGcBlock {
    targets: Vec<GcTarget>,
    stats: Arc<ShardStats>,
}

impl DotnetGcBlock {
    pub fn new(stats: Arc<ShardStats>) -> Self {
        Self {
            targets: Vec::new(),
            stats,
        }
    }

    fn event(&self, slot: u32, kind: &str, data: Map<String, Value>) -> StoreOp {
        StoreOp::Event(wolf_contract::Event {
            ts: Timestamp::now(),
            target: self.targets[slot as usize].id.clone(),
            kind: kind.to_owned(),
            data,
        })
    }
}

impl Handler<GcIn, StoreOp> for DotnetGcBlock {
    fn handle(&mut self, input: GcIn, emit: &mut dyn FnMut(StoreOp)) {
        match input {
            GcIn::Attach(attach) => {
                let GcAttach {
                    id,
                    first_seen,
                    info,
                    gc,
                    ec,
                    flags,
                } = *attach;
                let mut descriptors =
                    Vec::with_capacity(gc.descriptors().len() + ec.descriptors().len());
                descriptors.extend_from_slice(gc.descriptors());
                descriptors.extend_from_slice(ec.descriptors());
                self.targets.push(GcTarget { id, gc, ec, flags });
                let slot = self.targets.len() as u32 - 1;
                emit(StoreOp::Register {
                    target: self.targets[slot as usize].id.clone(),
                    first_seen,
                    descriptors: descriptors.into_boxed_slice(),
                });
                emit(self.target_attached_event(slot, &info));
            }
            GcIn::AttachFailed { slot, reason } => {
                let target = &self.targets[slot as usize];
                target.flags.detached.store(true, Ordering::Relaxed);
                target.flags.active.store(false, Ordering::Relaxed);
                emit(self.event(
                    slot,
                    TARGET_DETACHED,
                    event_data([("reason", json!(reason))]),
                ));
            }
            GcIn::Started { slot, session_id } => {
                emit(self.event(
                    slot,
                    SESSION_STARTED,
                    event_data([
                        ("kind", json!("telemetry")),
                        ("session_id", json!(session_id.to_string())),
                    ]),
                ));
            }
            GcIn::Item { slot, item } => {
                let Some(target) = self.targets.get_mut(slot as usize) else {
                    return;
                };
                match item {
                    SessionItem::Telemetry(telemetry) => {
                        telemetry.to_events(&target.id, &mut |event| emit(StoreOp::Event(event)));
                        telemetry.to_points(target.gc.ids(), &mut |point| {
                            emit(StoreOp::Point(point));
                        });
                    }
                    SessionItem::Counter { ts, counter } => match target.ec.point(ts, &counter) {
                        Some(point) => emit(StoreOp::Point(point)),
                        None => {
                            self.stats.counters_skipped.fetch_add(1, Ordering::Relaxed);
                        }
                    },
                    SessionItem::Ended(end) => {
                        target.flags.detached.store(true, Ordering::Relaxed);
                        target.flags.active.store(false, Ordering::Relaxed);
                        match end {
                            SessionEnd::StreamClosed => {
                                let reason = if target.flags.detaching.load(Ordering::Relaxed) {
                                    "stopped"
                                } else {
                                    "exited"
                                };
                                emit(self.event(
                                    slot,
                                    TARGET_DETACHED,
                                    event_data([("reason", json!(reason))]),
                                ));
                            }
                            error => {
                                let reason = error.to_string();
                                emit(self.event(
                                    slot,
                                    SESSION_LOST,
                                    event_data([
                                        ("kind", json!("telemetry")),
                                        ("reason", json!(reason)),
                                    ]),
                                ));
                                emit(self.event(
                                    slot,
                                    TARGET_DETACHED,
                                    event_data([("reason", json!("error"))]),
                                ));
                            }
                        }
                    }
                }
            }
        }
    }
}

impl DotnetGcBlock {
    fn target_attached_event(&self, slot: u32, info: &ProcessInfo) -> StoreOp {
        let mut data = event_data([
            ("command_line", json!(info.command_line)),
            ("os", json!(info.os)),
            ("arch", json!(info.arch)),
        ]);
        if let Some(version) = &info.clr_product_version {
            data.insert("clr_version".into(), json!(version));
        }
        if let Some(entrypoint) = &info.entrypoint_assembly {
            data.insert("entrypoint".into(), json!(entrypoint));
        }
        self.event(slot, TARGET_ATTACHED, data)
    }
}

fn event_data(pairs: impl IntoIterator<Item = (&'static str, Value)>) -> Map<String, Value> {
    Map::from_iter(
        pairs
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value)),
    )
}
