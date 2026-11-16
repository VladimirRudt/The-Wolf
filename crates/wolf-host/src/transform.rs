//! The transform stage (Vector-style naming): the pure, I/O-free mapping from source
//! records to contract points and events. Everything mutable about a target's *routing*
//! lives here; persistence lives in the sink. Split so the mapping stays testable and the
//! pipeline thread never mixes "what does this record mean" with "how do I store it".

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use serde_json::{Map, Value, json};
use wolf_contract::{Event, Point, TargetId, Timestamp};
use wolf_dotnet::{ContractMapper as _, EcSeries, GcSeries};
use wolf_ipc::{ProcessInfo, SessionId};
use wolf_process::ProcessSeries;

use crate::pipeline::PipelineStats;
use crate::record::Record;
use crate::sources::session::{SessionEnd, SessionItem};
use crate::telemetry::SelfTelemetry;

const TARGET_ATTACHED: &str = "target.attached";
const TARGET_DETACHED: &str = "target.detached";
const SESSION_STARTED: &str = "session.started";
const SESSION_LOST: &str = "session.lost";

/// Lifecycle flags shared with the host and the sources. Kept in `Arc`s so the host
/// thread can mark a target detaching (stop_session) and observe detaches without
/// touching the pipeline thread.
#[derive(Debug, Clone)]
pub struct TargetFlags {
    /// Process source liveness: false stops sampling of this target.
    pub active: Arc<AtomicBool>,
    /// Set by the host when it asks the runtime to stop the session: a clean end is
    /// expected ("stopped", not "exited").
    pub detaching: Arc<AtomicBool>,
    /// Set by the transform when the session stream ends.
    pub detached: Arc<AtomicBool>,
}

/// Everything the transform knows about one target.
#[derive(Debug)]
struct TargetRouting {
    id: TargetId,
    gc: GcSeries,
    ec: EcSeries,
    process: ProcessSeries,
    flags: TargetFlags,
}

/// The pure mapping stage of a pipeline. See the module docs.
#[derive(Debug)]
pub struct Transform {
    targets: Vec<TargetRouting>,
    self_telemetry: Option<SelfTelemetry>,
    stats: Arc<PipelineStats>,
}

impl Transform {
    pub fn new(stats: Arc<PipelineStats>) -> Self {
        Self {
            targets: Vec::new(),
            self_telemetry: None,
            stats,
        }
    }

    /// The host's own `wolf.*` series (D11): kept here so a `SelfReport` record maps to
    /// points like any other telemetry.
    pub fn register_self(&mut self, telemetry: SelfTelemetry) {
        self.self_telemetry = Some(telemetry);
    }

    /// Starts routing a target; returns its slot (local to this pipeline) and the shared
    /// lifecycle flags. Storage registration is the caller's job (it owns the sink).
    pub fn add_target(
        &mut self,
        id: TargetId,
        gc: GcSeries,
        ec: EcSeries,
        process: ProcessSeries,
    ) -> (u32, TargetFlags) {
        let flags = TargetFlags {
            active: Arc::new(AtomicBool::new(true)),
            detaching: Arc::new(AtomicBool::new(false)),
            detached: Arc::new(AtomicBool::new(false)),
        };
        self.targets.push(TargetRouting {
            id,
            gc,
            ec,
            process,
            flags: flags.clone(),
        });
        (self.targets.len() as u32 - 1, flags)
    }

    /// `target.attached` with the process metadata (`docs/collector-dotnet.md` §2).
    pub fn target_attached(&self, slot: u32, info: &ProcessInfo, events: &mut Vec<Event>) {
        let routing = &self.targets[slot as usize];
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
        events.push(Event {
            ts: Timestamp::now(),
            target: routing.id.clone(),
            kind: TARGET_ATTACHED.to_owned(),
            data,
        });
    }

    /// `session.started`.
    pub fn session_started(&self, slot: u32, session_id: SessionId, events: &mut Vec<Event>) {
        let routing = &self.targets[slot as usize];
        events.push(Event {
            ts: Timestamp::now(),
            target: routing.id.clone(),
            kind: SESSION_STARTED.to_owned(),
            data: event_data([
                ("kind", json!("telemetry")),
                ("session_id", json!(session_id.to_string())),
            ]),
        });
    }

    /// Attach-time failure after registration: record `target.detached` and move on.
    pub fn detach_with_error(&self, slot: u32, reason: &str, events: &mut Vec<Event>) {
        let routing = &self.targets[slot as usize];
        routing.flags.detached.store(true, Ordering::Relaxed);
        routing.flags.active.store(false, Ordering::Relaxed);
        events.push(Event {
            ts: Timestamp::now(),
            target: routing.id.clone(),
            kind: TARGET_DETACHED.to_owned(),
            data: event_data([("reason", json!(reason))]),
        });
    }

    /// One record. Never fails: mapping problems are counted, not propagated.
    pub fn map(
        &mut self,
        slot: u32,
        record: Record,
        points: &mut Vec<Point>,
        events: &mut Vec<Event>,
    ) {
        match record {
            Record::SelfReport { ts, totals, rss } => {
                if let Some(telemetry) = &self.self_telemetry {
                    points.extend(telemetry.points(ts, &totals, rss));
                }
            }
            Record::Session(item) => {
                let Some(routing) = self.targets.get_mut(slot as usize) else {
                    return;
                };
                match item {
                    SessionItem::Telemetry(telemetry) => {
                        telemetry.to_events(&routing.id, &mut |event| events.push(event));
                        telemetry.to_points(routing.gc.ids(), &mut |point| points.push(point));
                    }
                    SessionItem::Counter { ts, counter } => match routing.ec.point(ts, &counter) {
                        Some(point) => points.push(point),
                        None => {
                            self.stats.counters_skipped.fetch_add(1, Ordering::Relaxed);
                        }
                    },
                    SessionItem::Ended(end) => {
                        routing.flags.detached.store(true, Ordering::Relaxed);
                        routing.flags.active.store(false, Ordering::Relaxed);
                        let ts = Timestamp::now();
                        match end {
                            SessionEnd::StreamClosed => {
                                let reason = if routing.flags.detaching.load(Ordering::Relaxed) {
                                    "stopped"
                                } else {
                                    "exited"
                                };
                                events.push(Event {
                                    ts,
                                    target: routing.id.clone(),
                                    kind: TARGET_DETACHED.to_owned(),
                                    data: event_data([("reason", json!(reason))]),
                                });
                            }
                            error => {
                                let reason = error.to_string();
                                events.push(Event {
                                    ts,
                                    target: routing.id.clone(),
                                    kind: SESSION_LOST.to_owned(),
                                    data: event_data([
                                        ("kind", json!("telemetry")),
                                        ("reason", json!(reason.clone())),
                                    ]),
                                });
                                events.push(Event {
                                    ts,
                                    target: routing.id.clone(),
                                    kind: TARGET_DETACHED.to_owned(),
                                    data: event_data([("reason", json!("error"))]),
                                });
                            }
                        }
                    }
                }
            }
            Record::Sample { ts, sample } => {
                let Some(routing) = self.targets.get_mut(slot as usize) else {
                    return;
                };
                points.extend(routing.process.points(ts, &sample));
            }
        }
    }
}

fn event_data(pairs: impl IntoIterator<Item = (&'static str, Value)>) -> Map<String, Value> {
    Map::from_iter(
        pairs
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value)),
    )
}
