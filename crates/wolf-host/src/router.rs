//! The router actor: the single writer (D13). It drains every producer's ring mailbox,
//! maps messages to contract points and events, batches them in reused buffers, and
//! flushes to storage on the host's cadence. Everything mutable about a target lives here
//! and nowhere else.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use serde_json::{Map, Value, json};
use wolf_contract::{Event, Point, TargetId, Timestamp};
use wolf_dotnet::{ContractMapper as _, EcSeries, GcSeries};
use wolf_ipc::{ProcessInfo, SessionId};
use wolf_process::ProcessSeries;
use wolf_storage::Storage;

use crate::mailbox::{Consumer, Wakeup};
use crate::messages::{Input, Message};
use crate::session::{ReaderStatsSnapshot, SessionEnd, SessionItem};
use crate::telemetry::SelfTelemetry;

const TARGET_ATTACHED: &str = "target.attached";
const TARGET_DETACHED: &str = "target.detached";
const SESSION_STARTED: &str = "session.started";
const SESSION_LOST: &str = "session.lost";

/// Router-side counters (self-telemetry, D11).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RouterStats {
    pub points_written: u64,
    pub events_written: u64,
    pub batches: u64,
    /// Counter samples the catalogue doesn't track, or with broken layout/units.
    pub counters_skipped: u64,
}

/// Everything the router knows about one target.
#[derive(Debug)]
struct TargetRouting {
    id: TargetId,
    gc: GcSeries,
    ec: EcSeries,
    process: ProcessSeries,
    /// Shared with the sampler: false stops process sampling of this target.
    active: Arc<AtomicBool>,
    /// The host asked the runtime to stop the session: a clean end is expected.
    detaching: bool,
    detached: bool,
}

/// The single-writer actor. Generic over the backend so tests use an in-memory store.
#[derive(Debug)]
pub struct Router<S: Storage> {
    store: S,
    targets: Vec<TargetRouting>,
    mailboxes: Vec<Consumer<Message>>,
    wakeup: Arc<Wakeup>,
    /// Batch buffers: cleared after every flush, so they hold steady-state capacity.
    points: Vec<Point>,
    events: Vec<Event>,
    /// Staging for drained messages, reused across drains (kept out of the drain loop for
    /// borrow reasons; `mem::take` moves it, never reallocates in steady state).
    staging: Vec<Message>,
    self_telemetry: Option<SelfTelemetry>,
    stats: RouterStats,
}

impl<S: Storage> Router<S> {
    pub fn new(store: S, wakeup: Arc<Wakeup>) -> Self {
        Self {
            store,
            targets: Vec::new(),
            mailboxes: Vec::new(),
            wakeup,
            points: Vec::new(),
            events: Vec::new(),
            staging: Vec::new(),
            self_telemetry: None,
            stats: RouterStats::default(),
        }
    }

    pub fn store_mut(&mut self) -> &mut S {
        &mut self.store
    }

    pub fn store(&self) -> &S {
        &self.store
    }

    pub fn stats(&self) -> &RouterStats {
        &self.stats
    }

    /// A producer's mailbox the router should drain.
    pub fn add_mailbox(&mut self, consumer: Consumer<Message>) {
        self.mailboxes.push(consumer);
    }

    /// The host's own `wolf.*` series (D11): registered like any target.
    pub fn register_self(
        &mut self,
        telemetry: SelfTelemetry,
        first_seen: Timestamp,
    ) -> crate::Result<()> {
        self.store
            .register_target(telemetry.target().clone(), first_seen)?;
        self.store.register_series(telemetry.descriptors())?;
        self.self_telemetry = Some(telemetry);
        Ok(())
    }

    /// Registers a target and all its series; returns the slot producers tag messages with.
    pub fn add_target(
        &mut self,
        id: TargetId,
        first_seen: Timestamp,
        gc: GcSeries,
        ec: EcSeries,
        process: ProcessSeries,
    ) -> crate::Result<u32> {
        self.store.register_target(id.clone(), first_seen)?;
        let mut descriptors = Vec::with_capacity(
            gc.descriptors().len() + ec.descriptors().len() + process.descriptors().len(),
        );
        descriptors.extend_from_slice(gc.descriptors());
        descriptors.extend_from_slice(ec.descriptors());
        descriptors.extend_from_slice(process.descriptors());
        // One call: the store prepares the INSERT once for the whole batch.
        self.store.register_series(&descriptors)?;
        self.targets.push(TargetRouting {
            id,
            gc,
            ec,
            process,
            active: Arc::new(AtomicBool::new(true)),
            detaching: false,
            detached: false,
        });
        Ok(self.targets.len() as u32 - 1)
    }

    /// The sampler's liveness flag for a slot (shared with the sampler actor).
    pub fn active_flag(&self, slot: u32) -> Arc<AtomicBool> {
        Arc::clone(&self.targets[slot as usize].active)
    }

    /// `target.attached` with the process metadata (`docs/collector-dotnet.md` §2).
    pub fn target_attached(&mut self, slot: u32, info: &ProcessInfo) {
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
        self.events.push(Event {
            ts: Timestamp::now(),
            target: routing.id.clone(),
            kind: TARGET_ATTACHED.to_owned(),
            data,
        });
    }

    /// `session.started`.
    pub fn session_started(&mut self, slot: u32, session_id: SessionId) {
        let routing = &self.targets[slot as usize];
        self.events.push(Event {
            ts: Timestamp::now(),
            target: routing.id.clone(),
            kind: SESSION_STARTED.to_owned(),
            data: event_data([
                ("kind", json!("telemetry")),
                ("session_id", json!(session_id.to_string())),
            ]),
        });
    }

    /// The host is stopping this session: a clean stream end is expected now.
    pub fn begin_detach(&mut self, slot: u32) {
        if let Some(routing) = self.targets.get_mut(slot as usize) {
            routing.detaching = true;
            routing.active.store(false, Ordering::Relaxed);
        }
    }

    /// Attach-time failure after registration: record `target.detached` and move on.
    pub fn detach_with_error(&mut self, slot: u32, reason: &str) {
        let routing = &mut self.targets[slot as usize];
        routing.detached = true;
        routing.active.store(false, Ordering::Relaxed);
        let event = Event {
            ts: Timestamp::now(),
            target: routing.id.clone(),
            kind: TARGET_DETACHED.to_owned(),
            data: event_data([("reason", json!(reason))]),
        };
        self.events.push(event);
    }

    pub fn is_detached(&self, slot: u32) -> bool {
        self.targets
            .get(slot as usize)
            .is_none_or(|routing| routing.detached)
    }

    pub fn detached_count(&self) -> usize {
        self.targets
            .iter()
            .filter(|routing| routing.detached)
            .count()
    }

    /// One message. Never fails: mapping problems are counted, not propagated.
    pub fn on_message(&mut self, slot: u32, input: Input) {
        let Self {
            targets,
            points,
            events,
            stats,
            ..
        } = self;
        let Some(routing) = targets.get_mut(slot as usize) else {
            return;
        };
        match input {
            Input::Session(SessionItem::Telemetry(telemetry)) => {
                telemetry.to_events(&routing.id, &mut |event| events.push(event));
                telemetry.to_points(routing.gc.ids(), &mut |point| points.push(point));
            }
            Input::Session(SessionItem::Counter { ts, counter }) => {
                match routing.ec.point(ts, &counter) {
                    Some(point) => points.push(point),
                    None => stats.counters_skipped += 1,
                }
            }
            Input::Session(SessionItem::Ended(end)) => {
                routing.detached = true;
                routing.active.store(false, Ordering::Relaxed);
                let ts = Timestamp::now();
                match end {
                    SessionEnd::StreamClosed => {
                        let reason = if routing.detaching {
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
            Input::Sample { ts, sample } => {
                points.extend(routing.process.points(ts, &sample));
            }
        }
    }

    /// Drains every mailbox once; returns how many messages arrived.
    pub fn drain(&mut self) -> usize {
        let mut drained = 0;
        {
            let Self {
                mailboxes, staging, ..
            } = self;
            for mailbox in mailboxes {
                drained += mailbox.drain(&mut |message| staging.push(message));
            }
        }
        // Take the staging buffer out so `on_message` can borrow self; put the (emptied)
        // allocation back afterwards — no per-drain allocation in steady state.
        let mut messages = std::mem::take(&mut self.staging);
        for (slot, input) in messages.drain(..) {
            self.on_message(slot, input);
        }
        self.staging = messages;
        drained
    }

    /// Sleeps until any producer posts to an empty mailbox, at most `timeout`.
    pub fn wait(&self, timeout: Duration) {
        self.wakeup
            .wait_if(timeout, || self.mailboxes.iter().all(Consumer::is_empty));
    }

    /// One flush: both batches to storage. The buffers keep their capacity.
    pub fn flush(&mut self) -> crate::Result<()> {
        if !self.points.is_empty() {
            self.store.insert_points(&self.points)?;
            self.stats.points_written += self.points.len() as u64;
            self.points.clear();
        }
        if !self.events.is_empty() {
            self.store.insert_events(&self.events)?;
            self.stats.events_written += self.events.len() as u64;
            self.events.clear();
        }
        self.stats.batches += 1;
        Ok(())
    }

    /// The host's `wolf.*` points for one tick (D11): reader totals plus own RSS.
    pub fn self_report(&mut self, ts: Timestamp, totals: &ReaderStatsSnapshot) {
        let rss = wolf_process::sample(std::process::id())
            .map(|sample| sample.rss)
            .unwrap_or(0);
        if let Some(telemetry) = &self.self_telemetry {
            let points = telemetry.points(ts, totals, rss);
            self.points.extend(points);
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
