//! The pipeline: one shard's drain → transform → sink loop (D14). A pipeline owns its
//! targets exclusively and is the single writer of its shard's store; `N` pipelines run
//! on `N` threads and never share mutable state. This is the unit of scalability: more
//! load = more pipelines, and the fixed logical shard map ([`crate::placement`]) decides
//! which pipeline a target belongs to.
//!
//! The loop also runs the shard's retention on its own cadence — the store is the
//! pipeline's alone, so housekeeping never blocks another shard.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use wolf_contract::{TargetId, Timestamp};
use wolf_dotnet::{EcSeries, GcSeries};
use wolf_ipc::{ProcessInfo, SessionId};
use wolf_process::ProcessSeries;
use wolf_storage::Storage;

use crate::channel::{Consumer, Wakeup};
use crate::record::{Message, Record};
use crate::sink::Sink;
use crate::telemetry::SelfTelemetry;
use crate::transform::{TargetFlags, Transform};

/// Pipeline-side counters (self-telemetry, D11), shared with the host thread.
#[derive(Debug, Default)]
pub struct PipelineStats {
    pub points_written: AtomicU64,
    pub events_written: AtomicU64,
    pub batches: AtomicU64,
    /// Counter samples the catalogue doesn't track, or with broken layout/units.
    pub counters_skipped: AtomicU64,
}

impl PipelineStats {
    /// A consistent-enough copy for reporting (Relaxed; updated once per flush).
    pub fn snapshot(&self) -> PipelineStatsSnapshot {
        PipelineStatsSnapshot {
            points_written: self.points_written.load(Ordering::Relaxed),
            events_written: self.events_written.load(Ordering::Relaxed),
            batches: self.batches.load(Ordering::Relaxed),
            counters_skipped: self.counters_skipped.load(Ordering::Relaxed),
        }
    }
}

/// Plain copy of [`PipelineStats`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PipelineStatsSnapshot {
    pub points_written: u64,
    pub events_written: u64,
    pub batches: u64,
    pub counters_skipped: u64,
}

impl PipelineStatsSnapshot {
    /// Field-by-field sum over any number of pipelines.
    pub fn total(snapshots: impl Iterator<Item = Self>) -> Self {
        let mut total = Self::default();
        for s in snapshots {
            total.points_written += s.points_written;
            total.events_written += s.events_written;
            total.batches += s.batches;
            total.counters_skipped += s.counters_skipped;
        }
        total
    }
}

/// What the host keeps of a running pipeline: its alarm clock, its stop flag, its stats.
#[derive(Debug, Clone)]
pub struct PipelineHandle {
    pub wakeup: Arc<Wakeup>,
    pub stop: Arc<AtomicBool>,
    pub stats: Arc<PipelineStats>,
}

/// How often the pipeline flushes and runs retention.
#[derive(Debug, Clone, Copy)]
pub struct PipelineConfig {
    /// Batches land in storage this often.
    pub flush_interval: Duration,
    /// Full-resolution window and how often the retention job runs.
    pub retention: Option<RetentionConfig>,
}

/// Raw points older than `window` are downsampled and their segment files dropped, every
/// `every` (`docs/architecture.md` §4.1).
#[derive(Debug, Clone, Copy)]
pub struct RetentionConfig {
    pub window: Duration,
    pub every: Duration,
}

/// One shard's actor: channels in, batches out. Generic over the backend so tests use an
/// in-memory store.
#[derive(Debug)]
pub struct Pipeline<S: Storage> {
    transform: Transform,
    sink: Sink<S>,
    channels: Vec<Consumer<Message>>,
    wakeup: Arc<Wakeup>,
    stop: Arc<AtomicBool>,
    stats: Arc<PipelineStats>,
    flush_interval: Duration,
    retention: Option<RetentionConfig>,
    /// Staging for drained records, reused across drains (kept out of the drain loop for
    /// borrow reasons; `mem::take` moves it, never reallocates in steady state).
    staging: Vec<Message>,
}

impl<S: Storage> Pipeline<S> {
    pub fn new(store: S, wakeup: Arc<Wakeup>, config: PipelineConfig) -> Self {
        let stats = Arc::new(PipelineStats::default());
        Self {
            transform: Transform::new(Arc::clone(&stats)),
            sink: Sink::new(store, Arc::clone(&stats)),
            channels: Vec::new(),
            wakeup,
            stop: Arc::new(AtomicBool::new(false)),
            stats,
            flush_interval: config.flush_interval,
            retention: config.retention,
            staging: Vec::new(),
        }
    }

    /// The handles the host needs after the pipeline moves to its own thread.
    pub fn handle(&self) -> PipelineHandle {
        PipelineHandle {
            wakeup: Arc::clone(&self.wakeup),
            stop: Arc::clone(&self.stop),
            stats: Arc::clone(&self.stats),
        }
    }

    /// A source's channel the pipeline should drain. All channels are registered before
    /// the pipeline thread starts.
    pub fn add_channel(&mut self, consumer: Consumer<Message>) {
        self.channels.push(consumer);
    }

    /// Registers a target and all its series in the store and starts routing it; returns
    /// the slot sources tag records with, plus the shared lifecycle flags.
    pub fn attach(
        &mut self,
        id: TargetId,
        first_seen: Timestamp,
        gc: GcSeries,
        ec: EcSeries,
        process: ProcessSeries,
    ) -> crate::Result<(u32, TargetFlags)> {
        self.sink
            .store_mut()
            .register_target(id.clone(), first_seen)?;
        let mut descriptors = Vec::with_capacity(
            gc.descriptors().len() + ec.descriptors().len() + process.descriptors().len(),
        );
        descriptors.extend_from_slice(gc.descriptors());
        descriptors.extend_from_slice(ec.descriptors());
        descriptors.extend_from_slice(process.descriptors());
        // One call: the store prepares the INSERT once for the whole batch.
        self.sink.store_mut().register_series(&descriptors)?;
        Ok(self.transform.add_target(id, gc, ec, process))
    }

    /// The host's own `wolf.*` series (D11): registered like any target.
    pub fn register_self(
        &mut self,
        telemetry: SelfTelemetry,
        first_seen: Timestamp,
    ) -> crate::Result<()> {
        self.sink
            .store_mut()
            .register_target(telemetry.target().clone(), first_seen)?;
        self.sink
            .store_mut()
            .register_series(telemetry.descriptors())?;
        self.transform.register_self(telemetry);
        Ok(())
    }

    /// `target.attached` with the process metadata (`docs/collector-dotnet.md` §2).
    pub fn target_attached(&mut self, slot: u32, info: &ProcessInfo) {
        let (_, events) = self.sink.buffers();
        self.transform.target_attached(slot, info, events);
    }

    /// `session.started`.
    pub fn session_started(&mut self, slot: u32, session_id: SessionId) {
        let (_, events) = self.sink.buffers();
        self.transform.session_started(slot, session_id, events);
    }

    /// Attach-time failure after registration: record `target.detached` and move on.
    pub fn detach_with_error(&mut self, slot: u32, reason: &str) {
        let (_, events) = self.sink.buffers();
        self.transform.detach_with_error(slot, reason, events);
    }

    /// One record. Never fails: mapping problems are counted, not propagated.
    pub fn on_record(&mut self, slot: u32, record: Record) {
        let (points, events) = self.sink.buffers();
        self.transform.map(slot, record, points, events);
    }

    /// Drains every channel once; returns how many records arrived.
    pub fn drain(&mut self) -> usize {
        let mut drained = 0;
        {
            let Self {
                channels, staging, ..
            } = self;
            for channel in channels {
                drained += channel.drain(&mut |message| staging.push(message));
            }
        }
        // Take the staging buffer out so `on_record` can borrow self; put the (emptied)
        // allocation back afterwards — no per-drain allocation in steady state.
        let mut messages = std::mem::take(&mut self.staging);
        for (slot, record) in messages.drain(..) {
            self.on_record(slot, record);
        }
        self.staging = messages;
        drained
    }

    /// Sleeps until any producer posts to an empty channel, at most `timeout`.
    pub fn wait(&self, timeout: Duration) {
        self.wakeup
            .wait_if(timeout, || self.channels.iter().all(Consumer::is_empty));
    }

    /// One flush: both batches to storage.
    pub fn flush(&mut self) -> crate::Result<()> {
        self.sink.flush()
    }

    pub fn store(&self) -> &S {
        self.sink.store()
    }

    pub fn stats(&self) -> &PipelineStats {
        &self.stats
    }

    /// The pipeline's loop: drain, flush and retain on cadence, until the host sets the
    /// stop flag and every channel is empty. A storage failure ends the loop — the
    /// shard's purpose is gone — after a short unwind drain so producers blocked on full
    /// rings can finish their last sends (a wedged producer would hang the host's join).
    pub fn run(mut self) -> crate::Result<()> {
        let mut next_flush = Instant::now() + self.flush_interval;
        let mut next_retain = self.retention.map(|r| Instant::now() + r.every);
        let result = loop {
            self.drain();
            let now = Instant::now();
            if now >= next_flush {
                if let Err(error) = self.flush() {
                    break Err(error);
                }
                next_flush = now + self.flush_interval;
            }
            if let Some(retention) = &self.retention
                && next_retain.is_some_and(|next| now >= next)
            {
                let cutoff = Timestamp::from_nanos(
                    Timestamp::now().as_nanos()
                        - i64::try_from(retention.window.as_nanos()).unwrap_or(i64::MAX),
                );
                match self.sink.store_mut().retain(cutoff) {
                    Ok(done) if done.points_deleted > 0 => println!(
                        "mrwolf: retention: {} minutes downsampled, {} raw points dropped with their segments",
                        done.minutes_downsampled, done.points_deleted
                    ),
                    Ok(_) => {}
                    Err(error) => eprintln!("mrwolf: retention failed: {error}"),
                }
                next_retain = Some(now + retention.every);
            }
            if self.stop.load(Ordering::Relaxed) && self.channels.iter().all(Consumer::is_empty) {
                // The host sets the stop flag only after all sources have finished, so
                // this final flush loses nothing.
                break self.flush();
            }
            self.wait(Duration::from_millis(100));
        };
        if result.is_err() {
            self.drain_until_quiet();
        }
        result
    }

    /// After a fatal error: keep draining until the channels stay empty for a moment
    /// (deadline-bounded), so sources finishing their last sends never wedge.
    fn drain_until_quiet(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            self.drain();
            if self.channels.iter().all(Consumer::is_empty) {
                std::thread::sleep(Duration::from_millis(100));
                if self.channels.iter().all(Consumer::is_empty) {
                    break;
                }
            }
        }
    }
}
