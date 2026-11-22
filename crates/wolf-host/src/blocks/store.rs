//! The store block's handler: the single writer of one shard's store files. It owns the
//! batch buffers — cleared after every flush, so they hold steady-state capacity — plus
//! the flush and retention cadences. The block thread, channels and completion come from
//! `dataflow`; this struct is workflow only.

use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::sync::mpsc::Sender;
use std::time::{Duration, Instant};

use wolf_contract::{Event, Point, SeriesDescriptor, SeriesId, TargetId, Timestamp};
use wolf_storage::{Storage, TargetInfo};

use crate::dataflow::Handler;
use crate::stats::ShardStats;

/// What the upstream blocks emit downstream: storage operations for the store block.
#[derive(Debug)]
pub enum StoreOp {
    /// Register a target and its series descriptors (target first — `series.target`
    /// references `targets.id`). Idempotent on both halves, so several blocks may
    /// register the same target in any order.
    Register {
        target: TargetId,
        first_seen: Timestamp,
        descriptors: Box<[SeriesDescriptor]>,
    },
    Point(Point),
    Event(Event),
    /// A read the store block runs against its own store: the block is the single owner
    /// of the shard's files, so it is the only safe reader while the host runs (the
    /// `mrwolf watch` storage panel posts one of these to every shard and merges the
    /// replies). Reads see flushed batches only, not the block's pending buffers.
    Query(Query),
}

/// One storage read plus the channel for its answer.
#[derive(Debug)]
pub struct Query {
    pub op: QueryOp,
    pub reply: Sender<QueryOutcome>,
}

/// The reads the watch view can ask a shard for.
#[derive(Debug, Clone)]
pub enum QueryOp {
    /// Every target registered in the shard.
    Targets,
    /// Events of one target in the range.
    Events {
        target: TargetId,
        range: Range<Timestamp>,
    },
    /// Points of one series in the range.
    Points {
        series: SeriesId,
        range: Range<Timestamp>,
    },
}

/// One shard's answer; errors travel as text — the view only displays them.
pub type QueryOutcome = Result<QueryReply, String>;

/// The data side of a [`QueryOutcome`].
#[derive(Debug)]
pub enum QueryReply {
    Targets(Vec<TargetInfo>),
    Events(Vec<Event>),
    Points(Vec<Point>),
}

impl Query {
    /// Runs the read against the block's store and sends the outcome. Called on the
    /// store block's thread, between data ops (FIFO): a query never reorders writes.
    fn answer(self, store: &impl Storage) {
        let outcome = match &self.op {
            QueryOp::Targets => store.targets().map(QueryReply::Targets),
            QueryOp::Events { target, range } => {
                store.events(target, range.clone()).map(QueryReply::Events)
            }
            QueryOp::Points { series, range } => {
                store.points(*series, range.clone()).map(QueryReply::Points)
            }
        };
        let _ = self.reply.send(outcome.map_err(|error| error.to_string()));
    }
}

/// Cadence configuration for the store block.
#[derive(Debug, Clone, Copy)]
pub struct StoreConfig {
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

/// Terminal block of one shard: storage operations in, batches out. Generic over the
/// backend so tests use an in-memory store.
#[derive(Debug)]
pub struct StoreBlock<S: Storage> {
    store: S,
    points: Vec<Point>,
    events: Vec<Event>,
    stats: Arc<ShardStats>,
    config: StoreConfig,
    next_flush: Instant,
    next_retain: Option<Instant>,
    /// The `mrwolf watch` tap (P1-9): sees every op this block handles. The store block
    /// is the graph's fan-in point, so one tap covers all of a target's records.
    watch: Option<Arc<crate::watch::Watch>>,
    /// The first fatal (storage) error; set once, read by the host after completion.
    error: Option<crate::Error>,
}

impl<S: Storage> StoreBlock<S> {
    pub fn new(store: S, stats: Arc<ShardStats>, config: StoreConfig) -> Self {
        Self {
            store,
            points: Vec::new(),
            events: Vec::new(),
            stats,
            config,
            next_flush: Instant::now() + config.flush_interval,
            next_retain: config.retention.map(|r| Instant::now() + r.every),
            watch: None,
            error: None,
        }
    }

    /// Sets the watch tap (none by default, so tests and non-watch runs pay nothing).
    pub fn with_watch(mut self, watch: Option<Arc<crate::watch::Watch>>) -> Self {
        self.watch = watch;
        self
    }

    pub fn store(&self) -> &S {
        &self.store
    }

    /// The fatal error that faulted the block, if any.
    pub fn take_error(&mut self) -> Option<crate::Error> {
        self.error.take()
    }

    /// One flush: both batches to storage. The buffers keep their capacity.
    fn flush(&mut self) -> crate::Result<()> {
        if !self.points.is_empty() {
            self.store.insert_points(&self.points)?;
            self.stats
                .points_written
                .fetch_add(self.points.len() as u64, Ordering::Relaxed);
            self.points.clear();
        }
        if !self.events.is_empty() {
            self.store.insert_events(&self.events)?;
            self.stats
                .events_written
                .fetch_add(self.events.len() as u64, Ordering::Relaxed);
            self.events.clear();
        }
        self.stats.batches.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Records the first fatal error; later ones are secondary fallout of the first.
    fn fail(&mut self, error: crate::Error) {
        if self.error.is_none() {
            self.error = Some(error);
        }
    }
}

impl<S: Storage> Handler<StoreOp, ()> for StoreBlock<S> {
    fn handle(&mut self, input: StoreOp, _emit: &mut dyn FnMut(())) {
        if let Some(watch) = &self.watch {
            watch.tap(&input);
        }
        match input {
            StoreOp::Register {
                target,
                first_seen,
                descriptors,
            } => {
                let result = self
                    .store
                    .register_target(target, first_seen)
                    .and_then(|()| self.store.register_series(&descriptors));
                if let Err(error) = result {
                    self.fail(crate::Error::Storage(error));
                }
            }
            StoreOp::Point(point) => self.points.push(point),
            StoreOp::Event(event) => self.events.push(event),
            StoreOp::Query(query) => query.answer(&self.store),
        }
    }

    /// Flush on the flush interval, retain on the retention interval. A flush failure is
    /// fatal for the block — the shard's purpose is gone. Retention failures are logged,
    /// not fatal: the next run retries.
    fn tick(&mut self, _emit: &mut dyn FnMut(())) {
        let now = Instant::now();
        if now >= self.next_flush {
            if let Err(error) = self.flush() {
                self.fail(error);
                return;
            }
            self.next_flush = now + self.config.flush_interval;
        }
        let Some(retention) = self.config.retention else {
            return;
        };
        if self.next_retain.is_some_and(|next| now >= next) {
            let cutoff = Timestamp::from_nanos(
                Timestamp::now().as_nanos()
                    - i64::try_from(retention.window.as_nanos()).unwrap_or(i64::MAX),
            );
            match self.store.retain(cutoff) {
                Ok(done) if done.points_deleted > 0 => println!(
                    "mrwolf: retention: {} minutes downsampled, {} raw points dropped with their segments",
                    done.minutes_downsampled, done.points_deleted
                ),
                Ok(_) => {}
                Err(error) => eprintln!("mrwolf: retention failed: {error}"),
            }
            self.next_retain = Some(now + retention.every);
        }
    }

    /// Final flush; the driver completes the block after this.
    fn complete(&mut self) {
        if let Err(error) = self.flush() {
            self.fail(error);
        }
    }

    fn faulted(&self) -> bool {
        self.error.is_some()
    }
}
