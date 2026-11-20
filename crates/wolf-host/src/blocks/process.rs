//! The process block: OS memory of every live target of one shard on a fixed cadence
//! (`docs/collector-dotnet.md` §5). There is no separate producer thread — the block
//! samples its own targets in [`Handler::tick`], so the subsystem is one composable
//! unit: attach messages in, points out. Sampling itself is allocation-free (µs on
//! Windows, small procfs reads on Linux); the thread count is sampled on a slower
//! cadence because it needs a system-wide walk on Windows (`wolf_process::thread_count`).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use wolf_contract::{TargetId, Timestamp};
use wolf_process::{ProcessSeries, sample, thread_count};

use crate::blocks::store::StoreOp;
use crate::dataflow::Handler;
use crate::stats::ShardStats;

/// The block's input: attaching a target is a message, not a method call. Detach needs
/// no message — the dotnet-gc block clears the target's `active` flag and the block
/// stops sampling it.
#[derive(Debug)]
pub enum ProcessIn {
    /// Watch a new target; its first sample happens on the next tick.
    Attach(Box<ProcessTarget>),
}

/// One watched process.
#[derive(Debug)]
pub struct ProcessTarget {
    pub id: TargetId,
    pub first_seen: Timestamp,
    pub pid: u32,
    series: ProcessSeries,
    /// Shared liveness: cleared by the dotnet-gc block when the target detaches and by
    /// this block when the process is gone — the cheap liveness signal both already have.
    active: Arc<AtomicBool>,
    next: Instant,
    ticks: u32,
}

impl ProcessTarget {
    pub fn new(id: TargetId, first_seen: Timestamp, pid: u32, active: Arc<AtomicBool>) -> Self {
        Self {
            series: ProcessSeries::new(id.clone()),
            id,
            first_seen,
            pid,
            active,
            // Sample promptly after the attach, then on cadence.
            next: Instant::now(),
            ticks: 0,
        }
    }

    /// The shared liveness flag, for the host's reports.
    pub fn active(&self) -> &Arc<AtomicBool> {
        &self.active
    }
}

/// The OS-memory block of one shard. See the module docs.
#[derive(Debug)]
pub struct ProcessBlock {
    targets: Vec<ProcessTarget>,
    interval: Duration,
    /// Sample the thread count every Nth sample of a target (expensive on Windows).
    threads_every: u32,
    stats: Arc<ShardStats>,
}

impl ProcessBlock {
    pub fn new(interval: Duration, threads_every: u32, stats: Arc<ShardStats>) -> Self {
        Self {
            targets: Vec::new(),
            interval,
            threads_every,
            stats,
        }
    }

    fn sample(&mut self, index: usize, emit: &mut dyn FnMut(StoreOp)) {
        let target = &mut self.targets[index];
        target.ticks = target.ticks.wrapping_add(1);
        match sample(target.pid) {
            Ok(mut reading) => {
                if reading.threads.is_none() && target.ticks % self.threads_every == 0 {
                    reading.threads = thread_count(target.pid).ok();
                }
                let ts = Timestamp::now();
                for point in target.series.points(ts, &reading) {
                    emit(StoreOp::Point(point));
                }
            }
            Err(_) => {
                // Gone or unreadable: count it and stop polling this pid — the dotnet-gc
                // block reports the actual detach over its session stream.
                self.stats.sample_errors.fetch_add(1, Ordering::Relaxed);
                target.active.store(false, Ordering::Relaxed);
            }
        }
    }
}

impl Handler<ProcessIn, StoreOp> for ProcessBlock {
    fn handle(&mut self, input: ProcessIn, emit: &mut dyn FnMut(StoreOp)) {
        match input {
            ProcessIn::Attach(target) => {
                emit(StoreOp::Register {
                    target: target.id.clone(),
                    first_seen: target.first_seen,
                    descriptors: target.series.descriptors().to_vec().into_boxed_slice(),
                });
                self.targets.push(*target);
            }
        }
    }

    /// The sampling loop: every due, still-live target once. Runs on the block thread —
    /// the per-shard target count keeps a full pass in the microseconds.
    fn tick(&mut self, emit: &mut dyn FnMut(StoreOp)) {
        let now = Instant::now();
        for index in 0..self.targets.len() {
            let target = &self.targets[index];
            if !target.active.load(Ordering::Relaxed) || target.next > now {
                continue;
            }
            self.targets[index].next = now + self.interval;
            self.sample(index, emit);
        }
    }
}
