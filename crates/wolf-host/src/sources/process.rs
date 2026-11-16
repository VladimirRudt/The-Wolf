//! The process source: OS memory of every live target on a fixed cadence
//! (`docs/collector-dotnet.md` §5). Owns no contract knowledge — it forwards raw
//! [`ProcessSample`]s and the pipeline's transform maps them to catalogue points.
//!
//! One plain thread. Each target carries a producer into the channel of the pipeline that
//! owns it (D14), so the source itself needs no routing table. Sampling itself is
//! allocation-free (µs on Windows, small procfs reads on Linux); the thread count is
//! sampled on a slower cadence because it needs a system-wide walk on Windows
//! (`wolf_process::thread_count`).

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use wolf_contract::Timestamp;
use wolf_process::{sample, thread_count};

use crate::channel::Producer;
use crate::record::{Message, Record};

/// One target the process source watches.
#[derive(Debug)]
pub struct ProcessTarget {
    /// The slot the owning pipeline assigned to this target.
    pub slot: u32,
    pub pid: u32,
    /// Cleared by the pipeline's transform when the target detaches and by the source
    /// itself when the process is gone — the cheap liveness signal both already have.
    pub active: Arc<AtomicBool>,
    /// Producer into the channel of the pipeline that owns this target's shard.
    pub tx: Producer<Message>,
}

/// The process source's inputs. `stop` ends the loop promptly; `sample_errors` counts
/// dead or unreadable processes (self-telemetry).
#[derive(Debug)]
pub struct ProcessSource {
    pub targets: Mutex<Vec<ProcessTarget>>,
    pub interval: Duration,
    /// Sample the thread count every Nth tick (expensive on Windows).
    pub threads_every: u32,
    pub stop: Arc<AtomicBool>,
    pub sample_errors: Arc<AtomicU64>,
}

impl ProcessSource {
    /// The source loop: tick, sample every active target, sleep out the interval in short
    /// slices so `stop` is honoured quickly.
    pub fn run(&self) {
        let mut tick = 0u32;
        while !self.stop.load(Ordering::Relaxed) {
            let started = Instant::now();
            tick = tick.wrapping_add(1);
            let ts = Timestamp::now();
            let with_threads = tick % self.threads_every == 0;
            {
                let targets = self.targets.lock().unwrap_or_else(|e| e.into_inner());
                for target in targets.iter() {
                    if !target.active.load(Ordering::Relaxed) {
                        continue;
                    }
                    match sample(target.pid) {
                        Ok(mut reading) => {
                            if reading.threads.is_none() && with_threads {
                                reading.threads = thread_count(target.pid).ok();
                            }
                            target.tx.send((
                                target.slot,
                                Record::Sample {
                                    ts,
                                    sample: reading,
                                },
                            ));
                        }
                        Err(_) => {
                            // Gone or unreadable: count it and stop polling this pid — the
                            // session source reports the actual detach over its stream.
                            self.sample_errors.fetch_add(1, Ordering::Relaxed);
                            target.active.store(false, Ordering::Relaxed);
                        }
                    }
                }
            }
            let elapsed = started.elapsed();
            let mut remaining = self.interval.saturating_sub(elapsed);
            while remaining > Duration::ZERO && !self.stop.load(Ordering::Relaxed) {
                let slice = remaining.min(Duration::from_millis(50));
                std::thread::sleep(slice);
                remaining = remaining.saturating_sub(slice);
            }
        }
    }
}
