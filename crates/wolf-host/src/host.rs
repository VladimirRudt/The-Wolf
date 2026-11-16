//! Wiring and lifecycle: place targets on shards, spawn the sources and the pipeline
//! threads, drive the cadences no single shard owns (reports, self-telemetry), and shut
//! everything down gracefully (stop sessions, drain, flush).
//!
//! Threading (`docs/architecture.md` D13/D14): one `wolf-source-<pid>` thread per target,
//! one `wolf-process-source` thread, one `wolf-pipeline-<shard>` thread per physical
//! shard, plus the `wolf-runtime` thread driving tokio for request/response commands.
//! The main thread attaches targets, then only supervises.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use wolf_contract::Timestamp;
use wolf_ipc::{DiagnosticsClient, SessionId};
use wolf_storage::{ShardStore, shard_dir};

use crate::attach::{Attached, attach, target_id, telemetry_session_config};
use crate::channel::{Producer, Wakeup, channel};
use crate::pipeline::{
    Pipeline, PipelineConfig, PipelineHandle, PipelineStatsSnapshot, RetentionConfig,
};
use crate::placement::Placement;
use crate::record::{Message, Record};
use crate::sources::process::{ProcessSource, ProcessTarget};
use crate::sources::session::run_session_source;
use crate::sources::{SourceStats, SourceStatsSnapshot};
use crate::telemetry::SelfTelemetry;
use crate::transform::TargetFlags;
use crate::{Error, Result};

/// How long raw points stay at full resolution before retention downsamples them and
/// drops their day segments (`docs/architecture.md` §4: a few days).
const RAW_WINDOW: Duration = Duration::from_secs(72 * 60 * 60);

#[derive(Debug, Clone)]
pub struct HostConfig {
    /// Store directory: one `shard-NN/` subdirectory per pipeline (D14).
    pub db_dir: PathBuf,
    /// Physical shards = pipeline threads. Targets are placed by stable hash over 256
    /// fixed logical buckets (`placement`), so changing this never reshuffles old files.
    pub shards: u16,
    pub pids: Vec<u32>,
    /// Batches land in storage this often (per pipeline).
    pub flush_interval: Duration,
    /// OS memory sampling cadence.
    pub sample_interval: Duration,
    /// Thread count every Nth sample (expensive on Windows).
    pub threads_every_samples: u32,
    /// Self-telemetry report cadence.
    pub report_interval: Duration,
    /// Ring slots per session source. ~150 B per slot: 2048 slots ≈ 300 KiB per target.
    pub session_ring: usize,
    /// Ring slots of the process source's channel to each pipeline.
    pub sampler_ring: usize,
    /// Full-resolution window and how often each pipeline runs retention.
    pub retention: Option<RetentionConfig>,
}

impl Default for HostConfig {
    fn default() -> Self {
        Self {
            db_dir: PathBuf::from("wolf.db"),
            shards: 4,
            pids: Vec::new(),
            flush_interval: Duration::from_secs(1),
            sample_interval: Duration::from_secs(1),
            threads_every_samples: 60,
            report_interval: Duration::from_secs(60),
            session_ring: 2048,
            sampler_ring: 64,
            retention: Some(RetentionConfig {
                window: RAW_WINDOW,
                every: Duration::from_secs(60 * 60),
            }),
        }
    }
}

/// What one run did; printed by the CLI.
#[derive(Debug)]
pub struct HostReport {
    pub attached: usize,
    pub detached: usize,
    pub uptime: Duration,
    pub pipelines: PipelineStatsSnapshot,
    pub sources: SourceStatsSnapshot,
    pub sample_errors: u64,
}

/// One running target: what the host needs at stop time.
struct TargetCtx {
    /// The physical shard (pipeline) owning this target.
    pipeline: usize,
    slot: u32,
    pid: u32,
    client: DiagnosticsClient,
    session_id: SessionId,
    flags: TargetFlags,
    stats: Arc<SourceStats>,
    source: JoinHandle<()>,
}

/// Attaches, collects, and shuts down. Blocks until every target detaches or ctrl-c.
pub fn run(config: &HostConfig) -> Result<HostReport> {
    let started = Instant::now();
    let placement = Placement::new(config.shards);

    // One pipeline per physical shard. Everything is registered before the threads start.
    let pipeline_config = PipelineConfig {
        flush_interval: config.flush_interval,
        retention: config.retention,
    };
    let mut pipelines: Vec<Pipeline<ShardStore>> = Vec::new();
    for shard in 0..placement.physical() {
        let store = ShardStore::open(shard_dir(&config.db_dir, shard))?;
        pipelines.push(Pipeline::new(
            store,
            Arc::new(Wakeup::default()),
            pipeline_config,
        ));
    }

    // The host watches itself (D11): registered on the shard its own target id maps to,
    // fed over a small channel like any source.
    let self_telemetry = SelfTelemetry::new(target_id(std::process::id())?);
    let self_shard = placement.shard_of(self_telemetry.target()) as usize;
    let self_tx = {
        let pipeline = &mut pipelines[self_shard];
        let (tx, rx) = channel(&pipeline.handle().wakeup, 8);
        pipeline.add_channel(rx);
        pipeline.register_self(self_telemetry, Timestamp::now())?;
        tx
    };

    // tokio is for request/response commands only (D13); the data path stays on plain
    // threads. A driver thread keeps the runtime alive and turns ctrl-c into a flag.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let handle = runtime.handle().clone();
    let shutdown = Arc::new(AtomicBool::new(false));
    std::thread::Builder::new()
        .name("wolf-runtime".into())
        .spawn({
            let shutdown = Arc::clone(&shutdown);
            move || {
                runtime.block_on(async move {
                    let _ = tokio::signal::ctrl_c().await;
                    shutdown.store(true, Ordering::SeqCst);
                    // Park: the runtime must outlive the sources for stop_session.
                    std::future::pending::<()>().await
                });
            }
        })?;

    // Attach every pid; failures are reported and skipped.
    let mut ctxs = Vec::new();
    for &pid in &config.pids {
        match start_target(pid, config, placement, &handle, &mut pipelines) {
            Ok(ctx) => ctxs.push(ctx),
            Err(error) => eprintln!("mrwolf: pid {pid}: {error}"),
        }
    }
    if ctxs.is_empty() {
        return Err(Error::NoTargets);
    }

    // The process source watches every attached target, one channel per pipeline.
    let sampler_stop = Arc::new(AtomicBool::new(false));
    let sample_errors = Arc::new(AtomicU64::new(0));
    let process_source = {
        let mut txs: Vec<Option<Producer<Message>>> = Vec::new();
        txs.resize_with(pipelines.len(), || None);
        let mut targets = Vec::new();
        for ctx in &ctxs {
            let tx = match &txs[ctx.pipeline] {
                Some(tx) => tx.clone(),
                None => {
                    let pipeline = &mut pipelines[ctx.pipeline];
                    let (tx, rx) = channel(&pipeline.handle().wakeup, config.sampler_ring);
                    pipeline.add_channel(rx);
                    txs[ctx.pipeline] = Some(tx.clone());
                    tx
                }
            };
            targets.push(ProcessTarget {
                slot: ctx.slot,
                pid: ctx.pid,
                active: Arc::clone(&ctx.flags.active),
                tx,
            });
        }
        let source = ProcessSource {
            targets: Mutex::new(targets),
            interval: config.sample_interval,
            threads_every: config.threads_every_samples,
            stop: Arc::clone(&sampler_stop),
            sample_errors: Arc::clone(&sample_errors),
        };
        std::thread::Builder::new()
            .name("wolf-process-source".into())
            .spawn(move || source.run())?
    };

    // All channels are registered; start the pipelines.
    let mut pipelines_handles = Vec::new();
    let mut pipeline_threads = Vec::new();
    for (shard, pipeline) in pipelines.into_iter().enumerate() {
        pipelines_handles.push(pipeline.handle());
        pipeline_threads.push(
            std::thread::Builder::new()
                .name(format!("wolf-pipeline-{shard:02}"))
                .spawn(move || pipeline.run())?,
        );
    }

    supervise(
        config,
        &pipelines_handles,
        &pipeline_threads,
        &ctxs,
        &handle,
        &shutdown,
        &sampler_stop,
        &self_tx,
    );

    // Sources are done (supervise waits for them). A last report so short runs still
    // record self-telemetry, then stop the pipelines: they drain what is left, flush,
    // and exit.
    let totals = SourceStatsSnapshot::total(ctxs.iter().map(|ctx| ctx.stats.snapshot()));
    let rss = wolf_process::sample(std::process::id())
        .map(|sample| sample.rss)
        .unwrap_or(0);
    self_tx.send((
        0,
        Record::SelfReport {
            ts: Timestamp::now(),
            totals,
            rss,
        },
    ));
    for handle in &pipelines_handles {
        handle.stop.store(true, Ordering::SeqCst);
        handle.wakeup.notify();
    }

    let mut first_error = None;
    for (shard, thread) in pipeline_threads.into_iter().enumerate() {
        match thread.join() {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                eprintln!("mrwolf: pipeline {shard:02} failed: {error}");
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
            Err(_) => eprintln!("mrwolf: pipeline {shard:02} thread panicked"),
        }
    }
    let attached = ctxs.len();
    let detached = ctxs
        .iter()
        .filter(|ctx| ctx.flags.detached.load(Ordering::Relaxed))
        .count();
    for ctx in ctxs {
        if ctx.source.join().is_err() {
            eprintln!("mrwolf: pid {}: session source thread panicked", ctx.pid);
        }
    }
    if process_source.join().is_err() {
        eprintln!("mrwolf: process source thread panicked");
    }
    let report = HostReport {
        attached,
        detached,
        uptime: started.elapsed(),
        pipelines: PipelineStatsSnapshot::total(
            pipelines_handles
                .iter()
                .map(|handle| handle.stats.snapshot()),
        ),
        sources: totals,
        sample_errors: sample_errors.load(Ordering::Relaxed),
    };
    match first_error {
        Some(error) => Err(error),
        None => Ok(report),
    }
}

/// Attaches one pid: registers it on its shard, opens the telemetry session, spawns its
/// session source.
fn start_target(
    pid: u32,
    config: &HostConfig,
    placement: Placement,
    handle: &tokio::runtime::Handle,
    pipelines: &mut [Pipeline<ShardStore>],
) -> Result<TargetCtx> {
    let Attached {
        client,
        info,
        id,
        gc,
        ec,
        process,
    } = attach(pid, handle)?;
    let shard = placement.shard_of(&id) as usize;
    let pipeline = &mut pipelines[shard];
    let (slot, flags) = pipeline.attach(id.clone(), Timestamp::now(), gc, ec, process)?;
    pipeline.target_attached(slot, &info);
    let session = match client.start_session(&telemetry_session_config()) {
        Ok(session) => session,
        Err(error) => {
            let reason = format!("session failed to start: {error}");
            pipeline.detach_with_error(slot, &reason);
            return Err(Error::Ipc(error));
        }
    };
    pipeline.session_started(slot, session.id());
    let session_id = session.id();
    let (tx, rx) = channel(&pipeline.handle().wakeup, config.session_ring);
    pipeline.add_channel(rx);
    let stats = Arc::new(SourceStats::default());
    let source = std::thread::Builder::new()
        .name(format!("wolf-source-{pid}"))
        .spawn({
            let stats = Arc::clone(&stats);
            move || run_session_source(slot, session, tx, stats)
        })?;
    Ok(TargetCtx {
        pipeline: shard,
        slot,
        pid,
        client,
        session_id,
        flags,
        stats,
        source,
    })
}

/// The supervision loop: report on cadence, and start shutdown when ctrl-c arrives, when
/// every target has detached, or when a pipeline died (its error surfaces at join).
/// Shutdown order matters: sources must be finished before the pipelines' stop flags go
/// up, so no record is ever left undrained.
#[allow(clippy::too_many_arguments)]
fn supervise(
    config: &HostConfig,
    pipelines: &[PipelineHandle],
    pipeline_threads: &[JoinHandle<Result<()>>],
    ctxs: &[TargetCtx],
    handle: &tokio::runtime::Handle,
    shutdown: &Arc<AtomicBool>,
    sampler_stop: &Arc<AtomicBool>,
    self_tx: &Producer<Message>,
) {
    let mut next_report = Instant::now() + config.report_interval;
    let mut stopping = false;
    loop {
        let now = Instant::now();
        if now >= next_report {
            report(ctxs, pipelines, self_tx);
            next_report = now + config.report_interval;
        }
        let pipeline_died = pipeline_threads.iter().any(JoinHandle::is_finished);
        if !stopping
            && (shutdown.load(Ordering::Relaxed)
                || pipeline_died
                || ctxs
                    .iter()
                    .all(|ctx| ctx.flags.detached.load(Ordering::Relaxed)))
        {
            stopping = true;
            sampler_stop.store(true, Ordering::SeqCst);
            println!("mrwolf: stopping sessions...");
            stop_sessions(ctxs, handle);
        }
        if stopping && ctxs.iter().all(|ctx| ctx.source.is_finished()) {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// One report tick: self-telemetry into its pipeline, then the console line.
fn report(ctxs: &[TargetCtx], pipelines: &[PipelineHandle], self_tx: &Producer<Message>) {
    let totals = SourceStatsSnapshot::total(ctxs.iter().map(|ctx| ctx.stats.snapshot()));
    let rss = wolf_process::sample(std::process::id())
        .map(|sample| sample.rss)
        .unwrap_or(0);
    self_tx.send((
        0,
        Record::SelfReport {
            ts: Timestamp::now(),
            totals,
            rss,
        },
    ));
    let written =
        PipelineStatsSnapshot::total(pipelines.iter().map(|handle| handle.stats.snapshot()));
    println!(
        "mrwolf: targets={} events={} dropped={} bytes={} written={}p/{}e skipped={}",
        ctxs.iter()
            .filter(|ctx| !ctx.flags.detached.load(Ordering::Relaxed))
            .count(),
        totals.events,
        totals.dropped,
        totals.bytes,
        written.points_written,
        written.events_written,
        written.counters_skipped,
    );
}

/// Stops every live session; the runtime flushes and closes the stream, so the session
/// source sees end of stream and finishes what it has (graceful detach, P1-8).
fn stop_sessions(ctxs: &[TargetCtx], handle: &tokio::runtime::Handle) {
    for ctx in ctxs {
        // Mark first: the clean end that follows reports "stopped", not "exited".
        ctx.flags.detaching.store(true, Ordering::Relaxed);
        if ctx.flags.detached.load(Ordering::Relaxed) {
            continue;
        }
        if let Err(error) = handle.block_on(ctx.client.stop_session(ctx.session_id)) {
            eprintln!("mrwolf: pid {}: stop_session failed: {error}", ctx.pid);
        }
    }
}
