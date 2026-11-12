//! Actor wiring and lifecycle: attach targets, spawn the reader and sampler actors, drive
//! the router's cadence, and shut everything down gracefully (stop sessions, drain, flush).

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use wolf_contract::Timestamp;
use wolf_ipc::{DiagnosticsClient, SessionId};
use wolf_storage::{DuckDbStore, Storage};

use crate::attach::{Attached, attach, target_id, telemetry_session_config};
use crate::mailbox::{Wakeup, mailbox};
use crate::router::Router;
use crate::sampler::{Sampler, SamplerTarget};
use crate::session::{ReaderStats, ReaderStatsSnapshot, run_reader};
use crate::telemetry::SelfTelemetry;
use crate::{Error, Result};

/// How long raw points stay at full resolution before the retention job downsamples them
/// (`docs/architecture.md` §4: a few days).
const RAW_WINDOW: Duration = Duration::from_secs(72 * 60 * 60);

#[derive(Debug, Clone)]
pub struct HostConfig {
    pub db_path: PathBuf,
    pub pids: Vec<u32>,
    /// Batches land in storage this often.
    pub flush_interval: Duration,
    /// OS memory sampling cadence.
    pub sample_interval: Duration,
    /// Thread count every Nth sample (expensive on Windows).
    pub threads_every_samples: u32,
    /// Self-telemetry report cadence.
    pub report_interval: Duration,
    /// Ring slots per session reader. ~150 B per slot: 2048 slots ≈ 300 KiB per target.
    pub reader_ring: usize,
    /// Ring slots for the sampler (one message per target per tick).
    pub sampler_ring: usize,
    /// Full-resolution window and how often the retention job runs.
    pub retention: Option<RetentionConfig>,
}

#[derive(Debug, Clone, Copy)]
pub struct RetentionConfig {
    pub window: Duration,
    pub every: Duration,
}

impl Default for HostConfig {
    fn default() -> Self {
        Self {
            db_path: PathBuf::from("wolf.duckdb"),
            pids: Vec::new(),
            flush_interval: Duration::from_secs(1),
            sample_interval: Duration::from_secs(1),
            threads_every_samples: 60,
            report_interval: Duration::from_secs(60),
            reader_ring: 2048,
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
    pub router: crate::RouterStats,
    pub readers: ReaderStatsSnapshot,
    pub sample_errors: u64,
}

/// One running target: what the host needs at stop time.
struct TargetCtx {
    slot: u32,
    pid: u32,
    client: DiagnosticsClient,
    session_id: SessionId,
    active: Arc<AtomicBool>,
    stats: Arc<ReaderStats>,
    reader: JoinHandle<()>,
}

/// Attaches, collects, and shuts down. Blocks until every target detaches or ctrl-c.
pub fn run(config: &HostConfig) -> Result<HostReport> {
    let started = Instant::now();
    let store = DuckDbStore::open(&config.db_path)?;
    let wakeup = Arc::new(Wakeup::default());
    let mut router = Router::new(store, Arc::clone(&wakeup));

    // The host watches itself (D11).
    let self_telemetry = SelfTelemetry::new(target_id(std::process::id())?);
    router.register_self(self_telemetry, Timestamp::now())?;

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
                    // Park: the runtime must outlive the router loop for stop_session.
                    std::future::pending::<()>().await
                });
            }
        })?;

    // Attach every pid; failures are reported and skipped.
    let mut ctxs = Vec::new();
    for &pid in &config.pids {
        match start_target(pid, config, &handle, &mut router, &wakeup) {
            Ok(ctx) => ctxs.push(ctx),
            Err(error) => eprintln!("mrwolf: pid {pid}: {error}"),
        }
    }
    if ctxs.is_empty() {
        return Err(Error::NoTargets);
    }

    // The sampler actor watches every attached target.
    let sampler_stop = Arc::new(AtomicBool::new(false));
    let sample_errors = Arc::new(AtomicU64::new(0));
    let sampler = {
        let (tx, rx) = mailbox(&wakeup, config.sampler_ring);
        router.add_mailbox(rx);
        let targets = ctxs
            .iter()
            .map(|ctx| SamplerTarget {
                slot: ctx.slot,
                pid: ctx.pid,
                active: Arc::clone(&ctx.active),
            })
            .collect();
        let actor = Sampler {
            targets: Mutex::new(targets),
            tx,
            interval: config.sample_interval,
            threads_every: config.threads_every_samples,
            stop: Arc::clone(&sampler_stop),
            sample_errors: Arc::clone(&sample_errors),
        };
        std::thread::Builder::new()
            .name("wolf-sampler".into())
            .spawn(move || actor.run())?
    };

    let drive_result = drive(
        config,
        &mut router,
        &mut ctxs,
        &handle,
        &shutdown,
        &sampler_stop,
    );

    // Cleanup: stop sampling, stop sessions (if not already), drain readers dry, flush.
    sampler_stop.store(true, Ordering::SeqCst);
    stop_sessions(&mut router, &ctxs, &handle);
    while !ctxs.iter().all(|ctx| ctx.reader.is_finished()) {
        router.drain();
        router.wait(Duration::from_millis(50));
    }
    // A last report so short runs still record self-telemetry.
    let totals = ReaderStatsSnapshot::total(ctxs.iter().map(|ctx| ctx.stats.snapshot()));
    router.self_report(Timestamp::now(), &totals);
    if let Err(error) = router.flush() {
        eprintln!("mrwolf: final flush failed: {error}");
    }
    drive_result?;
    let report = HostReport {
        attached: ctxs.len(),
        detached: router.detached_count(),
        uptime: started.elapsed(),
        router: *router.stats(),
        readers: ReaderStatsSnapshot::total(ctxs.iter().map(|ctx| ctx.stats.snapshot())),
        sample_errors: sample_errors.load(Ordering::Relaxed),
    };
    for ctx in ctxs {
        if ctx.reader.join().is_err() {
            eprintln!("mrwolf: pid {}: reader thread panicked", ctx.pid);
        }
    }
    if sampler.join().is_err() {
        eprintln!("mrwolf: sampler thread panicked");
    }
    Ok(report)
}

/// Attaches one pid: registers it, opens the telemetry session, spawns its reader actor.
fn start_target(
    pid: u32,
    config: &HostConfig,
    handle: &tokio::runtime::Handle,
    router: &mut Router<DuckDbStore>,
    wakeup: &Arc<Wakeup>,
) -> Result<TargetCtx> {
    let Attached {
        client,
        info,
        id,
        gc,
        ec,
        process,
    } = attach(pid, handle)?;
    let slot = router.add_target(id.clone(), Timestamp::now(), gc, ec, process)?;
    router.target_attached(slot, &info);
    let session = match client.start_session(&telemetry_session_config()) {
        Ok(session) => session,
        Err(error) => {
            let reason = format!("session failed to start: {error}");
            router.detach_with_error(slot, &reason);
            return Err(Error::Ipc(error));
        }
    };
    router.session_started(slot, session.id());
    let session_id = session.id();
    let (tx, rx) = mailbox(wakeup, config.reader_ring);
    router.add_mailbox(rx);
    let stats = Arc::new(ReaderStats::default());
    let reader = std::thread::Builder::new()
        .name(format!("wolf-reader-{pid}"))
        .spawn({
            let stats = Arc::clone(&stats);
            move || run_reader(slot, session, tx, stats)
        })?;
    Ok(TargetCtx {
        slot,
        pid,
        client,
        session_id,
        active: router.active_flag(slot),
        stats,
        reader,
    })
}

/// The router loop: drain mailboxes, run the cadence, stop when all targets are gone or
/// ctrl-c arrives. Returns early only on a storage failure (the host's purpose is gone).
fn drive(
    config: &HostConfig,
    router: &mut Router<DuckDbStore>,
    ctxs: &mut [TargetCtx],
    handle: &tokio::runtime::Handle,
    shutdown: &Arc<AtomicBool>,
    sampler_stop: &Arc<AtomicBool>,
) -> Result<()> {
    let mut next_flush = Instant::now() + config.flush_interval;
    let mut next_report = Instant::now() + config.report_interval;
    let mut next_retain = config.retention.as_ref().map(|r| Instant::now() + r.every);
    let mut stopping = false;
    let mut first_error = None;
    loop {
        router.drain();
        let now = Instant::now();
        if now >= next_flush {
            if let Err(error) = router.flush() {
                first_error = Some(error);
                break;
            }
            next_flush = now + config.flush_interval;
        }
        if now >= next_report {
            let totals = ReaderStatsSnapshot::total(ctxs.iter().map(|ctx| ctx.stats.snapshot()));
            router.self_report(Timestamp::now(), &totals);
            println!(
                "mrwolf: targets={} events={} dropped={} bytes={} written={}p/{}e skipped={}",
                ctxs.iter()
                    .filter(|ctx| !router.is_detached(ctx.slot))
                    .count(),
                totals.events,
                totals.dropped,
                totals.bytes,
                router.stats().points_written,
                router.stats().events_written,
                router.stats().counters_skipped,
            );
            next_report = now + config.report_interval;
        }
        if let Some(retention) = &config.retention
            && next_retain.is_some_and(|next| now >= next)
        {
            let cutoff = Timestamp::from_nanos(
                Timestamp::now().as_nanos()
                    - i64::try_from(retention.window.as_nanos()).unwrap_or(i64::MAX),
            );
            match router.store_mut().retain(cutoff) {
                Ok(done) => println!(
                    "mrwolf: retention: {} minutes downsampled, {} raw points deleted",
                    done.minutes_downsampled, done.points_deleted,
                ),
                Err(error) => eprintln!("mrwolf: retention failed: {error}"),
            }
            next_retain = Some(now + retention.every);
        }
        if shutdown.load(Ordering::Relaxed) && !stopping {
            stopping = true;
            sampler_stop.store(true, Ordering::SeqCst);
            println!("mrwolf: stopping sessions...");
            stop_sessions(router, ctxs, handle);
        }
        if ctxs.iter().all(|ctx| router.is_detached(ctx.slot)) {
            break;
        }
        router.wait(Duration::from_millis(100));
    }
    match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

/// Stops every live session; the runtime flushes and closes the stream, so the reader
/// actor sees end of stream and finishes what it has (graceful detach, P1-8).
fn stop_sessions(
    router: &mut Router<DuckDbStore>,
    ctxs: &[TargetCtx],
    handle: &tokio::runtime::Handle,
) {
    for ctx in ctxs {
        if router.is_detached(ctx.slot) {
            continue;
        }
        router.begin_detach(ctx.slot);
        if let Err(error) = handle.block_on(ctx.client.stop_session(ctx.session_id)) {
            eprintln!("mrwolf: pid {}: stop_session failed: {error}", ctx.pid);
        }
    }
}
