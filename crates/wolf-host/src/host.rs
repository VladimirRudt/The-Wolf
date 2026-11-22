//! Wiring and lifecycle: build the dataflow graph (one dotnet-gc and one process block
//! linked into one store block per shard), place targets by stable hash, spawn the
//! session producers, supervise, and shut everything down gracefully. Shutdown is
//! completion-driven (D15): stopping the sessions ends the producers, their input
//! targets drop, and completion cascades into the store blocks.
//!
//! Threading (`docs/architecture.md` D13/D14): one `wolf-session-<pid>` thread per
//! target, one `wolf-dotnet-gc-<shard>`, one `wolf-process-<shard>` and one
//! `wolf-store-<shard>` thread per physical shard, plus the `wolf-runtime` thread
//! driving tokio for request/response commands. The main thread attaches, then only
//! supervises.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use wolf_contract::Timestamp;
use wolf_ipc::{DiagnosticsClient, SessionId};
use wolf_storage::{ShardStore, shard_dir};

use crate::attach::{Attached, attach, target_id, telemetry_session_config};
use crate::blocks::dotnet_gc::{DotnetGcBlock, GcAttach, GcIn, TargetFlags};
use crate::blocks::process::{ProcessBlock, ProcessIn, ProcessTarget};
use crate::blocks::store::{RetentionConfig, StoreBlock, StoreConfig, StoreOp};
use crate::dataflow::{BlockHandle, Target, action_block, transform_block};
use crate::placement::Placement;
use crate::producers::session::run_session_producer;
use crate::producers::{SessionStats, SessionStatsSnapshot};
use crate::stats::{ShardStats, ShardStatsSnapshot};
use crate::telemetry::SelfTelemetry;
use crate::watch::Watch;
use crate::{Error, Result};

/// How long raw points stay at full resolution before retention downsamples them and
/// drops their day segments (`docs/architecture.md` §4: a few days).
const RAW_WINDOW: Duration = Duration::from_secs(72 * 60 * 60);

#[derive(Debug, Clone)]
pub struct HostConfig {
    /// Store directory: one `shard-NN/` subdirectory per shard (D14).
    pub db_dir: PathBuf,
    /// Physical shards. Each is a dotnet-gc and a process block linked into one store
    /// block; targets are placed by stable hash over 256 fixed logical buckets
    /// (`placement`), so changing this never reshuffles old files.
    pub shards: u16,
    pub pids: Vec<u32>,
    /// Batches land in storage this often (per shard).
    pub flush_interval: Duration,
    /// OS memory sampling cadence.
    pub sample_interval: Duration,
    /// Thread count every Nth sample (expensive on Windows).
    pub threads_every_samples: u32,
    /// Self-telemetry report cadence.
    pub report_interval: Duration,
    /// Input buffer slots of each shard's dotnet-gc block (session records pile up
    /// here). ~150 B per slot: 4096 slots ≈ 600 KiB per shard.
    pub gc_ring: usize,
    /// Input buffer slots of each shard's store block (points and events, ~40 B per slot).
    pub store_ring: usize,
    /// Full-resolution window and how often each shard's store block runs retention.
    pub retention: Option<RetentionConfig>,
    /// Render the live terminal view (`mrwolf watch`, P1-9) instead of the console
    /// report: charts, a GC log, and a storage-query panel over the tapped store blocks.
    pub watch: bool,
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
            gc_ring: 4096,
            store_ring: 8192,
            retention: Some(RetentionConfig {
                window: RAW_WINDOW,
                every: Duration::from_secs(60 * 60),
            }),
            watch: false,
        }
    }
}

/// What one run did; printed by the CLI.
#[derive(Debug)]
pub struct HostReport {
    pub attached: usize,
    pub detached: usize,
    pub uptime: Duration,
    pub shards: ShardStatsSnapshot,
    pub sources: SessionStatsSnapshot,
    pub sample_errors: u64,
}

/// What the supervisor keeps of a running shard: the block handles and its counters.
struct ShardHandle {
    stats: Arc<ShardStats>,
    gc: BlockHandle<DotnetGcBlock>,
    process: BlockHandle<ProcessBlock>,
    store: BlockHandle<StoreBlock<ShardStore>>,
}

/// One running target: what the host needs at stop time.
struct TargetCtx {
    pid: u32,
    client: DiagnosticsClient,
    session_id: SessionId,
    flags: TargetFlags,
    stats: Arc<SessionStats>,
    source: JoinHandle<()>,
}

/// Attaches, collects, and shuts down. Blocks until every target detaches or ctrl-c.
pub fn run(config: &HostConfig) -> Result<HostReport> {
    let started = Instant::now();
    let placement = Placement::new(config.shards);
    let store_config = StoreConfig {
        flush_interval: config.flush_interval,
        retention: config.retention,
    };

    // The host watches itself (D11): registered on the shard its own target id maps to,
    // fed straight into the store block like any upstream block.
    let self_telemetry = SelfTelemetry::new(target_id(std::process::id())?);
    let self_shard = placement.shard_of(self_telemetry.target()) as usize;

    // The watch view (P1-9): a tap on every shard's store block — the graph's fan-in
    // point, so one tap covers every target — plus a clone of each store input for the
    // view's storage queries. The host's own self-telemetry target is excluded.
    let watch = config
        .watch
        .then(|| Arc::new(Watch::new().excluding(self_telemetry.target().clone())));
    let mut store_inputs: Vec<Target<StoreOp>> = Vec::new();

    // Build the graph: per shard a dotnet-gc and a process block, both linked into the
    // shard's store block.
    let mut gc_inputs: Vec<Target<GcIn>> = Vec::new();
    let mut process_inputs: Vec<Target<ProcessIn>> = Vec::new();
    let mut next_slots: Vec<u32> = Vec::new();
    let mut handles: Vec<ShardHandle> = Vec::new();
    let mut self_tx = None;
    for shard in 0..placement.physical() {
        let store = ShardStore::open(shard_dir(&config.db_dir, shard))?;
        let stats = Arc::new(ShardStats::default());
        let mut gc = transform_block(
            crate::dataflow::BlockOptions::named(format!("wolf-dotnet-gc-{shard:02}"))
                .capacity(config.gc_ring),
            DotnetGcBlock::new(Arc::clone(&stats)),
        );
        let mut process = transform_block(
            crate::dataflow::BlockOptions::named(format!("wolf-process-{shard:02}")),
            ProcessBlock::new(
                config.sample_interval,
                config.threads_every_samples,
                Arc::clone(&stats),
            ),
        );
        let store = action_block(
            crate::dataflow::BlockOptions::named(format!("wolf-store-{shard:02}"))
                .capacity(config.store_ring),
            StoreBlock::new(store, Arc::clone(&stats), store_config).with_watch(watch.clone()),
        );
        gc.link_to(store.input());
        process.link_to(store.input());
        if shard as usize == self_shard {
            self_tx = Some(store.input());
        }
        if watch.is_some() {
            store_inputs.push(store.input());
        }
        gc_inputs.push(gc.input());
        process_inputs.push(process.input());
        next_slots.push(0);
        handles.push(ShardHandle {
            stats,
            gc: gc.start(),
            process: process.start(),
            store: store.start(),
        });
    }
    let Some(self_tx) = self_tx else {
        return Err(Error::Graph("self-telemetry shard out of range".into()));
    };
    let _ = self_tx.post(StoreOp::Register {
        target: self_telemetry.target().clone(),
        first_seen: Timestamp::now(),
        descriptors: self_telemetry.descriptors().to_vec().into_boxed_slice(),
    });

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
                    // Park: the runtime must outlive the producers for stop_session.
                    std::future::pending::<()>().await
                });
            }
        })?;

    // Attach every pid; failures are reported and skipped.
    let mut ctxs = Vec::new();
    for &pid in &config.pids {
        match start_target(
            pid,
            placement,
            &handle,
            &gc_inputs,
            &process_inputs,
            &mut next_slots,
        ) {
            Ok(ctx) => ctxs.push(ctx),
            Err(error) => eprintln!("mrwolf: pid {pid}: {error}"),
        }
    }
    if ctxs.is_empty() {
        return Err(Error::NoTargets);
    }

    supervise(
        config,
        &handles,
        &ctxs,
        &handle,
        &shutdown,
        &self_telemetry,
        &self_tx,
        watch.as_deref(),
        &store_inputs,
    );

    // Producers are done (supervise waits for them). Drop every remaining input target:
    // completion cascades into the store blocks, draining all buffered records.
    drop(self_tx);
    drop(store_inputs);
    drop(gc_inputs);
    drop(process_inputs);

    let stats: Vec<Arc<ShardStats>> = handles
        .iter()
        .map(|handle| Arc::clone(&handle.stats))
        .collect();
    let mut first_error = None;
    for (shard, handle) in handles.into_iter().enumerate() {
        if handle.gc.join().is_err() {
            eprintln!("mrwolf: dotnet-gc {shard:02} thread panicked");
        }
        if handle.process.join().is_err() {
            eprintln!("mrwolf: process {shard:02} thread panicked");
        }
        match handle.store.join() {
            Ok(mut store) => {
                if let Some(error) = store.take_error() {
                    eprintln!("mrwolf: store {shard:02} failed: {error}");
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
            Err(_) => eprintln!("mrwolf: store {shard:02} thread panicked"),
        }
    }
    let attached = ctxs.len();
    let detached = ctxs
        .iter()
        .filter(|ctx| ctx.flags.detached.load(Ordering::Relaxed))
        .count();
    let sources = SessionStatsSnapshot::total(ctxs.iter().map(|ctx| ctx.stats.snapshot()));
    for ctx in ctxs {
        if ctx.source.join().is_err() {
            eprintln!("mrwolf: pid {}: session producer thread panicked", ctx.pid);
        }
    }
    let shards = ShardStatsSnapshot::total(stats.iter().map(|stats| stats.snapshot()));
    let report = HostReport {
        attached,
        detached,
        uptime: started.elapsed(),
        shards,
        sources,
        sample_errors: shards.sample_errors,
    };
    match first_error {
        Some(error) => Err(error),
        None => Ok(report),
    }
}

/// Attaches one pid: posts its attach messages to the owning shard's blocks (slot =
/// attach order, which the dotnet-gc block reproduces), opens the telemetry session,
/// spawns the producer.
fn start_target(
    pid: u32,
    placement: Placement,
    handle: &tokio::runtime::Handle,
    gc_inputs: &[Target<GcIn>],
    process_inputs: &[Target<ProcessIn>],
    next_slots: &mut [u32],
) -> Result<TargetCtx> {
    let Attached {
        client,
        info,
        id,
        gc,
        ec,
    } = attach(pid, handle)?;
    let shard = placement.shard_of(&id) as usize;
    let slot = next_slots[shard];
    next_slots[shard] += 1;
    let flags = TargetFlags::new();
    let first_seen = Timestamp::now();
    if process_inputs[shard]
        .post(ProcessIn::Attach(Box::new(ProcessTarget::new(
            id.clone(),
            first_seen,
            pid,
            Arc::clone(&flags.active),
        ))))
        .is_err()
    {
        return Err(Error::Graph(format!(
            "shard {shard:02} process block is not running"
        )));
    }
    if gc_inputs[shard]
        .post(GcIn::Attach(Box::new(GcAttach {
            id: id.clone(),
            first_seen,
            info,
            gc,
            ec,
            flags: flags.clone(),
        })))
        .is_err()
    {
        return Err(Error::Graph(format!(
            "shard {shard:02} dotnet-gc block is not running"
        )));
    }
    let session = match client.start_session(&telemetry_session_config()) {
        Ok(session) => session,
        Err(error) => {
            let _ = gc_inputs[shard].post(GcIn::AttachFailed {
                slot,
                reason: format!("session failed to start: {error}"),
            });
            return Err(Error::Ipc(error));
        }
    };
    let session_id = session.id();
    let _ = gc_inputs[shard].post(GcIn::Started { slot, session_id });
    let stats = Arc::new(SessionStats::default());
    let source = std::thread::Builder::new()
        .name(format!("wolf-session-{pid}"))
        .spawn({
            let stats = Arc::clone(&stats);
            let tx = gc_inputs[shard].clone();
            move || run_session_producer(slot, session, tx, stats)
        })?;
    Ok(TargetCtx {
        pid,
        client,
        session_id,
        flags,
        stats,
        source,
    })
}

/// The supervision loop: report on cadence, and start shutdown when ctrl-c arrives, when
/// every target has detached, or when a block died (its error surfaces at join).
/// Shutdown order matters: producers must be finished before their input targets drop,
/// so no record is ever left undrained. In watch mode the loop renders the live view
/// and handles its keys instead of printing the report line; the terminal is restored
/// when the view drops.
#[allow(clippy::too_many_arguments)]
fn supervise(
    config: &HostConfig,
    handles: &[ShardHandle],
    ctxs: &[TargetCtx],
    handle: &tokio::runtime::Handle,
    shutdown: &Arc<AtomicBool>,
    self_telemetry: &SelfTelemetry,
    self_tx: &Target<StoreOp>,
    watch: Option<&Watch>,
    store_inputs: &[Target<StoreOp>],
) {
    /// Frame cadence of the watch view; the report line cadence stays `report_interval`.
    const FRAME: Duration = Duration::from_millis(500);
    // Entering the view can fail (no terminal); fall back to the report line then.
    let mut view = match watch {
        Some(_) => match crate::watch::view::View::enter() {
            Ok(view) => Some(view),
            Err(error) => {
                eprintln!(
                    "mrwolf: no terminal for the watch view ({error}); using the report line"
                );
                None
            }
        },
        None => None,
    };
    let mut view_state = view
        .as_ref()
        .map(|_| crate::watch::view::ViewState::default());
    let mut next_frame = Instant::now();
    let mut next_report = Instant::now() + config.report_interval;
    let mut stopping = false;
    loop {
        let now = Instant::now();
        if let (Some(view), Some(view_state), Some(watch)) =
            (view.as_mut(), view_state.as_mut(), watch)
        {
            view_state.handle_input(watch, store_inputs, shutdown);
            if now >= next_frame {
                view.draw(watch, view_state);
                next_frame = now + FRAME;
            }
        }
        if now >= next_report {
            report(ctxs, handles, self_telemetry, self_tx, view.is_none());
            next_report = now + config.report_interval;
        }
        let block_died = handles
            .iter()
            .any(|h| h.gc.is_finished() || h.process.is_finished() || h.store.is_finished());
        if !stopping
            && (shutdown.load(Ordering::Relaxed)
                || block_died
                || ctxs
                    .iter()
                    .all(|ctx| ctx.flags.detached.load(Ordering::Relaxed)))
        {
            stopping = true;
            if view.is_none() {
                // In watch mode the line would land on the alternate screen and vanish.
                println!("mrwolf: stopping sessions...");
            }
            stop_sessions(ctxs, handle);
        }
        if stopping && ctxs.iter().all(|ctx| ctx.source.is_finished()) {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// One report tick: self-telemetry into the store block, then — unless the watch view
/// owns the screen — the console line.
fn report(
    ctxs: &[TargetCtx],
    handles: &[ShardHandle],
    self_telemetry: &SelfTelemetry,
    self_tx: &Target<StoreOp>,
    console: bool,
) {
    let totals = SessionStatsSnapshot::total(ctxs.iter().map(|ctx| ctx.stats.snapshot()));
    let rss = wolf_process::sample(std::process::id())
        .map(|sample| sample.rss)
        .unwrap_or(0);
    for point in self_telemetry.points(Timestamp::now(), &totals, rss) {
        let _ = self_tx.post(StoreOp::Point(point));
    }
    let written = ShardStatsSnapshot::total(handles.iter().map(|handle| handle.stats.snapshot()));
    if !console {
        return;
    }
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
/// producer sees end of stream and finishes what it has (graceful detach, P1-8).
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
