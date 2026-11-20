//! The dotnet-gc → store graph over an in-memory shard store: attach via messages,
//! mapping, batching, lifecycle events, self-telemetry, and the completion cascade.
//! (Block semantics themselves are unit-tested in `dataflow.rs`, the process block in
//! `process_block.rs`, placement in `placement.rs`.)

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use wolf_contract::{TargetId, Timestamp};
use wolf_dotnet::{DotnetTelemetry, EcSeries, GcKind, GcReason, GcSeries, HeapStats};
use wolf_host::{
    BlockHandle, BlockOptions, DotnetGcBlock, GcAttach, GcIn, SelfTelemetry, SessionStatsSnapshot,
    ShardStats, StoreBlock, StoreConfig, StoreOp, Target, TargetFlags, action_block,
    transform_block,
};
use wolf_ipc::ProcessInfo;
use wolf_storage::{ShardStore, Storage};

const BASE_NS: i64 = 1_760_843_645_000_000_000;

fn ts(nanos: i64) -> Timestamp {
    Timestamp::from_nanos(nanos)
}

fn target() -> TargetId {
    TargetId::new(4812, 1_760_843_645, "srv-a").expect("target")
}

fn gc_completed(generation: u32) -> wolf_dotnet::GcCompleted {
    wolf_dotnet::GcCompleted {
        number: 7,
        generation,
        kind: GcKind::Blocking,
        reason: GcReason(0),
        start_utc_ns: BASE_NS,
        end_utc_ns: BASE_NS + 1_000_000,
        pause_ns: 1_000_000,
        duration_ns: 1_000_000,
        compacting: Some(true),
        heap: HeapStats {
            size: [10, 20, 30, 40, 50],
            ..HeapStats::default()
        },
    }
}

fn process_info() -> ProcessInfo {
    ProcessInfo::for_test(4812, "OrderService")
}

/// One running shard: a dotnet-gc block linked to a store block over an in-memory store.
struct Shard {
    gc_input: Target<GcIn>,
    store_input: Target<StoreOp>,
    gc: BlockHandle<DotnetGcBlock>,
    store: BlockHandle<StoreBlock<ShardStore>>,
}

impl Shard {
    fn start() -> Self {
        let stats = Arc::new(ShardStats::default());
        let mut gc = transform_block(
            BlockOptions::named("test-dotnet-gc").capacity(64),
            DotnetGcBlock::new(Arc::clone(&stats)),
        );
        let store = action_block(
            BlockOptions::named("test-store").capacity(256),
            StoreBlock::new(
                ShardStore::open_in_memory().expect("store"),
                Arc::clone(&stats),
                StoreConfig {
                    flush_interval: Duration::from_millis(10),
                    retention: None,
                },
            ),
        );
        gc.link_to(store.input());
        Self {
            gc_input: gc.input(),
            store_input: store.input(),
            gc: gc.start(),
            store: store.start(),
        }
    }

    /// Attaches the default target: posts `Attach` and returns its lifecycle flags.
    fn attach(&self) -> TargetFlags {
        let id = target();
        let flags = TargetFlags::new();
        self.gc_input
            .post(GcIn::Attach(Box::new(GcAttach {
                id: id.clone(),
                first_seen: ts(BASE_NS),
                info: process_info(),
                gc: GcSeries::new(id.clone()),
                ec: EcSeries::new(id),
                flags: flags.clone(),
            })))
            .expect("attach");
        flags
    }

    /// Completes the graph (dropping the last input targets cascades completion) and
    /// returns the store block for inspection. The final flush runs in `complete`.
    fn finish(self) -> StoreBlock<ShardStore> {
        drop(self.gc_input);
        drop(self.store_input);
        self.gc.join().expect("dotnet-gc thread");
        self.store.join().expect("store thread")
    }
}

#[test]
fn telemetry_flows_through_the_graph() {
    let shard = Shard::start();
    shard.attach();
    shard
        .gc_input
        .post(GcIn::Item {
            slot: 0,
            item: wolf_host::SessionItem::Telemetry(DotnetTelemetry::Gc(gc_completed(2))),
        })
        .expect("session");
    let store = shard.finish();

    // Attach + GC event: target.attached (wall clock) and dotnet.gc (stream time).
    let id = target();
    let events = store
        .store()
        .events(&id, ts(0)..ts(i64::MAX))
        .expect("events");
    let kinds: Vec<&str> = events.iter().map(|event| event.kind.as_str()).collect();
    assert!(kinds.contains(&"target.attached"), "kinds: {kinds:?}");
    let gc = events
        .iter()
        .find(|event| event.kind == "dotnet.gc")
        .expect("dotnet.gc event");
    assert_eq!(gc.data["gc_number"], serde_json::json!(7));
    assert_eq!(gc.data["reason"], serde_json::json!("AllocSmall"));

    // The gen2 heap-size point (30 bytes) landed at the GC's end time.
    let gc_series = GcSeries::new(id.clone());
    let gen2 = gc_series.descriptors()[2].clone();
    assert_eq!(gen2.metric(), "dotnet.gc.heap.size");
    assert_eq!(
        store
            .store()
            .points(gen2.id(), ts(0)..ts(i64::MAX))
            .expect("points"),
        [wolf_contract::Point {
            series: gen2.id(),
            ts: ts(BASE_NS + 1_000_000),
            value: 30.0,
        }]
    );
}

#[test]
fn stream_end_detaches_the_target_with_a_reason() {
    let shard = Shard::start();
    let flags = shard.attach();
    shard
        .gc_input
        .post(GcIn::Item {
            slot: 0,
            item: wolf_host::SessionItem::Ended(wolf_host::SessionEnd::StreamClosed),
        })
        .expect("ended");
    let store = shard.finish();
    assert!(flags.detached.load(Ordering::Relaxed));
    assert!(!flags.active.load(Ordering::Relaxed));

    let id = target();
    let events = store
        .store()
        .events(&id, ts(0)..ts(i64::MAX))
        .expect("events");
    let detached = events
        .iter()
        .find(|event| event.kind == "target.detached")
        .expect("detached event");
    assert_eq!(detached.data["reason"], serde_json::json!("exited"));

    // A stop requested by the host reports "stopped" instead of "exited".
    let shard = Shard::start();
    let flags = shard.attach();
    flags.detaching.store(true, Ordering::Relaxed);
    shard
        .gc_input
        .post(GcIn::Item {
            slot: 0,
            item: wolf_host::SessionItem::Ended(wolf_host::SessionEnd::StreamClosed),
        })
        .expect("ended");
    let store = shard.finish();
    let events = store
        .store()
        .events(&id, ts(0)..ts(i64::MAX))
        .expect("events");
    let detached = events
        .iter()
        .find(|event| event.kind == "target.detached")
        .expect("detached event");
    assert_eq!(detached.data["reason"], serde_json::json!("stopped"));
}

#[test]
fn a_stream_error_records_session_lost_and_detached() {
    let shard = Shard::start();
    let flags = shard.attach();
    let error = std::io::Error::new(std::io::ErrorKind::ConnectionReset, "pipe broke");
    shard
        .gc_input
        .post(GcIn::Item {
            slot: 0,
            item: wolf_host::SessionItem::Ended(wolf_host::SessionEnd::Io(error)),
        })
        .expect("ended");
    let store = shard.finish();
    assert!(flags.detached.load(Ordering::Relaxed));

    let id = target();
    let events = store
        .store()
        .events(&id, ts(0)..ts(i64::MAX))
        .expect("events");
    let kinds: Vec<&str> = events.iter().map(|event| event.kind.as_str()).collect();
    assert!(kinds.contains(&"session.lost"), "kinds: {kinds:?}");
    assert!(kinds.contains(&"target.detached"), "kinds: {kinds:?}");
    let lost = events
        .iter()
        .find(|event| event.kind == "session.lost")
        .expect("session.lost event");
    assert_eq!(lost.data["kind"], serde_json::json!("telemetry"));
    assert!(
        lost.data["reason"]
            .as_str()
            .expect("reason")
            .contains("pipe broke")
    );
}

#[test]
fn self_telemetry_reports_wolf_series() {
    let shard = Shard::start();
    let host = TargetId::new(1, 1_760_843_645, "srv-a").expect("host target");
    let telemetry = SelfTelemetry::new(host.clone());
    let events_received = telemetry.descriptors()[0].id();
    let events_dropped = telemetry.descriptors()[1].id();
    // The host posts its own telemetry straight into the store block.
    shard
        .store_input
        .post(StoreOp::Register {
            target: host.clone(),
            first_seen: ts(BASE_NS),
            descriptors: telemetry.descriptors().to_vec().into_boxed_slice(),
        })
        .expect("register self");
    let totals = SessionStatsSnapshot {
        events: 42,
        dropped: 1,
        bytes: 4096,
        ..Default::default()
    };
    for point in telemetry.points(ts(BASE_NS + 1_000), &totals, 1_000_000) {
        shard
            .store_input
            .post(StoreOp::Point(point))
            .expect("point");
    }
    let store = shard.finish();

    let received = store
        .store()
        .points(events_received, ts(0)..ts(i64::MAX))
        .expect("received points");
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].value, 42.0);
    let dropped = store
        .store()
        .points(events_dropped, ts(0)..ts(i64::MAX))
        .expect("dropped points");
    assert_eq!(dropped[0].value, 1.0);

    // The host registered itself as a target.
    assert_eq!(
        store.store().targets().expect("targets"),
        [wolf_storage::TargetInfo {
            id: host,
            first_seen: ts(BASE_NS),
        }]
    );
}
