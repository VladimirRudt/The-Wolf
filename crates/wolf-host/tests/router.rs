//! The router actor over an in-memory store: message mapping, batching, lifecycle events,
//! self-telemetry. (Mailbox behaviour itself is unit-tested in `mailbox.rs`.)

use std::sync::Arc;

use wolf_contract::{TargetId, Timestamp};
use wolf_dotnet::{DotnetTelemetry, EcSeries, GcKind, GcReason, GcSeries, HeapStats};
use wolf_host::{Input, Router, SelfTelemetry, SessionEnd, SessionItem, Wakeup};
use wolf_process::{ProcessSample, ProcessSeries};
use wolf_storage::{DuckDbStore, Storage};

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

fn router_with_target() -> (Router<DuckDbStore>, u32) {
    let store = DuckDbStore::open_in_memory().expect("store");
    let wakeup = Arc::new(Wakeup::default());
    let mut router = Router::new(store, wakeup);
    let id = target();
    let slot = router
        .add_target(
            id.clone(),
            ts(BASE_NS),
            GcSeries::new(id.clone()),
            EcSeries::new(id.clone()),
            ProcessSeries::new(id),
        )
        .expect("add target");
    (router, slot)
}

fn sample() -> ProcessSample {
    ProcessSample {
        rss: 100,
        private: 80,
        virtual_memory: 200,
        limit: Some(1000),
        threads: Some(9),
    }
}

#[test]
fn telemetry_and_samples_batch_into_storage() {
    let (mut router, slot) = router_with_target();
    router.on_message(
        slot,
        Input::Session(SessionItem::Telemetry(DotnetTelemetry::Gc(gc_completed(2)))),
    );
    router.on_message(
        slot,
        Input::Sample {
            ts: ts(BASE_NS + 2_000_000),
            sample: sample(),
        },
    );
    router.flush().expect("flush");

    assert_eq!(router.stats().points_written, 13 + 5);
    assert_eq!(router.stats().events_written, 1);
    assert_eq!(router.stats().batches, 1);

    // The GC event landed with its catalogue name and payload.
    let id = target();
    let events = router
        .store()
        .events(&id, ts(0)..ts(i64::MAX))
        .expect("events");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].kind, "dotnet.gc");
    assert_eq!(events[0].data["gc_number"], serde_json::json!(7));
    assert_eq!(events[0].data["reason"], serde_json::json!("AllocSmall"));

    // The gen2 heap-size point (30 bytes) landed at the GC's end time.
    let gc_series = GcSeries::new(id.clone());
    let gen2 = gc_series.descriptors()[2].clone();
    assert_eq!(gen2.metric(), "dotnet.gc.heap.size");
    assert_eq!(
        router
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
    let (mut router, slot) = router_with_target();
    router.on_message(
        slot,
        Input::Session(SessionItem::Ended(SessionEnd::StreamClosed)),
    );
    assert!(router.is_detached(slot));
    assert_eq!(router.detached_count(), 1);
    router.flush().expect("flush");

    let id = target();
    let events = router
        .store()
        .events(&id, ts(0)..ts(i64::MAX))
        .expect("events");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].kind, "target.detached");
    assert_eq!(events[0].data["reason"], serde_json::json!("exited"));

    // A stop requested by the host reports "stopped" instead of "exited".
    let (mut router2, slot2) = router_with_target();
    router2.begin_detach(slot2);
    router2.on_message(
        slot2,
        Input::Session(SessionItem::Ended(SessionEnd::StreamClosed)),
    );
    router2.flush().expect("flush");
    let events = router2
        .store()
        .events(&id, ts(0)..ts(i64::MAX))
        .expect("events");
    assert_eq!(events[0].data["reason"], serde_json::json!("stopped"));
}

#[test]
fn a_stream_error_records_session_lost_and_detached() {
    let (mut router, slot) = router_with_target();
    let error = std::io::Error::new(std::io::ErrorKind::ConnectionReset, "pipe broke");
    router.on_message(
        slot,
        Input::Session(SessionItem::Ended(SessionEnd::Io(error))),
    );
    assert!(router.is_detached(slot));
    router.flush().expect("flush");

    let id = target();
    let events = router
        .store()
        .events(&id, ts(0)..ts(i64::MAX))
        .expect("events");
    let kinds: Vec<&str> = events.iter().map(|event| event.kind.as_str()).collect();
    assert_eq!(kinds, ["session.lost", "target.detached"]);
    assert_eq!(events[0].data["kind"], serde_json::json!("telemetry"));
    assert!(
        events[0].data["reason"]
            .as_str()
            .expect("reason")
            .contains("pipe broke")
    );
}

#[test]
fn self_telemetry_reports_wolf_series() {
    let store = DuckDbStore::open_in_memory().expect("store");
    let wakeup = Arc::new(Wakeup::default());
    let mut router = Router::new(store, wakeup);
    let host = TargetId::new(1, 1_760_843_645, "srv-a").expect("host target");
    let telemetry = SelfTelemetry::new(host.clone());
    let events_received = telemetry.descriptors()[0].id();
    let events_dropped = telemetry.descriptors()[1].id();
    router
        .register_self(telemetry, ts(BASE_NS))
        .expect("register self");

    let totals = wolf_host::ReaderStatsSnapshot {
        events: 42,
        dropped: 1,
        bytes: 4096,
        ..Default::default()
    };
    router.self_report(ts(BASE_NS + 1_000), &totals);
    router.flush().expect("flush");
    assert_eq!(router.stats().points_written, 4);

    let received = router
        .store()
        .points(events_received, ts(0)..ts(i64::MAX))
        .expect("received points");
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].value, 42.0);
    let dropped = router
        .store()
        .points(events_dropped, ts(0)..ts(i64::MAX))
        .expect("dropped points");
    assert_eq!(dropped[0].value, 1.0);

    // The host registered itself as a target.
    assert_eq!(
        router.store().targets().expect("targets"),
        [wolf_storage::TargetInfo {
            id: host,
            first_seen: ts(BASE_NS),
        }]
    );
}
