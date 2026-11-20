//! The process block against the test process itself: attach registers the series, the
//! tick samples live targets and emits points, a dead pid is counted and deactivated.
//! The handler is driven directly — no block threads needed.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use wolf_contract::{TargetId, Timestamp};
use wolf_host::{Handler, ProcessBlock, ProcessIn, ProcessTarget, ShardStats, StoreOp};

const BASE_NS: i64 = 1_760_843_645_000_000_000;

fn attach(block: &mut ProcessBlock, pid: u32, ops: &mut Vec<StoreOp>) -> Arc<AtomicBool> {
    let id = TargetId::new(pid, 1_760_843_645, "srv-a").expect("target");
    let active = Arc::new(AtomicBool::new(true));
    block.handle(
        ProcessIn::Attach(Box::new(ProcessTarget::new(
            id,
            Timestamp::from_nanos(BASE_NS),
            pid,
            Arc::clone(&active),
        ))),
        &mut |op| ops.push(op),
    );
    active
}

fn points(ops: &[StoreOp]) -> usize {
    ops.iter()
        .filter(|op| matches!(op, StoreOp::Point(_)))
        .count()
}

#[test]
fn attach_registers_and_the_tick_samples_live_targets() {
    let stats = Arc::new(ShardStats::default());
    let mut block = ProcessBlock::new(Duration::from_secs(60), 1, Arc::clone(&stats));
    let mut ops = Vec::new();
    let active = attach(&mut block, std::process::id(), &mut ops);

    // The attach registers the target and its process.* series.
    let [
        StoreOp::Register {
            target,
            descriptors,
            ..
        },
    ] = ops.as_slice()
    else {
        panic!("an attach emits exactly one register op, got {ops:?}");
    };
    assert_eq!(target.pid(), std::process::id());
    assert!(!descriptors.is_empty());

    // The first tick samples right away; the next one is not due yet.
    block.tick(&mut |op| ops.push(op));
    assert!(
        points(&ops) >= 3,
        "rss, private and virtual at least: {ops:?}"
    );
    let after_first = ops.len();
    block.tick(&mut |op| ops.push(op));
    assert_eq!(ops.len(), after_first, "the interval gates sampling");
    assert!(active.load(Ordering::Relaxed));
    assert_eq!(stats.sample_errors.load(Ordering::Relaxed), 0);
}

#[test]
fn a_dead_pid_is_counted_and_deactivated() {
    let stats = Arc::new(ShardStats::default());
    let mut block = ProcessBlock::new(Duration::from_secs(60), 1, Arc::clone(&stats));
    let mut ops = Vec::new();
    // Not a live process on any OS we run on.
    let active = attach(&mut block, 0x00FF_FFFE, &mut ops);

    block.tick(&mut |op| ops.push(op));
    assert_eq!(points(&ops), 0, "a dead pid produces no samples");
    assert!(!active.load(Ordering::Relaxed), "deactivated after failure");
    assert_eq!(stats.sample_errors.load(Ordering::Relaxed), 1);
}
