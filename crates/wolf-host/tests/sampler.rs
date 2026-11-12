//! The sampler actor against the test process itself: ticks produce process samples on
//! the ring mailbox and stop promptly on the flag.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use wolf_host::{Input, Sampler, SamplerTarget, Wakeup, mailbox};

#[test]
fn samples_the_own_process_until_stopped() {
    let wakeup = Arc::new(Wakeup::default());
    let (tx, mut rx) = mailbox(&wakeup, 64);
    let stop = Arc::new(AtomicBool::new(false));
    let sampler = Sampler {
        targets: Mutex::new(vec![SamplerTarget {
            slot: 0,
            pid: std::process::id(),
            active: Arc::new(AtomicBool::new(true)),
        }]),
        tx,
        interval: Duration::from_millis(20),
        threads_every: 1,
        stop: Arc::clone(&stop),
        sample_errors: Arc::new(AtomicU64::new(0)),
    };
    let actor = std::thread::spawn(move || sampler.run());
    std::thread::sleep(Duration::from_millis(150));
    let stopping = Instant::now();
    stop.store(true, Ordering::SeqCst);
    actor.join().expect("sampler thread");
    assert!(
        stopping.elapsed() < Duration::from_secs(2),
        "stop must be prompt"
    );

    let mut samples = 0;
    let mut rss = 0;
    rx.drain(&mut |(slot, input)| {
        assert_eq!(slot, 0);
        let Input::Sample { sample, .. } = input else {
            panic!("the sampler sends only samples");
        };
        samples += 1;
        rss = sample.rss;
    });
    assert!(samples >= 2, "several ticks in 150 ms, got {samples}");
    assert!(rss > 0, "the test process has resident memory");
}

#[test]
fn a_dead_pid_is_counted_and_deactivated() {
    let wakeup = Arc::new(Wakeup::default());
    let (tx, mut rx) = mailbox(&wakeup, 64);
    let stop = Arc::new(AtomicBool::new(false));
    let sample_errors = Arc::new(AtomicU64::new(0));
    let active = Arc::new(AtomicBool::new(true));
    let sampler = Sampler {
        targets: Mutex::new(vec![SamplerTarget {
            slot: 0,
            // Not a live process on any OS we run on.
            pid: 0x00FF_FFFE,
            active: Arc::clone(&active),
        }]),
        tx,
        interval: Duration::from_millis(10),
        threads_every: 1,
        stop: Arc::clone(&stop),
        sample_errors: Arc::clone(&sample_errors),
    };
    let actor = std::thread::spawn(move || sampler.run());
    std::thread::sleep(Duration::from_millis(100));
    stop.store(true, Ordering::SeqCst);
    actor.join().expect("sampler thread");

    assert!(!active.load(Ordering::Relaxed), "deactivated after failure");
    assert!(sample_errors.load(Ordering::Relaxed) >= 1);
    let mut messages = 0;
    rx.drain(&mut |_| messages += 1);
    assert_eq!(messages, 0, "a dead pid produces no samples");
}
