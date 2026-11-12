//! Mailboxes between actors: preallocated bounded **circular buffers** (crossbeam
//! `ArrayQueue`) — fixed memory per actor link, no allocation per message. Each producer
//! has its own mailbox; the router polls them round-robin.
//!
//! Backpressure is explicit (D13): a producer whose ring is full blocks in [`Producer::send`]
//! — for a session reader that means the pipe fills, the runtime's circular buffer fills,
//! and the runtime drops events, which the parser counts as sequence gaps. Nothing grows
//! unboundedly anywhere in the chain.
//!
//! Blocking is condvar-based, with timeouts so a lost notification costs one retry
//! interval instead of a hang.

use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::time::Duration;

use crossbeam_queue::ArrayQueue;

/// How long a producer waits for ring space before retrying. The consumer notifies when
/// it drains a full ring; the timeout only covers a missed notification.
const SPACE_RETRY: Duration = Duration::from_millis(10);

fn lock<'a, T>(mutex: &'a Mutex<T>) -> std::sync::MutexGuard<'a, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The router's alarm clock: producers ring it when a mailbox goes from empty to
/// non-empty. One for the whole router.
#[derive(Debug, Default)]
pub struct Wakeup {
    lock: Mutex<()>,
    cond: Condvar,
}

impl Wakeup {
    fn notify(&self) {
        // Holding the lock while notifying pairs with `wait_if`'s check-then-wait: either
        // the router sees the item in its check, or it is already waiting and wakes.
        let _guard = lock(&self.lock);
        self.cond.notify_one();
    }

    /// Sleeps up to `timeout` — but only if `condition` holds while checked under the
    /// lock, so an item that arrived just before the check is never slept through.
    pub fn wait_if(&self, timeout: Duration, condition: impl FnOnce() -> bool) {
        let guard = lock(&self.lock);
        if condition() {
            let _ = self.cond.wait_timeout(guard, timeout);
        }
    }
}

/// The producing end of one mailbox. `Send + Sync`; one per actor.
#[derive(Debug)]
pub struct Producer<T> {
    ring: Arc<ArrayQueue<T>>,
    wakeup: Arc<Wakeup>,
    space: Arc<(Mutex<()>, Condvar)>,
}

impl<T> Producer<T> {
    /// Appends one message, blocking while the ring is full (the backpressure point).
    /// Never allocates: the ring's slots exist since [`mailbox`].
    pub fn send(&self, mut item: T) {
        loop {
            match self.ring.push(item) {
                Ok(()) => {
                    // The router drains fully before sleeping, so "was empty" is the only
                    // transition it can miss. Single producer per ring: len is exact here.
                    if self.ring.len() == 1 {
                        self.wakeup.notify();
                    }
                    return;
                }
                Err(returned) => {
                    item = returned;
                    let guard = lock(&self.space.0);
                    let _ = self.space.1.wait_timeout(guard, SPACE_RETRY);
                }
            }
        }
    }
}

/// The consuming end of one mailbox; the router owns one per producer.
#[derive(Debug)]
pub struct Consumer<T> {
    ring: Arc<ArrayQueue<T>>,
    space: Arc<(Mutex<()>, Condvar)>,
}

impl<T> Consumer<T> {
    /// Pops every queued message into `each`; returns how many. Notifies waiting
    /// producers when a full ring was drained.
    pub fn drain(&mut self, each: &mut impl FnMut(T)) -> usize {
        let was_full = self.ring.len() == self.ring.capacity();
        let mut drained = 0;
        while let Some(item) = self.ring.pop() {
            each(item);
            drained += 1;
        }
        if drained > 0 && was_full {
            self.space.1.notify_all();
        }
        drained
    }

    pub fn is_empty(&self) -> bool {
        self.ring.is_empty()
    }
}

/// One bounded ring: `capacity` slots, preallocated.
pub fn mailbox<T>(wakeup: &Arc<Wakeup>, capacity: usize) -> (Producer<T>, Consumer<T>) {
    let ring = Arc::new(ArrayQueue::new(capacity));
    let space = Arc::new((Mutex::new(()), Condvar::new()));
    (
        Producer {
            ring: Arc::clone(&ring),
            wakeup: Arc::clone(wakeup),
            space: Arc::clone(&space),
        },
        Consumer { ring, space },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Instant;

    #[test]
    fn messages_arrive_in_order_without_allocation() {
        let wakeup = Arc::new(Wakeup::default());
        let (producer, mut consumer) = mailbox(&wakeup, 4);
        for i in 0..3 {
            producer.send(i);
        }
        let mut received = Vec::new();
        assert_eq!(consumer.drain(&mut |item| received.push(item)), 3);
        assert_eq!(received, [0, 1, 2]);
        assert!(consumer.is_empty());
    }

    #[test]
    fn a_full_ring_blocks_the_producer_until_drained() {
        let wakeup = Arc::new(Wakeup::default());
        let (producer, mut consumer) = mailbox(&wakeup, 2);
        producer.send(1);
        producer.send(2);

        let sent = Arc::new(AtomicBool::new(false));
        let handle = {
            let sent = Arc::clone(&sent);
            std::thread::spawn(move || {
                producer.send(3);
                sent.store(true, Ordering::Relaxed);
            })
        };
        std::thread::sleep(Duration::from_millis(100));
        assert!(!sent.load(Ordering::Relaxed), "blocked on the full ring");

        let mut received = Vec::new();
        consumer.drain(&mut |item| received.push(item));
        handle.join().expect("sender thread");
        consumer.drain(&mut |item| received.push(item));
        assert_eq!(received, [1, 2, 3]);
    }

    #[test]
    fn the_router_wakes_for_a_fresh_item_and_times_out_otherwise() {
        let wakeup = Arc::new(Wakeup::default());
        let (producer, mut consumer) = mailbox(&wakeup, 4);

        let started = Instant::now();
        wakeup.wait_if(Duration::from_millis(50), || consumer.is_empty());
        assert!(
            started.elapsed() >= Duration::from_millis(50),
            "slept empty"
        );

        // An item pushed before the check is never slept through.
        producer.send(42);
        let started = Instant::now();
        wakeup.wait_if(Duration::from_millis(50), || consumer.is_empty());
        assert!(
            started.elapsed() < Duration::from_millis(50),
            "saw the item"
        );
        assert_eq!(consumer.drain(&mut |_| {}), 1);
    }
}
