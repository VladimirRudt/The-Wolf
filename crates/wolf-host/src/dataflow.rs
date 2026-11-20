//! Minimal TPL-Dataflow-style processing blocks (D15). A block owns one bounded input
//! buffer, a handler, and optionally one outgoing link; blocks connect with
//! [`Block::link_to`] and run one thread each. No framework, no registry — the wiring in
//! `host.rs` is the only place that knows the graph's shape.
//!
//! Semantics follow .NET's TPL Dataflow:
//! - **BoundedCapacity**: posting to a full buffer blocks the producer — that is the
//!   backpressure. Nothing grows unboundedly.
//! - **Completion**: dropping the last [`Target`] clone of a block's input is the
//!   upstream's `Complete()`. The block drains what is left, runs
//!   [`Handler::complete`], and drops its own outgoing target — completion cascades
//!   through the graph. Posting to a completed or faulted block is declined: `post`
//!   returns the message back.
//! - **Fault**: a handler that reports [`Handler::faulted`] stops its block; the input
//!   buffer is discarded (TPL faults discard buffered messages) and the cascade unwinds.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::thread::JoinHandle;
use std::time::Duration;

use crossbeam_queue::ArrayQueue;

/// How long a producer waits for buffer space before rechecking. The consumer notifies
/// when it drains a full buffer; the timeout only covers a missed notification.
const SPACE_RETRY: Duration = Duration::from_millis(10);

/// Block configuration: thread name, input buffer capacity (`BoundedCapacity`), and the
/// tick that caps idle sleep (the latency bound for [`Handler::tick`] cadence work).
#[derive(Debug, Clone)]
pub struct BlockOptions {
    pub name: String,
    pub capacity: usize,
    pub tick: Duration,
}

impl BlockOptions {
    pub fn named(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            capacity: 64,
            tick: Duration::from_millis(100),
        }
    }

    pub fn capacity(mut self, capacity: usize) -> Self {
        self.capacity = capacity;
        self
    }

    pub fn tick(mut self, tick: Duration) -> Self {
        self.tick = tick;
        self
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The "sleep until something changes" primitive: a mutex and a condvar with names and a
/// small API, so call sites read as `wakeup.notify()` instead of `.0` / `.1`.
///
/// The lock is always held while notifying, which pairs with `wait_if`'s
/// check-under-lock: either the waiter sees the change in its condition, or it is
/// already asleep and gets woken — no lost wakeups.
#[derive(Debug, Default)]
struct Signal {
    /// Guards the condition the waiter checks; carries no data of its own.
    mutex: Mutex<()>,
    condvar: Condvar,
}

impl Signal {
    /// Wakes one waiter.
    fn notify(&self) {
        let _guard = lock(&self.mutex);
        self.condvar.notify_one();
    }

    /// Wakes every waiter (used when a full buffer is drained or the block closes —
    /// many producers may be parked).
    fn notify_all(&self) {
        let _guard = lock(&self.mutex);
        self.condvar.notify_all();
    }

    /// Sleeps up to `timeout`, but only if `condition` holds while checked under the
    /// lock — a change that arrived just before the check is never slept through.
    fn wait_if(&self, timeout: Duration, condition: impl FnOnce() -> bool) {
        let guard = lock(&self.mutex);
        if condition() {
            let _ = self.condvar.wait_timeout(guard, timeout);
        }
    }
}

/// One block's input buffer: the preallocated ring plus its completion state.
#[derive(Debug)]
struct Shared<T> {
    ring: ArrayQueue<T>,
    /// The block's alarm clock: producers ring it when the ring goes empty → non-empty,
    /// and when the last producer leaves.
    wakeup: Signal,
    /// Producers wait here for space; the block notifies after draining a full ring and
    /// on close.
    space: Signal,
    /// Live [`Target`] clones. Zero means no record can ever arrive again.
    producers: AtomicUsize,
    /// Completed or faulted: `post` declines.
    closed: AtomicBool,
}

impl<T> Shared<T> {
    fn new(capacity: usize) -> Self {
        Self {
            ring: ArrayQueue::new(capacity),
            wakeup: Signal::default(),
            space: Signal::default(),
            producers: AtomicUsize::new(0),
            closed: AtomicBool::new(false),
        }
    }
}

/// The posting end of a block's input buffer (`ITargetBlock`). Clone for more producers;
/// dropping the last clone completes the block's input.
pub struct Target<T> {
    shared: Arc<Shared<T>>,
}

impl<T> std::fmt::Debug for Target<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Target")
            .field("producers", &self.shared.producers.load(Ordering::Relaxed))
            .field("closed", &self.shared.closed.load(Ordering::Relaxed))
            .finish()
    }
}

impl<T> Clone for Target<T> {
    fn clone(&self) -> Self {
        self.shared.producers.fetch_add(1, Ordering::Relaxed);
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<T> Drop for Target<T> {
    fn drop(&mut self) {
        if self.shared.producers.fetch_sub(1, Ordering::AcqRel) == 1 {
            // The last producer is gone: wake the block so it can complete promptly.
            self.shared.wakeup.notify();
        }
    }
}

impl<T> Target<T> {
    /// Posts one message, blocking while the buffer is full (the backpressure point).
    /// Returns the message back if the block already completed or faulted — the caller
    /// decides whether that is worth logging.
    pub fn post(&self, mut message: T) -> Result<(), T> {
        loop {
            if self.shared.closed.load(Ordering::Acquire) {
                return Err(message);
            }
            match self.shared.ring.push(message) {
                Ok(()) => {
                    // The block drains fully before sleeping, so "was empty" is the only
                    // transition it can miss; a wrong len with several producers costs one
                    // sleep timeout.
                    if self.shared.ring.len() == 1 {
                        self.shared.wakeup.notify();
                    }
                    return Ok(());
                }
                Err(returned) => {
                    message = returned;
                    self.shared.space.wait_if(SPACE_RETRY, || true);
                }
            }
        }
    }
}

/// What a block does with its records. Implemented by the domain blocks (transform,
/// sink); the driver supplies `emit`, which posts downstream — blocking if the linked
/// block's buffer is full, dropping silently if it is gone.
pub trait Handler<In, Out>: Send {
    fn handle(&mut self, input: In, emit: &mut dyn FnMut(Out));

    /// Cadence work (flush, retention, sampling). Called once per driver iteration —
    /// after every drain and at least every `tick`, so idle blocks still make progress.
    /// Gets the same `emit` as `handle`, so cadence work can produce records too.
    fn tick(&mut self, _emit: &mut dyn FnMut(Out)) {}

    /// The input is drained and no producers remain: the last chance to flush.
    fn complete(&mut self) {}

    /// The block failed fatally: the driver closes the input, discards the buffer, runs
    /// `complete`, and returns the handler so the caller can see what happened.
    fn faulted(&self) -> bool {
        false
    }
}

/// One dataflow block. Construct with [`transform_block`] / [`action_block`], wire with
/// [`Block::link_to`], run with [`Block::start`].
pub struct Block<In, Out, H> {
    options: BlockOptions,
    handler: H,
    input: Arc<Shared<In>>,
    output: Option<Target<Out>>,
}

impl<In, Out, H> std::fmt::Debug for Block<In, Out, H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Block")
            .field("name", &self.options.name)
            .field("linked", &self.output.is_some())
            .finish()
    }
}

impl<In, Out, H> Block<In, Out, H>
where
    H: Handler<In, Out> + 'static,
    In: Send + 'static,
    Out: Send + 'static,
{
    fn new(options: BlockOptions, handler: H) -> Self {
        Self {
            input: Arc::new(Shared::new(options.capacity)),
            options,
            handler,
            output: None,
        }
    }

    /// The block's input port. Clone it for every producer; dropping the last clone
    /// completes this block (after draining).
    pub fn input(&self) -> Target<In> {
        Target {
            shared: {
                self.input.producers.fetch_add(1, Ordering::Relaxed);
                Arc::clone(&self.input)
            },
        }
    }

    /// `LinkTo` with `PropagateCompletion`: this block's records go to `target`, and this
    /// block's completion drops the link, completing the downstream side. One link per
    /// block — the graph needs no fan-out today.
    pub fn link_to(&mut self, target: Target<Out>) {
        assert!(self.output.is_none(), "one downstream link per block");
        self.output = Some(target);
    }

    /// Spawns the block's thread and returns its completion handle. The handler comes
    /// back from [`BlockHandle::join`], so its final state (store, error, counters) is
    /// inspectable.
    pub fn start(self) -> BlockHandle<H> {
        let name = self.options.name.clone();
        let join = std::thread::Builder::new()
            .name(name.clone())
            .spawn(move || drive(self))
            .expect("block thread");
        BlockHandle { name, join }
    }
}

/// A running block (`Completion` in TPL terms).
pub struct BlockHandle<H> {
    name: String,
    join: JoinHandle<H>,
}

impl<H> std::fmt::Debug for BlockHandle<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockHandle")
            .field("name", &self.name)
            .field("finished", &self.is_finished())
            .finish()
    }
}

impl<H> BlockHandle<H> {
    pub fn is_finished(&self) -> bool {
        self.join.is_finished()
    }

    /// Waits for the block to complete and returns its handler.
    pub fn join(self) -> std::thread::Result<H> {
        self.join.join()
    }
}

/// A block with an outgoing link (`TransformBlock<TIn, TOut>`).
pub fn transform_block<In, Out, H>(options: BlockOptions, handler: H) -> Block<In, Out, H>
where
    H: Handler<In, Out> + 'static,
    In: Send + 'static,
    Out: Send + 'static,
{
    Block::new(options, handler)
}

/// A terminal block (`ActionBlock<T>`): `Out = ()`, `emit` is never called.
pub fn action_block<In, H>(options: BlockOptions, handler: H) -> Block<In, (), H>
where
    H: Handler<In, ()> + 'static,
    In: Send + 'static,
{
    Block::new(options, handler)
}

/// The driver loop: drain the input buffer, tick, sleep until a producer posts or the
/// tick expires; complete when no producers remain, unwind early on fault.
fn drive<In, Out, H: Handler<In, Out>>(block: Block<In, Out, H>) -> H {
    let Block {
        options,
        mut handler,
        input,
        output,
    } = block;
    loop {
        let was_full = input.ring.len() == input.ring.capacity();
        let mut drained = 0;
        while let Some(message) = input.ring.pop() {
            let mut emit = |out: Out| {
                if let Some(target) = &output {
                    // A full downstream buffer blocks here — end-to-end backpressure.
                    // A completed downstream declines; the record is dropped.
                    let _ = target.post(out);
                }
            };
            handler.handle(message, &mut emit);
            drained += 1;
        }
        if drained > 0 && was_full {
            input.space.notify_all();
        }
        {
            let mut emit = |out: Out| {
                if let Some(target) = &output {
                    let _ = target.post(out);
                }
            };
            handler.tick(&mut emit);
        }
        if handler.faulted() {
            input.close();
            // TPL fault semantics: buffered messages are discarded. Producers see the
            // closed input and stop posting, so nothing wedges.
            while input.ring.pop().is_some() {}
            input.space.notify_all();
            break;
        }
        if input.producers.load(Ordering::Acquire) == 0 && input.ring.is_empty() {
            break;
        }
        // Sleep until a post (or the last producer leaving) wakes us, at most one tick.
        input.wakeup.wait_if(options.tick, || input.ring.is_empty());
    }
    input.close();
    handler.complete();
    handler
}

/// The completion side of [`Shared`].
impl<T> Shared<T> {
    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.space.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Collects every record; `die_at` faults the block after that many.
    #[derive(Default)]
    struct Collect {
        items: Vec<u64>,
        die_at: Option<u64>,
    }

    impl Handler<u64, ()> for Collect {
        fn handle(&mut self, input: u64, _emit: &mut dyn FnMut(())) {
            self.items.push(input);
        }
        fn faulted(&self) -> bool {
            self.die_at.is_some_and(|at| self.items.len() as u64 >= at)
        }
    }

    #[test]
    fn dropping_the_last_target_completes_the_block() {
        let block = action_block(BlockOptions::named("test"), Collect::default());
        let input = block.input();
        let handle = block.start();
        input.post(1).expect("open");
        input.post(2).expect("open");
        drop(input);
        let collected = handle.join().expect("block thread");
        assert_eq!(collected.items, [1, 2]);
    }

    #[test]
    fn posting_to_a_completed_block_is_declined() {
        let block = action_block(
            BlockOptions::named("test").tick(Duration::from_millis(10)),
            Collect {
                items: Vec::new(),
                // Fault on the very first tick.
                die_at: Some(0),
            },
        );
        let input = block.input();
        let handle = block.start();
        let collected = handle.join().expect("block thread");
        assert!(collected.items.is_empty());
        assert!(input.post(1).is_err(), "a closed input declines");
    }

    #[test]
    fn completion_cascades_through_a_link() {
        struct Double;
        impl Handler<u64, u64> for Double {
            fn handle(&mut self, input: u64, emit: &mut dyn FnMut(u64)) {
                emit(input * 2);
            }
        }
        let sink = action_block(BlockOptions::named("sink"), Collect::default());
        let mut transform = transform_block(BlockOptions::named("transform"), Double);
        transform.link_to(sink.input());
        let input = transform.input();
        let sink_handle = sink.start();
        let transform_handle = transform.start();
        input.post(21).expect("open");
        drop(input);
        transform_handle.join().expect("transform");
        let collected = sink_handle.join().expect("sink");
        assert_eq!(collected.items, [42]);
    }

    #[test]
    fn a_faulted_block_declines_and_unwinds() {
        let block = action_block(
            BlockOptions::named("test"),
            Collect {
                items: Vec::new(),
                die_at: Some(1),
            },
        );
        let input = block.input();
        let handle = block.start();
        input.post(1).expect("open");
        let collected = handle.join().expect("block thread");
        assert_eq!(collected.items, [1]);
    }
}
