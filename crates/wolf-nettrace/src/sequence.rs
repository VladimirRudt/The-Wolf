//! Dropped-event detection from per-thread sequence numbers (NetTrace v5 spec, "Event
//! Sequence Numbers").

use std::collections::HashMap;

/// Last sequence number seen per capture thread.
#[derive(Debug, Default)]
pub(crate) struct SequenceTracker {
    last: HashMap<u64, u32>,
}

impl SequenceTracker {
    /// Records an event; returns how many events this thread dropped just before it.
    pub(crate) fn on_event(&mut self, capture_thread_id: u64, sequence_number: u32) -> u64 {
        match self.last.insert(capture_thread_id, sequence_number) {
            None => 0,
            // A new thread reused the id: its counter starts again at 1.
            Some(_) if sequence_number == 1 => 0,
            Some(last) => gap(last, sequence_number),
        }
    }

    /// Records a sequence point: the latest number each live thread tried to log. Returns
    /// the events dropped since each thread's last seen event. Threads not listed have exited
    /// and are forgotten, which keeps the map bounded.
    pub(crate) fn on_sequence_point(&mut self, threads: &[(u64, u32)]) -> u64 {
        let mut dropped = 0;
        let mut live = HashMap::with_capacity(threads.len());
        for &(thread_id, sequence_number) in threads {
            if let Some(&last) = self.last.get(&thread_id) {
                dropped += gap(last, sequence_number.wrapping_add(1));
            }
            live.insert(thread_id, sequence_number);
        }
        self.last = live;
        dropped
    }
}

/// Numbers missing between `last` and `next`. Counters wrap at 2^32; a step backwards
/// (seen as a huge forward gap) is not a drop.
fn gap(last: u32, next: u32) -> u64 {
    let missing = next.wrapping_sub(last).wrapping_sub(1);
    if missing < 1 << 31 {
        u64::from(missing)
    } else {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consecutive_events_drop_nothing() {
        let mut tracker = SequenceTracker::default();
        assert_eq!(tracker.on_event(7, 1), 0);
        assert_eq!(tracker.on_event(7, 2), 0);
        assert_eq!(tracker.on_event(9, 40), 0);
    }

    #[test]
    fn a_gap_counts_the_missing_events() {
        let mut tracker = SequenceTracker::default();
        tracker.on_event(7, 3);
        assert_eq!(tracker.on_event(7, 7), 3);
    }

    #[test]
    fn wraparound_and_thread_reuse_are_not_drops() {
        let mut tracker = SequenceTracker::default();
        tracker.on_event(7, u32::MAX);
        assert_eq!(tracker.on_event(7, 0), 0);
        tracker.on_event(8, 500);
        assert_eq!(tracker.on_event(8, 1), 0);
    }

    #[test]
    fn sequence_point_finds_silent_drops_and_forgets_exited_threads() {
        let mut tracker = SequenceTracker::default();
        tracker.on_event(7, 10);
        tracker.on_event(8, 5);
        // Thread 7 logged up to 14 but we saw only 10; thread 8 is gone.
        assert_eq!(tracker.on_sequence_point(&[(7, 14), (9, 2)]), 4);
        // Thread 8 is unknown again: its next event is not compared with 5.
        assert_eq!(tracker.on_event(8, 100), 0);
        assert_eq!(tracker.on_event(7, 15), 0);
    }
}
