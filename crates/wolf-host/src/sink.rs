//! The sink stage (Vector-style naming): the single writer of one shard's store. It owns
//! the batch buffers — cleared after every flush, so they hold steady-state capacity —
//! and nothing else. Mapping is the transform's job; scheduling is the pipeline's.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use wolf_contract::{Event, Point};
use wolf_storage::Storage;

use crate::pipeline::PipelineStats;

/// Batches contract points and events and flushes them to one store. See the module docs.
#[derive(Debug)]
pub struct Sink<S: Storage> {
    store: S,
    points: Vec<Point>,
    events: Vec<Event>,
    stats: Arc<PipelineStats>,
}

impl<S: Storage> Sink<S> {
    pub fn new(store: S, stats: Arc<PipelineStats>) -> Self {
        Self {
            store,
            points: Vec::new(),
            events: Vec::new(),
            stats,
        }
    }

    pub fn store(&self) -> &S {
        &self.store
    }

    pub fn store_mut(&mut self) -> &mut S {
        &mut self.store
    }

    /// The batch buffers the transform maps into. Reused across flushes, so after warm-up
    /// they never reallocate.
    pub fn buffers(&mut self) -> (&mut Vec<Point>, &mut Vec<Event>) {
        (&mut self.points, &mut self.events)
    }

    /// One flush: both batches to storage. The buffers keep their capacity.
    pub fn flush(&mut self) -> crate::Result<()> {
        if !self.points.is_empty() {
            self.store.insert_points(&self.points)?;
            self.stats
                .points_written
                .fetch_add(self.points.len() as u64, Ordering::Relaxed);
            self.points.clear();
        }
        if !self.events.is_empty() {
            self.store.insert_events(&self.events)?;
            self.stats
                .events_written
                .fetch_add(self.events.len() as u64, Ordering::Relaxed);
            self.events.clear();
        }
        self.stats.batches.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}
