//! The session reader actor (D13): blocking reads straight into the nettrace parser's
//! buffer, GC correlation and counter decoding in place, results into its ring mailbox.
//!
//! [`SessionDecoder`] holds all decoding state and is I/O-free, so tests can drive it
//! with recorded streams; [`run_reader`] is the thin I/O loop around it.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use wolf_contract::Timestamp;
use wolf_dotnet::{
    Decoded, DotnetTelemetry, EcEvent, EcStringPool, GcCorrelator, Gen0Summary, add_gen0,
    decode_counter,
};
use wolf_ipc::EventSession;
use wolf_nettrace::{NettraceParser, ParseError};

use crate::mailbox::Producer;
use crate::messages::{Input, Message};

/// What a reader sends; the router maps it to contract points and events.
#[derive(Debug)]
pub enum SessionItem {
    /// A correlated GC (gen1/gen2) or a per-second gen0 summary.
    Telemetry(DotnetTelemetry),
    /// One `System.Runtime` counter sample; `ts` is the event time converted to UTC.
    Counter { ts: Timestamp, counter: EcEvent },
    /// The stream ended; carries why. Always the last item of a reader.
    Ended(SessionEnd),
}

/// Why a session's stream ended (`target.detached` / `session.lost` reasons, §2).
#[derive(Debug)]
pub enum SessionEnd {
    /// End of stream: the process exited or the session was stopped.
    StreamClosed,
    /// Corrupt stream; no resync is possible (`docs/collector-dotnet.md` §3.1).
    ParseError(ParseError),
    Io(io::Error),
}

impl std::fmt::Display for SessionEnd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StreamClosed => write!(f, "stream closed"),
            Self::ParseError(e) => write!(f, "corrupt stream: {e}"),
            Self::Io(e) => write!(f, "stream error: {e}"),
        }
    }
}

/// Cumulative reader-side counters, shared with the router (self-telemetry, D11).
/// Written by the reader thread only; read once a minute.
#[derive(Debug, Default)]
pub struct ReaderStats {
    pub bytes: AtomicU64,
    /// Parser counters (`wolf_nettrace::Stats`), copied after every commit.
    pub events: AtomicU64,
    /// Events the runtime dropped, from sequence-number gaps.
    pub dropped: AtomicU64,
    pub unknown: AtomicU64,
    pub skipped_blocks: AtomicU64,
    /// Correlator counters (`wolf_dotnet::CorrelatorStats`).
    pub gcs: AtomicU64,
    pub unreadable_gc: AtomicU64,
    pub stale: AtomicU64,
    /// EventCounters payloads that matched neither layout.
    pub unreadable_counters: AtomicU64,
}

impl ReaderStats {
    fn set(slot: &AtomicU64, value: u64) {
        slot.store(value, Ordering::Relaxed);
    }

    /// A consistent-enough copy for reporting (Relaxed; the reader updates in bursts).
    pub fn snapshot(&self) -> ReaderStatsSnapshot {
        let get = |slot: &AtomicU64| slot.load(Ordering::Relaxed);
        ReaderStatsSnapshot {
            bytes: get(&self.bytes),
            events: get(&self.events),
            dropped: get(&self.dropped),
            unknown: get(&self.unknown),
            skipped_blocks: get(&self.skipped_blocks),
            gcs: get(&self.gcs),
            unreadable_gc: get(&self.unreadable_gc),
            stale: get(&self.stale),
            unreadable_counters: get(&self.unreadable_counters),
        }
    }
}

/// Plain copy of [`ReaderStats`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReaderStatsSnapshot {
    pub bytes: u64,
    pub events: u64,
    pub dropped: u64,
    pub unknown: u64,
    pub skipped_blocks: u64,
    pub gcs: u64,
    pub unreadable_gc: u64,
    pub stale: u64,
    pub unreadable_counters: u64,
}

impl ReaderStatsSnapshot {
    /// Field-by-field sum over any number of readers.
    pub fn total(snapshots: impl Iterator<Item = Self>) -> Self {
        let mut total = Self::default();
        for s in snapshots {
            total.bytes += s.bytes;
            total.events += s.events;
            total.dropped += s.dropped;
            total.unknown += s.unknown;
            total.skipped_blocks += s.skipped_blocks;
            total.gcs += s.gcs;
            total.unreadable_gc += s.unreadable_gc;
            total.stale += s.stale;
            total.unreadable_counters += s.unreadable_counters;
        }
        total
    }
}

/// All state of one session's decode path: parser, GC correlation, gen0 summary and the
/// counter string pool. No I/O: feed bytes via [`Self::spare`] + [`Self::commit`].
#[derive(Debug)]
pub struct SessionDecoder {
    parser: NettraceParser,
    correlator: GcCorrelator,
    gen0_pending: Option<Gen0Summary>,
    pool: EcStringPool,
    stats: Arc<ReaderStats>,
}

impl SessionDecoder {
    pub fn new(stats: Arc<ReaderStats>) -> Self {
        Self {
            parser: NettraceParser::new(),
            correlator: GcCorrelator::new(),
            gen0_pending: None,
            pool: EcStringPool::new(),
            stats,
        }
    }

    /// Where the next chunk goes (`read_chunk(parser.spare())`).
    pub fn spare(&mut self) -> &mut [u8] {
        self.parser.spare()
    }

    /// Decodes `n` freshly read bytes; every produced item goes to `emit`.
    ///
    /// # Errors
    ///
    /// A corrupt stream. The session cannot be resynced; end it.
    pub fn commit(
        &mut self,
        n: usize,
        emit: &mut impl FnMut(SessionItem),
    ) -> Result<(), ParseError> {
        let Self {
            parser,
            correlator,
            gen0_pending,
            pool,
            stats,
        } = self;
        parser.commit(n, |event| {
            correlator.on_event(&event, &mut |gc| {
                if gc.generation == 0 {
                    // gen0 runs far too often to store one event per GC (§3.2).
                    add_gen0(gen0_pending, gc, &mut |summary| {
                        emit(SessionItem::Telemetry(DotnetTelemetry::Gen0Summary(
                            summary,
                        )));
                    });
                } else {
                    emit(SessionItem::Telemetry(DotnetTelemetry::Gc(gc)));
                }
            });
            match decode_counter(&event, pool) {
                Decoded::Other => {}
                Decoded::Unreadable => {
                    stats.unreadable_counters.fetch_add(1, Ordering::Relaxed);
                }
                Decoded::Value(counter) => {
                    emit(SessionItem::Counter {
                        ts: Timestamp::from_nanos(event.time_utc_ns()),
                        counter,
                    });
                }
            }
        })?;
        self.sync_stats();
        Ok(())
    }

    /// Session end: the pending gen0 summary is emitted at session stop (§3.2), then the
    /// final stats snapshot.
    pub fn finish(&mut self, emit: &mut impl FnMut(SessionItem)) {
        if let Some(summary) = self.gen0_pending.take() {
            emit(SessionItem::Telemetry(DotnetTelemetry::Gen0Summary(
                summary,
            )));
        }
        self.sync_stats();
    }

    fn sync_stats(&self) {
        let parser = self.parser.stats();
        ReaderStats::set(&self.stats.events, parser.events);
        ReaderStats::set(&self.stats.dropped, parser.dropped_events);
        ReaderStats::set(&self.stats.unknown, parser.unknown_events);
        ReaderStats::set(&self.stats.skipped_blocks, parser.skipped_blocks);
        let correlator = self.correlator.stats();
        ReaderStats::set(&self.stats.gcs, correlator.gcs);
        ReaderStats::set(&self.stats.unreadable_gc, correlator.unreadable_events);
        ReaderStats::set(&self.stats.stale, correlator.stale_entries);
    }
}

/// The reader actor's whole job: read, decode, forward. `slot` ties the stream to its
/// target in the router. A full mailbox blocks here — that is the backpressure (D13): the
/// pipe fills, the runtime drops, and the parser counts the sequence gaps.
pub fn run_reader(
    slot: u32,
    mut session: EventSession,
    tx: Producer<Message>,
    stats: Arc<ReaderStats>,
) {
    let mut decoder = SessionDecoder::new(stats);
    let mut forward = |item| tx.send((slot, Input::Session(item)));
    let end = loop {
        match session.read_chunk(decoder.spare()) {
            Ok(0) => break SessionEnd::StreamClosed,
            Ok(n) => {
                decoder.stats.bytes.fetch_add(n as u64, Ordering::Relaxed);
                if let Err(error) = decoder.commit(n, &mut forward) {
                    break SessionEnd::ParseError(error);
                }
            }
            Err(error) => break SessionEnd::Io(error),
        }
    };
    decoder.finish(&mut forward);
    tx.send((slot, Input::Session(SessionItem::Ended(end))));
}
