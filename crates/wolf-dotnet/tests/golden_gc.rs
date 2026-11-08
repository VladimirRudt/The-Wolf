//! Golden-file tests: every `testdata/nettrace/*.nettrace` recording with a `*.gc.expected.txt`
//! must correlate to the same GCs as TraceEvent's GC model (PerfView GCStats), written by
//! `tools/TraceCheck` in gc mode. Pause and duration may differ by rounding: TraceEvent
//! computes them as `double` milliseconds, we as exact clock ticks.

use std::path::{Path, PathBuf};

use wolf_dotnet::{GcCompleted, GcCorrelator};
use wolf_nettrace::{Event, EventMetadata, NettraceParser, TraceInfo};

const TOLERANCE_MS: f64 = 0.001;

fn recordings() -> Vec<(PathBuf, PathBuf)> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/nettrace");
    let mut pairs: Vec<_> = std::fs::read_dir(&dir)
        .expect("testdata/nettrace")
        .map(|entry| entry.expect("dir entry").path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "nettrace"))
        .map(|path| {
            let expected = path.with_extension("gc.expected.txt");
            (path, expected)
        })
        .filter(|(_, expected)| expected.exists())
        .collect();
    pairs.sort();
    assert!(!pairs.is_empty(), "no GC recordings in {}", dir.display());
    pairs
}

fn correlate(stream: &[u8]) -> Vec<GcCompleted> {
    let mut parser = NettraceParser::new();
    let mut correlator = GcCorrelator::new();
    let mut gcs = Vec::new();
    let mut rest = stream;
    while !rest.is_empty() {
        let spare = parser.spare();
        let n = spare.len().min(rest.len());
        spare[..n].copy_from_slice(&rest[..n]);
        rest = &rest[n..];
        parser
            .commit(n, |event| {
                correlator.on_event(&event, &mut |gc| gcs.push(gc))
            })
            .expect("parse");
    }
    let stats = correlator.stats();
    assert_eq!((stats.unreadable_events, stats.stale_entries), (0, 0));
    gcs.sort_by_key(|gc| gc.number);
    gcs
}

/// An event copied out of the parser's buffer.
struct Owned {
    metadata: EventMetadata,
    thread_id: u64,
    timestamp: i64,
    payload: Vec<u8>,
    trace: TraceInfo,
}

fn owned_events(stream: &[u8]) -> Vec<Owned> {
    let mut parser = NettraceParser::new();
    let mut events = Vec::new();
    let mut rest = stream;
    while !rest.is_empty() {
        let spare = parser.spare();
        let n = spare.len().min(rest.len());
        spare[..n].copy_from_slice(&rest[..n]);
        rest = &rest[n..];
        parser
            .commit(n, |event| {
                events.push(Owned {
                    metadata: event.metadata.clone(),
                    thread_id: event.thread_id,
                    timestamp: event.timestamp,
                    payload: event.payload.to_vec(),
                    trace: *event.trace,
                });
            })
            .expect("parse");
    }
    events
}

/// Same columns as TraceCheck: number generation type reason pause_ms duration_ms
/// compacting gen0 gen1 gen2 loh poh.
fn render(gc: &GcCompleted) -> Vec<String> {
    let compacting = match gc.compacting {
        Some(true) => "yes",
        Some(false) => "no",
        None => "-",
    };
    let mut fields = vec![
        gc.number.to_string(),
        gc.generation.to_string(),
        gc.kind.raw().to_string(),
        gc.reason.0.to_string(),
        format!("{:.6}", gc.pause_ns as f64 / 1e6),
        format!("{:.6}", gc.duration_ns as f64 / 1e6),
        compacting.to_owned(),
    ];
    fields.extend(gc.heap.size.iter().map(u64::to_string));
    fields
}

/// The runtime only keeps events in order within a thread. Regroup each window of events by
/// thread, highest thread id first (per-thread order kept), as differently flushed buffers
/// would: the GCs must not change.
#[test]
fn shuffled_thread_order_gives_the_same_gcs() {
    for (path, _) in recordings() {
        let stream = std::fs::read(&path).expect("recording");
        let mut events = owned_events(&stream);
        for window in [7, 64, 1000] {
            for chunk in events.chunks_mut(window) {
                chunk.sort_by_key(|event| std::cmp::Reverse(event.thread_id));
            }
            let mut correlator = GcCorrelator::new();
            let mut gcs = Vec::new();
            for owned in &events {
                let event = Event {
                    metadata: &owned.metadata,
                    thread_id: owned.thread_id,
                    timestamp: owned.timestamp,
                    payload: &owned.payload,
                    trace: &owned.trace,
                };
                correlator.on_event(&event, &mut |gc| gcs.push(gc));
            }
            gcs.sort_by_key(|gc| gc.number);
            assert_eq!(
                gcs,
                correlate(&stream),
                "{} with windows of {window}",
                path.display()
            );
        }
    }
}

#[test]
fn recordings_match_traceevent_gc_stats() {
    for (path, expected_path) in recordings() {
        let name = path.file_name().expect("file name").to_string_lossy();
        let expected = std::fs::read_to_string(&expected_path).expect("expected file");
        let mut expected = expected.lines();
        let actual = correlate(&std::fs::read(&path).expect("recording"));

        assert_eq!(
            expected.next(),
            Some(format!("gcs {}", actual.len()).as_str()),
            "{name}: GC count"
        );
        for (gc, line) in actual.iter().zip(expected) {
            let want: Vec<&str> = line.split(' ').collect();
            let got = render(gc);
            assert_eq!(got.len(), want.len(), "{name} #{}: columns", gc.number);
            for (column, (got, want)) in got.iter().zip(&want).enumerate() {
                if column == 4 || column == 5 {
                    let (got, want): (f64, f64) =
                        (got.parse().expect("ms"), want.parse().expect("ms"));
                    assert!(
                        (got - want).abs() <= TOLERANCE_MS,
                        "{name} #{}: column {column} is {got}, TraceEvent says {want}",
                        gc.number
                    );
                } else {
                    assert_eq!(got, want, "{name} #{}: column {column}", gc.number);
                }
            }
        }
    }
}
