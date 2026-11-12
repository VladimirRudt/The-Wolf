//! Drives the reader actor's decode path (`SessionDecoder`) with the recorded nettrace
//! streams, like the golden tests in `wolf-dotnet`, and checks the plumbing end to end:
//! GC telemetry, counters, gen0 summaries, clean stats.

use std::path::PathBuf;
use std::sync::Arc;

use wolf_dotnet::DotnetTelemetry;
use wolf_host::{ReaderStats, SessionDecoder, SessionItem};

fn recordings() -> Vec<PathBuf> {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../testdata/nettrace");
    let mut paths: Vec<_> = std::fs::read_dir(&dir)
        .expect("testdata/nettrace")
        .map(|entry| entry.expect("dir entry").path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "nettrace"))
        .collect();
    paths.sort();
    assert!(!paths.is_empty(), "no recordings in {}", dir.display());
    paths
}

/// Feeds a recording through the decoder in small chunks, as the reader thread would.
fn decode_all(stream: &[u8]) -> (Vec<SessionItem>, wolf_host::ReaderStatsSnapshot) {
    let stats = Arc::new(ReaderStats::default());
    let mut decoder = SessionDecoder::new(Arc::clone(&stats));
    let mut items = Vec::new();
    let mut rest = stream;
    while !rest.is_empty() {
        let spare = decoder.spare();
        let n = spare.len().min(rest.len());
        spare[..n].copy_from_slice(&rest[..n]);
        rest = &rest[n..];
        decoder
            .commit(n, &mut |item| items.push(item))
            .expect("parse");
    }
    decoder.finish(&mut |item| items.push(item));
    (items, stats.snapshot())
}

#[test]
fn recordings_decode_into_telemetry_and_counters_cleanly() {
    let mut saw_gc = false;
    let mut saw_counter = false;
    let mut saw_gen0_summary = false;
    for path in recordings() {
        let name = path
            .file_name()
            .expect("name")
            .to_string_lossy()
            .into_owned();
        let stream = std::fs::read(&path).expect("recording");
        let (items, stats) = decode_all(&stream);
        assert_eq!(stats.dropped, 0, "{name}: dropped events");
        assert_eq!(stats.unreadable_gc, 0, "{name}: unreadable GC payloads");
        assert_eq!(stats.unreadable_counters, 0, "{name}: unreadable counters");
        assert_eq!(stats.stale, 0, "{name}: stale correlator entries");
        assert!(stats.events > 0, "{name}: parser saw events");
        assert!(!items.is_empty(), "{name}: produced items");

        for item in &items {
            match item {
                SessionItem::Telemetry(DotnetTelemetry::Gc(_)) => saw_gc = true,
                SessionItem::Telemetry(DotnetTelemetry::Gen0Summary(_)) => {
                    saw_gen0_summary = true;
                }
                SessionItem::Counter { .. } => saw_counter = true,
                SessionItem::Ended(_) => panic!("the decoder never emits Ended itself"),
            }
        }
    }
    assert!(saw_gc, "some recording has a gen1/gen2 GC");
    assert!(saw_gen0_summary, "some recording has a gen0 summary");
    assert!(saw_counter, "some recording has EventCounters samples");
}
