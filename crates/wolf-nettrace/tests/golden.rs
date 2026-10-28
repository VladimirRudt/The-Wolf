//! Golden-file tests: every `testdata/nettrace/*.nettrace` recording must parse to exactly what
//! TraceEvent produced for it (`*.expected.txt`, written by `tools/TraceCheck`).

use std::fmt::Write;
use std::path::{Path, PathBuf};

use wolf_nettrace::{BUFFER_SIZE, NettraceParser};

fn recordings() -> Vec<PathBuf> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/nettrace");
    let mut paths: Vec<PathBuf> = std::fs::read_dir(&dir)
        .expect("testdata/nettrace")
        .map(|entry| entry.expect("dir entry").path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "nettrace"))
        .collect();
    paths.sort();
    assert!(!paths.is_empty(), "no recordings in {}", dir.display());
    paths
}

/// Parses `stream` in reads of at most `chunk` bytes and renders events like TraceCheck:
/// a summary line, then one sorted line per event.
fn render(stream: &[u8], chunk: usize) -> Vec<String> {
    let mut parser = NettraceParser::new();
    let mut lines = Vec::new();
    let mut rest = stream;
    while !rest.is_empty() {
        let spare = parser.spare();
        let n = chunk.min(spare.len()).min(rest.len());
        spare[..n].copy_from_slice(&rest[..n]);
        rest = &rest[n..];
        parser
            .commit(n, |event| {
                let mut line = format!(
                    "{} {} {} {} {} ",
                    event.timestamp,
                    event.thread_id,
                    event.metadata.provider,
                    event.metadata.event_id,
                    event.metadata.version
                );
                for byte in event.payload {
                    let _ = write!(line, "{byte:02x}");
                }
                lines.push(line);
            })
            .expect("parse");
    }
    assert!(parser.is_finished(), "stream did not end with the end tag");
    let stats = parser.stats();
    assert_eq!(stats.unknown_events, 0);
    assert_eq!(stats.skipped_blocks, 0);
    lines.sort();
    lines.insert(
        0,
        format!("events {} lost {}", stats.events, stats.dropped_events),
    );
    lines
}

#[test]
fn recordings_match_traceevent() {
    for path in recordings() {
        let stream = std::fs::read(&path).expect("recording");
        let expected_path = path.with_extension("expected.txt");
        let expected = std::fs::read_to_string(&expected_path)
            .unwrap_or_else(|_| panic!("missing {}", expected_path.display()));
        let expected: Vec<&str> = expected.lines().collect();

        for chunk in [1, 7, 4096, BUFFER_SIZE] {
            let actual = render(&stream, chunk);
            assert_eq!(
                actual,
                expected,
                "{} with {chunk}-byte reads",
                path.display()
            );
        }
    }
}
