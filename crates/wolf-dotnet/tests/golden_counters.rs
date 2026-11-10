//! Golden-file tests: every `testdata/nettrace/*.nettrace` recording with a
//! `*.counters.expected.txt` must decode to the same catalogue counter points as TraceEvent
//! computes them (TraceCheck counters mode: Mean counters pass through, Sum counters become
//! increment / interval_sec, MB-published counters are scaled to bytes).

use std::path::{Path, PathBuf};

use wolf_contract::Timestamp;
use wolf_dotnet::{Decoded, EcSeries, EcStringPool, decode_counter};
use wolf_nettrace::NettraceParser;

fn recordings() -> Vec<(PathBuf, PathBuf)> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/nettrace");
    let mut pairs: Vec<_> = std::fs::read_dir(&dir)
        .expect("testdata/nettrace")
        .map(|entry| entry.expect("dir entry").path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "nettrace"))
        .map(|path| {
            let expected = path.with_extension("counters.expected.txt");
            (path, expected)
        })
        .filter(|(_, expected)| expected.exists())
        .collect();
    pairs.sort();
    assert!(
        !pairs.is_empty(),
        "no counter recordings in {}",
        dir.display()
    );
    pairs
}

/// Same columns as TraceCheck counters mode: metric timestamp_qpc value.
fn render(series: &EcSeries, point: wolf_contract::Point, qpc: i64) -> String {
    let metric = series
        .descriptors()
        .iter()
        .find(|d| d.id() == point.series)
        .expect("series of the point")
        .metric();
    format!("{metric} {qpc} {}", point.value)
}

#[test]
fn recordings_match_traceevent_counter_values() {
    for (path, expected_path) in recordings() {
        let name = path.file_name().expect("file name").to_string_lossy();
        let expected = std::fs::read_to_string(&expected_path).expect("expected file");
        let mut expected = expected.lines();
        let header = expected.next().expect("header");
        let count: usize = header
            .strip_prefix("counters ")
            .expect("counters header")
            .parse()
            .expect("header count");

        let stream = std::fs::read(&path).expect("recording");
        let mut parser = NettraceParser::new();
        let series = EcSeries::new("pid:4812.1760843645@srv-a".parse().expect("target"));
        let pool = EcStringPool::new();
        let mut unreadable = 0u64;
        let mut actual: Vec<String> = Vec::new();
        let mut rest = &stream[..];
        while !rest.is_empty() {
            let spare = parser.spare();
            let n = spare.len().min(rest.len());
            spare[..n].copy_from_slice(&rest[..n]);
            rest = &rest[n..];
            parser
                .commit(n, |event| match decode_counter(&event, &pool) {
                    Decoded::Other => {}
                    Decoded::Unreadable => unreadable += 1,
                    Decoded::Value(counter) => {
                        let qpc = event.timestamp;
                        let ts = Timestamp::from_nanos(event.time_utc_ns());
                        if let Some(point) = series.point(ts, &counter) {
                            actual.push(render(&series, point, qpc));
                        }
                    }
                })
                .expect("parse");
        }

        assert_eq!(unreadable, 0, "{name}: unreadable counter payloads");
        assert_eq!(actual.len(), count, "{name}: header count");
        let actual: Vec<_> = actual
            .into_iter()
            .map(|line| {
                let mut fields = line.split(' ');
                let metric = fields.next().expect("metric");
                let qpc = fields.next().expect("qpc");
                let value: f64 = fields.next().expect("value").parse().expect("value");
                (metric.to_owned(), qpc.to_owned(), value)
            })
            .collect();
        let expected: Vec<_> = expected
            .map(|line| {
                let mut fields = line.split(' ');
                let metric = fields.next().expect("metric").to_owned();
                let qpc = fields.next().expect("qpc").to_owned();
                let value: f64 = fields.next().expect("value").parse().expect("value");
                (metric, qpc, value)
            })
            .collect();
        assert_eq!(actual.len(), expected.len(), "{name}: sample count");
        for ((metric, qpc, value), (want_metric, want_qpc, want_value)) in
            actual.iter().zip(expected.iter())
        {
            assert_eq!(metric, want_metric, "{name}: metric");
            assert_eq!(qpc, want_qpc, "{name}: qpc of {metric}");
            // G17 round-trips exactly, so the values must be bit-identical.
            assert_eq!(
                value.to_bits(),
                want_value.to_bits(),
                "{name}: value of {metric} at {qpc}"
            );
        }
    }
}
