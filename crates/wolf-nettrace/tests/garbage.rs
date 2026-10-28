//! Corrupted input never panics: flip random bytes in the golden recordings and parse them
//! in random chunk sizes. Errors are fine; panics (including arithmetic overflow in debug
//! builds) are not.

use std::path::Path;

use wolf_nettrace::NettraceParser;

/// xorshift64: deterministic, so a failure reproduces with the same seed.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

fn parse(stream: &[u8], rng: &mut Rng) {
    let mut parser = NettraceParser::new();
    let mut rest = stream;
    while !rest.is_empty() {
        let spare = parser.spare();
        let n = (1 + rng.below(8192)).min(spare.len()).min(rest.len());
        spare[..n].copy_from_slice(&rest[..n]);
        rest = &rest[n..];
        if parser.commit(n, |_| {}).is_err() {
            return;
        }
    }
}

#[test]
fn corrupted_recordings_never_panic() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/nettrace");
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    for entry in std::fs::read_dir(dir).expect("testdata/nettrace") {
        let path = entry.expect("dir entry").path();
        if path.extension().is_none_or(|ext| ext != "nettrace") {
            continue;
        }
        let original = std::fs::read(&path).expect("recording");
        for round in 0..2000 {
            let mut stream = original.clone();
            // Half the rounds hit the stream header and Trace object, which are tiny but
            // decide how everything after them is read.
            let range = if round % 2 == 0 { 160 } else { stream.len() };
            for _ in 0..1 + rng.below(8) {
                let at = rng.below(range);
                stream[at] = rng.next() as u8;
            }
            parse(&stream, &mut rng);
        }
    }
}
