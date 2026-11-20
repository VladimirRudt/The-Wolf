# Contributing

## Read first

1. `docs/architecture.md` — overall design, component boundaries, the decision log
   (D1–D15). Always read this first.
2. `docs/roadmap.md` — phased tasks with "Done when" criteria. Work on one task ID at a
   time; mark it ✅ when the criteria pass.
3. `docs/collector-dotnet.md` — the .NET/OS collector spec and metric catalogue.

## Ground rules

- **Component boundaries**: collectors measure and execute commands; they never analyze,
  detect anomalies, or store long-term. Analysis never collects.
- **Never crash on unknown input** — skip it and count it in self-telemetry.
- **Verify protocol details against primary sources** before coding: `dotnet/diagnostics`
  docs, the runtime event manifest (`ClrEtwAll.man`), `dotnet-gcdump` / `dotnet-counters`
  / TraceEvent source. Spec items marked *(verify)* are not authoritative.
- **Version every wire protocol and file format** from the first commit.
- Use metric and event names from `docs/collector-dotnet.md` §4 exactly.
- Privacy: never capture or output string contents or heap field values unless explicitly
  opted in.
- Timestamps: UTC, nanosecond precision, everywhere.
- Keep the reference scenario (`docs/architecture.md` §8, `samples/LeakyOrderService`)
  working end to end.

## The dataflow rules of thumb

Blocks (`dataflow.rs`) are the only structural primitive; keep them generic:

- Domain logic lives in handlers (`mapper.rs`, `writer.rs`) and producers
  (`producers/`) — never in `dataflow.rs`.
- Everything crossing a thread boundary is a `Record` / `StoreOp` on a bounded buffer.
  No shared mutable state except the documented lifecycle flags and stats atomics.
- New workflow = new wiring in `host.rs`, not new vocabulary in the blocks.

## Commands

```sh
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all --check
```

CI runs fmt, clippy, build and tests on Linux and Windows; keep it green.

## Tests

- Every parser gets golden-file tests using recorded real streams in `testdata/`.
  Regenerate expectations with `tools/TraceCheck` (see `CLAUDE.md` for the exact
  commands); check recordings for user names, paths and secrets before committing — every
  `.nettrace` contains the target's command line.
- Storage behaviour is tested against the sharded layout (`crates/wolf-storage/tests/
  shard_store.rs`), including the 1M-point write-path budget.
- The host's graph is tested end to end over an in-memory store
  (`crates/wolf-host/tests/graph.rs`).

## Style

- Rust 2024 edition, workspace lints (`unwrap_used` is denied; document fallible paths).
- Match the surrounding module docs: every file starts with a `//!` explaining *why* it
  exists and its invariants, not *what* the code does line by line.
