# Mr. Wolf — guide for AI assistants

Mr. Wolf is an AI-targeted .NET memory profiler written in Rust. Collectors attach to running .NET processes over the diagnostics socket (EventPipe), stream memory telemetry, capture heap snapshots (`.briefcase` files), and expose pre-computed analysis to an LLM through MCP tools.

## Read before working

- `docs/architecture.md` — overall design, component boundaries, collector contract, decisions. **Always read this first.**
- `docs/roadmap.md` — phased tasks with "Done when" criteria. Work on one task ID at a time.
- `docs/collector-dotnet.md` — .NET collector and OS process collector spec (IPC, EventPipe, metric catalogue, snapshots).
- `docs/briefcase-format.md` — heap snapshot file format (memory-mapped, columnar, CSR graph).

## Ground rules

1. Respect component boundaries: collectors measure and execute commands; they never analyze, detect anomalies, or store long-term.
2. Protocol details marked *(verify)* in the docs are not authoritative. Verify against primary sources before coding: `dotnet/diagnostics` docs (IPC protocol, `.nettrace` format), the runtime event manifest (`ClrEtwAll.man` in `dotnet/runtime`), and the source of `dotnet-gcdump`, `dotnet-counters`, TraceEvent.
3. Every parser gets golden-file tests using recorded real streams in `testdata/`.
4. Never crash on unknown input: skip it and count it in self-telemetry.
5. Version every wire protocol and file format from the first commit.
6. Use metric and event names from `docs/collector-dotnet.md` §4 exactly.
7. Privacy: never capture or output string contents or field values from heaps unless explicitly opted in.
8. Keep the reference scenario (`docs/architecture.md` §8, `samples/LeakyOrderService`) working end to end.
9. Timestamps are UTC with nanosecond precision everywhere.

## Tech stack

Rust (tokio, memmap2, bytemuck/zerocopy, duckdb). Cargo workspace with crates listed in `docs/collector-dotnet.md` §7. Sample apps in `samples/` target .NET 8.

## Commands

- Build: `cargo build --workspace`
- Test: `cargo test --workspace`
- Lint: `cargo clippy --workspace -- -D warnings` and `cargo fmt --check`
