# Mr. Wolf

> "Got a situation? Call Mr. Wolf."

Mr. Wolf is an AI-targeted memory profiler for .NET. It attaches to an already-running
process — no restart, no agent — streams memory telemetry over the diagnostics socket
(EventPipe), stores it locally in a sharded DuckDB layout, and is built to feed an LLM
small, pre-computed answers about *what happened and why* instead of raw data dumps.

**Status: early development.** Working today: attach to .NET 6+ processes, live GC/EventCounter
telemetry + OS memory, sharded time-segmented storage with drop-file retention. Heap
snapshots (`.briefcase`), analysis, rules and the MCP server are on the
[roadmap](docs/roadmap.md).

## Quickstart

```sh
# Terminal 1: something to watch
dotnet run --project samples/LeakyOrderService    # prints its PID

# Terminal 2: collect (add `watch` for a live view of heap, memory and GCs)
cargo run -p wolf-host -- 4812                    # ctrl-c to stop
cargo run -p wolf-host -- watch 4812              # same, with the live terminal view
cargo run -p wolf-host -- watch                   # pick targets and shards interactively
```

Telemetry lands in `wolf.db/shard-NN/` (`meta.duckdb`, one `seg-YYYY-MM-DD.duckdb` per
day, `longterm.duckdb` for downsampled history). See [INSTALL.md](INSTALL.md) for
prerequisites.

## How it works

- **Collectors are sensors and actuators** — they measure and execute commands (e.g. heap
  snapshot); they never analyze or decide.
- **The host is a dataflow graph** (TPL-Dataflow style): producer threads post records
  into bounded block buffers; per shard, a mapper block maps records to storage
  operations and a writer block persists them. Backpressure is explicit; completion
  cascades by dropping producers.
- **Storage is sharded and time-segmented**: one directory per shard, one file per UTC
  day; retention downsamples a sealed day and deletes its file.
- **Everything is versioned** (wire protocols, file formats) from day one.

The design rationale lives in [docs/architecture.md](docs/architecture.md); the collector
spec in [docs/collector-dotnet.md](docs/collector-dotnet.md); scalability reasoning in
[docs/scalability-notes.md](docs/scalability-notes.md).

## Repository layout

```
crates/
  wolf-contract    collector-agnostic types: targets, series, points, events, commands
  wolf-ipc         .NET diagnostics socket client (discover, ProcessInfo, EventPipe)
  wolf-nettrace    .nettrace (EventPipe stream) parser
  wolf-dotnet      GC correlation, EventCounters decoding, catalogue mapping
  wolf-process     OS process memory sampling (Linux /proc, Windows API)
  wolf-storage     Storage trait; sharded, day-segmented DuckDB layout + query layer
  wolf-host        the collector host: dataflow blocks, wiring, CLI binary (mrwolf)
  wolf-briefcase   heap snapshot file format (later phases)
samples/           .NET 10 apps with known memory behaviour (see samples/README.md)
testdata/          recorded golden streams for parser tests
docs/              architecture, collector spec, roadmap, scalability notes
```

## License

GPL-3.0-only.
