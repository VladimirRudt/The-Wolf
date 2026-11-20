# Installing and running Mr. Wolf

## Prerequisites

| Tool | Why | Notes |
|---|---|---|
| **Rust stable** (rustup) | building | `rust-toolchain.toml` pins the channel and adds `rustfmt` + `clippy` automatically |
| **.NET 10 SDK** | the sample apps and the `TraceCheck` test-data tool | only needed to run samples / regenerate golden files |
| Windows or Linux | supported targets | macOS is untested |

DuckDB is compiled from source (`bundled` feature), so the first build needs a C++
toolchain (MSVC Build Tools on Windows, `build-essential`/`clang` on Linux) and takes a
few minutes.

## Build

```sh
cargo build --workspace --release
```

The collector binary is `target/release/wolf-host` (it calls itself `mrwolf` in its
output; the dedicated CLI comes with later roadmap phases).

## Run

Start a sample app (it prints its PID), then attach:

```sh
dotnet run --project samples/LeakyOrderService   # or SteadyAllocator / LohChurn / NativeLeak
mrwolf --db wolf.db --shards 4 <pid> [<pid>...]
```

- `--db <dir>` — store directory; one `shard-NN/` subdirectory per shard
  (default: `wolf.db`). Not a single file.
- `--shards <n>` — physical shards: one mapper → writer block pair each (default: 4).
  Targets are placed by stable hash, so changing this never reshuffles old files.
- Stop with ctrl-c: sessions are stopped gracefully, buffers drained, everything flushed.

While running, the host prints a status line per minute (targets, events, drops, points
written). The store can be inspected after the host stops with any DuckDB client —
`QueryStore` unions all shard files (`targets`, `series`, `events`, `points`,
`points_1m`); live reads while the host is running are not supported yet (DuckDB allows
one writer *or* many readers per file).

## Verify the checkout

```sh
cargo test --workspace        # unit + golden-file tests (testdata/)
```
