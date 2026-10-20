# Mr. Wolf — Roadmap and tasks

Phased plan. Each task is sized to be implemented in one focused session (human + AI assistant) and has a **Done when** checklist. Read `architecture.md` first; specs are in `collector-dotnet.md` and `briefcase-format.md`.

## Ground rules for implementation (for AI assistants)

1. **Respect component boundaries** (`architecture.md` §2). Collectors don't analyze; analysis doesn't collect.
2. **Verify protocols against primary sources** before coding: `dotnet/diagnostics` docs (IPC, `.nettrace`), runtime event manifest, `dotnet-gcdump` / `dotnet-counters` / TraceEvent source. Spec items marked *(verify)* are not authoritative.
3. **Every parser gets golden-file tests** (recorded real streams in `testdata/`).
4. **Never crash on unknown input** — skip and count.
5. **Version formats** (wire and file) from the first commit.
6. Use the names from the metric catalogue (`collector-dotnet.md` §4) exactly.
7. Keep the reference scenario (`architecture.md` §8) working end to end as features land.

---

## Phase 0 — Repository foundation

| ID | Task | Done when |
|---|---|---|
| P0-1 | Cargo workspace with crates from `collector-dotnet.md` §7 (empty stubs), `rustfmt`, `clippy`, CI (build + test on Linux and Windows) | `cargo test` passes in CI on both OSes |
| P0-2 | `docs/` with these documents; `AGENTS.md` (or `CLAUDE.md`) pointing AI assistants to them and listing ground rules | Docs in repo; agent file present |
| P0-3 | `samples/` .NET 10 apps: `LeakyOrderService` (static event leak — reference scenario), `SteadyAllocator`, `LohChurn`, `NativeLeak` | Each runs with `dotnet run`; leak visible in `dotnet-counters` |
| P0-4 | `wolf-contract`: types Point, Event, Capability, Command, CommandResult, ArtifactRef, TargetId; serde; schema version constant | Unit tests for (de)serialization |

## Phase 1 — "Watch": live telemetry

| ID | Task | Done when |
|---|---|---|
| P1-1 | `wolf-ipc`: discover diagnostics sockets/pipes; connect; ProcessInfo command | Lists running .NET processes with runtime version and command line on Linux and Windows |
| P1-2 | `wolf-ipc`: CollectTracing / StopTracing commands; return a byte stream | Can start a session against a sample app and save the raw stream to a `.nettrace` file that PerfView/TraceEvent can open |
| P1-3 | `wolf-nettrace`: parser for header, metadata, event, stack and sequence-point blocks; yields typed events with payload bytes | Golden-file tests pass on streams recorded from P1-2 |
| P1-4 | `wolf-dotnet`: decode GCStart, GCEnd, GCHeapStats, suspend/restart events; produce `dotnet.gc` events with pause; gen0 per-second aggregation | Event counts and gen sizes match `dotnet-trace` + PerfView for the same run (± rounding) |
| P1-5 | `wolf-dotnet`: EventCounters (`System.Runtime`) → timer series per catalogue | Values match `dotnet-counters` within tolerance |
| P1-6 | `wolf-process`: OS memory metrics (Linux `/proc` + cgroup; Windows `GetProcessMemoryInfo`) | Values match `top`/`ps` / Task Manager |
| P1-7 | `wolf-storage`: storage trait; DuckDB impl for points, events, targets; batch inserts; range queries; retention job (downsample 1 s → 1 min) | Insert 1M points < few seconds; range query tests; retention test |
| P1-8 | `wolf-host`: run collectors per target, route to storage, self-telemetry, graceful detach | Runs for 1 h against `SteadyAllocator` with stable memory and no drops |
| P1-9 | `wolf watch <pid>`: terminal view (table or simple TUI chart) of gen sizes, heap total, process memory, GC events | Developer can watch `LeakyOrderService` grow live |

**Milestone 1:** attach to a running app with no restart and watch its memory live; data persists in DuckDB.

## Phase 2 — "Briefcase": heap snapshots and analysis

| ID | Task | Done when |
|---|---|---|
| P2-1 | `wolf-dotnet`: heap-dump session (replicate `dotnet-gcdump` config); spool GCBulk* events to temp file | Capture completes on sample apps; session closed; telemetry session unaffected |
| P2-2 | `wolf-briefcase`: writer (two-pass build, atomic rename, checksum) | Briefcase per-type object counts match `dotnet-gcdump report` for the same moment (± a few %) |
| P2-3 | `wolf-briefcase`: mmap reader with validation; fuzz test the validator | Corrupted files rejected without panics (fuzz run) |
| P2-4 | Safety limits for `heap_snapshot` (interval, concurrency, disk space) + snapshot events + artifact index in storage | Limit tests; rejected commands return reasons |
| P2-5 | Analysis: per-type stats (count, shallow) → DuckDB tables | Matches P2-2 counts |
| P2-6 | Analysis: dominators (start with Cooper–Harvey–Kennedy; Lengauer–Tarjan later) + retained size → sidecar | Verified on small hand-built graphs; runs on a 5M-object heap in reasonable time |
| P2-7 | Analysis: path to root (BFS from virtual root, parents sidecar) | Returns static event chain in `LeakyOrderService` |
| P2-8 | Analysis: diff of two snapshots by type name (count, shallow, retained deltas) | `OrderView` top growth in reference scenario |
| P2-9 | CLI: `wolf snapshot <pid>`, `wolf open <file>` (top types, top dominators), `wolf diff <a> <b>`, `wolf path <file> <type>` | Reference scenario diagnosable from CLI alone |

**Milestone 2:** manual leak investigation end to end from the CLI.

## Phase 3 — Rules, situation, allocations

| ID | Task | Done when |
|---|---|---|
| P3-1 | Rules engine: rule definitions (config file), evaluation over recent telemetry, `rule.fired` markers | Unit tests with synthetic series |
| P3-2 | Rule actions: send commands (e.g. two snapshots N minutes apart) | Reference scenario captures two briefcases automatically |
| P3-3 | Built-in rules: gen2 growth not reclaimed, LOH churn, high GC pause, high time-in-GC, process grows while GC heap flat (native leak), near container limit | Each rule fires on its sample app and stays quiet on `SteadyAllocator` |
| P3-4 | `wolf situation`: report from telemetry + rules + latest analysis | Readable report for each sample app |
| P3-5 | Allocation sampling (GCAllocationTick, Verbose level) with stacks; aggregation by (type, stack); symbol resolution via method load/rundown events | Top allocating types/stacks match PerfView for the same run |
| P3-6 | gen0/LOH high-frequency reconstruction → in-memory ring buffer | Live chart smooth between GCs |
| P3-7 | `wolf allocs`: top types, top stacks; folded-stack output for flame graphs (`inferno`) | Flame graph renders for `SteadyAllocator` |

## Phase 4 — MCP server and app

| ID | Task | Done when |
|---|---|---|
| P4-1 | MCP server skeleton (Rust) with read-only tools: `list_targets`, `list_markers`, `get_series` (with server-side downsampling), `list_events` | Tools callable from an MCP client |
| P4-2 | Analysis tools: `snapshot_summary`, `top_types`, `top_dominators`, `diff_snapshots`, `path_to_root`, `situation` | Reference scenario answered by an LLM using only these tools |
| P4-3 | Action tools with guardrails: `take_snapshot` (rate-limited, confirmation for production targets) | Limits enforced server-side and by collector |
| P4-4 | MCP app: live stacked memory chart (gen0/1/2/LOH/POH + unmanaged + process total), GC and rule markers | Chart matches the model in `architecture.md` §5 |
| P4-5 | Tool output budgets: every tool returns small, summarized results; truncation with "more available" hints | No tool returns > a few KB by default |
| P4-6 | Privacy: no string contents in any tool output | Review + test |

**Milestone 3:** the reference scenario is investigated by an LLM in chat.

## Phase 5 — Later

- **Vincent** deep mode (Rust cdylib, Profiling API): exact allocations, object lifetimes (MovedReferences2/SurvivingReferences2), GC handles, ms-level `GetGenerationBounds` polling.
- Reverse diagnostic ports for containers/Kubernetes.
- Postgres collector (same contract; memory via OS collector + Postgres views).
- Storage backends beyond DuckDB (ClickHouse / VictoriaMetrics).
- `wolf cleanup`: pattern detectors (duplicate strings needs opt-in string capture, event-handler leaks, undisposed finalizable objects, LOH churn).
- CPU profiling.

---

## Open questions

- Exact `.nettrace` version(s) to support first (verify against runtime versions in use at work).
- Whether EventPipe heap dumps provide per-object generation; if not, how to derive it.
- Rule configuration format (TOML/YAML).
- UI tech for the MCP app chart.
