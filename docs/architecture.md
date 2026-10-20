# Mr. Wolf — Architecture

> "Got a situation? Call Mr. Wolf."

Mr. Wolf is an AI-targeted memory profiler. Collectors gather memory telemetry and heap snapshots from running processes, store them, pre-compute analysis, and expose the results to an LLM through MCP tools so the LLM can explain *what happened and why* — without raw data ever flooding its context.

This document is the source of truth for the overall design. Implementation tasks live in `roadmap.md`; detailed specs live in `collector-dotnet.md` and `briefcase-format.md`.

**Scope right now: memory only.** CPU profiling may come later and must fit the same contracts.

---

## 1. Goals and non-goals

**Goals**
- Attach to an already-running .NET process in production **without restart** and observe its memory.
- Show a live, dotMemory-like memory chart (gen0/gen1/gen2/LOH/POH + unmanaged + process total).
- Detect anomalies automatically (rules) and capture evidence (heap snapshots) at the right moment.
- Pre-compute heavy analysis (dominators, retained size, diffs) so an LLM can answer "why is memory growing?" with small tool calls.
- Modular: more collectors later (Postgres, others) using the same contract.

**Non-goals (for now)**
- CPU profiling.
- .NET Framework support (no EventPipe there; would require ETW).
- The LLM as the alarm system. Detection is deterministic code; the LLM investigates.

---

## 2. Components

```
 Target process                    Collector host (Rust)             Core services
┌──────────────────┐ diagnostics  ┌──────────────────────┐  data   ┌─────────────────────────┐
│ .NET app         │ ◀──socket──▶ │ .NET collector        │ ──────▶ │ Storage                  │
│ (no agent in v1) │  EventPipe   │ Process (OS) collector│         │  - time series (DuckDB)  │
└──────────────────┘              │ (later: Postgres, …)  │         │  - events (DuckDB)       │
                                  └──────────▲───────────┘         │  - artifacts (.briefcase)│
                                             │ commands            ├─────────────────────────┤
                                             └─────────────────────│ Rules engine             │
                                                                   │ Analysis engine          │
                                                                   │ MCP server + MCP app (UI)│
                                                                   │ LLM (chat)               │
                                                                   └─────────────────────────┘
```

| Component | Responsibility | Must NOT do |
|---|---|---|
| **.NET collector** (in collector host) | Discover .NET processes, attach over the diagnostics socket, run EventPipe sessions, emit telemetry, execute commands (heap snapshot), write `.briefcase` artifacts, enforce safety limits | Analysis, anomaly detection, long-term storage, talking to the LLM |
| **Process collector** (in collector host) | Process-level OS memory for any PID (RSS/working set, private bytes, committed, container limit) | Anything runtime-specific |
| **Storage** | Durable time series, events, metadata, artifact index; retention/downsampling | Analysis |
| **Rules engine** | Evaluate rules over telemetry in near real time; write markers; send commands (e.g. take snapshot) | Heavy computation |
| **Analysis engine** | Dominators, retained size, per-type stats, path to root, snapshot diff; store small result tables | Collection |
| **MCP server** | High-level tools over storage + analysis; live chart data for the MCP app | Raw-data dumps into LLM context |
| **MCP app (UI)** | Live charts (from an in-memory ring buffer + storage), markers | |
| **Vincent** *(later, deep mode)* | Native Profiling API agent (Rust cdylib) for exact allocations, object lifetimes, GC handles | Anything EventPipe already provides |

**Guiding principle:** a collector is a **sensor and actuator**. It measures and executes commands. It does not decide, analyze, or store long-term.

---

## 3. Collector contract (all collectors)

Every collector implements the same contract so the rest of the system is collector-agnostic.

### 3.1 Target identity
`target_id` format: `pid:<pid>@<host>` (e.g. `pid:4812@srv-a`). Collectors also report target metadata: runtime/version, command line, start time.

### 3.2 Time-series point — a reading of a quantity
Something that always has a value; we sample it.
```text
{ ts_utc_ns, source: "dotnet", target: "pid:4812@srv-a",
  metric: "dotnet.gc.heap.size", labels: { generation: "gen2" },
  value: 157286400, unit: "bytes" }
```

### 3.3 Event — something that happened
Discrete occurrence with structured details; nothing exists between events.
```text
{ ts_utc_ns, source: "dotnet", target: "pid:4812@srv-a",
  kind: "dotnet.gc", data: { generation: 2, reason: "AllocSmall", type: "background",
                             pause_ms: 7.9, compacting: false } }
```
Rule of thumb: "what is it now?" → time series. "When did it happen and what were the details?" → event. Collectors send **raw events**; derived series (GCs per minute, pause p95) are computed by queries.

### 3.4 Capabilities
```text
{ source: "dotnet", capabilities: ["telemetry", "heap_snapshot", "alloc_sampling"],
  limits: { heap_snapshot: { min_interval_s: 300 } } }
```

### 3.5 Commands and results
```text
→ { command_id, command: "heap_snapshot", target, reason: "rule:gen2-growth", not_before?: ts }
← { command_id, status: "done" | "rejected" | "failed", artifact?: "<file>", details }
```
Collectors enforce their own safety limits (last line of defense) regardless of who sends the command.

### 3.6 Artifacts
Self-contained, versioned files (e.g. `.briefcase`) carrying their own metadata (when, which target, why) and resolved names, so they can be analyzed after the process is gone.

---

## 4. Data and storage

| Data | Volume | Store |
|---|---|---|
| Time series | Small (≈1.7M points/day per process at 20 metrics × 1 Hz) | DuckDB |
| Events | Small–medium | DuckDB |
| Artifacts (`.briefcase`) | Large (100 MB – several GB each) | Files on disk (object storage later), indexed in DuckDB |
| Metadata (targets, capabilities, artifact index, markers) | Tiny | DuckDB |
| Analysis results (per-type stats, top dominators, diffs) | Small tables | DuckDB |

Rules:
1. Collectors never persist long-term; they buffer briefly (optionally a small on-disk spool).
2. All storage access goes through a Rust storage **trait** so DuckDB can be swapped (ClickHouse, VictoriaMetrics) later.
3. Retention from day one: full-resolution telemetry kept a few days, then downsampled (1 s → 1 min); only last N snapshots or incident-linked snapshots kept.
4. High-frequency data (allocation-tick reconstruction, ms polling) goes to an in-memory ring buffer for the live UI and is stored only briefly at full resolution.
5. Timestamps: UTC, nanosecond precision, everywhere (needed to correlate .NET with Postgres later). Also keep a monotonic timestamp for intervals within one process when available.

---

## 5. Memory model shown to users (dotMemory-like)

```
total process memory    ← process collector (OS)
──────────────────────
unmanaged memory        = process total − GC committed     (computed)
LOH + POH               ┐
gen 2                   │ ← .NET collector (EventPipe)
gen 1                   │
gen 0                   ┘
```
- gen1/gen2/POH change only **at** GCs → draw as **step lines**, not interpolated.
- gen0/LOH/total/process change continuously → timer sampling + allocation-tick reconstruction.
- Leak signal: the **bottoms** of the managed-heap sawtooth keep rising (each GC frees less).
- Native vs managed signal: process grows while GC heap is flat → native problem.

---

## 6. Key decisions (decision log)

| # | Decision | Reason |
|---|---|---|
| D1 | Memory profiling first; CPU later | Focus |
| D2 | **Out-of-process EventPipe** is the foundation of the .NET collector | Attach to running prod apps without restart; documented, supported, low overhead |
| D3 | **Rust** for collector host, storage layer, analysis, MCP server | Single language; performance; safety |
| D4 | Profiling API agent (**Vincent**) only as optional **deep mode** later | Needed only for exact allocations, per-object lifetimes, per-handle tracking; startup-only flags |
| D5 | Separate **process (OS) collector** | GC heap is only part of process memory; needed for unmanaged band, OOM analysis, and Postgres |
| D6 | Detection by **rules engine** (code), investigation by **LLM** | LLM isn't continuously running; snapshots must be taken at the right moment |
| D7 | DuckDB + files, behind a storage trait | Local-first, zero setup, swappable |
| D8 | `.briefcase` = custom memory-mappable format | Instant open, no parsing, graph algorithms on flat arrays |
| D9 | Version every wire protocol and file format from day one | Formats will evolve; old snapshots must stay readable |
| D10 | Privacy by default: no string contents/field values in snapshots unless opted in | Heaps contain secrets/PII |
| D11 | Collector self-telemetry (own overhead, dropped events) | Distinguish "app slow" from "Mr. Wolf made it slow" |
| D12 | .NET 6+ targets only (EventPipe requires .NET Core 3.0+) | Modern runtimes |

---

## 7. Naming

| Name | Meaning |
|---|---|
| **Mr. Wolf** | Product. CLI binary `mrwolf` (alias `wolf`) |
| **briefcase** | Heap snapshot file (`*.briefcase`) |
| **situation** | Diagnosis report (`wolf situation`) |
| **cleanup** | Fix suggestions (`wolf cleanup`) |
| **Vincent** | Optional in-process Profiling API agent (deep mode, later) |
| **Jules** | Receiver/analysis side (collector host + analysis), informal name |

Planned CLI surface (not all in v1): `wolf watch`, `wolf situation`, `wolf snapshot`, `wolf open <file>`, `wolf diff <a> <b>`, `wolf allocs`, `wolf cleanup`.

---

## 8. End-to-end example (reference scenario)

App `OrderService` leaks: every request creates `OrderView`, which subscribes to static event `PriceFeed.PriceChanged` and never unsubscribes.

1. **Attach:** host finds the diagnostics socket of PID 4812, reads process info, opens a telemetry EventPipe session (GC events + `System.Runtime` counters at 1 s). Process collector starts sampling OS memory.
2. **Telemetry:** timer stream (heap total, alloc rate, process RAM) + GC stream (every gen1/gen2 GC in full; gen0 GCs aggregated per second). Chart: sawtooth with rising bottoms, gen2 staircase.
3. **Rule fires** at 03:14 ("gen2 grew >50% in 60 min, not reclaimed by gen2 GCs") → marker + two `heap_snapshot` commands 5 min apart.
4. **Snapshot:** host opens a second EventPipe session with the GC heap-dump keyword → runtime forces a GC and streams GCBulkType/Node/Edge/RootEdge → host builds `.briefcase` (two-pass build). Rate limit enforced.
5. **Analysis:** dominators, retained size, per-type totals, diff → small tables. Top growth: `OrderView` +41,210 objects, +86 MB retained.
6. **Chat:** user asks "why did memory grow?" → LLM calls `list_markers`, `get_series`, `diff_snapshots`, `path_to_root` → explains: static event → handler list → `OrderView` → `List<OrderLine>`; fix: unsubscribe / weak events. LLM context used: a few KB.
7. **Verify:** after fix, sawtooth bottoms flat, rule stays quiet.

---

## 9. Glossary

- **GC heap / managed heap** — memory holding .NET objects.
- **Generations (gen0/1/2)** — GC sorts objects by age; gen0 cheap/frequent, gen2 expensive.
- **LOH / POH** — Large Object Heap (~85 KB+ objects), Pinned Object Heap.
- **Root** — something the GC knows is alive (stack local, static via handle, GC handle, finalizer queue).
- **Shallow size** — bytes of one object. **Retained size** — bytes freed if that object died.
- **Dominator tree** — structure used to compute retained size (Lengauer–Tarjan / Cooper–Harvey–Kennedy).
- **Path to root** — reference chain explaining why an object is alive.
- **EventPipe** — runtime's cross-platform event tracing; reachable out-of-process via the diagnostics IPC socket.
- **Diagnostics IPC** — per-process named pipe / Unix socket every .NET Core 3.0+ process opens.
- **`.nettrace`** — EventPipe stream/file format.
- **Time series vs event** — see §3.
- **Flame graph** — call stacks drawn as stacked bars; width = how much (time or bytes).

## 10. References

- .NET diagnostics docs: EventPipe, diagnostics client library, `dotnet-counters`, `dotnet-gcdump`.
- `dotnet/diagnostics` repo — IPC protocol and `.nettrace` format documentation; source of `dotnet-trace`, `dotnet-gcdump`.
- `dotnet/runtime` — "Book of the Runtime" (`docs/design/coreclr/botr`), GC source.
- Microsoft.Diagnostics.Tracing.TraceEvent and PerfView source — reference implementations for parsing GC heap-dump events.
- "Pro .NET Memory Management" (Konrad Kokosa).
