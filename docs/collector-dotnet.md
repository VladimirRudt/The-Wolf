# Mr. Wolf — .NET collector and process collector spec

Part of the **collector host** (Rust). Implements the collector contract from `architecture.md` §3.

> **Note for implementers (human or AI):** exact event field layouts, keyword bit values, IPC message layouts and counter names below must be **verified against primary sources** before coding: the `dotnet/diagnostics` repo docs (IPC protocol, `.nettrace` format), the runtime event manifest (`ClrEtwAll.man` in `dotnet/runtime`), and the source of `dotnet-gcdump` / `dotnet-counters` / TraceEvent. Values marked *(verify)* are from memory and may be imprecise.

---

## 1. Responsibilities

1. Discover .NET processes and their metadata.
2. Attach over the diagnostics IPC channel (no restart, no agent).
3. Run a long-lived **telemetry session** per target.
4. Execute **commands**: `heap_snapshot` (v1), `alloc_sampling` (later).
5. Normalize everything to the common data model (time-series points, events, artifacts).
6. Enforce safety limits.
7. Report self-telemetry.

Out of scope: analysis, anomaly detection, long-term storage, LLM interaction.

---

## 2. Discovery and attach

**Linux/macOS:** each .NET Core 3.0+ process creates a Unix domain socket in `$TMPDIR` (default `/tmp`) named like `dotnet-diagnostic-<pid>-<key>-socket` *(verify)*.
**Windows:** named pipe `\\.\pipe\dotnet-diagnostic-<pid>` *(verify)*.

Steps:
1. List candidate sockets/pipes; extract PIDs; drop stale ones (process not alive).
2. Connect; send **ProcessInfo** command → runtime version, command line, OS/arch *(verify command id / response layout)*.
3. Register target `pid:<pid>@<host>` with metadata; emit `target.attached` event.
4. Open the telemetry session (§3).

**Containers:** collector must reach the socket (sidecar sharing `/tmp`) or use **reverse diagnostic ports** (`DOTNET_DiagnosticPorts`) where the app connects to the collector. Support for reverse ports: phase 2+.

**Permissions:** socket is accessible to the app's user or root; document this and fail with a clear error.

Lifecycle events: `target.attached`, `target.detached` (with reason: exited / socket closed / error), `session.started`, `session.lost`.

---

## 3. Telemetry session (long-lived)

Started with the IPC **CollectTracing** command (version 2+ supports rundown flag) *(verify)*. Disable rundown for telemetry sessions.

Providers:

| Provider | Keywords | Level | Purpose |
|---|---|---|---|
| `Microsoft-Windows-DotNETRuntime` | GC (`0x1`) | Informational (4) | GCStart, GCEnd, GCHeapStats, GCSuspendEEBegin/End, GCRestartEEBegin/End |
| `System.Runtime` (EventCounters) | — | Informational | Counters; filter arg `EventCounterIntervalSec=1` (configurable) |
| *(phase 3)* `Microsoft-Windows-DotNETRuntime` | GC (`0x1`) | Verbose (5) | adds GCAllocationTick |

*(verify)* On .NET 9+, runtime metrics are also published via `System.Diagnostics.Metrics` (meter `System.Runtime`) with new names; EventCounters remain available. Start with EventCounters; keep an abstraction so the metrics source can switch.

### 3.1 Parsing
- Implement the `.nettrace` stream reader in Rust: header, metadata blocks (event schemas), event blocks, stack blocks, sequence point blocks *(verify format version — "Nettrace" v4/v5)*.
- Decode only the events we need; skip others cheaply.
- Keep per-session metadata (event id → schema).

### 3.2 GC event handling
- **Pause duration** = `GCRestartEEEnd.ts − GCSuspendEEBegin.ts` for the GC (more accurate than GCStart/GCEnd).
- From `GCStart`: count (GC number), depth (generation), reason, type (blocking / background / foreground).
- From `GCHeapStats` (V2 where available): per-generation size and promoted bytes for gen0/1/2, LOH, POH; finalization-promoted count; pinned object count; GC handle count *(verify field names per version)*.
- Correlate events of one GC by GC number / ordering.

**Aggregation (volume control):**
- gen1 and gen2 GCs → one full `dotnet.gc` event each.
- gen0 GCs → one `dotnet.gc.gen0.summary` event per second: `{ count, total_pause_ms, max_pause_ms }`, plus the latest `dotnet.gc.heap.size` points.

### 3.3 Timer-driven telemetry
From counters every interval: total managed heap (includes allocations since last GC), GC committed bytes, fragmentation, allocation rate, GC counts, time in GC, working set *(verify exact counter names: e.g. `gc-heap-size`, `gc-committed`, `gc-fragmentation`, `alloc-rate`, `working-set`, `time-in-gc`)*.

### 3.4 High-frequency reconstruction (phase 3)
Using `GCAllocationTick` (~every 100 KB allocated; includes allocation kind small/large and amount *(verify)*):
- `gen0_estimate = gen0_after_last_gc + Σ small ticks since last GC`
- `loh_estimate = loh_after_last_gc + Σ large ticks since last GC`
Push estimates to the live ring buffer; store full resolution only briefly.

---

## 4. Metric catalogue (v1)

Time series (`metric` → `labels` → unit):

| Metric | Labels | Unit | Source | Cadence |
|---|---|---|---|---|
| `dotnet.gc.heap.size` | `generation`: gen0/gen1/gen2/loh/poh | bytes | GCHeapStats | per GC (step series) |
| `dotnet.gc.promoted` | `generation` | bytes | GCHeapStats | per GC |
| `dotnet.heap.total` | — | bytes | counter | timer |
| `dotnet.gc.committed` | — | bytes | counter | timer |
| `dotnet.gc.fragmentation` | — | percent | counter | timer |
| `dotnet.alloc.rate` | — | bytes/s | counter | timer |
| `dotnet.gc.time_in_gc` | — | percent | counter | timer |
| `dotnet.gc.pinned_objects` | — | count | GCHeapStats | per GC |
| `dotnet.gc.finalization_pending` | — | count | GCHeapStats | per GC |
| `dotnet.gc.handles` | — | count | GCHeapStats | per GC |

Events:

| Kind | Data |
|---|---|
| `dotnet.gc` | gc_number, generation, reason, type, pause_ms, compacting? |
| `dotnet.gc.gen0.summary` | count, total_pause_ms, max_pause_ms |
| `dotnet.snapshot.started` / `.finished` / `.failed` | command_id, artifact, objects, size, pause_ms, error |
| `target.attached` / `target.detached` | metadata / reason |
| `session.started` / `session.lost` | session kind, reason |

Self-telemetry (`source: "wolf"`): `wolf.collector.events_received`, `wolf.collector.events_dropped`, `wolf.collector.bytes_received`, `wolf.collector.cpu`, `wolf.collector.memory`, `wolf.collector.lag_ms`.

---

## 5. Process (OS) collector

Generic, works for any PID (shared with future collectors).

| Metric | Linux | Windows |
|---|---|---|
| `process.memory.rss` | `/proc/<pid>/status` VmRSS | `GetProcessMemoryInfo` WorkingSetSize |
| `process.memory.private` | `/proc/<pid>/smaps_rollup` Private_* | PrivateUsage |
| `process.memory.virtual` | VmSize | — / PagefileUsage |
| `process.memory.limit` | cgroup v2 `memory.max` (and `memory.current`) | job object limit if any |
| `process.threads` | `/proc/<pid>/status` Threads | thread count |

Cadence: 1 s default; configurable down to ~100 ms.
Derived (by storage/analysis, not the collector): `unmanaged = process.memory.private − dotnet.gc.committed`.

---

## 6. Command: `heap_snapshot`

### 6.1 Safety checks (collector-enforced)
- `min_interval_s` per target (default 300).
- At most 1 snapshot in progress per target, and a global max concurrency (default 1).
- Free disk space check (estimate ≈ 40 bytes/object; reject if insufficient).
- Optional: reject if heap > configured max size.
- Rejections return `status: "rejected"` with a reason; emit an event.

### 6.2 Capture (EventPipe heap dump, same approach as `dotnet-gcdump`)
1. Open a **second, separate** EventPipe session with the GC heap-dump keyword set (GC + GCHeapCollect + GCHeapDump + Type + GCHeapAndTypeNames, level Verbose) *(verify exact keywords by reading `dotnet-gcdump` source)*. This triggers an induced full GC and a heap walk.
2. Receive and spool to a temp file, without processing:
   - `GCBulkType` — type id → name, flags
   - `GCBulkNode` — object address, size, type id, edge count
   - `GCBulkEdge` — referenced address (edges belong to nodes in order, by edge count)
   - `GCBulkRootEdge` — root object address, root kind, flags, root id
   - `GCBulkRootStaticVar`, `GCBulkRootConditionalWeakTableElementEdge` and other root/edge kinds as encountered *(verify list)*
3. Detect completion (GC end for the induced GC / end markers *(verify)*), then stop the session.
4. Telemetry session keeps running throughout.
5. Measure pause (from the telemetry session's suspend/restart events) and record it.

### 6.3 Build (`.briefcase`)
Two-pass build, off the hot path — see `briefcase-format.md` §4.

### 6.4 Result
```text
{ command_id, status: "done", artifact: "srv-a_4812_2026-10-19T03-14-05.briefcase",
  objects: 3104552, size_mb: 105, pause_ms: 640 }
```
Plus artifact index row in storage.

---

## 7. Internal structure (suggested crates/modules)

```
crates/
  wolf-contract     # data model: Point, Event, Capability, Command, Artifact; serde
  wolf-ipc          # diagnostics IPC client (Unix socket / named pipe), commands
  wolf-nettrace     # .nettrace stream parser
  wolf-dotnet       # .NET collector: discovery, sessions, GC handling, aggregation, snapshot capture
  wolf-process      # OS process collector
  wolf-briefcase    # format writer/reader (mmap)
  wolf-storage      # storage trait + DuckDB implementation
  wolf-host         # binary: runs collectors, routes data to storage, accepts commands
```
Async runtime: tokio. Mmap: `memmap2`. Zero-copy casts: `bytemuck` or `zerocopy`. DuckDB: `duckdb` crate.

---

## 8. Error handling

- Process exits mid-session → `target.detached { reason: "exited" }`, flush buffers, keep data.
- Session stream error → retry once with backoff; then `session.lost`.
- Snapshot interrupted → delete temp files, `dotnet.snapshot.failed`, never leave a partial `.briefcase` in place.
- Unknown events/fields → skip and count (self-telemetry), never crash.

## 9. Testing

- **Test apps** (in repo under `samples/`): a leaky ASP.NET app (static event subscription, the reference scenario), a steady allocator, an LOH-heavy app, a native-leak app (`NativeMemory.Alloc`).
- **Cross-checks:** compare against `dotnet-counters` (telemetry) and `dotnet-gcdump` + PerfView/`dotnet-gcdump report` (snapshot object counts/sizes per type).
- **Golden files:** record `.nettrace` streams once; unit-test the parser against them.
