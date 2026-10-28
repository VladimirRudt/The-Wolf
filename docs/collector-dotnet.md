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

Verified against `dotnet/diagnostics` (`documentation/design-docs/ipc-protocol.md` and `Microsoft.Diagnostics.NETCore.Client`); implemented in `crates/wolf-ipc`.

**Linux/macOS:** each .NET Core 3.0+ process creates a Unix domain socket `dotnet-diagnostic-<pid>-<key>-socket` in the process's `$TMPDIR` (default `/tmp`). `<key>` is the process start time (jiffies since boot from `/proc/<pid>/stat` on Linux, Unix seconds on macOS). A reused PID can leave several sockets: the newest wins.
**Windows:** named pipe `\\.\pipe\dotnet-diagnostic-<pid>`.

**Messages:** 20-byte header — magic `"DOTNET_IPC_V1\0"` (14 bytes), `u16` total size (header + payload), `u8` command set, `u8` command id, `u16` reserved (0) — then the payload. Little-endian. Strings: `u32` length in UTF-16 units including the trailing NUL, then the units; length 0 = empty. Responses: `0xFF00` OK, `0xFFFF` error with an `i32` HRESULT payload (e.g. `0x80131385` UNKNOWN_COMMAND); the runtime closes the connection after an error.

Steps:
1. List sockets/pipes; extract PIDs; drop stale ones (Linux: no `/proc/<pid>`; elsewhere: connect fails).
2. Connect; send **ProcessInfo3** (`0x0408`), falling back to ProcessInfo2 (`0x0404`) and ProcessInfo (`0x0400`) on UNKNOWN_COMMAND, as `dotnet-counters` does. Payload: `u32` version (v3 only), `u64` pid, 16-byte runtime cookie (GUID), command line, OS, arch, then (v2+) entrypoint assembly and CLR product version, then (v3 payload version ≥ 1) runtime identifier. Newer versions append fields; ignore the rest. *The doc's C struct lists a different field order; the field list and the client code are authoritative.*
   Not covered yet: processes with a different `TMPDIR` and other mount namespaces (Microsoft's client finds them via `/proc/<pid>/environ` and `/proc/<pid>/root`).
3. Register target `pid:<pid>.<start>@<host>` (start time from the OS, see `architecture.md` §3.1) with metadata; emit `target.attached` event.
4. Open the telemetry session (§3).

**Containers:** collector must reach the socket (sidecar sharing `/tmp`) or use **reverse diagnostic ports** (`DOTNET_DiagnosticPorts`) where the app connects to the collector. Support for reverse ports: phase 2+.

**Permissions:** socket is accessible to the app's user or root; document this and fail with a clear error.

Lifecycle events: `target.attached`, `target.detached` (with reason: exited / socket closed / error), `session.started`, `session.lost`.

---

## 3. Telemetry session (long-lived)

Started with **CollectTracing3** (`0x0204`): `u32 circularBufferMB, u32 format (1 = NetTrace), bool requestRundown, bool requestStackwalk, array<provider>`, each provider `u64 keywords, u32 level, string name, string arguments` (`key=value;…`). Falls back to CollectTracing2 (`0x0203`, no stackwalk flag) on UNKNOWN_COMMAND. Telemetry sessions disable rundown and stacks. The OK response carries a `u64` session id; the raw nettrace stream follows on the same connection. **StopTracing** (`0x0201`, `u64` session id) goes on a new connection; the runtime then flushes and closes the stream (on Windows, the closed pipe surfaces as error 109/233 and is treated as end of stream). The streaming handshake is bounded by a 5 s timeout on both OSes: a socket read timeout on Unix; on Windows, where std has no pipe timeouts, the client polls `PeekNamedPipe` (non-consuming) until the whole response has arrived, then reads it. The ~100-byte request is written without a deadline: it fits the socket/pipe buffer. Verified against `EventPipeSession.cs` / `EventPipeSessionConfiguration.cs`; implemented in `wolf-ipc`: `DiagnosticsClient::start_session` is **blocking** (std I/O, called on the session's reader thread, D13) and returns an `EventSession` read with `read_chunk` until it returns 0; `stop_session` is async (tokio) on a new connection. The client remembers when a runtime lacks CollectTracing3, so the fallback costs one round trip per process, not per session.

Providers:

| Provider | Keywords | Level | Purpose |
|---|---|---|---|
| `Microsoft-Windows-DotNETRuntime` | GC (`0x1`) | Informational (4) | GCStart, GCEnd, GCHeapStats, GCSuspendEEBegin/End, GCRestartEEBegin/End |
| `System.Runtime` (EventCounters) | — | Informational | Counters; filter arg `EventCounterIntervalSec=1` (configurable) |
| *(phase 3)* `Microsoft-Windows-DotNETRuntime` | GC (`0x1`) | Verbose (5) | adds GCAllocationTick |

*(verify)* On .NET 9+, runtime metrics are also published via `System.Diagnostics.Metrics` (meter `System.Runtime`) with new names; EventCounters remain available. Start with EventCounters; keep an abstraction so the metrics source can switch.

### 3.1 Parsing
- Implement the `.nettrace` stream reader in Rust: header, metadata blocks (event schemas), event blocks, stack blocks, sequence point blocks.
- **Format version (verified in `dotnet/runtime` `src/native/eventpipe/` on release/6.0 … 10.0 and main):** every runtime we support writes **NetTrace v4 stream framing** (`Nettrace` + `!FastSerialization.1`, `Trace` object version 4, min reader version 4) **with v5 metadata tags** (OpCode, V2Params) inside metadata payloads. The serialization code is byte-identical in effect from .NET 6 to main (only refactors: explicit little-endian writers). No runtime writes v6 (no v6 constant exists in the writer); v6 is a reader-side format in TraceEvent for non-runtime producers. .NET Core 3.0/3.1 wrote v4 without metadata tags (out of scope, D12). Specs: PerfView `src/TraceEvent/EventPipe/NetTraceFormat_v5.md` (v4/v5, what we parse) and `NetTraceFormat.md` (v6). A v6 stream (different header) is rejected with an explicit unsupported-version error.
- Decode only the events we need; skip others cheaply.
- Keep per-session metadata (metadata id → provider, event id, name, version). Field descriptions and V5 tags are not read: payloads are decoded by known layouts (P1-4). The name matters for EventSource events (`EventCounters`); runtime events have an empty name.
- **Implemented in `wolf-nettrace` (P1-3), verified against the runtime writer (`ep-block.c`) and TraceEvent's reader:**
  - Event and metadata blocks always use header compression on NetTrace v4 (`ep-block.c`); an uncompressed block is skipped and counted.
  - Compressed sequence number: `+= delta + 1` when the capture-thread flag is set, else `+= 1` for non-metadata events (TraceEvent `ReadFromFormatV4`).
  - StackBlocks are framed and skipped: stacks are only recorded with stackwalk on (allocation sampling, P3-5).
  - Unknown block types, block versions above 2, and malformed block contents are skipped and counted; only broken framing ends the stream.
- **Privacy:** every session contains the runtime's `ProcessInfo` event (provider `Microsoft-DotNETCore-EventPipe`) with the target's **full command line**, which may contain user paths and secrets. The collector does not store it; golden recordings are scrubbed (same-length replacement keeps the stream valid) before committing.
- **Golden tests:** `tools/TraceCheck` (C#, TraceEvent 4.0.0, dynamic parser only so event ids stay raw) writes `testdata/nettrace/<name>.expected.txt`; `crates/wolf-nettrace/tests/golden.rs` parses every recording at several read sizes and must produce identical lines. Run TraceCheck manually when adding a recording (not in CI).
- **Chunks do not align with nettrace blocks.** `EventSession::read_chunk` returns whatever bytes the pipe has: a chunk can end in the middle of a block header, an event, or even a single integer, and one chunk can hold several blocks. The parser must therefore be **incremental**: it emits every complete block it can and keeps the incomplete tail for the next chunk. Blocks are padded to 4-byte **stream** offsets (counted from the `Nettrace` magic), so the parser tracks the stream offset of its buffer. It must never assume a read starts or ends on a boundary, and its golden tests must replay recordings split at arbitrary points (including 1-byte chunks) and get identical output.
- **Block size is bounded (verified):** the runtime allocates event, metadata and stack blocks at `100 * 1024` bytes each (`ep-file.c`, `ep_event_block_alloc` / `ep_metadata_block_alloc` / `ep_stack_block_alloc`); sequence point blocks are smaller. So the parser never holds more than one partial block plus one chunk.
- **Buffer: fixed size, allocated once, never grown** (~256 KB per session). The reader reads straight into it, like `PipeWriter.GetSpan`/`Advance`:
  - `spare()` returns the free space after `end`. It never clears memory: when everything is parsed it resets the cursors (`start = end = 0`); only when the tail has less than one max block free does it move the unparsed tail to the front (`copy_within`, ≤ 100 KB).
  - `commit(n, on_item)` advances `end`, parses every complete block, advances `start`, and passes each item to the callback with its payload **borrowed** from the buffer (no per-event allocation; the borrow ends when the callback returns).
  - A block header larger than the buffer is a corrupt stream: error, end the session (no resync possible).
- **Backpressure is the blocking read itself:** a slow callback stops `read_chunk`, the pipe fills, the runtime's circular buffer fills, and the runtime **drops** events rather than blocking the app. There is no queue between reading and parsing, so nothing grows; drops show up as sequence-number gaps and are counted in self-telemetry.
- **Events are emitted in file order; the parser never sorts.** The runtime does not time-sort the stream (NetTrace v5 spec, "Event TimeStamp sorting"); sorting would mean holding events back and copying their payloads. No planned consumer infers anything from arrival order: GC events are correlated by GC number, durations come from timestamps, storage orders by `ts` on query. The `IsSorted` flag is parsed past and not exposed.
- **Sequence points are rare** — the runtime writes one per 10 MB of event buffers, not on a timer (`ep-session.c`, `ep-buffer-manager.c`); telemetry recordings contain a single SPBlock at the end. They are not usable as a time watermark. The parser uses them only for **dropped-event detection**: per capture thread, `SequenceNumber` must increase by 1 (a gap = dropped events; a reset to 1 = thread id reused, treated as a new thread), and a sequence point's per-thread numbers expose threads whose events are all being dropped.
- The parser is synchronous and runtime-agnostic (no tokio, no threads of its own, D13): it runs on the session's reader thread.

### 3.2 GC event handling
- **Pause duration** = `GCRestartEEEnd.ts − GCSuspendEEBegin.ts` for the GC (more accurate than GCStart/GCEnd).
- From `GCStart`: count (GC number), depth (generation), reason, type (blocking / background / foreground).
- From `GCHeapStats` (V2 where available): per-generation size and promoted bytes for gen0/1/2, LOH, POH; finalization-promoted count; pinned object count; GC handle count *(verify field names per version)*.
- Correlate events of one GC by GC number, never by file order (events are not time-sorted, §3.1).

**Aggregation (volume control):**
- gen1 and gen2 GCs → one full `dotnet.gc` event each.
- gen0 GCs → one `dotnet.gc.gen0.summary` event per second: `{ count, total_pause_ms, max_pause_ms }`, plus the latest `dotnet.gc.heap.size` points. Why: gen0 can run at 100+ GCs/s on a busy server (~500 heap-size points/s, far over the per-target budget in `architecture.md` §4).
- The summary is **GC-driven, not clock-driven**: each completed gen0 GC is added to the pending summary; when a GC completes ≥ 1 s (by its own timestamp) after the summary started, the summary is emitted. No sorting or watermark needed. Trade-off: when the app goes idle, the last summary is emitted at the next GC or at session stop (live heap totals still come from counters, §3.3).

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

Self-telemetry (metric prefix `wolf.`): `wolf.collector.events_received`, `wolf.collector.events_dropped`, `wolf.collector.bytes_received`, `wolf.collector.cpu`, `wolf.collector.memory`, `wolf.collector.lag_ms`.

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

The .NET collector reports `{ name: "dotnet", commands: ["heap_snapshot"] }` (`architecture.md` §3.4). Command name, params and artifact kind `briefcase` are constants in `wolf-dotnet`, not in `wolf-contract`.

### 6.1 Safety checks (collector-enforced)
- `min_interval_s` per target (default 300).
- At most 1 snapshot in progress per target, and a global max concurrency (default 1).
- Free disk space check (estimate ≈ 40 bytes/object; reject if insufficient).
- Optional: reject if heap > configured max size.
- Rejections return `status: "rejected"` with the reason in `message`; emit an event.

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
{ command_id, status: "done",
  artifact: { kind: "briefcase", path: "srv-a_4812_2026-10-19T03-14-05.briefcase" } }
```
Measurements go into the `dotnet.snapshot.finished` event: `{ command_id, artifact, objects: 3104552, size_mb: 105, pause_ms: 640 }`. Plus artifact index row in storage.

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
