# Scalability notes — sharded pipeline architecture

> Working notes from the 2026 scalability redesign of `wolf-host`/`wolf-storage`. The
> normative design lives in `docs/architecture.md` (§4.1, D13, D14); this file collects
> the reasoning, the industry references, and what is still deferred.

## The question

The original host had one router thread: it drained every source's ring, mapped messages,
batched, wrote to one DuckDB file, and also ran retention on the side. Two concerns:

1. One router "looks" like a bottleneck.
2. The router mixed too many jobs: routing, mapping, writing, retention scheduling.

## What the literature/industry says

- **SEDA** (Welsh, Culler, Brewer, 2001 — [paper](https://www.mdw.la/papers/seda-sosp01.pdf)):
  decompose into *stages connected by bounded queues*; overload must be visible at
  enqueue time (backpressure / load shedding at ingress). Cassandra's stage pools descend
  from this.
- **OpenTelemetry Collector** ([architecture](https://signoz.io/blog/opentelemetry-architecture/)):
  `receivers → processors → exporters` pipelines; batching and memory limiting are their
  own stages; scale-out is horizontal (agent → gateway), never one giant collector.
- **Vector** ([buffering/backpressure](https://docs.datadoghq.com/observability_pipelines/scaling_and_performance/buffering_and_backpressure/)):
  sources → transforms → sinks; **each sink owns its buffer**; a stalled destination
  backpressures only its own sources.
- **VictoriaMetrics vmagent** ([sharding docs](https://docs.victoriametrics.com/victoriametrics/vmagent/)):
  one agent stays simple; too many targets → shard targets across N agents by stable hash,
  optionally replicated for HA with store-side dedup.
- **Kafka/TSDB principle**: scalability = *partitioning so each partition has exactly one
  writer*, never many writers to one structure.

One-sentence summary: **scale by adding partitions and processes, not writers; keep each
stage small enough to name.**

## Key reframe

"The single writer" does not mean one thread for the whole system. It means **one writer
per partition, many partitions**. The partition (shard) is the unit of parallelism,
ordering, retention, and failover. Our natural partition key is `target_id` — stable,
coordination-free, and all ordering requirements are per-series (hence per-target).

## What we built (D14)

```
readers/sampler  →  bounded ring channels  →  pipeline[shard] = transform + sink
                                                 └ single writer of shard-NN/ files

<db-dir>/shard-00/
  meta.duckdb            — targets, series, events, wolf_meta
  longterm.duckdb        — points_1m (downsampled history)
  seg-YYYY-MM-DD.duckdb  — raw points of one UTC day
```

1. **Sharded pipelines** — N pipeline threads (default 4), each owning its targets
   exclusively; zero shared mutable state between pipelines.
2. **One store directory per shard**, one DuckDB connection, ATTACHed segment catalogs
   (`appender_to_catalog_and_db` for writes).
3. **Day-segment files + drop-file retention** — retention downsamples a sealed day into
   `longterm.points_1m` (idempotent anti-join) and removes the file. No DELETE on the raw
   table; retention never competes with inserts; disk is actually reclaimed.
4. **Fixed 256 logical shards + placement map** — `FNV-1a(target_id) % 256 % N`. Changing
   the physical count remaps whole buckets; old files stay put (the query layer unions
   everything). This answers the "autoscaling reshuffles data" objection: you remap
   placement, you never reshard data.
5. **QueryStore** — read-only union over all shard files; the surface for CLI/MCP/TUI.

Actor names follow streaming vocabulary (Vector-style): **sources** (session per target,
process sampler) → **channels** → **pipeline** (transform → sink).

## High-load scenarios and the mechanism that answers each

| Scenario | First bottleneck | Mechanism |
|---|---|---|
| Many targets, one host | CPU on parse/mapping | More pipelines (shards); mapping is pure and parallel per target |
| One host not enough | — | Target→host hash sharding, vmagent-style; `target_id` needs no protocol change |
| Storage slower than ingestion | DuckDB write latency | *(deferred)* durable spool/WAL + tiered load shedding; drop-file retention already removes the DELETE contention |
| Burst from one target (GC storm) | one source's ring | Per-shard isolation: a hot target saturates one pipeline, others unaffected |

## Deferred / open

- **Durable spool (WAL)** between sources and pipelines: per-shard segment files, ack
  after sink flush, replay on restart; tiered load shedding when full (gen0 summaries and
  counters first, lifecycle events last). Listed in roadmap Phase 5.
- **Live cross-shard reads while the host runs**: DuckDB allows one writer *or* many
  readers per file. Today `QueryStore` works after the host stops (or on a copy). The MCP
  server (P4) will need to broker live reads — e.g. query-through-pipeline or a read
  replica handoff.
- **Multi-host placement**: the 256-bucket map is ready for `bucket_range → host`
  assignment with replication + store-side dedup (`(series_id, ts_utc_ns)` is a natural
  idempotency key).
- Storage-engine swap (ClickHouse/VictoriaMetrics) stays possible via the `Storage`
  trait (D7); `DuckDbStore` remains as the simple single-file backend.

## Verification state

- `wolf-storage`: shard segmentation, unioned reads, drop-file retention with longterm
  aggregate check, reopen persistence, cross-shard `QueryStore` — all tested
  (`tests/shard_store.rs`, `shard.rs` unit tests).
- `wolf-host`: pipeline mapping/lifecycle/self-telemetry tests (`tests/pipeline.rs`),
  placement stability/spread unit tests (`placement.rs`).
- Full gate green: `cargo fmt --check`, `clippy -D warnings`, `cargo test --workspace`.
