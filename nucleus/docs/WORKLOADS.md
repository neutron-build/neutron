# Workload Definitions

The named workloads the M11 performance program measures against. Each
definition fixes the SHAPE, the PARAMETERS, what the workload exercises, and
which existing harness runs it — so a later measurement can be repeated by
naming a workload instead of re-deriving a command line. What cannot be run
yet is stated as such; a definition that silently omitted its missing scale
would read as measured when it is not.

Harness names refer to the binaries under `src/bin/` (indexed in
`docs/PROBES.md`, which also carries the benchmark traps that have produced
wrong published numbers — read it before quoting any number these produce).
Every harness named below was verified against its argument parser; the
default flags quoted are the binary's own defaults.

## W1 — OLTP point read/write

**Shape.** Many short statements against a bounded hot set: PK inserts, PK
updates touching indexed columns, point selects split between the primary key
and a secondary B-tree, and deletes of the oldest rows. Workers run
concurrently and indefinitely; the live working set is a fixed-size ring
(insert new, delete oldest) so memory growth is a leak, not data.

**Parameters.** concurrency (workers), duration, ring capacity per worker,
preloaded row count and row width, sync mode, engine.

**Exercises.** Buffer-pool hit rate on hot pages, WAL fsync behavior per
commit, page-latch contention on the append/fast paths, the unique gate and
B-tree maintenance cost on every write, checkpoint behavior under sustained
churn, leak-free steady state, and recovery after the run.

**Harness.** `probe_soak` is this workload — op classes `insert`, `update`,
`select` (PK and secondary halves), `kv_set`, `delete`, mixed roughly half
inserts with the rest split across update/select/kv/delete
(`src/bin/probe_soak.rs`; workers' op mix at `:215-289`). Defaults:
`--engine buffered-disk` (the engine `nucleus serve` runs), `--concurrency 8`,
`--duration-secs 20`; `--rows-target N` bulk-loads before the concurrent
phase; `--json` emits the machine-readable record. Per-op p50/p95/p99,
throughput, RSS, footprint, WAL bytes/syncs, write amplification, hit rate,
checkpoint cost, and recovery time are all reported. `attr_pk_write --ab`
isolates the PK write path's cost by subtraction for attribution, not
throughput. `compete --backends ...` runs the SQL CRUD comparison against
other engines when a cross-system number is wanted.

**Not yet runnable.** Sustained concurrency beyond the measured 8-way and
working sets above the measured 1M rows need a dedicated machine (see
DATABASE_COMPLETION.md, "What larger runs require"); the extrapolation to
100M is arithmetic, not a measurement.

## W2 — Analytical scan + aggregate

**Shape.** Bulk load first, measure reads second: range scans over large
row counts, aggregation (GROUP BY) over scanned rows, and full-table reads
with projections. No sustained concurrent writes during the measurement
phase.

**Parameters.** row count (`NUCLEUS_SCALE_ROWS`), engine
(`NUCLEUS_SCALE_ENGINE`, default `buffered-disk`), row width, group
cardinality of the aggregate.

**Exercises.** Sequential-scan throughput and the prefetch window, tuple
decode cost, hash aggregation (including spill at the memory budget), the
columnar engine against the row engines on the same shape, and
load-path throughput (rows/s, WAL volume, write amplification) as a
first-class measurement rather than setup noise.

**Harness.** `tests/scale_load.rs` — the bulk-load plus read-phase harness;
per-phase wall time, rows/s, statement percentiles, footprint, WAL, write
amplification, hit rate, checkpoint, recovery
(`cargo test --release --features server --test scale_load -- --ignored
--nocapture`). `benchmark --models sql,columnar` covers the per-model
in-process sections (`bench_sql`, `bench_columnar` in `src/bin/benchmark.rs`).

**Not yet runnable.** 10M-100M rows: at the measured 1M footprint the
extrapolation is roughly 3.9 GB of data plus WAL before truncation and
minutes of pure load — a shared dev box is the wrong machine, and the
number must come from a run, not the extrapolation.

## W3 — Mixed

**Shape.** W1's concurrent point traffic with cross-model writes mixed in
(KV ops share the engine and the process), plus reads that deliberately
strike the preloaded range outside the hot ring. The mixed class exists
because per-op percentiles can hide which statement is slow, and because
specialty stores contending with relational pages is a distinct failure
mode from either alone.

**Parameters.** Same as W1, plus the preloaded range that read traffic
sprays over.

**Exercises.** Cache invalidation across models, KV fast-path cost beside
the SQL path, index coherence under churn (the soak's invariant checks), and
error storms under sustained load.

**Harness.** `probe_soak` with its default mix (this is the workload it
actually drives — see the op mix cited under W1). `stress.rs` and
`probe_concurrency(_threads)` are correctness harnesses over the same shape,
not throughput measurements; they gate invariants, not numbers.

**Not yet runnable.** Same hardware ceiling as W1. A mixed workload with a
measured ANALYTICAL component at scale (concurrent scans while churning) does
not exist as a harness yet; W2 is load-then-read only.

## W4 — Specialty (vector / FTS / graph)

**Shape.** Three sub-shapes, each measured WITH its correctness number on
the same data in the same run:

- **Vector.** HNSW index build, then k-NN queries; recall@k against exact
  brute-force k-NN computed in-process; `ef_search` swept against a fixed
  graph (rebuilding per point measures a different random graph each time —
  a trap the sweep was redesigned to avoid).
- **FTS.** Inverted-index build over a corpus, selective and broad `@@`
  queries, `ORDER BY BM25() LIMIT`, each compared for exact result-set
  equality against the sequential scan it replaces; both the raw-call and
  SQL (`@@`, `BM25()`) surfaces.
- **Graph.** Traversal and shortest-path queries with BFS/Dijkstra
  agreement against a textbook implementation.

**Parameters.** corpus size (docs/vectors/edges), vector dimension, k,
`ef_search` sweep range, query count, seeds (fixed for reproducibility).

**Exercises.** Index build cost, query latency at target recall (never
latency alone — a recall-free latency number is the publication trap
`docs/PROBES.md` exists to prevent), and index-vs-scan crossover.

**Harness.** `bench_paired` — modes `scale [n] [dim] [k] [queries]`,
`sweep`, `fts-sql [docs] [queries]` (`src/bin/bench_paired.rs`). Its
correctness gates also run as lib tests. `benchmark --models vector,fts,graph`
runs the per-model sections. For CROSS-SYSTEM numbers only:
`compete_vector` (pgvector/Qdrant, matched `m`/`ef`/metric, one corpus and
one ground truth), `compete_fts`, `compete_graph`.

**Not yet runnable.** Nothing structural; specialty shapes run at the
corpus sizes the harnesses parameterize. The constraint is publication, not
execution: `bench_paired` numbers are Nucleus-only (inline reference) and
must not be published as cross-system wins — only `compete_*` output may be.

## W5 — Distributed (deferred-M9)

**Status.** Deferred with M9. Recorded here so "no distributed workload" has
a definition to converge on rather than an absence.

**Intended shape.** A replicated write path measured end to end: client
commits through a leader, Raft replication to N followers, follower reads,
and failover under load (leader kill mid-stream; measured as seconds of
unavailability and rows at risk, not just pass/fail).

**Parameters (planned).** replica count, partition/fault schedule, commit
latency percentiles under fault versus quiescent, steady-state throughput.

**What exists today.** `probe_distributed` is a Raft SAFETY simulator —
deterministic, seeded (`--seed`), in-process clusters with injected
partitions/pauses/loss, asserting the four Raft safety theorems after every
step. It is a correctness harness and produces no throughput numbers.
`probe_raft_crash` covers crash-restart of the consensus core. Neither
drives the SQL layer over a live cluster, so no distributed PERFORMANCE
workload is runnable yet; that is the deferred M9 item, not a gap in this
document.

## Conventions for all workloads

- Numbers come from the harness, never hand-written; `sh scripts/metrics.sh
  --check` keeps the doc surfaces honest about counts.
- The engine under measurement is named in the output; the default is the
  engine the server runs, and RAM engines are selectable only for deliberate
  comparison.
- A workload result that has not been run at a scale is stated as not run,
  with the blocker (hardware, unimplemented harness, or deferred milestone).
