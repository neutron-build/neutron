# Probes, fuzzers, and benchmark harnesses

Every executable under `src/bin/` other than the server itself. These are the
correctness-finding tools: differential fuzzers with an external oracle,
oracle-free invariant checkers, crash-injection harnesses, and benchmarks.

Most of the real bugs in this engine were found here rather than by unit tests,
because a unit test asserts what the author already believed. Keep it that way:
when you fix a correctness bug, the regression belongs in the probe that would
have caught it, not only in a `#[test]`.

## How to run one

```sh
cd nucleus
cargo run --release --features server --bin probe_kv
```

Always `--release`: several probes are throughput-bound and a debug build
changes what they reach in a given iteration count. The feature set matters —
`--features "server rusqlite"` is required for any probe with SQLite as its
oracle, and building without it silently omits the binary.

Most take `--iterations N` (or a probe-specific equivalent). `timeout` does not
exist on macOS, so bound a run with the binary's own flag, never an external
timer.

## The gate — run before claiming a change is safe

```sh
cargo fmt --check                                  # FIRST — see below
cargo test --lib --features server
cargo clippy --all-targets --features server
cargo check --lib --no-default-features            # core-only build still compiles
sh scripts/metrics.sh --check                      # doc numbers still true

cargo run --release --features "server rusqlite" --bin probe_joins -- --iterations 3000
cargo run --release --features "server rusqlite" --bin fuzz -- --iterations 1500
cargo run --release --features "server rusqlite" --bin fuzz -- --iterations 800 --engine buffered-disk
cargo run --release --features "server rusqlite" --bin fuzz -- --iterations 800 --stream
cargo run --release --features server --bin probe_engines
cargo run --release --features server --bin probe_index_coherence
cargo run --release --features server --bin probe_streams_oracle -- --iterations 120
```

**`probe_streams_oracle` carries its own control.** `--negative-control
<streams|pubsub|cdc>` runs the probe twice at one seed — clean, then with that
section's model perturbed the way an engine bug would perturb it — and passes
only if the perturbation adds divergences to that section and none to the other
two. Run all three after touching the file. The first version of that control
compared against nothing and declared success on a divergence that was already
present; two of its three perturbations had in fact never been applied, because
they were keyed to an op index rather than to an eligible event.

**`cargo fmt --check` is first because it runs first in CI.** It was missing
from this list while sitting in the release checklist, and on 2026-08-11 it cost
a full CI cycle on the N22 fix: both **Nucleus Database Engine** and **Full
Regression Tests** have a Format step ahead of everything else, so two
hand-formatted closures failed the gates *before a single test ran*. Every
probe below had been run locally and proved nothing about those two runs. A
formatting failure is the cheapest possible red — and it masks the whole gate
behind it.

**`--stream` is the same argument one layer up.** `SET stream_results = on`
routes SELECTs down a different executor path, and until 2026-08-17 that path
had only Nucleus-vs-Nucleus unit tests behind it — a metamorphic check against
the implementation it is meant to validate. `--stream` aims the SQLite oracle at
it. It reports `streams served` and **fails when that is zero**, because the
streaming path declines silently on shapes it cannot serve (RLS, CTEs, some
ORDER BY forms) and is answered materialized instead: without the assertion, a
clean `--stream` run could be a clean run of the path it was meant to leave.
That is not hypothetical — the first version of this mode dropped every streamed
query as a "non-select result" and still printed 0 divergences, because
`Executor::execute` does not materialize and `run_nucleus` matched only
`ExecResult::Select`.

**The `--engine buffered-disk` line is not optional.** The default `mvcc` engine
has no paged storage, so a default-engine fuzz run covers nothing `DiskEngine`
does — which is the engine the server actually constructs. This was logged as
NU-008 and independently confirmed; a green default-engine run is not evidence
about the production path.

`metrics.sh --check` fails after almost any code change. Re-sync the baseline in
`DATABASE_COMPLETION.md`; do not edit the numbers to match a doc.

---

## Oracle coverage per data model

Nucleus ships 14 data models. This is which of them an *external* oracle checks
for wrong answers, and where none exists, why. Assembled 2026-08-17 by reading
the binaries rather than the table below, because a probe that exists is not the
same as a model that is covered.

| Model | Oracle | Where |
|---|---|---|
| SQL | SQLite | `fuzz`, `probe_sqlext`, `probe_joins`, `probe_types` |
| KV | reference impl | `probe_kv`, `probe_kv_coll` |
| Vector | brute-force f32 | `probe_vector`, `probe_vector_recall` |
| TimeSeries | reference impl | `probe_tsdoc` |
| Document | reference impl | `probe_tsdoc` |
| Graph | reference impl | `probe_graph`, `probe_graph_algo` |
| FTS | reference impl | `probe_fts`, `probe_fts_rank` |
| Geo | reference impl | `probe_geo` |
| Blob | differential + crash | `probe_blob` |
| Datalog | reference impl | `probe_datalog`, `probe_datalog_rich` |
| Columnar | SQLite | `fuzz --table-engine columnar` (added 2026-08-17) |
| Streams | reference impl | `probe_streams_oracle` (added 2026-08-17) |
| PubSub | reference impl | same binary, section 2 |
| CDC | reference impl | same binary, section 3 |

**All 14 models now have an external oracle.** The last three were closed on
2026-08-17 by `probe_streams_oracle`; before that they shared one reason rather
than three — `probe_streams` checks structural properties across
Streams/PubSub/CDC/Blob, so it finds crashes and self-inconsistency but cannot
find a wrong answer, and Blob was covered only because `probe_blob`
additionally has a real differential. Both binaries are kept: structure and
answers are different questions, and the structural one is far cheaper to run.

### What the streams oracle found immediately

`STREAM_XREAD` took a bare millisecond as its cursor while `STREAM_XADD`
returns `<ms>-<seq>`. A millisecond cannot address an entry, so a consumer that
read up to `<ms>-0` and resumed with `<ms>` was served only entries from a
*later* millisecond — everything else appended in the millisecond it last read
was unreachable, silently and permanently. It reproduced on **120 of 120**
iterations, because sub-microsecond appends share a millisecond constantly.

`STREAM_XACK` had already grown a full-id form for the same composition failure
(the note in `scalar_fns.rs` is explicit that the two ends of the same API did
not compose); there it cost convenience, here it cost entries. Fixed by
accepting both forms in `XREAD` and `XRANGE`. The oracle's no-gap check now
resumes from each entry's id and requires the next one, so a regression is
caught by the probe that found it.

### `--table-engine` and what it found immediately

`--engine` selects the STORAGE engine (paged, MVCC, memory). `--table-engine`
selects the per-table analytics engine — a different axis, and until 2026-08-17
the primary find-anything harness only ever built default heap tables. So
columnar and mergetree execution, which tonight's cost tests confirm are
genuinely separate scan/pruning/aggregate paths, had no external oracle at all.

Pointing SQLite at them found a wrong-answer bug on the first run:

| `--table-engine` | 300 iterations |
|---|---|
| heap | 0 divergences |
| columnar | 0 divergences |
| lsm | 0 divergences |
| **mergetree** | **20 divergences** |

Reproduce: `--seed 305419896 --iterations 95 --table-engine mergetree`. Rows come
back with a trailing column NULL where SQLite has a value. Plain inserts are
fine — a hand-written minimal case passes — so it needs the mutation sequence,
which points at part merging rather than the write path. Filed as N30.

## Differential fuzzers (external oracle)

The oracle is an independent implementation, so a divergence is a real
wrong-answer bug rather than a violated assumption. `rusqlite` required.

| Binary | Oracle | Covers |
|---|---|---|
| `fuzz` | SQLite | Random schemas, data, queries **and mutations**. The primary find-anything harness. |
| `probe_sqlext` | SQLite | Extended SQL beyond `fuzz` coverage. |
| `probe_joins` | SQLite | 2- and 3-table join shapes; reports plan-path coverage. |
| `probe_types` | SQLite | Extended column-type coverage, two strategies. |
| `probe_kv` | reference | KV strings + lists through the SQL surface. |
| `probe_kv_coll` | reference | Sets, sorted sets, hashes, HyperLogLog. |
| `probe_vector` | brute-force f32 | Every distance metric and KNN ordering. |
| `probe_fts` | reference | `FTS_INDEX` / `FTS_REMOVE` / `FTS_SEARCH` / `FTS_MATCH`. |
| `probe_fts_rank` | reference | Two ranking invariants specifically. |
| `probe_geo` | reference | Every geo function. |
| `probe_graph` | reference | Persistent graph store via SQL. |
| `probe_graph_algo` | reference | Algorithm-level properties `probe_graph` does not reach. |
| `probe_datalog` | reference | `DATALOG_ASSERT` / `RULE` / `QUERY` / `RETRACT` / `CLEAR`. |
| `probe_datalog_rich` | reference | Richer Datalog programs. |
| `probe_tsdoc` | reference | TimeSeries and Document, two sections in one binary. |
| `probe_streams_oracle` | reference | Streams, PubSub and CDC, three sections in one binary — full result comparison, not shape. Carries `--negative-control`. |
| `probe_engines` | Nucleus vs Nucleus | Identical SQL on two storage engines — oracle-free but cross-checked. |
| `probe_recover_engines` | round-trip | Persistence/recovery for durable engines other than durable-MVCC. |

## Oracle-free invariant checkers

No reference implementation — these assert structural properties, so they find
crashes, hangs, and self-inconsistency rather than wrong answers.

| Binary | Invariant |
|---|---|
| `probe_meta` | Metamorphic SQL: a rewrite that must not change the result set. |
| `probe_crash` | Executor must never panic on random or adversarial arguments to any registered function. |
| `probe_security` | Resource-exhaustion / DoS: never panic, abort, or hang. |
| `probe_streams` | Streams / PubSub / CDC / Blob structural properties. |
| `probe_index_coherence` | Derived indexes (btree, vector, FTS) stay coherent with the base table under heavy mutation. |
| `probe_efficiency` | PK-equality and range predicates actually use an index — asserts *rows scanned*, not just the result. |
| `probe_concurrency` | MVCC snapshot isolation via the storage adapter. |
| `probe_concurrency_threads` | Same, but real OS threads on one shared `Executor`. |
| `probe_serializable` | Outcome matches *some* serial order — the only true serializability oracle here. |

## Crash, durability, and fault injection

| Binary | What it does |
|---|---|
| `probe_crash_subprocess` | Real `SIGKILL` of a child at a random instant. The honest crash test. |
| `probe_crash_points` | Deterministic crash-point matrix (M3) — systematic where the above is random. |
| `probe_txn_atomicity` | **Is a user transaction atomic across a crash on the paged engine the server runs?** |
| `probe_crossmodel_commit_order` | Crash-injection proof for cross-model commit ordering (R3). |
| `probe_durability_torn` | Torn-write / power-loss approximation. |
| `probe_io_faults` | Disk-full, fsync failure, permission loss. |
| `probe_recover` | WAL recovery round-trip. |
| `probe_raft_crash` | Raft persistent state across reopen. |
| `probe_distributed` | Raft consensus invariants in simulation. |
| `probe_blob` | Blob chunk-store differential **and** crash consistency. |
| `probe_soak` | Sustained concurrent mixed-model load (T1.4 / M11). |

### `probe_txn_atomicity` is now a PASSING gate (was expected to fail)

It was a regression test for an open bug: a crash mid-`COMMIT` left a partial
transaction durable at the shipped 32 MB pool size, because buffer-pool
eviction wrote uncommitted pages to the data file inline. Measured before the
fix: 2 of 3 rounds torn at the shipped pool, 8 of 8 at a small one.

CAMPAIGN-02 closed it — page-WAL records carry real transaction ids, the steal
path logs a before-image, and recovery undoes any transaction with no COMMIT
record. Both configurations are now clean, in both directions: no torn state,
and a generation the child acknowledged is still there afterwards.

**Treat a failure here as a real regression.** Run it after anything touching
the buffer pool, the page WAL, or the commit path:

```sh
cargo run --release --features server --bin probe_txn_atomicity -- --rounds 3 --rows 400000 --pool 2048
cargo run --release --features server --bin probe_txn_atomicity -- --rounds 8 --rows 60000  --pool 256
```

The second is the harsher one — a smaller pool steals more. Do not "fix" a
failure by weakening the probe.

## Benchmarks

Benchmarks answer "how fast", never "is it correct". Read
`_internal/GROUND_TRUTH.md` before publishing any number from these.

| Binary | Features | Scope |
|---|---|---|
| `benchmark` | `bench-tools` | Standalone in-process report across every model. |
| `compete` | `bench-tools` | Nucleus vs PostgreSQL, SQLite, SurrealDB, CockroachDB, TiDB, MongoDB. |
| `compete_vector` | `bench-tools` | Nucleus HNSW vs **pgvector**, apples to apples. |
| `bench_paired` | `server` | Correctness-paired latency for vector, FTS, graph. |
| `pg_compare` | `server` | Head-to-head vs PostgreSQL 17 over pgwire. |
| `stress` | `server` | Load generation. |

**Two traps that have produced published-grade wrong numbers:**

1. `bench_paired` measures against an *inline brute-force reference*, not a
   competitor. Its numbers are Nucleus-only and must never be presented as a
   cross-system win. Its own header says so.
2. On macOS, write benchmarks against PostgreSQL are invalid unless
   `wal_sync_method` is equalised — Nucleus issues `F_FULLFSYNC` (~4,253 µs),
   stock PostgreSQL does not (~41 µs). Read wins are genuine; write comparisons
   at default settings are measuring the fsync mode, not the engine.

## Attribution harnesses

Not correctness tools — they split a measured cost between its parts, to stop
optimisation work being aimed at the wrong half.

| Binary | Question it answers |
|---|---|
| `attr_join` | How much of join cost is the planner vs materialisation. |
| `attr_pk_write` | Splits PRIMARY KEY insert cost in two. |
| `attr_delete` | Is `delete_leaf_entry` worth fixing. (Measured answer: **no** — see `c2a9dc0`.) |

Both `attr_join` and `attr_delete` interleave their arms in one process and
rotate order, and `attr_join` keeps a deliberately `cached` arm so the
result-cache trap stays visible in the output.

```sh
cargo run --release --features server --bin attr_join   -- --rounds 9
cargo run --release --features server --bin attr_delete -- --rows 20000 --deletes 200 --rounds 5
```

## Repro binaries

Short-lived, tied to one investigation. Delete once the bug is fixed and the
regression lives in a probe.

| Binary | Status |
|---|---|
| `repro_txn_delete` | 2,000 single-row DELETEs in one transaction on the paged engine. |
| `probe_param_vector` | Extended-protocol vector corruption: inline SQL vs bound parameter. |
| `probe_vector_recall` | Recall regression. Caught the HNSW bug where greedy neighbour selection produced no bridge edges and clustered recall hit **0.000**; fixed by the Alg.4 diversifying heuristic (`e4b8f21`). |
