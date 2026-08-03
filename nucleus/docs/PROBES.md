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
cargo test --lib --features server
cargo clippy --all-targets --features server
cargo check --lib --no-default-features            # core-only build still compiles
sh scripts/metrics.sh --check                      # doc numbers still true

cargo run --release --features "server rusqlite" --bin probe_joins -- --iterations 3000
cargo run --release --features "server rusqlite" --bin fuzz -- --iterations 1500
cargo run --release --features "server rusqlite" --bin fuzz -- --iterations 800 --engine buffered-disk
cargo run --release --features server --bin probe_engines
cargo run --release --features server --bin probe_index_coherence
```

**The `--engine buffered-disk` line is not optional.** The default `mvcc` engine
has no paged storage, so a default-engine fuzz run covers nothing `DiskEngine`
does — which is the engine the server actually constructs. This was logged as
NU-008 and independently confirmed; a green default-engine run is not evidence
about the production path.

`metrics.sh --check` fails after almost any code change. Re-sync the baseline in
`DATABASE_COMPLETION.md`; do not edit the numbers to match a doc.

---

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

### `probe_txn_atomicity` is expected to FAIL

It is a **regression test for an open bug**, not a passing gate. It demonstrates
that a crash mid-`COMMIT` leaves a partial transaction durable at the shipped
32 MB pool size. The route is buffer-pool eviction writing uncommitted pages
inline — not a WAL sync problem, which is why no audit finding named it.

Turning this green is the acceptance criterion for CAMPAIGN-02, and doing so
needs UNDO, not merely transaction identity in the page WAL. Do not "fix" it by
weakening the probe.

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
