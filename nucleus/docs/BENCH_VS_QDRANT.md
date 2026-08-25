# Nucleus vs Qdrant 1.19.0 — vector search, measured 2026-08-20

> **Postscript 2026-08-25 — the recall findings below are fixed; re-measure
> before quoting this document's Nucleus recall/latency columns.**
> Two mechanisms behind "Nucleus has queries its graph cannot reach":
>
> 1. **Layer assignment drew from a global RNG** (`rand::random()`), so every
>    build of identical data was a different graph — the 96/never/192/96
>    spread was a per-boot lottery. Layers are now derived from the node id
>    (splitmix64), making the graph a pure function of (ids, vectors,
>    insertion order).
> 2. **Query-time descent was greedy ef=1 per upper layer** and handed layer
>    0 a single entry point. On the clustered corpus a trapped descent parked
>    that entry 8.6 from the query while every true top-10 sat at ≤1.04; the
>    beam's admission filter then refused to cross the inter-cluster valley
>    (measured: valley edge 8.81 vs worst-kept 8.755 at ef=128 — the exact
>    zero-recall mechanism). Construction had the matching deviation from the
>    paper's Algorithm 1 (`ep ← W`, not `ep ← closest`). Both fixed: descent
>    is now a beam, layer 0 is seeded with the full beam, construction
>    carries the full result set between layers.
>
> After the fix (brute-force oracle, same corpus shape, `probe_vector_recall`
> section 2): first-perfect-ef across seeds 42/7/1234/99999 is
> **48/48/48/32**, zero-recall queries **0 at every ef on every seed** —
> Qdrant's stability class. At matched `ef` the beam descent costs roughly
> 1.5–1.8x more distance evaluations (measured under host contention on the
> same harness: p50@ef=64 ~230 µs before vs ~420 µs after), but at matched
> **recall** the comparison flips: 1.000 at ef=48 versus this document's
> 192/never. The Qdrant/pgvector columns are unaffected.

Reproduce:

```sh
podman run -d --name nucleus_bench_qdrant -p 56333:6333 docker.io/qdrant/qdrant:latest

cargo run --release --features bench-tools --bin compete_vector -- \
    --n 50000 --dim 128 --k 10 --queries 200 --repeats 5 \
    --m 16 --ef-construction 200 \
    --ef-search 10,16,24,32,48,64,96,128,192,256 \
    --pg "host=localhost port=5432 dbname=nucleus_bench" \
    --qdrant http://127.0.0.1:56333 --qdrant-segments 1 --qdrant-opt-threads 1
```

Four runs of that command. pgvector is in the same harness and in the same run,
so this is a three-way comparison — and pgvector doubles as the **control**. The
independently recorded figures for the Nucleus/pgvector pair are: Nucleus builds
**2.7–3.3x slower**, and the query gap at matched `ef` is **~1.2–1.4x**. Both
reproduced: build came out at 1.5–3.9x across the four runs (3.9x and 2.3x on
the two least contended), and the raw matched-`ef` query ratio is 1.31x at
`ef = 256` and 1.49x at `ef = 192` in Nucleus's favour. If that control had not
landed near its band, nothing in the Qdrant column would be worth reading.

## Host, and the handicaps

macOS 26.6.1 (Darwin 25.6.0), Apple Silicon, 10 cores, 24 GB.

| arm | how it runs | resources |
|---|---|---|
| Nucleus | `HnswIndex` in-process, no transport, no serialization | all 10 host cores, host RAM |
| pgvector 0.8.5 | PostgreSQL 17.11 (Homebrew) over a loopback socket | all 10 host cores; `shared_buffers` 128 MB, `maintenance_work_mem` 64 MB, `max_parallel_maintenance_workers` 2 |
| Qdrant 1.19.0 | podman 6.1.0 container, loopback HTTP/**JSON** | **4 vCPU, 4 GiB** — podman on macOS is a Linux VM, and the default machine is smaller than the host |

**Both handicaps run against Qdrant.** It gets 4 of the host's 10 cores and 4 GiB
of its 24, every request crosses a VM boundary, and it is driven over REST/JSON
rather than its faster gRPC interface. Nothing in this document corrects for
that. Where it changes a conclusion it is said again.

## Dataset, parameters, iteration count

| | |
|---|---|
| Corpus | 50,000 vectors, 128 dimensions, `f32`, clustered (embedding-like), seed 42 |
| Queries | 200, from the same generator, identical and in the same order for every engine |
| `k` | 10 |
| Metric | L2 (`vector_l2_ops` / `Euclid`) |
| Graph | `m = 16`, `ef_construction = 200`, one index per engine, built once |
| Sweep | `ef_search` ∈ {10, 16, 24, 32, 48, 64, 96, 128, 192, 256} against that one index |
| Latency samples | 200 queries × 5 replays = **1,000 per engine per operating point**, ×4 runs |
| Ground truth | exact brute-force k-NN, computed in the harness process, shared by all three engines |

Queries are sequential and single-threaded on every arm. No engine grades its own
homework: recall is scored against the harness's own brute-force answer, and by
DISTANCE rather than by id, so near-duplicates in clustered data cannot be
counted as misses.

## The rule this document is written to

**Compare at matched recall, never at matched `ef_search`.** An engine that
returns worse answers faster is not faster, and these three do genuinely
different amounts of work per unit of `ef` — Qdrant reaches recall 1.000 at
`ef = 48–64`, pgvector at 48–256, Nucleus at 96 or not at all. Reading the
`ef = 64` row of one column against the `ef = 64` row of another compares
parameter values, not engines. Two published Nucleus vector claims were
retracted on 2026-08-19 for exactly this class of error.

## Full sweep, cleanest run (run 1)

`build_s` repeats down each engine's rows because one index was built per engine
and swept; it is one measurement, not ten. `zero_q` counts queries where the
engine returned **none** of the true top-10 — the number that matters for
retrieval, because a mean of 0.995 hides a user who got nothing.

| engine | ef_search | build_s | recall | min_recall | zero_q | p50 µs | p95 µs |
|---|---:|---:|---:|---:|---:|---:|---:|
| nucleus | 10 | 27.9 | 0.970 | 0.000 | 3 | 33 | 45 |
| nucleus | 16 | 27.9 | 0.978 | 0.000 | 3 | 38 | 50 |
| nucleus | 24 | 27.9 | 0.984 | 0.000 | 3 | 48 | 63 |
| nucleus | 32 | 27.9 | 0.985 | 0.000 | 3 | 53 | 66 |
| nucleus | 48 | 27.9 | 0.985 | 0.000 | 3 | 62 | 74 |
| nucleus | 64 | 27.9 | 0.990 | 0.000 | 2 | 79 | 96 |
| nucleus | 96 | 27.9 | 0.995 | 0.000 | 1 | 96 | 110 |
| nucleus | 128 | 27.9 | 0.995 | 0.000 | 1 | 112 | 136 |
| nucleus | 192 | 27.9 | **1.000** | 1.000 | 0 | 152 | 192 |
| nucleus | 256 | 27.9 | 1.000 | 1.000 | 0 | 240 | 300 |
| pgvector | 10 | 7.2 | 0.949 | 0.000 | 7 | 145 | 211 |
| pgvector | 16 | 7.2 | 0.962 | 0.000 | 6 | 141 | 188 |
| pgvector | 24 | 7.2 | 0.979 | 0.000 | 4 | 147 | 199 |
| pgvector | 32 | 7.2 | 0.979 | 0.000 | 4 | 146 | 184 |
| pgvector | 48 | 7.2 | 0.980 | 0.000 | 4 | 152 | 190 |
| pgvector | 64 | 7.2 | 0.985 | 0.000 | 3 | 162 | 212 |
| pgvector | 96 | 7.2 | **1.000** | 1.000 | 0 | 171 | 225 |
| pgvector | 128 | 7.2 | 1.000 | 1.000 | 0 | 186 | 241 |
| pgvector | 192 | 7.2 | 1.000 | 1.000 | 0 | 226 | 317 |
| pgvector | 256 | 7.2 | 1.000 | 1.000 | 0 | 314 | 412 |
| qdrant | 10 | 2.1 | 0.896 | 0.000 | 2 | 395 | 478 |
| qdrant | 16 | 2.1 | 0.954 | 0.800 | 0 | 341 | 452 |
| qdrant | 24 | 2.1 | 0.983 | 0.800 | 0 | 359 | 436 |
| qdrant | 32 | 2.1 | 0.995 | 0.900 | 0 | 386 | 487 |
| qdrant | 48 | 2.1 | **1.000** | 1.000 | 0 | 386 | 469 |
| qdrant | 64 | 2.1 | 1.000 | 1.000 | 0 | 403 | 500 |
| qdrant | 96 | 2.1 | 1.000 | 1.000 | 0 | 403 | 489 |
| qdrant | 128 | 2.1 | 1.000 | 1.000 | 0 | 409 | 522 |
| qdrant | 192 | 2.1 | 1.000 | 1.000 | 0 | 435 | 704 |
| qdrant | 256 | 2.1 | 1.000 | 1.000 | 0 | 481 | 568 |

Transport floors, measured in the same run as a trivial request on the same
connection: pgvector `SELECT 1` p50 = **91 µs**, Qdrant `GET /healthz` p50 =
**154 µs**. Nucleus has no transport.

## Latency at matched recall

Each cell is the **first `ef_search` at which that engine met the recall target**
and the latency there. This is the comparison; the sweep above is its raw
material.

Nucleus's figure is an in-process function call — no socket, no JSON, no
protocol. The other two are given an engine-side estimate:

- **pgvector**: measured client p50 minus the `SELECT 1` floor from the same
  run. This under-subtracts, because a 128-float query costs more to move than
  `SELECT 1`, so the estimate is generous to Nucleus.
- **Qdrant**: its own `time` field, on every response. That bounds Qdrant's work
  **including the server's JSON parse and serialize**, so Qdrant's actual graph
  search is at most this and in reality less. Written `≤`.

Run 3 is excluded from every latency figure (see "the run that was thrown out").
Run 4's Qdrant container was contended mid-run — its self-reported handling time
doubled — so its Qdrant latency is excluded too and its Nucleus and pgvector
columns are kept.

| target recall | Nucleus (in-process) | pgvector (less floor) | Qdrant (engine-side bound) |
|---|---:|---:|---:|
| ≥ 0.99 | **79–85 µs** (`ef` 48–64) | **58–80 µs** (`ef` 24–96) | **≤120–142 µs** (`ef` 32–48) |
| = 1.000 | **112–152 µs** (`ef` 96–192, and once never) | **80–108 µs** (`ef` 48–128) | **≤127–145 µs** (`ef` 48–64) |

Read plainly:

- **At a 0.99 operating point the three are close and Nucleus holds its own.**
  79–85 µs against pgvector's 58–80 µs and Qdrant's ≤120–142 µs. No margin here
  is worth a claim, particularly since Qdrant's bound includes JSON work the
  other two figures exclude.
- **At recall 1.000 Nucleus is last.** It needs `ef = 96–192` where Qdrant needs
  48–64, and 112–152 µs of pure graph search is behind pgvector's 80–108 µs —
  1.9x behind in run 1, 1.1x in run 4 — and level with or behind Qdrant's
  ≤127–145 µs bound. Since that
  bound is inclusive of serialization, **Qdrant's engine is faster than Nucleus's
  at the top of the recall curve**, and the true margin is wider than the numbers
  above show.
- **The 386 µs Qdrant end-to-end figure is not an engine result.** It is
  REST/JSON crossing into a 4-vCPU Linux VM on a laptop. It is in the sweep table
  because it is what was measured and what a client on this exact setup would
  see. It must not be quoted as a Qdrant performance number.

## Recall quality — the finding that matters more than the latency

Contention changes how long an answer takes, not what the answer is, so recall is
the one column a noisy laptop cannot corrupt. It is also where the three engines
differ most.

`ef_search` at which each engine first reaches recall 1.000 on all 200 queries:

| run | Nucleus | pgvector | Qdrant |
|---|---:|---:|---:|
| 1 | 192 | 96 | **48** |
| 2 | **never** (max 0.995) | 128 | 64 |
| 3 | 192 | 256 | 64 |
| 4 | 96 | 48 | 64 |
| **range** | **96 – never** | **48 – 256** | **48 – 64** |

Two things follow, and only one of them is about speed:

1. **Qdrant's graph is better and more reproducible.** It clears every query at
   `ef = 48–64` in all four runs — a 1.3x spread — where Nucleus ranges from 96
   to never and pgvector from 48 to 256. HNSW construction is randomized, so
   graph quality varies run to run; Qdrant's variance is a fraction of the other
   two's. Note this is Qdrant with **two** segments merging their results, which
   is part of why.
2. **Nucleus has queries its graph cannot reach, and more effort does not always
   rescue them.** In run 2 one query was still returning *none* of the true top-10
   at `ef = 256` — 25x the value of `k`. `zero_q` at `ef = 128` was 1 in three of
   four runs for Nucleus, 1 in one of four for pgvector, and **0 in all four for
   Qdrant**.

A mean recall of 0.995 with `zero_q = 1` is not "99.5% as good". It is 199
perfect answers and one user whose result page was entirely wrong, which no
amount of search effort fixed. That is the single most actionable output of this
measurement, and it is a construction/connectivity problem, not an operating
point to be chosen differently.

## Build time

One index per engine per run, `m = 16`, `ef_construction = 200`, 50,000 vectors.

| run | Nucleus | pgvector | Qdrant | Nucleus ÷ Qdrant | container contended? |
|---|---:|---:|---:|---:|---|
| 1 | 27.9 s | 7.2 s | **2.1 s** | 13.3x | no |
| 2 | 18.3 s | 7.8 s | **2.5 s** | 7.3x | no |
| 3 | 23.5 s | 15.7 s | 7.0 s | 3.4x | yes (whole host) |
| 4 | 23.8 s | 8.5 s | 6.9 s | 3.4x | yes (container only) |

**Qdrant builds this index between 3.4x and 13.3x faster than Nucleus, and 7–13x
faster on the two runs where its container had the VM to itself.** pgvector is
1.5–3.9x faster than Nucleus, straddling the recorded 2.7–3.3x band and
confirming the control.

Contention is classified by the transport floor and by Qdrant's self-reported
handling time, not by feel. At `ef = 64` — identical work in every run — Qdrant
reported **133 µs (run 1), 145 µs (run 2), 924 µs (run 3), 298 µs (run 4)**.
Runs 3 and 4 are the container being starved, and their build numbers are read
as floors on Qdrant rather than as measurements of it.

Threading is not equal, and cannot be made equal without changing what each
product is:

- Nucleus's `HnswIndex::insert` loop is **single-threaded**, on a 10-core host.
- pgvector's `CREATE INDEX` runs with `max_parallel_maintenance_workers = 2`, so
  up to three processes, on the same 10-core host.
- Qdrant's optimizer was asked for **one** thread and **one** segment; it
  produced two segments, so at most two workers, inside a **4-vCPU** VM.

So part of Qdrant's margin is parallelism — but it has the smallest CPU budget of
the three and the widest margin, which is the opposite of what a
parallelism-only explanation predicts. The single-threaded-Nucleus caveat does
not rescue this number. It is a real gap and it is the largest one in this
document.

Qdrant's build excludes upload. The collection is created with
`indexing_threshold = 0` so points land unindexed; indexing is then switched on
and the harness polls until `status = green` **and** `indexed_vectors_count`
reaches 50,000. (Green alone is not enough — a collection with zero indexed
vectors is also green.) Upload was 1.7–2.7 s and is reported separately.

## Memory

| | measurement | per vector |
|---|---:|---:|
| raw vectors (floor) | 25.6 MB | 512 B |
| Nucleus, peak-RSS delta across the build | 29 / 54 / 59 / 59 MB | 613–1,230 B |
| Qdrant, container memory over baseline | ~50 MB peak (66 MB → 116 MB) | ~1,000 B |
| pgvector, `pg_relation_size` of the HNSW index | 40 MB (+28 MB table) | 840 B |

Nucleus and Qdrant hold the same order of magnitude for the same graph, and this
measurement is not sharp enough to say more than that. The Nucleus probe is
`getrusage(RUSAGE_SELF).ru_maxrss` differenced across the build; it is a
high-water mark rather than a live footprint, and it read 29 MB once and 54–59 MB
three times for identical work. Treat the range as the result. Qdrant's figure is
whole-container RSS including the server itself, taken by polling
`podman stats` through the run, so it is an over-count by an unknown constant.
**Neither number is precise enough to support a memory claim in either
direction.**

## The run that was thrown out, and how it was detected

This machine has previously measured 95.4% worst-case deviation on green runs,
and run 3 is a clean example: another process was running a `cargo` build at
~730% CPU, load average 17.8.

The tell was not the engine numbers, it was the **transport floor**.
`GET /healthz` p50 went 154 → 256 → 338 → 199 µs across the four runs and
`SELECT 1` moved with it. Neither request touches an index, so any movement there
is the machine. In run 3 Qdrant's client-side p50 read 3,658 µs at `ef = 64` —
9x the clean value — and its self-reported handling time at that same `ef` rose
from 133 µs to 924 µs, confirming the contention reached inside the container.

**Check the transport floor before reading any row of this document.** It is the
cheapest available control and it caught a run that would otherwise have produced
a confident 9x claim. The harness prints both floors on every run for this
reason.

Run-to-run spread on what is kept: Nucleus build 18.3–27.9 s (52%), Nucleus p50
at `ef = 64` 79–94 µs (19%), pgvector p50 at `ef = 64` 162–196 µs (21%), Qdrant
p50 at `ef = 64` 403 µs in both uncontended runs (<1%). **The latency ratios in
this document are the soft part; the recall table is the hard part.**

## What was not measured, and why

- **Qdrant over gRPC.** The harness speaks REST/JSON on port 6333. gRPC is
  Qdrant's faster transport, so every client-side Qdrant latency here understates
  the product. The self-reported handling time is a mitigation, not a fix.
- **Qdrant on equal hardware.** 4 vCPU and 4 GiB inside a VM, against 10 cores
  and 24 GB natively for the other two. Raising the podman machine to match the
  host was not done and would change the build column in Qdrant's favour.
- **Qdrant's real deployment shape.** One shard, one requested segment, no
  quantization, no payload filtering, vectors in RAM. Scalar and binary
  quantization are headline Qdrant features that would move both the latency and
  the memory columns; none of it is exercised.
- **Concurrency.** One query at a time from one thread on every arm. Qdrant and
  PostgreSQL are servers built to overlap requests; Nucleus's index is measured
  as a library call. Nothing here says anything about throughput under load.
- **Nucleus's served vector path.** This measures `HnswIndex` directly, not a
  `VECTOR` query arriving over pgwire through the executor. The pgwire path adds
  parsing, planning and protocol cost absent from the Nucleus column, and there
  is a separate known problem on it: vector lookups have been measured reading
  the whole table with the index only reordering the result. **Do not read this
  document as a measurement of `SELECT ... ORDER BY v <-> ...` on Nucleus.**
- **Larger corpora and higher dimensions.** 50,000 × 128 fits comfortably in RAM
  on every arm, so nothing here touches disk-backed vector storage — where
  Qdrant's `on_disk` modes and pgvector's buffer behaviour would start to matter.
- **A second data distribution.** Only the clustered generator was run. Uniform
  vectors are a harder recall problem and were not measured.
