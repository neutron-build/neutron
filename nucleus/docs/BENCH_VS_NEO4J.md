# Nucleus vs Neo4j 5.26.29 — graph traversal, measured 2026-08-20

Reproduce:

```sh
podman run -d --name nucleus_bench_neo4j -p 7474:7474 -p 7687:7687 \
    -e NEO4J_AUTH=none \
    -e NEO4J_server_memory_heap_initial__size=2G \
    -e NEO4J_server_memory_heap_max__size=2G \
    -e NEO4J_server_memory_pagecache_size=1G \
    docker.io/library/neo4j:5.26-community

cargo run --release --features bench-tools --bin compete_graph -- \
    --nodes 10000 --shortcuts 3 --queries 200 --warmup 200 --write-ops 200 --seed 42
```

Host: macOS 26.6.1 (Darwin 25.6.0), Apple M4, 10 cores, 24 GB, APFS. Nucleus
0.1.6, `Executor::new_with_persistence` over a real data directory, answering on
pgwire over a loopback TCP socket. Neo4j 5.26.29 Community in a podman container
— which on macOS means a Linux VM — answering Bolt over a loopback TCP socket.
`rustc` 1.97.0.

## The one-sentence version

**Nucleus's graph traversal is competitive with Neo4j's and its answers were
exactly right 4,400 times out of 4,400 — but four of the five ratios a naive
reading would produce are wrong, and the two clearest single findings are both
losses: Nucleus resolves a node by property 14x slower than Neo4j because its
Cypher subset has no property index, and its shortest path is 1.9x slower
because it is a unidirectional BFS where Neo4j's is bidirectional.**

## Dataset, parameters, iteration count

| | |
|---|---|
| Graph | 10,000 nodes, directed; ring edge `i → i+1` plus 3 random shortcuts per node, seed 42 |
| Edges | 40,000 (mean out-degree 4.00), deduplicated |
| Shape | Small-world. Sampled shortest paths: mean 6.59 hops, max 9 |
| Neighbourhood sizes | 1-hop 4.0, 2-hop 20.0, 3-hop 83.6 nodes (means over the sampled anchors) |
| Node properties | `k` (0..9999, the join key) and `grp` (`k % 10`, the pattern filter) |
| Anchors | 400 distinct nodes per read workload, sampled without replacement |
| Timed samples | 200 per arm, preceded by 200 untimed warm-up operations **on both engines** |
| Write samples | 200 per arm, preceded by 200 untimed |
| Ground truth | plain-Rust BFS over the generated adjacency list, computed in-process |
| Neo4j schema | range index on `:N(k)`, `db.awaitIndexes()` before any timing |
| Nucleus schema | none available — see "The anchor problem" |

Both engines are loaded from the same generator, edge for edge, in the same
order. Every query is answered sequentially, one connection, one round trip per
operation (except the client-BFS arm, which reports its round-trip count).

10,000 nodes is deliberate: `GraphStore::max_hot_nodes` is 100,000, above which
the store spills properties to a cold LSM tier. Staying an order of magnitude
below it keeps the tiering out of the measurement.

### Why 400 untimed warm-up operations

Because without them this document would have claimed a 10-14x sweep. Neo4j is a
JVM, and the first smoke run of this harness — 15 iterations, no warm-up —
produced a Neo4j p99 of 88 ms against a p50 of 2.3 ms and ratios of 10-14x
across every read workload. With both engines warmed, the same workloads land
between 1.3x and 3.5x. **An unwarmed JVM benchmarks its own JIT compiler.**

## Correctness first

Every timed and every warm-up operation was graded against the in-process BFS
before its latency sample was kept, and a mismatch aborts the run rather than
being recorded as a fast success — the defect that discredited everything under
`docs/benchmarks/`. What is checked:

- neighbourhood sets must equal `{v ≠ a : 1 ≤ dist(a,v) ≤ k}` exactly, by set
  identity, not by size;
- a returned path must start at `from`, end at `to`, have exactly the oracle's
  length, **and** consist of edges that exist in the generated graph — checking
  length alone would pass a fabricated path and checking edges alone would pass
  a valid but non-shortest one;
- an unreachable pair must return nothing, and a reachable one must not;
- node and edge counts are verified on both engines after the load and after the
  write arm, so an arm that silently wrote nothing cannot look excellent.

**Result: 4,400 / 4,400 Nucleus operations and 3,200 / 3,200 Neo4j operations
correct, in every one of five runs. Zero divergences, on either engine.** Nucleus's
graph model returns right answers; this document is only about how fast.

## The transport floor, and why the headline ratios are wrong without it

The harness times `SELECT 1` on Nucleus and `RETURN 1` on Neo4j. Neither touches
data, so both rows are pure transport plus protocol.

| | p50 |
|---|---:|
| Nucleus, pgwire, loopback, same process tree | **39 µs** |
| Neo4j, Bolt, loopback into a podman Linux VM | **537 µs** |

That 537 µs is not Neo4j the database. It is Bolt plus a virtualised network
hop on a macOS laptop, and an independent probe against the same container
measured it at 541 µs — reproducible to under 1%. **It is larger than the
engine-side cost of most of the queries below**, so a raw client-side ratio
mostly measures the container boundary.

Every ratio in this document is therefore given twice: raw, and net of each
engine's own floor. Net-of-floor over-subtracts nothing and under-subtracts the
per-record protocol cost of a large result — which matters most for Neo4j's
3-hop arm, where Bolt returns ~84 records against Nucleus's one JSON blob, so
Neo4j's net cost there is overstated by an unmeasured amount.

## Read results — cleanest run

| workload | Nucleus p50 | net | Neo4j p50 | net | raw | **net** |
|---|---:|---:|---:|---:|---:|---|
| transport floor (control) | 39 | — | 537 | — | — | — |
| anchor: resolve node by property | 358 | 319 | 560 | 23 | 1.6x N | **14x Neo4j** |
| 1-hop out-neighbours | 48 | 9 | 544 | 7 | 11.4x N | **a wash** |
| 2-hop set, `GRAPH_QUERY` | 399 | 360 | 624 | 87 | 1.6x N | 4.1x Neo4j |
| 2-hop set, client BFS (5 rtt) | 241 | 46 | 624 | 87 | 2.6x N | 1.9x N |
| 3-hop set, `GRAPH_QUERY` | 1,247 | 1,208 | 4,396 | 3,859 | 3.5x N | 3.2x N |
| 3-hop set, client BFS (21 rtt) | 2,350 | 1,531 | 4,396 | 3,859 | 1.9x N | 2.5x N |
| shortest path (mean 6.6 hops) | 676 | 637 | 878 | 341 | 1.3x N | **1.9x Neo4j** |
| pattern: 1-hop filtered on `b.grp` | 576 | 537 | 1,813 | 1,276 | 3.2x N | 2.4x N |

All figures µs. "N" = Nucleus faster. Net = p50 minus that engine's own transport
floor; for the client-BFS arm, minus one floor per round trip.

Spread on this run (p99/p50): Nucleus 1.10–3.48, Neo4j 1.13–4.06. The two widest
are both shortest path, where the sample genuinely varies — path lengths run from
1 to 9 hops.

### Reading the table net of the anchor as well

Three Nucleus arms go through `GRAPH_QUERY`, so they pay the 319 µs label scan
before they traverse anything; Neo4j's equivalents pay 23 µs. Subtracting each
engine's own anchor cost isolates the traversal:

| traversal only | Nucleus | Neo4j | |
|---|---:|---:|---|
| 2-hop expansion | 41 µs | 64 µs | 1.6x Nucleus |
| 3-hop expansion | 889 µs | 3,836 µs | 4.3x Nucleus |
| 1-hop + property filter | 218 µs | 1,253 µs | 5.7x Nucleus |

The 2-hop figure has an independent check: the client-BFS arm reaches the same
answer through a completely different surface (`GRAPH_NEIGHBORS`, five round
trips) and nets out at 46 µs against `GRAPH_QUERY`'s 41 µs. Two Nucleus code
paths agreeing to within 12% is the reason to believe either.

**Caveat on the 3-hop row, stated because it flatters Nucleus.** Cypher's
variable-length patterns are relationship-unique — a node may repeat in a path —
while Nucleus's expansion is node-unique, so Neo4j enumerates strictly more paths
before its `DISTINCT` collapses them. The *answers* are identical (the harness
proves that against the oracle, 400/400 on both sides), but the work is not.
Some part of that 4.3x is Cypher's semantics rather than Neo4j's speed, and a
Neo4j user chasing a distinct k-hop set would reach for `apoc.path.subgraphNodes`
— not available in the stock community image and not measured here.

## The two findings that are losses

### 1. The anchor problem: 14x, and it gets worse with scale

Nucleus's `GRAPH_*` SQL functions take an internal node id, so a traversal from a
known node starts for free. But nothing in Nucleus can *find* that node from a
property, and its Cypher subset resolves `MATCH (a:N {k: 42})` by
`nodes_by_label("N")` followed by a property compare per node — a full label
scan. 319 µs over 10,000 nodes is 32 ns per node, which is a perfectly
respectable scan; it is still a scan.

Neo4j does it in 23 µs through a range index, and **that gap widens linearly**:
at 100,000 nodes Nucleus's anchor resolution would be ~3.2 ms while Neo4j's stays
flat. This is the single most actionable output of the measurement. `PropertyIndex`
already exists in `src/graph/mod.rs` with `build_from` / `lookup` / `range` — it
is simply not wired into `cypher_executor::candidate_node_ids`, which is the same
"declared surface, no execution path" shape catalogued in
`_internal/ENGINE_PERFORMANCE_PROGRAM.md` §2.

### 2. Shortest path is unidirectional where Neo4j's is bidirectional

Net of transport, Neo4j finds a shortest path in 341 µs against Nucleus's 637 µs
— **1.9x, in Neo4j's favour**, and the raw 1.3x "Nucleus faster" row is entirely
the container boundary.

The cause is in the source, not inferred: `GraphStore::shortest_path` is a plain
BFS queue seeded only from `from`. At a mean distance of 6.6 hops with out-degree
4 that explores on the order of 4^6.6 nodes, where a bidirectional search meets in
the middle at roughly 2 × 4^3.3. Neo4j's `shortestPath()` is bidirectional. The
fix is well understood and self-contained.

Note what this row does *not* say: Nucleus's answers were right every time,
including path validity edge-for-edge. It finds the same shortest path, by a
more expensive route.

## The write comparison is NOT VALID in this environment

**Do not quote a write ratio from this benchmark.** The harness prints one; it is
not an engine result, and this section is the reason the harness prints
`DO NOT QUOTE THE WRITE RATIO` next to it.

`docs/BENCH_VS_POSTGRES.md` records the same trap for PostgreSQL: on macOS
`fsync(2)` returns when data reaches the drive, while `fcntl(F_FULLFSYNC)` forces
the drive's volatile cache, and the two differ by orders of magnitude. The
harness measures both on the same filesystem as the Nucleus data directory, in
the same process, on every run:

| | p50 on this filesystem |
|---|---:|
| `fsync(2)` | **17 µs** |
| `fcntl(F_FULLFSYNC)` | **3,896–3,940 µs** |

Against that scale:

| | measurement | net of its own floor |
|---|---:|---:|
| Nucleus single node insert, `synchronous_commit=on` | 3,814–4,047 µs | ~3,900 µs |
| Nucleus same insert, `synchronous_commit=off` (control) | 66–67 µs (clean runs) | — |
| Neo4j single node insert, measured in isolation | 683 µs | **142 µs** |

Nucleus's commit lands exactly on the `F_FULLFSYNC` figure — it takes a full
drive-cache barrier per graph write, and ~98% of the operation is that barrier.
Neo4j's commit, isolated and net of its Bolt floor, is **142 µs — 27x below one
barrier**. It cannot be issuing one. Nor is that a criticism of Neo4j: it runs
inside a Linux VM whose disk is a file on this same APFS volume, so a guest
`fsync` need not reach the host's drive at all.

**The two engines are buying different durability guarantees, and the latency
difference is mostly that.** Settling it requires a Linux host where both
engines' sync semantics are unambiguous, which was not available here.

Two consequences for anyone extending this harness:

1. **Nucleus's write rate here is a property of `F_FULLFSYNC`, not of the graph
   engine.** Its bulk load ran at 244–267 edges/s in four of the five runs —
   including two contended ones — because 1/3,900 µs = 256/s; only run 2, the
   most heavily contended, dropped it to 155/s. The engine is not the
   bottleneck and no engine work will move that number.
2. **Never interleave write arms when one engine issues device-wide barriers.**
   The read arms alternate per anchor so drift hits both equally; the first
   version of this harness did the same for writes, and Neo4j's insert p50 read
   3,212 µs. Run alone against the same container it is 683 µs. Nucleus's
   `F_FULLFSYNC` is a *device* barrier, so it was flushing Neo4j's dirty data and
   charging Neo4j 4.7x for the privilege. The write arms now run in contiguous
   per-engine blocks.

## Two Nucleus defects the harness found on the way

**`-[:REL*1..2]->` does not parse.** The most common Cypher spelling of a
variable-length pattern is rejected by Nucleus with `invalid float: 1..2`. The
lexer scans a number with `while digit || '.'`, so `1..2` is consumed as one
token and fails to parse as a float before the parser's entirely correct
`*min..max` branch is ever reached. The `*..2` spelling works and means the same
thing, and is what both engines are sent so the comparison stays honest. The
harness probes both spellings on both engines at startup and prints the result.

**There is no way to read a node's properties from the `GRAPH_*` surface.**
`GRAPH_NEIGHBORS` returns neighbour ids, edge ids and edge types. There is no
`GRAPH_GET_NODE`. So any property-filtered pattern — a completely ordinary graph
query — has to go through `GRAPH_QUERY`, which drags in the label-scan anchor
from finding 1. That is why the pattern row has only one Nucleus arm.

## Reproducibility, and the runs that were thrown out

Five full runs. **Two were clean and three were contended**, and the controls
caught all three without any judgement call:

| run | Nucleus floor | Neo4j floor | `synchronous_commit=off` | verdict |
|---|---:|---:|---:|---|
| 1 | not yet measured | not yet measured | 66 µs | clean |
| 2 | not yet measured | not yet measured | **4,043 µs** | discarded |
| 3 | 39 µs | 537 µs | 67 µs | **clean — the table above** |
| 4 | 104 µs | 1,548 µs | 87 µs | discarded (reads) |
| 5 | 151 µs | 2,026 µs | 129 µs | discarded (reads) |

A `synchronous_commit=off` insert does no drive barrier and a `SELECT 1` touches
no data, so movement in either is the machine and nothing else. In run 2 the
control moved 61x; in runs 4 and 5 the transport floor moved 2.7x and 3.9x. In
every case every other row had moved with it. The cause was visible in `ps`: an
unrelated Rust test binary at 180% CPU, load average 5–8.

**Check the two control rows before reading any other row of this table.** This
machine class has been measured at 95.4% worst-case deviation on green runs; the
controls are the cheapest defence against publishing that as a result.

Run-to-run agreement on the clean runs (1 and 3), Nucleus / Neo4j p50:

| workload | run 1 | run 3 |
|---|---|---|
| 1-hop | 78 / 782 | 48 / 544 |
| 2-hop, client BFS | 372 / 878 | 241 / 624 |
| 3-hop, `GRAPH_QUERY` | 731 / 2,570 | 1,247 / 4,396 |
| shortest path | 670 / 1,537 | 676 / 878 |
| anchor | 535 / 776 | 358 / 560 |

Run 1 predates the transport-floor control, so it cannot be normalised and is
shown raw. The two runs agree on direction everywhere and on magnitude within
~1.8x — which is the honest precision of this machine, and the reason no ratio
here is quoted to better than one decimal place.

## What was not measured, and why

- **Neo4j on a Linux host.** Everything about the write comparison, and 537 µs of
  every Neo4j read, is the podman VM boundary. This is the single change that
  would most improve the measurement.
- **Concurrency.** Every arm is one operation at a time from one connection.
  Nothing here says anything about throughput under load, and Neo4j's page cache
  and Nucleus's group-commit both exist to exploit concurrency neither got.
- **Batched loading.** Both engines were loaded one operation per round trip,
  autocommit, because that is the one shape whose transaction semantics are
  identical on both sides. Nobody bulk-loads a graph that way, and Neo4j's
  `LOAD CSV` / `CALL {} IN TRANSACTIONS` and Nucleus's explicit transactions
  would both be far faster.
- **APOC.** Neo4j's k-hop rows use the pattern-match spelling, which enumerates
  paths. `apoc.path.subgraphNodes` is the set-oriented idiom a Neo4j user would
  actually reach for, and it is not in the stock community image.
- **Weighted paths, PageRank, community detection.** `GraphStore` implements
  `dijkstra`, `pagerank`, `label_propagation` and `louvain_communities`, none of
  which are reachable from the SQL surface, so none could be compared.
- **Larger graphs.** 10,000 nodes fits in RAM on both sides with room to spare.
  Nothing here touches Nucleus's cold LSM tier or Neo4j's page-cache eviction,
  and the anchor finding predicts the gap grows with scale — untested.
- **Node deletion and edge deletion.** Only inserts and reads were timed.
- **Undirected and typed-edge traversal.** One edge type, `Outgoing` only.
