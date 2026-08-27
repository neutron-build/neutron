# Nucleus vs ClickHouse 26.7.3 — analytical SQL, measured 2026-08-27

> One-line context: ClickHouse is a dedicated columnar OLAP engine; Nucleus is
> a general-purpose multi-model engine whose SQL is row-based (pgwire) with a
> separate columnar MODEL store. This measures the **row-based SQL path**
> against ClickHouse's home turf. Expect to lose the pure-analytical columns;
> the point of the measurement is to know by how much, and where the
> generalist holds.

Environment: same machine as every other BENCH doc (macOS, Apple M4, 10
logical CPUs), Nucleus 1.0.0 in-process pgwire server vs ClickHouse 26.7.3.19
(brew, native TCP :9000), both warm, sequential sections. Dataset: 500K
users + 2.5M orders (10x the default), 500 timed iterations per query after
warm-up. Section ratio is nucleus_latency / clickhouse_latency — **below 1.0
= Nucleus faster**.

Reproduce (ClickHouse must be running first):

```sh
clickhouse server   # brew; then from nucleus/
cargo run --release --features bench-tools --bin compete -- \
    --backends nucleus,clickhouse --skip pg,sqlite,surrealdb,cockroach,tidb,mongodb,redis \
    --iterations 500 --rows 500000
```

## Results (2.5M orders)

| Query | Nucleus (row store) | ClickHouse (columnar) | latency ratio | Winner |
|---|---|---|---|---|
| Point Query (PK) | 29.1K ops/s | 9.2K ops/s | 0.31 | **Nucleus 3.2x** |
| COUNT(*) | 17.8K ops/s | 11.3K ops/s | 0.63 | **Nucleus 1.6x** |
| Range Scan | 9.7K ops/s | 9.1K ops/s | 0.94 | ~tie (Nucleus edge) |
| Filtering (WHERE) | 1.6K ops/s | 10.1K ops/s | 6.3 | **ClickHouse 6.3x** |
| Aggregation (AVG) | 1.2K ops/s | 10.8K ops/s | 9.0 | **ClickHouse 9.0x** |
| Ordering (ORDER BY) | 540 ops/s | 11.2K ops/s | 20.7 | **ClickHouse 20.7x** |

## Reading it honestly

- **The shape is exactly what the architectures predict**, which is itself
  the finding: the harness is measuring the engines, not itself. A row store
  with B-tree point lookups beats a columnar scanner 3x on PK queries;
  a columnar scanner that only touches one or two columns beats a row store
  6–21x when the answer requires sweeping all rows.
- **ORDER BY at 20.7x is the honest headline number.** Full-data sort is the
  most columnar-friendly workload in the suite, and Nucleus's row engine does
  a heap-at-a-time external sort. Anyone building an analytics workload on
  Nucleus SQL today should know this number.
- **Nucleus's columnar MODEL store is not this comparison.** `COLUMNAR_*`
  (batched insert measured at 329–865M rows/s in the standalone harness) is
  a separate store with its own API; competing SQL-to-SQL against ClickHouse
  exercises the row engine only. A SQL-planner-over-columnar-store path is
  future work; when it exists this doc must be re-run, not reinterpreted.
- At the 50K-row default scale (previous page of results in
  `compete_results.json`), aggregation and filtering read as near-parity —
  **the columnar advantage is a function of data size**, and quoting the
  small-scale numbers as "comparable to ClickHouse" would be the exact
  benchmark dishonesty this repo's rules exist to prevent.

## Harness fix that made this measurement possible

`compete` hardcoded warm-up/timed INSERT ids at 600K/700K/800K/900K, which
collide with the seeded order range (rows×5) past ~120K rows — every run
above that panicked on a duplicate key before the first measurement. The id
bases now sit above 10M. Also note the ClickHouse tables persist in its
server between runs; the harness drops/recreates them itself.
