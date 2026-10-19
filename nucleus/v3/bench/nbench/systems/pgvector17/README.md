# pgvector on PostgreSQL 17

Serves row: `vector-sift1m-pgvector`.

## Tuning and its sources

Same PostgreSQL guidance as `../postgres17/` (it is the same engine),
plus the HNSW build memory knob:

| Setting | Value | Source |
|---|---|---|
| `shared_buffers` | 6 GB | PostgreSQL docs §20.4 |
| `max_wal_size` | 8 GB | PostgreSQL docs §30.5 |
| `checkpoint_timeout` | 15 min | PostgreSQL docs §30.5 |
| `maintenance_work_mem` | 2 GB | pgvector README: "increasing maintenance_work_mem ... can improve HNSW index build performance" |
| port | 5433 | coexists with postgres17 on the host network |
| HNSW index | `m=16, ef_construction=200` | card B-0 Work item 3; pgvector README documents the parameters |
| distance operator | index `vector_l2_ops`, queries `ORDER BY v <-> q` | pgvector README: the ORDER BY operator must belong to the index's operator class. SIFT1M truth is Euclidean, so the index is L2 and the search must use `<->` (`<=>` is cosine: unservable by an L2 index — 1M-row seq scan — and ranked in the wrong space; gate nv3-B-0-gate-1 defect 1) |

No variants: durability is not swept for this row (read-mostly search
workload; the index build runs with default durability).

## Limits

`--memory 24g --cpus 16`, `--network host`, volume `nbench-pgvec-data`
plus read-only `nbench-pgvec-init` for `init/00-vector.sql`, and
`--shm-size 8g`: parallel HNSW builds coordinate through POSIX shared
memory and podman's default `/dev/shm` is 64MB (the postgres image docs
direct shared-memory-heavy workloads to raise it; without it the index
build failed with "could not resize shared memory segment ... No space
left on device", 2026-10-18).

## Licence / benchmark-publication check

pgvector: PostgreSQL Licence. PostgreSQL: PostgreSQL Licence. Neither
restricts publishing benchmarks; see `../../LICENSES.md`.
