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

No variants: durability is not swept for this row (read-mostly search
workload; the index build runs with default durability).

## Limits

`--memory 24g --cpus 16`, `--network host`, volume `nbench-pgvec-data`
plus read-only `nbench-pgvec-init` for `init/00-vector.sql`.

## Licence / benchmark-publication check

pgvector: PostgreSQL Licence. PostgreSQL: PostgreSQL Licence. Neither
restricts publishing benchmarks; see `../../LICENSES.md`.
