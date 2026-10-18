# Qdrant

Serves row: `vector-sift1m-qdrant`.

## Tuning and its sources

Server defaults are Qdrant's documented production defaults; all knobs
that matter for the row are per-collection, set at load time:

| Setting | Value | Source |
|---|---|---|
| HNSW `m` | 16 | card B-0 Work item 3; Qdrant docs "HNSW" (default m=16) |
| HNSW `ef_construct` | 200 | card B-0 Work item 3 |
| distance | Euclid | SIFT1M ground truth is L2; ann-benchmarks uses euclidean for this dataset |
| upload | `wait=true`, wait for `status=green` and `indexed_vectors_count == points_count` | Qdrant REST docs; keeps background indexing out of the measured QPS |

No variants (durability is not swept for the vector row).

## Limits

`--memory 24g --cpus 16`, `--network host` (REST 6333), volume
`nbench-qdrant-storage`.

## Licence / benchmark-publication check

Qdrant: Apache-2.0. No benchmark-publication restriction; see
`../../LICENSES.md`.
