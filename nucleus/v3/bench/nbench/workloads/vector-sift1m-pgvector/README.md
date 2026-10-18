# vector-sift1m-qdrant / vector-sift1m-pgvector

ann-benchmarks SIFT1M: 1,000,000 base vectors + 10,000 queries + top-100
ground truth, 128 dimensions, Euclidean distance.

## Dataset provenance

- Source: `http://ann-benchmarks.com/sift-128-euclidean.hdf5` — the file
  the ann-benchmarks project itself uses for this benchmark (fetched by
  `vector-sift1m-qdrant/prepare.sh`, sha256 recorded into
  `data/sift/sha256.txt`).
- The texmex original (`ftp.irisa.fr`) is unreachable from this network;
  the ann-benchmarks hdf5 carries the same vectors and neighbors.
- Conversion to fvecs/ivecs happens inside a throwaway container
  (`python:3.12-slim` + h5py); ids are 0-based, matching the ground
  truth indexing.

## Protocol

- HNSW parameters fixed per card B-0: `m=16`, `ef_construction=200`.
- `ef_search` swept (default 16/32/64/128/256).
- One search pass = the full 10k query set, sequential (single client),
  k=10; recall@10 computed against the ground-truth top-10; QPS =
  queries / wall time.
- The full ann-benchmarks *framework* (Python harness with its own
  plotting/tuning loop) is not run; the dataset, parameters and metrics
  follow its protocol, implemented by `nbench vec-*` so digest checks and
  result recording stay inside this harness. Qdrant is driven over its
  REST API; pgvector through a single psql session (per-query latencies
  are therefore recorded for Qdrant only; QPS is the compared metric).
