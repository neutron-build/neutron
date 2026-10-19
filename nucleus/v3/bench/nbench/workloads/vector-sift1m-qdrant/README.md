# vector-sift1m-qdrant / vector-sift1m-pgvector

ann-benchmarks SIFT1M: 1,000,000 base vectors + 10,000 queries + top-100
ground truth, 128 dimensions, Euclidean distance.

## Dataset provenance

- Source: `http://ann-benchmarks.com/sift-128-euclidean.hdf5` — the file
  the ann-benchmarks project itself uses for this benchmark (fetched by
  `vector-sift1m-qdrant/prepare.sh`).
- The download is verified against a **pinned reference** sha256 + byte
  size inside `prepare.sh` (not trust-on-first-use); the recorded
  checksum also lands in `data/sift/sha256.txt`. The pin's provenance:
  this file converts to exactly 1M base / 10k query vectors of dim 128,
  and its loaded content is independently pinned by the committed
  pgvector init digest (`vector-sift1m-pgvector/expected/init.txt`).
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
- Qdrant readiness is `status=green` **with all points present**. With
  Qdrant's default optimizer policy, small tail segments legitimately
  stay unindexed (exact-scanned) while the collection is green, so
  `indexed_vectors_count == points_count` is not the readiness bar; the
  indexed fraction is printed into the init log as provenance.
- Overhead matching (gate nv3-B-0-gate-1 finding 5): the pgvector arm
  times one psql session, so the Qdrant pass uses **one persistent
  HTTP connection (keep-alive)** for all 10k queries — per-query TCP
  setup is not part of either arm's measured time.
- Transport hygiene (gate verdict ddb00ba5 defect 1): each request on
  that connection is **one buffered write** on a `TCP_NODELAY` socket.
  Written as head + body writes, the head becomes its own small TCP
  segment and Nagle's algorithm holds the body until the peer's delayed
  ACK — measured at ~40 ms per query (p50 44.07 ms, 22.47 qps) while
  single-write probes of the same collection returned in 1.6–1.9 ms.
  A per-query **p50 sanity limit of 10 ms** fails the row loudly if a
  warm pass ever regresses into that regime (cold passes are exempt —
  engine warm-up after restart is what *cold* measures; the §5.2
  published statistics are warm-run medians, which stay guarded;
  missing/unknown pass kinds are enforced, fail-closed).
- The full ann-benchmarks *framework* (Python harness with its own
  plotting/tuning loop) is not run; the dataset, parameters and metrics
  follow its protocol, implemented by `nbench vec-*` so digest checks and
  result recording stay inside this harness. Qdrant is driven over its
  REST API; per-query latencies are recorded for Qdrant only, QPS is
  the compared metric across both arms.
- Load-path robustness: collection create/delete, point upserts and
  status polls are idempotent and re-sent with bounded backoff on
  transient transport failures (one dropped response at ~564k/1M points
  voided the 2026-10-18 qdrant load with the server healthy). Timed
  search passes never retry — a transient failure there fails the row
  instead of hiding inside the measurement.
