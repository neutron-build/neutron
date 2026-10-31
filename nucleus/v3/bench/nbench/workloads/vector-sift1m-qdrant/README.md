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

## Card B-0-F7: client-vs-server attribution and index-state evidence

Every timed query records the server's own `time` next to the
client-measured latency: pass metrics carry `server_p50_ms` /
`server_p99_ms`, `--latency-out F` writes the per-query
`client_ms,server_ms` pairs, and `--ids-out F` writes the per-query
result ids **before** the 10 ms sanity check, so a refused pass keeps
its recall evidence (`nbench vec-recall --ids F --truth F` recomputes
it). `--exact` swaps `hnsw_ef` for Qdrant brute force (a recall control
against the live index; marked `exact:true`, exempt from the ANN sanity
limit like cold passes — one explicit marker), and
`nbench vec-qdrant-info` dumps the collection's `result` document
(segment count, config, counts). The row's driver records
`collection-info.json` at init and a `.latency.csv` beside every pass
metrics file.

`diagnose.sh` (one command per client build) applies all of the above
under chain10's quiet-host window and classifies each pass with the
card's pre-registered c−s rule via `nbench qdrant-latency-verdict` —
the rule has exactly one, unit-tested, implementation. Its 2026-10-31
run on the preserved chain10 collection (grey, 92 segments,
`indexed_vectors_count` 3.86x points) measured c−s ≤ 1.54 ms with
s ≈ 8.5 ms: **the refused ~10 ms passes were server-side**, a Qdrant
configuration question for a P judgement (card Work 4 arm 2); see
`results/2026-10-31/vector-sift1m-qdrant.diagnosis.md`.

`remeasure.sh` is chain10's script with its row list cut to this row:
the quiet-host window, then `nbench run vector-sift1m-qdrant`. **Run it
with a release-built client** (`NB_BIN=target/release/nbench`): the
runner hands its own executable to the driver, and the debug client's
per-query JSON overhead (~0.4 ms measured) is harness time inside every
measured latency — chain10's refused warmup (10.01 ms) was a 9.66 ms
release-client pass plus that overhead. The sanity limit is unchanged;
a refusal is escalated, never re-run.
