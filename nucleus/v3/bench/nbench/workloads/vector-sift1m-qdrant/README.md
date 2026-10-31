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
- Qdrant readiness is a **settled index**, not a green poll (card
  B-0-F7-F1, verdict B-0-F7 finding 1: a single green poll raced the
  optimizer and let the same inputs land in three different index
  states — 100% indexed, ~98.4% indexed with an exact-scanned tail, or
  wedged at 92 segments with 3.86x indexed):
  - **J3 — the tail rule.** After the upload, `nbench vec-qdrant-load`
    always sends `PATCH /collections/{c}` with
    `{"optimizers_config":{"indexing_threshold":1}}` (Qdrant's unit is
    KB; `0` would *disable* indexing). Under the default 10000 KB
    threshold whether the tail segment stays exact-scanned depends on
    the segment layout — that made ef16 latency 9x apart between two
    honest builds. The pgvector arm's HNSW index covers every row, so a
    fully HNSW-indexed collection is the like-for-like configuration;
    the value lands in `collection-info.json` (§5.2 rules 2 and 6). No
    other optimizer setting is changed, and the rule is unconditional —
    one build procedure for every run.
  - **J2 — settled, with a window.** Readiness requires every one of
    green status, `optimizer_status == "ok"`, `update_queue.length ==
    0`, `points_count` equal to the expected count, and
    `indexed_vectors_count == points_count`, holding on every poll
    across a trailing window of `max(30 s, 2 × flush_interval_sec)`
    with polls every 2 s, and `segments_count`/`indexed_vectors_count`
    identical on every poll in the window
    (`nbench vec-qdrant-settle`).
  - **J4 — recorded per restart, and equal.** The init settle writes
    `index-state-init.json`; after every container restart the cold
    pass settles again with the full window and must match the init
    state on `points`, `indexed`, `segments` and `indexing_threshold_kb`
    (else the row fails with both states printed), warmup/run passes
    re-check the same equality with one poll, and every pass's metrics
    carry the state as numbers (`index_points`, `index_indexed`,
    `index_segments`, `index_threshold_kb`, `index_waited_s`).
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

`remeasure.sh` is card B-0-F7's **historical** re-measure script
(chain10's script with its row list cut to this row). It is superseded
by `reproduce.sh` (card B-0-F7-F1, below), which adds the two-run
reproducibility verdict; do not run it for new rows. Its 2026-10-31
run produced the row that verdict B-0-F7 found honest but not
reproducible — that is the evidence for this workload's settle rules.

## Card B-0-F7-F1: deterministic index state and the reproducibility rule

**One build profile (J5).** `nbench run` refuses any non-smoke row from
a debug build — the debug client's per-query JSON work (~0.3–0.4 ms
measured) is harness time inside the competitor's latency, about 100%
of a 0.34 ms p50, and the pgvector arm times a release binary — and
every results JSON records `provenance.nbench_build.profile`. Build
with `cargo build --release -p nbench`.

**`reproduce.sh` a|b|verdict|check|selftest** runs the two-run
protocol (card B-0-F7-F1b uses it; environment `NB_HARNESS` and
`NB_BIN`, a release build):

- `a` / `b` — one fresh-build row each, on a quiet host (13
  consecutive 10 s samples with load1 < 4, no `cargo`/`rustc` process
  and no **active** `nbench-*` user unit; deadline 30 min, then exit 3
  "host not quiet" — no run, not a refusal, and nothing is created).
  `NB_BIN`'s build profile is probed before anything starts: a debug
  client is refused with nbench's own J5 message (exit 4). A run that
  printed a `results:` line was **recorded** — on success or on VOID
  (a digest mismatch writes the JSON and exits 1); its JSON,
  `index-state-*.json`, `collection-info.json`, init log and every
  settle transcript land in `results/<date>/repro/`, and it is never
  re-run (exit 4 on retry). A FAILED run keeps its log and its
  work-dir evidence is copied into `repro/run-<x>/` before the script
  exits, so run B on the same UTC date cannot overwrite it. A killed
  attempt is kept as `run-*.killed.log` (one retry). Run B goes into
  the same `repro/` dir as run A, and refuses early if run A's
  outcome (FAILED or VOID) already decides INVALID. Run A is the row;
  run B is evidence only; there is no third run.
- `verdict` — writes `repro/verdict.txt`: `nbench qdrant-repro-verdict
  --a F --b F` when both JSONs exist, or `REPRO VERDICT=INVALID (run X
  refused/voided: …)` for a failed or voided run. On REPRODUCED only,
  run A's JSON is copied back as
  `results/<date>/vector-sift1m-qdrant.json` and the report
  regenerated.
- `check` — outcome-neutral re-derivation (the F1b Acceptance): the
  verdict recomputed from the committed files must equal verdict.txt's
  last line, REPRODUCED must hold exactly when the row file exists and
  is byte-identical to run-a.json, and exactly one `repro/` dir must
  exist.
- `selftest` — the stub-driven protocol test (gate round 1 rework
  acceptance): a fake `NB_BIN` replays `nbench run`'s success, VOID,
  FAILED and debug-refusal outcomes against a throwaway root with the
  quiet wait bypassed loudly; it needs no host, no podman and no
  dataset, and passes when the protocol above behaves as written.

**J6, the reproducibility rule (pre-registered, applied verbatim by
`nbench qdrant-repro-verdict`):** two fresh-build runs, A then B, on a
quiet host.

- **REPRODUCED** iff, at **every** `ef_search` point (the two runs must
  have the same set of points):
  - warm-median qps agree within 15%;
  - warm-median p50_ms agree within 15%;
  - warm-median recall_at_10 agree within 0.002 absolute.
- "Within 15%" means |a − b| ≤ 0.15 × min(a, b). Boundaries are
  inclusive, with a 1e-9 tolerance so float rounding cannot decide a
  decimal boundary.
- **INVALID** (checked first) if either run is `digest.voided`, lacks
  `provenance.nbench_build.profile == "release"`, or has any pass
  without the index-state metrics (every series' cold metrics must
  carry `index_indexed == index_points`, every warm aggregate
  `index_segments`).
- Otherwise the verdict is **NOT-REPRODUCED**. The row is refused and
  escalated to P; do not re-run until they agree.

`smoke.sh` runs the tagged smoke (two ef points, one run each — not a
measurement) and checks the settle machinery structurally: the state
files exist, `indexed == points`, and the cold states equal the init
segments.
