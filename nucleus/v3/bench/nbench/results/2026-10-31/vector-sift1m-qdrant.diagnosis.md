# Card B-0-F7 diagnosis — where qdrant's ~10 ms/query goes, and the recall shift

Date: 2026-10-31 (UTC). Harness: `bench/nbench` at card branch `card/v3-B-0-F7`
(instrumentation commit `9a743bb5`, driver `3bee0c53`). Qdrant image
`docker.io/qdrant/qdrant:latest@sha256:0e8273b9…` (pinned, reports 1.19.2),
the same volume chain10 measured (`nbench-qdrant-storage`), i.e. **the exact
collection chain10 refused**, still preserved when this card started.

## The questions (card Goal)

1. chain10 refused the qdrant row: warm per-query p50 10.01 ms > the 10 ms
   sanity limit (cold 9.95 ms exempt), while verdict ddb00ba5's single-request
   probes measured 1.6–1.9 ms — where does the ~8 ms/query go?
2. Recall@10 moved between collections built from the same pinned data
   (0.9457 chain7 → 0.8774 chain10). Does index state explain it?

## Method

Every timed query now records the server's own top-level `time` beside the
client-measured latency (`--latency-out` CSV; `server_p50_ms` in pass metrics).
`diagnose.sh` runs, per client build: chain10's quiet-host window (load1 < 4
twice, 60 s apart) before every measured pass; a `GET /collections/nbench-sift`
snapshot before/after each pass; one ef=16 pass over the full 10k query set;
one exact brute-force control over the first 250 queries. p50s are recomputed
from the CSVs with the harness's type-7 percentile; the verdicts come from
`nbench qdrant-latency-verdict` — the single unit-tested implementation of the
card's pre-registered rule (Work 4, applied verbatim):

- c − s > 2 ms → client or transport defect → fix it, the limit stays;
- else s > 5 ms → Qdrant configuration question for a P judgement (any limit
  change needs a P verdict with evidence);
- else → within tolerances.

## 1. The c−s table (Work 1, 3, 4)

All passes: ef=16, k=10, the full 10k SIFT1M query set, one keep-alive
`TCP_NODELAY` connection, single buffered write per request, against the
preserved chain10 collection.

| client | pass | c p50 (ms) | s p50 (ms) | c−s (ms) | verdict (card Work 4) | recall@10 |
|---|---|---|---|---|---|---|
| release | ef16 full | 9.661 | 8.444 | 1.216 | **SERVER-SIDE** | 0.87742 |
| debug   | ef16 full | 10.044¹ | 8.507 | 1.536 | **SERVER-SIDE** | 0.87742¹ |
| release | exact control (250 q) | (control) | (control) | (control) | not classified | **0.9992** |
| debug   | exact control (250 q) | (control) | (control) | (control) | not classified | **0.9992** |

¹ The debug pass was **refused by the unchanged 10 ms sanity check** (exit 1,
c p50 10.044) — reproducing chain10's refusal (10.01) to within 0.03 ms. The
latency CSV and per-query ids are written before the check, so the pass's
evidence survives; its recall (0.877420) was recomputed from the ids by
`nbench vec-recall`.

**The pre-registered rule fires its second arm for both builds**: c − s ≤ 2 ms
(1.22 release, 1.54 debug) and s > 5 ms (8.44/8.51). The ~8 ms is Qdrant's
own reported time per query, not the client, not the transport. Per the card,
this is **a Qdrant configuration question for a P judgement**;
`QDRANT_P50_SANITY_LIMIT_MS` stays 10.0 and nothing about the row is tuned.

The client builds differ by 0.383 ms (10.044 − 9.661): the debug client's
per-query JSON encode/parse overhead. It is not the defect (both passes are
within the 2 ms tolerance), but it alone flips a 9.7 ms server into a refused
10.0+ ms pass — the re-measure (Work 5) therefore runs the release-built
client, per card Work 3's identification of `target/debug/nbench` as a chain
methodology defect.

Run-to-run spread, for honesty: an earlier release pass on the same collection
measured c p50 9.50 ms (mean s 8.04 ms over 10k rows); the committed run above
measured 9.66/8.44. ±0.2 ms between quiet-host passes; both classify the same.

## 2. The index state (Work 2) — and it answers the recall question too

`GET /collections/nbench-sift` (full dump committed beside this file as
`…diagnosis-collection-info-chain10-era.txt`), identical at every snapshot
across the diagnosis (and across the 12 days the volume sat untouched):

```
status               grey            (never green again after the restart)
points_count         1,000,000
indexed_vectors_count 3,861,632      (3.86x the points — orphaned/mid-merge HNSW)
segments_count       92              (a search fans out to every segment)
update_queue.length  0               (nothing pending: this state is terminal)
config.hnsw_config   m=16, ef_construct=200, full_scan_threshold=10000
config.optimizer     indexing_threshold=10000, default_segment_number=0 (auto)
```

Readiness for the chain10 row was `green + all points` — and green does not
mean the optimizer settled. A 1M-point upload at batch 1024 left 92 segments
of ~10.9k vectors each; segments below `indexing_threshold` (10k) legitimately
stay unindexed (exact-scanned per query), and the collection then sat in this
grey, 3.86x-indexed, 92-segment state. Every search pays a fan-out across 92
small segments — measured here as the server's own 8.4–8.5 ms per query.

**Recall**: on this collection ef=16 recall@10 is 0.87742 — exactly chain10's
number, reproducing on both client builds (deterministic given the index).
The exact brute-force control over the same collection returns **0.9992**
(the remaining 0.0008 is float-tie ordering at the k boundary), which
validates the pinned data, the Euclidean distance convention, and the
harness's recall computation against the live index. So the shift
0.9457 (chain7) → 0.8774 (chain10) is **the index state**: chain7's collection
— measured hours after load, through the Nagle-era passes that took
7.4 min each — had settled (few merged segments, and 1.6–1.9 ms probes);
chain10's was measured ~3 minutes after upload, mid-optimization, and the
preserved state shows it never settled. The per-segment HNSW graphs a search
actually consults differ between those states; recall@10 at the same ef=16
moves with them.

What is measured vs inferred: measured — s p50 8.44/8.51 ms, c−s ≤ 1.54 ms,
92 segments, grey, 3.86x indexed count, recall 0.87742 @ ef16, 0.9992 exact.
Inferred (labeled): the fan-out over many small, partially-indexed segments
being the server-side cost; chain7's collection being settled (its state was
never recorded — the 1.6–1.9 ms probes imply it).

## 3. Instrumentation defects the first run caught (recorded, not hidden)

- The first execution of `diagnose.sh` shipped the table with s = 0.000: the
  CSV extraction awk had no `-F,`, so the comma-separated server column read
  empty — exactly the card's "server-time not recorded → the c−s table empty"
  mutant shape, caught by comparing the table against the raw CSV (real
  server column, mean 8.04 ms). Fixed; the committed passes re-ran clean.
- The operator edited `diagnose.sh` while bash was still executing it; bash
  mis-executed a word fragment ("hen: command not found") and the run's final
  snapshot lines were lost. The measurements before the crash were complete;
  both committed passes re-ran on the fixed, static script.

## 4. Consequence for the row (card Work 5)

The rule's second arm owns the outcome: **the sanity limit is unchanged**
(10.0 ms), no optimizer-settle wait or other readiness change is introduced
by this card (that is the P question), and the re-measure runs chain10's
script — quiet-host window, then `nbench run vector-sift1m-qdrant` — with the
release-built client. Its result (recorded row, or refusal by the unchanged
check with the reason surfaced per B-0-F6) lands in this directory as
`vector-sift1m-qdrant.json` with `REPORT.md`.
