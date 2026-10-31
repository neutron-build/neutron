Two rows share this directory: B-0-F7 and B-0-F7-F1b both ran on 2026-10-31
(UTC), so both wrote `vector-sift1m-qdrant.json` and `REPORT.md` at this path,
each overwriting the earlier row's files in place. Nothing was lost: the
retired row survives in git.

**Retired row (B-0-F7).** Commit `04479d54`, blob `f4b2f475`: not reproducible
(verdict B-0-F7, finding 1), must not be used as a baseline. Retrieve it:

    git show 04479d54:nucleus/v3/bench/nbench/results/2026-10-31/vector-sift1m-qdrant.json

Its `REPORT.md` survives in the same commit, blob `19db74e4`.

**Current row (B-0-F7-F1b).** `vector-sift1m-qdrant.json` now holds run A of
B-0-F7-F1b, byte-identical to `repro/vector-sift1m-qdrant.run-a.json`; J6 says
REPRODUCED (verdict B-0-F7-F1b, on bc58fb63; merged 52516aa4). `REPORT.md` was
regenerated from it, both overwritten in place because B-0-F7-F1b's runs fell
on B-0-F7's UTC date.

**Which file describes which row** (last writer named per file):
- Current row (B-0-F7-F1b): `vector-sift1m-qdrant.json`, `REPORT.md` (bc58fb63),
  and `repro/` — run A (6e34cd90), run B (326d92e5), `verdict.txt` (bc58fb63).
- Retired row (B-0-F7, 04479d54): `vector-sift1m-qdrant.row-collection-info-at-init.txt`,
  `vector-sift1m-qdrant.row-ef16-run-0.latency.csv`.
- B-0-F7 diagnosis of the retired row and the chain10 era (8ee0e27c):
  `vector-sift1m-qdrant.diagnosis.md` (appended at 04479d54),
  `vector-sift1m-qdrant.diagnosis-collection-info-chain10-era.txt`,
  `vector-sift1m-qdrant.diagnosis-debug-ef16.latency.csv`,
  `vector-sift1m-qdrant.diagnosis-exact-control.ids.jsonl`,
  `vector-sift1m-qdrant.diagnosis-exact-control.metrics.txt`,
  `vector-sift1m-qdrant.diagnosis-release-ef16.latency.csv`. Any path they
  cite for "the 2026-10-31 row" means the retired row.
- This note: `vector-sift1m-qdrant.SUPERSEDED.md`.

Card B-0-F7-F1b-F1, on behalf of verdict B-0-F7-F1b, finding 1.
