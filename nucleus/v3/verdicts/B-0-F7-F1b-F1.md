Card: B-0-F7-F1b-F1
Class: mechanical
Card-gate: P
Verdict: MERGE
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: nv3-B-0-F7-F1b-F1-gate-1 (P review sub-agent, Claude Code session 15bdaf15-a654-4473-ab7c-fafc2ac3b5ab)
Date: 2026-10-31
Gate-exit: GATE EXIT=0
Commit: 5117e3398b69742072af28cc46fa49d995c382ad
Mutants: card names none (doc-only). Own, on a /tmp copy of results/2026-10-31 (worktree never touched): ACC-UNATTRIBUTED-FILE (dummy-unattributed.txt) -> Acceptance 2 "MISSING dummy-unattributed.txt" rc 1, caught; ACC-NEAR-NAME (repro2) -> rc 1, caught; ACC-PREFIX-NAME (vector-sift1m-qdrant.row) -> rc 0, UNCAUGHT (substring grep); ACC-FILE-IN-REPRO (repro/new-unattributed.json) -> rc 0, UNCAUGHT (check is top-level ls by design)
Followups: none

Reviewer: Opus (claude-opus-5-5), P gate (provenance of a recorded row), round 1.

# Checks

- Gate: `scripts/gate.sh` run in the foreground on 5117e339. Every STEP EXIT=0, `GATE EXIT=0`.
- Scope: `git diff nucleus/v3-scaffold...HEAD --name-only` lists only
  `results/2026-10-31/vector-sift1m-qdrant.SUPERSEDED.md`. The merge-base is the scaffold tip 52516aa4. Every evidence file is byte-identical, `repro/` included.
- Identifiers, verified with git:
  - `git rev-parse 04479d54:…/vector-sift1m-qdrant.json` = f4b2f475ec3c….
  - `…/REPORT.md` at 04479d54 = 19db74e498d5….
  - 04479d54 is "B-0-F7 re-measured row RECORDED (2026-10-31)".
  - 8ee0e27c is "B-0-F7 diagnosis artefacts (2026-10-31)", an ancestor of 04479d54.
  - The bc58fb63 → 52516aa4 ancestry holds.
- Retrieval command: the note's `git show 04479d54:nucleus/v3/bench/nbench/results/2026-10-31/vector-sift1m-qdrant.json` works as written. It prints the B-0-F7 row JSON, and `| git hash-object --stdin` gives f4b2f475….
- Current row: `cmp` of HEAD's row file against `repro/vector-sift1m-qdrant.run-a.json` exits 0.
- Attribution: every file was checked against `git log -- <file>`, and every file matches.
  - REPORT.md and the row .json: bc58fb63, with 04479d54 before it.
  - All of `repro/run-a/` and `run-a.json`: 6e34cd90.
  - All of `repro/run-b/` and `run-b.json`: 326d92e5.
  - `verdict.txt`: bc58fb63.
  - The five `diagnosis-*` files: 8ee0e27c.
  - `diagnosis.md`: 8ee0e27c, then 04479d54, which only appended to it (+41/−0). The note says "appended at 04479d54", which is correct.
  - The two `row-*` files: 04479d54 only.
- Acceptance: all 5 lines pass. Line 2 fails on an unattributed top-level file, as intended (see Mutants).
- Content stays within the card's Do-not list:
  - no numbers restated;
  - no re-judgement;
  - no copy of the retired row;
  - the card id and finding are cited.

  The note is 34 lines against the card's "about 30". That is acceptable.

# Findings

None blocking.

Observation (no card): Acceptance 2 does a substring `grep -qF` over a top-level `ls`.
- Two cases slip through: a new file whose name is a substring of a name already cited, and any new file inside `repro/`.
- This card's evidence set is frozen, so this does not matter here.
- Future provenance cards should match whole tokens (`grep -qF "\`$f\`"`) and walk `find` if they reuse the pattern.
