Card: ENF-FASTGATE-1
Class: core
Card-gate: P
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: P
Gate-model: claude-opus-5-5 (medium)
Gate-session: claude-code session 5f879985-9e5b-4703-92bd-dc9096ee8e3d (T3 Code review sub-agent of T3 thread 45584af2-e651-490e-862c-3402320cffa3; independent confirming P gate 2 after rework 1, risk tier R; it wrote neither the card, any ENF-FASTGATE-1 commit, nor gate 1's verdict 78137144)
Date: 2026-11-27
Gate-exit: GATE EXIT=0
Commit: 5d7f549d03d8c414a1129fa12c3ee499e191dd12
Mutants: each planted by this gate in its own `git archive 5d7f549d nucleus/v3 .forgejo | tar -x -m` extract, whole suite per mutant, each in a private /run/user tmpfs. FG-12 -> GS24, GS25 x4, FGS3; FG-13 -> GS26; FG-14 -> GS27; FG-15 -> GS28; own: Z-ATTR-ROOT -> GS25 (root); Z-ATTR-VERDICTS -> GS25 (verdicts/). Gate 1's walk of FG-1..FG-11, RN-1..RN-4 and its own sweep on ce16f090 stands (rework 1 touches none of the code those mutants plant into except the tooling line, re-mutated here).
Followups: ENF-FASTGATE-1-F1

## Verdict in brief

**MERGE-WITH-FOLLOWUPS** on rework 1, `5d7f549d03d8c414a1129fa12c3ee499e191dd12`. Gate 1's blocking R1 is closed: an attribute-only change at any of the five ancestor `.gitattributes` paths now runs the suite, and through the scaffold's real `fresh-gate.sh` the R1 card now ends `GATE EXIT=1`, as the old gate.sh did. The rework only adds: five trigger paths, rows GS24–GS28 and FGS3, and a docstring. It removes no row and widens no skip. The owner's safety condition now holds. ENF-FASTGATE-1-F1 (the per-run suite marker) carries from gate 1, unchanged.

## Fresh gate (GATE-PROMPT step 1, Fast-track)

- Extract through the shallow `--no-local` clone: no EXTRACT FAILED line. Scaffold id `d155924534028dfc147aaad3d0991575fae4a2c3`; `fresh-gate.sh` sha256 `40a5b232a5ac2c0cd1629ab6f7aa24396e1a6379d36db536a96a84ba35404899` (same as gate 1's).
- Run under `systemd-run --user --scope --quiet -p CPUWeight=10000 --`, which worked, with `CARGO_TARGET_DIR=~/.cache/nv3-target/gate2-ENF-FASTGATE-1`, 22:44:33–23:04:44 MST, beside this gate's six mutant lanes (load ~18–25).
- `fresh-gate:` lines, verbatim: `fresh-gate: commit 5d7f549d03d8c414a1129fa12c3ee499e191dd12`, `fresh-gate: base d155924534028dfc147aaad3d0991575fae4a2c3`, `fresh-gate: checks passed (4945 files hashed in 3.12 s)`, `fresh-gate: 2 file(s) matched through their eol conversion`.
- Steps: `STEP test EXIT=0`, `STEP clippy EXIT=0`, `STEP fmt EXIT=0`, `STEP deny EXIT=0`, `STEP wasm32 EXIT=0`, `STEP core-paths-selftest EXIT=0`, `gate: enforcement-tests runs: the change touches the tooling: nucleus/v3/scripts/gate.sh nucleus/v3/scripts/promote-pass.sh nucleus/v3/scripts/test-enforcement.sh nucleus/v3/tools/run_card.py`, `STEP enforcement-tests EXIT=0`, `STEP api-surface EXIT=0`.
- Literal last line: `GATE EXIT=0`, rc 0, stderr empty.
- Cargo: 910 passed, 0 failed. Enforcement: 960 `ok`, 0 `FAIL`, `test-enforcement: all passed`. 36 `ok gate: GS` rows and 4 `ok fresh-gate: FGS` rows, which matches Acceptance line 1 (GS 36, FGS 4). GS24, GS25 ×4, GS26 (premise and row), GS27, GS28 (premise and row) and FGS3 are each `ok`.
- **Known load rows:** none failed in the fresh gate, so the enforcement step was not re-run.
- `$fg` removed afterwards.

## R1 is closed (`~/.cache/nv3-review/gate2-ENF-FASTGATE-1/probe/attr-e2e.sh`)

This is gate 1's end-to-end scenario, widened to all five locations. Each run uses a throwaway repository whose scaffold carries one gate.sh revision, and a card commit that adds only an attribute file setting `eol=crlf` on a tooling path. Each runs through the scaffold's real `fresh-gate.sh` (the extract above). The stub suite starts `set -euo pipefail`.

| Attribute file (card's only change) | Target | tip 5d7f549d | old gate.sh d1559245 | gate 1's commit ce16f090 |
|---|---|---|---|---|
| `.gitattributes` | suite | runs, reason names `.gitattributes`, `STEP enforcement-tests EXIT=2`, `GATE EXIT=1` | `EXIT=2`, `GATE EXIT=1` | `SKIPPED`, `GATE EXIT=0` |
| `nucleus/.gitattributes` | suite | runs (named), `EXIT=2`, `GATE EXIT=1` | `EXIT=2`, `GATE EXIT=1` | `SKIPPED`, `GATE EXIT=0` |
| `nucleus/v3/.gitattributes` (gate 1's R1) | suite | runs (named), `EXIT=2`, `GATE EXIT=1` | `EXIT=2`, `GATE EXIT=1` | `SKIPPED`, `GATE EXIT=0` |
| `nucleus/v3/docs/.gitattributes` | docs/README.md | runs (named), `EXIT=0`, `GATE EXIT=0` | runs, `GATE EXIT=0` | `SKIPPED` |
| `nucleus/v3/verdicts/.gitattributes` | verdicts/README.md | runs (named), `EXIT=0`, `GATE EXIT=0` | runs, `GATE EXIT=0` | `SKIPPED` |

Each run printed `fresh-gate: 1 file(s) matched through their eol conversion`. The tip now matches the old gate in every case. (The docs/verdicts stubs do not read their README, so green is the right result there; the point is that the suite runs.)

- **Inputs are complete.** The suite's tree inputs are still `$src/scripts/*`, `tools/run_card.py`, `CORE_PATHS` and the two READMEs (test-enforcement.sh 96–100, 559, 679, 5033, 5753, 5872). The only directories whose `.gitattributes` can apply to those paths are their ancestors, and those are the five listed. Attribute files under scripts/ and tools/ already fall inside the prefixes. GS23 still checks the list against the copies.
- **Nothing weakened.** `git diff 78137144 5d7f549d` shows:
  - gate.sh: a comment and the `tooling=` line, which only grows;
  - test-enforcement.sh: +42 lines, 0 removed. The one later edit, 5d7f549d, is to GS28's premise, a row this same rework added. It narrows the plain diff to `nucleus/v3/tools` and adds a positive `--ignore-submodules=none` check, so the premise is now stronger.
  - run_card.py: the docstring only (gate 1's NIT 3).

  No existing row, skip condition, engine step or `NV3_GATE_FULL` handling changed.

## Mutants (this gate's own planter, `mutants.py`; exact-match edits, count checked)

Six lanes ran at once in private `/run/user` tmpfs namespaces (the F1 isolation), plus one later lane. PRISTINE in the same batch: 960 ok, rc 0.

| Mutant | Target rows failed | Result |
|---|---|---|
| FG-12 ATTR-DROPPED | GS24, GS25 ×4, FGS3 | caught, as named |
| FG-13 INDEX-FLAGS-IGNORED | GS26 | caught, as named |
| FG-14 SKIP-SUBSTRING | GS27 | caught, as named |
| FG-15 NO-IGNORE-SUBMODULES (2 sites) | GS28 | caught, as named |
| Z-ATTR-ROOT (own: only `../../.gitattributes` dropped) | GS25 (root) | caught |
| Z-ATTR-VERDICTS (own: only `verdicts/.gitattributes` dropped; run alone) | GS25 (verdicts/) | caught, and the only FAIL |

**Load noise in the six-lane batch, not target rows.** These rows failed while the fresh gate's cargo steps ran beside the lanes:
- `runner e2e: R9 packed: …` (FG-12, FG-14, FG-15, Z-ATTR-ROOT);
- `runner e2e: V: opencode runs with …` ×2 and `runner e2e: Q: …self-ignoring .gitignore…` (FG-12);
- the K-sibling `runner e2e: Z: K's SIGTERM shape …` (FG-13, FG-14), which gate 1 also saw.

None of them reads gate.sh's decision. To check, FG-12 was re-run alone at low load (`FG-12.r2`): 954 ok, and exactly GS24, GS25 ×4 and FGS3 failed.

## Scope

- `git diff --name-only nucleus/v3-scaffold...5d7f549d` lists exactly the five Touch-only files and the verdict file.
- `git diff 78137144 5d7f549d` does not touch the verdict file.
- The worktree is clean.

## The owner's condition: ruling

**It holds.** A card-controlled change cannot make this gate skip enforcement where the old gate would have failed it and let that reach the scaffold:
- A skip needs a positive reading that no suite input changed. With rework 1, that covers every blob and every attribute file that can change those inputs' checked-out bytes. Every other byte-rewriting route (info/attributes, core.attributesFile, filters, non-eol attributes) is refused by fresh-gate.sh, as gate 1 established.
- Forged git state turns the decision to "run".
- The only variable, `NV3_GATE_FULL`, forces more testing, and promote and CI set it.
- Every engine step runs on every card.

The one remaining wrong skip is gate 1's NIT 1, a criss-cross history with two merge bases. It is not card-gate-equivalent, but it is scaffold-safe: the merge rewrites what it changes, and the promote's forced-full run tests the merged bytes. Unpinned code properties (Z-DEFER-WIDE, Z-DECISION-LATE) can change only through a scripts/ or tools/ edit, which runs the suite and gets a P read.

## NITs

- NIT: gate 1's NIT 1 (multiple merge bases) is still open, as the card's Rework 1 section records. It is scaffold-safe, so it is not a defect.
- NIT: GS26 leaves its hidden `run_card.py` edit in `$gs` until the next `gsb`'s `checkout -f` and `clean`. GS27 and GS28 call `gsb` first, so no row is affected. A local `checkout -f` right after GS26 would make that explicit.

## Follow-ups

- **ENF-FASTGATE-1-F1** (carried from gate 1, text unchanged; see verdict 78137144's Follow-ups): a per-run runtime marker in test-enforcement.sh, so that parallel suites do not share `/run/user/<uid>/nv3-suite-marker`. It needs the owner's word (protected scripts/). This gate ran every mutant in a private `/run/user` tmpfs because of it.

## History

- Gate 1, 781371447f1542babbb24d4d6281d6ae7a66c6e8, REWORK on ce16f0902e2b361556183f877cdfd5c793d10e15, for R1 (attribute files). Its rulings on change (2), the CI hunk, the parsers and its orchestrator actions after merge all stand.
- Gate 2 (this one): MERGE-WITH-FOLLOWUPS on rework 1, 5d7f549d03d8c414a1129fa12c3ee499e191dd12.
