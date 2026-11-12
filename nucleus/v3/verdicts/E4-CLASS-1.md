Card: E4-CLASS-1
Class: test
Card-gate: P
Verdict: MERGE
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: T3 review sub-agent, Claude Code (d57dbcb6-9932-4953-8bb3-4d3391204ea2, 2026-11-11), re-gate after REWORK f1dbeb0d
Date: 2026-11-11
Gate-exit: GATE EXIT=0
Commit: ae03fb8ef04d2149251ff8206078171f3ab8c393
Mutants: M-TWIN-EVERY-ROUND -> unmatched-real-c-seed, M-INTERNAL-EXC-EXIT1 -> internal-error-is-exit-2, M-TWIN-INDEX-OPTIONAL -> missing-twin-index-unmatched-round, M-TWIN-GATE-ONE-CLASS (first) -> missing-twin-index-first-class-takes-mw, M-TWIN-GATE-ONE-CLASS (last) -> missing-twin-index-last-class-takes-mw, G-TWIN-GATE-ANY -> missing-twin-index-first/last-class-takes-mw, G-TWIN-GATE-CLASS-ONLY -> unmatched-bad-seed-no-twin, G-GATE-BEFORE-ROUND-FIELDS -> missing-each-field-real-c-seed, G-NOCLASS-COUNTS-ZERO -> unmatched-round, G-TWIN-GATE-FIRST-AND-LAST -> UNCAUGHT (non-blocking, see finding 4); the 21 original rows and the first gate's G-rows were caught at 93f21097 (verdict f1dbeb0d) and the rework changes none of the code they target
Followups: none

## Plain statement

The rework fixes the one blocking defect of verdict `f1dbeb0d` without loosening the contract. A failing round on a real class-C seed (`"twin": []`) now exits 1 with every line printed, and a bad seed is exit 1. Nothing I could build moved toward exit 0. The FINAL baseline still reads 2 / 4 / 5 `unmatched=0`, exit 0, byte-identical to before. Apart from the idle-host CPU measurement and the rehearsal, the matcher is ready for the final run.

## How the gate was run

- **Fresh gate.** `fresh-gate.sh` and `git-env.sh` were extracted through the `--no-local --depth 1` clone of `nucleus/v3-scaffold` at `187cb226a9bf57ab2a7ba6b2b90fb28cdaf27c6b`, then run on `ae03fb8ef04d2149251ff8206078171f3ab8c393` (target `gate-E4-CLASS-1`). Lines: `fresh-gate: commit ae03fb8e…`, `fresh-gate: base 187cb226…`, `fresh-gate: checks passed (4539 files hashed in 1.29 s)`, `fresh-gate: 2 file(s) matched through their eol conversion`. stderr was empty, and the last line was `GATE EXIT=0`.
- **Scratch.** `git archive` exports of `ae03fb8e` (new) and `f1dbeb0d` (old) in my scratchpad. The worktree held no mutant, and `git status` was clean before this commit.
- **Hygiene.** No seed at or above 10000. The only runs were the five Acceptance diag seeds.

## Findings

**1. The amendment is sound and fail-closed.** The only contract change is the twin bullet: the twin entry is required only on a failing round whose seed meets every `seed` condition of at least one class. All the gating fields are in the nine seed fields that are required on every seed. `first_mismatch` now calls the same `seed_mismatch`, so no round can match without its twin being read. Round, `q` and media fields are still required on every failing round. Every other contract point is unchanged (I diffed the pre-rework copy). Scope: `f1dbeb0d..ae03fb8e` touches only `tools/e4_class_match.py` (+150 −11). The three commits are linear, with no amend. The table hashes to `55ae4734…1158`.

**2. Acceptance, reproduced.**
- Self-test: 130 cases, 0 failed.
- I rebuilt from the export and re-ran the five diag seeds (3988, 5168, 7177, 7727, 8657). The `.json` and `.err` are byte-identical to the worker's.
- With `--diag-dir`: `E4_CLASS_COUNTS refused_old_page=2 refused_old_header=4 amcheck_subtree_fallout=5 unmatched=0`, exit 0. The output is byte-identical to the old script's and to the worker's `match.out`. Without it: `unmatched=5`, exit 1, byte-identical to the old script's.
- The synthetic-C Acceptance line passes: exit 1, 13 lines, `unmatched=1`. The old script gave exit 2 (`no twin entry at index 0`) on the same file.

**3. Hostile inputs (71 whole-file variants of the baseline, old against new): every outcome as wanted.**
- **Exit 1 (old gave 2):**
  - a failing C round, alone, shaped like class 1, or on two seeds;
  - deadlock, panic, baton or defect on a C seed with a failing round;
  - the same flags on an MP class round with the twin removed or truncated;
  - `defect=""` with no twin;
  - an MP class round relabelled `mp` or `MP ` with no twin;
  - a failing MW round with no twin.
- **Exit 2, unchanged:**
  - a failing MP round whose twin is empty, truncated (9328 r1), missing `o1` or not an object;
  - a new failing MP round with no twin index;
  - a missing or non-string `class`, `deadlock=0`, a missing `defect`;
  - on a failing C round, a missing `o3`, `q`, `lost`, `mis`, `torn`, `served`, `rlen`, `terr`, `lls`, `pass_na`, `frontier` or `c`, a short `q` or a string `lost`;
  - a missing or null `twin`, and a twin entry with no `pass` or a string `pass`;
  - a bad seed with its round missing `o3`.
- **The first gate's set:** the same codes as before (opened class 1–2 rounds, class-3 variants, `terr`, `lls`, `cxx`, a failing twin, newline and Arabic-digit texts, NaN digest, no seeds).
- No variant gave exit 0 except the unmodified baseline.

**4. Mutants.** All four new rows are caught, and so are four of my own five. **G-TWIN-GATE-FIRST-AND-LAST** asks only `classes[0]` and `classes[-1]`, and it survives: no case uses a table of three or more classes where only a middle class takes the seed. It cannot affect any run, because the table is sha-pinned and its three seed signatures are equal. So it is non-blocking, and I'm not opening a card for it.

**5. Fable's perturbation claim: confirmed in kind, not in figures.** Fable's corpus is not on disk, so I built my own in-process corpus. It perturbed every seed, round and twin field of the 11 failing seeds and of 150 synthesised failing C, MW and MP rounds (14 values each, plus twin truncation and bad-seed flags): 157,865 records. 39,406 moved, all from exit 2 to exit 1, and all on seeds no class takes. Nothing else changed, including the count lines on every exit 0 and exit 1 outcome.

**6. The catch-all and printing failures.**
- `internal-error-is-exit-2` forces the catch-all and requires exit 2 with the one line. A RecursionError from deep JSON is also exit 2.
- **Printing failures can never produce a false exit 0.** The exit code is computed before printing.
  - Stdout closed: the right exit code (0, 1 or 2) and no lines.
  - `/dev/full`: exit 120.
  - An error part-way through printing: a traceback and exit 1.
- **What this means for the final run.** A failed print can turn a STOP or a pass into exit 1 or 120, or drop the evidence lines. It cannot turn a fail into a pass. For the final run it matters only for routing and evidence: P's gate should treat any exit other than 0, 1 or 2, or a missing last `E4_CLASS_COUNTS` line, as a STOP. That belongs to the REFRESH procedure, not to this card.

## What remains before the final E4 run

The idle-host CPU measurement and the rehearsal. Nothing in this card blocks the run.
