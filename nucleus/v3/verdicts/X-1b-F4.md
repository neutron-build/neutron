Card: X-1b-F4
Class: mechanical
Card-gate: S
Verdict: MERGE
Gate-tier: S
Gate-model: zai-coding-plan/glm-5.3
Gate-session: GLM gate subagent (T3 review session, card X-1b-F4)
Date: 2026-10-23
Gate-exit: GATE EXIT=0
Commit: 7bf3a07b
Mutants: DROP-NOTE (the 6-line spread note removed) -> the card's named catcher, the diff check (6 deletions vs HEAD; grep "1,592–1,770" count 0), ALTER-CELL (W5 (b) RSS cell 1,874 -> 1,897) -> re-derivation from verdict X-1b's re-measurement table ("1,874 vs 1,874" gate/RESULT; 1,897 appears nowhere in the record) plus the diff check showing a pre-existing measured cell changed, NOTE-GATE-MEDIAN (the note's gate median 1,728 -> 1,782) -> re-derivation from verdict X-1b ("1,728 vs 1,594"; 1,782 appears nowhere) plus the note's own +8% arithmetic (1,782/1,594 = +11.8%)
Followups: none

## Findings

**Gate run.** The literal last line was `GATE EXIT=0` on 7bf3a07b (every
step EXIT=0: test, clippy, fmt, deny, wasm32, core-paths-selftest,
enforcement-tests), run foreground with
`CARGO_TARGET_DIR=~/.cache/nv3-target/gate-X-1b-F4`.

**Scope.** `git diff nucleus/v3-scaffold...HEAD --stat`: only
`nucleus/v3/spikes/e1-bench/README.md` (6+) — exactly the Touch only
list. No protected path; the diff is additions-only, so every
pre-existing number, workload and decision-rule clause is
byte-identical.

**Correctness.** The note states five facts; each traces to verdict
X-1b (721c34c4, byte-identical in this worktree): the spread
1,592–1,770 MiB (verdict follow-up, verbatim), the gate median 1,728
vs the campaign's 1,594 (re-measurement table), +8% (verdict text;
re-derived: 1,728/1,594 − 1 = 8.4%), gate runs 2 and 3 at 1,770 MiB
(verdict text), and "raw `--out` JSONs were never committed" (checked:
`git ls-files spikes/e1-bench` has no results JSON). "This campaign's
median" matches the README's method line ("3 runs per workload, median
+ min/max"). The second 1,594 MiB occurrence, in decision-rule clause
4, was correctly left untouched per the card's Do-not. No code, so no
unsafe/unwrap/docs concerns.

**Re-measurement.** The +8% arithmetic re-derived independently; the
spread endpoints and 1,770 MiB cross-checked against the verdict's
record. Nothing was tuned: the note adds context to a cell, it does
not move any decision input.

**Mutation evidence.** All three mutants caught (list above); each
reverted with `git diff` clean afterwards.

**Process observations (non-blocking, no repo work implied).** The
worker log `cards/logs/X-1b-F4.attempt1.jsonl` ends at "Waiting for
the gate to finish" with no RESULT block; the pass verdict came from
the orchestrator's runner line. The worker ran its acceptance gate as
a background command. Neither affects the committed diff.
