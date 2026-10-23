Card: X-1b-F3
Class: mechanical
Card-gate: S
Verdict: MERGE
Gate-tier: S
Gate-model: zai-coding-plan/glm-5.3
Gate-session: GLM gate subagent (T3 review session, card X-1b-F3)
Date: 2026-10-23
Gate-exit: GATE EXIT=0
Commit: 60a58004
Mutants: A-W5-VALUE (65,487 -> 65,489, line 158) -> gate re-derivation caught (65,489 x 8,192 = 536,485,888 B matches no committed byte figure), B-W6-VALUE (649,794 -> 649,843, line 90) -> gate re-derivation caught (649,843 x 8,192 = 5,323,513,856 != 5,323,112,448 B), C-CODE-LINE (pool_frames_w5 131_072 -> 65_536) -> caught by diff review (non-comment code line, violates comment-only) and by e1-bench test w5_and_w6_pools_cover_arm_b_peak_trees_with_headroom (FAILED: 65,536 < 2 x 65,487)
Followups: none

## Findings

**Gate run.** The literal last line was `GATE EXIT=0` on 60a58004
(`CARGO_TARGET_DIR=~/.cache/nv3-target/gate-X-1b-F3`; `GATE EXIT=0` proves
every step exited 0 — the script sets gate=1 on any nonzero step). Matches the
worker's RESULT.

**Scope.** `git diff nucleus/v3-scaffold...HEAD --stat`: only
`nucleus/v3/spikes/e1-bench/src/lib.rs` (6+/6-) — exactly the Touch only
list. No protected path. Every changed line is a `//`/`///` comment line; the
code constants (`536_469_504`, `5_323_112_448`, `131_072`, `786_432`) are
byte-identical. No stale `65,489`/`649,843` remains anywhere in e1-bench
(lib.rs or README).

**Correctness.** Each corrected value re-derives exactly: 536,436,736 / 8,192
= 65,483; 536,469,504 / 8,192 = 65,487; 5,323,112,448 / 8,192 = 649,794
(all remainder 0). Pool-context choice is right at all six sites: lines
86/158/268 say "old, exactly-full 65,536-frame pool" and pair with
536,469,504 B (the test's own constant), so they take the old-pool 65,487;
65,483 (corrected pool, 536,436,736 B) correctly appears nowhere in lib.rs
since that byte figure is absent from the file (it lives in the F2-fixed
README line 221). Lines 90/162/276 pair with 5,323,112,448 B → 649,794,
matching README lines 70/280. No unsafe, no non-test unwrap/expect/panic
added (comment-only diff); pub-item docs remain and are now numerically
true.

**Experiment re-run.** Re-derived every figure myself from the committed
constants (script output in the gate transcript); all six comment values
match, and both old values re-derive to byte figures that match nothing
committed. The decision rule (comments only; corrected-vs-old-pool per
context) was applied verbatim; nothing was tuned.

**Honest limit.** As X-1b-F2's verdict already recorded for comment drift:
gate.sh catches none of the comment mutants — the re-derivation is the only
catcher. The code-line mutant is caught twice (diff review + pool test).
