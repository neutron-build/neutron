Card: X-1b
Class: experiment
Card-gate: P
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: claude-code review subagent, session eb46019c-ae4f-48fa-84e7-b3d72b1d4ce6
Date: 2026-10-23
Gate-exit: GATE EXIT=0
Commit: 79886d0a
Mutants: SEED-PER-ARM-W1 (rng salted by type_name::<E>()) -> w1_put_streams_are_identical_across_engine_types, SEED-PER-ARM-W2 (same, W2 reader) -> w2_reader_and_writer_streams_are_identical_across_engine_types, POOL-PER-ARM (make_c gets pool/4) -> factories_forward_their_pool_verbatim, VERIFY-BYPASS (report's arm_c_passed forced true) -> verify::tests::report_passed_requires_both_arms + verification_fails_a_broken_engine, W5-ALIVE-OFF (original never set false) -> w5_flags_a_reader_that_reads_wrong_values, W6-POOL-OLD (pool_frames_big back to 393,216) -> tests::w5_and_w6_pools_cover_arm_b_peak_trees_with_headroom, UNDO-BUDGET-HALF (make_c gets budget/2) -> arm_c_factory_forwards_the_undo_budget, SEED-BY-NAME (W1 rng salted by e.name().len()) -> UNCAUGHT, POOL-3/4 (make_c gets pool - pool/4) -> UNCAUGHT, VERIFY-BYPASS-RUN (verify::run replaces arm c's check with a pass) -> UNCAUGHT, W5-POOL-SHARED (w5 builds with cfg.pool_frames) -> UNCAUGHT, ARMA-RESEED (arm (a) re-seeds from the stream's first word) -> UNCAUGHT
Followups: X-1b-F1, X-1b-F2

## Findings (round 2, rework of verdict 7421fb37)

**Gate run.** `GATE EXIT=0` on 79886d0a (every step EXIT=0). Scope: the
diff against `nucleus/v3-scaffold` touches only `nucleus/v3/spikes/e1-bench/`
plus this file. No protected path, no `unsafe`, no non-test
unwrap/expect/panic, and the pub items have docs. Every mutant was
reverted, and `git status` was clean before this file was written.

**Rework checklist.**
1. W6 pool: met. It is 786,432 frames (6 GiB) on both arms. In the gate's
   re-run, (b)'s post-commit tree peaked at 5,323,661,312 B (82.6% of the
   pool) and (c)'s at 2,745,434,112 B (42.6%). W5 is 131,072 frames, and
   (b) peaked at 50.0% of it. W5 and W6 were re-measured. The README now
   states the head-of-line sample counts and says that p99 equals the max.
2. Integrity tests: met for every round-1 mutant. The recording `Sim<TAG>`
   engines run through `w1::run`/`w2::run`. A broken engine (its commit
   always fails) runs through `check_arm`/`report_passed`. A lying engine
   runs through W5. All four round-1 mutants, re-applied by the gate, fail
   a named test. The guards are partial (see F1).
3. Arm (a): met. The code now consumes `rng_for(W_W1, run, tid, 0)` and
   `w1`'s value derivation, so the comment is true.
4. W1/W2/W3 method and the decision-rule text are unchanged: `w1.rs`,
   `w2.rs` and `w3.rs` have no diff since 7421fb37, the only `w5.rs`
   change is the pool, and the rule quote is identical.

**Re-measurement.** Same release binary, quiet host (load1 ≤ 3.0),
3 runs each, medians. Gate vs RESULT:

| metric | (b) gate / RESULT | (c) gate / RESULT |
|---|---|---|
| W6 fill / commit s | 28.0/20.4 vs 28.2/20.4 | 19.5/23.3 vs 19.8/23.3 |
| W6 HOL p99 (= max) s | 17.73 vs 17.94 | 21.08 vs 21.16 |
| W6 delayed n (min–max) | 14 (11–15) vs 18 (4–32) | 4 (4–10) vs 9 (4–18) |
| W6 write-set / locks / escalations | 1.17e9 / 1 / 1, same | same |
| W5 peak tree B | 536,412,160 vs 536,436,736 | 274,546,688, same |
| W5 peak RSS MiB | 1,874 vs 1,874 | 1,728 vs 1,594 |
| W5 originals / get p50 ns | yes / 2,507 vs yes / 2,531 | yes / 2,948 vs yes / 2,905 |
| W1(a) CPU µs t=1/4/8/16 | 53.7/66.6/112.6/133.4 vs 55.7/76.6/108.1/137.1 | n/a |

The headline numbers reproduce within about 2%. Three cells are noisier:
- W5 (c) RSS is +8%: the gate's runs 2 and 3 came in at 1,770 MiB.
- W1(a) at t=4 is −13%. Arm (a) is reference only.
- **W6 head-of-line p50 is not reproducible.** The delayed set is bimodal
  (about 2–5 ms, or about the whole commit). The gate's medians are
  182 ms (b) and 20.8 s (c), against the README's 4.6 ms and 4.1 ms. Only
  p99/max is stable.

The decision is unaffected: W1 and W2 drive it, the rule gives
"Otherwise: pick (b)", and both flagged readings remain X-4's call.

**Non-blocking defects.**
- The README's W6 RSS uses mixed units. KiB/1,024,000 is labelled GiB:
  17.2 should read 16.8 GiB, and 14.8 should read 14.4 GiB.
- The README's "65,489 pages = 536,436,736 B" should be 65,483.
- In `measurement_integrity.rs`, `heavy_gate()` unwraps a poisonable
  mutex, so a single failure cascades into the CPU-window test.

## Follow-ups

**X-1b-F1 (test, S): harden the measurement-integrity guards.** Five gate
mutants survive the tests:
- SEED-BY-NAME: give the `Sim` tags distinct `name()`s.
- POOL-3/4: make the pool forwarding observable, for example a pool
  accessor or a recording check at pool > 1, instead of relying on 1→0.
- VERIFY-BYPASS-RUN: smoke-test `verify::run` on the real arms, and test
  main's abort path, for example by running the binary under a
  broken-arm feature.
- W5-POOL-SHARED, and the analogous W6 case: assert that each workload
  calls its factory with its named pool.
- ARMA-RESEED: add an arm (a) stream-parity test.

Make `heavy_gate` poison-tolerant. Touch only
`spikes/e1-bench/tests/` and test modules.

**X-1b-F2 (mechanical, S): correct the README.** Fix the W6 RSS units
(16.8 / 14.4 GiB) and the W5 page arithmetic. Mark the W6 head-of-line
p50 as unstable: it is bimodal over n = 4–32 and the gate's re-run gave
182 ms / 20.8 s, so X-4 should read only p99/max (≈ commit time). Note
the W5 (c) RSS spread (1,592–1,770 MiB).
