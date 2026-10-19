Card: X-1a-F2
Class: core
Card-gate: P
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: claude-code-6d9962b3-3a3e-424d-9c0a-90cb752b9159
Date: 2026-10-19
Gate-exit: GATE EXIT=0
Commit: 1fba52b5
Mutants: GC-TOO-EAGER -> tests/reference.rs S13 (232 S13 failures, plus 4 S12), O3-REF-GC-DROPS-NEWEST-LE -> tests/reference.rs S13 (944 S13 failures, plus 5 S12), ORACLE-RUNS-GC (with GC-TOO-EAGER) -> 0 S13 failures, only S12 (4) catches it: the no-op oracle rule is load-bearing, GEN-NO-GC -> suite::tests::s13_generator_coverage_pins ("Gc ops with >= 2 distinct live snapshots: 0 < 3000"), GEN-NO-MIDBEGIN (no slot closed at op 0, Begin branch disabled) -> suite::tests::s13_generator_coverage_pins (pin 2 fires first: 94 < 430), SP-RESET-MISSING (generator Begin keeps sp_next) -> suite::tests::s13_generator_coverage_pins (stale_rollback_targets assert; S13 on RefEngine passes, 0 failures), G-REPLAY-GC-NEVER (own: replay_schedule never calls gc() even with run_gc) -> UNCAUGHT (pins and S13 green; with GC-TOO-EAGER added, S13 sees 0 failures), G-REPLAY-BEGIN-NOOP (own: replay_schedule's Begin never opens a txn) -> UNCAUGHT (pins and S13 green), G-WATERMARK-PLUS-ONE (own: Snapshots::oldest returns min(active)+1) -> tests/reference.rs S13 (129 S13 failures, plus 3 S12)
Followups: X-1a-F2-F1

## Findings

Gate run on 1fba52b5: every STEP EXIT=0, `GATE EXIT=0`. Scope: the diff
against nucleus/v3-scaffold touches only `spikes/e1-mvcc/src/suite.rs`, and
within it only the S13 driver (`ScheduleOp`, `gen_schedule`, a new
`sched_iso` helper, `exec_scheduled`, `replay_schedule`, `run_schedule`,
the `s13_differential` doc) and a new `#[cfg(test)]` coverage module. No
other scenario, the reference engine and the common components are
untouched. There is no `unsafe` and no non-test `unwrap`/`expect`/`panic`.
`rng.below(0)` cannot panic (`n.max(1)`).

The oracle rule is implemented as decided: only the engine under test runs
`gc()`, both sides record `Out::Unit` for `Gc`/`Begin`, and `gc()` return
values are not compared. `run_all(&RefEngine::new)` is clean, so there is
nothing to escalate: on all 500 seeds RefEngine with GC is indistinguishable
from the no-GC oracle. GC-TOO-EAGER, O3 and my watermark off-by-one are now
caught by S13 itself, which is the gap round 1 named. Every reproduced count
matches the worker's RESULT.

The worker rebalanced the generator rates (ends 5%→11%, NextStmt 5%→6%,
100–299 ops, cap 24 instead of "e.g. 12") to reach the pin floors. The
change is documented in the generator doc and RESULT, kept inside S13, and
leaves Gc 5% and Begin 8% at the card's rates. I accept it.

1. **The coverage pins measure a twin replay, not the one S13 runs.**
   `coverage_replay` reimplements the Gc/Begin handling. A regression in
   `replay_schedule` that stops calling `gc()` or stops opening Begin txns
   keeps every test green (G-REPLAY-GC-NEVER, G-REPLAY-BEGIN-NOOP). Under
   the first, S13 silently stops catching GC-TOO-EAGER. Today the driver is
   correct, by review and because S13 catches the GC mutants. The card asked
   for a test-only replay, so this is non-blocking.
2. **The txn cap arithmetic does not match its doc.** `txn_budget =
   S13_TXN_CAP − closed`, so a schedule has 20 + 2·open txns (22–26), not
   at most 24 as the doc says. Schedules stay bounded.

## Follow-ups

**X-1a-F2-F1** (test, P or S): pin the replay that S13 actually runs. Wrap
the engine `replay_schedule` drives in a test-only counting `TxnEngine`
shim, or give `replay_schedule` an observer hook that `coverage_replay`
reuses instead of duplicating it. Assert that with `run_gc = true` the
number of `gc()` calls equals the number of `Gc` ops, and with `false` it is
zero. Assert that the number of `begin()` calls equals the initial opens
plus the `Begin` ops. G-REPLAY-GC-NEVER and G-REPLAY-BEGIN-NOOP must then be
caught. In the same card, make the budget `S13_TXN_CAP − open` (or fix the
doc) and re-state the measured pin counts if they move.
