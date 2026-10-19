Card: X-1a
Class: experiment
Card-gate: P
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: claude-code-x1a-gate-round2-2026-10-18
Date: 2026-10-18
Gate-exit: GATE EXIT=0
Commit: 50061d35
Mutants: RELEASE-BEFORE-VISIBLE -> S5 + S6a, RR-NO-CONFLICT -> S4 + S4b + S4d, SP-ROLLBACK-PARTIAL -> S9, VISIBLE-JUMPS -> S11a gated sub-check, GC-TOO-EAGER -> S12, O7-REF-RR-CHECK-BEFORE-LOCK (round-1 survivor: RR check before the lock wait only, f applied to the snapshot value) -> S4b + S4d (5/5 runs), O1-REF-UNIQUE-AT-SNAPSHOT (round-1 survivor) -> S6a + S6c + S6d, O5-REF-UNIQUE-TOMBSTONE-LIVE (round-1 survivor) -> S6d, O8-REF-OWN-DELETE-LIVE (own: an own Delete counts as live in insert_unique) -> S6e, O9-REF-RR-CHECK-GE (own: RR conflict uses ts >= snapshot) -> S4 + S11a, O7+NO-BARRIER (own: O7 with wait_parked_waiter made a no-op) -> UNCAUGHT 10/10 (expected; shows the barrier is load-bearing)
Followups: X-1a-F1, X-1a-F2, X-1a-F3

## Findings

Round 2 re-gate of the round-1 REWORK (bbd24734). The new work is d5a8964b
and 50061d35, and it touches only `spikes/e1-mvcc/` (suite.rs, the
`lock_table.rs` waiter counter, and a proptest regression seed).

**Gate run.** `GATE EXIT=0` on 50061d35, every STEP EXIT=0. Run in the
foreground with a 1500 s timeout and CARGO_TARGET_DIR=~/.cache/nv3-target/gate-X-1a.
The mutants ran in the scratch copy /tmp/opencode/x1agate2 with
`cargo test -q --release -p e1-mvcc --test reference` (300 s timeout per run)
and were reverted with `git checkout`. The card worktree is clean.

**Scope.** The full branch diff against nucleus/v3-scaffold is under
`nucleus/v3/spikes/e1-mvcc/`, plus this verdict file. No protected path is
touched and no dependencies were added.

**Oracle (re-confirmed): clean.** The round-2 diff does not touch
`reference.rs`. `RefEngine` is still spec-derived (C-T0 §3.2/§4/§5.1–5.4/§9.2)
and shares no engine code.

**D1: fixed and real.** In S4b (RR update_with), S4c (RC update_with/EPQ) and
S4d (RR put), the waiter begins before T1 writes, and T1 then takes the lock
uncommitted with NoWait. The waiter is spawned and calls the op with a 30 s
Block. `wait_parked_waiter` spins until the process-wide `WAITING` count is
non-zero, and only then does T1 commit.
- `WaitingGuard` is created inside both of `acquire`'s wait paths, after
  every pre-acquire step the engine runs. So the barrier proves the waiter
  ran its pre-checks while the blocker held the lock.
- O7 is now caught in 5/5 runs.
- With the barrier stubbed out, O7 survives 10/10 runs. The waiter thread is
  not scheduled before T1's commit, so the barrier (50061d35) is necessary,
  not decorative.
- S4c expects 102 and commit ts 3.

**D2: fixed and real.** The three fixed-order cases:
- S6c: the RR snapshot predates a committed insert, and the insert gets 23505.
- S6d: an insert over a tombstone newer than the RR snapshot succeeds while
  the snapshot still reads v0.
- S6e: own delete then insert_unique succeeds, and a second insert over the
  own write gets 23505.

S6a and S6b now begin the waiter before the holder resolves, and S6b also
pins the commit ts. O1 is caught by S6a, S6c and S6d. O5 is caught by S6d.

**Correctness.** Neither the counter nor the barrier changes engine
semantics. The only extra work is two atomics on the path that is about to
park. No new `unsafe`, `unwrap` or `expect` outside tests, and every pub item
has docs. Clean runs were stable, 5/5 repeats.

**Minor.** `tests/model.proptest-regressions` stores a seed that shrinks to
the SP-ROLLBACK-PARTIAL shape. It is almost certainly an artifact of a worker
mutant run. It is harmless and passes on the tip, so it is kept.

## Follow-ups

**X-1a-F1, X-1a-F2.** Carried over unchanged from round 1 (shared-component
overhead skews the E1′ ratios; S13 lacks `gc()` and mid-schedule begins). Both
must land before X-2 or X-3 measure.

**X-1a-F3: the suite's process-global hooks require one `run_all` per test
binary.** `WAITING`, like `apply_gate`, is process-global. A second test that
parks on a lock concurrently would satisfy `wait_parked_waiter` early and
silently weaken S4b–S4d and S6a/S6b back to scheduling luck. Today that is
safe: `run_all` is the only `#[test]` in `tests/reference.rs`, and the lib
tests are a separate binary. But X-2 and X-3 will add arm test files. Fix
either:
- in the docs: each `run_all` call in its own test binary, or
  `--test-threads=1`; or
- in code: make the barrier per-LockTable, exposed through a
  `#[doc(hidden)]` engine hook.

The barrier should also fail loudly if `waiting()` is non-zero on entry.
