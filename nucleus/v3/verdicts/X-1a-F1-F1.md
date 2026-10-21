Card: X-1a-F1-F1
Class: test
Card-gate: P
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: claude-code-ed7da482-5b63-4e2b-8cc5-7868f01997b0
Date: 2026-10-21
Gate-exit: GATE EXIT=0
Commit: be72f06f
Mutants: G1-STEP3-NO-RECHECK -> escalation_gaps::inserter_recheck_yields_to_a_late_claim (5/5 idle, 5/5 under 24 hogs), G4-ESC-PASS2-KEEPS-COUNTER -> escalation_gaps::escalation_does_not_inflate_the_row_counter (5/5) and inserter_recheck_yields_to_a_late_claim (5/5, fails in 0.4 s, no hang), RELEASE-SCANS-ALL-UNCOUNTED -> escalation_gaps::release_and_escalation_visit_only_own_shards_and_count_them (5/5), OWN-EXTRA-SCAN (gate: counted targeted loop kept, plus an uncounted full 64-shard retain scan in release_all) -> UNCAUGHT (accepted, stated residual), OWN-UNDO-RETURNS-OK (gate: step (3) undoes the row then returns Ok) -> escalation_gaps::inserter_recheck_yields_to_a_late_claim (5/5), OWN-UNDO-KEEPS-COUNTER (gate: remove_row skips the rows decrement) -> escalation_gaps::inserter_recheck_yields_to_a_late_claim (5/5 idle, 3/3 under 24 hogs, fails in 0.4 s, no hang), OWN2-PASS2-DEC-PER-SHARD (gate: pass 2 decrements rows once per shard, not per key) -> escalation_gaps::escalation_does_not_inflate_the_row_counter (5/5) and inserter_recheck_yields_to_a_late_claim (5/5), OWN2-NO-POSTCLAIM-RETEST (gate: try_escalate skips the post-claim re-test) -> UNCAUGHT by escalation_gaps (0/5; outside this card's seams), caught by the in-crate common::lock_table::tests::escalation_claim_reclaims_racing_inserters
Followups: X-1a-F1-F1-F1, X-1a-F1-F1-F2

## Findings

**Round 2.** Round 1 (verdict REWORK, commit 8751014d, on 30b58002) found
G4 sound but the G1 detector unsound under load (a stale `escalated` flag
gave false failures, 15/15 with 24 hogs), the escalant loop hanging on a
blocked escalation path, and G3's accounting overstated as independent.
It prescribed R1–R3. This round gates the rework commit be72f06f.

**Gate run.** `GATE EXIT=0` on be72f06f; every STEP EXIT=0. Run in the
foreground with `CARGO_TARGET_DIR=~/.cache/nv3-target/gate-X-1a-F1-F1`.

**Scope.** `git diff nucleus/v3-scaffold...HEAD --stat`: only
`spikes/e1-mvcc/tests/escalation_gaps.rs` (inside Touch only), plus the
round-1 verdict file the gate wrote. No `src/` change, no hook, nothing
else in protected paths. Every mutant was reverted and the tree was clean
before this file was written.

**R1 (resolved).** A round is now recorded only when the
`is_escalated(ESCALANT)` flag holds *and* a `NoWait` acquire by a third
txn (`PROBE`) on a fresh u64 key fails with `LockNotAvailable`. I checked
the soundness argument against `src/common/lock_table.rs`:
- `escalated` is set only after pass 2, so only a claim that survived its
  post-claim re-test can set it.
- A stale flag (release_all's window between clearing the owner and
  removing the index entry) has no owner, so the probe succeeds and is
  released.
- The escalant thread cannot start a new claim before its own
  `release_all` returns, so a stale flag never coexists with a new claim.
- The inserter's counted row blocks every later claim. The bump is
  ordered before the step-(3) table unlock, so a claim made after step
  (3) sees it.

Load probe on the unmutated tree (16 threads; busy-loop hogs started
while the test binary runs):
- G1 alone: 24 hogs **15/15 pass**, 32 hogs 10/10, 48 hogs 6/6.
- Whole test file: 24 hogs 10/10.
- Idle: 10/10.

0 false failures. The detector keeps its power under load: the G1 mutant
fails 5/5 with 24 hogs (10–94 flagged rounds per run).

**R2 (resolved).** Both escalant loops check `stop`. A cycle is bounded
(100 000 acquires or 5 s), and the bound runs a quiesced verification
rather than failing outright: park the inserter, confirm
`entries() == held_by(ESCALANT)`, then make one acquire, which must claim.
Under OWN-UNDO-KEEPS-COUNTER and G4 the test now fails in about 0.4 s
(it used to hang). With 24 hogs it does not hang either.

**R3 (resolved by statement).** The module docs and the G3 test docs now
say plainly what is covered. The exact count catches a scan that
*replaces* the targeted one (counted or uncounted). An *additional*
uncounted scan (OWN-EXTRA-SCAN) needs a `src/` hook, which the card
forbids. That meets the card's literal Work 3 ("a full scan … without the
counter moving") and the Do-not clause. The residual is a cost property
(L-RELEASE complexity), not a safety one, so I do not escalate it.
Whether to add the hook is listed as follow-up F2.

**Minor (non-blocking, not rework).**
1. The quiesced attempt assumes `escalation_retry_step() == 1`, which
   holds for `with_escalate_at(2)`. If someone raises the budget,
   `retry_at` could skip the one verification acquire and false-fail.
   The doc comment should say so.
2. The `pause`/`parked` handshake has a theoretical stale-`parked` read:
   the escalant could re-pause before the inserter clears `parked`. This
   needs a full cycle bound (≥100k acquires) to elapse inside one 1 ms
   inserter sleep. I could not provoke it.

## Follow-ups

**X-1a-F1-F1-F1 (test, Gate: P): pin G3-RELEASE-NO-NOTIFY outside the
S-suite.** Carried unchanged from round 1. Make
`release_wakes_the_waiter` / `release_wakes_waiter_on_released_key` (or a
new tests/ case) fail when `release_all` skips the shard condvars. Use a
`Wait::Block` far longer than the test's own join watchdog, so that the
late deadline re-test cannot mask the missing notify. Also cover the
pass-2 notify and the `table_cv` notify on release of an escalated owner.

**X-1a-F1-F1-F2 (core, Gate: P; owner decision first): independent
shard-visit accounting.** OWN-EXTRA-SCAN (an uncounted full retain scan
added beside a correct targeted loop) survives every black-box test,
because `shard_visits` is self-reported. If L-RELEASE's cost bound must be
pinned against additive scans, add a `cfg(test)`/test-hooks counter at
the shard-mutex lock site (every `shards[i].lock()` in `release_all` and
`try_escalate`), and assert it equals `shard_visits`. This touches `src/`,
so it needs the owner's approval; otherwise record the residual as
accepted and close the card.
