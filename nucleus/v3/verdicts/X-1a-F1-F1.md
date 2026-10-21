Card: X-1a-F1-F1
Class: test
Card-gate: P
Verdict: REWORK
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: claude-code-2e05071f-32e3-46fb-83dc-5f6a9c1e73be
Date: 2026-10-21
Gate-exit: GATE EXIT=0
Commit: 30b58002
Mutants: G1-STEP3-NO-RECHECK -> escalation_gaps::inserter_recheck_yields_to_a_late_claim (5/5), G4-ESC-PASS2-KEEPS-COUNTER -> escalation_gaps::escalation_does_not_inflate_the_row_counter (1/1; inserter_recheck hangs, killed by timeout 5/5), RELEASE-SCANS-ALL-UNCOUNTED -> escalation_gaps::release_and_escalation_visit_only_own_shards_and_count_them (1/1), OWN-EXTRA-SCAN (gate: targeted counted loop kept, plus an uncounted full 64-shard retain scan in release_all) -> UNCAUGHT, OWN-UNDO-RETURNS-OK (gate: step (3) undoes the row then returns Ok instead of waiting) -> escalation_gaps::inserter_recheck_yields_to_a_late_claim (5/5), OWN-UNDO-KEEPS-COUNTER (gate: remove_row skips the rows decrement) -> escalation_gaps::inserter_recheck_yields_to_a_late_claim only as a hang (timeout 5/5), escalation_does_not_inflate does not see it
Followups: X-1a-F1-F1-F1

## Findings

**Gate run.** `GATE EXIT=0` on 30b58002; every STEP EXIT=0. Run in the
foreground with `CARGO_TARGET_DIR=~/.cache/nv3-target/gate-X-1a-F1-F1`.

**Scope.** `git diff nucleus/v3-scaffold...HEAD --stat` is one new file,
`spikes/e1-mvcc/tests/escalation_gaps.rs` (+387), inside Touch only. No
`src/` change, no test hook, nothing in protected paths. The Do-not clause
held: the worker added no production hook for G3 (it did not ESCALATE
either; see finding 3).

**G4 (sound).** Deterministic. Three escalations in a row on one table, and
`escalation_attempts == 3` pins the counter to the entry count at each
attempt. Catches the card mutant.

**G1 (real coverage, but the detector is unsound under load: blocking).**
The race really does exercise the inserter's half. The card mutant fails
idle at 5/5 (225/512 rounds), and so does OWN-UNDO-RETURNS-OK. But:

1. **False failure on correct code under CPU oversubscription.** On an
   unmutated tree with 24 busy-loop hogs on the 16-thread box, it failed
   15/15, then 7/8. With 8 hogs it failed 0/8, and idle 0/15. A temporary
   probe (a third txn's `NoWait` acquire on a fresh key, reverted) ran on
   every flagged round: **95/95 had no live table owner**. The cause is a
   stale `escalated` flag. `release_all` clears `t.owner` and notifies
   *before* it removes the index entry that carries `escalated`. An escalant
   pre-empted in that gap stays `is_escalated` for several inserter rounds.
   The doc's soundness argument ("the flag drops microseconds after the
   owner clears") assumes no pre-emption. Parallel card runs make this a
   realistic gate flake.
2. **Hangs instead of failing.** The escalant's inner
   `while !lt.is_escalated(ESCALANT)` loop never checks `stop`. Under any
   regression that blocks escalation (G4, OWN-UNDO-KEEPS-COUNTER), the test
   never terminates. This defeats the doc's own "never turn a caught mutant
   into a hang" goal and would wedge `gate.sh`.

**G3 (named mutant caught; accounting not independent).** The *expected*
count is derived independently, but the *observed* value is still the
self-reported `shard_visits`. The sentinel checks cannot see a correct
owner-filtered full scan. OWN-EXTRA-SCAN (counted targeted loop plus an
uncounted full scan) therefore survives. The docs overstate this as
independent accounting.

**Card note (for the orchestrator).** Verdict X-1a-F1's third follow-up was
G3-RELEASE-NO-NOTIFY (the release-wakes tests are insensitive to a missing
notify). This card re-labelled G3 as RELEASE-SCANS-ALL-UNCOUNTED, so the
notify gap is still unpinned.

## Rework (this card)

- R1: make the G1 detector require a live foreign owner, not just the
  flag. For example, record a round only if `is_escalated(ESCALANT)` holds
  *and* a third txn's `NoWait` acquire on a fresh key fails. While the
  inserter's counted row is live, a completed escalation implies the claim
  survived its post-claim re-test. Re-verify 0 false failures with ≥24 hogs.
- R2: make the escalant loop observe `stop` (and bound it), so that a
  never-escalating regression fails rather than hangs.
- R3: state G3's residual plainly in the doc (the exact count catches a
  *replacing* uncounted scan; an *additional* uncounted scan needs a hook).
  If full independence is required, ESCALATE for the hook decision per the
  card's Do-not.

## Follow-ups

**X-1a-F1-F1-F1 (test, Gate: P): pin G3-RELEASE-NO-NOTIFY outside the
S-suite.** Make `release_wakes_the_waiter` /
`release_wakes_waiter_on_released_key` (or a new tests/ case) fail when
`release_all` skips the shard condvars. Use a `Wait::Block` far longer than
the test's own join watchdog, so the late deadline re-test cannot mask the
missing notify. Also cover pass-2 notify and the `table_cv` notify on
release of an escalated owner.
