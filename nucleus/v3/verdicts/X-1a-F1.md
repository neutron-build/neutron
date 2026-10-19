Card: X-1a-F1
Class: core
Card-gate: P
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: claude-code-618161c7-0e16-4ee3-accb-0107bda35487
Date: 2026-10-19
Gate-exit: GATE EXIT=0
Commit: 098a4646
Mutants: RELEASE-SCANS-ALL (old 64-shard retain loop, counted in shard_visits) -> costs::release_touches_only_own_shards, RELEASE-SKIPS-LAST -> costs::release_touches_only_own_shards + costs::release_wakes_waiter_on_released_key (+ 4 lib tests, S1/S2), ESCALATE-EVERY-ACQUIRE -> costs::escalation_defers_under_foreign_holder (attempt bound), ESCALATE-IGNORES-FOREIGN -> costs::escalation_defers_under_foreign_holder + lock_table::escalation_waits_for_foreign_rows + escalation_claim_reclaims_racing_inserters, NO-RECLAIM-AFTER-CLAIM -> lock_table::escalation_claim_reclaims_racing_inserters (6/6 runs), ARM-NO-FLAG -> costs::apply_gate_fast_path + commit::gate_parks_one_commit_after_its_ts + S11a, RELEASE-BEFORE-VISIBLE -> S5 (S6a not observed failing this run), RR-NO-CONFLICT -> S4 + S4b + S4d, VISIBLE-JUMPS -> S11a + sequencer unit + model proptest, RELEASE-SCANS-ALL-UNCOUNTED (gate: old loop without the shard_visits bump) -> UNCAUGHT, G1-STEP3-NO-RECHECK (gate: acquire step (3) owner re-check disabled) -> UNCAUGHT, G2-RELEASE-NO-DEC (gate: release_all never decrements the row counter) -> costs::escalation_defers_under_foreign_holder + lock_table::escalation_waits_for_foreign_rows, G3-RELEASE-NO-NOTIFY (gate: release_all skips the shard condvars) -> S-suite only (reference test, 210 s; both release-wakes unit tests pass after their Block deadline), G4-ESC-PASS2-KEEPS-COUNTER (gate: escalation pass 2 does not decrement the row counter) -> UNCAUGHT
Followups: X-1a-F1-F1, X-1a-F1-F2, X-1a-F1-F3

## Findings

**Gate run.** `GATE EXIT=0` on 098a4646; every STEP EXIT=0 (test, clippy,
fmt, deny, wasm32, core-paths-selftest, enforcement-tests). Run in the
foreground with `CARGO_TARGET_DIR=~/.cache/nv3-target/gate-X-1a-F1`.

**Scope.** `git diff nucleus/v3-scaffold...HEAD --stat` touches exactly the
four Touch-only files (lock_table.rs, commit.rs, tests/costs.rs,
tests/cost_profile.rs). Nothing in protected paths. Public signatures of
`LockTable` and `commit_with` unchanged; additions are
`escalation_retry_step()` (pub, documented, card-required) and the
`#[doc(hidden)]` `escalation_attempts()` / `shard_visits()`. The apply-gate
API (`arm`, `wait_parked`, `release`) is unchanged.

**Correctness.**
- Work 1: `release_all` removes the index entry, groups keys by shard,
  removes only keys still owned by `txn`, notifies only those shards, and
  clears/notifies the table owner first. An escalated txn's empty key set
  touches no shard. Notify after unlock is safe (waiters test under the
  shard mutex before waiting).
- Work 2: 64 index shards keyed by `TxnId`; the only nesting is index shard
  → key shard (acquire step 2); `release_all`, `remove_row` and
  `try_escalate` take them sequentially; the table mutex is always alone.
  No condvar wait under an index shard. Lock order matches the module docs.
- Work 4 / the escalation race: I re-derived the closure. The inserter's
  counter bump is sequenced before its step-(3) table lock; the escalant's
  claim is a table critical section followed by an Acquire load. Whichever
  critical section comes first in the mutex order, the other side observes
  it (step 3 sees the owner → undo; or the post-claim load sees the bump →
  un-claim). `total > own` cannot under-report: every decrement pairs with
  an earlier increment of the same row under the same shard mutex, and the
  escalant's own rows are only mutated by its own thread. Index-before-shard
  removal in `release_all`/`remove_row` only makes the test conservative.
  Sound.
- **L-ESCALATE-DEFER docs vs code: they match.** Never waits; first attempt
  at `count > escalate_at` (`retry_at` = 0); after a failure
  `retry_at = count + max(escalate_at/8, 1)`; O(1) test before and after the
  claim; pass 2 uses the txn's own key set; "S-2 decides the production rule"
  is stated. One nuance worth knowing: the retry step counts *new keys*
  (`keys.len()`), not acquire calls, because L-NOOP re-acquires return before
  step (4). That is consistent with "acquires by the same txn" in the
  card's sense and with the `costs` test.
- Work 3: the `ARMED` Release/Acquire pairing is correct for arm → commit.
- No `unsafe`, no non-test `unwrap`/`expect`/`panic` in the diff; pub items
  documented.

**Mutation.** Every card mutant is caught, and so is every X-1a carry-over.
NO-RECLAIM-AFTER-CLAIM was caught 6/6. Its catcher is white-box: the test
holds the table mutex, calls `try_escalate` directly and simulates the
inserter. That departs from the card's "inserters race an escalant", but the
worker documented why (the organic race was 0/8) and the test cannot
false-fail. Of my own mutants, G2 is caught and G3 is caught only by the
S-suite. Three survive:
- **G1-STEP3-NO-RECHECK.** The inserter half of the L-ESCALATE handshake has
  no test. Disabling it is a real L-EXCL break: an inserter that passes step
  (1) before the claim and inserts after the post-claim re-test keeps its
  row under a foreign table owner. The code is correct. This gap was already
  there before this card, which kept the step unchanged as required.
- **G4-ESC-PASS2-KEEPS-COUNTER.** After any escalation the row counter stays
  inflated permanently, so no txn on that table can ever escalate again.
  No test escalates twice on one table or checks `rows` against `entries()`
  when the table is quiescent.
- **RELEASE-SCANS-ALL-UNCOUNTED.** `shard_visits` is self-reported, so a
  regression to a full scan that does not bump it passes. This is inherent
  to counter-based tests. The worker disclosed it.

**Re-measurement (cost_profile, tip, i5-13500H, 16 threads, rustc 1.99.0
(b940084d7 2026-09-28)).** (a) 289 ns (worker: 445 ns). (c) 2.40M / 2.50M /
1.68M / 1.36M pairs/s at 1/4/8/16 threads (worker: 1.57M / 1.10M / 0.69M /
0.69M). (d) 13.8M commits/s. (a), (c) and (d) confirm the direction and size
of the RESULT's before/after claims. **(b) did not reproduce:** I got
`escalations = 1`, 1.03 s and 1.94M acq/s, against the worker's
`escalations = 0`, 4.36 s. Whether the big txn escalates depends on whether
the cycling foreign row happens to be free at an attempt instant. So (b) is
scheduling-dependent, and the RESULT's base-vs-tip (b) narrative is one
sample of a race, not a property. The deterministic claim it stands in for
is pinned by `costs::escalation_defers_under_foreign_holder`, so this does
not block the merge.

**Remaining shared cost (non-blocking for this card's scope, but relevant to
its Goal).** Every acquire still takes the global `table` mutex twice (steps
1 and 3), and every insert/remove does an RMW on the single `rows`
AtomicU64. Profile (c) shows aggregate disjoint-key throughput falling past
4 threads in both runs. Both E1′ arms pay this cost identically, which is
the same bias toward a ratio of 1 that F1 set out to remove.

**Minor.**
- The doc comment on `escalation_claim_reclaims_racing_inserters`
  (lock_table.rs:724-770) splices two drafts. The first half describes a
  probabilistic eight-inserter design that does not exist, and line 761 is
  `//` in the middle of the `///` block.
- `apply_gate::release()` stores `ARMED = false` after dropping the mutex. A
  release racing a fresh `arm()` from another thread could clear the new
  arm's flag. Tests are sequential today, so this does not bite yet.
- The unit test `release_wakes_waiter_on_released_key` (and the older
  `release_wakes_the_waiter`) cannot fail on a missing notify, because
  `wait_shard` re-tests the key at its deadline and succeeds late.

## Follow-ups

**X-1a-F1-F1 (test, Gate: P): pin the remaining escalation and counter
invariants.**
- Add a deterministic white-box test for the inserter half of the L-ESCALATE
  handshake (acquire step 3): an escalant claims between an inserter's step
  (1) and step (2), and the inserter must undo its row and wait. It must catch
  G1-STEP3-NO-RECHECK.
- Add a test that escalates, releases and escalates again on the same table,
  plus an `entries() == rows + owner` consistency check at quiescence. It
  must catch G4-ESC-PASS2-KEEPS-COUNTER.
- Make the release-wakes tests sensitive to a missing notify without a timing
  threshold, for example a Block far longer than the test's own join
  watchdog, or a hidden notify counter. It must catch G3-RELEASE-NO-NOTIFY
  outside the S-suite.

**X-1a-F1-F2 (core, Gate: P; before X-2/X-3 measure above 4 threads, or
those cards must report it): remove the remaining global per-acquire costs.**
The table-owner mutex is taken twice per acquire and the single `rows`
counter is updated on every row insert and removal. cost_profile (c) shows
throughput falling from 4 to 16 threads (2.50M → 1.36M pairs/s at the gate,
1.10M → 0.69M for the worker). Options include an atomic owner word with a
SeqCst bump/check handshake that replaces the two mutex trips, and striped
row counters summed by the escalant. The L-ESCALATE closure must be
re-derived and re-tested with NO-RECLAIM-AFTER-CLAIM and G1.

**X-1a-F1-F3 (mechanical, Gate: S): hygiene.**
- Repair the spliced doc comment at lock_table.rs:724-770.
- Move `ARMED.store(false)` in `apply_gate::release()` inside the state
  critical section, or re-check `armed` under the lock.
- Make cost_profile (b) deterministic, for example a fixed foreign duty cycle
  that holds the row across every attempt, plus a variant with no foreign
  holder. Report `escalation_attempts` alongside it, so the before/after
  comparison is reproducible rather than one sample of a race.
