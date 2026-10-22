Card: X-1a-F1-F2
Class: core
Card-gate: P
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: claude-code review subagent, session 863ca14f-b8d8-45a0-9f89-db9e67afdf54
Date: 2026-10-21
Gate-exit: GATE EXIT=0
Commit: bc8d67fd
Mutants: TABLE-MUTEX-STEP1 -> common::lock_table::tests::acquire_takes_no_table_mutex, TABLE-MUTEX-STEP3 -> common::lock_table::tests::acquire_takes_no_table_mutex, RELEASE-TAKES-TABLE-MUTEX -> common::lock_table::tests::acquire_takes_no_table_mutex, SINGLE-STRIPE -> common::lock_table::tests::rows_are_striped_by_key_shard, STRIPE-SUM-SKIPS-LAST -> common::lock_table::tests::escalation_sees_a_foreign_row_in_every_stripe (and escalation_claim_reclaims_racing_inserters), NO-TABLE-NOTIFY (release_all's clear only) -> common::lock_table::tests::release_wakes_table_owner_waiter_promptly, NO-RECLAIM-AFTER-CLAIM -> common::lock_table::tests::escalation_claim_reclaims_racing_inserters, G1-STEP3-NO-RECHECK -> escalation_gaps::inserter_recheck_yields_to_a_late_claim, G4-ESC-PASS2-KEEPS-COUNTER -> escalation_gaps::escalation_does_not_inflate_the_row_counter (and inserter_recheck_yields_to_a_late_claim), HANDSHAKE-ACQREL -> UNCAUGHT (expected by the card; orderings verified by review), SENTINEL-ACCEPTED (gate: the u64::MAX refusal removed) -> common::lock_table::tests::owner_word_boundary, UNDO-KEEPS-STRIPE (gate: remove_row skips the stripe decrement) -> escalation_gaps::inserter_recheck_yields_to_a_late_claim, UNCLAIM-NO-NOTIFY (gate: try_escalate's un-claim skips notify_all) -> UNCAUGHT, WAIT-NO-RECHECK (gate: wait_table_owner parks once before its first under-mutex re-check) -> UNCAUGHT
Followups: X-1a-F1-F2-F1

## Findings

**Gate run.** `GATE EXIT=0` (every step EXIT=0) on bc8d67fd with
`CARGO_TARGET_DIR=~/.cache/nv3-target/gate-X-1a-F1-F2`.

**Scope.** `git diff nucleus/v3-scaffold...HEAD --stat` touches only
`spikes/e1-mvcc/src/common/lock_table.rs` and `tests/cost_profile.rs`,
both on the Touch-only list. `cost_profile.rs` only gains the module-doc
paragraph and the (c′) block; (a), (b1/b2), (c) and (d) are unchanged.
The only new `pub` items are the `#[doc(hidden)]` `shard_of_key` and
`shard_of_txn`. No `unsafe`, and no non-test `unwrap`, `expect` or `panic`.

**Handshake ordering (re-derived independently).** The inserter does
`stripe.fetch_add(SeqCst)` (step 2, under the shard mutex) and then
`owner.load(SeqCst)` (step 3). The escalant does
`owner.compare_exchange(SeqCst, SeqCst)` (under the table mutex) and then
`stripe.load(SeqCst)` for every stripe. All four operations are SeqCst,
so they lie in the single total order S, which is consistent with
sequenced-before.

- If the inserter's owner load reads "none", it precedes the CAS in S.
  Then fetch_add <S owner-load <S CAS <S each stripe load.
- So the escalant's load of that stripe reads the increment, or a later
  value in that stripe's modification order. Every stripe operation is a
  SeqCst RMW, and the row's own decrement happens only at its later
  release.
- Per-stripe counters are exact under each shard's mutex and never
  negative, and own rows change only on the escalant's thread. So
  Σ ≥ own + 1, and the escalant un-claims.
- Otherwise the inserter's load follows the CAS and sees the claim, which
  stands until an un-claim; it then undoes its row.

The module docs state this correctly, including why Acquire/Release
(the store-buffering pattern) is insufficient. HANDSHAKE-ACQREL is
UNCAUGHT, as expected: the workspace has no weak-memory checker.

**Waking.** Every clear stores "none" under the table mutex, and
`wait_table_owner` re-checks under the same mutex before it parks. That
is correct. The un-claim notifies after it drops the mutex, which is
fine because the store happened under it.

**Fast paths.** `acquire_takes_no_table_mutex` holds the real guard
while a worker does acquire + release_all. It catches all three
mutex-reintroduction mutants, so it is structure-checked, not
self-reported.

**Profiles (gate re-run, interleaved base/tip ×3, load1 < 4 before each
run, i5-13500H 4P+8E/16T, rustc 1.99.0; base = `git archive`
of 7782a3bb).** Units are pairs/s at 1/4/8/16 threads.

- Base (c): 2.49/2.37/1.62/1.35M, 2.41/2.35/1.65/1.36M,
  2.52/2.30/1.65/1.39M. This reproduces the prior gate's
  2.40/2.50/1.68/1.36M.
- Tip (c): 2.51/6.08/9.13/5.84M, 2.54/7.32/9.16/6.34M,
  2.59/6.19/7.62/6.04M.
- Tip (c′): 2.59/7.55/9.85/12.02M, 2.35/7.96/10.64/13.14M,
  2.49/8.14/10.15/13.48M.

These are in family with the RESULT, and (c′) is somewhat higher. Tip (c)
at 16 threads is 4.3–4.7× base.

The fall of (c) from 8 to 16 threads is real shard collision, not a
shared cost. (c)'s keys are fixed, so I computed the collisions
directly: at 16 threads there are 12 distinct key shards (shard 4 holds
3 threads; shards 6 and 55 hold 2 each) and 12 distinct index shards. At
8 threads, 2 threads share key shard 55. (c′) rises monotonically.

Experiment-adjacent checks, both reverted:

- A thread-local harness counter in (c′) changed nothing measurable.
- Removing `release_all`'s global `visits.fetch_add(Relaxed)` also
  changed nothing measurable: three interleaved pairs were within host
  noise of ±30%.

**Decision for X-2/X-3: the per-acquire global cost is gone.** No
acquire or release touches the table mutex or a shared RMW, except for
the Relaxed `visits` counter that Work 4 mandates keeping, and that
counter has no measurable effect. The scaling that remains past 8
threads is consistent with P/E core heterogeneity.

**Minor.** The RESULT frames (c)'s collisions as probabilistic
("P≈0.15"). They are deterministic for (c)'s fixed keys, as computed
above. The conclusion stands.

## Follow-ups

- **X-1a-F1-F2-F1 (test): wakeup liveness for un-claim and for late
  arrivals.** Two gate mutants survived the suite:
  - UNCLAIM-NO-NOTIFY: `try_escalate`'s un-claim skips `notify_all`.
  - WAIT-NO-RECHECK: `wait_table_owner` parks before its first
    under-mutex re-check.

  Either one strands a table-owner waiter until its own `Block`
  deadline. That is a liveness and latency bug, not an L-EXCL break.
  Add white-box tests in the style of
  `release_wakes_table_owner_waiter_promptly`, each with a 5 s watchdog
  and a 60 s deadline:
  1. A waiter parked behind a claim that the post-claim re-test then
     un-claims must return promptly. Reuse the orchestration of
     `escalation_claim_reclaims_racing_inserters`.
  2. A waiter whose observed owner is cleared before it takes the table
     mutex must not park at all.
