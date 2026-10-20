Card: X-1a-F3
Class: core
Card-gate: P
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: claude-code-9c25d937-184c-4858-87e1-513c05528d8b
Date: 2026-10-19
Gate-exit: GATE EXIT=0
Commit: 5e02a1d3
Mutants: WAITING-GLOBAL -> hooks::waiting_is_per_table ("table B must not see table A's parked waiter"), GATE-GLOBAL -> hooks::apply_gate_is_per_engine ("engine B's commit parked on engine A's armed gate"), NO-ENTRY-CHECK -> suite::tests::barrier_rejects_preparked_waiter (30 s, deterministic), O7-REF-RR-CHECK-BEFORE-LOCK -> tests/reference.rs S4b + S4d (5/5 runs) and hooks::concurrent_run_all_in_one_binary S4b + S4d with the second run_all alongside (5/5 runs), O7+NO-BARRIER -> UNCAUGHT 0/10 (expected: the barrier is load-bearing), VISIBLE-JUMPS -> S11a gated C sub-check, G1-WRONG-TABLE (own: RefEngine::lock_table returns a table that is not the engine's) -> S4b, S4c, S4d, S6a, S6b "the waiter never parked", G2-WRONG-SEQ (own: RefEngine::sequencer returns a fresh Sequencer) -> S11a "the armed commit never parked", G3-GUARD-NO-DEC (own: WaitingGuard never decrements) -> hooks::waiting_is_per_table + suite::tests::barrier_rejects_preparked_waiter (the reference suite alone passes: every sub-scenario builds a fresh engine)
Followups: X-1a-F3-F1

## Findings

Gate run on 5e02a1d3: every STEP EXIT=0, `GATE EXIT=0`. Scope: the
three-dot diff touches only the 8 files on the Touch list. `tests/costs.rs`
matches the base. There is no `unsafe`, and no `unwrap`, `expect` or `panic`
outside test code. The two `TxnEngine` methods are required, have no default
body, are `#[doc(hidden)]`, and are documented as test observability.
`LockTable::waiting` and `Sequencer::apply_gate` are also `#[doc(hidden)]`.
S13 and every scenario expectation are unchanged. Only S11a uses the gate,
and only S4b–S4d and S6a/S6b use the barrier. I re-ran the hooks suite 5/5
clean (including `concurrent_run_all_in_one_binary`) and `costs` 6/6.

The isolation is real. The count is a field of each table, and the gate is
owned by each sequencer and read through `seq.apply_gate()` in
`commit_with`. G1 and G2 show the suite actually observes the engine's own
components, not just any table or sequencer. The F1 fast path is kept: one
Acquire load while the gate is unarmed.

1. **Process-global compatibility bridge kept (Do-not violation, forced by
   a card defect).** `apply_gate::LATEST` is a static
   `Mutex<Option<Weak<ApplyGate>>>`, written by every `Sequencer::new`.
   The public free functions `arm`, `wait_parked` and `release` (and
   `CompatArm`) act on whichever sequencer was created most recently. The
   card says "Delete the process-global statics" and "Do not … keep any
   process-global test hook". The cause is that the card's Touch list
   leaves out `tests/costs.rs`, which calls the free API. The worker
   escalated this transparently. No engine path reads `LATEST`, and its
   only user (`costs::apply_gate_fast_path`) owns the only sequencer in its
   binary, so the bridge is deterministic today. However, it is public and
   not `#[doc(hidden)]`. An X-2/X-3 test that calls `apply_gate::arm()` in
   a binary with concurrent tests would arm another test's engine, which is
   the coupling this card removes. It is non-blocking for this merge, but
   it must be gone before X-2/X-3 add tests.
2. **Accepted deviation: two-phase `ParkedBarrier`.** `enter` runs before
   the waiter is spawned, and `wait` runs after it. The card's literal
   design checks on entry after the spawn, which races the scenario's own
   waiter. The worker measured false failures under concurrent `run_all`
   with that design. The loud failure is preserved and NO-ENTRY-CHECK is
   caught.
3. Note: every sub-scenario calls `make()`, so in `run_all` the entry check
   guards only against leftovers on the same engine. G3 shows the drain
   invariant is pinned by `hooks.rs` and the lib test, not by the suite.

## Follow-ups

**X-1a-F3-F1: delete the apply-gate compatibility bridge.** Touch
`tests/costs.rs`, `src/common/commit.rs` and `src/common/sequencer.rs`.
Migrate `costs::apply_gate_fast_path` to `seq.apply_gate().{arm,
wait_parked, release}`. Then delete `LATEST`, `register_latest`, `latest`,
the free `arm`/`wait_parked`/`release`, `CompatArm`, and the
compatibility-bridge docs. Change `Sequencer.gate` from `Arc<ApplyGate>` to
a plain `ApplyGate`, and remove the `Weak` registration from
`Sequencer::new`. Mutant: GATE-GLOBAL is still caught by
`apply_gate_is_per_engine`. A grep for `static` in `commit.rs` must show
no gate state. X-2 and X-3 should list it in `Depends:`.
