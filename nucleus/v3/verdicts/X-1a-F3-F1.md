Card: X-1a-F3-F1
Class: core
Card-gate: P
Verdict: MERGE
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: claude-code-d15ff13b-b33a-4f09-a6a6-87568f4c4ce8
Date: 2026-10-20
Gate-exit: GATE EXIT=0
Commit: a39369cd
Mutants: GATE-GLOBAL -> hooks::apply_gate_is_per_engine (solo: "engine B's commit parked on engine A's armed gate"; full parallel binary: hooks::concurrent_run_all_in_one_binary fails, binary red), BRIDGE-BACK -> Acceptance greps `! grep -nw static` (commit.rs) and `! grep -rnE 'apply_gate::(arm|wait_parked|release)\b'` (costs.rs), COSTS-WRONG-SEQ -> costs::apply_gate_fast_path ("the armed commit, spawned after arm(), never parked"), ARM-REORDER -> UNCAUGHT (expected; full e1-mvcc suite green 2/2, checked by diff instead), G1-ARM-NO-FLAG (own: folded arm() drops the `armed` Release store) -> commit::tests::gate_parks_one_commit_after_its_ts + costs::apply_gate_fast_path ("never parked"), G2-GUARD-DROPPED (own: costs.rs binds `let _ = seq.apply_gate().arm();`, guard dropped at once) -> costs::apply_gate_fast_path ("never parked")
Followups: none

## Findings

Gate run `PATH=$HOME/.cargo/bin:$PATH CARGO_TARGET_DIR=$HOME/.cache/nv3-target/gate-X-1a-F3-F1 bash scripts/gate.sh`
on the tip (a39369cd): every STEP EXIT=0, `GATE EXIT=0`. It was run twice: once
before the mutants and once on the clean tree after them.

Scope: `git diff nucleus/v3-scaffold...HEAD --stat` shows exactly the three
Touch-only files (commit.rs, sequencer.rs, tests/costs.rs), 20+/120-. No
protected paths were touched. The branch base is the scaffold tip (12c225cb).

1. **Deletion is complete.** `LATEST`, `register_latest`, `latest`, the free
   `arm`/`wait_parked`/`release`, `CompatArm` with its `Drop`, `arm_now`, the
   `Arc`/`Weak` imports and the `# Compatibility bridge` doc section are gone.
   All three Acceptance greps and the Work 4 grep find nothing. A
   workspace-wide `git grep` (CompatArm, register_latest, arm_now,
   compatibility bridge, free-API call shapes, LATEST) finds hits only in the
   historical verdicts X-1a-F1.md and X-1a-F3.md, which are immutable records,
   plus unrelated TypeScript `PROTOCOL_LATEST`. No spec in `docs/`, no
   README and no remaining comment mentions the bridge, "pre-F3" or `Weak`.
   `suite.rs:19` still links `commit::apply_gate`, the module, which still
   exists. `ApplyGate::new` is `pub(crate)`, so tests cannot build a detached
   gate. The only way to reach a gate is `Sequencer::apply_gate`
   (`#[doc(hidden)]`, `&self.gate`).
2. **The fold-back keeps the order byte for byte.** The body of the new
   `arm()`, from `let mut s = self.lock();` through
   `self.armed.store(true, Ordering::Release);`, is identical to the body of
   the old `arm_now` (checked by `diff`). The order is: lock, set
   armed/taken/released/parked, `drop(s)`, then the Release comment and the
   store, then build the guard. `commit_with`, `maybe_park`, `wait_parked`,
   `release`, `State` and the `#[cfg(test)]` module are untouched. The
   ARM-REORDER read check passes by diff.
3. **The `_gate` deviation is correct.** `let _gate = ...` is a named binding,
   not `_`, so the guard lives until the end of the block, after the explicit
   `release()`. It is also the name the pre-card test already used. Mutant G2
   shows that the lifetime matters and that the test catches it. The two
   phases, the timeouts (2 s and 10 s) and the assertion messages are
   unchanged. Only the receiver changed.
4. **sequencer.rs:** the field is plain `gate: ApplyGate`, built in place with
   no registration. The `Arc` import is dropped (no other use remains), and
   the field doc and the S-GATE-SCOPE bullet are rewritten as the card asks.
   There is no `unsafe` and no new non-test `unwrap`/`expect`/`panic`.

Observation (non-blocking, out of this card's Touch list): under GATE-GLOBAL,
`hooks::apply_gate_is_per_engine` fails when run alone. In the full parallel
`hooks` run it can pass, because the concurrent `run_all`s' releases of the
shared gate let engine B's commit through. The binary is still red through
`concurrent_run_all_in_one_binary`, so the mutant is caught. The test's doc
claim holds only when the test runs alone. Worth knowing for any future
hooks.rs card; no follow-up is filed.
