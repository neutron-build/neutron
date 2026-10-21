Card: X-1a-F2-F1
Class: test
Card-gate: S
Verdict: MERGE
Gate-tier: S
Gate-model: glm-5.3 (zai-coding-plan)
Gate-session: nv3-X-1a-F2-F1-gate-1 (T3-delegated GLM gate subagent, gate round 1)
Date: 2026-10-21
Gate-exit: GATE EXIT=0
Commit: 17593bbf
Mutants: G-REPLAY-GC-NEVER (replay never calls gc()) -> suite::tests::s13_replay_pins_gc_and_begin_calls ("seed 0: gc() calls with run_gc=true ... left: 0, right: 22"; only failing test, 17/18 lib green), G-REPLAY-BEGIN-NOOP (replay Begin never opens a txn) -> suite::tests::s13_replay_pins_gc_and_begin_calls ("seed 0: begin() calls with run_gc=true ... left: 1, right: 22"; only failing test), G-REPLAY-GC-ALWAYS (own: `if run_gc` -> `if true`, the oracle side runs gc() too) -> suite::tests::s13_replay_pins_gc_and_begin_calls (oracle-side assert "seed 0: gc() calls with run_gc=false ... left: 22, right: 0"; only failing test — nothing else fires, confirming the F2 verdict that ORACLE-RUNS-GC alone is otherwise invisible), G-REPLAY-GC-DOUBLE (own: two gc() calls per Gc op) -> suite::tests::s13_replay_pins_gc_and_begin_calls ("left: 44, right: 22" — the pin is exact-count, not >=1)
Followups: none

## Findings

Gate run on 17593bbf (worktree clean, base 072dea5d): every STEP EXIT=0,
last line `GATE EXIT=0`.

Scope: the diff touches only `nucleus/v3/spikes/e1-mvcc/src/suite.rs`
(209+/10−), the card's single Touch-only entry. Outside `mod tests` the
changes are comments only (module doc, the `S13_TXN_CAP` doc, the budget
comment); the generator rates, `replay_schedule` semantics and the
coverage twin are untouched. All new code lives in the `#[cfg(test)]`
module. No `unsafe`; no `unwrap`/`expect`/`panic` anywhere in the diff.

The core fix is real, not a copy: `s13_replay_pins_gc_and_begin_calls`
imports and drives S13's **own** `replay_schedule` (I checked the import
— `coverage_replay` is not involved), observed through `CountingEngine`,
a test-only `TxnEngine` shim inside `#[cfg(test)]` that forwards the
whole trait surface and counts `begin`/`gc` with SeqCst atomics.
`want_begins` = `Some(iso)` count + `Begin` ops, `want_gcs` = `Gc` ops,
asserted for all 500 seeds on both sides (`run_gc` true and false).
`run_schedule`'s extra final-state `begin()` sits outside
`replay_schedule`, so it cannot pollute the counts. The oracle-side
assertions are stronger than the card's letter and, per my
G-REPLAY-GC-ALWAYS mutant, load-bearing.

Cap comment: I re-derived the arithmetic — budget = 24 − closed ∈
{21,22,23}, initial opens = 4 − closed, total txns ≤ 28 − 2·closed =
20 + 2·open ∈ {22,24,26} — and the new doc states exactly that; 24 is
now described as the knob, not the bound. The numbers are the bound (a
schedule that reaches n_ops with budget left holds fewer); the comment's
"may still emit" phrasing keeps that reading correct, and matches the
card's dictated wording. Code unchanged, as the card allows.

Reproduction: both card-named mutants reproduce the worker's exact
failure messages (`left: 0/1, right: 22` — seed 0: 1 initial open + 21
Begins, i.e. budget 24−3 exhausted, and 22 Gc ops), each with the pin as
the sole failing test. Both mutants reverted; `git diff` clean before
this verdict. The 18 lib tests and all integration tests pass on the
unmutated tree inside the gate.

History: attempts 2–3 failed on a card Acceptance defect (a nonexistent
`--test` suite target), since fixed by the card author to a bare
`cargo test`; the worker is not at fault. The rerun found the work
already committed as 17593bbf and re-proved it; everything material was
re-verified independently this session.
