Card: X-1a-F1-F3
Class: mechanical
Card-gate: S
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: claude-code-04144c62-d196-4f53-958b-434bf67ecb90 (P-tier reviewer, gate round 2: integration merge)
Date: 2026-10-21
Gate-exit: GATE EXIT=0
Commit: 9d8e15f8
Mutants: m1-release-armed-store-moved-back-after-unlock (own) -> UNCAUGHT (cargo test -p e1-mvcc --lib, 17/17 green); m2-arm-armed-store-moved-before-lock (own) -> UNCAUGHT (cargo test -p e1-mvcc --lib, 17/17 green); m3-b1-expected-attempts-off-by-one (own) -> cost_profile (b1) assert (left 8, right 7); m4-b2-expected-attempts-1-to-2 (own) -> cost_profile (b2) assert (left 1, right 2). All four re-applied on the integrated tip 9d8e15f8. Card names no mutants (Class: mechanical).
Followups: X-1a-F1-F3-F1

## Round 2

Round 1 (GLM S gate, Commit 80834b90) gated the work commits on a base
(072dea5d) that predates X-1a-F3-F1. The merge pass that resolved the
resulting `commit.rs` conflict in-pass (f177d05e, scratch/pass75, abandoned)
was refused by the audit ("the parents do not merge cleanly (conflicts must
be fixed on the card branch and re-gated)"). This round covers the
integration merge 9d8e15f8, made on the card branch, and replaces round 1's
verdict as the one under the branch tip.

- **Integration merge.** 9d8e15f8 = merge of `nucleus/v3-scaffold` at
  de9578f7 (X-1a-F3-F1: process-global apply-gate bridge deleted, `arm_now`
  folded back into a plain `arm`) into the card branch (73aa2eb4). One
  textual conflict, the tail of `ApplyGate::arm` in `commit.rs`: resolved to
  the scaffold's plain-`arm` shape (`drop(s);` then the `Arm { gate: self,
  armed: true }` guard is returned directly — no bridge) with this card's
  `armed.store(true, Release)` inside the critical section and its invariant
  comment. `release()` auto-merged with the store before `drop(s)` and its
  comment intact. No conflict markers remain; the merge tree is
  byte-identical to the abandoned in-pass resolution f177d05e's tree.
- **Store positions (textual check the card assigns the gate).** At
  9d8e15f8, `arm`: lock → state fields → `armed.store(true)` → `drop(s)`;
  `release`: lock → state fields → `armed.store(false)` → `drop(s)` →
  `notify_all`. Both stores sit inside the critical sections; `maybe_park`
  and its single Acquire fast-path load are the scaffold's, unchanged.
- **Scope.** `git diff de9578f7 9d8e15f8 --stat` = `commit.rs` (+43/−20),
  `lock_table.rs` (comment-only), `tests/cost_profile.rs`, and this
  verdict — the card's 3 Touch files plus its verdict. No protected path
  besides the verdict; `sequencer.rs`, `costs.rs` arrive from the scaffold
  side unmodified.
- **Gate.** `bash scripts/gate.sh` at 9d8e15f8 (own `CARGO_TARGET_DIR`):
  test/clippy/fmt/deny/wasm32/core-paths-selftest/enforcement-tests all
  `EXIT=0`, literal last line `GATE EXIT=0`.
- **Acceptance.** lib 17/17; `! grep -nF "multiplies the chances"` holds
  (no match); `--release --test cost_profile -- --ignored --nocapture` 1/1:
  (b1) escalations = 0, attempts = 8 (`1 + (2000000 - 1000001) / 125000 =
  8`); (b2) escalations = 1, attempts = 1 — same counts as round 1 and the
  RESULT.
- **Mergeability.** `git merge-tree --write-tree` of the card branch against
  de9578f7 is clean. The scaffold has since advanced to b6560ff3 (X-1a-F2-F1,
  touching only `suite.rs` and its verdict — disjoint from this card); the
  merge-tree against b6560ff3 is clean too.
- **Round-1 findings carried.** UNCAUGHT m1/m2 are expected for this class
  (probabilistic race; every gate test arms before spawning the committing
  thread) and the textual check above stands in for them. Rustdoc:
  `cargo doc -p e1-mvcc --no-deps` at 9d8e15f8 emits 3 warnings, not
  round 1's 9 — X-1a-F3-F1's bridge deletion removed six of the unresolved
  links. Left: unresolved `[ApplyGate]` (`commit.rs:72`), private-item
  `[IDX_SHARDS]` (`lock_table.rs:50`), private-item `[ParkedBarrier]`
  (`suite.rs:34`). None come from this card's edits → follow-up below,
  scope narrowed accordingly.

## Follow-up X-1a-F1-F3-F1

`e1-mvcc` is not actually rustdoc-clean: `cargo doc -p e1-mvcc --no-deps` emits 9 warnings — seven unresolved intra-doc links introduced by X-1a-F3's rewording of the apply-gate docs (`[ApplyGate]` and `[LATEST]` in `commit.rs`, `[arm]`/`[wait_parked]`/`[release]`/`[ApplyGate::arm]` in the `apply_gate` module docs, `[commit::apply_gate]` in `sequencer.rs`) and two public-doc links to private items (`[IDX_SHARDS]` in `lock_table.rs`, `[ParkedBarrier]` in `suite.rs`). The set is byte-identical at this card's base, so no card in flight made it worse, but any future card whose check is "cargo doc stays warning-free" inherits the ambiguity this gate had to resolve. The follow-up fixes the nine links (resolve or de-link, without changing prose meaning), making `cargo doc -p e1-mvcc` exit warning-free so the check becomes literal again. Touch: `commit.rs`, `sequencer.rs`, `lock_table.rs`, `suite.rs` docs only; note it must be based on a scaffold that includes both X-1a-F3-F1 and this card, since the `[ApplyGate]`/bridge wording lands differently on each side of that merge.

Round-2 note: on a scaffold that includes both X-1a-F3-F1 and this card
(as at 9d8e15f8), only three of the nine warnings remain — `[ApplyGate]` in
`commit.rs`, `[IDX_SHARDS]` in `lock_table.rs`, `[ParkedBarrier]` in
`suite.rs` — so the follow-up's Touch narrows to those three files; the
`sequencer.rs` link and the `apply_gate` module-doc links are already gone.
