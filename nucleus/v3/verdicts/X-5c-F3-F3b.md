Card: X-5c-F3-F3b
Class: experiment
Card-gate: P
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: T3 review sub-agent, Claude Code (X-5c-F3-F3b P gate and ESCALATE ruling, 2026-11-04)
Date: 2026-11-04
Gate-exit: GATE EXIT=0
Commit: 86ecf8ffa43da89121b0fdb2d4c867dcd418e9db
Mutants: CKPT-SYNC-OVER-HOLE -> ckpt_step1_waits_for_contiguous_durable (worker), META-RANGE-SKIP -> cover_ts_cross_check_forged_meta (worker), COVER-CHECK-SKIP -> wal_loss_below_ckpt_end_fails_closed (worker), COVER-END-AT-T -> wal_loss_below_ckpt_end_fails_closed segend variant (worker), COVER-AFTER-TRUNCATE -> wal_loss_below_ckpt_end_fails_closed (worker), COVER-AT-EOF -> wal_loss_below_ckpt_end_fails_closed (worker), TS-HI-CHECK-SKIP -> cover_ts_cross_check_unreachable_hi (worker), SEG-EXACT-NO-HOP -> wal_exact_segment_end_hops (worker), POISON-LOST-WAKEUP -> g8_poison_wakes_every_waiter (worker), POISON-WAITER-PARKED -> g8_poison_wakes_every_waiter (worker), WAL-ERR-NOT-POISON -> g8_wal_sync_error_poisons (worker), POISON-AFTER-UNLATCH -> equivalent under S-BATON (worker, README), no_r33 -> mf30/mf31/mf32 green (worker), X5-R33 harness reverts x3 -> tests/harness.rs x33_* (worker), RG1/RG2 -> x5r35_seed403_slice_feeds_the_derivation (worker), (own) G1 X33In wiring reverted to None -> UNCAUGHT by the suite (0..200 campaign: FAILING_ROUNDS 10 -> 20), (own) G2 c929c2a3 boot attribution order reverted -> smoke_16_seeds, cxx_highest_lpid_free_never_shrinks_next_lpid_rounds, g8_poison_ends_the_workload_instead_of_livelocking, r20_mpe_no_opened_image_means_no_slotted_finding, (own) G3 x33_explained upper bound dropped -> UNCAUGHT (suite and 0..200 both green), (own) G4 coverage `valid_end <= wal_ckpt_end` -> d1_every_boundary, d2_marked_chunk, d6_double_crash_idempotent and 7 more, (own) G5 step 0 without the commit_cs wait -> UNCAUGHT (0..200: pass counts identical, 80 seed records differ; likely equivalent, see Finding 5)
Followups: X-5c-F3-F3b-F1, X-5c-F3-F3b-F2

## How the gate was run

- **Fresh checkout.** `git -C ~/Workspace/neutron-v3 -c core.hooksPath=/dev/null worktree add --detach /tmp/gate-X-5c-F3-F3b 86ecf8ffa43da89121b0fdb2d4c867dcd418e9db`.
  - The common git dir (`~/Workspace/neutron/.git`) has no non-sample hooks, no `filter.*`, `core.hooksPath` or `core.attributesFile` config, and no `info/attributes`. `.gitattributes` sets line endings and binary flags only.
  - The index equals the HEAD tree (`git write-tree`).
  - All 287 `nucleus/v3` blobs match their working files (`hash-object --no-filters`), and status was clean.
- **Gate.** `CARGO_TARGET_DIR=$HOME/.cache/nv3-target/gate-X-5c-F3-F3b bash scripts/gate.sh` ran in the foreground. Every STEP was EXIT=0, and the literal last line is `GATE EXIT=0`.
- **Mutants and diagnostics** were applied as source patches in the gate checkout (`~/.cache/nv3-review/gate-F3F3b/mut.py`, logs beside it), and each was reverted with `git checkout`. Status was 0 lines before the checkout was removed.
- **Scope.** `git diff card/v3-X-5c...HEAD --stat` touches only `nucleus/v3/spikes/e4-recovery/` (17 files).
  - `src/model.rs` and `src/sim/` are untouched.
  - `src/oracle.rs` and `tests/oracle.rs` change only in 86eddb34, whose subject names X5-R33.
  - `results/campaign.json` changes only in f8251b84.
  - X5-R34/X5-R35's derivation and slice are untouched.
  - The `tests/directed.rs` and `tests/workload.rs` edits are signature plumbing, plus D4's widened catcher. D4 keeps every original catcher and adds `!pass`, which is X5-R11 (e)'s shape and acceptable.

## Measurements re-run by the gate

- **1749..1750 single seed** at 86ecf8ff: the seed record is byte-identical to the worker's `X-5c-F3-F3b.esc1.seed1749.json`. r2 fails `O-1(c) at c=40 … raw entry above the cut (max ts 41)` + `O-3(d)`, with `cover_err=false`.
- **0..200 at 86ecf8ff:** `FAILING_ROUNDS=10`, `TWIN_FAILING_ROUNDS=1`. It matches the worker's cited smoke (`s5.0200b`).
  - The seven live named rounds (19/39/59/69/109/149/159 r0) are `cover_err ∧ x33x ∧ pass`.
  - 199 r2 is `O-1(b) at c=26, tree 4` + `O-3(d)`.
  - There are 10 `cover_err` rounds, all MW, all explained and passing. The MW-ORPHAN shape count is 0.
- **The Acceptance prov** (`X-5c-F3-F3b.acc.prov`): `commit=86ecf8ff…`, `status_lines=0`, `exit=0` and `verify_exit=0`. The file's sha256 `8332df8d7b6eda07…` equals `json_sha256`.
  - `E4 SEEDS=10000 O1_FAIL=2625 O2_FAIL=35 O3_FAIL=83 DEADLOCK=0 PANIC=0 O1F_FAIL=2625 MONITOR=0`
  - `E4 FOLD FAILING_SEEDS=375 FAILING_ROUNDS=352 TWIN_FAILING_ROUNDS=36 DEFECT_SEEDS=0`. F3-F3a's file had 766 / 745.
- **Pre-registration order.** X5-R33's commit 86eddb34 was made at 21:58:27. The first smoke's file is stamped 21:58:57, and every later output from 22:03 on.
- **Per-candidate O-1 on the five ESCALATE rounds.** This used a gate-only diagnostic patch that printed every candidate's O-1 error, plus `E4_O1_DIAG` at a chosen candidate. It was reverted. See the ESCALATE ruling.

## ESCALATE ruling (runbook §4 items 1, 2, 4)

**(i) Classification: neither an S-1-F3 design gap nor a coverage deviation. The five are C-DIV's "missing ≤ T entry" family (X-5c-F3-F3a-F2) surfacing in class MW. The card's syntactic shape test misclassified them.**

The class-shape test reads `o1`, and `o1` is the error of the **first** failing candidate (`check_round` keeps the first `Err`, at `c = lo`). Whenever `lo` < frontier, that error is O-1(c) "raw entry above the cut". The failure that actually rules out every candidate sits at the frontier candidate. That candidate is inside `[lo, hi]` in all five rounds, and it fails for an image-content reason:

| Round | lo..hi (X5-R33) | T | frontier | the failure at the frontier candidate |
|---|---|---|---|---|
| 1749 r2 | 40..41 | 38 | 41 | O-1(b) tree 4: key …25 absent; owner ts 4, `le_T` |
| 5549 r2 | 30..33 | 29 | 32 | O-1(b) tree 4 (at c=33 too: key …15 absent; owner ts 3, `le_T`) |
| 7359 r2 | 11..15 | 9 | 12 | O-1(b) tree 3: key …1,107 absent; owner ts 3, `le_T` |
| 4139 r2 | 14..17 | 14 | 17 | O-1(d) at S=4 (a historical scan below the cut) |
| 5839 r2 | 43..50 | 43 | 50 | O-1(d) at S=8 |

- **What these failures show.** A committed transaction with ts ≤ T must be in image k as ordinary entries (S-STORE §13 (c)). Losing one is an image or CoW defect, independent of the WAL.
- **The WAL side behaves correctly.** In 1749, 4139 and 5839, replay reached `hi` itself. In 5549, ts 33's record ends above `D` and was never durable. In 7359, the walk stopped above `wal_ckpt_end` on MW damage. That is the owner-accepted "lost tail above the last checkpoint" (PLAN §10 item 9), and X5-R33's `a` already bounds it. I-WAL-COVER correctly did not refuse in any of the five.
- **Consequences.**
  - The 406 → 5 drop is the class closing as S-1-F3 predicted. The remaining five belong to the open C-DIV diagnosis.
  - **No S-1-F* spec follow-up is needed. No PLAN §10 owner item is touched**: not item 8 (Q1 = A holds) and not item 9 (W-BP is not implicated, and X5-R33 stands).
  - Not REWORK here: the card's Limits forbid the fix, which is F2's.
- **The worker was right to ESCALATE.** The card told it to treat any remaining shape as P's call.

**(ii) The chain.** Not blocked. The five are classed as C-DIV-in-MW and carried by X-5c-F3-F3b-F1, which the orchestrator may fold into X-5c-F3-F3a-F2. X-5c-F4 already depends on F2. F4's final E4 judgement must not count these five as an MW-ORPHAN (redesign) failure. They count against C-DIV's outcome.

**(iii) Q18 981/1000.** This goes on F4's retune list. It is not a store regression. Against F3-F3a's file, the decomposition is:
- **64 rounds** that counted Q18 before are now I-WAL-COVER refusals. They are refused before pass 2, so no GC runs inside a recovering image. That is structural under owner Q1 = A.
- **7 rounds** are no longer reached, because their seeds now end at a refusal.
- **The rest is schedule churn.** 140 common rounds lost Q18 and 44 gained it. 4,323 common rounds changed crash point or frontier, because steps 0, 1 and 3 add park and check points.
- **Without the structural 71,** Q18 would sit at about 1,052.

The retune must raise GC-in-recovery coverage on rounds that recover. It must never relax a refusal.

**(iv) The REFRESH and step 0′.** The REFRESH block is verbatim against verdict 4da828ef's "X-5c-F3-F3b Goal refresh" and its F3 follow-up. The only other differences from the pre-refresh copy are the Status line and the folded step 0′, and both are correct.
- Step 0′ landed first, as its own test-only commit (9580b10d).
- The worker demonstrated RG1 and RG2 failing `x5r35_seed403_slice_feeds_the_derivation`. That closes X-5c-F3-F3a-F3: the RG1/RG2 UNCAUGHT entries in 4da828ef's table now have a pin.

## Findings

1. **(medium, follow-up F2) c929c2a3's harness fix has no pin.** The fix threads `X33In` into the real rounds and fixes the boot-0 attribution order. G1, which reverts the threading, survives the whole release suite. Only a campaign catches it (0..200: 10 → 20 failing rounds). G2, the order revert, is caught by the seed tests. This breaks Limit 3 ("each fix carries a literal-config pin").
   - **Ruling on the process.** The fix landed after a judged run (the first smoke) had been read. It is accepted: it conforms the supply to X5-R33 (b)/(d)'s carried text (a supply that was never called, and attribution by writing life), it leaves the oracle untouched, and it does not widen "explained" (G3's site is unchanged by c929c2a3).
   - **The record.** The first smoke is recorded honestly and is never cited.
2. **(medium, follow-up F2) `x33_explained`'s upper bound is unpinned.** G3 (`[wal_start_lsn, ∞)`) is green in the suite and identical in pass/fail over 0..200. "Never widen explained" has only the oracle-level mf32, which takes the boolean as an input.
3. **(low, follow-up F2) The re-pointed 6149 arm is vacuous.** r0 is now an I-WAL-COVER refusal, so the serve path cannot run there.
4. **(nit) A second, unreachable copy of the coverage check sits after `truncate_wal`** (`src/store/ckpt.rs:1776`). The first copy (`:1573`) has already returned on the same predicate. It is harmless, but a later edit could make it COVER-AFTER-TRUNCATE. Delete it in any later store card.
5. **(note) G5, step 0 without the `commit_cs` wait, is likely equivalent in the prototype.** `wal_enqueue` sets `append` and `last_commit_ts` in one atomic step, so the X5-R33 (d) bounds hold without the wait. 0..200 shows 80 seeds re-scheduled and no pass change. S-1-F3 W5 (i)'s wait matters once placement and ts assignment split, which is A-6a's codec. A-6a's P tests should carry it.
6. **(note) `r33_cover` adds `!cfg.no_twin_rule`,** which the carried text does not have. It is reachable only under that instrumentation knob, and it changes no campaign judgement.

## Follow-ups

- **X-5c-F3-F3b-F1 (P, experiment; may fold into X-5c-F3-F3a-F2).** Confirm the five MW rounds as C-DIV-in-MW with a frontier-candidate diagnostic (`E4_O1_AT_FRONTIER`), trace each divergent key to the step where it leaves the image, and re-check the five after F2's outcome.
- **X-5c-F3-F3b-F2 (S, test).** Add three things:
  - a seed-level pin that fails on G1 and G2, the X5-R33 wiring and attribution;
  - a unit pin on `x33_explained`'s bounds that fails on G3;
  - a non-vacuous re-point of the 6149 arm (served under SEG-EXACT-NO-HOP).

## X-5c-F4 changes (for P's refresh at merge)

- **Depends:** add X-5c-F3-F3b (this MERGE-WITH-FOLLOWUPS, then card/v3-X-5c fast-forwarded), and X-5c-F3-F3b-F1 (or its fold into X-5c-F3-F3a-F2, which F4 already depends on).
- **Step 0:** re-point it at `X-5c-F3-F3b.acc.*` (commit 86ecf8ff…, sha256 `8332df8d7b6eda073c8bd6f0b93a15b491deb3aad5f70d576d35d4ca46e35202`) with the counts above.
- **The class table:** classify MW failing rounds by the frontier candidate's failure (F1's diagnostic), not by `o1`'s first-candidate text. The five rounds above are C-DIV-in-MW, and `MW_ORPHAN_SHAPE` (true) is 0.
- **The retune list:** SIZING (14,985), Q4 (0/1000), Q7 (169/200), Q8 (80/100), Q11 (0/100), Q12 (1/100), and **Q18 (981/1000, new)**, with the decomposition above. Refused MW rounds cannot count toward Q18.
