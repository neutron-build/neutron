Card: X-5c-F2
Class: experiment
Card-gate: P
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: T3 review sub-agent, Claude Code session fce2447a-89ed-4d19-92a3-a78037f8ef6b
Date: 2026-10-31
Gate-exit: GATE EXIT=0
Commit: 1f8e7c3f9e56d6e4ac25997e5bd9825c0d412320
Mutants: M-R28a K skip without r_active -> mf23_unreported_k_without_meta_slot_fault_fails, M-R28b exactness K tightening off -> mf21_reported_k_slot_fails + instrumentation_removals_are_load_bearing, M-R28c K guard off -> mf22_malformed_k_fails, M-R28d completeness skips every tainted slot -> mf20_tainted_outside_k_not_reported_fails (+mf16, mf23), M-R28e V_k uses the latest DWrite -> r28_v_k_is_the_history_at_the_opened_meta, M-R28f fallout loop restored -> r28_no_ground_truth_meta_finding, M-R28g harness K always empty -> r28_residual_seeds_clear_via_k, M-R29a/M-R29b n/a (branch (c): no amcheck change), M13 FOLD twin term dropped -> campaign::tests::f1f2c_fold_counts_a_twin_only_failure, M8 R anchor at the meta's issue -> f1f2d_r_anchor_is_the_sync_not_the_issue, reachable_hook_spec union-0 return off -> f1f2a_empty_masks_are_a_defect_not_a_hang, step 1(b) None arm keeps a chain -> f1f2b_repairs_without_a_store_reseed_the_twin_chain, step 1(e) defect never raised -> f1f2e_page_file_sync_data_yields_the_anchor0_defect, (own) G1 K condition 2 (durable==V_k) dropped -> r28_residual_seeds_clear_via_k, (own) step 1(b) caller keeps the old chain on None -> UNCAUGHT, (own) G2 K condition 3 (post-anchor write) dropped -> UNCAUGHT, (own) G3 X5-R6 explained counts K slots again -> UNCAUGHT, (own) G4 (ii) exactness K tightening off -> UNCAUGHT, (own) G5 (ii) K guard off -> UNCAUGHT, (own) G6 (ii) K skip without r_active -> UNCAUGHT, (own) G7 K condition 4 (M-NOINPLACE) dropped -> UNCAUGHT, (own) G8 served_k ignores K -> UNCAUGHT, (own) G9 K_ii against the wrong meta -> UNCAUGHT
Followups: X-5c-F2-F1, X-5c-F2-F2

## How the gate was run

- **Fresh checkout.** `git -C ~/Workspace/neutron-v3 -c core.hooksPath=/dev/null worktree add --detach /tmp/gate-X-5c-F2 1f8e7c3f…`.
- **Checks before trusting it:**
  - the shared hooks dir holds only `*.sample`;
  - no `filter.*`, `core.hooksPath` or `core.attributesFile` config;
  - no `info/attributes`;
  - the only `.gitattributes` is the repo-wide line-ending file (327fd3e8), unchanged by this card;
  - `git diff-index --cached HEAD` is clean, and `status --porcelain` is empty;
  - every `nucleus/v3` blob matches `git hash-object --no-filters` byte for byte.
- **The gate.** `PATH=$HOME/.cargo/bin:$PATH CARGO_TARGET_DIR=$HOME/.cache/nv3-target/gate-X-5c-F2 bash scripts/gate.sh` ran in the foreground. Every STEP gave EXIT=0 (test, clippy, fmt, deny, wasm32, core-paths-selftest, enforcement-tests), ending `GATE EXIT=0`. The log is `~/.cache/nv3-review/runlogs/X-5c-F2-gate.gatesh.log`. The checkout was removed afterwards.
- **Mutants.** They ran in a second hooks-off detached checkout of the same commit, `/tmp/gmut-X-5c-F2`. Each mutant was an exact-string patch (driver `~/.cache/nv3-review/x5cf2-gate/mut.py`, outside every worktree). The whole release crate suite ran on each, and each mutant was reverted with `git checkout`. The checkout was clean at the end and is removed. The results are in `mut.out`.
- **Targeted seeds.** These ran under `ulimit -v 25165824`, one seed each. No campaign and no verify were run.

## Scope and frozen files

- `git diff card/v3-X-5c...HEAD --stat` gives 9 files, all under `spikes/e4-recovery/` (README, `results/digest-stability.tsv`, `src/{campaign,harness,oracle,workload}.rs`, `tests/{harness,oracle,workload}.rs`).
- `src/model.rs`, `src/sim/`, `src/store/` (amcheck included: branch (c)) and `tests/directed.rs` are untouched. `results/campaign.json` was not committed.
- `src/oracle.rs` and `tests/oracle.rs` change in exactly one commit, 5bcb1701, whose subject names X5-R28.

## Findings

1. **Step 0 (X-5c-F1-F1): exact.** Re-derived with `python3 -I` against `X-5c-F1-gate.step4-truth.tsv`:
   - exactly 54 rows changed, and they are exactly the rows where truth ≠ the old label;
   - all 852 truth rows now match their step-4 label, with MPE+RED in commit order;
   - no earlier-step cause moved, and the file still holds 3,507 rows;
   - the counts are MPE 795 / MPE+RED 40 / RED 17 (13+4), so 835 MP-E and 57 redesign.

   The two banned phrases survive only as a quotation of what was deleted.
2. **Step 1 (X-5c-F1-F2): sound, and each pin caught its revert.**
   - (a) `union == 0` / an empty mask list now returns `Err`, and both callers turn it into a seed defect. No hang.
   - (b) `twin_chain_after_repair` re-seeds on `end_store == None`. Its caller wiring is unpinned (own mutant UNCAUGHT; the card allowed a function pin). Follow-up F1.
   - (c) M13 is pinned, and so is (d) M8, both at the `SyncReturned` return.
   - (e) The anchor-0 tripwire is raised only when anchor 0 is in force, and it is lifted to `rec.defect`.
3. **X5-R28 (b): transcription against the ruling, line by line.** Every clause is present: the fields with doc comments; the guard (not-referenced, not-tainted, in R, or ≤2) in (i) and (ii), independent of `r_active`; the completeness halves skipping meta slots and, under `r_active`, K; the exactness tightening with the ruling's literal text (the (ii) text prefixed "(ii)", as the file's other (ii) texts are); `explained` = "some tainted referenced slot outside K, or an active non-empty R"; and mf20–mf23, mp7 and the K arm. The text departs from the ruling in two places. Neither changes behaviour:
   - (i) the completeness skip is `*slot <= 2`, not "slots 1 and 2". Slot 0 can never be in `image_slots`: `parse_map_leaf` maps slot 0 to `None`, the walk returns at a root slot of 0, and the header left the set under X-5c-F1. So this is equivalent, and not a loosening;
   - (ii) `explained` drops K slots without checking `r_active_r6`. That is the ruling's own "the rule becomes" sentence. It differs from the conditional form only when K≠∅ while `r_active_r6` is false, which the harness gate (K only when `meta_rollback` and there is a meta-slot fault) never produces, and in that case it is stricter.

   The README's verbatim copy of R28–R31 matches the card word for word, except that one `**…**` around "`tests/oracle.rs`" was dropped.
4. **X5-R28 (a)/(c): the harness.** `kept_set_k` implements conditions 1–4 as written:
   - the r5 gate;
   - the shared `r5_anchor`, both halves;
   - `meta_dwrite_cutoff` takes the last meta DWrite with the opened `ckpt_id` and fails closed;
   - `v_at_meta` composes the DWrites strictly before the cutoff;
   - it never reads store memory or §3.3.

   K_ii is computed against the end image's meta. The fallout loop is deleted (M-R28f caught).

   **The rest of the K logic is unpinned.** No test catches dropping condition 3 or condition 4, computing K_ii against the wrong meta, any of the (ii) arms of the oracle amendment, or the `explained` tightening (own mutants G2–G7 and G9 UNCAUGHT). The ruling listed none of these as a test. The code is correct by review, so this does not block the merge. Follow-ups F1 (harness) and F2 (the oracle tests, which are frozen: P's text).
5. **X5-R28 (e): the decision rule, applied verbatim.** All six named slots are in K, and each round's O-3 is clear (worker records `runlogs/X-5c-F2.r28e.*`).
   - The gate re-ran 1787 (K slot 10, crc 8186a812) and 9647 (K slot 130, crc f4e1951f) at the tip: O-3 null, pass, `mrb=true`, and digests equal to the Acceptance file's.
   - Nothing needed escalating, and K was not widened.
6. **X5-R30 (a): `served_k`.** It is in the record (`sk`) and the digest. The gate instrumented the Err arm (stderr, reverted) and found every MP-A served range non-empty, so no `sk` is vacuous:
   - 1897 and 2437 served slot 4, with K={4}; 4718 served slot 8, with K={8}. All three have `sk=true`;
   - 2927, 4378 and 7528 served **meta slot 1**, and 8728 served **meta slot 2**. All four have `sk=false`.

   These four are a second shape for P at X5-R30 (c). Note also that `sk` is vacuously true on all 15,372 non-served rounds, so read it only where `served=true`. Both points are recorded in F3.
7. **X5-R29: branch (c), correctly taken.** In 4858 the failed node (lpid 2) has no children, yet {6, 7} are unreachable. The gate read the logs: the faultless class-C twin's end image reports the same two orphans, with no §3.3 failure, so these are real I-LPID-EXACT orphans. 9717 is a third mechanism. 688, 3718, 5238 and 5638 are exact fallout, but the ruling's fork is strict. amcheck is unchanged, and the rounds are handed to F3.
8. **The README.** The Status line, stage record, pin list and Acceptance-hosted "How to reproduce" are present. RESULT item 5 is deferred to F3, as the card says.
9. **Acceptance provenance.**
   - `commit=1f8e7c3f…` is the branch tip; `status_lines=0`, `exit=0`, `verify_exit=0`, and `verify_head` is the tip;
   - `json_sha256=56e47fb2…cdf34033` equals `sha256sum` of `X-5c-F2.acc.campaign.json`;
   - `VERIFY OK (--no-quotas) seeds=10000 digests reproduce`, and the 5 missed quotas are the known Q4, Q7, Q11, Q12 and SIZING.
10. **Every outcome change from the esc-2 file is explained** (gate diff of the two files: 29 changes).
    - 6 are the K seeds.
    - 23 come from X5-R28 (c). The deleted loop's ground-truth meta findings had credited fake XX001s in MP and MW: some O-3(c) rounds now pass, some MW Ok rounds now fail O-3(d) (MW-ORPHAN 436 → 446), and 8017, 8127 and 8529 now run past round 0. 8017 r1 is the +1 `O1_FAIL`, on a passing Err round.
    - The gate proved the attribution: with the loop restored, seeds 329, 8017, 2327 and 1529 reproduce their esc-2 outcomes exactly.
    - Class C did not move.

## F3 (refreshed by P at this gate)

The counts come from `X-5c-F2.acc.campaign.json`, under amend-1's record-field definitions, and they are written into `cards/X-5c-F3.local.md`:
- C-XX: 130 C rounds (Open 45, AmcheckIi 82, O-2 101), plus 15 MP (8 of them inside LIVELOCK rounds) and 13 MW rounds of O-3(c);
- C-DIV: 424, unchanged, with the r2 rate at 19.6%;
- MP-LIVELOCK: 41, every one directly after an R6 round;
- MP-A: 7;
- MW-ORPHAN: 446;
- other MW: 26 (13 of them under C-XX);
- MP O-1: 4;
- twins: 103 (44 MP, 59 MW);
- shape C: 6.

These sum to C 478 + MP 65 + MW 472 = 1,015 failing rounds. F3 is not split further: X5-R19's valve bounds an attempt. Its step-0 `commit=` check now names 1f8e7c3f explicitly, because the merged tip is this verdict commit. Status: ready. It may launch after `git -C ~/.cache/nv3-cards/X-5c merge --ff-only card/v3-X-5c-F2`.

## Follow-ups

- **X-5c-F2-F1 (test, Gate S; any time after F2 merges, never inside F3's frozen K computation).** Add literal-config harness pins that catch the gate's UNCAUGHT harness mutants. Each pin is demonstrated failing on its revert:
  - K condition 3: a tainted slot with durable == V_k whose only non-landed write was issued **before** the anchor is not in K;
  - K condition 4: an M-NOINPLACE-hit slot is not in K;
  - K_ii is computed against the end image's own meta;
  - `served_k`: a served range outside K gives false, and an empty served list gives the documented vacuous true;
  - the step 1(b) caller: an integration pin in which a re-paired twin with no store re-seeds the round's `twin_chain` (the caller-side revert passes today).

  This is test-only, inside `tests/harness.rs` and `tests/workload.rs`, with no behaviour change.
- **X-5c-F2-F2 (P ruling, then test; Gate P).** `tests/oracle.rs` is frozen, so P must first rule a tests-only text. That text adds must-fail cases for X5-R28 (b)'s arms that have no test today:
  - the `explained` tightening (a K slot alone does not explain an O-1 read XX001 under `r_active_r6`);
  - the (ii) structural guard;
  - the (ii) exactness tightening;
  - the (ii) K skip without `r_active`.

  Each is demonstrated against its revert. `src/oracle.rs` does not change.
