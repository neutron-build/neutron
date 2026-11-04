Card: X-5c-F3-F3a
Class: experiment
Card-gate: P
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: T3 review sub-agent, Claude Code (X-5c-F3-F3a P re-gate after REWORK, 2026-11-03)
Date: 2026-11-03
Gate-exit: GATE EXIT=0
Commit: 3c743d30e85981dca06a593921935be8e135e50a
Mutants: START-LSN-PREV-BOOT-CHUNK -> wal_start_lsn_ignores_previous_boots_chunks, G1 first-record floor dropped (`last_boot.unwrap_or(rec.txn.0)`) -> first_record_older_than_opened_meta_stops_the_walk, X5-R35 (g) 6 whole-capture E (helper returns `captured`) -> x5r34_recovery_truncation_zeros_never_ack, SEG-EXACT-NO-HOP -> wal_exact_segment_end_hops, SEG-TAIL-SHORT -> wal_tail_short_never_placed, ACK-OVER-HOLE -> ack_prefix_two_batches_in_flight, WAL-ONE-IN-FLIGHT -> ack_prefix_two_batches_in_flight, BOOT-ID-STOP-SKIP -> stale_boot_record_stops_the_walk, BOOT-ID-EQ-STOP -> meta_rollback_walk_crosses_into_newer_boot, BOOT-ID-CKPT-ONLY -> boot_id_fresh_after_rollback, BOOT-ID-NO-META-FLOOR -> boot_id_after_rollback_raised_then_no_records, TXN-BOOT-CONST -> txn_ids_never_collide_across_lives, OWN-1 -> r30_predicate_conditions_are_load_bearing, OWN-2 -> r30_predicate_conditions_are_load_bearing, meta_slot_fault drop -> r30_predicate_conditions_are_load_bearing, OWN-3 -> mf29_recovery_read_on_another_file_fails, G2 ACK-OVER cross-check off -> x5r34_forged_ack_over_hole_fails_loudly, G3 waiter guard dropped -> cdiv_syncon_ack_never_crosses_an_unwritten_batch, G4 SegmentEnd boot id 0 -> g8_poison_ends_the_workload_instead_of_livelocking, (own) RG1 run_round boundary `pre_workload = 0` -> UNCAUGHT (full release suite green; campaign: 403 r1 / 7803 r1 back to frontier < lo), (own) RG2 derive_written_acked fed the unsliced capture -> UNCAUGHT (same), (own) RG3 boot filter inverted -> wal_start_lsn_ignores_previous_boots_chunks
Followups: X-5c-F3-F3a-F1, X-5c-F3-F3a-F2, X-5c-F3-F3a-F3

This re-gate verdict replaces the REWORK verdict at 98313ae3. Its Findings 1 and 3 and P ruling X5-R35 have been applied. Its ESCALATE rulings, its Q8 ruling and its follow-ups F1 and F2 still stand unchanged.

## How the gate was run

- **Fresh checkout.** `git -C ~/Workspace/neutron-v3 -c core.hooksPath=/dev/null worktree add --detach /tmp/gate-X-5c-F3-F3a 3c743d30…`.
  - The common git dir (`~/Workspace/neutron/.git`) has no non-sample hooks, no `filter.*`, `core.hooksPath` or `core.attributesFile` config, and no `info/attributes`.
  - `.gitattributes` sets line endings only.
  - The index equals the HEAD tree (`git write-tree`).
  - All 287 `nucleus/v3` blobs match their working files (`hash-object --no-filters`), and status is clean.
- **Gate.** `CARGO_TARGET_DIR=$HOME/.cache/nv3-target/gate-X-5c-F3-F3a bash scripts/gate.sh` ran in the foreground (3m44s). Every STEP was EXIT=0, and the literal last line is `GATE EXIT=0`.
- **Mutants.** Applied as source patches in the gate checkout (`~/.cache/nv3-review/gate-F3F3a-rg/mut.py`, `result.txt`), each reverted with `git checkout`. Status was 0 lines before the checkout was removed.
- **Re-measured** with the binary built from the clean tip, before any mutant was applied:
  - the five Finding-1 repros (206, 296, 405, 705, 9823): all pass;
  - six of the 17 X5-R35 rounds (205, 403, 1174, 4612, 7803, plus 0..200);
  - 1174 and 4612: the seed records are byte-identical to the worker's `rw2.s*.json`;
  - 0..200: the seeds are identical to `rw2.0200.json` (FAILING_ROUNDS 17, TWIN 1, DEFECT 0);
  - the Acceptance record check on the 10k file: `ACK_OVER_ROUNDS 0`, `C_FRONTIER_BELOW_LO 0`, `MISSING_FIELDS 0`.
- **Provenance.**
  - The 10k json sha256 `46297043…0c42c` comes from the worker's run at 6fa80bb5 (README). The runner's own Acceptance re-run at the tip 3c743d30 (prov `commit=3c743d30…`, `status_lines=0`, `exit=0`, `verify_exit=0`) reproduced it byte for byte.
  - `git diff 6fa80bb5..3c743d30` changes only `README.md`, so the digests do not depend on the README-only tip commit.
  - The rulings commit 3c8b038e (17:31:08) comes before every rework runlog (the first is `rw2.s20` at 17:40:36). The 17 and the 37 ran at the code tip 313a236b.
  - The prov records a load of 22.8 at the end of the 10k, a co-scheduled host. The digests are deterministic and verify reproduced them, so this has no effect.

## Scope and limits

- `card/v3-X-5c...HEAD` touches only `spikes/e4-recovery/` plus this verdict.
- The rework's code is three files: `src/harness.rs` (93039525), `src/store/wal.rs` (313a236b) and two test files.
- **Limit 6 holds.** `git diff 342745c6..3c743d30 -- tests/` has no removed line. It only adds three tests, and no existing assertion changed.
- **Item 6 (Limit-2 exception) is faithful to the verdict.**
  - X5-R35 (a) can only be implemented by changing what `derive_written_acked` reads. Item 6 mirrors Limit 2's existing step-1 exception: one commit, its subject naming the ruling, scoped to (a)/(b), with Limit 6 kept.
  - The exception was barely needed. 93039525 leaves `derive_written_acked`'s body untouched, slices `E` at its call site, and adds the helper `x5r34_workload_events`.
  - X5-R5's R, X5-R28's K, Q12's fold and the monitors still read the full capture, as (a) scopes it.
- The corrected (f′) first term (k+1) matches `ckpt.rs` (`(st.k + 1) as u32`).

## Findings

1. **Finding 1 is fixed, and only for the current boot (check ii).**
   - `wal_start_lsn`'s `None` arm now holds back only when `!boot_id_older(txn.0, st.boot_id)`. Because no chunk can carry a newer id than the running boot, this means "this boot" exactly. That is S-STORE §14 step 8: "Chunks of the previous boot that have no marker … do not hold back `wal_start_lsn`".
   - `recover` sets `st.boot_id` before the boot checkpoint computes the start, so the filter is live at the checkpoint that mattered.
   - The marked arm is unchanged, and so is current-boot hold-back: RG3 (the filter inverted) is caught.
   - `boot_id_older` is the walk's serial-number comparison.
   - The pin decodes the durable stream, asserts meta 2's `wal_start_lsn == p0`, and asserts the replay. START-LSN-PREV-BOOT-CHUNK fails it.
   - The G1 pin closes the first verdict's UNCAUGHT.
2. **The 1174 r2 / 4612 r2 classing is correct (check i).**
   - Both have `frontier == lo == hi` (40 and 24) and `ack_over` empty.
   - O-1(b) (the scan at c) and O-1(c) (the raw cut) pass. Only O-1(d) fails, and at a historical sample point: S=6 and S=2, with `gc_w = 0`.
   - Those S values lie in round 0's commits (round 0's lo is 33 for 1174 and 6 for 4612). They are therefore at or below the T of every meta that round 2's recovery can open.
   - So every acked commit is present at c. What diverges is image-held MVCC history, which is the C-DIV (d) shape that F2 owns (§13 (c)).
   - X5-R34 (h) 3 is defined on `0 < frontier < lo`, and (h) 1 classes "a named round that then fails another clause … under that clause". This is not an (h) 3 store loss.
   - X5-R35 (c) holds: all 17 left `frontier < lo`.
3. **FOLLOW-UP: X5-R35's run_round wiring is unpinned (RG1, RG2).**
   - (g) 6 tests the helper with a boundary it supplies by hand. No test fails if `run_round` computes `pre_workload` wrongly (RG1) or feeds `derive_written_acked` the unsliced capture (RG2).
   - Both mutants are real: they bring back 403 r1 (frontier 3 < lo 7) and 7803 r1 (4 < 8).
   - Today only the campaign record check (`C_FRONTIER_BELOW_LO`) would catch them. This is non-blocking, because the committed 10k was produced by the correct wiring and verify reproduced it.
4. **Finding 3 is done.** The 37 were re-run at 313a236b: all pass, `ack_over` is empty, and no class-C `0 < frontier < lo` round remains, named or new in 0..200.
5. **The class table checks out.** MW-ORPHAN is 406 by my count of the shape (`o1` O-1(c) raw-above-cut, and `o3` O-3(d) silent Ok), which matches the worker.
6. **Sound.** No `unsafe` and no non-test `unwrap`. The diagnostics are unchanged. Docs are present on the new pub items (`boot_id_older`, `x5r34_workload_events`).

## X-5c-F3-F3b Goal refresh (from `X-5c-F3-F3a.acc.campaign.json`, sha256 46297043…)

- **MW-ORPHAN:** 451 in F3's file, 445 in F3-F1's file, and **406** in F3-F3a's file (all 406 failing).
- **The 0..200 named rounds grow from six to eight:** 19 r0, **39 r0**, 59 r0, **69 r0**, 109 r0, 149 r0, 159 r0, 199 r2.
  - 39 r0 (lo 8, frontier 5) and 69 r0 (lo 21, frontier 12) became orphan-shaped at the first run's step 3 (`X-5c-F3-F3a.s3.0200`), not in the rework.
  - Step 5 (a)'s smoke list and the COVER-CHECK-SKIP demonstration should name all eight.
- **X5-R32 (d) 2's five:** 4699 r0 is now orphan-shaped. 1889 r2, 3509 r0, 5229 r1 and 8399 r1 pass.
- **Carried texts and frozen list.** F3-F3b's carried texts and its frozen list (Limit 2, "X5-R34's derivation and cross-check") must add X5-R35 and its slice (`x5r34_workload_events` and `pre_workload`).

## Follow-ups

- **X-5c-F3-F3a-F1, X-5c-F3-F3a-F2:** as written in the REWORK verdict, unchanged. F2 runs on this Acceptance file, with C-DIV at 256 (b 101 / c 96 / d 59); 1174 r2 and 4612 r2 are (d) repros.
- **X-5c-F3-F3a-F3 (S, test): pin X5-R35's run_round wiring.**
  - Add a literal-config harness test that runs `run_round` (or the seed driver) on seed 403 through round 1.
  - It asserts that round 1 derives `lo == 3 ≤ frontier`, that ts 7 is not acked, and that `ack_over` is empty.
  - The pin must fail on RG1 (`pre_workload = 0`) and on RG2 (`derive_written_acked(&events, …)`). Demonstrate both, revert them, and record them.
  - Test-only: no harness or store code changes, and if seed 403 moves, re-point it under the Limit-6 shape-maintenance rule.
  - It may land in X-5c-F3-F3b instead, if the orchestrator prefers to fold it into that card.
