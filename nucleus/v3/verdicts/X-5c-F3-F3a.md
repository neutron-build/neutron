Card: X-5c-F3-F3a
Class: experiment
Card-gate: P
Verdict: REWORK
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: T3 review sub-agent, Claude Code (X-5c-F3-F3a P gate, 2026-11-03)
Date: 2026-11-03
Gate-exit: GATE EXIT=0
Commit: 342745c68edc1ced27c3a4510aec8fc12ea22c4f
Mutants: SEG-EXACT-NO-HOP -> wal_exact_segment_end_hops, SEG-TAIL-SHORT -> wal_tail_short_never_placed, ACK-OVER-HOLE -> ack_prefix_two_batches_in_flight (the unchanged cdiv pin passes under it, as the worker recorded), WAL-ONE-IN-FLIGHT (take refused while a batch is in flight) -> ack_prefix_two_batches_in_flight, BOOT-ID-STOP-SKIP -> stale_boot_record_stops_the_walk, BOOT-ID-EQ-STOP -> meta_rollback_walk_crosses_into_newer_boot, BOOT-ID-CKPT-ONLY -> boot_id_fresh_after_rollback, BOOT-ID-NO-META-FLOOR -> boot_id_after_rollback_raised_then_no_records, TXN-BOOT-CONST (recover sets boot_id = 1) -> txn_ids_never_collide_across_lives, OWN-1 -> r30_predicate_conditions_are_load_bearing, OWN-2 -> r30_predicate_conditions_are_load_bearing, meta_slot_fault drop -> r30_predicate_conditions_are_load_bearing, OWN-3 -> mf29_recovery_read_on_another_file_fails, (own) G1 first-record floor dropped (`last_boot.unwrap_or(rec.txn.0)`) -> UNCAUGHT (full release suite green), (own) G2 ACK-OVER-HOLE cross-check disabled -> x5r34_forged_ack_over_hole_fails_loudly, (own) G3 waiter helper-take guard dropped -> cdiv_syncon_ack_never_crosses_an_unwritten_batch, (own) G4 SegmentEnd carries boot id 0 -> g8_poison_ends_the_workload_instead_of_livelocking (incidental catcher; full suite)
Followups: X-5c-F3-F3a-F1, X-5c-F3-F3a-F2

## How the gate was run

- **Fresh checkout.** `git -C ~/Workspace/neutron-v3 -c core.hooksPath=/dev/null worktree add --detach /tmp/gate-X-5c-F3-F3a 342745c6…`. The common git dir is `~/Workspace/neutron/.git`. It has no non-sample hooks, no `filter.*` or `core.hooksPath` config, and no `info/attributes`. The root `.gitattributes` sets line endings only, with no filter. The index equals the HEAD tree. Every `nucleus/v3` blob matches its working file (`git hash-object --no-filters`), and status is clean.
- **Gate.** `cd /tmp/gate-X-5c-F3-F3a/nucleus/v3 && CARGO_TARGET_DIR=$HOME/.cache/nv3-target/gate-X-5c-F3-F3a bash scripts/gate.sh` ran in the foreground. Every STEP was EXIT=0, and the literal last line is `GATE EXIT=0`. A first attempt ran without `~/.cargo/bin` on PATH (STEP EXIT=127, cargo not found). It was environment-only and was re-run.
- **Mutants** were applied in the gate checkout as source patches (script at `~/.cache/nv3-review/gate-F3F3a-mut/mut.py`, results beside it). Each ran on its pin's release test binary; G1 and G4 ran on the whole release suite. Each was reverted with `git checkout`, and status was 0 lines before removal. My diagnostic patches (stderr only) were reverted the same way.
- **Re-measured.** I ran all 37 X5-R34 (h) 1 rounds as single seeds at the tip. That is the (h) 3 tip re-run, and the worker did not do it. I also single-seed re-ran all 117 Acceptance class-C `0 < frontier < lo` rounds with walk diagnostics (`~/.cache/nv3-review/runlogs/gate-F3F3a/`; `flo.classes.txt` holds the per-round class). No 10k run and no verify were run.
- **Provenance.** The Acceptance prov has commit=342745c6, status_lines=0, exit=0 and verify_exit=0, and its json sha256 `9e8a1396…3606a` matches. 4cefcb43..tip changes no code. The rulings commit 31effe00 (04:30:08) precedes the first runlog (`s0baseline`, 04:44).

## Scope

`git diff card/v3-X-5c...HEAD` touches only `spikes/e4-recovery/` (15 files).
- `src/sim/` changes only by the one `durable_dir_bindings` accessor, and `model.rs` is unchanged.
- The oracle tests change in exactly one commit (94254d12, mf29), and `src/oracle.rs` is unchanged.
- `results/campaign.json` changes only in 929ade38.
- `derive_written_acked` changes only in 13da010b (X5-R34).
- One existing test changed outside the accepted shape maintenance: D7 (finding 4).

## The orchestrator amendment (BOOT-ID-NO-META-FLOOR, f′): faithful in substance, with one wording error

S-STORE §14 step 8 at 7b3d3ac1 is byte-identical to the merged draft 6 on the scaffold. It gives the boot id as the newest of three terms:
- the low 32 bits of **its** `ckpt_id`, meaning the boot checkpoint's own id, k+1;
- 1 + the opened meta's `boot_id`;
- 1 + the newest accepted record's boot id.

It names the mutant BOOT-ID-NO-META-FLOOR and the variant *raised id, then no records*. The amendment carries all of this, with one error. It reads "the **opened meta's** `ckpt_id` low 32 bits", which is k, not k+1. That is off by one against S-STORE whenever the opened meta was not written by a boot checkpoint. The code follows S-STORE (`ckpt.rs` `(st.k + 1) as u32`), and the variant test matches §14 step 8's fixture. **Ruling:** the amendment is accepted, and its first term must be corrected to "the low 32 bits of the `ckpt_id` the boot checkpoint writes (k+1)" before the rework attempt (an orchestrator edit). This is not a spec issue.

## Findings

1. **REWORK: 100 store losses introduced by step 4** (a prototype deviation from S-STORE §14 step 8 and §12.6).
   - **The defect.** `wal.rs` `wal_start_lsn`'s open-transaction (`None`) arm holds `wal_start_lsn` back for every unmarked chunk in `open_chunks`, the previous boot's included. `ckpt.rs` retains those chunks after recovery (`unwrap_or(true)`).
   - **The consequence.** The boot checkpoint writes meta k+1 with the new `boot_id` and a `wal_start_lsn` that points at the previous boot's chunk. The next walk's first record is then older than the opened meta's `boot_id`, so the §12.5 first-record floor stops the walk, and every acked commit of the new boot is lost. It happens with no fault (class C).
   - **What S-STORE says.** §14 step 8 (present in the base at 6db60b2b): "Chunks of the previous boot that have no marker … do not hold back `wal_start_lsn` (§12.6)." Before step 4, every life was boot 1, so the floor never fired.
   - **Scale.** In 100 of the Acceptance file's 117 class-C `0 < frontier < lo` rounds, the stop is on a previous-boot `K_CHUNK` at the opened meta's `wal_start_lsn` (146 first-record stops counting nested recoveries; every one is a chunk). Examples: 206 r1, 296 r1, 405 r1, 705 r1 and 9823 r1 (meta ckpt 1, boot 1, wstart 5059, first record `(0, 5)` K_CHUNK).
   - **Fix.** The arm applies only to chunks of the current boot.
   - **Pin.** A literal config: boot b−1 leaves a cut-off chunked txn below p0; boot b's boot checkpoint runs; b appends and acks SyncOn commits and crashes before its next checkpoint. Recovery must replay them (frontier ≥ max acked). The mutant START-LSN-PREV-BOOT-CHUNK (the old arm) must fail it.
   - **Also pin G1** (the first-record floor), which nothing catches today: a walk whose first record is older than the opened meta's `boot_id` stops there.
   - **Movement.** Then single-seed the five named repros: each must leave `frontier < lo`.
2. **ESCALATE→ruled (P ruling X5-R35, below): 17 false losses from X5-R34's E.** `new_round` runs at the end of round r, so round r+1's captured E also holds round r's recovery and the harness boot checkpoint. That includes step 5's truncation zero-write over [p0, segment end) and its sync.
   - Where the workload had enqueued SyncOn records but never written them before the crash, (W)'s "last covering write" is the synced zero-fill. D runs past them, and `lo` is raised to commits that never reached disk. 403 r1: round 1 has no WAL record write at all, yet D=65536 and lo=7.
   - The other 17 rounds are 205 r2, 381 r1, 403 r1, 774 r2, 1174 r2, 1203 r1, 3805 r2, 3883 r2, 4193 r1, 4601 r1, 4612 r2, 5030 r1, 7203 r2, 7803 r1, 8200 r2, 8404 r1 and 8825 r2. In each, the walk stops at zeros.
   - X5-R34 (b) 1's premise ("the workload pass's events … because `new_round` cleared the list") is false of the capture. The worker implemented the text faithfully, so this is a ruling defect, not the worker's.
3. **REWORK: (h) 3's tip re-run of the 37 was not done.** At the tip, 403 r1 (frontier 3, lo 7) and 7803 r1 (4, 8) are failing class-C `frontier < lo` rounds that (h) 3 escalates with a trace. They were reported only inside the 117 as "P's triage". The other 35 pass. The rework re-runs the 37 at its own tip and records the table.
4. **Process (accepted): D7's mutant arm was inverted in 8b71e560,** against Limit 6 and "Do not". The new expectation is the one S-STORE §12.5 designs ("Stale records come from older boots", stopped by identity), so I accept it as a P ruling. The worker should have escalated before changing the assertion. The lost catcher is ESCALATE 1 below.
5. **Recorded deviation (accepted): ACK-OVER-HOLE leaves the unchanged cdiv pin green,** because the guarded waiter never takes a batch above a held one. The new pin catches the mutant, M-WALa still fails cdiv, and G3 shows the guard is load-bearing.
6. **Minor.** Step 6 used about 7 of its 40 minutes. This is permitted, but it is the reason ESCALATE 2 arrives thin.
7. **Sound.**
   - X5-R34 (b)–(g): the (b) 3 accessor reads durable bindings, β is respected (G2), and every self-test shows its demonstration.
   - The hop, the tail rule, multi-batch accounting with `check_wal_inv`, the ordered serial-number stop, and the three-way id are all correct.
   - mf29 matches its text.
   - No `unsafe` and no non-test `unwrap`.

## P ruling X5-R35 (gate, 2026-11-03; post-data: the 17 rounds above were seen; binding from the rework's first run)

- **(a)** X5-R34's `E` is the slice of `disk.events` from the workload pass's first event to its end. The recovery and the harness boot checkpoint that precede the workload in `run_round`'s capture are not in `E`. (b) 1–5, (c)–(f) and (i) are otherwise unchanged. `Rc` and β are read over this `E`.
- **(b)** A new self-test (g) 6 must fail: a recovery truncation zero-write plus its sync covers an enqueued, never-written SyncOn record, which must not be acked. Show that it fails on 13da010b's derivation, then revert.
- **(c)** The rule. At the rework's tip, each of the 17 named rounds must leave `frontier < lo` or be recorded as moved (X5-R34 (h) 6). Any that keeps it is an (h) 3 store loss.
- **(d)** This raises no `lo` and tightens nothing else. It removes an unsatisfiable lower bound: a commit whose bytes never reached disk.

## ESCALATE rulings

- **1 (TRUNCATE-LOGICAL-ONLY uncaught).** Not a blocker and not a spec issue. Under §12.5, physical truncation matters only in the Residual: two boots sharing an id. That needs a torn first record plus a post-sync lost meta (class MP), and the mutant then stands in for the MW revert. Ruled as a P-tier test follow-up, **X-5c-F3-F3a-F1** (card written, Status: waiting). It does not change F3-F3b's scope.
- **2 (C-DIV (b)/(c)/(d) remainder).** Not a blocker. The evidence (a live key whose owner commit is ≤ T and ≤ the frontier, missing from the recovered scan) points at image content (§13 (c)), not the WAL. So it is neither F3-F3b's coverage work nor F4's retune. Ruled as a P-tier diagnosis follow-up, **X-5c-F3-F3a-F2** (card written, Status: waiting; it runs on the reworked Acceptance file, because every repro moves). X-5c-F4 should gain `Depends: X-5c-F3-F3a-F2` (orchestrator). F3-F3b's scope does not change, but its carried texts and frozen list must add X5-R35, and its Goal counts are refreshed at this card's re-gate.

## Q8 ruling

Q8 is not new and not this card's. F3-F1's own verify already missed it (83/100, `X-5c-F3-F1.acc.verify.out:20`), and no one recorded it. Here it is 80/100. SyncOff acked = written, which X5-R34 leaves unchanged, so the derivation is not the cause. Q8 joins X5-R17's retune list in **X-5c-F4** beside Q4, Q7, Q11, Q12 and SIZING (orchestrator's edit). It is not a follow-up card.

## Follow-ups

- **X-5c-F3-F3a-F1 (P, test).** Pin TRUNCATE-LOGICAL-ONLY with §12.5's Residual fixture in the prototype. Boot b's first record is torn, and its meta k+1 is LOST after sync, so boot b′ recovers from k with the same id. Under the mutant, b's later same-id record lines up after b′'s records and is replayed. With real truncation, the round passes. If the fixture cannot be built with the sim's existing faults, ESCALATE.
- **X-5c-F3-F3a-F2 (P, diagnosis).** Root-cause C-DIV's "missing ≤ T entry" shape from the reworked Acceptance file's repros: one each of (b), (c) and (d), with `E4_O1_DIAG` plus the image's page history for the key. Fix a prototype deviation with a pin, or ESCALATE.
