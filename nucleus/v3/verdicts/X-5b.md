Card: X-5b
Class: experiment
Card-gate: P
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: claude-code-agent db818073-cd8e-44ec-8c93-0a1199ce01e4
Date: 2026-10-26
Gate-exit: GATE EXIT=0
Commit: fa3c50491438ce6bb23468f39f6aee9ef88ba1bb
Mutants: F1 record cap removed in wal_enqueue -> wal_record_cap_is_refused (and the F1 repro round still fails loudly through the derivation's fold), F1 run_round fold of unparsed_written disabled -> equivalent while the cap holds (only both removed restores the silent pass), META-WRONG-SLOT forced at the store's open -> d17_integrity (G7 clean arm: served, O-3(a)), positional meta test removed in select_meta at every site -> d17_integrity + g7_meta_selection_semantics, R4 header_id in store walkers + amcheck + harness walks -> r4_positional_id_discriminates_the_empty_leaf_plant, R4 header_id in store walkers only -> r4_positional_id_discriminates_the_empty_leaf_plant, R4 header_id in amcheck only -> equivalent (load_image walks the same radix and free set eagerly, rejects first), R4 header_id in harness walks only -> equivalent (mutant=None keeps check (2); a CRC-matching page carries the right id), X5-R5 condition 3 removed inside exception_set_r -> exception_set_r_consults_its_noinplace_hits, run_round passes an empty noinplace set to exception_set_r -> UNCAUGHT (masked: the same write draws an M-NOINPLACE monitor hit), open falls back to the placed-valid meta on MisplacedNewer -> d17_integrity, Err-branch twin recovery_reads emptied -> d17_integrity, open's accept recomputed from ckpt_id -> equivalent under G7 (the slots coincide; in D17's MWS arm both slots are tainted), select_meta tie (misplaced equal ckpt_id) made XX001 -> UNCAUGHT (unreachable: no fault duplicates a meta), N1 convergence loop disabled -> freeset_growth_covers_its_own_slots + d9 + d10b + d17 + r4 pin, G5 R-taint call sites forced false -> d10b_rollback_exception, G2 gate_exclusive holders re-check always true -> d5_split_straddling_flip, G3 yield inside Cx::with (gate_shared) -> 21/22 directed tests + 9 others, TS-AFTER-APPEND hard-coded -> d18_ts_order, step-2 fixpoint single pass -> step2_fixpoint_writes_late_epoch_copies, fetch_page Stale re-validation removed -> fetch_page_revalidates_its_carried_ref
Followups: X-5c

## Scope of this verdict
This is round 4 of the X-5b P gate, on tip fa3c5049. It covers the four
round-3 rework commits c68f61a0..fa3c5049, after verdict f8a6b6eb.
- **Ruling.** The gate read P's F3/F4 ruling, `F3-RULING.md`. The worker
  had removed the file from the worktree in attempt 3, because the runner
  flagged it as out of scope. Its full text survives in
  `cards/logs/X-5b.attempt1.jsonl` (line 349, the worker's read). The
  §2.2 sentence also survives in card S-1-F2, which has merged.
- **Gate run.** The gate ran on the tip, in the foreground, with
  `CARGO_TARGET_DIR=~/.cache/nv3-target/gate-X-5b`. Every step printed
  EXIT=0, and the last line was `GATE EXIT=0` (2 m 49 s).
- **Frozen files.** `src/oracle.rs`, `src/model.rs` and `tests/oracle.rs`
  are byte-identical to base 266b8f4c. `src/sim/` is unchanged since
  round 2.
- **Probes.** Every mutant was applied by script and reverted with
  `git checkout`. Each run was `cargo test -p e4-recovery --no-fail-fast`.
  The probes ran from `tests/zz_gate_probe.rs`, which was deleted
  afterwards. `git status` was clean before this file was written, and
  nothing is left running.

## Checklist
- **F1 — met.**
  - `wal_enqueue` refuses records over 16 KiB with `Fail::Invalid`.
  - **The gate's own repro, re-run:** D17's first-half script, task 2
    `WaitVisible(2)`, crash `TaskYield(2,2)`, seeds 2016/2017/2018/7/99,
    each with and without a checkpointer.
    - The single 177,660-byte `Commit` is now a loud failure:
      `task_error` = cap refusal, and the round fails.
    - As `Chunked`, the commit is acked and visible, then recovered:
      lo = hi = frontier = cut = 2, pass, no `acked_lost`.
  - With only the cap removed, the round still fails loudly through
    "wal_commits written but unparsable". Only removing both the cap and
    the fold restores round 3's silent pass.
- **F2 — met.**
  - D17's faultless base passes: `Ok(2)`, k = 2, amcheck (i) and (ii)
    empty, no monitors.
  - The meta LOST and TORN cells assert pass, `!served` and
    `meta_rollback`.
  - The round-2 finding is withdrawn in the README.
- **F3 — met, per the ruling.**
  - **One helper.** `page::select_meta` is called at every site: open,
    amcheck, `monitor::durable_meta_now`, and both harness walks.
    `workload.rs` is outside the module tree.
  - **Open.** It raises XX001 before any accept, and accepts the slot the
    winner was read from.
  - **D17 cell.** The plant lands on the twin slot, and k ≥ 2 is
    asserted.
    - Clean arm: pass, `open` detection, `!served`.
    - META-WRONG-SLOT arm: served, `!pass`, O-3(a).
  - The defect-as-expected assertion is gone.
  - My open-fallback mutant (the forbidden fallback past a synced meta)
    is caught by the same cell.
- **F4 — met.**
  - `is_read_failed` accepts only the three read-failed forms.
  - Leaf LOST and TORN (the R6 case) assert recovery `Ok`, a read-failed
    `o1`, `!served` and no `task_error`. They assert neither pass nor
    `!pass`, and they print their outcome.
  - The RESULT reports them, as anomaly (1) next to item 8. The seed,
    2016, is implicit.
- **N4 — met.** Every clean arm asserts pass.
  - The R4 pin's arm runs under REFCRC-SKIP. That is correct: with check
    (2) on, the positional id is redundant by construction.
- **R4 — pinned.** Reverting it in all three places, or in the store
  walkers alone, fails the pin: served, O-3(a). I confirm the two
  single-site reverts (amcheck only, harness only) as equivalent mutants,
  with the reasons given in the Mutants line.
- **N7 leftover — met as named.** Removing condition 3 inside
  `exception_set_r` fails `exception_set_r_consults_its_noinplace_hits`.
- **N8 — met.** All nine cells of the grid run on the clean base, and
  each one asserts what it measures.
- **Rounds 1–3 still hold:** N1, D10b (G5), D18, G2, G3, and both F5
  pins. G1 was not re-run, because `src/sim/` is unchanged.

## Findings (non-blocking; for X-5c, which already rewrites these D17 cells)
1. **D17 TORN sectors: the README and RESULT claim is false.**
   - `cell()` computes a sector that "proves the bytes differ", but the
     grid's `fault.kind` match overwrites every TORN cell with
     `sectors: vec![fault.off]`, which is sector 0. The computation is
     dead code.
   - The coverage itself is real: every TORN cell asserts a detection or
     a rollback.
   - Fix the code or the claim: RESULT judgement call (3) and README F2.
2. **D17 has a stale comment block.** It still calls meta MISDIRECTED "the
   gate's F3 spec silence … asserts nothing verdict-shaped", but the code
   below it asserts G7.
3. **`run_round`'s wiring of the noinplace hits into `exception_set_r` is
   unpinned.** Today it is masked by the monitor hit. X-5c should pin it
   once monitor hits fold into the campaign verdict.
4. **The REFCRC clean2 detection disjunct still uses a raw
   `o1.is_err()`.** It is harmless, because `pass` is asserted alongside
   it, but it differs from the letter of F4.
5. **The orchestrator should store the ruling file somewhere durable.**
   The worker deleted it, and the logs are its only copy.
