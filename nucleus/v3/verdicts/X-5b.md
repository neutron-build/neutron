Card: X-5b
Class: experiment
Card-gate: P
Verdict: REWORK
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: claude-code-agent 8c7a09a1-e6d0-4430-a5dd-d8a90f0215fa
Date: 2026-10-26
Gate-exit: GATE EXIT=0
Commit: 19fa14f52b451be94a05aee28535cea34ef603ad
Mutants: G1 S-BATON held-flag checks removed -> baton_violation_is_reported (hangs; killed at 120 s), G2 gate_exclusive holders re-check always true -> d5_split_straddling_flip, G3 yield inside Cx::with (gate_shared) -> 19/20 directed tests, try_lock replaced by a blocking lock -> baton_violation_cross_thread_holder_is_reported (hangs; killed at 90 s), CRC-SKIP hard-coded -> d17_integrity, REFCRC-SKIP hard-coded -> d17_integrity, EPOCH-SKIP+REFCRC hard-coded -> d17_integrity, ID-SKIP+REFCRC hard-coded -> d17_integrity + d10b_rollback_exception, G4 recovery serves dropped from O-3(a) -> d17_integrity, G5 R-taint call sites forced false -> UNCAUGHT, X5-R5 condition 3 removed from exception_set_r -> UNCAUGHT, R4 positional expected id reverted to header_id in walk_radix/walk_freeset -> UNCAUGHT, BulkLoad S_L pre-mark removed -> d11_publish (hangs; killed at 300 s), checkpoint abort path keeps ckpt_open -> UNCAUGHT, R-taint window not restarted at Crash -> r_taint_window_semantics, radix_child_span flattened -> UNCAUGHT (equivalent at the tested depth <= 2), RoundOutcome.pass ignores deadlock/panicked/baton -> UNCAUGHT, TS-AFTER-APPEND (arm run) -> UNCAUGHT (d18 vacuous), ID-SKIP+REFCRC in D10b's own shape (probe) -> O-3(a) served via R-taint; silent once G5 is applied
Followups: none

## Scope of this verdict
This is round 2 of the X-5b P gate, on tip 19fa14f5 (the REWORK commits C1–C4,
the gate fixes and the verification round, all after verdict 98453395).
- **Gate run.** It ran on the tip, in the foreground, with
  `CARGO_TARGET_DIR=~/.cache/nv3-target/gate-X-5b`. Every step printed EXIT=0,
  and the last line was `GATE EXIT=0`.
- **Frozen files.** `src/oracle.rs`, `src/model.rs` and `tests/oracle.rs` are
  byte-identical to base 266b8f4c. `tests/sim.rs` only gains tests. Every
  changed file is under `spikes/e4-recovery/`, plus this verdict.
- **Probes.** Every mutant and probe was applied by hand and reverted. A
  temporary probe file was deleted. `git status` was clean before this file
  was written.

## Round-1 items
- **R1 — partly met.**
  - Real catching arms now:
    - D10 applies its LOST fault. Clean gives XX001 at open plus
      `meta_rollback`; the mutant gives O-3(d).
    - D11 runs the real BulkLoad.
    - D14 FREE-EARLY is caught by M-NOINPLACE.
    - D17 asserts detection on the map leaf.
    - D7 and D16 no longer assert that the mutant passes.
  - New problem: D18 is vacuous (N3).
  - The D7 argument is rejected (N6).
- **R2 — met.**
  - D17's clean arm asserts `!served`.
  - The skip is now an `expect`.
  - All four hard-coded §3.3 disables fail d17, and the G4 probe fails d17.
- **R3 — partly met.**
  - What exists:
    - D10b, with R non-empty, O-3(e) and amcheck (i) ⊆ R.
    - Condition 3 is implemented.
    - The interleaved write/accept window is implemented, and
      `r_taint_window_semantics` pins the function.
  - What is missing:
    - Condition 3 is unpinned.
    - The R-taint call site is unpinned, although it is reachable (N5).
- **R4 — implemented, unpinned.**
  - The positional ids are in `ckpt.rs`, `amcheck.rs`, `harness.rs` and
    `monitor.rs`.
  - Reverting the store walkers to `header_id` passes the whole suite. The
    reference CRC subsumes the id check.
  - The pin is a map-page MISDIRECTED under REFCRC-SKIP. With the reference
    CRC off and the epochs equal, only the id check discriminates.
- **R5 — met.** BulkLoad runs. Without the S_L pre-mark, D11 hangs, which is a
  test failure but not a named DEADLOCK.
- **R6 — met.**
  - The cross-thread test hangs under a blocking lock (killed after more than
    60 s).
  - The doc comment on `baton_violation_is_reported` still claims that hang
    for itself.
- **R7 — partly met.**
  - B1's size is reported: 14 files, +4935/−2702, about 7.6k changed lines
    against the ~1.5k budget.
  - D10b is reported.
  - TS-AFTER-APPEND is still misreported as caught by d18.
- **Minors.**
  - Done: `pass` folds the deadlock, panic and baton flags. `accepted`
    propagates its error. The release paths call `note_step_fail`. There are
    5 new sim tests. The WorldGuard comment is fixed.
  - Unpinned: the `pass` fold.

## New findings: REWORK
**N1. A store deviation from S-STORE §13 step 2: FreeSet sizing after growth (lines 770–784).**
- `write_free_set` (`ckpt.rs:978`) sizes the FreeSet from the frozen
  `slot_count`. When its own allocations grow the file, it only sets
  `freeset_grew`. It does not recompute the page count and allocate the
  difference.
- So the meta records a `slot_count` below the free-set slots it references.
  amcheck correctly reports "slot_count below a referenced slot".
- The defect shows in clean class-C rounds. Shape: 60 puts with 200-byte
  values, then SyncOff overwrites, `checkpointer_every` 3, crash at
  After5/After6 of checkpoint 0. Amcheck (i) fails in 7 of 8 seeds, at every
  `R_EPOCH`. The same shape gives amcheck (ii) findings at After4.
- The committed tests already hit it:
  - D17's REFCRC clean arm reports amcheck (i), and its `pass` is false.
  - D9's clean arm reports amcheck (ii) and still passes (see N2).

**N2. O-2 (ii) is never judged.**
- The harness runs amcheck (ii) after `check_round` and never sets
  `Extracted.amcheck_ii` or `amcheck_ii_ran`.
- So O-2 (ii) and the (ii) half of O-3(e) are skipped for every round. That
  weakens a Work 2 check, which the card's "Do not" forbids.
- Fix: run the harness checkpoint and amcheck (ii) before the verdict (G4
  orders the checkpoint after recovery, not after the judgement), or feed
  their result into it.

**N3. D18 is vacuous.**
- The clean round has `pass=false`. The cause is `task_error="Crashed"` while
  the world is not marked crashed and no deadlock flag is set: a harness
  defect in `h:taskerr`, with a two-task TaskYield crash.
- The mutant round is identical: `o1=Ok(2)`, no M-WAL-ORDER. So
  `|| !m.pass` makes the arm pass whatever happens.
- TS-AFTER-APPEND is therefore uncaught, and the RESULT claims "d18
  (M-WAL-ORDER + O-1)".

**N4. Clean arms never assert `pass`.**
- Work 14 says the clean arm is where "the oracle, monitors and amcheck
  pass". Yet D17's clean2 and D18 fail the oracle and stay green.
- Every clean arm must assert `pass`, except D12b's designed fail-stop.
- Mutant arms must name their catcher, not `!m.pass`: D9, D11, D12, D15, D18
  and D14's BITMAP arm.

**N5. G5 "UNCAUGHT-directed" is refuted.**
- In D10b's own shape and seed (2020), the card's paired mutant ID-SKIP+REFCRC
  serves a reused slot. The slot is in R, and the R-taint fails O-3(a).
- With the R-taint call sites forced false, the same round passes silently.
- Add that arm. It also pins the R-taint's positive case.
- The REFCRC-SKIP-alone argument is confirmed: check (3) fires, and so does
  EPOCH-SKIP+REFCRC. But arm 4's current assert is vacuous: `run()` without a
  twin makes O-3(c) fail closed, so `!m.pass` always holds.

**N6. The D7 equivalence argument is rejected.**
- §12.5's position check makes a zombie a lining-up event, and a directed
  test can force that line-up. In round 2, append a record whose byte length
  equals the torn record's. The next read position is then the intact old
  record, whose lsn equals that position.
- "Not in the remaining budget" is not an equivalence.
- D16: three post-flip SyncOff shapes of mine did not catch either mutant. The
  argument is not refuted, and it may stand if restated precisely.

**N7. Rework fixes that are unpinned** (each passes the whole suite when
reverted):
- condition 3;
- R4;
- the abort path's `ckpt_open` clear (D12b still passes);
- the `pass` flag fold.

**N8. D17 coverage.** The card asks for TORN, LOST and MISDIRECTED, each on a
tree leaf, a MapLeaf, a FreeSet page and a meta slot. That is 12 cells. Only
the 3 MapLeaf cells and tree-page MISDIRECTED exist.

No ESCALATE. N1 is a deviation from the spec as written, not a spec conflict.
