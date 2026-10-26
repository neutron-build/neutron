Card: X-5b
Class: experiment
Card-gate: P
Verdict: REWORK
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: claude-code-agent 5512ac43-9865-4805-b550-c2a80f9f3fd1
Date: 2026-10-26
Gate-exit: GATE EXIT=0
Commit: 3b509c30d24ab659a80cc41395883698e3c19a51
Mutants: N1 convergence loop disabled -> freeset_growth_covers_its_own_slots (+ d9, d10b), N2 amcheck (ii) feed dropped -> d14_monitors (BITMAP arm), G5 R-taint call sites forced false -> d10b_rollback_exception, X5-R5 condition 3 removed at the exception_set_r call site -> UNCAUGHT, fold_pass reverted to the draft -> pass_fold_flags_are_load_bearing, abort path keeps ckpt_open -> d12b_gcw_bound, R4 header_id in the store walkers -> UNCAUGHT, R4 header_id in amcheck -> UNCAUGHT, R4 header_id in the harness walks -> UNCAUGHT, R4 header_id in all three -> UNCAUGHT, ckpt freeze eviction drain removed -> freeset_growth_covers_its_own_slots + d6, step-2 fixpoint re-scan removed -> UNCAUGHT, ckpt2:record re-validation removed -> d10b_rollback_exception, fetch_page Stale re-validation removed -> UNCAUGHT, G2 gate_exclusive holders re-check always true -> d5_split_straddling_flip, G3 yield inside Cx::with (gate_shared) -> 20/21 directed tests, CRC-SKIP hard-coded -> d17_integrity, TS-AFTER-APPEND (arm) -> d18_ts_order (M-WAL-ORDER + O-1), TRUNCATE-LOGICAL-ONLY (arm) -> d7_zombie_tail round 2, wal_enqueue refuses records over 16 KiB (probe) -> d14_monitors + d17_integrity (their scripts exceed the cap)
Followups: none

## Scope of this verdict
This is round 3 of the X-5b P gate, on tip 3b509c30 (the five round-2 rework
commits 9a3c012f..3b509c30, after verdict b76eb0ef).
- **Gate run.** It ran on the tip, in the foreground, with
  `CARGO_TARGET_DIR=~/.cache/nv3-target/gate-X-5b`. Every step printed
  EXIT=0, and the last line was `GATE EXIT=0` (2 m 47 s).
- **Frozen files.** `src/oracle.rs`, `src/model.rs` and `tests/oracle.rs` are
  byte-identical to base 266b8f4c. `tests/sim.rs` only gains tests plus one
  doc correction. `src/sim/` is unchanged since round 2. Every changed file is
  under `spikes/e4-recovery/`.
- **Probes.** Every mutant was applied by hand and reverted. The probes ran
  from a temporary test file, `tests/zz_gate_probe.rs`, which was deleted
  afterwards. `git status` was clean before this file was written. Nothing is
  left running.

## N1–N8 and the R7 leftover
- **N1 — met.**
  - The convergence loop follows §13 step 2.
  - Disabling it fails `freeset_growth_covers_its_own_slots` with round 2's
    exact finding ("slot_count below a referenced slot", seed 2100).
  - My own sweep was clean: 1440 class-C rounds, covering all 10 hooks,
    checkpoint index 0 and 1, `R_EPOCH` 1/2/4, `freeset_leaf_bits` 128 and
    512, F-CRASH keep/tear 0/0 and 600/300, 6 seeds, and 5 commits with
    SyncOn/SyncOff overwrites. Every round had `pass`, amcheck (i) and (ii)
    empty, and no monitor hits.
- **N2 — met.**
  - Amcheck (ii) feeds the final `check_round`.
  - Dropping the feed fails D14's BITMAP arm, because O-2 then goes
    unjudged.
- **N3 — met.**
  - D18's clean arm asserts `pass` with M-WAL-ORDER silent.
  - The TS-AFTER-APPEND arm asserts both M-WAL-ORDER and an O-1 failure.
  - `fail_of` no longer masks store errors.
- **N4 — not met in D17.** See F2–F4. The meta LOST cell asserts only
  `!served` on a failing round. The meta MISDIRECTED cell asserts a failing
  clean round as the expected result. The leaf LOST cell accepts
  `o1.is_err()`. None of the cells asserts `pass`. The other tests comply.
- **N5 — met.**
  - D10b's arms 4 and 5 serve through the R-taint, and O-3 (a) fails.
  - Arm 6 (`no_r_taint`) passes silently.
  - Forcing the R-taint call sites to false fails d10b.
  - Minor: D10b's doc item 1 still says the id check rejects the reuse
    ("different radix base"). That contradicts arm 4, where only the
    reference CRC discriminates.
- **N6 — met.** D7 is a real two-round zombie. Its clean round 2 passes. The
  mutant's round 2 replays the lined-up tail and fails O-1.
- **N7 — partly met.**
  - Pinned: `ckpt_open` (d12b) and the pass-fold helper.
  - Condition 3 is pinned only as a helper. Removing it where
    `exception_set_r` calls `r_slot_qualifies` (an empty hit set) is
    UNCAUGHT. The doc says the opposite.
  - **R4 is still unpinned.** Reverting the positional id to `header_id`
    in the store walkers, in amcheck, in the harness walks, or in all three
    at once leaves the whole suite green. The R4 pin's claim ("under the
    pre-R4 walkers … the page is served") is false: something other than
    the id check catches the planted page.
- **N8 — partly met, stated honestly.**
  - Present: LOST and MISDIRECTED on a tree leaf, the free-set root and the
    meta slot; the three map-leaf cells; and tree-page MISDIRECTED. The
    TORN cells on the leaf, the free set and the meta are absent, and the
    test says so.
  - But the grid runs on a broken base (F1/F2), so its results do not mean
    what the README says.
- **R7 leftover — met.** TS-AFTER-APPEND is caught by d18.
- **Round-1 and round-2 catchers still hold:** G2 → d5, G3 → 20/21 directed
  tests, CRC-SKIP hard-coded → d17. G1 was not re-run, because `src/sim/`
  is unchanged.

## New findings: REWORK
**F1. Store and harness: an acked SyncOn commit is lost silently (§12.2,
Work 4.1).**
- `wal_enqueue` has no record cap. Work 4.1 says "a record is at most
  16 KiB", and §12.2 says no record spans segments. Yet a 177,705-byte
  `Commit` record is placed across 64 KiB segments.
- Demonstration, using D17's first-half script with no checkpointer:
  1. The 420×400 B SyncOn commit is acked and visible: a second task's
     `WaitVisible(2)` returned.
  2. Then the crash: `TaskYield(2, 2)`.
  3. Recovery's frontier is 1, so the commit is lost.
  4. The round **passes**. `derive_written_acked` skips the record too
     (`off + len > seg`), so lo = hi = 1. The oracle never sees the loss.
- D14 also uses 25,425-byte records, over the cap. With a 16 KiB refusal
  in `wal_enqueue`, only d14 and d17 fail.
- Fix:
  - Refuse over-cap payloads loudly, and chunk the scripts.
  - The Work 2.1 derivation must not drop a `wal_commits` entry whose
    `[lsn, end)` bytes were written but cannot be parsed. Use the
    returned extents, or fail loudly.

**F2. The round-2 "open finding" is misdiagnosed.**
- D17's **faultless base round** already fails: `O-1(c) at c=1 … max ts 2`,
  lo = hi = 1, opened T = 2. This is F1, not a meta rollback.
- With the same 420 puts as `Chunked`, the base round passes (Ok(2)), and
  meta LOST and TORN become clean rollbacks that pass.
- Withdraw the README's finding, put the chunked script in place, and
  assert `pass` on those cells.

**F3. Spec silence, an ESCALATE item for P (§2.2): meta MISDIRECTED onto the
twin slot.**
- Open takes the highest valid `ckpt_id`, with no positional check that
  the meta in slot s satisfies s = 1 + (ckpt_id mod 2).
- What happens:
  1. Meta 2 lands in slot 2. The clean store opens it, and O-3 (a) fails:
     the read covered the fault's tainted landing range.
  2. Checkpoint 3 then overwrites slot 2, which holds the only valid meta.
     A torn write there destroys both metas, which breaks §2.2's own
     guarantee ("a torn meta write can only destroy the older of the
     two").
- D17 asserts this failing clean round as the expected outcome. The card
  says "any other silence … is an ESCALATE, never a local design choice".
- P must rule: either a positional meta check in §2.2, or an oracle ruling.
  The rework reports the case under RESULT item 8 and must not assert it as
  the test's expected outcome.

**F4. The card's open X-5c ruling case was reached and not reported.**
- On the chunked script, D17's leaf LOST cell gives this sequence:
  1. Recovery returns `Ok`.
  2. The extraction's scan hits XX001 (`det=[]`).
  3. O-1 (b) reports "scan read failed", and O-3 (d) fails.
- That is the Notes' question exactly: is O-1 required when recovery
  returns `Ok` alongside an XX001?
- The arm's `o1.is_err()` disjunct accepts the case silently. Report it
  under RESULT item 8, and do not pick a reading.

**F5. Rework fixes that are unpinned** (each leaves the suite, and my
1440-round sweep, green when reverted):
- R4, and condition 3 at its call site (see N7);
- the step-2 fixpoint re-scan;
- `fetch_page`'s `Stale` re-validation.

RESULT item 5 requires a pinned regression test for each. The `ckpt2:record`
re-validation is caught only incidentally, through D10b's REFCRC arm.

No ESCALATE verdict: F3 is routed to P as an item. The rest are local
defects.
