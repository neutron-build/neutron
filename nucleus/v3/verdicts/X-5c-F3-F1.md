Card: X-5c-F3-F1
Class: experiment
Card-gate: P
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: T3 review sub-agent, Claude Code (X-5c-F3-F1 P gate, 2026-11-02)
Date: 2026-11-02
Gate-exit: GATE EXIT=0
Commit: 62d4e7396f544c17d6d7c1f93875cd5b00275699
Mutants: M-WALa concurrent take + max accounting -> cdiv_syncon_ack_never_crosses_an_unwritten_batch, M-SEGa valid_end advance removed -> segend_valid_end_survives_a_missing_next_segment, G-2 `if !inside_op` -> g8_read_never_poisons_gated_op_does, M-TWa Err-arm twin extend removed -> twin_supply_carries_a_failed_recovery_reads, M-CXXc dir pos-1 check removed -> cxx_dir_reput_never_appends_a_duplicate, M-R32c K exclusion dropped -> mf26_kept_slot_is_not_damaged_referenced_fails, G-1 replay probe `Err(_) => break 'outer` -> UNCAUGHT (full release suite green; equivalent mutant, accepted below), (own) OWN-1 r30_predicates drops meta_rollback -> UNCAUGHT, (own) OWN-2 r30_predicates drops the uncovered-accept condition -> UNCAUGHT, (own) OWN-3 X5-R32 D_read ignores the page-file inode -> UNCAUGHT
Followups: X-5c-F3-F1-F1, X-5c-F3-F1-F2, X-5c-F3-F1-F3, X-5c-F3-F1-F4, X-5c-F3-F1-F5

## How the gate was run

- **Fresh checkout.** `git -C ~/Workspace/neutron-v3 -c core.hooksPath=/dev/null worktree add --detach /tmp/gate-X-5c-F3-F1 62d4e739…`. Common git dir `~/Workspace/neutron/.git`: no non-sample hooks, no `filter.*`/`core.hooksPath`/`core.attributesfile`/`include` config, no `info/attributes`. Root `.gitattributes` is line-endings only and has no filter. `git write-tree` equals `62d4e739^{tree}`, and `git status` is clean. All 285 `nucleus/v3` files match their commit blobs byte for byte (sha256).
- **Gate.** `cd /tmp/gate-X-5c-F3-F1/nucleus/v3 && CARGO_TARGET_DIR=$HOME/.cache/nv3-target/gate-X-5c-F3-F1 bash scripts/gate.sh` was run in the foreground, rc=0. STEP test/clippy/fmt/deny/wasm32/core-paths-selftest/enforcement-tests all EXIT=0. Literal last line: `GATE EXIT=0`.
- **Mutants** were applied in the gate checkout and run on the release suite (filtered to the pin; G-1 ran on the whole suite). Each was reverted with `git checkout`, and `git status` was clean before the checkout was removed.
- **Re-measured** (single seeds at the tip under `ulimit -v 25165824`): 20, 21, 1057, 1167, 1628 and 6149. All six seed records are **byte-identical** to the Acceptance JSON. At e6ebbdda (step 1 only) I ran the 13 moved X5-R32 seeds plus control 1167. No 10k campaign and no verify were run.

## Scope

`git diff 038bb9cc...62d4e739` touches only `nucleus/v3/spikes/e4-recovery/` (15 files). Frozen paths: `derive_written_acked`'s body is byte-identical to the base (sha256 of the extracted fn), `src/model.rs` is unchanged, and `src/sim/` is unchanged. The oracle (`src/oracle.rs`, `tests/oracle.rs`) changes in exactly one commit, e34b75ef, whose subject names X5-R32. `results/campaign.json` is touched only in step 0 (7b709ccb). `tests/directed.rs` and `tests/oracle.rs` only gain lines (D4 untouched).

`tests/workload.rs` re-points the seeds of three existing pins: r20 38/137→48/308, r28 6157→5808, g8 1108→7. Their assertions are unchanged. I restored the base file at the tip: all three old seeds fail **only at their shape preconditions** (r20:729 "lost its R set", r28:854 "rollback condition must hold", g8:995 `rounds[1]` absent), not at the property under test. So this is shape maintenance, not a test bent to fit a fix. Accepted.

## Findings by check

1. **WAL contiguous ack.** Shape (b): one batch in flight (`WalState.in_flight`), and a Busy taker parks on `WalAcks`. The park is lost-wakeup-free because the Busy check and `park_locked` run while this task holds the baton. Release and wake happen on every exit. `written` advances only when the batch starts at `written`; any other start is a loud `Fail::Invalid`. `durable` stays within [start, end]. Seed 21 r0 passes (lo=13, frontier=14), re-measured byte-identical. D4 is untouched and green. M-WALa is caught.
   - The step-1 (e) cross-check (55 of 4,656 commits disagree) was correctly escalated, not patched. See F1: I found the derivation is **per-inode and `max`-based**. `returned_end` is the max write end per file, durable is the max over syncs, and acked is judged per record. That is not S-1-F3's I-ACK-PREFIX `durable_end` (the contiguous stream prefix), and it ignores G1's directory durability.
   - 20 r2 re-measured: ts=64 is derived-acked, its own wait never returned, and frontier=63 < lo=64. Whether the bytes below it were a durable prefix is **not yet established**. If they were, this is a store loss of a durable record (I-REDO-COMPLETE). If not, the oracle's `lo` is wrong. The final file has **37** `frontier < lo` rounds (36 (b), 1 (a)), down from 93.
2. **SegmentEnd valid end.** `valid_end = seg + WAL_SEG` on an accepted SegmentEnd. M-SEGa is caught. 6149 r1 is not served (re-measured: MW-ORPHAN's O-1(c)+O-3(d)).
3. **G-1 / G-2.** G-2 is caught by the new pin. G-1 is UNCAUGHT on the full suite, as the worker demonstrated.
   - **P ruling here: G-1 and G-1b are accepted as equivalent mutants.** `Sched::acquire`/`park` return `Crashed` for every later step once `core.crashed` is set, so the swallowed probe's crash re-surfaces at the next env call with the same `SimFail`, and no env write can land in between.
   - The pin stays: it guards any swallow that yields `Ok` or a different error. This closes the worker's G-1 escalation.
4. **X5-R32.** The oracle diff matches (b) line for line: H, D, D_ii, D_read, the sites with Read23, the explained map, the four-way arm, `r32`, `no_r32`, and the six cases with literal-text asserts. The harness supply matches (a). M-R32c is caught.
   - **The 17-round rule was applied correctly, but recorded incompletely.**
     - 4 rounds have r32=true and pass: 1167, 4348, 6047 and 7367 r1.
     - 13 rounds moved, and the README omits the commit. **I attribute all 13 to e6ebbdda:** at e6ebbdda, 7, 3307 and 8837 r1 pass with no O-3; 7748 r1 fails O-3(e)(ii); and 1628, 2047, 2307, 3887, 4707, 6767, 9168, 9508 and 9928 lose the named round because the chain stops after an r0/r1 recovery-Err detection. Control 1167 r1 still shows O-3(c) there.
     - No round keeps the shape with r32=false. D was never widened.
   - The 5 MW rounds moved or pass. The final file has **0** O-3(c) rounds, and all 13 r32 rounds are class MP: 167, 788, 1167, 2347, 3547, 4348, 4587, 5387, 6047, 7347, 7367, 8368 and 9347, all r1.
5. **X5-R30.** `r30_predicates` is literal to the card. M-R30a–e are caught, per the worker and spot-checked via the pins. But **OWN-1 (meta_rollback) and OWN-2 (uncovered) are UNCAUGHT** (F3). This does not affect the judgment: all 7 served rounds in the final file have `mrb=true`.
6. **Root-cause work.**
   - C-XX sub-shape 1: the directory duplicate re-put (`tree::dir_put_locked`, §6.8) is a correct fix, with M-CXXc caught.
   - Escalated with evidence: sub-shape 2 (a stale parent made during chunked replay, 151 r1); 150 r1 not reached; the C-DIV per-key cut not built (X5-R19 valve, 18:34→18:50, attempt end); and the §12.2/§12.5 exact-boundary silence.
   - Nothing was hidden: no oracle loosening beyond X5-R32, no K/R/model/sim change, no seed filter.
   - Caveat: `E4_CXX_DIAG` runs amcheck inside recovery, which issues env calls and so may move later crash points. The 151 r1 sub-shape 2 trace must be re-confirmed against the plain run (F2).
   - **C-XX went up from 64 to 76 rounds** in the Acceptance file.
7. **Provenance.**
   - `.prov`: commit=62d4e739 (= tip), status_lines=0, exit=0, verify_exit=0, verify_head=tip.
   - `sha256sum` = `json_sha256` = f3a1e605…a1af17.
   - An earlier Acceptance run at a96620bd (code-identical: 62d4e739 adds 8 README lines) produced the same sha256.
   - The record check passed.
   - README nit: RESULT 5 says `prov commit=a96620bd` (F5).

**S-1-F3 consistency (draft 2).**
- (a) is consistent with I-ACK-PREFIX: serializing is a stricter, legal writer. But the draft says "more than one batch may be in flight; the rule is stated on the frontiers", while F3-F1's comments call one-in-flight "§12.4's serialized writer". F3-F3 must port the frontier rule, not the serialization (F4).
- (b) matches W4 (iii) word for word.
- G-2's pin is §9.2's negative half.
- Two conflicts are flagged to S-1-F3 (F4): `derive_written_acked` does not compute the draft's `durable_end`; and the draft does not cover a record that ends exactly at a segment boundary with no room for SegmentEnd (the walk exits at `off == WAL_SEG` and orphans the next segment).

## Refreshed counts (Acceptance file f3a1e605…, 62d4e739)

`E4 SEEDS=10000 O1_FAIL=2617 O2_FAIL=54 O3_FAIL=542 DEADLOCK=0 PANIC=0 O1F_FAIL=2627 MONITOR=0` / `E4 FOLD FAILING_SEEDS=901 FAILING_ROUNDS=860 TWIN_FAILING_ROUNDS=68 DEFECT_SEEDS=0` (F3: 955/914/71).

**RESULT item 6 (post-data amendments, for X-5c's RESULT):**
- X5-R30 (c) moves 7 served MP rounds, all r0, each with (skx∨smx)∧pass_na: skx 1057, 1897, 4718; smx 2927, 7238, 7528, 8728. This gives **853 failing rounds / 894 failing seeds**.
- X5-R32 moved the 13 r32 rounds listed in item 4.

Failing rounds after X5-R30:
- **C 390:** C-XX 76 (Open 22, AmcheckIi 54); C-DIV 314 ((b) 151 of which 36 f<lo, (c) 99, (d) 62, (a) 2 of which 1 f<lo; by round r0 19 / r1 28 / r2 267).
- **MW 453:** MW-ORPHAN 445, other 8.
- **MP 10:** O-3(e)(ii) 6; O-1(b) 3 (3607 r1, 4818 r0, 5227 r1); O-1(a) 1 (5928 r1).
- Twins: 68 (MP 24, MW 44).

Quotas missed (unjudged, --no-quotas): Q4 0/1000, Q7 181/200, Q8 83/100, Q11 0/100, Q12 2/100, SIZING 15052.

## What remains before X-5c-F4

X-5c-F3-F3 (S-1-F3's design in the prototype) needs S-1-F3 draft 2 re-gated with F4's two items folded in. F1's ruling must land before F4's triage reads `lo`, because the 37 f<lo rounds hinge on it. F2 may run in parallel with F3-F3, but its fixes re-run the 10k (X5-R3).

## Follow-ups

**X-5c-F3-F1-F1 (P ruling: derive_written_acked vs I-ACK-PREFIX).** The cross-check found 55 of 4,656 commits disagreeing.
- Derived-only rounds: 20 r2 and 115 r1, plus probably most of the final file's 37 `frontier < lo` rounds.
- Client-only rounds: 23, where a same-round segment recycle masks early records.

The frozen derivation is per-inode and `max`-based, and blind to the stream prefix and to G1 dirsync. Trace 20 r2: is ts=64 (lsn 661312, segment 10) behind a non-durable prefix byte or directory entry? If so, P amends the derivation to S-1-F3's `durable_end`, pre-registered and post-data under RESULT item 6. If ts=64 was a durable prefix record that recovery skipped, it is a class C I-REDO-COMPLETE break: ESCALATE as a store bug.

**X-5c-F3-F1-F2 (C-XX / C-DIV root-cause, stacked; experiment).**
- C-XX: 76 rounds. 151 r1 sub-shape 2 is a mapped inner page naming an unmapped child after chunked replay; re-confirm it without `E4_CXX_DIAG`'s perturbation, or make that diag's amcheck non-yielding. 150 r1 is an Open XX001 with two nested crashes.
- C-DIV: the per-key model-vs-recovered cut for one each of (b), (c) and (d), with the r2 skew at 267 of 314. Same budget valve.

**X-5c-F3-F1-F3 (P-authored test card: the predicate and arm pins).**
- Add must-false pins to `r30_predicate_conditions_are_load_bearing` for meta_slot_fault, meta_rollback and uncovered accepts (OWN-1 and OWN-2 must fail them).
- Add oracle case mf29, P text required since the oracle tests are frozen: a non-page-file read overlapping `s·4096` must not explain `Open` (OWN-3 must fail it).

**X-5c-F3-F1-F4 (fold into S-1-F3's re-gate).**
- (i) The §12.2/§12.5 exact-boundary silence: a record ending at `WAL_SEG` leaves no room for SegmentEnd, and the walk orphans the next segment. Rule it: forbid a zero-room tail, or define the hop.
- (ii) State that I-ACK-PREFIX's `durable_end` is what E4's `lo` must be derived from (ties to F1).
- (iii) F3-F3 ports the frontier rule, not F3-F1's one-batch serialization.

**X-5c-F3-F1-F5 (mechanical README record fixes; S).**
- The 17-round table gets "moved by e6ebbdda" with each new outcome, as attributed above.
- RESULT 5's prov commit becomes 62d4e739 (code-identical to a96620bd, same sha256).
- The step-7/8 diag artifacts are renamed or copied to `X-5c-F3-F1.<step>.*`.
