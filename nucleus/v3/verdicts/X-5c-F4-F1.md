Card: X-5c-F4-F1
Class: experiment
Card-gate: P
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: T3 review sub-agent, Claude Code (X-5c-F4-F1 P re-gate, 2026-11-04)
Date: 2026-11-04
Gate-exit: GATE EXIT=0
Commit: 5b93857d3376a0482bc81c3718e47075f425c0d9
Mutants: M-SZC-ABORTRESET -> szc_survives_the_sim_crash_unwind_into_the_record, M-SZC-NOCK -> szc_survives_the_sim_crash_unwind_into_the_record, M-SZC-ANY (at round_rec) -> szc_a_completed_interval_never_sets_the_record_flag, M-SZC-ANY (at szc_of) -> szc_a_completed_4f_interval_and_a_small_cut, M-W11-ANYSYNC -> x5_r38_w11_crash_walk_reaches_the_boundary, G-CWIN-NODIGEST -> cwin_is_a_digest_input_only_when_set, M-BASEDRIFT -> x5_r37_base_tuning_reproduces_1b0ce54e, M-Q11UNWIRED -> q11_stale_record_at_recycled_segment_start_counts, M-Q11ANY -> q11_mid_segment_stale_does_not_count, M-W12ALL -> x5_r38_w12_crash_drops_the_rename, M-CKPOISONb -> g8_poison_mid_checkpoint_ends_the_workload_quietly, M-CKPOISONa -> arm deleted (accepted, REWORK verdict ruling (iv)), G-SZC-NOFOLD -> szc_survives_the_sim_crash_unwind_into_the_record, G-SZC-ENDNORESET -> szc_a_completed_4f_interval_and_a_small_cut, G-W11-NORETIRE -> UNCAUGHT (behaviour moves; pin handed to X-5c-F4-F1-F1), G-SZC-ABORTNORESET -> UNCAUGHT (no effect in the file; pin handed to X-5c-F4-F1-F1), G-W11-NOCLOSE -> UNCAUGHT (no effect on two samples), G-W11-GT -> UNCAUGHT (no effect on two samples)
Followups: X-5c-F4-F1-F1

This verdict overwrites REWORK c1db3197. The card's Author line is P, and P's earlier ruling and REWORK verdict are the source of the rework list; this is a separate session, and the orchestrator requested the gate knowing this. REFRESH 6 and X-5c-F4-F1-F1 point into this file, so what they need is carried verbatim under "Carried from REWORK c1db3197".

## How the gate was run

- **Fresh gate.** The scaffold's `fresh-gate.sh`, extracted through the `--no-local` clone, scaffold 3fb03646a7e3739c7d2897578aeae67b42f01833, on the full commit, from `~/Workspace/neutron-v3`, as a background task. Lines: `fresh-gate: commit 5b93857d…`, `fresh-gate: base 3fb03646…`, `fresh-gate: checks passed (4507 files hashed in 1.26 s)`, `fresh-gate: 2 file(s) matched through their eol conversion`. Every STEP EXIT=0. Literal last line `GATE EXIT=0`. stderr empty. `$fg` removed.
- **Scope.** `git diff c1db3197..HEAD` touches only `nucleus/v3/spikes/e4-recovery/` (README, `harness.rs`, `ckpt.rs`, `mod.rs`, `wal.rs`, `tests/workload.rs`). No new non-test `unwrap`/`expect`/`panic`/`unsafe`. The only removed test line is a doc comment on the X5-R37 list. The worktree is clean and the tip is unchanged. The dead first runner left nothing: the four commits are the only ones after c1db3197.
- **Scratch.** All mutants, probes and variants ran on `git archive` exports under `~/.cache/nv3-review/regate-X-5c-F4-F1/`, never in the worktree. The diagnostic build adds a snapshot taken at the instant the crash fires, inside the scheduler's fire branch and `deliver_crash`, plus record fields; digest code is untouched. Run-hygiene note: the gate's wide runs are the gate's own, as in the REWORK gate.

## REWORK items: all closed

| Item | Result |
|---|---|
| 1. `ckpt.rs` abort path | Closed. Reset only when `!matches!(e, Fail::Sim(_))`. Pins on seeds 41 r2 and 303 r2 and 660 r2; all three named mutants fail a pin. See finding 1 for the remainder. |
| 2. `wal.rs` W11 | Closed. `st.wal.written >= b` added, doc corrected, walk pin added, `E4_W11_DIAG` gone. 276 still hits. The `w11_open` field in `harness.rs` is a small observable serving the required pin, never a digest input; accepted. |
| 3. `cwin` digest pin | Closed. |
| 4. README | Closed, one markup nit (finding 4). Four-seed list re-derived from the files: exact. MP root cause, probe and finding-3 correction present. |

## Measurements re-run by the gate

**Full 10k at the tip** (`run --seeds 0..10000 --threads 12`, diagnostic build), against F4's file (sha `bec7056c…`):
- `E4 SEEDS=10000 O1_FAIL=2365 O2_FAIL=35 O3_FAIL=72 DEADLOCK=0 PANIC=0 O1F_FAIL=2365 MONITOR=0` and `FAILING_SEEDS=94 FAILING_ROUNDS=76 TWIN_FAILING_ROUNDS=20 DEFECT_SEEDS=0`: identical to F4's.
- **Digest set: exactly 4, 8422, 9226, 9606 moved**, on the q11 flag alone: 4 r0, 8422 r1 and r2, 9226 r0 and r1, 9606 r0. Same round counts everywhere. All 53 literals in `x5_r37_base_tuning_reproduces_1b0ce54e` match the file, including 41 and 303; seed 4's differs from F4's by its own listed movement.
- **`ckpt_closed_dirty` is alive:** non-zero in 7,972 of 15,386 rounds, ≥ 4·F in 91. It was 0 in all rounds at the REWORK tip.
- **`szc`:** 149 rounds (C 145, MP 1, MW 3): 58 through the cut arm, 91 through the checkpoint arm. It was 58 at the REWORK tip. With a recovered state: 147 (C 145, MP 0, MW 2).

**`szc` against the A+ arm, compared per round with the at-fire snapshot** (`fired` and either `dirty_now ≥ 4·F` at the fire, or `ckpt_open` at the fire with its fold ≥ 4·F): **15,385 of 15,386 agree; one undercounts** (finding 1). No stale value: an observable with no open checkpoint occurs 0 times, at the fire or in `obs`. All 67 poison ends have `szc` false. `szc` is never true where the at-fire state says false.

**W11 and W12 probes, reproduced exactly** (probe, not evidence). Shares 100/100 over 0..200: 304 rounds, Q11 8, Q12 5, Q5a 3, picks w11 9 / w12 18, `w11-` 5, W11 yield 4/9, W12 yield 5/18: every RESULT number. `w11_share` 1000 over 0..200: W11 yield 45/79, `w11-` 72. Per-pick checks, which the single-seed pin cannot give: the window is open at the fire in 79 of 79 picks, and every pick that recovered a state has its walk at or past `b` (73 of 73). The ruling's "so the walk reaches `b`" holds across the driver's own picks.

**REFRESH 6's table still stands.** k5's knobs at this tip against the REWORK gate's scratch build: 0 of 100 digests moved. The table's numbers were taken with the fixes this tip now carries.

## Findings

1. **Non-blocking, handed to X-5c-F4-F1-F1 step 0: `szc` undercounts by one, from a pre-existing swallowed crash.** Seed 2376 r2, `uniform@y:17768`: at the fire the checkpoint is open with a fold of 235 ≥ 4·F = 184, yet `szc` is false; `obs` shows closed 9, open 0. In all, 15 rounds of 15,386 (all uniform) have the observable move after the fire. The trace: `wal_recycle` tests `cx.env().rename(…).map_err(Fail::Sim).is_ok()` (`src/store/wal.rs:1108`, there since X-5b B1, unchanged by this card). A crash delivered at the rename's yield therefore reads as "not renamed", so `wal_recycle` returns `Ok`; `After6` and `ckpt:end` run after the crash; the checkpointer's next poll takes its final checkpoint; and that `ckpt:begin` folds again before the gate's yield unwinds it. That also clears `ckpt_splits`/`ckpt_merges` and sets the Q4 position code to `Before0`, so X-5c-F4-F1-F1's in-window count needs it fixed first.
   - **The direction is conservative:** a false negative only, so it cannot pass a quota it should not. It sits outside this card's Limits (store observables only), and the rework did not cause it, so it does not block this card. It must land before F4's file, because MP ≥ 100 may be tight.
   - **A plain `?` is wrong; measured.** The same call returns `Err` in a world that has not crashed: "`to` exists" is routine (111 occurrences over 0..200, counted in the plain-`?` variant), and the store relies on the quiet skip. A plain `?` moved 67 of 200 digests, and 3 of the pinned seeds in 0..200.
   - **The fix below was run over the full 10k in scratch:** `szc` equals the at-fire arm in 15,386 of 15,386, 0 rounds move after the fire, **0 digests move**, and the only non-diagnostic record change in the file is seed 2376 r2's `szc`. Counts become: `szc` 150, of which C 146, MP 1, MW 3; checkpoint arm 92; with a state 148.
2. **Non-blocking, handed to X-5c-F4-F1-F1 step 0: two ruling clauses have no failing test.**
   - **G-W11-NORETIRE** (`recycled_unsynced` never retired, breaking ruling (i)'s "no completed `sync_data` since"): passes all 227 tests while moving 9 of 200 digests at share 1000 (picks 79 → 88). Seed 30 r0 reads `w11-` at the tip and `w11` under the mutant.
   - **G-SZC-ABORTNORESET** (the non-crash reset removed): passes all 227. It changed nothing across the file's 67 poison rounds, so it is latent, but REFRESH 6 tells F4 to copy this rule for `ckpt_unflipped`.
3. **No action: two mutants measured equivalent.** G-W11-NOCLOSE and G-W11-GT pass all 227 and move 0 digests at share 1000 over 0..200 and on k5 over 0..100: one window per pass there, and `written == b` never decides. Nothing is required. If F4 sees several windows in a pass, the close clause becomes load-bearing and a pin is due.
4. **Nit: ruling (i) in the README is word-for-word, with one backtick moved** (`≥ b` in backticks in the ruling, ≥ `b` in the README, and the same in the `wal.rs` doc). The request asked for verbatim, so it is reported; the meaning is unchanged. X-5c-F4-F1-F1's README step fixes it.
5. **Noted: X-5c-F4-F1-F1's decision rule could escalate falsely** (my own text). See the card change below.
6. **Finding 7 of the REWORK verdict did not recur:** `ack_prefix_two_batches_in_flight` passed in all six full-suite mutant runs at load about 30, with the gate's 12-thread campaign running. The worker's non-reproduction stands. Earlier failures are not explained by this; treat it as a load-sensitive test with no card.

M-W12ALL and M-CKPOISONb as "minimal semantic equivalents": accepted; they are the same edits this gate used in c1db3197.

## Verdict reasoning

Every REWORK item is closed, the digest set holds, the owner's arm is measured live, and the one exception is a pre-existing conservative undercount with a tested fix, outside this card's Limits. Hence MERGE-WITH-FOLLOWUPS. No threshold moved and none is proposed.

## X-5c-F4-F1-F1 card changes (paste verbatim; it stays Status: waiting until this verdict's commit is on `card/v3-X-5c-F4-F1`, then ready)

Replace Limit 2 and the `Do not` line "Change the store, the oracle, the model, the sim, K or R." with:

> 2. **Store:** step 0's one change in `wal_recycle`, and nothing else.

> - Change the store beyond step 0, the oracle, the model, the sim, K or R.

Insert before step 1 of Work:

> 0. **Close the re-gate's findings (verdict X-5c-F4-F1 MERGE-WITH-FOLLOWUPS, findings 1, 2 and 4). Three commits, each naming its finding, before any probe.**
>    - **(a) `wal_recycle` must not swallow a delivered crash.** In `src/store/wal.rs`, replace `if cx.env().rename(&name, &to_name).map_err(Fail::Sim).is_ok() {` with:
>      ```rust
>      let renamed = cx.env().rename(&name, &to_name).map_err(Fail::Sim);
>      if let Err(e) = &renamed
>          && cx.with("wal_recycle:crashed", |c| Ok(c.sched_mut().crashed))?
>      {
>          return Err(e.clone());
>      }
>      if renamed.is_ok() {
>      ```
>      **Do not use a plain `?`.** The rename also fails in an uncrashed world ("`to` exists"), and the store relies on that quiet skip.
>      Pin `szc_rename_crash_keeps_the_open_fold`: `run_seed_with(2376, None, &Tuning::BASE)` r2 has crash `uniform@y:17768` and `szc` true. Add `(2376, 3116539881)` to the X5-R37 list, from F4's file. No digest moves: the gate ran this change over the whole 10k, and only seed 2376 r2's `szc` changed.
>    - **(b) Pin `x5_r38_w11_needs_no_completed_sync_since`:** BASE with `w11_share` 1000, seed 30 r0 has `cwin` `Some("w11-")`.
>    - **(c) Pin `szc_non_crash_abort_resets_the_fold`:** a literal schedule in which a checkpoint with a non-zero fold fails with a non-`Sim` error (the `g8_poison_mid_checkpoint_ends_the_workload_quietly` shape or `wal_io_fail`) ends with `ckpt_closed_dirty` 0. If no such schedule exists, show why with a literal schedule and stop for P; do not delete the reset.
>    - **README:** move the one backtick so ruling (i) reads `≥ b` exactly as in the verdict, in the README and the `wal.rs` doc.

Add to the mutant table:

> | M-RENAME-SWALLOW: `is_ok()` restored | `szc_rename_crash_keeps_the_open_fold` |
> | M-RENAME-ANYERR: a plain `?` on the rename | `x5_r37_base_tuning_reproduces_1b0ce54e` |
> | G-W11-NORETIRE: the `recycled_unsynced.retain` line removed | `x5_r38_w11_needs_no_completed_sync_since` |
> | G-SZC-ABORTNORESET: the non-crash reset removed | `szc_non_crash_abort_resets_the_fold` |

In step 4's "Met" bullet, replace "with no Q5a, Q7 or Q18 falling below its row" with:

> with no Q5a, Q7 or Q18 below the lower of its row and BASE's own count × 50 on the same seeds (from F4's file over 0..200: Q5a 3, Q7 5, Q18 20; so floors of 150, 200 and 1,000)

*Why:* BASE itself reads Q5a 3 (150 per 10k against a row of 200) on these 200 seeds, while the whole file reads 288. As written, the rule would escalate to the owner on a sampling artefact before Q4 was even judged. This changes a probe's stop rule, fixed before any probe. **No quota row changes:** F4's whole file is still judged at every row. In the same bullet, measure BASE's wall in the same session as the setting's.

In "Read", `verdicts/X-5c-F4-F1.md`'s finding 3 and probe table are now under "Carried from REWORK c1db3197".

## X-5c-F4 card changes (paste as REFRESH 7, after REFRESH 6, overriding it where they differ)

> **REFRESH 7 (P re-gate X-5c-F4-F1, MERGE-WITH-FOLLOWUPS, 2026-11-04).**
> - **Status.** X-5c-F4-F1's rework has passed. F4 is blocked only on X-5c-F4-F1-F1: fast-forward `card/v3-X-5c-F4` to **its** gated tip, which carries the `wal_recycle` crash fix and the pins, then relaunch `--keep`. F4 must not start from this card's tip alone.
> - **"This verdict" in REFRESH 6** now means the "Carried from REWORK c1db3197" section of `verdicts/X-5c-F4-F1.md`: ruling (i), the probe table and the seeds. The text is unchanged. Every REFRESH 6 instruction stands; the `ckpt_unflipped` rule was re-checked against the code at this tip.
> - **The baseline for X5-R39's rows (BASE, whole file, with the crash fix; measured by the gate, not evidence for FINAL):** `szq` 148 (C 146, MP 0, MW 2). Checkpoint arm with a state, 90 rounds, by hook: After0 15, Mid2 11, After2 11, After4 10, After5 9, After3 8, Before0 8, Mid6 6, After6 5, After1 3, uniform 4. So SZI starts near 60 against 300.
> - **Q11 and the rename skip.** A failed recycling rename ("`to` exists") is routine and binds no window. If Q11 falls short, report how often it happens on FINAL before escalating. Do not change it.

## Carried from REWORK c1db3197 (verbatim; REFRESH 6 and X-5c-F4-F1-F1 refer here)

**Probes (0..N, labelled, never evidence).** Abort fix and diagnostics applied; rows marked `+W11` also have finding 2's fix. `szq`/`szi` are computed from the diagnostics per REFRESH 5's definitions. Wall × (10000/N) on a host with other load: BASE is 15.6 min/10k.

| Set | N | Knobs (over BASE) | Min/10k | SZC | MP | MW | SZI | Q4 | Q5a | Q7 | Q8 | Q11 | Q12 | Q18 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| base | 400 | none | 15.6 | 150 | 0 | 0 | 50 | 0 | 200 | 325 | 325 | 25 | 0 | 1150 |
| k4 +W11 | 800 | chunked 700–900, value_lo 280, gc 6–8, every 8–12, drop 1/2, burst 600/4, shares .10/.10 | 39 | 1525 | **12** | 125 | 512 | 62 | 112 | 50 | 550 | 312 | 188 | 662 |
| k5 +W11 | 800 | chunked 900–1200, budget 20–31, value_lo 280, gc 8, every 8–12, drop 1/2, shares .10/.10 | 50 | 3200 | **88** | 225 | 888 | 12 | 288 | 62 | 525 | 112 | 212 | 488 |
| k1 | 400 | k5's chunked and budget, gc 12, every 4–8, burst 600/8, shares .20/.20 | 61 | 2475 | **175** | 175 | 450 | 25 | 625 | 225 | 275 | 0 | 325 | 3475 |
| k2 | 400 | k1 with every 12–19 | 82 | 3400 | **100** | 225 | 1050 | 25 | 175 | 25 | 500 | 25 | 450 | 250 |
| k3 | 400 | k1 with chunked 1200–1500, budget 30–45, every 16–24 | killed: over 54 min for 400 seeds | | | | | | | | | | | |

All counts × (10000/N). MP rests on 1–7 raw hits per row, so read it as ±40% at best.

**Window yields, per pick.** W11 as built, `w11_share` 1000, BASE, 0..200: **0 of 150 picks hit Q11**. W11 with finding 2's fix, same seeds: **45 of 79 (57%)**; 72 empty fallbacks. On k4/k5, 18/44 and 6/24. W12: 46 of 151 at BASE; 15/67 and 17/65 on k4/k5, consistent with the worker's prefix explanation.

**Finding 3 of c1db3197.**

3. **P's error, not the worker's: ESCALATE 4's refutation is withdrawn.** The "Q4 132 of 309" in ruling nv3-X-5c-F4-esc-1 is q[3], **Q5-partial**. In P's saved file (`X-5c-F4-esc1-q4probe-0..200.json`) q[0] is 0 and q[3] is 132; its 309 rounds and q[19] = 49 (Q18) match the ruling's other figures. P's patch re-run here: Q4 0 of 309. The worker's burst knobs reproduce that: 600/8 gives 0 of 309, 900/16 gives 0 of 308. In-window hook rounds at 900/16: 88, of which 5 have a split and 0 a merge. Probes top out at 62 per 10k against 1,000. REFRESH 4's Q4 bullet is wrong and F4 cannot meet Q4 with it. See follow-up X-5c-F4-F1-F1.

**Finding 8 of c1db3197.**

8. **New-class notice for F4 step 4 (probes only).** Class C `O-3(b): XX001` with O-1 failures appear on retuned knobs: k4 seed 221 r1 (After0, AmcheckIi), k4 seed 370 r1 (Open), k5 seed 423 r0 (After0, AmcheckIi). Seeds 221 and 423 reproduce with shares at 0; 370 does not. These are not part of this card, and X5-R16 applies. They may be real.

### Rulings of c1db3197

**(i) W11.** The worker's reading is accepted in substance: `durable == b` never occurs, because `durable` advances only after the batch's sync loop and the batch already holds `b`'s record (seed 4: durable 261147, written 263952, `b` 262144). The implementation is not accepted (finding 2). This text replaces step 3's W11 bullet and X5-R38's W11 clause:

> **W11.** Let `b` be a segment boundary whose segment a `wal_recycle` rename bound and which has had no completed `sync_data` since. W11 opens when a `sync_data` of the segment below `b` completes with `wal.written ≥ b`: that segment's whole extent, filler or boundary-ending record included, is then durable, so the walk reaches `b`. It closes when a `sync_data` of a segment at or above `b` completes. "Untouched" means the durable image: a write at `≥ b` may be issued and still volatile, and F-CRASH decides whether it survives. Q11 then requires that `b`'s first sector does not survive with new content.

**(ii) `szc`.** Not as shipped (finding 1). With the fix, it measures the owner's quota arm exactly, from the fold to the checkpoint's end. Nested recoveries and the harness checkpoint are excluded because `obs` is taken when the tasks end; a round's `dirty_now` starts clear because the harness checkpoint folded it. Poison ends count as the round's crash, and F4 reports that count as `INFO` (0 of 67 at the tip). REFRESH 5 otherwise suffices, once corrected as in (v).

**(iii) MP.** F4's retune raises it, since r0 can now reach `szq` (k5: 4 hits). The worker's "workload property" is half the cause; the larger half is that `szq` excludes refused rounds and 81% of MP rounds refuse (409 of 2,190 hold a state). Measured MP per 10k: 12 at 39 min, 88 at 50 min, 175 at 61 min, 100 at 82 min. **MP ≥ 100 is shown only at or past the 40-minute budget, on 1–7 raw hits; it is neither proven reachable nor proven unreachable inside it.** No threshold moves. **Owner item 1**, below.

**(iv) M-CKPOISONa's deletion: accepted.** The reasoning holds (`cx.with` takes no yield; `ckpt:begin` refuses before any hook; `checkpoint_poison_end` is the same end), and the full-file check is stronger than the worker's single schedule: none of the 67 poison rounds' seeds moved. M-CKPOISONb was re-run and is caught.

**(v) F4 cannot relaunch.** It waits for this card's rework gate and X-5c-F4-F1-F1. Paste the text under "F4 card changes".

### REFRESH 6 as issued (already pasted in the F4 card)

**REFRESH 6 (P gate X-5c-F4-F1, REWORK, 2026-11-04).**
- **Status.** Blocked until X-5c-F4-F1's rework passes its P gate and `card/v3-X-5c-F4` is fast-forwarded to it, **and** X-5c-F4-F1-F1 is done. Then relaunch `--keep`.
- **REFRESH 5 corrections.** "F4-F1 needs no change" is withdrawn. `ckpt_unflipped`: replace "Also cleared wherever `ckpt_open` is cleared" with "cleared on `ckpt:end` and on the abort path of a non-crash failure; **never on a sim-crash unwind**, because `obs` is read after the unwind". Add mutant **M-SZI-ABORTRESET** (cleared on every abort), which the `After4` pin must fail. Report poison-end `szq` as `INFO`.
- **REFRESH 4's Q4 bullet is withdrawn.** P's "132 of 309" was Q5-partial; Q4 was 0. Use X-5c-F4-F1-F1's result. Q4's threshold is unchanged.
- **W11.** X5-R38's W11 clause is the text in this verdict's ruling (i).
- **Probes (not evidence): start here.** See this verdict's table. k5 is nearest on SIZING, MP short at 88; Q7 62 and Q18 488 there are short, and k1 shows gc 12 and every 4–8 restore them at more wall. Do not use k3's region: over 54 min per 400 seeds. Shares of 0.10 gave Q11 112–312 and Q12 188–212 with the fixed window; 0.05 is untested and may fall short on k5's knobs.
- **Unreachable, restated.** If any row, MP above all, cannot be met at ≤ 40 min with every other quota held, ESCALATE with the probe numbers and raw counts. Never lower a threshold, narrow a definition or exceed the budget unasked.
- **Step 4 notice.** Class C XX001 on retuned knobs (seeds 221, 423, 370 in this verdict) gets X5-R16's treatment if it recurs on FINAL.
- **Pin.** Add `x5_r37_final_stays_in_card_ranges`: FINAL stays inside 5.1 and 5.2's ranges, since the existing knob-range tests derive under BASE.

### Owner items (both still open, unchanged)

1. **MP ≥ 100, the refused-round exclusion and the 40-minute budget may not all fit.** Measured: 88 at 50 min, 12 at 39 min, 175 at 61 min. Nothing is changed. F4 tries first and escalates with numbers if it falls short. The thresholds are the owner's, and X5-R17 calls the budget a time budget, not a correctness rule. The owner needs to choose only if F4 escalates.
2. **Q4 ≥ 1,000: the evidence given before was wrong.** The ruling that told the worker Q4 was reachable misread a column. Measured, with REFRESH 4's knobs: at most 62 per 10k. X-5c-F4-F1-F1 tries a pre-registered alignment first. If it cannot reach 1,000, this returns to the owner under the standing rule, and nothing is lowered.

## Follow-ups

**X-5c-F4-F1-F1** (the existing card, `cards/X-5c-F4-F1-F1.local.md`, changed as above). Step 0 closes this verdict's findings 1, 2 and 4: the `wal_recycle` crash fix and three pins. Then Q4's reachability: separate per-round split and merge counts in the record, a pre-registered X5-R40 script alignment that runs burst GC inside an open checkpoint, three settings over 0..200 at most, and a fixed decision rule, with an unmet rule going to the owner.
