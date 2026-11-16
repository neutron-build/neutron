Card: X-5c-F4-F2
Class: experiment
Card-gate: P
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: T3 review sub-agent, Claude Code (6e204c43-2320-42c2-95a3-3f79c1049226, 2026-11-10)
Date: 2026-11-10
Gate-exit: GATE EXIT=0
Commit: c9c6e8b9750d2a73d4bb11b1dfb7f41dac0c4ebe
Mutants: M-SZQ-REFUSED -> x5_r39_a_refused_round_is_szc_but_never_szq, M-SZI-BEFORE0 -> x5_r39_before0_crash_never_sets_szi, M-SZI-AFTER5 -> x5_r39_after5_crash_never_sets_szi, M-SZI-ABORTRESET -> x5_r39_after4_crash_with_a_4f_fold_is_szi, M-SZ-EXIT -> x5_r39_a_per_seed_sizing_miss_is_not_an_exit_input, M-SZ-NOFLOOR -> x5_r39_row_floors_at_the_edge, M-SZ-NOREPRO -> x5_r39_flipped_szi_fails_reproduction, M-MP-MAINEXIT -> x5_r45_szc_mp_99_in_each_mode, M-MP-NOEXIT -> x5_r45_szc_mp_99_in_each_mode, M-MP-CLASS -> x5_r45_seed_set_checks_short_over_wrong_class, M-MP-TUNING -> x5_r44_tuning_id_is_mp_pressure_for_that_set_only, M-HELD-QUOTA -> x5_r45_held_out_exits_zero_with_every_quota_short, M-HELD-RANGE -> x5_r45_seed_set_checks_short_over_wrong_class, M-SET-MISMATCH -> x5_r45_the_recorded_set_gates_verify, M-REFUSAL-ALL -> x5_r46_mp_refusal_counts_class_mp_only, M-PROBE-RANGE -> x5_r44_probe_forms_seed_lists_and_refusals, M-PROBE-SET-NAME -> x5_r44_probe_forms_seed_lists_and_refusals, M-CENSUS-ZERO -> x5f2s3_joint_arm_census_line, M-CENSUS-NOSTALE -> x5f2s3_joint_arm_census_line, M-CENSUS-NOTFROMRUN -> x5f2s3_joint_arm_census_line, OWN-ABORT-KEEP -> szc_non_crash_abort_resets_the_fold, OWN-SEEDSET-LENONLY -> x5_r45_seed_set_checks_short_over_wrong_class, OWN-CENSUS-ARMSEEDS-ALL -> x5f2s3_joint_arm_census_line, OWN-MW-AS-MP -> x5_r39_rows_shape_and_class_split, OWN-SZI-NOFOLD -> UNCAUGHT, OWN-TUNING-ARM -> UNCAUGHT
Followups: X-5c-F4-F2-F1, X-5c-F4-F2-F2, X-5c-F4-F2-F3

## Plain statement

The driver measures what REFRESH 9, REFRESH 10 and G5-CENSUS define, and no seed at or above 10000 has run. All 20 card mutants are caught. Two of the gate's own mutants survive; the code they alter is correct by reading, and they are pin gaps, not defects. **X-5c-F4-F2-F2 must land before the final run**, because one of those gaps becomes observable only once F3 makes `MP_PRESSURE` differ from FINAL.

## How the gate was run

- **Fresh gate.** The scaffold's `fresh-gate.sh`, extracted through the `--no-local` clone, scaffold `187cb226a9bf57ab2a7ba6b2b90fb28cdaf27c6b`. Lines: `fresh-gate: commit c9c6e8b9750d2a73d4bb11b1dfb7f41dac0c4ebe`, `fresh-gate: base 187cb226a9bf57ab2a7ba6b2b90fb28cdaf27c6b`, `fresh-gate: checks passed (4527 files hashed in 1.43 s)`, `fresh-gate: 2 file(s) matched through their eol conversion`. Last line `GATE EXIT=0`; stderr empty.
- **Scope.** Against the card's base `0d0f3da4`, six files change, all on the Touch-only list. The `nucleus/v3-scaffold...HEAD` stat is the whole stacked chain, as REFRESH 10 expects for this branch.
- **Scratch.** Every run and mutant used a `git archive` export of the commit (`GIT_NO_REPLACE_OBJECTS=1`). The worktree held no mutant and was clean before this file was written.
- **Hygiene.** The gate ran `run --set main` twice (once instrumented) and tests. Seeds `0..9999` only. No `verify` over a whole set beyond what the runner did, no `seeded-faults`, no probe seed.

## The six questions

**1. Held-out integrity: holds.**
- `--set` refuses a differing range and `mp-pressure` refuses any well-formed `--seeds`. The probe form refuses `hi >= 10000`, and `--probe` runs only the 200 MP-class seeds of `30000..31000`, refusing `--set` and `--seeds`. A probe file records no set, so `verify --set` refuses it.
- **This card ran none of them.** I parsed every shell command in both attempt logs: `--set held-out`, `--set mp-pressure`, `--probe` and ranges at or above 10000 appear only in card text, source text and one commit message, never as an executed command. The runner's Acceptance ran `--set main` only.
- **Tests.** Execution calls in the added tests use seeds 1926, 1005, 344 and 9149. Five-digit literals are pure-function arguments or record fields. The one `campaign::main` loop passes only refused arguments, and the pure assertion on the bound precedes it, so M-PROBE-RANGE fails before the loop.
- **What the driver does not do.** It has no lock against an operator. `run --set held-out` is the intended route; `seeded-faults --seeds` and `mutants --seeds` remain unbounded because Limit 7 keeps their behaviour; `verify --set held-out` re-runs the set for any file recording that set. Only the procedure guards these (X-5c-F4's PID-1 Acceptance lines). No change is asked here.

**2. FINAL equals BASE, `DIGESTS_MOVED 0`: confirmed.** The two constants' source text is identical and neither is edited. My own `run --set main` exited 0 and the comparator against `41c307a4…1cd2` exited 0 with `DIGESTS_MOVED 0 FAILING_ROUNDS 19 MONITOR 0`. My `main.json` is byte-identical to the worker's (sha256 `da9098a8…f36c`).

**3. `JOINT_ARM_CENSUS` against G5: conforms.** The real line is `JOINT_ARM_CENSUS set=main seeds=10000 arm_rounds=0 stale_reads=0 arm_seeds=[]`, immediately before `E4 SEEDS=`. I generated synthetic cases with the real `joint_arm_census_line` and text-edited the rest, then applied X-5c-F4's own Acceptance line, extracted from the card:

| Case | Reading | Line |
|---|---|---|
| all zero | `arm_rounds=0 stale_reads=0 arm_seeds=[]` | PASS |
| live seed | `arm_rounds=1 stale_reads=0 arm_seeds=[4242]`, names the pin's seed | STOP |
| stale read with arm | `arm_rounds=1 stale_reads=5` | STOP, visibly void |
| stale read, no arm | `arm_rounds=0 stale_reads=3` | STOP |
| short, wrong set | `seeds=9999`; `set=held-out` | STOP |
| live, unsorted input | `arm_seeds=[3,9]`, ascending | STOP |
| missing, repeated, malformed, trailing text | | STOP |

X-5c-F4's three census lines stand before `gate.sh` for all three sets. The line changes no exit rule. The STOP, void and required-pin consequences are P's reading of the printed numbers, as (e) rules.

**4. `szq`, `szi`, the SZC rows and `MP_PRESSURE`: correct.**
- An independent recount of the file gives 15,410 rounds, `szc` 145, `szq` 143 (C 141, MP 0, MW 2), `szi` 58 (C 57, MW 1), and MP refusal 1777/2193 = 0.8103. All match `verify`'s lines. No round breaks `szi ⇒ szq ⇒ szc` or `szq ⇒ !rfs`.
- `szi` by hook is After0 15, After1 3, Mid2 11, After2 11, After3 8, After4 9, uniform 1, and zero at Before0, After5, Mid6 and After6, as X5-R39 defines.
- **The carrier.** The card asks for a flag cleared where step 5 sets `meta_durable_k`. Touch-only excludes `store/mod.rs` and `harness.rs`, so the worker carried it in the hook position code, which closes at the `After5` hook, later than the card's site. I added the card's literal flag on a scratch build (cleared in `ckpt:post5`, and on a non-crash abort) and compared every round of `0..9999`: 0 differences, 0 rounds in the gap, and an output file byte-identical to the uninstrumented run. This is a measurement at BASE. The README's claim that the two agree "for every value `szi` can take" does not cover that gap (F1, F2).
- `MP_PRESSURE` is `Tuning { ..Tuning::FINAL }`, selected only by the `mp-pressure` set and `--probe mp-pressure`, in `run` and in `verify`. The equal constants mean this is established by reading the code and the id pin, not by behaviour (OWN-TUNING-ARM).

**5. Mutants.** The table is the `Mutants:` line. All 20 card rows and four of six own rows are caught; each ran with a real test failure, not a build failure. Survivors:
- **OWN-SZI-NOFOLD** drops `ckpt_closed_dirty >= 4·F` from `szi`. Every pin that needs `szi` false has it false for another reason, so X5-R39's fold term has no pin.
- **OWN-TUNING-ARM** points `TuningId::MpPressure` at FINAL. Unobservable at this tip because the constants are equal, as the card's own step 2 notes, but after F3's retune it would run the MP-pressure set on the wrong tuning with nothing failing.

**6. Fresh gate.** Above. `GATE EXIT=0`.

## Findings

1. **Non-blocking, record.** RESULT item 2 and REFRESH 5 require X5-R39's and X5-R44's tables verbatim. The README's are transcriptions, with different columns and R44's budget column dropped; their content agrees. The README discloses why: the local cards were not in the worktree. Its anomaly (b) also says the card's comparator line names a different file, whereas the card as it stands names the file of record.
2. **Non-blocking now, required before the final run.** The two survivors above.
3. **Low.** By reading (not run, since one case would start a judged-set run): `parse_seeds_opt` skips a `--seeds` value with no `..`. So `run --set mp-pressure --seeds 5` is accepted despite "always refused", and `run --seeds 5` becomes a full `main` run. Neither form reaches a held-out seed through the probe route.

## If the bulk-load review regenerates the baseline

I did not examine that defect. This card changes no store write or record format: its `store/ckpt.rs` diff is comments plus the pure `ckpt_unflipped`. **The driver code does not need redoing.** What is tied to the current bytes and would be redone if digests move:
- the `DIGESTS_MOVED 0` reading and the Acceptance's `run --set main`, `verify` and census line, against the new baseline of record;
- the four execution pins' seed choices and literal digests (seeds 1926, 1005, 344, 9149), which may need re-deriving, with M-SZQ-REFUSED and the three `szi` hook mutants re-run on them.

If digests do not move, nothing here is redone.

## Follow-ups

**X-5c-F4-F2-F1 (README record, non-blocking).** Replace the two transcribed tables with REFRESH 5's and REFRESH 9's verbatim text, correct anomaly (b), and narrow the "every value `szi` can take" sentence to what was measured: zero differences over `0..9999` at BASE, with the post-`meta_durable_k`, pre-`After5` gap named as not excluded by construction. Fits inside X-5c-F4-F3's README record.

**X-5c-F4-F2-F2 (pins; must land before X-5c-F4's final run).** Three items. A literal pin with `szq` true and the crash inside the window but `ckpt_closed_dirty < 4·F`, asserting `szi` false, with OWN-SZI-NOFOLD as its must-fail row. Once F3 makes `MP_PRESSURE` differ from FINAL, a pin that `CampaignSet::MpPressure.tuning()` and the `--probe mp-pressure` form select those values, with OWN-TUNING-ARM as its must-fail row. And, at F3's tip, the literal-flag comparison over `0..9999` on a scratch build, recorded as probe evidence, since a retune changes where crashes land.

**X-5c-F4-F2-F3 (`--seeds` parsing, low).** Make a present `--seeds` whose value is not `A..B` an exit-2 refusal in every `run` form, with a pure pin, so a malformed flag cannot silently become a judged-set run.
