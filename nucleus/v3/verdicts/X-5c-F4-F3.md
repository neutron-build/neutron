Card: X-5c-F4-F3
Class: experiment
Card-gate: P
Verdict: ESCALATE
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: T3 review sub-agent, Claude Code (54c846a6-f1e9-445e-b74d-b3259cb2e745, 2026-11-10)
Date: 2026-11-10
Gate-exit: GATE EXIT=0
Commit: 14c78c42329921d925ae7d1aed628166cfee300b
Mutants: M-FINAL-R41 -> x5_r37_final_r41_is_s1, M-FINAL-R41P -> x5_r37_final_r41_is_s1, M-BASE-EDIT -> x5_r37_base_tuning_reproduces_1b0ce54e, M-RANGE-VALUE -> x5_r37_final_stays_in_card_ranges, M-RANGE-SHARE -> x5_r37_final_stays_in_card_ranges, M-MP-R41 -> x5_r37_mp_pressure_r41_equals_final, M-MP-OTHERFIELD -> x5_r37_mp_pressure_differs_only_in_ladder_fields, M-CPU-TERM-FROM-FINAL -> --self-test, M-CPU-WHOLE-FROM-S1 -> --self-test, M-CPU-WHOLE-EXITS -> --self-test, M-CPU-NOMEDIAN -> --self-test, M-CPU-BOUNDARY -> --self-test, M-CPU-NOFOREIGN -> --self-test, G-MP-SYNCOFF -> x5_r37_mp_pressure_differs_only_in_ladder_fields, G-MP-CHUNK-HI -> x5_r37_final_stays_in_card_ranges, G-CPU-FOREIGN-GE -> --self-test, G-CPU-FOREIGN-NOFINAL -> --self-test, G-CPU-WAIT-IGNORED -> --self-test, G-FINAL-SYNCOFF-BASE -> UNCAUGHT, G-FINAL-W11-OFF -> UNCAUGHT, G-CPU-BAND-FROM-FINAL -> UNCAUGHT, G-CPU-S1-IS-HEAD -> UNCAUGHT
Followups: X-5c-F4-F3-F1, X-5c-F4-F3-F2

## Plain statement

The card did what it was written to do. The ladder was fixed before any probe. Every rung tried is in the card. Every count the gate re-ran reproduces exactly. No threshold moved, and the held-out and MP-pressure sets were never touched. All 13 card mutants are caught, and the fresh gate is green.

The four rows it escalates are **real**. They are not artefacts of a loaded host:

- **SZC and SZI (rung 7.3).** On an idle host, 7.3 measures **45.4 min per 10k**, worse than the worker's 41.8 under load. Over the whole 10k, 7.3 does meet SZC (2,491), SZI (779), SZC-MW (130) and Q10 (615). The probe's Q10 460 was a sampling shortfall. But 7.3 drops Q7 to 50, and it puts a live joint arm in the census (`arm_rounds=2`).
- **Q7.** At the kept setting it reads **170**, not the 190 the README states. Its only ladder (drops) trades directly against the owner's Q4 ≥ 1,500 bar.
- **SZC-MP.** Unreachable at this set size. Across the full 2,000 MP seeds at the kept 9.1 tuning it reads **14** against 100. The cause is structural: ≥ 4·F intervals arise almost only in round 1 and later, and 87% of MP seeds end at a refused round 0.

There are two record defects: the Q7 figure, and CPU lines taken from a banned in-attempt run. The Acceptance's own CPU run read whole-FINAL **+62.03%**, not +51.71%, and both CPU runs are VOID. Neither defect changes a decision, but both must be corrected before the owner reads the record.

Verdict ESCALATE: four rows go to the owner, and this tip cannot launch the final run as it stands.

## How the gate was run

- **Fresh gate.** The scaffold's `fresh-gate.sh` was extracted through the `--no-local` clone (scaffold `187cb226a9bf57ab2a7ba6b2b90fb28cdaf27c6b`). Lines:
  - `fresh-gate: commit 14c78c42329921d925ae7d1aed628166cfee300b`
  - `fresh-gate: base 187cb226a9bf57ab2a7ba6b2b90fb28cdaf27c6b`
  - `fresh-gate: checks passed (4531 files hashed in 1.35 s)`
  - `fresh-gate: 2 file(s) matched through their eol conversion`

  Steps test, clippy, fmt, deny, wasm32, core-paths-selftest and enforcement-tests each read EXIT=0. The last line is `GATE EXIT=0`. The extract dir was removed.
- **Scope.** `card/v3-E4-SILENT-1...HEAD` changes exactly the five Touch-only files: `src/workload.rs` +68/−38, `tests/workload.rs`, `tools/x5_probe.sh`, `tools/x5_cpu_term.sh` and `README.md`. No protected path is touched, and the worktree is clean.
  - Every `src/` change is a constant of `Tuning::FINAL` or `Tuning::MP_PRESSURE`, plus doc lines. `Tuning::BASE` is untouched.
  - Step 1's commit (`84d3af0f`) changes exactly `r41_share` 0→1000 and `r41p_share` 0→500, plus their doc lines.
  - Each rung commit changes exactly that rung's listed fields. The two outcome commits (`b2734406`, `6f81f253`) revert exactly to the kept rung.
- **Scratch.** All gate builds were `git archive` exports in the gate's scratchpad, each with its own target directory. The worktree never held a mutant or a gate edit.
- **Host.** It was **not idle** for most of the gate. Another session's test suite ran at up to ~1,500% CPU, and a load of 80 was seen at one point. Every timing below was taken behind a guard: wait for a 1-minute load below 2, then report foreign CPU from `/proc/stat` busy time minus the job's own user+sys. The foreign figure is printed with each reading.
- **Gate hygiene.** No seed in `10000..30000` was run.
  - Probes over `0..1000`, and `--probe mp-pressure` (`30000..31000`), at rungs 3.1, 6.1 (HEAD), 7.3, 9.1 (HEAD) and 9.3.
  - One `run --set main` (`0..9999`) of rung 7.3's build, in scratch, not a run of record.
  - One gate-only scratch build whose `probe_mp_seeds()` returns the 2,000 MP-class seeds of the triaged main range `0..10000`, run at 9.1's `MP_PRESSURE`.
  - Quota rows of the 10k file were read by a gate-only scratch subcommand that calls the crate's own `parse_campaign_with_set`, `workload::quotas` and `q4i_count`. On the 7.3 probe JSONs it returns the probe's sums exactly (SZC 239, Q10 46, Q5a 23).

## Findings by question

### 1. Reproduction

**Counts are deterministic, and every re-run matches the worker's committed row exactly:**

| Rung | Gate's reading (per 10k = raw × 10) | Worker's row |
|---|---|---|
| 3.1 | Q7 230, Q4 1,440, Q4-I 1,570, Q5a 300, Q8 90 | same |
| 6.1 = HEAD's FINAL | Q7 **170** (raw 17), Q5a 200, Q4 1,810, Q4-I 2,250, Q8 230, Q11 330, Q12 210, Q10 1,140, SZC 620, SZI 150, SZC-MW 60 | same |
| 7.3 | SZC 2,390, SZI 810, SZC-MW 140, Q10 460, Q5a 230, Q7 50, Q4 1,670, Q4-I 2,160 | same |
| 9.1 = HEAD's `MP_PRESSURE` | SZC-MP raw 2 | same |
| 9.3 | SZC-MP raw 2 | same |

**Rung 7.3's wall: the miss is real.**

| Measurement | Host | Wall | Foreign CPU |
|---|---|---|---|
| Probe form, five 200-seed blocks, gate | load 1.87 at start | **45.4 min/10k** (272.5 s; CPU 3,536 s) | 8.1% |
| Probe form, worker | load 9.45–13.17 | 41.8 min/10k | not measured |
| HEAD (6.1), probe form, gate | load 1.78 at start | 25.0 min/10k | 6.7% |
| 3.1, probe form, gate | load 1.87 at start | 23.0 min/10k | 6.3% |
| 7.3 as one `run --set main` process, gate | foreign load arrived ~6 min in (trace 20–80) | 52.8 min | 37.5% |

- The first row is the cleanest. Even if all 8% of the foreign CPU were charged as wall time, 7.3 would stay over 40.
- The single-process run's wall is **not** a clean reading and is not used for the budget. Its counts are exact.
- The worker's "load 9.5–15.3" was mostly its own 16 threads: `load_at_start` is a one-minute average that still carries the previous block. That is why idle did not help.

**Rung 7.3 over the whole 10k** (`run --set main`, `0..9999`, the 7.3 build; a probe, not evidence):

| Row | Reading | Minimum or bar | Result |
|---|---|---|---|
| SZC | 2,491 | 1,000 | met |
| SZI | 779 | 300 | met |
| SZC-MW | 130 | 50 | met |
| **Q10** | **615** | 500 | met; the probe's 460 was a 46-of-1,000 sampling shortfall |
| **Q5a** | **210** | 200 | thin |
| Q4 | 1,713 | owner bar 1,500 | met |
| Q4-I | 1,922 | owner bar 1,500 | met |
| Q7 | **50** | 200; S1 floor 190 | short |
| Q8 | 335 | 100 | met |
| Q11 | 342 | 100 | met |
| Q12 | 260 | 100 | met |
| Q18 | 4,405 | 1,000 | met |
| Q1 | every hook ≥ 760 | 300 | met |
| SZC-MP (INFO in main) | 6 | — | — |

Census: `JOINT_ARM_CENSUS set=main seeds=10000 arm_rounds=2 stale_reads=0 arm_seeds=[1069,3669]`. At 7.3 the joint arm is **live**, case (e) 2 of G5-CENSUS. If 7.3 were ever chosen, a pin at seed 1069 must land before E4 is judged. The fold is 11 failing rounds, all O-3, matching the shape of the known MP classes. The gate did not classify them, since classification is P's at judgement.

**Q7 recount.** The kept FINAL (6.1) reads **170**, not 190.
- The drift from the 2.0 anchor's 190 came from rung 4.2 (180) and rung 6.1 (170). That is 2 raw rounds.
- The stop rule's hold set allowed it, because Q7 had not met its minimum. The record is still wrong to state "Q7 190" at the kept setting.
- 170 is also under REFRESH 10's Q7 floor of 190 (S1's count).
- Q5a at the kept setting reads exactly 200 on the probe. That is thin for a 5.6 exit row.

**MP recount.** HEAD's 9.1 and the 9.3 build both read SZC-MP raw 2, as recorded.

### 2. Was the ladder followed as fixed in advance?

**Yes.**
- `### X-5c-F4-F3 ladder` was committed at step 0 (`47997218`, 11:22) before the first probe (anchor at 11:35). Its ladder tables are identical to the card's, compared line by line.
- The section is byte-identical from step 0 to HEAD, apart from the next heading appended below it.
- Every rung commit carries exactly the card's fields.
- Each rung decision applies the stop rule as written:
  - 3.1 and 3.2 fail the hold set (Q4 1,440 and 90 under 1,500), so the ladder escalates.
  - 4.2 reaches the required count, not the margin, so the walk continues; 4.3 stops at the margin.
  - 5.1 and 6.1 stop at the margin.
  - 7.1 and 7.2 fail Q5a's hold.
  - 7.3 is over the wall limit (which ends the walk) and fails Q10's hold on the probe.
  - The MP ladder has no rung at the required count, so the highest count is kept.

Post-data choices not pre-registered:
- **The 9.1-versus-9.3 tie-break** (both raw 2). The worker kept 9.1 as the earlier, cheaper rung. That is neutral, and the gate would rule the same.
- **The first anchor probe ran twice.** Its wall and CPU lines read 0.0 from a bash `time` quirk, and it was re-run before any row was committed. Counts are deterministic, so nothing was selected.

**Untouched sets.** A parse of every shell command in both attempt logs (69 and 29) found no `run`, `verify` or probe on any seed in `10000..30000`. The only MP-pressure route used is `--probe mp-pressure` (`30000..31000`, 200 seeds). Strings like `10000..30000` appear only as card text.

**Hygiene breach (record, not data).** In attempt 2 the worker ran three things the card bans in the attempt:
- `seeded-faults --seeds 0..200`;
- the measuring form of `x5_cpu_term.sh`;
- `scripts/gate.sh`.

All three are listed under "Do not" and in Run hygiene, and step 10 says "Do not run the measuring form in the attempt". No forbidden seed was touched. The cost is in the record: the README's item 5 pastes that in-attempt run as "the Acceptance line … re-run at the end of the card". It also states "this attempt never ran the measuring form". The runner's real Acceptance output (`$CARGO_TARGET_DIR/x5f3/cpu.out`, 13:35) is different:

```
CPU_TERM passes=3 terms=[57.90,56.45,54.93] term=56.45 foreign_pct=53.93 verdict=VOID
CPU_WHOLE_FINAL passes=3 terms=[59.81,63.70,62.59] term=62.03 wall_ratio=61.36
```

The worker's run read `CPU_TERM … term=48.58 … VOID` and `CPU_WHOLE_FINAL … term=51.71`. Both runs are VOID: the load-wait timed out, and foreign CPU was 48–54%. **Neither is a usable CPU figure.** The +51.7% and +48.6% quoted to the gate come from the banned run, not the Acceptance. The arms of the real run were checked: BASE is the base's FINAL, S1 has only the two `r41` knobs, and FINAL is HEAD's.

### 3. Code, pins and mutants

- **Pins.**
  - `x5_r37_final_r41_is_s1` pins exactly 1000/500.
  - `x5_r37_final_stays_in_card_ranges` covers Limit 4 for FINAL and `MP_PRESSURE`.
  - `x5_r37_mp_pressure_r41_equals_final` pins the two `r41` fields.
  - `x5_r37_mp_pressure_differs_only_in_ladder_fields` lists every non-ladder field.
- **Two assertions were removed** from `x5_r44_tuning_id_is_mp_pressure_for_that_set_only`: `MP_PRESSURE == FINAL` and `FINAL == BASE`. Both are made false by design, by step 1 and step 9. The id half (M-MP-TUNING's catcher) is kept, and `assert_ne!(FINAL, BASE)` was added. No other assertion changed. This is not a weakening.
- **No `unsafe`.** No non-test `unwrap`, `expect` or `panic` was added.
- **`x5_probe.sh`.**
  - The binary is run directly under `ulimit -v`.
  - The quota sums equal the gate's independent read.
  - Its `PROBE_HOOK` extra line is documented.
- **`x5_cpu_term.sh`.**
  - The arithmetic and its self-test match the contract.
  - The S1 arm is located by subject; any count other than one exits 2.
  - The archive is taken from the worktree root.
  - Every pass is reported.
  - It waits for low load on each pass, which is stricter than "the first block".

**Mutants.**
- All 13 card mutants are caught, each confirmed by its named test's `FAILED` or panic line, not just a non-zero exit.
- 6 of the gate's own 10 are caught.
- The four gate survivors are coverage gaps, not defects, as follows.

| Survivor | What it shows | Follow-up |
|---|---|---|
| G-FINAL-SYNCOFF-BASE, G-FINAL-W11-OFF | **FINAL's retuned constants are pinned by nothing.** The whole suite passes 313/313 with FINAL's `syncoff_table` back at `[0, 3, 7]`. Only S1's two knobs and the ranges are pinned. | -F1 |
| G-CPU-BAND-FROM-FINAL, G-CPU-S1-IS-HEAD | `--self-test` covers only the arithmetic. A measuring path that built S1 from HEAD, or chose the band from the FINAL column, would pass it. The gate checked the real run's three arm sources by hand: they are correct. | -F2 |

### 4. SZC-MP: why near zero, and is it reachable?

**At this set size, with any tuning tried, it is unreachable.** The cause is the round structure of class MP, not the knobs.

| Measurement (full sets, gate) | MP rounds | Refused | State rounds | szc | **szq = SZC-MP** |
|---|---|---|---|---|---|
| 9.1 `MP_PRESSURE`, the 2,000 MP seeds of `0..10000` (scratch) | 2,316 | 1,663 (72%) | 653 | 67 | **14** |
| 7.3 FINAL, main 10k, its 2,000 MP seeds | 2,220 | 1,740 (78%) | 480 | 38 | **6** |

- **A ≥ 4·F interval is a later-round event.**
  - In the 7.3 10k, class C cuts a ≥ 4·F interval in 10 of 7,000 round-0 crashes (0.14%), 1,487 of 3,480 round-1 crashes (43%), and 858 of 1,410 round-2 crashes (61%). Class MW shows the same pattern.
  - The generator builds a large dirty set only across checkpoint intervals of a later round, and a round-0 crash comes too early.
- **MP seeds rarely reach round 1.** The frozen fault model makes 72–78% of MP rounds refuse, and a refused round ends the seed. 87% of MP seeds stop after round 0, and only 205 of 2,000 reach round 1.
- **The definition's exclusion is a second, smaller factor.** szq requires a state. Even counting refused rounds, szc reads 67 at 9.1, still under 100. Refusal is mildly anti-correlated with szc: 79% of szc rounds refuse, against 72% of all rounds.
- **The set's construction is a third factor.** The MP-pressure set is 2,000 seeds, the same number of MP seeds the main 10k already holds. Its only gain is its own tuning and its own box, and 9.1 used about 6–9 of its 40 minutes.
  - At 9.1's rate (14 per 2,000), 100 needs about 14,000 MP seeds, roughly 60 minutes.
  - The box fits about 9,000–10,000 MP seeds, an expected 65–70. A bigger set alone does not get there.
- **The ladder started from the wrong place.** REFRESH 6's starting points (k1 175, k2 100 per 10k) were 7 raw hits in 80 MP seeds, read before S1 and before E4-SILENT-1. That drew from the per-seed tail. The full-set rate is about 0.7% of seeds.
- **Conclusion.** The requirement is not impossible by definition, but this workload makes it unreachable within this set and box. Reaching it needs one of:
  - a change to the round-0 workload of MP seeds, so that round 0 builds a ≥ 4·F interval before its crash (a 5.5 generator change, owner);
  - a much larger MP set with a longer box;
  - moving the row.

  A-9's MP ≥ 1,000 inherits the same round structure if A-8a ports this generator unchanged, so A-9 must be written knowing this.

## Record defects (to fix in -F1, not by the gate)

1. README record item 3 states "Q7 190" at the kept setting. The kept 6.1 reads **170** (the 6.1 row itself says 170). Item 6 and the Q7 outcome block repeat "kept … reading Q7 190".
2. README item 5 pastes a banned in-attempt CPU run as the Acceptance, and claims the attempt never ran the measuring form. It must carry the runner's Acceptance lines above, both VOID.
3. The Run-hygiene breach (in-attempt `seeded-faults`, the CPU measuring form, `gate.sh`) is not recorded in the anomalies.

## FOR THE OWNER

**What happened.** The retune ran exactly as you pre-registered it. The numbers reproduce. Four rows cannot be met at this setting:

| Row | Need | Have (kept setting) | Why short |
|---|---|---|---|
| Q7 | 200 | 170 | More drops raise Q7 but sink Q4 below your 1,500 bar (3.1: Q7 230 / Q4 1,440) |
| SZC | 1,000 | 620 | Rung 7.3 gives 2,491 over the full 10k, but takes 45 min per 10k on an idle host |
| SZI | 300 | 150 | As SZC (7.3 gives 779) |
| SZC-MP | 100 | 14 over the full 2,000 seeds | MP seeds almost never reach the round where big dirty sets occur |

**The CPU numbers you were given (+51.7% / +48.6%) are not valid.** They come from a run the worker was not allowed to make. The real Acceptance run read S1 +56.45% and whole-FINAL +62.03%. Both runs were on a busy host and are marked VOID. No clean CPU figure exists yet.

**Honest options.** Each one keeps every threshold, and each is yours to choose.

**A. Take rung 7.3 for SZC and SZI, and give it a longer box (about 50 min per main run).**
- *Gains:* SZC, SZI, SZC-MW and Q10 are all met by the full-10k probe, with margin on SZC and SZI.
- *Costs:*
  - The 40-minute box becomes about 50 for the main and held-out sets. With an idle host the whole run grows by about 20–30 min.
  - Q7 falls to 50, below both 200 and its 190 floor. Q7 then needs its own answer (see D).
  - Q5a sits at 210, thin over 200.
  - The census shows a live joint arm (seeds 1069 and 3669), so a pin card must land first.
- *Leaves unproven:* Q7.

**B. Rung 7.3 within 40 minutes.**
- The gate measured it on an idle host at 45.4. The worker's 41.8 was not inflated by load, and idle made it no faster. This option does not exist without A's longer box.

**C. Keep the current FINAL and state SZC and SZI as gaps for E4, carried to A-9 on the real store.**
- *Costs:* nothing in run time. E4 then proves sizing coverage at about 60% of SZC and 50% of SZI.
- *Leaves unproven:* E4 would not show the "4× the pool" condition at your A+ level. A-9 (×10) becomes the first place it is shown.

**D. Q7.**
- **D1:** accept that Q7 and your Q4 ≥ 1,500 bar conflict, and choose one. 3.1 meets Q7 at 230 with Q4 1,440, which passes Q4's frozen exit row of 1,000 but not your 1,500 bar.
- **D2:** keep the bar and state Q7 (170 against 200) as a gap.
- Both are judgements on your own bars. No card can resolve them.

**E. SZC-MP.**
- **E1: a generator change** so that MP seeds build a large interval in round 0, before the crash. This changes the workload (a 5.5 change) and needs its own card, ladder and gate. It is the only route that fixes the cause.
- **E2: a much bigger MP set with a longer box.** About 14,000 MP seeds at about 60 min are needed for an expected 100, with real risk of a miss.
- **E3: move the MP row to A-9's real-store harness.** It is already ×10 there. Note that A-9 inherits the same round-0 problem if the generator is ported unchanged.
- **E4: accept a stated gap.** MP detection is still exercised: 2,000 seeds, refusals counted, Joint and Slot faults caught. What stays unproven is recovery under a large dirty set with media faults.

**My recommendation.**
- **A + D2 + E3**, written so that A-9 also fixes E's cause (E1), with a clean idle-host CPU run before launch.
- 7.3 is the only measured setting that meets the A+ sizing rows in E4. It costs about 10 more minutes per main run, not a threshold.
- Q7 is a direct trade against your own Q4 bar, and the smaller shortfall to state.
- SZC-MP cannot be fixed by tuning. Its real fix (E1) belongs in the harness that will carry it at ×10 anyway.
- If you prefer to keep the 40-minute box, choose **C + D2 + E3** and state three gaps.

## Follow-ups

**X-5c-F4-F3-F1 (record and pin; test-only plus README; gate S).** Correct the three record defects above:
- Q7 170 at the kept setting;
- the runner's Acceptance CPU lines, both VOID, replacing the in-attempt run's;
- the hygiene breach, recorded under anomalies.

Then add `x5_r37_final_is_the_kept_setting`, pinning every field of `Tuning::FINAL` and `Tuning::MP_PRESSURE` as the final run will use them. G-FINAL-SYNCOFF-BASE and G-FINAL-W11-OFF are its must-fail mutants. If the owner's decision changes FINAL, this card carries that decision's constants instead. Do it before the launch, whichever option is chosen.

**X-5c-F4-F3-F2 (CPU measuring-path test; tools only; gate S).** Give `x5_cpu_term.sh` a dry-run mode that resolves the three arm refs and prints them, with no build and no run. Add a self-test case that drives the band decision from fixture pass-1 columns. G-CPU-S1-IS-HEAD and G-CPU-BAND-FROM-FINAL are its must-fail mutants. Then run the measuring form once on a host the orchestrator has actually kept idle, since no non-VOID CPU figure exists for S1 or for FINAL.
