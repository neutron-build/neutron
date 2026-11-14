Card: X-5c-F4
Class: experiment
Card-gate: P
Verdict: ESCALATE
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: T3 review sub-agent, Claude Code (55931517-09e9-4381-88a1-f567b38eee60, 2026-11-14)
Date: 2026-11-14
Gate-exit: GATE EXIT=0
Commit: cb2aca0d9b5f15e13b29d81dae8f992b7127bf70
Mutants: G5-R48-TREADS-KEPT -> x5f4_joint_arm_live_at_1069_is_fresh ([0, 12]) and x5f4_joint_arm_live_at_3669_is_fresh ([0, 56]) at main's seeds; UNCAUGHT at held-out's live seeds 10669/13469/16669 (no pin yet; scratch counts 24/19/24 -> X-5c-F4-F6), M-GATE-TWIN-SKIP -> e4_class_match.py self-test (unmatched-twin-fail), M-GATE-GE-OFF1 -> e4_class_match.py self-test (unmatched-ge-below)
Followups: X-5c-F4-F6, X-5c-F4-F7, X-5c-F4-F8

**Where this verdict lives.** The request named this file. It also forbade any write to the card branch, so GATE-PROMPT step 7's commit to `nucleus/v3/verdicts/X-5c-F4.md` was not made, and REFRESH 10's "P's gate of this run commits the three census lines" was not done either. Both are listed under Gaps. The orchestrator may commit this file as the branch verdict.

## The verdict in one paragraph

**ESCALATE, to the owner.**

**The run itself is sound and stands as evidence.** It was launched once, ran on one boot, and nothing was worked around. Every set exited 0 and every `verify` exited 0. Each JSON hashes to its `.prov`. The boxes and the idle host held. A 77-seed sample re-run by the gate reproduces every record exactly. The fresh gate reads `GATE EXIT=0`.

**Main passes on its own.** Every exit row, bar, floor and seeded fault passes. The file is byte-identical to the pre-registered baseline. The class match exits 0 with 2 / 4 / 5.

**E4 is still open, for three reasons:**
1. The held-out census reads three live joint arms with zero stale reads. That is G5-CENSUS (e) 2: a test-only pin is required before E4 is judged (X-5c-F4-F6). The run stands once it passes.
2. Held-out's class match exits 1. One failing twin (seed 17948, twin 2) is unmatched, so the frozen pass definition counts it as a new class in held-out.
3. MP-pressure's class match exits 1. Two failing MP rounds (26018 r0, 26958 r0) fall outside the three frozen signatures, so they are new too.

By the card, items 2 and 3 go to the owner. None of the three is a defect in the run.

## For the owner: E4 in plain language

**E4 is not passed yet.** The final run did what it was built to do. The fresh seeds and the MP-pressure seeds found cases that the frozen pass list does not cover.

**What holds**
- **The run can be trusted.** It used one commit, one boot and one attempt, on an idle host. All three sets finished inside their time boxes. Every file reproduces: the run's own `verify` re-ran every seed, and the gate re-ran a sample of 77 seeds and got identical records.
- **The main 10,000 seeds pass.** Every coverage row and your bars are met (Q4 1,713, Q4-I 1,921, Q7 442, SZC 2,421, SZI 777). The planted Joint and Slot faults are caught. S1's CPU cost is +55.44%, against a limit of +60%. Nothing crashed, deadlocked or hit a monitor. All 11 failing rounds are MP rounds in the three classes you ruled on: a refused open in 6 rounds, and the checker's over-report in 5, each with its diagnostic proven.
- **MP-pressure meets its row.** SZC-MP is 148, against a minimum of 100.
- **No silent wrong data was found anywhere.** Every failing round in all three sets either refused to open, or opened at a correct state with the damage reported.

**What keeps E4 open**
1. **The held-out joint-arm census (mechanical).** Three held-out seeds exercise a rare test-harness path, the joint arm, and all three read clean (0 stale reads). The card's rule asks for one more test that pins this before E4 is judged (X-5c-F4-F6, below). That test changes no result. Once it passes its gate, the run stands.
2. **Held-out seed 17948 (a new failure, so your decision).**
   - **What it is.** It is a twin round. A twin replays a damaged round with the disk damage removed, and is judged as a plain crash. This twin's store refused to open and served nothing, but a plain crash must recover, so it fails.
   - **The gate's evidence.** Before that round, the harness re-seeded the twin from the real round's state, and that state already held the real round's earlier disk damage. The rule doing this was added by X-5c-F4-F1-F1-F3-F2-F1-F2. So the twin was not damage-free, as Work 2.5 says it must be.
   - **What the gate's reading is not.** It is not a pass. The frozen rule counts the round as new, and "a class a landed card claims to have fixed that recurs counts as new" applies too.
   - **Why it matters.** Twin results feed the digests of the MP and MW seeds. Fixing the re-seed would change those digests, so the files would no longer reproduce, and E4 would need a new final run. Under PLAN §10 item 12, that call is yours.
3. **MP-pressure seeds 26018 and 26958 (new shapes, so your decision).**
   - Both stores refused to open. Their recorded flags (`skx` or `smx` true) can only be true when no caller received any data.
   - Those are the two facts your MP-A ruling ("documented, detected refusal, narrow form") rests on.
   - They miss the frozen signatures on one field only, `rlen`, the size of X5-R5's exception set. 26018 is an old-page refusal with `rlen=1`, where class 1 requires 0. 26958 is an old-header refusal with `rlen=0`, where class 2 requires at least 1.
   - The table's `rlen` conditions were drawn from the baseline's data.
   - The table is frozen, and no one may classify by hand. So they stay new unless you extend MP-A to them.

**Decisions that are yours**
- **(a) Seed 17948's twin.** After P's Work 2.5 ruling (card X-5c-F4-F7), choose one:
  - accept the re-seeded twin's inherited damage as a known harness limit, and judge E4 on the record as it stands;
  - or have the re-seed fixed. The files then no longer reproduce, so E4 needs a new final run, which is your call under §10 item 12.
- **(b) MP-A's extent.** Choose one:
  - extend MP-A's narrow form to any `rlen`, for refused opens with no caller data. That is a post-data amendment to the frozen table, recorded as such. The table is then re-frozen and the matcher re-run on the MP-pressure file.
  - or keep E4 open on 26018 and 26958 until they are root-caused (X-5c-F4-F8).
- **(c) Nothing reruns on its own.** The gate reran no set.

## 1. Run integrity

| | main | held-out | mp-pressure |
|---|---|---|---|
| `commit` | T | T | T |
| `status_lines` | 0 | 0 | 0 |
| `boot_id` (run and verify) | `f6eb27d6-bb8c-40f3-8f8a-383d8f88edcf` | same | same |
| `exit` / `verify_exit` | 0 / 0 | 0 / 0 | 0 / 0 |
| `box_wall_s` (box) | 2,425.408 (≤ 3,000) | 2,423.494 (≤ 3,000) | 520.948 (≤ 2,400) |
| guard | `verdict=CLEAN`, foreign −0.89% | CLEAN, −0.84% | CLEAN, −2.01% |
| 1-min load after the wait | 1.76 | 1.95 | 1.87 |
| run | 22:18:24 → 23:01:09 (`line_s` 2,565) | 23:42:12 → 00:24:56 (2,564) | 01:05:39 → 01:16:40 (661) |
| verify | 23:01:09 → 23:42:12 (2,463 s) | 00:24:56 → 01:05:39 (2,443 s) | 01:16:40 → 01:25:30 (530 s) |
| verify / run wall | 1.016 | 1.008 | 1.017 |
| `json_sha256` | `cb947c19…ae45` = file (byte-identical to the baseline `runlogs/X-5c-F4-F5.head.main.json`) | `e655a61b…621a` = file | `75d719b8…3ce7` = file |
| seeds, exact set, file's `set` | 10,000, exact `0..9999`, `main` | 10,000, exact `10000..19999`, `held-out` | 2,000, exact `s%10∈{7,8}` in `20000..29999`, `mp-pressure` |
| `rerun_after_reboot`, `void.*` | none, none | none, none | none, none |

**The worktree and the chain.**
- `~/.cache/nv3-cards/X-5c-F4` is at T on `card/v3-X-5c-F4`, with an empty `git status`.
- `card/v3-E4-CLASS-1` is T, and `card/v3-X-5c-PINS-2` is `fc275882…`.
- `fc275882`, `ec9f84a6`, `84d3af0f` and `8d679ccd` are all ancestors of T.
- The diff against the Base (`card/v3-E4-CLASS-1`) is empty, and the runner reported `scope ok`.
- The class table at T hashes to `55ae4734…1158`.

**The agent attempt was read-only.**
- `X-5c-F4.attempt1.jsonl`: 15 shell reads, 5 file reads and 2 greps. No edit, no commit, and no Acceptance line.
- The ledger's run `9d41d198` starts and ends at T.

**The runner.**
- The unit `nv3-X-5c-F4-final` ran from 22:11:22 to 01:31:35.
- The journal says "Consumed 1d 20h 53min 57.923s CPU time, 12.9G memory peak", with no OOM line.
- Every line passed except line 13, the held-out census.

**Pre-launch.**
- **Rehearsal:** `REHEARSAL PASSED`, 11-11 about 23:00. Its script sha `868b6ff8…9951` equals the card's extract.
- **Two `VOID` CPU measurements**, each with the foreign load found, which is the card's VOID path:
  - void-1: `foreign_pct=11.25`;
  - void-2: a `WAIT … timeout=1` and `foreign_pct=56.95`;
  - QUEUE names another session's (prismqi's) `cargo build` and `cargo test`.
  - All three terms ever measured were 52.4 to 59.0, so this was not re-measuring for a better number.
- **The third measurement:** `CPU_TERM passes=3 terms=[58.61,55.44,54.59] term=55.44 foreign_pct=0.00 verdict=OK`, with the arms line exact. The run's line 2 re-read it and passed.
- **The host reboot** (11-12 about 13:32, the current boot) came before the final-run window, because measurement 3 started on this boot. It touches nothing: the card says the CPU record binds T and not the boot, and the rehearsal binds T, which did not change.
- **The roughly 47-hour gap** from the rehearsal to the launch breaks no card rule. The worktree stayed at T and clean, and line 2 re-checked T, the table sha and the CPU record at launch.

**The idle window.**
- All three run guards are CLEAN; a negative foreign share means no foreign CPU.
- The verify/run wall ratios (1.008–1.017) sit inside the card's idle-host expectation of 1.00–1.08.
- The journal shows other user units only as short `t3code-opencode-*` scopes:
  - 7 during the run (22:11–01:31), at 6.4–8.6 s of CPU and at most 401 M each;
  - 2 more that overlapped the CPU measurement (3.2 s and 7.0 s), which still read `foreign_pct=0.00`.
  - None is a build or a test.

**Nothing was worked around.** No line was edited, no pattern widened, no set sharded or rerun.

**The VOID history, the reboot and the long wait do not affect validity** under REFRESH 11 and 11a.

## 2. The pass decision (E4-CLASS-1's script, REFRESH 11a's lines)

**How it was run.**
- The card's lines, verbatim, from a `git archive` of T. `L` pointed at `$A`, because the files are not in `runlogs/` yet (Gaps 1). Each `.prov` sha was checked against its file first, and all three agree.
- `read_match` is as the card defines it. **No STOP and no traceback. Every stderr is empty.**
- **`$BIN` for main** is T's own release build, from the export.
- **`$BIN` for held-out and MP** is a `git archive` of T with only these edits (diffs in `gate-X-5c-F4-final/exp-{heldout,mp}.diff`):
  - held-out: `run_form`'s probe bound `hi >= CAMPAIGN_SEEDS` becomes `hi > 30_000`, and the probe runs FINAL;
  - MP: the same edit, plus `run_range_into` calling `run_seed_with(s, mutant, &Tuning::MP_PRESSURE)`.
- **Every diagnostic run reproduced its seed's digest.** The script checks it, and every class-3 candidate matched.

| Set | No-diag reading | Diagnostic seeds | Reading of record (`--diag-dir`) |
|---|---|---|---|
| main | `MATCH_READ exit=1 unmatched=5` | 3988 5168 7177 7727 8657 | **`MATCH_READ exit=0 unmatched=0`**, `E4_CLASS_COUNTS refused_old_page=2 refused_old_header=4 amcheck_subtree_fallout=5 unmatched=0`. Byte-identical to E4-CLASS-1's `match.out` (`~/.cache/nv3-target/E4-CLASS-1/e4class/match.out`) |
| held-out | `MATCH_READ exit=1 unmatched=7` | 11118 11718 11788 11997 16587 17398 | **`MATCH_READ exit=1 unmatched=1`**, `E4_CLASS_COUNTS refused_old_page=5 refused_old_header=3 amcheck_subtree_fallout=6 unmatched=1` |
| mp-pressure | `MATCH_READ exit=1 unmatched=6` | 24537 25218 26417 27238 | **`MATCH_READ exit=1 unmatched=2`**, `E4_CLASS_COUNTS refused_old_page=3 refused_old_header=7 amcheck_subtree_fallout=4 unmatched=2` |

**The unmatched lines, verbatim:**
```
E4_CLASS_UNMATCHED 17948 twin 2 pass=false
E4_CLASS_UNMATCHED 26018 round 0 no-class refused_old_page:round.rlen refused_old_header:round.skx amcheck_subtree_fallout:round.c
E4_CLASS_UNMATCHED 26958 round 0 no-class refused_old_page:round.skx refused_old_header:round.rlen amcheck_subtree_fallout:round.c
```

**The records, verbatim from the files:**
- **held-out 17948 `twin[2]`:** `{"pass": false, "o1": "O-1(a) at c=25: published {} != model {2, 3, 4}"}`.
  - Its real round 2 passes: `crash "poison"`, hook `After5`, `torn 1`, `c null`, `frontier 0`, `rfs 1`, the same `o1` text, `pass true`.
  - Twins 0 and 1 pass.
- **mp 26018 r0:** `uniform@y:20717`, `lost 1 mis 1 torn 1`, `c null`, `frontier 0`, `mrb true`, `served true`, `o3 "O-3(a): served corruption"`, `skx true`, `smx false`, `rlen 1`, `pass_na true`, `cwin w11`, `twin[0]` passing.
- **mp 26958 r0:** `uniform@y:6622`, `mis 2`, `c null`, `frontier 0`, `mrb true`, `served true`, `o3 "O-3(a): served corruption"`, `skx false`, `smx true`, `rlen 0`, `pass_na true`, `twin[0]` passing.

**The reading (REFRESH 11's).**
- **Main exit 0:** every failing record matched one class, with class 3's diagnostic proven.
- **Held-out and MP-pressure exit 1:** a new class, so E4 stays open and the case goes to the owner. In held-out this is the owner's rule.
- **No hand classification was done**, and the table and the script were not touched.

**Evidence gathered for the follow-ups. It is not a classification, and the matcher's result stands.**
- **17948, twin re-seed.** A scratch copy of the held-out export added stderr-only prints. The record and digest stayed equal to the file's (2914550144):
  ```
  GATE_TWIN seed=17948 round=0 path=first pass=true o1=Ok(18) frontier=18 served=false amcheck_i=0 end_store=true
  GATE_TWIN seed=17948 round=1 path=first pass=true o1=Ok(37) frontier=37 served=false amcheck_i=0 end_store=true
  GATE_RESEED seed=17948 round=2 cause=dirs-differ
  GATE_TWIN seed=17948 round=2 path=first pass=false o1=Err("O-1(a) at c=25: published {} != model {2, 3, 4}") frontier=0 served=false amcheck_i=3 end_store=false
  ```
  - `E4_MPE_DIAG`'s class C (twin) blocks read `amcheck_i findings []` in rounds 0 and 1.
  - In round 2 they read findings on slots 80, 4 and 122, all `range_tainted=true`, with no media fault of the twin's own. Those are the slots the real chain's own faults damaged: r0 `Lost … (page slot 4)`, and r1 `Misdirected { to_off: 499712 } … (page slot 80)`, where 499712 is slot 122.
  - Both re-seed arms copy the real chain's disk (`disk: ch.disk.clone()`, `src/workload.rs` 2233–2239 and 2262–2269 at T).
  - The twin's recovery returned no store and served nothing.
- **26018 and 26958.** `r30_predicates` (`src/harness.rs` about 6859) returns `(false, false)` unless `workload_served` is empty, with no R-taint and no uncovered accepts. So `skx=true` and `smx=true` each record that no caller received data. With `c=null` and `frontier=0`, the store did not open.
- **Saved under `gate-X-5c-F4-final/unmatched/`:** the outputs (`h17948.*`, `m26018.*`, `m26958.*`, `r17948.*`).

## 3. Owner rows

**Main, `verify --set main`'s table.** Every row is `MET`, and the run printed `VERIFY OK seeds=10000 digests reproduce, every quota met`.
- **5.6 rows:**
  - Q1 760–801 per hook (minimum 300); Q2 4,308; Q3 1,246; Q4 1,713;
  - Q5a 449, Q5b 6,967, Q5c 5,396; Q6a 2,212, Q6b 1,320;
  - Q7 442; Q8 323; Q9 9,890; Q10 610; Q11 320; Q12 254;
  - Q13a/b/c 897 / 948 / 920; Q14 495; Q15 1,200; Q16 6,789; Q17 10,087; Q18 4,405.
- **A+ rows:** SZC 2,421 (minimum 1,000); SZC-MW 130 (50); SZI 777 (300); Q4-I 1,921 (1,000). SZC-MP is 6, and it is `INFO`, as REFRESH 10 sets.
- **Bars:** `OWNER_BARS q4=1713 q4i=1921`, both at or above 1,500.
- **Floors:** Q5a, Q7 and Q18 at 449, 442 and 4,405, against 200, 190 and 990.
- **Run lines:** `MONITOR=0`, `DEFECT_SEEDS=0`, `DEADLOCK=0`, `PANIC=0`, `TWIN_FAILING_ROUNDS=0`.
- **Seeded faults** (line 3, at FINAL):
  - Joint `M-REUSE-DROPS-IMG` new-failing 35 (at least 5) `CAUGHT`;
  - Slot `M-FREE-HOME-ALLOC` new-failing 50 (at least 5) `CAUGHT`;
  - no `NOT CAUGHT`.
- **CPU:** S1 +55.44% (at most +60.00%), `OK`. `CPU_WHOLE_FINAL` is +198.36%, wall ratio 185.97; it is reported and never an exit input (§10 item 13 (b)).
- **Compare:** `DIGESTS_MOVED 0 FAILING_ROUNDS 11 MONITOR 0`, `NEW_FAILING 0`, `TWIN_FAILING 0`, `MPA_CLASS base 6 tip 6`.
- **Census:** exactly `JOINT_ARM_CENSUS set=main seeds=10000 arm_rounds=2 stale_reads=0 arm_seeds=[1069,3669]`.
- **Wall:** 2,425.408 s, within 3,000.
- **`MP_REFUSAL rounds=2220 refused=1746 rate=0.7865`.**

**Held-out: every row is `INFO`, as the set's rule says.** For the owner's information, every main threshold would hold here too:
- Q1 748–802; Q4 1,715; Q4-I 1,866; Q5a 460; Q7 506; Q18 4,289;
- SZC 2,400; SZC-MW 129; SZI 732; SZC-MP 12;
- `MONITOR=0 DEFECT_SEEDS=0 DEADLOCK=0 PANIC=0`, but `TWIN_FAILING_ROUNDS=1` (17948);
- wall 2,423.494 s;
- `MP_REFUSAL rounds=2254 refused=1753 rate=0.7777`.

**MP-pressure.**
- Its one exit row, SZC-MP, reads 148 (minimum 100), `MET`.
- `MONITOR=0 DEFECT_SEEDS=0`. Wall 520.948 s, within 2,400.
- `MP_REFUSAL rounds=2342 refused=1635 rate=0.6981`.
- Its other rows are `INFO` and are not judged.

## 4. Reproduction sample (the owner's "the gate re-runs a sample")

- **Method.** Each seed was run alone (`run --seeds s..s+1 --threads 1`), and its JSON seed record was compared field by field with the file's.
- **Binaries:** T's own for main, and the gate's held-out and MP exports for the other two.
- **The sample:** every failing seed, the census arm seeds, and 10 seeds per set drawn by `random.Random(20261114)` (plan in `gate-X-5c-F4-final/sample/plan.json`).
- **Result: 77 of 77 records are equal.**

| Set | Seeds | Equal |
|---|---|---|
| main | 23: 11 failing, 1069 and 3669, and 402, 1049, 2114, 3951, 7176, 7945, 8155, 8290, 9081, 9629 | 23 / 23 |
| held-out | 28: 15 failing including 17948, the three arm seeds, and 10434, 12524, 13615, 13879, 14451, 16219, 16517, 16906, 17057, 18865 | 28 / 28 |
| mp-pressure | 26: 16 failing, and 21548, 22178, 22468, 23788, 24698, 25397, 25828, 26757, 29137, 29678 | 26 / 26 |

**What else was checked.**
- No set was rerun.
- **"Untriaged means untouched" holds.** The 357 chain commits and the README at T name no seed in `10000..29999` (the hits are yield indices, offsets and counts). No campaign-format result file under `~/.cache/nv3-target`, `~/.cache/nv3-review` or `/tmp` dated before 22:18 on 11-13 holds such a seed: 54,615 files were scanned.

## 5. The census ruling (G5-CENSUS (e), REFRESH 11)

**The three lines, verbatim, from each run `.out` of record:**
```
JOINT_ARM_CENSUS set=main seeds=10000 arm_rounds=2 stale_reads=0 arm_seeds=[1069,3669]
JOINT_ARM_CENSUS set=held-out seeds=10000 arm_rounds=3 stale_reads=0 arm_seeds=[10669,13469,16669]
JOINT_ARM_CENSUS set=mp-pressure seeds=2000 arm_rounds=0 stale_reads=0 arm_seeds=[]
```

**Main and MP-pressure read as required.**

**Held-out reads (e) 2, a live seed with zero stale reads.**
- It is not (e) 4: the file has exactly one line, it is well-formed, and `seeds=10000` equals the set.
- It is not (e) 3: `stale_reads=0`.
- **So the run is not void.** A pin is required, and E4 is not judged until it lands.

**What (e) 2 requires here.**
- **One test-only literal pin** at held-out's lowest live seed, **10669**, at held-out's tuning (`Tuning::FINAL`).
- It asserts that the arm ran and that every stale-read count is zero.
- **G5-R48-TREADS-KEPT** comes back as a must-fail row with a non-zero stale count.
- **"If every live seed reads zero under the mutant, P rules" does not arise.** The gate measured 24 at 10669, 19 at 13469 and 24 at 16669.
- **The run stands once that pin passes its P gate.** It moves no digest, and those rounds ran with a fresh supply.

**The census's arms are confirmed independently.** A scratch example calling `run_seed_with(s, None, &Tuning::FINAL)` printed these, and every digest equals the held-out file's:

| Seed | Rounds | `twin_joint_arm` | `twin_joint_stale_reads` | Under G5-R48-TREADS-KEPT | Release time (load < 3) |
|---|---|---|---|---|---|
| 10669 | 2 | `[false, true]` | `[0, 0]` | `[0, 24]` | 4.6 s |
| 13469 | 3 | `[false, true, false]` | `[0, 0, 0]` | `[0, 19, 0]` | 14.6 s |
| 16669 | 2 | `[false, true]` | `[0, 0]` | `[0, 24]` | 3.6 s |

**One reading considered and set aside.** Read literally over all three lines, (e) 2's order ("the first set that has one: main, then held-out, then mp-pressure") lands on main's 1069. X-5c-F4-F4 already pinned that seed, so on that reading no new pin is needed. REFRESH 11 is the later text and governs. It says held-out "must read `arm_rounds=0` … a live arm with `stale_reads=0` needs a required pin", so held-out's live arm needs its own pin.

**In the meantime the mutant is not unguarded.** At T, the main pins already fail under it (§7).

## 6. Fresh gate (GATE-PROMPT step 1)

**The extract.** The scaffold's `fresh-gate.sh` was taken through the shallow `--no-local` clone, scaffold `187cb226a9bf57ab2a7ba6b2b90fb28cdaf27c6b`. No `EXTRACT FAILED` line.

**The run.** `cd ~/Workspace/neutron-v3 && PATH=$HOME/.cargo/bin:$HOME/.local/bin:$PATH CARGO_TARGET_DIR=$HOME/.cache/nv3-target/gate-X-5c-F4-final bash "$fg/fresh-gate.sh" cb2aca0d9b5f15e13b29d81dae8f992b7127bf70`, from 02:10:27 to 02:16:49, exit 0, with an empty stderr.

**Its lines:**
```
fresh-gate: commit cb2aca0d9b5f15e13b29d81dae8f992b7127bf70
fresh-gate: base 187cb226a9bf57ab2a7ba6b2b90fb28cdaf27c6b
fresh-gate: checks passed (4539 files hashed in 1.40 s)
fresh-gate: 2 file(s) matched through their eol conversion
STEP test EXIT=0 / STEP clippy EXIT=0 / STEP fmt EXIT=0 / STEP deny EXIT=0 / STEP wasm32 EXIT=0 / STEP core-paths-selftest EXIT=0 / STEP enforcement-tests EXIT=0
GATE EXIT=0
```
- **Cleanup.** `$fg` was removed, and no `/tmp` worktree is left.
- **The runner's own line 15** (`gate.sh` at T) also passed.
- **Timing used later.** The crate's 95-test binary (`tests/workload.rs`) took 167.15 s in debug in this run. Only light gate work ran beside it: file scans, Python matcher runs and 3 single-seed diagnostics of about 5 s each.

## 7. Mutants

This card's attempt changed nothing, so no card mutant applies. Its code was gated in X-5c-F4-F2 through -F5, PINS-2 and E4-CLASS-1. The gate aimed three mutants at the logic this verdict rests on. Each was applied to a scratch copy, never to the worktree, so nothing needed reverting there. `git status` in the worktree is empty.

| Mutant | Where | Result |
|---|---|---|
| **G5-R48-TREADS-KEPT** (delete the one `treads.clear();`, `src/workload.rs:2661` at T) | export of T, `cargo test --release --test workload` | caught by `x5f4_joint_arm_live_at_1069_is_fresh` (`[0, 12]`) and `x5f4_joint_arm_live_at_3669_is_fresh` (`[0, 56]`). `r48_joint_supply_is_fresh_at_7469` passes, as expected: no arm at 7469 under BASE. The clean control passes all three. **UNCAUGHT at 10669, 13469 and 16669** (24, 19, 24 stale reads) until X-5c-F4-F6 lands |
| **M-GATE-TWIN-SKIP** (a failing twin is not counted) | copy of `e4_class_match.py` | caught by self-test `unmatched-twin-fail` (`SELF-TEST FAILED`). On the held-out file it would read exit 0, `unmatched=0` |
| **M-GATE-GE-OFF1** (`ge` reads `>= want - 1`) | copy of `e4_class_match.py` | caught by self-test `unmatched-ge-below`. On the MP file it would move 26958 into class 2 (`refused_old_header=8 … unmatched=1`) |

## 8. Gaps and anomalies, verbatim where possible

1. **The files of record are not in `runlogs/` yet.**
   - The card says: "The orchestrator copies `$A/X-5c-F4.final.*` (never `void.*`) to `runlogs/` under the same names, and records each JSON's sha256 against its `.prov`."
   - `runlogs/` holds only `X-5c-F4.final.cpu.{arms,out,err}` and `X-5c-F4.final.runner.out`. The QUEUE row records only main's sha.
   - **This gate did not copy them.** The request makes `runlogs/X-5c-F4.final.*` read-only for the gate, and the card gives the step to the orchestrator.
   - The gate read `$A` in place. `gate-X-5c-F4-final/A.sha256` is a manifest of all 29 `X-5c-F4.final.*` files, against which the copy can be checked. There is no `void.*`.
2. **Not done by this gate, because the branch was read-only:**
   - REFRESH 10, "Reading of (e) here": "P's gate of this run commits the three census lines and ESCALATEs".
   - GATE-PROMPT step 7's verdict commit.
   - The three lines are in §5 above. X-5c-F4-F6 carries the README record.
3. **No record of the post-launch property check.** REFRESH 11a's post-launch `systemctl --user show … -p MemoryMax …` is not in QUEUE or RUNLOG. The unit was `--collect`ed, so `MemoryMax=24G` cannot be confirmed now. The peak was 12.9 G with no OOM, so it did not bind.
4. **Locations differ from the card; the content is complete.**
   - The VOID evidence is in `runlogs/X-5c-F4.cpu-void-{1,2}/`, with arms, out and err. The card says `~/.cache/nv3-review/X-5c-F4.cpu-void.$n.out`.
   - The rehearsal log is `runlogs/X-5c-F4.rehearsal.out`. The card says `~/.cache/nv3-review/X-5c-F4.rehearsal.log`.
   - The agent attempt noted the log as absent.
5. **The X5-R52 (g) rule.** It says "any failing twin entry is a STOP". The held-out twin failure is that case. The compare line runs on main only, so it could not see it; the class match did.
6. **The debug test budget is already exceeded.** X-5c Work 7's "the gate's workspace test stays under 60 s for this crate" no longer holds at T: the `tests/workload.rs` binary takes 167 s in debug. This is noted for X-5c-F4-F6's sizing and is not a finding against this run.

## 9. Follow-up cards

### X-5c-F4-F6: the held-out joint-arm pin (G5-CENSUS (e) 2). Test-only. **Required before E4 is judged.**

**Header.**
- Class: test.
- Gate: P.
- Base: `card/v3-X-5c-F4` at T `cb2aca0d9b5f15e13b29d81dae8f992b7127bf70`.
- Touch only: `nucleus/v3/spikes/e4-recovery/tests/workload.rs` and `nucleus/v3/spikes/e4-recovery/README.md`.
- Runner: one GLM attempt is enough. No campaign-class command runs.

**Why.**
- The final run's held-out census read `JOINT_ARM_CENSUS set=held-out seeds=10000 arm_rounds=3 stale_reads=0 arm_seeds=[10669,13469,16669]`, and its Acceptance line 13 failed.
- G5 (e) 2, as REFRESH 11 reads it, says: "a live arm with stale_reads=0 needs a required pin before E4 is judged, and the run stands once that pin passes".
- Main's arms, 1069 and 3669, are pinned by X-5c-F4-F4.

**Work.**
1. **Add `x5f4_joint_arm_live_at_10669_is_fresh`** right after `x5f4_joint_arm_live_at_3669_is_fresh`, in its exact form:
   - `let rec = workload::run_seed_with(10669, None, &workload::Tuning::FINAL);` (held-out's tuning is FINAL: `CampaignSet::HeldOut.tuning()`);
   - `assert_eq!(rec.twin_joint_arm.len(), rec.rounds.len(), …)`;
   - `assert_eq!(rec.twin_joint_arm, [false, true], …)`;
   - `assert!(rec.twin_joint_stale_reads.iter().all(|n| *n == 0), …{:?}…)`, so that the message prints the counts.
   - The doc comment states the census line verbatim, the measured literal, and the G5 (f) rule at FINAL: a failure is a **STOP for P, never an edit**. Do not re-pick the seed, retune, or loosen an assertion.
2. **Optional, P's call at F6's gate.** The same form at 16669 (`[false, true]`, about 3.6 s release) and 13469 (`[false, true, false]`, about 14.6 s release). The rule's minimum is 10669. 13469 costs about 3 to 4 times a typical pin, in a binary that already takes 167 s in debug under `gate.sh`.
3. **README.** In the final-run record:
   - the three census lines verbatim, as in §5;
   - "held-out's live joint arms (10669, 13469, 16669) read zero stale reads; G5-CENSUS (e) 2 discharged by `x5f4_joint_arm_live_at_10669_is_fresh` (X-5c-F4-F6)".

**Literals** (measured by this gate at T; digests equal the held-out file `e655a61b…`): 10669 has 2 rounds, `twin_joint_arm [false, true]`, stale `[0, 0]`, digest 3149242640.

**Mutant, must fail, then revert.** G5-R48-TREADS-KEPT: delete the one `treads.clear();` in the R21 (c) joint arm (`src/workload.rs:2661` at T, immediately before `joint_arm_ran = true;`).
- The new pin must fail its stale-read assertion with `[0, 24]`.
- The 1069 and 3669 pins fail too, with `[0, 12]` and `[0, 56]`.
- After the revert, `git diff` is clean.

**Acceptance.**
- `cargo test -q --release -p e4-recovery` passes.
- `git diff card/v3-X-5c-F4...HEAD --stat` lists only the two files.
- No file under `src/` changes.
- `scripts/gate.sh` passes.

**The gate's check.** The F6 gate re-runs seed 10669 alone at FINAL and confirms its digest is still 3149242640. A test-only change moves no digest, so no campaign re-runs.

**Limits.**
- No change to `src/`, `Tuning::FINAL`, `MP_PRESSURE`, the class table, the matcher, or any existing assertion.
- Held-out seeds may now be run, because every set's `.prov` has `exit=`.

**After F6's P gate passes:** G5 is discharged for this run. **E4 is not closed by F6.** The held-out and MP-pressure class-match readings stay with the owner.

### X-5c-F4-F7 (proposed; P rules, the owner decides): the twin re-seed imports real-chain media damage, at held-out 17948 twin 2

**What it asks P.** Work 2.5 defines the twin as "the same seed, schedule and F-CRASH resolutions, with the media faults removed — judged as class C". Does that allow a twin re-seeded from a real MP or MW chain that carries earlier media damage? The re-seed comes from X-5c-F4-F1-F1-F3-F2-F1-F2's directory re-seed (`dirs-differ`), and from the broken-chain re-seed, which has the same clone.

**The evidence** is in §2: the four `GATE_TWIN` and `GATE_RESEED` lines, the diagnostic blocks, and the code lines.

**What it should produce.**
- **(i) P's ruling.** Either the twin harness defect is a known limit, or it needs fixing. If fixing, say how: for example, re-seed only from undamaged state, or judge an inherited-damage twin as the real class.
- **(ii) A count, only if the owner allows a diagnostic over the judged files.** Count how many MP and MW seeds' twins re-seeded from a damaged chain in each set. Gathering it needs a stderr-only replay of every MP and MW seed; that is not a set rerun for evidence, but it is the owner's call.

**Limits.** No fix and no rerun without the owner.
- Twin outcomes are digest inputs (`SeedRec::finish` folds `self.twin`).
- So a fix moves the digest of every affected seed, and the final files would no longer reproduce.
- That makes a new final run an owner decision under §10 item 12.

### X-5c-F4-F8 (proposed; the owner decides): MP-pressure 26018 r0 and 26958 r0 under MP-A

**Mostly an owner decision.** It needs a card only if the owner wants more evidence.
- **The facts (§2):**
  - both rounds are refused opens (`c=null`, `frontier=0`);
  - `skx=true` and `smx=true` each imply `workload_served` empty, with no R-taint and no uncovered accepts;
  - twin 0 passes in both;
  - the only difference from classes 1 and 2 is `rlen`.
- **Option (a): extend MP-A's narrow form so it no longer constrains `rlen`.**
  - It is a post-data amendment, recorded as such.
  - The table gets a new sha. "No edit to the table or the script after launch" yields only to an owner ruling.
  - The matcher is then re-run on the MP-pressure file. Its reading would be expected to change from `unmatched=2` to `unmatched=0`.
- **Option (b): keep E4 open on these two until a diagnostic replay shows what was read and refused.** That means the recovery error and the served ranges, the way the class table's own replay did it.

## 10. What this gate could not check or did not do

- **`MemoryMax` and the other unit properties:** see Gaps 3.
- **Whether any held-out seed beyond the census's three has a live arm.** That would need a full held-out rerun, which is forbidden. The census line is the record, and the gate confirmed its three seeds.
- **Seeds in 10000..30000 run before launch.** The scan covered `~/.cache/nv3-target`, `~/.cache/nv3-review` and `/tmp`, plus the chain's commits and README. Anything outside them was not checked. The probe form has refused such ranges since X-5c-F4-F2.
- **The root cause of 26018 and 26958** beyond their recorded flags. The gate did not root-cause them.
- **GATE-PROMPT step 3, the correctness review of the diff.** There is no diff: HEAD equals the Base. The code under test was reviewed in its own cards' gates.
- **Timing.** The pin-cost times in §5 and the 167 s debug binary time come from this gate's runs. They were measured at low load (§5) or with the host to itself (§6), and they are only estimates for F6's sizing. No verdict input depends on timing.

## Files (all under `~/.cache/nv3-review/gate-X-5c-F4-final/` unless named)

- **Match outputs:**
  - the no-diag runs: `{main,heldout,mp}.nodiag.{out,err}`;
  - the readings of record: `{main,heldout,mp}.match.{out,err}`;
  - the diagnostic runs: `{main,heldout,mp}.diag/`;
  - the seed lists: `*.diag-seeds`.
- **Exports:** `exp-main/` (T), `exp-heldout/` and `exp-mp/` (diffs in `exp-*.diff`), `exp-reseed/` (diff in `exp-reseed.diff`), `exp-census/` and `exp-census-mut/` (the scratch example).
- **Build trees:** `~/.cache/nv3-target/gate-X-5c-F4-final/` (1.6 G, safe to delete).
- **Evidence:**
  - `sample/`: the 77 re-runs and `plan.json`;
  - `census.final.out` and `census-mut.final.out`;
  - `mut/`: the three mutants;
  - `unmatched/`: the three unmatched seeds' diagnostics;
  - `A.sha256` and `A.ok-files.txt`.
- **Fresh gate:** `fresh-gate.{out,err,rc,start}`.
- **Untouched:** `$A`, `runlogs/X-5c-F4.final.*`, the worktree, the branch, the card, QUEUE, PLAN, the table and the matcher.
