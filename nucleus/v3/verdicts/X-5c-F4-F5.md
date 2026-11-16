Card: X-5c-F4-F5
Class: experiment
Card-gate: P
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: T3 review sub-agent, Claude Code (de50780a-728f-4a11-98f2-ff60df0ce214, 2026-11-10)
Date: 2026-11-10
Gate-exit: GATE EXIT=0
Commit: 7a1eb285b614fe68876a905db8c55509bb552516
Mutants: M-FILL-IN-FINAL -> x5f5_final_digests_unmoved, M-FILL-EVERY-ROUND -> x5r55_fill_not_drawn_at_zero, M-FILL-MAIN-STREAM -> x5r55_fill_leaves_the_rest_of_the_script, M-FILL-DUP-KEY -> x5r55_fill_shape, M-FILL-LATE -> x5r55_fill_shape, M-MP-FILL-OFF -> equivalent (MP_PRESSURE's fill is already 0; ladder A kept the anchor), M-W7-NONDROP -> x5r56_w7_not_drawn_at_zero, M-W7-TARGETED -> x5r56_w7_not_drawn_at_zero, M-W7-SHARED-STREAM -> x5r56_unselected_round_keeps_its_point, M-W7-W11-LIST -> x5r56_w7_pick_lands_in_the_walk, M-W7-IN-MP -> x5_r37_final_is_the_kept_setting, M-BASE-EDIT -> x5_r37_base_tuning_reproduces_1b0ce54e, OWN-W7-LIST-NEVER-FOLDED -> x5r56_w7_pick_lands_in_the_walk, OWN-W7-SELECT-INVERT -> x5r56_unselected_round_keeps_its_point, OWN-W7-START-EARLY -> UNCAUGHT (no observable effect: 0 of 78 picks miss), OWN-W7-END-LATE -> UNCAUGHT (1 of 78 picks misses the walk)
Followups: X-5c-F4-F5-F1, X-5c-F4-F5-F2

## Plain statement

Both ladders followed their pre-registered rules and stopped where those rules say. No post-data choice was made.
- **Ladder A** stopped at its anchor A.0, so the X5-R55 fill is 0 in every tuning. The anchor met SZC-MP through the `value_lo` 280 that MP_PRESSURE inherits from FINAL since X-5c-F4-F4, not through the fill.
- **Ladder B** stopped at B.2 (`w7_share` 200).

The gate ran the main 10k at HEAD on an idle host, behind the load guard. **Every exit row holds there together:**
- Q7 442, Q4 1,713, Q4-I 1,921, Q5a 449, Q10 610;
- SZC 2,421, SZI 777, SZC-MW 130;
- the census and the 11 failing rounds are unchanged;
- MONITOR 0, DEFECT_SEEDS 0;
- wall 2,715.7 s, under 3,000 s.

All 390 seeds that moved are uniform-mode. All 401 `w7` rounds crash inside a DropTree walk and pass recovery.

All 12 card mutants were re-applied: 11 are caught and 1 is equivalent, as the card allows. Two of my own four mutants survive. They test the walk window's edges, and they become follow-up F1. The fresh gate is green.

**This gate's HEAD 10k is the main-set baseline for the final run.**
- File: `~/.cache/nv3-review/runlogs/X-5c-F4-F5.head.main.json`
- sha256: `cb947c1950ee0f5a35fff45f572630328a0d34e2eb2b5fcbbecc1d2cc1b6ae45`

## How the gate was run

- **Fresh gate.** The scaffold's `fresh-gate.sh` was extracted through the `--no-local` shallow clone (scaffold `187cb226a9bf57ab2a7ba6b2b90fb28cdaf27c6b`). Lines:
  - `fresh-gate: commit 7a1eb285b614fe68876a905db8c55509bb552516`
  - `fresh-gate: base 187cb226a9bf57ab2a7ba6b2b90fb28cdaf27c6b`
  - `fresh-gate: checks passed (4534 files hashed in 1.64 s)`
  - `fresh-gate: 2 file(s) matched through their eol conversion`

  Steps test, clippy, fmt, deny, wasm32, core-paths-selftest and enforcement-tests each read EXIT=0. The last line is `GATE EXIT=0`. The extract dir was removed.
- **Scratch.** Every build ran from a `git archive` export of `7a1eb285`, each with its own target directory. The worktree never held a gate edit, and `git status` is clean.
- **The HEAD 10k ran alone.** The host was idle when it started (load 0.57). The fresh gate, the mutants and the MP read all ran after it finished.

## Findings

### 1. The HEAD main 10k (the gap the card left)

Command: `x5_guard.sh … e4-campaign run --set main --threads 16`, built from the HEAD export, under `ulimit -v 25165824`. Output: `~/.cache/nv3-review/runlogs/X-5c-F4-F5.head.main.{json,out,err}` and `.head.guard`.

- `GUARD wait=ok load_at_start=0.60 3.20 14.13 wall_s=2715.663 cpu_s=36954.883 busy_s=40616.110 foreign_pct=9.91 verdict=VOID`
  - The verdict is VOID on 9.9% foreign CPU: other sessions' work plus the gate's own light reads.
  - Load can only lengthen wall, so 2,715.7 s is an upper bound. It is 284 s (9.5%) under the 3,000 s box, and agrees with F4-F4's 2,775.5 s and the A-end's 2,706.5 s.
- `JOINT_ARM_CENSUS set=main seeds=10000 arm_rounds=2 stale_reads=0 arm_seeds=[1069,3669]`
- `E4 SEEDS=10000 O1_FAIL=2060 O2_FAIL=0 O3_FAIL=11 DEADLOCK=0 PANIC=0 O1F_FAIL=2061 MONITOR=0`
- `E4 FOLD FAILING_SEEDS=11 FAILING_ROUNDS=11 TWIN_FAILING_ROUNDS=0 DEFECT_SEEDS=0`

The rows were recounted from the JSON by a scratch example that calls the crate's own `parse_campaign_with_set`, `workload::quotas`, `is_exit_row`, `verify_quota_missed` and `q4i_count`. On F4-F4's file it reproduces that verdict's table exactly.

| Row | HEAD | F4-F4 (A-end, identical) | Required / bar | Result |
|---|---|---|---|---|
| Q4 | 1,713 | 1,713 | owner 1,500 | met |
| Q4-I | 1,921 | 1,922 | owner 1,500 | met |
| Q5a | 449 | 210 | 200; floor 200 | met |
| **Q7** | **442** | 50 | 200; floor 190 | **met** |
| Q8 | 323 | 335 | 100 | met |
| Q10 | 610 | 615 | 500 | met |
| Q11 | 320 | 342 | 100 | met |
| Q12 | 254 | 260 | 100 | met |
| Q18 | 4,405 | 4,405 | 1,000; floor 990 | met |
| SZC | 2,421 | 2,491 | 1,000 | met |
| SZI | 777 | 779 | 300 | met |
| SZC-MW | 130 | 130 | 50 | met |
| Q1 hooks | 760–801 | 760–801 | 300 | met (unchanged) |

- **Every other 5.6 row is met:** Q2 4,308, Q3 1,246, Q5b 6,967, Q5c 5,396, Q6a 2,212, Q6b 1,320, Q9 9,890, Q13a–c 897/948/920, Q14 495, Q15 1,200, Q16 6,789 and Q17 10,087.
- **`missed_exit_rows 0`.** SZC-MP reads 6, which is INFO in main.
- **`MP_REFUSAL rounds=2220 refused=1746 rate=0.7865`.**
- **Failing rounds.** The same 11 MP rounds as F4-F4: 1257, 2927, 3988, 5168, 5808, 7528, 8657 and 8728 in round 0, and 7177, 7727 and 9328 in round 1. No new failure and none healed.
- **Joint.** All rows hold together at HEAD, not ladder by ladder.

### 2. Rules fixed before measuring

- `### X-5c-F4-F5 rules and ladders` was committed at step 0 (`ccecb0cd`, 19:41), before the first probe (A.0, 19:56). It has the same sha256 at step 0 and at HEAD (`b4d32acc…`).
- Its ladder tables, hold sets, margins and wall rule match the card's.
- Each rung commit changes only its constant: B.1 and B.2 change only FINAL's `w7_share`, and A.0 changes no constant.
- **Ladder A** stopped at the first rung by the stop rule: raw 19 ≥ 13, empty hold set, wall 6.9 ≤ 40.
- **Ladder B** stopped at B.2 by the stop rule:
  - B.0 equalled F3's 7.3 probe row;
  - B.1 read exactly 200, a `thin` candidate under the 250 margin, so the walk continued;
  - B.2 read 410 ≥ 250, with the hold set and the wall passing.
- B.1's GUARD was VOID, but its wall (47.3) was under the limit, which the wall rule accepts.
- **The walk pin's seed is the rule's seed, not a luck scan.** The gate's scan finds seeds 10–17 have no drop in round 0, and 18 is the first with a non-empty walk list.
- **The one post-probe commit is test-only.** `0bc00c6a` adds seed 111 to `x5r56_unselected_round_keeps_its_point` so that M-W7-SHARED-STREAM is caught. The card fixed no seed for that pin, and the pin was only strengthened.

### 3. X5-R55: confined to MP_PRESSURE, and exercising nothing at the kept setting

- **The A-end compare reproduces.** Re-run by the gate: `DIGESTS_MOVED 0 FAILING_ROUNDS 11 MONITOR 0`, with every list empty, identical to the runner's output.
- **The A-end JSON is byte-identical to F4-F4's file of record** (both sha256 `549495c5…`). The A-end tree holds no `w7_share` or `drop_walk_yields`.
- **The fill is dead code at the kept constants.** Ladder A stopped at A.0, so `r0_fill_txns` and `r0_fill_ops` are 0 in BASE, FINAL and MP_PRESSURE.
  - So the question "does the fill exercise a large dirty interval cut by a crash with media faults, or inflate the count?" does not arise in this tree. The code ran only in the pins.
  - The pins hold its shape, and the five fill mutants are caught.
- **What did raise SZC-MP** is the anchor's `value_lo` 280, inherited from FINAL. It is an honest workload change: values of 280–400 B grow the dirty set in round 0 itself.
- **The probe's ×10 over-reads, but the row holds over the full set.** The gate read HEAD's MP_PRESSURE over the 2,000 MP-class seeds of `0..10000`, using the F3 gate's scratch route (a scratch `probe_mp_seeds()`, never `10000..30000`):
  - **SZC-MP 160 per 2,000** (probe ×10: 190; required 100);
  - `MP_REFUSAL rounds=2327 refused=1646 rate=0.7073`;
  - by round, szc/szq: r0 403/140, r1 89/19, r2 1/1. Round 0 now carries the condition;
  - 12 failing rounds, all class MP, the same O-3 fold shape as main.

  Not classified here, because the class table is P's at judgement.

### 4. X5-R56: targeted, and an honest test of DropTree-walk recovery

- **Code.**
  - `w7_pick` returns `None` (no stream) at share 0, in a targeted round, and in a non-drop round.
  - The call site is inside the `if uniform` branch. An unselected round runs exactly `window_crash_point`.
  - `S_W7` (19) is a new tag. The window list is folded at `h:dropdone` from `yields + 1` at `h:dropwalk` through the last yield before the close. It is never a digest input.
- **Moves at HEAD.** 390 seeds moved against F4-F4.
  - Every one is uniform-mode (`(s/10)%2==1`). No targeted seed moved, so `r41`'s rounds, the Q1 hooks and Q4 (1,713) are untouched.
  - Every moved seed has a `w7` or `w7-` round. Every round before a seed's first pick is field-for-field unchanged.
  - Labels: 401 `w7` and 1 `w7-`.
- **Honesty.** All 401 `w7` rounds have `q7_drop_walk` set, fired and passed. `q7_drop_walk` is the harness's real `drop_walk` flag at the crash, not the label.
  - Plain-uniform Q7 rounds fell from 46 to 36, because picks displaced them. The rest of the gain comes from crashes landing inside a walk.
  - Q5a rose from 210 to 449: 201 `w7` crashes land with a ≥ 2-segment chunked transaction open. That is a real state at the crash, because the walk runs alongside the chunked commit. It is not double counting, but about 45% of Q5a now depends on `w7`.
  - Q8, Q11, Q12 and SZC each lost a little and stay well above their rows.

### 5. Untouched sets

The gate parsed every tool command in both attempt logs (186 and 20 tool calls):
- no `verify`, `seeded-faults` or `run --set`, and no seed in `10000..30000`;
- the only MP route used was `--probe mp-pressure`;
- `10000..30000` appears only as card or README text;
- attempt 2 ran single seeds 7, 100 and 1234 on the A-end build.

The runner's Acceptance ran `--set main` (`0..10000`) and `seeded-faults 0..200` only.

### 6. Mutants

- **Card mutants.** All 12 were re-applied on the scratch export. Each ran the 58 `x5` tests and was reverted.
  - 11 are caught by their named test, each with a FAILED line.
  - **M-MP-FILL-OFF** is a no-op: MP_PRESSURE's fill is already 0, so it is equivalent by the card's own clause. A fill set in MP_PRESSURE would fail `x5_r37_final_is_the_kept_setting`'s literal.
- **Own mutants.**
  - OWN-W7-LIST-NEVER-FOLDED (the fold never extends the list) is caught by `x5r56_w7_pick_lands_in_the_walk`.
  - OWN-W7-SELECT-INVERT (`>=` for `<`) is caught by `x5r56_unselected_round_keeps_its_point`.
  - **OWN-W7-START-EARLY** (`yields` for `yields + 1`) survives. A scratch scan at share 1000 over the uniform seeds of `0..400` finds 0 of 78 picks missing the walk, so it has no observable effect there.
  - **OWN-W7-END-LATE** (`last + 1`) survives and is **not** equivalent: 1 of 78 picks lands after the walk closed (`q7` false). No pin checks the window's edges. The real code reads 0 of 78 in this scan and 401 of 401 in the HEAD 10k, so this is a pin gap, not a defect. Follow-up F1.

The scratch export's `git status` was clean after every revert.

### 7. Scope and code

- `card/v3-X-5c-F4-F4...HEAD` touches exactly the four Touch-only files.
- No `unsafe`, no new dependency, and no non-test `unwrap`, `expect` or `panic`. The pub items are documented.
- BASE is unchanged, which `x5_r37_base_tuning_reproduces_1b0ce54e` confirms.
- **`campaign.rs`'s `cwin` reader** maps `"w7"` and `"w7-"` to `None`. The file is outside the Touch-only list, so the card could not change it.
  - `verify` compares digests, `q4i`, `szq`, `szi` and `rfs`, never `cwin`.
  - Q7 counts `q[6]`.
  - The only other caller is a test.

  The loss is cosmetic today. Follow-up F2.

## What remains before the final run

1. **Merge this card.** Then P's X-5c-F4 REFRESH must carry the following:
   - the main baseline, this gate's HEAD 10k: `~/.cache/nv3-review/runlogs/X-5c-F4-F5.head.main.json`, sha256 `cb947c1950ee0f5a35fff45f572630328a0d34e2eb2b5fcbbecc1d2cc1b6ae45`;
   - Q7 is now met (442), and the stated-gap clause of decision (3) is not needed;
   - FINAL = 7.3 + `w7_share` 200, and MP_PRESSURE is unchanged from X-5c-F4-F4;
   - the census line `arm_seeds=[1069,3669]` is unchanged.
2. **Keep the host idle for the final run.**
   - `run` takes about 45 minutes per 10k against the 3,000 s box.
   - `verify`'s re-run takes about the same, against the runner's 60-minute kill. That margin, about 14 minutes, is the tight one.
3. **The pending idle-host `x5_cpu_term.sh` measurement** (from F4-F4) is still open.
4. **F1 and F2** are not blocking. F1 should land before E4 is judged if the pins are to guard the window's edges.

The held-out set and the MP final set have never been run. Their first run is the final run's.

## Follow-ups

**X-5c-F4-F5-F1: pin the drop-walk window's edges.** OWN-W7-END-LATE extends each walk's range one yield past `h:dropdone`, and no test catches it, although 1 pick in 78 then lands after the walk. OWN-W7-START-EARLY also survives, with no observable effect in that scan. Add a test-only pin that, at the test-local tuning `w7_share` 1000 with `w11_share` and `w12_share` 0, runs a fixed seed range (for example the uniform seeds of `0..400`) and asserts that every round labelled `w7` has `q7_drop_walk` set. Show END-LATE failing it. Do not change `src/`.

**X-5c-F4-F5-F2: `campaign.rs` reads back the `w7` labels.** The `cwin` parser maps `"w7"` and `"w7-"` to `None`, so a parsed file loses X5-R56's labels. No consumer reads the label today: digests come from the file, and Q7 from `q[6]`. Add the two arms, plus a round-trip test that writes and parses a record carrying each of `w11`, `w12`, `w11-`, `w12-`, `w7` and `w7-`. This touches `src/campaign.rs`, so it is a protected-path decision for P.
