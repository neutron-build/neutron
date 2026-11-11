Card: X-5c-F4-F4
Class: experiment
Card-gate: P
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: T3 review sub-agent, Claude Code (0ff68eda-9b00-4779-b967-4a3033916f67, 2026-11-10)
Date: 2026-11-10
Gate-exit: GATE EXIT=0
Commit: 6af3a6c55c3ac92852dfbf0a8343f16175c3de3f
Mutants: G-FINAL-SYNCOFF-BASE -> x5_r37_final_is_the_kept_setting, G-FINAL-W11-OFF -> x5_r37_final_is_the_kept_setting, M-FINAL-NOT-73 -> x5_r37_final_is_the_kept_setting, M-MP-GC -> x5_r37_final_is_the_kept_setting, M-BASE-EDIT -> x5_r37_base_tuning_reproduces_1b0ce54e, G5-R48-TREADS-KEPT -> x5f4_joint_arm_live_at_1069_is_fresh + x5f4_joint_arm_live_at_3669_is_fresh, G-CPU-S1-IS-HEAD -> x5_cpu_term.sh --self-test, G-CPU-BAND-FROM-FINAL -> x5_cpu_term.sh --self-test, M-CPU-DRYRUN-MULTI -> x5_cpu_term.sh --self-test, M-GUARD-NOFOREIGN -> x5_guard.sh --self-test, M-GUARD-BOUNDARY -> x5_guard.sh --self-test, M-GUARD-TIMEOUT-CLEAN -> x5_guard.sh --self-test, M-GUARD-EXIT0 -> x5_guard.sh --self-test, OWN-CPU-DRY-WRITES -> x5_cpu_term.sh --self-test, OWN-CPU-FINAL-IS-S1 -> x5_cpu_term.sh --self-test, OWN-GUARD-WAIT-IGNORED -> UNCAUGHT, OWN-GUARD-NO-STEAL -> UNCAUGHT
Followups: X-5c-F4-F4-F1

## Plain statement

The card did what it was written to do. `Tuning::FINAL` is rung 7.3, and only its four constants changed. The 10k at FINAL reproduces the F3 gate's rung-7.3 table on every row. The census names exactly the two seeds that are now pinned, and both pins catch the mutant they exist for. All 13 card mutants are caught, and the fresh gate is green.

The 3,000-second box holds as an upper bound. The orchestrator's 10k ran in 2,775.5 s with 15.6% foreign CPU, which leaves 7.5% headroom. Load can only lengthen wall, so an idle host would be no slower.

The remaining gaps are small, and none blocks a merge:
- The guard reports load from before its own wait. That is why "load 22 at start" was read.
- Two of my own guard mutants survive.
- A `BAND` log line in `x5_cpu_term.sh` loses its value.
- One record line promises a commit that will not come.

The worker again ran banned work inside its attempts. Nothing from it was committed.

## How the gate was run

- **Fresh gate.** The scaffold's `fresh-gate.sh` was extracted through the `--no-local` clone (scaffold `187cb226a9bf57ab2a7ba6b2b90fb28cdaf27c6b`). Lines:
  - `fresh-gate: commit 6af3a6c55c3ac92852dfbf0a8343f16175c3de3f`
  - `fresh-gate: base 187cb226a9bf57ab2a7ba6b2b90fb28cdaf27c6b`
  - `fresh-gate: checks passed (4533 files hashed in 6.87 s)`
  - `fresh-gate: 2 file(s) matched through their eol conversion`

  Steps test, clippy, fmt, deny, wasm32, core-paths-selftest and enforcement-tests each read EXIT=0. The last line is `GATE EXIT=0`. The extract dir was removed.
- **Scratch.** Mutants and the quota reader ran on one `git archive` export of the tip, taken from the worktree root into the gate's scratchpad, with its own target directory. The worktree never held a gate edit. The export's `src/` and `tools/` were compared against HEAD after every mutant.
- **Host.** It was not idle. A foreign pytest ran at about one core, and the load was about 25 when the gate started. No timing in this verdict comes from the gate's own runs.
- **Gate hygiene.** The gate ran no range, probe, `verify` or `seeded-faults`, and no seed in `10000..30000`. Only tests and the card's scratch tests ran. Seeds 1069 and 3669 ran inside the pins.

## Findings by question

### 1. Scope: exactly the four constants

- `card/v3-X-5c-F4-F3..HEAD` changes exactly the five Touch-only files: `src/workload.rs` +22/−9, `tests/workload.rs`, `tools/x5_cpu_term.sh`, `tools/x5_guard.sh` and `README.md`.
- The `src/` diff is `ckpt_every_lo` 4→8, `chunked_ops_lo` 320→700, `chunked_ops_span` 381→201 and `value_lo` 8→280, plus FINAL's doc comment. No other line changed. `BASE` and `MP_PRESSURE`'s explicit fields are untouched.
- `git diff db572b5f HEAD -- src/` over the `pub const FINAL` block differs in doc lines only. FINAL equals rung 7.3 field for field. The rest of the diff is MP_PRESSURE's 9.1 fields, which come from the Base and not from this card.
- The test diff adds three tests and changes no existing one. `x5_r37_final_r41_is_s1`, `x5_r37_final_stays_in_card_ranges`, `x5_r37_mp_pressure_r41_equals_final` and `x5_r37_mp_pressure_differs_only_in_ladder_fields` are unchanged and pass.
- `### X-5c-F4-F3 ladder` and `### X-5c-F4-F3 probe rows` (407 lines) have the same sha256 at the Base and at HEAD (`da2c66a9…`).
- No `unsafe`, no new dependency, and no non-test `unwrap`, `expect` or `panic`.

### 2. The quota rows of the 10k (recounted from the JSON)

The file is `~/.cache/nv3-review/runlogs/X-5c-F4-F4.main.json`, sha256 `549495c5c59aa0c6f605164aec6683f26363165a3b78a641cb470045f229e330`. It is byte-identical to `$CARGO_TARGET_DIR/x5f4/main.json`. A scratch example read it through the crate's own `campaign::parse_campaign_with_set`, `workload::quotas` and `campaign::q4i_count`: `set=Some("main") seeds=10000`.

| Row | This 10k | Min / bar | F3 gate's 7.3 table | Result |
|---|---|---|---|---|
| SZC | 2,491 | 1,000 | 2,491 | met |
| SZI | 779 | 300 | 779 | met |
| SZC-MW | 130 | 50 | 130 | met |
| Q10 | 615 | 500 | 615 | met |
| Q5a | 210 | 200 | 210 | met, thin |
| Q4 | 1,713 | owner bar 1,500 | 1,713 | met |
| Q4-I | 1,922 | owner bar 1,500 | 1,922 | met |
| Q7 | **50** | 200; floor 190 | 50 | **short, as stated; X-5c-F4-F5's** |
| Q8 / Q11 / Q12 | 335 / 342 / 260 | 100 | same | met |
| Q18 | 4,405 | 1,000 | 4,405 | met |
| Q1 | every hook 760–801 | 300 | ≥ 760 | met |
| SZC-MP (INFO in main) | 6 | — | 6 | — |

Every other 5.6 row is met: Q2 4,310, Q3 1,245, Q5b 7,155, Q5c 5,594, Q6a 2,213, Q6b 1,320, Q9 10,013, Q13a–c 904/955/923, Q14 482, Q15 1,200, Q16 6,881 and Q17 10,233.

**Every row equals the F3 gate's 7.3 table. There is no STOP.** The stdout reads:
- `E4 SEEDS=10000 O1_FAIL=2055 O2_FAIL=0 O3_FAIL=11 DEADLOCK=0 PANIC=0 O1F_FAIL=2056 MONITOR=0`
- `E4 FOLD FAILING_SEEDS=11 FAILING_ROUNDS=11 TWIN_FAILING_ROUNDS=0 DEFECT_SEEDS=0`

The fold is the same 11 as at F3.

Q7 reads as the record states it (item 5): 50, short of row 200 and floor 190, handed to X-5c-F4-F5. Until -F5 lands a fix, the final run's main `verify` would exit on it.

### 3. Is the 3,000-second box honestly supported?

**Yes, as an upper bound. A separate clean idle timing is not needed before the final run.**

- **The guard line.** `GUARD wait=ok load_at_start=22.35 22.88 23.47 wall_s=2775.529 cpu_s=37036.662 busy_s=42812.990 foreign_pct=15.60 verdict=VOID`.
- **`load_at_start` is read before the wait** (`x5_guard.sh` line 111 reads it, then `wait_load` runs at line 112). It is the load the wait cleared, not the load when the command started. `wait=ok` means the 1-minute load fell below 2 before the 10k began. The timestamps agree:
  - `seeded.out` was written at 18:19:15;
  - `main.json` was written at 19:10:10;
  - that is 3,055 s, of which 2,775.5 s is the 10k, so about 280 s went to the wait and start-up.
- **The foreign share.** 15.6% is about two cores' worth on average across the run. That was the unrelated host load. The VOID verdict comes from this share alone.
- **The readings agree with each other.**

  | Reading | Wall | Foreign CPU |
  |---|---|---|
  | F3 probe extrapolation | 45.4 min | 8.1% |
  | This single-process 10k | 46.3 min | 15.6% |
  | F3's single-process 10k | 52.8 min | 37.5% |

  The trend runs one way: more foreign load, more wall. CPU contention can only lengthen wall, so an idle-host 10k reads at most 2,775 s, 225 s (7.5%) under 3,000.
- **No separate timing is needed.** A dedicated idle 10k would cost another ~46 minutes and could only confirm a number already under the box. The final run's own guarded `run` line is the timing of record.
- **What does need an idle host:**
  - **The final run itself.** This Acceptance was not kept idle, despite the card's Notes.
  - **The pending idle-host `x5_cpu_term.sh` measurement.** It is separate from the box.
- **The tight margin is `verify`.** The box does not bound it, and the runner kills at 60 minutes. At about 46 minutes per pass, that leaves about 14 minutes. Goal 2 already asks P's REFRESH to say so.

### 4. The joint-arm pins against G5-CENSUS (e) 2

- **The census line is exact.** `JOINT_ARM_CENSUS set=main seeds=10000 arm_rounds=2 stale_reads=0 arm_seeds=[1069,3669]`, the line the Acceptance regex requires.
- **Measured before pinning.** Step 2 measured before it pinned, as the card requires. Each seed has `rounds.len()` 2, `twin_joint_arm` `[false, true]` and `twin_joint_stale_reads` `[0, 0]`. That is one arm round per seed with no stale reads, matching `arm_rounds=2`. No STOP condition was met.
- **What the pins assert.**
  - **(a)** One diagnostic entry per executed round.
  - **(b)** The arm runs in round 1 and in no other round, written as the literal `[false, true]`.
  - **(c)** Every stale-read entry is 0, and the failure message names G5-R48-TREADS-KEPT.

  Both docs name the census line, verdict `2c041aae`, G5-CENSUS (e) 2, and the rule that a moved arm is a STOP for P, never an edit.
- **The mutant.** The gate re-ran G5-R48-TREADS-KEPT. Both pins fail on the stale-read assertion:
  - `seed 1069 … [0, 12]`;
  - `seed 3669 … [0, 56]`.

  These are the README's counts, 12 and 56.
- **BASE is unchanged.** `r48_joint_supply_is_fresh_at_7469` still passes at BASE under the mutant, as recorded.

The ruling's two conditions are met. The STOP is cleared by the required pins, and the census names exactly the pinned seeds.

### 5. The folded follow-ups

- **X-5c-F4-F3-F1 (the record): done.** `### Corrections (X-5c-F4-F4)` quotes the old text for each change:
  - Q7 170, not 190, in item 1, item 3 and item 6. The ladder's own Q7 outcome block correctly stays history, and its correction sits in the list.
  - Item 5 now carries the runner's VOID lines `CPU_TERM … term=56.45 … VOID` and `CPU_WHOLE_FINAL … term=62.03`, withdraws +48.58 and +51.71, and states that no usable CPU figure exists.
  - The F3 hygiene breach is now under Anomalies.

  `x5_r37_final_is_the_kept_setting` writes out every field of FINAL and MP_PRESSURE, with no `..` and no constant reference. I checked it against the card's literal.
- **X-5c-F4-F3-F2 (the CPU tool): done.**
  - `--dry-run` and the measuring form both resolve the arms through `resolve_arms`.
  - The band decision (`band_passes`) reads only the S1 and BASE pass-1 columns.
  - Self-test cases (a), (b) and (c) exist, and the fixture repository is removed afterwards.
  - Acceptance line 4 resolves `s1=84d3af0f…` and `final=6af3a6c5…`, and writes nothing under the prefix.
- **`tools/x5_guard.sh`: done, to the card's form.**
  - The command's stdout, stderr and exit code pass through.
  - The guard writes exactly one `GUARD` line.
  - foreign = busy − cpu, with the same 5.00% rule.
  - The exit code is never the verdict.
  - The self-test covers the four cases the card lists.

  Gaps are in Follow-ups (F1 (a)–(c)).

### 6. Mutants

All 13 card rows were re-applied by the gate on the scratch export or a script copy. Each named test fails by its panic or `self-test FAIL` line:

| Mutant | Caught by (line seen) |
|---|---|
| G-FINAL-SYNCOFF-BASE | `x5_r37_final_is_the_kept_setting` panicked at tests/workload.rs:455 |
| G-FINAL-W11-OFF | same, :455 |
| M-FINAL-NOT-73 | same, :455 |
| M-MP-GC | same, :498 (the MP literal) |
| M-BASE-EDIT | `x5_r37_base_tuning_reproduces_1b0ce54e` panicked at :231 |
| G5-R48-TREADS-KEPT | both pins, stale `[0, 12]` and `[0, 56]` |
| G-CPU-S1-IS-HEAD | `self-test FAIL dry-run-resolves` (s1 = final) |
| G-CPU-BAND-FROM-FINAL | `band-decision-one-pass` and `band-decision-three-passes` |
| M-CPU-DRYRUN-MULTI | `self-test FAIL dry-run-two-s1` (rc=0, CPU_ARMS printed) |
| M-GUARD-NOFOREIGN | `foreign-5-00-clean`, `foreign-5-01-void`, … |
| M-GUARD-BOUNDARY | `foreign-5-00-clean`, `foreign-is-busy-minus-cpu` |
| M-GUARD-TIMEOUT-CLEAN | `wait-timeout-void` |
| M-GUARD-EXIT0 | `wrapped-exit-3` (rc=0) |
| OWN-CPU-DRY-WRITES (dry-run makes `<prefix>.src.S1`) | `dry-run-resolves` |
| OWN-CPU-FINAL-IS-S1 (`ARM_FINAL=$s1_commit`) | `dry-run-resolves` |
| **OWN-GUARD-WAIT-IGNORED** (`wait_load` returns `ok` at once) | **UNCAUGHT**: `GUARD_SELFTEST OK` |
| **OWN-GUARD-NO-STEAL** (busy drops `/proc/stat` field 9, steal) | **UNCAUGHT**: `GUARD_SELFTEST OK` |

No assertion was weakened. Every pre-existing pin is unchanged. The two survivors are outside what the card required the self-test to cover: its self-test is "with no wait", and its fixtures put all busy time in `user`. They are follow-up (b), not rework. On this bare-metal host steal is 0, so OWN-GUARD-NO-STEAL cannot have changed this run's reading. The 280 s gap before the 10k (§3) shows that the real wait did run this time. After every mutant, `src/` and `tools/` in the export matched HEAD.

### 7. Run history and hygiene

- **Logs.**
  - Attempt 1 committed steps 0–5, from 16:21:44 to 16:59:55.
  - From 17:00 it ran the Acceptance lines inside the attempt: the test suite, the self-tests, the dry-run, the build, **`seeded-faults --seeds 0..200`** and then **the guarded 10k**. The runner killed the 10k at its 60-minute cap.
  - Attempt 2 started another 10k and was killed after 30 minutes of silence.
  - Attempt 3's log has no tool call. It was killed after 30 minutes of silence, with 0/0 tokens.

  These runs are banned by Limit 3, Run hygiene and "Do not". No seed in `10000..30000` was touched.
- **Nothing was committed after 16:59:55.** The tip is still `6af3a6c5`, and the worktree is clean apart from ignored files.
- **The files of record are the orchestrator's.** The orchestrator's line 7 begins with `rm -f` of `main.*`, and every `x5f4/main.*` and `seeded.*` file has an 18:16–19:10 mtime.
- **Cause.** `WORKER-PROMPT.local.md` line 43 says "Run every Acceptance command for real". The card bans the long Acceptance lines in the attempt. The worker followed the standing prompt over the card, as in F3. This is the second card in a row, so it is a prompt conflict, not worker error (orchestrator item below).
- **Where the Acceptance ran.** The orchestrator ran the 10 lines in its own shells, not in the runner's container, as the card's Notes intended. The counts are deterministic and equal the F3 gate's independent 7.3 run, so the data stand.

### 8. Record defects (minor)

- **(i)** Record item 7 says "the commit that follows records the lines here verbatim". No such commit exists, and the card forbids one after the attempt. The lines live in this verdict, in "Acceptance lines of record" below.
- **(ii)** `## Card X-5c-F4-F4's record` sits above the `## Card X-5c-F4-F4` heading and its decision section, the reverse of every earlier card. This is cosmetic.

## Acceptance lines of record (RESULT 7, copied by the gate)

```
GUARD wait=ok load_at_start=22.35 22.88 23.47 wall_s=2775.529 cpu_s=37036.662 busy_s=42812.990 foreign_pct=15.60 verdict=VOID
E4 SEEDS=10000 O1_FAIL=2055 O2_FAIL=0 O3_FAIL=11 DEADLOCK=0 PANIC=0 O1F_FAIL=2056 MONITOR=0
E4 FOLD FAILING_SEEDS=11 FAILING_ROUNDS=11 TWIN_FAILING_ROUNDS=0 DEFECT_SEEDS=0
549495c5c59aa0c6f605164aec6683f26363165a3b78a641cb470045f229e330  main.json
JOINT_ARM_CENSUS set=main seeds=10000 arm_rounds=2 stale_reads=0 arm_seeds=[1069,3669]
```

`load_at_start` is the load before the guard's wait (§3), not at the command's start.

## X-5c-F4-F5's main-set comparison baseline

The baseline for -F5's main set (`DIGESTS_MOVED`, the quota table and the census) is **this card's Acceptance 10k at FINAL = rung 7.3, tip `6af3a6c55c3ac92852dfbf0a8343f16175c3de3f`:**

- **The file.** `~/.cache/nv3-review/runlogs/X-5c-F4-F4.main.json`, sha256 `549495c5c59aa0c6f605164aec6683f26363165a3b78a641cb470045f229e330`, with the stdout lines and census above.
- **Its name differs from this card's Notes.** The orchestrator saved it as `.main.json`, not `.main.campaign.json`. -F5 must name this exact path and sha.
- **`main.out` was not copied.** The orchestrator should copy `$CARGO_TARGET_DIR/x5f4/main.out` to `~/.cache/nv3-review/runlogs/X-5c-F4-F4.main.campaign.out` before -F5 launches.
- **What -F5 may move.** Its SZC-MP fix is confined to the MP-pressure set, so main must read `DIGESTS_MOVED 0` for that part. Its Q7 change touches FINAL and moves main digests by design. -F5 must therefore report moved digests per change against this file. It must not compare against the F3 tip (rung 6.1) or `db572b5f`'s tree.
- **Its literal.** -F5 updates `x5_r37_final_is_the_kept_setting` in the same commit as any change to FINAL or MP_PRESSURE.

## For the orchestrator (not cards)

- **The prompt conflict.** `WORKER-PROMPT.local.md` line 43 ("Run every Acceptance command for real") conflicts with cards that ban range runs inside the attempt. That produced two breaches in a row and three killed attempts here. It needs a ruling before -F5 runs: either the worker prompt defers to a card's ban, or long lines leave Acceptance.
- **An idle host.** Keep the host idle for the final run. This Acceptance was not idle.

## Follow-ups

**X-5c-F4-F4-F1 (tools and record; may be folded into X-5c-F4-F5, which touches the same tools):**

(a) **Guard load reading.** `x5_guard.sh` should record the load after the wait (as `load_at_start`, keeping the pre-wait reading under its own name, such as `load_before_wait`), because the current field led the orchestrator to read "load 22 at start" on a run that started below 2.

(b) **Guard self-test gaps.** The self-test should cover:
- the wait decision, through an injectable load source (OWN-GUARD-WAIT-IGNORED survives);
- a fixture `/proc/stat` pair whose busy time sits in `nice`, `irq`, `softirq` and `steal` (OWN-GUARD-NO-STEAL survives).

(c) **The lost BAND value.** In `x5_cpu_term.sh`'s measuring form, `np=$(band_passes)` runs in a subshell, so `BAND_TERM` never reaches the parent. The `BAND pass1_term=… inside +55..+65` line prints an empty value. Only the log line is affected; the decision is correct. Fix it with `band_passes` writing to a global and returning a status, or with the term recomputed in the caller. Add a self-test case that checks the logged value.

(d) **The record.** Replace record item 7's promise of a following commit with a pointer to this verdict's "Acceptance lines of record". Optionally, move `## Card X-5c-F4-F4's record` below the card's decision section.
