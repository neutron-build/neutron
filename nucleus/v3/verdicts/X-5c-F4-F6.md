Card: X-5c-F4-F6
Class: test
Card-gate: P
Verdict: MERGE
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: T3 review sub-agent, Claude Code (75ef391a-b3f4-4045-9eb4-2d56a067eb0b, 2026-11-14)
Date: 2026-11-14
Gate-exit: GATE EXIT=0
Commit: daee4f53b1044f6e8091a57bb9cee7b33af51438
Mutants: G5-R48-TREADS-KEPT -> x5f4_joint_arm_live_at_10669_is_fresh ([0, 24]) (and the 1069 pin with [0, 12], the 3669 pin with [0, 56]), M-ARM-OFF -> x5f4_joint_arm_live_at_10669_is_fresh (arm assertion, left [false, false]), M-JOINT-SKIP -> x5f4_joint_arm_live_at_10669_is_fresh (arm assertion, left [false, false]), M-STALE-HIDDEN -> UNCAUGHT (by the pin and by the whole crate suite; a property of the ruled pin form, see finding 1)
Followups: none

**Where this verdict lives.** The request named this file and made the card branch read-only for the gate. So GATE-PROMPT step 7's commit to `nucleus/v3/verdicts/X-5c-F4-F6.md` was **not** made. The orchestrator may commit this file as the branch verdict.

## The verdict in one paragraph

The card does what §9 of `X-5c-F4.final.verdict.md` and the card ask, and nothing more:
- **One new pin.** `x5f4_joint_arm_live_at_10669_is_fresh` is a line-for-line copy of the 3669 pin's form, at `Tuning::FINAL`.
- **One README record.** It carries the three census lines verbatim, the discharge sentence, and the mutant's three failing messages.
- **Scope.** The diff is additions only (106 lines, 0 deletions) in exactly the two Touch-only files.
- **Gate run.** The fresh gate on the full commit passed.
- **Digest.** Seed 10669 re-run alone at FINAL still reads digest 3149242640.
- **The named mutant.** G5-R48-TREADS-KEPT fails the new pin with `[0, 24]`.

**G5-CENSUS (e) 2 is discharged for the final run** (X-5c-F4's run at T `cb2aca0d`), on the reading of §5: held-out's lowest live seed is now pinned, and the pin passes its P gate. **E4 is not closed by this card.** Seeds 17948, 26018 and 26958 stay with the owner, and this gate does not rule on them.

## 1. Fresh gate (GATE-PROMPT step 1)

- **The extract.** Shallow `--no-local` clone of `nucleus/v3-scaffold`, scaffold id **`187cb226a9bf57ab2a7ba6b2b90fb28cdaf27c6b`**. No `EXTRACT FAILED` line.
- **The run.** `cd ~/Workspace/neutron-v3 && PATH=$HOME/.cargo/bin:$HOME/.local/bin:$PATH CARGO_TARGET_DIR=$HOME/.cache/nv3-target/gate-X-5c-F4-F6 bash "$fg/fresh-gate.sh" daee4f53b1044f6e8091a57bb9cee7b33af51438`, 02:58:35 to 03:06:20, rc=0, stderr empty (0 bytes).
- **Its lines:**
```
fresh-gate: commit daee4f53b1044f6e8091a57bb9cee7b33af51438
fresh-gate: base 187cb226a9bf57ab2a7ba6b2b90fb28cdaf27c6b
fresh-gate: checks passed (4539 files hashed in 1.35 s)
fresh-gate: 2 file(s) matched through their eol conversion
STEP test EXIT=0
STEP clippy EXIT=0
STEP fmt EXIT=0
STEP deny EXIT=0
STEP wasm32 EXIT=0
STEP core-paths-selftest EXIT=0
STEP enforcement-tests EXIT=0
GATE EXIT=0
```
- **Literal last line:** `GATE EXIT=0`.
- **Cleanup.** `$fg` (`/tmp/tmp.GqitwS42bV`) was removed afterwards.
- **Run once.** The runner's own `scripts/gate.sh` line also passed (runner log line 5).

## 2. Scope

The scope check used `git diff cb2aca0d...HEAD`, because the card stacks on the chain:
```
 nucleus/v3/spikes/e4-recovery/README.md         | 66 +++++++++++++++++++++++++
 nucleus/v3/spikes/e4-recovery/tests/workload.rs | 40 +++++++++++++++
 2 files changed, 106 insertions(+)
```
- **One commit.** `cb2aca0d..HEAD` is exactly `daee4f53`, with subject prefix `nucleus v3: X-5c-F4-F6 -`.
- **Merge base.** The merge base is `cb2aca0d…`, which is T.
- **Branch refs.** `card/v3-X-5c-F4` = T and `card/v3-X-5c-F4-F6` = `daee4f53…`.
- **Nothing removed or edited.** Zero `-` lines in the diff, so no existing assertion, literal, tuning or comment changed.
- **No `src/` change.**
- **Nothing uncommitted.** `git status --porcelain --untracked-files=all` in the worktree is empty, before and after this gate.
- **Worker commands.** The worker log contains no campaign-class command: no `run --set`, `verify`, `seeded-faults`, `--probe` or stress loop. Its mutant ran on a scratch export under `$CARGO_TARGET_DIR/x5f4f6-scratch/`.

## 3. Review of the pin and the README record

**The test** (`tests/workload.rs:1987-2006` at HEAD):
- **Placement.** It sits right after `x5f4_joint_arm_live_at_3669_is_fresh`.
- **Form.** It has the same three assertions in the same order, with the same message shapes:
  - `run_seed_with(10669, None, &workload::Tuning::FINAL)`;
  - `len == rounds.len()`;
  - `twin_joint_arm == [false, true]`;
  - `twin_joint_stale_reads.iter().all(|n| *n == 0)`, with `{:?}` printing the counts.
- **The doc comment states:**
  - the held-out census line (wrapped across `///` lines inside one code span, the same convention as the 1069 pin);
  - the measured literals: rounds 2, `[false, true]`, `[0, 0]`, digest 3149242640;
  - "STOP for P, never an edit (the card's Limits 2, G5-CENSUS (f)…)";
  - that G5-R48-TREADS-KEPT must fail it with `[0, 24]`.
- **Pins at 13469 and 16669.** None were added, as Work 2 requires.

**The README** (`## Card X-5c-F4-F6's record`, appended at the end):
- **The three census lines.** They sit in one fenced block and match byte for byte:
  - §5 of the final verdict;
  - the lines printed in `runlogs/` (the X-5c-F4 runner's PASS/FAIL lines and the census greps).
- **The discharge sentence.** It is present word for word ("held-out's live joint arms (10669, 13469, 16669) read zero stale reads; G5-CENSUS (e) 2 discharged by `x5f4_joint_arm_live_at_10669_is_fresh` (X-5c-F4-F6)"). It is wrapped across three Markdown lines, which render as one.
- **The mutant's three failing messages.** They are present, with `[0, 24]`, `[0, 12]` and `[0, 56]`, and are identical to what this gate reproduced.
- **RESULT item 3.** "No rule text changed; no `src/` change; post-data amendments: none." is present and true.

## 4. Digest check (seed 10669 alone at FINAL)

- **How it ran.** On a clean scratch export of `daee4f53` (`gate-X-5c-F4-F6/clean/`), using the final gate's scratch example `gate_census.rs` (`run_seed_with(seed, None, &Tuning::FINAL)`):
```
GATE_CENSUS seed=10669 tuning=final rounds=2 arm=[false, true] stale=[0, 0] paired=[true, true] digest=3149242640 defect=None twin_pass=[true, true] round_pass=[true, true] ms=4308
```
- **The file of record.** `runlogs/X-5c-F4.final.heldout.campaign.json` has sha256 `e655a61b0c1ef84e4cfd1688b057d06f7219f0f9937d7f322efdcc5d1f97621a`, which matches. It was read only. Its seed 10669 entry reads `"digest":3149242640`.
- **Result: the digest is unchanged.**
- **Other seeds.** 1069 (1368123406) and 3669 (2239733510) also equal the final gate's census.
- **Clean control.** The three `x5f4_joint_arm_live_at*` pins pass (3 passed, release).

## 5. Mutants

Each mutant ran on its own `git archive` export of `daee4f53` under `~/.cache/nv3-review/gate-X-5c-F4-F6/<name>/`, with its own target `~/.cache/nv3-target/gate-X-5c-F4-F6/s-<name>`. None touched the worktree, so nothing needed reverting there. Each was checked by `diff` against the clean export (one hunk each, two for M-STALE-HIDDEN).

| Mutant | Change | 10669 census under mutant | Result |
|---|---|---|---|
| **G5-R48-TREADS-KEPT** (card) | delete `treads.clear();` at `src/workload.rs:2661` | stale `[0, 24]`, digest 3149242640 (unchanged), round_pass `[true, true]` | **caught** by `x5f4_joint_arm_live_at_10669_is_fresh`: `seed 10669: every joint-arm re-run must start from a fresh supply — zero stale reads (G5-R48-TREADS-KEPT, dropping treads.clear(), reverts this): [0, 24]` (workload.rs:2000). The 1069 and 3669 pins also fail, with `[0, 12]` and `[0, 56]` |
| **M-ARM-OFF** (gate) | `:2782` `rec.twin_joint_arm.push(joint_arm_ran)` becomes `push(false)` | arm `[false, false]` | **caught**: `assertion left == right failed: seed 10669: the arm must run at round 1 … left: [false, false] right: [false, true]` (workload.rs:1994). All three pins fail |
| **M-JOINT-SKIP** (gate) | `:2637` `if !tout.obs.crash_fired {` becomes `if false && …` (the joint re-derive never runs) | arm `[false, false]`, digest 773700814, `defect=Some("twin round 1: the crash spec never fired (Mid2@0)")` | **caught** (arm assertion, as above). All three pins fail |
| **M-STALE-HIDDEN** (gate) | TREADS-KEPT plus `:2677` `joint_stale_reads = treads.len()` becoming `= 0` | stale `[0, 0]`, digest 3149242640, round_pass `[true, true]` | **UNCAUGHT** by the 3 pins, and by the whole crate suite: `cargo test --release -p e4-recovery` reads all green (workload binary 96 passed) |

## Findings

1. **M-STALE-HIDDEN is UNCAUGHT. It is not a defect of this card.**
   - The pin observes G5-R48-TREADS-KEPT only through the diagnostic counter `twin_joint_stale_reads`.
   - Under TREADS-KEPT, seed 10669's digest (3149242640) and every round and twin pass are unchanged. The same holds at 1069 and 3669.
   - So at these seeds the stale supply does not move any outcome the suite asserts, and a mutant that also zeroes the counter goes unseen.
   - This is the pin form §5 and §9 ruled ("asserts that the arm ran and that every stale-read count is zero"). The card could not do otherwise without a `src/` seam, which its Limits forbid.
   - It means the discharge rests on the counter being honest. That counter is `treads.len()` read one line after the clear, and this gate inspected it at `src/workload.rs:2661-2677`.
   - Recorded for the owner. No card is opened.
2. **The debug test budget.** The fresh gate's `tests/workload.rs` binary took **201.07 s** in debug (96 tests). It ran concurrently with this gate's release scratch builds, so the figure is not a clean measurement. The final gate measured 167.15 s at T. This adds to the gap already recorded in §8 item 6 of the final verdict. It is not a finding against this card.
3. **A worker-log anomaly, verbatim.** The worker's `git commit` printed `error: Unable to create '/home/tyler/Workspace/neutron/.git/packed-refs.lock': Read-only file system`. The commit nonetheless landed: the branch ref reads `daee4f53…`, and fresh-gate's checks passed on that full id. This looks like the runner's sandbox refusing a post-commit ref pack. It is harmless here, and the runner owner may want to know.
4. **No defects.** No `unsafe`. The new test uses no `unwrap`/`expect` (assertions only, test code). There are no new pub items.

## What this gate did not do

- No campaign set, `verify`, `seeded-faults`, `--probe` or stress loop.
- No verdict commit to the branch, which is read-only per the request.
- No write to any `runlogs/` file, the card, QUEUE or PLAN.
- No ruling on 17948, 26018 or 26958.

## Files (under `~/.cache/nv3-review/gate-X-5c-F4-F6/`)

- **Fresh gate:** `fresh-gate.out` and `fresh-gate.err` (the latter is empty).
- **Scratch runs:** `run-scratch.sh` and `scratch.out` (clean control and four mutants, census plus pins).
- **M-STALE-HIDDEN full-suite run:** `stalezero-full.out`.
- **Exports:** `clean/`, `treads/`, `armoff/`, `stalezero/`, `skipjoint/` (scratch, never committed).
