Card: X-5c-F4-F1-F1-CXX-F1
Class: test
Card-gate: P
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: nv3-X-5c-F4-F1-F1-CXX-F1-gate-2 (T3 review sub-agent, Claude Code session c309e0a0-70a7-4535-80da-5c705ead57b1)
Date: 2026-11-06
Gate-exit: GATE EXIT=0
Commit: 17981efa3dc70b3ef1ea43823ac021130b07305a
Mutants: M-R48-REAL-MASKS-ONLY -> r48_repaired_twin_fires_at_the_joint_spec (also r21c, x5_r37), G6-UNPIN-BEFORE-FREES -> drop_last_step_keeps_its_pages_pinned (at 467), G1-R48-NO-JOINT -> r48 (also r21c, x5_r37), G2-R48-TWIN-MASKS-ONLY -> r48 (also r21c, x5_r37), G3-R48-OR-NOT-AND -> r48 (also r21c, x5_r37), G4-R48-REAL-KEEPS-OLD-SPEC -> x5_r37_base_tuning_reproduces_1b0ce54e, G5-R48-TREADS-KEPT -> UNCAUGHT, G6b-R48-RERUN-FROM-END -> x5_r37_base_tuning_reproduces_1b0ce54e, G7-R48-FINAL-SPECS-STALE -> r48 (also r21c, x5_r37), G8-R48-TWIN-FINAL-STALE -> r48 (also r21c), G9-R48-DRAWN-IS-SPEC2 -> UNCAUGHT, G10-R48-ALWAYS-JOINT -> UNCAUGHT, G11-R48-GUARD-REMOVED -> UNCAUGHT, G12-R48-NO-JOINT-NO-GUARD -> r48 (also x5_r37)
Followups: X-5c-F4-F1-F1-CXX-F1-F1

## Summary

X5-R48 is implemented as ruled and fixes the cause rather than hiding the symptom. Over 0..10000, against this card's real base, exactly one seed's record changes: 7469 goes from a defect seed to three passing rounds. No round goes pass to fail, and DEFECT_SEEDS is now 0. No existing check was weakened. One uncaught mutant is a real pin gap on a line that protects the oracle (finding 2). The shipped line is correct, so it goes to a follow-up.

The card's "C-XX root cause" is the re-pair defect its Goal describes (X5-R16 NEVER-FIRED on the R21 (c) arm). The C-XX store class itself was closed by the parent card; it stays at 0 here.

## Gate run

Scaffold `fresh-gate.sh` and `git-env.sh` through the `--no-local` clone, scaffold id ed321a0b9b96f2ebfc0661e1dd2df93c92a106b0, `CARGO_TARGET_DIR=~/.cache/nv3-target/gate-X-5c-F4-F1-F1-CXX-F1`, with no other job of mine running:
- `fresh-gate: commit 17981efa3dc70b3ef1ea43823ac021130b07305a`
- `fresh-gate: base ed321a0b9b96f2ebfc0661e1dd2df93c92a106b0`
- `fresh-gate: checks passed (4511 files hashed in 1.36 s)`
- `fresh-gate: 2 file(s) matched through their eol conversion`
- STEP test, clippy, fmt, deny, wasm32, core-paths-selftest and enforcement-tests all `EXIT=0`; 46 suites, 463 passed, 0 failed
- `GATE EXIT=0`, stderr empty

`runner e2e: K` is green here with no wrapper. The worker's subreaper wrapper changed no step, and I accept it, as O1C-F1's gate did.

Scope: `86e17a34...17981efa` touches README.md, src/workload.rs, tests/store.rs and tests/workload.rs of the crate, seven commits, nothing else. The scaffold diff adds only this crate and 13 verdict files, and all 13 are already in the base, so they belong to the stack and not to this card. In `src/`, the change is confined to the R21 (c) re-pair block of `run_seed_with` (Limit 1). No `unsafe`, `unwrap`, `expect` or `panic` added. X5-R48 is in the README byte for byte, in the step-0 commit, before any code.

## The 10k window (the card's Notes)

`e4-campaign run` at BASE over 0..10000, lists of 200 seeds, `ulimit -v 25165824`, on a scratch export. No seed at or above 10000 ran.

| Against | Digests moved | Records differing | fail -> pass | pass -> fail |
|---|---|---|---|---|
| The real base, O1C-F1's `post10k.json` | **7469** | **7469** | none | **none** |
| CXX's tip, `tip3-10k.json` | 1066, 6555, 7469, 7590 | the same four | 1066, 6555, 7590 r0 | **none** |

- **7469:** digest 1670717970 -> 1393729414. Defect `twin round 1: the crash spec never fired (Mid2@1)` -> none. Rounds `Mid2@0, Mid2@1` -> `Mid2@0, Mid2@4, Mid2@0`, all fired and passing. Twins 3/3 pass.
- **The card's expected "only 7469 against CXX's tip":** written before O1C-F1 became the base. The other three are O1C-F1's own named movers, with its digests. Accepted, and fully attributed.
- **At the tip:** DEFECT_SEEDS 0; no deadlock, panic or baton. 21 failing rounds, all class MP (not this card's). 6 failing twin rounds, unchanged.

Comparing records, not only digests, rules out a change hiding in a field the digest does not cover.

## Root cause, not symptom

I checked the claim three ways.

- **The cause is what the card says.** A census build (stderr only; all 10000 records identical to the tip's) reproduces, at 7469 r1, twin masks `[3ff,3f7,3ff,3f7,3ff,3ff,3ff,3ff]`, real masks `[3f7,3ff,3f7,3ff,3ff,3ff,3ff,3ff]`, and joint `[3f7,3f7,3f7,3f7,3ff,3ff,3ff,3ff]` -> `Mid2@4`. `Mid2` fires inside step 2's write loop (`store/ckpt.rs:1145`), so a clear bit is a checkpoint with no step-2 write, not a fault.
- **The guard still stands.** `crash_fired_defect` on the re-paired twin is untouched and now judges the final attempt. The fix makes the crash fire; it does not excuse a crash that did not. G12, which removes both the fix and the guard (the literal masking shape), fails the pin on `round 1 must run the joint spec`.
- **Nothing else moved.** The joint arm is reached by 7469 alone of 10000. Four seeds reach the re-pair arm; 8469, 8869 and 9469 fire and do not enter it. Round 1 still crashes at the drawn hook, and the seed regains round 2, so E4 sees more of this seed, not less.

Against X5-R48's clauses, by reading: AND over the shorter length (`zip`); same drawn `(hook, ckpt_index)`; same `(seed, round)`; re-run from a clone taken before the first re-pair attempt; `script2`, `spec2` and `crash_desc` name the joint spec; `twin_final_spec` and `real_final_spec` come from `spec2`. On refusal nothing changes and the guard trips. There is no third attempt, because the block has no loop. In the twin's final record, nested points, supply and chain continuation all come from the final attempt.

**`hook` and `ckpt_index` in this block are `script.crash`'s,** the same values the block's first re-derive uses. So "the same drawn" holds.

## Existing tests: nothing weakened

- **tests/store.rs:** adds one call at seed 467 and doc text. The body is unchanged, and seed 38 still runs first.
- **tests/workload.rs:** r21c's assertions are unchanged and its list only grows (7469 back). The only literal change is 7469's row in `x5_r37`, as Limit 3 allows. The other 53 rows are byte-identical. A new pin is added.
- **The removed lines are doc text only:** no assert, `#[test]` or check is removed, and no `#[ignore]` is added.
- **The new literal is earned:** with the fix removed (G1), `x5_r37` fails with `left: 1393729414, right: 1670717970`. The value matches the parent gate's independent trial and my window.
- **r21c's rewritten doc is accurate for its list:** the census shows 2669 re-deriving in the twin's first attempt, 9469 reaching the re-pair with a firing twin, and 4049, 4309 and 9809 deriving nothing.

## Findings

### 1. The README record names the wrong base (minor; -F1)

Step 6 says "pre = CXX's tip (= this card's base...)". The base is O1C-F1 at 86e17a34, and it differs from CXX's tip in 3 digests. The conclusion survives, because those three are not in the X5-R37 list or in 0..200, and my window covers both comparisons. But F4's worker reads this README for attribution.

Step 4's "only 9469 reaches the re-pair path with a firing twin" follows the card's wording but is true only within the literal list. Over 0..10000, 8469 and 8869 do too.

### 2. G5-R48-TREADS-KEPT is uncaught, and it is a real gap (minor; -F1)

`run_round` extends the caller's collector (`harness.rs:3009`), so `treads.clear()` is what keeps the unfired attempt's reads out of the supply. It is live: 17 stale reads at 7469. Removing it passes all 253 tests in the crate.

The direction is the concern. In O-3(c) (`oracle.rs:611`, `:621`), an extra read can only clear `all_uncovered`, on both the page and the WAL arm, so a leak can only suppress a failure. It did not change 7469's digest. The shipped line is correct, so this is regression cover and not a defect. But it is the one line in the arm that guards what the oracle sees, and the record exposes nothing to pin it with.

### 3. Three uncaught mutants, accepted with no card

- **G10-R48-ALWAYS-JOINT:** passes the crate, and moves no record on any of the 4 re-pair seeds of 0..10000. When the hook is reachable, `idx2` is the real round's first reach and the twin fired there, so the joint draw returns the same spec. It could differ only on the fallback draw, which no seed reaches. "Unchanged" holds by the 10k and by reading.
- **G9-R48-DRAWN-IS-SPEC2:** differs only on that same fallback arm, which has no live seed. The shipped call passes the drawn values.
- **G11-R48-GUARD-REMOVED:** at the tip no seed trips this guard, so nothing can pin it without a shape the frozen sim does not produce. The guard is unchanged code, and G12 covers the masking combination.

The Refusal clause has no live shape either: the census shows only `joint-ok`. `reachable_hook_spec`'s refusal has its own pin.

### 4. The worker's anomalies

Mid2 is `CKPT_HOOKS[3]`, so its mask value is 0x8 and its bit position is 3, as the worker says. The parent verdict's "bit 8" is a slip with no effect. `X5-R37` red at intermediate commits is the card's own step order and is green at the tip.

## Mutants

Run on a scratch export; the worktree stayed clean. An unmutated control passed first. An earlier driver run was invalid (the export lacked `nucleus/v2`, so nothing compiled) and is discarded; the driver now refuses a run with no test lines.

| Mutant | Catching test | Result |
|---|---|---|
| M-R48-REAL-MASKS-ONLY (card) | `r48`, also `r21c`, `x5_r37` | caught, `never fired (Mid2@1)` |
| G6-UNPIN-BEFORE-FREES (card) | `drop_last_step_keeps_its_pages_pinned` | caught at 467, after 38 passes |
| G1-R48-NO-JOINT (fix removed) | `r48`, `r21c`, `x5_r37` | caught |
| G2-R48-TWIN-MASKS-ONLY | `r48`, `r21c`, `x5_r37` | caught, `round 1 ... (Mid2@0)` |
| G3-R48-OR-NOT-AND | `r48`, `r21c`, `x5_r37` | caught |
| G4-R48-REAL-KEEPS-OLD-SPEC | `x5_r37` | caught, by the digest only |
| G5-R48-TREADS-KEPT | none, whole crate | **UNCAUGHT**, finding 2 |
| G6b-R48-RERUN-FROM-END | `x5_r37` | caught, by the digest only |
| G7-R48-FINAL-SPECS-STALE | `r48`, `r21c`, `x5_r37` | caught |
| G8-R48-TWIN-FINAL-STALE | `r48`, `r21c` | caught, Work 2.5 break |
| G9-R48-DRAWN-IS-SPEC2 | none, whole crate | **UNCAUGHT**, finding 3 |
| G10-R48-ALWAYS-JOINT | none, whole crate | **UNCAUGHT**, finding 3 |
| G11-R48-GUARD-REMOVED | none, whole crate | **UNCAUGHT**, finding 3 |
| G12-R48-NO-JOINT-NO-GUARD | `r48`, `x5_r37` | caught |

G4 and G6b break the pairing or the start clause while `r48` stays green. Only the X5-R37 literal catches them, so that literal is carrying real weight.

Artifacts: `~/.cache/nv3-review/X-5c-F4-F1-F1-CXX-F1/gate/` (`mut.py`, `mut.out`, `mut2.out`, `mutlog/`, `tip-10k.json` sha256 prefix f4d6f1ff0c2731b9, `census-10k.json`, `census.log`, `cmp10k.py`, `g10/`, `fresh.out`).

## Follow-ups

**X-5c-F4-F1-F1-CXX-F1-F1** (Class test, Gate S, non-blocking; can stack anywhere after this card and before F4's relaunch). Two parts. (a) Pin the supply's freshness: a diagnostic in the `twin_paired` manner (never a digest input, never serialized) that lets a test show the joint-arm supply holds only the final attempt's reads at seed 7469, which `G5-R48-TREADS-KEPT` must fail. (b) README: step 6's "pre" names O1C-F1 at 86e17a34 as the base and attributes 1066, 6555 and 7590 to it, and step 4's "only 9469" is scoped to the literal list, naming 8469 and 8869. It moves no digest, and no literal may move.
