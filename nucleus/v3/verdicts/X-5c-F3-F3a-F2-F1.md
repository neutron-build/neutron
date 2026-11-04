Card: X-5c-F3-F3a-F2-F1
Class: experiment
Card-gate: P
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: T3 review sub-agent, Claude Code (X-5c-F3-F3a-F2-F1 P gate, 2026-11-04)
Date: 2026-11-04
Gate-exit: GATE EXIT=0
Commit: 20c53912e9e8e97dfc244de2b10adc2c0b4dbcfd
Mutants: R36-REVERT -> x5_r36_scan_at_reads_the_drop_incarnation AND x5_r36_seed112_r2_no_longer_fails_o1b_on_tree_3, R36-FLOOR-BY-S -> x5_r36_scan_at_reads_the_drop_incarnation, R36-FLOOR-IGNORES-CUT -> x5_r36_scan_at_reads_the_drop_incarnation, REUSE-FLAG-OFF -> x5_r36_reuse_on_dropping_predicate, FRONTIER-AS-HI -> x5_r36_o1f_line_pins_the_frontier_candidate_of_5549_r2 (the 1749 pin: UNCAUGHT, frontier = hi), LE-T-FROM-FRONTIER -> x5_r36_o1f_line_pins_the_frontier_candidate_of_5549_r2 (the 1749 pin: UNCAUGHT, finding 1), (own) G-O1D-CUT-AS-S (O-1(d) at scan_at(t,S,S)) -> x5_r36_seed112_r2_no_longer_fails_o1b_on_tree_3, (own) G-DIR-READ-EMPTY (opened_dir_dropping returns {}) -> UNCAUGHT (full suite), (own) G-DROP-FLOOR-STRICT (drop_floor over ..c) -> UNCAUGHT (full suite), (own) G-O1B-CUT-FRONTIER (O-1(b) at scan_at(t,c,frontier)) -> UNCAUGHT (full suite)
Followups: X-5c-F3-F3a-F2-F1-F1

## How the gate was run

- **Fresh checkout.** `git -C ~/Workspace/neutron-v3 -c core.hooksPath=/dev/null worktree add --detach /tmp/gate-X-5c-F3-F3a-F2-F1 20c53912e9e8e97dfc244de2b10adc2c0b4dbcfd`.
  - The common git dir (`~/Workspace/neutron/.git`) has no non-sample hooks, no `filter.*` config, no `core.hooksPath`, and no `info/attributes`. The only `.gitattributes` is the tracked repo-root file (eol/binary only, 327fd3e8).
  - The index equals the commit (`diff --cached` clean). Every `nucleus/v3` blob matches its working file (`hash-object --no-filters`, 289 files). Status was clean.
- **Gate.** `CARGO_TARGET_DIR=$HOME/.cache/nv3-target/gate-X-5c-F3-F3a-F2-F1 bash scripts/gate.sh` ran in the foreground. Every STEP was EXIT=0, and the literal last line is `GATE EXIT=0`.
- **Mutants, probes and scratch edits** were applied in the gate checkout and reverted with `git checkout`. Status was 0 lines after each one, and the release binary was rebuilt clean before every measurement that is quoted below. The outputs are in `~/.cache/nv3-review/gate-F2F1/`.
- **Scope.** `git diff card/v3-X-5c...HEAD --stat` touches only `nucleus/v3/spikes/e4-recovery/`. `src/model.rs`, `src/oracle.rs` and `tests/oracle.rs` change only in 5a41648b, whose subject names X5-R36. `src/workload.rs` gains only the `reuse_on_dropping` field and its copy. The non-test diff has no `unsafe`, `unwrap`, `expect` or `panic!`.
- **Step 0.** The X5-R36 block in 8e2f9813's README (from `**X5-R36 (the drop` through the `(f) Pre-data` bullet) is `cmp`-identical to the same span of `verdicts/X-5c-F3-F3a-F2.md` at 31c00f25. 8e2f9813 was committed at 03:42:37. The worker's first `e4-campaign` run is at 03:49:08 in the attempt log. Before step 0 it read only source files and the heads of two committed pre-card result files. **Pre-data holds.**

## Measurements re-run by the gate (gate checkout, release)

| Run | Digest ×2 | Outcome | RESULT says |
|---|---|---|---|
| 112 r2 | 2157138087 ×2 | pass, c=frontier=lo=hi=15 | same |
| 63 r2 | 2414083785 ×2 | pass, c=24=frontier | same |
| 104 r2 | 3776021738 ×2 | pass, c=25=frontier | same |
| 1749 r2 | 1584051239 ×2 | pass, c=41, lo 40/hi 41 | same |
| 5549 r2 | 308297210 ×2 | pass, c=32=frontier, hi 33 | same |
| 7359 r2 | 670834677 ×2 | pass, c=12=frontier, record lo 14 | same |
| `run --seeds 0..200 --threads 16` | — | `E4 SEEDS=200 O1_FAIL=45 O2_FAIL=0 O3_FAIL=2 DEADLOCK=0 PANIC=0 O1F_FAIL=45 MONITOR=0` / `E4 FOLD FAILING_SEEDS=2 FAILING_ROUNDS=2 TWIN_FAILING_ROUNDS=0 DEFECT_SEEDS=0`. Failing rounds: 8 r0 `O-3(e): (ii) reported slot 7 …` and 178 r1 `O-3(e): (ii) reported slot 91 …`. `reuse` is 0 of 305 rounds. | same |

More runs by the gate:
- **`E4_O1_AT_FRONTIER=1`.** The digests are identical with it on and off for 1749, 5549, 7359, 112 and 63. The clone seam is byte-invisible.
- **`E4_O1_DIAG=1` and `E4_KEY_DIAG=1` over 0..200** at the tip: neither fires, because no recovered round fails O-1. 0 digests moved.
- **The same two diagnostics under R36-REVERT.** That window reproduces F2's tip exactly: 10 failing rounds plus 1 twin. Both diagnostics fire (592 and 543 stderr lines), and 0 of 200 digests move (see ruling 2 for why this is weak).
- **A reader probe** (temporary eprintln, reverted). `opened_dir_dropping` read 338 opened directories over 0..200, 8 of them with a `dropping` entry (for example 53 r2 `{…, 4: true, …}` and 76 r2), and none had a `CreateTree` in (T, frontier]. So the reader works on real images, and `reuse = 0` is a true zero, not a blind one.

The decision rule was applied as written. X5-R36's text is landed exactly: `scan_at` per (a), O-1(b) at (c,c), O-1(d) at (S,c), and the four harness sites at the recovered frontier per (b). Nothing in (c)'s list moved. No seed was excluded, and no knob changed.

## Findings

1. **The le_T agreement helper is vacuous** (`tests/harness.rs`, `check_le_t_agrees_with_owner_and_t`). The line prints its flags as `Some("owner_ts=…`, but the helper splits on `" owner_ts="` (with a leading space). It never parses an owner and returns early on every line.
   - LE-T-FROM-FRONTIER is caught only by the 5549 literal (`owner_ts=32` and `le_T=false`).
   - The 1749 pin's docstring, and the worker's judgment call 1, say the 1749 pin catches FRONTIER-AS-HI and LE-T-FROM-FRONTIER. It catches neither. FRONTIER-AS-HI is structural there (frontier = hi = 41). With the parse fixed in scratch, the 1749 pin does catch LE-T-FROM-FRONTIER (owner 40, T 38, `le_T=true` under the mutant).
   - Not blocking: the card's Work 5 literal on 5549 holds, and all six named mutants are caught.
2. **`opened_dir_dropping` has no pin from durable bytes** (G-DIR-READ-EMPTY is UNCAUGHT). Pin (iii) tests only the predicate, on a hand-built map. The probe above shows the reader is correct today. It is F4's ESCALATE tripwire, so it needs a regression pin.
3. **No pin covers the boundaries.** Both mutants survive:
   - G-DROP-FLOOR-STRICT (`d == c`). It is unobservable in the workload's histories, because the drop commit carries the catalog `Delete`, so `t` is unpublished at `c = d`. Still, no unit case covers it.
   - G-O1B-CUT-FRONTIER (O-1(b)'s model cut). The wiring is correct (reviewed), but no pin holds it.
4. **The `o1_at_frontier_lines` header doc is stale.** It says the fallback is "the tree's first key". The code picks the newest owner at `c`, and a tree whose keys are all ownerless prints nothing.
5. **A latent diagnostic-purity defect** (ruling 2): `E4_O1_DIAG` and `E4_KEY_DIAG` still read through the judged world.

## Rulings on the judgment calls

1. **The 1749 pin rework is accepted, but the worker's reading is too broad.** The Do-not forbids reshaping a check so the ruling passes. Oracle and self-test expectations are never re-pinned. A diagnostic pin is different when its literals recorded the output of the model defect the ruling removes (owner_ts=4, le_T=true at c=41). It may be re-pinned, but only in the Work step that owns it, disclosed in RESULT, and with every mutant the old pin caught still caught. All three hold here: the bound literals are kept and `T=38` is added. Its "catches strictly more" claim is false (finding 1). The 5549 pin, not 1749, carries both mutants.
2. **Yes, this is a diagnostic-purity defect.** The invariant is "diagnostics are never digest inputs". The worker measured a pool read through the judged world moving 1749's digest, and both diagnostics use that same reader. My 0-of-200 under R36-REVERT is weak evidence, because the firing rounds are mostly the seed's last round, so a pool change has no later round to show up in.
   - Not blocking: judged runs have every E4_* variable off.
   - The fix is in the follow-up.
   - Until it lands, any X5-R16 trace taken with E4_* variables must record the seed's digest with them on and off. If the two differ, the trace is not evidence.
3. **The fallback, the newest owner at c, is accepted.** It is what makes the owner > T case load-bearing (5549: 32 > 29). The doc is fixed in the follow-up.
4. **Serializing `reuse` without making it a digest input is accepted.** This is the `x33x` precedent: the digest covers outcomes, and (e) says the flag is surfaced, never an outcome. F4 counts it from the file.
5. **Computing the flag only on the recovery-Ok arm is accepted.** (e)'s hazard is a create that pass 2 replayed into a mid-drop image. A failed open replays nothing and has no frontier, and such a round still counts by its outcome.
6. **The MW lo relabel is accepted.** 1749 is 40, 5549 is 30 and 7359 is 11, and each matches the gate's own O1F lines.

**The two MP O-3(e) rounds are a known class, not a new one.** They are MP O-3(e)(ii), the X5-R29 shape-C/exactness family that X5-R29 (c) handed on as an I-LPID-EXACT store deviation. The F3-F3b file of record lists 11 such rounds, and its named repros are exactly 8 r0 and 178 r1. They get no X5-R16 treatment before F4. In E4's judgement they count under X5-R3 unchanged.

## X-5c-F4: go, with these card changes (orchestrator edit, verbatim)

**F4 may launch** once `card/v3-X-5c` is fast-forwarded to this verdict's commit. The follow-up does not block F4. It must land before X-5c's `--keep` relaunch.
- **Depends:** X-5c-F3-F3a-F2-F1 → "(P gate MERGE-WITH-FOLLOWUPS, this verdict; card/v3-X-5c fast-forwarded)". Status → ready after that fast-forward.
- **Step 0 note, baseline measured:** C-DIV 0 at F2-F1's 0..200. FAILING_ROUNDS 2, both MP O-3(e)(ii) (8 r0, 178 r1). `reuse` is 0 of 305. 1749/5549/7359 r2 pass (1584051239 / 308297210 / 670834677). Neither number is quota or judgement evidence.
- **Class table:** the MP O-3(e)(ii) row is a carried class (X5-R29 (c)), never "new" under step 4.
- **Pins the retune must not disturb** (add to the F3 hand-off list): `x5_r36_scan_at_reads_the_drop_incarnation`, `x5_r36_seed112_r2_no_longer_fails_o1b_on_tree_3`, `x5_r36_reuse_on_dropping_predicate`, `o1_at_frontier_diag_names_the_frontier_candidate_of_1749_r2` and `x5_r36_o1f_line_pins_the_frontier_candidate_of_5549_r2`. If a retune moves a pinned seed's workload, re-express the pin as a literal config that reproduces the same round. Never change its assertions.
- **Diagnosis hygiene:** as in ruling 2, until X-5c-F3-F3a-F2-F1-F1 lands.

## Follow-up

- **X-5c-F3-F3a-F2-F1-F1 (standard; Gate: S; test and diagnostic hygiene).** It fixes the following:
  - **(a)** The helper's owner parse (`"owner_ts="`). Add a non-vacuity assertion: at least one line was checked. Correct the 1749 docstring (it catches LE-T-FROM-FRONTIER, never FRONTIER-AS-HI).
  - **(b)** A pin for `opened_dir_dropping` from durable bytes, on a real mid-drop image (for example 53 r2 or 76 r2, tree 4 `dropping`). It must kill G-DIR-READ-EMPTY.
  - **(c)** A unit case `c = d` for `drop_floor`, killing G-DROP-FLOOR-STRICT. Add an O-1(b) cut pin killing G-O1B-CUT-FRONTIER.
  - **(d)** The stale fallback doc (comment only).
  - **(e)** Move `E4_O1_DIAG` and `E4_KEY_DIAG` onto the throwaway clone that O1F uses. Add a pin that arms both through a test-only capture seam (as `o1f_capture_arm`), which also fires them on passing rounds, on a multi-round seed. It asserts the seed digest is identical armed and unarmed. Revert-demonstrate it on a seed where a judged-world read moves the digest (1749 moved for O1F). If no such seed exists for these two, record that.

  No oracle, model or digest change. Every seed digest must be unchanged.
