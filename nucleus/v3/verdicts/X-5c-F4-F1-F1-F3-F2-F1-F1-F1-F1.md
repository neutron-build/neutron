Card: X-5c-F4-F1-F1-F3-F2-F1-F1-F1-F1
Class: test
Card-gate: P
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: Claude Code session 8b4a3204-3b2f-4f37-be2b-cd3bf971e648
Date: 2026-11-08
Gate-exit: GATE EXIT=0
Commit: 6cbbdac01c8e857febeb6cc87a26b632d13ea2ce
Mutants: J-MON-LASTFILE -> monitor-sums-every-file + monitor-every-run-out-file, J-MON-LASTLINE-ONLY -> monitor-sums-every-line, M-MON-ANYFILE -> monitor-every-run-out-file, M-MON-NO-TIE -> monitor-run-out-count-mismatch, J-MPA-NO-MRB -> mpa-cap-counts-mrb-only, J-MPA-NO-SERVED -> mpa-cap-counts-served-only, J-MPA-BASE-NO-CLASS -> mpa-cap-base-mp-only, J-T6A-ANY-T -> twin-t6a-most-recent-t, J-META-T-FIRST -> twin-t6a-most-recent-t, J-T6A-LT -> twin-t6a-ts-equal-t, J-T6B-NO-X33X -> twin-t6b-x33x-short, J-T6B-FIRST-LISTING -> twin-t6b-last-listing, J-T6B-ANYKIND -> twin-t6b-real-class-only + twin-t6-no-real-listing, J-T6A-ANYKIND -> twin-t6a-twin-segments-only, J-T1-SEG-ANYROUND -> twin-t1-same-round-only, J-T4-K-MAX -> twin-t4-first-k, J-T3-ANYROUND -> twin-t3-round-le-i, J-WALK-OTHERSEED -> twin-walk-foreign-seed, G-EXIT-NOBAD -> exit-bad-seeds-only, G-T4-NO-SHORT -> twin-t4-no-short, G-TWIN-PREFILTER-ALLJ -> twin-prefilter-fault-at-i-only, G-T3-ANYCLASS -> twin-t3-mw-only + twin-t3-probe-only, M-T1-ORIGINAL -> twin-t1-probe-only + twin-t1-probe-no-twin-err + twin-t1-pairing-rerun-differs, M-T1-NO-TWIN-REQ -> twin-t1-probe-no-twin-err, M-T1-NO-DIFFER -> twin-t1-pairing-rerun-differs + twin-t1-probe-differs-from-twin, M-T1-DIFFER-TWIN-ONLY -> twin-t1-probe-differs-from-twin, M-T1-ONE-ERR -> twin-t1-identical-rerun-ok, M-T3-PROBE -> twin-t3-probe-only, M-T6-NO-META -> twin-t6-drop-above-meta, M-T6-ANY-ROUND -> twin-t6-drop-other-round, M-T6-NO-REAL -> twin-t6-real-replayed-drop + twin-t6-no-real-listing, M-T6-ANY-TS -> twin-t6-real-listed-above-frontier, M-T6-CROSS-CLOCK -> twin-t6-cross-clock, M-MPA-NO-CAP -> mpa-cap-grown-voids, M-MPA-CAP-GE -> mpa-cap-equal-keeps, M-MPA-CAP-ANYCLASS -> mpa-cap-counts-class-mp-only, M-MON-NO-STOP -> monitor-run-stops, M-CMP-NO-PF -> mpa-pass-fail + mpa-cap-grown-voids, M-CMP-MPA-NOJUDGE -> mpa-skx-smx-false + mpa-pass-na-false, M-CMP-META-ANYSLOT -> meta-only-some-finding, M-CMP-TWIN-NODIR -> twin-t2-both-dirs-lack-N, M-CMP-TWIN-ANYCLASS -> twin-mp-seed-prefilter-fails, J-MON-GT1 -> monitor-run-stops, J-MPA-LINE-ORDER -> mpa-class-line, J-MPA-VOID-KEEPS-ROW -> mpa-cap-grown-voids, J-MPA-VOID-NO-FAIL -> mpa-cap-grown-voids, G-MON-NO-RAISE -> monitor-every-run-out-file, G-T6A-ALLSTARTS-OR -> twin-t6a-most-recent-t, G-MON-FIRSTLINE -> UNCAUGHT, G-MON-FIRSTFILE -> UNCAUGHT, G-MON-EMPTY-OK -> UNCAUGHT, G-MON-NO-MONITOR-FIELD -> UNCAUGHT, G-MON-MAX -> UNCAUGHT, G-TIE-ONE-WAY -> UNCAUGHT
Followups: X-5c-F4-F1-F1-F3-F2-F1-F1-F1-F1-F1

## Verdict in one paragraph

MERGE-WITH-FOLLOWUPS. Both refusals work on every shape I tried, MONITOR above 0 stops the comparison from any file or line, and the re-read is byte-identical to the Base comparator's output. Nothing that counts as exempt moved. All 46 mutants the card names die. Six of my eight own mutants survive, all in the new `read_monitor`/count-tie logic. I confirmed the correct behaviour at the tip for each of the six, so these are test gaps and the code is right. The README also records a false ESCALATE about the card's Acceptance line (below). Neither affects a reading.

## Gate run

- Scaffold `187cb226a9bf57ab2a7ba6b2b90fb28cdaf27c6b`, extracted through the `--no-local` shallow clone. No `EXTRACT FAILED`.
- `fresh-gate: commit 6cbbdac01c8e857febeb6cc87a26b632d13ea2ce`
- `fresh-gate: base 187cb226a9bf57ab2a7ba6b2b90fb28cdaf27c6b`
- `fresh-gate: checks passed (4520 files hashed in 1.31 s)`
- `fresh-gate: 2 file(s) matched through their eol conversion`
- All seven `STEP` lines `EXIT=0`, last line `GATE EXIT=0`.

## Scope

- `git diff --name-only 991ee5fe 6cbbdac0` lists exactly `spikes/e4-recovery/README.md` and `tools/x5r52_compare.py`. Nothing under `src/`, `tests/`, `examples/` or `results/` changed, and the patch file did not change. The wider `nucleus/v3-scaffold...HEAD` diff is the chain beneath this card.
- By AST extraction from the committed blobs, only `cmd_compare`, `read_monitor` and `self_test` differ from `991ee5fe`. `check_mp`, `meta_only_eligible`, `mpa_pass_fail`, `twin_ddl_prefilter`, `compare_reading`, `compare_exit_code`, `format_rows`, `check_twin` and `walk_seed_segments` are byte-identical.
- The README diff deletes one line, the old Status opening. X5-R51, X5-R52 and X5-R53 are untouched.
- The worktree is clean. All mutants and scratch inputs are outside it.

## 1. `read_monitor`, on scratch inputs

I split the real 10k run JSON into three `--run` files and ran the tip and `991ee5fe` comparators on each `--run-out` set.

| Run-out set (three files) | Tip | `991ee5fe` |
|---|---|---|
| All carry the line, MONITOR=0 | 0 | 0 |
| No `E4 SEEDS` line, first / middle / last position | 2 / 2 / 2 | 0 / 0 / 0 |
| Empty file, first / middle / last position | 2 / 2 / 2 | 0 / 0 / 0 |
| Last file cut mid-line, before `MONITOR=` | 2 | 0 |
| `MONITOR=` with no value, middle | 2 | 0 |
| MONITOR=1 in first / middle / last file | 1 / 1 / 1 | 1 / 1 / 1 |
| One file with MONITOR=0 then MONITOR=1 | 1 | 1 |

Each MONITOR>0 run at the tip carries the `(-1, 'run', None, 'bullet3-monitor')` row. MONITOR=1 in two files prints `MONITOR 2`, so the sum is a sum. The real `.campaign.out` holds exactly one `E4 SEEDS` line, so the per-file requirement fits what the campaign prints. `t5_checks` also calls `read_monitor`, with one file, and reads the same as before.

## 2. The count tie

Three `--run` with two `--run-out`, and three with four, both exit 2 with the `ERROR --run count` line. `991ee5fe` exits 0 on both. The check sits after the sha256 checks and before any file is read, as the card asks.

**Not closed, and outside this card's Work:** the tie is a count only. Passing the same `.out` for all three runs exits 0, so a wrong or repeated `.out` still hides another run's MONITOR. -F2's gate should check that its five `.out` files are distinct and belong to their runs. A missing or non-UTF-8 `--run-out` is a traceback, exit 1, at both ends. That is unchanged, and it is never exit 0.

## 3. Mutants

Every mutant is a scratch copy of the committed `6cbbdac0` blob, run through `self-test` by my own driver, which checks the named cases against the FAIL lines.

- **Method limit.** The 46 card-named mutants use the worker's anchored replacements from its scratch `mutants.py`. I read every replacement against the card's description and found each faithful, but those definitions are not independently authored. My eight are.
- All 46 die on their named cases. The sixteen survivors of the prior verdict are dead.
- **Own mutants caught:** G-MON-NO-RAISE, G-T6A-ALLSTARTS-OR.

**Own mutants UNCAUGHT, and the tip's behaviour for each:**

| Mutant | What it does | Why it survives | Behaviour at the tip |
|---|---|---|---|
| G-MON-FIRSTLINE | Reads each file's first `E4 SEEDS` line only | `monitor-sums-every-line` puts MONITOR=1 first | 0-then-1 in one file exits 1 |
| G-MON-FIRSTFILE | Reads the first run-out file only | `monitor-sums-every-file` puts MONITOR=1 first | MONITOR=1 in middle or last file exits 1 |
| G-MON-EMPTY-OK | Accepts an empty file | The refusal case's file has text | Empty file exits 2 in all positions |
| G-MON-NO-MONITOR-FIELD | `E4 SEEDS` without `MONITOR=` counts as seen | The refusal case's file has no `E4 SEEDS` at all | Cut-off line exits 2 |
| G-MON-MAX | Takes the maximum, not the sum | No case has two non-zero counts | 1+1 prints `MONITOR 2` |
| G-TIE-ONE-WAY | Refuses only too few run-outs | The case has too few, never too many | Three runs, four run-outs exits 2 |

**Ruling: test gaps, not rework,** the same ruling as the prior verdict. The card's four MONITOR cases are built exactly as the card specifies, and it is that specification which leaves the first-position and truncated-line mirrors unpinned. The cost is that these checks, which -F2's five-file step relies on, are held by this review only until the follow-up lands. G-MON-MAX cannot change an exit, only the printed count.

## 4. The re-read

Inputs matched their sha256: run `6b686266…739a`, reference `f4d6f1ff…c923`, run-out `cf3de932…6059`. Comparators were extracted from the committed blobs.

- Tip, `991ee5fe` and `26bcee4b` all exit 0.
- Tip against `991ee5fe`: **byte-identical**. Tip against `26bcee4b`, without the `MPA_CLASS` line: byte-identical.
- DIGESTS_MOVED 2589, FAILING_ROUNDS 19, MONITOR 0, MOVED_MP_PENDING 52, META_ONLY_ELIGIBLE 4 [(1927,1),(7257,0),(7658,0),(8278,0)], MPA_PASS_FAIL 1 [(7078,0)], `MPA_CLASS base 11 tip 10`, TWIN_DDL_PENDING 2 [(2599,2),(2859,1)], HEALED 3, TWIN_HEALED 1 [(6539,1)], NEW_FAILING 0, BAD_SEEDS 0. This is the verdict's record: 0 new failing, the same exempt rounds.
- The output block in the README matches my tip output verbatim.

## 5. Errors in the README record

None of these changes a reading or a rule.

1. **The worker's ESCALATE is false.** The README says the card's Acceptance line names `runlogs/X-5c-F4-F1-F1-CXX-F1.acc`, a path that does not exist, and that running the line verbatim fails. The card names `runlogs/X-5c-F4-F1-F1-F3-F2-F1.acc`, which exists and has the pinned sha256. The card's file time (22:03) precedes the worker's commits (22:44). In the worker's log the wrong path first appears in its own reasoning (line 53), and the card text it was given earlier carries the right path. The runner's own Acceptance passing agrees. The worker's re-read used the correct file, so no result depends on this, but the README's statement should not stand as the record.
2. The Status paragraph says "seventeen new cases beside the refusals' four". There are seventeen in all, thirteen beside the four (61 to 78).
3. Step 2 says `twin_diag` gains seven knobs. It gains six; the other item in the list is the new `D4_TS5`/`D4_TS7` records.

## Follow-ups

- **X-5c-F4-F1-F1-F3-F2-F1-F1-F1-F1-F1 (tools, `self_test` and README record only).** Add `self-test` cases that kill G-MON-FIRSTLINE (MONITOR=0 then 1 in one file), G-MON-FIRSTFILE (MONITOR=1 in the last file only), G-MON-EMPTY-OK (an empty run-out), G-MON-NO-MONITOR-FIELD (an `E4 SEEDS` line cut before `MONITOR=`), G-MON-MAX (two non-zero counts, asserting the printed total) and G-TIE-ONE-WAY (more run-outs than runs). Correct the three README errors in §5. No change to `read_monitor`, `cmd_compare` or any classifier. It need not block -F2, because the behaviour is confirmed at this commit, but -F2's Limits freeze `x5r52_compare.py`, so P decides whether it lands first or after -F2's comparison. P should also decide whether a run-out must be tied to its run, for example `E4 SEEDS=N` against that run's seed count. That would be a new refusal and is not in this follow-up as written.
