Card: B-0-F7-F1b
Class: experiment
Card-gate: P
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: nv3-B-0-F7-F1b-gate-1 (P review sub-agent, Claude Code session b5892ed2-ddc6-4483-85fc-45b3d8b56216)
Date: 2026-10-31
Gate-exit: GATE EXIT=0
Commit: bc58fb63ea4afa4879034c81523c294f1932db52
Mutants: card names none (no new code). Own, applied to throwaway copies of the committed evidence under /tmp (worktree never touched): EV-EF32-P50-OVER (run B ef32 p50 0.6601, spread 0.0862 > bound 0.08609) -> gate J6 script + nbench qdrant-repro-verdict both NOT-REPRODUCED, caught; EV-EF32-P50-AT-BOUND (b = 1.15 x a exactly) -> both REPRODUCED, inclusive boundary correct; EV-COLD-PARTIAL-INDEX (run A ef128 cold index_indexed 984064) -> both INVALID, caught; EV-DEBUG-PROFILE (run B profile debug) -> both INVALID, caught; EV-RECALL-OVER (run B ef256 recall -0.0021) -> both NOT-REPRODUCED, caught; EV-ROW-IS-RUN-B (row file replaced by run B) -> reproduce.sh check rc 1, caught; EV-SECOND-REPRO-DIR -> reproduce.sh check rc 1, caught
Followups: B-0-F7-F1b-F1, B-0-F7-F1b-F2

Reviewer: Opus (claude-opus-5-5), P gate (experiment judgement), round 1,
on tip bc58fb63 (run A 6e34cd90, run B 326d92e5).

**J6 says REPRODUCED, and it was applied verbatim. Both runs were taken on a
quiet host, in the settled, fully indexed state, with a release client and
inside J1. Run A is the row.** The one defect is documentary. Because the runs
happened on the same UTC date as B-0-F7, the harness wrote the new row over
B-0-F7's row file at the same path. The supersede note does not
disambiguate them. The retired row is intact in git (04479d54, blob
f4b2f475), so no evidence is lost. That makes it a follow-up, not a rework.

# What was verified

- **Gate run.** `PATH=$HOME/.cargo/bin:$PATH CARGO_TARGET_DIR=$HOME/.cache/nv3-target/gate-B-0-F7-F1b bash scripts/gate.sh` ran in the foreground on a clean tree at bc58fb63. It took 190 s, and every STEP had EXIT=0. Literal last line: **GATE EXIT=0**. The runner's own acceptance run (runlog `B-0-F7-F1b.out`) passed all 5 lines in 1 attempt, with no kills.
- **Scope.** `git diff nucleus/v3-scaffold...HEAD --stat`: 220 files, all under `nucleus/v3/bench/nbench/results/2026-10-31/`:
  - `repro/` (run-a/, run-b/, both run JSONs, verdict.txt);
  - the row file and `REPORT.md` (written by `reproduce.sh`);
  - `vector-sift1m-qdrant.SUPERSEDED.md`.

  There is no source, script, workload or digest change. The B-0-F7 diagnosis artefacts in that dir are byte-unchanged. The overwrite of the row file and REPORT.md is finding 1.
- **Quiet host.**
  - Work 1 (transcript, 14:51:59Z): load 1.20; `pgrep -x cargo/rustc` returned nothing; there were no `nbench-*` units.
  - Run A's quiet window was satisfied at 14:54:28Z (load1 0.78 → 0.19, 13 consecutive samples).
  - Run B's quiet window was satisfied at 15:16:15Z (load1 0.19 → 0.27).

  `reproduce.sh` only checks *before* each run, so I also checked the user journal for 07:45–08:40 local. It shows only this card's own units:
  - run A: one `podman --version`, the `rm -f` and 6 `nbench-qdrant` containers, 07:54:29–08:09:09;
  - run B: the same set, 08:16:15–08:30:44.

  There was no foreign container or unit. No other `~/.cache/nv3-target/*` or card worktree has files modified inside either window, so nothing was built. The only other activity was two Claude sessions (orchestrator side, 56 s and 3 m 45 s of CPU over about 45 min). That is negligible, and it was the same for both runs. The worker itself only slept and tailed logs during the runs, and its `cargo build` finished at 14:52, before run A's window opened.
- **No re-runs.** `reproduce.sh a`, `b` and `verdict` were each invoked exactly once (transcript, 14:52:27Z, 15:14:14Z and 15:34:47Z). There is no `.killed.log`, no exit 3 and no FAILED line. Run A was committed before run B started.
- **J6, re-applied independently.** My own Python reads the committed JSONs via `git show HEAD:`; it does not use `qdrant-repro-verdict`.
  - Neither run is voided, and both have `provenance.nbench_build.profile = "release"` (J5).
  - Both runs have the same ef set {16, 32, 64, 128, 256}.
  - All 15 comparisons pass. Spread as a fraction of its bound:

    | ef | qps | p50 | recall |
    |---|---|---|---|
    | 16 | 0.74 | 0.59 | 0.12 |
    | 32 | 0.49 | **0.95** | 0.41 |
    | 64 | 0.11 | 0.17 | 0.15 |
    | 128 | 0.14 | 0.11 | 0.10 |
    | 256 | 0.01 | 0.04 | 0.01 |

  - Result: **REPRODUCED**. That equals `verdict.txt` line for line, and `reproduce.sh check` reports consistent.
- **J2/J4.** In both runs:
  - init `settled status=green points=1000000 indexed=1000000 segments=8 threshold_kb=1 window_s=30` (waited 84.2 s / 80.1 s);
  - all 5 cold settles show the same state, each after a full 30 s window (waited_s 30.05–30.06);
  - every warm aggregate's `index_*` samples have min = max = the init state.

  The J3 PATCH answered 200 in both runs. This is exactly the deterministic state that B-0-F7 lacked. The gate's 427 qps at ef16 was the 984,064-indexed state, which can no longer pass readiness.
- **J1.** The largest warm p50 is 3.58 ms (ef256) and the largest cold p50 is 3.58 ms, both well under 10 ms. At ef16, server p50 is 0.205 / 0.193 ms against the 0.21 ms fully indexed reference in B-0-F7.
- **Row = run A.** `results/2026-10-31/vector-sift1m-qdrant.json` is byte-identical to `repro/vector-sift1m-qdrant.run-a.json` (checked via `git show`, and by `check`).
- **Method not tuned.** J6, the 10 ms limit, `reproduce.sh` and the harness are unchanged since the F1 merge (70733f58). The rule was fixed before the data.
- **No gate re-measurement** (a deviation from GATE-PROMPT step 5, by the request's instruction, because X-5c-F1 is on the host). I judge none necessary:
  - the two pre-registered runs *are* the independent replication;
  - their conditions are corroborated by the journal;
  - J6 forbids a third run, so a gate measurement could not change the word.

# Findings

1. **(Minor, documentary.) Same-date overwrite of B-0-F7's row file.**
   - What happened: `nbench run` writes `results/<UTC date>/<row>.json`, and both runs took place on 2026-10-31, B-0-F7's date. So run A replaced B-0-F7's committed row (blob f4b2f475, commit 04479d54) in the working tree. `reproduce.sh` then moved it into repro/ and deleted REPORT.md. `verdict` wrote run A back to the same path and regenerated REPORT.md.
   - This breaks the letter of the card's "Do not edit or delete the 2026-10-31 files". The cause is the harness's designed flow meeting a date the card did not foresee, not a hand edit. The worker disclosed it under RESULT "anomalies (1)".
   - Consequence: the SUPERSEDED note says "the 2026-10-31 qdrant row … must not be used as a baseline" and names its replacement as the **same path**. B-0-F7's verdict and `diagnosis.md` also cite that path for the old numbers. The audit trail therefore depends on git history that the note does not name. The `row-*.txt`/`row-ef16-run-0.latency.csv` files in the same dir belong to the retired row.
   - Not blocking: the file now at that path is the correct, REPRODUCED row, and nothing was lost.
2. **(Minor, harness.) The quiet-window evidence is not committed.** `quiet_wait` logs to stderr only, and `run-*.log` (the `nbench run` tee) is gitignored by `*.log`. The first/last-sample lines live only in the local transcript and RESULT.

# Follow-ups

- **B-0-F7-F1b-F1** (P-authored, mechanical; `results/2026-10-31/vector-sift1m-qdrant.SUPERSEDED.md` only):
  - Rewrite the note so it names the retired row unambiguously: "B-0-F7's row, commit 04479d54, blob f4b2f475 (`git show 04479d54:nucleus/v3/bench/nbench/results/2026-10-31/vector-sift1m-qdrant.json`)".
  - State that the same path now holds run A of B-0-F7-F1b (verdict bc58fb63's REPRODUCED), and that REPORT.md was regenerated from it.
  - State that the `row-*` and `diagnosis*` files in this dir describe the retired row.

  This must land before NB-1 reads the row. It does not block C1.
- **B-0-F7-F1b-F2** (bench, S impl / P gate; `reproduce.sh` and its README):
  - `a` must refuse, before the quiet window, when `results/<UTC date>/<row>.json` is already tracked by git. Alternatively, `nbench run` must be pointed at a scratch results dir, so that a repro can never overwrite a committed row.
  - `quiet_wait`'s satisfied line (first/last load1 and the UTC time) must be appended to a tracked `repro/run-<x>.quiet.txt`.

  Add selftest scenarios for both. This is not needed for C1, because no further qdrant repro is scheduled.

# P0 condition C1

The qdrant row is now recorded, reproducible under the pre-registered rule, and in the deterministic index state. **C1 closes when this branch is merged into nucleus/v3-scaffold** (well before 2026-11-06). Neither follow-up blocks C1. F1 should land before NB-1 consumes the row.
