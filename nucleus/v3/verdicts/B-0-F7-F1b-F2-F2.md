Card: B-0-F7-F1b-F2-F2
Class: bench
Card-gate: P
Verdict: MERGE
Gate-tier: P
Gate-model: claude-opus-5-5 (medium)
Gate-session: nv3-B-0-F7-F1b-F2-F2-gate-1 (P review sub-agent, Claude Code, T3 thread 45584af2-e651-490e-862c-3402320cffa3)
Date: 2026-11-27
Gate-exit: GATE EXIT=0
Commit: 22d7e606dc7f63f386223b13a35c61a1724b38f8
Mutants: MARKER-OMITTED -> S7b full-line + marker-present, S10b (5 FAILs); MARKER-ON-REAL -> S9 no-marker line, S10c (2 FAILs); MARKER-ON-BYPASS -> S7a end-anchored run-a + run-b (2 FAILs); CHECK-QUIET-UNWIRED -> S10a + S10b (6 FAILs); CHECK-BYPASSED-ONLY -> S10b (3 FAILs); CHECK-IGNORES-TRACKED -> S1/S2/S3 check consistent + S10e (4 FAILs); CHECK-REQUIRES-QUIET -> S10d (1 FAIL)
Followups: none

Reviewer: Opus (claude-opus-5-5, effort medium), P gate, tier M (card mutants only, per FAST-TRACK §2), round 1, on card tip 22d7e606.

## Gate run
Scaffold `fresh-gate.sh` + `git-env.sh` came from a shallow `--no-local` clone of
`nucleus/v3-scaffold` at d155924534028dfc147aaad3d0991575fae4a2c3 (no EXTRACT FAILED).
It ran under `systemd-run --user --scope --quiet -p CPUWeight=10000 --` (the scope worked).
fresh-gate lines:
- `fresh-gate: commit 22d7e606dc7f63f386223b13a35c61a1724b38f8`
- `fresh-gate: base d155924534028dfc147aaad3d0991575fae4a2c3`
- `fresh-gate: checks passed (4944 files hashed in 3.17 s)`
- `fresh-gate: 2 file(s) matched through their eol conversion`

Literal last line: `GATE EXIT=0` (wrapper rc=0). No `FAIL`/`not ok` rows and no known
load or zombie rows (FAST-TRACK §3), so no re-run was needed.

## Scope
`git diff nucleus/v3-scaffold...HEAD --stat`: only the qdrant workload's `reproduce.sh`
(+229/-18) and `README.md` (+17). Both are in Touch only. One commit, clean worktree.
Nothing in the diff calls `systemd-run`, `sudo`, `nsenter`, podman or docker. Git use in
the selftest stays in its own mktemp roots (`git init`/`add`/`commit` in S10 only).

## Correctness
- **Real line byte-identical.** The scaffold's `QUIET_LINE=... no nbench unit)` literal
  appears exactly once in the scaffold and once at the tip. The tip only appends `$marker`,
  a `local` that starts empty and is set only inside the `= "window"` branch. S9 observes
  the line through the production path with `NB_REPRO_SELFTEST` unset
  (`env -u NB_REPRO_SELFTEST -u NB_REPRO_SELFTEST_LOADAVG`):
  `2026-11-28T06:08:16Z quiet-host window satisfied (load1 0.50 -> 0.50, 13 consecutive samples; no cargo/rustc; no nbench unit)`.
  S7b's line:
  `... no nbench unit) [NB_REPRO_SELFTEST=window: stubbed loadavg, not a measurement]`.
- **Unchanged.** The BYPASSED branch returns before `local marker`, and its text is
  untouched. The window rule (13 samples, load1 < 4, pgrep/systemctl, 1800 s, exit 3) is
  unchanged. The multiset of `exit N`/`return N` sites is unchanged apart from the hoisted
  stub printfs.
- **`check` arm.** It sits between the row-file block and `check: consistent`. It flags a
  file only when it is tracked (`ls-files --error-unmatch`, relative to `-C $ROOT`) and
  greps for BYPASSED or the marker. It is inert for a missing file and in non-git roots.
- **2026-10-31 repro stays consistent.** In a git clone of the tip, real
  `reproduce.sh check` (stub `NB_BIN` that echoes the committed `verdict.txt`) printed
  `check: consistent (REPRODUCED)`, rc 0. The tip has no committed `run-*.quiet.txt`.
- `bash -n` and `shellcheck -S warning` are clean. Baseline selftest:
  `REPRO SELFTEST OK`, 126 `  ok` lines.

## Mutants
Each mutant ran in its own `git archive 22d7e606 nucleus/v3/bench | tar -x -m` extract,
applied with `perl -0pi`, with the diff checked non-empty. Every one ended
`REPRO SELFTEST FAIL`; the catching rows are listed in the header. The worktree was never
edited.

## Notes
NIT: the new comments in S9 and `st_run_real` cite the loadavg `cut` read as `:132`. After
this card's insertions it is at `:157`. Several card-era line references in the script now
drift the same way.
NIT: `st_run_real` reads `$real_bin` from the enclosing scope, so it must not be called
before S9 sets it. This is documented and matches `st_run_win`'s `$win_bin` pattern.
