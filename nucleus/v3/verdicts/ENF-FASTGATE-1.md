Card: ENF-FASTGATE-1
Class: core
Card-gate: P
Verdict: REWORK
Gate-tier: P
Gate-model: claude-opus-5-5 (max)
Gate-session: claude-code session 2cfd6850-7710-4861-b604-4eed7f711100 (T3 Code review sub-agent of T3 thread 45584af2-e651-490e-862c-3402320cffa3; P gate of record, risk tier R; it wrote neither the card nor any ENF-FASTGATE-1 commit)
Date: 2026-11-27
Gate-exit: GATE EXIT=0
Commit: ce16f0902e2b361556183f877cdfd5c793d10e15
Mutants: each planted by this gate in its own `git archive | tar -x -m` extract of ce16f090, whole suite per mutant, each run in a private /run/user tmpfs (table under Mutants). Card: FG-1 -> GS4-GS15, GS17-GS22, promote row, FGS2; FG-2 -> GS14; FG-3 -> GS15, GS19; FG-4 -> promote row; FG-5 -> GS15, GS16 premise, GS17; FG-6 -> GS10-GS13; FG-7 -> GS18, GS22; FG-8 -> GS21; FG-9 -> GS1, GS2, GS16 premise, GS21 premise, FGS1; FG-10 -> GS8 x3, GS23; FG-11 -> GS1-GS3, GS5-GS22, GS16/GS21 premises, promote premise, FGS1; RN-1 -> GD, kp_each; RN-2 -> GF, ACC failing line (+19); RN-3 -> GD, GF; RN-4 -> M row. Own sweep: Z-NV3-IGNORED -> GS4, promote row; Z-PROMOTE-EMPTY -> promote row; Z-COMMITTED-IGNORED -> GS5-GS9, GS17, FGS2; Z-FIRST-REF-ONLY -> GS15, GS17; Z-STAGED-ONLY -> GS10; Z-UNSTAGED-ONLY -> GS11; Z-IGNORED-HIDDEN -> GS13; Z-SKIP-AS-PASS -> GS1-GS3, GS16/GS21 premises, promote premise, FGS1; Z-RUN-BRANCH-EMPTY -> as FG-1; Z-TAG-STANDS-IN -> GS15, GS19 (reason text only); Z-DEFER-SILENT -> GD; Z-INDEX-FLAGS-IGNORED UNCAUGHT; Z-SKIP-SUBSTRING UNCAUGHT; Z-NO-IGNORE-SUBMODULES UNCAUGHT; Z-DECISION-LATE UNCAUGHT (disclosed); Z-DEFER-WIDE UNCAUGHT; Z-CI-NOT-FULL UNCAUGHT (the suite reads no CI file). Probe: X-MASK-OFF (anomaly).
Followups: ENF-FASTGATE-1-F1

## Summary

The commit does what the card asks, inside its Limits. The fresh gate is green, and all 15 of the card's mutants fail their named rows. Change (2), the runner deferring the gate line, is safe: it removes no check that the merge path relies on.

Change (1) is not yet safe by the owner's condition. It fails in one case, which this gate demonstrated end to end with the scaffold's real `fresh-gate.sh`. A card that changes only a `.gitattributes` file can rewrite the checked-out bytes of a tooling script without changing its blob. The new decision then skips the suite at the card gate (`GATE EXIT=0`), where the old gate.sh failed it (`STEP enforcement-tests EXIT=2`, `GATE EXIT=1`).

The promote does not make up for this. Its forced-full run happens in a pass worktree made before the merge (ORCHESTRATOR §5 lines 110–114), and git does not rewrite a file whose blob is unchanged, so that run sees the old bytes and passes. The defect would reach the scaffold. CI (a fresh clone, forced full) is the first run to go red, after the scaffold has moved.

That falsifies the safeguard in P judgement (1), "a tooling regression is still caught before the scaffold moves", for this class. It also falsifies the card's Notes, "a card that wants it must actually leave ... the suite's other inputs alone — and then the suite's result is the scaffold's". An S card can reach this path: `nucleus/v3/.gitattributes` is inside `nucleus/v3/` and not in CORE_PATHS, so an S verdict merges it.

The fix is five plain pathspecs and a few rows (R1 below), and it is within the owner's conditional yes, because it is what makes change (1) meet the condition. The rest of the design held under every forcing route this gate tried.

## The owner's condition: ruling

- **Engine quality: the condition holds.** Every engine step (`test`, `clippy`, `fmt`, `deny`, `wasm32`, `core-paths-selftest`, `api-surface`) still runs on every gate run, unchanged (Limit 1, read in the diff; the fresh gate ran each of them). The only step the decision can skip is `enforcement-tests`, and the suite runs no engine code: its tree inputs are the five copies at lines 96–100 plus `$src/scripts` reads (all under scripts/), and the fake cargo answers every cargo call.
- **Runner deferral (change 2): the condition holds.** The runner's gate run happened in the worker's own long-lived worktree, which no gate trusts; the gate session's fresh-gate run is the full gate on a verified fresh checkout; the promote runs another. Nothing downstream reads the runner's `pass` as a gate. ORCHESTRATOR §2 sends `pass = true` to gating, and §3 sends it to a gate, which runs fresh-gate (GATE-PROMPT step 1); merge-card.sh needs a verdict; no script or tool reads RUNLOG. `Cargo.lock` is gitignored (`.gitignore:153`), so the runner's uncommitted-change check lost no lock-file drift that the old in-worktree gate run used to trip.
- **Tooling: the condition holds except R1.** Probed independently (see "What this gate did"): pathspec env vars, `diff.relative`/`diff.ignoreSubmodules`, a same-name tag, replace refs, index flags, a symlinked tools/, a foreign `GIT_DIR`, a failing or warning git, no ref, the origin ref alone, and a stale or forged origin ref (which only widens) all run the suite or are inert. The promote forces full and nothing in gate.sh before the decision can unset `NV3_GATE_FULL`. Two cases skip wrongly. One is R1, attributes, which reaches the scaffold. The other is a criss-cross history with two merge bases (NIT 1), which does not reach it: a merge rewrites every file whose content it changes, so the promote tests the real merged bytes.

## What this gate did itself

- **Fresh gate** (GATE-PROMPT step 1, Fast-track). I extracted both scripts through the shallow `--no-local` clone; there was no EXTRACT FAILED line. Printed scaffold id: `d155924534028dfc147aaad3d0991575fae4a2c3`; `fresh-gate.sh` sha256 `40a5b232a5ac2c0cd1629ab6f7aa24396e1a6379d36db536a96a84ba35404899`. I started it at once, with no load wait, under `systemd-run --user --scope --quiet -p CPUWeight=10000 --`; the scope worked. It ran on `ce16f0902e2b361556183f877cdfd5c793d10e15` with `CARGO_TARGET_DIR=~/.cache/nv3-target/gate-ENF-FASTGATE-1`, from 16:37:23 to 16:57:36.
  - `fresh-gate:` lines, verbatim: `fresh-gate: commit ce16f0902e2b361556183f877cdfd5c793d10e15`, `fresh-gate: base d155924534028dfc147aaad3d0991575fae4a2c3`, `fresh-gate: checks passed (4944 files hashed in 1.49 s)`, `fresh-gate: 2 file(s) matched through their eol conversion`.
  - Steps: `STEP test EXIT=0`, `STEP clippy EXIT=0`, `STEP fmt EXIT=0`, `STEP deny EXIT=0`, `STEP wasm32 EXIT=0`, `STEP core-paths-selftest EXIT=0`, then `gate: enforcement-tests runs: the change touches the tooling: nucleus/v3/scripts/gate.sh nucleus/v3/scripts/promote-pass.sh nucleus/v3/scripts/test-enforcement.sh nucleus/v3/tools/run_card.py`, then `STEP enforcement-tests EXIT=0` and `STEP api-surface EXIT=0`.
  - Literal last line: `GATE EXIT=0`. Exit 0, stderr empty.
  - Cargo tests: 910 passed, 0 failed. Enforcement: 949 `ok`, 0 `FAIL` (`test-enforcement: all passed`). Inside the run: 26 GS rows ok, 3 FGS rows ok, the promote premise and row, GD premise, GD, GF, the new M row and `kp_each` all ok.
  - No known load row failed, so the step was not re-run. `$fg` was removed afterwards.
  - The decision's own line confirms the enforcement step ran for this card, as the card requires.
- **Scope and Limits.** `git diff --name-only d1559245… ce16f090…` lists exactly the five Touch-only files; the worktree is clean. The only removed lines are the ones each Limit allows:
  - gate.sh's old `step enforcement-tests` line;
  - promote-pass.sh's one command;
  - `kp_each`;
  - `gate_passed`'s one-line docstring;
  - the old `result` dict and retry message;
  - the dead `gate_passed` check in the loop.

  No other existing row changed (Limit 5); CI gained `env` plus a comment only (Limit 3).
- **Acceptance.**
  - Line 1, checked on my pristine extract: 26 `ok gate: GS` rows, 3 `ok fresh-gate: FGS` rows, the promote, GD and GF rows ok, rc 0.
  - Line 2: ok, in the worktree and by ids in the main repository.
  - Line 3: ok, on the commit's promote-pass.sh.
  - Line 4: the fresh gate.
- **Pristine suites** (fresh `git archive | tar -x -m` extracts of the tip):
  - Host, the only host suite running: 949 ok, 0 FAIL, rc 0, 298 s.
  - Inside a private mount namespace whose `/run/user/1000` is an empty tmpfs (fresh-gate's own mask): 949 ok, 0 FAIL, rc 0, 474 s, beside fresh-gate's suite and six mutants.
- **Probes of the shipped decision** (my own scratch repository holding the tip's gate.sh and git-env.sh; `~/.cache/nv3-review/gate-ENF-FASTGATE-1/probe/`).
  - Run the suite, as they should:
    - `GIT_LITERAL_PATHSPECS`, `GIT_GLOB_PATHSPECS` and `GIT_ICASE_PATHSPECS`, each with a tooling change;
    - a gitlink added under tools/ with `diff.relative=true` and `diff.ignoreSubmodules=all` in the repository config;
    - an assume-unchanged edited tools/ file (named by `git_state_refuse --index`, although plain `git diff HEAD` shows nothing);
    - tools/ replaced by a symlink to a spikes/ copy;
    - a foreign `GIT_DIR`;
    - `NV3_GATE_FULL=0`.
  - A tag `nucleus/v3-scaffold` at HEAD is ignored by the decision: it uses full refnames. A short-name `git rev-parse nucleus/v3-scaffold` does resolve to that tag; see NIT 4.
  - An empty `NV3_GATE_FULL` skips (by design: "unset or empty").
  - A caller's `BASH_ENV` can unset `NV3_GATE_FULL`. That is operator environment, not card input, and it can rewrite any bash script anyway.
  - Skip wrongly:
    - **P7:** a root `.gitattributes` with eol=crlf for the suite script skips, with CR bytes in the worktree file (R1).
    - **P13:** a criss-cross history skips although its tools/ differs from the scaffold's (NIT 1).
- **R1 end to end** (`probe/attr-e2e.sh`). A throwaway repository; the card commit adds only `nucleus/v3/.gitattributes` = `scripts/test-enforcement.sh text eol=crlf`; the stub suite starts with `set -euo pipefail`. Each run, through the scaffold's real `fresh-gate.sh`:
  - With the tip's gate.sh: `fresh-gate: 1 file(s) matched through their eol conversion`, `STEP enforcement-tests SKIPPED (no scripts/ or tools/ change)`, `GATE EXIT=0`.
  - With the base's gate.sh: `scripts/test-enforcement.sh: line 2: set: pipefail: invalid option name`, `STEP enforcement-tests EXIT=2`, `GATE EXIT=1`.
  - A pass worktree made before `git merge --no-ff card/v3-X`: 0 CR bytes in the suite script, `git status` clean, and `NV3_GATE_FULL=1 bash scripts/gate.sh` gives `STEP enforcement-tests EXIT=0`, `GATE EXIT=0`.
  - The same merge commit in a fresh checkout (what CI and every later fresh-gate see): `STEP enforcement-tests EXIT=2`, `GATE EXIT=1`.
- **Parsers re-checked.**
  - `gate_passed`, through the tip's run_card.py:
    - true on this fresh gate's own output;
    - true on a skip-shaped green output;
    - false on a skip-shaped red one.
  - `failure_text`, on a skip-shaped output with `STEP clippy EXIT=1`, lists only that line. `STEP enforcement-tests SKIPPED (...)` cannot fullmatch `STEP \S+ EXIT=[1-9][0-9]*`, and the reason line starts with `gate:`. A raw CR inside the reason line, from a config value, splits it in Python's `splitlines` but never into a matching line. The runner no longer feeds gate output to `failure_text` anyway.
  - Last-line readers: promote-pass.sh (`tail -n 1`), fresh-gate.sh (passes gate.sh's stdout through, so its caller reads the last line) and CI (exit code only). Each reads the last line or the exit code, and both are unchanged.
  - Verdict field: enforce-lib.sh:193 reads `Gate-exit:`, which is verdict text, not gate output.
  - No reader counts STEP lines or requires an `enforcement-tests` line.
  - `validate_card` still requires `GATE_CMD` last.
- **Runner.** GD, GF, the M row and `kp_each` read right, and RN-1 to RN-4 fail them. The existing rows whose Acceptance ends in the gate line do not lose meaning. Each checks its own earlier lines, for example XAE's second line `ct-alive.sh`, not the gate. The one exception was `kp_each`'s container count, which the card edits.

## Mutants

**Method.** Each mutant was planted by this gate in its own `git archive ce16f090 nucleus/v3 .forgejo | tar -x -m` extract, never in the worktree. Each was an exact-string edit that had to match exactly once (`mutants.py` in this gate's scratch). The whole suite, `bash scripts/test-enforcement.sh`, ran in each extract.

Each run happened inside a private mount namespace whose `/run/user/1000` is an empty tmpfs: the mask fresh-gate itself uses, here so that parallel suites never share the marker (see the anomaly section). The isolated pristine run is green (949 ok), so the isolation breaks no row.

Six lanes ran at once. Two more ran for the anomaly probe, so the peak was 8 suites, two over the Fast-track guidance.

The host also carried gate-A-3c's mutant binaries at 300%+ CPU each. Under that load, one unrelated row failed as load noise in 11 of this gate's 36 suite runs (FG-7, FG-8, FG-9, RN-1, RN-2, Z-SKIP-SUBSTRING, Z-NO-IGNORE-SUBMODULES, Z-TAG-STANDS-IN, Z-DECISION-LATE, Z-DEFER-WIDE, X-MASK-OFF.del): `runner e2e: Z: K's SIGTERM shape passes under a PID 1 that reaps no orphans`, with "premise failed: nothing holds the group of N (no zombie: the adopter did not work)". It is a sibling of the known K row, and it passed in both pristine runs, the fresh gate and round 1. One run also showed `premise: GUC: a -c core.untrackedCache=true beats the environment`, a known FAST-TRACK §3 load row. Neither is a target row. The table lists only the other FAIL rows.

| Mutant | Rows that failed (target rows) | Result |
|---|---|---|
| FG-1 SKIP-ALWAYS | GS4–GS15, GS17–GS22 (GS8 ×3), promote row, FGS2 (step SKIPPED in the container) | caught, as named |
| FG-2 SKIP-ON-UNKNOWN-BASE | GS14 | caught, as named |
| FG-3 SKIP-ON-GIT-FAILURE | GS15, GS19 | caught, as named |
| FG-4 PROMOTE-SKIPS | promote row (its gate printed SKIPPED, then `promote-pass: gate is red`) | caught, as named |
| FG-5 ORIGIN-IGNORED | GS15, GS16 premise, GS17 | caught, as named |
| FG-6 WORKTREE-IGNORED | GS10, GS11, GS12, GS13 | caught, as named |
| FG-7 STATE-IGNORED | GS18, GS22 | caught, as named |
| FG-8 STDERR-DROPPED | GS21 | caught, as named |
| FG-9 SKIP-SETS-GATE | GS1, GS2, GS16 premise, GS21 premise, FGS1 | caught, as named |
| FG-10 LIST-NARROWED | GS8 ×3, GS23 | caught, as named |
| FG-11 SUITE-ENV-LEAK (run with `NV3_GATE_FULL=1`) | the named GS1–GS3, GS16 premise, GS21 premise, promote premise, FGS1; also GS5–GS22, whose reason line becomes "NV3_GATE_FULL is set" | caught (wider than named) |
| RN-1 RUNNER-STILL-RUNS-GATE | GD, `kp_each` | caught, as named |
| RN-2 RUNNER-PASS-IGNORES-ACCEPTANCE | GF, ACC "the failing line fails the run", and 19 more rows whose Acceptance lines it defers (GD, the M rows, `kp_each`, ACC premises, opt-in) | caught, as named |
| RN-3 GATE-KEY-DROPPED | GD, GF | caught, as named |
| RN-4 RETRY-TEXT-OLD | the new M row | caught, as named |
| Z-NV3-IGNORED (the decision ignores `NV3_GATE_FULL`) | GS4, promote row | caught |
| Z-PROMOTE-EMPTY (`NV3_GATE_FULL= bash scripts/gate.sh`) | promote row | caught |
| Z-COMMITTED-IGNORED (the committed tooling diff is read and ignored) | GS5–GS9 (GS8 ×3), GS17, FGS2 | caught |
| Z-FIRST-REF-ONLY (`break` after the first ref found) | GS15, GS17 | caught |
| Z-STAGED-ONLY (`git diff --cached … HEAD`) | GS10 | caught |
| Z-UNSTAGED-ONLY (`git diff …` without HEAD) | GS11 | caught |
| Z-IGNORED-HIDDEN (`ls-files --others --exclude-standard`) | GS13 | caught |
| Z-INDEX-FLAGS-IGNORED (`git_state_refuse` without `--index`) | none (949 ok, rc 0) | UNCAUGHT (NIT 2) |
| Z-SKIP-SUBSTRING (`[[ $enf == *skip* ]]`) | none | UNCAUGHT (NIT 2) |
| Z-SKIP-AS-PASS (SKIPPED line printed as `STEP enforcement-tests EXIT=0`) | GS1, GS2, GS3, GS16 premise, GS21 premise, promote premise, FGS1 | caught |
| Z-RUN-BRANCH-EMPTY (reason printed, suite never run) | GS4–GS15, GS17–GS22 (GS8 ×3), promote row, FGS2 | caught |
| Z-NO-IGNORE-SUBMODULES (both `--ignore-submodules=none` dropped) | none | UNCAUGHT (NIT 2; equivalent unless `diff.ignoreSubmodules` is configured) |
| Z-TAG-STANDS-IN (short refnames through `rev-parse`) | GS15, GS19, through their reason text only | caught (NIT 2) |
| Z-DECISION-LATE (decision read after the engine steps) | none | UNCAUGHT (disclosed by the card; NIT 2) |
| Z-DEFER-WIDE (runner defers any line containing `scripts/gate.sh`) | none | UNCAUGHT (NIT 2) |
| Z-DEFER-SILENT (the `DEFERRED:` print removed) | GD | caught |
| Z-CI-NOT-FULL (the CI `env` removed) | none (949 ok, rc 0) | UNCAUGHT by construction: the suite reads no `.forgejo/` file (CI ruling) |
| X-MASK-OFF (anomaly probe: mask_env never hides /run/user) | without the deleter: 9 mask/opt-in/trust rows; with the deleter: only the opt-in "keeps" row | see the anomaly section |

## Defect (REWORK)

**R1. The decision does not treat `.gitattributes` as a suite input, so an attribute-only change skips the one fresh-checkout run of the suite.**

- **How the bytes change.** A `.gitattributes` in an ancestor directory of a tooling path can rewrite that file's checked-out bytes in every fresh checkout without changing its blob. The ancestors are the repository root, `nucleus/`, `nucleus/v3/`, and `nucleus/v3/docs/` and `nucleus/v3/verdicts/` for the two READMEs. Attribute files inside scripts/ and tools/ are already under the list's prefixes.
- **Why fresh-gate lets it through.** fresh-gate.sh accepts exactly one such rewrite, an `eol=crlf` conversion to pure CRLF (its step 4c exception), so its checks pass.
- **What the decision misses.** It compares blob ids (`git diff <mb> HEAD`) and the worktree through the same conversion (`git diff HEAD`), so it reads "no change" and skips.
- **The old flow ran the suite on those bytes.** Its fresh checkout was the one run before the scaffold moved that saw them, and bash fails on CRLF.
- **The promote cannot replace that run.** Its forced-full gate runs in a pass worktree made before `merge-card.sh` merges. Git rewrites no file whose blob is unchanged, so that run reads the old bytes. Measured above: an attribute-only card is green at the card gate and green at the promote, then red in every fresh checkout after the scaffold moves.
- **An S card can reach it.** `nucleus/v3/.gitattributes` is in V3 and not in CORE_PATHS, so enforce-lib.sh (lines 317–333) merges it on an S verdict, given a card whose Touch only names it. Root, `nucleus/` and docs/ attribute files need a P verdict.
- **The worst case is likely a fail-closed denial of service, not a bypass.** CRLF bash mostly stops with errors.
  - Targets: any tooling script the gate's own steps do not execute, for example the suite, enforce-lib.sh, merge-card.sh, audit-merges.sh, queue-check.sh or promote-pass.sh, broken in every fresh checkout.
  - The gate's steps execute gate.sh, git-env.sh, check-core-paths.sh, check-api-surface.sh and core-paths-lib.sh, so they would catch an attribute on any of those.
  - But this is exactly the "tooling defect that reaches the scaffold although the old flow would have stopped it" that the owner's condition excludes.
- **This is the only route found.** The other ways to change checked-out bytes without a blob change are refused by fresh-gate.sh: `info/attributes`, `core.attributesFile`, filter drivers, and any attribute other than an eol=crlf conversion (ident, working-tree-encoding).

**Card amendment for the rework** (the orchestrator records it in the card before relaunch):

1. Work 1, `<tooling>`: `scripts tools CORE_PATHS docs/README.md verdicts/README.md ../../.gitattributes ../.gitattributes .gitattributes docs/.gitattributes verdicts/.gitattributes`. Add to its reason: "and every attribute file that can apply to one of them. An attribute rewrites a tooling file's bytes in every fresh checkout without changing its blob (fresh-gate accepts an eol=crlf conversion). A pass worktree keeps the old bytes of an unchanged blob, so only the card gate's fresh checkout runs the suite on them (verdict ENF-FASTGATE-1 R1)."
   - These are plain pathspecs. This gate checked from `nucleus/v3`: `git diff --name-only -- ../../.gitattributes ../.gitattributes .gitattributes docs/.gitattributes verdicts/.gitattributes` lists every changed one (tested for the root, `nucleus/`, `nucleus/v3/` and docs/ files) under no env, `GIT_LITERAL_PATHSPECS=1`, `GIT_GLOB_PATHSPECS=1` and `GIT_ICASE_PATHSPECS=1`.
   - Any equivalent form is acceptable, for example running the suite for any changed path named `.gitattributes`.
2. Work 4, additions only:
   - GS24 and GS25: an attribute-only commit, at `nucleus/v3/.gitattributes` and at the root `.gitattributes`, runs the suite, and the reason names the file.
   - FGS3, in fresh-gate's real container: a commit adding only `nucleus/v3/.gitattributes` with `scripts/test-enforcement.sh text eol=crlf` over the throwaway's stub suite. It shows no SKIPPED line, the reason names `nucleus/v3/.gitattributes`, the CRLF stub fails (`STEP enforcement-tests EXIT=` non-zero), and the run ends `GATE EXIT=1`, exit 1.
3. Mutation evidence: FG-12 ATTR-DROPPED, the five attribute entries removed. It must fail GS24, GS25 and FGS3.
4. Acceptance line 1: GS count 26 → 28, FGS count 3 → 4.

Everything else in the commit stands; the re-gate covers the decision, the new rows, FG-12 and FG-1 to FG-11 again.

## CI hunk: keep it

The `.forgejo/workflows/nucleus-v3.yml` hunk stays.

- **Without it, CI would never run the suite on the scaffold.** On a scaffold push, the scaffold ref in CI's `fetch-depth: 0` checkout is HEAD itself, so the change under test is empty and the decision would skip every time. With no scaffold ref at all it would run. That would remove the only fresh-clone suite run after a promote.
- **That run catches R1 today,** after the fact, and it re-checks the suite in a second environment.
- **It costs no development time.** CI runs after the push, not on the card's path.
- **It keeps CI as it was before the card.** It removes nothing; it keeps CI from losing a check.
- **It is within the card's Touch only and Limit 3,** and within the yes for "those speed up changes", because it only declines to speed CI up.
- **No QUEUE note is needed.**

## NITs (not defects; for the rework if convenient)

- NIT 1, multiple merge bases. `git merge-base "$ref" HEAD` picks one base. A card whose history has two merge bases with the scaffold skips when its tooling equals the picked (older) base's, although that tooling differs from the scaffold's (probe P13).
  - This is scaffold-safe: the merge rewrites what it changes, and the promote tests that.
  - But the skip is then not a reading of the change.
  - `git diff "$ref"...HEAD` would warn on stderr ("multiple merge bases, using …"), which the decision would turn into a run. The explicit merge-base loses that warning.
  - `git merge-base --all` with a diff per base, or a run when there is more than one base, closes it.
- NIT 2, properties the code has but no row pins. All are correct today, and none can matter inside fresh-gate's verified fresh checkout. Each mutant below survived a whole-suite run:
  - the `--index` refusal (Z-INDEX-FLAGS-IGNORED, 949 ok);
  - `--ignore-submodules=none` (Z-NO-IGNORE-SUBMODULES);
  - the exact-word test `[ "$enf" = skip ]` (Z-SKIP-SUBSTRING);
  - the runner deferring exactly `GATE_CMD` and not any line naming gate.sh (Z-DEFER-WIDE);
  - the decision read before the steps (Z-DECISION-LATE; the card discloses it).

  The full-refname rule is caught only through reason text: Z-TAG-STANDS-IN fails GS15 and GS19 because their reasons spell `refs/…`. No row plants a same-name tag.
- NIT 3. `gate_passed`'s new docstring says promote-pass.sh applies its rule (rc 0 and the last line). promote-pass.sh reads only the last line (`|| true`). The two are equivalent while gate.sh exits with its own GATE value.
- NIT 4, pre-existing, outside this card. fresh-gate.sh's `--base` default and GATE-PROMPT's scope command (`git diff nucleus/v3-scaffold...HEAD`) resolve the short name, which a tag `refs/tags/nucleus/v3-scaffold` would shadow (probe P5). Contained workers cannot write refs/tags, so this is only a same-uid outside-the-container concern. The decision's full refnames are right.

## Anomaly: the podman opt-in row under parallel suites

**It is a real pre-existing test-isolation defect, not load, and it needs a follow-up (F1).**

- **The shared path.** Since before this card (base lines 3880–3881, 4244, 4251, 4254, 4899–4908; tip lines 4048–4052, 4415, 4422, 4425, 5089–5098), the suite touches and removes one fixed host path, `/run/user/$(id -u)/nv3-suite-marker`. Every host-level suite run by the same uid uses it: a promote beside a gate, parallel mutant runs, an implementer's run. One suite's `rm -f` can land inside another suite's window, between touching the marker (4051) and the opt-in listing (4422).
- **Load does not reproduce it.** I ran every mutant in a private mount namespace whose `/run/user/1000` is an empty tmpfs (the mask fresh-gate itself uses). Up to eight suites ran at once at load ~25–28, beside another gate's heavy cargo mutants. In 35 such runs the row failed only where the run's own mutant explains it: RN-2 defers the listing line, X-MASK-OFF.ctl's runner refused to start, and X-MASK-OFF.del ran the deleter. It passed in the other 32, in the host pristine run and in the fresh gate.
- **A concurrent `rm` reproduces it deterministically.** I put a deleter in the same private namespace, removing the marker every 50 ms in place of a concurrent suite's `rm`. The row failed (X-MASK-OFF.del).
- **It is worse than a false FAIL.**
  - Wherever `/run/user/<uid>` holds nothing but the marker, the marker is the only signal several rows read. That is so in fresh-gate's own container, and in my isolated runs. The rows: MASK-NONE, MASK-UNDOABLE, the nesting row, "the no-line run passes, contained", and the runner's own `runhide=0` start check.
  - The probe mutant X-MASK-OFF makes `mask_env` never hide `/run/user/<uid>`.
    - Without the deleter: 9 rows red. The runner itself refuses with `[XOM] cannot contain opencode … runhide=1`.
    - With the deleter: every one of those rows is `ok`. The only red row is the opt-in row, the one gates have been waving through as load (ENF-ZOMBIE-1 verdict, finding 5).
- **Why no run of record has been blinded yet.** On the host, other entries in the runtime dir (the bus socket) keep the companion checks honest. Inside fresh-gate's container, the private tmpfs keeps other suites out.
- **What it costs today:** false FAILs, and a habit of dismissing this row.
- **One more vacuous row.** "runner opt-in: a card with no Container line keeps the runtime dir masked" (`! grep -qx nv3-suite-marker "$1"`) passes when its listing file is missing. It did so in the control run, where the runner never started.

## Follow-ups

- **ENF-FASTGATE-1-F1: a per-run runtime marker in test-enforcement.sh.**
  - Protected `scripts/`, tooling hardening: per the QUEUE head line's NOT COVERED precedent it needs the owner's word, or a card the owner authorises for this script. It is not part of this card's rework (Limit 5 forbids editing existing rows).
  - The defect is as in the anomaly section above.
  - Fix:
    - Give the marker a per-run name (for example `nv3-suite-marker.$(basename "$t")`) in one variable, used by every row and probe that names it (tip 4048–4425 and 5089–5098, the python checks included).
    - Make each "not visible" assertion require its listing file to exist.
    - Treat the opt-in row as a real row again, not as load.
  - Rows:
    - Two suites started together both pass.
    - X-MASK-OFF fails the mask rows even with a concurrent deleter of a foreign marker.
    - A missing XOM listing fails the masked row.

## Orchestrator actions after the reworked card merges (agent context, not cards)

- WORKER-PROMPT.local.md lines 49 and 57–59, and FAST-TRACK.local.md §3 item 1 (line 39), still say the runner runs the gate after a runner card's attempt, reported as `RUNNER`. Replace that with "the runner defers it (`DEFERRED:`, `"gate": "deferred"`); the gate session's fresh-gate run is the full gate".
- ORCHESTRATOR.local.md §2's `pass = true` and the RUNLOG `pass` column now mean "pass without the gate".
- ORCHESTRATOR §3 item 1 ("runs `scripts/gate.sh`") is already superseded by GATE-PROMPT's fresh-gate rule; reword it while there.
