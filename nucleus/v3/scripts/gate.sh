#!/usr/bin/env bash
# The full workspace gate (PLAN-v3 §7.4). Every card's last Acceptance line.
#
# Prints `STEP <name> EXIT=<n>` after each step and `GATE EXIT=<n>` last, and
# exits with that code. Orchestrators and gates read only the GATE line: a
# runner summary that says "pass" next to `GATE EXIT=1` is a failure (§7.6.4).
# Steps run even after a failure so one run reports every problem.
#
# One step can be skipped: enforcement-tests, the tooling self-tests, when the
# change under test leaves the tooling alone (enforcement_decision below). A
# skipped step prints `STEP enforcement-tests SKIPPED (...)`, which no reader
# takes for an `EXIT=` line, and never changes GATE EXIT. NV3_GATE_FULL set to
# anything non-empty runs every step; promote-pass.sh and CI set it.
set -uo pipefail
cd "$(dirname "$0")/.."

gate=0
step() {
  local name=$1
  shift
  "$@" 2>&1
  local rc=$?
  echo "STEP $name EXIT=$rc"
  [ "$rc" -eq 0 ] || gate=1
}

# ENF-FASTGATE-1 (the owner's 2026-11-27 yes, QUEUE head line): the
# enforcement suite tests the runner, merge and audit tooling, never engine
# code, and it is the slowest and flakiest step, so it runs only when the
# change under test touches what it tests: scripts/ and tools/, plus the three
# other files it copies from this tree (CORE_PATHS and the two READMEs).
# Every engine step below runs on every card. promote-pass.sh always runs it
# (NV3_GATE_FULL=1), once per pass, so a tooling regression still stops the
# scaffold from moving.
#
# The change under test is base...HEAD for each scaffold ref that exists
# (local and origin: two refs that disagree give the union, and an older ref
# only widens it), plus anything staged, unstaged or untracked (ignored files
# included) under those paths. The skip happens only when all of that was
# read and is empty: no NV3_GATE_FULL, no git state that hides changes
# (git-env.sh neutralises it and git_state_refuse names it), at least one ref
# resolved, every git call succeeded and wrote nothing on stderr. Anything
# else runs the suite, and the reason is printed above its STEP line. The
# decision is the exact word `skip` on stdout, never the absence of a reason,
# so a decision that dies part-way runs the suite.
#
# Read before any step runs, so nothing a step writes changes it. In a
# subshell, so git-env.sh's variables do not reach the steps.
tooling=(scripts tools CORE_PATHS docs/README.md verdicts/README.md)
enforcement_decision() (
  full() { echo "$*"; exit 0; }
  [ -z "${NV3_GATE_FULL:-}" ] || full "NV3_GATE_FULL is set"
  # shellcheck source=git-env.sh
  . scripts/git-env.sh || full "cannot source scripts/git-env.sh"
  git_state_refuse --index >/dev/null || full "git state hides changes: ${GIT_STATE_PROBLEMS//$'\n'/; }"
  local found=0 ref mb out
  for ref in refs/heads/nucleus/v3-scaffold refs/remotes/origin/nucleus/v3-scaffold; do
    git show-ref -q --verify "$ref" || continue
    mb=$(git merge-base "$ref" HEAD) || full "no merge-base of $ref and HEAD"
    out=$(git diff --name-only --no-renames --no-relative --ignore-submodules=none "$mb" HEAD -- "${tooling[@]}") || full "git diff $mb HEAD failed"
    [ -z "$out" ] || full "the change touches the tooling: ${out//$'\n'/ }"
    found=1
  done
  [ "$found" -eq 1 ] || full "no nucleus/v3-scaffold ref to diff against"
  out=$(git diff --name-only --no-renames --no-relative --ignore-submodules=none HEAD -- "${tooling[@]}") || full "git diff HEAD failed"
  [ -z "$out" ] || full "uncommitted tooling changes: ${out//$'\n'/ }"
  out=$(git ls-files --others --full-name -- "${tooling[@]}") || full "git ls-files --others failed"
  [ -z "$out" ] || full "untracked tooling files: ${out//$'\n'/ }"
  echo skip
)
enf=$(enforcement_decision 2>&1) || enf="the decision failed (exit $?): $enf"
[[ $enf != *$'\n'skip ]] || enf="git wrote to stderr: ${enf%$'\n'skip}"

# The crates that must build for the browser (PLAN-v3 §1.6). A crate is added
# here by the card that makes it wasm-safe.
wasm_crates=(-p nucleus-env -p nucleus-store)

step test cargo test -q --workspace
step clippy cargo clippy -q --workspace --all-targets -- -D warnings
step fmt cargo fmt --all -- --check
step deny cargo deny --log-level error check
step wasm32 cargo build -q --target wasm32-unknown-unknown "${wasm_crates[@]}"
step core-paths-selftest bash scripts/check-core-paths.sh --self-test
if [ "$enf" = skip ]; then
  echo "STEP enforcement-tests SKIPPED (no scripts/ or tools/ change)"
else
  echo "gate: enforcement-tests runs: ${enf//$'\n'/; }"
  step enforcement-tests bash scripts/test-enforcement.sh
fi
step api-surface bash scripts/check-api-surface.sh
echo "GATE EXIT=$gate"
exit "$gate"
