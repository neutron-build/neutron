#!/usr/bin/env bash
# The full workspace gate (PLAN-v3 §7.4). Every card's last Acceptance line.
#
# Prints `STEP <name> EXIT=<n>` after each step and `GATE EXIT=<n>` last, and
# exits with that code. Orchestrators and gates read only the GATE line: a
# runner summary that says "pass" next to `GATE EXIT=1` is a failure (§7.6.4).
# Steps run even after a failure so one run reports every problem.
set -uo pipefail
cd "$(dirname "$0")/.."

gate=0
step() {
  local name=$1
  shift
  "$@"
  local rc=$?
  echo "STEP $name EXIT=$rc"
  [ "$rc" -eq 0 ] || gate=1
}

# The crates that must build for the browser (PLAN-v3 §1.6). A crate is added
# here by the card that makes it wasm-safe.
wasm_crates=(-p nucleus-env -p nucleus-store)

step test cargo test -q --workspace
step clippy cargo clippy -q --workspace --all-targets -- -D warnings
step fmt cargo fmt --all -- --check
step deny cargo deny --log-level error check
step wasm32 cargo build -q --target wasm32-unknown-unknown "${wasm_crates[@]}"
step core-paths-selftest bash scripts/check-core-paths.sh --self-test
echo "GATE EXIT=$gate"
exit "$gate"
