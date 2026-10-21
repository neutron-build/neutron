#!/usr/bin/env bash
# Entry point of the README↔JSON consistency check for the E6 results table
# (card X-6-F2-F1; verdicts X-6-F2 9fa57ac7 / 45a788f9 found the table was
# never validated against the committed record).
#
# The card's acceptance invokes this script by path from nucleus/v3:
#
#   cd nucleus/v3 && bash spikes/e6-opfs/check-results.sh
#
# Invariants this wrapper owns:
#   * cwd independence — the checker is located relative to this file, so
#     the command above works from any directory;
#   * fail-closed — the wrapper never turns an environment problem into a
#     pass. The check itself is harness/check-results.mjs, on the spike's
#     existing node toolchain (the same runtime harness/run.mjs requires;
#     no new dependency): it re-derives every Results-table cell from the
#     committed results/full-*.json and exits 0 only on an exact match.
#     This wrapper propagates that exit code verbatim (via exec) and exits
#     2 — the checker's "structural error" code — if node or the checker
#     is missing, because an environment that cannot run the check must
#     not count as passing it.
#
# Arguments pass through unchanged. --readme=<copy> and --json=<copy> swap
# one input for a copy outside the repo (the card's mutation evidence:
# README-DRIFT, JSON-MUTANT); see check-results.mjs for their semantics.

set -u

here="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
checker="$here/harness/check-results.mjs"

if [ ! -f "$checker" ]; then
  echo "ERROR the checker $checker is missing; the Results check cannot run" >&2
  exit 2
fi
if ! command -v node >/dev/null 2>&1; then
  echo "ERROR node is not on PATH — the E6 Results check runs on node (the spike's" >&2
  echo "harness toolchain, cf. harness/run.mjs) and cannot be skipped or substituted" >&2
  exit 2
fi

exec node "$checker" "$@"
