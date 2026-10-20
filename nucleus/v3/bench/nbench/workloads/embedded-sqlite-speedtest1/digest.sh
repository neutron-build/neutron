#!/usr/bin/env bash
# Result-set digest for embedded-sqlite-speedtest1 (PLAN-v3 §5.2 rule 4).
#
# Strict at init and final: every speedtest1 run prints its own
# "Verification Hash" line (--verify; accumulator + 24-byte hash over
# all query results — 3.54.1 has no --sha3); hashing the *set* of unique
# hash lines across all runs of this row pins the whole row to one
# content fingerprint. The runner additionally compares per-run hash
# lines and voids the row if any two disagree.
set -euo pipefail
. "${NB_HARNESS}/lib/nb-lib.sh"

if ! grep -h "Verification Hash" "${NB_OUT:?}"/*.log 2>/dev/null | sort -u | sha256sum | cut -d' ' -f1; then
  echo "no Verification Hash lines found in logs" >&2
  exit 1
fi
