#!/usr/bin/env bash
# Prove the Quint gate is not vacuous.
#
# `quint test --match '.*_test'` exits 0 when it matches nothing, and
# `quint run` exits 0 on a spec whose invariant is never evaluated. Both look
# exactly like a passing verification run. This hands Quint work it must fail,
# and asserts the real suite is not empty.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(dirname "$SCRIPT_DIR")"

if ! command -v quint &>/dev/null; then
    echo "canary: quint not installed" >&2
    exit 1
fi

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

cat > "$TMP/canary.qnt" <<'QNT'
// Must FAIL. If this passes, the checker is not evaluating anything.
module canary {
  var n: int
  action init = n' = 0
  action step = n' = n + 1
  // Deliberately false the moment a single step is taken.
  val inv = n == 0
}
QNT

echo "=== Canary: a violated invariant must be reported ==="
if quint run --invariant=inv --max-steps=5 --max-samples=50 "$TMP/canary.qnt" \
        > "$TMP/out" 2>&1; then
    echo "CANARY FAILED: quint reported no violation of 'n == 0' after stepping n." >&2
    echo "The verification gate is vacuous — a green run proves nothing." >&2
    cat "$TMP/out" >&2
    exit 1
fi
echo "Violation reported as expected:"
grep -iE "violation|error|invariant" "$TMP/out" | head -3 || head -3 "$TMP/out"

# A `--match` that matches nothing exits 0. Count what actually ran.
echo ""
echo "=== Canary: the real conformance suite must not be empty ==="
TESTS=$(grep -cE "^[[:space:]]*run [A-Za-z0-9_]*_test" "$ROOT/conformance/conformance_test.qnt" || true)
echo "conformance_test.qnt declares $TESTS matching tests."
if [ "${TESTS:-0}" -lt 1 ]; then
    echo "CANARY FAILED: no tests match '.*_test', so `quint test` verifies nothing." >&2
    exit 1
fi

echo "=== Canary passed: the gate rejects what it must ==="
