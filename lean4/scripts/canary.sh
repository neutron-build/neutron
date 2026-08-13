#!/usr/bin/env bash
# Prove the proof checker is not vacuous.
#
# `lake build` going green tells you the build succeeded. It does not, by
# itself, tell you that anything was CHECKED — a workflow that restores a cache
# and compiles nothing looks identical to one that verified 68 theorems, and a
# `verify.sh` that silently skipped would look identical again.
#
# So: hand Lean a proposition that must not typecheck, and fail if it does.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(dirname "$SCRIPT_DIR")"
cd "$ROOT/Nucleus"

if ! command -v lake &>/dev/null; then
    echo "canary: lake not installed" >&2
    exit 1
fi

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

cat > "$TMP/Canary.lean" <<'LEAN'
-- Must NOT typecheck. If Lean accepts this, the checker is not running.
theorem canary_false : False := by trivial
LEAN

echo "=== Canary: a false proposition must be rejected ==="
if lake env lean "$TMP/Canary.lean" > "$TMP/out" 2>&1; then
    echo "CANARY FAILED: Lean accepted 'theorem canary_false : False'." >&2
    echo "The proof check is vacuous — a green run proves nothing." >&2
    cat "$TMP/out" >&2
    exit 1
fi

echo "Canary rejected as expected:"
head -3 "$TMP/out"

# The other half: assert the suite is not empty. A verify.sh pointed at zero
# files also exits 0.
FILES=$(find "$ROOT" -name '*.lean' -not -path '*/.lake/*' | wc -l | tr -d ' ')
THEOREMS=$(find "$ROOT" -name '*.lean' -not -path '*/.lake/*' -exec grep -Ehc \
    '^[[:space:]]*(private |protected )*(theorem|lemma) ' {} + 2>/dev/null \
    | awk '{n+=$1} END {print n+0}')
echo "Suite: $FILES files, $THEOREMS theorems."
if [ "$FILES" -lt 20 ] || [ "$THEOREMS" -lt 50 ]; then
    echo "CANARY FAILED: the suite shrank unexpectedly ($FILES files, $THEOREMS theorems)." >&2
    echo "Either files were removed, or the build is looking in the wrong place." >&2
    exit 1
fi

echo "=== Canary passed: the checker rejects what it must ==="
