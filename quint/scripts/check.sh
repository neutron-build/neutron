#!/usr/bin/env bash
# Type-check every Quint spec across all subdirectories.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
SPECS_DIR="$ROOT_DIR/specs"
CONFORMANCE_QNT="$ROOT_DIR/conformance/conformance_test.qnt"

echo "=== Quint Type Checking ==="

if ! command -v quint &>/dev/null; then
    echo "Quint not installed. Install: npm i -g @informalsystems/quint"
    exit 1
fi

fail=0
for spec in \
    "$SPECS_DIR"/common/*.qnt \
    "$SPECS_DIR"/nucleus/*.qnt \
    "$SPECS_DIR"/framework/*.qnt \
    "$SPECS_DIR"/realtime/*.qnt \
    "$CONFORMANCE_QNT"; do
    echo ""
    echo "--- Checking $(basename "$spec") ---"
    if quint typecheck "$spec" 2>&1; then
        echo "  Type checking passed"
    else
        echo "  Type checking FAILED"
        fail=1
    fi
done

echo ""
if [ "$fail" -ne 0 ]; then
    echo "=== Type checking FAILED ==="
    exit 1
fi
echo "=== All specs type-checked ==="
