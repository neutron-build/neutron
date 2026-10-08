#!/usr/bin/env bash
# CI: type check + simulate + Quint conformance tests + Rust conformance tests.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
CONFORMANCE_DIR="$ROOT_DIR/conformance"

echo "=== Step 1: Quint Type Checking ==="
python3 "$SCRIPT_DIR/test_manifest.py"
bash "$SCRIPT_DIR/check.sh"

echo ""
echo "=== Step 2: Random Simulation ==="
bash "$SCRIPT_DIR/simulate.sh"

echo ""
echo "=== Step 3: Quint Conformance Tests ==="
python3 "$SCRIPT_DIR/manifest.py" test

echo ""
echo "=== Step 4: Rust Conformance Tests ==="
cd "$CONFORMANCE_DIR"
cargo test 2>&1

echo ""
echo "=== CI passed ==="
