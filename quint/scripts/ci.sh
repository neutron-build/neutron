#!/usr/bin/env bash
# CI: type check + simulate + conformance tests.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONFORMANCE_DIR="$(dirname "$SCRIPT_DIR")/conformance"

echo "=== Step 1: Quint Type Checking ==="
bash "$SCRIPT_DIR/check.sh"

echo ""
echo "=== Step 2: Random Simulation ==="
bash "$SCRIPT_DIR/simulate.sh"

echo ""
echo "=== Step 3: Conformance Tests ==="
cd "$CONFORMANCE_DIR"
cargo test 2>&1

echo ""
echo "=== CI passed ==="
