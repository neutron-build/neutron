#!/usr/bin/env bash
# CI script: run both standard tests and Verus verification.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"

echo "=== Step 1: Standard Rust tests ==="
cargo test --all 2>&1

echo ""
echo "=== Step 2: Verus verification ==="
bash scripts/verify.sh

echo ""
echo "=== CI passed ==="
