#!/usr/bin/env bash
# CI: translate + verify all proofs.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=== Step 1: Aeneas Translation ==="
bash "$SCRIPT_DIR/translate.sh"

echo ""
echo "=== Step 2: Lean 4 Proof Verification ==="
bash "$SCRIPT_DIR/verify.sh"

echo ""
echo "=== CI passed ==="
