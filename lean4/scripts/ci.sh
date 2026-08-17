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
echo "=== Step 3: Axiom Audit ==="
# Step 2 proves the proofs elaborate. It does not prove they prove anything:
# a `theorem` whose body is an `axiom` builds green, and until 2026-08-17 two
# headline results were exactly that, resting on axioms that were false.
bash "$SCRIPT_DIR/axioms.sh"

echo ""
echo "=== CI passed ==="
