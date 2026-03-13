#!/usr/bin/env bash
# Verify all Lean 4 proofs using `lake build`.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(dirname "$SCRIPT_DIR")"

echo "=== Lean 4: Checking all proofs ==="
cd "$ROOT/Nucleus"

if ! command -v lake &>/dev/null; then
    echo "Lean 4 / Lake not installed."
    echo "Install: https://leanprover-community.github.io/install/linux.html"
    exit 1
fi

lake build

echo "=== All proofs verified ==="
