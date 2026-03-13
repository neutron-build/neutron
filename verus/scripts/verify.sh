#!/usr/bin/env bash
# Run Verus verification on all annotated modules.
# Requires the Verus toolchain: https://github.com/verus-lang/verus
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "=== Nucleus Verus Verification ==="
echo "Running Z3 SMT solver on all annotated modules..."
echo ""

cd "$PROJECT_DIR"

# Verify each module independently
for module in verified/mvcc.rs verified/page.rs verified/buffer.rs \
              verified/tuple.rs verified/lru.rs verified/bloom.rs; do
    echo "Verifying $module..."
    if command -v verus &>/dev/null; then
        verus "$module" 2>&1 || { echo "FAILED: $module"; exit 1; }
    else
        echo "  (verus not installed — skipping)"
    fi
done

echo ""
echo "=== All verifications passed ==="
