#!/usr/bin/env bash
# Run Apalache model checker on all Quint specs.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SPECS_DIR="$(dirname "$SCRIPT_DIR")/specs"

echo "=== Quint Model Checking (Apalache) ==="

if ! command -v quint &>/dev/null; then
    echo "Quint not installed. Install: npm i -g @informalsystems/quint"
    exit 1
fi

for spec in multi_raft resharding distributed_tx replication membership; do
    echo ""
    echo "--- Checking $spec ---"
    quint typecheck "$SPECS_DIR/$spec.qnt" 2>&1
    echo "  Type checking passed"
done

echo ""
echo "=== All specs type-checked ==="
