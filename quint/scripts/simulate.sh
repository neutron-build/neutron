#!/usr/bin/env bash
# Run random simulation on Quint specs.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SPECS_DIR="$(dirname "$SCRIPT_DIR")/specs"

echo "=== Quint Random Simulation ==="

if ! command -v quint &>/dev/null; then
    echo "Quint not installed."
    exit 1
fi

for spec in multi_raft resharding distributed_tx; do
    echo ""
    echo "--- Simulating $spec (1000 traces, 50 steps) ---"
    quint run --max-samples=1000 --max-steps=50 "$SPECS_DIR/$spec.qnt" 2>&1 || true
done

echo ""
echo "=== Simulation complete ==="
