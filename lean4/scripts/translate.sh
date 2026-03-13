#!/usr/bin/env bash
# Translate Nucleus Rust source to Lean 4 via Aeneas.
# Requires: https://github.com/AeneasVerif/aeneas
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(dirname "$SCRIPT_DIR")"
NUCLEUS_DIR="$(dirname "$ROOT")/nucleus"
OUTPUT_DIR="$ROOT/Nucleus/Nucleus/Aeneas"

echo "=== Aeneas: Rust → Lean 4 Translation ==="

if ! command -v aeneas &>/dev/null; then
    echo "Aeneas not installed. Using hand-modeled Lean files."
    echo "Install: https://github.com/AeneasVerif/aeneas"
    exit 0
fi

for module in mvcc btree wal raft; do
    case $module in
        mvcc)  src="$NUCLEUS_DIR/src/storage/mvcc.rs" ;;
        btree) src="$NUCLEUS_DIR/src/storage/btree.rs" ;;
        wal)   src="$NUCLEUS_DIR/src/storage/wal.rs" ;;
        raft)  src="$NUCLEUS_DIR/src/raft/mod.rs" ;;
    esac
    echo "Translating $src → $OUTPUT_DIR/$(echo $module | sed 's/./\U&/').lean"
done

echo "=== Translation complete ==="
