#!/bin/sh
# nucleus/scripts/metrics.sh -- Compute codebase metrics and validate docs.
#
# Usage:
#   sh scripts/metrics.sh          # print current metrics
#   sh scripts/metrics.sh --check  # validate docs match reality (exit 1 on drift)
set -eu

NUCLEUS_DIR="$(cd "$(dirname "$0")/.." && pwd)"
SRC_DIR="$NUCLEUS_DIR/src"

# -- Compute metrics from source -----------------------------------------------

LOC=$(find "$SRC_DIR" -name '*.rs' -exec cat {} + | wc -l | tr -d ' ')
TESTS=$(grep -r '#\[test\]' "$SRC_DIR" --include='*.rs' | wc -l | tr -d ' ')
MODULES=$(find "$SRC_DIR" -mindepth 1 -maxdepth 1 -type d -not -name bin | wc -l | tr -d ' ')
RS_FILES=$(find "$SRC_DIR" -name '*.rs' | wc -l | tr -d ' ')
WAL_COUNT=$(find "$SRC_DIR" -name '*wal*' -name '*.rs' | wc -l | tr -d ' ')
TIERED_COUNT=$(find "$SRC_DIR" -name 'tiered.rs' | wc -l | tr -d ' ')

# Model-specific WAL existence
WAL_KV="no";       [ -f "$SRC_DIR/storage/kv_wal.rs" ]      && WAL_KV="yes"
WAL_GRAPH="no";    [ -f "$SRC_DIR/graph/wal.rs" ]            && WAL_GRAPH="yes"
WAL_DOC="no";      [ -f "$SRC_DIR/document/doc_wal.rs" ]     && WAL_DOC="yes"
WAL_VECTOR="no";   [ -f "$SRC_DIR/vector/wal.rs" ]           && WAL_VECTOR="yes"
WAL_BLOB="no";     [ -f "$SRC_DIR/blob/wal.rs" ]             && WAL_BLOB="yes"
WAL_FTS="no";      [ -f "$SRC_DIR/fts/fts_wal.rs" ]          && WAL_FTS="yes"
WAL_COLUMNAR="no"; [ -f "$SRC_DIR/storage/columnar_wal.rs" ] && WAL_COLUMNAR="yes"

# Count durable models
DURABLE=0
for w in $WAL_KV $WAL_GRAPH $WAL_DOC $WAL_VECTOR $WAL_BLOB $WAL_FTS $WAL_COLUMNAR; do
    [ "$w" = "yes" ] && DURABLE=$((DURABLE + 1))
done
[ -f "$SRC_DIR/storage/wal.rs" ] && DURABLE=$((DURABLE + 1))

# -- Default mode: print metrics -----------------------------------------------

if [ "${1:-}" != "--check" ]; then
    echo "LOC=$LOC"
    echo "TESTS=$TESTS"
    echo "MODULES=$MODULES"
    echo "RS_FILES=$RS_FILES"
    echo "WAL_COUNT=$WAL_COUNT"
    echo "TIERED_COUNT=$TIERED_COUNT"
    echo "DURABLE=$DURABLE"
    echo ""
    echo "WAL status per model:"
    echo "  Relational: yes (storage/wal.rs)"
    echo "  Columnar:   $WAL_COLUMNAR"
    echo "  KV:         $WAL_KV"
    echo "  FTS:        $WAL_FTS"
    echo "  Vector:     $WAL_VECTOR"
    echo "  Graph:      $WAL_GRAPH"
    echo "  Document:   $WAL_DOC"
    echo "  Blob:       $WAL_BLOB"
    exit 0
fi

# -- Check mode: validate docs match reality -----------------------------------
#
# Rather than broadly grepping for "N tests" (which matches per-module lines),
# we check a curated list of (file, line-pattern, metric, tolerance) tuples.
# Each doc's "header" or "summary" line has a known shape we can target.

rm -f "$NUCLEUS_DIR/scripts/.metrics_fail"
FAIL_COUNT=0

# check_line <file> <line-pattern> <number-keyword> <actual> <tolerance-pct>
# Finds a line matching <line-pattern>, then extracts "N <number-keyword>" from it.
check_line() {
    _file="$1"
    _line_pattern="$2"
    _num_keyword="$3"
    _actual="$4"
    _tol="${5:-0}"
    _basename=$(basename "$_file")

    if [ ! -f "$_file" ]; then
        return
    fi

    _line=$(grep -i "$_line_pattern" "$_file" 2>/dev/null | head -1 || true)
    if [ -z "$_line" ]; then
        return
    fi

    # Extract "N keyword" pair from the line (e.g., "1,999 tests" or "~135K LOC")
    _numstr=$(echo "$_line" | grep -ioE '[~]?[0-9][0-9,]*[kK]?[[:space:]]+'"$_num_keyword" | head -1 | grep -oE '[~]?[0-9][0-9,]*[kK]?' || true)
    if [ -z "$_numstr" ]; then
        return
    fi

    _approx=0
    case "$_numstr" in
        '~'*) _approx=1; _numstr=$(echo "$_numstr" | sed 's/^~//');;
    esac

    case "$_numstr" in
        *[kK])
            _numstr=$(echo "$_numstr" | sed 's/[kK]$//' | tr -d ',')
            _numstr=$((_numstr * 1000))
            ;;
        *)
            _numstr=$(echo "$_numstr" | tr -d ',')
            ;;
    esac

    _effective_tol=$_tol
    [ "$_approx" -eq 1 ] && _effective_tol=5

    _diff=$((_actual - _numstr))
    [ "$_diff" -lt 0 ] && _diff=$((-_diff))

    _threshold=0
    if [ "$_effective_tol" -gt 0 ] && [ "$_actual" -gt 0 ]; then
        _threshold=$((_actual * _effective_tol / 100))
    fi

    if [ "$_diff" -gt "$_threshold" ]; then
        echo "FAIL  $_basename: $_num_keyword claims $_numstr, actual is $_actual"
        echo "1" > "$NUCLEUS_DIR/scripts/.metrics_fail"
    else
        echo "OK    $_basename: $_num_keyword ($_numstr vs $_actual)"
    fi
}

echo "Checking doc metrics against source code..."
echo "  Actual: LOC=$LOC  TESTS=$TESTS  MODULES=$MODULES"
echo ""

D="$NUCLEUS_DIR"

# STATUS.md header: "Tests: **N passing**"  and "Modules: **N**"
check_line "$D/STATUS.md"         "^> Tests:"               "passing" "$TESTS"  0
check_line "$D/STATUS.md"         "^> Modules:"             "Modules" "$MODULES" 0

# NUCLEUS-ROADMAP.md line 4: "103K LOC, 53 modules, 2171 tests, ..."
check_line "$D/NUCLEUS-ROADMAP.md" "LOC.*modules.*tests"    "tests"   "$TESTS"  0
check_line "$D/NUCLEUS-ROADMAP.md" "LOC.*modules.*tests"    "modules" "$MODULES" 0
check_line "$D/NUCLEUS-ROADMAP.md" "LOC.*modules.*tests"    "LOC"     "$LOC"    5

# AUDIT-REPORT.md header: "N tests passing | N modules | ~N lines of Rust"
check_line "$D/AUDIT-REPORT.md"   "tests passing"           "tests"     "$TESTS"    0
check_line "$D/AUDIT-REPORT.md"   "tests passing"           "modules"   "$MODULES"  0
check_line "$D/AUDIT-REPORT.md"   "lines of Rust"           "lines"     "$LOC"      5

# TODO-NEXT.md header: "N tests passing | N modules | ~N lines of Rust"
check_line "$D/TODO-NEXT.md"      "tests passing"           "tests"     "$TESTS"    0
check_line "$D/TODO-NEXT.md"      "tests passing"           "modules"   "$MODULES"  0
check_line "$D/TODO-NEXT.md"      "lines of Rust"           "lines"     "$LOC"      5

# COMPETITOR-GAPS.md footer: "Tests: N passing"
check_line "$D/COMPETITOR-GAPS.md" "Tests:.*passing"        "passing" "$TESTS"  0

echo ""
if [ -f "$NUCLEUS_DIR/scripts/.metrics_fail" ]; then
    rm -f "$NUCLEUS_DIR/scripts/.metrics_fail"
    echo "Doc metrics are stale. Run 'sh scripts/metrics.sh' to see current values."
    exit 1
else
    echo "All doc metrics match source code."
    exit 0
fi
