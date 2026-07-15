#!/bin/sh
# Compute source metrics without pretending static declarations are test runs.
#
# Usage:
#   sh scripts/metrics.sh          # print the current inventory
#   sh scripts/metrics.sh --check  # verify the canonical plan's baseline
set -eu

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SRC="$ROOT/src"
TEST_DIR="$ROOT/tests"

count_matching_lines() {
    _dir=$1
    _pattern=$2
    find "$_dir" -type f -name '*.rs' -exec grep -Eh "$_pattern" {} + 2>/dev/null \
        | wc -l | tr -d ' '
}

LOC=$(find "$SRC" -type f -name '*.rs' -exec wc -l {} + | awk 'END {print $1}')
RS_FILES=$(find "$SRC" -type f -name '*.rs' | wc -l | tr -d ' ')
MODULES=$(find "$SRC" -mindepth 1 -maxdepth 1 -type d -not -name bin | wc -l | tr -d ' ')
UNIT_DECLARED=$(count_matching_lines "$SRC" '^[[:space:]]*#\[(tokio::)?test([^]]*)?\]')
INTEGRATION_DECLARED=$(count_matching_lines "$TEST_DIR" '^[[:space:]]*#\[(tokio::)?test([^]]*)?\]')
DECLARED_TESTS=$((UNIT_DECLARED + INTEGRATION_DECLARED))
UNIT_IGNORED=$(count_matching_lines "$SRC" '^[[:space:]]*#\[ignore([^]]*)?\]')
INTEGRATION_IGNORED=$(count_matching_lines "$TEST_DIR" '^[[:space:]]*#\[ignore([^]]*)?\]')
IGNORED_TESTS=$((UNIT_IGNORED + INTEGRATION_IGNORED))
BINARY_PROTOCOL_STUBS=$(count_matching_lines "$SRC/binary_wire/tests" '^[[:space:]]*#\[ignore([^]]*)?\]')
STRESS_IGNORED=$(find "$TEST_DIR" -type f -name '*.rs' -exec grep -Ehi \
    '^[[:space:]]*#\[ignore([^]]*)?(stress|scale|large|concurrent|crash|overflow|expression)' {} + \
    2>/dev/null | wc -l | tr -d ' ')
UNCLASSIFIED_INTEGRATION_IGNORES=$((INTEGRATION_IGNORED - STRESS_IGNORED))
WAL_FILES=$(find "$SRC" -type f -name '*wal*.rs' | wc -l | tr -d ' ')
TIERED_FILES=$(find "$SRC" -type f -name 'tiered.rs' | wc -l | tr -d ' ')

print_metrics() {
    echo "SOURCE_LOC=$LOC"
    echo "SOURCE_RS_FILES=$RS_FILES"
    echo "TOP_LEVEL_MODULES=$MODULES"
    echo "DECLARED_UNIT_TESTS=$UNIT_DECLARED"
    echo "DECLARED_INTEGRATION_TESTS=$INTEGRATION_DECLARED"
    echo "DECLARED_TESTS=$DECLARED_TESTS"
    echo "IGNORED_UNIT_TESTS=$UNIT_IGNORED"
    echo "IGNORED_INTEGRATION_TESTS=$INTEGRATION_IGNORED"
    echo "IGNORED_TESTS=$IGNORED_TESTS"
    echo "BINARY_PROTOCOL_STUBS=$BINARY_PROTOCOL_STUBS"
    echo "INTENTIONAL_STRESS_IGNORES=$STRESS_IGNORED"
    echo "UNCLASSIFIED_INTEGRATION_IGNORES=$UNCLASSIFIED_INTEGRATION_IGNORES"
    echo "WAL_SOURCE_FILES=$WAL_FILES"
    echo "TIERED_SOURCE_FILES=$TIERED_FILES"
    echo "EXECUTED_TESTS=not-measured (use CI/test output; cfg and parameterized tests change runtime counts)"
}

if [ "${1:-}" != "--check" ]; then
    print_metrics
    exit 0
fi

PLAN="$ROOT/DATABASE_COMPLETION.md"
if [ ! -f "$PLAN" ]; then
    echo "FAIL: canonical DATABASE_COMPLETION.md is missing" >&2
    exit 1
fi

assert_plan_value() {
    _label=$1
    _actual=$2
    if grep -Fq "$_label: $_actual" "$PLAN"; then
        echo "OK: $_label=$_actual"
    else
        echo "FAIL: DATABASE_COMPLETION.md must contain '$_label: $_actual'" >&2
        return 1
    fi
}

echo "Checking canonical plan metrics..."
fail=0
assert_plan_value "Source LOC" "$LOC" || fail=1
assert_plan_value "Source Rust files" "$RS_FILES" || fail=1
assert_plan_value "Top-level modules" "$MODULES" || fail=1
assert_plan_value "Declared unit tests" "$UNIT_DECLARED" || fail=1
assert_plan_value "Declared integration tests" "$INTEGRATION_DECLARED" || fail=1
assert_plan_value "Ignored tests" "$IGNORED_TESTS" || fail=1
assert_plan_value "Binary-protocol stubs" "$BINARY_PROTOCOL_STUBS" || fail=1
if [ "$UNCLASSIFIED_INTEGRATION_IGNORES" -ne 0 ]; then
    echo "FAIL: $UNCLASSIFIED_INTEGRATION_IGNORES integration ignores are not categorized" >&2
    fail=1
else
    echo "OK: every integration ignore is categorized stress/scale"
fi

if [ "$fail" -ne 0 ]; then
    echo "Current values:" >&2
    print_metrics >&2
    exit 1
fi
echo "Canonical plan metrics are current."
