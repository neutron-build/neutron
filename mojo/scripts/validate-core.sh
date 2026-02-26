#!/usr/bin/env bash
set -euo pipefail

LIST_ONLY=0
if [[ "${1:-}" == "--list-only" ]]; then
  LIST_ONLY=1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CORE_DIR="$ROOT_DIR/neutron-mojo"
TEST_DIR="$CORE_DIR/test"
REPORT_DIR="$ROOT_DIR/reports"

mkdir -p "$REPORT_DIR"

resolve_mojo() {
  if [[ -n "${MOJO_BIN:-}" && -x "${MOJO_BIN}" ]]; then
    echo "${MOJO_BIN}"
    return 0
  fi

  if command -v mojo >/dev/null 2>&1; then
    command -v mojo
    return 0
  fi

  local pixi_mojo="$CORE_DIR/.pixi/envs/default/bin/mojo"
  if [[ -x "$pixi_mojo" ]]; then
    echo "$pixi_mojo"
    return 0
  fi

  return 1
}

shopt -s nullglob
TEST_FILES=("$TEST_DIR"/test_*.mojo)
shopt -u nullglob

TOTAL="${#TEST_FILES[@]}"
if [[ "$TOTAL" -eq 0 ]]; then
  echo "ERROR: No core tests found at $TEST_DIR"
  exit 1
fi

TIMESTAMP="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
SUMMARY_FILE="$REPORT_DIR/core-validation-latest.md"
RUN_SUMMARY_FILE="$REPORT_DIR/core-validation-${RUN_ID}.md"
CSV_FILE="$REPORT_DIR/core-validation-${RUN_ID}.csv"
LOG_FILE="$REPORT_DIR/core-validation-${RUN_ID}.log"

MOJO_BIN_PATH="(not found)"
if resolved="$(resolve_mojo 2>/dev/null)"; then
  MOJO_BIN_PATH="$resolved"
fi

if [[ "$LIST_ONLY" -eq 1 ]]; then
  {
    echo "# Mojo Core Validation (List Only)"
    echo
    echo "- Timestamp (UTC): $TIMESTAMP"
    echo "- Mojo binary: \`$MOJO_BIN_PATH\`"
    echo "- Core test files discovered: $TOTAL"
    echo "- Execution: not run (\`--list-only\`)"
    echo
    echo "## Tests"
    for test_path in "${TEST_FILES[@]}"; do
      echo "- \`$(basename "$test_path")\`"
    done
  } > "$SUMMARY_FILE"
  cp "$SUMMARY_FILE" "$RUN_SUMMARY_FILE"
  echo "Wrote list-only summary: $SUMMARY_FILE"
  exit 0
fi

if [[ "$MOJO_BIN_PATH" == "(not found)" ]]; then
  echo "ERROR: Mojo executable not found."
  echo "Set MOJO_BIN or install the neutron-mojo pixi environment first."
  exit 1
fi

# If we run the Mojo binary directly from a pixi env, activate MAX variables.
if [[ "$MOJO_BIN_PATH" == *"/.pixi/envs/default/bin/mojo" ]]; then
  ENV_PREFIX="$(dirname "$(dirname "$MOJO_BIN_PATH")")"
  ACTIVATE_SCRIPT="$ENV_PREFIX/etc/conda/activate.d/10-activate-max.sh"
  if [[ -f "$ACTIVATE_SCRIPT" ]]; then
    export CONDA_PREFIX="$ENV_PREFIX"
    # shellcheck disable=SC1090
    source "$ACTIVATE_SCRIPT"
  fi
fi

PASS_COUNT=0
FAIL_COUNT=0
SKIP_HINT_COUNT=0

{
  echo "test,status,exit_code"
} > "$CSV_FILE"

{
  echo "Mojo core validation run"
  echo "timestamp=$TIMESTAMP"
  echo "mojo_bin=$MOJO_BIN_PATH"
  echo
} > "$LOG_FILE"

for test_path in "${TEST_FILES[@]}"; do
  test_name="$(basename "$test_path")"
  echo ">>> Running $test_name" | tee -a "$LOG_FILE"

  set +e
  output="$(
    cd "$CORE_DIR"
    "$MOJO_BIN_PATH" run -I src "test/$test_name" 2>&1
  )"
  exit_code=$?
  set -e

  printf "%s\n" "$output" >> "$LOG_FILE"
  echo >> "$LOG_FILE"

  status="pass"
  if [[ "$exit_code" -ne 0 ]]; then
    status="fail"
    FAIL_COUNT=$((FAIL_COUNT + 1))
  else
    PASS_COUNT=$((PASS_COUNT + 1))
  fi

  if grep -q "SKIP" <<<"$output"; then
    SKIP_HINT_COUNT=$((SKIP_HINT_COUNT + 1))
  fi

  echo "${test_name},${status},${exit_code}" >> "$CSV_FILE"
done

{
  echo "# Mojo Core Validation"
  echo
  echo "- Timestamp (UTC): $TIMESTAMP"
  echo "- Mojo binary: \`$MOJO_BIN_PATH\`"
  echo "- Total tests: $TOTAL"
  echo "- Passed: $PASS_COUNT"
  echo "- Failed: $FAIL_COUNT"
  echo "- Tests with SKIP output hints: $SKIP_HINT_COUNT"
  echo
  echo "## Artifacts"
  echo
  echo "- CSV: \`$(basename "$CSV_FILE")\`"
  echo "- Log: \`$(basename "$LOG_FILE")\`"
} > "$SUMMARY_FILE"

cp "$SUMMARY_FILE" "$RUN_SUMMARY_FILE"

echo "Validation summary: $SUMMARY_FILE"
echo "Validation CSV: $CSV_FILE"
echo "Validation log: $LOG_FILE"

if [[ "$FAIL_COUNT" -ne 0 ]]; then
  exit 1
fi
