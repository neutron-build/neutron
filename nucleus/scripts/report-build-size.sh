#!/usr/bin/env bash
# Report the size of the cargo target directories, and fail if one crosses a
# ceiling.
#
# Why this exists: nothing prunes `target/`. `nucleus/target/debug` reached
# 60 GB and took a development machine to zero bytes free — every profile, every
# dependency, incremental artifacts, and a separate test binary per probe_*,
# fuzz, bench, compete and stress target. Nothing was wrong with any single
# artifact; the directory simply grew until the disk ran out, twice, and both
# times it was cleared by hand after the fact. A-017.
#
# This is a REPORT first: it prints sizes on every run so growth is visible in
# the log before it is a problem. The ceiling only exists so "visible" does not
# depend on somebody reading the log.
#
# Usage:
#   sh scripts/report-build-size.sh            # report, fail past the ceiling
#   CEILING_GB=40 sh scripts/report-build-size.sh
#   sh scripts/report-build-size.sh --report-only
set -euo pipefail

CEILING_GB="${CEILING_GB:-25}"
REPORT_ONLY=0
[ "${1:-}" = "--report-only" ] && REPORT_ONLY=1

repo_root=$(cd "$(dirname "$0")/../.." && pwd)
status=0
total_kb=0

for dir in nucleus/target rust/target desktop/src-tauri/target; do
    path="$repo_root/$dir"
    [ -d "$path" ] || continue

    kb=$(du -sk "$path" 2>/dev/null | awk '{print $1}')
    total_kb=$((total_kb + kb))
    gb=$(awk -v k="$kb" 'BEGIN { printf "%.1f", k / 1048576 }')
    printf '%-32s %8s GB\n' "$dir" "$gb"

    # Per-profile breakdown, so a runaway profile is identifiable rather than
    # just a large total.
    for profile in debug release; do
        [ -d "$path/$profile" ] || continue
        pkb=$(du -sk "$path/$profile" 2>/dev/null | awk '{print $1}')
        pgb=$(awk -v k="$pkb" 'BEGIN { printf "%.1f", k / 1048576 }')
        printf '  %-30s %8s GB\n' "$profile" "$pgb"
    done
done

total_gb=$(awk -v k="$total_kb" 'BEGIN { printf "%.1f", k / 1048576 }')
printf '%-32s %8s GB  (ceiling %s GB)\n' "TOTAL" "$total_gb" "$CEILING_GB"

over=$(awk -v t="$total_gb" -v c="$CEILING_GB" 'BEGIN { print (t > c) ? 1 : 0 }')
if [ "$over" -eq 1 ] && [ "$REPORT_ONLY" -eq 0 ]; then
    echo
    echo "FAIL: cargo target directories total ${total_gb} GB, over the ${CEILING_GB} GB ceiling."
    echo "Run 'cargo clean' in the offending directory, or raise CEILING_GB if this is now normal."
    status=1
fi

exit "$status"
