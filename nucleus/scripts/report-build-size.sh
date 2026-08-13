#!/bin/sh
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
#
# Every documented invocation runs this under `sh`, which on Ubuntu is dash, so
# it stays POSIX: `set -o pipefail` is a bashism and dash exits 2 on it, which
# is how this failed the nucleus gate the first time it ran in CI.
set -eu

CEILING_GB="${CEILING_GB:-25}"
FLOOR_GB="${FLOOR_GB:-15}"
REPORT_ONLY=0
[ "${1:-}" = "--report-only" ] && REPORT_ONLY=1

repo_root=$(cd "$(dirname "$0")/../.." && pwd)
status=0
total_kb=0

# Discovered, not hardcoded. The list used to name desktop/src-tauri/target,
# which does not exist — the real one is desktop/target, and it reached 7.4 GB
# completely unseen by the very script written to see it. A hardcoded list of
# build directories rots the moment a tree is added or moved.
TARGET_DIRS=$(find "$repo_root" -maxdepth 3 -type d -name target \
    -not -path '*/node_modules/*' -not -path '*/.git/*' 2>/dev/null \
    | sed "s|^$repo_root/||" | sort)

for dir in $TARGET_DIRS; do
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

# Free space matters more than the total. Nucleus refuses writes below a 3%
# free-disk watermark, so a full disk does not present as "disk full" — it
# presents as the database going read-only mid-run, or as a conformance suite
# failing every case. Both happened on 2026-08-13.
free_kb=$(df -k "$repo_root" | awk 'NR==2 {print $4}')
free_gb=$(awk -v k="$free_kb" 'BEGIN { printf "%.1f", k / 1048576 }')
printf '%-32s %8s GB  (floor %s GB)\n' "FREE ON VOLUME" "$free_gb" "$FLOOR_GB"

low=$(awk -v f="$free_gb" -v m="$FLOOR_GB" 'BEGIN { print (f < m) ? 1 : 0 }')
if [ "$low" -eq 1 ]; then
    echo
    echo "WARNING: only ${free_gb} GB free. Nucleus goes read-only under 3% free"
    echo "and a build can consume several GB in minutes."
    echo "Reclaim with: sh nucleus/scripts/reclaim-disk.sh"
    [ "$REPORT_ONLY" -eq 0 ] && status=1
fi

over=$(awk -v t="$total_gb" -v c="$CEILING_GB" 'BEGIN { print (t > c) ? 1 : 0 }')
if [ "$over" -eq 1 ] && [ "$REPORT_ONLY" -eq 0 ]; then
    echo
    echo "FAIL: cargo target directories total ${total_gb} GB, over the ${CEILING_GB} GB ceiling."
    echo "Run 'cargo clean' in the offending directory, or raise CEILING_GB if this is now normal."
    status=1
fi

exit "$status"
