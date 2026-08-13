#!/bin/sh
# Reclaim disk without touching anything that is not regenerable.
#
#   sh nucleus/scripts/reclaim-disk.sh            # safe tier only
#   sh nucleus/scripts/reclaim-disk.sh --deep     # also drop cargo debug trees
#
# This exists because the failure is not "disk full". Nucleus refuses writes
# below a 3% free-disk watermark, so a near-full volume presents as the database
# going read-only mid-session, or as a test suite failing every case with an
# error that reads like a product bug. It has cost real time three times now.
#
# Nothing here touches source, git objects, application data, or any cache
# belonging to a program that is not a build tool.
set -eu

DEEP=0
[ "${1:-}" = "--deep" ] && DEEP=1

repo_root=$(cd "$(dirname "$0")/../.." && pwd)

free_gb() {
    df -k "$repo_root" | awk 'NR==2 { printf "%.1f", $4 / 1048576 }'
}

before=$(free_gb)
echo "Free before: ${before} GB"
echo

# ── tier 1: build-tool caches, always safe ────────────────────────────────
echo "Cargo incremental state (regenerated on next build):"
find "$repo_root" -maxdepth 4 -type d -name incremental -path '*/target/*' 2>/dev/null \
    | while read -r d; do
        printf '  %s  %s\n' "$(du -sh "$d" 2>/dev/null | awk '{print $1}')" "${d#"$repo_root"/}"
        rm -rf "$d"
    done

if command -v go >/dev/null 2>&1; then
    echo "Go build cache:"
    go clean -cache 2>/dev/null && echo "  cleared"
fi

if command -v pnpm >/dev/null 2>&1; then
    echo "pnpm store (unreferenced packages only):"
    pnpm store prune 2>&1 | tail -2 | sed 's/^/  /'
fi

if command -v brew >/dev/null 2>&1; then
    echo "Homebrew downloads:"
    brew cleanup --prune=all 2>&1 | tail -1 | sed 's/^/  /'
fi

echo "Stale conformance engine data directories:"
for d in /tmp/nucleus-live /tmp/nucleus-live-* /tmp/nucleus-l1 /tmp/nucleus-eng /tmp/nucleus-v2; do
    [ -d "$d" ] || continue
    # Never remove one with a live server behind it.
    if [ -f "$d/server.pid" ] && kill -0 "$(cat "$d/server.pid")" 2>/dev/null; then
        echo "  skipping $d (server still running)"
        continue
    fi
    echo "  $d"
    rm -rf "$d"
done

# ── tier 2: whole debug trees, opt-in ─────────────────────────────────────
if [ "$DEEP" -eq 1 ]; then
    echo
    echo "Cargo debug trees (--deep; next test run rebuilds from scratch):"
    if pgrep -x rustc >/dev/null 2>&1 || pgrep -x cargo >/dev/null 2>&1; then
        echo "  REFUSING: a cargo/rustc process is running. Removing a target tree"
        echo "  underneath a live build produces confusing failures. Try again after."
    else
        find "$repo_root" -maxdepth 3 -type d -path '*/target/debug' 2>/dev/null \
            | while read -r d; do
                printf '  %s  %s\n' "$(du -sh "$d" 2>/dev/null | awk '{print $1}')" "${d#"$repo_root"/}"
                rm -rf "$d"
            done
    fi
fi

echo
after=$(free_gb)
echo "Free after:  ${after} GB"
awk -v a="$before" -v b="$after" 'BEGIN { printf "Reclaimed:   %.1f GB\n", b - a }'
