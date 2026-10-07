#!/usr/bin/env bash
# Branches named s/* (standard-tier work, V2 plan §3) must not change a path
# listed in CORE_PATHS.
#
#   check-core-paths.sh BASE [BRANCH]   diff BASE...HEAD; exit 1 on a violation
#   check-core-paths.sh --self-test
#
# BRANCH defaults to $CORE_PATHS_BRANCH, then the checked-out branch (CI checks
# out detached, so pass it). CORE_PATHS is read from BASE, not the working
# tree, so a branch cannot exempt itself by editing the list. Renames count on
# both sides.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=core-paths-lib.sh
. "$here/core-paths-lib.sh"
v2=$(cd "$here/.." && pwd)

check() {
  local base=$1 branch=${2:-${CORE_PATHS_BRANCH:-}}
  if [ -z "$branch" ]; then
    branch=$(git -C "$v2" symbolic-ref --short -q HEAD || true)
  fi
  branch=${branch#refs/heads/}
  case $branch in
    s/*) ;;
    *)
      echo "core-paths: branch '${branch:-<detached>}' is not s/*, nothing to enforce"
      return 0
      ;;
  esac
  if ! git -C "$v2" rev-parse -q --verify "$base^{commit}" >/dev/null; then
    echo "core-paths: base ref '$base' not found (fetch full history in CI)" >&2
    return 2
  fi

  local prefix list changed p hit bad=0
  prefix=$(git -C "$v2" rev-parse --show-prefix)
  if ! list=$(git -C "$v2" show "$base:${prefix}CORE_PATHS" 2>/dev/null); then
    echo "core-paths: no CORE_PATHS at $base, using the working tree copy" >&2
    list=$(cat "$v2/CORE_PATHS")
  fi
  list=$(printf '%s\n' "$list" | core_entries)
  changed=$(git -C "$v2" diff --name-only --no-renames --relative "$base...HEAD")

  while IFS= read -r p; do
    [ -n "$p" ] || continue
    if hit=$(is_core "$p" "$list"); then
      echo "core-paths: $branch changes $p (CORE_PATHS: $hit)"
      bad=1
    fi
  done <<EOF
$changed
EOF
  if [ $bad -ne 0 ]; then
    echo "core-paths: s/* branches may not change core paths; move this work to a premium branch." >&2
    return 1
  fi
  echo "core-paths: $branch touches no core path"
}

self_test() {
  local r v fails=0
  tmp=$(mktemp -d)
  trap 'rm -rf "$tmp"' EXIT
  r=$tmp/repo
  v=$r/nucleus/v2
  v2=$v
  mkdir -p "$v/crates/a/src" "$v/crates/b/src/state" "$v/docs"
  git -C "$tmp" init -q -b main repo
  g() {
    git -C "$r" -c user.name=t -c user.email=t@t -c commit.gpgsign=false \
      -c core.hooksPath=/dev/null "$@"
  }
  printf '# c\ncrates/a/\ncrates/b/src/state/\ndocs/spec.md\ngates\ncrates/*/src/lock.rs\n' >"$v/CORE_PATHS"
  touch "$v/crates/a/src/lib.rs" "$v/crates/b/src/other.rs" "$v/docs/spec.md" "$r/README"
  g add -A && g commit -q -m base

  # case WANT BRANCH SCRIPT: SCRIPT runs in nucleus/v2 on a fresh BRANCH off main.
  case_() {
    local want=$1 br=$2 got
    g checkout -q -B "$br" main
    (cd "$v" && eval "$3")
    g add -A && g commit -q -m change
    if check main "$br" >/dev/null 2>&1; then got=pass; else got=fail; fi
    if [ "$got" != "$want" ]; then
      echo "self-test FAIL: [$br] $3 -> $got, want $want"
      fails=1
    fi
  }
  case_ pass s/ok      'echo x >> crates/b/src/other.rs'
  case_ fail s/dir     'echo x >> crates/a/src/lib.rs'
  case_ pass p/dir     'echo x >> crates/a/src/lib.rs'
  case_ pass feature   'echo x >> crates/a/src/lib.rs'
  case_ fail s/nested  'echo x > crates/b/src/state/m.rs'
  case_ fail s/exact   'echo x >> docs/spec.md'
  case_ pass s/sibling 'echo x > docs/spec.md.bak'
  case_ fail s/bare    'mkdir -p gates && echo x > gates/g.rs'
  case_ pass s/prefix  'echo x > gatesx'
  case_ fail s/glob    'mkdir -p crates/c/src && echo x > crates/c/src/lock.rs'
  case_ fail s/rename  'git mv crates/a/src/lib.rs crates/b/src/lib.rs'
  case_ fail s/unlist  'sed -i.bak "/crates\/a\//d" CORE_PATHS && rm CORE_PATHS.bak && echo x >> crates/a/src/lib.rs'
  case_ pass s/outside 'echo x >> ../../README'
  if check no-such-ref s/x >/dev/null 2>&1; then
    echo "self-test FAIL: unknown base ref accepted"
    fails=1
  fi
  if [ $fails -ne 0 ]; then return 1; fi
  echo "core-paths self-test: ok"
}

case ${1:-} in
  --self-test) self_test ;;
  ""|-h|--help) sed -n '2,11p' "$0"; exit 2 ;;
  *) check "$@" ;;
esac
