# shellcheck shell=bash
# Shared CORE_PATHS matching. Sourced by check-core-paths.sh and
# check-api-surface.sh so both read the list the same way. Bash 3.2 safe.
#
# Entry forms (paths relative to nucleus/v2):
#   dir/          directory prefix
#   a/b.rs        exact file, or a directory of that name
#   crates/*/x/   glob (*, ?, [..]); `*` also matches `/`

# Entries from CORE_PATHS text on stdin: comments and blank lines dropped.
core_entries() {
  sed -e 's/#.*//' -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' | grep -v '^$' || true
}

# core_match PATH ENTRY. Unquoted ENTRY on the right of == is a glob on purpose.
core_match() {
  local p=$1 e=$2
  # shellcheck disable=SC2053
  case $e in
    */) [[ $p == $e* ]] ;;
    *) [[ $p == $e || $p == $e/* ]] ;;
  esac
}

# is_core PATH ENTRIES (newline-separated). Prints the matching entry.
is_core() {
  local p=$1 e
  while IFS= read -r e; do
    [ -n "$e" ] || continue
    if core_match "$p" "$e"; then
      printf '%s\n' "$e"
      return 0
    fi
  done <<EOF
$2
EOF
  return 1
}

# base_core_entries BASE V3DIR: the CORE_PATHS entries at BASE, on stdout.
# Exit 0: present and read. Exit 3: absent at a valid BASE (it predates the
# file); nothing printed, and each caller decides what that means. Exit 1: git
# could not answer; one line on stderr names BASE and the failed step, and
# nothing is printed on stdout, so no caller can mistake it for a list.
# Classified with `ls-tree`, not `cat-file -e BASE:path`. On git 2.47 that exits
# 128 for an absent path and for a bogus BASE alike, and 1 for a blob missing
# from the object store: "failed means absent" reads the missing blob as absent,
# and no exit code tells absent from unreadable. `ls-tree` reads only trees: an
# entry means present (the blob read is then checked on its own), no entry with
# exit 0 means absent, and non-zero means BASE itself cannot be read. A base git
# can diff (diff compares trees, never this blob) can still fail the read.
base_core_entries() {
  local base=$1 v3dir=$2 tmp rc=0 n rec meta mode type id text
  tmp=$(mktemp) || { echo "cannot create a temporary file to read $v3dir/CORE_PATHS at $base" >&2; return 1; }
  git ls-tree --full-tree -z "$base" -- "$v3dir/CORE_PATHS" >"$tmp" 2>/dev/null || rc=$?
  if [ "$rc" -ne 0 ]; then rm -f "$tmp"; echo "git ls-tree $base failed (exit $rc)" >&2; return 1; fi
  if [ ! -s "$tmp" ]; then rm -f "$tmp"; return 3; fi
  n=$(tr -cd '\000' <"$tmp" | wc -c | tr -d ' ')
  IFS= read -r -d '' rec <"$tmp" || true
  rm -f "$tmp"
  if [ "$n" != 1 ]; then echo "git ls-tree $base listed $n entries for $v3dir/CORE_PATHS, not one" >&2; return 1; fi
  meta=${rec%%$'\t'*}
  read -r mode type id <<EOF
$meta
EOF
  # A symlink is a blob too; only a regular file holds the list.
  case "$type:$mode" in
    blob:100644 | blob:100755) ;;
    *) echo "$v3dir/CORE_PATHS at $base is a $type (mode $mode), not a regular file" >&2; return 1 ;;
  esac
  # Into a variable, not piped into core_entries: a pipe drops git's exit status.
  text=$(git cat-file blob "$id" 2>/dev/null) || rc=$?
  if [ "$rc" -ne 0 ]; then echo "git cat-file blob $id at $base failed (exit $rc)" >&2; return 1; fi
  printf '%s\n' "$text" | core_entries
}
