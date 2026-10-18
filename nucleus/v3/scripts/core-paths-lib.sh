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
