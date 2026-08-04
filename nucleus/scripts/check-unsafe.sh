#!/usr/bin/env bash
# Keep unsafe Rust confined to reviewed low-level, FFI/SIMD, or test modules.
set -euo pipefail

cd "$(dirname "$0")/.."
actual="$(mktemp)"
expected="$(mktemp)"
trap 'rm -f "$actual" "$expected"' EXIT

# grep, not rg: ripgrep is not installed on the CI runner, so this script
# exited 127 (command not found) under `set -e` and the whole
# dependency/license/unsafe job failed without ever running the check.
grep -rlE '^[[:space:]]*(pub[[:space:]]+)?unsafe fn|^[[:space:]]*unsafe impl|unsafe[[:space:]]*\{' \
  src --include='*.rs' \
  | sort >"$actual"
sed -E 's/[[:space:]]+#.*$//; /^[[:space:]]*(#|$)/d' scripts/unsafe-allowlist.txt \
  | sort >"$expected"

if ! diff -u "$expected" "$actual"; then
  echo "Unsafe-code boundary changed. Review safety invariants, then update the allowlist."
  exit 1
fi
echo "Unsafe Rust remains confined to $(wc -l <"$actual" | tr -d ' ') reviewed files."
