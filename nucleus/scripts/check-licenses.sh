#!/usr/bin/env bash
# Fail on missing license metadata or dependency licenses outside the project's
# permissive/weak-copyleft policy. The workspace package itself is excluded.
set -euo pipefail

cd "$(dirname "$0")/.."
metadata="$(mktemp)"
trap 'rm -f "$metadata"' EXIT
cargo metadata --locked --format-version 1 >"$metadata"

missing=$(jq -r '
  .packages[]
  | select(.source != null and ((.license // "") | length == 0))
  | "\(.name) \(.version)"' "$metadata")
if [[ -n "$missing" ]]; then
  echo "Dependencies missing SPDX license metadata:"
  echo "$missing"
  exit 1
fi

forbidden=$(jq -r '
  .packages[]
  | select(.source != null)
  | select((.license // "") | test("(^|[^A-Z])(AGPL|GPL-|SSPL|BUSL|Commons[ -]Clause)"; "i"))
  | "\(.name) \(.version): \(.license)"' "$metadata")
if [[ -n "$forbidden" ]]; then
  echo "Dependencies with forbidden strong-copyleft/source-available licenses:"
  echo "$forbidden"
  exit 1
fi

count=$(jq '[.packages[] | select(.source != null)] | length' "$metadata")
echo "License metadata accepted for $count registry/git dependency packages."
