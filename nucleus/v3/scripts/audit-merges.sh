#!/usr/bin/env bash
# Re-check every change to nucleus/v3 on a branch (PLAN-v3 §7.6.1). CI runs it on
# nucleus/v3-scaffold, so anything that bypassed merge-card.sh is caught.
#
#   audit-merges.sh [REF]     (default HEAD)
#
# Every first-parent commit on REF that changes nucleus/v3 or the v3 CI workflow
# must be a merge titled "nucleus v3: merge card/v3-<ID> (...)" whose second
# parent passes the rules merge-card.sh applies (enforce-lib.sh, audit mode) and
# whose own tree is exactly the clean merge of its parents. A direct commit, a
# merge with another title, or an edit made inside a merge fails the audit.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=core-paths-lib.sh
. "$here/core-paths-lib.sh"
# shellcheck source=enforce-lib.sh
. "$here/enforce-lib.sh"
cd "$here/.."
ref=${1:-HEAD}
# The commit list goes through a file, not `done < <(git rev-list ...)`:
# process substitution drops git's exit status, so a bogus ref read as an empty
# history and printed "audit: ok (0 commits)". An audit that cannot list refuses.
lst=$(mktemp)
trap 'rm -f "$lst"' EXIT
git rev-list --first-parent "$ref" -- "${AUDIT_PATHS[@]}" >"$lst" || { echo "audit: cannot list commits on $ref (git rev-list failed)" >&2; exit 1; }
bad=0 n=0
while read -r sha; do
  n=$((n + 1))
  subject=$(git log -1 --format=%s "$sha")
  parents=$(git log -1 --format=%P "$sha" | wc -w)
  id=$(printf '%s' "$subject" | sed -n 's/^nucleus v3: merge card\/v3-\([A-Za-z0-9._-]*\) (.*/\1/p')
  if [ "$parents" -ne 2 ] || [ -z "$id" ]; then
    echo "audit: $(git rev-parse --short "$sha") '$subject' changes $V3 but is not a card merge"
    bad=1
    continue
  fi
  if ! errs=$(clean_merge "$sha"); then
    printf 'audit: %s (%s): %s\n' "$id" "$(git rev-parse --short "$sha")" "$errs"
    bad=1
  fi
  if ! errs=$(check_merge "$id" "$sha^1" "$sha^2" audit); then
    printf 'audit: %s (%s):\n%s\n' "$id" "$(git rev-parse --short "$sha")" "$errs"
    bad=1
  fi
done <"$lst"
[ "$bad" -eq 0 ] && echo "audit: ok ($n commits)"
exit $bad
