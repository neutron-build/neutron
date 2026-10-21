#!/usr/bin/env bash
# No staged card may be silently dropped (PLAN-v3 §7.3.3, §7.6.5).
#
# Every cards/*.local.md that is a card (`# CARD` title) with `Status: ready`
# must either have a verdict merged on nucleus/v3-scaffold, or appear in
# QUEUE.local.md (in the cards dir) on a line with its ID and a state word
# (queued|running|gating|blocked|waiting). Exit 1 lists the orphans.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=git-env.sh
. "$here/git-env.sh"
cd "$here/.."
# The merged verdicts are read from nucleus/v3-scaffold's tree, which replace
# refs and grafts can rewrite: that state (git-env.sh) fails the check, after
# it has run under the environment that neutralises it.
state=0
git_state_refuse >&2 || state=1
# Cards are local-only (never committed); they live in the v3 checkout.
cards=${NV3_CARDS:-$HOME/Workspace/neutron-v3/nucleus/v3/cards}
q="$cards/QUEUE.local.md"
[ -f "$q" ] || { echo "queue-check: no $q" >&2; exit 1; }
orphans=0
for c in "$cards"/*.local.md; do
  head -1 "$c" | grep -q '^# CARD ' || continue
  grep -q 'Status:[[:space:]]*ready' "$c" || continue
  id=$(head -1 "$c" | sed -n 's/^# CARD \([^:]*\):.*/\1/p')
  git cat-file -e "nucleus/v3-scaffold:nucleus/v3/verdicts/$id.md" 2>/dev/null && continue
  if grep -Eq "(^|[^A-Za-z0-9-])$id([^A-Za-z0-9-]|$).*(queued|running|gating|blocked|waiting)" "$q"; then continue; fi
  echo "queue-check: orphan card $id (not merged, not in $q with a state)"
  orphans=1
done
[ "$orphans" -eq 0 ] && [ "$state" -eq 0 ] && echo "queue-check: ok"
exit $((orphans | state))
