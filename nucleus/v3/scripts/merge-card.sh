#!/usr/bin/env bash
# Merge one gated card branch into the current scratch branch (PLAN-v3 §7.3.4, §7.6.1).
#
#   merge-card.sh <CARD-ID>
#
# Refuses unless:
#   - the current branch is scratch/* (merge passes are built there first);
#   - card/v3-<ID> carries nucleus/v3/verdicts/<ID>.md with Verdict MERGE or
#     MERGE-WITH-FOLLOWUPS, a Gate-exit of `GATE EXIT=0`, and a Gate-tier at
#     least the card's `Gate:` line (P > S);
#   - any change under CORE_PATHS has Gate-tier P;
#   - any change under docs/ comes from a card with Class: spec and Gate-tier P;
#   - every follow-up the verdict names is listed in QUEUE.local.md;
#   - the card's local file exists (cards/<ID>.local.md), so its tier is known.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
v3dir=$(cd "$here/.." && pwd)
cd "$v3dir"
# Cards are local-only (never committed); they live in the v3 checkout.
cards=${NV3_CARDS:-$HOME/Workspace/neutron-v3/nucleus/v3/cards}
id=${1:?usage: merge-card.sh <CARD-ID>}
branch=card/v3-$id
cur=$(git rev-parse --abbrev-ref HEAD)
die() { echo "merge-card: $id: $*" >&2; exit 1; }

case $cur in scratch/*) ;; *) die "current branch is $cur, not scratch/*" ;; esac
git rev-parse -q --verify "$branch" >/dev/null || die "no branch $branch"
card=$cards/$id.local.md
[ -f "$card" ] || die "no local card $card"
card_gate=$(sed -n 's/.*Gate:[[:space:]]*\([PS]\).*/\1/p' "$card" | head -1)
card_class=$(sed -n 's/^Class:[[:space:]]*\([a-z]*\).*/\1/p' "$card" | head -1)
[ -n "$card_gate" ] || die "card has no Gate: P|S line"

v=$(git show "$branch:nucleus/v3/verdicts/$id.md" 2>/dev/null) || die "no verdicts/$id.md on $branch"
field() { printf '%s\n' "$v" | sed -n "s/^$1:[[:space:]]*//p" | head -1; }
verdict=$(field Verdict)
tier=$(field Gate-tier)
gexit=$(field Gate-exit)
model=$(field Gate-model)
case $verdict in MERGE|MERGE-WITH-FOLLOWUPS) ;; *) die "verdict is '$verdict'" ;; esac
[ "$gexit" = "GATE EXIT=0" ] || die "Gate-exit is '$gexit', not 'GATE EXIT=0'"
[ -n "$model" ] || die "verdict has no Gate-model"
case $tier in P|S) ;; *) die "Gate-tier '$tier' is not P or S" ;; esac
if [ "$card_gate" = P ] && [ "$tier" != P ]; then die "card needs a P gate, verdict tier is $tier"; fi

base=$(git merge-base HEAD "$branch")
core=$(bash scripts/check-core-paths.sh "$base" "$branch")
if [ -n "$core" ] && [ "$tier" != P ]; then die "CORE_PATHS changed without a P verdict:
$core"; fi
if ! bash scripts/check-core-paths.sh --docs "$base" "$branch" >/dev/null; then
  [ "$card_class" = spec ] && [ "$tier" = P ] || die "docs/ changed by a non-spec card or without a P verdict"
fi
if [ "$verdict" = MERGE-WITH-FOLLOWUPS ]; then
  for f in $(field Followups | tr ',' ' '); do
    grep -q -- "$f" "$cards/QUEUE.local.md" 2>/dev/null || die "follow-up $f is not in $cards/QUEUE.local.md"
  done
fi
git merge --no-ff -q -m "nucleus v3: merge $branch ($verdict, gate $tier/$model)" "$branch"
echo "merge-card: $id merged into $cur ($verdict, gate $tier/$model)"
