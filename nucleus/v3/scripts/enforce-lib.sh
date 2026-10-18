# shellcheck shell=bash
# Merge rules shared by merge-card.sh (before a merge) and audit-merges.sh (CI,
# after). One implementation so the two can never disagree. PLAN-v3 §7.6.
# Sourced from nucleus/v3/scripts; every git pathspec is anchored at the
# repository root (":/...") because callers run from nucleus/v3.

V3=nucleus/v3

# vfield TEXT KEY: the value of the first `KEY: value` line.
vfield() { printf '%s\n' "$1" | sed -n "s/^$2:[[:space:]]*//p" | head -1 | sed 's/[[:space:]]*$//'; }

# changed BASE HEAD [pathspec...]: changed paths, repository-relative.
# --no-renames: a rename must report its source path too, or moving a CORE file
# out of a protected directory would look like an ordinary new file.
changed() { local b=$1 h=$2; shift 2; git diff --no-renames --name-only "$b...$h" -- "$@"; }

# check_merge ID BASE HEAD MODE [CARD_GATE CARD_CLASS QUEUE_FILE]
# MODE is "merge" (pre-merge, card file known) or "audit" (CI, from git only).
# Prints one line per violation; returns 1 if there were any.
check_merge() {
  local id=$1 base=$2 head=$3 mode=$4 card_gate=${5:-} card_class=${6:-} queue=${7:-}
  local own="$V3/verdicts/$id.md" errs=() v
  if ! v=$(git show "$head:$own" 2>/dev/null); then echo "no $own at the branch tip"; return 1; fi
  local verdict tier gexit model vcard vgate vclass vcommit
  verdict=$(vfield "$v" Verdict); tier=$(vfield "$v" Gate-tier); gexit=$(vfield "$v" Gate-exit)
  model=$(vfield "$v" Gate-model); vcard=$(vfield "$v" Card); vgate=$(vfield "$v" Card-gate)
  vclass=$(vfield "$v" Class); vcommit=$(vfield "$v" Commit)

  [ "$vcard" = "$id" ] || errs+=("verdict Card is '$vcard', not '$id'")
  case $verdict in MERGE|MERGE-WITH-FOLLOWUPS) ;; *) errs+=("verdict is '$verdict'") ;; esac
  [ "$gexit" = "GATE EXIT=0" ] || errs+=("Gate-exit is '$gexit', not 'GATE EXIT=0'")
  [ -n "$model" ] || errs+=("verdict has no Gate-model")
  case $tier in P|S) ;; *) errs+=("Gate-tier '$tier' is not P or S") ;; esac
  case $vgate in P|S) ;; *) errs+=("Card-gate '$vgate' is not P or S") ;; esac
  if [ "$mode" = merge ]; then
    [ "$vgate" = "$card_gate" ] || errs+=("verdict Card-gate $vgate differs from the card's Gate: $card_gate")
    [ "$vclass" = "$card_class" ] || errs+=("verdict Class $vclass differs from the card's Class: $card_class")
  fi
  if [ "$vgate" = P ] && [ "$tier" != P ]; then errs+=("card needs a P gate; verdict tier is $tier"); fi

  # The verdict must be the last commit, cover the commit before it, and touch only itself.
  local tipfiles reviewed
  tipfiles=$(git diff-tree --no-commit-id --name-only -r "$head" -- ':/')
  [ "$tipfiles" = "$own" ] || errs+=("the branch tip must be the verdict commit (changing only $own); it changes: $(echo $tipfiles)")
  reviewed=$(git rev-parse -q --verify "$vcommit^{commit}" 2>/dev/null || true)
  [ -n "$reviewed" ] && [ "$reviewed" = "$(git rev-parse "$head^")" ] || errs+=("verdict Commit '$vcommit' is not the commit under the verdict ($(git rev-parse --short "$head^"))")

  # What the card changed, excluding its own verdict file.
  local f rel core_entries
  core_entries=$(git show "$base:$V3/CORE_PATHS" 2>/dev/null | core_entries)
  while IFS= read -r f; do
    [ -n "$f" ] || continue
    [ "$f" = "$own" ] && continue
    case $f in
      "$V3"/*) rel=${f#"$V3"/} ;;
      *) [ "$tier" = P ] || errs+=("$f is outside $V3 and needs a P verdict"); continue ;;
    esac
    case $rel in
      # The format description is P-tier infrastructure; other verdicts are off limits.
      verdicts/README.md) [ "$tier" = P ] || errs+=("$f needs a P verdict"); continue ;;
      verdicts/*) errs+=("$f: a card may change only its own verdict file"); continue ;;
      # The specs index is P-tier infrastructure; specs themselves need a spec card.
      docs/README.md) [ "$tier" = P ] || errs+=("$f needs a P verdict"); continue ;;
      docs/*) { [ "$tier" = P ] && [ "$vclass" = spec ]; } || errs+=("$f: docs/ changes need Class: spec and a P verdict") ;;
    esac
    if [ "$tier" != P ] && is_core "$rel" "$core_entries" >/dev/null; then errs+=("$f is under CORE_PATHS and needs a P verdict"); fi
  done < <(changed "$base" "$head" ':/')

  if [ "$mode" = merge ] && [ "$verdict" = MERGE-WITH-FOLLOWUPS ]; then
    local fl; fl=$(vfield "$v" Followups | tr ',' ' ')
    if [ -z "${fl// /}" ] || [ "$fl" = none ]; then errs+=("MERGE-WITH-FOLLOWUPS names no follow-ups"); fi
    for f in $fl; do
      [ "$f" = none ] && continue
      grep -Eq "^- $(printf '%s' "$f" | sed 's/[.[\*^$]/\\&/g') \|" "$queue" 2>/dev/null || errs+=("follow-up $f is not a QUEUE entry ('- $f | ...')")
    done
  fi
  if [ ${#errs[@]} -gt 0 ]; then printf '%s\n' "${errs[@]}"; return 1; fi
  return 0
}

# clean_merge SHA: the merge commit's tree must equal the clean merge of its two
# parents, so nothing was edited inside the merge itself ("evil merge"). A card
# that does not merge cleanly is rebased and re-gated; conflicts are never
# resolved inside a merge commit.
clean_merge() {
  local sha=$1 t
  t=$(git merge-tree --write-tree "$sha^1" "$sha^2" 2>/dev/null) || { echo "the parents do not merge cleanly (conflicts must be fixed on the card branch and re-gated)"; return 1; }
  [ "$(printf '%s\n' "$t" | head -1)" = "$(git rev-parse "$sha^{tree}")" ] || { echo "the merge commit's tree differs from the clean merge of its parents (edited inside the merge)"; return 1; }
}

# The paths whose every change the audit walks.
AUDIT_PATHS=(":/$V3" ":/.forgejo/workflows/nucleus-v3.yml")
