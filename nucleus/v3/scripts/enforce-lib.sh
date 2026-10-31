# shellcheck shell=bash
# Merge rules shared by merge-card.sh (before a merge) and audit-merges.sh (CI,
# after). One implementation so the two can never disagree. PLAN-v3 §7.6.
# Sourced from nucleus/v3/scripts; every git pathspec is anchored at the
# repository root (":/...") because callers run from nucleus/v3.

V3=nucleus/v3
# Every git call below runs with replace refs, grafts and fsmonitor off, even
# for a caller that sources only this file (git-env.sh explains why). Callers
# also run git_state_refuse, which names that state.
# shellcheck source=git-env.sh
. "$(dirname "${BASH_SOURCE[0]}")/git-env.sh"

# vfield TEXT KEY: the value of the first `KEY: value` line.
vfield() { printf '%s\n' "$1" | sed -n "s/^$2:[[:space:]]*//p" | head -1 | sed 's/[[:space:]]*$//'; }

# changed BASE HEAD [pathspec...]: changed paths, repository-relative, each
# NUL-terminated (read with `read -r -d ''`).
# --no-renames: a rename must report its source path too, or moving a CORE file
# out of a protected directory would look like an ordinary new file.
# -z: without it git C-quotes any path with a non-ASCII byte, `"`, `\` or a
# control character ("nucleus/v3/docs/\303\251.md"), and a quoted path no
# longer matches nucleus/v3/* -- docs/é.md would pass as an outside-v3 file.
# core.quotePath=false is not enough: it still quotes `"`, `\` and controls.
# --no-relative: diff.relative, from any config scope (`git -c` and
# GIT_CONFIG_PARAMETERS included, which no GIT_ENV entry beats), makes a diff
# run from a subdirectory print cwd-relative paths and drop everything outside
# it. audit-merges.sh runs this from nucleus/v3, where a P merge's docs/ change
# then read as an outside-v3 file and .forgejo/ vanished (X-6-F1-F2-F2-F1).
# Only a flag beats every scope. diff-tree does not read the key.
# --ignore-submodules=none: diff.ignoreSubmodules=all in any config scope, and
# submodule.<name>.ignore=all in a worktree .gitmodules (tracked or untracked),
# drop an added gitlink from the listing; the .gitmodules one shapes diff-tree
# too, so the tip check below passes the same flag.
# Callers must check its exit status: a failed listing (a bad BASE) prints
# nothing, and `done < <(changed ...)` would read that as "nothing changed".
changed() { local b=$1 h=$2; shift 2; git diff -z --no-renames --no-relative --ignore-submodules=none --name-only "$b...$h" -- "$@"; }

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
  # Commit must be a literal hex id, matched before git sees it: a ref or revision
  # expression (card/v3-X~1, HEAD^, <hex>~0) is re-evaluated at merge time, so it
  # follows the branch and "covers" whatever is under the verdict now, reviewed
  # or not. The class is spelled out so no locale reads a-f as a collation range.
  # Even a bare hex value never goes through a lookup: git resolves refs before
  # short object ids, so a branch or tag named like the prefix would redirect the
  # field to any commit. The value is compared as text with tip^'s full id, and
  # must name no other commit: rev-parse refused an ambiguous prefix, and without
  # that a late commit ground to share the reviewed prefix would reuse the verdict.
  # Only commits count, as the ^{commit} peel ignored blobs and trees.
  local tipfiles=()
  mapfile -d '' -t tipfiles < <(git diff-tree -z --ignore-submodules=none --no-commit-id --name-only -r "$head" -- ':/')
  { [ ${#tipfiles[@]} -eq 1 ] && [ "${tipfiles[0]}" = "$own" ]; } || errs+=("the branch tip must be the verdict commit (changing only $own); it changes: ${tipfiles[*]}")
  if [[ $vcommit =~ ^[0123456789abcdef]{7,40}$ ]]; then
    local parent objs kinds ty name ncommit=0 mine=0 bad=0
    # Through $(...) with the status checked: a failed listing prints nothing,
    # which must not read as "no other commit".
    if ! parent=$(git rev-parse -q --verify "$head^1^{commit}"); then
      errs+=("cannot resolve the commit under the verdict ($head^1)")
    elif [[ $parent != "$vcommit"* ]]; then
      errs+=("verdict Commit '$vcommit' is not the commit under the verdict ($(git rev-parse --short "$head^"))")
    elif ! objs=$(git rev-parse --disambiguate="$vcommit") || ! kinds=$(git cat-file --batch-check='%(objecttype) %(objectname)' <<<"$objs"); then
      errs+=("cannot list the objects whose id starts with verdict Commit '$vcommit' (git failed)")
    else
      while read -r ty name; do
        case $ty in
          commit) ncommit=$((ncommit + 1)); [ "$name" = "$parent" ] && mine=1 ;;
          tree | blob | tag) ;;
          *) bad=1 ;; # "<id> missing", or an empty listing
        esac
      done <<<"$kinds"
      if [ "$bad" -eq 1 ]; then
        errs+=("cannot list the objects whose id starts with verdict Commit '$vcommit' (unreadable object)")
      elif [ "$ncommit" -gt 1 ]; then
        errs+=("verdict Commit '$vcommit' is ambiguous: $ncommit commits start with it; use a longer id")
      elif [ "$mine" -ne 1 ]; then
        errs+=("verdict Commit '$vcommit' is not the commit under the verdict ($(git rev-parse --short "$head^"))")
      fi
    fi
  else
    errs+=("verdict Commit '$vcommit' is not a hex commit id (7-40 lowercase hex digits; revision expressions are refused)")
  fi

  # What the card changed, excluding its own verdict file.
  local f rel core_entries lst crc=0 cerr creason
  # A base that predates CORE_PATHS (exit 3) has no CORE paths; the bootstrap
  # merge needs that. A failed read (exit 1) must not become that same empty
  # list, which would let an S verdict through on scripts/: it is an error, and
  # the other checks still run so every violation is reported.
  cerr=$(mktemp) || { echo "cannot create a temporary file for the CORE_PATHS read"; return 1; }
  core_entries=$(base_core_entries "$base" "$V3" 2>"$cerr") || crc=$?
  creason=$(cat "$cerr")
  rm -f "$cerr"
  case $crc in
    0 | 3) ;;
    *) core_entries=; errs+=("cannot read CORE_PATHS from $base ($creason); refusing to judge CORE paths") ;;
  esac
  # Through a file: process substitution drops git's exit status, and NUL-
  # separated paths cannot be held in a variable.
  lst=$(mktemp) || { echo "cannot create a temporary file for the path list"; return 1; }
  changed "$base" "$head" ':/' >"$lst" || errs+=("cannot list paths changed in $base...$head (git diff failed)")
  while IFS= read -r -d '' f; do
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
  done <"$lst"
  rm -f "$lst"

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
