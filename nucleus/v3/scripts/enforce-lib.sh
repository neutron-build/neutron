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
# verify_objects first (git-env.sh says why): a forged object makes the diff
# list nothing. Its lines go to stderr and stay in VERIFY_PROBLEMS.
# _changed_names is the same listing unverified, for check_merge's audit mode
# only (it says why); every other caller uses changed.
_changed_names() { local b=$1 h=$2; shift 2; git diff -z --no-renames --no-relative --ignore-submodules=none --name-only "$b...$h" -- "$@"; }
changed() { local b=$1 h=$2; shift 2; verify_objects "$b" "$h" || { printf '%s\n' "$VERIFY_PROBLEMS" >&2; return 1; }; _changed_names "$b" "$h" "$@"; }

# SHORT_COMMIT_MERGES: the card merges whose verdict names its commit by a
# short id, from before merge-card required the full 40-hex one
# (X-6-F1-F2-F3-F1-F1). audit-merges.sh judges only these under the old 7-40 hex
# rule (check_merge says why it is keyed by merge commit id). Frozen: nothing
# appends to it and no script writes it, so a short-id merge made after that
# card fails the audit. Oldest first. Generated at that card's base
# (nucleus/v3-scaffold = 043739fb: 44 of its 89 audited merges; every merge
# after 266b8f4c, X-5, names the full id) with the audit's own listing, run
# from nucleus/v3:
#   . scripts/git-env.sh; . scripts/core-paths-lib.sh; . scripts/enforce-lib.sh
#   git rev-list --first-parent nucleus/v3-scaffold -- "${AUDIT_PATHS[@]}" | tac |
#     while read -r m; do
#       id=$(git log -1 --format=%s "$m" | sed -n 's/^nucleus v3: merge card\/v3-\([A-Za-z0-9._-]*\) (.*/\1/p')
#       f=$(mktemp); verify_blob "$(git rev-parse "$m^2:$V3/verdicts/$id.md")" "$f" || echo "UNVERIFIED $m"
#       [[ $(vfield "$(cat "$f")" Commit) =~ ^[0123456789abcdef]{40}$ ]] || echo "  $m # $id"
#       rm -f "$f"
#     done
# BEGIN SHORT_COMMIT_MERGES (frozen)
SHORT_COMMIT_MERGES=(
  ac4ff9a04406de0afae5122d38fb97264381854b # S-0
  738efa93a77ab37e45dc01c84e9d9be0cfac3e58 # X-0
  a0788eeef2917762e6e8ae6b117f15c4ead6b340 # X-6
  75afaba1cfc79f704a70267b9ce89b306d115865 # X-0-F1
  831ea23f04798b82f33919e0ee9310ed17c25ffb # X-1a
  c5ee3ce6c93f4aa8708e49fba52e1b70b96b1da7 # X-6-F1
  09d6d94ded2658a1c454d5fda8a610899ec0621c # S-0-F1
  16fde176f8aaa587505c9d84a8e7748c44351f2c # S-0-F2
  592ba2a1c306001e624be11b1d7490f167eb6fe8 # X-0-F3
  98b4702d2ca402cf87de64807329e0e199c4ff09 # S-0-F2-F1
  ceae821e14b12d4743b566bdafbfb2a5f9047e6d # X-0-F3-F1
  544a205c5ef0ad7cea7f3f167e4e7964fbf544de # S-0-F2-F2
  cb3caec080c8d7f9f7b9ead20ce177dcb047cd12 # X-1a-F1
  67b4cb1d6f4ac83c0f3801e0b3816ad4d3cf40c5 # X-1a-F2
  8522c8c46ea38d690f5496c444fd0c571ad7a8bd # X-0-F1-F1
  1a7106f41868082286c22de5c95bd633fd16d6b2 # S-0-F2-F1-F1
  7729abfdca179af54da34316ebc3c958160a421e # X-0-F1-F1-F1
  072dea5d6c0b8eb66bb4cbb5a78c729b570781d3 # X-1a-F3
  5e4880664e40be69dc05309926451ea38b5cf791 # S-0-F2-F2-F1
  fa84ef119beb8c377d56fb03a6373350bf734db2 # X-6-F1-F3
  0a9261adc51ff24d6da135f93d8f3471da9d50ba # X-6-F1-F1
  693a155f227e5ecf79cf60ac4db028a03b87b9c3 # S-0-F2-F2-F1-F1
  9ed8c29cb29d38d3fd312e91ed940ee2caf4cc27 # S-0-F2-F1-F1-F1
  9e45fc61685a5bfe0a1ae2aa352f30ec18bf8577 # X-6-F1-F3-F1
  12c225cb36cd84a0be51e1922ee71228e3f96f56 # X-6-F1-F2
  de9578f756793f4d63ce60800f07411be4fc96ac # X-1a-F3-F1
  b6560ff32eb9e74d3dce73cfe55de32a2b0edf33 # X-1a-F2-F1
  ef070bbb24566fcb3d150d6dd189f5b69b13963b # X-1a-F1-F3
  e4f2a268e5f281016402003f45fc8bcc9064f78c # X-6-F2-F1
  a974206f67e212c2601d914f6d5c44f490e2d746 # X-0-F2
  7782a3bbf9a71c8c812719b69c6ba41b77247e97 # X-1a-F1-F1
  3ca0350f6958920bd3ae7d3250ef84f8e0fca19a # X-1a-F1-F2
  1ef49f4e3410e5edafccf3780b55c5acb551b898 # X-2
  7425b94fae299f476130301a519c55271fd6574c # X-3
  7ccd1cff8ef926f95a240b15d4b9c6a346251efe # X-1b
  391e816c7d5d3dd661cba47053616e30baa7c727 # S-1
  45f7c17a3312e7c3a15d2bd1f2e8dba3e9a8ace0 # X-1b-F2
  a59bf36e26dfc7578539d09c5cde359c4ad6e4b5 # S-1-F1
  b69b45894f5f413d58fe9887d2d3d7c8eb20d40e # X-1b-F3
  97f421b8cf0c3967a82d742d177b5afbb1539fab # X-1b-F4
  2b3924def8316c87a661dea31b4935d686250261 # S-2
  5ff1bd06d10d62b23c4dfa352b5728b1cee662ae # S-2-F1
  6fad35e5539d08fcacf3e2db64cef103e55ef57b # S-3
  266b8f4c6f08d07a6d3d222786b325bd3894a3a5 # X-5
)
# END SHORT_COMMIT_MERGES

# short_commit_grandfathered MODE MERGE_SHA: MODE is the literal "audit" and
# MERGE_SHA, a full 40-hex merge commit id, is string-equal to an entry of
# SHORT_COMMIT_MERGES. Keyed on the two arguments only, never on an environment
# variable, a file or a marker: those are trusted state a worker could write.
short_commit_grandfathered() {
  local e
  [ "$1" = audit ] && [[ $2 =~ ^[0123456789abcdef]{40}$ ]] || return 1
  for e in "${SHORT_COMMIT_MERGES[@]}"; do [ "$e" = "$2" ] && return 0; done
  return 1
}

# check_merge ID BASE HEAD MODE [CARD_GATE CARD_CLASS QUEUE_FILE | MERGE_SHA]
# MODE is "merge" (pre-merge, card file known) or "audit" (CI, from git only).
# Merge mode takes CARD_GATE CARD_CLASS QUEUE_FILE; audit mode takes MERGE_SHA,
# the audited merge's full commit id (only the Commit rule reads it).
# Prints one line per violation; returns 1 if there were any.
# Audit mode verifies no objects itself (the commit+tree closure of the
# audited ref is verified once by audit-merges.sh; the skips below say why);
# every other mode verifies the ranges it reads.
check_merge() {
  local id=$1 base=$2 head=$3 mode=$4 card_gate= card_class= queue= merge_sha=
  if [ "$mode" = audit ]; then merge_sha=${5:-}; else card_gate=${5:-} card_class=${6:-} queue=${7:-}; fi
  local own="$V3/verdicts/$id.md" errs=() v vid vtmp
  # The verdict's content decides every check below, so its blob is hashed
  # (git-env.sh's verify_blob) in both modes, and its fields are read from the
  # verified file, never from a second read. verify_reachable does not cover
  # it: it hashes commits and trees only.
  if ! vid=$(git rev-parse -q --verify "$head:$own"); then echo "no $own at the branch tip"; return 1; fi
  vtmp=$(mktemp) || { echo "cannot create a temporary file for the verdict"; return 1; }
  if ! verify_blob "$vid" "$vtmp" "$head:$own" 2>/dev/null; then rm -f "$vtmp"; printf '%s\n' "$VERIFY_PROBLEMS"; return 1; fi
  v=$(cat "$vtmp"); rm -f "$vtmp"
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
  # field to any commit. The value is compared as text with tip^'s full id.
  # It must be all 40 digits (X-6-F1-F2-F3-F1-F1): a short id must also name no
  # other commit, or a late commit ground to share the reviewed prefix reuses
  # the verdict, but that count reads only the objects present where it runs.
  # Deleting the unreachable reviewed commit's object (any same-uid worker can)
  # leaves the ground commit alone with the prefix, and no hash check helps:
  # there is nothing left to verify. A fresh CI clone is already in that state.
  # A 40-hex id cannot collide. The merges made before this rule
  # (SHORT_COMMIT_MERGES) keep the old rule, ambiguity count included and never
  # weaker, in audit mode only, keyed by the audited merge's own commit id: that
  # id pins the merge's parents and so the verdict blob at its second parent, a
  # chain audit-merges.sh hash-verifies (verify_reachable, then verify_blob
  # above), and no new merge can carry a listed id. A date is text a writer
  # chooses, and an ancestry cutoff adds a read for no gain. The list is frozen,
  # so a short-id merge made after it fails the audit. The legacy path keys on
  # the MODE and MERGE_SHA arguments only (short_commit_grandfathered).
  # In it only commits count, as the ^{commit} peel ignored blobs and trees.
  # The tip check reads HEAD^1's trees, the reviewed commit's, which the range
  # check below does not verify: HEAD^1 is neither the merge base nor the tip.
  # Audit mode skips this call and the one in changed (verdict
  # X-6-F1-F2-F3-F1-esc-1, ESC-1): every object a per-merge audit check reads
  # is reachable from the audited REF (HEAD and BASE are the parents of a
  # merge that rev-list --first-parent REF listed, and every merge base,
  # range commit and tree under them is their ancestor or below one), and
  # audit-merges.sh's verify_reachable REF has re-hashed that whole commit+tree
  # closure under the same git_env(). The skip keys on the literal MODE
  # argument only, never on an environment variable, a file or a marker: those
  # are trusted state a worker could write (the card's no-cache rule).
  local tipfiles=() vl
  if [ "$mode" != audit ] && ! verify_objects "$head^1" "$head"; then
    while IFS= read -r vl; do errs+=("$vl"); done <<<"$VERIFY_PROBLEMS"
  fi
  mapfile -d '' -t tipfiles < <(git diff-tree -z --ignore-submodules=none --no-commit-id --name-only -r "$head" -- ':/')
  { [ ${#tipfiles[@]} -eq 1 ] && [ "${tipfiles[0]}" = "$own" ]; } || errs+=("the branch tip must be the verdict commit (changing only $own); it changes: ${tipfiles[*]}")
  if [[ $vcommit =~ ^[0123456789abcdef]{40}$ ]]; then
    local full
    if ! full=$(git rev-parse -q --verify "$head^1^{commit}"); then
      errs+=("cannot resolve the commit under the verdict ($head^1)")
    elif [ "$vcommit" != "$full" ]; then
      errs+=("verdict Commit '$vcommit' is not the commit under the verdict ($(git rev-parse --short "$head^"))")
    fi
  elif ! short_commit_grandfathered "$mode" "$merge_sha"; then
    if [[ $vcommit =~ ^[0123456789abcdef]+$ ]]; then
      errs+=("verdict Commit '$vcommit' is not a full 40-hex commit id (short ids are accepted only for the merges frozen in SHORT_COMMIT_MERGES)")
    else
      errs+=("verdict Commit '$vcommit' is not a hex commit id (40 lowercase hex digits; revision expressions are refused)")
    fi
  elif [[ $vcommit =~ ^[0123456789abcdef]{7,40}$ ]]; then
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
  if [ "$mode" = audit ]; then
    # Unverified: see the skip at the tip check.
    _changed_names "$base" "$head" ':/' >"$lst" || errs+=("cannot list paths changed in $base...$head (git diff failed)")
  elif ! changed "$base" "$head" ':/' >"$lst"; then
    if [ -n "$VERIFY_PROBLEMS" ]; then
      while IFS= read -r vl; do errs+=("cannot list paths changed in $base...$head: $vl"); done <<<"$VERIFY_PROBLEMS"
    else
      errs+=("cannot list paths changed in $base...$head (git diff failed)")
    fi
  fi
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
# It verifies no objects itself (verdict X-6-F1-F2-F3-F1-esc-1, ESC-1): its
# only caller is audit-merges.sh, which has already run verify_reachable on a
# ref containing SHA, so SHA, both parents, their merge bases and every tree
# under them are verified. Any new caller must verify first (verify_objects
# SHA^1 SHA^2 and SHA^1 SHA, or verify_reachable on a ref containing SHA).
clean_merge() {
  local sha=$1 t
  t=$(git merge-tree --write-tree "$sha^1" "$sha^2" 2>/dev/null) || { echo "the parents do not merge cleanly (conflicts must be fixed on the card branch and re-gated)"; return 1; }
  [ "$(printf '%s\n' "$t" | head -1)" = "$(git rev-parse "$sha^{tree}")" ] || { echo "the merge commit's tree differs from the clean merge of its parents (edited inside the merge)"; return 1; }
}

# The paths whose every change the audit walks.
AUDIT_PATHS=(":/$V3" ":/.forgejo/workflows/nucleus-v3.yml")
