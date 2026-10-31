# shellcheck shell=bash
# The git environment of the enforcement scripts, and the refusal of git state
# that hides changes (X-6-F1-F2). Sourced by every script that reads git;
# the runner's twin is GIT_ENV in tools/run_card.py, and the two stay identical.
#
# Three pieces of repository state rewrite what every git call reports, and all
# three live in files shared by every worktree (the common dir's refs, its
# info/, and .git/config, which `git config` in a worktree writes by default):
# - refs/replace/* swap one object for another; a committed tools/ edit then
#   diffs as nothing, and replace refs are pushable, so they reach CI too;
# - info/grafts gives a commit other parents, which moves merge-bases;
# - a core.fsmonitor hook answering "nothing changed" hides worktree edits
#   from status and diff.
# On git 2.47.3 GIT_NO_REPLACE_OBJECTS disables replace refs (even under
# core.useReplaceRefs=true) but not grafts, which need GIT_GRAFT_FILE. Git
# prints its grafts-deprecated hint for any graft file it can open, /dev/null
# included, on every call; advice.graftFileDeprecated=false silences it, since
# run_card.py's run() folds stderr into what it parses. So the
# environment turns all three off (neutralise), and git_state_refuse names the
# state (refuse): a branch carrying it is stopped and named, not just read
# correctly. Nothing here clears the state; the operator decides.
#
# Two channels get one layer each, by decision (X-6-F1-F2-F2):
# - GIT_CONFIG_PARAMETERS (a caller's `git -c`, inherited by children) applies
#   after the GIT_CONFIG_COUNT entries on git 2.47.3, so an inherited
#   -c core.fsmonitor=<hook> beats the core.fsmonitor=false below: refuse only.
#   git_state_refuse's read keeps the variable and names the hook (origin
#   "command line:"), and every entry point exits non-zero on it. It is not
#   stripped: taking one key out of git's quoted format is a second parser, in
#   bash and in Python, on the enforcement path, for a channel that already
#   fails closed. It is not unset: that drops every caller -c silently and
#   blinds the refusal, so a nested script could no longer name the hook. The
#   index flags are refuse-only for the same kind of reason (verdict
#   X-6-F1-F2, Ruling 2).
# - GIT_REPLACE_REF_BASE puts replace refs outside refs/replace/, where the
#   refusal does not look: neutralise only. GIT_NO_REPLACE_OBJECTS disables
#   replacement whatever the base, and refs under another base do nothing for
#   a git call without the variable, so the refusal is not extended to them.
#
# Two caches hide changes too (X-6-F1-F2-F3, probed on git 2.47.3):
# - a hand-written commit-graph: git trusts its parents, generation numbers and
#   changed-path Bloom filters without reading the commit objects, so one
#   interior base commit given the card's commit as parent moves base...card's
#   merge-base and drops the card's tools/ file from the diff, and a zeroed
#   Bloom filter drops a commit from a walk limited to a plain path (the
#   runner's `log -- verdicts/<ID>.md`; `:/` pathspecs, as the audit's, make
#   git skip the filters);
# - an untracked cache (index extension UNTR) whose directory stat still
#   matches lists no new file; with core.trustctime=false a restored directory
#   mtime is enough, so no forgery of the index is needed.
# Both get both layers. core.commitGraph=false and core.untrackedCache=false
# below make git ignore the files (under the second, git drops the extension
# at its next index write: a cache, rebuilt when the setting allows). Since a
# GIT_CONFIG_PARAMETERS `-c core.commitGraph=true` beats them, as it beats
# every entry here, the refusal reads both keys under the environment and
# names a value that is not false (only a command-line entry can win), and
# names GIT_TEST_COMMIT_GRAPH, git's test switch that forces graph reads over
# the config. It also runs `git commit-graph verify`, which names a forged
# parent or generation (not a Bloom filter) whatever the config says; a graph
# that verifies, such as the ones gc writes, is not refused.
# Two neighbours are refused outright, since nothing neutralises them and git
# never makes them on its own:
# - a shallow repository: shallow entries cut parents, so
#   `merge-base --is-ancestor` says no for a ref that does carry a worker
#   commit, and the runner's ref check (R1) passes it;
# - alternates (objects/info/alternates or GIT_ALTERNATE_OBJECT_DIRECTORIES):
#   git reads an alternate's commit-graph too, and verify checks only the
#   repository's own.
# Probed and inert, so nothing is added for them: what alternates add beyond
# their commit-graph (objects are addressed by hash); core.ignoreStat (it hides
# edits only through the assume-unchanged bit it sets on `git add`, which the
# --index refusal names).
#
# Settings that reshape what diff and status print (X-6-F1-F2-F2-F1, probed on
# git 2.47.3) hide a change without touching any object. A flag on the call
# beats every config scope, GIT_CONFIG_PARAMETERS included, and every worktree
# file, so the callers pass one instead of an entry here:
# - --no-relative: diff.relative makes `git diff` from a subdirectory print
#   cwd-relative paths and drop everything outside it, so the audit, run from
#   nucleus/v3, read a P merge's docs/ change as an outside-v3 file;
# - --ignore-submodules=none: diff.ignoreSubmodules=all, and
#   submodule.<name>.ignore=all in a worktree .gitmodules (tracked or not,
#   which shapes diff-tree too), drop an added gitlink from the listing;
# - --untracked-files=all: status.showUntrackedFiles=no lists no new file.
# Excludes (core.excludesFile, the XDG default ignore file, info/exclude, an
# untracked .gitignore that ignores itself) hide untracked files from status
# under any flag, and refusing them would refuse normal setups. So the
# worktree checks list them by source: `--ignored=matching` lists every
# ignored path, and one is dropped only when `git check-ignore -v` names a
# tracked .gitignore at or above it as the rule. A tracked .gitignore is a
# listed change whenever it is new or edited, and it hides only paths below
# its own directory.
# core.trustctime=false and core.checkStat=minimal let a same-size in-place
# edit with its mtime put back pass every stat-based read as unchanged. No
# flag exists for them, so they get both layers: the entries below put git's
# defaults back over every config file, and since a GIT_CONFIG_PARAMETERS
# entry beats them, git_state_refuse names any false trustctime and any
# checkStat but default, at every scope.
# core.ignoreCase=true on a case-sensitive filesystem drops an untracked case
# variant of a tracked file (tools/RUN.py beside tools/run.py) from status,
# --ignored included, and from ls-files --others. No flag or listing beats it,
# so core.ignoreCase=false goes in below, and the refusal names a -c that wins
# over it. A file-scope true is not refused: git sets it at init on a
# case-insensitive filesystem, where false only lists more.
# A clean filter turns a same-size worktree edit back into the index blob, and
# status compares content only through it (a size change alone is reported),
# so the edit passes status and diff. Its driver can have any name, so nothing
# neutralises it, and a listing hashed without filters would flag every
# eol=crlf file: the --index refusal names a filter attribute on a tracked
# path whose driver has a clean or process command, at any scope.
# Probed and inert, so nothing is added for them: color.ui/diff/status=always,
# diff.noprefix, diff.mnemonicPrefix and diff.srcPrefix (--name-only,
# --porcelain and --format print no colour or prefix); diff.renames,
# status.renames and diff.renameLimit (every path listing passes --no-renames,
# and an emptiness check does not care); core.quotePath (every path listing is
# -z); status.relativePaths (porcelain ignores it); log.showSignature (a signed
# commit's extra line breaks the audit's subject and parent-count parse, and
# only adds lines to stray_commits); core.fileMode=false (it hides a worktree
# mode change only; committed modes are tree data);
# merge.renames/merge.directoryRenames (they choose where the parents'
# content goes, never other content); textconv and diff.external (the verdict
# is read with `git show <rev>:<path>`, which textconvs no blob, and
# --name-only runs no external diff).
export GIT_NO_REPLACE_OBJECTS=1
export GIT_GRAFT_FILE=/dev/null

# core.fsmonitor=false, advice.graftFileDeprecated=false, core.commitGraph=false,
# core.untrackedCache=false, core.trustctime=true, core.checkStat=default and
# core.ignoreCase=false go in as GIT_CONFIG_* entries (command scope, which beats every config file),
# appended after any the caller set so theirs still apply. A script run by a
# script finds its own entries already last and does not add them twice.
# GIT_ENV_BASE_COUNT is the count without them, for the reads that must see the
# configured values (core.fsmonitor, core.trustctime, core.checkStat,
# commit-graph verify). A count that is not a number is left alone: git then
# refuses every call, which fails closed.
GIT_ENV_CONFIG=(core.fsmonitor false advice.graftFileDeprecated false core.commitGraph false core.untrackedCache false core.trustctime true core.checkStat default core.ignoreCase false)
_git_env_init() {
  local n=${GIT_CONFIG_COUNT:-0} k=$((${#GIT_ENV_CONFIG[@]} / 2)) i key val same=1
  case $n in *[!0123456789]*) GIT_ENV_BASE_COUNT=; return ;; esac
  if [ "$n" -ge "$k" ]; then
    for ((i = 0; i < k; i++)); do
      key=GIT_CONFIG_KEY_$((n - k + i)) val=GIT_CONFIG_VALUE_$((n - k + i))
      { [ "${!key:-}" = "${GIT_ENV_CONFIG[2 * i]}" ] && [ "${!val:-}" = "${GIT_ENV_CONFIG[2 * i + 1]}" ]; } || same=0
    done
    if [ "$same" -eq 1 ]; then GIT_ENV_BASE_COUNT=$((n - k)); return; fi
  fi
  for ((i = 0; i < k; i++)); do
    export "GIT_CONFIG_KEY_$((n + i))=${GIT_ENV_CONFIG[2 * i]}" "GIT_CONFIG_VALUE_$((n + i))=${GIT_ENV_CONFIG[2 * i + 1]}"
  done
  export GIT_CONFIG_COUNT=$((n + k))
  GIT_ENV_BASE_COUNT=$n
}
_git_env_init
GIT_STATE_PROBLEMS=

# git_state_refuse [--index]: prints one line per piece of git state that hides
# changes, "<problem> (git state hides changes)", and returns 1 if there is
# any; GIT_STATE_PROBLEMS holds the same lines: replace refs, a graft file, a
# shallow repository, alternates, a core.fsmonitor hook, a core.commitGraph,
# core.untrackedCache or core.ignoreCase that is not false under this
# environment, GIT_TEST_COMMIT_GRAPH, a commit-graph that fails
# `git commit-graph verify`, and a false core.trustctime or a core.checkStat
# other than default at any scope. A git failure is a problem too, never
# "nothing found". --index also refuses index entries marked assume-unchanged
# (lowercase `ls-files -v` tag) or skip-worktree (S), which status and diff
# skip, and a clean filter on a tracked path (a `filter` attribute whose driver
# has filter.<driver>.clean or .process set). Only a caller that judges its
# worktree needs --index (the others read committed trees). Callers print,
# still run their own checks under the environment above, and then exit
# non-zero.
git_state_refuse() {
  local p=() out ref common gitdir tmp rc=0 origin val tag path shallow spath apath key
  if out=$(git for-each-ref --format='%(refname)' refs/replace/); then
    while IFS= read -r ref; do [ -n "$ref" ] && p+=("replace ref $ref"); done <<<"$out"
  else
    p+=("git for-each-ref refs/replace/ failed (cannot check for replace refs)")
  fi
  if { read -r common && read -r gitdir; } < <(git rev-parse --path-format=absolute --git-common-dir --git-dir) && [ -n "$gitdir" ]; then
    [ ! -e "$common/info/grafts" ] || p+=("graft file $common/info/grafts")
    [ "$gitdir" = "$common" ] || [ ! -e "$gitdir/info/grafts" ] || p+=("graft file $gitdir/info/grafts")
  else
    p+=("git rev-parse --git-common-dir --git-dir failed (cannot check for a graft file)")
  fi
  # Neither has a neutraliser, and git makes neither on its own (clone makes
  # them only when asked: --depth, --shared, --reference). --git-path follows
  # the object directory wherever it is.
  if { read -r shallow && read -r spath && read -r apath; } < <(git rev-parse --path-format=absolute --is-shallow-repository --git-path shallow --git-path objects/info/alternates) && [ -n "$apath" ]; then
    [ "$shallow" != true ] || p+=("shallow repository (${GIT_SHALLOW_FILE:-$spath})")
    [ ! -e "$apath" ] || p+=("alternates file $apath")
  else
    p+=("git rev-parse --is-shallow-repository failed (cannot check for a shallow repository or alternates)")
  fi
  [ -z "${GIT_ALTERNATE_OBJECT_DIRECTORIES:-}" ] || p+=("GIT_ALTERNATE_OBJECT_DIRECTORIES is '$GIT_ALTERNATE_OBJECT_DIRECTORIES' in the environment")
  # NUL-separated records go through a file: $(...) drops NULs.
  tmp=$(mktemp) || { echo "cannot create a temporary file to check git state (git state hides changes)"; GIT_STATE_PROBLEMS="cannot create a temporary file to check git state"; return 1; }
  # Read without the override, which would make this always read false.
  GIT_CONFIG_COUNT=$GIT_ENV_BASE_COUNT git config -z --show-origin --get-all core.fsmonitor >"$tmp" || rc=$?
  case $rc in
    0) while IFS= read -r -d '' origin && IFS= read -r -d '' val; do
         case ${val,,} in false | no | off | 0) ;; *) p+=("core.fsmonitor is '$val' ($origin)") ;; esac
       done <"$tmp" ;;
    1) ;; # unset
    *) p+=("git config --get-all core.fsmonitor failed (exit $rc; cannot check for an fsmonitor hook)") ;;
  esac
  # Without the override too, which only appends git's default: every entry at
  # every scope is named, a command-line one included, since only that scope
  # beats the environment's. --type=bool reads the value as git does (no, off,
  # 0 and an empty value are false; a bare key is true); a value git cannot
  # parse fails the read, as it fails every git call.
  rc=0
  GIT_CONFIG_COUNT=$GIT_ENV_BASE_COUNT git config -z --show-origin --type=bool --get-all core.trustctime >"$tmp" || rc=$?
  case $rc in
    0) while IFS= read -r -d '' origin && IFS= read -r -d '' val; do
         [ "$val" != false ] || p+=("core.trustctime is '$val' ($origin)")
       done <"$tmp" ;;
    1) ;; # unset
    *) p+=("git config --get-all core.trustctime failed (exit $rc; cannot check core.trustctime)") ;;
  esac
  rc=0
  GIT_CONFIG_COUNT=$GIT_ENV_BASE_COUNT git config -z --show-origin --get-all core.checkStat >"$tmp" || rc=$?
  case $rc in
    0) while IFS= read -r -d '' origin && IFS= read -r -d '' val; do
         [ "${val,,}" = default ] || p+=("core.checkStat is '$val' ($origin)")
       done <"$tmp" ;;
    1) ;; # unset
    *) p+=("git config --get-all core.checkStat failed (exit $rc; cannot check core.checkStat)") ;;
  esac
  # Read under the override, which only a command-line entry can beat: any
  # value but false means git reads the cache (or, for core.ignoreCase, lists
  # no untracked case variant of a tracked file), and so does no value at all
  # (exit 1: the environment's entry is missing).
  for key in core.commitGraph core.untrackedCache core.ignoreCase; do
    rc=0
    git config -z --show-origin --get "$key" >"$tmp" || rc=$?
    case $rc in
      0) { IFS= read -r -d '' origin && IFS= read -r -d '' val; } <"$tmp" || val=
         case ${val,,} in false | no | off | 0) ;; *) p+=("$key is '$val' ($origin), over the environment's false") ;; esac ;;
      1) p+=("$key is not set under the environment (its false is missing)") ;;
      *) p+=("git config --get $key failed (exit $rc; cannot check that the environment's $key=false holds)") ;;
    esac
  done
  val=${GIT_TEST_COMMIT_GRAPH:-}
  case ${val,,} in '' | false | no | off | 0) ;; *) p+=("GIT_TEST_COMMIT_GRAPH is '$val' in the environment (forces commit-graph reads)") ;; esac
  # Without the override, as the fsmonitor read: on git 2.47.3 verify checks the
  # file whatever core.commitGraph says, and a git that skipped it under false
  # would turn this check off without a word. The -c keeps the grafts hint,
  # which the override also silences, out of the line. Exit 1 is a bad graph;
  # no graph exits 0.
  rc=0
  out=$(GIT_CONFIG_COUNT=$GIT_ENV_BASE_COUNT git -c advice.graftFileDeprecated=false commit-graph verify 2>&1) || rc=$?
  case $rc in
    0) ;;
    1) out=${out%%$'\n'*}; p+=("commit-graph fails verification (${out:-no output})") ;;
    *) p+=("git commit-graph verify failed (exit $rc; cannot check the commit-graph)") ;;
  esac
  if [ "${1:-}" = --index ]; then
    # From the top, repository-relative: callers run from nucleus/v3, and a
    # plain ls-files there would list only that directory.
    if git ls-files -v -z --full-name -- ':/' >"$tmp"; then
      while IFS= read -r -d '' out; do
        tag=${out:0:1} path=${out:2}
        case $tag in [abcdefghijklmnopqrstuvwxyz]) p+=("$path is marked assume-unchanged in the index (ls-files -v tag $tag)") ;; esac
        case $tag in S | s) p+=("$path is marked skip-worktree in the index (ls-files -v tag $tag)") ;; esac
      done <"$tmp"
    else
      p+=("git ls-files -v failed (cannot check the index flags)")
    fi
    # A clean filter can turn a same-size worktree edit back into the index
    # blob, and status compares content only through it, so the edit passes
    # status and diff (X-6-F1-F2-F2-F1). Attributes come from the worktree,
    # info/attributes and core.attributesFile, and the driver from any config
    # scope, so check-attr reads them as status does. No flag turns filters
    # off, nothing names the drivers ahead of time, and hashing the worktree
    # without filters would list every eol=crlf file: refuse only. A driver
    # configured with no path using it (a global filter.lfs.*) is not named.
    # No value is skipped: check-attr prints the same word for a state and
    # for a driver of that name, and git runs filter.set.clean for
    # `filter=set` (likewise unset, unspecified). Looking up filter.set.* for a
    # bare `filter` attribute costs one config read, and no one configures a
    # driver of those names in normal use. The count for such a word may
    # include paths in that state.
    local top d n kind first drv=()
    declare -A dfirst=() dn=()
    if top=$(git rev-parse --show-toplevel) && git ls-files -z --full-name -- ':/' >"$tmp" && (cd "$top" && git check-attr -z --stdin filter <"$tmp") >"$tmp.attr"; then
      while IFS= read -r -d '' path && IFS= read -r -d '' _ && IFS= read -r -d '' val; do
        [ -n "${dfirst[$val]:-}" ] || { dfirst[$val]=$path; drv+=("$val"); }
        dn[$val]=$((${dn[$val]:-0} + 1))
      done <"$tmp.attr"
      for d in "${drv[@]}"; do
        n=${dn[$d]} first=${dfirst[$d]}
        for kind in clean process; do
          rc=0
          GIT_CONFIG_COUNT=$GIT_ENV_BASE_COUNT git config -z --show-origin --get-all "filter.$d.$kind" >"$tmp" || rc=$?
          case $rc in
            0) while IFS= read -r -d '' origin && IFS= read -r -d '' val; do
                 p+=("filter=$d is on $n tracked path(s) (first: $first), and filter.$d.$kind is '$val' ($origin)")
               done <"$tmp" ;;
            1) ;; # unset
            *) p+=("git config --get-all filter.$d.$kind failed (exit $rc; cannot check the clean filter)") ;;
          esac
        done
      done
    else
      p+=("git check-attr filter failed (cannot check for clean filters)")
    fi
  fi
  rm -f "$tmp" "$tmp.attr"
  GIT_STATE_PROBLEMS=
  [ ${#p[@]} -gt 0 ] || return 0
  GIT_STATE_PROBLEMS=$(printf '%s (git state hides changes)\n' "${p[@]}")
  printf '%s\n' "$GIT_STATE_PROBLEMS"
  return 1
}
