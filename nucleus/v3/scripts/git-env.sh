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
export GIT_NO_REPLACE_OBJECTS=1
export GIT_GRAFT_FILE=/dev/null

# core.fsmonitor=false and advice.graftFileDeprecated=false go in as
# GIT_CONFIG_* entries (command scope, which beats every config file), appended
# after any the caller set so theirs still apply. A script run by a script finds
# its own entries already last and does not add them twice. GIT_ENV_BASE_COUNT
# is the count without them, for the one read that must see the configured
# value. A count that is not a number is left alone: git then refuses every
# call, which fails closed.
GIT_ENV_CONFIG=(core.fsmonitor false advice.graftFileDeprecated false)
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
# any; GIT_STATE_PROBLEMS holds the same lines. A git failure is a problem too,
# never "nothing found". --index also refuses index entries marked
# assume-unchanged (lowercase `ls-files -v` tag) or skip-worktree (S): status
# and diff skip them. Only a caller that judges its worktree needs it (the
# others read committed trees). Callers print, still run their own checks
# under the environment above, and then exit non-zero.
git_state_refuse() {
  local p=() out ref common gitdir tmp rc=0 origin val tag path
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
  fi
  rm -f "$tmp"
  GIT_STATE_PROBLEMS=
  [ ${#p[@]} -gt 0 ] || return 0
  GIT_STATE_PROBLEMS=$(printf '%s (git state hides changes)\n' "${p[@]}")
  printf '%s\n' "$GIT_STATE_PROBLEMS"
  return 1
}
