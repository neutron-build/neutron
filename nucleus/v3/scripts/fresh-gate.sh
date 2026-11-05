#!/usr/bin/env bash
# Run the full gate on a fresh, verified checkout of one commit
# (X-6-F1-F2-F2-F1-F1; verdict X-6-F1-F2-F2-F1, ruling X-6-F1-F2-F3-F1-esc-1).
#
#   fresh-gate.sh [--base REF] COMMIT        (--base defaults to nucleus/v3-scaffold)
#
# Works on the repository of the current directory, not on this script's own
# (as merge-card.sh does), so a gate can run a copy extracted from the
# scaffold's objects. Callers must run the scaffold's copy, never the card's:
# the worker can edit the card's copy, and a main-checkout working file is
# read through stat data that can lie (GATE-PROMPT step 1 gives the extract).
#
# Success: stdout is gate.sh's output unchanged, after this script's
# `fresh-gate:` lines, and its last line is gate.sh's own `GATE EXIT=<n>`; the
# exit status is n. Refusal: one `fresh-gate: <problem>` line per problem on
# stderr, `FRESH-GATE REFUSED` as the last stdout line, exit 2, and never a
# `GATE EXIT=` line, so no reader can take a refusal for a gate result.
#
# The threat is index-stat poisoning: a same-uid worker can make git believe an
# edited tracked file is unchanged (a clean filter configured for one status
# and removed, or a direct index write), so status, diff and every refusal see
# a clean worktree while the file holds other bytes. The data never merges,
# but any gate that reads a worktree can be faked green. A fresh checkout gets
# a new index and no untracked files, so it drops the worker's index. It is
# not enough alone: probed on git 2.47.3, a post-checkout hook in the common
# dir runs during `worktree add`, and a smudge filter (repo config plus
# info/attributes) rewrites files at checkout. So hooks, filter drivers, the
# attribute files and config includes are refused before the checkout, the
# checkout is made with hooks off, and then its index and files are checked
# against the commit before gate.sh runs.
#
# The config checks come first because a filter command runs inside
# `worktree add` itself: a check made after it would only report a command
# that has already run. core.hooksPath=/dev/null on this script's own git
# calls is defence in depth against a hook planted between the check and the
# checkout; gate.sh's git calls do not carry it, which is why hooks are
# refused rather than only switched off.
#
# Files are compared by hash against the tree's blob ids (hash-object
# --no-filters against ls-tree), never against `git show`/`cat-file` output:
# a checkout reads every tip blob and nothing on that path verifies blobs, so
# a swapped loose blob would be read twice and agree with itself (ruling
# X-6-F1-F2-F3-F1-esc-1). Status and diff are not used: they trust the index
# stat data this script exists to distrust. Hashing the whole tree costs about
# 1 s. verify_objects (git-env.sh) covers the range's commits and trees, so the
# gate both reviewed and ran honest objects.
#
# Bound: a same-uid writer racing these checks (editing the checkout or the
# common dir between a check and gate.sh) is out of scope, as for every
# consumer. Nothing here repairs an object, deletes a hook or config it finds,
# or writes into objects/: it refuses, and the operator decides.
#
# gate.sh runs the commit's code (its tests, its build scripts), so it runs in
# the runner's container (CONTAIN in tools/run_card.py; X-6-F1-F1-F2-F1-F1), with
# a TMPDIR of its own under the scratch dir: whatever it starts dies with the
# namespace, whatever session it moved to, before this script exits. Without
# this a forge left by a test lands on the branch after the gate was read. No
# container means no gate: a refusal by name, never an uncontained run. What
# this cannot stop is what the runner cannot: a daemon outside asked to run
# something, a file planted to run later, and an `unshare` planted on PATH that
# answers the probe (X-6-F1-F1-F2-F1-F2). A SIGKILL of this script leaves the
# container running, as a SIGKILLed runner does; that run printed no GATE EXIT=.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=git-env.sh
. "$here/git-env.sh"
start=$PWD
t0=$(date +%s%N)
P=()
# refuse_if_any: if any problem is recorded, print them and stop (exit 2).
refuse_if_any() {
  [ ${#P[@]} -gt 0 ] || return 0
  printf 'fresh-gate: %s\n' "${P[@]}" >&2
  echo "FRESH-GATE REFUSED"
  exit 2
}
die() { P+=("$*"); refuse_if_any; }
# add_lines TEXT: one problem per non-empty line (the git-env.sh functions'
# problem lists).
add_lines() { local l; while IFS= read -r l; do [ -z "$l" ] || P+=("$l"); done <<<"$1"; }
usage="usage: fresh-gate.sh [--base REF] COMMIT"

base_ref=nucleus/v3-scaffold commit_ref=
while [ $# -gt 0 ]; do
  case $1 in
    --base) [ $# -ge 2 ] || die "$usage"; base_ref=$2; shift 2 ;;
    -*) die "unknown option '$1' ($usage)" ;;
    *) [ -z "$commit_ref" ] || die "$usage"; commit_ref=$1; shift ;;
  esac
done
[ -n "$commit_ref" ] || die "$usage"
commit=$(git rev-parse --verify -q "$commit_ref^{commit}") || die "cannot resolve commit '$commit_ref'"
echo "fresh-gate: commit $commit"
base=$(git rev-parse --verify -q "$base_ref^{commit}") || die "cannot resolve base '$base_ref'"
echo "fresh-gate: base $base"

# The scratch dir holds the checkout ($d/wt) and this script's lists. mktemp
# puts it under $TMPDIR or /tmp, outside every worktree. The trap removes the
# checkout on every path, and writes only to stderr, so gate.sh's GATE EXIT=
# line (or FRESH-GATE REFUSED) stays the last stdout line.
d=$(mktemp -d) || die "cannot create a temporary directory"
# end_gate: if a signal ended this script while the gate ran, end the gate's
# container before the checkout goes. Its unshare is this shell's foreground
# child, so this reads this shell's own children file (one known process's
# file, no scan) and unshare's for the init. --kill-child SIGKILLs the init,
# and the kernel empties the namespace before the init can become a zombie, so
# the wait is for the init to be gone or a zombie (it is not this shell's to
# reap). A member stuck in uninterruptible sleep holds this wait, as it holds
# the runner's: the checkout stays and no gate result was printed. The builtin
# read, not cat: a command substitution would list its own subshell as a child.
gate_running=
end_gate() {
  [ -n "$gate_running" ] || return 0
  local kids=() inits=() k i s
  read -r -a kids <"/proc/$$/task/$$/children" || true
  for k in "${kids[@]}"; do
    inits=()
    { read -r -a inits <"/proc/$k/task/$k/children"; } 2>/dev/null || true
    kill -KILL "$k" 2>/dev/null || true
    for i in "${inits[@]}"; do
      while { read -r s <"/proc/$i/stat"; } 2>/dev/null && [[ ${s##*) } != Z* ]]; do sleep 0.05; done
    done
  done
}
cleanup() {
  end_gate
  if [ -e "$d/wt" ]; then
    (cd "$start" && git -c core.hooksPath=/dev/null worktree remove --force "$d/wt") >&2 || echo "fresh-gate: warning: git worktree remove failed; remove $d/wt by hand" >&2
  fi
  (cd "$start" && git -c core.hooksPath=/dev/null worktree prune) >&2 || echo "fresh-gate: warning: git worktree prune failed" >&2
  rm -rf "$d" || echo "fresh-gate: warning: cannot remove $d" >&2
}
trap cleanup EXIT

# cfg_named REGEX WHY: name every config entry whose key matches REGEX, at
# every scope (the command line and GIT_CONFIG_PARAMETERS included; keys are
# matched in git's canonical lower case). A failed read is a problem, never
# "nothing found".
cfg_named() {
  local re=$1 why=$2 rc=0 origin kv
  git config -z --show-origin --get-regexp "$re" >"$d/cfg" || rc=$?
  case $rc in
    0) while IFS= read -r -d '' origin && IFS= read -r -d '' kv; do
         if [[ $kv == *$'\n'* ]]; then P+=("${kv%%$'\n'*} is '${kv#*$'\n'}' ($origin; $why)"); else P+=("$kv is set ($origin; $why)"); fi
       done <"$d/cfg" ;;
    1) ;; # none
    *) P+=("git config --get-regexp '$re' failed (exit $rc; cannot check for $why)") ;;
  esac
}

# Step 2, before anything can run: the git state git-env.sh refuses (no
# --index: the worktree judged is the fresh one, below), and whatever could
# run or rewrite files during the checkout or gate.sh's git calls. The hooks
# check is the common dir's hooks/ (any entry but a *.sample file) and
# core.hooksPath (any scope). A filter driver of any name is refused (no name
# is skipped: git runs filter.set.smudge for `filter=set`, verdict
# X-6-F1-F2-F2-F1 R1), and so is every attribute source a driver could be
# attached through outside the tree, and every config include (it can bring
# in any of these). The XDG attributes file is not refused: without a driver,
# an attribute can only convert eol, ident or encoding, which the content
# check below sees.
config_checks() {
  local common rc=0 h
  git_state_refuse >/dev/null || add_lines "$GIT_STATE_PROBLEMS"
  common=$(git rev-parse --path-format=absolute --git-common-dir) || { P+=("git rev-parse --git-common-dir failed (cannot check hooks or info/attributes)"); return; }
  cfg_named '^filter\.' 'filters run at checkout'
  cfg_named '^core\.hookspath$' 'hooks run at checkout and in git calls'
  if [ -e "$common/hooks" ]; then
    find "$common/hooks/" -mindepth 1 -maxdepth 1 ! \( -type f -name '*.sample' \) -print0 >"$d/hooks" || rc=$?
    if [ "$rc" -ne 0 ]; then P+=("cannot list $common/hooks (find exit $rc; cannot check for hooks)"); else while IFS= read -r -d '' h; do P+=("hook $h (hooks run at checkout and in git calls)"); done <"$d/hooks"; fi
  fi
  cfg_named '^core\.attributesfile$' 'attributes apply at checkout'
  [ ! -e "$common/info/attributes" ] && [ ! -L "$common/info/attributes" ] || P+=("attributes file $common/info/attributes exists (attributes apply at checkout)")
  cfg_named '^include(if)?\.' 'an include can add any of these'
  # Config-defined hooks (hook.<name>.command) do not run on git 2.47.3, but
  # newer git runs them, and neither the hooks/ check nor
  # core.hooksPath=/dev/null stops them (verdict X-6-F1-F2-F2-F1-F1 R4).
  cfg_named '^hook\.' 'config hooks run in git calls on newer git'
}
config_checks

# The same flags as CONTAIN in tools/run_card.py (a row compares them), and the
# same probe: everything the container relies on is checked by running it, before
# anything is checked out. Enabling user namespaces needs root: the owner's call.
CONTAIN=(unshare --user --map-current-user --pid --fork --kill-child --mount-proc --)
container_check() {
  local want got rc=0 why
  if ! command -v unshare >/dev/null; then P+=("cannot contain the gate, so not running it: unshare (util-linux) is not on PATH"); return; fi
  if [ ! -e "/proc/$$/task/$$/children" ]; then P+=("cannot contain the gate, so not running it: /proc/$$/task/$$/children is missing (a kernel without CONFIG_PROC_CHILDREN; this script finds the namespace's init there)"); return; fi
  want="1 $(id -u) $(id -g) sh"
  got=$(timeout 30 "${CONTAIN[@]}" sh -c 'echo "$$ $(id -u) $(id -g) $(cat /proc/1/comm)"' </dev/null 2>"$d/contain.err") || rc=$?
  if [ "$rc" -ne 0 ] || [ "$got" != "$want" ]; then
    why=$(head -n 1 "$d/contain.err")
    P+=("cannot contain the gate, so not running it: ${CONTAIN[*]} sh: exit $rc, inside it printed '$got' (want '$want'): ${why:-no stderr}. Unprivileged user namespaces are needed; enabling them needs root and is the owner's call.")
  fi
}
container_check
refuse_if_any

# Step 3, the checkout: detached, hooks off.
rc=0
git -c core.hooksPath=/dev/null worktree add -q --detach "$d/wt" "$commit" >&2 || rc=$?
[ "$rc" -eq 0 ] || die "git worktree add failed (exit $rc)"
cd "$d/wt"
g() { git -c core.hooksPath=/dev/null "$@"; }

# Step 4a, index = commit: (mode, id, path) of every index entry, all at stage
# 0, equal the tree's, by path. write-tree runs only when they match, so it
# finds every tree it would write already stored and writes no object.
rc=0; g ls-tree -r -z --full-tree "$commit" >"$d/tree" || rc=$?
[ "$rc" -eq 0 ] || die "git ls-tree failed (exit $rc; cannot read the commit's tree)"
rc=0; g ls-files -s -z >"$d/index" || rc=$?
[ "$rc" -eq 0 ] || die "git ls-files -s failed (exit $rc; cannot read the checkout's index)"
declare -A tmode=() tid=() seen=() ftype=() fperm=() flink=()
order=()
while IFS= read -r -d '' e; do # <mode> <type> <id>TAB<path>
  m=${e%%$'\t'*} p=${e#*$'\t'}
  tmode[$p]=${m%% *} tid[$p]=${m##* }
  order+=("$p")
done <"$d/tree"
n0=${#P[@]}
while IFS= read -r -d '' e; do # <mode> <id> <stage>TAB<path>
  m=${e%%$'\t'*} p=${e#*$'\t'}
  mode=${m%% *} rest=${m#* }; id=${rest%% *} stage=${rest#* }
  [ "$stage" = 0 ] || P+=("$p: index entry at stage $stage")
  if [ -z "${tid[$p]+x}" ]; then P+=("$p: in the index ($mode $id), not in the tree")
  elif [ "$mode $id" != "${tmode[$p]} ${tid[$p]}" ]; then P+=("$p: index says $mode $id, tree says ${tmode[$p]} ${tid[$p]}"); fi
  seen[$p]=1
done <"$d/index"
for p in "${order[@]}"; do [ -n "${seen[$p]:-}" ] || P+=("$p: in the tree, not in the index"); done
if [ ${#P[@]} -eq "$n0" ]; then
  wtree=$(g write-tree) || die "git write-tree failed (cannot check the index against the commit)"
  ctree=$(g rev-parse --verify -q "$commit^{tree}") || die "git rev-parse $commit^{tree} failed"
  [ "$wtree" = "$ctree" ] || P+=("the index writes tree $wtree, the commit's tree is $ctree")
fi

# Step 4b, nothing extra: the filesystem itself (not status, which trusts the
# index), every non-directory outside .git. One find gives type, permission
# bits and link target for the content check too.
rc=0; find . -path ./.git -prune -o ! -type d -printf '%y\0%m\0%l\0%P\0' >"$d/fs" || rc=$?
[ "$rc" -eq 0 ] || die "cannot list the checkout (find exit $rc)"
while IFS= read -r -d '' y && IFS= read -r -d '' m && IFS= read -r -d '' l && IFS= read -r -d '' p; do
  ftype[$p]=$y fperm[$p]=$m flink[$p]=$l
  [ -n "${tid[$p]+x}" ] || P+=("$p: in the checkout, not in the tree")
done <"$d/fs"

# Step 4c, content = tree. Regular files and symlink targets are hashed in one
# hash-object --stdin-paths batch (a symlink's blob is its target's bytes, so
# each target goes into a scratch file). --stdin-paths reads one path a line
# and C-unquotes a line that starts with '"', and strips a trailing CR, so a
# path that starts with '"' or holds a newline or ends in CR goes in quoted.
mkdir "$d/links"
hp=() hn=() hid=() kind=()
: >"$d/paths"
for p in "${order[@]}"; do
  mode=${tmode[$p]} id=${tid[$p]}
  case $mode in
    100644 | 100755)
      if [ -z "${ftype[$p]+x}" ]; then P+=("$p: missing from the checkout"); continue; fi
      [ "${ftype[$p]}" = f ] || { P+=("$p: is a '${ftype[$p]}' in the checkout, tree mode is $mode"); continue; }
      x=off; (((8#${fperm[$p]} & 8#100) == 0)) || x=on
      { [ "$mode" = 100755 ] && [ "$x" = on ]; } || { [ "$mode" = 100644 ] && [ "$x" = off ]; } || P+=("$p: executable bit is $x in the checkout, tree mode is $mode")
      q=$p kind+=(file) ;;
    120000)
      if [ -z "${ftype[$p]+x}" ]; then P+=("$p: missing from the checkout"); continue; fi
      [ "${ftype[$p]}" = l ] || { P+=("$p: is a '${ftype[$p]}' in the checkout, tree mode is $mode"); continue; }
      q=$d/links/${#hp[@]}
      printf '%s' "${flink[$p]}" >"$q"
      kind+=(link) ;;
    160000)
      # A gitlink is not checked out: its directory must exist and be empty
      # (a failed find prints a word, so it fails closed).
      if [ ! -d "$p" ] || [ -L "$p" ] || [ -n "$(find "./$p" -mindepth 1 -print -quit || echo failed)" ]; then P+=("$p: a gitlink whose directory is missing or not empty"); fi
      continue ;;
    *) P+=("$p: unexpected tree mode $mode"); continue ;;
  esac
  hp+=("$q") hn+=("$p") hid+=("$id")
  if [[ $q == \"* || $q == *$'\n'* || $q == *$'\r' ]]; then
    q=${q//\\/\\\\} q=${q//\"/\\\"} q=${q//$'\n'/\\n} q=${q//$'\r'/\\r}
    printf '"%s"\n' "$q" >>"$d/paths"
  else
    printf '%s\n' "$q" >>"$d/paths"
  fi
done
got=()
if [ ${#hp[@]} -gt 0 ]; then
  rc=0; g hash-object --no-filters --stdin-paths <"$d/paths" >"$d/got" || rc=$?
  [ "$rc" -eq 0 ] || die "git hash-object failed (exit $rc; cannot check the content)"
  mapfile -t got <"$d/got"
  [ ${#got[@]} -eq ${#hp[@]} ] || die "git hash-object printed ${#got[@]} ids for ${#hp[@]} paths (cannot check the content)"
fi
mismatch=()
for i in "${!hp[@]}"; do
  [ "${got[i]}" = "${hid[i]}" ] || mismatch+=("$i")
done
# The one exception, eol conversion (the two `text eol=crlf` .ps1 files here).
# A mismatch passes only for a regular file whose every attribute (check-attr
# -a) is text, eol or crlf, whose eol attribute is crlf, in which every CR and
# every LF belongs to a CRLF pair, and whose hash-object --path (which applies
# the conversion; no filter driver exists, step 2) equals its id.
# Why so narrow: clean conversion is many-to-one (CRLF and LF both clean to
# LF), so "cleans to the id" alone accepts a forged blob whose CRs survive the
# checkout. Under eol=lf (this repository's `* text=auto eol=lf` covers every
# text file) a blob forged with a CR before some LF checks out byte for byte,
# gate.sh runs those bytes, and they still clean to the true content (verdict
# X-6-F1-F2-F2-F1-F1 R1). On an eol=crlf path whose file is pure CRLF, the
# honest blob checks out to these same bytes (every LF of the cleaned content
# gets its CR back), so whatever blob produced them changes nothing. A lone LF
# or a lone CR is refused, because the honest checkout would not write it.
# Any other attribute is refused outright: ident's clean is many-to-one too
# (`$Id: anything $` cleans to `$Id$`), and so is working-tree-encoding. A
# file with no eol attribute (core.autocrlf alone) does not qualify. The
# exception compares hashes, so a swapped blob is refused under it too.
declare -A conv_bad=() eol_of=()
eol=0 mp=()
for i in "${mismatch[@]}"; do [ "${kind[i]}" != file ] || mp+=("${hn[i]}"); done
if [ ${#mp[@]} -gt 0 ]; then
  rc=0; printf '%s\0' "${mp[@]}" | g check-attr -a -z --stdin >"$d/attr" || rc=$?
  [ "$rc" -eq 0 ] || die "git check-attr failed (exit $rc; cannot check eol attributes)"
  while IFS= read -r -d '' p && IFS= read -r -d '' a && IFS= read -r -d '' v; do
    case $a in text | crlf) ;; eol) eol_of[$p]=$v ;; *) conv_bad[$p]=1 ;; esac
  done <"$d/attr"
fi
# crlf_only FILE: every CR and every LF in FILE belongs to a CRLF pair. od and
# awk read bytes (a file may hold NUL); awk exits 3 for "no", so any other
# failure stops the run instead of reading as an answer.
crlf_only() {
  local rc=0
  od -An -v -tx1 -- "$1" | LC_ALL=C awk '{ for (i = 1; i <= NF; i++) { if (($i == "0a") != (p == "0d")) bad = 1; p = $i } } END { if (p == "0d") bad = 1; exit bad ? 3 : 0 }' || rc=$?
  case $rc in
    0) return 0 ;;
    3) return 1 ;;
    *) die "cannot read the line endings of $1 (exit $rc; cannot check its eol conversion)" ;;
  esac
}
for i in "${mismatch[@]}"; do
  p=${hn[i]}
  if [ "${kind[i]}" = file ] && [ -z "${conv_bad[$p]:-}" ] && [ "${eol_of[$p]:-}" = crlf ] && crlf_only "$p"; then
    h=$(g hash-object --path="$p" -- "$p") || die "git hash-object --path failed for $p (cannot check its eol conversion)"
    [ "$h" = "${hid[i]}" ] && { eol=$((eol + 1)); continue; }
  fi
  P+=("$p: content hashes to ${got[i]}, tree says ${hid[i]}")
done

# Step 4d: the fresh index carries no flag or clean filter, and the objects of
# the range hash to their ids (the tip's blobs are covered by 4c).
git_state_refuse --index >/dev/null || add_lines "$GIT_STATE_PROBLEMS"
verify_objects "$base" "$commit" || add_lines "$VERIFY_PROBLEMS"
[ -f nucleus/v3/scripts/gate.sh ] || P+=("the commit has no nucleus/v3/scripts/gate.sh")
refuse_if_any

# Step 5: the gate, in the foreground and in its container, its output passed
# through. Nothing is printed after it; the trap writes only to stderr. The time
# printed is all of the checks, from the start (checkout and verify_objects
# included), not the gate's. unshare returns after its init did, and the kernel
# empties the namespace before that, so nothing of the gate is alive after this
# line. "$d/contained" tells a container that failed to start from a gate that
# failed: unshare's own failure is an exit 1 with no GATE EXIT= line, and the
# contract gives that a refusal.
ms=$((($(date +%s%N) - t0) / 1000000))
printf 'fresh-gate: checks passed (%d files hashed in %d.%02d s)\n' "${#hp[@]}" $((ms / 1000)) $((ms % 1000 / 10))
[ "$eol" -eq 0 ] || echo "fresh-gate: $eol file(s) matched through their eol conversion"
export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-$HOME/.cache/nv3-target/fresh-${commit:0:12}}
mkdir "$d/tmp"
rc=0
gate_running=1
TMPDIR=$d/tmp "${CONTAIN[@]}" /bin/sh -c ': >"$1" && cd nucleus/v3 && bash scripts/gate.sh' fresh-gate "$d/contained" || rc=$?
gate_running=
[ -e "$d/contained" ] || die "the gate's container did not start (unshare exit $rc), so the gate did not run"
exit "$rc"
