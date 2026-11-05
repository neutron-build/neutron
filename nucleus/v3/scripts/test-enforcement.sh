#!/usr/bin/env bash
# Scenario tests for the enforcement scripts (PLAN-v3 §7.6), run by gate.sh.
#
# Builds a throwaway repository (and a bare "origin") under a temp dir, copies
# the real scripts and runner into it, and checks that every rule both refuses
# what it must and accepts what it must. Nothing touches the real repository.
set -euo pipefail
src=$(cd "$(dirname "$0")/.." && pwd)        # the real nucleus/v3
t=$(mktemp -d)
# --- an abort is a FAIL line (X-6-F1-F2-F3-F1-F4 Step 3) ---------------------
# aborted_at records the line set -e stopped at (the ERR trap fires only for
# commands whose failure aborts the script: rows run their commands inside
# expect_ok's if or behind ||, which suspend it, and functions do not inherit
# it), and suite_finished marks the planned final exit. The EXIT trap keeps
# rm -rf "$t" and the exit status, and adds a FAIL line for an early abort —
# an exit that did not come from the final `exit $(( fails > 0 ))` — so a
# mutant run reports where it stopped instead of silently losing every later
# row. The Step 3 self-test row at the end of the suite lifts the marked block
# verbatim into a throwaway script: removing the print here (TRAP-SILENT)
# fails that row.
# BEGIN suite-abort-trap (lifted verbatim by the Step 3 self-test row)
aborted_at=0
suite_finished=0
trap 'aborted_at=$LINENO' ERR
trap 'rc=$?; if [ "${suite_finished:-0}" -eq 0 ] && [ "$rc" -ne 0 ]; then echo "FAIL suite aborted (set -e) near line ${aborted_at:-0}; later rows did not run"; fi; rm -rf "$t"' EXIT
# END suite-abort-trap
fails=0
pass() { echo "ok   $1"; }
fail() { echo "FAIL $1"; fails=$((fails + 1)); }
# expect_ok NAME CMD... / expect_no NAME CMD...
expect_ok() { local n=$1; shift; if "$@" >"$t/out" 2>&1; then pass "$n"; else fail "$n"; sed 's/^/     /' "$t/out"; fi; }
expect_no() { local n=$1; shift; if "$@" >"$t/out" 2>&1; then fail "$n (was accepted)"; sed 's/^/     /' "$t/out"; else pass "$n"; fi; }

export GIT_AUTHOR_NAME=t GIT_AUTHOR_EMAIL=t@t GIT_COMMITTER_NAME=t GIT_COMMITTER_EMAIL=t@t
# The git-state scenarios (X-6-F1-F2) check that the runner and the scripts
# switch off replace refs, grafts and fsmonitor themselves, so none of that may
# come from the caller (the runner and git-env.sh export it to their children).
unset GIT_NO_REPLACE_OBJECTS GIT_GRAFT_FILE GIT_CONFIG_COUNT
git init -q --bare "$t/origin.git"
r=$t/repo
git init -q -b nucleus/v3-scaffold "$r"
echo repo >"$r/README"
git -C "$r" add -A && git -C "$r" commit -qm "pre-v3 history"
# Bootstrap nucleus/v3 exactly as the real repository does (S-0): a P card that
# adds the scripts plus docs/README.md and verdicts/README.md, merged by the real
# merge-card.sh run from outside the (still v3-less) scratch branch.
cards=$t/cards
mkdir -p "$cards"
export NV3_CARDS=$cards
printf '# QUEUE\n' >"$cards/QUEUE.local.md"
printf '# CARD BOOT: t\n\nClass: core        Author: P (t)        Status: ready\nBase: nucleus/v3-scaffold        Depends: —        Gate: P\n' >"$cards/BOOT.local.md"
git -C "$r" checkout -q -b card/v3-BOOT
mkdir -p "$r/nucleus/v3/scripts" "$r/nucleus/v3/tools" "$r/nucleus/v3/verdicts" "$r/nucleus/v3/spikes" "$r/nucleus/v3/docs" "$r/.forgejo/workflows"
cp "$src"/scripts/*.sh "$r/nucleus/v3/scripts/"
cp "$src/CORE_PATHS" "$r/nucleus/v3/"
cp "$src/tools/run_card.py" "$r/nucleus/v3/tools/"
cp "$src/verdicts/README.md" "$r/nucleus/v3/verdicts/README.md"
cp "$src/docs/README.md" "$r/nucleus/v3/docs/README.md"
echo "ci" >"$r/.forgejo/workflows/nucleus-v3.yml"
# The copied gate must not recurse into these tests.
printf '#!/usr/bin/env bash\nexit 0\n' >"$r/nucleus/v3/scripts/test-enforcement.sh"
echo base >"$r/nucleus/v3/spikes/base.txt"
git -C "$r" add -A && git -C "$r" commit -qm "nucleus v3: BOOT work"
printf 'Card: BOOT\nClass: core\nCard-gate: P\nVerdict: MERGE\nGate-tier: P\nGate-model: test-model\nGate-session: t\nDate: 2026-10-17\nGate-exit: GATE EXIT=0\nCommit: %s\nMutants: n/a\nFollowups: none\n' "$(git -C "$r" rev-parse HEAD)" >"$r/nucleus/v3/verdicts/BOOT.md"
git -C "$r" add -A && git -C "$r" commit -qm "nucleus v3: BOOT verdict MERGE"
git -C "$r" checkout -q -b scratch/boot nucleus/v3-scaffold
if (cd "$r" && bash "$src/scripts/merge-card.sh" BOOT) >"$t/out" 2>&1; then pass "bootstrap: the S-0-shaped P card merges via merge-card"; else fail "bootstrap merge"; sed 's/^/     /' "$t/out"; fi
git -C "$r" checkout -q nucleus/v3-scaffold && git -C "$r" merge -q --ff-only scratch/boot
git -C "$r" remote add origin "$t/origin.git"
git -C "$r" push -q origin nucleus/v3-scaffold

# A fake cargo so the copied gate.sh runs without building anything.
# FAKE_FAIL names the cargo subcommand to fail.
mkdir -p "$t/bin"
cat >"$t/bin/cargo" <<'F'
#!/usr/bin/env bash
[ "${1:-}" = "${FAKE_FAIL:-none}" ] && { echo "fake cargo $1 failed" >&2; exit 1; }
exit 0
F
chmod +x "$t/bin/cargo"
export PATH="$t/bin:$PATH"
v3=$r/nucleus/v3

# card ID GATE CLASS: write the local card file.
card() { printf '# CARD %s: test\n\nClass: %s        Author: S (t)        Status: ready\nBase: nucleus/v3-scaffold        Depends: —        Gate: %s\n' "$1" "$3" "$2" >"$cards/$1.local.md"; }
# branch ID FILE...: card/v3-ID with one commit changing FILEs (repo-relative).
branch() {
  local id=$1; shift
  git -C "$r" checkout -q -b "card/v3-$id" nucleus/v3-scaffold
  for f in "$@"; do mkdir -p "$(dirname "$r/$f")"; echo "$id" >>"$r/$f"; done
  git -C "$r" add -A && git -C "$r" commit -qm "nucleus v3: $id work"
}
# verdict ID TIER VERDICT CLASS CARDGATE [GATEEXIT] [FOLLOWUPS]: commit verdicts/ID.md at the tip.
verdict() {
  local id=$1 tier=$2 vd=$3 cls=$4 cg=$5 ge=${6:-GATE EXIT=0} fu=${7:-none}
  printf 'Card: %s\nClass: %s\nCard-gate: %s\nVerdict: %s\nGate-tier: %s\nGate-model: test-model\nGate-session: t\nDate: 2026-10-17\nGate-exit: %s\nCommit: %s\nMutants: n/a\nFollowups: %s\n' \
    "$id" "$cls" "$cg" "$vd" "$tier" "$ge" "$(git -C "$r" rev-parse HEAD)" "$fu" >"$(mkdir -p "$v3/verdicts" && echo "$v3/verdicts/$id.md")"
  git -C "$r" add -A && git -C "$r" commit -qm "nucleus v3: $id verdict $vd"
}
scratch() { git -C "$r" checkout -q -B scratch/pass1 nucleus/v3-scaffold; }
mc() { (cd "$v3" && bash scripts/merge-card.sh "$1"); }

# --- merge-card -------------------------------------------------------------
card A S standard; branch A nucleus/v3/spikes/a.txt; verdict A S MERGE standard S
scratch; expect_ok "S card, S verdict, spikes only: merges" mc A
# check_merge as audit-merges.sh calls it: a base git cannot diff against is
# refused by the path listing's own exit status, not read as "no paths".
cm() { (cd "$v3" && . scripts/core-paths-lib.sh && . scripts/enforce-lib.sh && check_merge A "$1" card/v3-A audit); }
cm_refused() { local o rc=0; o=$(cm "$1" 2>&1) || rc=$?; echo "$o"; [ "$rc" -ne 0 ] && grep -qF "cannot list paths changed in $1...card/v3-A" <<<"$o"; }
expect_ok "check_merge: a bogus base is refused" cm_refused no-such-ref
expect_ok "check_merge: the same call with base nucleus/v3-scaffold passes" cm nucleus/v3-scaffold

card B S standard; branch B nucleus/v3/scripts/gate.sh; verdict B S MERGE standard S
scratch; expect_no "S verdict on a CORE change (scripts/) is refused" mc B
card C P core; branch C nucleus/v3/scripts/extra.sh; verdict C P MERGE core P
scratch; expect_ok "P verdict on a CORE change merges" mc C

card D P standard; branch D nucleus/v3/docs/X.md; verdict D P MERGE standard P
scratch; expect_no "docs/ change by a non-spec card is refused" mc D
card E P spec; branch E nucleus/v3/docs/Y.md; verdict E P MERGE spec P
scratch; expect_ok "docs/ change by a P spec card merges" mc E

# Paths git would C-quote ("nucleus/v3/docs/\303\251.md") must not slip past the
# nucleus/v3/ rules as outside-v3 files: non-ASCII, and `"`, which
# core.quotePath=false would still quote.
card DU P core; branch DU "nucleus/v3/docs/é.md"; verdict DU P MERGE core P
scratch; expect_no "docs/é.md by a P core (non-spec) card is refused" mc DU
card DQ P core; branch DQ 'nucleus/v3/docs/a"b.md'; verdict DQ P MERGE core P
scratch; expect_no 'docs/a"b.md by a P core (non-spec) card is refused' mc DQ
card EU P spec; branch EU "nucleus/v3/docs/é.md"; verdict EU P MERGE spec P
scratch; expect_ok "docs/é.md by a P spec card merges" mc EU
card SU S standard; branch SU "nucleus/v3/scripts/é.sh"; verdict SU S MERGE standard S
scratch; expect_no "S verdict on CORE scripts/é.sh is refused" mc SU
card AU S standard; branch AU "nucleus/v3/spikes/ü.txt"; verdict AU S MERGE standard S
scratch; expect_ok "S card adding spikes/ü.txt merges (seen as inside nucleus/v3)" mc AU
expect_ok "check-core-paths lists CORE scripts/é.sh" bash -c "cd '$v3' && bash scripts/check-core-paths.sh nucleus/v3-scaffold card/v3-SU | grep -qxF 'CORE scripts/é.sh (scripts/)'"
# A base git cannot diff against must fail the listing, not list nothing (S-0-F2-F2).
expect_no "check-core-paths: a bogus base exits non-zero" bash -c "cd '$v3' && bash scripts/check-core-paths.sh no-such-ref card/v3-SU"
# CORE_PATHS is read from the base fail-closed (S-0-F2-F2-F1-F1): a read git
# fails is refused, never replaced by the worktree's list or an empty one.
# ccp_refused DIR BASE HEAD: check-core-paths in DIR fails and names the read.
ccp_refused() { local o rc=0; o=$(cd "$1" && bash scripts/check-core-paths.sh "$2" "$3" 2>&1) || rc=$?; echo "$o"; [ "$rc" -ne 0 ] && grep -qF "check-core-paths: cannot read CORE_PATHS from $2 (" <<<"$o"; }
expect_ok "check-core-paths: a bogus base is refused by the CORE_PATHS read" ccp_refused "$v3" no-such-ref card/v3-SU
# The real exposure: a base git can diff (trees only) but whose CORE_PATHS blob
# is gone. Built in a copy, so the deleted object cannot break later scenarios.
mb=$t/mb
cp -a "$r" "$mb"
git -C "$mb" checkout -q -b mb-base nucleus/v3-scaffold
echo '# mb-scenario' >>"$mb/nucleus/v3/CORE_PATHS"
git -C "$mb" commit -qam "nucleus v3: mb-base CORE_PATHS"
card MB S standard
git -C "$mb" checkout -q -b card/v3-MB
echo MB >"$mb/nucleus/v3/scripts/mb.sh"
git -C "$mb" add -A && git -C "$mb" commit -qm "nucleus v3: MB work"
printf 'Card: MB\nClass: standard\nCard-gate: S\nVerdict: MERGE\nGate-tier: S\nGate-model: test-model\nGate-session: t\nDate: 2026-10-20\nGate-exit: GATE EXIT=0\nCommit: %s\nMutants: n/a\nFollowups: none\n' "$(git -C "$mb" rev-parse HEAD)" >"$mb/nucleus/v3/verdicts/MB.md"
git -C "$mb" add -A && git -C "$mb" commit -qm "nucleus v3: MB verdict MERGE"
git -C "$mb" checkout -q nucleus/v3-scaffold
mbblob=$(git -C "$mb" rev-parse mb-base:nucleus/v3/CORE_PATHS)
rm -f "$mb/.git/objects/${mbblob:0:2}/${mbblob:2}"
mb_diffs() { git -C "$mb" diff -z --name-only mb-base...card/v3-MB >"$t/mbl" && tr '\0' '\n' <"$t/mbl" | grep -qxF nucleus/v3/scripts/mb.sh; }
expect_ok "premise: git diff mb-base...card/v3-MB succeeds and lists scripts/mb.sh" mb_diffs
expect_no "premise: git show mb-base:nucleus/v3/CORE_PATHS fails (blob deleted)" git -C "$mb" show mb-base:nucleus/v3/CORE_PATHS
expect_ok "check-core-paths: a base whose CORE_PATHS blob is missing is refused" ccp_refused "$mb/nucleus/v3" mb-base card/v3-MB
cm_mb() { local o rc=0; o=$(cd "$mb/nucleus/v3" && . scripts/core-paths-lib.sh && . scripts/enforce-lib.sh && check_merge MB mb-base card/v3-MB audit 2>&1) || rc=$?; echo "$o"; [ "$rc" -ne 0 ] && grep -qF "cannot read CORE_PATHS from mb-base" <<<"$o"; }
expect_ok "check_merge: a base whose CORE_PATHS blob is missing is refused" cm_mb
# Control: a base that predates CORE_PATHS (the scratch root, "pre-v3 history")
# still falls back to the worktree's list.
root=$(git -C "$r" rev-list --max-parents=0 nucleus/v3-scaffold)
root_absent() { local o; o=$(git -C "$r" ls-tree --full-tree "$root" -- nucleus/v3/CORE_PATHS) && [ -z "$o" ]; }
expect_ok "premise: ls-tree of the root commit lists no CORE_PATHS" root_absent
expect_ok "check-core-paths: a base that predates CORE_PATHS uses the worktree's list" bash -c "cd '$v3' && bash scripts/check-core-paths.sh '$root' card/v3-B | grep -qxF 'CORE scripts/gate.sh (scripts/)'"
# A base whose CORE_PATHS is not a regular file is refused (S-0-F2-F2-F1-F1-F1).
# git stores a symlink as a blob holding the link text, so accepting any blob
# would read "CORE_PATHS.real" as the CORE list and let an S verdict through on
# scripts/. Built in copies of $r, like mb, so later scenarios see none of it.
# cpcard DIR BASE ID GATEEXIT FILE...: in DIR, card/v3-ID off BASE with one
# commit adding FILEs, then an otherwise valid S verdict with that Gate-exit.
cpcard() {
  local d=$1 b=$2 id=$3 ge=$4 f; shift 4
  git -C "$d" checkout -q -b "card/v3-$id" "$b"
  for f in "$@"; do mkdir -p "$(dirname "$d/$f")"; echo "$id" >"$d/$f"; done
  git -C "$d" add -A && git -C "$d" commit -qm "nucleus v3: $id work"
  printf 'Card: %s\nClass: standard\nCard-gate: S\nVerdict: MERGE\nGate-tier: S\nGate-model: test-model\nGate-session: t\nDate: 2026-10-31\nGate-exit: %s\nCommit: %s\nMutants: n/a\nFollowups: none\n' "$id" "$ge" "$(git -C "$d" rev-parse HEAD)" >"$d/nucleus/v3/verdicts/$id.md"
  git -C "$d" add -A && git -C "$d" commit -qm "nucleus v3: $id verdict MERGE"
}
# cpm DIR ID BASE HEAD: check_merge in the copy DIR, as cm_mb runs it.
cpm() { (cd "$1/nucleus/v3" && . scripts/core-paths-lib.sh && . scripts/enforce-lib.sh && check_merge "$2" "$3" "$4" audit); }
# cpm_refused DIR ID BASE HEAD: check_merge fails and names the CORE_PATHS read.
cpm_refused() { local o rc=0; o=$(cpm "$@" 2>&1) || rc=$?; echo "$o"; [ "$rc" -ne 0 ] && grep -qF "cannot read CORE_PATHS from $3" <<<"$o"; }
# ccp_why DIR BASE HEAD WHY: ccp_refused, and the output also gives WHY.
ccp_why() { local o rc=0; o=$(ccp_refused "$1" "$2" "$3") || rc=$?; echo "$o"; [ "$rc" -eq 0 ] && grep -qF -- "$4" <<<"$o"; }
# cp_has WANT... -- CMD...: CMD exits non-zero and its output holds every WANT
# (out_has, which is defined further down).
cp_has() { local w=() o rc=0 x; while [ "$1" != -- ]; do w+=("$1"); shift; done; shift; o=$("$@" 2>&1) || rc=$?; echo "$o"; [ "$rc" -ne 0 ] || return 1; for x in "${w[@]}"; do grep -qF -- "$x" <<<"$o" || { echo "missing: $x"; return 1; }; done; }
# lstree_is DIR BASE PREFIX: BASE's CORE_PATHS entry starts with PREFIX.
lstree_is() { local o; o=$(git -C "$1" ls-tree --full-tree "$2" -- nucleus/v3/CORE_PATHS) && [[ $o == "$3"* ]]; }
# diff_lists DIR RANGE PATH...: git diff RANGE lists every PATH.
diff_lists() { local o p; o=$(git -C "$1" diff --name-only "$2") || return 1; shift 2; for p in "$@"; do grep -qxF -- "$p" <<<"$o" || return 1; done; }

sl=$t/sl
cp -a "$r" "$sl"
git -C "$sl" checkout -q -b sl-base nucleus/v3-scaffold
git -C "$sl" mv nucleus/v3/CORE_PATHS nucleus/v3/CORE_PATHS.real
ln -s CORE_PATHS.real "$sl/nucleus/v3/CORE_PATHS"
git -C "$sl" add -A && git -C "$sl" commit -qm "nucleus v3: sl-base CORE_PATHS is a symlink"
card SL S standard
cpcard "$sl" sl-base SL "GATE EXIT=0" nucleus/v3/scripts/sl.sh
# Off sl-base too: one more violation before the CORE read (Gate-exit) and one
# after it (docs/), so a check_merge that stops at the failed read loses one.
card SLX S standard
cpcard "$sl" sl-base SLX "GATE EXIT=1" nucleus/v3/scripts/slx.sh nucleus/v3/docs/slx.md
git -C "$sl" checkout -q nucleus/v3-scaffold
expect_ok "premise: sl-base's CORE_PATHS is a symlink (ls-tree 120000 blob)" lstree_is "$sl" sl-base "120000 blob "
expect_ok "premise: git diff sl-base...card/v3-SL lists scripts/sl.sh" diff_lists "$sl" sl-base...card/v3-SL nucleus/v3/scripts/sl.sh
expect_ok "check-core-paths: a base whose CORE_PATHS is a symlink is refused" ccp_why "$sl/nucleus/v3" sl-base card/v3-SL "(mode 120000), not a regular file"
expect_ok "check_merge: a base whose CORE_PATHS is a symlink is refused" cpm_refused "$sl" SL sl-base card/v3-SL

# A directory at the path: the reason pin. Without the type check the tree id
# still fails `cat-file blob`, so this row pins only the reason text.
dr=$t/dr
cp -a "$r" "$dr"
git -C "$dr" checkout -q -b dr-base nucleus/v3-scaffold
git -C "$dr" mv nucleus/v3/CORE_PATHS nucleus/v3/CORE_PATHS.list
mkdir "$dr/nucleus/v3/CORE_PATHS"
git -C "$dr" mv nucleus/v3/CORE_PATHS.list nucleus/v3/CORE_PATHS/list
git -C "$dr" commit -qm "nucleus v3: dr-base CORE_PATHS is a directory"
card DR S standard
cpcard "$dr" dr-base DR "GATE EXIT=0" nucleus/v3/scripts/dr.sh
git -C "$dr" checkout -q nucleus/v3-scaffold
expect_ok "premise: dr-base's CORE_PATHS is a directory (ls-tree 040000 tree)" lstree_is "$dr" dr-base "040000 tree "
expect_ok "check-core-paths: a base whose CORE_PATHS is a directory is refused as a tree" ccp_why "$dr/nucleus/v3" dr-base card/v3-DR "is a tree (mode 040000)"
expect_ok "check_merge: a base whose CORE_PATHS is a directory is refused" cpm_refused "$dr" DR dr-base card/v3-DR

# The failed read is one error among the others (S-0-F2-F2-F1-F1 Work 3).
expect_ok "premise: git diff sl-base...card/v3-SLX lists scripts/slx.sh and docs/slx.md" diff_lists "$sl" sl-base...card/v3-SLX nucleus/v3/scripts/slx.sh nucleus/v3/docs/slx.md
expect_ok "check_merge: a failed CORE_PATHS read still reports every other violation" cp_has \
  "cannot read CORE_PATHS from sl-base" "Gate-exit is 'GATE EXIT=1', not 'GATE EXIT=0'" "nucleus/v3/docs/slx.md: docs/ changes need Class: spec and a P verdict" \
  -- cpm "$sl" SLX sl-base card/v3-SLX

card F S standard; branch F nucleus/v3/spikes/f.txt; verdict F S MERGE standard S "GATE EXIT=1"
scratch; expect_no "Gate-exit GATE EXIT=1 is refused" mc F

card G S standard; branch G nucleus/v3/spikes/g.txt; verdict G S MERGE standard S
echo late >>"$r/nucleus/v3/scripts/promote-pass.sh"; git -C "$r" commit -qam "nucleus v3: G late change"
scratch; expect_no "a commit after the verdict is refused" mc G

card H S standard; branch H nucleus/v3/spikes/h.txt nucleus/v3/verdicts/A.md; verdict H S MERGE standard S
scratch; expect_no "changing another card's verdict is refused" mc H

card I S standard; branch I .forgejo/workflows/nucleus-v3.yml; verdict I S MERGE standard S
scratch; expect_no "S verdict on a change outside nucleus/v3 is refused" mc I

card J P core; branch J nucleus/v3/spikes/j.txt; verdict J P MERGE core S
scratch; expect_no "verdict Card-gate S on a Gate: P card is refused" mc J
card K P core; branch K nucleus/v3/spikes/k.txt; verdict K S MERGE core P
scratch; expect_no "S verdict on a Gate: P card is refused" mc K

card L S standard; branch L nucleus/v3/spikes/l.txt; verdict L S MERGE-WITH-FOLLOWUPS standard S "GATE EXIT=0" "L-F1"
printf -- '- L-F10 | queued | S | other\n' >>"$cards/QUEUE.local.md"
scratch; expect_no "follow-up L-F1 is not satisfied by L-F10" mc L
printf -- '- L-F1 | queued | S | follow-up\n' >>"$cards/QUEUE.local.md"
scratch; expect_ok "follow-up listed exactly: merges" mc L
card M S standard; branch M nucleus/v3/spikes/m.txt; verdict M S MERGE-WITH-FOLLOWUPS standard S "GATE EXIT=0" "none"
scratch; expect_no "MERGE-WITH-FOLLOWUPS with no follow-ups is refused" mc M

card N S standard; branch N nucleus/v3/spikes/n.txt; verdict N S REWORK standard S
scratch; expect_no "a REWORK verdict is refused" mc N

card G2 S standard; branch G2 nucleus/v3/spikes/g2.txt; verdict G2 S MERGE standard S
echo late >>"$r/nucleus/v3/spikes/g2.txt"; git -C "$r" commit -qam "nucleus v3: G2 late non-core change"
scratch; expect_no "a late non-CORE commit after the verdict is refused" mc G2
# The Commit rule refuses G2 too, so only the reason pins the tip check's
# "first file is the verdict" test: a one-file tip that is not the verdict.
# That test is not implied by Commit = tip^, since a verdict naming its own
# 7-hex prefix is a 2^28 grind.
scratch; expect_ok "G2's refusal names the tip rule (verdict is not the branch tip)" bash -c "cd '$v3' && rc=0 && o=\$(bash scripts/merge-card.sh G2 2>&1) || rc=\$?; echo \"\$o\"; [ \"\$rc\" -ne 0 ] && grep -qF 'the branch tip must be the verdict commit' <<<\"\$o\""
card G3 S standard; branch G3 nucleus/v3/spikes/g3.txt
echo extra >>"$r/nucleus/v3/spikes/g3b.txt"; git -C "$r" add -A; verdict G3 S MERGE standard S
scratch; expect_no "a verdict commit that also changes another file is refused" mc G3
# G3's extra file sorts before verdicts/G3.md, so the tip check's "first file is
# the verdict" test refuses it on its own. An extra file sorting after the
# verdict leaves the verdict first; only the "exactly one file" count refuses it.
# zz/ and é.txt (0xC3 > 'v') are non-CORE, so no other rule refuses these.
# tip_first ID: the verdict file is the first path the tip commit changes.
tip_first() { [ "$(git -C "$r" diff-tree -z --no-commit-id --name-only -r "card/v3-$1" -- ':/' | tr '\0' '\n' | head -1)" = "nucleus/v3/verdicts/$1.md" ]; }
card G5 S standard; branch G5 nucleus/v3/spikes/g5.txt
mkdir -p "$v3/zz" && echo extra >>"$v3/zz/x.txt"; git -C "$r" add -A; verdict G5 S MERGE standard S
expect_ok "premise: G5's tip lists verdicts/G5.md before zz/x.txt" tip_first G5
scratch; expect_no "a verdict commit that also changes a file sorting after it (zz/x.txt) is refused" mc G5
card G5U S standard; branch G5U nucleus/v3/spikes/g5u.txt
echo extra >>"$v3/é.txt"; git -C "$r" add -A; verdict G5U S MERGE standard S
expect_ok "premise: G5U's tip lists verdicts/G5U.md before é.txt" tip_first G5U
scratch; expect_no "a verdict commit that also changes a non-ASCII file sorting after it (é.txt) is refused" mc G5U
# A verdict re-committed over a late change: the tip is again verdict-only (the
# tip-files check passes), but its Commit still names the old reviewed commit,
# not tip^. Only the Commit = tip^ check can refuse this.
card G4 S standard; branch G4 nucleus/v3/spikes/g4.txt; verdict G4 S MERGE standard S
echo late >>"$r/nucleus/v3/spikes/g4.txt"; git -C "$r" commit -qam "nucleus v3: G4 late non-core change"
sed -i 's/^Date: .*/Date: 2026-10-18/' "$v3/verdicts/G4.md"; git -C "$r" commit -qam "nucleus v3: G4 verdict MERGE (re-committed)"
scratch; expect_no "a verdict re-committed over a late change (stale Commit) is refused" mc G4
# G4 with a symbolic Commit: card/v3-ID~1 is re-evaluated at merge time, so it
# names tip^ -- the unreviewed late change -- and passes both the tip check and
# Commit = tip^. Only the hex-only rule can refuse it. ~0000001 is git's ~1
# spelled with seven hex digits, so only an anchored match refuses G6Z.
# symbolic ID COMMIT: verdict with Commit rewritten to COMMIT, then a late
# change, then the verdict re-committed with a new Date.
symbolic() {
  card "$1" S standard; branch "$1" "nucleus/v3/spikes/${1,,}.txt"; verdict "$1" S MERGE standard S
  sed -i "s|^Commit: .*|Commit: $2|" "$v3/verdicts/$1.md"; git -C "$r" commit -qam "nucleus v3: $1 verdict MERGE (symbolic Commit)"
  echo late >>"$v3/spikes/${1,,}.txt"; git -C "$r" commit -qam "nucleus v3: $1 late non-core change"
  sed -i 's/^Date: .*/Date: 2026-10-18/' "$v3/verdicts/$1.md"; git -C "$r" commit -qam "nucleus v3: $1 verdict MERGE (re-committed)"
}
# commit_is_tip_parent ID: the tip verdict's Commit resolves to card/v3-ID^.
commit_is_tip_parent() {
  local c; c=$(git -C "$r" show "card/v3-$1:nucleus/v3/verdicts/$1.md" | sed -n 's/^Commit: //p')
  [ "$(git -C "$r" rev-parse -q --verify "$c^{commit}")" = "$(git -C "$r" rev-parse "card/v3-$1^")" ]
}
symbolic G6 'card/v3-G6~1'
expect_ok "premise: G6's Commit card/v3-G6~1 resolves to tip^" commit_is_tip_parent G6
scratch; expect_no "a symbolic verdict Commit (card/v3-G6~1) re-committed over a late change is refused" mc G6
symbolic G6Z 'card/v3-G6Z~0000001'
expect_ok "premise: G6Z's Commit card/v3-G6Z~0000001 resolves to tip^" commit_is_tip_parent G6Z
scratch; expect_no "a verdict Commit with hex digits inside a revision (card/v3-G6Z~0000001) is refused" mc G6Z
# The full 40-hex id is accepted (the short-hex scenarios are the 7+ control).
card G6F S standard; branch G6F nucleus/v3/spikes/g6f.txt
printf 'Card: %s\nClass: %s\nCard-gate: %s\nVerdict: %s\nGate-tier: %s\nGate-model: test-model\nGate-session: t\nDate: 2026-10-17\nGate-exit: %s\nCommit: %s\nMutants: n/a\nFollowups: %s\n' \
  G6F standard S MERGE S "GATE EXIT=0" "$(git -C "$r" rev-parse HEAD)" none >"$v3/verdicts/G6F.md"
git -C "$r" add -A && git -C "$r" commit -qm "nucleus v3: G6F verdict MERGE"
scratch; expect_ok "a verdict with a full 40-hex Commit merges" mc G6F
# G4 again, with the verdict's Commit shortened, plus a branch named with that
# short id pointing at the late change. git resolves refs before short object
# ids, so a lookup of the field follows the branch to tip^. Merge mode and an
# unlisted audit refuse any short id (X-6-F1-F2-F3-F1-F1); the ref-free compare
# still guards the full form and the merges in SHORT_COMMIT_MERGES.
card G7 S standard; branch G7 nucleus/v3/spikes/g7.txt; verdict G7 S MERGE standard S
c7=$(git -C "$r" rev-parse --short "$(sed -n 's/^Commit: //p' "$v3/verdicts/G7.md")")
echo late >>"$v3/spikes/g7.txt"; git -C "$r" commit -qam "nucleus v3: G7 late non-core change"
sed -i -e 's/^Date: .*/Date: 2026-10-18/' -e "s/^Commit: .*/Commit: $c7/" "$v3/verdicts/G7.md"; git -C "$r" commit -qam "nucleus v3: G7 verdict MERGE (re-committed)"
g7ref=$c7
git -C "$r" branch "$g7ref" card/v3-G7^
g7_shadows() { [ "$(git -C "$r" rev-parse -q --verify "$c7^{commit}")" = "$(git -C "$r" rev-parse card/v3-G7^)" ]; }
expect_ok "premise: G7's Commit $c7 resolves through the ref to tip^" g7_shadows
scratch; expect_no "a branch named after the reviewed commit's prefix does not cover a late change (G7)" mc G7
git -C "$r" checkout -q -B scratch/g7 nucleus/v3-scaffold
git -C "$r" merge -q --no-ff -m "nucleus v3: merge card/v3-G7 (MERGE, gate S/test-model)" card/v3-G7
expect_no "audit rejects a titled merge whose short Commit a same-named branch redirects (G7)" bash -c "cd '$v3' && bash scripts/audit-merges.sh scratch/g7"
expect_ok "G7's hex-named branch is deleted" git -C "$r" branch -D "$g7ref"
# A ground prefix collision: X (reviewed) and Y (a late change) on the same
# parent share a 7-hex prefix, so the short Commit names both. A prefix compare
# alone would let Y reuse X's verdict. Birthday search: about 2^14 hashes a side.
card G7C S standard; branch G7C nucleus/v3/spikes/g7c.txt
g7tx=$(git -C "$r" rev-parse "HEAD^{tree}")
echo late >>"$v3/spikes/g7c.txt"; git -C "$r" commit -qam "nucleus v3: G7C late non-core change"
g7ty=$(git -C "$r" rev-parse "HEAD^{tree}")
# collide TREE_X TREE_Y PARENT: prints "X_BODY_NONCE Y_BODY_NONCE X Y HASHES" for
# two commit bodies whose ids share their first 7 hex digits.
collide() { PYTHONDONTWRITEBYTECODE=1 python3 - "$@" <<'P'
import hashlib, sys
tx, ty, parent = sys.argv[1:4]
def body(tree, side, n):
    return (f"tree {tree}\nparent {parent}\nauthor t <t@t> 1700000000 +0000\n"
            f"committer t <t@t> 1700000000 +0000\n\nG7C {side} {n}\n").encode()
def oid(b):
    return hashlib.sha1(b"commit %d\0" % len(b) + b).hexdigest()
seen = {"X": {}, "Y": {}}
for n in range(1 << 20):
    for side, tree, other in (("X", tx, "Y"), ("Y", ty, "X")):
        h = oid(body(tree, side, n))
        seen[side].setdefault(h[:7], (n, h))
        if h[:7] in seen[other]:
            m, g = seen[other][h[:7]]
            x, y = ((n, h), (m, g)) if side == "X" else ((m, g), (n, h))
            print(x[0], y[0], x[1], y[1], 2 * n + 1 + (side == "Y"))
            sys.exit(0)
print("G7C: no 7-hex collision in 2^20 nonces a side", file=sys.stderr)
sys.exit(1)
P
}
g7out=$(collide "$g7tx" "$g7ty" "$(git -C "$r" rev-parse nucleus/v3-scaffold)") || { fail "G7C: the collision search failed"; exit 1; }
read -r g7nx g7ny g7x g7y g7hashes <<<"$g7out"
echo "     G7C: a 7-hex collision after $g7hashes hashes (X nonce $g7nx, Y nonce $g7ny)"
g7body() { printf 'tree %s\nparent %s\nauthor t <t@t> 1700000000 +0000\ncommitter t <t@t> 1700000000 +0000\n\nG7C %s %s\n' "$1" "$(git -C "$r" rev-parse nucleus/v3-scaffold)" "$2" "$3"; }
[ "$(g7body "$g7tx" X "$g7nx" | git -C "$r" hash-object -t commit -w --stdin)" = "$g7x" ] || fail "G7C: git's id for X differs from the computed one"
[ "$(g7body "$g7ty" Y "$g7ny" | git -C "$r" hash-object -t commit -w --stdin)" = "$g7y" ] || fail "G7C: git's id for Y differs from the computed one"
g7c=${g7x:0:7}
g7c_verdict() {
  printf 'Card: %s\nClass: %s\nCard-gate: %s\nVerdict: %s\nGate-tier: %s\nGate-model: test-model\nGate-session: t\nDate: 2026-10-17\nGate-exit: %s\nCommit: %s\nMutants: n/a\nFollowups: %s\n' \
    "$1" standard S MERGE S "GATE EXIT=0" "$2" none >"$v3/verdicts/$1.md"
  git -C "$r" add -A && git -C "$r" commit -qm "nucleus v3: $1 verdict MERGE"
}
# The verdict as reviewed (on X), then the same file committed on Y.
git -C "$r" checkout -q -B card/v3-G7C "$g7x"; g7c_verdict G7C "$g7c"
git -C "$r" checkout -q -B card/v3-G7C "$g7y"; g7c_verdict G7C "$g7c"
g7c_premise() {
  [ "$(git -C "$r" cat-file -t "$g7x")" = commit ] && [ "$(git -C "$r" cat-file -t "$g7y")" = commit ] &&
    [ "$g7x" != "$g7y" ] && [[ $g7y == "$g7c"* ]] && [ "$(git -C "$r" rev-parse card/v3-G7C^)" = "$g7y" ]
}
expect_ok "premise: G7C's X and Y are distinct commits sharing the prefix" g7c_premise
scratch; expect_no "a verdict whose short Commit also names a ground collision is refused (G7C)" mc G7C
# Merge mode no longer reaches the ambiguity count (X-6-F1-F2-F3-F1-F1): any
# short id is refused first. The count itself is the L2 row's (audit, listed).
scratch; expect_ok "G7C's refusal names the full-id rule" bash -c "cd '$v3' && rc=0 && o=\$(bash scripts/merge-card.sh G7C 2>&1) || rc=\$?; echo \"\$o\"; [ \"\$rc\" -ne 0 ] && grep -qF \"verdict Commit '$g7c' is not a full 40-hex commit id\" <<<\"\$o\""
# Control: the same Y under a full 40-hex Commit is unambiguous and merges.
card G7D S standard
git -C "$r" checkout -q -B card/v3-G7D "$g7y"; g7c_verdict G7D "$g7y"
scratch; expect_ok "the same Y with a full 40-hex Commit merges (G7D)" mc G7D
# tip^'s own full id, spelled so git still resolves it to tip^ but the field is
# not plain lowercase hex: <full>~0 (G6E) and the id in uppercase (G6U). The
# text compare with tip^'s full id refuses both as well, so a refusal alone
# cannot show the hex rule is there. The reason pins it: with the end anchor
# dropped (^...{40}, no $) or A-F added to the class, the 40-hex rule passes
# the value and the refusal names the compare instead.
# hexrule_refusal ID VALUE: merge-card refuses ID, naming VALUE under the hex rule.
hexrule_refusal() { local o rc=0; o=$(mc "$1" 2>&1) || rc=$?; echo "$o"; [ "$rc" -ne 0 ] && grep -qF "verdict Commit '$2' is not a hex commit id" <<<"$o"; }
card G6E S standard; branch G6E nucleus/v3/spikes/g6e.txt
g6e="$(git -C "$r" rev-parse HEAD)~0"; g7c_verdict G6E "$g6e"
expect_ok "premise: G6E's Commit $g6e resolves to tip^" commit_is_tip_parent G6E
scratch; expect_no "a verdict Commit of tip^'s full id plus ~0 is refused (G6E)" mc G6E
scratch; expect_ok "G6E's refusal names the hex rule (end anchor)" hexrule_refusal G6E "$g6e"
# An id with no letter has no uppercase form, so G6U would not be about case.
# Add work commits until a letter falls in the first seven digits (about 1 id
# in 27 lacks one there), which more than ensures one in the full id.
card G6U S standard; branch G6U nucleus/v3/spikes/g6u.txt
g6un=0
while [[ $(git -C "$r" rev-parse HEAD | cut -c1-7) != *[abcdef]* ]] && [ "$g6un" -lt 64 ]; do
  echo "$g6un" >>"$v3/spikes/g6u.txt"; git -C "$r" commit -qam "nucleus v3: G6U work ($g6un)"; g6un=$((g6un + 1))
done
g6u=$(git -C "$r" rev-parse HEAD | tr abcdef ABCDEF); g7c_verdict G6U "$g6u"
g6u_premise() { [[ ${g6u:0:7} == *[ABCDEF]* ]] && commit_is_tip_parent G6U; }
expect_ok "premise: G6U's Commit $g6u has a letter in its first seven digits and resolves to tip^" g6u_premise
scratch; expect_no "a verdict Commit of tip^'s full id in uppercase is refused (G6U)" mc G6U
scratch; expect_ok "G6U's refusal names the hex rule (lowercase class)" hexrule_refusal G6U "$g6u"

card R S standard
git -C "$r" checkout -q -b card/v3-R nucleus/v3-scaffold
git -C "$r" mv nucleus/v3/scripts/promote-pass.sh nucleus/v3/spikes/p.sh && git -C "$r" commit -qm "nucleus v3: R move"
verdict R S MERGE standard S
scratch; expect_no "renaming a CORE file out of scripts/ is refused" mc R

card V S standard; branch V nucleus/v3/verdicts/README.md; verdict V S MERGE standard S
scratch; expect_no "S verdict on verdicts/README.md is refused" mc V
card W P core; branch W nucleus/v3/docs/README.md; verdict W P MERGE core P
scratch; expect_ok "P core card may update docs/README.md" mc W
card W2 S standard; branch W2 nucleus/v3/docs/README.md; verdict W2 S MERGE standard S
scratch; expect_no "S verdict on docs/README.md is refused" mc W2

card E2 S standard; branch E2 nucleus/v3/spikes/e2.txt; verdict E2 S MERGE standard S

# --- audit ------------------------------------------------------------------
git -C "$r" checkout -q -B scratch/pass2 nucleus/v3-scaffold
(cd "$v3" && bash scripts/merge-card.sh A && bash scripts/merge-card.sh C) >/dev/null
expect_ok "audit accepts card merges" bash -c "cd '$v3' && bash scripts/audit-merges.sh scratch/pass2"
git -C "$r" checkout -q -b scratch/direct scratch/pass2
echo x >>"$v3/spikes/base.txt"; git -C "$r" commit -qam "direct commit"
expect_no "audit rejects a direct commit" bash -c "cd '$v3' && bash scripts/audit-merges.sh scratch/direct"
git -C "$r" checkout -q -b scratch/plain scratch/pass2
git -C "$r" merge -q --no-ff -m "merge card/v3-B" card/v3-B
expect_no "audit rejects a merge not made by merge-card" bash -c "cd '$v3' && bash scripts/audit-merges.sh scratch/plain"
git -C "$r" checkout -q -b scratch/forged scratch/pass2
git -C "$r" merge -q --no-ff -m "nucleus v3: merge card/v3-B (MERGE, gate S/x)" card/v3-B
expect_no "audit re-checks rules on a correctly titled merge (B: S verdict on CORE)" bash -c "cd '$v3' && bash scripts/audit-merges.sh scratch/forged"
git -C "$r" checkout -q -b scratch/evil scratch/pass2
git -C "$r" merge -q --no-ff --no-commit card/v3-E2 >/dev/null 2>&1
echo evil >>"$v3/scripts/gate.sh"; git -C "$r" add -A
git -C "$r" commit -qm "nucleus v3: merge card/v3-E2 (MERGE, gate S/test-model)"
expect_no "audit rejects an edit made inside a correctly titled merge" bash -c "cd '$v3' && bash scripts/audit-merges.sh scratch/evil"
git -C "$r" checkout -q -b scratch/ci scratch/pass2
echo changed >>"$r/.forgejo/workflows/nucleus-v3.yml"; git -C "$r" commit -qam "ci tweak"
expect_no "audit rejects a direct commit to the v3 CI workflow" bash -c "cd '$v3' && bash scripts/audit-merges.sh scratch/ci"
git -C "$r" checkout -q -b scratch/rename scratch/pass2
git -C "$r" merge -q --no-ff -m "nucleus v3: merge card/v3-R (MERGE, gate S/test-model)" card/v3-R
expect_no "audit rejects a titled merge of a CORE rename with an S verdict" bash -c "cd '$v3' && bash scripts/audit-merges.sh scratch/rename"
git -C "$r" checkout -q -b scratch/recommit scratch/pass2
git -C "$r" merge -q --no-ff -m "nucleus v3: merge card/v3-G4 (MERGE, gate S/test-model)" card/v3-G4
expect_no "audit rejects a titled merge whose verdict Commit is not tip^ (G4)" bash -c "cd '$v3' && bash scripts/audit-merges.sh scratch/recommit"
# A bogus ref must fail, naming the failed rev-list: read through process
# substitution it was an empty history, "audit: ok (0 commits)", exit 0.
expect_no "audit: a bogus ref fails" bash -c "cd '$v3' && bash scripts/audit-merges.sh no-such-ref"
audit_refused() { local o rc=0; o=$(cd "$v3" && bash scripts/audit-merges.sh "$1" 2>&1) || rc=$?; echo "$o"; [ "$rc" -ne 0 ] && grep -qF "cannot list commits on $1" <<<"$o"; }
expect_ok "audit: a bogus ref names the failed rev-list" audit_refused no-such-ref
# Control: a real ref with no v3 commits is still ok, not a listing failure.
git -C "$r" branch -f scratch/prev3 "$(git -C "$r" rev-list --max-parents=0 nucleus/v3-scaffold)"
audit_zero() { local o; o=$(cd "$v3" && bash scripts/audit-merges.sh "$1" 2>&1) || { echo "$o"; return 1; }; echo "$o"; grep -qxF "audit: ok (0 commits)" <<<"$o"; }
expect_ok "audit: a ref with no v3 commits is ok (0 commits)" audit_zero scratch/prev3

# --- gate.sh ----------------------------------------------------------------
gate_out() { local rc=0; (cd "$v3" && FAKE_FAIL=${1:-none} bash scripts/gate.sh) >"$t/gate" 2>&1 || rc=$?; echo "$rc" >"$t/gate.rc"; }
gate_out clippy
if [ "$(cat "$t/gate.rc")" = 1 ] && [ "$(tail -n 1 "$t/gate")" = "GATE EXIT=1" ] && grep -qx "STEP clippy EXIT=1" "$t/gate" && grep -qx "STEP fmt EXIT=0" "$t/gate"; then
  pass "gate: a failing step gives GATE EXIT=1, exit 1, and later steps still run"
else fail "gate: failing step not reported"; sed 's/^/     /' "$t/gate"; fi
gate_out none
if [ "$(cat "$t/gate.rc")" = 0 ] && [ "$(tail -n 1 "$t/gate")" = "GATE EXIT=0" ]; then pass "gate: all green gives GATE EXIT=0"; else fail "gate: green run"; sed 's/^/     /' "$t/gate"; fi

# --- queue-check ------------------------------------------------------------
git -C "$r" checkout -q nucleus/v3-scaffold
q() { (cd "$v3" && bash scripts/queue-check.sh); }
qc=$t/qcards; mkdir -p "$qc"; printf '# QUEUE\n- Q1 | queued | S | x\n' >"$qc/QUEUE.local.md"
printf '# CARD Q1: t\nClass: test        Author: S (t)        Status: ready\n' >"$qc/Q1.local.md"
expect_ok "queue-check: a queued ready card is fine" env NV3_CARDS="$qc" bash -c "cd '$v3' && bash scripts/queue-check.sh"
printf '# CARD Q2: t\nClass: test        Author: S (t)        Status: ready\n' >"$qc/Q2.local.md"
expect_no "queue-check: a ready card missing from the queue is an orphan" env NV3_CARDS="$qc" bash -c "cd '$v3' && bash scripts/queue-check.sh"
printf -- '- Q2 | done | S | x\n' >>"$qc/QUEUE.local.md"
expect_no "queue-check: a queue line without a state word does not count" env NV3_CARDS="$qc" bash -c "cd '$v3' && bash scripts/queue-check.sh"

# --- runner card rules ------------------------------------------------------
py() { PYTHONDONTWRITEBYTECODE=1 python3 - "$v3/tools/run_card.py" "$@" <<'P'
import importlib.util, sys
spec = importlib.util.spec_from_file_location("rc", sys.argv[1]); rc = importlib.util.module_from_spec(spec); spec.loader.exec_module(rc)
case = sys.argv[2]
G = "cd nucleus/v3 && bash scripts/gate.sh"
checks = {
  "gate-last":   rc.validate_card(["a/"], ["cargo test", G]) is None,
  "no-gate":     rc.validate_card(["a/"], ["cargo test"]) is not None,
  "fake-gate":   rc.validate_card(["a/"], ["echo GATE EXIT=0 # scripts/gate.sh"]) is not None,
  "gate-middle": rc.validate_card(["a/"], [G, "cargo test"]) is not None,
  "no-touch":    rc.validate_card([], [G]) is not None,
  "pass-real":   rc.gate_passed(0, "STEP x EXIT=0\nGATE EXIT=0\n"),
  "pass-lies":   not rc.gate_passed(0, "test prints GATE EXIT=0\nSTEP test EXIT=1\nGATE EXIT=1\n"),
  "pass-rc":     not rc.gate_passed(1, "GATE EXIT=0\n"),
  # A note after the path makes the entry match nothing (X-6-F1-F3).
  "touch-note":  rc.validate_card(["nucleus/v3/bench/nbench/ (the runner's driver-error construction)"], [G])
                 == "Touch only entry 'nucleus/v3/bench/nbench/ (the runner's driver-error construction)' is not a bare path or glob (put notes under Limits:)",
  "touch-dir":   rc.validate_card(["nucleus/v3/spikes/e1-mvcc/tests/"], [G]) is None,
  "touch-glob":  rc.validate_card(["nucleus/v3/spikes/e1-mvcc/src/*.rs"], [G]) is None,
}
sys.exit(0 if checks[case] else 1)
P
}
for c in gate-last no-gate fake-gate gate-middle no-touch pass-real pass-lies pass-rc touch-note touch-dir touch-glob; do expect_ok "runner: $c" py "$c"; done
# ft CASE: failure_text on synthetic output (RUN-F1). A failing gate step's
# STEP line must reach the retry text even when later steps push it out of the
# 60-line tail; with none, the text is the old `$ cmd` plus tail.
ft() { PYTHONDONTWRITEBYTECODE=1 python3 - "$v3/tools/run_card.py" "$@" <<'P'
import importlib.util, sys
spec = importlib.util.spec_from_file_location("rc", sys.argv[1]); rc = importlib.util.module_from_spec(spec); spec.loader.exec_module(rc)
case = sys.argv[2]
def filler(n, at):
    lines = [f"filler {i}" for i in range(1, n + 1)]
    for i, l in at.items():
        lines[i - 1] = l
    return lines
H = "Failing steps (from the full output):"
if case == "STEP-EARLY":
    lines = filler(200, {3: "STEP test EXIT=0", 5: "STEP fmt EXIT=1", 7: "STEP clippy EXIT=101", 200: "GATE EXIT=1"})
    t = rc.failure_text("c", "\n".join(lines) + "\n")
    ok = (t.startswith(f"$ c\n{H}\nSTEP fmt EXIT=1\nSTEP clippy EXIT=101\n") and "STEP test EXIT=0" not in t
          and t.endswith("\n".join(lines[-60:])))
elif case == "STEP-NONE":
    lines = filler(100, {})
    t = rc.failure_text("c", "\n".join(lines) + "\n")
    ok = t == "$ c\n" + "\n".join(lines[-60:])
elif case == "STEP-INDENTED":
    lines = filler(100, {2: "     STEP fmt EXIT=1"})
    t = rc.failure_text("c", "\n".join(lines) + "\n")
    ok = H not in t
print(t)
sys.exit(0 if ok else 1)
P
}
expect_ok "runner: STEP-EARLY a failing STEP line outside the tail is listed, in order, EXIT=0 not" ft STEP-EARLY
expect_ok "runner: STEP-NONE with no failing STEP line the retry text is the old cmd plus tail" ft STEP-NONE
expect_ok "runner: STEP-INDENTED an indented (nested) STEP line is not listed" ft STEP-INDENTED
# bp WANT CMD: bash_permission on the copied runner's real bash map gives WANT.
# The deny list is a guard-rail (the scope check is the enforcement), but a
# worker should not spend attempts on a rewrite that fails its card anyway.
bp() { PYTHONDONTWRITEBYTECODE=1 python3 - "$v3/tools/run_card.py" "$@" <<'P'
import importlib.util, sys
spec = importlib.util.spec_from_file_location("rc", sys.argv[1]); rc = importlib.util.module_from_spec(spec); spec.loader.exec_module(rc)
got = rc.bash_permission(sys.argv[3])
print("got:", got, "expected:", sys.argv[2])
sys.exit(0 if got == sys.argv[2] else 1)
P
}
bash_deny=(
  'git rebase -i HEAD~2'
  'git commit --amend --no-edit'
  'git commit -q -a --amend'
  'git update-ref HEAD HEAD~1'
  'git switch -C card/v3-X HEAD~1'
  'git restore --staged x'
  'git -C . reset --hard'
  'git -c core.hooksPath=/dev/null checkout x'
  'git --git-dir=.git log'
  'cd nucleus/v3 && git reset --soft HEAD~1'
  'true; git rebase --abort'
  '/usr/bin/git reset HEAD~1'
  'GIT_INDEX_FILE=x git reset HEAD~1'
  'git replace --graft HEAD HEAD~2'
  'git update-index --skip-worktree nucleus/v3/verdicts/X.md'
  'git update-index --assume-unchanged x'
  'git config core.fsmonitor /tmp/x'
  'git branch -f card/v3-X HEAD~1'
  # a flag before -f: only the " * " separator forms of the branch patterns deny it
  'git branch -q -f card/v3-X HEAD~1'
  'git stash push'
  'git push origin HEAD'
  'rm -rf target'
  'git fetch . +HEAD:main'
  'git fetch --update-head-ok . HEAD:x'
  'git tag -f v1'
  'git tag v1 HEAD~1'
  'git reflog expire --all --expire=now'
  'git pull . main'
  'GIT_DIR=../../.git git commit -m x'
  'env GIT_WORK_TREE=/tmp git add x'
  'GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0=core.hooksPath GIT_CONFIG_VALUE_0=/dev/null git commit -m x'
  'git fast-import'
)
bash_allow=(
  'git status'
  'git status --porcelain'
  'git diff nucleus/v3-scaffold...HEAD --stat'
  'git log --oneline -5'
  'git add -A'
  'git commit -m "nucleus v3: X-1 replace the reset path"'
  'git show --stat HEAD'
  'git rev-parse HEAD'
  'git branch --show-current'
  'cd nucleus/v3 && cargo test -q -p e1-mvcc'
  'cd nucleus/v3 && bash scripts/gate.sh'
  'git log --oneline -3'
  'git diff --stat HEAD~1'
  'git show HEAD --stat'
  'git commit -m "fix the tag parser"'
  # the | is quoted, so this is one command; "|rm" is not "rm" after a space or "/"
  'git commit -m "a|rm -rf b"'
)
for c in "${bash_deny[@]}"; do expect_ok "runner: bash deny $c" bp deny "$c"; done
for c in "${bash_allow[@]}"; do expect_ok "runner: bash allow $c" bp allow "$c"; done
# A bare rm is denied only because a trailing " *" is optional ("rm *"). It is
# not in bash_deny, whose row name would be a prefix of the "rm -rf target" one.
expect_ok "runner: bash deny a bare rm" bp deny rm
git -C "$r" checkout -q -b card/v3-RS nucleus/v3-scaffold
git -C "$r" mv nucleus/v3/docs/README.md nucleus/v3/spikes/moved.md && git -C "$r" commit -qm "nucleus v3: RS move"
rs() { PYTHONDONTWRITEBYTECODE=1 python3 - "$v3/tools/run_card.py" "$r" <<'P'
import importlib.util, pathlib, sys
spec = importlib.util.spec_from_file_location("rc", sys.argv[1]); rc = importlib.util.module_from_spec(spec); spec.loader.exec_module(rc)
bad = rc.scope_violations(pathlib.Path(sys.argv[2]), "nucleus/v3-scaffold", ["nucleus/v3/spikes/"])
sys.exit(0 if any("nucleus/v3/docs/README.md" in b for b in bad) else 1)
P
}
expect_ok "runner: moving a protected file out of docs/ is a scope violation" rs
git -C "$r" checkout -q nucleus/v3-scaffold
# runner_protected BRANCH PATH: scope_violations flags PATH as protected (not merely
# "outside Touch only") on BRANCH, with the worktree as it stands.
runner_protected() { git -C "$r" checkout -q "$1" && PYTHONDONTWRITEBYTECODE=1 python3 - "$v3/tools/run_card.py" "$r" "$2" <<'P'
import importlib.util, pathlib, sys
spec = importlib.util.spec_from_file_location("rc", sys.argv[1]); rc = importlib.util.module_from_spec(spec); spec.loader.exec_module(rc)
bad = rc.scope_violations(pathlib.Path(sys.argv[2]), "nucleus/v3-scaffold", ["nucleus/v3/spikes/"])
print(bad)
sys.exit(0 if sys.argv[3] + " (protected)" in bad else 1)
P
}
expect_ok "runner: committed nucleus/v3/docs/é.md is protected" runner_protected card/v3-DU "nucleus/v3/docs/é.md"
expect_ok 'runner: committed nucleus/v3/docs/a"b.md is protected' runner_protected card/v3-DQ 'nucleus/v3/docs/a"b.md'
git -C "$r" checkout -q card/v3-AU && echo x >"$v3/tools/ü.py"
expect_ok "runner: untracked nucleus/v3/tools/ü.py is protected" runner_protected card/v3-AU "nucleus/v3/tools/ü.py"
rm -f "$v3/tools/ü.py"
git -C "$r" checkout -q nucleus/v3-scaffold
# rb BASE WANT: scope_violations on the clean scaffold checkout; WANT "git-fail"
# expects a violation naming the failed git listing, "none" expects nothing.
rb() { PYTHONDONTWRITEBYTECODE=1 python3 - "$v3/tools/run_card.py" "$r" "$@" <<'P'
import importlib.util, pathlib, sys
spec = importlib.util.spec_from_file_location("rc", sys.argv[1]); rc = importlib.util.module_from_spec(spec); spec.loader.exec_module(rc)
bad = rc.scope_violations(pathlib.Path(sys.argv[2]), sys.argv[3], ["nucleus/v3/spikes/"])
print(bad)
if sys.argv[4] == "none":
    sys.exit(0 if bad == [] else 1)
sys.exit(0 if any("git diff" in b and "failed (exit" in b and "cannot list changed files" in b for b in bad) else 1)
P
}
expect_ok "runner: a bogus base is a scope violation" rb no-such-ref git-fail
expect_ok "runner: the same call with base nucleus/v3-scaffold flags nothing" rb nucleus/v3-scaffold none
# rsc CARD BRANCH SINCE WANT: stray_commits with ROOT at the test repo; SINCE
# "now" is the current time. WANT "git-fail" expects an entry naming the failed
# listing, "none" expects [], anything else an entry containing WANT.
rsc() { PYTHONDONTWRITEBYTECODE=1 python3 - "$v3/tools/run_card.py" "$r" "$@" <<'P'
import importlib.util, pathlib, sys, time
spec = importlib.util.spec_from_file_location("rc", sys.argv[1]); rc = importlib.util.module_from_spec(spec); spec.loader.exec_module(rc)
rc.ROOT = pathlib.Path(sys.argv[2])
got = rc.stray_commits(sys.argv[3], sys.argv[4], time.time() if sys.argv[5] == "now" else float(sys.argv[5]))
print(got)
want = sys.argv[6]
if want == "none":
    sys.exit(0 if got == [] else 1)
if want == "git-fail":
    sys.exit(0 if got and all("git log" in g and "failed (exit" in g for g in got) else 1)
sys.exit(0 if any(want in g for g in got) else 1)
P
}
# ZZ is not in "no-such-branch", so git's "unknown revision" stderr cannot match
# the card id: before S-0-F2-F2-F1 this returned [] and the run went on.
expect_ok "runner: a failed stray-commit listing stops the run" rsc ZZ no-such-branch now git-fail
expect_ok "runner: a valid branch with no stray commits lists nothing" rsc RS card/v3-RS now none
zs0=$(date +%s)
git -C "$r" branch -f card/v3-ZS nucleus/v3-scaffold
git -C "$r" checkout -q -B scratch/zs nucleus/v3-scaffold
git -C "$r" commit -q --allow-empty -m "nucleus v3: ZS work"
git -C "$r" checkout -q nucleus/v3-scaffold
expect_ok "runner: a commit naming the card on another branch is stray" rsc ZS card/v3-ZS "$zs0" "nucleus v3: ZS work"
# ZO and ZB (S-0-F2-F2-F1-F2): stray_commits' two filters. Each premise lists the
# same commit with only the filter under test opened up, so the row's [] can come
# from that filter alone. The listing reads every branch, so the scratch branches
# are deleted afterwards.
# ZO: committed (committer and author date) an hour before the window opens, on a
# branch other than the card's. --since keeps it out (STRAY-SINCE-DROPPED lists it).
zo0=$(( $(date +%s) - 3600 ))
git -C "$r" branch -f card/v3-ZO nucleus/v3-scaffold
git -C "$r" checkout -q -B scratch/zo nucleus/v3-scaffold
GIT_COMMITTER_DATE="@$zo0 +0000" GIT_AUTHOR_DATE="@$zo0 +0000" git -C "$r" commit -q --allow-empty -m "nucleus v3: ZO work"
git -C "$r" checkout -q nucleus/v3-scaffold
expect_ok "runner: premise: ZO's early commit is listed by a window opened before it" rsc ZO card/v3-ZO "$((zo0 - 60))" "nucleus v3: ZO work"
expect_ok "runner: a commit naming the card dated before the window is not stray" rsc ZO card/v3-ZO "$((zo0 + 60))" none
# ZB: inside the window, on the card's own branch: the worker's own work. --not
# BRANCH keeps it out (dropping `--not <branch>` lists it).
zb0=$(date +%s)
git -C "$r" checkout -q -B card/v3-ZB nucleus/v3-scaffold
git -C "$r" commit -q --allow-empty -m "nucleus v3: ZB work"
git -C "$r" checkout -q nucleus/v3-scaffold
expect_ok "runner: premise: ZB's commit is listed against another branch in the same window" rsc ZB card/v3-ZO "$zb0" "nucleus v3: ZB work"
expect_ok "runner: a commit naming the card on its own branch is not stray" rsc ZB card/v3-ZB "$zb0" none
git -C "$r" branch -q -D scratch/zo card/v3-ZO card/v3-ZB

# --- promote-pass -----------------------------------------------------------
git -C "$r" checkout -q -B scratch/pass3 nucleus/v3-scaffold
(cd "$v3" && bash scripts/merge-card.sh A) >/dev/null
qa=$t/qa; mkdir -p "$qa"; printf '# QUEUE\n' >"$qa/QUEUE.local.md"
promote() { (cd "$v3" && NV3_CARDS="$qa" FAKE_FAIL=${1:-none} bash scripts/promote-pass.sh scratch/pass3); }
expect_no "promote: a red gate is not promoted" promote clippy
echo dirty >>"$v3/spikes/base.txt"
expect_no "promote: a dirty worktree is not promoted" promote
git -C "$r" checkout -q -- nucleus/v3/spikes/base.txt
expect_ok "promote: green pass is pushed" promote
[ "$(git -C "$t/origin.git" rev-parse nucleus/v3-scaffold)" = "$(git -C "$r" rev-parse scratch/pass3)" ] && pass "promote: origin is the pass tip" || fail "promote: origin not updated"
git -C "$r" checkout -q -B scratch/pass4 card/v3-BOOT~1
(cd "$v3" && bash scripts/merge-card.sh C) >/dev/null 2>&1 || true
promote4() { (cd "$v3" && NV3_CARDS="$qa" bash scripts/promote-pass.sh scratch/pass4); }
expect_no "promote: a non-fast-forward is refused" promote4

# --- runner: the gate's verdict commit on a card branch (X-6-F1) ------------
# A gate commits verdicts/<ID>.md on the card branch between rework rounds; the
# runner must not report it as the worker's, or the worker reverts it (X-6 N1).
git -C "$r" checkout -q -B card/v3-RV nucleus/v3-scaffold
echo rv >"$v3/spikes/rv.txt" && git -C "$r" add -A && git -C "$r" commit -qm "nucleus v3: RV work"
verdict RV P REWORK core P
gate=$(git -C "$r" rev-parse HEAD)
# rv CARD PRE_HEAD FLAGGED...: scope_violations must flag exactly FLAGGED ("none" for nothing).
rv() { PYTHONDONTWRITEBYTECODE=1 python3 - "$v3/tools/run_card.py" "$r" "$@" <<'P'
import importlib.util, pathlib, sys
spec = importlib.util.spec_from_file_location("rc", sys.argv[1]); rc = importlib.util.module_from_spec(spec); spec.loader.exec_module(rc)
bad = rc.scope_violations(pathlib.Path(sys.argv[2]), "nucleus/v3-scaffold", ["nucleus/v3/spikes/"], sys.argv[3], sys.argv[4])
got = sorted(b.split(" (")[0] for b in bad)
want = sorted(w for w in sys.argv[5:] if w != "none")
print("flagged:", got, "expected:", want)
sys.exit(0 if got == want else 1)
P
}
vf=nucleus/v3/verdicts/RV.md
# rvfrom SHA: a fresh worker round starting at SHA.
rvfrom() { git -C "$r" checkout -q -B rvtmp "$1"; }
rvfrom "$gate"; echo more >>"$v3/spikes/rv.txt"; git -C "$r" commit -qam "nucleus v3: RV rework"
expect_ok "runner: the gate's REWORK verdict on the card branch is not a violation" rv RV "$gate" none
expect_ok "runner: another card's verdict file is a violation" rv OTHER "$gate" "$vf"
rvfrom "$gate"; echo edit >>"$r/$vf"
expect_ok "runner: an uncommitted worker edit of the verdict is a violation" rv RV "$gate" "$vf"
git -C "$r" checkout -q -- "$vf"
rvfrom "$gate"; echo edit >>"$r/$vf"; git -C "$r" commit -qam "nucleus v3: RV rework"
expect_ok "runner: a committed worker edit of the verdict is a violation" rv RV "$gate" "$vf"
rvfrom "$gate"; echo forged >>"$r/$vf"; git -C "$r" commit -qam "nucleus v3: RV verdict MERGE"
expect_ok "runner: a worker commit forging the verdict subject is a violation" rv RV "$gate" "$vf"
rvfrom "$gate"; git -C "$r" rm -q "$vf"; git -C "$r" commit -qm "nucleus v3: RV verdict gone"
expect_ok "runner: a committed worker deletion of the gate verdict is a violation" rv RV "$gate" "$vf"
rvfrom "$gate"; git -C "$r" rm -q "$vf"; git -C "$r" commit -q --amend --allow-empty --no-edit
expect_ok "runner: an amend that drops the gate's verdict is a violation" rv RV "$gate" "$vf"
rvfrom "$gate"; echo more >>"$v3/spikes/rv.txt"; git -C "$r" commit -qam "nucleus v3: RV rework"; git -C "$r" rebase -q --onto "$gate~1" "$gate"
expect_ok "runner: a rebase that drops the gate's verdict commit is a violation" rv RV "$gate" "$vf"
rvfrom "$gate"; echo more >>"$v3/spikes/rv.txt"; git -C "$r" commit -qa --amend --no-edit
expect_ok "runner: amending the gate's commit but keeping the verdict is not a violation" rv RV "$gate" none
rvfrom "$gate"; git -C "$r" rm -q "$vf"; git -C "$r" commit -qm "nucleus v3: RV verdict withdrawn"; pre=$(git -C "$r" rev-parse HEAD); echo forged >"$r/$vf"
expect_ok "runner: an untracked verdict after the gate removed it is a violation" rv RV "$pre" "$vf"
rm -f "$r/$vf"
rvfrom "$gate"; echo other >"$r/$vf"; git -C "$r" commit -qam "nucleus v3: OTHER verdict MERGE"
expect_ok "runner: a verdict last changed by another card's verdict commit is a violation" rv RV "$(git -C "$r" rev-parse HEAD)" "$vf"
rvfrom "$gate"; echo prefix >"$r/$vf"; git -C "$r" commit -qam "nucleus v3: RV-F1 verdict MERGE"
expect_ok "runner: a verdict commit for a longer card ID does not vouch for this card's verdict" rv RV "$(git -C "$r" rev-parse HEAD)" "$vf"
rvfrom "$gate"; echo reverted >"$r/$vf"; git -C "$r" commit -qam "nucleus v3: RV revert protected file"
expect_ok "runner: a verdict last changed by a worker before this run is a violation" rv RV "$(git -C "$r" rev-parse HEAD)" "$vf"
rvfrom "$gate"; echo x >"$v3/tools/extra.py"; git -C "$r" add -A; git -C "$r" commit -qm "nucleus v3: RV tools"
expect_ok "runner: a worker edit to another protected path is still a violation" rv RV "$gate" nucleus/v3/tools/extra.py
rvfrom "$gate~1"; echo o >"$v3/verdicts/OTHER.md"; echo f >"$v3/verdicts/RV-F1.md"; echo y >"$v3/tools/extra2.py"; echo g >"$r/$vf"
git -C "$r" add -A; git -C "$r" commit -qm "nucleus v3: RV verdict REWORK"
expect_ok "runner: a verdict commit exempts only the card's own verdict file" rv RV "$(git -C "$r" rev-parse HEAD)" nucleus/v3/tools/extra2.py nucleus/v3/verdicts/OTHER.md nucleus/v3/verdicts/RV-F1.md
# A non-verdict commit between the gate's verdict commit and pre_head, the verdict
# untouched: the log must follow the file, or the commit on top hides the gate's
# (X-6-F1 F5; LOG-NO-PATH would report a valid verdict as protected, the X-6 N1 symptom).
rvfrom "$gate"; echo more >>"$v3/spikes/rv.txt"; git -C "$r" commit -qam "nucleus v3: RV rework"; pre=$(git -C "$r" rev-parse HEAD)
expect_ok "runner: a non-verdict commit after the gate's verdict, before this run, is not a violation" rv RV "$pre" none
git -C "$r" checkout -q nucleus/v3-scaffold

# --- git state that hides changes (X-6-F1-F2) -------------------------------
# Replace refs, grafts and an fsmonitor hook rewrite what git reports; index
# flags hide worktree edits. The runner and the scripts run git with the first
# three off (neutralise: the hidden file is still listed) and name all four
# (refuse). All of it is shared by every branch of $r, so each case removes its
# state and checks that it is gone (no_state).
# plain ARGS...: git in $r with none of the protection, for the premises.
plain() { env -u GIT_NO_REPLACE_OBJECTS -u GIT_GRAFT_FILE -u GIT_CONFIG_COUNT git -C "$r" "$@"; }
no_state() { [ -z "$(git -C "$r" for-each-ref refs/replace/)" ] && [ ! -e "$r/.git/info/grafts" ] && ! git -C "$r" config core.fsmonitor && [ -z "$(git -C "$r" ls-files -v | grep -v '^H ')" ]; }
expect_ok "git state: none in the test repository before these scenarios" no_state
# gs BASE HIDDEN STATE [CARD PRE_HEAD]: scope_violations on $r as it stands lists
# HIDDEN as protected ("-" skips that half) and a "(git state hides changes)"
# line containing STATE ("-" skips that half, "none" requires no such line).
gs() { PYTHONDONTWRITEBYTECODE=1 python3 - "$v3/tools/run_card.py" "$r" "$@" <<'P'
import importlib.util, pathlib, sys
spec = importlib.util.spec_from_file_location("rc", sys.argv[1]); rc = importlib.util.module_from_spec(spec); spec.loader.exec_module(rc)
base, hidden, state = sys.argv[3:6]
bad = rc.scope_violations(pathlib.Path(sys.argv[2]), base, ["nucleus/v3/spikes/"], *sys.argv[6:8])
print("\n".join(bad) or "(no violations)")
st = [b for b in bad if b.endswith(" (git state hides changes)")]
ok = hidden == "-" or hidden + " (protected)" in bad
ok = ok and (state == "-" or (not st if state == "none" else any(state in b for b in st)))
sys.exit(0 if ok else 1)
P
}
# out_has WANT... -- CMD...: CMD exits non-zero and its output holds every WANT.
out_has() { local w=() o rc=0; while [ "$1" != -- ]; do w+=("$1"); shift; done; shift; o=$("$@" 2>&1) || rc=$?; echo "$o"; [ "$rc" -ne 0 ] || return 1; for x in "${w[@]}"; do grep -qF -- "$x" <<<"$o" || { echo "missing: $x"; return 1; }; done; }
# gshim WORD CMD...: CMD with a git that fails every call one of whose
# arguments is exactly WORD ("fatal: gshim refused WORD", exit 128) and runs the
# real git otherwise (X-6-F1-F2-F1). The refusals' fail-closed lines need a git
# that fails on demand, and no repository state makes for-each-ref or ls-files
# fail. The shim PATH and GSHIM_FAIL are set for CMD alone and never exported,
# so every other scenario runs the real git. CMD may be a function (gs, out_has,
# a python heredoc helper): bash passes a function's prefix assignments to the
# function's children and drops them when it returns. Wrap only the command
# under test, as in `out_has LINE -- gshim WORD bash -c ...`.
gshim_real=$(command -v git)
mkdir -p "$t/gshim"
{ echo '#!/usr/bin/env bash'
  echo 'for a in "$@"; do [ -n "${GSHIM_FAIL:-}" ] && [ "$a" = "$GSHIM_FAIL" ] && { echo "fatal: gshim refused $a" >&2; exit 128; }; done'
  printf 'exec %q "$@"\n' "$gshim_real"; } >"$t/gshim/git"
chmod +x "$t/gshim/git"
gshim() { local w=$1; shift; PATH="$t/gshim:$PATH" GSHIM_FAIL=$w "$@"; }
gshim_refuses() { local o rc=0; o=$(gshim rev-parse git -C "$r" rev-parse HEAD 2>&1) || rc=$?; echo "$o"; [ "$rc" -eq 128 ] && [ "$o" = "fatal: gshim refused rev-parse" ]; }
expect_ok "premise: gshim: a call with no argument equal to GSHIM_FAIL runs the real git" gshim no-such-word git -C "$r" rev-parse -q --verify nucleus/v3-scaffold
expect_ok "premise: gshim: a call with an argument equal to GSHIM_FAIL exits 128, naming it" gshim_refuses
expect_ok "premise: gshim: the next plain git call is the real git again" bash -c "[ \"\$(command -v git)\" = '$gshim_real' ] && [ -z \"\${GSHIM_FAIL:-}\" ]"

# One definition per language, kept identical; a caller's GIT_CONFIG_* entries
# are appended to, never clobbered, and a nested source adds nothing.
env_same() { local sh py; sh=$(cd "$v3" && env -i PATH="$PATH" bash -c '. scripts/git-env.sh; env' | grep '^GIT_' | sort)
  py=$(env -i PATH="$PATH" PYTHONDONTWRITEBYTECODE=1 python3 -c 'import importlib.util,sys; s=importlib.util.spec_from_file_location("rc",sys.argv[1]); rc=importlib.util.module_from_spec(s); s.loader.exec_module(rc); [print(f"{k}={v}") for k,v in sorted(rc.git_env({}).items()) if k.startswith("GIT_")]' "$v3/tools/run_card.py" | sort)
  echo "sh: $sh"; echo "py: $py"; [ -n "$sh" ] && [ "$sh" = "$py" ]; }
expect_ok "git env: git-env.sh and run_card.py's GIT_ENV set the same variables" env_same
cfg_sh() { (cd "$v3" && GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0=nv3.probe GIT_CONFIG_VALUE_0=kept bash -c '. scripts/git-env.sh; . scripts/git-env.sh; echo "count $GIT_CONFIG_COUNT"; [ "$GIT_CONFIG_COUNT" = 8 ] && [ "$(git config nv3.probe)" = kept ] && [ "$(git config core.fsmonitor)" = false ]'); }
expect_ok "git env: git-env.sh appends to a caller's GIT_CONFIG_COUNT entries" cfg_sh
cfg_py() { PYTHONDONTWRITEBYTECODE=1 python3 - "$v3/tools/run_card.py" "$r" <<'P'
import importlib.util, os, pathlib, sys
spec = importlib.util.spec_from_file_location("rc", sys.argv[1]); rc = importlib.util.module_from_spec(spec); spec.loader.exec_module(rc)
caller = dict(os.environ, GIT_CONFIG_COUNT="1", GIT_CONFIG_KEY_0="nv3.probe", GIT_CONFIG_VALUE_0="kept")
env = rc.git_env(caller)
got = [rc.run(["git", "config", k], sys.argv[2], env=caller)[1].strip() for k in ("nv3.probe", "core.fsmonitor")]
print(got, env["GIT_CONFIG_COUNT"])
sys.exit(0 if got == ["kept", "false"] and env["GIT_CONFIG_COUNT"] == "8" and rc.git_env(env) == env else 1)
P
}
expect_ok "git env: run_card.py appends to a caller's GIT_CONFIG_COUNT entries" cfg_py

# A card that would merge, made before any state exists.
card GM S standard; branch GM nucleus/v3/spikes/gm.txt; verdict GM S MERGE standard S
# R1 replace: a committed tools/ edit X, replaced by Y (same parent, the
# parent's tree); the index and worktree follow the replaced HEAD.
git -C "$r" checkout -q -B card/v3-GR1 nucleus/v3-scaffold
echo x >"$v3/tools/gr1.py"; git -C "$r" add -A; git -C "$r" commit -qm "nucleus v3: GR1 work"
gx=$(git -C "$r" rev-parse HEAD)
git -C "$r" replace "$gx" "$(git -C "$r" commit-tree "$gx^^{tree}" -p "$gx^" -m "nucleus v3: GR1 work")"
plain reset -q; rm -f "$v3/tools/gr1.py"
r1_hidden() { [ -z "$(plain diff --name-only nucleus/v3-scaffold...HEAD)" ] && [ -z "$(plain status --porcelain)" ]; }
expect_ok "premise: R1 replace: plain diff and status list nothing" r1_hidden
expect_ok "runner: R1 replace: the committed tools/ file is still protected" gs nucleus/v3-scaffold nucleus/v3/tools/gr1.py -
expect_ok "runner: R1 replace: the replace ref is named" gs nucleus/v3-scaffold - "replace ref refs/replace/$gx"
scratch
expect_ok "merge-card: refused while a replace ref exists, naming refs/replace" out_has "refused:" "refs/replace/$gx" -- mc GM
expect_ok "audit: fails while a replace ref exists" out_has "audit: replace ref refs/replace/$gx" -- bash -c "cd '$v3' && bash scripts/audit-merges.sh scratch/pass2"
git -C "$r" replace -d "$gx" >/dev/null
expect_ok "git state: R1's replace ref is gone" no_state
scratch; expect_ok "merge-card: control: GM merges once the replace ref is gone" mc GM
# The S-0-F2-F1-F1-F1-F1 case: G7C's reviewed X replaced by a blob, so a
# replace-honouring cat-file sees one commit for the prefix, not two. Merge
# mode now refuses the short id before any count (X-6-F1-F2-F3-F1-F1), and an
# audit fails outright while a replace ref exists (git_state_refuse).
git -C "$r" replace -f "$g7x" "$(echo blob | git -C "$r" hash-object -w --stdin)"
expect_ok "premise: G7C: plain cat-file reports X as a blob" bash -c "[ \"\$(env -u GIT_NO_REPLACE_OBJECTS git -C '$r' cat-file -t $g7x)\" = blob ]"
scratch; expect_ok "merge-card: G7C under a replace ref is still refused (short id)" out_has "verdict Commit '$g7c' is not a full 40-hex commit id" "refs/replace/$g7x" -- mc G7C
git -C "$r" replace -d "$g7x" >/dev/null
expect_ok "git state: G7C's replace ref is gone" no_state

# R2 replace-graft-base: the base tip gr2base gains the card's commit X as a
# parent, so base...HEAD's merge-base moves to X and hides X's change. (The
# card branches from gr2base's parent: X under gr2base itself would be a cycle.)
git -C "$r" checkout -q -B gr2base nucleus/v3-scaffold
git -C "$r" commit -q --allow-empty -m "nucleus v3: GR2 base tip"
git -C "$r" checkout -q -B card/v3-GR2 nucleus/v3-scaffold
echo x >"$v3/tools/gr2.py"; git -C "$r" add -A; git -C "$r" commit -qm "nucleus v3: GR2 work"
gx2=$(git -C "$r" rev-parse HEAD) gb2=$(git -C "$r" rev-parse gr2base)
git -C "$r" replace --graft "$gb2" "$gb2^" "$gx2"
r2_hidden() { [ -z "$(plain diff --name-only gr2base...HEAD 2>/dev/null)" ] && [ -z "$(plain status --porcelain)" ]; }
expect_ok "premise: R2 replace --graft: plain diff and status list nothing" r2_hidden
expect_ok "runner: R2 replace --graft: the committed tools/ file is still protected" gs gr2base nucleus/v3/tools/gr2.py -
expect_ok "runner: R2 replace --graft: the replace ref is named" gs gr2base - "replace ref refs/replace/$gb2"
expect_ok "check-core-paths: under R2 it still lists the hidden CORE file" bash -c "cd '$v3' && bash scripts/check-core-paths.sh gr2base card/v3-GR2 2>/dev/null | grep -qxF 'CORE tools/gr2.py (tools/)'"
expect_ok "check-core-paths: under R2 it exits non-zero, naming the replace ref" out_has "replace ref refs/replace/$gb2 (git state hides changes)" -- bash -c "cd '$v3' && bash scripts/check-core-paths.sh gr2base card/v3-GR2"
git -C "$r" replace -d "$gb2" >/dev/null
expect_ok "git state: R2's replace ref is gone" no_state

# R3 grafts: R2 through info/grafts, which GIT_NO_REPLACE_OBJECTS does not disable.
echo "$gb2 $(git -C "$r" rev-parse "$gb2^") $gx2" >"$r/.git/info/grafts"
r3_hidden() { [ -z "$(plain diff --name-only gr2base...HEAD 2>/dev/null)" ] && [ -z "$(GIT_NO_REPLACE_OBJECTS=1 plain diff --name-only gr2base...HEAD 2>/dev/null)" ]; }
expect_ok "premise: R3 grafts: plain diff lists nothing, with GIT_NO_REPLACE_OBJECTS=1 too" r3_hidden
expect_ok "runner: R3 grafts: the committed tools/ file is still protected" gs gr2base nucleus/v3/tools/gr2.py -
expect_ok "runner: R3 grafts: the graft file is named" gs gr2base - "graft file $r/.git/info/grafts"
expect_ok "check-core-paths: under R3 it still lists the hidden CORE file" bash -c "cd '$v3' && bash scripts/check-core-paths.sh gr2base card/v3-GR2 2>/dev/null | grep -qxF 'CORE tools/gr2.py (tools/)'"
expect_ok "audit: fails while a graft file exists" out_has "audit: graft file $r/.git/info/grafts" -- bash -c "cd '$v3' && bash scripts/audit-merges.sh scratch/pass2"
rm -f "$r/.git/info/grafts"
expect_ok "git state: R3's graft file is gone" no_state

# R4/R5: an uncommitted edit of the gate's verdict, hidden by an index flag, which
# no environment neutralises: only the refusal stops it.
rvfrom "$gate"; echo edit >>"$r/$vf"; git -C "$r" update-index --skip-worktree "$vf"
expect_ok "premise: R4 skip-worktree: plain status lists nothing" bash -c "[ -z \"\$(git -C '$r' status --porcelain)\" ]"
expect_ok "runner: R4 skip-worktree: the flagged verdict file is named" gs nucleus/v3-scaffold - "$vf is marked skip-worktree in the index" RV "$gate"
git -C "$r" update-index --no-skip-worktree "$vf"; git -C "$r" checkout -q -- "$vf"
expect_ok "git state: R4's flag is gone" no_state
rvfrom "$gate"; echo edit >>"$r/$vf"; git -C "$r" update-index --assume-unchanged "$vf"
expect_ok "premise: R5 assume-unchanged: plain status lists nothing" bash -c "[ -z \"\$(git -C '$r' status --porcelain)\" ]"
expect_ok "runner: R5 assume-unchanged: the flagged verdict file is named" gs nucleus/v3-scaffold - "$vf is marked assume-unchanged in the index" RV "$gate"
git -C "$r" update-index --no-assume-unchanged "$vf"; git -C "$r" checkout -q -- "$vf"
expect_ok "git state: R5's flag is gone" no_state

# R6 fsmonitor: a hook giving the protocol-v2 empty reply ("tok\0") hides an
# uncommitted tools/ edit. The premise runs first: a status with fsmonitor off
# rewrites the index, after which the hook no longer hides anything.
git -C "$r" checkout -q nucleus/v3-scaffold
printf '#!/bin/sh\nprintf "tok\\0"\n' >"$t/fsm"; chmod +x "$t/fsm"
git -C "$r" config core.fsmonitor "$t/fsm"; plain status --porcelain >/dev/null
echo "# R6 edit" >>"$v3/tools/run_card.py"
expect_ok "premise: R6 fsmonitor: plain status lists nothing" bash -c "[ -z \"\$(git -C '$r' status --porcelain)\" ]"
expect_ok "runner: R6 fsmonitor: the uncommitted tools/ edit is still protected" gs nucleus/v3-scaffold nucleus/v3/tools/run_card.py -
expect_ok "runner: R6 fsmonitor: the hook is named" gs nucleus/v3-scaffold - "core.fsmonitor is '$t/fsm'"
git -C "$r" config --unset core.fsmonitor; git -C "$r" checkout -q -- nucleus/v3/tools/run_card.py
expect_ok "git state: R6's hook is gone" no_state
expect_ok "runner: control: a clean branch names no git state" gs nucleus/v3-scaffold - none

# GIT_CONFIG_PARAMETERS (X-6-F1-F2-F2): a caller's -c, inherited by children,
# applies after the GIT_CONFIG_COUNT entries on this git, so an inherited
# -c core.fsmonitor=<hook> beats the environment's core.fsmonitor=false. The
# refusal is its only layer, by decision: its read keeps the variable and names
# the hook at command-line scope. $xcp is set for the one command under test,
# never exported (XP below exports it around one run); $t/fsm is R6's hook.
xcp="'core.fsmonitor'='$t/fsm'"
cp_beats() { local v; v=$(GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0=core.fsmonitor GIT_CONFIG_VALUE_0=false GIT_CONFIG_PARAMETERS=$xcp git -C "$r" config core.fsmonitor); echo "$v"; [ "$v" = "$t/fsm" ]; }
expect_ok "premise: GIT_CONFIG_PARAMETERS beats GIT_CONFIG_COUNT on this git" cp_beats
cp_refuse() { local o rc=0; o=$(cd "$v3" && GIT_CONFIG_PARAMETERS=$xcp bash -c '. scripts/git-env.sh; git_state_refuse' 2>&1) || rc=$?; echo "$o"; echo "exit $rc"; [ "$rc" -eq 1 ] && grep -qF -- "core.fsmonitor is '$t/fsm' (command line:" <<<"$o"; }
expect_ok "git-env.sh: an inherited -c core.fsmonitor hook is refused, at command-line scope" cp_refuse
cp_audit() { (cd "$v3" && GIT_CONFIG_PARAMETERS=$xcp bash scripts/audit-merges.sh nucleus/v3-scaffold); }
expect_ok "audit: fails on an inherited -c core.fsmonitor hook" out_has "audit: core.fsmonitor is '$t/fsm'" -- cp_audit
cp_py() { GIT_CONFIG_PARAMETERS=$xcp PYTHONDONTWRITEBYTECODE=1 python3 - "$v3/tools/run_card.py" "$r" "$t/fsm" <<'P'
import importlib.util, os, pathlib, sys
spec = importlib.util.spec_from_file_location("rc", sys.argv[1]); rc = importlib.util.module_from_spec(spec); spec.loader.exec_module(rc)
probs = rc.git_state_problems(pathlib.Path(sys.argv[2]))
print(os.environ.get("GIT_CONFIG_PARAMETERS"), probs)
sys.exit(0 if any(f"core.fsmonitor is '{sys.argv[3]}' (command line:" in p for p in probs) else 1)
P
}
expect_ok "runner: an inherited -c core.fsmonitor hook is named, at command-line scope" cp_py

# GIT_REPLACE_REF_BASE (X-6-F1-F2-F2): R1 with the replacement under another
# base, refs/alt/, which the refusal (refs/replace/) does not list. The
# environment is its only layer, by decision: GIT_NO_REPLACE_OBJECTS disables
# replacement whatever the base, so the hidden file is still listed. The
# variable is set per command; without it refs/alt/ does nothing.
git -C "$r" checkout -q -B card/v3-GRB nucleus/v3-scaffold
echo x >"$v3/tools/grb.py"; git -C "$r" add -A; git -C "$r" commit -qm "nucleus v3: GRB work"
grbx=$(git -C "$r" rev-parse HEAD)
git -C "$r" update-ref "refs/alt/$grbx" "$(git -C "$r" commit-tree "$grbx^^{tree}" -p "$grbx^" -m "nucleus v3: GRB work")"
GIT_REPLACE_REF_BASE=refs/alt/ plain reset -q; rm -f "$v3/tools/grb.py"
grb_hidden() { local d; d=$(GIT_REPLACE_REF_BASE=refs/alt/ plain diff --name-only nucleus/v3-scaffold...card/v3-GRB); echo "$d"; [ -z "$d" ]; }
expect_ok "premise: GRB: plain diff under GIT_REPLACE_REF_BASE=refs/alt/ lists nothing" grb_hidden
grb_inert() { local d; d=$(plain diff --name-only nucleus/v3-scaffold...card/v3-GRB); echo "$d"; [ "$d" = nucleus/v3/tools/grb.py ]; }
expect_ok "premise: GRB: plain diff without the variable lists the file" grb_inert
expect_ok "check-core-paths: under GIT_REPLACE_REF_BASE=refs/alt/ it still lists the hidden CORE file" bash -c "cd '$v3' && GIT_REPLACE_REF_BASE=refs/alt/ bash scripts/check-core-paths.sh nucleus/v3-scaffold card/v3-GRB 2>/dev/null | grep -qxF 'CORE tools/grb.py (tools/)'"
grb_gs() { GIT_REPLACE_REF_BASE=refs/alt/ gs nucleus/v3-scaffold nucleus/v3/tools/grb.py -; }
expect_ok "runner: under GIT_REPLACE_REF_BASE=refs/alt/ the committed tools/ file is still protected" grb_gs
git -C "$r" update-ref -d "refs/alt/$grbx"
git -C "$r" checkout -q nucleus/v3-scaffold
expect_ok "git state: GRB's refs/alt/ ref is gone" bash -c "[ -z \"\$(git -C '$r' for-each-ref refs/alt/)\" ]"
expect_ok "git state: none after GRB" no_state

# promote-pass reads its worktree: a skip-worktree-hidden dirty file stops the
# push, after the gate, queue-check and the audit have run.
git -C "$r" checkout -q -B scratch/pass5 nucleus/v3-scaffold
(cd "$v3" && bash scripts/merge-card.sh E2) >/dev/null
o5=$(git -C "$t/origin.git" rev-parse nucleus/v3-scaffold)
echo dirty >>"$v3/spikes/base.txt"; git -C "$r" update-index --skip-worktree nucleus/v3/spikes/base.txt
expect_ok "premise: promote: plain status lists nothing" bash -c "[ -z \"\$(git -C '$r' status --porcelain)\" ]"
promote5() { (cd "$v3" && NV3_CARDS="$qa" bash scripts/promote-pass.sh scratch/pass5); }
expect_ok "promote: a skip-worktree-hidden dirty file is refused after the checks ran" out_has "nucleus/v3/spikes/base.txt is marked skip-worktree" "GATE EXIT=0" "audit: ok" "not promoting" -- promote5
expect_ok "promote: origin did not move" bash -c "[ \"\$(git -C '$t/origin.git' rev-parse nucleus/v3-scaffold)\" = '$o5' ]"
git -C "$r" update-index --no-skip-worktree nucleus/v3/spikes/base.txt; git -C "$r" checkout -q -- nucleus/v3/spikes/base.txt
expect_ok "git state: the promote scenario's flag is gone" no_state
git -C "$r" checkout -q nucleus/v3-scaffold

# The refusals fail closed (X-6-F1-F2-F1 a): a git read they cannot make is a
# problem line, never "nothing found". On a clean $r, gshim fails one read.
expect_ok "git state: none before the fail-closed rows" no_state
# gsr WORD LINE: git_state_refuse --index from $v3, the WORD read failing:
# exit 1, with "LINE (git state hides changes)" as a whole line.
gsr() { local o rc=0; o=$(cd "$v3" && gshim "$1" bash -c '. scripts/git-env.sh; git_state_refuse --index' 2>&1) || rc=$?; echo "$o"; echo "exit $rc"; [ "$rc" -eq 1 ] && grep -qxF -- "$2 (git state hides changes)" <<<"$o"; }
expect_ok "git-env.sh: a failed for-each-ref refs/replace/ is a problem" gsr refs/replace/ "git for-each-ref refs/replace/ failed (cannot check for replace refs)"
expect_ok "git-env.sh: a failed rev-parse --git-common-dir is a problem" gsr --git-common-dir "git rev-parse --git-common-dir --git-dir failed (cannot check for a graft file)"
expect_ok "git-env.sh: a config --get-all core.fsmonitor exit other than 0/1 is a problem" gsr core.fsmonitor "git config --get-all core.fsmonitor failed (exit 128; cannot check for an fsmonitor hook)"
expect_ok "git-env.sh: a failed ls-files -v is a problem" gsr ls-files "git ls-files -v failed (cannot check the index flags)"
# A caller folds the failure into its exit. $qs holds a fine queue (QS1 queued)
# here; the queue-check scenario below adds the orphan QS2 to it.
qs=$t/qs; mkdir -p "$qs"; printf '# QUEUE\n- QS1 | queued | S | x\n' >"$qs/QUEUE.local.md"
printf '# CARD QS1: t\nClass: test        Author: S (t)        Status: ready\n' >"$qs/QS1.local.md"
qsc() { (cd "$v3" && NV3_CARDS="$qs" bash scripts/queue-check.sh); }
expect_ok "premise: queue-check: the QS1 queue is fine" bash -c "[ \"\$(cd '$v3' && NV3_CARDS='$qs' bash scripts/queue-check.sh)\" = 'queue-check: ok' ]"
expect_ok "queue-check: a failed for-each-ref refs/replace/ fails it" out_has "git for-each-ref refs/replace/ failed (cannot check for replace refs) (git state hides changes)" -- gshim refs/replace/ qsc
# gsp: git_state_problems($r, index=True) under the caller's gshim (gshim WORD
# gsp) gives an entry ending "(cannot check git state)" that holds "exit 128"
# and the shim's refusal of WORD.
gsp() { PYTHONDONTWRITEBYTECODE=1 python3 - "$v3/tools/run_card.py" "$r" <<'P'
import importlib.util, os, pathlib, sys
spec = importlib.util.spec_from_file_location("rc", sys.argv[1]); rc = importlib.util.module_from_spec(spec); spec.loader.exec_module(rc)
probs = rc.git_state_problems(pathlib.Path(sys.argv[2]), index=True)
print(probs)
w = f"gshim refused {os.environ['GSHIM_FAIL']}"
sys.exit(0 if probs and any(p.endswith("(cannot check git state)") and "exit 128" in p and w in p for p in probs) else 1)
P
}
expect_ok "runner: a failed for-each-ref refs/replace/ is a problem" gshim refs/replace/ gsp
expect_ok "runner: a failed ls-files -v is a problem" gshim ls-files gsp
expect_ok "runner: scope_violations names the failed git state read" gshim refs/replace/ gs nucleus/v3-scaffold - "gshim refused refs/replace/ (cannot check git state)"
expect_ok "git state: none after the fail-closed rows" no_state

# The index flags are read from the top (X-6-F1-F2-F1 c): promote-pass runs
# from nucleus/v3, and a flagged file outside it hides its edit from the clean
# check and the gate just the same.
git -C "$r" checkout -q -B scratch/pass6 nucleus/v3-scaffold
echo dirty >>"$r/README"; git -C "$r" update-index --skip-worktree README
expect_ok "premise: promote top-level: plain status lists nothing" bash -c "[ -z \"\$(git -C '$r' status --porcelain)\" ]"
expect_ok "premise: promote top-level: ls-files -v in nucleus/v3 lists README only with ':/'" bash -c "cd '$v3' && ! git ls-files -v --full-name | grep -qx '. README' && git ls-files -v --full-name -- ':/' | grep -qx 'S README'"
promote6() { (cd "$v3" && NV3_CARDS="$qa" bash scripts/promote-pass.sh scratch/pass6); }
expect_ok "promote: a skip-worktree flag outside nucleus/v3 is refused" out_has "README is marked skip-worktree in the index" "GATE EXIT=0" "audit: ok" "not promoting" -- promote6
git -C "$r" update-index --no-skip-worktree README; git -C "$r" checkout -q -- README
expect_ok "git state: the top-level promote scenario's flag is gone" no_state
git -C "$r" checkout -q nucleus/v3-scaffold

# queue-check reads the merged verdicts from the scaffold's tree, which a replace
# ref can rewrite (X-6-F1-F2-F1 d): the state fails it, after the orphan check
# has run. Both commits are unreachable, as in XS.
qx=$(git -C "$r" commit-tree "nucleus/v3-scaffold^{tree}" -m "nucleus v3: QS replaced")
git -C "$r" replace "$qx" "$(git -C "$r" commit-tree "nucleus/v3-scaffold^{tree}" -m "nucleus v3: QS replacement")"
printf '# CARD QS2: t\nClass: test        Author: S (t)        Status: ready\n' >"$qs/QS2.local.md"
expect_ok "queue-check: a replace ref fails it, and the orphan check still ran" out_has "replace ref refs/replace/$qx (git state hides changes)" "queue-check: orphan card QS2" -- qsc
printf -- '- QS2 | queued | S | x\n' >>"$qs/QUEUE.local.md"
qs_state_only() { local o rc=0; o=$(qsc 2>&1) || rc=$?; echo "$o"; echo "exit $rc"; [ "$rc" -ne 0 ] && grep -qxF "replace ref refs/replace/$qx (git state hides changes)" <<<"$o" && ! grep -q 'orphan\|queue-check: ok' <<<"$o"; }
expect_ok "queue-check: with no orphan, the replace ref alone fails it" qs_state_only
git -C "$r" replace -d "$qx" >/dev/null
expect_ok "git state: queue-check's replace ref is gone" no_state
expect_ok "queue-check: control: with the replace ref gone and QS2 queued it is ok" bash -c "[ \"\$(cd '$v3' && NV3_CARDS='$qs' bash scripts/queue-check.sh)\" = 'queue-check: ok' ]"

# --- caches and neighbours that hide changes (X-6-F1-F2-F3) -----------------
# A hand-written commit-graph or a stale untracked cache hides changes; the
# environment turns both off and the refusal names a forged graph, a -c or
# GIT_TEST_COMMIT_GRAPH that turns the graph back on, a shallow repository and
# alternates. no_state2 checks this block's state is gone, as no_state does
# the rest.
gobj=$r/.git/objects/info
no_state2() { [ ! -e "$gobj/commit-graph" ] && [ ! -e "$gobj/commit-graphs" ] && [ ! -e "$gobj/alternates" ] && [ "$(git -C "$r" rev-parse --is-shallow-repository)" = false ] && ! git -C "$r" config core.untrackedCache && ! git -C "$r" config core.trustctime && ! git -C "$r" config status.showUntrackedFiles && ! git -C "$r" config core.ignoreStat; }
expect_ok "git state: none of the cache state before these scenarios" no_state2
# both LINE: git_state_refuse --index (git-env.sh, from $v3) exits 1 and names
# LINE, and so does git_state_problems($r), each under the caller's environment.
both() { local o rc=0; o=$(cd "$v3" && bash -c '. scripts/git-env.sh; git_state_refuse --index' 2>&1) || rc=$?; echo "sh (exit $rc): $o"; { [ "$rc" -eq 1 ] && grep -qF -- "$1" <<<"$o"; } || return 1
  PYTHONDONTWRITEBYTECODE=1 python3 - "$v3/tools/run_card.py" "$r" "$1" <<'P'
import importlib.util, pathlib, sys
spec = importlib.util.spec_from_file_location("rc", sys.argv[1]); rc = importlib.util.module_from_spec(spec); spec.loader.exec_module(rc)
probs = rc.git_state_problems(pathlib.Path(sys.argv[2]))
print("py:", probs)
sys.exit(0 if any(sys.argv[3] in p for p in probs) else 1)
P
}
# clean_both BASE: git_state_refuse --index exits 0 and scope_violations names
# no git state.
clean_both() { local o rc=0; o=$(cd "$v3" && bash -c '. scripts/git-env.sh; git_state_refuse --index' 2>&1) || rc=$?; echo "sh (exit $rc): $o"; [ "$rc" -eq 0 ] && gs "$1" - none; }
# cgpatch GRAPH B1 X: in commit-graph file GRAPH, B1's first parent becomes X
# (both must be in it); the trailing SHA-1 is rewritten. The card author's probe.
cgpatch() { python3 -I - "$@" <<'P'
import sys, hashlib, struct
p, b1, x = sys.argv[1], bytes.fromhex(sys.argv[2]), bytes.fromhex(sys.argv[3])
d = bytearray(open(p, "rb").read()); toc = {}
for i in range(d[6] + 1):
    cid, off = struct.unpack(">4sQ", d[8 + 12 * i: 20 + 12 * i]); toc[cid] = off
n = (toc[b"CDAT"] - toc[b"OIDL"]) // 20
oids = [bytes(d[toc[b"OIDL"] + 20 * i: toc[b"OIDL"] + 20 * i + 20]) for i in range(n)]
struct.pack_into(">I", d, toc[b"CDAT"] + 36 * oids.index(b1) + 20, oids.index(x))
d[-20:] = hashlib.sha1(bytes(d[:-20])).digest(); open(p, "wb").write(bytes(d))
P
}
# bloomzero GRAPH X: X's changed-path Bloom filter in GRAPH becomes all zero
# bits ("changed no path"); the trailing SHA-1 is rewritten.
bloomzero() { python3 -I - "$@" <<'P'
import sys, hashlib, struct
p, x = sys.argv[1], bytes.fromhex(sys.argv[2])
d = bytearray(open(p, "rb").read()); toc = {}
for i in range(d[6] + 1):
    cid, off = struct.unpack(">4sQ", d[8 + 12 * i: 20 + 12 * i]); toc[cid] = off
n = (toc[b"CDAT"] - toc[b"OIDL"]) // 20
k = [bytes(d[toc[b"OIDL"] + 20 * i: toc[b"OIDL"] + 20 * i + 20]) for i in range(n)].index(x)
end = struct.unpack(">I", d[toc[b"BIDX"] + 4 * k: toc[b"BIDX"] + 4 * k + 4])[0]
start = struct.unpack(">I", d[toc[b"BIDX"] + 4 * k - 4: toc[b"BIDX"] + 4 * k])[0] if k else 0
assert end > start
d[toc[b"BDAT"] + 12 + start: toc[b"BDAT"] + 12 + end] = bytes(end - start)
d[-20:] = hashlib.sha1(bytes(d[:-20])).digest(); open(p, "wb").write(bytes(d))
P
}
# GCG commit-graph: R2's geometry with an interior base commit. gcgbase is the
# scaffold plus B1 and B2; card/v3-GCG commits tools/gcg.py (X) on the
# scaffold. A written graph, patched so that B1's first parent is X, moves the
# merge-base to X. (Patching the tip does nothing: git reads command-line tips
# from the object.)
git -C "$r" checkout -q -B gcgbase nucleus/v3-scaffold
git -C "$r" commit -q --allow-empty -m "nucleus v3: GCG B1"; git -C "$r" commit -q --allow-empty -m "nucleus v3: GCG B2"
git -C "$r" checkout -q -B card/v3-GCG nucleus/v3-scaffold
echo x >"$v3/tools/gcg.py"; git -C "$r" add -A; git -C "$r" commit -qm "nucleus v3: GCG work"
gcx=$(git -C "$r" rev-parse HEAD) gcb1=$(git -C "$r" rev-parse gcgbase~1)
git -C "$r" commit-graph write --reachable; chmod u+w "$gobj/commit-graph"; cgpatch "$gobj/commit-graph" "$gcb1" "$gcx"
gcg_hidden() { local d m; d=$(plain diff --name-only gcgbase...card/v3-GCG); m=$(plain merge-base gcgbase card/v3-GCG); echo "diff: $d"; echo "merge-base: $m"; ! grep -qxF nucleus/v3/tools/gcg.py <<<"$d" && [ "$m" = "$gcx" ]; }
expect_ok "premise: GCG commit-graph: plain diff gcgbase...card drops tools/gcg.py, and the merge-base is X" gcg_hidden
gcg_verify_false() { local o rc=0; o=$(GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0=core.commitGraph GIT_CONFIG_VALUE_0=false git -C "$r" commit-graph verify 2>&1) || rc=$?; echo "$o"; echo "exit $rc"; [ "$rc" -eq 1 ] && grep -qF "commit-graph parent for $gcb1 is $gcx" <<<"$o"; }
expect_ok "premise: GCG: commit-graph verify checks the file under core.commitGraph=false too (no skip)" gcg_verify_false
expect_ok "runner: GCG commit-graph: the committed tools/ file is still protected" gs gcgbase nucleus/v3/tools/gcg.py -
expect_ok "check-core-paths: under GCG it still lists the hidden CORE file" bash -c "cd '$v3' && bash scripts/check-core-paths.sh gcgbase card/v3-GCG 2>/dev/null | grep -qxF 'CORE tools/gcg.py (tools/)'"
expect_ok "GCG: both refusals name the forged parent" both "commit-graph fails verification (commit-graph parent for $gcb1 is $gcx"
expect_ok "runner: GCG: scope_violations names it" gs gcgbase - "commit-graph fails verification"
expect_ok "audit: fails while the commit-graph fails verification" out_has "audit: commit-graph fails verification" -- bash -c "cd '$v3' && bash scripts/audit-merges.sh nucleus/v3-scaffold"
# A verify that cannot run is a problem, never "nothing found". gshim matches
# whole arguments (X-6-F1-F2-F1-F1): merge-card, promote-pass and enforce-lib
# pass --verify, which must not trip the word verify.
expect_ok "premise: gshim: an argument that contains GSHIM_FAIL without equalling it runs the real git" gshim verify git -C "$r" rev-parse -q --verify nucleus/v3-scaffold
expect_ok "git-env.sh: a commit-graph verify that cannot run is a problem" gsr verify "git commit-graph verify failed (exit 128; cannot check the commit-graph)"
expect_ok "runner: a commit-graph verify that cannot run is a problem" gshim verify gsp
expect_ok "git-env.sh: a failed rev-parse --is-shallow-repository is a problem" gsr --is-shallow-repository "git rev-parse --is-shallow-repository failed (cannot check for a shallow repository or alternates)"
expect_ok "runner: a failed rev-parse --is-shallow-repository is a problem" gshim --is-shallow-repository gsp
expect_ok "git-env.sh: a failed config --get core.commitGraph is a problem" gsr core.commitGraph "git config --get core.commitGraph failed (exit 128; cannot check that the environment's core.commitGraph=false holds)"
expect_ok "runner: a failed config --get core.untrackedCache is a problem" gshim core.untrackedCache gsp
rm -rf "$gobj/commit-graph" "$gobj/commit-graphs"
expect_ok "git state: GCG's commit-graph is gone" no_state2
# Control: a fresh, honest graph (gc writes these) is not refused.
git -C "$r" commit-graph write --reachable --changed-paths
expect_ok "runner: control: an honest commit-graph names no git state" clean_both gcgbase
rm -rf "$gobj/commit-graph" "$gobj/commit-graphs"
expect_ok "git state: the honest commit-graph is gone" no_state2

# GCB Bloom filter: X's changed-path filter zeroed in an otherwise honest graph
# drops X from a walk limited to a plain path, the runner's gate_committed
# `log -- <file>`. verify does not read the filters, so the environment is the
# layer, and the refusal names whatever turns the graph back on over it: a
# caller's -c (GIT_CONFIG_PARAMETERS beats GIT_CONFIG_COUNT) or
# GIT_TEST_COMMIT_GRAPH.
git -C "$r" commit-graph write --reachable --changed-paths; chmod u+w "$gobj/commit-graph"; bloomzero "$gobj/commit-graph" "$gcx"
gcb_plain() { local h; h=$(git -C "$r" log -1 --format=%H card/v3-GCG -- nucleus/v3/tools/gcg.py); echo "log: $h"; [ -z "$h" ] && git -C "$r" commit-graph verify; }
expect_ok "premise: GCB Bloom: plain log -- tools/gcg.py finds nothing, and verify passes" gcb_plain
gcb_py() { PYTHONDONTWRITEBYTECODE=1 python3 - "$v3/tools/run_card.py" "$r" "$gcx" <<'P'
import importlib.util, sys
spec = importlib.util.spec_from_file_location("rc", sys.argv[1]); rc = importlib.util.module_from_spec(spec); spec.loader.exec_module(rc)
rcode, out = rc.run(["git", "log", "-1", "--format=%H", "card/v3-GCG", "--", "nucleus/v3/tools/gcg.py"], sys.argv[2])
print(rcode, out)
sys.exit(0 if rcode == 0 and out.strip() == sys.argv[3] else 1)
P
}
expect_ok "runner: GCB Bloom: run()'s log -- tools/gcg.py still finds X" gcb_py
expect_ok "git-env.sh: GCB Bloom: log -- tools/gcg.py still finds X" bash -c "cd '$r' && . nucleus/v3/scripts/git-env.sh && [ \"\$(git log -1 --format=%H card/v3-GCG -- nucleus/v3/tools/gcg.py)\" = '$gcx' ]"
xcg="'core.commitgraph'='true'"
expect_ok "premise: GCB: a -c core.commitGraph=true beats the environment (log finds nothing)" bash -c "cd '$r' && . nucleus/v3/scripts/git-env.sh && [ -z \"\$(GIT_CONFIG_PARAMETERS=\"\$1\" git log -1 --format=%H card/v3-GCG -- nucleus/v3/tools/gcg.py)\" ]" _ "$xcg"
gcb_cp() { GIT_CONFIG_PARAMETERS=$xcg both "core.commitGraph is 'true' (command line:), over the environment's false"; }
expect_ok "GCB: both refusals name an inherited -c core.commitGraph=true" gcb_cp
expect_ok "premise: GCB: GIT_TEST_COMMIT_GRAPH=1 beats the environment (log finds nothing)" bash -c "cd '$r' && . nucleus/v3/scripts/git-env.sh && [ -z \"\$(GIT_TEST_COMMIT_GRAPH=1 git log -1 --format=%H card/v3-GCG -- nucleus/v3/tools/gcg.py)\" ]"
gcb_test() { GIT_TEST_COMMIT_GRAPH=1 both "GIT_TEST_COMMIT_GRAPH is '1' in the environment (forces commit-graph reads)"; }
expect_ok "GCB: both refusals name GIT_TEST_COMMIT_GRAPH=1" gcb_test
rm -rf "$gobj/commit-graph" "$gobj/commit-graphs"

# GAL alternates: GCG's forged graph moved into an alternate object directory.
# git reads it from there, and verify checks only $r's own graph.
galt=$t/galt; mkdir -p "$galt/info"
git -C "$r" commit-graph write --reachable; mv "$gobj/commit-graph" "$galt/info/commit-graph"; chmod u+w "$galt/info/commit-graph"; cgpatch "$galt/info/commit-graph" "$gcb1" "$gcx"
echo "$galt" >"$gobj/alternates"
gal_hidden() { local d; d=$(plain diff --name-only gcgbase...card/v3-GCG); echo "diff: $d"; ! grep -qxF nucleus/v3/tools/gcg.py <<<"$d" && git -C "$r" commit-graph verify; }
expect_ok "premise: GAL alternates: plain diff drops tools/gcg.py, and verify passes" gal_hidden
expect_ok "runner: GAL alternates: the committed tools/ file is still protected" gs gcgbase nucleus/v3/tools/gcg.py -
expect_ok "GAL: both refusals name the alternates file" both "alternates file $gobj/alternates"
rm -f "$gobj/alternates"
gal_env_hidden() { local d; d=$(GIT_ALTERNATE_OBJECT_DIRECTORIES=$galt plain diff --name-only gcgbase...card/v3-GCG); echo "diff: $d"; ! grep -qxF nucleus/v3/tools/gcg.py <<<"$d"; }
expect_ok "premise: GAL: GIT_ALTERNATE_OBJECT_DIRECTORIES does the same with no file" gal_env_hidden
gal_env() { GIT_ALTERNATE_OBJECT_DIRECTORIES=$galt both "GIT_ALTERNATE_OBJECT_DIRECTORIES is '$galt' in the environment"; }
expect_ok "GAL: both refusals name GIT_ALTERNATE_OBJECT_DIRECTORIES" gal_env
rm -rf "$galt"
git -C "$r" checkout -q nucleus/v3-scaffold

# GSH shallow: card/v3-GSH has the worker commit W; gsh-other (C, on W) is a
# ref the run made. With C in .git/shallow, C has no parents, so
# merge-base --is-ancestor W C says no and ref_violations' R1 misses the ref.
git -C "$r" checkout -q -B card/v3-GSH nucleus/v3-scaffold
git -C "$r" commit -q --allow-empty -m "nucleus v3: GSH work"
git -C "$r" checkout -q -B gsh-other; git -C "$r" commit -q --allow-empty -m "GSH other"
gshc=$(git -C "$r" rev-parse HEAD); git -C "$r" checkout -q card/v3-GSH
# gsh_rv 1|0: ref_violations, with gsh-other left out of the snapshot (made
# during the run), names gsh-other as carrying W (1) or does not (0).
gsh_rv() { PYTHONDONTWRITEBYTECODE=1 python3 - "$v3/tools/run_card.py" "$r" "$1" <<'P'
import importlib.util, pathlib, sys
spec = importlib.util.spec_from_file_location("rc", sys.argv[1]); rc = importlib.util.module_from_spec(spec); spec.loader.exec_module(rc)
wt = pathlib.Path(sys.argv[2])
before = rc.ref_snapshot(wt); before.pop("refs/heads/gsh-other")
pre = rc.run(["git", "rev-parse", "nucleus/v3-scaffold"], wt)[1].strip()
bad = rc.ref_violations(wt, before, "card/v3-GSH", pre)
print(bad)
hit = any(b.startswith("refs/heads/gsh-other: ") and "carries this run's commit" in b for b in bad)
sys.exit(0 if hit == (sys.argv[3] == "1") else 1)
P
}
expect_ok "premise: GSH: without a shallow file ref_violations names gsh-other as carrying W" gsh_rv 1
echo "$gshc" >"$r/.git/shallow"
expect_ok "premise: GSH shallow: merge-base --is-ancestor W C exits 1" bash -c "git -C '$r' merge-base --is-ancestor card/v3-GSH gsh-other; [ \$? -eq 1 ]"
expect_ok "premise: GSH shallow: ref_violations no longer names gsh-other" gsh_rv 0
expect_ok "GSH: both refusals name the shallow file" both "shallow repository ($r/.git/shallow)"
expect_ok "runner: GSH: scope_violations names it" gs nucleus/v3-scaffold - "shallow repository ($r/.git/shallow)"
rm -f "$r/.git/shallow"
# The same shallow list from git's GIT_SHALLOW_FILE, with no file in $r: the
# refusal names the file git reads.
echo "$gshc" >"$t/gsh.shallow"
expect_ok "premise: GSH: GIT_SHALLOW_FILE alone makes merge-base --is-ancestor W C exit 1" bash -c "GIT_SHALLOW_FILE='$t/gsh.shallow' git -C '$r' merge-base --is-ancestor card/v3-GSH gsh-other; [ \$? -eq 1 ]"
gsh_env() { GIT_SHALLOW_FILE=$t/gsh.shallow both "shallow repository ($t/gsh.shallow)"; }
expect_ok "GSH: both refusals name a GIT_SHALLOW_FILE shallow list" gsh_env
rm -f "$t/gsh.shallow"
git -C "$r" checkout -q nucleus/v3-scaffold

# GUC untracked cache: with core.untrackedCache=true, core.trustctime=false and
# status.showUntrackedFiles=all (so --untracked-files=all reads the cache too),
# an untracked tools/gucz.py renamed to tools/guc.py, with the directory's
# times put back, leaves the cached listing valid: status shows the old name.
# guc_hide re-arms it, since a status under core.untrackedCache=false drops the
# cache from the index. The directory is first dated 10s back: this git
# compares whole seconds, and a directory changed in the second the index is
# written is racy, so git would rescan it.
git -C "$r" config core.untrackedCache true; git -C "$r" config core.trustctime false; git -C "$r" config status.showUntrackedFiles all
guc_hide() { rm -f "$v3/tools/guc.py"; echo z >"$v3/tools/gucz.py"; touch -d "@$(($(date +%s) - 10))" "$v3/tools"; git -C "$r" status --porcelain >/dev/null; git -C "$r" status --porcelain >/dev/null
  touch -r "$v3/tools" "$t/guc.ref"; mv "$v3/tools/gucz.py" "$v3/tools/guc.py"; touch -r "$t/guc.ref" "$v3/tools"; }
guc_hidden() { local s; s=$(git -C "$r" status --porcelain --no-renames --untracked-files=all); echo "$s"; grep -qxF '?? nucleus/v3/tools/gucz.py' <<<"$s" && ! grep -qxF '?? nucleus/v3/tools/guc.py' <<<"$s"; }
guc_hide
expect_ok "premise: GUC untracked cache: plain status --untracked-files=all shows the old name only" guc_hidden
expect_ok "runner: GUC: the untracked tools/ file is still protected" gs nucleus/v3-scaffold nucleus/v3/tools/guc.py -
guc_hide
expect_ok "premise: GUC: hidden again" guc_hidden
expect_ok "git-env.sh: GUC: promote-pass's status --porcelain lists tools/guc.py" bash -c "cd '$v3' && . scripts/git-env.sh && git status --porcelain | grep -qxF '?? nucleus/v3/tools/guc.py'"
guc_hide
xuc="'core.untrackedcache'='true'"
expect_ok "premise: GUC: a -c core.untrackedCache=true beats the environment" bash -c "cd '$v3' && . scripts/git-env.sh && ! GIT_CONFIG_PARAMETERS=\"\$1\" git status --porcelain | grep -qxF '?? nucleus/v3/tools/guc.py'" _ "$xuc"
guc_cp() { GIT_CONFIG_PARAMETERS=$xuc both "core.untrackedCache is 'true' (command line:), over the environment's false"; }
expect_ok "GUC: both refusals name an inherited -c core.untrackedCache=true" guc_cp
git -C "$r" config --unset core.untrackedCache; git -C "$r" config --unset core.trustctime; git -C "$r" config --unset status.showUntrackedFiles
git -C "$r" update-index --no-untracked-cache; rm -f "$v3/tools/guc.py" "$v3/tools/gucz.py" "$t/guc.ref"

# GIS core.ignoreStat: `git add` under it marks the entry assume-unchanged, so
# a second edit is hidden from plain status; the --index refusal names the bit.
git -C "$r" config core.ignoreStat true
echo e1 >>"$r/README"; git -C "$r" add README; echo e2 >>"$r/README"
expect_ok "premise: GIS ignoreStat: plain status shows the staged edit only" bash -c "[ \"\$(git -C '$r' status --porcelain)\" = 'M  README' ]"
expect_ok "GIS: both refusals name the assume-unchanged bit" both "README is marked assume-unchanged in the index"
git -C "$r" config --unset core.ignoreStat; git -C "$r" update-index --no-assume-unchanged README; git -C "$r" reset -q -- README; git -C "$r" checkout -q -- README
expect_ok "git state: none after GUC and GIS" no_state
expect_ok "git state: none of the cache state after GUC and GIS" no_state2

# --- output-shaping config and worktree files (X-6-F1-F2-F2-F1) -------------
# Settings that reshape what diff and status print hide a change without
# touching an object: flags beat diff.relative, submodule ignore settings and
# status.showUntrackedFiles=no at any scope; untracked files are listed by the
# source that hides them; core.trustctime, core.checkStat and core.ignoreCase
# are neutralised and refused; an active clean filter is refused. Config is set
# per command (GIT_CONFIG_PARAMETERS as a prefix, never exported) or in $r's
# config with a cleanup row; no_state3 checks this block's state is gone.
no_state3() { [ ! -e "$r/.gitmodules" ] && ! grep -qxF nucleus/v3/tools/ex.py "$r/.git/info/exclude" && [ ! -e "$r/.git/info/attributes" ] && ! git -C "$r" config core.trustctime && ! git -C "$r" config core.checkStat && ! git -C "$r" config core.ignoreCase && [ -z "$(git -C "$r" config --get-regexp '^filter\.')" ] && [ -z "$(git -C "$r" for-each-ref refs/heads/st-base refs/heads/ex-base refs/heads/scratch/dr refs/heads/card/v3-DS refs/heads/card/v3-DSC)" ] && [ -z "$(git -C "$r" status --porcelain --untracked-files=all --ignored=matching)" ]; }
expect_ok "git state: none of the output-shaping state before these scenarios" no_state3
# DR diff.relative: from nucleus/v3, `git diff` prints cwd-relative paths and
# drops everything outside it. Card I (S verdict) changes the v3 workflow;
# card D (P verdict, Class standard) changes nucleus/v3/docs/X.md.
xdr="'diff.relative'='true'"
dr_hidden() { local d; d=$(cd "$v3" && GIT_CONFIG_PARAMETERS=$xdr git diff --name-only nucleus/v3-scaffold...card/v3-I -- ':/'); echo "$d"; [ "$d" = verdicts/I.md ]; }
expect_ok "premise: DR: under diff.relative, diff from nucleus/v3 drops .forgejo/ and strips the prefix" dr_hidden
dr_tree() { local d; d=$(cd "$v3" && GIT_CONFIG_PARAMETERS=$xdr git diff-tree --no-commit-id --name-only -r card/v3-D -- ':/'); echo "$d"; [ "$d" = nucleus/v3/verdicts/D.md ]; }
expect_ok "premise: DR: diff-tree ignores diff.relative (so the tip check needs no flag)" dr_tree
dr_cm() { (cd "$v3" && . scripts/core-paths-lib.sh && . scripts/enforce-lib.sh && GIT_CONFIG_PARAMETERS=$xdr check_merge I nucleus/v3-scaffold card/v3-I audit); }
expect_ok "check_merge: DR: under diff.relative the S verdict's v3 workflow change is still outside nucleus/v3" out_has ".forgejo/workflows/nucleus-v3.yml is outside nucleus/v3 and needs a P verdict" -- dr_cm
# A titled P merge of card D on a throwaway ref, built with commit-tree.
drt=$(git -C "$r" merge-tree --write-tree nucleus/v3-scaffold card/v3-D)
git -C "$r" update-ref refs/heads/scratch/dr "$(git -C "$r" commit-tree "$drt" -p nucleus/v3-scaffold -p card/v3-D -m "nucleus v3: merge card/v3-D (MERGE, gate P/test-model)")"
dr_audit() { (cd "$v3" && GIT_CONFIG_PARAMETERS=$xdr bash scripts/audit-merges.sh scratch/dr); }
expect_ok "audit: DR: under diff.relative a P merge's non-spec docs/ change is still refused" out_has "nucleus/v3/docs/X.md: docs/ changes need Class: spec and a P verdict" -- dr_audit
# This row passed before the card too: every listed path is under nucleus/v3,
# so the prefix strip is a no-op (NO-REL-CCP is an equivalent mutant).
expect_ok "check-core-paths: DR: under diff.relative it lists the CORE file" bash -c "cd '$v3' && GIT_CONFIG_PARAMETERS=\"\$1\" bash scripts/check-core-paths.sh nucleus/v3-scaffold card/v3-GRB | grep -qxF 'CORE tools/grb.py (tools/)'" _ "$xdr"
git -C "$r" update-ref -d refs/heads/scratch/dr

# DS submodule ignore: card/v3-DS commits a gitlink at nucleus/v3/tools/ds with
# an S verdict. The empty directory keeps `git add -A` from staging its removal.
card DS S standard
git -C "$r" checkout -q -b card/v3-DS nucleus/v3-scaffold
git -C "$r" update-index --add --cacheinfo "160000,$(git -C "$r" rev-parse nucleus/v3-scaffold),nucleus/v3/tools/ds"
git -C "$r" commit -qm "nucleus v3: DS work"; mkdir -p "$v3/tools/ds"
verdict DS S MERGE standard S
xds="'diff.ignoresubmodules'='all'"
ds_hidden() { local d; d=$(GIT_CONFIG_PARAMETERS=$1 git -C "$r" diff --name-only nucleus/v3-scaffold...card/v3-DS); echo "$d"; [ "$d" = nucleus/v3/verdicts/DS.md ]; }
expect_ok "premise: DS: the gitlink is committed" bash -c "git -C '$r' ls-tree card/v3-DS nucleus/v3/tools/ds | grep -q '^160000 commit '"
expect_ok "premise: DS (a): under diff.ignoreSubmodules=all plain diff drops the gitlink" ds_hidden "$xds"
ds_cm() { (cd "$v3" && . scripts/core-paths-lib.sh && . scripts/enforce-lib.sh && check_merge DS nucleus/v3-scaffold card/v3-DS audit); }
ds_cm_cp() { GIT_CONFIG_PARAMETERS=$xds ds_cm; }
expect_ok "check_merge: DS (a): under diff.ignoreSubmodules=all the CORE gitlink still needs a P verdict" out_has "nucleus/v3/tools/ds is under CORE_PATHS and needs a P verdict" -- ds_cm_cp
ds_gs() { GIT_CONFIG_PARAMETERS=$xds gs nucleus/v3-scaffold nucleus/v3/tools/ds -; }
expect_ok "runner: DS (a): under diff.ignoreSubmodules=all the committed gitlink is still protected" ds_gs
printf '[submodule "ds"]\n\tpath = nucleus/v3/tools/ds\n\turl = ./x\n\tignore = all\n' >"$r/.gitmodules"
expect_ok "premise: DS (b): with an untracked .gitmodules ignore=all plain diff drops the gitlink" ds_hidden ""
expect_ok "check_merge: DS (b): with an untracked .gitmodules ignore=all the gitlink still needs a P verdict" out_has "nucleus/v3/tools/ds is under CORE_PATHS and needs a P verdict" -- ds_cm
expect_ok "runner: DS (b): with an untracked .gitmodules ignore=all the gitlink is still protected" gs nucleus/v3-scaffold nucleus/v3/tools/ds -
# DS (c): the verdict commit itself also adds the gitlink; .gitmodules hides it
# from diff-tree, which merge-card's tip check reads. It is put back after the
# verdict commit, whose `git add -A` would commit it.
card DSC S standard
mv "$r/.gitmodules" "$t/ds.gitmodules"
git -C "$r" checkout -q -b card/v3-DSC nucleus/v3-scaffold
echo dsc >"$v3/spikes/dsc.txt"; git -C "$r" add nucleus/v3/spikes/dsc.txt; git -C "$r" commit -qm "nucleus v3: DSC work"
mkdir -p "$v3/tools/ds"; git -C "$r" update-index --add --cacheinfo "160000,$(git -C "$r" rev-parse nucleus/v3-scaffold),nucleus/v3/tools/ds"
verdict DSC S MERGE standard S
mv "$t/ds.gitmodules" "$r/.gitmodules"
dsc_hidden() { local d; d=$(git -C "$r" diff-tree --no-commit-id --name-only -r card/v3-DSC); echo "$d"; [ "$d" = nucleus/v3/verdicts/DSC.md ] && git -C "$r" ls-tree card/v3-DSC nucleus/v3/tools/ds | grep -q '^160000 '; }
expect_ok "premise: DS (c): with .gitmodules ignore=all plain diff-tree lists only the verdict" dsc_hidden
scratch
expect_ok "merge-card: DS (c): the tip check still names a gitlink the verdict commit adds" out_has "it changes: nucleus/v3/tools/ds nucleus/v3/verdicts/DSC.md" -- mc DSC
rm -f "$r/.gitmodules"; git -C "$r" checkout -q nucleus/v3-scaffold; rmdir "$v3/tools/ds" 2>/dev/null || true
git -C "$r" branch -q -D card/v3-DS card/v3-DSC; rm -f "$cards/DS.local.md" "$cards/DSC.local.md"

# SU status.showUntrackedFiles=no: promote-pass's clean check still sees an
# untracked file. scratch/pass7 is the scaffold tip.
git -C "$r" checkout -q -B scratch/pass7 nucleus/v3-scaffold
promote7() { (cd "$v3" && NV3_CARDS="$qa" bash scripts/promote-pass.sh scratch/pass7); }
xsu="'status.showuntrackedfiles'='no'"
echo su >"$v3/spikes/su.txt"
expect_ok "premise: SU: under status.showUntrackedFiles=no plain status lists nothing" bash -c "[ -z \"\$(GIT_CONFIG_PARAMETERS=\"\$1\" git -C '$r' status --porcelain)\" ]" _ "$xsu"
su_promote() { GIT_CONFIG_PARAMETERS=$xsu promote7; }
expect_ok "promote: SU: an untracked file under status.showUntrackedFiles=no is not clean" out_has "promote-pass: worktree is not clean" "?? nucleus/v3/spikes/su.txt" -- su_promote
rm -f "$v3/spikes/su.txt"

# EX excludes: an untracked tools/ex.py hidden from status by each exclude
# source in turn is still listed, and named by path.
echo ex >"$v3/tools/ex.py"
ex_hidden() { local s; s=$(git -C "$r" status --porcelain --untracked-files=all); echo "$s"; ! grep -qF ex.py <<<"$s"; }
expect_ok "premise: EX: control: plain status lists tools/ex.py" bash -c "git -C '$r' status --porcelain --untracked-files=all | grep -qxF '?? nucleus/v3/tools/ex.py'"
printf 'ex.py\n' >"$t/ex.ignore"
xex="'core.excludesfile'='$t/ex.ignore'"
ex_a() { GIT_CONFIG_PARAMETERS=$xex "$@"; }
expect_ok "premise: EX (a): a -c core.excludesFile hides it from plain status" ex_a ex_hidden
expect_ok "runner: EX (a): under a -c core.excludesFile tools/ex.py is still protected" ex_a gs nucleus/v3-scaffold nucleus/v3/tools/ex.py -
expect_ok "promote: EX (f): under a -c core.excludesFile the worktree is not clean" out_has "promote-pass: worktree is not clean" "!! nucleus/v3/tools/ex.py" -- ex_a promote7
# EX rework (verdict R3): a check-ignore that fails, with an ignored path to
# name, never reads as "nothing hidden by a tracked rule": the runner's
# listing raises (the dirty check's and scope_violations' listing alike), and
# promote-pass dies.
wtc_raises() { PYTHONDONTWRITEBYTECODE=1 python3 - "$v3/tools/run_card.py" "$r" <<'P'
import importlib.util, pathlib, subprocess, sys
spec = importlib.util.spec_from_file_location("rc", sys.argv[1]); rc = importlib.util.module_from_spec(spec); spec.loader.exec_module(rc)
try:
    print("listed:", rc.worktree_changes(pathlib.Path(sys.argv[2])))
except subprocess.CalledProcessError as e:
    print("raised:", e.cmd, e.returncode, e.stderr)
    sys.exit(0 if "check-ignore" in e.cmd and e.returncode == 128 else 1)
sys.exit(1)
P
}
expect_ok "runner: EX: a failed check-ignore makes worktree_changes raise (the dirty check's listing)" ex_a gshim check-ignore wtc_raises
ci_gs() { PYTHONDONTWRITEBYTECODE=1 python3 - "$v3/tools/run_card.py" "$r" <<'P'
import importlib.util, pathlib, sys
spec = importlib.util.spec_from_file_location("rc", sys.argv[1]); rc = importlib.util.module_from_spec(spec); spec.loader.exec_module(rc)
bad = rc.scope_violations(pathlib.Path(sys.argv[2]), "nucleus/v3-scaffold", ["nucleus/v3/spikes/"])
print("\n".join(bad) or "(no violations)")
sys.exit(0 if any(b.startswith("git check-ignore ") and "gshim refused check-ignore" in b and b.endswith("(cannot list changed files)") for b in bad) else 1)
P
}
expect_ok "runner: EX: scope_violations reports a failed check-ignore, not a listing" ex_a gshim check-ignore ci_gs
expect_ok "promote: EX: a failed check-ignore dies, never reads as clean" out_has "promote-pass: cannot name the rules hiding ignored paths (git check-ignore exit 128)" -- ex_a gshim check-ignore promote7
mkdir -p "$t/xdg/git"; printf '*.py\n' >"$t/xdg/git/ignore"
ex_b() { XDG_CONFIG_HOME=$t/xdg "$@"; }
expect_ok "premise: EX (b): the XDG default ignore file hides it from plain status" ex_b ex_hidden
expect_ok "runner: EX (b): under the XDG ignore file tools/ex.py is still protected" ex_b gs nucleus/v3-scaffold nucleus/v3/tools/ex.py -
mkdir -p "$r/.git/info"; touch "$r/.git/info/exclude"; cp "$r/.git/info/exclude" "$t/exclude.orig"; echo nucleus/v3/tools/ex.py >>"$r/.git/info/exclude"
expect_ok "premise: EX (c): an info/exclude line hides it from plain status" ex_hidden
expect_ok "runner: EX (c): under an info/exclude line tools/ex.py is still protected" gs nucleus/v3-scaffold nucleus/v3/tools/ex.py -
cp "$t/exclude.orig" "$r/.git/info/exclude"
printf '*\n' >"$v3/tools/.gitignore"
ex_d_hidden() { local s; s=$(git -C "$r" status --porcelain --untracked-files=all); echo "$s"; ! grep -qF nucleus/v3/tools/ <<<"$s"; }
expect_ok "premise: EX (d): a self-ignoring untracked tools/.gitignore hides itself and ex.py" ex_d_hidden
expect_ok "runner: EX (d): under a self-ignoring untracked .gitignore tools/ex.py is still protected" gs nucleus/v3-scaffold nucleus/v3/tools/ex.py -
expect_ok "runner: EX (d): the self-ignoring untracked .gitignore is protected too" gs nucleus/v3-scaffold nucleus/v3/tools/.gitignore -
expect_ok "promote: EX (d): under a self-ignoring untracked .gitignore the worktree is not clean" out_has "promote-pass: worktree is not clean" "!! nucleus/v3/tools/.gitignore" "!! nucleus/v3/tools/ex.py" -- promote7
rm -f "$v3/tools/.gitignore"
# EX (e) control: a rule in a tracked .gitignore hides a path below it, and the
# listing trusts it. ex-base commits a top-level .gitignore naming tools/ex.py
# and nucleus/v3/spikes/.gitignore naming exh.py.
git -C "$r" checkout -q -b ex-base nucleus/v3-scaffold
printf 'nucleus/v3/tools/ex.py\n' >"$r/.gitignore"; printf 'exh.py\n' >"$v3/spikes/.gitignore"
git -C "$r" add .gitignore nucleus/v3/spikes/.gitignore; git -C "$r" commit -qm "ex base: tracked ignore rules"
expect_ok "premise: EX (e): check-ignore names the tracked top-level .gitignore for tools/ex.py" bash -c "[ \"\$(git -C '$r' check-ignore -v nucleus/v3/tools/ex.py)\" = \$'.gitignore:1:nucleus/v3/tools/ex.py\tnucleus/v3/tools/ex.py' ]"
# gsa BASE PATH: scope_violations on $r names nothing under PATH.
gsa() { PYTHONDONTWRITEBYTECODE=1 python3 - "$v3/tools/run_card.py" "$r" "$@" <<'P'
import importlib.util, pathlib, sys
spec = importlib.util.spec_from_file_location("rc", sys.argv[1]); rc = importlib.util.module_from_spec(spec); spec.loader.exec_module(rc)
bad = rc.scope_violations(pathlib.Path(sys.argv[2]), sys.argv[3], ["nucleus/v3/spikes/"])
print("\n".join(bad) or "(no violations)")
sys.exit(0 if not any(b.startswith(sys.argv[4]) for b in bad) else 1)
P
}
expect_ok "runner: EX (e): control: a path a tracked .gitignore hides is not listed" gsa ex-base nucleus/v3/tools/ex.py
# A core.excludesFile naming a tracked .gitignore applies its rules everywhere,
# not only below its own directory: tools/exh.py is not under spikes/.
echo exh >"$v3/tools/exh.py"
xexh="'core.excludesfile'='nucleus/v3/spikes/.gitignore'"
ex_h() { GIT_CONFIG_PARAMETERS=$xexh "$@"; }
expect_ok "premise: EX (h): a -c core.excludesFile naming tracked spikes/.gitignore hides tools/exh.py, named as that file" ex_h bash -c "[ \"\$(git -C '$r' check-ignore -v nucleus/v3/tools/exh.py)\" = \$'nucleus/v3/spikes/.gitignore:1:exh.py\tnucleus/v3/tools/exh.py' ] && ! git -C '$r' status --porcelain -uall | grep -qF exh.py"
expect_ok "runner: EX (h): a tracked .gitignore used as core.excludesFile hides nothing outside its directory" ex_h gs ex-base nucleus/v3/tools/exh.py -
# promote-pass under (e) alone: the clean check passes (ex-base's commits then
# fail the audit, so nothing is pushed); under (h) it does not.
git -C "$r" checkout -q -B scratch/pass7 ex-base; rm -f "$v3/tools/exh.py"
promote_clean_ok() { local o rc=0; o=$(promote7 2>&1) || rc=$?; echo "$o"; [ "$rc" -ne 0 ] && grep -qF "promote-pass: merge audit failed" <<<"$o" && ! grep -qF "worktree is not clean" <<<"$o"; }
expect_ok "promote: EX (e): control: a path a tracked .gitignore hides leaves the worktree clean" promote_clean_ok
echo exh >"$v3/tools/exh.py"
expect_ok "promote: EX (h): a tracked .gitignore used as core.excludesFile leaves tools/exh.py listed" out_has "promote-pass: worktree is not clean" "!! nucleus/v3/tools/exh.py" -- ex_h promote7
rm -f "$v3/tools/ex.py" "$v3/tools/exh.py" "$t/ex.ignore" "$t/exclude.orig"; rm -rf "$t/xdg"
git -C "$r" checkout -q nucleus/v3-scaffold; git -C "$r" branch -q -D scratch/pass7 ex-base

# ST stat trust: an uncommitted, same-size edit of tools/st.py with its mtime
# put back. The file is dated 10s back and the index rewritten, then the edit
# waits a second, so the index is not racy and the edit's ctime differs.
git -C "$r" checkout -q -b st-base nucleus/v3-scaffold
printf 'print("st")\n' >"$v3/tools/st.py"; git -C "$r" add nucleus/v3/tools/st.py; git -C "$r" commit -qm "st base"
cp "$v3/tools/st.py" "$t/st.orig"
touch -m -d "@$(($(date +%s) - 10))" "$v3/tools/st.py"; plain status --porcelain >/dev/null; sleep 1
touch -r "$v3/tools/st.py" "$t/st.ref"; printf 'print("ST")\n' >"$v3/tools/st.py"; touch -m -r "$t/st.ref" "$v3/tools/st.py"
st_hidden() { local s; s=$(plain status --porcelain); echo "$s"; [ -z "$s" ]; }
st_sh() { (cd "$v3" && . scripts/git-env.sh && git status --porcelain | grep -qxF ' M nucleus/v3/tools/st.py'); }
st_refuse() { (cd "$v3" && bash -c '. scripts/git-env.sh; git_state_refuse'); }
expect_ok "premise: ST: control: plain status lists the edit" bash -c "git -C '$r' status --porcelain | grep -qxF ' M nucleus/v3/tools/st.py'"
for st in "core.trustctime false" "core.checkStat minimal"; do
  k=${st% *} v=${st#* }
  git -C "$r" config "$k" "$v"
  expect_ok "premise: ST: under $k=$v in the repository config plain status lists nothing" st_hidden
  expect_ok "runner: ST: under $k=$v in the repository config the edit is still protected" gs st-base nucleus/v3/tools/st.py -
  expect_ok "runner: ST: $k=$v in the repository config is named" gs st-base - "$k is '$v' (file:"
  expect_ok "git-env.sh: ST: under $k=$v in the repository config status lists the edit" st_sh
  expect_ok "git-env.sh: ST: $k=$v in the repository config is refused" out_has "$k is '$v' (file:" -- st_refuse
  git -C "$r" config --unset "$k"
  xst="'${k,,}'='$v'"
  st_cp() { GIT_CONFIG_PARAMETERS=$xst "$@"; }
  expect_ok "premise: ST: under a -c $k=$v plain status lists nothing" st_cp st_hidden
  expect_ok "runner: ST: a -c $k=$v is named, at command-line scope" st_cp gs st-base - "$k is '$v' (command line:)"
  expect_ok "git-env.sh: ST: a -c $k=$v is refused, at command-line scope" st_cp out_has "$k is '$v' (command line:)" -- st_refuse
done
expect_ok "git-env.sh: a failed config --get-all core.trustctime is a problem" gsr core.trustctime "git config --get-all core.trustctime failed (exit 128; cannot check core.trustctime)"
expect_ok "git-env.sh: a failed config --get-all core.checkStat is a problem" gsr core.checkStat "git config --get-all core.checkStat failed (exit 128; cannot check core.checkStat)"
expect_ok "runner: a failed config --get-all core.trustctime is a problem" gshim core.trustctime gsp
expect_ok "runner: a failed config --get-all core.checkStat is a problem" gshim core.checkStat gsp

# SF clean filter: with the edit still in place, info/attributes gives st.py a
# filter whose clean command prints the committed content. status compares the
# same-size file only through it, so the edit passes status and diff, under
# the environment too: the refusal is the layer.
echo 'nucleus/v3/tools/st.py filter=sf' >"$r/.git/info/attributes"
xsf="'filter.sf.clean'='cat $t/st.orig'"
sf_hidden() { local s d; s=$(cd "$v3" && . scripts/git-env.sh && GIT_CONFIG_PARAMETERS=$xsf git status --porcelain); d=$(cd "$v3" && . scripts/git-env.sh && GIT_CONFIG_PARAMETERS=$xsf git diff --name-only HEAD); echo "status: $s"; echo "diff: $d"; [ -z "$s" ] && [ -z "$d" ]; }
expect_ok "premise: SF: under a -c clean filter, status and diff under git-env.sh list nothing" sf_hidden
sf_line="filter=sf is on 1 tracked path(s) (first: nucleus/v3/tools/st.py), and filter.sf.clean is 'cat $t/st.orig'"
sf_refuse() { (cd "$v3" && GIT_CONFIG_PARAMETERS=$xsf bash -c '. scripts/git-env.sh; git_state_refuse --index'); }
expect_ok "git-env.sh: SF: a -c clean filter on a tracked path is refused, at command-line scope" out_has "$sf_line (command line:)" -- sf_refuse
sf_gs() { GIT_CONFIG_PARAMETERS=$xsf gs st-base - "$sf_line (command line:)"; }
expect_ok "runner: SF: a -c clean filter on a tracked path is named, at command-line scope" sf_gs
git -C "$r" config filter.sf.clean "cat $t/st.orig"
expect_ok "SF: both refusals name a clean filter in the repository config" both "$sf_line (file:"
expect_ok "git-env.sh: SF: a failed check-attr is a problem" gsr check-attr "git check-attr filter failed (cannot check for clean filters)"
expect_ok "runner: SF: a failed check-attr is a problem" gshim check-attr gsp
expect_ok "git-env.sh: SF: a failed config --get-all filter.sf.clean is a problem" gsr filter.sf.clean "git config --get-all filter.sf.clean failed (exit 128; cannot check the clean filter)"
# SF rework (verdict R1): a driver named like an attribute state. check-attr
# prints `set` both for a bare `filter` and for `filter=set`, yet git runs
# filter.set.clean for the second (likewise unset and unspecified), so no
# value may be skipped. Premise: hash-object --path applies the attribute's
# filter, so the edited file hashes to the committed blob under the driver.
# For unspecified the count covers every unattributed path, so that row
# matches from ", and" on.
git -C "$r" config --unset filter.sf.clean
st_blob=$(git -C "$r" rev-parse st-base:nucleus/v3/tools/st.py)
for sd in set unset unspecified; do
  echo "nucleus/v3/tools/st.py filter=$sd" >"$r/.git/info/attributes"
  xsd="'filter.$sd.clean'='cat $t/st.orig'"
  sd_hash() { (cd "$v3" && . scripts/git-env.sh && GIT_CONFIG_PARAMETERS=$xsd git -C "$r" hash-object --path=nucleus/v3/tools/st.py nucleus/v3/tools/st.py) | grep -qxF "$st_blob" && ! git -C "$r" hash-object --no-filters nucleus/v3/tools/st.py | grep -qxF "$st_blob"; }
  expect_ok "premise: SF: git runs filter.$sd.clean for filter=$sd: the edit hashes to the committed blob" sd_hash
  sd_line=", and filter.$sd.clean is 'cat $t/st.orig' (command line:)"
  [ "$sd" = unspecified ] || sd_line="filter=$sd is on 1 tracked path(s) (first: nucleus/v3/tools/st.py)$sd_line"
  sd_both() { GIT_CONFIG_PARAMETERS=$xsd both "$sd_line"; }
  expect_ok "SF: both refusals name a -c clean filter for a driver named '$sd'" sd_both
done
# SF rework (verdict R2): a driver with only filter.<driver>.process set. The
# refusal reads config only, so the command need not run.
echo 'nucleus/v3/tools/st.py filter=sf' >"$r/.git/info/attributes"
xsp="'filter.sf.process'='nv3-no-such-process'"
expect_ok "premise: SF: under a -c filter.sf.process alone, filter.sf.clean is unset" bash -c "! GIT_CONFIG_PARAMETERS=\"\$1\" git -C '$r' config --get-all filter.sf.clean" _ "$xsp"
sp_both() { GIT_CONFIG_PARAMETERS=$xsp both "filter=sf is on 1 tracked path(s) (first: nucleus/v3/tools/st.py), and filter.sf.process is 'nv3-no-such-process' (command line:)"; }
expect_ok "SF: both refusals name a -c process filter with no clean command" sp_both
git -C "$r" config filter.sf.clean "cat $t/st.orig"
# Controls: a driver no path uses (a global filter.lfs.* in normal use) and a
# filter attribute with no driver are not refused. The filtered status wrote
# the edited file's stat into the index (see RESULT: a residual of this
# layer), so the file is restored by copy: checkout would take it as clean.
rm -f "$r/.git/info/attributes"
cp "$t/st.orig" "$v3/tools/st.py"
expect_ok "SF: control: a configured driver no tracked path uses is not refused" clean_both st-base
git -C "$r" config --unset filter.sf.clean; echo 'nucleus/v3/tools/st.py filter=sf' >"$r/.git/info/attributes"
expect_ok "SF: control: a filter attribute with no driver configured is not refused" clean_both st-base
rm -f "$r/.git/info/attributes" "$t/st.orig" "$t/st.ref"

# IC core.ignoreCase: on this case-sensitive filesystem, true drops an
# untracked case variant of a tracked file from every untracked listing. The
# environment's false beats the config files; a -c beating it is refused.
printf 'x\n' >"$v3/tools/ST.py"
ic_hidden() { local s o; s=$(plain status --porcelain --untracked-files=all --ignored=matching); o=$(plain ls-files --others); echo "status: $s"; echo "others: $o"; [ -z "$s" ] && [ -z "$o" ]; }
expect_ok "premise: IC: control: plain status lists tools/ST.py" bash -c "git -C '$r' status --porcelain -uall | grep -qxF '?? nucleus/v3/tools/ST.py'"
git -C "$r" config core.ignoreCase true
expect_ok "premise: IC: under core.ignoreCase=true plain status and ls-files --others list nothing" ic_hidden
expect_ok "runner: IC: under core.ignoreCase=true in the repository config tools/ST.py is still protected" gs st-base nucleus/v3/tools/ST.py none
expect_ok "git-env.sh: IC: under core.ignoreCase=true in the repository config status lists tools/ST.py" bash -c "cd '$v3' && . scripts/git-env.sh && git status --porcelain -uall | grep -qxF '?? nucleus/v3/tools/ST.py'"
git -C "$r" config --unset core.ignoreCase
xic="'core.ignorecase'='true'"
ic_cp() { GIT_CONFIG_PARAMETERS=$xic both "core.ignoreCase is 'true' (command line:), over the environment's false"; }
expect_ok "IC: both refusals name an inherited -c core.ignoreCase=true" ic_cp
rm -f "$v3/tools/ST.py"
git -C "$r" checkout -q nucleus/v3-scaffold; git -C "$r" branch -q -D st-base
expect_ok "git state: none after the output-shaping scenarios" no_state
expect_ok "git state: none of the cache state after the output-shaping scenarios" no_state2
expect_ok "git state: none of the output-shaping state after these scenarios" no_state3

# --- forged objects: loose swaps and .idx offsets (X-6-F1-F2-F3-F1) ---------
# A same-uid worker can overwrite a loose object file (or a pack .idx offset)
# so one id returns another object's bytes: the plain diff then hides the
# change, and rev-list --objects --verify-objects exits 0 (it re-hashes only
# objects the walk did not parse). Every scenario below proves the hide with a
# premise row, refuses through each consumer, restores the original bytes and
# checks no_state4. Forging stays inside this throwaway repository (and its
# copies); Python in the forging helpers (zlib for loose objects, struct for
# the .idx) is allowed by the card's Limits. Every id is captured before its
# object is forged (git's read path hash-checks a forged object, so it cannot
# be rev-parsed), and the forged bytes are written only after chmod u+w.
# Two git behaviours shape the rows (probed on 2.47.3): `git status` and
# `git checkout` hash-check HEAD's tree and refuse (fail closed, exit 128) when
# it is forged, while diff, rev-list and pack-objects read the lie. So a row
# that needs HEAD at a forged tip keeps HEAD there from before the forgery,
# scenarios that need HEAD on two branches forge two windows (no checkout
# moves under a forged tree), and the runner's own refusal for a forged HEAD
# tree is worktree_changes' status line — the verify_objects line reaches the
# runner whenever HEAD's tree is honest ((b), (c)) and the twins directly.
# objlo REPO OID: the loose object file's path (premise rows assert it exists).
objlo() { printf '%s\n' "$1/.git/objects/${2:0:2}/${2:2}"; }
forged_oid=() forged_repo=() forged_save=()
# save_obj REPO OID: remember OID's current bytes for restoration and no_state4.
save_obj() { local i=${#forged_oid[@]}; forged_oid+=("$2"); forged_repo+=("$1"); forged_save+=("$t/forge.$i"); cp -p "$(objlo "$1" "$2")" "$t/forge.$i"; }
# forge_from REPO OID SRCOID: OID's loose file becomes SRCOID's bytes (the id
# then lies about its content; SRCOID itself stays honest).
forge_from() { chmod u+w "$(objlo "$1" "$2")"; cp "$(objlo "$1" "$3")" "$(objlo "$1" "$2")"; }
# forge_commit REPO OID BODY: OID's loose file becomes a commit whose raw body
# (tree/parent/author/... lines) is BODY's, zlib-compressed as git stores it.
forge_commit() {
  local lo; lo=$(objlo "$1" "$2"); chmod u+w "$lo"
  PYTHONDONTWRITEBYTECODE=1 python3 - "$lo" "$3" <<'P'
import sys, zlib
lo, body = sys.argv[1], open(sys.argv[2], "rb").read()
open(lo, "wb").write(zlib.compress(b"commit %d\0" % len(body) + body, 1))
P
}
# restore_forged: put every saved object file back (they are read-only again
# after a cp -p restore, so each window reopens its own); no_state4: they are
# all back byte for byte, and no pack moved into $r ((d) repacks only its copy).
restore_forged() { local i; for i in "${!forged_oid[@]}"; do chmod u+w "$(objlo "${forged_repo[$i]}" "${forged_oid[$i]}")"; cp -p "${forged_save[$i]}" "$(objlo "${forged_repo[$i]}" "${forged_oid[$i]}")"; done; }
no_state4() { local i; for i in "${!forged_oid[@]}"; do cmp -s "${forged_save[$i]}" "$(objlo "${forged_repo[$i]}" "${forged_oid[$i]}")" || return 1; done; [ -z "$(ls "$r/.git/objects/pack" 2>/dev/null)" ]; }
# The two twins (git-env.sh's verify_objects and run_card.py's) as commands a
# row can run: each prints the problem lines and exits 0 when there are none.
vsh() { local repo=$1; (cd "$repo/nucleus/v3" && . scripts/git-env.sh && { verify_objects "$2" "$3" || { printf '%s\n' "$VERIFY_PROBLEMS"; return 1; }; }); }
vpy() { PYTHONDONTWRITEBYTECODE=1 python3 - "$1" "$2" "$3" <<'P'
import importlib.util, pathlib, sys
spec = importlib.util.spec_from_file_location("rc", sys.argv[1] + "/nucleus/v3/tools/run_card.py")
rc = importlib.util.module_from_spec(spec); spec.loader.exec_module(rc)
lines = rc.verify_objects(pathlib.Path(sys.argv[1]), sys.argv[2], sys.argv[3])
for l in lines: print(l)
sys.exit(1 if lines else 0)
P
}
# vparity_bad REPO BASE TIP: both twins print the same non-empty problem lines
# ((h), for each forged range); vparity_ok: both print nothing.
vparity_bad() { local sh py; sh=$(vsh "$1" "$2" "$3" | LC_ALL=C sort); py=$(vpy "$1" "$2" "$3" | LC_ALL=C sort); echo "sh: $sh"; echo "py: $py"; [ -n "$sh" ] && [ "$sh" = "$py" ]; }
vparity_ok() { local sh py; sh=$(vsh "$1" "$2" "$3" | LC_ALL=C sort); py=$(vpy "$1" "$2" "$3" | LC_ALL=C sort); echo "sh: $sh"; echo "py: $py"; [ -z "$sh" ] && [ "$sh" = "$py" ]; }
# cmv ID BASE HEAD GATE CLASS: check_merge in merge mode (audit mode is cm).
cmv() { (cd "$v3" && . scripts/core-paths-lib.sh && . scripts/enforce-lib.sh && check_merge "$1" "$2" "$3" merge "$4" "$5"); }
# audit_novr REF: audit-merges.sh with the verify_reachable call dropped (the
# NO-VERIFY-REACHABLE mutant's script), run from inside the repository: the
# copies live under $v3/scripts/.novr because audit-merges.sh cd's to its own
# directory's parent. novr_ok: that audit prints "audit: ok" (the premise that
# the per-merge checks really do skip; of the throwaway refs only (f)'s lie is
# invisible to the walk — (a)-(c)'s forged trees are read by the walk's own
# per-merge checks, which is why their audit rows catch but cannot show it).
audit_novr() {
  local d=$v3/scripts/.novr rc=0
  mkdir -p "$d"; cp "$v3"/scripts/*.sh "$d"/
  sed -i '/^verify_reachable "\$ref" ||/d' "$d/audit-merges.sh"
  (cd "$v3" && bash scripts/.novr/audit-merges.sh "$1") || rc=$?
  rm -rf "$d"
  return "$rc"
}
novr_ok() { local o rc=0; o=$(audit_novr "$1") || rc=$?; echo "$o"; [ "$rc" -eq 0 ] && grep -qF "audit: ok" <<<"$o"; }
# rco BASE TIP: the RANGE-CARD-ONLY mutant's object set (commits of BASE..TIP,
# trees under TIP only) through the real verification pipeline; exit 0 when it
# reports nothing (the (b) premise: the range-only set cannot see the base side).
rco() { PYTHONDONTWRITEBYTECODE=1 python3 - "$v3/tools/run_card.py" "$r" "$1" "$2" <<'P'
import importlib.util, pathlib, shutil, subprocess, sys, tempfile
spec = importlib.util.spec_from_file_location("rc", sys.argv[1]); rc = importlib.util.module_from_spec(spec); spec.loader.exec_module(rc)
wt, base, tip = pathlib.Path(sys.argv[2]), sys.argv[3], sys.argv[4]
env = rc.git_env()
def git(*a):
    p = subprocess.run(["git", *a], cwd=wt, env=env, capture_output=True, text=True)
    if p.returncode: raise SystemExit(f"git {a[0]} failed (exit {p.returncode})")
    return p.stdout.split()
want = git("rev-list", f"{base}..{tip}", "--") + git("rev-list", "--objects", "--no-object-names", "--filter=blob:none", "--no-walk", tip, "--")
d = pathlib.Path(tempfile.mkdtemp())
try:
    probs = rc._verify_ids(wt, "verify_objects", want, d)
finally:
    shutil.rmtree(d, ignore_errors=True)
print(probs)
sys.exit(1 if probs else 0)
P
}
# gempty WORD CMD...: like gshim, but the refused call answers nothing and
# exits 0 (a rev-list that lies empty), so the empty-list refusal is reached;
# gshim's hard failure never gets that far (verify_objects reports the step).
gemptydir=$t/gempty
mkdir -p "$gemptydir"
{ echo '#!/usr/bin/env bash'
  echo 'for a in "$@"; do [ -n "${GEMPTY_FAIL:-}" ] && [ "$a" = "$GEMPTY_FAIL" ] && { '"$gshim_real"' "$@" >/dev/null 2>&1; exit 0; }; done'
  printf 'exec %q "$@"\n' "$gshim_real"; } >"$gemptydir/git"
chmod +x "$gemptydir/git"
gempty() { local w=$1; shift; PATH="$gemptydir:$PATH" GEMPTY_FAIL=$w "$@"; }
# idx_swap IDX A B: in the v2 .idx, A's offset-table slot is pointed at B's
# bytes (A's id then returns B's object). A literal two-entry swap was probed
# and is caught the same way, but it makes both ids lie and the diff shows the
# reverse change; the one-sided form is the loose swap (a) in pack clothing.
idx_swap() { python3 - "$@" <<'P'
import struct, sys
path, a, b = sys.argv[1], bytes.fromhex(sys.argv[2]), bytes.fromhex(sys.argv[3])
d = bytearray(open(path, "rb").read())
n = struct.unpack(">I", d[8 + 255 * 4:8 + 256 * 4])[0]
fan = 8 + 256 * 4
def pos(oid):
    lo, hi = 0, n
    while lo < hi:
        mid = (lo + hi) // 2
        e = bytes(d[fan + 20 * mid:fan + 20 * mid + 20])
        if e < oid: lo = mid + 1
        elif e > oid: hi = mid
        else: return mid
    raise SystemExit(f"oid {oid.hex()} is not in the idx")
off = fan + 20 * n + 4 * n
ia, ib = pos(a), pos(b)
ob = struct.unpack(">I", d[off + 4 * ib:off + 4 * ib + 4])[0]
print(f"idx_swap: {a.hex()} (slot {ia}) -> offset {ob} (slot {ib}'s)")
struct.pack_into(">I", d, off + 4 * ia, ob)
open(path, "wb").write(bytes(d))
P
}

# mc_window_reset P0 BRANCH: after a merge-card window's expect_* row, put the
# repository back whatever merge-card did, so a mutant that wrongly merges
# cannot abort the rest of the suite at the next checkout (X-6-F1-F2-F3-F1-F4
# Step 2). restore_forged goes first — git must not read the worktree under
# forged bytes — then the scratch branch resets --hard to P0 (recorded before
# the window; HEAD never left the branch), then a forced checkout re-syncs the
# worktree to the branch the window started on. Every step is || true and the
# helper returns 0: a failed step must not abort the suite either. No git
# clean: untracked fixtures may exist. restore_forged is idempotent, so the
# window's own restore row still runs after it.
mc_window_reset() {
  local p0=$1 b=$2
  restore_forged || true
  git -C "$r" reset -q --hard "$p0" || true
  git -C "$r" checkout -q -f "$b" || true
  return 0
}
# mc_unmoved AFTER P0: exit 1, printing the move, when the scratch branch tip
# captured right after the window's mc ran (AFTER) is not the P0 recorded
# before it — a merge-card that merged moved it.
mc_unmoved() { [ "$1" = "$2" ] || { echo "branch tip moved: $2 -> $1"; return 1; }; }

# (a) Card side: card/v3-OV's tip root tree file holds the scaffold tip's bytes.
card OV S standard; branch OV nucleus/v3/tools/ov.py; verdict OV S MERGE standard S
ovt=$(git -C "$r" rev-parse "card/v3-OV^{tree}")
sct=$(git -C "$r" rev-parse "nucleus/v3-scaffold^{tree}")
expect_ok "premise: (a) OV's root tree and the scaffold's are loose objects" bash -c "[ -f '$(objlo "$r" "$ovt")' ] && [ -f '$(objlo "$r" "$sct")' ]"
# The throwaway audit ref, merged honestly before anything is forged.
git -C "$r" checkout -q -B scratch/oa nucleus/v3-scaffold
git -C "$r" merge -q --no-ff -m "nucleus v3: merge card/v3-OV (MERGE, gate S/test-model)" card/v3-OV
git -C "$r" checkout -q card/v3-OV
save_obj "$r" "$ovt"
forge_from "$r" "$ovt" "$sct"
a_hidden() { local d; d=$(plain diff --name-only nucleus/v3-scaffold...card/v3-OV); echo "diff: $d"; [ -z "$d" ]; }
expect_ok "premise: (a) plain diff scaffold...card/v3-OV is empty" a_hidden
expect_ok "premise: (a) rev-list --objects --verify-objects over the range exits 0" plain rev-list --objects --verify-objects nucleus/v3-scaffold...card/v3-OV
# The runner refuses through worktree_changes' status: git hash-checks HEAD's
# forged tree and exits 128, so the status listing's refusal comes first and
# names the mismatched id; the verify_objects line itself is pinned by the
# twins and, through the runner, by (b) and (c).
a_runner() { PYTHONDONTWRITEBYTECODE=1 python3 - "$v3/tools/run_card.py" "$r" "$ovt" <<'P'
import importlib.util, pathlib, sys
spec = importlib.util.spec_from_file_location("rc", sys.argv[1]); rc = importlib.util.module_from_spec(spec); spec.loader.exec_module(rc)
bad = rc.scope_violations(pathlib.Path(sys.argv[2]), "nucleus/v3-scaffold", ["nucleus/v3/spikes/"])
print("\n".join(bad) or "(no violations)")
sys.exit(0 if any(f"hash mismatch {sys.argv[3]}" in b and b.endswith("(cannot list changed files)") for b in bad) else 1)
P
}
expect_ok "(a) the runner refuses, naming the mismatched tree" a_runner
expect_ok "(a) merge-mode check_merge refuses (the lie hides the verdict path)" out_has "no nucleus/v3/verdicts/OV.md at the branch tip" -- cmv OV nucleus/v3-scaffold card/v3-OV S standard
expect_ok "(a) check-core-paths exits 1 naming the tree" out_has "check-core-paths: object $ovt fails its hash" -- bash -c "cd '$v3' && bash scripts/check-core-paths.sh nucleus/v3-scaffold card/v3-OV"
expect_ok "(a) audit names the tree (verify_reachable)" out_has "audit: object $ovt fails its hash" -- bash -c "cd '$v3' && bash scripts/audit-merges.sh scratch/oa"
expect_ok "(a) both twins name the tree, identically" vparity_bad "$r" nucleus/v3-scaffold card/v3-OV
restore_forged
# merge-card needs HEAD on scratch/*, and no checkout moves under a forged
# tree: a second forging window with HEAD already in place.
git -C "$r" checkout -q -B scratch/pass1 nucleus/v3-scaffold
p0=$(git -C "$r" rev-parse scratch/pass1); pb=$(git -C "$r" rev-parse --abbrev-ref HEAD)
forge_from "$r" "$ovt" "$sct"
expect_ok "(a) merge-card refuses, naming the tree" out_has "object $ovt fails its hash" -- mc OV
pa=$(git -C "$r" rev-parse "$pb")
mc_window_reset "$p0" "$pb"
expect_ok "forged objects: (a) merge-card left scratch/pass1 unmoved" mc_unmoved "$pa" "$p0"
restore_forged
expect_ok "forged objects: (a) restored" no_state4

# (a″) A forgery that only verification refuses (X-6-F1-F2-F3-F1-F4): T1, the
# scaffold tip's tree plus V's verdict blob (built through a scratch index as
# (a′) builds v2y), becomes V's root-tree file's bytes, and W's root-tree file
# takes the scaffold tip's tree bytes. V's tree then reads as "scaffold +
# verdict", so the verdict blob is found and verifies (it is honest); the tip
# diff-tree W V lists only the verdict; diff scaffold...V lists only the
# verdict, so tools/ov.py is hidden from the S-verdict CORE rule; and the
# Commit is honest. Every rule except verification passes — the discrimination
# (a) lacked: its forged tip tree lost the verdict path, so check_merge
# returned "no verdicts/OV.md at the branch tip" before any verification.
# HEAD stays where (a)'s window left it (scratch/pass1, whose tree sct is not
# forged here); (a)'s fixtures (card OV, ovt, sct) are reused after (a)'s
# window restored them, and every id is captured before its object is forged.
ovvb=$(git -C "$r" rev-parse "card/v3-OV:nucleus/v3/verdicts/OV.md")
ovw=$(git -C "$r" rev-parse "card/v3-OV^")
ovwt=$(git -C "$r" rev-parse "$ovw^{tree}")
GIT_INDEX_FILE=$t/ov3.idx git -C "$r" read-tree "$sct"
GIT_INDEX_FILE=$t/ov3.idx git -C "$r" update-index --add --cacheinfo "100644,$ovvb,nucleus/v3/verdicts/OV.md"
ov3t1=$(GIT_INDEX_FILE=$t/ov3.idx git -C "$r" write-tree)
rm -f "$t/ov3.idx"
expect_ok "premise: (a″) T1 (scaffold tree + the verdict blob) is a new loose tree, distinct from both root trees, and W's tree is loose" bash -c "[ '$ov3t1' != '$sct' ] && [ '$ov3t1' != '$ovt' ] && [ '$ov3t1' != '$ovwt' ] && [ -f '$(objlo "$r" "$ov3t1")' ] && [ -f '$(objlo "$r" "$ovwt")' ]"
save_obj "$r" "$ovt"
save_obj "$r" "$ovwt"
forge_from "$r" "$ovt" "$ov3t1"
forge_from "$r" "$ovwt" "$sct"
a2_hidden() { local d; d=$(plain diff --name-only nucleus/v3-scaffold...card/v3-OV); echo "diff: $d"; [ "$d" = nucleus/v3/verdicts/OV.md ]; }
expect_ok "premise: (a″) plain diff scaffold...card/v3-OV prints exactly the verdict path" a2_hidden
# The discriminating premise: a /tmp copy of the scripts (under $t) with both
# merge-mode verification points removed — the tip check's verify_objects call,
# and check_merge's `changed` call replaced by the unverified _changed_names —
# accepts this forgery in merge mode: only verification refuses. Unlike
# audit_novr's copy (audit-merges.sh cd's home, so it lives under
# $v3/scripts), this one is sourced straight and lives under $t with its
# neighbours; the real scripts are never touched.
a2lib=$t/a2lib
mkdir -p "$a2lib"
cp "$v3"/scripts/*.sh "$a2lib"/
sed -i 's/^  if \[ "$mode" != audit \] && ! verify_objects "$head^1" "$head"; then$/  if false; then/' "$a2lib/enforce-lib.sh"
sed -i 's/^  elif ! changed /  elif ! _changed_names /' "$a2lib/enforce-lib.sh"
a2_libcopy() {
  local o
  o=$(diff "$v3/scripts/enforce-lib.sh" "$a2lib/enforce-lib.sh")
  echo "$o"
  [ "$(grep -c '^< ' <<<"$o")" -eq 2 ] && [ "$(grep -c '^> ' <<<"$o")" -eq 2 ] &&
    grep -qF '>   if false; then' <<<"$o" &&
    grep -qF '>   elif ! _changed_names ' <<<"$o"
}
expect_ok "premise: (a″) the /tmp copy drops exactly the two merge-mode verify points" a2_libcopy
a2_noverifies() {
  local o rc=0
  o=$(cd "$v3" && . "$a2lib/core-paths-lib.sh" && . "$a2lib/enforce-lib.sh" && check_merge OV nucleus/v3-scaffold card/v3-OV merge S standard) || rc=$?
  echo "$o"
  echo "exit $rc"
  [ "$rc" -eq 0 ] && [ -z "$o" ]
}
expect_ok "premise: (a″) without the two verify points merge-mode check_merge accepts the forgery (only verification refuses)" a2_noverifies
rm -rf "$a2lib"
expect_ok "(a″) merge-mode check_merge refuses, naming V's tree" out_has "object $ovt fails its hash" -- cmv OV nucleus/v3-scaffold card/v3-OV S standard
restore_forged
expect_ok "forged objects: (a″) restored" no_state4

# (a') The tip check's own range: a verdict tip that also adds tools/ov2.py,
# with the work commit's root tree swapped for the tip tree minus the verdict,
# so the tip's diff-tree lists only the verdict, every other rule passes (a P
# card) and only verify_objects head^1 head refuses (the NO-VERIFY-TIP row).
card OV2 P core
git -C "$r" checkout -q -b card/v3-OV2 nucleus/v3-scaffold
echo ov2 >"$v3/spikes/ov2.txt"; git -C "$r" add -A; git -C "$r" commit -qm "nucleus v3: OV2 work"
w2=$(git -C "$r" rev-parse HEAD); w2t=$(git -C "$r" rev-parse "HEAD^{tree}")
echo ov2 >"$v3/tools/ov2.py"
printf 'Card: OV2\nClass: core\nCard-gate: P\nVerdict: MERGE\nGate-tier: P\nGate-model: test-model\nGate-session: t\nDate: 2026-11-03\nGate-exit: GATE EXIT=0\nCommit: %s\nMutants: n/a\nFollowups: none\n' "$(git -C "$r" rev-parse HEAD)" >"$v3/verdicts/OV2.md"
git -C "$r" add -A; git -C "$r" commit -qm "nucleus v3: OV2 verdict MERGE"
ov2py=$(git -C "$r" rev-parse "card/v3-OV2:nucleus/v3/tools/ov2.py")
# The tip tree minus the verdict file, built through a scratch index.
GIT_INDEX_FILE=$t/ov2.idx git -C "$r" read-tree "$w2t"
GIT_INDEX_FILE=$t/ov2.idx git -C "$r" update-index --add --cacheinfo "100644,$ov2py,nucleus/v3/tools/ov2.py"
v2y=$(GIT_INDEX_FILE=$t/ov2.idx git -C "$r" write-tree)
rm -f "$t/ov2.idx"
git -C "$r" checkout -q -B scratch/pass1 nucleus/v3-scaffold
p0=$(git -C "$r" rev-parse scratch/pass1); pb=$(git -C "$r" rev-parse --abbrev-ref HEAD)
save_obj "$r" "$w2t"
forge_from "$r" "$w2t" "$v2y"
a2_tip() { local d; d=$(git -C "$r" diff-tree --no-commit-id --name-only -r card/v3-OV2); echo "tip: $d"; [ "$d" = nucleus/v3/verdicts/OV2.md ]; }
expect_ok "premise: (a') under the swap the tip's diff-tree lists only the verdict" a2_tip
expect_ok "(a') merge-card refuses through the tip check, naming the work tree" out_has "object $w2t fails its hash" -- mc OV2
pa=$(git -C "$r" rev-parse "$pb")
mc_window_reset "$p0" "$pb"
expect_ok "forged objects: (a') merge-card left scratch/pass1 unmoved" mc_unmoved "$pa" "$p0"
restore_forged
expect_ok "forged objects: (a') restored" no_state4

# (b) Base side: the scaffold tip's root tree file holds OV's root tree bytes.
git -C "$r" checkout -q -B scratch/ob nucleus/v3-scaffold
git -C "$r" merge -q --no-ff -m "nucleus v3: merge card/v3-OV (MERGE, gate S/test-model)" card/v3-OV
git -C "$r" checkout -q card/v3-OV
save_obj "$r" "$sct"
forge_from "$r" "$sct" "$ovt"
b_hidden() { local d; d=$(plain diff --name-only nucleus/v3-scaffold...card/v3-OV); echo "diff: $d"; [ -z "$d" ]; }
expect_ok "premise: (b) plain diff is empty" b_hidden
expect_ok "premise: (b) a BASE..TIP-only verification (RANGE-CARD-ONLY's set) reports nothing" rco nucleus/v3-scaffold card/v3-OV
expect_ok "(b) the runner's scope check names the scaffold tree" gs nucleus/v3-scaffold - "object $sct fails its hash"
expect_ok "(b) merge-mode check_merge refuses, naming the scaffold tree" out_has "object $sct fails its hash" -- cmv OV nucleus/v3-scaffold card/v3-OV S standard
# (b) check-core-paths: the CORE_PATHS read walks the forged scaffold tree
# first, and git's ls-tree hash-checks it, so the refusal names the failed
# step (fail closed); the object-naming row is (a)'s, where the base tree is
# honest and verify_objects is what refuses.
expect_ok "(b) check-core-paths exits 1 on the forged base tree (its CORE_PATHS read fails closed)" out_has "check-core-paths: cannot read CORE_PATHS from nucleus/v3-scaffold (git ls-tree nucleus/v3-scaffold failed" -- bash -c "cd '$v3' && bash scripts/check-core-paths.sh nucleus/v3-scaffold card/v3-OV"
expect_ok "(b) audit names the scaffold tree (verify_reachable)" out_has "audit: object $sct fails its hash" -- bash -c "cd '$v3' && bash scripts/audit-merges.sh scratch/ob"
expect_ok "(b) both twins name the scaffold tree, identically" vparity_bad "$r" nucleus/v3-scaffold card/v3-OV
restore_forged
# merge-card window: HEAD is the scaffold tip itself, whose tree is the forged
# one; the status read's failure is swallowed by the emptiness test and the
# verify call before check_merge names the object.
git -C "$r" checkout -q -B scratch/pass1 nucleus/v3-scaffold
p0=$(git -C "$r" rev-parse scratch/pass1); pb=$(git -C "$r" rev-parse --abbrev-ref HEAD)
forge_from "$r" "$sct" "$ovt"
expect_ok "(b) merge-card refuses, naming the scaffold tree" out_has "object $sct fails its hash" -- mc OV
pa=$(git -C "$r" rev-parse "$pb")
mc_window_reset "$p0" "$pb"
expect_ok "forged objects: (b) merge-card left scratch/pass1 unmoved" mc_unmoved "$pa" "$p0"
restore_forged
# (b') merge-card's own call: the scratch tip's tree, which no check_merge
# range covers (the merge base is below it): the NO-VERIFY-MERGE-CARD row and
# the only row whose forged object enters the want set through BASE alone.
# HEAD moves to pass2 before the forgery (no checkout moves under one).
git -C "$r" checkout -q scratch/pass2
p2t=$(git -C "$r" rev-parse "scratch/pass2^{tree}")
p0=$(git -C "$r" rev-parse scratch/pass2); pb=$(git -C "$r" rev-parse --abbrev-ref HEAD)
save_obj "$r" "$p2t"
forge_from "$r" "$p2t" "$sct"
expect_ok "(b') merge-card refuses on a forged scratch-tip tree outside the card's range" out_has "object $p2t fails its hash" -- mc A
pa=$(git -C "$r" rev-parse "$pb")
mc_window_reset "$p0" "$pb"
expect_ok "forged objects: (b') merge-card left scratch/pass2 unmoved" mc_unmoved "$pa" "$p0"
restore_forged
git -C "$r" checkout -q -B scratch/pass1 nucleus/v3-scaffold
expect_ok "forged objects: (b) restored" no_state4

# (c) Interior base commit: base history ob is M -> S1 -> S2 (two empty
# commits), card/v3-OI is M -> X -> T; S1's loose commit file becomes a commit
# with S1's tree and X as parent, so the merge base moves into the card and
# X's tools/oi.py drops out of every range read.
git -C "$r" checkout -q -b ob nucleus/v3-scaffold
git -C "$r" commit -q --allow-empty -m "ob S1"
s1=$(git -C "$r" rev-parse HEAD); s1t=$(git -C "$r" rev-parse "HEAD^{tree}")
git -C "$r" commit -q --allow-empty -m "ob S2"
card OI S standard
git -C "$r" checkout -q -b card/v3-OI nucleus/v3-scaffold
echo oi >"$v3/tools/oi.py"; git -C "$r" add -A; git -C "$r" commit -qm "nucleus v3: OI work"
ox=$(git -C "$r" rev-parse HEAD)
printf 'Card: OI\nClass: standard\nCard-gate: S\nVerdict: MERGE\nGate-tier: S\nGate-model: test-model\nGate-session: t\nDate: 2026-11-03\nGate-exit: GATE EXIT=0\nCommit: %s\nMutants: n/a\nFollowups: none\n' "$(git -C "$r" rev-parse HEAD)" >"$v3/verdicts/OI.md"
git -C "$r" add -A; git -C "$r" commit -qm "nucleus v3: OI verdict MERGE"
git -C "$r" checkout -q -B scratch/oc ob
git -C "$r" merge -q --no-ff -m "nucleus v3: merge card/v3-OI (MERGE, gate S/test-model)" card/v3-OI
git -C "$r" checkout -q card/v3-OI
printf 'tree %s\nparent %s\nauthor t <t@t> 1700000000 +0000\ncommitter t <t@t> 1700000000 +0000\n\nob S1 forged\n' "$s1t" "$ox" >"$t/s1.body"
save_obj "$r" "$s1"
forge_commit "$r" "$s1" "$t/s1.body"
c_mb() { local m; m=$(plain merge-base ob card/v3-OI); echo "merge-base: $m"; [ "$m" = "$ox" ]; }
expect_ok "premise: (c) plain merge-base ob card/v3-OI is X" c_mb
c_diff() { local d; d=$(plain diff --name-only ob...card/v3-OI); echo "diff: $d"; ! grep -qxF nucleus/v3/tools/oi.py <<<"$d"; }
expect_ok "premise: (c) plain diff ob...card/v3-OI drops tools/oi.py" c_diff
expect_ok "(c) the runner's scope check names S1" gs ob - "object $s1 fails its hash"
expect_ok "(c) merge-mode check_merge refuses, naming S1" out_has "object $s1 fails its hash" -- cmv OI ob card/v3-OI S standard
expect_ok "(c) audit names S1 (verify_reachable)" out_has "audit: object $s1 fails its hash" -- bash -c "cd '$v3' && bash scripts/audit-merges.sh scratch/oc"
expect_ok "(c) both twins name S1, identically" vparity_bad "$r" ob card/v3-OI
restore_forged
expect_ok "forged objects: (c) restored" no_state4

# (d) Pack .idx offset swap, in a cp -a copy: the repack must not disturb $r's
# loose objects ((k) and (l) forge blobs loose), and git looks in packs first.
dc=$t/dc
cp -a "$r" "$dc"
git -C "$dc" repack -q -a -d
dcidx=$(ls "$dc"/.git/objects/pack/*.idx)
expect_ok "premise: (d) OV's tree and the scaffold's are packed, not loose" bash -c "[ \$(ls '$dc'/.git/objects/pack/*.idx | wc -l) -eq 1 ] && [ ! -e '$(objlo "$dc" "$ovt")' ] && [ ! -e '$(objlo "$dc" "$sct")' ]"
chmod u+w "$dcidx"
idx_swap "$dcidx" "$ovt" "$sct"
d_hidden() { local d rc=0; d=$(git -C "$dc" diff --name-only nucleus/v3-scaffold...card/v3-OV) || rc=$?; echo "diff (rc $rc): $d"; [ "$rc" -eq 0 ] && [ -z "$d" ]; }
expect_ok "premise: (d) the plain diff hides tools/ov.py" d_hidden
d_sh() { vsh "$dc" nucleus/v3-scaffold card/v3-OV; }
expect_ok "(d) the shell twin names the tree" out_has "object $ovt fails its hash" -- d_sh
d_py() { vpy "$dc" nucleus/v3-scaffold card/v3-OV; }
expect_ok "(d) the Python twin names the tree" out_has "object $ovt fails its hash" -- d_py
expect_ok "(d) both twins name the tree, identically" vparity_bad "$dc" nucleus/v3-scaffold card/v3-OV

# (e) gate_committed: G is the gate's verdict commit for verdicts/OG.md, W a
# later worker commit changing it again, R an honest commit on top (HEAD's own
# tree stays honest: git hash-checks it in status). W's root tree file holds
# G's bytes, so the path-limited log skips W and finds G.
card OG S standard
git -C "$r" checkout -q -b card/v3-OG nucleus/v3-scaffold
echo og >"$v3/spikes/og.txt"; git -C "$r" add -A; git -C "$r" commit -qm "nucleus v3: OG work"
printf 'Card: OG\nClass: standard\nCard-gate: S\nVerdict: MERGE\nGate-tier: S\nGate-model: test-model\nGate-session: t\nDate: 2026-11-03\nGate-exit: GATE EXIT=0\nCommit: %s\nMutants: n/a\nFollowups: none\n' "$(git -C "$r" rev-parse HEAD)" >"$v3/verdicts/OG.md"
git -C "$r" add -A; git -C "$r" commit -qm "nucleus v3: OG verdict MERGE"
gog=$(git -C "$r" rev-parse HEAD); gtog=$(git -C "$r" rev-parse "HEAD^{tree}")
echo forged >>"$v3/verdicts/OG.md"; git -C "$r" commit -qam "nucleus v3: OG rework"
wog=$(git -C "$r" rev-parse HEAD); wtog=$(git -C "$r" rev-parse "HEAD^{tree}")
echo og2 >"$v3/spikes/og2.txt"; git -C "$r" add -A; git -C "$r" commit -qm "nucleus v3: OG more work"
save_obj "$r" "$wtog"
forge_from "$r" "$wtog" "$gtog"
# The premise (NO-VERIFY-GATE-COMMITTED is not equivalent): the runner with
# the call dropped reads the lie's history and trusts the forged W.
sed '/^    if _verify_history(wt, base, pre_head):/,+1d' "$v3/tools/run_card.py" >"$t/run_card-nogc.py"
e_premise() { PYTHONDONTWRITEBYTECODE=1 python3 - "$t/run_card-nogc.py" "$r" nucleus/v3-scaffold nucleus/v3/verdicts/OG.md OG "$wog" <<'P'
import importlib.util, pathlib, sys
spec = importlib.util.spec_from_file_location("rc", sys.argv[1]); rc = importlib.util.module_from_spec(spec); spec.loader.exec_module(rc)
wt, base, own, cid, pre = pathlib.Path(sys.argv[2]), *sys.argv[3:7]
print(rc.gate_committed(wt, base, own, cid, pre))
sys.exit(0 if rc.gate_committed(wt, base, own, cid, pre) else 1)
P
}
expect_ok "premise: (e) without the call gate_committed trusts the forged W" e_premise
e_row() { PYTHONDONTWRITEBYTECODE=1 python3 - "$v3/tools/run_card.py" "$r" nucleus/v3-scaffold nucleus/v3/verdicts/OG.md OG "$wog" "$wtog" <<'P'
import importlib.util, pathlib, sys
spec = importlib.util.spec_from_file_location("rc", sys.argv[1]); rc = importlib.util.module_from_spec(spec); spec.loader.exec_module(rc)
wt, base, own, cid, pre, wtree = pathlib.Path(sys.argv[2]), *sys.argv[3:8]
probs = rc.verify_objects(wt, base, pre)
gc = rc.gate_committed(wt, base, own, cid, pre)
bad = rc.scope_violations(wt, base, ["nucleus/v3/spikes/"], cid, pre)
print("verify:", probs)
print("gate_committed:", gc)
print("\n".join(bad) or "(no violations)")
sys.exit(0 if any(f"object {wtree} fails its hash" in p for p in probs) and gc is False and own + " (protected)" in bad else 1)
P
}
expect_ok "(e) with the call: verify names W's tree, gate_committed is False, the verdict is protected" e_row
restore_forged
expect_ok "forged objects: (e) restored" no_state4

# (f) Audit listing: a titled merge, then a direct commit D changing
# nucleus/v3/tools/od.py; D's root tree file holds D^'s bytes, so the
# path-limited first-parent listing is TREESAME at D and never yields it.
card OD S standard; branch OD nucleus/v3/spikes/od.txt; verdict OD S MERGE standard S
git -C "$r" checkout -q -B scratch/of nucleus/v3-scaffold
git -C "$r" merge -q --no-ff -m "nucleus v3: merge card/v3-OD (MERGE, gate S/test-model)" card/v3-OD
echo od >"$v3/tools/od.py"; git -C "$r" add -A; git -C "$r" commit -qm "direct commit D"
dsha=$(git -C "$r" rev-parse HEAD); dtree=$(git -C "$r" rev-parse "HEAD^{tree}")
dptree=$(git -C "$r" rev-parse "$dsha^^{tree}")
save_obj "$r" "$dtree"
forge_from "$r" "$dtree" "$dptree"
f_listed() { local o; o=$(plain rev-list --first-parent scratch/of -- :/nucleus/v3); echo "$o"; ! grep -qxF "$dsha" <<<"$o"; }
expect_ok "premise: (f) the listing does not yield D" f_listed
expect_ok "premise: (f) without verify_reachable the audit prints ok" novr_ok scratch/of
expect_ok "(f) audit exits 1 naming D's tree" out_has "audit: object $dtree fails its hash" -- bash -c "cd '$v3' && bash scripts/audit-merges.sh scratch/of"
restore_forged
expect_ok "forged objects: (f) restored" no_state4

# (f') The evil-merge row: the merge's own tree carries an edit (tools/od2.py)
# and its first parent's root tree file holds the merge's tree bytes, so
# merge-tree reproduces the evil tree and clean_merge passes under the lie.
# The first parent commits only README (outside nucleus/v3) so the listing
# stays quiet; under the lie the listing reads the forged tree and names the
# first parent as a direct commit, so the per-merge checks cannot all pass and
# only verify_reachable catches this forgery (the premise that the per-merge
# checks skip is (f)'s).
git -C "$r" checkout -q -B scratch/ofp nucleus/v3-scaffold
echo ofp >>"$r/README"; git -C "$r" commit -qam "OFP first parent"
ct=$(git -C "$r" rev-parse "HEAD^{tree}")
git -C "$r" merge -q --no-ff --no-commit card/v3-OD >/dev/null 2>&1 || true
echo od2 >"$v3/tools/od2.py"; git -C "$r" add -A; git -C "$r" commit -qm "nucleus v3: merge card/v3-OD (MERGE, gate S/test-model)"
mt=$(git -C "$r" rev-parse "HEAD^{tree}")
save_obj "$r" "$ct"
forge_from "$r" "$ct" "$mt"
expect_ok "(f') audit exits 1 naming the first parent's tree" out_has "audit: object $ct fails its hash" -- bash -c "cd '$v3' && bash scripts/audit-merges.sh scratch/ofp"
restore_forged
expect_ok "forged objects: (f') restored" no_state4

# (g) Honest control: both twins return nothing on honest ranges, a card that
# merged before the forgeries still merges through verification, the audit
# accepts it, and a range with no merge base verifies clean.
expect_ok "(g) both twins are clean on scaffold...card/v3-OV" vparity_ok "$r" nucleus/v3-scaffold card/v3-OV
oroot=$(git -C "$r" commit-tree "$(git -C "$r" rev-parse nucleus/v3-scaffold^{tree})" -m "orphan root")
expect_ok "(g) both twins are clean on a range with no merge base" vparity_ok "$r" "$oroot" card/v3-A
card OVC S standard; branch OVC nucleus/v3/spikes/ovc.txt; verdict OVC S MERGE standard S
scratch
expect_ok "(g) an honest card still merges through verification" mc OVC
expect_ok "(g) the audit accepts the merge" bash -c "cd '$v3' && bash scripts/audit-merges.sh scratch/pass1"

# (i) Failure is a problem: a git step verify_objects depends on failing is a
# problem line, never "nothing found"; the runner and merge-mode check_merge
# refuse. gshim fails the call; gempty makes it answer nothing and exit 0
# (the empty listing an honest repository cannot produce).
expect_ok "(i) the shell twin reports a failed pack-objects" out_has "verify_objects: git pack-objects failed (exit 128; cannot verify objects)" -- gshim pack-objects vsh "$r" nucleus/v3-scaffold card/v3-A
expect_ok "(i) the Python twin reports a failed pack-objects" out_has "verify_objects: git pack-objects failed (exit 128; cannot verify objects)" -- gshim pack-objects vpy "$r" nucleus/v3-scaffold card/v3-A
expect_ok "(i) a failed index-pack is a problem" out_has "verify_objects: git index-pack failed (exit 128; cannot verify objects)" -- gshim index-pack vsh "$r" nucleus/v3-scaffold card/v3-A
expect_ok "(i) a failed show-index is a problem" out_has "verify_objects: git show-index failed (exit 128; cannot verify objects)" -- gshim show-index vsh "$r" nucleus/v3-scaffold card/v3-A
expect_ok "(i) a failed rev-list is a problem" out_has "verify_objects: git rev-list failed (exit 128; cannot verify objects)" -- gshim rev-list vsh "$r" nucleus/v3-scaffold card/v3-A
expect_ok "(i) the runner's scope check names the failed verify step" gshim pack-objects gs nucleus/v3-scaffold - "verify_objects: git pack-objects failed"
expect_ok "(i) merge-mode check_merge refuses on a failed verify step" out_has "verify_objects: git pack-objects failed (exit 128; cannot verify objects)" -- gshim pack-objects cmv A nucleus/v3-scaffold card/v3-A S standard
expect_ok "(i) a rev-list that answers nothing is a problem (shell)" out_has "verify_objects: the object listing is empty (cannot verify objects)" -- gempty rev-list vsh "$r" nucleus/v3-scaffold card/v3-A
expect_ok "(i) a rev-list that answers nothing is a problem (Python)" out_has "verify_objects: the object listing is empty (cannot verify objects)" -- gempty rev-list vpy "$r" nucleus/v3-scaffold card/v3-A

# (j) Cost: verify_objects on the widest range (the scratch root to a
# two-merge pass) and verify_reachable over the scaffold, each under 2 s; the
# measured times print in the row output.
j_obj() { local t0 t1 ms rc=0; t0=$(date +%s%N); (cd "$v3" && . scripts/git-env.sh && verify_objects "$root" scratch/pass2) || rc=$?; t1=$(date +%s%N); ms=$(((t1 - t0) / 1000000)); echo "     (j) verify_objects $root...scratch/pass2: ${ms} ms"; [ "$rc" -eq 0 ] && [ "$ms" -lt 2000 ]; }
expect_ok "(j) verify_objects on the widest range is under 2 s" j_obj
j_reach() { local t0 t1 ms rc=0; t0=$(date +%s%N); (cd "$v3" && . scripts/git-env.sh && verify_reachable nucleus/v3-scaffold) || rc=$?; t1=$(date +%s%N); ms=$(((t1 - t0) / 1000000)); echo "     (j) verify_reachable nucleus/v3-scaffold: ${ms} ms"; [ "$rc" -eq 0 ] && [ "$ms" -lt 2000 ]; }
expect_ok "(j) verify_reachable on the scaffold is under 2 s" j_reach

# (k) CORE_PATHS at BASE (the w7 reproduction): the base's CORE_PATHS blob
# file holds the bytes of a copy without the tools/ line, and an S verdict on
# tools/k.py. Built in a copy: the base commit is this scenario's own, so the
# forged blob is not the scaffold's.
kb=$t/kb
cp -a "$r" "$kb"
git -C "$kb" checkout -q -b kb-base nucleus/v3-scaffold
printf '# kb CORE_PATHS\n' >>"$kb/nucleus/v3/CORE_PATHS"
git -C "$kb" commit -qam "kb-base CORE_PATHS"
git -C "$kb" checkout -q -b card/v3-OK kb-base
echo k >"$kb/nucleus/v3/tools/k.py"; git -C "$kb" add -A; git -C "$kb" commit -qm "nucleus v3: OK work"
printf 'Card: OK\nClass: standard\nCard-gate: S\nVerdict: MERGE\nGate-tier: S\nGate-model: test-model\nGate-session: t\nDate: 2026-11-03\nGate-exit: GATE EXIT=0\nCommit: %s\nMutants: n/a\nFollowups: none\n' "$(git -C "$kb" rev-parse HEAD)" >"$kb/nucleus/v3/verdicts/OK.md"
git -C "$kb" add -A; git -C "$kb" commit -qm "nucleus v3: OK verdict MERGE"
git -C "$kb" checkout -q -B scratch/ok kb-base
git -C "$kb" merge -q --no-ff -m "nucleus v3: merge card/v3-OK (MERGE, gate S/test-model)" card/v3-OK
kbid=$(git -C "$kb" rev-parse kb-base:nucleus/v3/CORE_PATHS)
grep -v '^tools/$' "$kb/nucleus/v3/CORE_PATHS" >"$t/cp.notools"
kbnt=$(git -C "$kb" hash-object -w "$t/cp.notools")
save_obj "$kb" "$kbid"
forge_from "$kb" "$kbid" "$kbnt"
k_premise() { local o; o=$(git -C "$kb" cat-file blob "$kbid"); echo "$o"; ! grep -qxF tools/ <<<"$o"; }
expect_ok "premise: (k) plain cat-file shows no tools/ line" k_premise
kbv="blob $kbid (kb-base:nucleus/v3/CORE_PATHS) fails its hash"
expect_ok "(k) merge-mode check_merge refuses, naming the blob" out_has "$kbv" -- bash -c "cd '$kb/nucleus/v3' && . scripts/core-paths-lib.sh && . scripts/enforce-lib.sh && check_merge OK kb-base card/v3-OK merge S standard"
expect_ok "(k) audit-mode check_merge refuses, naming the blob" out_has "$kbv" -- bash -c "cd '$kb/nucleus/v3' && . scripts/core-paths-lib.sh && . scripts/enforce-lib.sh && check_merge OK kb-base card/v3-OK audit"
expect_ok "(k) check-core-paths exits 1, naming the blob" out_has "check-core-paths: cannot read CORE_PATHS from kb-base (blob $kbid" -- bash -c "cd '$kb/nucleus/v3' && bash scripts/check-core-paths.sh kb-base card/v3-OK"
k_audit() { local o rc=0; o=$(cd "$kb/nucleus/v3" && bash scripts/audit-merges.sh scratch/ok 2>&1) || rc=$?; echo "$o"; [ "$rc" -ne 0 ] && grep -qF "blob $kbid" <<<"$o"; }
expect_ok "(k) audit on a ref merging it exits 1, naming the blob" k_audit
restore_forged
expect_ok "forged objects: (k) restored" no_state4

# (l) Verdict at HEAD: card/v3-OL is a P card with a tools/ change and an
# honest Gate-tier: S verdict; its verdict blob file is forged to read
# Gate-tier: P, which without the check would pass the P-gate rule.
card OL P core
git -C "$r" checkout -q -b card/v3-OL nucleus/v3-scaffold
echo ol >"$v3/tools/ol.py"; git -C "$r" add -A; git -C "$r" commit -qm "nucleus v3: OL work"
printf 'Card: OL\nClass: core\nCard-gate: P\nVerdict: MERGE\nGate-tier: S\nGate-model: test-model\nGate-session: t\nDate: 2026-11-03\nGate-exit: GATE EXIT=0\nCommit: %s\nMutants: n/a\nFollowups: none\n' "$(git -C "$r" rev-parse HEAD)" >"$v3/verdicts/OL.md"
git -C "$r" add -A; git -C "$r" commit -qm "nucleus v3: OL verdict MERGE"
olid=$(git -C "$r" rev-parse "card/v3-OL:nucleus/v3/verdicts/OL.md")
sed 's/^Gate-tier: S$/Gate-tier: P/' "$v3/verdicts/OL.md" >"$t/ol.p"
olf=$(git -C "$r" hash-object -w "$t/ol.p")
git -C "$r" checkout -q -B scratch/pass1 nucleus/v3-scaffold
p0=$(git -C "$r" rev-parse scratch/pass1); pb=$(git -C "$r" rev-parse --abbrev-ref HEAD)
save_obj "$r" "$olid"
forge_from "$r" "$olid" "$olf"
l_premise() { local o; o=$(git -C "$r" show "card/v3-OL:nucleus/v3/verdicts/OL.md"); echo "$o"; grep -qxF "Gate-tier: P" <<<"$o"; }
expect_ok "premise: (l) plain git show reads Gate-tier: P" l_premise
olv="blob $olid (card/v3-OL:nucleus/v3/verdicts/OL.md) fails its hash"
expect_ok "(l) merge-mode check_merge refuses, naming the blob" out_has "$olv" -- cmv OL nucleus/v3-scaffold card/v3-OL P core
expect_ok "(l) audit-mode check_merge refuses, naming the blob" out_has "$olv" -- bash -c "cd '$v3' && . scripts/core-paths-lib.sh && . scripts/enforce-lib.sh && check_merge OL nucleus/v3-scaffold card/v3-OL audit"
expect_ok "(l) merge-card refuses, naming the blob" out_has "$olv" -- mc OL
pa=$(git -C "$r" rev-parse "$pb")
mc_window_reset "$p0" "$pb"
expect_ok "forged objects: (l) merge-card left scratch/pass1 unmoved" mc_unmoved "$pa" "$p0"
restore_forged
expect_ok "forged objects: (l) restored" no_state4

# (m) Honest control for the blob checks: the scaffold's CORE_PATHS and an
# honest verdict blob verify clean (every existing check_merge,
# check-core-paths, merge-card and audit row above now runs through
# verify_blob and verify_objects).
m_blobs() { (cd "$v3" && . scripts/git-env.sh && verify_blob "$(git rev-parse nucleus/v3-scaffold:nucleus/v3/CORE_PATHS)" "$t/mv3" "nucleus/v3-scaffold:nucleus/v3/CORE_PATHS" && verify_blob "$(git rev-parse card/v3-OV:nucleus/v3/verdicts/OV.md)" "$t/mv3" "card/v3-OV:nucleus/v3/verdicts/OV.md"); }
expect_ok "(m) honest CORE_PATHS and verdict blobs verify clean" m_blobs

# (n) verify_blob fail-closed: a hash-object that cannot run is a problem
# line, and the consumers refuse, never reading the verdict as verified.
expect_ok "(n) check_merge refuses when hash-object fails (merge mode)" out_has "verify_blob: git hash-object failed (exit 128; cannot verify objects)" -- gshim hash-object cmv A nucleus/v3-scaffold card/v3-A S standard
expect_ok "(n) check_merge refuses when hash-object fails (audit mode)" out_has "verify_blob: git hash-object failed (exit 128; cannot verify objects)" -- gshim hash-object bash -c "cd '$v3' && . scripts/core-paths-lib.sh && . scripts/enforce-lib.sh && check_merge A nucleus/v3-scaffold card/v3-A audit"
n_ccp() { local o rc=0; o=$(gshim hash-object bash -c "cd '$v3' && bash scripts/check-core-paths.sh nucleus/v3-scaffold card/v3-B" 2>&1) || rc=$?; echo "$o"; [ "$rc" -ne 0 ] && grep -qF "verify_blob: git hash-object failed" <<<"$o"; }
expect_ok "(n) check-core-paths exits 1, not 0, when hash-object fails" n_ccp

# (e')-(e'''') gate_committed's own history set (X-6-F1-F2-F3-F1-F2): the log
# gate_committed trusts compares every interior commit's tree with its parent's,
# and row (e) only forged W's tip tree, an endpoint verify_objects already
# held. Here G, the gate's verdict commit, gets W's bytes: with G's whole root
# tree file forged in (e'), or only its nucleus/v3/verdicts subtree in (e''),
# W reads TREESAME at the verdict, the log skips it and returns G, whose
# subject is the gate's, while verify_objects(base, R) still sees nothing — the
# interior trees were never in its set. Both forgeries sit in this scratch
# repository, are preceded by id captures and premise rows, and are restored
# (no_state4); the new fixtures write the full 40-hex Commit: (git rev-parse
# HEAD). vhist REPO BASE TIP: _verify_history as a command, as vpy is for
# verify_objects (its problem lines; exit 0 when there are none). gc_eq RUNNER
# REPO BASE OWN CARD PRE WANT: gate_committed in the unit mode (ledger None);
# prints what it returned, exits 0 when it equals WANT (True/False).
vhist() { PYTHONDONTWRITEBYTECODE=1 python3 - "$1" "$2" "$3" <<'P'
import importlib.util, pathlib, sys
spec = importlib.util.spec_from_file_location("rc", sys.argv[1] + "/nucleus/v3/tools/run_card.py")
rc = importlib.util.module_from_spec(spec); spec.loader.exec_module(rc)
lines = rc._verify_history(pathlib.Path(sys.argv[1]), sys.argv[2], sys.argv[3])
for l in lines: print(l)
sys.exit(1 if lines else 0)
P
}
gc_eq() { PYTHONDONTWRITEBYTECODE=1 python3 - "$@" <<'P'
import importlib.util, pathlib, sys
spec = importlib.util.spec_from_file_location("rc", sys.argv[1]); rc = importlib.util.module_from_spec(spec); spec.loader.exec_module(rc)
got = rc.gate_committed(pathlib.Path(sys.argv[2]), sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6])
print("gate_committed:", got)
sys.exit(0 if got is (sys.argv[7] == "True") else 1)
P
}

# (e') The interior gate commit: card/v3-OH is work, G = a verdict REWORK
# commit, W = a worker commit rewriting the verdict to MERGE, R = an honest
# commit (HEAD stays at R, whose tree is honest, so status works). G's root
# tree file holds W's root-tree bytes.
card OH S standard
git -C "$r" checkout -q -b card/v3-OH nucleus/v3-scaffold
echo oh >"$v3/spikes/oh.txt"; git -C "$r" add -A; git -C "$r" commit -qm "nucleus v3: OH work"
ohw=$(git -C "$r" rev-parse HEAD)
printf 'Card: OH\nClass: standard\nCard-gate: S\nVerdict: REWORK\nGate-tier: S\nGate-model: test-model\nGate-session: t\nDate: 2026-11-03\nGate-exit: GATE EXIT=0\nCommit: %s\nMutants: n/a\nFollowups: none\n' "$ohw" >"$v3/verdicts/OH.md"
git -C "$r" add -A; git -C "$r" commit -qm "nucleus v3: OH verdict REWORK"
goh=$(git -C "$r" rev-parse HEAD); gtoh=$(git -C "$r" rev-parse "HEAD^{tree}")
gvoh=$(git -C "$r" rev-parse "HEAD:nucleus/v3/verdicts")
sed 's/^Verdict: REWORK$/Verdict: MERGE/' "$v3/verdicts/OH.md" >"$t/oh.merge"
cp "$t/oh.merge" "$v3/verdicts/OH.md"
git -C "$r" add -A; git -C "$r" commit -qm "nucleus v3: OH work"
woh=$(git -C "$r" rev-parse HEAD); wtoh=$(git -C "$r" rev-parse "HEAD^{tree}")
wvoh=$(git -C "$r" rev-parse "HEAD:nucleus/v3/verdicts")
echo oh2 >"$v3/spikes/oh.txt"; git -C "$r" add -A; git -C "$r" commit -qm "nucleus v3: OH more work"
roh=$(git -C "$r" rev-parse HEAD)
expect_ok "premise: (e') G's and W's root trees are loose objects" bash -c "[ -f '$(objlo "$r" "$gtoh")' ] && [ -f '$(objlo "$r" "$wtoh")' ]"
save_obj "$r" "$gtoh"
forge_from "$r" "$gtoh" "$wtoh"
e1_log() { local h; h=$(plain log -1 --format=%H nucleus/v3-scaffold.."$roh" -- nucleus/v3/verdicts/OH.md); echo "log: $h"; [ "$h" = "$goh" ]; }
expect_ok "premise: (e') the plain log skips W and returns G (the hide works)" e1_log
# Step A reverted at the call site (verify_objects' set), built like row (e)'s
# premise: the old set is not enough — this premise is the card's proof.
sed 's/^    if _verify_history(wt, base, pre_head):/    if verify_objects(wt, base, pre_head):/' "$v3/tools/run_card.py" >"$t/run_card-e1.py"
e1_premise() { PYTHONDONTWRITEBYTECODE=1 python3 - "$t/run_card-e1.py" "$r" nucleus/v3-scaffold nucleus/v3/verdicts/OH.md OH "$roh" <<'P'
import importlib.util, pathlib, sys
spec = importlib.util.spec_from_file_location("rc", sys.argv[1]); rc = importlib.util.module_from_spec(spec); spec.loader.exec_module(rc)
wt, base, own, cid, pre = pathlib.Path(sys.argv[2]), *sys.argv[3:7]
gc = rc.gate_committed(wt, base, own, cid, pre)
print("gate_committed:", gc)
print("verify_objects:", rc.verify_objects(wt, base, pre))
sys.exit(0 if gc is True and rc.verify_objects(wt, base, pre) == [] else 1)
P
}
expect_ok "premise: (e') Step A reverted: gate_committed trusts the forged G and verify_objects(base, R) is []" e1_premise
expect_ok "(e') gate_committed(wt, base, f, OH, R) is False" gc_eq "$v3/tools/run_card.py" "$r" nucleus/v3-scaffold nucleus/v3/verdicts/OH.md OH "$roh" False
expect_ok "(e') the same with pre_head = W is False" gc_eq "$v3/tools/run_card.py" "$r" nucleus/v3-scaffold nucleus/v3/verdicts/OH.md OH "$woh" False
e1_scope() { PYTHONDONTWRITEBYTECODE=1 python3 - "$v3/tools/run_card.py" "$r" nucleus/v3-scaffold nucleus/v3/verdicts/OH.md OH "$roh" <<'P'
import importlib.util, pathlib, sys
spec = importlib.util.spec_from_file_location("rc", sys.argv[1]); rc = importlib.util.module_from_spec(spec); spec.loader.exec_module(rc)
bad = rc.scope_violations(pathlib.Path(sys.argv[2]), sys.argv[3], ["nucleus/v3/spikes/"], sys.argv[4], sys.argv[5])
print("\n".join(bad) or "(no violations)")
sys.exit(0 if "nucleus/v3/verdicts/OH.md (protected)" in bad else 1)
P
}
expect_ok "(e') scope_violations lists nucleus/v3/verdicts/OH.md (protected)" e1_scope
expect_ok "(e') the helper names G's root tree" out_has "object $gtoh fails its hash" -- vhist "$r" nucleus/v3-scaffold "$roh"
restore_forged
expect_ok "forged objects: (e') restored" no_state4

# (e'') A subtree: the same G and W (they differ only under
# nucleus/v3/verdicts/), but G's nucleus/v3/verdicts subtree file holds W's
# subtree bytes and every root tree stays honest — the hide narrowed to the one
# subtree the log reads at f.
expect_ok "premise: (e'') G's and W's verdicts subtrees are loose objects" bash -c "[ -f '$(objlo "$r" "$gvoh")' ] && [ -f '$(objlo "$r" "$wvoh")' ]"
save_obj "$r" "$gvoh"
forge_from "$r" "$gvoh" "$wvoh"
e2_log() { local h; h=$(plain log -1 --format=%H nucleus/v3-scaffold.."$roh" -- nucleus/v3/verdicts/OH.md); echo "log: $h"; [ "$h" = "$goh" ]; }
expect_ok "premise: (e'') the plain log still skips W and returns G" e2_log
expect_ok "(e'') gate_committed(wt, base, f, OH, R) is False" gc_eq "$v3/tools/run_card.py" "$r" nucleus/v3-scaffold nucleus/v3/verdicts/OH.md OH "$roh" False
expect_ok "(e'') the helper names the forged subtree" out_has "object $gvoh fails its hash" -- vhist "$r" nucleus/v3-scaffold "$roh"
restore_forged
expect_ok "forged objects: (e'') restored" no_state4

# (e''') Honest controls: on the unforged card/v3-OH the log's last commit at
# the verdict is W, a worker commit (no verdict subject), so gate_committed is
# False; on card/v3-OJ no commit after the gate's verdict touches it, so
# gate_committed is True; and the helper is clean on a scaffold-only range.
expect_ok "(e''') on the unforged card/v3-OH gate_committed(wt, base, f, OH, R) is False (W, not G)" gc_eq "$v3/tools/run_card.py" "$r" nucleus/v3-scaffold nucleus/v3/verdicts/OH.md OH "$roh" False
card OJ S standard
git -C "$r" checkout -q -b card/v3-OJ nucleus/v3-scaffold
echo oj >"$v3/spikes/oj.txt"; git -C "$r" add -A; git -C "$r" commit -qm "nucleus v3: OJ work"
printf 'Card: OJ\nClass: standard\nCard-gate: S\nVerdict: MERGE\nGate-tier: S\nGate-model: test-model\nGate-session: t\nDate: 2026-11-03\nGate-exit: GATE EXIT=0\nCommit: %s\nMutants: n/a\nFollowups: none\n' "$(git -C "$r" rev-parse HEAD)" >"$v3/verdicts/OJ.md"
git -C "$r" add -A; git -C "$r" commit -qm "nucleus v3: OJ verdict MERGE"
goj=$(git -C "$r" rev-parse HEAD)
expect_ok "(e''') on card/v3-OJ, whose last verdict commit is the gate's, gate_committed is True" gc_eq "$v3/tools/run_card.py" "$r" nucleus/v3-scaffold nucleus/v3/verdicts/OJ.md OJ "$goj" True
expect_ok "(e''') the helper returns nothing on nucleus/v3-scaffold~1..nucleus/v3-scaffold" vhist "$r" nucleus/v3-scaffold~1 nucleus/v3-scaffold

# (e'''') Failure is a problem: only the trees listing fails — gshim refuses
# the one rev-list call carrying --stdin (the helper's other listings and every
# other git step run), so the row pins that listing's own failure, not an
# earlier one's — and gate_committed on the honest card/v3-OJ is False.
expect_ok "(e'''') a failed trees listing makes gate_committed False (never True)" gshim --stdin gc_eq "$v3/tools/run_card.py" "$r" nucleus/v3-scaffold nucleus/v3/verdicts/OJ.md OJ "$goj" False

# Cost: the helper on the same widest range as (j), under 2 s; the measured
# time prints in the row's output.
e_cost() { local t0 t1 ms rc=0; t0=$(date +%s%N); vhist "$r" "$root" scratch/pass2 || rc=$?; t1=$(date +%s%N); ms=$(((t1 - t0) / 1000000)); echo "     (e cost) _verify_history $root...scratch/pass2: ${ms} ms"; [ "$rc" -eq 0 ] && [ "$ms" -lt 2000 ]; }
expect_ok "(e cost) _verify_history on the widest range is under 2 s" e_cost

# --- runner: refs outside the card branch (X-6-F1-F3-F1) -------------------
# The ref store is shared by every worktree, so ref_violations refuses only ref
# changes it can pin on this run (R1) or that touch a protected ref (R2), and
# lets concurrent automation through. A template repository: nucleus/v3-scaffold
# (two commits), main and tag t0 at its tip, nucleus/zz, another card branch
# card/v3-ZZ, an origin ref, and card/v3-RF one worker commit past pre_head.
rf0=$t/rf0
git init -q -b nucleus/v3-scaffold "$rf0"
git -C "$rf0" commit -q --allow-empty -m "rf base 0"
git -C "$rf0" commit -q --allow-empty -m "rf base 1"
git -C "$rf0" branch main && git -C "$rf0" tag t0 && git -C "$rf0" branch nucleus/zz && git -C "$rf0" branch card/v3-ZZ
git -C "$rf0" update-ref refs/remotes/origin/nucleus/v3-scaffold nucleus/v3-scaffold
git -C "$rf0" checkout -q -b card/v3-RF
git -C "$rf0" commit -q --allow-empty -m "nucleus v3: RF work"
# rf WANT CMD: in a fresh copy of the template, snapshot the refs, run CMD there,
# and ref_violations (pre_head card/v3-RF~1) must name exactly the refs in WANT
# (comma-separated; "none" for nothing).
rf() { local d; d=$(mktemp -d "$t/rf.XXXX"); cp -a "$rf0/." "$d"; PYTHONDONTWRITEBYTECODE=1 python3 - "$v3/tools/run_card.py" "$d" "$@" <<'P'
import importlib.util, pathlib, subprocess, sys
spec = importlib.util.spec_from_file_location("rc", sys.argv[1]); rc = importlib.util.module_from_spec(spec); spec.loader.exec_module(rc)
wt = pathlib.Path(sys.argv[2])
pre = subprocess.run(["git", "rev-parse", "card/v3-RF~1"], cwd=wt, capture_output=True, text=True, check=True).stdout.strip()
before = rc.ref_snapshot(wt)
subprocess.run(["bash", "-euc", sys.argv[4]], cwd=wt, check=True)
bad = rc.ref_violations(wt, before, "card/v3-RF", pre)
got = sorted({b.split(": ")[0] for b in bad})
want = sorted(w for w in sys.argv[3].split(",") if w != "none")
print("\n".join(bad))
print("named:", got, "expected:", want)
sys.exit(0 if got == want else 1)
P
}
expect_ok "runner refs: RF1 nothing changed is no violation" rf none true
expect_ok "runner refs: RF2 the card branch advancing is no violation" rf none 'git commit -q --allow-empty -m "nucleus v3: RF more"'
expect_ok "runner refs: RF3 fetch +card:main is a violation" rf refs/heads/main 'git fetch -q . +card/v3-RF:main'
expect_ok "runner refs: RF4 another card branch at this run's commit is a violation" rf refs/heads/card/v3-OTHER 'git branch card/v3-OTHER card/v3-RF'
expect_ok "runner refs: RF5 tag -f to an old commit is a violation" rf refs/tags/t0 'git tag -f t0 nucleus/v3-scaffold~1'
expect_ok "runner refs: RF6 a new tag at an old commit is a violation" rf refs/tags/t1 'git tag t1 nucleus/v3-scaffold~1'
expect_ok "runner refs: RF7 rewinding nucleus/v3-scaffold is a violation" rf refs/heads/nucleus/v3-scaffold 'git update-ref refs/heads/nucleus/v3-scaffold nucleus/v3-scaffold~1'
expect_ok "runner refs: RF8 deleting nucleus/zz is a violation" rf refs/heads/nucleus/zz 'git update-ref -d refs/heads/nucleus/zz'
# RF9: what other runners, merge passes, pushes and sessions do meanwhile (REF-STRICT
# refuses this, which is why the rule is attributable and not "any ref").
rf9='o=$(git commit-tree "nucleus/v3-scaffold^{tree}" -p card/v3-ZZ -m "nucleus v3: ZZ work")
git update-ref refs/heads/card/v3-ZZ "$o"; git update-ref refs/heads/scratch/passZ "$o"
m=$(git commit-tree "nucleus/v3-scaffold^{tree}" -p nucleus/v3-scaffold -p "$o" -m "nucleus v3: merge card/v3-ZZ")
git update-ref refs/heads/nucleus/v3-scaffold "$m"; git update-ref refs/remotes/origin/nucleus/v3-scaffold "$m"; git update-ref refs/stash "$o"'
expect_ok "runner refs: RF9 concurrent legitimate ref moves are no violation" rf none "$rf9"
# RF10: a snapshot outside a repository raises, and a git failure in the check is
# one entry: neither reads as "no changes" (REF-FAIL-OPEN).
mkdir -p "$t/notrepo"
rf10() { GIT_CEILING_DIRECTORIES=$t PYTHONDONTWRITEBYTECODE=1 python3 - "$v3/tools/run_card.py" "$t/notrepo" "$rf0" <<'P'
import importlib.util, pathlib, subprocess, sys
spec = importlib.util.spec_from_file_location("rc", sys.argv[1]); rc = importlib.util.module_from_spec(spec); spec.loader.exec_module(rc)
try:
    snap = rc.ref_snapshot(pathlib.Path(sys.argv[2]))
    print("snapshot outside a repository returned", snap)
    raised = False
except subprocess.CalledProcessError:
    raised = True
wt = pathlib.Path(sys.argv[3])
bad = rc.ref_violations(wt, rc.ref_snapshot(wt), "card/v3-RF", "no-such-rev")
print(bad)
sys.exit(0 if raised and len(bad) == 1 and "failed (exit" in bad[0] and "cannot check refs" in bad[0] else 1)
P
}
expect_ok "runner refs: RF10 a failed snapshot raises and a failed check is a violation" rf10
# RF11: R1 covers refs that existed before the run, not only new ones: an existing
# card branch moved onto this run's commit (G-R1-CREATED-ONLY; RF4 only creates).
expect_ok "runner refs: RF11 an existing card branch moved onto this run's commit is a violation" rf refs/heads/card/v3-ZZ 'git fetch -q . +card/v3-RF:card/v3-ZZ'
# RF18: R1 is ancestry, not equality: a branch on a commit built on top of this
# run's commit carries it too (G-R1-TIP-ONLY: every other R1 row sits on W itself).
expect_ok "runner refs: RF18 a new branch on a descendant of this run's commit is a violation" rf refs/heads/card/v3-DESC 'git branch card/v3-DESC "$(git commit-tree "card/v3-RF^{tree}" -p card/v3-RF -m "nucleus v3: DESC on top")"'

# RV1-RV5 (X-6-F1-F2-F3-F1-F3): ref_violations hash-verifies the commits its
# ancestry rules read, so a forged commit cannot bend R1 or R2. A template like
# rf0's: the scaffold tip S ("rv base 1"; pre_head card/v3-RV~1 = S) with main,
# tag t0, nucleus/zz, nucleus/c1..c4, card/v3-ZZ and an origin ref at it, and
# card/v3-RV one worker commit W past S. Every row copies the template (as rf's
# rows copy rf0), so each forgery stays inside its own throwaway copy of the
# scratch repository and no row reads another's moves; save_obj, restore_forged
# and no_state4 guard the bytes as (a)-(l) do. #12 (X-6-F1-F3-F1-F2) is not
# merged at base, so there is no second-parent tree probe and no (r4) row.
rv0=$t/rv0
git init -q -b nucleus/v3-scaffold "$rv0"
git -C "$rv0" commit -q --allow-empty -m "rv base 0"
git -C "$rv0" commit -q --allow-empty -m "rv base 1"
git -C "$rv0" branch main && git -C "$rv0" tag t0 && git -C "$rv0" branch nucleus/zz && git -C "$rv0" branch card/v3-ZZ
for i in 1 2 3 4; do git -C "$rv0" branch "nucleus/c$i" nucleus/v3-scaffold; done
git -C "$rv0" update-ref refs/remotes/origin/nucleus/v3-scaffold nucleus/v3-scaffold
git -C "$rv0" checkout -q -b card/v3-RV
git -C "$rv0" commit -q --allow-empty -m "nucleus v3: RV work"
rvS=$(git -C "$rv0" rev-parse nucleus/v3-scaffold)
rvW=$(git -C "$rv0" rev-parse card/v3-RV)
# rvc: a fresh copy of the template (this row's own scratch repository).
rvc() { local d; d=$(mktemp -d "$t/rv.XXXX"); cp -a "$rv0/." "$d"; printf '%s\n' "$d"; }
# rvo RUNNER REPO: ref_violations through RUNNER (the real runner, or a
# reverted copy for a premise) on REPO as it stands, before = the template's
# pristine snapshot; prints one line per entry, "(no violations)" when none.
# pre_head comes from the template, never from the copy: git's read path
# hash-checks a forged commit asked for by name (probed: rev-parse, rev-list
# and merge-base all exit 128 on a forged tip), so the harness itself must
# never resolve one.
rvo() { PYTHONDONTWRITEBYTECODE=1 python3 - "$1" "$2" "$rv0" <<'P'
import importlib.util, pathlib, subprocess, sys
spec = importlib.util.spec_from_file_location("rc", sys.argv[1]); rc = importlib.util.module_from_spec(spec); spec.loader.exec_module(rc)
wt, b = pathlib.Path(sys.argv[2]), pathlib.Path(sys.argv[3])
pre = subprocess.run(["git", "rev-parse", "nucleus/v3-scaffold"], cwd=b, capture_output=True, text=True, check=True).stdout.strip()
print("\n".join(rc.ref_violations(wt, rc.ref_snapshot(b), "card/v3-RV", pre)) or "(no violations)")
P
}
# Step A reverted at both call sites (the F3-NO-VERIFY behaviour), as (e')
# reverts its call by sed: the premise rows prove each hide really hides.
sed -e 's/^        probs = verify_objects(wt, pre_head, own)$/        probs = []/' \
    -e 's/^            probs = verify_objects(wt, old or pre_head, new) if new else \[\]$/            probs = []/' \
    -e 's/^                    unverified = _verify_ids(wt, "ref_violations", p.stdout.split(), d)$/                    unverified = []/' \
    "$v3/tools/run_card.py" >"$t/run_card-rv-nover.py"
# RV1: the R2 rewind dressed as a fast-forward (the gate's reproduction). P
# sits on the scaffold's first commit and X on P, so nucleus/zz S -> X is a
# rewind; P's loose file then becomes the same body naming S as its parent,
# and the walk from X reaches S: the rewind reads as a fast-forward.
rv1d=$(rvc)
rv1p=$(git -C "$rv1d" commit-tree "nucleus/v3-scaffold^{tree}" -p nucleus/v3-scaffold~1 -m "rv1 P")
rv1x=$(git -C "$rv1d" commit-tree "nucleus/v3-scaffold^{tree}" -p "$rv1p" -m "rv1 X")
git -C "$rv1d" update-ref refs/heads/nucleus/zz "$rv1x"
rv1_rewind() { local rc=0; git -C "$rv1d" merge-base --is-ancestor "$rvS" "$rv1x" || rc=$?; echo "is-ancestor exit: $rc"; [ "$rc" -eq 1 ]; }
expect_ok "premise: RV1 X does not descend from S (the move is a rewind)" rv1_rewind
expect_ok "premise: RV1 P is a loose object" bash -c "[ -f '$(objlo "$rv1d" "$rv1p")' ]"
printf 'tree %s\nparent %s\nauthor t <t@t> 1700000000 +0000\ncommitter t <t@t> 1700000000 +0000\n\nrv1 P forged\n' "$(git -C "$rv1d" rev-parse "nucleus/v3-scaffold^{tree}")" "$rvS" >"$t/rv1p.body"
save_obj "$rv1d" "$rv1p"
forge_commit "$rv1d" "$rv1p" "$t/rv1p.body"
rv1_dressed() { local rc=0; git -C "$rv1d" merge-base --is-ancestor "$rvS" "$rv1x" || rc=$?; echo "is-ancestor exit: $rc"; [ "$rc" -eq 0 ]; }
expect_ok "premise: RV1 under the forgery the rewind reads as a fast-forward" rv1_dressed
rv1_old() { local o; o=$(rvo "$t/run_card-rv-nover.py" "$rv1d"); echo "$o"; ! grep -qF "refs/heads/nucleus/zz" <<<"$o"; }
expect_ok "premise: RV1 without the verifies the rewind is silent" rv1_old
rv1_row() { local o; o=$(rvo "$v3/tools/run_card.py" "$rv1d"); echo "$o"; grep -qF "refs/heads/nucleus/zz" <<<"$o" && grep -qF "object $rv1p fails its hash" <<<"$o"; }
expect_ok "runner refs: RV1 the rewind dressed as a fast-forward is refused, naming P" rv1_row
restore_forged
expect_ok "forged objects: RV1 restored" no_state4
# RV1's pin for F3-VERIFY-THEN-JUDGE (problems appended but R1/R2 still judged
# on the lie): the card's own RV1 assertion cannot catch it, because under
# RV1's forgery the fast-forward test passes, mutant or not. So nucleus/zz
# honestly fast-forwards S -> Y through M (Y -> M -> S), and M's loose file
# becomes the same body naming the scaffold's first commit: under the lie S
# is no longer an ancestor of Y, so judging after the failed verify would
# also name the honest move a rewind. M, not Y, is forged because git
# hash-checks a forged commit read as a range tip (it answers exit 128, a
# failed step, and never reaches the hash mismatch) — only a commit reached
# through honest links can fail its hash inside verify_objects.
rv1q=$(rvc)
rv1m=$(git -C "$rv1q" commit-tree "nucleus/v3-scaffold^{tree}" -p "$rvS" -m "rv1 pin M")
rv1y=$(git -C "$rv1q" commit-tree "nucleus/v3-scaffold^{tree}" -p "$rv1m" -m "rv1 pin Y")
git -C "$rv1q" update-ref refs/heads/nucleus/zz "$rv1y"
rv1_ff() { local rc=0; git -C "$rv1q" merge-base --is-ancestor "$rvS" "$rv1y" || rc=$?; echo "is-ancestor exit: $rc"; [ "$rc" -eq 0 ]; }
expect_ok "premise: RV1 pin nucleus/zz S -> Y is an honest fast-forward" rv1_ff
printf 'tree %s\nparent %s\nauthor t <t@t> 1700000000 +0000\ncommitter t <t@t> 1700000000 +0000\n\nrv1 pin M forged\n' "$(git -C "$rv1q" rev-parse "nucleus/v3-scaffold^{tree}")" "$(git -C "$rv1q" rev-parse nucleus/v3-scaffold~1)" >"$t/rv1m.body"
save_obj "$rv1q" "$rv1m"
forge_commit "$rv1q" "$rv1m" "$t/rv1m.body"
rv1_lie() { local rc=0; git -C "$rv1q" merge-base --is-ancestor "$rvS" "$rv1y" || rc=$?; echo "is-ancestor exit: $rc"; [ "$rc" -eq 1 ]; }
expect_ok "premise: RV1 pin under the forgery the honest fast-forward reads as a rewind" rv1_lie
rv1_pin() { local o; o=$(rvo "$v3/tools/run_card.py" "$rv1q"); echo "$o"; grep -qF "refs/heads/nucleus/zz" <<<"$o" && grep -qF "object $rv1m fails its hash" <<<"$o" && ! grep -qF "may move only as a fast-forward" <<<"$o"; }
expect_ok "runner refs: RV1 pin a forged fast-forward interior yields only the verification entry, never a fast-forward one" rv1_pin
restore_forged
expect_ok "forged objects: RV1 pin restored" no_state4
# RV2: the R1 taint hidden. side-rv2 (absent from the template) is created at
# C, whose history runs C -> M -> W through the worker commit W; M's loose
# file then becomes the same body naming pre_head as its parent, so W drops
# out of C's ancestry and the taint disappears from every ancestry read. M,
# not C, is forged for the same reason as RV1 pin's: the ref's tip C must
# stay readable (git hash-checks a forged tip), and the lie only needs the
# walk from C to miss W.
rv2d=$(rvc)
rv2m=$(git -C "$rv2d" commit-tree "card/v3-RV^{tree}" -p "$rvW" -m "rv2 M")
rv2c=$(git -C "$rv2d" commit-tree "card/v3-RV^{tree}" -p "$rv2m" -m "rv2 C")
git -C "$rv2d" update-ref refs/heads/side-rv2 "$rv2c"
rv2_taint() { local rc=0; git -C "$rv2d" merge-base --is-ancestor "$rvW" "$rv2c" || rc=$?; echo "is-ancestor exit: $rc"; [ "$rc" -eq 0 ]; }
expect_ok "premise: RV2 C descends from the worker commit W" rv2_taint
printf 'tree %s\nparent %s\nauthor t <t@t> 1700000000 +0000\ncommitter t <t@t> 1700000000 +0000\n\nrv2 M forged\n' "$(git -C "$rv2d" rev-parse "card/v3-RV^{tree}")" "$rvS" >"$t/rv2m.body"
save_obj "$rv2d" "$rv2m"
forge_commit "$rv2d" "$rv2m" "$t/rv2m.body"
rv2_hidden() { local rc=0; git -C "$rv2d" merge-base --is-ancestor "$rvW" "$rv2c" || rc=$?; echo "is-ancestor exit: $rc"; [ "$rc" -eq 1 ]; }
expect_ok "premise: RV2 under the forgery W is no longer an ancestor of C" rv2_hidden
rv2_old() { local o; o=$(rvo "$t/run_card-rv-nover.py" "$rv2d"); echo "$o"; ! grep -qF "refs/heads/side-rv2" <<<"$o"; }
expect_ok "premise: RV2 without the verifies the taint is silent" rv2_old
rv2_row() { local o; o=$(rvo "$v3/tools/run_card.py" "$rv2d"); echo "$o"; grep -qF "refs/heads/side-rv2" <<<"$o" && grep -qF "object $rv2m fails its hash" <<<"$o"; }
expect_ok "runner refs: RV2 the hidden taint is refused, naming the forged M" rv2_row
restore_forged
expect_ok "forged objects: RV2 restored" no_state4
# RV3: the worker list forged. The card branch gets a second worker commit
# (W2, the honest tip; the run's pre_head stays the scaffold tip), and W1's
# loose file becomes its body with a changed message line, so the id no
# longer matches its bytes; the honest side-rv3 is created so the per-ref
# loop would have work. W1, not the tip, is forged as in RV1 pin: a forged
# tip would make the worker listing itself fail closed as a git step, never
# reaching the hash mismatch. Without a trusted worker list R1 cannot be
# judged: the answer is the worker-commits-unverifiable entry alone (early
# return, fail closed).
rv3d=$(rvc)
rv3w1=$(git -C "$rv3d" rev-parse card/v3-RV)
git -C "$rv3d" commit -q --allow-empty -m "nucleus v3: RV more work"
rv3w2=$(git -C "$rv3d" rev-parse card/v3-RV)
rv3s=$(git -C "$rv3d" commit-tree "nucleus/v3-scaffold^{tree}" -p "$rvS" -m "rv3 side")
git -C "$rv3d" update-ref refs/heads/side-rv3 "$rv3s"
printf 'tree %s\nparent %s\nauthor t <t@t> 1700000000 +0000\ncommitter t <t@t> 1700000000 +0000\n\nnucleus v3: RV work (message forged)\n' "$(git -C "$rv3d" rev-parse "card/v3-RV^{tree}")" "$rvS" >"$t/rv3w1.body"
save_obj "$rv3d" "$rv3w1"
forge_commit "$rv3d" "$rv3w1" "$t/rv3w1.body"
rv3_row() { local o; o=$(rvo "$v3/tools/run_card.py" "$rv3d"); echo "$o"; grep -qF "refs/heads/card/v3-RV: worker commits unverifiable: object $rv3w1 fails its hash" <<<"$o" && ! grep -qF "side-rv3" <<<"$o" && [ "$(grep -vcF "worker commits unverifiable" <<<"$o")" -eq 0 ]; }
expect_ok "runner refs: RV3 a forged worker commit fails the check closed (only unverifiable entries)" rv3_row
restore_forged
expect_ok "forged objects: RV3 restored" no_state4
# RV4: honest controls. A real fast-forward of nucleus/zz is no violation; a
# real rewind still gets only the fast-forward entry (no verification entry);
# deleting nucleus/zz still gets the deletion entry; and main moved under
# RV1's forgery gets its protected entry and the verification entry (the rules
# that need no ancestry fire on a lie too).
rv4a=$(rvc)
git -C "$rv4a" update-ref refs/heads/nucleus/zz "$(git -C "$rv4a" commit-tree "nucleus/v3-scaffold^{tree}" -p "$rvS" -m "rv4 ff")"
rv4_ff() { local o; o=$(rvo "$v3/tools/run_card.py" "$rv4a"); echo "$o"; [ "$o" = "(no violations)" ]; }
expect_ok "runner refs: RV4 an honest fast-forward of nucleus/zz is no violation" rv4_ff
rv4b=$(rvc)
rv4p=$(git -C "$rv4b" commit-tree "nucleus/v3-scaffold^{tree}" -p nucleus/v3-scaffold~1 -m "rv4 P")
rv4x=$(git -C "$rv4b" commit-tree "nucleus/v3-scaffold^{tree}" -p "$rv4p" -m "rv4 X")
git -C "$rv4b" update-ref refs/heads/nucleus/zz "$rv4x"
rv4_rewind() { local o; o=$(rvo "$v3/tools/run_card.py" "$rv4b"); echo "$o"; grep -qF "refs/heads/nucleus/zz" <<<"$o" && grep -qF "may move only as a fast-forward" <<<"$o" && ! grep -qF "objects fail verification" <<<"$o"; }
expect_ok "runner refs: RV4 an honest rewind is refused once, as a fast-forward violation only" rv4_rewind
rv4c=$(rvc)
git -C "$rv4c" update-ref -d refs/heads/nucleus/zz
rv4_del() { local o; o=$(rvo "$v3/tools/run_card.py" "$rv4c"); echo "$o"; grep -qF "refs/heads/nucleus/zz" <<<"$o" && grep -qF "(protected: nucleus/* may not be deleted)" <<<"$o"; }
expect_ok "runner refs: RV4 deleting nucleus/zz is still refused" rv4_del
rv4d=$(rvc)
rv4mp=$(git -C "$rv4d" commit-tree "nucleus/v3-scaffold^{tree}" -p nucleus/v3-scaffold~1 -m "rv4 main P")
rv4mx=$(git -C "$rv4d" commit-tree "nucleus/v3-scaffold^{tree}" -p "$rv4mp" -m "rv4 main X")
git -C "$rv4d" update-ref refs/heads/main "$rv4mx"
printf 'tree %s\nparent %s\nauthor t <t@t> 1700000000 +0000\ncommitter t <t@t> 1700000000 +0000\n\nrv4 main P forged\n' "$(git -C "$rv4d" rev-parse "nucleus/v3-scaffold^{tree}")" "$rvS" >"$t/rv4mp.body"
save_obj "$rv4d" "$rv4mp"
forge_commit "$rv4d" "$rv4mp" "$t/rv4mp.body"
rv4_main() { local o; o=$(rvo "$v3/tools/run_card.py" "$rv4d"); echo "$o"; grep -qF "refs/heads/main" <<<"$o" && grep -qF "(protected: may not change)" <<<"$o" && grep -qF "object $rv4mp fails its hash" <<<"$o"; }
expect_ok "runner refs: RV4 main moved under the forgery is refused twice: protected and unverifiable" rv4_main
restore_forged
expect_ok "forged objects: RV4 restored" no_state4
# RV5: a git failure inside the verifies is an entry, never []. gshim's
# pack-objects stops the worker verify first, so its entry is the
# worker-commits-unverifiable one naming the step; GSHIM_FAIL "<S>...<Y>"
# (full ids) matches no other git call ref_violations makes, so it fails
# exactly the per-ref verify's boundary rev-list, with the move standing
# honest otherwise.
rv5a=$(rvc)
git -C "$rv5a" update-ref refs/heads/nucleus/zz "$(git -C "$rv5a" commit-tree "nucleus/v3-scaffold^{tree}" -p "$rvS" -m "rv5 ff")"
rv5_pack() { local o; o=$(gshim pack-objects rvo "$v3/tools/run_card.py" "$rv5a"); echo "$o"; grep -qF "refs/heads/card/v3-RV: worker commits unverifiable: verify_objects: git pack-objects failed (exit 128; cannot verify objects)" <<<"$o"; }
expect_ok "runner refs: RV5 a failed pack-objects in the worker verify is an entry, never []" rv5_pack
rv5b=$(rvc)
rv5z=$(git -C "$rv5b" commit-tree "nucleus/v3-scaffold^{tree}" -p "$rvS" -m "rv5 per-ref ff")
git -C "$rv5b" update-ref refs/heads/nucleus/zz "$rv5z"
rv5_perref() { local o; o=$(gshim "$rvS...$rv5z" rvo "$v3/tools/run_card.py" "$rv5b"); echo "$o"; grep -qF "refs/heads/nucleus/zz" <<<"$o" && grep -qF "objects fail verification: verify_objects: git rev-list failed (exit 128; cannot verify objects)" <<<"$o"; }
expect_ok "runner refs: RV5 a failed rev-list in the per-ref verify is an entry, never []" rv5_perref
# Cost: ten honest moved refs (nucleus/zz and nucleus/c1..c4 fast-forward,
# side-rvc1..c5 are created), one ref_violations call under 2 s; the measured
# time prints in the row's output.
rvcost=$(rvc)
for i in 1 2 3 4 5; do git -C "$rvcost" update-ref "refs/heads/nucleus/c$i" "$(git -C "$rvcost" commit-tree "nucleus/v3-scaffold^{tree}" -p "$rvS" -m "rv cost ff $i")"; done
git -C "$rvcost" update-ref refs/heads/nucleus/zz "$(git -C "$rvcost" commit-tree "nucleus/v3-scaffold^{tree}" -p "$rvS" -m "rv cost ff zz")"
for i in 1 2 3 4 5; do git -C "$rvcost" update-ref "refs/heads/side-rvc$i" "$(git -C "$rvcost" commit-tree "nucleus/v3-scaffold^{tree}" -p "$rvS" -m "rv cost side $i")"; done
rv_cost() { local t0 t1 ms o; t0=$(date +%s%N); o=$(rvo "$v3/tools/run_card.py" "$rvcost"); t1=$(date +%s%N); ms=$(((t1 - t0) / 1000000)); echo "$o"; echo "     (RV cost) ref_violations over 10 moved refs: ${ms} ms"; [ "$ms" -lt 2000 ] && [ "$o" = "(no violations)" ]; }
expect_ok "runner refs: RV cost ten honest moved refs verify in under 2 s" rv_cost

# RV6-RV10 (X-6-F1-F2-F3-F1-F3-F1): the union verify of the full ancestry.
# Step A lists `rev-list <T> --` (no ^, no ...; T = pre_head, own and every
# changed ref's old and new) before the ancestry rules, because a symmetric
# difference's excluded side is computed through the same forged parents it
# should distrust: two loose forgeries forming a cycle (verdict
# X-6-F1-F2-F3-F1-F3, Finding 1) hide from old...new and pre_head...new while
# both verify_objects calls answer []. These rows pin that bypass closed, and
# are written under the owner's 2026-11-03 forged-object exception (the
# forgeries stay inside these throwaway copies, as RV1-RV5's do).
# The one edit above this block (ESC1 ruling 2, on the X-6-F1-F2-F3-F1-F2
# precedent) inserts a third key into the run_card-rv-nover.py sed: with it
# the nover copy drops the union verify too, restoring RV1/RV2's original
# "without the verifies" meaning. Without it the copy keeps the union check
# and those premises pass only because the union's `refs:` line is no per-ref
# entry. The canary rows pin both copies' shape. The nounion copy below drops
# only the union verify: that is e9a5d17b's ref_violations, the behaviour
# RV6's and RV7's "is fooled" premises probe.
rv_nover_key() {
  local o
  o=$(diff "$v3/tools/run_card.py" "$t/run_card-rv-nover.py")
  echo "$o"
  [ "$(printf '%s\n' "$o" | grep -c '^> ')" -eq 3 ] &&
    [ "$(printf '%s\n' "$o" | sed -n 's/^>  *//p' | LC_ALL=C sort | tr '\n' '|')" = "probs = []|probs = []|unverified = []|" ]
}
expect_ok "premise: RV nover copy drops the two verify_objects calls and the union verify" rv_nover_key
sed -e 's/^                    unverified = _verify_ids(wt, "ref_violations", p.stdout.split(), d)$/                    unverified = []/' "$v3/tools/run_card.py" >"$t/run_card-rv-nounion.py"
rv_nounion_key() {
  local o
  o=$(diff "$v3/tools/run_card.py" "$t/run_card-rv-nounion.py")
  echo "$o"
  [ "$(printf '%s\n' "$o" | grep -c '^> ')" -eq 1 ] &&
    [ "$(printf '%s\n' "$o" | sed -n 's/^>  *//p')" = "unverified = []" ]
}
expect_ok "premise: RV nounion copy drops only the union verify" rv_nounion_key
# rvob RUNNER REPO BEFORE: rvo's body verbatim except the third python
# argument is "$3", so `before` and pre_head come from BEFORE's honest refs,
# never from the forged copy (RV8 moves a ref whose old differs from
# pre_head; the copy's own snapshot cannot supply that old).
rvob() { PYTHONDONTWRITEBYTECODE=1 python3 - "$1" "$2" "$3" <<'P'
import importlib.util, pathlib, subprocess, sys
spec = importlib.util.spec_from_file_location("rc", sys.argv[1]); rc = importlib.util.module_from_spec(spec); spec.loader.exec_module(rc)
wt, b = pathlib.Path(sys.argv[2]), pathlib.Path(sys.argv[3])
pre = subprocess.run(["git", "rev-parse", "nucleus/v3-scaffold"], cwd=b, capture_output=True, text=True, check=True).stdout.strip()
print("\n".join(rc.ref_violations(wt, rc.ref_snapshot(b), "card/v3-RV", pre)) or "(no violations)")
P
}
# RV6: the R2 cycle (verdict Finding 1's R2 bend, dated per ESC1 ruling 3).
# nucleus/zz S -> X over honest X -> Y -> P -> S^1; P is forged to name S and
# S^1 to name Y, so S -> S^1 -> Y -> P -> S is a cycle and the rewind stops
# reading as one: is-ancestor S X flips 1 -> 0 and verify_objects(S, X) is []
# (the symmetric difference is X alone), so e9a5d17b answers (no violations).
# The bend needs X, Y and P's forged body dated strictly after S (git 2.47.3
# pops the two-sided paint queue newest committer date first, and with
# core.commitGraph=false there are no generation numbers), so P, Y and X
# carry sct+100/+200/+300 and P's forged body keeps that date — RV1-RV3's
# fixed 1700000000 is older than S and does not bend. The union listing has
# no excluded side, so P and S^1 are reached through honest links, listed,
# and fail their hashes.
rv6d=$(rvc)
rv6s1=$(git -C "$rv6d" rev-parse nucleus/v3-scaffold~1)
sct=$(git -C "$rv6d" log -1 --format=%ct "$rvS")
rv6p=$(GIT_AUTHOR_DATE="@$((sct+100)) +0000" GIT_COMMITTER_DATE="@$((sct+100)) +0000" git -C "$rv6d" commit-tree "nucleus/v3-scaffold^{tree}" -p "$rv6s1" -m "rv6 P")
rv6y=$(GIT_AUTHOR_DATE="@$((sct+200)) +0000" GIT_COMMITTER_DATE="@$((sct+200)) +0000" git -C "$rv6d" commit-tree "nucleus/v3-scaffold^{tree}" -p "$rv6p" -m "rv6 Y")
rv6x=$(GIT_AUTHOR_DATE="@$((sct+300)) +0000" GIT_COMMITTER_DATE="@$((sct+300)) +0000" git -C "$rv6d" commit-tree "nucleus/v3-scaffold^{tree}" -p "$rv6y" -m "rv6 X")
git -C "$rv6d" update-ref refs/heads/nucleus/zz "$rv6x"
rv6_nocg() { [ ! -e "$rv6d/.git/objects/info/commit-graph" ] && [ ! -e "$rv6d/.git/objects/info/commit-graphs" ]; }
expect_ok "premise: RV6 the copy has no commit-graph" rv6_nocg
rv6_rewind() { local rc=0; git -C "$rv6d" merge-base --is-ancestor "$rvS" "$rv6x" || rc=$?; echo "is-ancestor exit: $rc"; [ "$rc" -eq 1 ]; }
expect_ok "premise: RV6 nucleus/zz S -> X is an honest rewind (is-ancestor exits 1)" rv6_rewind
rv6_loose() { [ -f "$(objlo "$rv6d" "$rv6p")" ] && [ -f "$(objlo "$rv6d" "$rv6s1")" ]; }
expect_ok "premise: RV6 P and S^1 are loose objects" rv6_loose
printf 'tree %s\nparent %s\nauthor t <t@t> %s +0000\ncommitter t <t@t> %s +0000\n\nrv6 P forged\n' "$(git -C "$rv6d" rev-parse "nucleus/v3-scaffold^{tree}")" "$rvS" "$((sct+100))" "$((sct+100))" >"$t/rv6p.body"
git -C "$rv6d" cat-file commit "$rv6s1" >"$t/rv6s1.body"
sed "1a parent $rv6y" "$t/rv6s1.body" >"$t/rv6s1f.body"
save_obj "$rv6d" "$rv6p"
save_obj "$rv6d" "$rv6s1"
forge_commit "$rv6d" "$rv6p" "$t/rv6p.body"
forge_commit "$rv6d" "$rv6s1" "$t/rv6s1f.body"
rv6_bend() { local rc=0; git -C "$rv6d" merge-base --is-ancestor "$rvS" "$rv6x" || rc=$?; echo "is-ancestor exit: $rc"; [ "$rc" -eq 0 ]; }
expect_ok "premise: RV6 under the forgery the rewind reads as a fast-forward (is-ancestor exits 0)" rv6_bend
rv6_old() { local o; o=$(rvo "$t/run_card-rv-nounion.py" "$rv6d"); echo "$o"; [ "$o" = "(no violations)" ]; }
expect_ok "premise: RV6 the nounion copy (e9a5d17b) is fooled" rv6_old
rv6_row() {
  local o u
  o=$(rvo "$v3/tools/run_card.py" "$rv6d"); echo "$o"
  u=$(grep '^refs: objects fail verification: ' <<<"$o")
  { grep -qF "object $rv6p fails its hash" <<<"$u" || grep -qF "object $rv6s1 fails its hash" <<<"$u"; } &&
    ! grep -qF "may move only as a fast-forward" <<<"$o" &&
    ! grep -qF "(no violations)" <<<"$o"
}
expect_ok "runner refs: RV6 the R2 cycle is refused by the union verify, naming P or S^1" rv6_row
restore_forged
expect_ok "forged objects: RV6 restored" no_state4
# RV7: the R1 cycle (verdict Finding 1's R1 bend; Step A's probe confirmed it,
# and it is not date-dependent: with M forged, W is unreachable from C under
# any walk order). side-rv7 is created at C over honest C -> C2 -> M -> W; M
# is forged to name S and S^1 to name C2, so W drops out of C's ancestry and
# the taint disappears from every ancestry read while both verifies answer
# []. The union listing still reaches M and S^1 through honest links.
rv7d=$(rvc)
rv7m=$(git -C "$rv7d" commit-tree "card/v3-RV^{tree}" -p "$rvW" -m "rv7 M")
rv7c2=$(git -C "$rv7d" commit-tree "card/v3-RV^{tree}" -p "$rv7m" -m "rv7 C2")
rv7c=$(git -C "$rv7d" commit-tree "card/v3-RV^{tree}" -p "$rv7c2" -m "rv7 C")
git -C "$rv7d" update-ref refs/heads/side-rv7 "$rv7c"
rv7s1=$(git -C "$rv7d" rev-parse nucleus/v3-scaffold~1)
rv7_taint() { local rc=0; git -C "$rv7d" merge-base --is-ancestor "$rvW" "$rv7c" || rc=$?; echo "is-ancestor exit: $rc"; [ "$rc" -eq 0 ]; }
expect_ok "premise: RV7 C descends from the worker commit W" rv7_taint
git -C "$rv7d" cat-file commit "$rv7m" >"$t/rv7m.body"
sed "s/^parent $rvW$/parent $rvS/" "$t/rv7m.body" >"$t/rv7mf.body"
git -C "$rv7d" cat-file commit "$rv7s1" >"$t/rv7s1.body"
sed "1a parent $rv7c2" "$t/rv7s1.body" >"$t/rv7s1f.body"
save_obj "$rv7d" "$rv7m"
save_obj "$rv7d" "$rv7s1"
forge_commit "$rv7d" "$rv7m" "$t/rv7mf.body"
forge_commit "$rv7d" "$rv7s1" "$t/rv7s1f.body"
rv7_hidden() { local rc=0; git -C "$rv7d" merge-base --is-ancestor "$rvW" "$rv7c" || rc=$?; echo "is-ancestor exit: $rc"; [ "$rc" -eq 1 ]; }
expect_ok "premise: RV7 under the forgery W is no longer an ancestor of C" rv7_hidden
rv7_old() { local o; o=$(rvo "$t/run_card-rv-nounion.py" "$rv7d"); echo "$o"; [ "$o" = "(no violations)" ]; }
expect_ok "premise: RV7 the nounion copy (e9a5d17b) is fooled" rv7_old
rv7_row() {
  local o u
  o=$(rvo "$v3/tools/run_card.py" "$rv7d"); echo "$o"
  u=$(grep '^refs: objects fail verification: ' <<<"$o")
  { grep -qF "object $rv7m fails its hash" <<<"$u" || grep -qF "object $rv7s1 fails its hash" <<<"$u"; } &&
    ! grep -qF "carries this run's commit" <<<"$o" &&
    ! grep -qF "(no violations)" <<<"$o"
}
expect_ok "runner refs: RV7 the R1 cycle is refused by the union verify, naming M or S^1" rv7_row
restore_forged
expect_ok "forged objects: RV7 restored" no_state4
# RV8: old != pre_head, the forged commit reachable only from old (ESC1
# ruling 1). rv8b is the before repository: nucleus/zz moves to O over honest
# O -> F -> S. The after repository rv8d is a copy of it whose zz moves
# S -> X (X -> S), so before/after differ exactly on zz, F hangs only from
# old, and rvob reads `before` (and pre_head) from rv8b's honest refs. F's
# forgery changes only its message line, so no ancestry bends and F just
# fails its hash. Two lines must name it: the union's `refs:` line (F is in
# old's ancestry; catches F3F1-DROP-OLD) and the per-ref old...new line
# (catches G-OLD-AS-PREHEAD, which verifies pre_head...new and misses F).
# rv8b itself is never forged.
rv8b=$(rvc)
rv8f=$(git -C "$rv8b" commit-tree "nucleus/v3-scaffold^{tree}" -p "$rvS" -m "rv8 F")
rv8o=$(git -C "$rv8b" commit-tree "nucleus/v3-scaffold^{tree}" -p "$rv8f" -m "rv8 O")
git -C "$rv8b" update-ref refs/heads/nucleus/zz "$rv8o"
rv8d=$(mktemp -d "$t/rv.XXXX")
cp -a "$rv8b/." "$rv8d"
rv8x=$(git -C "$rv8d" commit-tree "nucleus/v3-scaffold^{tree}" -p "$rvS" -m "rv8 X")
git -C "$rv8d" update-ref refs/heads/nucleus/zz "$rv8x"
rv8_before() { [ "$(git -C "$rv8b" rev-parse refs/heads/nucleus/zz)" = "$rv8o" ]; }
expect_ok "premise: RV8 the before repository's nucleus/zz is O" rv8_before
rv8_oldlist() { git -C "$rv8d" rev-list "$rv8o" -- | grep -qxF "$rv8f"; }
expect_ok "premise: RV8 rev-list O -- lists F (F hangs from old)" rv8_oldlist
rv8_notips() { ! git -C "$rv8d" rev-list "$rvS" "$rvW" "$rv8x" -- | grep -qxF "$rv8f"; }
expect_ok "premise: RV8 rev-list S W X -- does not list F (F is reachable only from old)" rv8_notips
expect_ok "premise: RV8 F is a loose object" bash -c "[ -f '$(objlo "$rv8d" "$rv8f")' ]"
git -C "$rv8d" cat-file commit "$rv8f" >"$t/rv8f.body"
sed 's/^rv8 F$/rv8 F forged/' "$t/rv8f.body" >"$t/rv8ff.body"
save_obj "$rv8d" "$rv8f"
forge_commit "$rv8d" "$rv8f" "$t/rv8ff.body"
rv8_row() {
  local o u
  o=$(rvob "$v3/tools/run_card.py" "$rv8d" "$rv8b"); echo "$o"
  u=$(grep '^refs: objects fail verification: ' <<<"$o")
  grep -qF "object $rv8f fails its hash" <<<"$u" &&
    grep "^refs/heads/nucleus/zz: $rv8o -> $rv8x objects fail verification: " <<<"$o" | grep -qF "object $rv8f fails its hash" &&
    ! grep -qF "may move only as a fast-forward" <<<"$o" &&
    ! grep -qF "(no violations)" <<<"$o"
}
expect_ok "runner refs: RV8 a forgery reachable only from old is named by the union line and the per-ref old...new line" rv8_row
restore_forged
expect_ok "forged objects: RV8 restored" no_state4
# RV9: a forged tip. side-rv9 is created at honest C, whose loose file then
# becomes its body with a changed message line. A plain `rev-list <tips> --`
# reads a forged tip fine (it exits 0 and lists the lie, unlike the range
# forms, which hash-check a tip named on the command line and exit 128), so
# the union verify names C by its hash failure — the per-ref verify's failed
# git step alone is not the row.
rv9d=$(rvc)
rv9c=$(git -C "$rv9d" commit-tree "nucleus/v3-scaffold^{tree}" -p "$rvS" -m "rv9 C")
git -C "$rv9d" update-ref refs/heads/side-rv9 "$rv9c"
git -C "$rv9d" cat-file commit "$rv9c" >"$t/rv9c.body"
sed 's/^rv9 C$/rv9 C forged/' "$t/rv9c.body" >"$t/rv9cf.body"
save_obj "$rv9d" "$rv9c"
forge_commit "$rv9d" "$rv9c" "$t/rv9cf.body"
rv9_row() {
  local o u
  o=$(rvo "$v3/tools/run_card.py" "$rv9d"); echo "$o"
  u=$(grep '^refs: objects fail verification: ' <<<"$o")
  grep -qF "object $rv9c fails its hash" <<<"$u"
}
expect_ok "runner refs: RV9 a forged tip fails the union verify by its hash, not by a failed git step" rv9_row
restore_forged
expect_ok "forged objects: RV9 restored" no_state4
# RV10: a failed union listing is an entry, never [] (F3F1-FAIL-OPEN). No
# argument is unique to the union `rev-list <sorted tips> --`, so the shim
# word is side-rv10's full id C: it also fails the per-ref verify's
# --no-walk listing and C's merge-base, whose entries appear beside the
# union's own. The row pins that line, exactly.
rv10d=$(rvc)
rv10c=$(git -C "$rv10d" commit-tree "nucleus/v3-scaffold^{tree}" -p "$rvS" -m "rv10 C")
git -C "$rv10d" update-ref refs/heads/side-rv10 "$rv10c"
rv10_row() {
  local o
  o=$(gshim "$rv10c" rvo "$v3/tools/run_card.py" "$rv10d"); echo "$o"
  grep -qxF "refs: objects fail verification: ref_violations: git rev-list failed (exit 128; cannot verify objects)" <<<"$o" &&
    ! grep -qF "(no violations)" <<<"$o"
}
expect_ok "runner refs: RV10 a failed union rev-list is an entry, never []" rv10_row
# RV11: the union listing is the full ancestry, second parents included (verdict
# X-6-F1-F2-F3-F1-F3-F1, Finding 1: G-FIRST-PARENT, a --first-parent union, was
# UNCAUGHT because every RV1-RV10 construction is linear; the scaffold itself
# has merge commits). side-rv11 is created at the honest merge G (first parent
# S, second parent B, and B -> S), then B's loose file becomes its body with a
# changed message line, under the owner's 2026-11-03 forged-object exception: no
# ancestry bends, B just fails its hash, and it is reachable from G only as a
# second parent — exactly a commit a --first-parent union would leave
# unverified, free to be the excluding half of a cycle against the per-ref
# ranges. The per-ref line names B as well (old is (none), so the range is
# pre_head...new): expected, and the row's conjunct is the union line alone,
# the only one G-FIRST-PARENT drops.
rv11d=$(rvc)
rv11b=$(git -C "$rv11d" commit-tree "nucleus/v3-scaffold^{tree}" -p "$rvS" -m "rv11 B")
rv11g=$(git -C "$rv11d" commit-tree "nucleus/v3-scaffold^{tree}" -p "$rvS" -p "$rv11b" -m "rv11 G")
git -C "$rv11d" update-ref refs/heads/side-rv11 "$rv11g"
rv11_fp() { ! git -C "$rv11d" rev-list --first-parent "$rv11g" -- | grep -qxF "$rv11b"; }
expect_ok "premise: RV11 rev-list --first-parent G does not list B" rv11_fp
rv11_full() { git -C "$rv11d" rev-list "$rv11g" -- | grep -qxF "$rv11b"; }
expect_ok "premise: RV11 rev-list G lists B (the full ancestry)" rv11_full
expect_ok "premise: RV11 B is a loose object" bash -c "[ -f '$(objlo "$rv11d" "$rv11b")' ]"
git -C "$rv11d" cat-file commit "$rv11b" >"$t/rv11b.body"
sed 's/^rv11 B$/rv11 B forged/' "$t/rv11b.body" >"$t/rv11bf.body"
save_obj "$rv11d" "$rv11b"
forge_commit "$rv11d" "$rv11b" "$t/rv11bf.body"
rv11_row() {
  local o u
  o=$(rvo "$v3/tools/run_card.py" "$rv11d"); echo "$o"
  u=$(grep '^refs: objects fail verification: ' <<<"$o")
  grep -qF "object $rv11b fails its hash" <<<"$u" &&
    ! grep -qF "(no violations)" <<<"$o"
}
expect_ok "runner refs: RV11 a forged commit reachable only as a merge's second parent is named by the union verify" rv11_row
restore_forged
expect_ok "forged objects: RV11 restored" no_state4

# --- runner end to end: main() against a fake opencode (X-6-F1-F1) ----------
# The trust ledger (X-6-F1 F1), the gate's verdict between runs (F5) and killed-
# attempt accounting (nv3-B-0-esc-1) live in main()'s attempt loop, so these run
# main() itself: the copied runner with WT_ROOT, TARGET_DIR, CARDS and the
# ledger repointed under $e, and the timing constants shrunk.
e=$t/e2e; ewt=$e/wt
mkdir -p "$e/cards/logs"
printf 'worker prompt\n' >"$e/cards/WORKER-PROMPT.local.md"
# The runner copies the card and prompt into the worktree and writes opencode.json
# there. The real repository ignores those files, and so does this base.
git -C "$r" checkout -q -B e2e/base nucleus/v3-scaffold
printf '**/*.local.*\nopencode.json\n' >"$r/.gitignore"
git -C "$r" add .gitignore && git -C "$r" commit -qm "e2e base: ignore local files"
git -C "$r" checkout -q nucleus/v3-scaffold
# The fake opencode, next to the fake cargo. FAKE_OC lists each invocation's
# actions (';' between invocations, the last one repeating; ' ' between
# actions). FAKE_OC_N counts invocations, FAKE_OC_N.pid holds the latest pid.
# The card ID is the worktree's name.
cat >"$t/bin/opencode" <<'F'
#!/usr/bin/env bash
set -euo pipefail
n=$(( $(cat "$FAKE_OC_N" 2>/dev/null || echo 0) + 1 )); echo "$n" >"$FAKE_OC_N"; echo $$ >"$FAKE_OC_N.pid"
IFS=';' read -ra inv <<<"$FAKE_OC"
i=$(( n < ${#inv[@]} ? n - 1 : ${#inv[@]} - 1 ))
id=$(basename "$PWD")
v=nucleus/v3/verdicts/$id.md
for a in ${inv[$i]}; do
  case $a in
    forge) echo forged >"$v"; git add "$v"; git commit -qm "nucleus v3: $id verdict MERGE" ;;
    work) echo "$n" >>"nucleus/v3/spikes/$id.txt"; git add nucleus/v3/spikes; git commit -qm "nucleus v3: $id work" ;;
    quota) echo '{"type":"error","sessionID":"ses_fake","error":{"name":"provider.quota"}}' ;;
    silent) echo '{"type":"step_finish","sessionID":"ses_fake","part":{"tokens":{"input":1234,"output":56,"reasoning":7}}}'; sleep 30; exit 0 ;;
    mute) sleep 30; exit 0 ;;
    refmove) git fetch -q . +HEAD:refs/heads/main ;;
    foreign) o=$(git commit-tree "nucleus/v3-scaffold^{tree}" -p nucleus/v3-scaffold -m "nucleus v3: ZF work")
      git update-ref refs/heads/card/v3-ZF "$o"; git update-ref refs/heads/scratch/passZ "$o"
      m=$(git commit-tree "nucleus/v3-scaffold^{tree}" -p nucleus/v3-scaffold -p "$o" -m "nucleus v3: merge card/v3-ZF")
      git update-ref refs/heads/nucleus/v3-scaffold "$m"; git update-ref refs/remotes/origin/nucleus/v3-scaffold "$m"; git update-ref refs/stash "$o" ;;
    side) git update-ref "refs/heads/side-$id" "$(git commit-tree "HEAD^{tree}" -p HEAD -m "nucleus v3: $id side")" ;;
    adopt) git merge -q --ff-only "side-$id" ;;
    # The git environment the child inherits: .env holds the three values the
    # runner switches (the V row); .cfg holds its whole command-scope config
    # (keys lowercased), for a row on an entry added later.
    gitenv) { git config core.fsmonitor || echo unset; printenv GIT_NO_REPLACE_OBJECTS || echo unset; printenv GIT_GRAFT_FILE || echo unset; } >"$FAKE_OC_N.env"
      git config --list --show-scope | grep '^command' >"$FAKE_OC_N.cfg" || true ;;
    # An untracked file that a self-ignoring untracked .gitignore hides (Q).
    hideuntracked) echo hidden >"nucleus/v3/spikes/$id-hidden.txt"; printf '*\n' >nucleus/v3/spikes/.gitignore ;;
    # The prompt or retry message this invocation got (its last argument; RUN-F1).
    msg) printf '%s\n' "${@: -1}" >"$FAKE_OC_N.msg$n" ;;
    # A forge left running after opencode exits normally (BG): this fake's bash,
    # without job control, keeps the subshell and its sleep in opencode's process
    # group. Real opencode 2.2.5 does not: it starts tool shells in a new session,
    # so they escape the group kill (verdict X-6-F1-F1-F2, card X-6-F1-F1-F2-F1).
    # TERM is ignored from before the fork, so no race leaves the subshell
    # killable by TERM and a kill that is not SIGKILL leaves it alive (OWN-SIGTERM).
    # FAKE_OC_N.bg holds the subshell's pid.
    bgforge) trap '' TERM; ( sleep 30; echo forged >"$v"; git add "$v"; git commit -qm "nucleus v3: $id verdict MERGE" ) >/dev/null 2>&1 & echo $! >"$FAKE_OC_N.bg"; trap - TERM ;;
    # A forge that leaves opencode's session and group, as real opencode 2.2.5's
    # tool shells do (CT, X-6-F1-F1-F2-F1). .ocid and .forgeid hold each one's pgid
    # and sid, .ocns the fake's pid namespace, pid and uid. E2E_CT_MARK is in the
    # forge's environment and not the fake's, so ct-alive.sh finds the forge alone.
    # The loop waits for .forgeid, so a fast exit cannot kill the forge unrecorded.
    setsidforge) ps -o pgid=,sid= -p $$ >"$FAKE_OC_N.ocid"
      printf '%s %s %s\n' "$(readlink /proc/self/ns/pid)" "$$" "$(id -u)" >"$FAKE_OC_N.ocns"
      trap '' TERM
      E2E_CT_MARK=$FAKE_OC_N setsid bash -c 'ps -o pgid=,sid= -p $$ >"$1.forgeid"; sleep "$2"; echo forged >"$3"; git add "$3"; git commit -qm "nucleus v3: $4 verdict MERGE"' _ "$FAKE_OC_N" "${E2E_FORGE_S:-2}" "$v" "$id" >/dev/null 2>&1 &
      trap - TERM
      for _ in {1..100}; do [ -s "$FAKE_OC_N.forgeid" ] && break; sleep 0.05; done ;;
    # opencode itself leaves unshare's group, then goes silent: the runner's killpg
    # reaches unshare alone, and only --kill-child can end the namespace.
    leavegroup) exec setsid bash -c 'ps -o pgid=,sid= -p $$ >"$FAKE_OC_N.leftid"; echo "{\"type\":\"step_finish\",\"sessionID\":\"ses_fake\",\"part\":{\"tokens\":{\"input\":1,\"output\":1,\"reasoning\":0}}}"; sleep 30' ;;
    # X-6-F1-F1-F2-F1-F1: accforge commits acc-forge.sh, worker code an Acceptance
    # line runs: a forge in its own session, the mark in its environment alone, and
    # the line's pid namespace in .accns. The loop waits for .forgeid, as above.
    accforge) cat >nucleus/v3/spikes/acc-forge.sh <<'A'
#!/usr/bin/env bash
id=$(basename "$PWD"); v=nucleus/v3/verdicts/$id.md
readlink /proc/self/ns/pid >"$FAKE_OC_N.accns"
trap '' TERM
E2E_CT_MARK=$FAKE_OC_N setsid bash -c 'ps -o pgid=,sid= -p $$ >"$1.forgeid"; sleep "$2"; echo forged >"$3"; git add "$3"; git commit -qm "nucleus v3: $4 verdict MERGE"' _ "$FAKE_OC_N" "${E2E_FORGE_S:-2}" "$v" "$id" >/dev/null 2>&1 &
trap - TERM
for _ in {1..100}; do [ -s "$FAKE_OC_N.forgeid" ] && break; sleep 0.05; done
A
      git add nucleus/v3/spikes; git commit -qm "nucleus v3: $id acc forge" ;;
    # The container's TMPDIR, and a file the worker leaves in it.
    tmprec) echo "$TMPDIR" >>"$FAKE_OC_N.tmps"; : >"$TMPDIR/left" ;;
    # A heartbeat in its own session, for a contained Acceptance line that cannot see
    # the host's /proc: the file stops changing once the forge is dead. Bounded, so
    # the no-container premise leaves nothing behind.
    hbforge) setsid bash -c 'for _ in {1..200}; do date +%s%N >"$1.hb"; sleep 0.05; done' _ "$FAKE_OC_N" >/dev/null 2>&1 &
      for _ in {1..100}; do [ -s "$FAKE_OC_N.hb" ] && break; sleep 0.05; done ;;
  esac
done
echo '{"type":"step_finish","sessionID":"ses_fake","part":{"tokens":{"input":100,"output":10,"reasoning":1}}}'
F
chmod +x "$t/bin/opencode"
# The runner starts opencode through `unshare` (CONTAIN). In a real namespace the
# fake's $$ is a namespace pid, and .n.pid, res's kill, the XKP killpg row and the
# XBG `ps` check all read that number as a host pid. So those rows run through this
# no-op shim: it drops the flags and execs, and the fake keeps its host pid. They
# test the runner's logic and the group kill of same-group children, as before. The
# CT rows below set E2E_NS=real and get the real binary. The seam is here on PATH,
# not in the runner, so nothing in run_card.py can switch containment off.
# E2E_NS: unset, the no-op (and it answers the runner's probe as a namespace would);
# real, the real unshare; fail, a host with user namespaces off; lie, the flags are
# dropped and the probe runs for real, so it reports no namespace.
E2E_REAL_UNSHARE=$(command -v unshare || true); export E2E_REAL_UNSHARE
cat >"$t/bin/unshare" <<'F'
#!/usr/bin/env bash
case ${E2E_NS:-noop} in
  real) exec "$E2E_REAL_UNSHARE" "$@" ;;
  fail) echo "unshare: unshare failed: Operation not permitted" >&2; exit 1 ;;
esac
while [ "$#" -gt 0 ] && [ "$1" != -- ]; do shift; done; shift
if [ "$1" = sh ] && [ "${E2E_NS:-noop}" = noop ]; then echo "1 $(id -u) $(id -g) sh"; exit 0; fi
exec "$@"
F
chmod +x "$t/bin/unshare"
# The driver: one main() call, stdout captured, SystemExit caught. Writes
# OUT.out (stdout plus any exit message), OUT.rc and OUT.json (the result line).
# E2E_SIGTERM_AFTER sends SIGTERM to the runner that many seconds in.
cat >"$t/e2e.py" <<'P'
import contextlib, importlib.util, io, os, pathlib, signal, sys, threading, traceback
spec = importlib.util.spec_from_file_location("rc", sys.argv[1]); rc = importlib.util.module_from_spec(spec); spec.loader.exec_module(rc)
out, e = pathlib.Path(sys.argv[2]), pathlib.Path(os.environ["E2E"])
rc.WT_ROOT, rc.TARGET_DIR, rc.CARDS, rc.LEDGER_DIR = e / "wt", e / "target", e / "cards", e / "cards" / "logs"
rc.NO_OUTPUT_S, rc.STALL_S = int(os.environ.get("E2E_NO_OUTPUT_S", 1)), int(os.environ.get("E2E_STALL_S", 1))
rc.ATTEMPT_S, rc.POLL_S = 60, 0.1
sys.argv = ["run_card.py", *sys.argv[3:], "--min-free-gb", "0"]
term = os.environ.get("E2E_SIGTERM_AFTER")
timer = threading.Timer(float(term), os.kill, (os.getpid(), signal.SIGTERM)) if term else None
buf, code = io.StringIO(), 0
with contextlib.redirect_stdout(buf):
    if timer:
        timer.start()
    try:
        rc.main()
    except SystemExit as x:
        code = x.code
    except Exception:
        code = "runner raised:\n" + traceback.format_exc()
    finally:
        if timer:
            timer.cancel()
if isinstance(code, str):
    buf.write(code + "\n")
    code = 1
text = buf.getvalue()
out.with_suffix(".out").write_text(text)
out.with_suffix(".rc").write_text(f"{code or 0}\n")
js = [l for l in text.splitlines() if l.startswith("{")]
out.with_suffix(".json").write_text(js[-1] if js else "{}")
P
# ec ID: write the e2e card (Touch only spikes/, Acceptance `true` and the
# gate) and print its path.
ec() { printf '# CARD %s: e2e\n\nClass: test        Author: S (t)        Status: ready\nBase: e2e/base        Depends: —        Gate: S\n\n## Goal\nt\n\n## Touch only\n- nucleus/v3/spikes/\n\n## Acceptance\n- `true`\n- `cd nucleus/v3 && bash scripts/gate.sh`\n' "$1" >"$e/cards/$1.local.md"; echo "$e/cards/$1.local.md"; }
# e2e NAME FAKE_OC ARGS...: one run of card ID (NAME up to its first "-") on
# base e2e/base, with the fake opencode doing FAKE_OC.
e2e() { local n=$1 oc=$2; shift 2; rm -f "$e/$n.n"
  (cd "$r" && FAKE_OC=$oc FAKE_OC_N=$e/$n.n E2E=$e PYTHONDONTWRITEBYTECODE=1 python3 "$t/e2e.py" "$v3/tools/run_card.py" "$e/$n" "$(ec "${n%%-*}")" --base e2e/base "$@") || true; }
# res NAME EXPR: Python EXPR holds for the run's result JSON r, exit code rc
# (None if the driver died), stdout out, the card's ledger records L (and
# moved-aside ledgers A), the card branch's tip H, and dead() (the run's last
# opencode was already gone). A live one is killed first, whatever EXPR says.
res() { PYTHONDONTWRITEBYTECODE=1 python3 - "$e/$1" "$r" "$2" <<'P'
import glob, json, os, pathlib, signal, subprocess, sys
b, repo, expr = sys.argv[1:4]
cid, e = pathlib.Path(b).name.split("-")[0], pathlib.Path(b).parent
pidf, gone = pathlib.Path(b + ".n.pid"), True
for kill in (os.killpg, os.kill) if pidf.exists() else ():  # os.kill: a child left in the runner's own group
    try:
        kill(int(pidf.read_text()), signal.SIGKILL)
        gone = False
    except ProcessLookupError:
        pass
dead = lambda: gone
read = lambda suffix: pathlib.Path(b + suffix).read_text() if pathlib.Path(b + suffix).exists() else None
r = json.loads(read(".json") or "{}")
rc = int(read(".rc")) if read(".rc") else None
out = read(".out") or ""
recs = lambda p: [json.loads(l) for l in pathlib.Path(p).read_text().splitlines() if l.strip()]
lp = e / "cards" / "logs" / f"{cid}.ledger.jsonl"
L = recs(lp) if lp.exists() else []
A = [recs(p) for p in sorted(glob.glob(str(e / "cards" / "logs" / f"{cid}.ledger.*.jsonl")))]
H = subprocess.run(["git", "-C", repo, "rev-parse", f"card/v3-{cid}"], capture_output=True, text=True).stdout.strip()
print(out)
print("result:", r, "rc:", rc, "ledger:", L, "tip:", H)
sys.exit(0 if eval(expr) else 1)
P
}
# by_hand ID SUBJECT: a commit of verdicts/ID.md in ID's worktree, outside any run.
by_hand() { (cd "$ewt/$1" && echo "$2" >"nucleus/v3/verdicts/$1.md" && git add "nucleus/v3/verdicts/$1.md" && git commit -qm "$2"); }
# closed NAME: the ledger's last record ends the run its previous record started, at the branch tip.
closed='L[-1].get("end") == H and L[-1]["run"] == L[-2]["run"] and "start" in L[-2]'
flagged() { res "$1" "r['scope'] == 'FAIL' and not r['pass'] and 'nucleus/v3/verdicts/${1%%-*}.md (protected)' in out"; }

# A: within one run, attempt 1 forges and attempt 2 does clean work (PREHEAD-PER-ATTEMPT).
e2e XA "forge;work" --attempts 2
expect_ok "runner e2e: A: a verdict forged in attempt 1 is still protected in attempt 2" \
  res XA "rc == 1 and r['scope'] == 'FAIL' and 'nucleus/v3/verdicts/XA.md (protected)' in out.split('attempt 2:')[1].split('final scope')[0]"
# B: across runs, run 1's scope failure ends it; the --keep rerun does clean work (ANCHOR-IGNORED).
e2e XB-1 forge --attempts 1
e2e XB-2 work --attempts 1 --keep
expect_ok "runner e2e: B: a verdict forged in an earlier run is protected on --keep" flagged XB-2
# C: run 1 forges and is killed for silence; its range must still close (END-NOT-ON-KILL).
e2e XC-1 "forge silent" --attempts 1
expect_ok "runner e2e: C: a run whose last attempt was killed closes its ledger range" res XC-1 "$closed and r['killed'][0]['reason'] == 'silence' and r['scope'] == 'FAIL'"
e2e XC-2 work --attempts 1 --keep
expect_ok "runner e2e: C: a verdict forged before a kill is protected on --keep" flagged XC-2
# D: a start record with no end (SIGKILL), then the forge it left (OPEN-RANGE-TRUSTED).
git -C "$r" worktree add -q -B card/v3-XD "$ewt/XD" e2e/base
printf '{"run": "sigkilled", "start": "%s", "t": 0}\n' "$(git -C "$ewt/XD" rev-parse HEAD)" >"$e/cards/logs/XD.ledger.jsonl"
by_hand XD "nucleus v3: XD verdict MERGE"
e2e XD work --attempts 1 --keep
expect_ok "runner e2e: D: a verdict committed inside an open (SIGKILLed) range is protected" flagged XD
# E: the gate's REWORK verdict between runs is trusted (ANCHOR-OVERREACH; keeps the X-6 N1 fix).
e2e XE-1 work --attempts 1
expect_ok "runner e2e: E: premise: run 1 passes" res XE-1 "rc == 0 and r['pass'] and $closed"
by_hand XE "nucleus v3: XE verdict REWORK"
e2e XE-2 work --attempts 1 --keep
expect_ok "runner e2e: E: the gate's verdict between runs is not a violation" res XE-2 "rc == 0 and r['pass'] and r['scope'] == 'ok' and '(protected)' not in out"
# F: as E, with an orchestrator commit to spikes/ on top of the gate's verdict (LOG-NO-PATH).
e2e XF-1 work --attempts 1
by_hand XF "nucleus v3: XF verdict REWORK"
(cd "$ewt/XF" && echo note >nucleus/v3/spikes/XF-note.txt && git add nucleus/v3/spikes && git commit -qm "nucleus v3: XF orchestrator note")
e2e XF-2 work --attempts 1 --keep
expect_ok "runner e2e: F: a commit on top of the gate's verdict between runs is not a violation" res XF-2 "rc == 0 and r['pass'] and r['scope'] == 'ok' and '(protected)' not in out"
# G: a killed attempt's tokens count, and the kill names the silence (KILLED-TOKENS-DROPPED, SILENCE-MSG-GENERIC).
e2e XG silent --attempts 1
expect_ok "runner e2e: G: a killed attempt's tokens are counted and its silence reported" \
  res XG "r['in'] == 1234 and r['out'] == 56 and r['reasoning'] == 7 and 'killed after' in out and 'of silence' in out and r['killed'][0]['reason'] == 'silence' and r['killed'][0]['silence_s'] >= 1"
# G2: a fresh run moves the old ledger aside and starts a new one.
e2e XG-2 quota --attempts 1
expect_ok "runner e2e: G: a fresh run moves the old ledger aside" res XG-2 "len(A) == 1 and len(A[0]) == 2 and len(L) == 2 and A[0][0]['run'] != L[0]['run']"
# H: no output at all is a provider stall, never "of silence".
e2e XH mute --attempts 1
expect_ok "runner e2e: H: no output at all is reported as such" \
  res XH "'no output at all after' in out and 'of silence' not in out and [k['reason'] for k in r['killed']] == ['no-output'] * 3 and r['in'] == 0"
# I: the quota sys.exit closes the range, and the forge before it stays protected.
e2e XI-1 "forge quota" --attempts 1
expect_ok "runner e2e: I: a quota exit closes the ledger range" res XI-1 "'provider usage limit reached' in out and $closed"
e2e XI-2 work --attempts 1 --keep
expect_ok "runner e2e: I: a verdict forged before a quota exit is protected on --keep" flagged XI-2
# J: --keep on a pre-ledger worktree whose branch has a verdict-subject commit (NO-LEDGER-TRUSTED).
git -C "$r" worktree add -q -B card/v3-XJ "$ewt/XJ" e2e/base
by_hand XJ "nucleus v3: XJ verdict REWORK"
e2e XJ-1 work --attempts 1 --keep
expect_ok "runner e2e: J: --keep without a ledger refuses to start" \
  res XJ-1 "rc == 1 and 'no trust ledger for XJ; rerun without --keep, or pass --trust-existing after inspecting the branch' in out and L == [] and 'attempt 1:' not in out"
e2e XJ-2 work --attempts 1 --keep --trust-existing
expect_ok "runner e2e: J: --trust-existing records the decision and starts" res XJ-2 "rc == 0 and r['pass'] and L[0].get('trust-existing') is True and L[1]['end'] == L[0]['start'] and $closed"
# K: SIGTERM mid-attempt kills the child before the end record (CHILD-SURVIVES; C and I
# cannot see it, since their child is already dead when main() exits).
export E2E_SIGTERM_AFTER=1 E2E_STALL_S=30
e2e XK "forge silent" --attempts 1
unset E2E_SIGTERM_AFTER E2E_STALL_S
expect_ok "runner e2e: K: SIGTERM kills opencode and closes the ledger range" res XK "rc == 143 and dead() and $closed"
# BG: opencode exits normally and leaves a backgrounded forge in its group; the
# runner kills the group before acceptance (X-6-F1-F1-F2; BG-NO-KILL-ON-EXIT,
# BG-FINALLY-ONLY, BG-PID-NOT-GROUP, OWN-SIGTERM). K cannot see it: its opencode
# is alive when killed. This covers same-group children only, so it proves nothing
# about real opencode, whose tool shells are in their own sessions and escape the
# kill (verdict X-6-F1-F1-F2 R1). Containing those is X-6-F1-F1-F2-F1.
# e2ea NAME ACCEPT FAKE_OC ARGS...: as e2e, but the card's Acceptance is ACCEPT
# and then the gate (e2e's ec rewrites its card with `true` on every call).
e2ea() { local n=$1 acc=$2 oc=$3 id=${1%%-*}; shift 3; rm -f "$e/$n.n" "$e/$n.n.bg"
  printf '# CARD %s: e2e\n\nClass: test        Author: S (t)        Status: ready\nBase: e2e/base        Depends: —        Gate: S\n\n## Goal\nt\n\n## Touch only\n- nucleus/v3/spikes/\n\n## Acceptance\n- `%s`\n- `cd nucleus/v3 && bash scripts/gate.sh`\n' "$id" "$acc" >"$e/cards/$id.local.md"
  (cd "$r" && FAKE_OC=$oc FAKE_OC_N=$e/$n.n E2E=$e PYTHONDONTWRITEBYTECODE=1 python3 "$t/e2e.py" "$v3/tools/run_card.py" "$e/$n" "$e/cards/$id.local.md" --base e2e/base "$@") || true; }
# bg_unforged ID: card/v3-ID exists, no commit on it has the forge's subject,
# and its tip tree has no verdicts/ID.md.
bg_unforged() { local b=card/v3-$1 subjects
  git -C "$r" rev-parse -q --verify "$b^{commit}" >/dev/null || { echo "no branch $b"; return 1; }
  subjects=$(git -C "$r" log --format=%s "$b") || return 1
  if grep -qxF "nucleus v3: $1 verdict MERGE" <<<"$subjects"; then echo "the forge landed on $b"; return 1; fi
  if git -C "$r" cat-file -e "$b:nucleus/v3/verdicts/$1.md" 2>/dev/null; then echo "verdicts/$1.md is in $b's tip"; return 1; fi
}
# The liveness check is an Acceptance command, so it runs after opencode exited
# and before the end record. A zombie counts as dead (its reaper may not have
# run yet); a missing pid file fails it, so it cannot pass vacuously.
bg_alive_check="bash -c 'p=\$(cat $e/XBG.n.bg) || exit 1; s=\$(ps -o stat= -p \"\$p\" 2>/dev/null) || true; [ -z \"\$s\" ] || [ \"\${s#Z}\" != \"\$s\" ]'"
e2ea XBG "$bg_alive_check" "work bgforge" --attempts 1
expect_ok "runner e2e: BG: premise: bgforge recorded its background pid" grep -qxE '[0-9]+' "$e/XBG.n.bg"
expect_ok "runner e2e: BG: a same-group background child is killed when opencode exits normally (fake only; real tool shells escape, X-6-F1-F1-F2-F1)" res XBG "rc == 0 and r['pass'] and dead() and $closed"
expect_ok "runner e2e: BG: a same-group background forge never lands (fake only; real tool shells escape, X-6-F1-F1-F2-F1)" bg_unforged XBG
# The finally's kill_child must not killpg again: by then the pid may be an
# unrelated job's pgid (verdict R2). Driver copies change os.killpg, as L's copy
# changes stray_commits; the premises prove the copies were patched.
# XKP logs each killpg and passes it on: one normal exit, exactly one killpg.
sed '/^spec = importlib.util.spec_from_file_location/a real_kp = os.killpg; os.killpg = lambda pid, sig: (open(os.environ["KP_LOG"], "a").write(f"{pid} {int(sig)}\\n"), real_kp(pid, sig))[1]' "$t/e2e.py" >"$t/e2e-kp.py"
expect_ok "runner e2e: BG: premise: the driver copy logs killpg" grep -qF 'os.environ["KP_LOG"]' "$t/e2e-kp.py"
rm -f "$e/XKP.n" "$e/XKP.kp"
(cd "$r" && FAKE_OC=work FAKE_OC_N=$e/XKP.n KP_LOG=$e/XKP.kp E2E=$e PYTHONDONTWRITEBYTECODE=1 python3 "$t/e2e-kp.py" "$v3/tools/run_card.py" "$e/XKP" "$(ec XKP)" --base e2e/base --attempts 1) || true
bg_one_kill() { local want got; want="$(cat "$e/XKP.n.pid") $(python3 -c 'import signal; print(int(signal.SIGKILL))')"; got=$(grep "^$(cat "$e/XKP.n.pid") " "$e/XKP.kp"); echo "want: $want"; echo "got: $got"; [ "$got" = "$want" ]; }
expect_ok "runner e2e: BG: a normal exit calls killpg once, with SIGKILL, on opencode's group" bg_one_kill
expect_ok "runner e2e: BG: the one-killpg run passes and closes its range" res XKP "rc == 0 and r['pass'] and dead() and $closed"
# XPE: killpg raises PermissionError after a normal exit. It stays uncaught and
# fails the run, and the mark lets the finally write the end record (verdict R3).
sed '/^spec = importlib.util.spec_from_file_location/a os.killpg = lambda pid, sig: (_ for _ in ()).throw(PermissionError(1, "Operation not permitted (row)"))' "$t/e2e.py" >"$t/e2e-pe.py"
expect_ok "runner e2e: BG: premise: the driver copy makes killpg raise" grep -qF 'Operation not permitted (row)' "$t/e2e-pe.py"
rm -f "$e/XPE.n"
(cd "$r" && FAKE_OC=work FAKE_OC_N=$e/XPE.n E2E=$e PYTHONDONTWRITEBYTECODE=1 python3 "$t/e2e-pe.py" "$v3/tools/run_card.py" "$e/XPE" "$(ec XPE)" --base e2e/base --attempts 1) || true
expect_ok "runner e2e: BG: a raising killpg after a normal exit crashes the run and still leaves an end record" res XPE "rc == 1 and not r.get('pass') and 'PermissionError' in out and $closed"
# A marked child whose leader is still alive is killed again, not waited on
# forever: the first killpg raises, the second call must still kill. timeout
# turns a hang into a failure.
bg_live_leader() { PYTHONDONTWRITEBYTECODE=1 timeout 20 python3 - "$v3/tools/run_card.py" <<'P'
import importlib.util, os, signal, subprocess, sys
spec = importlib.util.spec_from_file_location("rc", sys.argv[1]); rc = importlib.util.module_from_spec(spec); spec.loader.exec_module(rc)
c = subprocess.Popen(["sleep", "30"], process_group=0)
real = os.killpg
def deny(pid, sig):
    raise PermissionError(1, "Operation not permitted (row)")
os.killpg = deny
try:
    rc.kill_child(c)
    sys.exit("the first kill_child did not raise")
except PermissionError:
    pass
os.killpg = real
rc.kill_child(c)
sys.exit(0 if c.returncode == -signal.SIGKILL else f"returncode {c.returncode}")
P
}
expect_ok "runner e2e: BG: a second kill_child still kills a leader that is alive" bg_live_leader
# --- containment: real unshare around a fake that leaves the session (X-6-F1-F1-F2-F1) ---
# Real opencode starts tool shells with setsid, so the group kill misses them
# (verdict X-6-F1-F1-F2 R1). setsidforge does the same. The fake's pid file holds a
# namespace pid in these runs, so each is removed before res, which would otherwise
# signal the host process with that number. The binary itself is a gate step (it is
# not in CI): the card's RESULT records that proof. CT-NO-CONTAIN, CT-KILL-LEADER-ONLY.
expect_ok "runner e2e: CT: premise: this host runs the real unshare (no skip: without it these rows fail)" test -x "$E2E_REAL_UNSHARE"
ct_rc() { PYTHONDONTWRITEBYTECODE=1 timeout 60 python3 - "$v3/tools/run_card.py" "$@" <<'P'
import importlib.util, os, select, shutil, signal, subprocess, sys, tempfile, threading, time
spec = importlib.util.spec_from_file_location("rc", sys.argv[1]); rc = importlib.util.module_from_spec(spec); spec.loader.exec_module(rc)
which = sys.argv[2]
def alive(pid):
    try:
        with open(f"/proc/{pid}/stat") as f:
            return f.read().rsplit(")", 1)[1].split()[0] != "Z"
    except OSError:
        return False
def dead_soon(pid):
    # A SIGKILLed same-group member is not waited on, so give it a moment.
    for _ in range(100):
        if not alive(pid):
            return True
        time.sleep(0.02)
    return False
def spawn(script):
    f = tempfile.NamedTemporaryFile(delete=False, mode="r")
    c = subprocess.Popen(["bash", "-c", script.replace("PIDFILE", f.name)], process_group=0)
    while not f.seek(0) and not (text := f.read().strip()):
        time.sleep(0.01)
    return c, int(text)
def exited_with_member():  # the leader is reaped, a same-group member lives on
    c, bg = spawn("sleep 30 & echo $! >PIDFILE; exit 0")
    while c.poll() is None:
        time.sleep(0.01)
    assert alive(bg), "premise: the member outlives its leader"
    return c, bg
def fail(why):
    if bg_left:
        os.kill(bg_left[0], signal.SIGKILL)
    sys.exit(why)
bg_left = []
if which == "second-child":
    # G2-MARK-CLASS-WIDE: one child's mark must not skip the next child's killpg.
    c1, bg1 = exited_with_member()
    rc.kill_child(c1)
    if not dead_soon(bg1):
        sys.exit("premise: the first child's member survived")
    c2, bg2 = exited_with_member()
    bg_left.append(bg2)
    rc.kill_child(c2)
    if not dead_soon(bg2):
        fail("the second child's member survived")
elif which == "second-group":
    # G2-SECOND-KILLS-PID-ONLY: a repeat kill on a live leader reaches its group.
    c, bg = spawn("sleep 30 & echo $! >PIDFILE; wait")
    bg_left.append(bg)
    real = os.killpg
    def deny(pid, sig):
        raise PermissionError(1, "Operation not permitted (row)")
    os.killpg = deny
    try:
        rc.kill_child(c)
        fail("the first kill_child did not raise")
    except PermissionError:
        pass
    os.killpg = real
    # Timed: kill_child waits on the leader's child's pidfd, here the member, so a
    # kill that missed it returns only when its sleep ends, with the member dead.
    t0 = time.time()
    rc.kill_child(c)
    took = time.time() - t0
    if took > 10:
        fail(f"the repeat kill took {took:.0f} s: the member's own end finished it, not the kill")
    if not dead_soon(bg):
        fail("the live leader's member survived the repeat kill")
elif which == "signal-window":
    # Verdict F1: a SIGTERM just before killpg leaves no mark, so the finally kills.
    c, bg = exited_with_member()
    bg_left.append(bg)
    signal.signal(signal.SIGTERM, rc._sigterm)
    src = open(sys.argv[1]).read().splitlines()
    line = next(i + 1 for i, l in enumerate(src) if l.strip() == "os.killpg(proc.pid, signal.SIGKILL)")
    def tr(fr, ev, arg):
        if fr.f_code.co_name != "kill_child":
            return None
        def lt(fr, ev, arg):
            if ev == "line" and fr.f_lineno == line:
                sys.settrace(None)
                os.kill(os.getpid(), signal.SIGTERM)
            return lt
        return lt
    sys.settrace(tr)
    try:
        rc.kill_child(c)
        fail("premise: the SIGTERM did not land inside kill_child")
    except SystemExit as e:
        sys.settrace(None)
        if e.code != 143:
            fail(f"premise: SystemExit {e.code}")
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    rc.kill_child(c)  # the finally's call
    if not dead_soon(bg):
        fail("the member survived: the mark was set though killpg never ran")
elif which == "init-wait":
    # CT-NO-INIT-WAIT: kill_child must not return before the init's pidfd is ready.
    # A pipe stands in for the pidfd; reaping the leader alone proves nothing.
    c, _ = spawn("sleep 30 & echo $! >PIDFILE; wait")
    r, w = os.pipe()
    os.pidfd_open = lambda pid: r
    threading.Timer(0.5, os.close, (w,)).start()
    t0 = time.time()
    rc.kill_child(c)
    took = time.time() - t0
    if took < 0.4:
        sys.exit(f"kill_child returned after {took:.3f} s, before the init was gone")
    try:
        os.fstat(r)
        sys.exit("the pidfd was left open")
    except OSError:
        pass
elif which == "init-wait-signal":
    # A SIGTERM during that wait raises out; the finally's call must finish the wait.
    c, _ = spawn("sleep 30 & echo $! >PIDFILE; wait")
    r, w = os.pipe()
    os.pidfd_open = lambda pid: r
    real, calls = select.select, []
    def interrupted(*a):
        calls.append(a)
        if len(calls) == 1:
            raise SystemExit(143)
        return real(*a)
    select.select = interrupted
    os.close(w)
    try:
        rc.kill_child(c)
        sys.exit("premise: the first kill_child did not raise")
    except SystemExit as e:
        if e.code != 143:
            raise
    rc.kill_child(c)
    if len(calls) != 2:
        sys.exit(f"the finally's call did not resume the wait: {len(calls)} wait(s)")
elif which == "real-empty":
    # On the real binary: when kill_child returns, nothing with the mark is alive.
    mark = f"E2E_CT_MARK=ct-real-{os.getpid()}".encode()
    def members():
        hits = []
        for d in os.listdir("/proc"):
            if d.isdigit():
                try:
                    with open(f"/proc/{d}/environ", "rb") as f:
                        if mark in f.read().split(b"\0"):
                            hits.append(d)
                except OSError:
                    pass
        return hits
    env = dict(os.environ, E2E_NS="real", E2E_CT_MARK=mark.decode().split("=", 1)[1])
    c = subprocess.Popen([*rc.CONTAIN, "bash", "-c", "setsid sleep 30 & setsid sleep 30 & sleep 30"], env=env, process_group=0)
    for _ in range(300):
        if len(members()) >= 4:  # unshare, the init and its two sessions
            break
        time.sleep(0.01)
    else:
        sys.exit(f"premise: the scan never saw the members: {members()}")
    rc.kill_child(c)
    left = members()
    if left:
        sys.exit(f"alive when kill_child returned: {left}")
elif which == "unavailable":
    # Each host-feature check names itself, and the suite's own host passes them.
    if rc.containment_problem() is not None:
        sys.exit(f"premise: refused on this host: {rc.containment_problem()}")
    real_which, shutil.which = shutil.which, lambda *a: None
    if "unshare (util-linux) is not on PATH" not in (rc.containment_problem() or ""):
        sys.exit("no unshare is not refused")
    shutil.which = real_which
    real_pidfd = os.pidfd_open
    del os.pidfd_open
    if "os.pidfd_open" not in (rc.containment_problem() or ""):
        sys.exit("no pidfd_open is not refused")
    os.pidfd_open = real_pidfd
    real_getpid, os.getpid = os.getpid, lambda: 999999999
    if "CONFIG_PROC_CHILDREN" not in (rc.containment_problem() or ""):
        sys.exit("no children file is not refused")
    os.getpid = real_getpid
else:
    sys.exit("unknown " + which)
P
}
expect_ok "runner e2e: CT: when kill_child returns, no member of a real container is alive, setsid or not" ct_rc real-empty
expect_ok "runner e2e: CT: kill_child waits for the init's pidfd, not just for the leader" ct_rc init-wait
expect_ok "runner e2e: CT: a SIGTERM during the init wait leaves the finally's call to finish it" ct_rc init-wait-signal
expect_ok "runner e2e: CT: one child's mark does not skip the next child's group kill (G2-MARK-CLASS-WIDE)" ct_rc second-child
expect_ok "runner e2e: CT: a repeat kill on a live leader reaches its group (G2-SECOND-KILLS-PID-ONLY)" ct_rc second-group
expect_ok "runner e2e: CT: a SIGTERM just before killpg leaves no mark, so the finally kills (verdict F1)" ct_rc signal-window
expect_ok "runner e2e: CT: no unshare, pidfd_open or children file is refused by name" ct_rc unavailable
# ct-alive.sh FAKE_OC_N: fails if a process carrying the forge's mark is alive. As
# an Acceptance command it runs after opencode's exit and before the end record.
cat >"$t/ct-alive.sh" <<'S'
#!/usr/bin/env bash
hits=$(grep -lzxaF "E2E_CT_MARK=$1" /proc/[0-9]*/environ 2>/dev/null)
[ -z "$hits" ] || { echo "survivor: $hits"; exit 1; }
S
ct_gone() { bash "$t/ct-alive.sh" "$e/$1.n"; }
# ct_unforged ID: bg_unforged, and the forge's first step left no file either.
ct_unforged() { bg_unforged "$1" && [ ! -e "$ewt/$1/nucleus/v3/verdicts/$1.md" ]; }
# ct_ids NAME: the forge's pgid and sid both differ from the fake's, and it leads
# its own session, so these rows cannot pass the way a same-group forge would.
# Each `local x=` and read's || matters: a refused run writes no file, and an unset
# name under set -u would end the whole suite with its stdout still in expect_ok's
# redirect, so its abort line would be lost with the file.
ct_ids() { local op= os= fp= fs=; read -r op os <"$e/$1.n.ocid" || return 1; read -r fp fs <"$e/$1.n.forgeid" || return 1; echo "fake $op $os, forge $fp $fs"; [ "$op" != "$fp" ] && [ "$os" != "$fs" ] && [ "$fp" = "$fs" ]; }
# ct_in_ns NAME: the fake ran as pid 1 of another pid namespace, with the real uid.
ct_in_ns() { local ns= p= u=; read -r ns p u <"$e/$1.n.ocns" || return 1; echo "$ns $p $u; suite $(readlink /proc/self/ns/pid) $(id -u)"; [ "$ns" != "$(readlink /proc/self/ns/pid)" ] && [ "$p" = 1 ] && [ "$u" = "$(id -u)" ]; }
# ct_owner ID: the honest commit made inside landed, under the suite's identity,
# and its file and objects belong to the real uid.
ct_owner() { local c=$ewt/$1 h f o me; me=$(id -u)
  h=$(git -C "$c" log -1 --format=%H --grep="^nucleus v3: $1 work$") || return 1; [ -n "$h" ] || { echo "no honest commit"; return 1; }
  o=$(git -C "$c" var GIT_COMMITTER_IDENT); o=${o% * *}
  [ "$(git -C "$c" log -1 --format='%cn <%ce>' "$h")" = "$o" ] || { echo "wrong identity: not $o"; return 1; }
  f=$c/nucleus/v3/spikes/$1.txt; o=$(git -C "$c" rev-parse --git-common-dir)/objects/${h:0:2}/${h:2}
  echo "file $(stat -c %u "$f") object $(stat -c %u "$o") me $me"; [ "$(stat -c %u "$f")" = "$me" ] && [ "$(stat -c %u "$o")" = "$me" ]; }
# The forge's delay outlasts the silence kill (about 2.4 s, one idle second past a
# 1 s limit, rounded) under load: a forge landing before a kill is a worker's commit
# inside its range, which the existing rows cover, and not what these prove.
export E2E_FORGE_S=8 E2E_NS=real
# Normal exit: the forge is dead when Acceptance starts, and the honest commit lands.
e2ea XCN "bash $t/ct-alive.sh $e/XCN.n" "work setsidforge" --attempts 1
rm -f "$e/XCN.n.pid"
expect_ok "runner e2e: CT: nothing of the forge is alive once the normal-exit run ends" ct_gone XCN
expect_ok "runner e2e: CT: premise: the normal-exit run was in a real namespace" ct_in_ns XCN
expect_ok "runner e2e: CT: premise: the forge's pgid and sid differ from the fake's" ct_ids XCN
expect_ok "runner e2e: CT: a setsid forge is dead before Acceptance after a normal exit, and the run passes" res XCN "rc == 0 and r['pass'] and $closed"
expect_ok "runner e2e: CT: an honest commit made in the container lands, owned by the real uid" ct_owner XCN
# Silence kill.
e2e XCS "work setsidforge silent" --attempts 1
rm -f "$e/XCS.n.pid"
expect_ok "runner e2e: CT: nothing of the forge is alive once the silence-kill run ends" ct_gone XCS
expect_ok "runner e2e: CT: premise: the silence-kill forge's pgid and sid differ from the fake's" ct_ids XCS
expect_ok "runner e2e: CT: the silence kill is reported and its range closes" res XCS "$closed and r['killed'][0]['reason'] == 'silence'"
# SIGTERM to the runner.
export E2E_SIGTERM_AFTER=2 E2E_STALL_S=30
e2e XCT "work setsidforge silent" --attempts 1
unset E2E_SIGTERM_AFTER E2E_STALL_S
rm -f "$e/XCT.n.pid"
expect_ok "runner e2e: CT: nothing of the forge is alive once the SIGTERM run ends" ct_gone XCT
expect_ok "runner e2e: CT: premise: the SIGTERM forge's pgid and sid differ from the fake's" ct_ids XCT
expect_ok "runner e2e: CT: SIGTERM exits 143 and closes the range" res XCT "rc == 143 and $closed"
# opencode itself leaves unshare's group: only --kill-child ends the namespace.
e2e XCL "work setsidforge leavegroup" --attempts 1
rm -f "$e/XCL.n.pid"
expect_ok "runner e2e: CT: nothing of the forge is alive when opencode left the group (CT-NO-KILL-CHILD)" ct_gone XCL
expect_ok "runner e2e: CT: premise: opencode led its own session and group there" grep -qxE ' *1 +1' "$e/XCL.n.leftid"
expect_ok "runner e2e: CT: the leave-group run's silence kill closes its range" res XCL "$closed and r['killed'][0]['reason'] == 'silence'"
# Past every forge's delay, none of them landed. A surviving forge would have by now.
sleep $((E2E_FORGE_S + 2))
for x in XCN XCS XCT XCL; do expect_ok "runner e2e: CT: the setsid forge never lands ($x)" ct_unforged "$x"; done
unset E2E_NS
# Premise, and today's bug: the same forge, spawned as before this card (the no-op
# shim), leaves the fake's group, survives into Acceptance and lands after the run.
ct_landed() { local i s; for i in $(seq 1 60); do s=$(git -C "$r" log --format=%s "card/v3-$1") || return 1; grep -qxF "nucleus v3: $1 verdict MERGE" <<<"$s" && return 0; sleep 0.5; done; echo "the forge never landed"; return 1; }
e2ea XCP "bash $t/ct-alive.sh $e/XCP.n" "work setsidforge" --attempts 1
expect_ok "runner e2e: CT: premise: without the container the setsid forge survives into Acceptance" res XCP "not r.get('pass') and 'FAIL: bash $t/ct-alive.sh' in out"
expect_ok "runner e2e: CT: premise: without the container the forge's pgid and sid differ too" ct_ids XCP
expect_ok "runner e2e: CT: premise: without the container the setsid forge lands" ct_landed XCP
# Fail closed: no container, no run. Nothing is touched before the refusal.
ct_untouched() { [ ! -e "$e/$1.n" ] && [ ! -e "$ewt/$1" ] && ! git -C "$r" rev-parse -q --verify "card/v3-$1" >/dev/null; }
E2E_NS=fail e2e XCR work --attempts 1
expect_ok "runner e2e: CT: user namespaces off: the runner refuses and names why (CT-FALLBACK)" res XCR "rc == 1 and 'cannot contain opencode' in out and 'Operation not permitted' in out and \"owner's call\" in out and 'attempt 1:' not in out and L == []"
expect_ok "runner e2e: CT: the refusal started no opencode, worktree or branch" ct_untouched XCR
E2E_NS=lie e2e XCQ work --attempts 1
expect_ok "runner e2e: CT: an unshare that exits 0 with no namespace is refused too" res XCQ "rc == 1 and 'cannot contain opencode' in out and \"want '1 \" in out and 'attempt 1:' not in out and L == []"
expect_ok "runner e2e: CT: that refusal started no opencode, worktree or branch" ct_untouched XCQ
# --- Acceptance through the container (X-6-F1-F1-F2-F1-F1) ---------------------
# Measured with real opencode before this card: a committed script ran a setsid forge
# from an Acceptance line, the run passed, and the forge landed 7.6 s after the end
# record. Each line now runs in its own container, ended before the next line starts.
# The CT row "dead before Acceptance" reads a /proc that is now the line's own, so
# the heartbeat rows below are what still see an opencode forge from inside a line.
# ACC-NO-CONTAIN, ACC-NO-END, ACC-TIMEOUT-LEADER-ONLY, TMPDIR-SHARED.
acc_rc() { PYTHONDONTWRITEBYTECODE=1 timeout 60 python3 - "$v3/tools/run_card.py" "$@" <<'P'
import importlib.util, os, signal, subprocess, sys, tempfile, threading, time
spec = importlib.util.spec_from_file_location("rc", sys.argv[1]); rc = importlib.util.module_from_spec(spec); spec.loader.exec_module(rc)
which = sys.argv[2]
env = dict(os.environ, E2E_NS="real")
def sp(cmd, **extra):
    return rc.spawn_contained(["/bin/sh", "-c", cmd], os.getcwd(), dict(env, **extra), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
def members(mark):
    hits = []
    for d in os.listdir("/proc"):
        if d.isdigit():
            try:
                with open(f"/proc/{d}/environ", "rb") as f:
                    if f"E2E_CT_MARK={mark}".encode() in f.read().split(b"\0"):
                        hits.append(d)
            except OSError:
                pass
    return hits
if which == "timeout-init-wait":
    # ACC-TIMEOUT-LEADER-ONLY: past the timeout, not before the init is gone. A pipe
    # stands in for the pidfd, as the CT row's does: reaping unshare proves nothing.
    r, w = os.pipe()
    os.pidfd_open = lambda pid: r
    c = sp("sleep 30")
    for _ in range(3000):  # forked, so the kill finds an init to wait on, however busy the host
        if open(f"/proc/{c.pid}/task/{c.pid}/children").read().split():
            break
        time.sleep(0.01)
    else:
        sys.exit("premise: unshare never forked")
    threading.Timer(2.0, os.close, (w,)).start()
    t0 = time.time()
    try:
        rc.finish_contained(c, 0.5)
        sys.exit("no TimeoutExpired")
    except subprocess.TimeoutExpired:
        pass
    took = time.time() - t0
    if took < 1.9:
        sys.exit(f"returned after {took:.2f} s, before the init was gone")
elif which == "timeout-real":
    # On the real binary: when the timeout raises, no member is alive and the TMPDIR is gone.
    mark = f"acc-real-{os.getpid()}"
    c = sp("setsid sleep 30 & setsid sleep 30 & sleep 30", E2E_CT_MARK=mark)
    tmp = c.tmpdir
    for _ in range(3000):
        if len(members(mark)) >= 4:  # unshare, the init (sh or its last sleep), its two sessions
            break
        time.sleep(0.01)
    else:
        sys.exit(f"premise: the scan never saw the members: {members(mark)}")
    try:
        rc.finish_contained(c, 0.2)
        sys.exit("no TimeoutExpired")
    except subprocess.TimeoutExpired:
        pass
    if members(mark):
        sys.exit(f"alive when the timeout raised: {members(mark)}")
    if os.path.exists(tmp):
        sys.exit("the TMPDIR outlived the timeout")
elif which == "init-order":
    # G3-INIT-AFTER-KILL: the pidfd is looked up before the kill. Measured: a lookup
    # after it misses the init 17 times in 100, so the order is what the wait rests on.
    c = rc.spawn_contained(["sleep", "30"], os.getcwd(), env)
    for _ in range(3000):
        if open(f"/proc/{c.pid}/task/{c.pid}/children").read().split():
            break
        time.sleep(0.01)
    else:
        sys.exit("premise: unshare never forked")
    calls, real_init, real_kp = [], rc.container_init, os.killpg
    def spy_init(pid):
        fd = real_init(pid)
        calls.append(("init", fd is not None))
        return fd
    def spy_kp(pid, sig):
        calls.append(("killpg", True))
        return real_kp(pid, sig)
    rc.container_init, os.killpg = spy_init, spy_kp
    rc.end_container(c)
    if calls != [("init", True), ("killpg", True)]:
        sys.exit(f"call order: {calls}")
elif which == "compat":
    # The output and exit code gate_passed and failure_text read, contained against
    # what run() returns for the same line, byte for byte.
    wd = tempfile.mkdtemp(dir=os.environ["ACC_T"])
    lines = ["echo out; echo err >&2", "printf 'no newline'; exit 3", "echo err >&2; echo out", "exit 130", "nosuchcmd-acc-rc", "printf 'a\\r\\nb\\n'", "true",
             "echo 'STEP fmt EXIT=1'; echo 'GATE EXIT=1'; exit 1", "echo 'GATE EXIT=0'", "echo 'GATE EXIT=0'; echo late >&2", "cd /nonexistent-acc-rc && echo no", ": & exit 7"]
    for line in lines:
        plain = rc.run(line, wd, env=env)
        contained = rc.finish_contained(rc.spawn_contained(["/bin/sh", "-c", line], wd, rc.git_env(env), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True), 30)
        if plain != contained:
            sys.exit(f"{line!r}: run() gives {plain!r}, contained gives {contained!r}")
        if rc.gate_passed(*plain) != rc.gate_passed(*contained) or rc.failure_text(line, plain[1]) != rc.failure_text(line, contained[1]):
            sys.exit(f"{line!r}: gate_passed or failure_text differ")
elif which == "stdin":
    # A line that reads stdin ends at once: a background group stops on a terminal read.
    got = rc.finish_contained(sp("cat; echo done"), 10)
    if got != (0, "done\n"):
        sys.exit(f"got {got!r}")
elif which == "tmpdir":
    # TMPDIR-SHARED: two containers see different, writable TMPDIRs that are not the
    # inherited one, outside the working directory, and gone afterwards.
    shared = tempfile.mkdtemp(dir=os.environ["ACC_T"])
    env["TMPDIR"] = shared
    seen = []
    for _ in range(2):
        c = sp('echo "$TMPDIR"; : >"$TMPDIR/left"')
        tmp = c.tmpdir
        code, out = rc.finish_contained(c, 10)
        if code or out.strip() != tmp:
            sys.exit(f"exit {code}, saw {out.strip()!r}, spawned {tmp!r}")
        if os.path.exists(tmp):
            sys.exit(f"{tmp} outlived its container")
        seen.append(tmp)
    if seen[0] == seen[1] or shared in seen or any(s.startswith(os.getcwd() + "/") for s in seen):
        sys.exit(f"TMPDIRs: {seen}, inherited {shared}, cwd {os.getcwd()}")
elif which == "no-fallback":
    # A container that stops working after the probe is a failed line, never a bare run.
    rc.CONTAIN = ["/bin/false"]
    marker = os.path.join(os.environ["ACC_T"], "acc-nofallback")
    code, _ = rc.finish_contained(sp(f": >{marker}"), 10)
    if code == 0 or os.path.exists(marker):
        sys.exit(f"exit {code}, ran: {os.path.exists(marker)}")
else:
    sys.exit("unknown " + which)
P
}
export ACC_T=$t
expect_ok "runner e2e: ACC: a timeout waits for the init's pidfd, not just for unshare (ACC-TIMEOUT-LEADER-ONLY)" acc_rc timeout-init-wait
expect_ok "runner e2e: ACC: when a timeout raises, no member of a real container is alive and its TMPDIR is gone" acc_rc timeout-real
expect_ok "runner e2e: CT: the init's pidfd is looked up before the kill (G3-INIT-AFTER-KILL)" acc_rc init-order
expect_ok "runner e2e: ACC: output and exit code match run()'s byte for byte, through gate_passed and failure_text" acc_rc compat
expect_ok "runner e2e: ACC: a line reading stdin ends at once" acc_rc stdin
expect_ok "runner e2e: ACC: two containers see different TMPDIRs, not the inherited one, gone afterwards (TMPDIR-SHARED)" acc_rc tmpdir
expect_ok "runner e2e: ACC: a container that fails to start is a failed line, never a bare run" acc_rc no-fallback
# The value the card keeps: 60 minutes per command.
acc_s() { PYTHONDONTWRITEBYTECODE=1 python3 -c 'import importlib.util,sys; s=importlib.util.spec_from_file_location("rc",sys.argv[1]); rc=importlib.util.module_from_spec(s); s.loader.exec_module(rc); sys.exit(0 if rc.ACCEPT_S == 3600 else f"ACCEPT_S {rc.ACCEPT_S}")' "$v3/tools/run_card.py"; }
expect_ok "runner e2e: ACC: an Acceptance command's timeout is still 60 minutes" acc_s
# A driver copy, as L's: E2E_ACCEPT_S shrinks the timeout; E2E_SIGTERM_ON sends the
# runner a SIGTERM once that file exists; E2E_SEL_LOG logs each init wait with the
# ledger's end-record count at that moment, and the first wait gets a SIGTERM.
cat >"$t/e2e-acc.ins" <<'P'
import select, time
if os.environ.get("E2E_ACCEPT_S"):
    rc.ACCEPT_S = float(os.environ["E2E_ACCEPT_S"])
if os.environ.get("E2E_SIGTERM_ON"):
    def watch(on=pathlib.Path(os.environ["E2E_SIGTERM_ON"])):
        while not on.exists():
            time.sleep(0.02)
        os.kill(os.getpid(), signal.SIGTERM)
    threading.Thread(target=watch, daemon=True).start()
if os.environ.get("E2E_SEL_LOG"):
    real_select, sel_calls = select.select, []
    sel_ledger = pathlib.Path(os.environ["E2E"]) / "cards" / "logs" / (pathlib.Path(sys.argv[2]).name.split("-")[0] + ".ledger.jsonl")
    def spy_select(*a):
        sel_calls.append(a)
        ends = sum('"end"' in l for l in sel_ledger.read_text().splitlines())
        with open(os.environ["E2E_SEL_LOG"], "a") as f:
            f.write(f"{len(sel_calls)} ends={ends}\n")
        if len(sel_calls) == 1:
            os.kill(os.getpid(), signal.SIGTERM)
            time.sleep(30)
        return real_select(*a)
    select.select = spy_select
P
sed "/^spec = importlib.util.spec_from_file_location/r $t/e2e-acc.ins" "$t/e2e.py" >"$t/e2e-acc.py"
expect_ok "runner e2e: ACC: premise: the driver copy has the additions" grep -qF 'rc.ACCEPT_S = float' "$t/e2e-acc.py"
# e2eb NAME ACCEPT FAKE_OC ARGS...: e2ea through that driver.
e2eb() { local n=$1 acc=$2 oc=$3 id=${1%%-*}; shift 3; rm -f "$e/$n.n" "$e/$n.sel"
  printf '# CARD %s: e2e\n\nClass: test        Author: S (t)        Status: ready\nBase: e2e/base        Depends: —        Gate: S\n\n## Goal\nt\n\n## Touch only\n- nucleus/v3/spikes/\n\n## Acceptance\n- `%s`\n- `cd nucleus/v3 && bash scripts/gate.sh`\n' "$id" "$acc" >"$e/cards/$id.local.md"
  (cd "$r" && FAKE_OC=$oc FAKE_OC_N=$e/$n.n E2E=$e PYTHONDONTWRITEBYTECODE=1 python3 "$t/e2e-acc.py" "$v3/tools/run_card.py" "$e/$n" "$e/cards/$id.local.md" --base e2e/base "$@") || true; }
# acc_ids NAME: the line ran in another pid namespace, and its forge led its own session.
acc_ids() { local fp= fs= ns=; read -r fp fs <"$e/$1.n.forgeid" || return 1; read -r ns <"$e/$1.n.accns" || return 1; echo "forge $fp $fs, line $ns, suite $(readlink /proc/self/ns/pid)"; [ "$fp" = "$fs" ] && [ "$ns" != "$(readlink /proc/self/ns/pid)" ]; }
# sel_two NAME: two init waits, the second (the finally's) before any end record.
sel_two() { cat "$e/$1.sel"; [ "$(cat "$e/$1.sel")" = $'1 ends=0\n2 ends=0' ]; }
acc_forge="bash nucleus/v3/spikes/acc-forge.sh"
export E2E_NS=real
# On a line that passes.
e2ea XAN "$acc_forge" "work accforge" --attempts 1
rm -f "$e/XAN.n.pid"
expect_ok "runner e2e: ACC: premise: the passing line was in a real namespace, and its forge led a session" acc_ids XAN
expect_ok "runner e2e: ACC: nothing of a passing line's forge is alive once the run ends" ct_gone XAN
expect_ok "runner e2e: ACC: a run whose Acceptance left a forge passes and closes its range" res XAN "rc == 0 and r['pass'] and $closed"
# On a line that fails.
e2ea XAF "$acc_forge; exit 1" "work accforge" --attempts 1
rm -f "$e/XAF.n.pid"
expect_ok "runner e2e: ACC: premise: the failing line was in a real namespace, and its forge led a session" acc_ids XAF
expect_ok "runner e2e: ACC: nothing of a failing line's forge is alive once the run ends" ct_gone XAF
expect_ok "runner e2e: ACC: the failing line fails the run and its range closes" res XAF "rc == 1 and not r['pass'] and 'FAIL: $acc_forge; exit 1' in out and $closed"
# SIGTERM to the runner during a line.
E2E_SIGTERM_ON=$e/XAT.n.forgeid e2eb XAT "$acc_forge; sleep 60" "work accforge" --attempts 1
rm -f "$e/XAT.n.pid"
expect_ok "runner e2e: ACC: premise: the SIGTERM landed during the line, with its forge recorded" acc_ids XAT
expect_ok "runner e2e: ACC: nothing of the forge is alive once a SIGTERM during a line ends the run" ct_gone XAT
expect_ok "runner e2e: ACC: a SIGTERM during a line exits 143 and closes the range" res XAT "rc == 143 and $closed"
# Past the timeout: the end path, then the exception that today's subprocess.run raises.
E2E_ACCEPT_S=5 e2eb XAO "$acc_forge; sleep 60" "work accforge" --attempts 1
rm -f "$e/XAO.n.pid"
expect_ok "runner e2e: ACC: premise: the timed-out line was in a real namespace, and its forge led a session" acc_ids XAO
expect_ok "runner e2e: ACC: nothing of the forge is alive once a timeout ends the run" ct_gone XAO
expect_ok "runner e2e: ACC: a timeout stops the run with TimeoutExpired, as before, and closes its range" res XAO "rc == 1 and not r.get('pass') and 'TimeoutExpired' in out and $closed"
# Nothing else ends a line: silent past STALL_S and NO_OUTPUT_S (1 s here), it passes.
e2ea XAQ "sleep 4" work --attempts 1
rm -f "$e/XAQ.n.pid"
expect_ok "runner e2e: ACC: a line silent past the attempt limits is only ended by its own timeout" res XAQ "rc == 0 and r['pass'] and r['killed'] == [] and $closed"
# G3-FINALLY-GUARDED: a SIGTERM during the init wait, through main(). The first wait
# raises with unshare already reaped, so a finally guarded by poll() would skip the
# call and write the end record with the namespace still dying.
E2E_SEL_LOG=$e/XFS.sel e2eb XFS true "work setsidforge silent" --attempts 1
rm -f "$e/XFS.n.pid"
expect_ok "runner e2e: CT: a SIGTERM during opencode's init wait: the finally waits again, before the end record (G3-FINALLY-GUARDED)" sel_two XFS
expect_ok "runner e2e: CT: that run exits 143 and closes its range" res XFS "rc == 143 and $closed"
expect_ok "runner e2e: CT: nothing of the forge is alive after that SIGTERM" ct_gone XFS
# The same during a line's init wait: the finally must know the line's child too.
E2E_SIGTERM_ON=$e/XFA.n.forgeid E2E_SEL_LOG=$e/XFA.sel e2eb XFA "$acc_forge; sleep 60" "work accforge" --attempts 1
rm -f "$e/XFA.n.pid"
expect_ok "runner e2e: ACC: a second SIGTERM during a line's init wait: the finally waits again, before the end record" sel_two XFA
expect_ok "runner e2e: ACC: that run exits 143 and closes its range" res XFA "rc == 143 and $closed"
expect_ok "runner e2e: ACC: nothing of the forge is alive after that second SIGTERM" ct_gone XFA
# TMPDIR through main(): one per container, opencode's included, none left behind.
acc_tmpline='echo "$TMPDIR" >>"$FAKE_OC_N.tmps"; : >"$TMPDIR/left"'
e2ea XTM "$acc_tmpline" "work tmprec" --attempts 1
rm -f "$e/XTM.n.pid"
# tmps_all NAME N: N different TMPDIRs, all gone, none under the worktree or the inherited one.
tmps_all() { local x; cat "$e/$1.n.tmps"; [ "$(sort -u "$e/$1.n.tmps" | grep -c '')" = "$2" ] && [ "$(grep -c '' "$e/$1.n.tmps")" = "$2" ] || return 1
  while IFS= read -r x; do [ -n "$x" ] && [ ! -e "$x" ] && [ "$x" != "${TMPDIR:-}" ] || return 1; case $x in "$ewt"/*) return 1 ;; esac; done <"$e/$1.n.tmps"; }
expect_ok "runner e2e: ACC: opencode and a line get different TMPDIRs, gone after the run" tmps_all XTM 2
expect_ok "runner e2e: ACC: the TMPDIR run passes" res XTM "rc == 0 and r['pass'] and $closed"
# A restart after no output gets a fresh one too, and the killed one is gone. The limit
# outlasts the fake's first write under load: a kill before tmprec records nothing.
E2E_NO_OUTPUT_S=3 e2e XTR "tmprec mute" --attempts 1
rm -f "$e/XTR.n.pid"
expect_ok "runner e2e: ACC: each opencode start gets its own TMPDIR, the killed ones gone too" tmps_all XTR 3
# An opencode forge is dead while a line runs, seen from inside the line's container.
acc_hb_still='a=$(cat "$FAKE_OC_N.hb"); sleep 1; [ "$a" = "$(cat "$FAKE_OC_N.hb")" ]'
e2ea XHB "$acc_hb_still" "work hbforge" --attempts 1
rm -f "$e/XHB.n.pid"
expect_ok "runner e2e: ACC: an opencode forge is dead before Acceptance, seen from inside a line" res XHB "rc == 0 and r['pass'] and $closed"
# Past every forge's delay, none of them landed. A surviving forge would have by now.
sleep $((E2E_FORGE_S + 2))
for x in XAN XAF XAT XAO XFS XFA; do expect_ok "runner e2e: ACC: the setsid forge never lands ($x)" ct_unforged "$x"; done
unset E2E_NS
# Premise, and what the no-op shim stands for: a planted `unshare` on PATH that answers
# the probe switches the container off and the forge lands (verdict F2, measured with
# real opencode in the card's RESULT). This card does not close that: X-6-F1-F1-F2-F1-F2.
e2ea XAP "$acc_forge" "work accforge" --attempts 1
expect_ok "runner e2e: ACC: premise: through the no-op shim the same line's forge survives" ct_landed XAP
expect_ok "runner e2e: ACC: premise: through the no-op shim that run still passes" res XAP "rc == 0 and r['pass']"
# The heartbeat's premise: with no container the same line sees the forge beat.
e2ea XHP "$acc_hb_still" "work hbforge" --attempts 1
expect_ok "runner e2e: ACC: premise: through the no-op shim a line sees the opencode forge's heartbeat" res XHP "not r.get('pass') and 'FAIL: a=' in out"
# ACC-NO-END: a same-group child of one line, the only thing kill_child's killpg (not
# the namespace) reaches, must be dead when the next line starts. Through the no-op
# shim, where the group kill is all there is, as for BG. .sg proves line 1 ran.
acc_sg='E2E_CT_MARK=$FAKE_OC_N.sg bash -c "sleep 30" >/dev/null 2>&1 & echo $! >"$FAKE_OC_N.sg"'
e2ea XAE "$acc_sg"$'`\n- `'"bash $t/ct-alive.sh $e/XAE.n.sg" work --attempts 1
expect_ok "runner e2e: ACC: premise: the first line recorded its same-group child" grep -qxE '[0-9]+' "$e/XAE.n.sg"
expect_ok "runner e2e: ACC: a line's same-group child is dead before the next line starts (ACC-NO-END)" res XAE "rc == 0 and r['pass'] and $closed"
# The one-killpg row above counts opencode's group. Each line adds its own, once.
kp_each() { local n; cat "$e/XKP.kp"; n=$(grep -c '' "$e/XKP.kp"); [ "$n" = 3 ] && [ "$(cut -d ' ' -f 1 "$e/XKP.kp" | sort -u | grep -c '')" = 3 ] && ! grep -qv ' 9$' "$e/XKP.kp"; }
expect_ok "runner e2e: ACC: opencode and each line get one SIGKILL killpg, no group twice" kp_each
# L: main()'s stray stop names a failed listing as such (S-0-F2-F2-F1-F2; MAIN-MSG
# always says "committed outside its worktree"). No fake-opencode action makes
# the real git log fail without first tripping the ref check, so a copy of the
# driver passes the real stray_commits a branch that does not exist: its own
# failure entry then reaches main(). The premise proves the copy was patched.
sed '/^spec = importlib.util.spec_from_file_location/a real_sc = rc.stray_commits; rc.stray_commits = lambda card, branch, since: real_sc(card, "no-such-branch", since)' "$t/e2e.py" >"$t/e2e-lf.py"
expect_ok "runner e2e: L: premise: the driver copy points stray_commits at a missing branch" grep -qF 'real_sc(card, "no-such-branch", since)' "$t/e2e-lf.py"
rm -f "$e/XLF.n"
(cd "$r" && FAKE_OC=work FAKE_OC_N=$e/XLF.n E2E=$e PYTHONDONTWRITEBYTECODE=1 python3 "$t/e2e-lf.py" "$v3/tools/run_card.py" "$e/XLF" "$(ec XLF)" --base e2e/base --attempts 2) || true
expect_ok "runner e2e: L: a failed stray-commit listing stops the run and is named as one" \
  res XLF "rc == 1 and not r['pass'] and r['scope'] == 'OUTSIDE' and r['attempts'] == 1 and '[XLF] cannot list commits to check for stray ones; stopping:' in out.splitlines() and 'committed outside its worktree' not in out and any(l.startswith('git log ') and 'no-such-branch' in l and 'failed (exit' in l and l.endswith('(cannot list commits; treating as stray)') for l in out.splitlines())"
# Control: a real stray commit (the side action names the card on side-XOW, which
# the ref check allows) gets the other message, so the selector is not constant.
e2e XOW side --attempts 2
expect_ok "runner e2e: L: control: a commit naming the card on another branch is named a stray commit" \
  res XOW "rc == 1 and r['scope'] == 'OUTSIDE' and r['attempts'] == 1 and '[XOW] worker committed outside its worktree; stopping:' in out.splitlines() and 'cannot list commits' not in out and any(l.endswith(' nucleus v3: XOW side') for l in out.splitlines())"
git -C "$r" branch -q -D side-XOW
# --- runner: a malformed trust ledger trusts nothing (X-6-F1-F1-F1) ---------
# worker_commits reads only well-formed records; anything else is None ("trust
# nothing"), never a crash, a skip or an empty set. "--all" as a start once
# reached rev-list as an option and emptied the worker set; a nonexistent id
# fails the rev-list (G-REVLIST-FAIL-TRUSTS); "not json" is G-MALFORMED-TRUSTS.
# ledger_wc WANT LEDGER...: worker_commits over $r with base e2e/base, one
# ledger file per LEDGER (its whole text). WANT "none" requires None for every
# one, "empty" requires set(). A raised exception is a mismatch, printed as such.
# The LEDGER token @deep stands for one line of 100000 "[" then 100000 "]": too
# deep for json.loads (RecursionError at Python's default limit), and too long
# to pass comfortably as an argument. Not named wc: a function of that name
# shadowed coreutils wc for the rest of the suite, and in the subshells (cm,
# cpm, dr_cm) whose core-paths-lib.sh runs wc -c (verdict X-6-F1-F1-F1).
ledger_wc() { PYTHONDONTWRITEBYTECODE=1 python3 - "$v3/tools/run_card.py" "$r" "$t/ledger_wc" "$@" <<'P'
import importlib.util, pathlib, sys
spec = importlib.util.spec_from_file_location("rc", sys.argv[1]); rc = importlib.util.module_from_spec(spec); spec.loader.exec_module(rc)
repo, d, want, cases = pathlib.Path(sys.argv[2]), pathlib.Path(sys.argv[3]), sys.argv[4], sys.argv[5:]
d.mkdir(exist_ok=True)
ok = bool(cases)
for i, text in enumerate(cases):
    path = d / f"{i}.ledger.jsonl"
    text = "[" * 100000 + "]" * 100000 if text == "@deep" else text
    path.write_text(text + "\n")
    try:
        got = rc.worker_commits(repo, "e2e/base", path)
    except Exception as x:
        got = f"raised {type(x).__name__}: {x}"
    good = got is None if want == "none" else got == set()
    ok = ok and good
    print("ok  " if good else "BAD ", repr(text if len(text) < 200 else text[:20] + f"... ({len(text)} chars)"), "->", repr(got))
sys.exit(0 if ok else 1)
P
}
wo=$(git -C "$r" rev-parse e2e/base)
wcu=$(tr a-f A-F <<<"$wo")
wcx=deadbeefdeadbeefdeadbeefdeadbeefdeadbeef
expect_ok "runner ledger: premise: the uppercase id differs and the made-up id is no object here" \
  bash -c '[ "$1" != "$2" ] && ! git -C "$3" cat-file -e "$4^{commit}"' _ "$wcu" "$wo" "$r" "$wcx"
# The end cases and "$wo^0" below are ones git accepts, so a check weaker than
# OID_RE.fullmatch (a str test for ends, .match for starts) would trust them.
ledger_revs_resolve() { plain rev-parse --verify "$wo^0" && plain rev-parse --verify e2e/base; }
expect_ok "runner ledger: premise: \$wo^0 and e2e/base resolve, so those cases discriminate" ledger_revs_resolve
expect_ok "runner ledger: malformed records trust nothing" ledger_wc none \
  'not json' \
  '[]' \
  "{\"run\": 1, \"start\": \"$wo\"}" \
  '{"run": "a", "start": 123}' \
  '{"run": "a", "start": {}}' \
  '{"run": "a", "start": "--all"}' \
  '{"run": "a", "start": "HEAD"}' \
  "{\"run\": \"a\", \"start\": \"${wo:0:39}\"}" \
  "{\"run\": \"a\", \"start\": \"$wcu\"}" \
  "{\"run\": \"a\", \"start\": \"$wo\"}"$'\n''{"run": "a", "end": 123}' \
  "{\"run\": \"a\", \"start\": \"$wo\"}"$'\n''{"run": "a", "end": "--all"}' \
  "{\"run\": \"a\", \"start\": \"$wcx\"}" \
  "{\"run\": \"a\", \"start\": \"$wo\"}"$'\n''{"run": "a", "end": "HEAD"}' \
  "{\"run\": \"a\", \"start\": \"$wo\"}"$'\n''{"run": "a", "end": "e2e/base"}' \
  "{\"run\": \"a\", \"start\": \"$wo\"}"$'\n'"{\"run\": \"a\", \"end\": \"${wo:0:39}\"}" \
  "{\"run\": \"a\", \"start\": \"$wo\"}"$'\n'"{\"run\": \"a\", \"end\": \"$wcu\"}" \
  "{\"run\": \"a\", \"start\": \"$wo^0\"}" \
  '@deep'
expect_ok "runner ledger: control: a well-formed closed range lists an empty set, not None" ledger_wc empty \
  "{\"run\": \"a\", \"start\": \"$wo\"}"$'\n'"{\"run\": \"a\", \"end\": \"$wo\"}"
# The same ledgers end to end (as D: a hand-written ledger, a hand-made verdict
# commit, then a --keep run doing clean work): the verdict must be flagged. On
# the code before this card, XMB passed (the forged verdict was exempt) and XMD
# raised TypeError out of scope_violations. XME is the control: a well-formed
# closed range, so the verdict lies between runs and is trusted, as in E. (XMA
# to XME: none is a substring of another e2e id, nor another id of one of them.)
# xm ID LEDGER: a fresh XM* worktree at e2e/base, LEDGER (with TIP replaced by
# its HEAD) as its trust ledger, the hand-made verdict, and the --keep run.
xm() { git -C "$r" worktree add -q -B "card/v3-$1" "$ewt/$1" e2e/base
  printf '%s\n' "${2//TIP/$(git -C "$ewt/$1" rev-parse HEAD)}" >"$e/cards/logs/$1.ledger.jsonl"
  by_hand "$1" "nucleus v3: $1 verdict MERGE"
  e2e "$1" work --attempts 1 --keep; }
# flagged_raw ID: flagged's test, for a ledger res cannot parse (res reads every
# line as JSON). It also requires the bad line still first, then this run's
# start and end: the runner trusts nothing and repairs nothing. Like res, it
# first kills a leftover opencode child (its .n.pid), so the two behave alike.
flagged_raw() { PYTHONDONTWRITEBYTECODE=1 python3 - "$e/$1" "$e/cards/logs/$1.ledger.jsonl" "$1" <<'P'
import json, os, pathlib, signal, sys
b, lp, cid = sys.argv[1:4]
pidf = pathlib.Path(b + ".n.pid")
for kill in (os.killpg, os.kill) if pidf.exists() else ():  # os.kill: a child left in the runner's own group
    try:
        kill(int(pidf.read_text()), signal.SIGKILL)
    except ProcessLookupError:
        pass
read = lambda suffix: pathlib.Path(b + suffix).read_text() if pathlib.Path(b + suffix).exists() else ""
r, out, lines = json.loads(read(".json") or "{}"), read(".out"), pathlib.Path(lp).read_text().splitlines()
print(out)
print("result:", r, "ledger:", lines)
tail = [json.loads(l) for l in lines[1:]]
sys.exit(0 if r.get("scope") == "FAIL" and r.get("pass") is False and f"nucleus/v3/verdicts/{cid}.md (protected)" in out
         and lines[0] == "not json" and len(tail) == 2 and "start" in tail[0] and tail[1].get("end") and tail[0]["run"] == tail[1]["run"] else 1)
P
}
xm XMA 'not json'
expect_ok "runner e2e: XMA: an unparsable ledger line trusts nothing (G-MALFORMED-TRUSTS)" flagged_raw XMA
xm XMB '{"run":"r","start":"--all","t":0}'$'\n''{"run":"r","end":"TIP","t":0}'
expect_ok "runner e2e: XMB: an option-shaped start (--all) trusts nothing" flagged XMB
xm XMC "{\"run\":\"r\",\"start\":\"$wcx\",\"t\":0}"
expect_ok "runner e2e: XMC: a start git does not know trusts nothing (G-REVLIST-FAIL-TRUSTS)" flagged XMC
xm XMD '{"run":"r","start":123,"t":0}'
expect_ok "runner e2e: XMD: a non-string start trusts nothing, without raising" flagged XMD
xm XME '{"run":"r","start":"TIP","t":0}'$'\n''{"run":"r","end":"TIP","t":0}'
expect_ok "runner e2e: XME: control: a well-formed closed range trusts the verdict between runs" \
  res XME "rc == 0 and r['pass'] and r['scope'] == 'ok' and '(protected)' not in out"
# The ledger helper is ledger_wc, not wc: a function named wc would shadow
# coreutils wc here and in every subshell after it (cm, cpm, dr_cm), where
# core-paths-lib.sh's base_core_entries counts with wc -c. expect_ok runs the
# first row in this shell, so it sees any such function; the second makes the
# call that a shadowed wc -c breaks. git-env.sh is sourced too: it defines the
# verify_blob that base_core_entries needs (enforce-lib.sh sources it for cm).
wc_is_file() { [ "$(type -t wc)" = file ]; }
expect_ok "ledger block: coreutils wc is not shadowed" wc_is_file
core_entries_sub() { (cd "$v3" && . scripts/git-env.sh && . scripts/core-paths-lib.sh && base_core_entries nucleus/v3-scaffold nucleus/v3) >"$t/bce.out" || return 1
  cat "$t/bce.out"; grep -q '^tools/' "$t/bce.out"; }
expect_ok "ledger block: base_core_entries in a subshell reads CORE_PATHS (its wc -c is coreutils')" core_entries_sub
# --- runner: a branch rewritten since its ledger's runs (X-6-F1-F1-F3) ------
# Ledger ranges are keyed on commit ids, so a rebase or amend between runs gives
# an earlier run's forged verdict a new id in no range, and a --keep run trusted
# it as the gate's (X-6-F1-F1 Finding 5). Now --keep refuses while a closed
# range's end is no longer an ancestor of HEAD; --trust-existing moves that
# ledger aside and trusts HEAD as is; an intact ledger is never overridden. B, E
# and F above are the controls: a gate verdict, or an orchestrator commit, on
# top of end is not a rewrite. Every rewrite pins the committer date, so the
# rewritten commit gets a new id even within the same second. The card's XAM
# and XGN are XWA and XWG here, because XA and XG are e2e ids and stray_commits
# matches `card_id in` a subject. (XRW, XWA, XWG, XTI: none is a substring of
# another e2e id, nor another id of one of them.)
rwdate='@1700000000 +0000'
# ledger_at ID N KEY: field KEY of record N of ID's ledger.
ledger_at() { python3 -c 'import json, sys; print(json.loads(open(sys.argv[1]).read().splitlines()[int(sys.argv[2])])[sys.argv[3]])' "$e/cards/logs/$1.ledger.jsonl" "$2" "$3"; }
# anc ID REV WANT: `merge-base --is-ancestor REV card/v3-ID` exits WANT (0 or
# 1), or with WANT "fail" exits anything else.
anc() { local rc=0; git -C "$r" merge-base --is-ancestor "$2" "card/v3-$1" 2>/dev/null || rc=$?
  if [ "$3" = fail ]; then [ "$rc" -gt 1 ]; else [ "$rc" = "$3" ]; fi; }
# rw_refused NAME ENDS: the run refused before attempt 1 with the exact line,
# ENDS (a Python expression) naming the ends, and left the ledger as it was
# (two records, none moved aside).
rw_refused() { res "$1" "rc == 1 and \"[${1%%-*}] the branch was rewritten since its ledger's runs (ends no longer in HEAD's history: \" + $2 + \"); rerun without --keep, or pass --trust-existing after inspecting the branch\" in out.splitlines() and 'attempt 1:' not in out and len(L) == 2 and A == []"; }
# XRW: a forged verdict, then a rebase of the branch onto its base (REBASE-LAUNDERS).
e2e XRW-1 forge --attempts 1
expect_ok "runner e2e: XRW: premise: run 1's forge fails scope and closes its range at the forged tip" res XRW-1 "r['scope'] == 'FAIL' and $closed"
xrw_end=$(ledger_at XRW 1 end)
(cd "$ewt/XRW" && GIT_COMMITTER_DATE=$rwdate git rebase -q --force-rebase e2e/base)
expect_ok "runner e2e: XRW: premise: the rebase leaves the old end out of the tip's history" anc XRW "$xrw_end" 1
e2e XRW-2 work --attempts 1 --keep
expect_ok "runner e2e: XRW: --keep refuses a branch rebased since its ledger's runs" rw_refused XRW-2 "L[-1]['end']"
e2e XRW-3 work --attempts 1 --keep --trust-existing
expect_ok "runner e2e: XRW: --trust-existing moves the ledger aside, trusts HEAD and starts" \
  res XRW-3 "rc == 0 and r['pass'] and len(A) == 1 and len(A[0]) == 2 and A[0][1]['end'] == '$xrw_end' and len(L) == 4 and L[0].get('trust-existing') is True and L[1]['end'] == L[0]['start'] == L[2]['start'] and $closed and '[XRW] --trust-existing: card/v3-XRW rewritten since its ledger; trusted as is at ' + L[0]['start'] + ' (old ledger moved to ' in out"
# XWA: run 1 passes, then its tip is amended. start is still an ancestor, so only
# a check of end sees it (REWRITE-CHECK-START).
e2e XWA-1 work --attempts 1
expect_ok "runner e2e: XWA: premise: run 1 passes" res XWA-1 "rc == 0 and r['pass'] and $closed"
xwa_start=$(ledger_at XWA 0 start) xwa_end=$(ledger_at XWA 1 end)
(cd "$ewt/XWA" && GIT_COMMITTER_DATE=$rwdate git commit -q --amend --no-edit)
expect_ok "runner e2e: XWA: premise: the amend leaves the old end out of the tip's history" anc XWA "$xwa_end" 1
expect_ok "runner e2e: XWA: premise: start is still in the tip's history" anc XWA "$xwa_start" 0
e2e XWA-2 work --attempts 1 --keep
expect_ok "runner e2e: XWA: --keep refuses a branch amended since its ledger's runs" rw_refused XWA-2 "L[-1]['end']"
# XWG: a hand-written ledger (as D) whose closed range ends at a well-formed id
# that is no object, so merge-base fails (REWRITE-FAIL-OPEN).
git -C "$r" worktree add -q -B card/v3-XWG "$ewt/XWG" e2e/base
printf '{"run": "r", "start": "%s", "t": 0}\n{"run": "r", "end": "%s", "t": 0}\n' "$(git -C "$ewt/XWG" rev-parse HEAD)" "$wcx" >"$e/cards/logs/XWG.ledger.jsonl"
expect_ok "runner e2e: XWG: premise: merge-base fails on the end" anc XWG "$wcx" fail
xwg_rc=0; git -C "$ewt/XWG" merge-base --is-ancestor "$wcx" HEAD 2>/dev/null || xwg_rc=$?
e2e XWG work --attempts 1 --keep
expect_ok "runner e2e: XWG: an end git cannot judge refuses, naming the failed merge-base" \
  rw_refused XWG "'$wcx (git merge-base failed, exit $xwg_rc)'"
# XTI: a forged verdict and an intact ledger: --trust-existing must not reset it
# (TRUST-OVERRIDES-INTACT).
e2e XTI-1 forge --attempts 1
expect_ok "runner e2e: XTI: premise: run 1's range ends at the tip (the ledger is intact)" anc XTI "$(ledger_at XTI 1 end)" 0
e2e XTI-2 work --attempts 1 --keep --trust-existing
expect_ok "runner e2e: XTI: --trust-existing on an intact ledger leaves the forge protected" flagged XTI-2
expect_ok "runner e2e: XTI: --trust-existing on an intact ledger says it was ignored and changes nothing" \
  res XTI-2 "'[XTI] --trust-existing ignored: the ledger is intact' in out.splitlines() and A == [] and len(L) == 4 and not any(x.get('trust-existing') for x in L)"
# R: refs outside the card branch, moved from inside a run (X-6-F1-F3-F1).
# refmove is `git fetch . +HEAD:main` (the fake binary is not under the deny list).
main_at='subprocess.run(["git", "-C", repo, "rev-parse", "main"], capture_output=True, text=True).stdout.strip()'
git -C "$r" branch -f main nucleus/v3-scaffold
e2e XR1 "work refmove" --attempts 3
expect_ok "runner e2e: RF1: moving main stops the run, prints the restore and does not run it" \
  res XR1 "rc == 1 and r['scope'] == 'OUTSIDE' and r['attempts'] == 1 and 'refs/heads/main: ' in out and 'git update-ref refs/heads/main ' in out and $main_at == H"
# The restore sets main back from the card tip to its old commit, old before new:
# update-ref's compare-and-swap order (G-RESTORE-SWAPPED prints them swapped).
main_old=$(git -C "$r" rev-parse nucleus/v3-scaffold)
restore_main="'  to restore (not run; check for a concurrent update first): git update-ref refs/heads/main $main_old ' + H in out.splitlines()"
expect_ok "runner e2e: RF1: the restore line is update-ref main <old> <new>, with the real commits" res XR1 "$restore_main"
# RF2: the move in a killed attempt 1 is caught by its own check (REF-SKIP-KILLED
# would stop at attempt 2 instead).
git -C "$r" branch -f main nucleus/v3-scaffold
e2e XR2 "refmove silent;work" --attempts 3
expect_ok "runner e2e: RF2: a ref moved in a killed attempt stops the run at that attempt" \
  res XR2 "rc == 1 and r['scope'] == 'OUTSIDE' and r['attempts'] == 1 and r['killed'][0]['reason'] == 'silence' and 'refs/heads/main: ' in out and 'attempt 2:' not in out"
git -C "$r" branch -f main nucleus/v3-scaffold
# RF4: a side ref made in attempt 1 carries this run's commit only once attempt 2
# merges it, so only a snapshot taken once per run sees it (REF-SNAP-PER-ATTEMPT).
e2e XR4 "side silent;adopt" --attempts 3
expect_ok "runner e2e: RF4: a side ref adopted in a later attempt is caught against the run's snapshot" \
  res XR4 "rc == 1 and r['scope'] == 'OUTSIDE' and r['attempts'] == 2 and 'refs/heads/side-XR4: (none) -> ' in out and \"carries this run's commit\" in out"
# RF5: a quota exit skips the loop's check and the final one, so only the at-exit
# check in main()'s finally reports the move (G-NO-EXIT-CHECK prints nothing).
git -C "$r" branch -f main nucleus/v3-scaffold
e2e XR5 "work refmove quota" --attempts 3
expect_ok "runner e2e: RF5: a ref moved before a quota exit is reported at exit, with its restore" \
  res XR5 "rc == 1 and 'provider usage limit reached' in out and '[XR5] refs changed outside the card branch (at exit):' in out.splitlines() and 'refs changed outside the card branch; stopping' not in out and 'final ref check' not in out and $restore_main"
git -C "$r" branch -f main nucleus/v3-scaffold
# RF3 (control, last: it moves nucleus/v3-scaffold): concurrent legitimate moves
# during a run let it pass (REF-STRICT refuses it).
e2e XR3 "work foreign" --attempts 1
expect_ok "runner e2e: RF3: concurrent legitimate ref moves during a run pass" \
  res XR3 "rc == 0 and r['pass'] and r['scope'] == 'ok' and 'refs changed' not in out"
# S: a replace ref in the repository stops main() before attempt 1 (X-6-F1-F2).
# Both commits are unreachable, so nothing else in $r is rewritten meanwhile.
xs=$(git -C "$r" commit-tree "e2e/base^{tree}" -m "nucleus v3: XS replaced")
git -C "$r" replace "$xs" "$(git -C "$r" commit-tree "e2e/base^{tree}" -m "nucleus v3: XS replacement")"
e2e XS work --attempts 1
expect_ok "runner e2e: S: a replace ref stops the run before attempt 1, naming it" \
  res XS "rc == 1 and 'git state hides changes; not starting' in out and 'replace ref refs/replace/$xs' in out and 'attempt 1:' not in out and not os.path.exists('$e/XS.n')"
git -C "$r" replace -d "$xs" >/dev/null
expect_ok "git state: XS's replace ref is gone" no_state
# V: the opencode child inherits the runner's git environment (X-6-F1-F2-F1 b).
# This script unset the caller's protection at the top, so these values can
# only come from git_env. The effective core.fsmonitor is read, not
# GIT_CONFIG_COUNT or key indexes, which later entries may shift.
e2e XV "gitenv work" --attempts 1
expect_ok "runner e2e: V: opencode runs with replace refs, grafts and fsmonitor off" \
  res XV "open('$e/XV.n.env').read().split() == ['false', '1', '/dev/null'] and 'command\tcore.fsmonitor=false' in open('$e/XV.n.cfg').read().splitlines()"
# P: an inherited -c core.fsmonitor hook (GIT_CONFIG_PARAMETERS, refuse-only;
# X-6-F1-F2-F2) stops main() before attempt 1, naming it. Exported around this
# one run, as K does E2E_SIGTERM_AFTER. (Not XCP: stray_commits matches card ids
# by substring, and XC is a card here.)
export GIT_CONFIG_PARAMETERS=$xcp
e2e XP work --attempts 1
unset GIT_CONFIG_PARAMETERS
expect_ok "runner e2e: P: an inherited -c core.fsmonitor hook stops the run before attempt 1, naming it" \
  res XP "rc == 1 and 'git state hides changes; not starting' in out and \"core.fsmonitor is '$t/fsm'\" in out and 'attempt 1:' not in out"
# V again (X-6-F1-F2-F3): the child also runs with the commit-graph and the
# untracked cache off, read from XV's command-scope config dump.
expect_ok "runner e2e: V: opencode runs with the commit-graph and the untracked cache off" \
  res XV "{'command\tcore.commitgraph=false', 'command\tcore.untrackedcache=false'} <= set(open('$e/XV.n.cfg').read().splitlines())"
# Q (X-6-F1-F2-F2-F1 EX g): the worker leaves an untracked spikes/ file hidden
# by a self-ignoring untracked spikes/.gitignore. spikes/ is the card's Touch
# only, so only the dirty check can stop it. work runs first: under `*`, its
# `git add nucleus/v3/spikes` would skip the new spikes/XQ.txt and commit nothing.
e2e XQ "work hideuntracked" --attempts 1
expect_ok "runner e2e: Q: an untracked file a self-ignoring .gitignore hides fails the dirty check, by name" \
  res XQ "rc == 1 and not r['pass'] and 'Uncommitted changes (commit the card' in out and '!! nucleus/v3/spikes/XQ-hidden.txt' in out and '!! nucleus/v3/spikes/.gitignore' in out"

# M (RUN-F1): the first Acceptance command fails with its STEP line 101 lines
# from the end, as gate.sh's fmt line was in X-5c-F1. Attempt 2's message must
# still name it. (XMSG: no other e2e id is a substring of it, nor it of one.)
ecm() { cat >"$e/cards/$1.local.md" <<C
# CARD $1: e2e

Class: test        Author: S (t)        Status: ready
Base: e2e/base        Depends: —        Gate: S

## Goal
t

## Touch only
- nucleus/v3/spikes/

## Acceptance
- \`printf 'STEP fmt EXIT=1\\n'; seq 100; exit 1\`
- \`cd nucleus/v3 && bash scripts/gate.sh\`
C
echo "$e/cards/$1.local.md"; }
msg_premise() { PYTHONDONTWRITEBYTECODE=1 python3 - "$v3/tools/run_card.py" "$(ecm XMSG)" <<'P'
import importlib.util, pathlib, subprocess, sys
spec = importlib.util.spec_from_file_location("rc", sys.argv[1]); rc = importlib.util.module_from_spec(spec); spec.loader.exec_module(rc)
cid, touch, accept = rc.parse_card(pathlib.Path(sys.argv[2]))
p = subprocess.run(["bash", "-c", accept[0]], capture_output=True, text=True)
lines = (p.stdout + p.stderr).splitlines()
print(cid, accept, p.returncode, lines[:2], len(lines))
sys.exit(0 if cid == "XMSG" and rc.validate_card(touch, accept) is None and accept[0] == "printf 'STEP fmt EXIT=1\\n'; seq 100; exit 1"
         and p.returncode == 1 and "STEP fmt EXIT=1" in lines and "STEP fmt EXIT=1" not in lines[-60:] else 1)
P
}
expect_ok "runner e2e: M: premise: the card parses, and its first command's STEP line is outside the 60-line tail" msg_premise
rm -f "$e/XMSG.n"
(cd "$r" && FAKE_OC="msg work" FAKE_OC_N=$e/XMSG.n E2E=$e PYTHONDONTWRITEBYTECODE=1 python3 "$t/e2e.py" "$v3/tools/run_card.py" "$e/XMSG" "$(ecm XMSG)" --base e2e/base --attempts 2) || true
expect_ok "runner e2e: M: a run whose first Acceptance command always fails does not pass" res XMSG "rc == 1 and not r['pass'] and r['attempts'] == 2"
expect_ok "runner e2e: M: attempt 2's message lists the failing STEP line from outside the tail" \
  res XMSG "'Failing steps (from the full output):\nSTEP fmt EXIT=1\n' in open('$e/XMSG.n.msg2').read()"


# --- fresh-gate.sh (X-6-F1-F2-F2-F1-F1) -------------------------------------
# fresh-gate.sh refuses hooks, filter drivers, attribute files and includes
# before it makes a fresh checkout with hooks off, then checks the checkout's
# index and files against the commit by hash, and only then runs the commit's
# own gate.sh. Every card commit here carries a stub gate.sh (the real one
# would run these tests recursively) that prints the sha1sum of the tracked
# spikes/fg.txt, so a row can tell which bytes the gate ran on, then its
# GATE EXIT line. Each run gets its own empty TMPDIR, so a row can check that
# nothing from the run is left.
git -C "$r" checkout -q nucleus/v3-scaffold
# FG10's commit: the widest branch tip, taken before this section adds any.
# "command wc": the runner-ledger rows above define a shell function wc(),
# which prints nothing here, so a plain wc made every width empty and the pick
# random (verdict X-6-F1-F2-F2-F1-F1 R5; a pick of scratch/prev3, which has no
# nucleus/v3, aborted the suite).
fgw=$(for c in $(git -C "$r" for-each-ref --format='%(objectname)' refs/heads/); do echo "$(git -C "$r" ls-tree -r --full-tree "$c" | command wc -l) $c"; done | sort -n | tail -n 1 | cut -d ' ' -f 2)
# The premise counts with grep -c, not wc, so it does not share the pick's
# counter: $fgw is a nonempty commit holding nucleus/v3/scripts, and no branch
# tip is wider.
fgw_widest() {
  local n m c
  n=$(git -C "$r" ls-tree -r --full-tree "$fgw" | grep -c '') || return 1
  echo "pick: $fgw ($n entries)"
  case $n in '' | *[!0-9]*) return 1 ;; esac
  [ "$n" -gt 0 ] && [ "$(git -C "$r" cat-file -t "$fgw:nucleus/v3/scripts")" = tree ] || return 1
  for c in $(git -C "$r" for-each-ref --format='%(objectname)' refs/heads/); do
    m=$(git -C "$r" ls-tree -r --full-tree "$c" | grep -c '') || m=0
    [ "$m" -le "$n" ] || { echo "wider: $c ($m entries)"; return 1; }
  done
}
expect_ok "premise: FG10: the pick is the widest branch tip, nonempty, and holds nucleus/v3/scripts" fgw_widest
echo "     (FG10) widest commit $fgw, $(git -C "$r" ls-tree -r --full-tree "$fgw" | grep -c '') entries"
fg_orig=$(printf 'orig\n' | sha1sum | cut -d ' ' -f 1)
fg_edit=$(printf 'EDIT\n' | sha1sum | cut -d ' ' -f 1)
# fgstub DIR ID [EXIT]: in DIR (a worktree of $r on card/v3-ID), commit the stub
# gate.sh (printing GATE EXIT=EXIT, default 0) and spikes/fg.txt holding "orig".
fgstub() {
  local ge=${3:-0}
  printf '#!/usr/bin/env bash\ncd "$(dirname "$0")/.." && sha1sum spikes/fg.txt && echo "GATE EXIT=%s"\nexit %s\n' "$ge" "$ge" >"$1/nucleus/v3/scripts/gate.sh"
  printf 'orig\n' >"$1/nucleus/v3/spikes/fg.txt"
  git -C "$1" add nucleus/v3/scripts/gate.sh nucleus/v3/spikes/fg.txt && git -C "$1" commit -qm "nucleus v3: $2 work"
}
# fgcard ID [EXIT] [BASE]: card/v3-ID off BASE (default the scaffold) with the
# stub, made in $r, which is left back on the scaffold.
fgcard() { git -C "$r" checkout -q -b "card/v3-$1" "${3:-nucleus/v3-scaffold}" && fgstub "$r" "$1" "${2:-0}" && git -C "$r" checkout -q nucleus/v3-scaffold; }
# fgrun [--shim WORD] ARGS...: fresh-gate.sh ARGS from $v3 (under gshim WORD),
# with a fresh empty TMPDIR; stdout, stderr and the exit status go to
# $t/fg.out, $t/fg.err and $t/fg.rc, and the worktree list before the run to
# $t/fg.wt0.
fgrun() {
  local shim=() rc=0
  if [ "$1" = --shim ]; then shim=(gshim "$2"); shift 2; fi
  rm -rf "$t/fgtmp"; mkdir "$t/fgtmp"
  plain worktree list --porcelain >"$t/fg.wt0"
  (cd "$v3" && TMPDIR=$t/fgtmp "${shim[@]}" bash scripts/fresh-gate.sh "$@") >"$t/fg.out" 2>"$t/fg.err" || rc=$?
  echo "$rc" >"$t/fg.rc"
}
fg_show() { echo "exit $(cat "$t/fg.rc")"; sed 's/^/out: /' "$t/fg.out"; sed 's/^/err: /' "$t/fg.err"; }
# fg_gate N ID: the run reached the gate: exit N, last stdout line GATE EXIT=N,
# card/v3-ID named by its full id, and the stub hashed "orig".
fg_gate() { fg_show; [ "$(cat "$t/fg.rc")" = "$1" ] && [ "$(tail -n 1 "$t/fg.out")" = "GATE EXIT=$1" ] && grep -qxF "fresh-gate: commit $(git -C "$r" rev-parse "card/v3-$2")" "$t/fg.out" && grep -qxF "$fg_orig  spikes/fg.txt" "$t/fg.out"; }
# fg_refused WANT...: a refusal: exit 2, last stdout line FRESH-GATE REFUSED,
# no GATE EXIT= line anywhere, and stderr names every WANT.
fg_refused() {
  local x; fg_show
  [ "$(cat "$t/fg.rc")" = 2 ] && [ "$(tail -n 1 "$t/fg.out")" = "FRESH-GATE REFUSED" ] && ! grep -qF 'GATE EXIT=' "$t/fg.out" "$t/fg.err" || return 1
  for x in "$@"; do grep -F -- "$x" "$t/fg.err" | grep -q '^fresh-gate: ' || { echo "missing: fresh-gate: ...$x"; return 1; }; done
}
# fg_clean: the worktree list is as it was before the run, and nothing is left
# in the run's TMPDIR.
fg_clean() { local w; w=$(plain worktree list --porcelain); echo "$w"; ls -A "$t/fgtmp"; [ "$w" = "$(cat "$t/fg.wt0")" ] && [ -z "$(ls -A "$t/fgtmp")" ]; }

# FG1 honest control, FG2 a failing gate.
fgcard FG1
fgrun card/v3-FG1
expect_ok "fresh-gate: FG1: an honest commit reaches its gate (exit 0, last line GATE EXIT=0, commit named in full)" fg_gate 0 FG1
expect_ok "fresh-gate: FG1: the checkout is removed and nothing is left in TMPDIR" fg_clean
fgcard FG2 1
fgrun card/v3-FG2
expect_ok "fresh-gate: FG2: a failing gate: exit 1, last line GATE EXIT=1" fg_gate 1 FG2
expect_ok "fresh-gate: FG2: the checkout is removed" fg_clean

# fresh-gate's container (X-6-F1-F1-F2-F1-F1). The stub gate leaves a forge in its own
# session and passes. FG_HOLD holds it, for a signal during the gate. Everything goes
# by FG_MARK: .ns the gate's pid namespace, .tmp its TMPDIR, .landed the forge's work.
fgforge() {
  git -C "$r" checkout -q -b "card/v3-$1" nucleus/v3-scaffold
  cat >"$r/nucleus/v3/scripts/gate.sh" <<'G'
#!/usr/bin/env bash
readlink /proc/self/ns/pid >"$FG_MARK.ns"
echo "$TMPDIR" >"$FG_MARK.tmp"
: >"$TMPDIR/left"
E2E_CT_MARK=$FG_MARK setsid bash -c 'sleep "$2"; git -C "$3" update-ref "refs/fgforge/$4" HEAD; echo landed >"$1.landed"' _ "$FG_MARK" "${FG_FORGE_S:-3}" "$FG_REPO" "$(basename "$FG_MARK")" >/dev/null 2>&1 &
if [ -n "${FG_HOLD:-}" ]; then : >"$FG_MARK.started"; E2E_CT_MARK=$FG_MARK sleep 60; fi
echo "GATE EXIT=0"
exit 0
G
  git -C "$r" add nucleus/v3/scripts/gate.sh && git -C "$r" commit -qm "nucleus v3: $1 work" && git -C "$r" checkout -q nucleus/v3-scaffold
}
fgforge FGC
export FG_REPO=$r
# fgc_ran MARK: the gate ran and printed its line, in another pid namespace.
fgc_ran() { fg_show; [ "$(cat "$t/fg.rc")" = 0 ] && [ "$(tail -n 1 "$t/fg.out")" = "GATE EXIT=0" ] && [ -s "$1.ns" ] && [ "$(cat "$1.ns")" != "$(readlink /proc/self/ns/pid)" ]; }
# fgc_tmp MARK: its TMPDIR was under the scratch dir, under the run's TMPDIR, and is gone.
fgc_tmp() { local x; x=$(cat "$1.tmp"); echo "$x"; case $x in "$t/fgtmp/"*/tmp) ;; *) return 1 ;; esac; [ ! -e "$x" ]; }
# fgc_nope MARK: the forge did not land, past its delay.
fgc_nope() { [ ! -e "$1.landed" ] && ! git -C "$r" rev-parse -q --verify "refs/fgforge/$(basename "$1")" >/dev/null; }
FG_MARK=$t/fgc1 E2E_NS=real fgrun card/v3-FGC
expect_ok "fresh-gate: FGC: the gate runs in a real container, exit 0, last line GATE EXIT=0" fgc_ran "$t/fgc1"
expect_ok "fresh-gate: FGC: its TMPDIR is under the scratch dir and gone" fgc_tmp "$t/fgc1"
expect_ok "fresh-gate: FGC: the checkout is removed and nothing is left in TMPDIR" fg_clean
expect_ok "fresh-gate: FGC: nothing of the forge is alive when fresh-gate returns" bash "$t/ct-alive.sh" "$t/fgc1"
FG_MARK=$t/fgc2 E2E_NS=real fgrun card/v3-FGC
fgc_tmp2() { [ "$(cat "$t/fgc1.tmp")" != "$(cat "$t/fgc2.tmp")" ]; }
expect_ok "fresh-gate: FGC: two runs see different TMPDIRs" fgc_tmp2
# A SIGTERM during the gate: the container ends first, and no gate result is printed.
fgc_term() { local p rc=0 i; (cd "$v3" && TMPDIR=$t/fgtmp E2E_NS=real FG_HOLD=1 FG_MARK=$t/fgc3 FG_FORGE_S=3 exec bash scripts/fresh-gate.sh card/v3-FGC) >"$t/fg.out" 2>"$t/fg.err" & p=$!
  for i in {1..600}; do [ -e "$t/fgc3.started" ] && break; sleep 0.05; done
  [ -e "$t/fgc3.started" ] || { echo "the gate never started"; kill -KILL "$p"; return 1; }
  kill -TERM "$p"; wait "$p" || rc=$?
  echo "exit $rc"; bash "$t/ct-alive.sh" "$t/fgc3" || return 1
  [ "$rc" = 143 ] && ! grep -qF 'GATE EXIT=' "$t/fg.out"; }
rm -rf "$t/fgtmp"; mkdir "$t/fgtmp"; plain worktree list --porcelain >"$t/fg.wt0"
expect_ok "fresh-gate: FGC: a SIGTERM during the gate ends its container first, exit 143, no GATE EXIT= line" fgc_term
expect_ok "fresh-gate: FGC: the SIGTERM run leaves no checkout" fg_clean
sleep 5
expect_ok "fresh-gate: FGC: the forge never lands" fgc_nope "$t/fgc1"
expect_ok "fresh-gate: FGC: the forge never lands (second run)" fgc_nope "$t/fgc2"
expect_ok "fresh-gate: FGC: the forge never lands (SIGTERM)" fgc_nope "$t/fgc3"
# Premise, the same commit with no container (the no-op shim): the forge lands.
FG_MARK=$t/fgc4 fgrun card/v3-FGC
fgc_landed() { local i; for i in {1..300}; do [ -e "$t/fgc4.landed" ] && return 0; sleep 0.1; done; echo "the forge never landed"; return 1; }
expect_ok "fresh-gate: FGC: premise: through the no-op shim the gate's forge lands" fgc_landed
git -C "$r" update-ref -d refs/fgforge/fgc4
# No container, no gate: refused by name, nothing checked out, the gate never ran.
fgcard FGR
E2E_NS=fail fgrun card/v3-FGR
expect_ok "fresh-gate: FGC: user namespaces off: refused by name (FG-FALLBACK)" fg_refused "cannot contain the gate, so not running it" "Operation not permitted" "the owner's call"
expect_ok "fresh-gate: FGC: that refusal ran no gate" bash -c "! grep -qF '$fg_orig' '$t/fg.out'"
expect_ok "fresh-gate: FGC: that refusal leaves nothing" fg_clean
E2E_NS=lie fgrun card/v3-FGR
expect_ok "fresh-gate: FGC: an unshare that exits 0 with no namespace is refused too" fg_refused "cannot contain the gate, so not running it" "(want '1 "
expect_ok "fresh-gate: FGC: that refusal ran no gate" bash -c "! grep -qF '$fg_orig' '$t/fg.out'"
# No unshare at all: everything else on PATH, as links.
mkdir "$t/fgnoun"; ln -s /usr/bin/* "$t/fgnoun/"; rm -f "$t/fgnoun/unshare"
PATH=$t/fgnoun fgrun card/v3-FGR
expect_ok "fresh-gate: FGC: no unshare on PATH is refused by name" fg_refused "unshare (util-linux) is not on PATH"
# A container that answers the probe and then fails to start: a refusal, not a gate result.
mkdir "$t/fgnostart"
cat >"$t/fgnostart/unshare" <<'F'
#!/usr/bin/env bash
while [ "$#" -gt 0 ] && [ "$1" != -- ]; do shift; done; shift
if [ "$1" = sh ]; then echo "1 $(id -u) $(id -g) sh"; exit 0; fi
echo "unshare: unshare failed: Operation not permitted" >&2
exit 1
F
chmod +x "$t/fgnostart/unshare"
PATH=$t/fgnostart:$PATH fgrun card/v3-FGR
expect_ok "fresh-gate: FGC: a container that fails to start is a refusal, the gate never ran" fg_refused "the gate's container did not start (unshare exit 1)"
expect_ok "fresh-gate: FGC: that refusal ran no gate" bash -c "! grep -qF '$fg_orig' '$t/fg.out'"
expect_ok "fresh-gate: FGC: that refusal leaves nothing" fg_clean
# The runner and the gate use one container form.
fgc_same() { local a b; a=$(sed -n 's/^CONTAIN=(\(.*\))$/\1/p' "$v3/scripts/fresh-gate.sh"); b=$(PYTHONDONTWRITEBYTECODE=1 python3 -c 'import importlib.util,sys; s=importlib.util.spec_from_file_location("rc",sys.argv[1]); rc=importlib.util.module_from_spec(s); s.loader.exec_module(rc); print(" ".join(rc.CONTAIN))' "$v3/tools/run_card.py"); echo "fresh-gate: $a"; echo "runner: $b"; [ -n "$a" ] && [ "$a" = "$b" ]; }
expect_ok "fresh-gate: FGC: its container flags equal the runner's CONTAIN" fgc_same

# FG3, the index-stat poisoning ESCALATE (verdict X-6-F1-F2-F2-F1): in a card
# worktree, a same-size EDIT passes one status under a transient clean filter,
# which writes EDIT's stat into the index. The mtime is set back 10 s so the
# entry is not racy (git would re-read a racy one). Then the attribute and
# the config are gone, and plain status still lists nothing.
w3=$t/fg3w
git -C "$r" worktree add -q -b card/v3-FG3 "$w3" nucleus/v3-scaffold
fgstub "$w3" FG3
sleep 1.2
printf 'EDIT\n' >"$w3/nucleus/v3/spikes/fg.txt"
touch -m -d "@$(($(date +%s) - 10))" "$w3/nucleus/v3/spikes/fg.txt"
echo 'nucleus/v3/spikes/fg.txt filter=h' >"$r/.git/info/attributes"
git -C "$w3" -c filter.h.clean='echo orig' status --porcelain >/dev/null
rm -f "$r/.git/info/attributes"
# fg_poisoned DIR: plain status in DIR lists nothing while fg.txt holds EDIT.
fg_poisoned() { local s; s=$(env -u GIT_NO_REPLACE_OBJECTS -u GIT_GRAFT_FILE -u GIT_CONFIG_COUNT git -C "$1" status --porcelain); echo "status: $s"; [ -z "$s" ] && [ "$(cat "$1/nucleus/v3/spikes/fg.txt")" = EDIT ]; }
expect_ok "premise: FG3: after a transient clean filter, plain status in the card worktree is empty while fg.txt holds EDIT" fg_poisoned "$w3"
# fg_ran_orig: the gate ran, and the stub hashed the committed bytes, not EDIT.
fg_ran_orig() { fg_gate 0 FG3 && ! grep -qF "$fg_edit" "$t/fg.out"; }
fgrun card/v3-FG3
expect_ok "fresh-gate: FG3: the gate runs on the committed content (the stub hashes orig, not EDIT)" fg_ran_orig
expect_ok "fresh-gate: FG3: the checkout is removed" fg_clean
# FG3b, the same by a direct index write: --cacheinfo gives the entry the
# committed id (and zero stat data), and the stat touch writes the edited
# file's stat into that entry, as a hand-written index would.
# idx_stat INDEX PATH FILE: in the v2/v3 index INDEX, PATH's entry gets FILE's
# stat data (ctime, mtime, dev, ino, uid, gid, size; mode kept), checksum
# rewritten.
idx_stat() { python3 -I - "$@" <<'P'
import hashlib, os, struct, sys
idx, path, f = sys.argv[1], os.fsencode(sys.argv[2]), sys.argv[3]
d = bytearray(open(idx, "rb").read())
sig, ver, n = struct.unpack(">4sII", d[:12])
if sig != b"DIRC" or ver not in (2, 3):
    raise SystemExit(f"idx_stat: unsupported index (version {ver})")
p = 12
for _ in range(n):
    flags = struct.unpack(">H", d[p + 60:p + 62])[0]
    q = p + 62 + (2 if ver == 3 and flags & 0x4000 else 0)
    end = d.index(b"\0", q)
    if bytes(d[q:end]) == path:
        st = os.stat(f)
        struct.pack_into(">IIIIII", d, p, int(st.st_ctime), st.st_ctime_ns % 10**9, int(st.st_mtime), st.st_mtime_ns % 10**9, st.st_dev & 0xFFFFFFFF, st.st_ino & 0xFFFFFFFF)
        struct.pack_into(">III", d, p + 28, st.st_uid, st.st_gid, st.st_size & 0xFFFFFFFF)
        d[-20:] = hashlib.sha1(bytes(d[:-20])).digest()
        open(idx, "wb").write(bytes(d))
        sys.exit(0)
    p += (q - p + (end - q) + 8) & ~7
raise SystemExit(f"idx_stat: {sys.argv[2]} is not in the index")
P
}
w3b=$t/fg3bw
git -C "$r" worktree add -q --detach "$w3b" card/v3-FG3
printf 'EDIT\n' >"$w3b/nucleus/v3/spikes/fg.txt"
touch -m -d "@$(($(date +%s) - 10))" "$w3b/nucleus/v3/spikes/fg.txt"
git -C "$w3b" update-index --cacheinfo "100644,$(git -C "$r" rev-parse card/v3-FG3:nucleus/v3/spikes/fg.txt),nucleus/v3/spikes/fg.txt"
idx_stat "$(git -C "$w3b" rev-parse --path-format=absolute --git-path index)" nucleus/v3/spikes/fg.txt "$w3b/nucleus/v3/spikes/fg.txt"
expect_ok "premise: FG3b: after a direct index write, plain status in the worktree is empty while fg.txt holds EDIT" fg_poisoned "$w3b"
fgrun card/v3-FG3
expect_ok "fresh-gate: FG3b: the gate runs on the committed content (the stub hashes orig, not EDIT)" fg_ran_orig
expect_ok "fresh-gate: FG3b: the checkout is removed" fg_clean
git -C "$r" worktree remove --force "$w3"
git -C "$r" worktree remove --force "$w3b"

# FG4, a post-checkout hook in the common dir: it writes a marker outside the
# checkout and rewrites a tracked file. Planted only while $r stays on the
# scaffold (its own checkouts would run it).
fgcard FG4
fghook=$r/.git/hooks/post-checkout
printf '#!/bin/sh\ntouch "%s"\necho hooked >nucleus/v3/spikes/fg.txt\n' "$t/fg4.marker" >"$fghook"
chmod +x "$fghook"
git -C "$r" worktree add -q --detach "$t/fg4p" card/v3-FG4
expect_ok "premise: FG4: a plain worktree add runs the hook (marker written)" test -e "$t/fg4.marker"
git -C "$r" worktree remove --force "$t/fg4p"; rm -f "$t/fg4.marker"
fgrun card/v3-FG4
expect_ok "fresh-gate: FG4: a post-checkout hook is refused, by name (exit 2, no GATE EXIT= line)" fg_refused "hook " "hooks/post-checkout"
expect_ok "fresh-gate: FG4: the hook never ran (no marker)" test ! -e "$t/fg4.marker"
expect_ok "fresh-gate: FG4: no checkout is left" fg_clean
rm -f "$fghook"

# FG5, a smudge filter: repo config plus info/attributes. The command writes a
# marker, so a check made after the checkout would come too late.
fgcard FG5
git -C "$r" config filter.fg.smudge "touch '$t/fg5.marker'; cat"
echo 'nucleus/v3/spikes/fg.txt filter=fg' >"$r/.git/info/attributes"
git -C "$r" worktree add -q --detach "$t/fg5p" card/v3-FG5
expect_ok "premise: FG5: a plain worktree add runs the smudge filter (marker written)" test -e "$t/fg5.marker"
git -C "$r" worktree remove --force "$t/fg5p"; rm -f "$t/fg5.marker"
fgrun card/v3-FG5
expect_ok "fresh-gate: FG5: a smudge filter is refused, naming filter.fg.smudge and info/attributes" fg_refused "filter.fg.smudge is 'touch '$t/fg5.marker'; cat' (file:" "info/attributes exists"
expect_ok "fresh-gate: FG5: the refusal comes before the checkout (no marker)" test ! -e "$t/fg5.marker"
expect_ok "fresh-gate: FG5: no checkout is left" fg_clean
git -C "$r" config --remove-section filter.fg
rm -f "$r/.git/info/attributes"

# FG6, a filter from the command line (GIT_CONFIG_PARAMETERS), and a driver
# named `set` (verdict R1: check-attr prints `set` for a bare attribute too,
# but git runs filter.set.smudge for filter=set).
fg6() { GIT_CONFIG_PARAMETERS=$1 fgrun card/v3-FG1; }
fg6 "'filter.fg.smudge'='cat'"
expect_ok "fresh-gate: FG6: a -c filter.fg.smudge is refused, at command-line scope" fg_refused "filter.fg.smudge is 'cat' (command line:"
fg6 "'filter.set.smudge'='cat'"
expect_ok "fresh-gate: FG6: a -c filter.set.smudge (a driver named set) is refused" fg_refused "filter.set.smudge is 'cat' (command line:"
expect_ok "fresh-gate: FG6: no checkout is left" fg_clean

# FG7, config includes: they can bring in any of the above.
printf '[nv3]\n\tprobe = fg7\n' >"$t/fg7.inc"
git -C "$r" config include.path "$t/fg7.inc"
fgrun card/v3-FG1
expect_ok "fresh-gate: FG7: an include.path in the repository config is refused, by name" fg_refused "include.path is '$t/fg7.inc' (file:"
git -C "$r" config --remove-section include
git -C "$r" config "includeIf.onbranch:fg7/**.path" "$t/fg7.inc"
fgrun card/v3-FG1
expect_ok "fresh-gate: FG7: an includeIf path is refused, by name" fg_refused "includeif.onbranch:fg7/**.path is '$t/fg7.inc' (file:"
git -C "$r" config --remove-section "includeIf.onbranch:fg7/**"
rm -f "$t/fg7.inc"

# FG8, the eol exception's control: a committed `text eol=crlf` attribute and
# an LF-only file, which the checkout writes with CRLF.
git -C "$r" checkout -q -b card/v3-FG8 nucleus/v3-scaffold
fgstub "$r" FG8
printf 'nucleus/v3/spikes/fg.ps1 text eol=crlf\n' >"$r/.gitattributes"
printf 'one\ntwo\n' >"$v3/spikes/fg.ps1"
git -C "$r" add .gitattributes nucleus/v3/spikes/fg.ps1 2>/dev/null && git -C "$r" commit -qm "nucleus v3: FG8 crlf file"
git -C "$r" checkout -q nucleus/v3-scaffold
git -C "$r" worktree add -q --detach "$t/fg8p" card/v3-FG8
fg8_crlf() { local h b; h=$(git -C "$r" hash-object --no-filters "$t/fg8p/nucleus/v3/spikes/fg.ps1"); b=$(git -C "$r" rev-parse card/v3-FG8:nucleus/v3/spikes/fg.ps1); echo "checkout $h, blob $b"; [ "$h" != "$b" ] && grep -q $'\r' "$t/fg8p/nucleus/v3/spikes/fg.ps1"; }
expect_ok "premise: FG8: the checkout's fg.ps1 (CRLF) does not hash to its blob (LF)" fg8_crlf
git -C "$r" worktree remove --force "$t/fg8p"
fg8_ok() { fg_gate 0 FG8 && grep -qxF "fresh-gate: 1 file(s) matched through their eol conversion" "$t/fg.out"; }
fgrun card/v3-FG8
expect_ok "fresh-gate: FG8: a text eol=crlf file passes through its eol conversion, and the gate runs" fg8_ok
expect_ok "fresh-gate: FG8: the checkout is removed" fg_clean

# FG9, a failed step is a refusal, never a pass: exit 2, FRESH-GATE REFUSED,
# no GATE EXIT= line, no checkout left.
fgrun --shim worktree card/v3-FG1
expect_ok "fresh-gate: FG9: a failed worktree add is a refusal" fg_refused "git worktree add failed (exit 128)"
expect_ok "fresh-gate: FG9: (worktree) no checkout is left" fg_clean
fgrun --shim hash-object card/v3-FG1
expect_ok "fresh-gate: FG9: a failed hash-object is a refusal" fg_refused "git hash-object failed (exit 128; cannot check the content)"
expect_ok "fresh-gate: FG9: (hash-object) no checkout is left" fg_clean
fgrun --shim ls-tree card/v3-FG1
expect_ok "fresh-gate: FG9: a failed ls-tree is a refusal" fg_refused "git ls-tree failed (exit 128; cannot read the commit's tree)"
expect_ok "fresh-gate: FG9: (ls-tree) no checkout is left" fg_clean
fgrun card/v3-FG-NOSUCH
expect_ok "fresh-gate: FG9: a commit that does not resolve is a refusal" fg_refused "cannot resolve commit 'card/v3-FG-NOSUCH'"
expect_ok "fresh-gate: FG9: (no commit) nothing is left" fg_clean

# FG10, cost: the checks (the script's own timing, which stops before the
# gate) on $r's widest branch tip ($fgw, above), with the stub on top.
fgcard FG10 0 "$fgw"
fgrun card/v3-FG10
sed -n 's/^fresh-gate: checks passed/     (FG10) checks passed/p' "$t/fg.out"
fg10() { local s; fg_gate 0 FG10 || return 1; s=$(sed -n 's/^fresh-gate: checks passed ([0-9]* files hashed in \([0-9]*\)\.[0-9]* s)$/\1/p' "$t/fg.out"); [ -n "$s" ] && [ "$s" -lt 5 ]; }
expect_ok "fresh-gate: FG10: the checks on the widest commit finish in under 5 s" fg10

expect_ok "fresh-gate: no hook, filter, attribute file or include is left" bash -c "[ -z \"\$(find '$r/.git/hooks' -mindepth 1 ! -name '*.sample')\" ] && [ ! -e '$r/.git/info/attributes' ] && [ -z \"\$(git -C '$r' config --get-regexp '^(filter|include|includeif)\\.')\" ]"
expect_ok "git state: none after the fresh-gate scenarios" no_state
expect_ok "git state: none of the output-shaping state after the fresh-gate scenarios" no_state3

# FG11, FG11b and FG12, the forged-object rows (owner exception 2026-11-03).
# A checkout reads every tip blob and nothing hash-checks blobs on that path,
# so a swapped loose blob checks out as the other bytes while the tree and the
# checkout's index (ls-files -s) still name its own id: the blind spot the
# content hash closes. Plain status is asserted neither way (ruling esc-1): on
# tmpfs the checkout's files and its index share one timestamp granule, every
# entry is racily clean, and status re-hashes it, so whether it sees the swap
# depends on the fs clock. Only loose objects unique to one FG branch are
# forged; the honest fg.txt blob ("orig") is the source bytes, every id is
# captured before its object is forged, and each window restores before the
# next move (no_state4).

# FG11, a swapped blob: card/v3-FG11's fg11.txt blob (unique to it) holds
# fg.txt's bytes, so a plain checkout writes them while the tree names
# fg11.txt's own id.
fgcard FG11
git -C "$r" checkout -q card/v3-FG11
printf 'fg11\n' >"$v3/spikes/fg11.txt"
git -C "$r" add nucleus/v3/spikes/fg11.txt && git -C "$r" commit -qm "nucleus v3: FG11 work"
git -C "$r" checkout -q nucleus/v3-scaffold
fg_src=$(git -C "$r" rev-parse card/v3-FG11:nucleus/v3/spikes/fg.txt)
fg11_id=$(git -C "$r" rev-parse card/v3-FG11:nucleus/v3/spikes/fg11.txt)
expect_ok "premise: FG11: both blobs are loose objects" bash -c "[ -f '$(objlo "$r" "$fg_src")' ] && [ -f '$(objlo "$r" "$fg11_id")' ]"
save_obj "$r" "$fg11_id"
forge_from "$r" "$fg11_id" "$fg_src"
git -C "$r" worktree add -q --detach "$t/fg11p" card/v3-FG11
fg11_hidden() {
  local s
  s=$(env -u GIT_NO_REPLACE_OBJECTS -u GIT_GRAFT_FILE -u GIT_CONFIG_COUNT git -C "$t/fg11p" ls-files -s -- nucleus/v3/spikes/fg11.txt)
  echo "index: $s"
  [ "$(git -C "$r" hash-object --no-filters "$t/fg11p/nucleus/v3/spikes/fg11.txt")" = "$fg_src" ] && [ "$s" = "$(printf '100644 %s 0\tnucleus/v3/spikes/fg11.txt' "$fg11_id")" ]
}
expect_ok "premise: FG11: a plain worktree add checks out the other bytes, and the checkout's index names the tree's own id" fg11_hidden
git -C "$r" worktree remove --force "$t/fg11p"
fgrun card/v3-FG11
expect_ok "fresh-gate: FG11: a swapped blob is refused, naming the path and both ids, before the gate runs" fg_refused "nucleus/v3/spikes/fg11.txt: content hashes to $fg_src, tree says $fg11_id"
expect_ok "fresh-gate: FG11: no checkout is left" fg_clean
restore_forged
expect_ok "forged objects: FG11 restored" no_state4

# FG11b, the same forgery under the eol exception: card/v3-FG8's fg.ps1 (text
# eol=crlf) blob holds fg.txt's bytes, which the checkout converts to CRLF.
# The exception must re-hash with the conversion and compare with the tree id
# (it never trusts a path), and these bytes do not hash to that id either.
fg11b_id=$(git -C "$r" rev-parse card/v3-FG8:nucleus/v3/spikes/fg.ps1)
fg11b_got=$(printf 'orig\r\n' | git -C "$r" hash-object --no-filters --stdin)
expect_ok "premise: FG11b: fg.ps1's blob is a loose object" bash -c "[ -f '$(objlo "$r" "$fg11b_id")' ]"
save_obj "$r" "$fg11b_id"
forge_from "$r" "$fg11b_id" "$fg_src"
git -C "$r" worktree add -q --detach "$t/fg11bp" card/v3-FG8
fg11b_hidden() {
  local s
  s=$(env -u GIT_NO_REPLACE_OBJECTS -u GIT_GRAFT_FILE -u GIT_CONFIG_COUNT git -C "$t/fg11bp" ls-files -s -- nucleus/v3/spikes/fg.ps1)
  echo "index: $s"
  [ "$(git -C "$r" hash-object --no-filters "$t/fg11bp/nucleus/v3/spikes/fg.ps1")" = "$fg11b_got" ] && [ "$s" = "$(printf '100644 %s 0\tnucleus/v3/spikes/fg.ps1' "$fg11b_id")" ]
}
expect_ok "premise: FG11b: the checkout holds the other bytes, eol-converted to CRLF, and the checkout's index names the tree's own id" fg11b_hidden
git -C "$r" worktree remove --force "$t/fg11bp"
fgrun card/v3-FG8
expect_ok "fresh-gate: FG11b: a swapped blob is still refused under the eol exception, naming the path and both ids" fg_refused "nucleus/v3/spikes/fg.ps1: content hashes to $fg11b_got, tree says $fg11b_id"
expect_ok "fresh-gate: FG11b: no checkout is left" fg_clean
restore_forged
expect_ok "forged objects: FG11b restored" no_state4

# FG12, the base side (X-6-F1-F2-F3-F1 scenario (b)): the base branch's
# root-tree file holds the card tip's root-tree bytes, so every range read
# from the base agrees with the lie and nothing at the tip is forged: the
# checkout and the content check pass, and verify_objects — whose set holds
# BASE's own trees — is what refuses. The base is a dedicated branch, never
# the scaffold: checkout and status hash-check HEAD's tree, and $r's HEAD
# stays on the scaffold throughout.
git -C "$r" checkout -q -b fg12-base nucleus/v3-scaffold
printf 'fg12\n' >"$v3/spikes/fg12.txt"
git -C "$r" add nucleus/v3/spikes/fg12.txt && git -C "$r" commit -qm "nucleus v3: FG12 base"
git -C "$r" checkout -q nucleus/v3-scaffold
fgcard FG12 0 fg12-base
fg12b=$(git -C "$r" rev-parse "fg12-base^{tree}")
fg12t=$(git -C "$r" rev-parse "card/v3-FG12^{tree}")
expect_ok "premise: FG12: both root trees are loose objects" bash -c "[ -f '$(objlo "$r" "$fg12b")' ] && [ -f '$(objlo "$r" "$fg12t")' ]"
save_obj "$r" "$fg12b"
forge_from "$r" "$fg12b" "$fg12t"
fg12_hidden() { local d; d=$(plain diff --name-only fg12-base...card/v3-FG12); echo "diff: $d"; [ -z "$d" ]; }
expect_ok "premise: FG12: under the swap the plain base...tip diff is empty" fg12_hidden
fgrun --base fg12-base card/v3-FG12
expect_ok "fresh-gate: FG12: a base-side tree holding the tip's tree bytes is refused, naming the base tree, before the gate runs" fg_refused "object $fg12b fails its hash"
expect_ok "fresh-gate: FG12: no checkout is left" fg_clean
restore_forged
expect_ok "forged objects: FG12 restored" no_state4
expect_ok "git state: none after the forged-object rows" no_state

# FG15, config-defined hooks (verdict X-6-F1-F2-F2-F1-F1 R4): git 2.47.3 does
# not run hook.<name>.command, but newer git does, and neither the hooks/
# check nor core.hooksPath=/dev/null stops it, so the key is refused by name,
# from the repository config and from the command line. No marker row: on
# this git the hook would not run either way.
git -C "$r" config hook.fg15.command "touch $t/fg15.marker"
git -C "$r" config hook.fg15.event post-checkout
fgrun card/v3-FG1
expect_ok "fresh-gate: FG15: a hook.<name>.command in the repository config is refused, by name" fg_refused "hook.fg15.command is 'touch $t/fg15.marker' (file:" "hook.fg15.event is 'post-checkout' (file:"
expect_ok "fresh-gate: FG15: no checkout is left" fg_clean
git -C "$r" config --remove-section hook.fg15
GIT_CONFIG_PARAMETERS="'hook.fg15.command'='true'" fgrun card/v3-FG1
expect_ok "fresh-gate: FG15: a -c hook.fg15.command is refused, at command-line scope" fg_refused "hook.fg15.command is 'true' (command line:"
expect_ok "fresh-gate: FG15: no hook config is left" bash -c "[ -z \"\$(git -C '$r' config --get-regexp '^hook\\.')\" ] && [ ! -e '$t/fg15.marker' ]"

# --- GLM rows go here (rework B, owner exception 2026-11-03) ---------------
# Rework B (verdict 3fdb25e0, R1 and R3): the rows that pin the tightened eol
# exception (eol=crlf and no lone LF or CR), the refusal of every other
# conversion attribute (ident), and the symlink hash. Each premise was probed
# on git 2.47.3 before the row was written; every forgery is a loose object
# unique to its own FG branch, every id is captured before its object is
# forged, and each window restores before the next move (no_state4), as
# FG11-FG12 do.

# FG11c, the verdict R1 forgery: the repository's own rule (`text=auto eol=lf`)
# written for one path. The forged blob holds the honest bytes with a CR before
# the LF; smudge is the identity under eol=lf, so the CR survives the checkout
# and the gate would run it, while clean strips the CR again, so hash-object
# --path still names the tree id — exactly what the old, attribute-only
# exception accepted. The eol=crlf requirement (and the no-lone-LF one) is what
# refuses it (mutants FG-EXCEPTION-LF, FG-EOL-ANY).
git -C "$r" checkout -q -b card/v3-FG11c nucleus/v3-scaffold
fgstub "$r" FG11c
printf 'nucleus/v3/spikes/fg11c.sh text=auto eol=lf\n' >"$r/.gitattributes"
printf 'STRICT=1\n' >"$v3/spikes/fg11c.sh"
git -C "$r" add .gitattributes nucleus/v3/spikes/fg11c.sh && git -C "$r" commit -qm "nucleus v3: FG11c cr-before-lf blob"
git -C "$r" checkout -q nucleus/v3-scaffold
fg11c_id=$(git -C "$r" rev-parse card/v3-FG11c:nucleus/v3/spikes/fg11c.sh)
fg11c_src=$(printf 'STRICT=1\r\n' | git -C "$r" hash-object -w --no-filters --stdin)
expect_ok "premise: FG11c: both blobs are loose objects" bash -c "[ -f '$(objlo "$r" "$fg11c_id")' ] && [ -f '$(objlo "$r" "$fg11c_src")' ]"
save_obj "$r" "$fg11c_id"
forge_from "$r" "$fg11c_id" "$fg11c_src"
git -C "$r" worktree add -q --detach "$t/fg11cp" card/v3-FG11c
fg11c_hidden() {
  local s a
  a=$(env -u GIT_NO_REPLACE_OBJECTS -u GIT_GRAFT_FILE -u GIT_CONFIG_COUNT git -C "$t/fg11cp" check-attr -a -- nucleus/v3/spikes/fg11c.sh)
  s=$(env -u GIT_NO_REPLACE_OBJECTS -u GIT_GRAFT_FILE -u GIT_CONFIG_COUNT git -C "$t/fg11cp" ls-files -s -- nucleus/v3/spikes/fg11c.sh)
  echo "attrs: $a"; echo "index: $s"
  [ "$a" = "$(printf 'nucleus/v3/spikes/fg11c.sh: text: auto\nnucleus/v3/spikes/fg11c.sh: eol: lf')" ] \
    && [ "$(git -C "$r" hash-object --no-filters "$t/fg11cp/nucleus/v3/spikes/fg11c.sh")" = "$fg11c_src" ] \
    && [ "$s" = "$(printf '100644 %s 0\tnucleus/v3/spikes/fg11c.sh' "$fg11c_id")" ] \
    && [ "$(env -u GIT_NO_REPLACE_OBJECTS -u GIT_GRAFT_FILE -u GIT_CONFIG_COUNT git -C "$t/fg11cp" hash-object --path=nucleus/v3/spikes/fg11c.sh -- nucleus/v3/spikes/fg11c.sh)" = "$fg11c_id" ]
}
expect_ok "premise: FG11c: attrs text=auto eol=lf; the checkout holds the CR bytes; the index names the tree id; hash-object --path equals the tree id (the old rule's hole)" fg11c_hidden
git -C "$r" worktree remove --force "$t/fg11cp"
fgrun card/v3-FG11c
expect_ok "fresh-gate: FG11c: a CR-before-LF forgery under text=auto eol=lf is refused, naming the path and both ids, before the gate runs" fg_refused "nucleus/v3/spikes/fg11c.sh: content hashes to $fg11c_src, tree says $fg11c_id"
expect_ok "fresh-gate: FG11c: no checkout is left" fg_clean
restore_forged
expect_ok "forged objects: FG11c restored" no_state4

# FG11d, a lone-LF forgery under eol=crlf (pins the CRLF-pair condition rework
# A added): text=auto leaves a blob that already holds a CRLF as it is on
# checkout, so the forged lone LF survives, and clean strips only CRLFs, so
# hash-object --path names the tree id again. The no-lone-LF check is what
# refuses it (mutant FG-NO-LONE-LF).
git -C "$r" checkout -q -b card/v3-FG11d nucleus/v3-scaffold
fgstub "$r" FG11d
printf 'nucleus/v3/spikes/fg11d.ps1 text=auto eol=crlf\n' >"$r/.gitattributes"
printf 'a\nb\n' >"$v3/spikes/fg11d.ps1"
git -C "$r" add .gitattributes nucleus/v3/spikes/fg11d.ps1 2>/dev/null && git -C "$r" commit -qm "nucleus v3: FG11d lone-lf blob"
git -C "$r" checkout -q nucleus/v3-scaffold
fg11d_id=$(git -C "$r" rev-parse card/v3-FG11d:nucleus/v3/spikes/fg11d.ps1)
fg11d_src=$(printf 'a\r\nb\n' | git -C "$r" hash-object -w --no-filters --stdin)
expect_ok "premise: FG11d: both blobs are loose objects" bash -c "[ -f '$(objlo "$r" "$fg11d_id")' ] && [ -f '$(objlo "$r" "$fg11d_src")' ]"
save_obj "$r" "$fg11d_id"
forge_from "$r" "$fg11d_id" "$fg11d_src"
git -C "$r" worktree add -q --detach "$t/fg11dp" card/v3-FG11d
fg11d_hidden() {
  local s a
  a=$(env -u GIT_NO_REPLACE_OBJECTS -u GIT_GRAFT_FILE -u GIT_CONFIG_COUNT git -C "$t/fg11dp" check-attr -a -- nucleus/v3/spikes/fg11d.ps1)
  s=$(env -u GIT_NO_REPLACE_OBJECTS -u GIT_GRAFT_FILE -u GIT_CONFIG_COUNT git -C "$t/fg11dp" ls-files -s -- nucleus/v3/spikes/fg11d.ps1)
  echo "attrs: $a"; echo "index: $s"
  [ "$a" = "$(printf 'nucleus/v3/spikes/fg11d.ps1: text: auto\nnucleus/v3/spikes/fg11d.ps1: eol: crlf')" ] \
    && [ "$(git -C "$r" hash-object --no-filters "$t/fg11dp/nucleus/v3/spikes/fg11d.ps1")" = "$fg11d_src" ] \
    && [ "$s" = "$(printf '100644 %s 0\tnucleus/v3/spikes/fg11d.ps1' "$fg11d_id")" ] \
    && [ "$(env -u GIT_NO_REPLACE_OBJECTS -u GIT_GRAFT_FILE -u GIT_CONFIG_COUNT git -C "$t/fg11dp" hash-object --path=nucleus/v3/spikes/fg11d.ps1 -- nucleus/v3/spikes/fg11d.ps1)" = "$fg11d_id" ]
}
expect_ok "premise: FG11d: attrs text=auto eol=crlf; the checkout holds the forged bytes unconverted (a lone LF); the index names the tree id; hash-object --path equals the tree id" fg11d_hidden
git -C "$r" worktree remove --force "$t/fg11dp"
fgrun card/v3-FG11d
expect_ok "fresh-gate: FG11d: a lone-LF forgery under text=auto eol=crlf is refused, naming the path and both ids, before the gate runs" fg_refused "nucleus/v3/spikes/fg11d.ps1: content hashes to $fg11d_src, tree says $fg11d_id"
expect_ok "fresh-gate: FG11d: no checkout is left" fg_clean
restore_forged
expect_ok "forged objects: FG11d restored" no_state4

# FG13, an ident path (verdict R3, pins "other attributes are refused"):
# ident's clean collapses any $Id: ... $ to $Id$, the same many-to-one hole as
# R1, so the exception must refuse the path outright. The forged keyword is
# *foreign* — `$Id: x y z $`, with spaces inside — which git's smudge leaves
# as it is on 2.47.3 (an expanded `$Id: <40 hex> $` would be re-smudged to the
# honest bytes and the forgery would do nothing). The path is eol=crlf because
# that is the only way to reach the attribute check now (under eol=lf, R1's
# rule alone refuses, and FG-ATTR-ANY would not be caught).
git -C "$r" checkout -q -b card/v3-FG13 nucleus/v3-scaffold
fgstub "$r" FG13
printf 'nucleus/v3/spikes/fg13.ps1 text eol=crlf ident\n' >"$r/.gitattributes"
printf 'a $Id$\n' >"$v3/spikes/fg13.ps1"
git -C "$r" add .gitattributes nucleus/v3/spikes/fg13.ps1 2>/dev/null && git -C "$r" commit -qm "nucleus v3: FG13 ident blob"
git -C "$r" checkout -q nucleus/v3-scaffold
fg13_id=$(git -C "$r" rev-parse card/v3-FG13:nucleus/v3/spikes/fg13.ps1)
fg13_src=$(printf 'a $Id: x y z $\n' | git -C "$r" hash-object -w --no-filters --stdin)
fg13_got=$(printf 'a $Id: x y z $\r\n' | git -C "$r" hash-object --no-filters --stdin)
expect_ok "premise: FG13: both blobs are loose objects" bash -c "[ -f '$(objlo "$r" "$fg13_id")' ] && [ -f '$(objlo "$r" "$fg13_src")' ]"
save_obj "$r" "$fg13_id"
forge_from "$r" "$fg13_id" "$fg13_src"
git -C "$r" worktree add -q --detach "$t/fg13p" card/v3-FG13
fg13_hidden() {
  local s a
  a=$(env -u GIT_NO_REPLACE_OBJECTS -u GIT_GRAFT_FILE -u GIT_CONFIG_COUNT git -C "$t/fg13p" check-attr -a -- nucleus/v3/spikes/fg13.ps1)
  s=$(env -u GIT_NO_REPLACE_OBJECTS -u GIT_GRAFT_FILE -u GIT_CONFIG_COUNT git -C "$t/fg13p" ls-files -s -- nucleus/v3/spikes/fg13.ps1)
  echo "attrs: $a"; echo "index: $s"
  [ "$a" = "$(printf 'nucleus/v3/spikes/fg13.ps1: text: set\nnucleus/v3/spikes/fg13.ps1: eol: crlf\nnucleus/v3/spikes/fg13.ps1: ident: set')" ] \
    && [ "$(git -C "$r" hash-object --no-filters "$t/fg13p/nucleus/v3/spikes/fg13.ps1")" = "$fg13_got" ] \
    && [ "$s" = "$(printf '100644 %s 0\tnucleus/v3/spikes/fg13.ps1' "$fg13_id")" ] \
    && [ "$(env -u GIT_NO_REPLACE_OBJECTS -u GIT_GRAFT_FILE -u GIT_CONFIG_COUNT git -C "$t/fg13p" hash-object --path=nucleus/v3/spikes/fg13.ps1 -- nucleus/v3/spikes/fg13.ps1)" = "$fg13_id" ] \
    && od -An -v -tx1 -- "$t/fg13p/nucleus/v3/spikes/fg13.ps1" | LC_ALL=C awk '{ for (i = 1; i <= NF; i++) { if (($i == "0a") != (p == "0d")) bad = 1; p = $i } } END { if (p == "0d") bad = 1; exit bad ? 3 : 0 }'
}
expect_ok "premise: FG13: attrs text set, eol crlf, ident set; the checkout holds the foreign ident unexpanded (pure CRLF, not the honest checkout's bytes); the index names the tree id; hash-object --path equals the tree id" fg13_hidden
git -C "$r" worktree remove --force "$t/fg13p"
fgrun card/v3-FG13
expect_ok "fresh-gate: FG13: a forged blob on an ident path is refused, naming the path, the forged bytes' hash and the tree id, before the gate runs" fg_refused "nucleus/v3/spikes/fg13.ps1: content hashes to $fg13_got, tree says $fg13_id"
expect_ok "fresh-gate: FG13: no checkout is left" fg_clean
restore_forged
expect_ok "forged objects: FG13 restored" no_state4

# FG14, a swapped symlink blob (verdict R3, pins the symlink hash): the
# checkout materialises whatever target the forged blob names, the fresh index
# and the tree agree on fg14.lnk's own id, and only hashing the readlink bytes
# against the tree id refuses (mutant FG-LINK-SKIP).
git -C "$r" checkout -q -b card/v3-FG14 nucleus/v3-scaffold
fgstub "$r" FG14
ln -s fg.txt "$v3/spikes/fg14.lnk"
ln -s ../scripts/gate.sh "$v3/spikes/fg14b.lnk"
git -C "$r" add nucleus/v3/spikes/fg14.lnk nucleus/v3/spikes/fg14b.lnk && git -C "$r" commit -qm "nucleus v3: FG14 symlinks"
git -C "$r" checkout -q nucleus/v3-scaffold
fg14_id=$(git -C "$r" rev-parse card/v3-FG14:nucleus/v3/spikes/fg14.lnk)
fg14b_id=$(git -C "$r" rev-parse card/v3-FG14:nucleus/v3/spikes/fg14b.lnk)
expect_ok "premise: FG14: both symlink blobs are loose objects" bash -c "[ -f '$(objlo "$r" "$fg14_id")' ] && [ -f '$(objlo "$r" "$fg14b_id")' ]"
save_obj "$r" "$fg14_id"
forge_from "$r" "$fg14_id" "$fg14b_id"
git -C "$r" worktree add -q --detach "$t/fg14p" card/v3-FG14
fg14_hidden() {
  local s
  s=$(env -u GIT_NO_REPLACE_OBJECTS -u GIT_GRAFT_FILE -u GIT_CONFIG_COUNT git -C "$t/fg14p" ls-files -s -- nucleus/v3/spikes/fg14.lnk)
  echo "index: $s"; echo "readlink: $(readlink "$t/fg14p/nucleus/v3/spikes/fg14.lnk")"
  [ -L "$t/fg14p/nucleus/v3/spikes/fg14.lnk" ] \
    && [ "$(readlink "$t/fg14p/nucleus/v3/spikes/fg14.lnk")" = ../scripts/gate.sh ] \
    && [ "$s" = "$(printf '120000 %s 0\tnucleus/v3/spikes/fg14.lnk' "$fg14_id")" ]
}
expect_ok "premise: FG14: a plain worktree add makes fg14.lnk a symlink to ../scripts/gate.sh (the other blob's target) while the index names its own tree id" fg14_hidden
git -C "$r" worktree remove --force "$t/fg14p"
fgrun card/v3-FG14
expect_ok "fresh-gate: FG14: a swapped symlink blob is refused, naming the path and both ids, before the gate runs" fg_refused "nucleus/v3/spikes/fg14.lnk: content hashes to $fg14b_id, tree says $fg14_id"
expect_ok "fresh-gate: FG14: no checkout is left" fg_clean
restore_forged
expect_ok "forged objects: FG14 restored" no_state4
# FG11e, a lone-CR forgery under text eol=crlf (verdict R6, pins the no-lone-CR
# half of the CRLF-pair condition, which FG11d's lone LF cannot reach): clean
# collapses only CRLF pairs, so the forged \r\r\n cleans back to the honest
# \r\n blob — hash-object --path still names the tree id — while smudge gives
# an LF that already has a CR before it no second one, so the lone CR survives
# the checkout and the gate would run bytes the honest checkout (x=1\r\n)
# never holds. The blob is committed raw (hash-object -w --no-filters plus
# update-index --cacheinfo) because git add would clean it to x=1\n. The byte
# premise below pins only the no-lone-LF half — exactly what FG-LONE-CR-OK
# keeps — so the lone CR is left for the refusal row to pin.
git -C "$r" checkout -q -b card/v3-FG11e nucleus/v3-scaffold
fgstub "$r" FG11e
printf 'nucleus/v3/spikes/fg11e.ps1 text eol=crlf\n' >"$r/.gitattributes"
git -C "$r" add .gitattributes
fg11e_raw=$(printf 'x=1\r\n' | git -C "$r" hash-object -w --no-filters --stdin)
git -C "$r" update-index --add --cacheinfo 100644,"$fg11e_raw",nucleus/v3/spikes/fg11e.ps1
git -C "$r" commit -qm "nucleus v3: FG11e lone-cr blob"
git -C "$r" checkout -q nucleus/v3-scaffold
fg11e_id=$(git -C "$r" rev-parse card/v3-FG11e:nucleus/v3/spikes/fg11e.ps1)
fg11e_src=$(printf 'x=1\r\r\n' | git -C "$r" hash-object -w --no-filters --stdin)
expect_ok "premise: FG11e: the committed blob is the raw x=1\r\n one (git add would have cleaned it), and both blobs are loose objects" bash -c "[ '$fg11e_id' = '$fg11e_raw' ] && [ -f '$(objlo "$r" "$fg11e_id")' ] && [ -f '$(objlo "$r" "$fg11e_src")' ]"
save_obj "$r" "$fg11e_id"
forge_from "$r" "$fg11e_id" "$fg11e_src"
git -C "$r" worktree add -q --detach "$t/fg11ep" card/v3-FG11e
fg11e_hidden() {
  local s a
  a=$(env -u GIT_NO_REPLACE_OBJECTS -u GIT_GRAFT_FILE -u GIT_CONFIG_COUNT git -C "$t/fg11ep" check-attr -a -- nucleus/v3/spikes/fg11e.ps1)
  s=$(env -u GIT_NO_REPLACE_OBJECTS -u GIT_GRAFT_FILE -u GIT_CONFIG_COUNT git -C "$t/fg11ep" ls-files -s -- nucleus/v3/spikes/fg11e.ps1)
  echo "attrs: $a"; echo "index: $s"
  [ "$a" = "$(printf 'nucleus/v3/spikes/fg11e.ps1: text: set\nnucleus/v3/spikes/fg11e.ps1: eol: crlf')" ] \
    && [ "$(git -C "$r" hash-object --no-filters "$t/fg11ep/nucleus/v3/spikes/fg11e.ps1")" = "$fg11e_src" ] \
    && [ "$s" = "$(printf '100644 %s 0\tnucleus/v3/spikes/fg11e.ps1' "$fg11e_id")" ] \
    && [ "$(env -u GIT_NO_REPLACE_OBJECTS -u GIT_GRAFT_FILE -u GIT_CONFIG_COUNT git -C "$t/fg11ep" hash-object --path=nucleus/v3/spikes/fg11e.ps1 -- nucleus/v3/spikes/fg11e.ps1)" = "$fg11e_id" ] \
    && od -An -v -tx1 -- "$t/fg11ep/nucleus/v3/spikes/fg11e.ps1" | LC_ALL=C awk '{ for (i = 1; i <= NF; i++) { if ($i == "0a" && p != "0d") bad = 1; p = $i } } END { exit bad ? 3 : 0 }'
}
expect_ok "premise: FG11e: attrs text set, eol crlf; the checkout holds the source bytes (a lone CR, no lone LF); the index names the tree id; hash-object --path equals the tree id" fg11e_hidden
git -C "$r" worktree remove --force "$t/fg11ep"
fgrun card/v3-FG11e
expect_ok "fresh-gate: FG11e: a lone-CR forgery under text eol=crlf is refused, naming the path and both ids, before the gate runs" fg_refused "nucleus/v3/spikes/fg11e.ps1: content hashes to $fg11e_src, tree says $fg11e_id"
expect_ok "fresh-gate: FG11e: no checkout is left" fg_clean
restore_forged
expect_ok "forged objects: FG11e restored" no_state4
expect_ok "fresh-gate: no attribute file is left in the scratch worktree after the GLM rows" bash -c "[ ! -e '$r/.gitattributes' ]"
expect_ok "git state: none after the GLM forged-object rows" no_state
# --- end of GLM rows --------------------------------------------------------
# --- full 40-hex verdict Commit (X-6-F1-F2-F3-F1-F1) ------------------------
# Merge mode takes only tip^'s full id; an audit takes a short one only for a
# merge listed in SHORT_COMMIT_MERGES by its exact merge commit id, and then
# under the old rule, ambiguity count included. Plain merges, no forged object.
# hcard ID LEN: card/v3-ID with one spikes/ commit, then a verdict naming the
# first LEN digits of that commit's id (tip^ once the verdict is committed).
hcard() { card "$1" S standard; branch "$1" "nucleus/v3/spikes/${1,,}.txt"; g7c_verdict "$1" "$(git -C "$r" rev-parse HEAD | cut -c1-"$2")"; }
# hcommit ID: the Commit field of card/v3-ID's tip verdict.
hcommit() { git -C "$r" show "card/v3-$1:nucleus/v3/verdicts/$1.md" | sed -n 's/^Commit: //p'; }
# hpre ID LEN: the Commit is LEN digits long and a prefix of tip^'s full id.
hpre() { local c; c=$(hcommit "$1"); echo "Commit: $c"; [ "${#c}" -eq "$2" ] && [[ $(git -C "$r" rev-parse "card/v3-$1^") == "$c"* ]]; }
# hnot ID: the full-id refusal line for card/v3-ID's verdict.
hnot() { echo "verdict Commit '$(hcommit "$1")' is not a full 40-hex commit id"; }
# hmerge REF ID TITLE-SUFFIX: REF off nucleus/v3-scaffold with a plain titled
# merge of card/v3-ID, as an orchestrator bypassing merge-card would make it.
hmerge() { git -C "$r" checkout -q -B "$1" nucleus/v3-scaffold && git -C "$r" merge -q --no-ff -m "nucleus v3: merge card/v3-$2 ($3)" "card/v3-$2"; }
hcard H1 12
expect_ok "premise: H1's Commit is a 12-hex prefix of tip^'s full id" hpre H1 12
scratch; expect_ok "H1: merge-card refuses a correct 12-hex prefix of tip^, naming the full-id rule" out_has "$(hnot H1)" -- mc H1
hcard H2 39
expect_ok "premise: H2's Commit is a 39-hex prefix of tip^'s full id" hpre H2 39
scratch; expect_ok "H2: merge-card refuses a correct 39-hex prefix of tip^, naming the full-id rule" out_has "$(hnot H2)" -- mc H2
hmerge scratch/h3 H1 "MERGE, gate S/test-model"
h3=$(git -C "$r" rev-parse scratch/h3)
expect_ok "H3: audit fails on a titled merge of H1's branch (unlisted), naming the full-id rule" out_has "audit: H1 (" "$(hnot H1)" -- bash -c "cd '$v3' && bash scripts/audit-merges.sh scratch/h3"
# L2's merge: G7C's branch (Y, sharing X's 7-hex prefix) under a titled merge.
expect_ok "premise: L2: G7C's X and Y are still distinct commits sharing the prefix" g7c_premise
hmerge scratch/l2 G7C "MERGE, gate S/test-model"
l2=$(git -C "$r" rev-parse scratch/l2)
# L3's merge: G4-shaped, a 12-hex Commit of the reviewed commit, then a late
# change and the verdict re-committed, so the value is no prefix of tip^.
card L3 S standard; branch L3 nucleus/v3/spikes/l3.txt
l3c=$(git -C "$r" rev-parse HEAD | cut -c1-12); g7c_verdict L3 "$l3c"
echo late >>"$v3/spikes/l3.txt"; git -C "$r" commit -qam "nucleus v3: L3 late non-core change"
sed -i 's/^Date: .*/Date: 2026-10-18/' "$v3/verdicts/L3.md"; git -C "$r" commit -qam "nucleus v3: L3 verdict MERGE (re-committed)"
l3_premise() { [ "$(hcommit L3)" = "$l3c" ] && [[ $(git -C "$r" rev-parse card/v3-L3^) != "$l3c"* ]] && [[ $(git -C "$r" rev-parse card/v3-L3~3) == "$l3c"* ]]; }
expect_ok "premise: L3's Commit names the reviewed commit, not tip^ (a late change sits between)" l3_premise
hmerge scratch/l3 L3 "MERGE, gate S/test-model"
l3=$(git -C "$r" rev-parse scratch/l3)
# L5's merge: H3's merge rebuilt with another title, so the same parents (the
# same verdict) under a new merge commit id.
hmerge scratch/l5 H1 "MERGE, gate S/rebuilt"
l5=$(git -C "$r" rev-parse scratch/l5)
l5_premise() {
  local m
  [ "$l5" != "$h3" ] && [ "$(git -C "$r" rev-parse "$l5^1" "$l5^2")" = "$(git -C "$r" rev-parse "$h3^1" "$h3^2")" ] || return 1
  for m in "$h3" "$l2" "$l3"; do [ "${l5:0:7}" != "${m:0:7}" ] || return 1; done
}
expect_ok "premise: L5's merge has H3's parents, a new id, and no 7-digit prefix shared with a listed id" l5_premise
# The legacy copy: the scripts in $v3/scripts/.legacy (audit-merges.sh cd's to
# its own directory's parent, as for audit_novr), the marker block of its
# enforce-lib.sh holding exactly the test merges named. The real file is never
# changed. legacy_copy ID...: write it.
legacy_copy() {
  local d=$v3/scripts/.legacy m
  mkdir -p "$d"; cp "$v3"/scripts/*.sh "$d"/
  {
    sed -n '1,/^# BEGIN SHORT_COMMIT_MERGES (frozen)$/p' "$v3/scripts/enforce-lib.sh"
    echo 'SHORT_COMMIT_MERGES=('; for m in "$@"; do echo "  $m # test"; done; echo ')'
    sed -n '/^# END SHORT_COMMIT_MERGES$/,$p' "$v3/scripts/enforce-lib.sh"
  } >"$d/enforce-lib.sh"
}
legacy_copy "$h3" "$l2" "$l3"
# The copy differs from the real file only inside the marker block, and lists
# exactly H3's, L2's and L3's merges.
legacy_premise() {
  local d=$v3/scripts/.legacy
  cmp <(sed '/^# BEGIN SHORT_COMMIT_MERGES (frozen)$/,/^# END SHORT_COMMIT_MERGES$/d' "$v3/scripts/enforce-lib.sh") \
    <(sed '/^# BEGIN SHORT_COMMIT_MERGES (frozen)$/,/^# END SHORT_COMMIT_MERGES$/d' "$d/enforce-lib.sh") &&
    [ "$(bash -c '. "$1/enforce-lib.sh" && echo "${SHORT_COMMIT_MERGES[*]}"' _ "$d")" = "$h3 $l2 $l3" ]
}
expect_ok "premise: the legacy copy differs only in its marker block, listing H3, L2 and L3" legacy_premise
audit_legacy() { (cd "$v3" && bash scripts/.legacy/audit-merges.sh "$1"); }
legacy_ok() { local o rc=0; o=$(audit_legacy "$1" 2>&1) || rc=$?; echo "$o"; [ "$rc" -eq 0 ] && grep -qE "^audit: ok \([0-9]+ commits\)$" <<<"$o"; }
expect_ok "L1: the copy's audit accepts H3's listed merge (the legacy path works)" legacy_ok scratch/h3
expect_ok "L2: a listed G7C-shaped merge is still refused as ambiguous (never weaker than today)" out_has "verdict Commit '$g7c' is ambiguous" -- audit_legacy scratch/l2
expect_ok "L3: a listed merge whose short Commit is not a prefix of tip^ is refused" out_has "verdict Commit '$l3c' is not the commit under the verdict" -- audit_legacy scratch/l3
scratch; expect_ok "L4: merge-card in the copy still refuses H1's branch (the list has no effect in merge mode)" out_has "$(hnot H1)" -- bash -c "cd '$v3' && bash scripts/.legacy/merge-card.sh H1"
# L4b: merge mode reads its 5th argument as CARD_GATE, never as a merge id, so
# even a listed id there leaves the Commit rule at 40 hex.
cm_legacy() { (cd "$v3" && . scripts/.legacy/core-paths-lib.sh && . scripts/.legacy/enforce-lib.sh && check_merge "$@"); }
expect_ok "L4b: check_merge in merge mode ignores a listed id given as its 5th argument" out_has "$(hnot H1)" -- cm_legacy H1 nucleus/v3-scaffold card/v3-H1 merge "$h3" standard
expect_ok "L5: the copy's audit refuses H3's verdict under a rebuilt merge (keyed by the exact merge id)" out_has "audit: H1 (" "$(hnot H1)" -- audit_legacy scratch/l5
rm -rf "$v3/scripts/.legacy"
expect_ok "the legacy copy is removed" bash -c "[ ! -e '$v3/scripts/.legacy' ]"
# R1: the real list, as generated at X-6-F1-F2-F3-F1-F1's base (its comment
# gives the command): 44 entries, each a full 40-hex id on its own line with a
# card id comment, all distinct. "command wc": the ledger rows define wc().
r1_list() {
  local blk n
  blk=$(sed -n '/^# BEGIN SHORT_COMMIT_MERGES (frozen)$/,/^# END SHORT_COMMIT_MERGES$/p' "$src/scripts/enforce-lib.sh" | sed '1,2d;$d' | sed '$d')
  n=$(bash -c '. "$1/scripts/enforce-lib.sh" && printf "%s\n" "${SHORT_COMMIT_MERGES[@]}"' _ "$src" | LC_ALL=C grep -cE '^[0123456789abcdef]{40}$')
  echo "sourced 40-hex entries: $n; block lines: $(command wc -l <<<"$blk")"
  [ "$n" -eq 44 ] && [ "$(bash -c '. "$1/scripts/enforce-lib.sh" && echo "${#SHORT_COMMIT_MERGES[@]}"' _ "$src")" -eq 44 ] &&
    [ "$(command wc -l <<<"$blk")" -eq 44 ] && ! LC_ALL=C grep -vqE '^  [0123456789abcdef]{40} # [A-Za-z0-9._-]+$' <<<"$blk" &&
    [ "$(awk '{print $1}' <<<"$blk" | LC_ALL=C sort -u | command wc -l)" -eq 44 ]
}
expect_ok "R1: the real SHORT_COMMIT_MERGES has 44 distinct full 40-hex ids, one per line with a card id" r1_list
expect_ok "git state: none after the full-id rows" no_state
# --- Step 3 self-test (X-6-F1-F2-F3-F1-F4) -----------------------------------
# A throwaway script that sources nothing but the suite's own abort-trap block
# (lifted verbatim between the markers above) and runs `false` under set -e:
# its output must have the FAIL aborted line and its exit status must be
# non-zero, without aborting this suite. Lifting the real block — not a
# re-implementation — is what makes the TRAP-SILENT mutant (the print removed
# above) fail this row.
step3_selftest() {
  local self=${BASH_SOURCE[0]} s o rc=0
  s=$t/step3-selftest.sh
  {
    printf '#!/usr/bin/env bash\nset -euo pipefail\nt=$(mktemp -d) # the lifted trap cleans this\n'
    sed -n '/^# BEGIN suite-abort-trap (lifted verbatim by the Step 3 self-test row)$/,/^# END suite-abort-trap$/p' "$self" | sed '1d;$d'
    printf 'false\nsuite_finished=1\nexit 0\n'
  } >"$s"
  o=$(bash "$s" 2>&1) || rc=$?
  printf 'exit %d\n%s\n' "$rc" "$o"
  [ "$rc" -ne 0 ] && grep -qF "FAIL suite aborted (set -e) near line" <<<"$o"
}
expect_ok "Step 3 self-test: a set -e abort prints the FAIL aborted line and exits non-zero" step3_selftest
echo
if [ "$fails" -eq 0 ]; then echo "test-enforcement: all passed"; else echo "test-enforcement: $fails FAILED"; fi
suite_finished=1
exit $(( fails > 0 ))
