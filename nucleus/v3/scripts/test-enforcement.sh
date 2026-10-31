#!/usr/bin/env bash
# Scenario tests for the enforcement scripts (PLAN-v3 §7.6), run by gate.sh.
#
# Builds a throwaway repository (and a bare "origin") under a temp dir, copies
# the real scripts and runner into it, and checks that every rule both refuses
# what it must and accepts what it must. Nothing touches the real repository.
set -euo pipefail
src=$(cd "$(dirname "$0")/.." && pwd)        # the real nucleus/v3
t=$(mktemp -d)
trap 'rm -rf "$t"' EXIT
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
printf 'Card: BOOT\nClass: core\nCard-gate: P\nVerdict: MERGE\nGate-tier: P\nGate-model: test-model\nGate-session: t\nDate: 2026-10-17\nGate-exit: GATE EXIT=0\nCommit: %s\nMutants: n/a\nFollowups: none\n' "$(git -C "$r" rev-parse --short HEAD)" >"$r/nucleus/v3/verdicts/BOOT.md"
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
    "$id" "$cls" "$cg" "$vd" "$tier" "$ge" "$(git -C "$r" rev-parse --short HEAD)" "$fu" >"$(mkdir -p "$v3/verdicts" && echo "$v3/verdicts/$id.md")"
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
printf 'Card: MB\nClass: standard\nCard-gate: S\nVerdict: MERGE\nGate-tier: S\nGate-model: test-model\nGate-session: t\nDate: 2026-10-20\nGate-exit: GATE EXIT=0\nCommit: %s\nMutants: n/a\nFollowups: none\n' "$(git -C "$mb" rev-parse --short HEAD)" >"$mb/nucleus/v3/verdicts/MB.md"
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
  printf 'Card: %s\nClass: standard\nCard-gate: S\nVerdict: MERGE\nGate-tier: S\nGate-model: test-model\nGate-session: t\nDate: 2026-10-31\nGate-exit: %s\nCommit: %s\nMutants: n/a\nFollowups: none\n' "$id" "$ge" "$(git -C "$d" rev-parse --short HEAD)" >"$d/nucleus/v3/verdicts/$id.md"
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
# G4 again, plus a branch named with the verdict's short Commit pointing at the
# late change. git resolves refs before short object ids, so a lookup of the
# field follows the branch to tip^; only a ref-free compare refuses G7.
card G7 S standard; branch G7 nucleus/v3/spikes/g7.txt; verdict G7 S MERGE standard S
c7=$(sed -n 's/^Commit: //p' "$v3/verdicts/G7.md")
echo late >>"$v3/spikes/g7.txt"; git -C "$r" commit -qam "nucleus v3: G7 late non-core change"
sed -i 's/^Date: .*/Date: 2026-10-18/' "$v3/verdicts/G7.md"; git -C "$r" commit -qam "nucleus v3: G7 verdict MERGE (re-committed)"
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
scratch; expect_no "a verdict whose short Commit also names a ground collision is refused as ambiguous (G7C)" mc G7C
scratch; expect_ok "G7C's refusal names the ambiguity" bash -c "cd '$v3' && rc=0 && o=\$(bash scripts/merge-card.sh G7C 2>&1) || rc=\$?; echo \"\$o\"; [ \"\$rc\" -ne 0 ] && grep -qF \"verdict Commit '$g7c' is ambiguous\" <<<\"\$o\""
# Control: the same Y under a full 40-hex Commit is unambiguous and merges.
card G7D S standard
git -C "$r" checkout -q -B card/v3-G7D "$g7y"; g7c_verdict G7D "$g7y"
scratch; expect_ok "the same Y with a full 40-hex Commit merges (G7D)" mc G7D
# tip^'s own short id, spelled so git still resolves it to tip^ but the field is
# not plain lowercase hex: <short>~0 (G6E) and the id in uppercase (G6U). The
# text compare with tip^'s full id refuses both as well, so a refusal alone
# cannot show the hex rule is there. The reason pins it: with the end anchor
# dropped (^...{7,40}, no $) or A-F added to the class, the hex rule passes the
# value and the refusal names the compare instead.
# hexrule_refusal ID VALUE: merge-card refuses ID, naming VALUE under the hex rule.
hexrule_refusal() { local o rc=0; o=$(mc "$1" 2>&1) || rc=$?; echo "$o"; [ "$rc" -ne 0 ] && grep -qF "verdict Commit '$2' is not a hex commit id" <<<"$o"; }
card G6E S standard; branch G6E nucleus/v3/spikes/g6e.txt
g6e="$(git -C "$r" rev-parse --short HEAD)~0"; g7c_verdict G6E "$g6e"
expect_ok "premise: G6E's Commit $g6e resolves to tip^" commit_is_tip_parent G6E
scratch; expect_no "a verdict Commit of tip^'s short id plus ~0 is refused (G6E)" mc G6E
scratch; expect_ok "G6E's refusal names the hex rule (end anchor)" hexrule_refusal G6E "$g6e"
# An id whose first seven digits are all 0-9 has no uppercase form there, and a
# letter after them would let a dropped end anchor pass the value too (the
# unanchored match reads only seven digits), so G6U would not be about case
# alone. Add work commits until a letter falls in the first seven (about 1 id in
# 27 lacks one).
card G6U S standard; branch G6U nucleus/v3/spikes/g6u.txt
g6un=0
while [[ $(git -C "$r" rev-parse HEAD | cut -c1-7) != *[abcdef]* ]] && [ "$g6un" -lt 64 ]; do
  echo "$g6un" >>"$v3/spikes/g6u.txt"; git -C "$r" commit -qam "nucleus v3: G6U work ($g6un)"; g6un=$((g6un + 1))
done
g6u=$(git -C "$r" rev-parse --short HEAD | tr abcdef ABCDEF); g7c_verdict G6U "$g6u"
g6u_premise() { [[ ${g6u:0:7} == *[ABCDEF]* ]] && commit_is_tip_parent G6U; }
expect_ok "premise: G6U's Commit $g6u has a letter in its first seven digits and resolves to tip^" g6u_premise
scratch; expect_no "a verdict Commit of tip^'s short id in uppercase is refused (G6U)" mc G6U
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
)
for c in "${bash_deny[@]}"; do expect_ok "runner: bash deny $c" bp deny "$c"; done
for c in "${bash_allow[@]}"; do expect_ok "runner: bash allow $c" bp allow "$c"; done
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

# One definition per language, kept identical; a caller's GIT_CONFIG_* entries
# are appended to, never clobbered, and a nested source adds nothing.
env_same() { local sh py; sh=$(cd "$v3" && env -i PATH="$PATH" bash -c '. scripts/git-env.sh; env' | grep '^GIT_' | sort)
  py=$(env -i PATH="$PATH" PYTHONDONTWRITEBYTECODE=1 python3 -c 'import importlib.util,sys; s=importlib.util.spec_from_file_location("rc",sys.argv[1]); rc=importlib.util.module_from_spec(s); s.loader.exec_module(rc); [print(f"{k}={v}") for k,v in sorted(rc.git_env({}).items()) if k.startswith("GIT_")]' "$v3/tools/run_card.py" | sort)
  echo "sh: $sh"; echo "py: $py"; [ -n "$sh" ] && [ "$sh" = "$py" ]; }
expect_ok "git env: git-env.sh and run_card.py's GIT_ENV set the same variables" env_same
cfg_sh() { (cd "$v3" && GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0=nv3.probe GIT_CONFIG_VALUE_0=kept bash -c '. scripts/git-env.sh; . scripts/git-env.sh; echo "count $GIT_CONFIG_COUNT"; [ "$GIT_CONFIG_COUNT" = 3 ] && [ "$(git config nv3.probe)" = kept ] && [ "$(git config core.fsmonitor)" = false ]'); }
expect_ok "git env: git-env.sh appends to a caller's GIT_CONFIG_COUNT entries" cfg_sh
cfg_py() { PYTHONDONTWRITEBYTECODE=1 python3 - "$v3/tools/run_card.py" "$r" <<'P'
import importlib.util, os, pathlib, sys
spec = importlib.util.spec_from_file_location("rc", sys.argv[1]); rc = importlib.util.module_from_spec(spec); spec.loader.exec_module(rc)
caller = dict(os.environ, GIT_CONFIG_COUNT="1", GIT_CONFIG_KEY_0="nv3.probe", GIT_CONFIG_VALUE_0="kept")
env = rc.git_env(caller)
got = [rc.run(["git", "config", k], sys.argv[2], env=caller)[1].strip() for k in ("nv3.probe", "core.fsmonitor")]
print(got, env["GIT_CONFIG_COUNT"])
sys.exit(0 if got == ["kept", "false"] and env["GIT_CONFIG_COUNT"] == "3" and rc.git_env(env) == env else 1)
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
# replace-honouring cat-file sees one commit for the prefix, not two.
git -C "$r" replace -f "$g7x" "$(echo blob | git -C "$r" hash-object -w --stdin)"
expect_ok "premise: G7C: plain cat-file reports X as a blob" bash -c "[ \"\$(env -u GIT_NO_REPLACE_OBJECTS git -C '$r' cat-file -t $g7x)\" = blob ]"
scratch; expect_ok "merge-card: G7C under a replace ref is still refused as ambiguous" out_has "verdict Commit '$g7c' is ambiguous" "refs/replace/$g7x" -- mc G7C
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
  esac
done
echo '{"type":"step_finish","sessionID":"ses_fake","part":{"tokens":{"input":100,"output":10,"reasoning":1}}}'
F
chmod +x "$t/bin/opencode"
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

echo
if [ "$fails" -eq 0 ]; then echo "test-enforcement: all passed"; else echo "test-enforcement: $fails FAILED"; fi
exit $(( fails > 0 ))
