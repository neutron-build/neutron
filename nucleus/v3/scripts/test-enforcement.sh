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
forge_from "$r" "$ovt" "$sct"
expect_ok "(a) merge-card refuses, naming the tree" out_has "object $ovt fails its hash" -- mc OV
restore_forged
expect_ok "forged objects: (a) restored" no_state4

# (a') The tip check's own range: a verdict tip that also adds tools/ov2.py,
# with the work commit's root tree swapped for the tip tree minus the verdict,
# so the tip's diff-tree lists only the verdict, every other rule passes (a P
# card) and only verify_objects head^1 head refuses (the NO-VERIFY-TIP row).
card OV2 P core
git -C "$r" checkout -q -b card/v3-OV2 nucleus/v3-scaffold
echo ov2 >"$v3/spikes/ov2.txt"; git -C "$r" add -A; git -C "$r" commit -qm "nucleus v3: OV2 work"
w2=$(git -C "$r" rev-parse HEAD); w2t=$(git -C "$r" rev-parse "HEAD^{tree}")
echo ov2 >"$v3/tools/ov2.py"
printf 'Card: OV2\nClass: core\nCard-gate: P\nVerdict: MERGE\nGate-tier: P\nGate-model: test-model\nGate-session: t\nDate: 2026-11-03\nGate-exit: GATE EXIT=0\nCommit: %s\nMutants: n/a\nFollowups: none\n' "$(git -C "$r" rev-parse --short HEAD)" >"$v3/verdicts/OV2.md"
git -C "$r" add -A; git -C "$r" commit -qm "nucleus v3: OV2 verdict MERGE"
ov2py=$(git -C "$r" rev-parse "card/v3-OV2:nucleus/v3/tools/ov2.py")
# The tip tree minus the verdict file, built through a scratch index.
GIT_INDEX_FILE=$t/ov2.idx git -C "$r" read-tree "$w2t"
GIT_INDEX_FILE=$t/ov2.idx git -C "$r" update-index --add --cacheinfo "100644,$ov2py,nucleus/v3/tools/ov2.py"
v2y=$(GIT_INDEX_FILE=$t/ov2.idx git -C "$r" write-tree)
rm -f "$t/ov2.idx"
git -C "$r" checkout -q -B scratch/pass1 nucleus/v3-scaffold
save_obj "$r" "$w2t"
forge_from "$r" "$w2t" "$v2y"
a2_tip() { local d; d=$(git -C "$r" diff-tree --no-commit-id --name-only -r card/v3-OV2); echo "tip: $d"; [ "$d" = nucleus/v3/verdicts/OV2.md ]; }
expect_ok "premise: (a') under the swap the tip's diff-tree lists only the verdict" a2_tip
expect_ok "(a') merge-card refuses through the tip check, naming the work tree" out_has "object $w2t fails its hash" -- mc OV2
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
forge_from "$r" "$sct" "$ovt"
expect_ok "(b) merge-card refuses, naming the scaffold tree" out_has "object $sct fails its hash" -- mc OV
restore_forged
# (b') merge-card's own call: the scratch tip's tree, which no check_merge
# range covers (the merge base is below it): the NO-VERIFY-MERGE-CARD row and
# the only row whose forged object enters the want set through BASE alone.
# HEAD moves to pass2 before the forgery (no checkout moves under one).
git -C "$r" checkout -q scratch/pass2
p2t=$(git -C "$r" rev-parse "scratch/pass2^{tree}")
save_obj "$r" "$p2t"
forge_from "$r" "$p2t" "$sct"
expect_ok "(b') merge-card refuses on a forged scratch-tip tree outside the card's range" out_has "object $p2t fails its hash" -- mc A
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
printf 'Card: OI\nClass: standard\nCard-gate: S\nVerdict: MERGE\nGate-tier: S\nGate-model: test-model\nGate-session: t\nDate: 2026-11-03\nGate-exit: GATE EXIT=0\nCommit: %s\nMutants: n/a\nFollowups: none\n' "$(git -C "$r" rev-parse --short HEAD)" >"$v3/verdicts/OI.md"
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
printf 'Card: OG\nClass: standard\nCard-gate: S\nVerdict: MERGE\nGate-tier: S\nGate-model: test-model\nGate-session: t\nDate: 2026-11-03\nGate-exit: GATE EXIT=0\nCommit: %s\nMutants: n/a\nFollowups: none\n' "$(git -C "$r" rev-parse --short HEAD)" >"$v3/verdicts/OG.md"
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
printf 'Card: OK\nClass: standard\nCard-gate: S\nVerdict: MERGE\nGate-tier: S\nGate-model: test-model\nGate-session: t\nDate: 2026-11-03\nGate-exit: GATE EXIT=0\nCommit: %s\nMutants: n/a\nFollowups: none\n' "$(git -C "$kb" rev-parse --short HEAD)" >"$kb/nucleus/v3/verdicts/OK.md"
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
printf 'Card: OL\nClass: core\nCard-gate: P\nVerdict: MERGE\nGate-tier: S\nGate-model: test-model\nGate-session: t\nDate: 2026-11-03\nGate-exit: GATE EXIT=0\nCommit: %s\nMutants: n/a\nFollowups: none\n' "$(git -C "$r" rev-parse --short HEAD)" >"$v3/verdicts/OL.md"
git -C "$r" add -A; git -C "$r" commit -qm "nucleus v3: OL verdict MERGE"
olid=$(git -C "$r" rev-parse "card/v3-OL:nucleus/v3/verdicts/OL.md")
sed 's/^Gate-tier: S$/Gate-tier: P/' "$v3/verdicts/OL.md" >"$t/ol.p"
olf=$(git -C "$r" hash-object -w "$t/ol.p")
git -C "$r" checkout -q -B scratch/pass1 nucleus/v3-scaffold
save_obj "$r" "$olid"
forge_from "$r" "$olid" "$olf"
l_premise() { local o; o=$(git -C "$r" show "card/v3-OL:nucleus/v3/verdicts/OL.md"); echo "$o"; grep -qxF "Gate-tier: P" <<<"$o"; }
expect_ok "premise: (l) plain git show reads Gate-tier: P" l_premise
olv="blob $olid (card/v3-OL:nucleus/v3/verdicts/OL.md) fails its hash"
expect_ok "(l) merge-mode check_merge refuses, naming the blob" out_has "$olv" -- cmv OL nucleus/v3-scaffold card/v3-OL P core
expect_ok "(l) audit-mode check_merge refuses, naming the blob" out_has "$olv" -- bash -c "cd '$v3' && . scripts/core-paths-lib.sh && . scripts/enforce-lib.sh && check_merge OL nucleus/v3-scaffold card/v3-OL audit"
expect_ok "(l) merge-card refuses, naming the blob" out_has "$olv" -- mc OL
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
# wc WANT LEDGER...: worker_commits over $r with base e2e/base, one ledger file
# per LEDGER (its whole text). WANT "none" requires None for every one, "empty"
# requires set(). A raised exception is a mismatch, printed as such.
wc() { PYTHONDONTWRITEBYTECODE=1 python3 - "$v3/tools/run_card.py" "$r" "$t/wc" "$@" <<'P'
import importlib.util, pathlib, sys
spec = importlib.util.spec_from_file_location("rc", sys.argv[1]); rc = importlib.util.module_from_spec(spec); spec.loader.exec_module(rc)
repo, d, want, cases = pathlib.Path(sys.argv[2]), pathlib.Path(sys.argv[3]), sys.argv[4], sys.argv[5:]
d.mkdir(exist_ok=True)
ok = bool(cases)
for i, text in enumerate(cases):
    path = d / f"{i}.ledger.jsonl"
    path.write_text(text + "\n")
    try:
        got = rc.worker_commits(repo, "e2e/base", path)
    except Exception as x:
        got = f"raised {type(x).__name__}: {x}"
    good = got is None if want == "none" else got == set()
    ok = ok and good
    print("ok  " if good else "BAD ", repr(text), "->", repr(got))
sys.exit(0 if ok else 1)
P
}
wo=$(git -C "$r" rev-parse e2e/base)
wcu=$(tr a-f A-F <<<"$wo")
wcx=deadbeefdeadbeefdeadbeefdeadbeefdeadbeef
expect_ok "runner ledger: premise: the uppercase id differs and the made-up id is no object here" \
  bash -c '[ "$1" != "$2" ] && ! git -C "$3" cat-file -e "$4^{commit}"' _ "$wcu" "$wo" "$r" "$wcx"
expect_ok "runner ledger: malformed records trust nothing" wc none \
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
  "{\"run\": \"a\", \"start\": \"$wcx\"}"
expect_ok "runner ledger: control: a well-formed closed range lists an empty set, not None" wc empty \
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
# start and end: the runner trusts nothing and repairs nothing.
flagged_raw() { PYTHONDONTWRITEBYTECODE=1 python3 - "$e/$1" "$e/cards/logs/$1.ledger.jsonl" "$1" <<'P'
import json, pathlib, sys
b, lp, cid = sys.argv[1:4]
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

echo
if [ "$fails" -eq 0 ]; then echo "test-enforcement: all passed"; else echo "test-enforcement: $fails FAILED"; fi
exit $(( fails > 0 ))
