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
git -C "$r" checkout -q nucleus/v3-scaffold

echo
if [ "$fails" -eq 0 ]; then echo "test-enforcement: all passed"; else echo "test-enforcement: $fails FAILED"; fi
exit $(( fails > 0 ))
