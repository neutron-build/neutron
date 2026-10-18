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

card B S standard; branch B nucleus/v3/scripts/gate.sh; verdict B S MERGE standard S
scratch; expect_no "S verdict on a CORE change (scripts/) is refused" mc B
card C P core; branch C nucleus/v3/scripts/extra.sh; verdict C P MERGE core P
scratch; expect_ok "P verdict on a CORE change merges" mc C

card D P standard; branch D nucleus/v3/docs/X.md; verdict D P MERGE standard P
scratch; expect_no "docs/ change by a non-spec card is refused" mc D
card E P spec; branch E nucleus/v3/docs/Y.md; verdict E P MERGE spec P
scratch; expect_ok "docs/ change by a P spec card merges" mc E

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
card G3 S standard; branch G3 nucleus/v3/spikes/g3.txt
echo extra >>"$r/nucleus/v3/spikes/g3b.txt"; git -C "$r" add -A; verdict G3 S MERGE standard S
scratch; expect_no "a verdict commit that also changes another file is refused" mc G3

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
}
sys.exit(0 if checks[case] else 1)
P
}
for c in gate-last no-gate fake-gate gate-middle no-touch pass-real pass-lies pass-rc; do expect_ok "runner: $c" py "$c"; done
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

echo
if [ "$fails" -eq 0 ]; then echo "test-enforcement: all passed"; else echo "test-enforcement: $fails FAILED"; fi
exit $(( fails > 0 ))
