#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT/Nucleus"
command -v lake >/dev/null || { echo 'lake unavailable' >&2; exit 1; }
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
cat > "$TMP/Positive.lean" <<'LEAN'
import Nucleus.Spec.BtreeSpec
import Nucleus.Spec.WalSpec
import Nucleus.Spec.RaftSpec
#check Nucleus.Spec.insert_get
#check Nucleus.Spec.wal_flushed_survives
#check Nucleus.Spec.step_down_becomes_follower
#check Nucleus.Spec.insert_get_unrelated
#check Nucleus.Spec.Quorum.positive_election_reachable
#check Nucleus.Spec.Quorum.reachable_election_safety
#eval IO.println "POSITIVE_CONTROL_PASS audited=6"
LEAN
python3 "$ROOT/../.github/scripts/run_with_timeout.py" lake env lean "$TMP/Positive.lean" > "$TMP/positive" 2>&1 || { cat "$TMP/positive"; exit 1; }
grep -q 'POSITIVE_CONTROL_PASS audited=6' "$TMP/positive" || { cat "$TMP/positive"; exit 1; }
cat > "$TMP/Negative.lean" <<'LEAN'
import Nucleus.Spec.BtreeSpec
import Nucleus.Spec.WalSpec
import Nucleus.Spec.RaftSpec
theorem canary_false : False := by trivial
LEAN
if python3 "$ROOT/../.github/scripts/run_with_timeout.py" lake env lean "$TMP/Negative.lean" > "$TMP/negative" 2>&1; then echo 'False theorem accepted' >&2; exit 1; fi
grep -Eq 'tactic.*(trivial|assumption).*failed' "$TMP/negative" || { cat "$TMP/negative"; exit 1; }
grep -q '⊢ False' "$TMP/negative" || { cat "$TMP/negative"; exit 1; }
if grep -Eqi 'unknown module|unknown identifier|file not found|unexpected token|timeout|failed to download' "$TMP/negative"; then cat "$TMP/negative"; exit 1; fi
echo 'CANARY PASS: 6 elaborated theorem references and expected false-theorem rejection'
