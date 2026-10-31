#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
command -v quint >/dev/null || { echo 'quint unavailable' >&2; exit 1; }
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
cat > "$TMP/canary.qnt" <<'QNT'
module canary {
 var n: int
 action init = n' = 0
 action step = n' = n + 1
 val good = n >= 0
 val bad = n == 0
 run positive_test = init.then(step).expect(n == 1)
}
QNT
python3 "$ROOT/../.github/scripts/run_with_timeout.py" quint test --backend=typescript --seed=20261007 --match='positive_test' "$TMP/canary.qnt" > "$TMP/positive" 2>&1 || { cat "$TMP/positive"; exit 1; }
grep -Eq '1 passing' "$TMP/positive" || { cat "$TMP/positive"; exit 1; }
python3 "$ROOT/../.github/scripts/run_with_timeout.py" quint run --backend=typescript --seed=20261007 --max-steps=2 --invariant=good "$TMP/canary.qnt" > "$TMP/good" 2>&1 || { cat "$TMP/good"; exit 1; }
if python3 "$ROOT/../.github/scripts/run_with_timeout.py" quint run --backend=typescript --seed=20261007 --max-steps=2 --invariant=bad "$TMP/canary.qnt" > "$TMP/bad" 2>&1; then
 echo 'False invariant accepted' >&2; exit 1
fi
grep -Eqi 'Invariant violated|Invariant.*violat' "$TMP/bad" || { cat "$TMP/bad"; echo 'Not an invariant rejection' >&2; exit 1; }
if grep -Eqi 'QNT[0-9]+|syntax error|parsing failed|typechecking failed|timeout|not found' "$TMP/bad"; then cat "$TMP/bad"; exit 1; fi
echo 'CANARY PASS: 1 executed positive scenario and expected invariant violation'
