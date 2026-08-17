#!/usr/bin/env bash
# Audit what the Lean proofs actually rest on.
#
# `lake build` proves the proofs elaborate; it does not prove they prove
# anything. A `theorem` whose body is an `axiom` builds green, and on
# 2026-08-17 two headline results were exactly that — with three of the
# underlying axioms false, which made `False` derivable and every theorem in
# those modules vacuous. This walks every theorem and fails on any dependency
# outside the allow-list in `AxiomAudit.lean`.
#
# One module at a time, deliberately: several roots cannot be imported into a
# single file (`Aeneas.Mvcc` and `Aeneas.Wal` both declare `Aeneas.TxId`), and
# a single-file audit would have had to skip whichever lost the collision.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(dirname "$SCRIPT_DIR")"
LIB="$ROOT/Nucleus"

cd "$LIB"

if ! command -v lake &>/dev/null; then
    echo "Lean 4 / Lake not installed."
    exit 1
fi

# Every root from lakefile.lean. Kept in sync by the check below: a module on
# disk that is not audited here is a module whose proofs nothing looked at.
MODULES=(
  Nucleus.Aeneas.Mvcc
  Nucleus.Aeneas.Btree
  Nucleus.Aeneas.Wal
  Nucleus.Aeneas.Raft
  Nucleus.Spec.MvccSpec
  Nucleus.Spec.BtreeSpec
  Nucleus.Spec.WalSpec
  Nucleus.Spec.RaftSpec
  Nucleus.Proofs.MvccProofs
  Nucleus.Proofs.BtreeProofs
  Nucleus.Proofs.WalProofs
  Nucleus.Proofs.RaftProofs
  Nucleus.Helpers.Tactics
  Nucleus.Helpers.Lemmas
  Nucleus.Crypto.Hmac
  Nucleus.Crypto.Pkce
  Nucleus.Crypto.ConstantTime
  Nucleus.Crypto.HmacSpec
  Nucleus.Crypto.HmacProofs
  Nucleus.Structures.Lru
  Nucleus.Structures.Bloom
  Nucleus.Structures.SlidingWindow
  Nucleus.Structures.LruSpec
  Nucleus.Structures.BloomSpec
  Nucleus.Structures.SlidingWindowSpec
)

# A root listed in lakefile.lean but missing here would be silently unaudited.
lakefile_roots=$(grep -oE '`Nucleus\.[A-Za-z.]+' lakefile.lean | tr -d '`' | sort)
audited=$(printf '%s\n' "${MODULES[@]}" | sort)
if [ "$lakefile_roots" != "$audited" ]; then
    echo "FAIL: the audited module list disagrees with lakefile.lean's roots."
    diff <(echo "$lakefile_roots") <(echo "$audited") || true
    exit 1
fi

# Build first, always. `lake env lean` loads the compiled `.olean`s, not the
# sources — so against a stale build this audits code that is no longer in the
# tree and reports it clean. Found by negative-testing this very script: an
# axiom added on purpose passed, because the module had not been rebuilt.
echo "=== Lean 4: building before audit (an audit of a stale build is not an audit) ==="
lake build

echo "=== Lean 4: axiom audit (${#MODULES[@]} modules) ==="

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
fail=0
total=0

for m in "${MODULES[@]}"; do
    f="$TMP/audit.lean"
    { echo "import $m"; sed '1,/^-- AUDIT BODY$/d' "$SCRIPT_DIR/AxiomAudit.lean"; } > "$f"
    if out=$(lake env lean "$f" 2>&1); then
        n=$(echo "$out" | grep -oE 'AXIOM AUDIT OK: [0-9]+' | grep -oE '[0-9]+' || echo 0)
        total=$((total + n))
        printf '  ok  %-40s %s theorem(s)\n' "$m" "$n"
    else
        echo "  FAIL $m"
        echo "$out" | sed 's/^/       /'
        fail=1
    fi
done

if [ "$fail" -ne 0 ]; then
    echo "=== Axiom audit FAILED — a theorem depends on an axiom that is not on"
    echo "    the allow-list. Either discharge it, or add it to the list in"
    echo "    scripts/AxiomAudit.lean with the reason it is an assumption."
    exit 1
fi

echo "=== Axiom audit passed: $total theorem(s), none on an unlisted axiom ==="
