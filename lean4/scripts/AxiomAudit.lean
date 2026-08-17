/-
  Axiom audit — the gate behind "these proofs are axiom-clean".

  `lake build` succeeding says every proof *elaborates*. It says nothing about
  what those proofs rest on: a `theorem` whose body is an `axiom` builds
  perfectly, and until 2026-08-17 two of this project's headline results were
  exactly that. Worse, three of the axioms they rested on were **false** —
  `no_false_negatives_core`, `lru_size_le_capacity` and `lru_capacity_pos` all
  asserted properties of arbitrary record values that no constructor can
  produce, so `False` was derivable inside two modules and every theorem in
  them was vacuous.

  This walks every theorem in one module, collects the axioms each actually
  depends on, and fails on anything outside the allow-list below. `sorryAx` is
  not on that list, so an unfinished proof fails here too.

  `scripts/axioms.sh` prepends `import <module>` to everything after the AUDIT
  BODY marker and runs it once per root. Run that, not this file.
-/
import Lean
-- AUDIT BODY
import Lean

open Lean Elab Command in
run_cmd do
  -- Lean's own three, plus the three assumptions this project states
  -- deliberately. Everything else was discharged on 2026-08-17: 25 axioms
  -- became 3. Listing them here means assuming a NEW one is a deliberate edit
  -- to this file rather than something that slips in under a green build.
  let allowed : List Name :=
    [ `propext, `Classical.choice, `Quot.sound,
      -- `sha256` is `opaque`, so its output length is a property of the real
      -- function rather than of this model — assumed, not provable here.
      `Nucleus.Crypto.sha256_output_len,
      -- Hardness assumptions. Not theorems anyone can prove.
      `Nucleus.Crypto.Spec.sha256_collision_resistant,
      `Nucleus.Crypto.Proofs.hmac_prf_security ]
  let env ← getEnv
  let mut checked := 0
  let mut bad : Array (Name × Name) := #[]
  for (n, ci) in env.constants.toList do
    unless (`Nucleus).isPrefixOf n do continue
    if n.isInternal then continue
    unless (match ci with | .thmInfo _ => true | _ => false) do continue
    checked := checked + 1
    for a in ← Lean.collectAxioms n do
      unless allowed.contains a do bad := bad.push (n, a)
  for (n, a) in bad do
    logError m!"{n} depends on unlisted axiom {a}"
  if bad.isEmpty then
    logInfo m!"AXIOM AUDIT OK: {checked} theorems, none depending on an unlisted axiom"
  else
    logError m!"AXIOM AUDIT FAILED: {bad.size} dependency/dependencies outside the allow-list"
