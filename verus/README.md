# Verus

Verus models and planned implementation annotations. No shipping-code assurance
is claimed. See [the property registry](VERIFIED.md) for precise states and scope.

Active model targets live in `checked/` and the pinned `manifest.json`.
`scripts/verify.sh` requires that exact tool release and reports actual checked
obligations; missing tools or malformed/empty results fail the gate.
Commented `verus!` sketches under `specs/` and `proofs/` are excluded.

The abstract pin-preservation, conflict-admission and operation-trace obligations
need execution with the pinned tool before their registry state changes to checked.
No probability proof or Rust implementation linkage is currently certified.
