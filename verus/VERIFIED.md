# Verus property registry

No implementation-linked Verus proof has been checked. Commented templates under
`specs/` and `proofs/` are sketches, excluded from the active manifest and counts.

| Property | State | Scope and exclusions |
|---|---|---|
| Pinned ID survives modeled eviction | Active model, unchecked | `checked/model_obligations.rs`; abstract sets, no shipping buffer linkage |
| Conflicting writer is rejected | Active model, unchecked | Executable two-key/two-outcome admission model; excludes complete SSI/conflict graph correctness |
| Exact comparison operation trace and equal-length trace equality | Active model, unchecked | Executable byte accesses, loop accumulator, equality and iteration count for equal lengths; excludes compiler, hardware and wall-clock timing |
| JWT cryptographic correctness | Sketch | No executed proof |
| Session randomness/collision bound | Active conditional model, unchecked | Independent uniform generation over a domain of size N, q draws: collision probability ≤ q(q−1)/(2N), capped at 1. CSPRNG and independence are assumptions requiring separate assurance; finite random IDs are never absolutely unique. Length alone establishes no entropy. |
| MVCC visibility/SSI | Sketch | Commented mirrors, no implementation-linked proof |

`manifest.json` pins release `0.2026.10.04.426d8b0`. `scripts/verify.sh` fails when
the tool is absent, at a different version, when a target is missing, or when
verification fails/reports zero obligations. A passing result is not recorded
until that exact invocation executes. Executable Rust examples in template files
are not Cargo tests and are not included in checked proof counts.

`compare_bytes` maps manually to the equal-length loop of the source-pinned
`nucleus/src/transport/mod.rs::constant_time_eq_token`; its production function is
private. This mapping is not extraction or an implementation refinement. The WAL
adapter calls public production operations, separately from Verus.

The finite collision bound requires independently justified pair-event counts
and event-cover/subadditivity. Uniform independent draws over the named domain
can justify those premises; constant or repeated IDs violate them. The active
conditional inequality and bounded Python enumerations are not a proof that the
shipping generator is uniform, independent or cryptographically secure.

`scripts/canary.py` first verifies the positive target, then requires real proof
rejection for two conflicting commits and early comparison return. Those real
tool controls, the exact function/property/source-hash receipt and the official
pinned-release CI job must run before any state becomes checked.
