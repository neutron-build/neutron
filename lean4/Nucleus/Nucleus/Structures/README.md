# Data Structure Proofs

Machine-checked proofs of data structure properties used across the Neutron ecosystem.

## Built Modules

| File | Source | Properties |
|------|--------|-----------|
| `Lru.lean` | rs/crates/neutron-cache/src/l1.rs | LRU cache model (get, set, del, evict) |
| `LruSpec.lean` | — | Capacity bound, set-get roundtrip, delete removes, no duplicates — the first and last for well-formed caches (`LruWellFormed`) |
| `Bloom.lean` | nucleus/src/storage/lsm.rs | Bloom filter model (insert, mayContain, positions) |
| `BloomSpec.lean` | — | No false negatives (for well-formed filters, `BloomWellFormed`), insert monotonicity, min one hash |
| `SlidingWindow.lean` | rs/crates/neutron/src/rate_limit.rs | Sliding window model (estimate, tick, record) |
| `SlidingWindowSpec.lean` | — | Non-negative estimate, at-max rejects, tick bounded, rollover preserves |

## Planned

- **Ring Buffer** — Capacity invariant, FIFO ordering

## Approach

1. Define the data structure as an inductive type in Lean 4
2. Implement operations as pure functions
3. Prove invariants hold after every operation
4. Use `Nucleus.Helpers.Lemmas` for shared list/set reasoning
5. State the invariant the constructors actually establish, prove they establish
   it and the operations preserve it, and carry it as a hypothesis

Step 5 is not style. Until 2026-08-17 both spec files stated their headline
properties for an *arbitrary record value* of the structure and closed the gap
with axioms — and three of those axioms were false, because a `BloomFilter`
whose bit array is shorter than its declared width, or an `LruCache` holding
more entries than its capacity, is a value the type permits and no constructor
produces. `False` was derivable in both modules. `bash ../../scripts/axioms.sh`
now fails on any theorem resting on an unlisted axiom.

This list previously ended with "Mark `sorry` for complex proofs requiring case
analysis (to be completed with LeanCopilot)". There are zero `sorry` in the
tree and no LeanCopilot in the build; the advice described a plan, not the
practice, and following it would have reintroduced exactly what was cleaned up.
