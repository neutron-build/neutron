# Data Structure Proofs

Machine-checked proofs of data structure properties used across the Neutron ecosystem.

## Built Modules

| File | Source | Properties |
|------|--------|-----------|
| `Lru.lean` | rs/crates/neutron-cache/src/l1.rs | LRU cache model (get, set, del, evict) |
| `LruSpec.lean` | — | Capacity bound, set-get roundtrip, delete removes, no duplicates |
| `Bloom.lean` | nucleus/src/storage/lsm.rs | Bloom filter model (insert, mayContain, positions) |
| `BloomSpec.lean` | — | No false negatives, insert monotonicity, min one hash |
| `SlidingWindow.lean` | rs/crates/neutron/src/rate_limit.rs | Sliding window model (estimate, tick, record) |
| `SlidingWindowSpec.lean` | — | Non-negative estimate, at-max rejects, tick bounded, rollover preserves |

## Planned

- **Ring Buffer** — Capacity invariant, FIFO ordering

## Approach

1. Define the data structure as an inductive type in Lean 4
2. Implement operations as pure functions
3. Prove invariants hold after every operation
4. Use `Nucleus.Helpers.Lemmas` for shared list/set reasoning
5. Mark `sorry` for complex proofs requiring case analysis (to be completed with LeanCopilot)
