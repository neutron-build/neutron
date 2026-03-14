# Verified Functions Registry

> This file tracks which functions have Verus `requires`/`ensures` annotations
> and supporting ghost specifications / proof lemmas.
>
> Verus annotations go **inline** in the `.rs` source files behind `#[cfg(verus_keep_ghost)]`.
> Ghost specs and proof lemmas live in this directory as standalone `.rs` files.

## Status: Phase 1 ANNOTATED (MVCC visibility)

Ghost specifications and proof lemmas are complete. Phase 1 inline annotations
have been added to `nucleus/src/storage/txn.rs` for the MVCC visibility functions.
Annotations are behind commented `#[cfg(verus_keep_ghost)]` blocks — ready for
Verus compilation when the tool is installed.

### Annotated Functions

| Source File | Function | Properties |
|-------------|----------|------------|
| `nucleus/src/storage/txn.rs` | `Snapshot::is_visible()` | Bootstrap always visible, own changes visible, aborted never visible, active never visible, future txns not visible, in-progress-at-snapshot not visible |
| `nucleus/src/storage/txn.rs` | `RowVersion::is_visible()` | Creator visibility propagates, undeleted rows visible if creator visible, deletion by invisible txn doesn't hide row |

---

## Verified Specifications

### Nucleus (specs/nucleus/)

| File | Spec Type | Executable Tests | Properties Specified |
|------|-----------|-----------------|---------------------|
| `buffer_spec.rs` | `PinCounts` ghost struct, `BufferSpec::verify_pin_invariant()` | 0 (invariant is a pure fn) | Pinned pages non-evictable, capacity bounds |
| `mvcc_spec.rs` | `CommittedSet`, `VersionChains` ghost structs, `MvccSpec::visible()` | 5 tests | Snapshot visibility, no dirty reads, no phantom reads, version chain ordering, no-overlap |
| `page_spec.rs` | `AllocatedPages` ghost struct, `PageSpec::verify_conservation()` | 0 (invariant is a pure fn) | No double-free, allocated + free = total conservation |

### Framework (specs/framework/)

| File | Spec Type | Executable Tests | Properties Specified |
|------|-----------|-----------------|---------------------|
| `jwt_spec.rs` | `TimingWitness` ghost struct, `JwtSpec::constant_time_eq()` | 5 tests | XOR accumulation = equality, timing independence (ops = input length), no short-circuit |
| `rate_limit_spec.rs` | `RateLimitSpec` ghost struct, `RateLimitSpec::estimate()` | 5 tests | Sliding window interpolation, no undercount, window start/end behavior |
| `session_spec.rs` | `SessionIdSpec`, `SessionRegistry` ghost structs, `SessionSpec::valid_session_id()` | 2 tests | Minimum entropy (128 bits), session length validation, collision freedom, monotonic registration |

---

## Proof Lemmas

### Nucleus (proofs/nucleus/)

| File | Lemmas | Dependencies |
|------|--------|-------------|
| `buffer_lemmas.rs` | `evict_maintains_invariant`, `pin_increments_one` | `buffer_spec.rs` |
| `mvcc_lemmas.rs` | `visibility_anti_monotonic_delete`, `snapshot_determinism`, `no_write_skew` | `mvcc_spec.rs` |
| `page_lemmas.rs` | `allocate_conserves`, `free_conserves` | `page_spec.rs` |

### Framework (proofs/framework/)

| File | Lemmas | Dependencies |
|------|--------|-------------|
| `jwt_lemmas.rs` | `xor_self_is_zero`, `zero_xor_implies_equal`, `timing_independence`, `constant_time_correctness` | `jwt_spec.rs` |
| `rate_limit_lemmas.rs` | `estimate_monotonic_in_current`, `no_undercount`, `estimate_at_window_start`, `estimate_at_window_end`, `rotation_correctness`, `estimate_upper_bound` | `rate_limit_spec.rs` |
| `session_lemmas.rs` | `collision_resistance`, `registration_monotonic`, `entropy_from_csprng` | `session_spec.rs` |

---

## Verification Targets (for inline annotations)

When inline annotations are added, these functions will get `requires`/`ensures`:

### Nucleus (nucleus/src/)

| File | Function | Properties to Prove | Spec File |
|------|----------|---------------------|-----------|
| `storage/txn.rs` | `Snapshot::is_visible()` | No dirty reads, snapshot isolation, no phantom reads | `mvcc_spec.rs` |
| `storage/txn.rs` | `RowVersion::is_visible()` | Visibility correctness, delete visibility | `mvcc_spec.rs` |
| `storage/buffer.rs` | `pin()` / `unpin()` | Capacity bounds, pinned pages not evictable | `buffer_spec.rs` |
| `storage/page.rs` | `allocate()` / `free()` | No double-free, conservation (allocated + free = total) | `page_spec.rs` |
| `storage/tuple.rs` | `serialize()` / `deserialize()` | Roundtrip correctness | (planned) |

### Rust Framework (rs/crates/)

| File | Function | Properties to Prove | Spec File |
|------|----------|---------------------|-----------|
| `neutron/src/jwt.rs` | `constant_time_eq()` | Timing-attack resistance, correctness | `jwt_spec.rs` |
| `neutron/src/session.rs` | Session ID generation | Uniqueness, entropy ≥ 128 bits | `session_spec.rs` |
| `neutron/src/rate_limit.rs` | Window calculation | No underestimation, bounded estimate | `rate_limit_spec.rs` |
| `neutron-webauthn/src/authentication.rs` | ES256 verification | No false negatives | (planned) |

---

## Architecture

```
verus/
├── specs/           ← Ghost type definitions + executable spec functions
│   ├── nucleus/     ← MVCC visibility, buffer pin counts, page allocation
│   └── framework/   ← JWT constant-time, rate limiting, session IDs
├── proofs/          ← Multi-step proof lemmas (Z3 hints)
│   ├── nucleus/     ← MVCC anti-monotonicity, snapshot determinism, SSI
│   └── framework/   ← XOR accumulation, timing independence, collision resistance
└── scripts/         ← CI and verification runner scripts
```

All spec files contain both:
1. Commented `verus! { }` blocks with ghost types (for when Verus is invoked)
2. Executable Rust code with `#[cfg(test)]` unit tests (for standard `cargo test`)
