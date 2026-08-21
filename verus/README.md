# Verus

Rust source verification via Verus (Z3/SMT): ghost specifications and proof
lemmas for Nucleus and the Rust web framework.

**Deferred.** This track waits until the target crates are mature. No
verification has been executed — not locally, not in CI — and nothing in any
build depends on this directory.

## What exists

| Path | Contents |
|---|---|
| `specs/nucleus/` | Ghost specs for MVCC snapshot visibility, buffer-pool pin invariants, page allocation conservation |
| `specs/framework/` | Ghost specs for JWT constant-time comparison, sliding-window rate limiting, session IDs |
| `proofs/` | Multi-step proof lemmas (with Z3 hints) for each spec |
| `VERIFIED.md` | Registry: what is annotated, what is specified, planned inline targets |
| `scripts/` | `verify.sh` and `ci.sh` — stale, see below |

Each spec file pairs a commented `verus! { }` block (ghost types, active when
Verus is invoked) with executable Rust and `#[cfg(test)]` unit tests. Phase 1
inline annotations for the MVCC visibility functions sit in
`nucleus/src/storage/txn.rs` behind commented `#[cfg(verus_keep_ghost)]`
blocks, ready for Verus compilation when the toolchain is installed. Planned
further targets: buffer pool `pin`/`unpin` and page `allocate`/`free` in
Nucleus, and JWT/session/rate-limit in `rust/crates/neutron` — see
[VERIFIED.md](VERIFIED.md).

## What does not work yet

- This directory has no `Cargo.toml`. The spec and lemma files are standalone
  and belong to no crate, so their `#[cfg(test)]` tests do not run anywhere.
- `scripts/verify.sh` verifies files under a `verified/` directory that does
  not exist in this tree, and prints success without checking anything when
  the `verus` binary is absent.
- `scripts/ci.sh` runs `cargo test --all` in a directory with no manifest, and
  no CI workflow invokes either script.

The active formal-verification track is the Lean 4 models in
[`../lean4`](../lean4) — hand-written models of the core algorithms, not the
shipping Rust. Verus is the complementary, future track: annotations on the
Rust source itself.

## License

MIT. See the repository root [LICENSE](../LICENSE).
