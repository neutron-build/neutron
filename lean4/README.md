# Neutron — Lean 4 Verification Suite

Machine-checked Lean 4 proofs of the core algorithm designs behind Nucleus: MVCC visibility, B-tree structure, write-ahead logging, Raft consensus, HMAC, Bloom filters, LRU caching, and sliding-window rate limiting.

## What this is

This suite contains **hand-written Lean 4 models** of Nucleus's core algorithms and machine-checked proofs about them. Each algorithm is expressed as a Lean model (its data types and operations), paired with a specification of the properties it should satisfy, and a proof discharging those properties in the Lean kernel.

Concretely:

- **26 `.lean` files**
- **68 theorems**
- **0 `sorry`** — every proof term is complete; nothing is admitted or left as a hole
- **25 `axiom` declarations** — see [Axioms and assumptions](#axioms-and-assumptions)

The proofs are checked by the Lean 4 kernel on `lake build`. "Zero `sorry`" is literally true: there are no gaps stubbed out with `sorry`. The proofs hold *modulo* the explicit axioms listed below.

Algorithms covered:

| Area | Model | What is proven |
|---|---|---|
| MVCC | `Aeneas/Mvcc.lean` | Snapshot visibility rules (committed-only, snapshot isolation) |
| B-tree | `Aeneas/Btree.lean` | Structural / ordering invariants |
| WAL | `Aeneas/Wal.lean` | Write-ahead log ordering and replay |
| Raft | `Aeneas/Raft.lean` | Log-matching / election-safety style properties |
| HMAC | `Crypto/Hmac.lean`, `Crypto/HmacProofs.lean` | Construction correctness (modulo SHA-256 assumptions) |
| Constant-time | `Crypto/ConstantTime.lean` | Bitwise-comparison correctness |
| PKCE | `Crypto/Pkce.lean` | OAuth PKCE challenge/verifier handling |
| Bloom filter | `Structures/Bloom.lean`, `Structures/BloomSpec.lean` | No-false-negatives (modulo a load-bearing axiom) |
| LRU cache | `Structures/Lru.lean`, `Structures/LruSpec.lean` | Capacity bound and eviction ordering (modulo list lemmas) |
| Sliding window | `Structures/SlidingWindow.lean`, `Structures/SlidingWindowSpec.lean` | Rate-limit counting |

## What this is NOT

Read this section before citing the suite anywhere.

- **It does not certify the shipping Nucleus binary.** These are models of the *algorithm designs*, not a verification of the production Rust code. A proof here says the design is sound; it does not say the compiled engine faithfully implements that design.
- **The models are hand-written, not machine-extracted.** The `Aeneas/` directory is named for the aspiration of using the [Aeneas](https://github.com/AeneasVerif/aeneas) Rust-to-Lean toolchain, but the files are hand-modeled. As `Aeneas/Mvcc.lean` states in its own header: *"In production, Aeneas auto-generates this; here we provide the hand-modeled version."* No Aeneas extraction runs in this build. There is no verified link between the Rust source and these Lean models.
- **It is not "100% proven."** Zero `sorry` is real, but 25 axioms carry weight — some are load-bearing (see below). The correct claim is "0 `sorry` across 68 theorems, machine-checked modulo explicit, auditable axioms," not "the algorithm is correct forever" or "the running database is verified."
- **It makes no runtime or query-result guarantees.** Nothing here implies that a given query against Nucleus returns the right row, or that the binary behaves as modeled.

If you need a one-line summary: *machine-checked Lean models of Nucleus's core algorithms — the designs, not the binary — with 0 `sorry` and a documented axiom base.*

## Axioms and assumptions

The 25 axioms fall into three buckets. They are the trust boundary of the suite; everything else is proven from them.

1. **Foundational identities** (`Crypto/ConstantTime.lean`). Bitwise facts such as `n ^^^ n = 0`, `0 ||| n = n`, and `a ^^^ b = 0 → a = b`. These are true and could in principle be replaced by kernel-checked proofs; they are axiomatized here for convenience.

2. **Standard cryptographic assumptions** (`Crypto/HmacSpec.lean`, `Crypto/HmacProofs.lean`, `Crypto/Hmac.lean`). SHA-256 collision resistance, HMAC PRF security, and SHA-256 output length/determinism. These are the usual, unprovable-by-design hardness assumptions any crypto proof stands on. Axiomatizing them is standard practice.

3. **Open structural obligations** (`Structures/BloomSpec.lean`, `Structures/LruSpec.lean`). This is the bucket to be honest about. Some of these list/fold lemmas are genuine proof debt, and at least one **assumes the property it appears to prove**: `no_false_negatives_core` is a `private axiom` that *is* the Bloom no-false-negatives property — the exported theorem re-exports it. `LruSpec` likewise leans on eight list-lemma axioms (`find_after_*`, `filter_count_*`, `lru_size_le_capacity`). Discharging these lemmas is the path to an honestly axiom-clean result.

To audit the axiom footprint of any theorem, use `#print axioms <name>` in Lean.

## Build

Requires the Lean toolchain pinned in `Nucleus/lean-toolchain` (`leanprover/lean4:v4.14.0`) and `mathlib`, both fetched by Lake.

```bash
cd Nucleus
lake exe cache get   # optional: fetch prebuilt mathlib artifacts
lake build           # builds all libraries and kernel-checks every proof
```

A successful `lake build` means the Lean kernel has re-checked all 68 theorems.

## Layout

The Lake project lives under `Nucleus/`; the source tree is `Nucleus/Nucleus/`.

```
lean4/
  README.md                 this file
  Nucleus/
    lakefile.lean           Lake package + mathlib dependency
    lean-toolchain          pinned Lean version
    Nucleus/
      Aeneas/               hand-written models of the core algorithms
        Mvcc.lean  Btree.lean  Wal.lean  Raft.lean
      Spec/                 property specifications for each model
        MvccSpec.lean  BtreeSpec.lean  WalSpec.lean  RaftSpec.lean
      Proofs/               the proofs discharging those specifications
        MvccProofs.lean  BtreeProofs.lean  WalProofs.lean  RaftProofs.lean
      Crypto/               HMAC, constant-time compare, PKCE
        Hmac.lean  HmacSpec.lean  HmacProofs.lean  ConstantTime.lean  Pkce.lean
      Structures/           Bloom filter, LRU cache, sliding-window limiter
        Bloom.lean  BloomSpec.lean  Lru.lean  LruSpec.lean
        SlidingWindow.lean  SlidingWindowSpec.lean
      Helpers/              shared tactics and lemmas
        Tactics.lean  Lemmas.lean
```

Each algorithm follows the same three-file pattern where applicable: a model (the data types and operations), a `Spec` (the properties to hold), and `Proofs` (the kernel-checked derivations).

## License

MIT. See the repository root `LICENSE`.
