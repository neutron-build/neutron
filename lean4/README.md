# Neutron — Lean 4 Verification Suite

Machine-checked Lean 4 proofs of the core algorithm designs behind Nucleus: MVCC visibility, B-tree structure, write-ahead logging, Raft consensus, HMAC, Bloom filters, LRU caching, and sliding-window rate limiting.

## What this is

This suite contains **hand-written Lean 4 models** of Nucleus's core algorithms and machine-checked proofs about them. Each algorithm is expressed as a Lean model (its data types and operations), paired with a specification of the properties it should satisfy, and a proof discharging those properties in the Lean kernel.

Exact propositions, assumptions and exclusions are recorded in
[PROPERTY_REGISTRY.md](PROPERTY_REGISTRY.md). The BTree is an unordered first-child
map abstraction. WAL replay proves LSN-keyed record deduplication, and reachable
Raft safety covers a three-node pre-leadership transition subset. Ordered B-tree
implementation correctness and full Raft protocol safety remain unproved.

Source theorem counts are inventory and do not measure assurance or test coverage.

The proofs are checked by the Lean 4 kernel on `lake build`, and `scripts/axioms.sh` then checks what those proofs actually rest on — a `theorem` whose body is an `axiom` builds green, which is how two headline results here sat on false assumptions until 2026-08-17.

Algorithms covered:

| Area | Model | What is proven |
|---|---|---|
| MVCC | `Aeneas/Mvcc.lean` | Snapshot visibility rules (committed-only, snapshot isolation) |
| B-tree | `Aeneas/Btree.lean` | Get-after-insert in an unordered abstraction |
| WAL | `Aeneas/Wal.lean` | Recovery subsequence and LSN-deduplicated record replay |
| Raft | `Aeneas/Raft.lean` | Local lemmas and reachable pre-leadership safety only |
| HMAC | `Crypto/Hmac.lean`, `Crypto/HmacProofs.lean` | Construction correctness (modulo SHA-256 assumptions) |
| Constant-time | `Crypto/ConstantTime.lean` | Bitwise-comparison correctness |
| PKCE | `Crypto/Pkce.lean` | OAuth PKCE challenge/verifier handling |
| Bloom filter | `Structures/Bloom.lean`, `Structures/BloomSpec.lean` | No-false-negatives, for well-formed filters |
| LRU cache | `Structures/Lru.lean`, `Structures/LruSpec.lean` | Capacity bound, no duplicate keys, set/get round-trip, for well-formed caches |
| Sliding window | `Structures/SlidingWindow.lean`, `Structures/SlidingWindowSpec.lean` | Rate-limit counting |

## What this is NOT

Read this section before citing the suite anywhere.

- **It does not certify the shipping Nucleus binary.** These are models of the *algorithm designs*, not a verification of the production Rust code. A proof here says the design is sound; it does not say the compiled engine faithfully implements that design.
- **The models are hand-written, not machine-extracted.** The `Aeneas/` directory is named for the aspiration of using the [Aeneas](https://github.com/AeneasVerif/aeneas) Rust-to-Lean toolchain, but the files are hand-modeled. As `Aeneas/Mvcc.lean` states in its own header: *"In production, Aeneas auto-generates this; here we provide the hand-modeled version."* No Aeneas extraction runs in this build. There is no verified link between the Rust source and these Lean models.
- **It is not "100% proven."** Zero `sorry` is real and, since 2026-08-17, so is "no axioms beyond Lean's own and three stated cryptographic assumptions". The correct claim is "0 `sorry` across the model theorems, machine-checked, resting on 3 declared assumptions" — not "the algorithm is correct forever" and not "the running database is verified." The models are hand-written and are not the shipping binary; that caveat is unchanged and is the important one.
- **It makes no runtime or query-result guarantees.** Nothing here implies that a given query against Nucleus returns the right row, or that the binary behaves as modeled.

If you need a one-line summary: *machine-checked Lean models of Nucleus's core algorithms — the designs, not the binary — with 0 `sorry` and a documented axiom base.*

## Axioms and assumptions

**Three axioms remain**, all of them assumptions about an opaque hash function.
They are the trust boundary of the suite; everything else is proven from them
and from Lean's own `propext` / `Quot.sound` / `Classical.choice`.

| Axiom | Where | Why it is an assumption |
|---|---|---|
| `sha256_output_len` | `Crypto/Hmac.lean` | `sha256` is `opaque`; its output length is a property of the real function, not of this model. |
| `sha256_collision_resistant` | `Crypto/HmacSpec.lean` | A strong global injectivity axiom over the Nat-list model; not bounded cryptographic security. |
| `hmac_prf_security` | `Crypto/HmacProofs.lean` | Likewise. |

`bash scripts/axioms.sh` walks every theorem in every module, collects what it
actually depends on, and **fails on any dependency outside that list** —
including `sorryAx`. It runs in `scripts/ci.sh`.

### What this section used to say, and why it is worth reading

Until 2026-08-17 there were 25 axioms in three buckets, and the third bucket
was described here as "the bucket to be honest about … genuine proof debt".
That description was too kind in one direction and too harsh in another.

**Too harsh:** the four "foundational identities" in `Crypto/ConstantTime.lean`
were axiomatized on the stated grounds that proving them "would need Mathlib".
`Nat.xor_self`, `Nat.zero_or`, `Nat.xor_assoc`, `Nat.testBit_or` and
`Nat.le_of_testBit` are all in the Lean 4 core library this project already
builds against. Each became a one- to three-line proof. `sha256_deterministic`
— `sha256 m = sha256 m` — was an axiom asserting reflexivity, and is `rfl`.

**Too kind:** nobody had asked whether the structural axioms were *true*.
Three were not. `no_false_negatives_core`, `lru_size_le_capacity` and
`lru_capacity_pos` each assert a property of an *arbitrary record value* of
their structure — `⟨[], 1, 1⟩` for a Bloom filter whose bit array is shorter
than its declared width, `⟨[e], 0, 0⟩` for a cache over its capacity. No
constructor produces those values, but the type permits them, so each axiom
proves `False`, and the exported `no_false_negatives`, `capacity_bound` and
`no_duplicates` were false *statements* rather than unproven ones.

The fix in both files was the same: state the invariant the code actually
maintains (`BloomWellFormed`, `LruWellFormed`), prove that the constructors
establish it and the operations preserve it, and carry it as a hypothesis.

Two lessons, both cheap to re-apply elsewhere:

1. **An axiom justified by "the library doesn't have it" silently becomes debt
   the moment the library grows it.** Nothing re-checks the justification, so
   the assumption outlives its reason.
2. **An axiom about a structure type is a claim about every value of that type,
   including the ones no constructor can build.** That is where all three false
   ones came from.

To audit one theorem by hand: `#print axioms <name>` in Lean.

## Build

Requires the Lean toolchain pinned in `Nucleus/lean-toolchain`
(`leanprover/lean4:v4.14.0`). There are **no external dependencies** — the
proofs are self-contained on the Lean 4 core library, and `lakefile.lean` says
why Mathlib was dropped.

```bash
cd Nucleus
lake build           # builds all libraries and kernel-checks every proof
```

A successful `lake build` elaborates the declared Lean proofs. The axiom audit emits module counts and a deduplicated theorem inventory; these are not coverage measures.
It does not mean they rest on nothing: run `bash scripts/axioms.sh` for that
(and `scripts/ci.sh` runs both).

## Layout

The Lake project lives under `Nucleus/`; the source tree is `Nucleus/Nucleus/`.

```
lean4/
  README.md                 this file
  Nucleus/
    lakefile.lean           Lake package (no external dependencies)
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

The crypto assumptions use unbounded Nat-list "bytes". Both the named SHA collision
resistance axiom and the HMAC PRF axiom assert strong algebraic injectivity. They
are not security-game bounds for finite 256-bit outputs and bounded adversaries.
No entropy follows from a token's length. Replacing Nat by a finite byte type while
retaining unrestricted injectivity would change the consistency problem and is
not an authorized proof of real cryptographic security.
