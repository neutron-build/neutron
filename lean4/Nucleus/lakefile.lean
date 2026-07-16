import Lake
open Lake DSL

package «Nucleus» where
  leanOptions := #[
    ⟨`autoImplicit, false⟩
  ]

@[default_target]
lean_lib «Nucleus» where
  -- Modules live under `Nucleus/` and are named `Nucleus.*` (matching every
  -- `import Nucleus.…` in the sources), so the source root is the package dir.
  srcDir := "."
  roots := #[
    `Nucleus.Aeneas.Mvcc,
    `Nucleus.Aeneas.Btree,
    `Nucleus.Aeneas.Wal,
    `Nucleus.Aeneas.Raft,
    `Nucleus.Spec.MvccSpec,
    `Nucleus.Spec.BtreeSpec,
    `Nucleus.Spec.WalSpec,
    `Nucleus.Spec.RaftSpec,
    `Nucleus.Proofs.MvccProofs,
    `Nucleus.Proofs.BtreeProofs,
    `Nucleus.Proofs.WalProofs,
    `Nucleus.Proofs.RaftProofs,
    `Nucleus.Helpers.Tactics,
    `Nucleus.Helpers.Lemmas,
    `Nucleus.Crypto.Hmac,
    `Nucleus.Crypto.Pkce,
    `Nucleus.Crypto.ConstantTime,
    `Nucleus.Crypto.HmacSpec,
    `Nucleus.Crypto.HmacProofs,
    `Nucleus.Structures.Lru,
    `Nucleus.Structures.Bloom,
    `Nucleus.Structures.SlidingWindow,
    `Nucleus.Structures.LruSpec,
    `Nucleus.Structures.BloomSpec,
    `Nucleus.Structures.SlidingWindowSpec
  ]

-- No external dependencies: the proofs are self-contained on the Lean 4 core
-- library. (Mathlib was previously declared but never imported — a handful of
-- lemmas that would use it are stated as axioms instead; see Crypto/ConstantTime
-- and Structures/BloomSpec. Depending on `master` also floated the toolchain off
-- the pinned `lean-toolchain`, breaking a reproducible build.)
