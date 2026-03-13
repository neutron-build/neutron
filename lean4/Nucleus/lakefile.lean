import Lake
open Lake DSL

package «Nucleus» where
  leanOptions := #[
    ⟨`autoImplicit, false⟩
  ]

@[default_target]
lean_lib «Nucleus» where
  srcDir := "Nucleus"
  roots := #[
    `Aeneas.Mvcc,
    `Aeneas.Btree,
    `Aeneas.Wal,
    `Aeneas.Raft,
    `Spec.MvccSpec,
    `Spec.BtreeSpec,
    `Spec.WalSpec,
    `Spec.RaftSpec,
    `Proofs.MvccProofs,
    `Proofs.BtreeProofs,
    `Proofs.WalProofs,
    `Proofs.RaftProofs,
    `Helpers.Tactics,
    `Helpers.Lemmas,
    `Crypto.Hmac,
    `Crypto.Pkce,
    `Crypto.ConstantTime,
    `Crypto.HmacSpec,
    `Crypto.HmacProofs,
    `Structures.Lru,
    `Structures.Bloom,
    `Structures.SlidingWindow,
    `Structures.LruSpec,
    `Structures.BloomSpec,
    `Structures.SlidingWindowSpec
  ]

require mathlib from git
  "https://github.com/leanprover-community/mathlib4" @ "master"
