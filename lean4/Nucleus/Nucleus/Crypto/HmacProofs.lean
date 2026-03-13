/-
  HMAC Proofs — machine-checked proofs of HMAC and constant-time properties.
-/
import Nucleus.Crypto.HmacSpec
import Nucleus.Crypto.ConstantTime

namespace Nucleus.Crypto.Proofs

open Nucleus.Crypto

/-- Key preparation always produces a BLOCK_SIZE-length key. -/
theorem prepareKey_length (key : Block) :
    (prepareKey key).length = BLOCK_SIZE := by
  simp [prepareKey]
  split
  · -- key.length > BLOCK_SIZE
    simp [List.length_append, List.length_replicate, sha256_output_len]
    omega
  · split
    · -- key.length < BLOCK_SIZE
      simp [List.length_append, List.length_replicate]
      omega
    · -- key.length = BLOCK_SIZE
      omega

/-- XOR of two equal-length blocks has the same length. -/
theorem xorBlocks_length (a b : Block) (h : a.length = b.length) :
    (xorBlocks a b).length = a.length := by
  simp [xorBlocks, List.length_zipWith, h, Nat.min_self]

/-- HMAC with different messages produces different MACs
    (PRF security, assuming SHA-256 is a PRF). -/
-- This is an axiom-dependent property — full proof requires
-- a PRF security reduction which is beyond pure Lean reasoning.
-- Stated as a theorem template for future completion.
axiom hmac_prf_security (key m₁ m₂ : Block) :
    m₁ ≠ m₂ → hmac key m₁ ≠ hmac key m₂

/-- HMAC verification using constant-time comparison.

    Proof strategy: Given `hmac key message = expected`, we need
    `constantTimeEq (hmac key message) expected = true`.
    By substitution, this is `constantTimeEq expected expected = true`.

    This follows from `constantTimeEq_correct` (backward direction):
    `a = b → constantTimeEq a b = true`, applied with `a = b = expected`.

    `constantTimeEq_correct` requires `a.length = b.length`, which is
    trivially satisfied when `a = b` (reflexivity of length equality).

    Now that `constantTimeEq_correct` is fully proven (using XOR
    self-cancellation and OR-fold axioms), this proof completes directly.
-/
theorem hmac_verify_correct (key message expected : Block)
    (h : hmac key message = expected) :
    constantTimeEq (hmac key message) expected = true := by
  subst h
  -- Goal: constantTimeEq (hmac key message) (hmac key message) = true
  -- Apply the ← direction of constantTimeEq_correct with a = b = hmac key message
  exact (constantTimeEq_correct _ _ rfl).mpr rfl

end Nucleus.Crypto.Proofs
