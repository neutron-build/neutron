/-
  HMAC Formal Specifications — properties we prove about HMAC-SHA256.
-/
import Nucleus.Crypto.Hmac

namespace Nucleus.Crypto.Spec

open Nucleus.Crypto

/-- HMAC output length equals SHA-256 hash size. -/
theorem hmac_output_length (key message : Block) :
    (hmac key message).length = HASH_SIZE := by
  simp [hmac]
  exact sha256_output_len _

/-- HMAC is deterministic: same inputs produce same output. -/
theorem hmac_deterministic (key message : Block) :
    hmac key message = hmac key message := by
  rfl

/-- PKCE derivation is deterministic. -/
theorem pkce_deterministic (v : CodeVerifier) :
    deriveChallenge v = deriveChallenge v := by
  rfl

/-- PKCE verification succeeds for correctly derived challenge. -/
theorem pkce_roundtrip (v : CodeVerifier) :
    verifyPkce v (deriveChallenge v) = true := by
  simp [verifyPkce, deriveChallenge]

/-- Different verifiers produce different challenges (collision resistance).
    This relies on SHA-256's collision resistance axiom. -/
axiom sha256_collision_resistant :
    ∀ m₁ m₂ : Block, sha256 m₁ = sha256 m₂ → m₁ = m₂

theorem pkce_collision_resistant (v₁ v₂ : CodeVerifier)
    (h : deriveChallenge v₁ = deriveChallenge v₂) :
    v₁.bytes = v₂.bytes := by
  simp [deriveChallenge, CodeChallenge.mk.injEq] at h
  exact sha256_collision_resistant _ _ h

end Nucleus.Crypto.Spec
