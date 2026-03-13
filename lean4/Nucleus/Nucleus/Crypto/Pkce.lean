/-
  PKCE (RFC 7636) — Aeneas-translated model of Proof Key for Code Exchange.

  Models the PKCE flow from `rs/crates/neutron-oauth/src/pkce.rs`.
  challenge = BASE64URL(SHA256(verifier))
-/

namespace Nucleus.Crypto

/-- A verifier is a 32-byte random string. -/
structure CodeVerifier where
  bytes : List Byte
  deriving Repr, BEq

/-- A challenge derived from a verifier. -/
structure CodeChallenge where
  bytes : List Byte
  deriving Repr, BEq

/-- Derive a challenge from a verifier: SHA256(verifier). -/
def deriveChallenge (v : CodeVerifier) : CodeChallenge :=
  { bytes := sha256 v.bytes }

/-- Verify that a challenge matches a verifier. -/
def verifyPkce (v : CodeVerifier) (c : CodeChallenge) : Bool :=
  (deriveChallenge v).bytes == c.bytes

end Nucleus.Crypto
