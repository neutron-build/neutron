/-
  HMAC-SHA256 — Aeneas-translated model of the HMAC construction.

  Models the core HMAC computation from `rs/crates/neutron/src/jwt.rs`.
  HMAC(K, M) = H((K ⊕ opad) ‖ H((K ⊕ ipad) ‖ M))
  where ipad = 0x36 repeated, opad = 0x5c repeated.
-/

namespace Nucleus.Crypto

/-- A byte is a natural number in [0, 256). -/
abbrev Byte := Nat

/-- A block is a list of bytes. -/
abbrev Block := List Byte

/-- SHA-256 block size in bytes. -/
def BLOCK_SIZE : Nat := 64

/-- SHA-256 output size in bytes. -/
def HASH_SIZE : Nat := 32

/-- IPAD constant (0x36 repeated). -/
def ipad : Block := List.replicate BLOCK_SIZE 0x36

/-- OPAD constant (0x5c repeated). -/
def opad : Block := List.replicate BLOCK_SIZE 0x5c

/-- XOR two blocks element-wise. -/
def xorBlocks (a b : Block) : Block :=
  List.zipWith (· ^^^ ·) a b

/-- Abstract hash function (models SHA-256). -/
opaque sha256 : Block → Block

/-- SHA-256 always produces HASH_SIZE bytes. -/
axiom sha256_output_len (m : Block) : (sha256 m).length = HASH_SIZE

/-- SHA-256 is deterministic. -/
axiom sha256_deterministic (m : Block) : sha256 m = sha256 m

/-- Pad or hash the key to BLOCK_SIZE. -/
def prepareKey (key : Block) : Block :=
  if key.length > BLOCK_SIZE then
    -- Key too long: hash it, then pad with zeros
    sha256 key ++ List.replicate (BLOCK_SIZE - HASH_SIZE) 0
  else if key.length < BLOCK_SIZE then
    -- Key too short: pad with zeros
    key ++ List.replicate (BLOCK_SIZE - key.length) 0
  else
    key

/-- HMAC-SHA256 computation. -/
def hmac (key message : Block) : Block :=
  let k := prepareKey key
  let inner := sha256 (xorBlocks k ipad ++ message)
  sha256 (xorBlocks k opad ++ inner)

/-- Constant-time comparison of two byte sequences.
    Models `constant_time_eq` from jwt.rs. -/
def constantTimeEq (a b : Block) : Bool :=
  if a.length ≠ b.length then false
  else
    let diff := List.zipWith (· ^^^ ·) a b
    diff.foldl (· ||| ·) 0 == 0

end Nucleus.Crypto
