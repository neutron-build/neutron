/-
  Bloom Filter — Aeneas-translated model of the probabilistic membership filter.

  Models the bloom filter from `nucleus/src/storage/lsm.rs`.
  Uses k hash functions over a bit array for O(1) membership testing.
-/

namespace Nucleus.Structures

/-- A bloom filter is a bit array with a number of hash functions. -/
structure BloomFilter where
  bits : List Bool
  numBits : Nat
  numHashes : Nat
  deriving Repr

/-- Abstract hash function pair for double hashing. -/
opaque hashPair : List Byte → Nat × Nat
  where Byte := Nat

/-- Create a bloom filter sized for n keys with given bits per key. -/
def BloomFilter.new (numKeys bitsPerKey : Nat) : BloomFilter :=
  let nBits := (numKeys * bitsPerKey).max 64
  let nHashes := (bitsPerKey * 69 / 100).max 1  -- ≈ bitsPerKey * ln(2)
  { bits := List.replicate nBits false,
    numBits := nBits,
    numHashes := nHashes }

/-- Compute the k bit positions for a key using double hashing. -/
def BloomFilter.positions (bf : BloomFilter) (key : List Nat) : List Nat :=
  let (h1, h2) := hashPair key
  List.range bf.numHashes |>.map fun i =>
    (h1 + i * h2) % bf.numBits

/-- Insert a key into the bloom filter (set bits). -/
def BloomFilter.insert (bf : BloomFilter) (key : List Nat) : BloomFilter :=
  let positions := bf.positions key
  let newBits := positions.foldl (fun bits pos =>
    bits.set pos true) bf.bits
  { bf with bits := newBits }

/-- Query whether a key may be in the filter. -/
def BloomFilter.mayContain (bf : BloomFilter) (key : List Nat) : Bool :=
  let positions := bf.positions key
  positions.all fun pos =>
    match bf.bits.get? pos with
    | some b => b
    | none => false

end Nucleus.Structures
