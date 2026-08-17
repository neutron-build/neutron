/-
  Bloom Filter Formal Specifications.

  History worth keeping: until 2026-08-17 this file carried eight `private
  axiom`s, and two of them were not merely unproven — they were **false**.
  `no_false_negatives_core` asserted `(bf.insert key).mayContain key = true`
  for an ARBITRARY `BloomFilter`, and the exported `no_false_negatives`
  theorem was literally that axiom re-exported. Take `bf = ⟨[], 1, 1⟩`: its
  positions are `[0]`, `List.set` on the empty list is a no-op, and
  `mayContain` reads `none` and answers `false`. So the axiom proved `False`,
  which makes every theorem in a module importing it vacuous, and made the
  headline claim a false statement rather than an unproven one.

  The missing ingredient in both cases was well-formedness: `bits.length =
  numBits` and `0 < numBits`, which `BloomFilter.new` establishes and
  `insert` preserves, but which an arbitrary record value does not satisfy.
  Every theorem below now carries it as a hypothesis and every axiom is gone —
  the list/fold lemmas are proved by induction against core Lean 4.
-/
import Nucleus.Structures.Bloom

namespace Nucleus.Structures.Spec

open Nucleus.Structures

/-! ### Well-formedness

    A `BloomFilter` is a plain record, so nothing about the type stops a caller
    constructing one whose bit array is shorter than its declared width. Every
    correctness property here depends on it not being: a position is computed
    `% numBits`, and is only a valid index into `bits` when the two agree.
-/

/-- The bit array matches the declared width, and the width is positive.
    Established by `BloomFilter.new` (`new_bloomWellFormed`) and preserved by
    `BloomFilter.insert` (`insert_bloomWellFormed`). -/
structure BloomWellFormed (bf : BloomFilter) : Prop where
  /-- The bit array is exactly as long as the filter says it is. -/
  bits_length : bf.bits.length = bf.numBits
  /-- A zero-width filter has no valid positions at all; `% 0` is not an index. -/
  numBits_pos : 0 < bf.numBits

/-! ### List/fold lemmas

    `insert` folds `List.set _ true` over the computed positions. These are the
    three facts that fold needs, all proved by induction on the position list.
-/

/-- Setting bits never changes the length of the bit array. -/
theorem foldl_set_length {positions : List Nat} {bits : List Bool} :
    (positions.foldl (fun bs pos => bs.set pos true) bits).length = bits.length := by
  induction positions generalizing bits with
  | nil => rfl
  | cons p ps ih =>
    simp only [List.foldl_cons]
    rw [ih, List.length_set]

/-- Monotonicity: the fold only ever writes `true`, so a bit that is already
    set stays set. Needed both on its own and as the step case of the lemma
    below — once a position's bit is set, later positions must not clear it. -/
theorem foldl_set_preserves_true {positions : List Nat} {bits : List Bool} {pos : Nat}
    (h : bits[pos]? = some true) :
    (positions.foldl (fun bs p => bs.set p true) bits)[pos]? = some true := by
  induction positions generalizing bits with
  | nil => simpa using h
  | cons p ps ih =>
    simp only [List.foldl_cons]
    refine ih ?_
    by_cases hp : p = pos
    · subst hp
      obtain ⟨hb, _⟩ := List.getElem?_eq_some_iff.mp h
      exact List.getElem?_set_self hb
    · rw [List.getElem?_set_ne hp]
      exact h

/-- The core insert property: every position in the fold ends up set. -/
theorem foldl_set_true_get {positions : List Nat} {bits : List Bool} {pos : Nat}
    (h_mem : pos ∈ positions) (h_bound : pos < bits.length) :
    (positions.foldl (fun bs p => bs.set p true) bits)[pos]? = some true := by
  induction positions generalizing bits with
  | nil => cases h_mem
  | cons p ps ih =>
    simp only [List.foldl_cons]
    rcases List.mem_cons.mp h_mem with rfl | hps
    · exact foldl_set_preserves_true (List.getElem?_set_self h_bound)
    · exact ih hps (by rw [List.length_set]; exact h_bound)

/-! ### Well-formedness is established and preserved -/

/-- `BloomFilter.new` produces a well-formed filter: `bits` is a `replicate` of
    exactly `numBits` elements, and the width is at least 64. -/
theorem new_bloomWellFormed (numKeys bitsPerKey : Nat) :
    BloomWellFormed (BloomFilter.new numKeys bitsPerKey) := by
  constructor
  · simp [BloomFilter.new]
  · simp only [BloomFilter.new]
    exact Nat.lt_of_lt_of_le (by decide) (Nat.le_max_right _ _)

/-- `insert` preserves well-formedness: it rewrites bits in place (`List.set`
    never changes length) and leaves `numBits` alone. -/
theorem insert_bloomWellFormed (bf : BloomFilter) (key : List Nat) (h : BloomWellFormed bf) :
    BloomWellFormed (bf.insert key) := by
  constructor
  · simp only [BloomFilter.insert]
    rw [foldl_set_length]
    exact h.bits_length
  · simpa only [BloomFilter.insert] using h.numBits_pos

/-- Every computed position is a valid index into the bit array. Positions are
    `_ % numBits`, so this needs `0 < numBits` for `Nat.mod_lt` and
    `bits.length = numBits` to transport the bound — exactly the two halves of
    `BloomWellFormed`, and exactly what the old axiom silently assumed. -/
theorem positions_lt_length (bf : BloomFilter) (key : List Nat) (h : BloomWellFormed bf) :
    ∀ pos ∈ bf.positions key, pos < bf.bits.length := by
  intro pos hpos
  simp only [BloomFilter.positions, List.mem_map, List.mem_range] at hpos
  obtain ⟨i, _, rfl⟩ := hpos
  rw [h.bits_length]
  exact Nat.mod_lt _ h.numBits_pos

/-! ### Main theorems -/

/-- **No false negatives**: after inserting a key into a well-formed filter,
    `mayContain` returns `true` for that key.

    `insert` sets every bit in `bf.positions key`; `mayContain` reads exactly
    those positions back, because `insert` changes only `bits` and `positions`
    depends only on `numBits`/`numHashes`. Well-formedness is what makes each
    of those positions a real index rather than a read past the end.

    This was an axiom until 2026-08-17 — and a false one, because it claimed
    the property for filters whose bit array is shorter than their width. -/
theorem no_false_negatives (bf : BloomFilter) (key : List Nat) (h : BloomWellFormed bf) :
    (bf.insert key).mayContain key = true := by
  have hpos : (bf.insert key).positions key = bf.positions key := by
    simp [BloomFilter.insert, BloomFilter.positions]
  simp only [BloomFilter.mayContain, hpos, List.all_eq_true]
  intro pos hmem
  have hbound : pos < bf.bits.length := positions_lt_length bf key h pos hmem
  have hset : ((bf.insert key).bits)[pos]? = some true := by
    simp only [BloomFilter.insert]
    exact foldl_set_true_get hmem hbound
  rw [List.get?_eq_getElem?, hset]

/-- Insert only sets bits, never clears them (monotonicity). Independent of
    well-formedness: the fold writes `true` or nothing. -/
theorem insert_monotone (bf : BloomFilter) (key : List Nat) (pos : Nat)
    (h_set : bf.bits[pos]? = some true) :
    (bf.insert key).bits[pos]? = some true := by
  simp only [BloomFilter.insert]
  exact foldl_set_preserves_true h_set

/-- Bloom filter bit count is always numBits. -/
theorem bits_length_invariant (bf : BloomFilter) (key : List Nat) :
    (bf.insert key).numBits = bf.numBits := by
  simp [BloomFilter.insert]

/-- New bloom filter has no bits set.

    Proof: `BloomFilter.new` creates `bits := List.replicate nBits false`, and
    every element of a `replicate n false` is `false`. -/
theorem new_all_false (numKeys bitsPerKey : Nat) :
    let bf := BloomFilter.new numKeys bitsPerKey
    bf.bits.all (· == false) = true := by
  simp only [BloomFilter.new]
  suffices h : ∀ n : Nat, (List.replicate n false).all (· == false) = true by
    exact h _
  intro n
  induction n with
  | zero => simp [List.replicate, List.all]
  | succ k ih =>
    simp [List.replicate_succ, List.all_cons, ih]

/-- Number of hash functions is at least 1. -/
theorem min_one_hash (numKeys bitsPerKey : Nat) :
    (BloomFilter.new numKeys bitsPerKey).numHashes ≥ 1 := by
  simp only [BloomFilter.new]
  exact Nat.le_max_right _ _

end Nucleus.Structures.Spec
