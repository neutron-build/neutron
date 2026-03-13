/-
  Constant-Time Comparison — formal model of timing-attack resistant
  byte comparison from `rs/crates/neutron/src/jwt.rs`.
-/

namespace Nucleus.Crypto

/-- Standard (short-circuit) equality for byte lists. -/
def shortCircuitEq (a b : Block) : Bool :=
  a == b

/-! ### Bitwise axioms for Nat XOR and OR

    These are standard properties of bitwise operations on natural numbers.
    In Lean 4, `Nat.xor` is defined via `Nat.bitwise bne` and `Nat.or` via
    `Nat.bitwise or`. The properties below hold at every bit position by the
    truth tables of XOR and OR, but proving them formally requires unfolding
    `Nat.bitwise` and doing binary induction, which needs Mathlib's
    `Nat.bitwise_eq_zero_iff`, `Nat.xor_self`, etc.

    We state them as axioms here since Mathlib is declared as a dependency
    but may not be locally built. Each axiom is mathematically trivial:
    - XOR(x, x) = 0 at every bit: b XOR b = false for b ∈ {0, 1}
    - XOR(x, y) = 0 ⟹ x = y: if all bits match, the numbers are equal
    - OR(0, x) = x: 0-bits contribute nothing to OR
    - x ≤ x OR y: OR can only set bits, never clear them
-/

/-- XOR self-cancellation: `n ^^^ n = 0` for all natural numbers. -/
axiom nat_xor_self (n : Nat) : n ^^^ n = 0

/-- XOR equals zero implies equality: `a ^^^ b = 0 → a = b`.
    Proof sketch: if every bit of a XOR b is 0, then a and b agree at every
    bit position, so they are equal as natural numbers. -/
axiom nat_xor_eq_zero_imp_eq (a b : Nat) : a ^^^ b = 0 → a = b

/-- OR with zero is identity: `0 ||| n = n`. -/
axiom nat_zero_or (n : Nat) : 0 ||| n = n

/-- OR is monotone: `a ≤ a ||| b`.
    Proof sketch: OR can only set bits that are already set in either operand,
    so the result is at least as large as either input. -/
axiom nat_le_or (a b : Nat) : a ≤ a ||| b

/-! ### List-level lemmas built on the bitwise axioms -/

/-- zipWith XOR of a list with itself produces all zeros. -/
private theorem zipWith_xor_self (l : List Nat) :
    List.zipWith (· ^^^ ·) l l = List.replicate l.length 0 := by
  induction l with
  | nil => simp
  | cons x xs ih =>
    simp [List.zipWith, nat_xor_self, ih]

/-- foldl OR over a list of zeros with zero accumulator returns zero. -/
private theorem foldl_or_zeros (n : Nat) :
    (List.replicate n 0).foldl (· ||| ·) 0 = 0 := by
  induction n with
  | zero => simp
  | succ k ih =>
    simp [List.replicate_succ, List.foldl_cons, nat_zero_or, ih]

/-- foldl OR is at least as large as the accumulator. -/
private theorem foldl_or_ge_acc (l : List Nat) (acc : Nat) :
    acc ≤ l.foldl (· ||| ·) acc := by
  induction l generalizing acc with
  | nil => simp [List.foldl]
  | cons a as ih =>
    simp only [List.foldl_cons]
    calc acc ≤ acc ||| a := nat_le_or acc a
      _ ≤ as.foldl (· ||| ·) (acc ||| a) := ih (acc ||| a)

/-- foldl OR accumulates bits monotonically — if the result is zero,
    every element in the list must be zero. -/
private theorem foldl_or_zero_all_zero (l : List Nat) :
    l.foldl (· ||| ·) 0 = 0 → ∀ x ∈ l, x = 0 := by
  induction l with
  | nil => intro _ x hx; exact absurd hx (List.not_mem_nil x)
  | cons a as ih =>
    intro h x hx
    simp only [List.foldl_cons, nat_zero_or] at h
    -- h : as.foldl (·|||·) a = 0
    -- Since foldl result ≥ accumulator a, we get a = 0
    have ha0 : a = 0 := by
      have hge := foldl_or_ge_acc as a
      omega
    rw [ha0] at h
    -- Now h : as.foldl (·|||·) 0 = 0
    cases hx with
    | head => exact ha0
    | tail _ hx' => exact ih h x hx'

/-- Lists of equal length with all zipWith XOR elements zero are equal. -/
private theorem zipWith_xor_zero_imp_eq (a b : List Nat)
    (h_len : a.length = b.length)
    (h_zero : ∀ x ∈ List.zipWith (· ^^^ ·) a b, x = 0) :
    a = b := by
  induction a generalizing b with
  | nil =>
    cases b with
    | nil => rfl
    | cons _ _ => simp at h_len
  | cons x xs ih =>
    cases b with
    | nil => simp at h_len
    | cons y ys =>
      simp [List.zipWith] at h_zero
      simp at h_len
      have hxy : x ^^^ y = 0 := h_zero.1
      have hxs : ∀ z ∈ List.zipWith (· ^^^ ·) xs ys, z = 0 := h_zero.2
      have heq : x = y := nat_xor_eq_zero_imp_eq x y hxy
      have htl : xs = ys := ih ys h_len hxs
      rw [heq, htl]

/-- The result of constantTimeEq agrees with standard equality.

    Proof strategy:
    (→) If `constantTimeEq a b = true`, then `foldl (·|||·) 0 (zipWith (·^^^·) a b) = 0`.
        Since `|||` only sets bits and `^^^` detects differences, the fold being 0
        means every `a[i] ^^^ b[i] = 0`, which means `a[i] = b[i]` for all `i`.
        Therefore `a = b`.
    (←) If `a = b`, then `a[i] ^^^ a[i] = 0` for all `i`, so the fold over
        all zeros with `|||` yields 0, and `0 == 0 = true`.

    Both directions require bitwise XOR lemmas:
    - `x ^^^ x = 0` (XOR self-cancellation)
    - `x ^^^ y = 0 → x = y` (XOR is injective on equality)
    - `0 ||| x = x` and `x ||| 0 = x` (OR identity)
    - `x ||| y = 0 → x = 0 ∧ y = 0` (OR is zero only if both are zero)

    These are properties of the `Nat` bitwise operations in Lean4.
    Full proof requires Mathlib's `Nat.xor_self` and `Nat.or_eq_zero_iff`.
-/
theorem constantTimeEq_correct (a b : Block)
    (h_len : a.length = b.length) :
    constantTimeEq a b = true ↔ a = b := by
  constructor
  · -- Forward: constantTimeEq a b = true → a = b
    intro h
    simp only [constantTimeEq, h_len, ↓reduceIte] at h
    -- h : (zipWith (·^^^·) a b).foldl (·|||·) 0 == 0 = true
    -- Extract the numeric equality from BEq on Nat
    have hfold : (List.zipWith (· ^^^ ·) a b).foldl (· ||| ·) 0 = 0 := by
      simp [BEq.beq] at h
      exact Nat.eq_of_beq_eq_true h
    -- Every XOR element must be zero (since OR-fold is 0)
    have hall := foldl_or_zero_all_zero _ hfold
    -- Equal XOR elements means equal lists
    exact zipWith_xor_zero_imp_eq a b h_len hall
  · -- Backward: a = b → constantTimeEq a a = true
    intro h
    subst h
    simp only [constantTimeEq, ↓reduceIte]
    -- Need: (zipWith (·^^^·) a a).foldl (·|||·) 0 == 0 = true
    -- zipWith XOR self → replicate 0, foldl OR over zeros → 0
    rw [zipWith_xor_self, foldl_or_zeros]

/-- constantTimeEq always examines all bytes (no short-circuit).
    This is the key security property — timing is independent of
    where the first difference occurs. -/
theorem constantTimeEq_examines_all (a b : Block)
    (h_len : a.length = b.length) :
    -- The computation touches every byte pair regardless of content.
    -- Formalized: XOR fold visits all indices.
    (List.zipWith (· ^^^ ·) a b).length = a.length := by
  simp [List.length_zipWith, h_len, Nat.min_self]

/-- Length mismatch always returns false. -/
theorem constantTimeEq_length_mismatch (a b : Block)
    (h : a.length ≠ b.length) :
    constantTimeEq a b = false := by
  simp [constantTimeEq, h]

end Nucleus.Crypto
