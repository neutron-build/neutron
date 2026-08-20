/-
  LRU Cache Formal Specifications.

  History worth keeping: until 2026-08-17 this file carried nine `private
  axiom`s, and three of them were **false**, not merely unproven.
  `lru_size_le_capacity` and `lru_capacity_pos` asserted the cache invariants
  of an ARBITRARY `LruCache` record — `⟨[e], 0, 0⟩` refutes both — and
  `filter_count_map_replace` claimed at most one entry per key after a
  replacing `map`, which fails whenever the list already held two. Each of the
  three yields `False`, and two exported theorems (`capacity_bound`,
  `no_duplicates`) were false statements for the same reason: they quantified
  over every value of the record type, including ones no constructor produces.

  The fix is the invariant those axioms were reaching for, stated once as
  `LruWellFormed` and actually maintained: `LruCache.new` establishes it and
  `LruCache.set` / `LruCache.del` preserve it. Every axiom is gone; the list
  lemmas the `find_after_*` / `filter_count_*` axioms stood in for are proved
  by induction against core Lean 4.
-/
import Nucleus.Structures.Lru

namespace Nucleus.Structures.Spec

open Nucleus.Structures

variable {α : Type}

/-! ### Well-formedness

    An `LruCache` is a plain record, so a caller can write down a value whose
    entry list is longer than its capacity, whose capacity is zero, or which
    holds one key twice. None is reachable through `new`, `set` or `del`, and
    every property below depends on that.
-/

/-- The three invariants `new` establishes and `set`/`del` preserve. -/
structure LruWellFormed (cache : LruCache α) : Prop where
  /-- The cache holds no more entries than its capacity. -/
  size_le_capacity : cache.entries.length ≤ cache.capacity
  /-- Capacity is at least one, so there is always room for an entry. -/
  capacity_pos : 1 ≤ cache.capacity
  /-- No key appears twice. -/
  no_dup : ∀ k : String, (cache.entries.filter (fun e => e.key == k)).length ≤ 1

/-! ### List lemmas

    These are what the `find_after_*` and `filter_count_*` axioms stood in for.
-/

/-- A selective `map` that rewrites the entries matching `k` changes no key's
    multiplicity: a matching entry becomes `entry` (whose key is also `k`), and
    a non-matching one is untouched.

    This is what the false `filter_count_map_replace` was reaching for: it
    asserted the *bound* (`≤ 1`) where only *preservation* holds, so it was
    true exactly when its input already satisfied the invariant — which is the
    thing being proved. -/
theorem filter_length_map_replace (l : List (CacheEntry α)) (k k' : String)
    (entry : CacheEntry α) (hentry : entry.key = k) :
    ((l.map (fun e => if e.key == k then entry else e)).filter
        (fun e => e.key == k')).length
      = (l.filter (fun e => e.key == k')).length := by
  induction l with
  | nil => rfl
  | cons a t ih =>
    simp only [List.map_cons, List.filter_cons]
    by_cases hak : (a.key == k) = true
    · have hak' : a.key = k := eq_of_beq hak
      rw [if_pos hak, hentry, hak']
      by_cases hkk : (k == k') = true
      · rw [if_pos hkk, if_pos hkk, List.length_cons, List.length_cons, ih]
      · rw [if_neg hkk, if_neg hkk, ih]
    · rw [if_neg hak]
      by_cases hkk : (a.key == k') = true
      · rw [if_pos hkk, if_pos hkk, List.length_cons, List.length_cons, ih]
      · rw [if_neg hkk, if_neg hkk, ih]

/-- `any p = false` means nothing survives the filter. -/
theorem filter_eq_nil_of_any_false {p : CacheEntry α → Bool} {l : List (CacheEntry α)}
    (h : l.any p = false) : l.filter p = [] := by
  induction l with
  | nil => rfl
  | cons a t ih =>
    simp only [List.any_cons, Bool.or_eq_false_iff] at h
    simp [List.filter_cons, h.1, ih h.2]

/-- Erasing an element cannot increase any key's multiplicity. -/
theorem filter_length_eraseIdx_le (p : CacheEntry α → Bool) (l : List (CacheEntry α))
    (i : Nat) :
    ((l.eraseIdx i).filter p).length ≤ (l.filter p).length := by
  induction l generalizing i with
  | nil => simp
  | cons a t ih =>
    cases i with
    | zero =>
      simp only [List.eraseIdx_cons_zero, List.filter_cons]
      split <;> simp
    | succ j =>
      simp only [List.eraseIdx_cons_succ, List.filter_cons]
      split <;> simp [ih j]

/-- Narrowing a filter cannot increase its length. -/
theorem filter_and_length_le_left (p q : CacheEntry α → Bool) (l : List (CacheEntry α)) :
    (l.filter (fun x => p x && q x)).length ≤ (l.filter p).length := by
  induction l with
  | nil => simp
  | cons a t ih =>
    simp only [List.filter_cons]
    by_cases hp : p a = true
    · by_cases hq : q a = true
      · rw [if_pos (by simp [hp, hq]), if_pos hp]
        simpa using ih
      · rw [if_neg (by simp [hq]), if_pos hp]
        simp only [List.length_cons]
        omega
    · rw [if_neg (by simp [hp]), if_neg hp]
      exact ih

/-- Erasing preserves "no entry matches". -/
theorem any_eraseIdx_false {p : CacheEntry α → Bool} {l : List (CacheEntry α)}
    (h : l.any p = false) (i : Nat) : (l.eraseIdx i).any p = false :=
  List.any_eq_false.mpr fun x hx =>
    List.any_eq_false.mp h x (List.mem_of_mem_eraseIdx hx)

/-- Appending to a list where nothing matches: the lookup finds the appended
    entry. -/
theorem find?_concat_of_none {p : CacheEntry α → Bool} {l : List (CacheEntry α)}
    {entry : CacheEntry α} (h : l.any p = false) (hp : p entry = true) :
    (l.concat entry).find? p = some entry := by
  rw [List.concat_eq_append, List.find?_append]
  rw [List.find?_eq_none.mpr (List.any_eq_false.mp h)]
  simp [List.find?_cons, hp]

/-- After the replacing `map`, looking the key up returns the replacement. -/
theorem find?_map_replace {l : List (CacheEntry α)} {k : String} {entry : CacheEntry α}
    (hkey : entry.key = k) (hany : l.any (fun e => e.key == k) = true) :
    (l.map fun e => if e.key == k then entry else e).find?
      (fun e => e.key == k) = some entry := by
  induction l with
  | nil => simp at hany
  | cons a t ih =>
    simp only [List.map_cons, List.find?_cons]
    by_cases hak : (a.key == k) = true
    · rw [if_pos hak]
      simp [hkey]
    · rw [if_neg hak]
      have hrest : t.any (fun e => e.key == k) = true := by
        simp only [List.any_cons, Bool.or_eq_true] at hany
        rcases hany with h | h
        · exact absurd h hak
        · exact h
      simp only [hak, Bool.false_eq_true]
      simpa using ih hrest

/-- `findLru` only ever returns an in-bounds index: it is `findIdx?`, which
    yields an index strictly below the list length. -/
theorem findLru_lt_length (entries : List (CacheEntry α)) (idx : Nat)
    (h : findLru entries = some idx) : idx < entries.length := by
  unfold findLru at h
  cases entries with
  | nil => simp at h
  | cons a t => exact (List.findIdx?_eq_some_iff_findIdx_eq.mp h).1

/-! ### The invariant is established and preserved -/

/-- A new cache is well-formed. -/
theorem new_lruWellFormed (cap : Nat) : LruWellFormed (LruCache.new cap : LruCache α) := by
  refine ⟨by simp [LruCache.new], ?_, by simp [LruCache.new]⟩
  simp only [LruCache.new]
  exact Nat.le_max_right _ _

/-- `set` preserves well-formedness — the induction step the two false
    well-formedness axioms were substituting for. All three branches are
    checked: overwrite (`map`; length and every key's multiplicity unchanged),
    evict (`eraseIdx` then `concat`; length unchanged because `findLru`'s index
    is in bounds), and append (length grows by one, from strictly below
    capacity). -/
theorem set_lruWellFormed (cache : LruCache α) (k : String) (v : α)
    (h : LruWellFormed cache) : LruWellFormed (cache.set k v) := by
  have hkey : ({ key := k, value := v, accessTime := cache.clock + 1 : CacheEntry α }).key = k :=
    rfl
  simp only [LruCache.set]
  by_cases hany : (cache.entries.any fun e => e.key == k) = true
  · rw [if_pos hany]
    refine ⟨?_, h.capacity_pos, fun k' => ?_⟩
    · simpa only [List.length_map] using h.size_le_capacity
    · rw [filter_length_map_replace cache.entries k k' _ hkey]
      exact h.no_dup k'
  · rw [if_neg hany]
    have hnone : (cache.entries.any fun e => e.key == k) = false := by simpa using hany
    by_cases hcap : cache.entries.length ≥ cache.capacity
    · rw [if_pos hcap]
      cases hfind : findLru cache.entries with
      | some idx =>
        have hidx := findLru_lt_length cache.entries idx hfind
        refine ⟨?_, h.capacity_pos, fun k' => ?_⟩
        · simp only [List.length_concat, List.length_eraseIdx, if_pos hidx]
          have := h.size_le_capacity
          omega
        · dsimp only
          rw [List.concat_eq_append, List.filter_append]
          simp only [List.length_append, List.filter_cons, List.filter_nil]
          by_cases hk : (k == k') = true
          · have hk' : k = k' := eq_of_beq hk
            subst hk'
            rw [filter_eq_nil_of_any_false (any_eraseIdx_false hnone idx)]
            simp [hkey]
          · rw [if_neg (by simpa [hkey] using hk)]
            simpa using
              Nat.le_trans (filter_length_eraseIdx_le _ _ idx) (h.no_dup k')
      | none =>
        -- `findLru` answers `none` only for an empty list, so the cache ends
        -- up holding exactly the new entry.
        refine ⟨?_, h.capacity_pos, fun k' => ?_⟩
        · simpa using h.capacity_pos
        · dsimp only
          simp only [List.filter_cons, List.filter_nil]
          split <;> simp
    · rw [if_neg hcap]
      refine ⟨?_, h.capacity_pos, fun k' => ?_⟩
      · simp only [List.length_concat]
        omega
      · rw [List.concat_eq_append, List.filter_append]
        simp only [List.length_append, List.filter_cons, List.filter_nil]
        by_cases hk : (k == k') = true
        · have hk' : k = k' := eq_of_beq hk
          subst hk'
          rw [filter_eq_nil_of_any_false hnone]
          simp [hkey]
        · rw [if_neg (by simpa [hkey] using hk)]
          simpa using h.no_dup k'

/-- `del` preserves well-formedness: it only removes entries. -/
theorem del_lruWellFormed (cache : LruCache α) (k : String) (h : LruWellFormed cache) :
    LruWellFormed (cache.del k) := by
  refine ⟨?_, h.capacity_pos, fun k' => ?_⟩
  · exact Nat.le_trans (List.length_filter_le _ _) h.size_le_capacity
  · simp only [LruCache.del]
    rw [List.filter_filter]
    exact Nat.le_trans (filter_and_length_le_left _ _ _) (h.no_dup k')

/-- Every branch of `set` leaves capacity alone. -/
theorem capacity_preserved (cache : LruCache α) (k : String) (v : α) :
    (cache.set k v).capacity = cache.capacity := by
  simp only [LruCache.set]
  split
  · rfl
  · split
    · split <;> rfl
    · rfl

/-! ### Main theorems -/

/-- Capacity bound: a well-formed cache still fits its capacity after a set.

    This was proved from `lru_size_le_capacity`, which asserted the premise for
    every record value and was therefore false; the statement was false for the
    same reason — `⟨[e,e,e], 0, 0⟩` takes the evict branch and keeps three
    entries against a capacity of zero. -/
theorem capacity_bound (cache : LruCache α) (k : String) (v : α)
    (h : LruWellFormed cache) : (cache.set k v).size ≤ cache.capacity := by
  have hset := set_lruWellFormed cache k v h
  simpa [LruCache.size, capacity_preserved] using hset.size_le_capacity

/-- Get after set returns the inserted value. True for any cache: whichever
    branch `set` takes, the entry it inserts carries `key = k`, and `get`'s
    `find?` reaches it. -/
theorem set_get_same (cache : LruCache α) (k : String) (v : α) [BEq α] :
    (cache.set k v |>.get k).1 = some v := by
  have hkey : ({ key := k, value := v, accessTime := cache.clock + 1 : CacheEntry α }).key = k :=
    rfl
  have hp : (fun e : CacheEntry α => e.key == k)
      { key := k, value := v, accessTime := cache.clock + 1 } = true := by simp
  simp only [LruCache.set]
  by_cases hany : (cache.entries.any fun e => e.key == k) = true
  · rw [if_pos hany]
    simp only [LruCache.get]
    rw [find?_map_replace hkey hany]
  · rw [if_neg hany]
    have hnone : (cache.entries.any fun e => e.key == k) = false := by simpa using hany
    by_cases hcap : cache.entries.length ≥ cache.capacity
    · rw [if_pos hcap]
      cases hfind : findLru cache.entries with
      | some idx =>
        dsimp only
        simp only [LruCache.get]
        rw [find?_concat_of_none (any_eraseIdx_false hnone idx) hp]
      | none =>
        dsimp only
        simp [LruCache.get, List.find?_cons, hkey]
    · rw [if_neg hcap]
      simp only [LruCache.get]
      rw [find?_concat_of_none hnone hp]

/-- Delete removes the key. -/
theorem del_removes (cache : LruCache α) (k : String) :
    ∀ e ∈ (cache.del k).entries, e.key ≠ k := by
  intro e he
  simp only [LruCache.del, List.mem_filter] at he
  simpa using he.2

/-- Empty cache has size zero. -/
theorem new_cache_empty (cap : Nat) :
    (LruCache.new cap : LruCache α).size = 0 := by
  simp [LruCache.new, LruCache.size]

/-- New cache capacity is at least 1. -/
theorem new_cache_min_capacity (cap : Nat) :
    (LruCache.new cap : LruCache α).capacity ≥ 1 := by
  simp only [LruCache.new]
  exact Nat.le_max_right _ _

/-- No duplicate keys after a set on a well-formed cache. -/
theorem no_duplicates (cache : LruCache α) (k : String) (v : α)
    (h : LruWellFormed cache) :
    ((cache.set k v).entries.filter (fun e => e.key == k)).length ≤ 1 :=
  (set_lruWellFormed cache k v h).no_dup k

end Nucleus.Structures.Spec
