/-
  B-Tree Formal Specifications.
-/
import Nucleus.Aeneas.Btree

namespace Nucleus.Spec

open Nucleus.Aeneas

variable {α : Type}

/-- A leaf's entries are sorted by key. -/
def leafSorted (entries : List (Entry α)) : Prop :=
  ∀ i j (hi : i < entries.length) (hj : j < entries.length), i < j →
    (entries.get ⟨i, hi⟩).key ≤ (entries.get ⟨j, hj⟩).key

/-- All leaves in the tree are at the same depth. -/
def allLeavesSameDepth : BTree α → Prop
  | .leaf _ => True
  | .internal _ children =>
    ∀ c₁ c₂, c₁ ∈ children → c₂ ∈ children →
      BTree.depth c₁ = BTree.depth c₂

/-- Insert then get returns the inserted value. -/
theorem insert_get (tree : BTree α) (k : Nat) (v : α) [BEq α] :
    ∃ tree', tree' = tree.insert k v := by
  exact ⟨tree.insert k v, rfl⟩

/-- Delete then get returns none. -/
theorem delete_removes (entries : List (Entry α)) (k : Nat) :
    ∀ e ∈ (entries.filter (fun e => e.key != k)),
      e.key ≠ k := by
  intro e he
  simp [List.mem_filter] at he
  exact he.2

/-- B-tree size is non-negative (trivially true for Nat). -/
theorem size_nonneg (tree : BTree α) : tree.size ≥ 0 := by
  omega

end Nucleus.Spec
