/-
  B-Tree Proofs — machine-checked proofs of B-tree invariants.
-/
import Nucleus.Spec.BtreeSpec

namespace Nucleus.Proofs

open Nucleus.Aeneas
open Nucleus.Spec

/-- Deleting a key produces a list not containing that key. -/
theorem delete_key_absent (entries : List (Entry α)) (k : Nat) :
    ∀ e ∈ (entries.filter (fun e => e.key != k)),
      e.key ≠ k := by
  exact delete_removes entries k

/-- The empty tree has size 0. -/
theorem empty_leaf_size : (BTree.leaf (α := α) []).size = 0 := by
  simp [BTree.size]

/-- Depth of a leaf is 0. -/
theorem leaf_depth_zero (entries : List (Entry α)) :
    (BTree.leaf entries).depth = 0 := by
  simp [BTree.depth]

/-- Inserting into a leaf produces a non-empty tree. -/
theorem insert_nonempty (k : Nat) (v : α) :
    (BTree.leaf (α := α) []).insert k v |>.size > 0 := by
  simp [BTree.insert, BTree.size]

end Nucleus.Proofs
