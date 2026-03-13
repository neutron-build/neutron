/-
  MVCC Proofs — machine-checked proofs of snapshot isolation properties.
-/
import Nucleus.Spec.MvccSpec
import Nucleus.Helpers.Lemmas

namespace Nucleus.Proofs

open Nucleus.Aeneas
open Nucleus.Spec

/-- If a row is visible, it must be committed (commit_ts > 0). -/
theorem visible_implies_committed (snap : Snapshot) (row : RowVersion)
    (h : snap.isVisible row = true) :
    row.commitTs > 0 := by
  simp [Snapshot.isVisible] at h
  split at h
  · contradiction
  · omega

/-- Visibility is decidable — it either returns true or false. -/
theorem visibility_decidable (snap : Snapshot) (row : RowVersion) :
    snap.isVisible row = true ∨ snap.isVisible row = false := by
  cases h : snap.isVisible row
  · right; rfl
  · left; rfl

/-- Two snapshots at the same time see the same visibility for a row. -/
theorem same_snapshot_same_visibility (s1 s2 : Snapshot) (row : RowVersion)
    (h_eq : s1.startTs = s2.startTs)
    (h_active : s1.activeTxns = s2.activeTxns) :
    s1.isVisible row = s2.isVisible row := by
  simp [Snapshot.isVisible, h_eq, h_active]

end Nucleus.Proofs
