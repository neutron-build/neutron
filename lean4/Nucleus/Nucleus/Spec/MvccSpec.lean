/-
  MVCC Formal Specifications — the properties we prove about MVCC.
-/
import Nucleus.Aeneas.Mvcc

namespace Nucleus.Spec

open Nucleus.Aeneas

/-- Specification: snapshot isolation — visible rows were committed before snapshot. -/
theorem snapshot_isolation (snap : Snapshot) (row : RowVersion)
    (_h_start : snap.startTs > 0)
    (h_visible : snap.isVisible row = true) :
    row.commitTs > 0 ∧ row.commitTs ≤ snap.startTs := by
  simp [Snapshot.isVisible] at h_visible
  obtain ⟨h_committed, h_before, _⟩ := h_visible
  exact ⟨Nat.pos_of_ne_zero h_committed, h_before⟩

/-- Specification: no dirty reads — uncommitted rows are invisible. -/
theorem no_dirty_reads (snap : Snapshot) (row : RowVersion)
    (h_uncommitted : row.commitTs = 0) :
    snap.isVisible row = false := by
  simp [Snapshot.isVisible, h_uncommitted]

/-- Specification: no phantom reads — future commits are invisible. -/
theorem no_phantom_reads (snap : Snapshot) (row : RowVersion)
    (h_future : row.commitTs > snap.startTs) :
    snap.isVisible row = false := by
  unfold Snapshot.isVisible
  split
  · rfl
  · simp [h_future]

end Nucleus.Spec
