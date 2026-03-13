/-
  MVCC Formal Specifications — the properties we prove about MVCC.
-/
import Nucleus.Aeneas.Mvcc

namespace Nucleus.Spec

open Nucleus.Aeneas

/-- Specification: snapshot isolation — visible rows were committed before snapshot. -/
theorem snapshot_isolation (snap : Snapshot) (row : RowVersion)
    (h_start : snap.startTs > 0)
    (h_visible : snap.isVisible row = true) :
    row.commitTs > 0 ∧ row.commitTs ≤ snap.startTs := by
  simp [Snapshot.isVisible] at h_visible
  split at h_visible <;> simp_all
  split at h_visible <;> simp_all
  split at h_visible <;> simp_all
  split at h_visible <;> simp_all
  omega

/-- Specification: no dirty reads — uncommitted rows are invisible. -/
theorem no_dirty_reads (snap : Snapshot) (row : RowVersion)
    (h_uncommitted : row.commitTs = 0) :
    snap.isVisible row = false := by
  simp [Snapshot.isVisible, h_uncommitted]

/-- Specification: no phantom reads — future commits are invisible. -/
theorem no_phantom_reads (snap : Snapshot) (row : RowVersion)
    (h_future : row.commitTs > snap.startTs) :
    snap.isVisible row = false := by
  simp [Snapshot.isVisible]
  split
  · rfl
  · split
    · rfl
    · omega

end Nucleus.Spec
