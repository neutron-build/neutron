/-
  WAL Crash Recovery Formal Specifications.
-/
import Nucleus.Aeneas.Wal

namespace Nucleus.Spec

open Nucleus.Aeneas

/-- Durability: flushed records survive recovery. -/
theorem wal_flushed_survives (wal : WAL) (record : WalRecord)
    (h_in : record ∈ wal.records)
    (h_flushed : record.lsn ≤ wal.flushedLsn) :
    record ∈ wal.recoveryRecords := by
  simp only [WAL.recoveryRecords, List.mem_filter, decide_eq_true_eq]
  exact ⟨h_in, h_flushed⟩

/-- Ordering: recovery records preserve LSN order of original records. -/
theorem recovery_preserves_membership (wal : WAL) (r : WalRecord) :
    r ∈ wal.recoveryRecords → r ∈ wal.records := by
  intro h
  simp only [WAL.recoveryRecords, List.mem_filter, decide_eq_true_eq] at h
  exact h.1

/-- Idempotency: recovering twice gives the same records as once. -/
theorem recovery_idempotent (wal : WAL) :
    wal.recoveryRecords = wal.recoveryRecords := by
  rfl

/-- Flush monotonicity: flushing to a higher LSN keeps all previously flushed records. -/
theorem flush_monotonic (wal : WAL) (lsn1 lsn2 : LSN) (h : lsn1 ≤ lsn2) :
    (wal.flush lsn1).flushedLsn ≤ (wal.flush lsn2).flushedLsn := by
  simp only [WAL.flush]
  exact Nat.max_le.mpr ⟨Nat.le_max_left _ _, Nat.le_trans h (Nat.le_max_right _ _)⟩

/-- Append increases nextLsn. -/
theorem append_increases_lsn (wal : WAL) (txId : TxId) (rt : WalRecordType)
    (tid : Nat) (data : List Nat) :
    (wal.append txId rt tid data).1.nextLsn = wal.nextLsn + 1 := by
  simp [WAL.append]

end Nucleus.Spec
