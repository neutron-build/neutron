/-
  WAL Proofs — machine-checked proofs of crash recovery properties.
-/
import Nucleus.Spec.WalSpec

namespace Nucleus.Proofs

open Nucleus.Aeneas
open Nucleus.Spec

/-- Appending a record increases the WAL's next LSN by exactly 1. -/
theorem append_lsn_increment (wal : WAL) (txId : TxId) (rt : WalRecordType)
    (tid : Nat) (data : List Nat) :
    (wal.append txId rt tid data).1.nextLsn = wal.nextLsn + 1 := by
  exact append_increases_lsn wal txId rt tid data

/-- Recovery records are a subset of all records. -/
theorem recovery_subset (wal : WAL) (r : WalRecord)
    (h : r ∈ wal.recoveryRecords) :
    r ∈ wal.records := by
  exact recovery_preserves_membership wal r h

/-- An empty WAL has no recovery records. -/
theorem empty_wal_no_recovery :
    (WAL.mk [] 0 0).recoveryRecords = [] := by
  simp [WAL.recoveryRecords]

end Nucleus.Proofs
