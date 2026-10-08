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

/-- A recovered record database, keyed by durable LSN. Existing LSNs are not reapplied.
    This abstracts record recovery, not table materialization/transaction visibility. -/
abbrev RecoveredDatabase := LSN → Option WalRecord

def replay (state : RecoveredDatabase) (records : List WalRecord) : RecoveredDatabase :=
  fun lsn => match state lsn with
    | some record => some record
    | none => records.find? (fun record => record.lsn == lsn)

/-- Replaying the same records twice does not change recovered database state. -/
theorem replay_idempotent (state : RecoveredDatabase) (records : List WalRecord) :
    replay (replay state records) records = replay state records := by
  funext lsn
  simp only [replay]
  cases state lsn <;> simp
  cases records.find? (fun record => record.lsn == lsn) <;> simp

/-- Filtering recovery records preserves their sequence order as a subsequence. -/
theorem recovery_order_subsequence (wal : WAL) :
    wal.recoveryRecords.Sublist wal.records := by
  exact List.filter_sublist wal.records

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

namespace Nucleus.Spec
open Nucleus.Aeneas
/-- Table recovery assumptions: strictly increasing LSNs and exactly one terminal
    decision per transaction. Reused transaction IDs and conflicting LSN payloads
    are excluded rather than silently resolved by first-duplicate-wins. -/
def validRecoveryLog (wal : WAL) : Prop :=
  wal.records.Pairwise (fun a b => a.lsn < b.lsn) ∧
  ∀ a ∈ wal.records, ∀ b ∈ wal.records,
    a.txId = b.txId →
    (a.recordType = .commitTx ∨ a.recordType = .abortTx) →
    (b.recordType = .commitTx ∨ b.recordType = .abortTx) → a.lsn = b.lsn

def tableMutations (wal : WAL) : List WalRecord :=
  wal.recoveryRecords.filter (fun r => wal.isCommitted r.txId &&
    (r.recordType == .insert || r.recordType == .update || r.recordType == .delete))

/-- A mutation payload starts with a row key; remaining Nats are the row value.
    This table/key format is a hand model, not the production page-image WAL. -/
def rowRecovery (wal : WAL) (table key : Nat) : Option (List Nat) :=
  match (tableMutations wal).reverse.find? (fun r => r.tableId == table && r.data.head? == some key) with
  | none => none
  | some r => if r.recordType == .delete then none else some r.data.tail

theorem recovered_mutations_committed (wal : WAL) (r : WalRecord)
    (h : r ∈ tableMutations wal) : wal.isCommitted r.txId = true := by
  simp only [tableMutations, List.mem_filter, Bool.and_eq_true] at h
  exact h.2.1

theorem recovered_mutations_durable (wal : WAL) (r : WalRecord)
    (h : r ∈ tableMutations wal) : r.lsn ≤ wal.flushedLsn := by
  have hmem : r ∈ wal.recoveryRecords := (List.mem_filter.mp h).1
  simpa [WAL.recoveryRecords] using (List.mem_filter.mp hmem).2

theorem last_committed_delete_removes (wal : WAL) (table key : Nat) (r : WalRecord)
    (last : (tableMutations wal).reverse.find? (fun r => r.tableId == table && r.data.head? == some key) = some r)
    (deleted : r.recordType = .delete) : rowRecovery wal table key = none := by
  simp [rowRecovery, last, deleted]

theorem last_committed_write_materializes (wal : WAL) (table key : Nat) (r : WalRecord)
    (last : (tableMutations wal).reverse.find? (fun r => r.tableId == table && r.data.head? == some key) = some r)
    (notDeleted : r.recordType ≠ .delete) : rowRecovery wal table key = some r.data.tail := by
  simp [rowRecovery, last, notDeleted]
end Nucleus.Spec
