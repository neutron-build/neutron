/-
  WAL (Write-Ahead Log) — Aeneas-translated model.
  Models the core types from `nucleus/src/storage/wal.rs`.
-/

namespace Nucleus.Aeneas

/-- Log Sequence Number — monotonically increasing. -/
abbrev LSN := Nat

/-- Types of WAL records. -/
inductive WalRecordType where
  | insert
  | update
  | delete
  | beginTx
  | commitTx
  | abortTx
  | checkpoint
  deriving Repr, BEq

/-- A single WAL record. -/
structure WalRecord where
  lsn : LSN
  txId : TxId
  recordType : WalRecordType
  tableId : Nat
  data : List Nat  -- simplified payload
  deriving Repr, BEq

/-- The WAL state. -/
structure WAL where
  records : List WalRecord
  nextLsn : LSN
  flushedLsn : LSN
  deriving Repr

/-- Append a record to the WAL. -/
def WAL.append (wal : WAL) (txId : TxId) (rt : WalRecordType)
    (tableId : Nat) (data : List Nat) : WAL × LSN :=
  let record : WalRecord := {
    lsn := wal.nextLsn,
    txId := txId,
    recordType := rt,
    tableId := tableId,
    data := data,
  }
  ({ wal with
    records := wal.records ++ [record],
    nextLsn := wal.nextLsn + 1 }, wal.nextLsn)

/-- Flush WAL to disk up to a given LSN. -/
def WAL.flush (wal : WAL) (upToLsn : LSN) : WAL :=
  { wal with flushedLsn := max wal.flushedLsn upToLsn }

/-- Recover: replay all flushed records for a given transaction. -/
def WAL.recoverTx (wal : WAL) (txId : TxId) : List WalRecord :=
  wal.records.filter (fun r => r.txId == txId && r.lsn ≤ wal.flushedLsn)

/-- Check if a transaction is committed in the WAL. -/
def WAL.isCommitted (wal : WAL) (txId : TxId) : Bool :=
  wal.records.any (fun r =>
    r.txId == txId && r.recordType == .commitTx && r.lsn ≤ wal.flushedLsn)

/-- Get all records that need replay during recovery. -/
def WAL.recoveryRecords (wal : WAL) : List WalRecord :=
  wal.records.filter (fun r => r.lsn ≤ wal.flushedLsn)

end Nucleus.Aeneas
