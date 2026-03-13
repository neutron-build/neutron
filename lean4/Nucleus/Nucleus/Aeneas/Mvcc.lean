/-
  MVCC — Aeneas-translated model of Nucleus's MVCC implementation.

  This models the core types and functions from `nucleus/src/storage/mvcc.rs`,
  translated into Lean 4 for formal verification. In production, Aeneas
  auto-generates this; here we provide the hand-modeled version.
-/

namespace Nucleus.Aeneas

/-- Transaction ID / timestamp. -/
abbrev TxId := Nat

/-- A snapshot represents a point-in-time view of the database. -/
structure Snapshot where
  startTs : TxId
  activeTxns : List TxId
  deriving Repr, BEq

/-- A version of a row, tracking its lifecycle through MVCC. -/
structure RowVersion where
  insertTs : TxId
  commitTs : TxId    -- 0 = uncommitted
  deleteTs : TxId    -- 0 = not deleted
  createdBy : TxId
  deriving Repr, BEq

/-- Core visibility check: is this row version visible to the snapshot? -/
def Snapshot.isVisible (snap : Snapshot) (row : RowVersion) : Bool :=
  -- Uncommitted: invisible
  if row.commitTs == 0 then false
  -- Committed after snapshot: invisible
  else if row.commitTs > snap.startTs then false
  -- Created by an active transaction: invisible
  else if snap.activeTxns.contains row.createdBy then false
  -- Deleted before snapshot: invisible
  else if row.deleteTs > 0 && row.deleteTs ≤ snap.startTs then false
  -- Visible
  else true

/-- A row version is committed if its commit timestamp is non-zero. -/
def RowVersion.isCommitted (row : RowVersion) : Bool :=
  row.commitTs > 0

/-- A row version is deleted if its delete timestamp is non-zero. -/
def RowVersion.isDeleted (row : RowVersion) : Bool :=
  row.deleteTs > 0

end Nucleus.Aeneas
