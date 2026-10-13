//! nucleus-catalog: the typed table layer (C-T6 DECISION 2). Table
//! definitions in `RelRow.def` (the C-Q3s/codec §3 bytes), index
//! definitions in `IdxRow.def` (§6), the [`Db`] handle, `open_table` with
//! the C-T0 §4 relation rule, CREATE TABLE, insert/update/delete with
//! C-T6 §6 index maintenance by state (`index`, driven by the
//! statement-scoped `_with` DML entry points), point get and scan — all
//! on top of `nucleus-txn`, which stays normative for every
//! transactional rule.
//!
//! Not here yet: CREATE INDEX and index scans (C-T6b2b), ALTER TABLE's
//! validating scans and rewrites (C-T6b3a/C-T6r), large values (§5
//! chunks) — and a unique index born DeleteOnly/WriteOnly cannot exist
//! through the SQL layer in 2.0 (§6: the online path never builds unique
//! indexes).

pub mod db;
pub mod ddl;
pub mod defs;
pub mod error;
mod index;

pub use db::{table_lo, Db, IndexHandle, ScannedRow, Stmt, TableHandle, KEY_LIMIT};
pub use defs::{IndexColumn, IndexDef, IndexState};
pub use error::DbError;
