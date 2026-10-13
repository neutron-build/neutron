//! nucleus-catalog: the typed table layer (C-T6 DECISION 2). Table
//! definitions in `RelRow.def` (the C-Q3s/codec §3 bytes), index
//! definitions in `IdxRow.def` (§6), the [`Db`] handle, `open_table` with
//! the C-T0 §4 relation rule, CREATE TABLE, insert without index
//! maintenance, point get and scan — all on top of `nucleus-txn`, which
//! stays normative for every transactional rule.
//!
//! Not here yet: index maintenance (C-T6b2a), ALTER TABLE's validating
//! scans and rewrites (C-T6b3a/C-T6r), large values (§5 chunks) and index
//! builds (§6.1/§7) — a table with any index refuses DML with 0A000
//! rather than corrupt one.

pub mod db;
pub mod ddl;
pub mod defs;
pub mod error;

pub use db::{table_lo, Db, IndexHandle, ScannedRow, Stmt, TableHandle, KEY_LIMIT};
pub use defs::{IndexColumn, IndexDef, IndexState};
pub use error::DbError;
