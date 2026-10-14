//! nucleus-catalog: the typed table layer (C-T6 DECISION 2). Table
//! definitions in `RelRow.def` (the C-Q3s/codec §3 bytes), index
//! definitions in `IdxRow.def` (§6), the [`Db`] handle, `open_table` with
//! the C-T0 §4 relation rule, CREATE TABLE, the no-rewrite ALTERs of §3
//! (ADD/DROP COLUMN, SET/DROP NOT NULL — `ddl`, lock first, never
//! rewriting a row; the rewrite-requiring ones are 0A000 naming C-T6r),
//! insert/update/delete with C-T6 §6 index maintenance by state (`index`,
//! driven by the statement-scoped `_with` DML entry points), point get
//! and scan — all on top of `nucleus-txn`, which stays normative for
//! every transactional rule.
//!
//! Not here yet: rewrite-requiring ALTERs (C-T6r), large values (§5
//! chunks) and the online index build ladder (§7, C-T6e) — CREATE INDEX
//! itself is C-T6 §6.1's born-public path ([`Db::create_index`]), and a
//! unique index born DeleteOnly or WriteOnly cannot exist through the
//! SQL layer in 2.0 (§6: the online path never builds unique indexes).
//! amcheck-lite (C-T6 §8: I-ROWFMT, I-IDX/I-UNIQUE, I-IDX-WO, I-IDX-DO
//! at a registered snapshot) is the [`amcheck`] module's checker.

pub mod amcheck;
pub mod db;
pub mod ddl;
pub mod defs;
pub mod error;
mod index;

pub use db::{table_lo, Db, IndexHandle, ScannedRow, Stmt, TableHandle, KEY_LIMIT};
pub use ddl::{refuse_rewrite, RewriteKind};
pub use defs::{IndexColumn, IndexDef, IndexState};
pub use error::DbError;
