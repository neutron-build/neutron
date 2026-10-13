//! Typed DDL entry points (C-T6 §3, §6.1, §10). Every DDL here is
//! transactional: the catalog rows go through the §5 write path inside the
//! caller's txn, so an abort or `ROLLBACK TO` leaves no row, no mapped
//! storage and no index entry.
//!
//! [`Db::create_index`] is C-T6 §6.1's **born-public** CREATE INDEX — the
//! only in-transaction build 2.0 has (the online ladder of §7 is later
//! cards' work; a non-empty pre-existing relation inside a transaction is
//! 0A000, §9.1). It never leaves the caller's txn: the entries for the
//! txn's own rows are written through the same statement's write path, so
//! an abort discards index and entries together.
//!
//! The §3 schema-evolution entry points ([`Db::add_column`],
//! [`Db::drop_column`], [`Db::set_not_null`]) are PostgreSQL's
//! no-rewrite ALTERs: every one takes AccessExclusive **before** reading
//! the def, enumerating indexes or scanning (§3, §10), rewrites only the
//! def through the write path, and keeps every storage id. The
//! rewrite-requiring ALTERs (§9.3-§9.4) stay 0A000 through
//! [`refuse_rewrite`] until card C-T6r.

use std::ops::Bound;

use nucleus_codec::row::TableSchema;
use nucleus_codec::{KeyType, Value};
use nucleus_kv::{Key, OrderedKv};

use nucleus_txn::boot::Core;
use nucleus_txn::catalog::{table_range, Catalog, Ddl, IdxStorage, RelKind, RelRow};
use nucleus_txn::commit::SyncCommit;
use nucleus_txn::locks::RelLockMode;
use nucleus_txn::read::{scan, NoSsi};
use nucleus_txn::txn::Isolation;
use nucleus_txn::visibility::ReadCtx;
use nucleus_txn::write::LockWait;
use nucleus_txn::TxnError;

use crate::db::{chunk_prefix, ghost_id, Db, IndexHandle, Stmt, TableHandle};
use crate::defs::{IndexDef, IndexState};
use crate::error::DbError;
use crate::index;

impl<K: OrderedKv> Db<K> {
    /// CREATE TABLE: encode the definition (§3 bytes), then
    /// [`Catalog::catalog_create_table`](nucleus_txn::catalog::Catalog::catalog_create_table)
    /// — AccessExclusive on the fresh oid, the storage range mapped, the
    /// `/sys/catalog/rel/{oid}` row placed through the write path.
    /// Returns `(oid, storage_id)`.
    pub fn create_table(
        &self,
        st: &Stmt<'_>,
        name: &[u8],
        schema: &TableSchema,
    ) -> Result<(u64, u64), DbError> {
        let def = schema.encode()?;
        let ddl = Ddl::new(&self.core, st.txn(), st.ctx());
        self.catalog
            .catalog_create_table(&ddl, name, RelKind::Table, &def)
            .map_err(DbError::Txn)
    }

    // ---- C-T6 §3 schema evolution (lock first, then read/scan) --------

    /// The §4 read ctx of a DDL statement: the latest committed catalog
    /// plus the txn's own earlier statements (`seq < seq0`).
    fn ddl_read_ctx(&self, st: &Stmt<'_>) -> ReadCtx {
        ReadCtx {
            txn: st.txn().id,
            snapshot: self.core.visible_ts(),
            stmt_seq: st.seq0(),
        }
    }

    /// The relation row read under the DDL's AccessExclusive (§3/§10: the
    /// def is only read after the lock): absent → 42P01.
    fn read_rel_under_lock(&self, st: &Stmt<'_>, oid: u64) -> Result<RelRow, DbError> {
        let view = self.core.open_view();
        Catalog::read_rel(&self.core, &view, &self.ddl_read_ctx(st), oid)
            .map_err(DbError::Txn)?
            .ok_or(DbError::UndefinedTable)
    }

    /// §3's AccessExclusive, taken **before** any def read, index
    /// enumeration or scan (the §3/§10 discipline this card pins): a
    /// writer in flight when the DDL arrives is waited out here, and
    /// whatever it commits before the lock is granted is visible to the
    /// reads the DDL makes under the lock.
    fn lock_access_exclusive(&self, st: &Stmt<'_>, oid: u64) -> Result<(), DbError> {
        self.locks
            .lock_relation(
                &self.core,
                st.txn(),
                oid,
                RelLockMode::AccessExclusive,
                LockWait::Block,
                None,
            )
            .map(|_| ())
            .map_err(DbError::Txn)
    }

    /// ALTER TABLE ... ADD COLUMN (C-T6 §3, no rewrite). `default` is an
    /// **already-evaluated non-volatile constant** (§3: evaluated once by
    /// the caller at DDL time and stored as `missing`); the volatile
    /// ones, `serial` and identity columns are refused by the caller
    /// through [`refuse_rewrite`] and never reach here.
    ///
    /// AccessExclusive first, then the def read; `NOT NULL` with **no**
    /// non-NULL default is allowed only if no row is visible at the
    /// latest committed state plus the txn's own writes (the scan runs
    /// under the lock; any row → 23502) — a non-NULL `default` needs no
    /// scan, old rows read it as `missing` (PostgreSQL's fast default).
    /// `NOT NULL` with a default, or plain nullable ADD COLUMN, never
    /// scans. The def rewrite goes through
    /// [`Catalog::catalog_alter_rel_def`]; no row is rewritten (§3 lazy
    /// upgrade), no storage id moves, and the column's attnum is
    /// `ncols + 1` (attnums are never reused).
    pub fn add_column(
        &self,
        st: &Stmt<'_>,
        oid: u64,
        name: &str,
        ty: KeyType,
        not_null: bool,
        default: Option<Value>,
    ) -> Result<(), DbError> {
        self.lock_access_exclusive(st, oid)?;
        let rel = self.read_rel_under_lock(st, oid)?;
        let schema = TableSchema::decode(&rel.def)?;
        // A duplicate live name is 42701 (§3: a dropped column's name may
        // be reused, so only live names collide).
        if schema.columns.iter().any(|c| !c.dropped && c.name == name) {
            return Err(DbError::DuplicateColumn);
        }
        if not_null && default.is_none() {
            let ctx = self.ddl_read_ctx(st);
            if !scan_rows_at(&self.core, rel.storage_id, &ctx)?.is_empty() {
                return Err(DbError::NotNullViolation);
            }
        }
        let next = schema.add_column(name, ty, not_null, default)?;
        let ddl = Ddl::new(&self.core, st.txn(), st.ctx());
        self.catalog
            .catalog_alter_rel_def(&ddl, oid, &next.encode()?)
            .map_err(DbError::Txn)
    }

    /// ALTER TABLE ... DROP COLUMN (C-T6 §3, no rewrite): AccessExclusive
    /// first, then the def read. A primary-key column is refused (0A000,
    /// §9.4); an unknown or already-dropped attnum is 42703. Every index
    /// in **any** state whose def uses the column is dropped in the same
    /// txn through [`Catalog::catalog_drop_index`] (as PostgreSQL does
    /// without CASCADE) — enumerated under the lock, so an index a
    /// concurrent txn committed while this DDL waited for its
    /// AccessExclusive is seen and dropped too — then the def update sets
    /// `dropped` and clears `not_null`. Rows keep their bytes (dropped
    /// attnums decode as skipped); the column's attnum is never reused.
    pub fn drop_column(&self, st: &Stmt<'_>, oid: u64, attnum: u16) -> Result<(), DbError> {
        self.lock_access_exclusive(st, oid)?;
        let rel = self.read_rel_under_lock(st, oid)?;
        let schema = TableSchema::decode(&rel.def)?;
        check_live_attnum(&schema, attnum)?;
        if schema.pk.contains(&attnum) {
            return Err(DbError::FeatureNotSupported(
                "dropping a primary-key column: 0A000 (C-T6 §9.4)",
            ));
        }
        let idxs = {
            let view = self.core.open_view();
            let ctx = self.ddl_read_ctx(st);
            Catalog::scan_idxs_of_rel(&self.core, &view, &ctx, oid).map_err(DbError::Txn)?
        };
        let mut dependent = Vec::new();
        for idx in &idxs {
            let def = IndexDef::decode(&idx.def)?;
            if def.columns.iter().any(|c| c.attnum == attnum) {
                dependent.push(idx.oid);
            }
        }
        let ddl = Ddl::new(&self.core, st.txn(), st.ctx());
        for idx_oid in dependent {
            self.catalog
                .catalog_drop_index(&ddl, idx_oid)
                .map_err(DbError::Txn)?;
        }
        let next = schema.drop_column(attnum)?;
        self.catalog
            .catalog_alter_rel_def(&ddl, oid, &next.encode()?)
            .map_err(DbError::Txn)
    }

    /// ALTER COLUMN ... SET NOT NULL (`not_null = true`) / DROP NOT NULL
    /// (`false`) (C-T6 §3, no rewrite): AccessExclusive first, then the
    /// def read; an unknown or already-dropped attnum is 42703.
    ///
    /// SET NOT NULL is a **validating scan** of the latest committed
    /// state plus the txn's own writes under the lock: any NULL — stored
    /// or a NULL `missing` (the §3 decode rule yields both as `None`) —
    /// is 23502; only then is the flag set. DROP NOT NULL is the def
    /// rewrite alone (§3: no scan). Either way no row is rewritten and
    /// the storage id stays.
    pub fn set_not_null(
        &self,
        st: &Stmt<'_>,
        oid: u64,
        attnum: u16,
        not_null: bool,
    ) -> Result<(), DbError> {
        self.lock_access_exclusive(st, oid)?;
        let rel = self.read_rel_under_lock(st, oid)?;
        let schema = TableSchema::decode(&rel.def)?;
        check_live_attnum(&schema, attnum)?;
        if not_null {
            let Some(pos) = schema.live_columns().position(|c| c.attnum == attnum) else {
                return Err(DbError::Txn(TxnError::Invariant(
                    "set_not_null: the attnum check passed but the column is not live".into(),
                )));
            };
            let ctx = self.ddl_read_ctx(st);
            for (_, bytes) in scan_rows_at(&self.core, rel.storage_id, &ctx)? {
                let live = schema.decode_row(&bytes)?;
                if live[pos].is_none() {
                    return Err(DbError::NotNullViolation);
                }
            }
        }
        let next = schema.set_not_null(attnum, not_null)?;
        let ddl = Ddl::new(&self.core, st.txn(), st.ctx());
        self.catalog
            .catalog_alter_rel_def(&ddl, oid, &next.encode()?)
            .map_err(DbError::Txn)
    }

    /// CREATE INDEX, born `Public` (C-T6 §6.1) — unique or not, inside the
    /// caller's txn, with the entries of the txn's own rows written
    /// through this statement's write path (§5): the row and the entries
    /// commit, abort and roll back to a savepoint together. Returns the
    /// index oid.
    ///
    /// The two §6.1 cases, told apart by reading the relation row at the
    /// latest committed catalog twice — once with the txn's own ctx (its
    /// earlier statements included), once with a ghost ctx (committed
    /// state only, so the txn's own DDL is invisible on that side):
    ///
    /// - **(a) the relation is the txn's own** (own row present, committed
    ///   row absent): the index is created at AccessExclusive — the mode
    ///   the txn's CREATE TABLE already holds to its end — with
    ///   `built_at = 0`, and the own rows are a scan at the statement's
    ///   snapshot with the own ctx. The storage is new and unmapped to
    ///   every other txn, so no committed row can exist in it and any
    ///   snapshot gives the same answer.
    ///
    /// - **(b) a pre-existing relation**: take `lock_relation(Share)`
    ///   **before** anything else — every writer holds RowExclusive, which
    ///   conflicts, so once the Share is granted no txn that has written
    ///   the relation is still in flight and none can start until this txn
    ///   ends — then register the snapshot `S_e`, scan the table at `S_e`
    ///   with the ghost ctx: any committed row visible there → 0A000 (the
    ///   in-block build is C-T6h's, §9.1); empty → the index is created at
    ///   Share with `built_at = S_e`, and the own rows are a scan at `S_e`
    ///   with the own ctx (no committed row is visible at `S_e`, so those
    ///   are exactly the txn's own live writes — a row the txn can still
    ///   see at an older statement snapshot is not its own and must not be
    ///   indexed).
    ///
    /// Either way the index row is placed by
    /// [`Catalog::catalog_create_index_with`] (a fresh storage id from
    /// `alloc_storage_id`, every entry range mapped to the owner) with the
    /// def rewritten to `state = Public`, `built_at` as above and
    /// `concurrently = false` (§7's CONCURRENTLY is the online path's
    /// business), and each own row gets its entry through
    /// [`Core::insert_key`](nucleus_txn::boot::Core::insert_key) — the §6
    /// key limit applies and the unique rule of a `/u/` entry with
    /// `same_row` covers duplicates among the own rows (23505).
    ///
    /// `S_e` stays registered until both `S_e` scans have run (§3.1). An
    /// index born public under case (b) has `built_at = S_e`, which is
    /// above the creating RR/SER txn's own snapshot — its own reads never
    /// use it (§9.1); a case-(a) index has `built_at = 0`.
    pub fn create_index(
        &self,
        st: &Stmt<'_>,
        rel_oid: u64,
        def: &IndexDef,
    ) -> Result<u64, DbError> {
        // §6.1's classification: the latest committed catalog, own writes
        // included on one side and excluded on the other.
        let own = ReadCtx {
            txn: st.txn().id,
            snapshot: self.core.visible_ts(),
            stmt_seq: st.seq0(),
        };
        let ghost = ReadCtx {
            txn: ghost_id(&self.core),
            snapshot: self.core.visible_ts(),
            stmt_seq: 0,
        };
        let own_rel = {
            let view = self.core.open_view();
            Catalog::read_rel(&self.core, &view, &own, rel_oid).map_err(DbError::Txn)?
        };
        let ghost_rel = {
            let view = self.core.open_view();
            Catalog::read_rel(&self.core, &view, &ghost, rel_oid).map_err(DbError::Txn)?
        };

        let (rel, built_at, mode, own_rows) = match (own_rel, ghost_rel) {
            (Some(rel), None) => {
                // (a) the txn's own relation: the committed row is absent,
                // the own row present.
                let ctx = ReadCtx {
                    txn: st.txn().id,
                    snapshot: st.snapshot(),
                    stmt_seq: st.seq0(),
                };
                let rows = scan_rows_at(&self.core, rel.storage_id, &ctx)?;
                (rel, 0, RelLockMode::AccessExclusive, rows)
            }
            (Some(_), Some(_)) => {
                // (b) pre-existing: Share first (§6.1: the emptiness check
                // runs under it — a writer that had the relation when we
                // asked is waited out here, and none can start after), then
                // the relation row re-read under the lock (a DROP that
                // committed while we waited must still be 42P01, a
                // TRUNCATE's new storage the one scanned).
                self.locks
                    .lock_relation(
                        &self.core,
                        st.txn(),
                        rel_oid,
                        RelLockMode::Share,
                        LockWait::Block,
                        None,
                    )
                    .map_err(DbError::Txn)?;
                let rel = {
                    let view = self.core.open_view();
                    Catalog::read_rel(&self.core, &view, &own, rel_oid).map_err(DbError::Txn)?
                };
                let Some(rel) = rel else {
                    return Err(DbError::UndefinedTable);
                };
                let guard = self.core.registry.take_snapshot();
                let s_e = guard.ts();
                let ghost_at = ReadCtx {
                    txn: ghost_id(&self.core),
                    snapshot: s_e,
                    stmt_seq: 0,
                };
                if !scan_rows_at(&self.core, rel.storage_id, &ghost_at)?.is_empty() {
                    return Err(DbError::FeatureNotSupported(
                        "CREATE INDEX on a non-empty relation inside a transaction",
                    ));
                }
                let own_at = ReadCtx {
                    txn: st.txn().id,
                    snapshot: s_e,
                    stmt_seq: st.seq0(),
                };
                let rows = scan_rows_at(&self.core, rel.storage_id, &own_at)?;
                (rel, s_e.0, RelLockMode::Share, rows)
            }
            // Neither read sees a row: the relation does not exist for
            // this statement (a committed row implies the own read sees it
            // too, so both-absent is the only remaining shape).
            (None, _) => return Err(DbError::UndefinedTable),
        };

        let storage_id = self
            .catalog
            .alloc_storage_id(&self.core)
            .map_err(DbError::Txn)?;
        let def = IndexDef {
            state: IndexState::Public,
            built_at,
            concurrently: false,
            ..def.clone()
        };
        let ddl = Ddl::new(&self.core, st.txn(), st.ctx());
        let (idx_oid, _) = self
            .catalog
            .catalog_create_index_with(
                &ddl,
                rel_oid,
                IdxStorage::Id(storage_id),
                &def.encode()?,
                mode,
            )
            .map_err(DbError::Txn)?;

        // The entries of the txn's own rows, through this statement's
        // write path (§6.1: they commit and abort with the index row).
        let schema = TableSchema::decode(&rel.def)?;
        let handle = IndexHandle {
            oid: idx_oid,
            storage_id,
            def,
        };
        let h = TableHandle {
            txn: st.txn().id,
            seq0: st.seq0(),
            lock: mode,
            oid: rel_oid,
            storage_id: rel.storage_id,
            schema: schema.clone(),
            def_version: schema.version,
            indexes: vec![handle.clone()],
        };
        for (row_key, bytes) in own_rows {
            let live = schema.decode_row(&bytes)?;
            let (key, value, rule) = index::entry_for_insert(&h, &handle, &row_key, &live)?;
            self.core
                .insert_key(st.txn(), &key, None, value, st.ctx(), rule)
                .map_err(DbError::Txn)?;
        }
        Ok(idx_oid)
    }
}

/// The §7.2 route for a plain CREATE (UNIQUE) INDEX outside a transaction
/// block: one fresh RC txn that begins, runs [`Db::create_index`] and
/// commits — so a unique index succeeds on an empty pre-existing table
/// (§6.1 case (b) in its own txn) and returns 0A000 on a non-empty one.
/// The commit blocks for its ack, so the core must have a commit pipeline
/// running ([`spawn_commit_thread`](nucleus_txn::commit::spawn_commit_thread)
/// or an equivalent driver); a failure after the entries were placed
/// aborts the txn, leaving no index row and no entry.
pub fn create_index_own_txn<K: OrderedKv>(
    db: &Db<K>,
    rel_oid: u64,
    def: &IndexDef,
) -> Result<u64, DbError> {
    let txn = db.core.begin(Isolation::ReadCommitted);
    let built =
        Stmt::new(&txn, db.core.visible_ts()).and_then(|st| db.create_index(&st, rel_oid, def));
    match built {
        Ok(idx_oid) => db
            .core
            .commit(txn, SyncCommit::On)
            .map(|_| idx_oid)
            .map_err(DbError::Txn),
        Err(e) => {
            db.core.abort(txn).map_err(DbError::Txn)?;
            Err(e)
        }
    }
}

/// The rewrite-requiring ALTERs (C-T6 §9.3-§9.4): each stays 0A000 until
/// card C-T6r implements the rewrite. The SQL layer calls
/// [`refuse_rewrite`] instead of ever handing these to
/// [`Db::add_column`](crate::db::Db::add_column) (whose `default` is an
/// already-evaluated non-volatile constant) or a future
/// `alter_column_type`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RewriteKind {
    /// `ADD COLUMN` with a volatile default (`random()`, `now()`, …): the
    /// value is not a constant, so no `missing` can stand in for it.
    VolatileDefault,
    /// `ADD COLUMN ... serial` (a sequence default plus uniqueness).
    Serial,
    /// `ADD COLUMN ... GENERATED ... AS IDENTITY`.
    Identity,
    /// `ALTER COLUMN ... TYPE` without binary compatibility.
    AlterColumnType,
    /// `ALTER TABLE ... ADD PRIMARY KEY` (§9.4).
    AddPrimaryKey,
}

/// The §9.3/§9.4 refusal: a rewrite-requiring ALTER is 0A000 naming the
/// card that will implement it — never a silent partial behaviour.
pub fn refuse_rewrite(kind: RewriteKind) -> DbError {
    DbError::FeatureNotSupported(match kind {
        RewriteKind::VolatileDefault => "volatile ADD COLUMN default requires a rewrite: C-T6r",
        RewriteKind::Serial => "serial ADD COLUMN requires a rewrite: C-T6r",
        RewriteKind::Identity => "identity ADD COLUMN requires a rewrite: C-T6r",
        RewriteKind::AlterColumnType => "ALTER COLUMN TYPE requires a rewrite: C-T6r",
        RewriteKind::AddPrimaryKey => "ADD PRIMARY KEY requires a rewrite: C-T6r",
    })
}

/// §3's unknown-column rule for the typed ALTERs: an attnum that is
/// unknown or already dropped is 42703 (the codec's own DDL helpers
/// refuse the same with `InvalidDdl`, which is XX000 — the typed layer
/// checks first so the client sees PostgreSQL's SQLSTATE).
fn check_live_attnum(schema: &TableSchema, attnum: u16) -> Result<(), DbError> {
    let live = usize::from(attnum)
        .checked_sub(1)
        .and_then(|i| schema.columns.get(i))
        .is_some_and(|c| !c.dropped);
    if live {
        Ok(())
    } else {
        Err(DbError::UndefinedColumn)
    }
}

/// Every live row of table storage `sid` visible to `ctx`, in key order,
/// through one registered view (§3.1): the §5 `[table_lo, table_lo ‖ 0xFF)`
/// bound of `Db::scan` (§5 — large-value chunks are never rows).
fn scan_rows_at<K: OrderedKv>(
    core: &Core<K>,
    sid: u64,
    ctx: &ReadCtx,
) -> Result<Vec<(Key, Vec<u8>)>, TxnError> {
    let lo = table_range(sid).0;
    let hi = chunk_prefix(&lo);
    let view = core.open_view();
    scan(
        core,
        &view,
        (
            Bound::Included(lo.as_slice()),
            Bound::Excluded(hi.as_slice()),
        ),
        ctx,
        &mut NoSsi,
    )
    .collect::<Result<Vec<_>, _>>()
}
