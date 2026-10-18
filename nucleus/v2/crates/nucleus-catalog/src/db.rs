//! The typed table layer over `nucleus-txn` (C-T6 §2-§4): the [`Db`]
//! handle, one statement's position ([`Stmt`], the §3 ordinal counter),
//! `open_table` with the §4 relation rule, and the DML/reads — insert
//! (hidden-rowid keys, the §3 2704-byte key limit), update and delete
//! (row-level EPQ, PK changes through `update_pk`), point get and scan
//! (§5's `[table_lo, table_lo ‖ 0xFF)` bound, toast chunks excluded).
//! Index maintenance is C-T6 §6's, driven by the statement-scoped entry
//! points (`insert_with` and the `insert_by_oid`/`update`/`delete` that
//! open their own handle): every index of the statement's handle is
//! maintained according to its state, with entries computed from the row
//! version the row op acted on (its EPQ base) — see C-T6 §7.3 for why a
//! stale handle is legal. The deprecated legacy [`Db::insert`] (through
//! a possibly cross-statement handle) keeps refusing indexed tables with
//! 0A000: its handle may predate the index, so it never maintains.
//!
//! The caller owns snapshot registration (§3.1): an RR/SER txn's snapshot
//! must have been taken (and stay registered) before its statements run —
//! [`Ssi::begin`](nucleus_txn::ssi::Ssi::begin) for SERIALIZABLE,
//! [`Registry::take_snapshot`](nucleus_txn::registry::Registry::take_snapshot)
//! otherwise; RC re-registers per statement. Every KV read here opens its
//! own registered view.

use std::cell::Cell;
use std::ops::Bound;
use std::sync::Arc;

use nucleus_codec::row::{value_repr_eq, Datum, RowError, TableSchema};
use nucleus_codec::{decode_key_prefix, encode_key, KeyColumn, KeyType, Value};
use nucleus_kv::{Key, OrderedKv};
use nucleus_txn::boot::Core;
use nucleus_txn::catalog::{index_ranges, table_range, Catalog, Ddl, IdxStorage};
use nucleus_txn::commit::SyncCommit;
use nucleus_txn::encoding::VersionValue;
use nucleus_txn::locks::{LockManager, RelLockMode};
use nucleus_txn::read::{read_key, scan, NoSsi};
use nucleus_txn::ssi::Ssi;
use nucleus_txn::txn::{Isolation, Txn};
use nucleus_txn::visibility::ReadCtx;
use nucleus_txn::write::{
    CommittedVersion, Epq, EpqDecision, LockWait, RowOp, RowOutcome, StmtCtx, UniqueRule,
};
use nucleus_txn::{Seq, Ts, TxnError, TxnId};

use crate::defs::{IndexDef, IndexState};
use crate::error::DbError;
use crate::index::{self, IndexRow};
use crate::toast;

/// The encoded primary key is limited to this many bytes (§3); the insert
/// that exceeds it fails with 54000 ([`DbError::ProgramLimitExceeded`]).
pub const KEY_LIMIT: usize = 2704;

/// A table opened by [`Db::open_table`]: the catalog as this statement sees
/// it (§4: the latest committed catalog plus the txn's own earlier
/// statements), pinned for the statement's DML and reads.
///
/// **Handle reuse:** a `TableHandle` is single-statement — the `_with` DML
/// entry points accept it only in the `(txn, seq0)` statement whose
/// `open_table` made it (a handle from another txn or a later statement is
/// `TxnError::Invariant`, and a handle older than a concurrent index-state
/// commit is the legal stale statement of C-T6 §7.3, never an error).
#[derive(Debug, Clone)]
pub struct TableHandle {
    /// The txn that opened the table (its rowids are drawn from it).
    pub txn: TxnId,
    pub seq0: Seq,
    /// The relation-lock mode the statement holds (§4).
    pub lock: RelLockMode,
    pub oid: u64,
    pub storage_id: u64,
    /// The decoded `RelRow.def` (the **latest** definition: rows decode
    /// under it, §4 "a newer def.version is not an error").
    pub schema: TableSchema,
    /// `schema.version`, named separately for call sites that only need it.
    pub def_version: u32,
    /// The relation's indexes at the same catalog read.
    pub indexes: Vec<IndexHandle>,
}

/// One index of an opened table.
#[derive(Debug, Clone)]
pub struct IndexHandle {
    pub oid: u64,
    pub storage_id: u64,
    pub def: IndexDef,
}

/// One scanned row: its logical key and its decoded live datums (§2
/// order).
pub type ScannedRow = (Key, Vec<Option<Datum>>);

/// The typed `Db` handle (DECISION 2): the parts a statement drives — the
/// core, the SSI state, the lock manager and the catalog — over any
/// `OrderedKv`. Build one from parts that were installed together
/// ([`Db::new`]); the commit pipeline stays the caller's.
pub struct Db<K: OrderedKv> {
    pub core: Arc<Core<K>>,
    pub ssi: Arc<Ssi>,
    pub locks: Arc<LockManager>,
    pub catalog: Catalog,
}

impl<K: OrderedKv> Db<K> {
    /// Assembles the handle. The `ssi` must be the one installed on
    /// `core` (`Catalog::open` checks it), and `locks`/`catalog` the ones
    /// opened against the same core.
    pub fn new(
        core: Arc<Core<K>>,
        ssi: Arc<Ssi>,
        locks: Arc<LockManager>,
        catalog: Catalog,
    ) -> Db<K> {
        Db {
            core,
            ssi,
            locks,
            catalog,
        }
    }

    /// Opens the handle the way a boot does (C-T6 §7, §9.5): assembles it
    /// as [`Db::new`] and runs [`Db::cleanup_unfinished_builds`]
    /// **before returning it** — every non-`Public` index whose def says a
    /// plain (non-`CONCURRENTLY`) build is dropped, so no statement can
    /// see a crashed plain build's half-built index. A CONCURRENTLY
    /// build's non-public index is left (§7 Failure) for
    /// [`resume_build`](crate::build::resume_build) or DROP INDEX.
    ///
    /// The core must have a commit pipeline running (or be driven by
    /// one): the cleanup commits each drop and waits for its ack.
    pub fn open(
        core: Arc<Core<K>>,
        ssi: Arc<Ssi>,
        locks: Arc<LockManager>,
        catalog: Catalog,
    ) -> Result<Db<K>, DbError> {
        let db = Db::new(core, ssi, locks, catalog);
        db.cleanup_unfinished_builds()?;
        Ok(db)
    }

    /// Drops every non-`Public` index of the committed catalog whose def
    /// has `concurrently == false`, each in its own RC txn (§7 Failure:
    /// a plain build that crashed leaves its index to the next open, not
    /// to the crash). A drop that finds the row already gone is the
    /// no-op `catalog_drop_index` makes it (a concurrent DROP INDEX won
    /// the race); any other failure fails the open.
    pub fn cleanup_unfinished_builds(&self) -> Result<(), DbError> {
        // The committed catalog, read through a ghost ctx at visible_ts:
        // at open nothing else runs.
        let ctx = ReadCtx {
            txn: ghost_id(&self.core),
            snapshot: self.core.visible_ts(),
            stmt_seq: 0,
        };
        let rels = {
            let view = self.core.open_view();
            Catalog::scan_rels(&self.core, &view, &ctx).map_err(DbError::Txn)?
        };
        let mut doomed: Vec<u64> = Vec::new();
        for (oid, _) in rels {
            let idxs = {
                let view = self.core.open_view();
                Catalog::scan_idxs_of_rel(&self.core, &view, &ctx, oid).map_err(DbError::Txn)?
            };
            for idx in idxs {
                let def = IndexDef::decode(&idx.def)?;
                if def.state != IndexState::Public && !def.concurrently {
                    doomed.push(idx.oid);
                }
            }
        }
        for idx_oid in doomed {
            let txn = self.core.begin(Isolation::ReadCommitted);
            let body = (|| -> Result<(), DbError> {
                let st = Stmt::new(&txn, self.core.visible_ts())?;
                let ddl = Ddl::new(&self.core, &txn, st.ctx());
                self.catalog
                    .catalog_drop_index(&ddl, idx_oid)
                    .map_err(DbError::Txn)?;
                Ok(())
            })();
            match body {
                Ok(()) => {
                    self.core
                        .commit(txn, SyncCommit::On)
                        .map_err(DbError::Txn)?;
                }
                Err(e) => {
                    self.core.abort(txn).map_err(DbError::Txn)?;
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    /// §4's statement entry: take the relation lock in `mode` (a conflict
    /// parks through the §6 wait protocol), then read the relation and
    /// index rows at the **latest committed catalog plus this txn's earlier
    /// statements** (own intents with `seq < seq0`).
    ///
    /// **RR/SER relation rule** (§4, C-T6 §4): the relation row at the
    /// txn's snapshot `S` and the latest **committed** row are both read
    /// through a ghost reader (a `TxnId` this process never allocates), so
    /// the txn's own writes are ignored on both sides — a storage id
    /// installed by the txn's own DDL can never differ from itself, while
    /// a foreign TRUNCATE or rewrite committed after `S` can only show in
    /// the latest read. Present at `S` with a different storage id →
    /// 40001. Absent at `S` → proceed (reads empty, as in PostgreSQL);
    /// absent at the latest read → 42P01.
    pub fn open_table(
        &self,
        st: &Stmt<'_>,
        oid: u64,
        mode: RelLockMode,
    ) -> Result<TableHandle, DbError> {
        self.locks
            .lock_relation(&self.core, st.txn, oid, mode, LockWait::Block, None)
            .map_err(DbError::Txn)?;
        // §4: the statement's catalog read — latest committed state, own
        // earlier statements included (own intents at seq < seq0).
        let own = ReadCtx {
            txn: st.txn.id,
            snapshot: self.core.visible_ts(),
            stmt_seq: st.seq0,
        };
        let rel = {
            let view = self.core.open_view();
            Catalog::read_rel(&self.core, &view, &own, oid).map_err(DbError::Txn)?
        };
        let Some(rel) = rel else {
            return Err(DbError::UndefinedTable);
        };
        if matches!(
            st.txn.isolation,
            Isolation::RepeatableRead | Isolation::Serializable
        ) {
            let ghost = |snapshot: Ts| ReadCtx {
                txn: ghost_id(&self.core),
                snapshot,
                stmt_seq: 0,
            };
            let at_s = {
                let view = self.core.open_view();
                Catalog::read_rel(&self.core, &view, &ghost(st.snapshot), oid)
                    .map_err(DbError::Txn)?
            };
            let latest = {
                let view = self.core.open_view();
                Catalog::read_rel(&self.core, &view, &ghost(self.core.visible_ts()), oid)
                    .map_err(DbError::Txn)?
            };
            if let (Some(at_s), Some(latest)) = (at_s, latest) {
                if at_s.storage_id != latest.storage_id {
                    return Err(DbError::Txn(TxnError::SerializationFailure));
                }
            }
            // Absent at S → proceed (§4); absent at the latest committed
            // state can only follow an own DROP that the own read above
            // already reported. Never an error here.
        }
        let idxs = {
            let view = self.core.open_view();
            Catalog::scan_idxs_of_rel(&self.core, &view, &own, oid).map_err(DbError::Txn)?
        };
        let indexes = index_handles(idxs)?;
        let schema = TableSchema::decode(&rel.def)?;
        let def_version = schema.version;
        Ok(TableHandle {
            txn: st.txn.id,
            seq0: st.seq0,
            lock: mode,
            oid,
            storage_id: rel.storage_id,
            schema,
            def_version,
            indexes,
        })
    }

    // ---- DML and reads (§4: the statement's relation lock first) --------

    /// INSERT by relation oid (§5/§6): `open_table(RowExclusive)` then
    /// [`Db::insert_with`] — the symmetric sibling of [`Db::update`] and
    /// [`Db::delete`]: the statement opens its own handle, so its
    /// catalog read (made under the RowExclusive it holds, §4) is exactly
    /// the index list it maintains — every WriteOnly/Public index of it
    /// gets the new row's entry, a DeleteOnly index gets nothing (§6).
    /// Hidden-rowid tables draw `ord` from the statement's counter
    /// ([`Stmt::next_ord`]), so two inserts of one statement can never
    /// collide.
    pub fn insert_by_oid(
        &self,
        st: &Stmt<'_>,
        oid: u64,
        live: &[Option<Datum>],
    ) -> Result<(), DbError> {
        let t = self.open_table(st, oid, RelLockMode::RowExclusive)?;
        self.insert_with(st, &t, live)
    }

    /// **Deprecated** legacy INSERT through a possibly cross-statement
    /// handle (the C-T6b1 contract, pinned by the `t6b1_*` tests) — use
    /// [`Db::insert_by_oid`] (by oid, maintains) or [`Db::insert_with`]
    /// (statement-scoped handle, maintains) instead. RowExclusive, the
    /// relation row re-read under it (a handle the catalog moved under is
    /// 55000, an absent row 42P01), the **index rows re-scanned at the
    /// same read** — and a table the latest catalog says is indexed
    /// refuses with 0A000 naming this legacy handle-based path as the
    /// deprecated one: this entry point accepts handles from any
    /// statement, so it can never know which index states its own
    /// statement saw, and a stale list is exactly the silent
    /// missing-maintenance write.
    pub fn insert(
        &self,
        st: &Stmt<'_>,
        t: &TableHandle,
        live: &[Option<Datum>],
    ) -> Result<(), DbError> {
        self.lock_and_validate(st, t)?;
        // §4/M-3: the writer's catalog read covers the index rows too —
        // including an index committed since the handle's opening.
        let own = writer_ctx(st, self.core.visible_ts());
        let indexes = self.scan_index_handles(&own, t.oid)?;
        if !indexes.is_empty() {
            return Err(DbError::FeatureNotSupported(
                "index maintenance: deprecated handle-based insert (use insert_by_oid/insert_with)",
            ));
        }
        self.write_insert(st, t, live, indexes)
    }

    /// The statement-scoped INSERT (item 4a/4): like the deprecated
    /// legacy [`Db::insert`], but the handle must be the one this very
    /// statement's
    /// `open_table(RowExclusive..)` made — its RowExclusive was taken
    /// before its catalog read, and no `ROLLBACK TO` runs inside a
    /// statement, so the lock is held throughout. Anything else is
    /// `TxnError::Invariant` **before any catalog read**: a
    /// cross-statement or cross-txn handle is a caller bug (the
    /// pre-CREATE-INDEX, pre-DROP and pre-TRUNCATE stale-handle paths of
    /// the C-T6b1-fix gate's follow-ups all die here). The handle's own
    /// index list is the statement's catalog read, so it is exactly what
    /// this statement maintains — every WriteOnly/Public index gets the
    /// new row's entry (`entry_for_insert` + `insert_key`, the §6 key
    /// limit enforced), a DeleteOnly index gets nothing, and a handle
    /// older than a concurrent SUE-mode index-state commit is the legal
    /// stale statement of C-T6 §7.3, never compared with the current
    /// catalog.
    pub fn insert_with(
        &self,
        st: &Stmt<'_>,
        t: &TableHandle,
        live: &[Option<Datum>],
    ) -> Result<(), DbError> {
        check_handle(st, t)?;
        self.lock_and_validate(st, t)?;
        self.write_insert(st, t, live, t.indexes.clone())
    }

    /// UPDATE by primary key (§5/§6): `open_table(RowExclusive)` then
    /// [`Db::update_with`]. Returns whether a row was updated.
    pub fn update(
        &self,
        st: &Stmt<'_>,
        oid: u64,
        pk: &[Option<Value>],
        set: &[(u16, Option<Datum>)],
    ) -> Result<bool, DbError> {
        let t = self.open_table(st, oid, RelLockMode::RowExclusive)?;
        self.update_with(st, &t, pk, set)
    }

    /// UPDATE by primary key through a statement-scoped handle (item 4a,
    /// same contract as [`Db::insert_with`]): read the row at the
    /// statement's snapshot (SER registers its SIREAD); absent →
    /// `Ok(false)`. The new row is `set` re-applied to the base the row
    /// op actually acted on — under RC EPQ (§5.2) that is the newest
    /// committed version, and the callback returns `Apply(Update)` for it
    /// (a tombstone skips); without EPQ it is the read row. `set`
    /// assignments name 1-based attnums; an unknown attnum is 42703. A
    /// primary-key change goes through `update_pk` (§5.0; the §3 key
    /// limit applies to the new key). After `Applied`: the §5 chunks of
    /// every value the statement moved out of line are written, every
    /// old raw pointer the new row no longer carries (dropped attnums
    /// included) has its chunks deleted through `Core::delete_entry`,
    /// and every index of the handle is maintained per its state with
    /// both rows detoasted — the old entries computed from the EPQ base;
    /// `Skipped` → `Ok(false)` (and no chunk is written).
    pub fn update_with(
        &self,
        st: &Stmt<'_>,
        t: &TableHandle,
        pk: &[Option<Value>],
        set: &[(u16, Option<Datum>)],
    ) -> Result<bool, DbError> {
        check_handle(st, t)?;
        if t.schema.hidden_rowid {
            return Err(DbError::FeatureNotSupported("rowid updates"));
        }
        let key = pk_key(t.storage_id, &t.schema, pk)?;
        let Some(read) = self.read_row_bytes(st, &key)? else {
            return Ok(false);
        };
        // No EPQ yet: the base is the read row, and `set` applies to it.
        let mut epq = UpdateEpq::new(t, st, pk, set, read)?;
        let outcome = if pks_differ(pk, &epq.last.new_pk) {
            let new_key = pk_key(t.storage_id, &t.schema, &epq.last.new_pk)?;
            self.core
                .update_pk(
                    st.txn,
                    &key,
                    &new_key,
                    epq.last.new_bytes.clone(),
                    st.ctx(),
                    &mut epq,
                )
                .map_err(DbError::Txn)?
        } else {
            self.core
                .row_op(
                    st.txn,
                    &key,
                    None,
                    RowOp::Update {
                        value: epq.last.new_bytes.clone(),
                        key_cols_changed: epq.last.key_cols_changed,
                    },
                    st.ctx(),
                    &mut epq,
                )
                .map_err(DbError::Txn)?
        };
        if let Some(err) = epq.error.take() {
            return Err(err);
        }
        match outcome {
            RowOutcome::Applied => {}
            RowOutcome::Skipped(_) => return Ok(false),
        }
        // §5, after `Applied`: the new chunks first, then every old raw
        // pointer (dropped attnums included) the new row does not carry.
        toast::write_chunks(
            &self.core,
            st.txn,
            &st.ctx(),
            t.storage_id,
            &epq.last.new_toasts,
        )?;
        toast::delete_stale_chunks(
            &self.core,
            st.txn,
            &st.ctx(),
            t.storage_id,
            &epq.base_bytes,
            &epq.last.new_bytes,
        )?;
        let (base_live, new_live) = self.update_maintenance_rows(st, t, &epq)?;
        let new_key = if pks_differ(pk, &epq.last.new_pk) {
            pk_key(t.storage_id, &t.schema, &epq.last.new_pk)?
        } else {
            key.clone()
        };
        index::maintain_update(
            &self.core,
            st.txn,
            &st.ctx(),
            t,
            &t.indexes,
            IndexRow {
                key: &key,
                live: &base_live,
            },
            IndexRow {
                key: &new_key,
                live: &new_live,
            },
        )?;
        Ok(true)
    }

    /// DELETE by primary key (§5/§6): `open_table(RowExclusive)` then
    /// [`Db::delete_with`]. Returns whether a row was deleted.
    pub fn delete(&self, st: &Stmt<'_>, oid: u64, pk: &[Option<Value>]) -> Result<bool, DbError> {
        let t = self.open_table(st, oid, RelLockMode::RowExclusive)?;
        self.delete_with(st, &t, pk)
    }

    /// DELETE by primary key through a statement-scoped handle (item 4a,
    /// same contract as [`Db::insert_with`]): the same read/EPQ pattern
    /// as [`Db::update_with`] with `RowOp::Delete`; after `Applied`,
    /// every raw pointer of the base version (dropped attnums included)
    /// has its chunks deleted through `Core::delete_entry` (§5), and
    /// every entry of the base version is deleted in **every** index
    /// state, blindly through [`Core::delete_entry`] (§5.0) — so the
    /// delete never skips and never raises 40001 (§6).
    pub fn delete_with(
        &self,
        st: &Stmt<'_>,
        t: &TableHandle,
        pk: &[Option<Value>],
    ) -> Result<bool, DbError> {
        check_handle(st, t)?;
        if t.schema.hidden_rowid {
            return Err(DbError::FeatureNotSupported("rowid deletes"));
        }
        let key = pk_key(t.storage_id, &t.schema, pk)?;
        let Some(read) = self.read_row_bytes(st, &key)? else {
            return Ok(false);
        };
        let mut epq = DeleteEpq {
            base_bytes: read,
            base_ts: None,
        };
        let outcome = self
            .core
            .row_op(st.txn, &key, None, RowOp::Delete, st.ctx(), &mut epq)
            .map_err(DbError::Txn)?;
        match outcome {
            RowOutcome::Applied => {}
            RowOutcome::Skipped(_) => return Ok(false),
        }
        // §5, after `Applied`: nothing carries the base's pointers
        // onward, so every chunk of every raw pointer goes.
        toast::delete_row_chunks(&self.core, st.txn, &st.ctx(), t.storage_id, &epq.base_bytes)?;
        let mut base_live = decode_row_at_era(&t.schema, &epq.base_bytes)?;
        self.detoast_base(st, t, &mut base_live, epq.base_ts)?;
        index::maintain_delete(
            &self.core,
            st.txn,
            &st.ctx(),
            t,
            &t.indexes,
            IndexRow {
                key: &key,
                live: &base_live,
            },
        )?;
        Ok(true)
    }

    /// Detoasts a base row for maintenance (§5): at the commit ts of the
    /// version an EPQ pass re-based on (`register_at(v.ts)`), else at the
    /// statement's read position — where the txn's own earlier intents
    /// resolve too.
    fn detoast_base(
        &self,
        st: &Stmt<'_>,
        t: &TableHandle,
        live: &mut [Option<Datum>],
        base_ts: Option<Ts>,
    ) -> Result<(), DbError> {
        match base_ts {
            Some(ts) => {
                toast::detoast_row_at_version(&self.core, &t.schema, t.storage_id, live, ts)
            }
            None => {
                toast::detoast_row_at(&self.core, &t.schema, t.storage_id, live, &st.read_ctx())
            }
        }
    }

    /// The detoasted maintenance rows of an applied update (§5): the base
    /// resolved at its read position, and the new row with the
    /// statement's own toasted values in hand and its carried pointers
    /// taken from the resolved base (a carried pointer's chunks are the
    /// base's, read exactly once).
    fn update_maintenance_rows(
        &self,
        st: &Stmt<'_>,
        t: &TableHandle,
        epq: &UpdateEpq<'_>,
    ) -> Result<DetoastedPair, DbError> {
        let mut base_live = decode_row_at_era(&t.schema, &epq.base_bytes)?;
        self.detoast_base(st, t, &mut base_live, epq.base_ts)?;
        let mut new_live = decode_row_at_era(&t.schema, &epq.last.new_bytes)?;
        for (pos, datum) in new_live.iter_mut().enumerate() {
            let Some(Datum::Toast(p)) = datum.as_ref() else {
                continue;
            };
            if let Some(d) = epq.last.new_toasts.iter().find(|d| d.id == p.id) {
                // The statement's own value, in hand.
                *datum = Some(Datum::Inline(d.value.clone()));
            } else {
                // A pointer carried from the base: its resolved value.
                match base_live.get(pos) {
                    Some(Some(Datum::Inline(v))) => *datum = Some(Datum::Inline(v.clone())),
                    _ => {
                        return Err(DbError::Row(RowError::Corrupt(
                            "out-of-line datum with no chunks to read",
                        )))
                    }
                }
            }
        }
        Ok((base_live, new_live))
    }

    /// The §4 writer's read of the deprecated legacy [`Db::insert`]:
    /// RowExclusive, then the relation row re-read at the latest
    /// committed catalog plus the txn's earlier statements — a handle the
    /// catalog has moved under is refused with 55000, an absent row with
    /// 42P01 (never write into a dropped or retired storage).
    /// Belt-and-braces for [`Db::insert_with`] (and so for
    /// [`Db::insert_by_oid`]) too: a statement-scoped handle's relation
    /// row cannot move (every rel-row DDL holds AccessExclusive, which
    /// conflicts with the statement's own RowExclusive).
    fn lock_and_validate(&self, st: &Stmt<'_>, t: &TableHandle) -> Result<(), DbError> {
        self.locks
            .lock_relation(
                &self.core,
                st.txn,
                t.oid,
                RelLockMode::RowExclusive,
                LockWait::Block,
                None,
            )
            .map_err(DbError::Txn)?;
        let own = writer_ctx(st, self.core.visible_ts());
        let rel = {
            let view = self.core.open_view();
            Catalog::read_rel(&self.core, &view, &own, t.oid).map_err(DbError::Txn)?
        };
        let Some(rel) = rel else {
            return Err(DbError::UndefinedTable);
        };
        let def_version = TableSchema::decode(&rel.def)?.version;
        if rel.storage_id != t.storage_id || def_version != t.def_version {
            return Err(DbError::ObjectNotInPrerequisiteState("stale table handle"));
        }
        Ok(())
    }

    /// The index rows of relation `oid` at the writer's read of `own`
    /// (§4): the statement's catalog read covers them too.
    fn scan_index_handles(&self, own: &ReadCtx, oid: u64) -> Result<Vec<IndexHandle>, DbError> {
        let idxs = {
            let view = self.core.open_view();
            Catalog::scan_idxs_of_rel(&self.core, &view, own, oid).map_err(DbError::Txn)?
        };
        index_handles(idxs)
    }

    /// The shared row-write body of the insert entry points: the row key,
    /// the key-existence insert, then the §5 chunks and the index
    /// maintenance — the statement's own list for `insert_with` and
    /// [`Db::insert_by_oid`] (the only paths that reach here with a
    /// non-empty list; the deprecated legacy `insert` refuses indexed
    /// tables before this).
    fn write_insert(
        &self,
        st: &Stmt<'_>,
        t: &TableHandle,
        live: &[Option<Datum>],
        indexes: Vec<IndexHandle>,
    ) -> Result<(), DbError> {
        // §5: over-threshold values go out of line — ids assigned while
        // the new row value is built, the row storing the pointers and
        // the statement holding the bytes for the chunks.
        let toasted = toast::build_row(st, &t.schema, live)?;
        let value = t.schema.encode_row(&toasted.live)?;
        let key = if t.schema.hidden_rowid {
            let ord = st.next_ord()?;
            rowid_key(t.storage_id, st.txn, st.seq0, ord)?
        } else {
            // The pk key encodes the caller's inline values (a pk column
            // may itself be out of line in the stored row).
            let pk = pk_values(&t.schema, live)?;
            pk_key(t.storage_id, &t.schema, &pk)?
        };
        self.core
            .insert_key(
                st.txn,
                &key,
                None,
                value,
                st.ctx(),
                UniqueRule::Unique { same_row: None },
            )
            .map_err(DbError::Txn)?;
        // §5: the chunks land only after the row op returned `Applied`
        // (`Ok` here — a key-existence op never skips), in the same
        // statement, so an aborted or refused insert writes none.
        toast::write_chunks(&self.core, st.txn, &st.ctx(), t.storage_id, &toasted.new)?;
        index::maintain_insert(
            &self.core,
            st.txn,
            &st.ctx(),
            t,
            &indexes,
            // §5: index keys of a toasted column use the detoasted
            // value — the caller's inline datums are exactly that.
            IndexRow { key: &key, live },
        )?;
        Ok(())
    }

    /// The row bytes of `key` at the statement's snapshot (§4): SER
    /// registers the SIREAD (§8.1); other levels read through a plain
    /// registered view. `None` = not found (including tombstones).
    fn read_row_bytes(&self, st: &Stmt<'_>, key: &[u8]) -> Result<Option<Vec<u8>>, DbError> {
        let ctx = st.read_ctx();
        let found = match st.txn.isolation {
            Isolation::Serializable => self
                .ssi
                .read_key(&self.core, st.txn.id, key, &ctx)
                .map_err(DbError::Txn)?,
            _ => {
                let view = self.core.open_view();
                read_key(&self.core, &view, key, &ctx, &mut NoSsi).map_err(DbError::Txn)?
            }
        };
        Ok(found)
    }

    /// Point read of the primary-key row `pk` (one value per pk column,
    /// `None` = NULL) at the statement's snapshot, decoded under the
    /// handle's definition and **detoasted** (§5: chunks read at the
    /// statement's snapshot through a fresh view, no SIREAD). NOT NULL
    /// binds per column from `not_null_since`
    /// (`decode_row_at_era`: a pre-constraint NULL is served, a stray
    /// one at or after the constraint's version is XX001). SERIALIZABLE
    /// registers its SIREAD on the row key (§8.1); other levels read
    /// through a plain registered view. A hidden-rowid table has no
    /// callable key: 0A000 (scan it instead).
    pub fn get(
        &self,
        st: &Stmt<'_>,
        t: &TableHandle,
        pk: &[Option<Value>],
    ) -> Result<Option<Vec<Option<Datum>>>, DbError> {
        self.locks
            .lock_relation(
                &self.core,
                st.txn,
                t.oid,
                RelLockMode::AccessShare,
                LockWait::Block,
                None,
            )
            .map_err(DbError::Txn)?;
        if t.schema.hidden_rowid {
            return Err(DbError::FeatureNotSupported("rowid point reads"));
        }
        let key = pk_key(t.storage_id, &t.schema, pk)?;
        let ctx = st.read_ctx();
        let bytes = match st.txn.isolation {
            Isolation::Serializable => self
                .ssi
                .read_key(&self.core, st.txn.id, &key, &ctx)
                .map_err(DbError::Txn)?,
            _ => {
                let view = self.core.open_view();
                read_key(&self.core, &view, &key, &ctx, &mut NoSsi).map_err(DbError::Txn)?
            }
        };
        match bytes {
            Some(b) => {
                let mut live = decode_row_at_era(&t.schema, &b)?;
                toast::detoast_row_at(&self.core, &t.schema, t.storage_id, &mut live, &ctx)?;
                Ok(Some(live))
            }
            None => Ok(None),
        }
    }

    /// Every row of the table at the statement's snapshot, in key order,
    /// decoded under the handle's definition and **detoasted** (§5: each
    /// row's chunks read at the statement's snapshot, no SIREAD). NOT
    /// NULL binds per column from `not_null_since`
    /// (`decode_row_at_era`: a pre-constraint NULL is served, a stray
    /// one at or after the constraint's version is XX001). The
    /// scan is bounded to `[table_lo, table_lo ‖ 0xFF)` (§5): `0xFF`
    /// never starts an encoded key, so large-value chunks
    /// (`/t/{sid}/0xFF…`) are never rows. SERIALIZABLE registers the
    /// range SIREAD (§8.1).
    pub fn scan(&self, st: &Stmt<'_>, t: &TableHandle) -> Result<Vec<ScannedRow>, DbError> {
        self.locks
            .lock_relation(
                &self.core,
                st.txn,
                t.oid,
                RelLockMode::AccessShare,
                LockWait::Block,
                None,
            )
            .map_err(DbError::Txn)?;
        let lo = table_lo(t.storage_id);
        let hi = chunk_prefix(&lo);
        let ctx = st.read_ctx();
        let rows = match st.txn.isolation {
            Isolation::Serializable => self
                .ssi
                .scan(&self.core, st.txn.id, &lo, &hi, &ctx)
                .map_err(DbError::Txn)?,
            _ => {
                let view = self.core.open_view();
                scan(
                    &self.core,
                    &view,
                    (
                        Bound::Included(lo.as_slice()),
                        Bound::Excluded(hi.as_slice()),
                    ),
                    &ctx,
                    &mut NoSsi,
                )
                .collect::<Result<Vec<_>, _>>()
                .map_err(DbError::Txn)?
            }
        };
        let mut out = Vec::with_capacity(rows.len());
        for (key, bytes) in rows {
            let mut live = decode_row_at_era(&t.schema, &bytes)?;
            toast::detoast_row_at(&self.core, &t.schema, t.storage_id, &mut live, &ctx)?;
            out.push((key, live));
        }
        Ok(out)
    }

    /// A verified index scan (C-T6 §4, §6): every entry of index `idx_oid`
    /// whose key value lies in `[lo, hi)` (one value per index column,
    /// `None` = NULL, compared in the index's column order — P-ORDER), at
    /// the statement's snapshot, with the row each entry names fetched and
    /// verified in the entry's own visibility regime (§6): a fetched row
    /// not visible in that regime, or whose index columns do not re-encode
    /// to the entry's key, is [`RowError::Corrupt`] (XX001) — never a
    /// silently wrong result. A snapshot-visible entry the txn did not
    /// write is verified against the row at the snapshot (fetched without
    /// the txn's own overlay); an entry the txn itself wrote, against the
    /// own-overlaid row. A snapshot entry whose row the txn has since
    /// overwritten is skipped — its row is the txn's own write now, served
    /// (iff in range) through the txn's own entry. Returns
    /// `(row key, decoded row)` pairs in index order
    /// (`/i/` entries before `/u/`, each in key order).
    ///
    /// The statement takes AccessShare on the owning relation (§4), reads
    /// the index and relation rows at the latest committed catalog plus
    /// its txn's earlier statements, and serves the read only if the index
    /// is `Public` with `built_at <= st.snapshot` (§4: "an index serves a
    /// read at `S` iff `Public` and `built_at <= S`"); anything else is
    /// 0A000 ("index not usable at snapshot"). The post-lock catalog
    /// reads run at a fresh post-lock `visible_ts()`, `open_table`'s
    /// lock-then-read shape (the pre-lock read only resolves the owner to
    /// lock): a DROP INDEX or DROP TABLE that committed while the
    /// statement waited for its AccessShare is 42704 / 42P01 — never a
    /// serve of the pre-drop state. Under RR/SER the §4
    /// relation rule runs too, exactly as
    /// [`Db::open_table`](Db::open_table) runs it for every other
    /// relation access: the relation row read at the statement's snapshot
    /// and at the latest committed state (both through a ghost reader),
    /// present at the snapshot with a different storage id — a foreign
    /// TRUNCATE or rewrite committed after it — is 40001, never a silent
    /// serve of pre-rewrite data. The entry range read goes
    /// through `Ssi::scan` under SERIALIZABLE (the range SIREAD registers
    /// before the scan's view, §8.1) and each row fetch through
    /// `Ssi::read_key` (its SIREAD registered before its own view, §8.1
    /// "index → heap fetch").
    pub fn index_scan(
        &self,
        st: &Stmt<'_>,
        idx_oid: u64,
        lo: Bound<Vec<Option<Value>>>,
        hi: Bound<Vec<Option<Value>>>,
    ) -> Result<Vec<ScannedRow>, DbError> {
        // Resolve the owner first (as the catalog's DROP INDEX does), then
        // take §4's relation lock. The pre-lock ctx (`pre`) serves only
        // that owner resolution; every post-lock catalog read below runs
        // at a fresh `visible_ts()` — a concurrent DROP INDEX or DROP
        // TABLE that committed while we waited shows in those reads
        // (42704 / 42P01), never a serve of the pre-drop state.
        let pre = writer_ctx(st, self.core.visible_ts());
        let owner = {
            let view = self.core.open_view();
            let row = Catalog::read_idx(&self.core, &view, &pre, idx_oid).map_err(DbError::Txn)?;
            let Some(row) = row else {
                return Err(DbError::UndefinedObject);
            };
            row.owning_rel
        };
        self.locks
            .lock_relation(
                &self.core,
                st.txn,
                owner,
                RelLockMode::AccessShare,
                LockWait::Block,
                None,
            )
            .map_err(DbError::Txn)?;
        // §4: the statement's catalog read, under the lock and at a fresh
        // post-lock `visible_ts()` — everything that committed while the
        // statement waited for its AccessShare is visible in it (a ctx
        // frozen before the lock would serve the pre-wait state). The
        // relation row is read first, `open_table`'s shape: a table
        // dropped under the wait is 42P01 here.
        let own = writer_ctx(st, self.core.visible_ts());
        let rel = {
            let view = self.core.open_view();
            Catalog::read_rel(&self.core, &view, &own, owner).map_err(DbError::Txn)?
        };
        let Some(rel) = rel else {
            return Err(DbError::UndefinedTable);
        };
        // §4's RR/SER relation rule, as `open_table` runs it for every
        // other relation access: the relation row at the statement's
        // snapshot and at the latest committed state, both through a
        // ghost reader (a `TxnId` this process never allocates), so the
        // txn's own writes are ignored on both sides — present at the
        // snapshot with a different storage id (a foreign TRUNCATE or
        // rewrite committed after it) is 40001; absent at the snapshot →
        // proceed (§4). A storage id installed by the txn's own DDL can
        // never differ from itself on these reads.
        if matches!(
            st.txn.isolation,
            Isolation::RepeatableRead | Isolation::Serializable
        ) {
            let ghost = |snapshot: Ts| ReadCtx {
                txn: ghost_id(&self.core),
                snapshot,
                stmt_seq: 0,
            };
            let at_s = {
                let view = self.core.open_view();
                Catalog::read_rel(&self.core, &view, &ghost(st.snapshot), owner)
                    .map_err(DbError::Txn)?
            };
            let latest = {
                let view = self.core.open_view();
                Catalog::read_rel(&self.core, &view, &ghost(self.core.visible_ts()), owner)
                    .map_err(DbError::Txn)?
            };
            if let (Some(at_s), Some(latest)) = (at_s, latest) {
                if at_s.storage_id != latest.storage_id {
                    return Err(DbError::Txn(TxnError::SerializationFailure));
                }
            }
        }
        // The index row re-read, same fresh post-lock ctx: an index
        // dropped under the wait is 42704 here (the pre-lock ctx would
        // still see it).
        let idx = {
            let view = self.core.open_view();
            Catalog::read_idx(&self.core, &view, &own, idx_oid).map_err(DbError::Txn)?
        };
        let Some(idx) = idx else {
            return Err(DbError::UndefinedObject);
        };
        let def = IndexDef::decode(&idx.def)?;
        if def.state != IndexState::Public || def.built_at > st.snapshot().0 {
            return Err(DbError::FeatureNotSupported("index not usable at snapshot"));
        }
        let IdxStorage::Id(isid) = idx.storage else {
            return Err(DbError::FeatureNotSupported("index storage ranges: C-T6b2"));
        };
        let schema = TableSchema::decode(&rel.def)?;
        let handle = IndexHandle {
            oid: idx_oid,
            storage_id: isid,
            def,
        };
        let cols = index::key_columns(&schema, &handle)?;
        let h = TableHandle {
            txn: st.txn.id,
            seq0: st.seq0,
            lock: RelLockMode::AccessShare,
            oid: owner,
            storage_id: rel.storage_id,
            schema: schema.clone(),
            def_version: schema.version,
            indexes: vec![handle.clone()],
        };

        // Encode the bounds once (P-ORDER: memcmp of encoded keys equals
        // the SQL order of the key values). A bound narrows the scan of
        // each entry range by `prefix ‖ enc(bound)`; the two half-open
        // cases it cannot express exactly are filtered in memory below:
        // an excluded lower bound must also drop the entries **equal** to
        // it (their keys carry a row-key suffix past the bound, so the
        // range start alone does not exclude them), and an included upper
        // bound must drop the entries above it (the scan runs to the range
        // end). P-PREFIX (no encoding is a prefix of another) makes both
        // filters exact: within a scan that starts at `prefix ‖ enc(b)`,
        // every entry's value part is `>= enc(b)`, and none is a proper
        // prefix of it.
        let enc = |vals: &Vec<Option<Value>>| -> Result<Vec<u8>, DbError> {
            let mut out = Vec::new();
            encode_key(&cols, vals, &mut out)
                .map_err(|_| DbError::Row(RowError::Corrupt("index scan bound")))?;
            Ok(out)
        };
        let (lo_bytes, drop_eq_lo) = match &lo {
            Bound::Included(v) => (Some(enc(v)?), false),
            Bound::Excluded(v) => (Some(enc(v)?), true),
            Bound::Unbounded => (None, false),
        };
        let (hi_bytes, keep_le_hi) = match &hi {
            Bound::Included(v) => (Some(enc(v)?), true),
            Bound::Excluded(v) => (Some(enc(v)?), false),
            Bound::Unbounded => (None, false),
        };

        let ctx = ReadCtx {
            txn: st.txn.id,
            snapshot: st.snapshot,
            stmt_seq: st.seq0,
        };
        let ranges = index_ranges(isid);
        let mut out: Vec<ScannedRow> = Vec::new();
        // `/i/` sorts before `/u/`: scan it first so the result is in
        // index order across both layouts of the storage.
        let layouts = [(false, &ranges[1]), (true, &ranges[0])];
        for (unique_range, (prefix, range_end)) in layouts {
            let mut start = prefix.clone();
            if let Some(b) = &lo_bytes {
                start.extend_from_slice(b);
            }
            let end = match &hi_bytes {
                Some(b) if !keep_le_hi => {
                    let mut e = prefix.clone();
                    e.extend_from_slice(b);
                    e
                }
                _ => range_end.clone(),
            };
            let entries = match st.txn.isolation {
                Isolation::Serializable => self
                    .ssi
                    .scan(&self.core, st.txn.id, &start, &end, &ctx)
                    .map_err(DbError::Txn)?,
                _ => {
                    let view = self.core.open_view();
                    scan(
                        &self.core,
                        &view,
                        (
                            Bound::Included(start.as_slice()),
                            Bound::Excluded(end.as_slice()),
                        ),
                        &ctx,
                        &mut NoSsi,
                    )
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(DbError::Txn)?
                }
            };
            for (key, payload) in entries {
                let part = &key[prefix.len()..];
                // Split the entry into its value part and the row key it
                // names: a `/u/` entry carries the row key as its payload
                // (§6), an `/i/` entry as the suffix past its value part.
                let (value_part, row_key): (&[u8], Key) = if unique_range {
                    (part, payload.clone())
                } else {
                    let (_, used) = decode_key_prefix(&cols, part)
                        .map_err(|_| DbError::Row(RowError::Corrupt("index entry key")))?;
                    (&part[..used], part[used..].to_vec())
                };
                if drop_eq_lo && lo_bytes.as_deref() == Some(value_part) {
                    continue;
                }
                if keep_le_hi && hi_bytes.as_deref().is_some_and(|b| value_part > b) {
                    continue;
                }
                // The row fetch (§8.1: the point SIREAD registers before
                // the fetch's own view under SERIALIZABLE) runs at the
                // reader's regime: the txn's own intents overlay the
                // snapshot (§4 step 1), so this is the row the statement
                // must return whenever the entry survives verification.
                let bytes = match st.txn.isolation {
                    Isolation::Serializable => self
                        .ssi
                        .read_key(&self.core, st.txn.id, &row_key, &ctx)
                        .map_err(DbError::Txn)?,
                    _ => {
                        let view = self.core.open_view();
                        read_key(&self.core, &view, &row_key, &ctx, &mut NoSsi)
                            .map_err(DbError::Txn)?
                    }
                };
                let Some(bytes) = bytes else {
                    return Err(DbError::Row(RowError::Corrupt(
                        "index entry names a row not visible at the snapshot",
                    )));
                };
                let mut live = decode_row_at_era(&schema, &bytes)?;
                // §5: an indexed toasted column's entry is computed from
                // the detoasted value — the chunks read at the scan's
                // snapshot, no SIREAD.
                toast::detoast_row_at(&self.core, &schema, rel.storage_id, &mut live, &ctx)?;
                // §6: verify the entry against the row **in the entry's
                // own visibility regime** (C-T6e3fix). A ghost reader at
                // the statement's snapshot — an id this process never
                // allocates, so the own-overlay branch of §4 step 1 never
                // fires for it — tells the two regimes of the enumerated
                // entry apart: a committed version visible at the
                // snapshot, or the txn's own intent. Each is verified
                // against the row read in the same regime. The old form
                // compared every entry against the own-overlaid fetch,
                // crossing regimes: the legal §5.3 history where a
                // concurrent DELETE commits above the snapshot and the
                // txn re-inserts the same pk (the old entry stays live at
                // the snapshot; its row is the txn's own write now) was
                // reported XX001 on consistent state.
                let ghost = ReadCtx {
                    txn: ghost_id(&self.core),
                    snapshot: st.snapshot,
                    stmt_seq: 0,
                };
                let entry_at_s = {
                    let view = self.core.open_view();
                    read_key(&self.core, &view, &key, &ghost, &mut NoSsi).map_err(DbError::Txn)?
                };
                let rebuild_matches = |row: &[Option<Datum>]| -> Result<bool, DbError> {
                    let (want_key, want_value, _) = index::entry(&h, &handle, &row_key, row)?;
                    Ok(want_key == key && want_value == payload)
                };
                if entry_at_s.as_deref() == Some(payload.as_slice()) {
                    // A committed entry visible at the snapshot: verify
                    // against the row at the snapshot, without the own
                    // overlay (the same regime that makes the entry
                    // visible).
                    let s_bytes = {
                        let view = self.core.open_view();
                        read_key(&self.core, &view, &row_key, &ghost, &mut NoSsi)
                            .map_err(DbError::Txn)?
                    };
                    let Some(s_bytes) = s_bytes else {
                        return Err(DbError::Row(RowError::Corrupt(
                            "index entry names a row not visible at the snapshot",
                        )));
                    };
                    if bytes == s_bytes {
                        // No own write shadows the row: the regimes agree,
                        // one verification and one row out.
                        if !rebuild_matches(&live)? {
                            return Err(DbError::Row(RowError::Corrupt(
                                "index entry does not match its row",
                            )));
                        }
                        out.push((row_key, live));
                    } else {
                        // The txn has overwritten the row (§4 step 1: an
                        // own intent shadows it). The snapshot pair must
                        // still re-encode — maintenance wrote this entry;
                        // a mismatch is real corruption. What the entry
                        // serves is decided in the reader's regime: the
                        // own row re-encodes to this same entry only when
                        // the own write kept it (its entry intent sits on
                        // this very key), and then this entry carries the
                        // own row out; otherwise the entry is skipped —
                        // its row is the txn's own write now, served (iff
                        // its value is in range) through the txn's own
                        // entry for the new row.
                        let mut s_live = decode_row_at_era(&schema, &s_bytes)?;
                        toast::detoast_row_at(
                            &self.core,
                            &schema,
                            rel.storage_id,
                            &mut s_live,
                            &ghost,
                        )?;
                        if !rebuild_matches(&s_live)? {
                            return Err(DbError::Row(RowError::Corrupt(
                                "index entry does not match its row",
                            )));
                        }
                        if rebuild_matches(&live)? {
                            out.push((row_key, live));
                        }
                    }
                } else {
                    // The txn's own entry (no committed version at the
                    // snapshot matches it): verify against the
                    // own-overlaid fetch — the regime that produced it.
                    if !rebuild_matches(&live)? {
                        return Err(DbError::Row(RowError::Corrupt(
                            "index entry does not match its row",
                        )));
                    }
                    out.push((row_key, live));
                }
            }
        }
        Ok(out)
    }
}

// ---- handle checks, keys-by-oid and the row-level EPQ (§5.2) ------------------

/// Decodes one row version read at a snapshot or re-based by EPQ, under
/// the handle's (latest committed) definition with C-T6 §2/§3's
/// era-scoped NOT NULL rule: the column list, `missing` defaults and
/// dropped positions are the latest definition's — §4 "rows decode
/// under the latest definition" — and a NULL bit on attnum `a` is
/// corruption (XX001) iff `a.not_null && v >= a.not_null_since`, the
/// def version at which that column's constraint (last) came into
/// force. The rule itself lives in one code path,
/// [`TableSchema::decode_row`](nucleus_codec::row::TableSchema::decode_row):
/// this is the DML/EPQ seam over it, and amcheck's I-ROWFMT decodes
/// through the same function, never a second copy. The marker arrives
/// exact through the def bytes (§3's flags-gated `be32` word), so the
/// per-column era survives every reopen and every unrelated later DDL.
fn decode_row_at_era(schema: &TableSchema, bytes: &[u8]) -> Result<Vec<Option<Datum>>, DbError> {
    schema.decode_row(bytes).map_err(DbError::Row)
}

/// The writer's §4 read ctx: the latest committed catalog plus this txn's
/// earlier statements (`seq < seq0`).
fn writer_ctx(st: &Stmt<'_>, visible_ts: Ts) -> ReadCtx {
    ReadCtx {
        txn: st.txn.id,
        snapshot: visible_ts,
        stmt_seq: st.seq0,
    }
}

/// Item 4a's statement-scoped handle contract: the `_with` DML entry
/// points accept a handle only from the `(txn, seq0)` statement whose
/// `open_table` made it — one statement, so the RowExclusive was taken
/// before the handle's catalog read and is held throughout (no
/// `ROLLBACK TO` runs inside a statement). A handle from another txn or a
/// later statement is `TxnError::Invariant` **before any catalog read**
/// (the C-T6b1-fix gate's cross-statement stale-handle paths all die
/// here). The handle is never compared with the current catalog: a handle
/// older than a concurrent index-state commit is the legal stale
/// statement of C-T6 §7.3.
fn check_handle(st: &Stmt<'_>, t: &TableHandle) -> Result<(), DbError> {
    if t.txn != st.txn.id || t.seq0 != st.seq0 {
        return Err(DbError::Txn(TxnError::Invariant(format!(
            "table handle opened by txn {:?} stmt {} used in txn {:?} stmt {}: open a fresh handle per statement",
            t.txn, t.seq0, st.txn.id, st.seq0
        ))));
    }
    // §4: a statement that writes a relation reads its catalog rows only
    // while holding RowExclusive or stronger — a handle opened at a read
    // mode is not a writer's catalog read.
    if !matches!(
        t.lock,
        RelLockMode::RowExclusive | RelLockMode::Exclusive | RelLockMode::AccessExclusive
    ) {
        return Err(DbError::Txn(TxnError::Invariant(format!(
            "table handle of relation {} opened at {:?}: DML needs a handle opened at RowExclusive or stronger (§4)",
            t.oid, t.lock
        ))));
    }
    Ok(())
}

/// Decodes the index rows a catalog scan returned into handles (§4). An
/// index with an explicit entry range has no storage id the typed layer
/// can name: 0A000 (C-T6b2).
fn index_handles(idxs: Vec<nucleus_txn::catalog::IdxRow>) -> Result<Vec<IndexHandle>, DbError> {
    let mut indexes = Vec::with_capacity(idxs.len());
    for idx in idxs {
        match idx.storage {
            IdxStorage::Id(storage_id) => indexes.push(IndexHandle {
                oid: idx.oid,
                storage_id,
                def: IndexDef::decode(&idx.def)?,
            }),
            IdxStorage::Range(_, _) => {
                return Err(DbError::FeatureNotSupported("index storage ranges: C-T6b2"))
            }
        }
    }
    Ok(indexes)
}

/// A maintenance read's two detoasted rows: the EPQ base and the new
/// version (§5 — index entries are computed from detoasted values).
type DetoastedPair = (Vec<Option<Datum>>, Vec<Option<Datum>>);

/// `set` applied to one base row (§3 lazy upgrade at the current version):
/// the encoded new row, its pk values and the caller's `key_cols_changed`
/// verdict. §5: values over the threshold go out of line while the new
/// row value is built — ids from the statement's ordinal counter, the
/// bytes held for the chunks the statement writes iff the row op applies.
struct Applied {
    new_bytes: Vec<u8>,
    new_pk: Vec<Option<Value>>,
    key_cols_changed: bool,
    /// The values this pass moved out of line (id, bytes, value).
    new_toasts: Vec<toast::ToastDatum>,
}

fn apply_set(
    t: &TableHandle,
    st: &Stmt<'_>,
    caller_pk: &[Option<Value>],
    set: &[(u16, Option<Datum>)],
    base_bytes: &[u8],
) -> Result<Applied, DbError> {
    let base = decode_row_at_era(&t.schema, base_bytes)?;
    let mut live = base.clone();
    for &(attnum, ref datum) in set {
        let Some(pos) = t.schema.live_columns().position(|c| c.attnum == attnum) else {
            return Err(DbError::UndefinedColumn);
        };
        live[pos] = datum.clone();
    }
    let key_cols_changed = index::key_cols_changed(&t.schema, &t.indexes, set, &base);
    let new_pk = pk_values_resolved(&t.schema, &live, caller_pk)?;
    // §5: over-threshold values go out of line (a datum already out of
    // line — a column `set` does not name — carries its pointer).
    let toasted = toast::build_row(st, &t.schema, &live)?;
    let new_bytes = t.schema.encode_row(&toasted.live)?;
    Ok(Applied {
        new_bytes,
        new_pk,
        key_cols_changed,
        new_toasts: toasted.new,
    })
}

/// The UPDATE row-level EPQ (§5.2): re-evaluate the statement against the
/// newest committed version — decode it, re-apply `set`, `Apply(Update)`
/// (a tombstone is `Skip`) — and remember the base, which is where entry
/// maintenance and the §5 old-pointer sweep are computed from. With no
/// EPQ pass the base stays the read row.
struct UpdateEpq<'a> {
    t: &'a TableHandle,
    st: &'a Stmt<'a>,
    pk: &'a [Option<Value>],
    set: &'a [(u16, Option<Datum>)],
    /// The base version of the last pass (the read row until a pass
    /// re-bases on a newer committed version).
    base_bytes: Vec<u8>,
    /// The commit ts of the version an EPQ pass re-based on; `None`
    /// while the base is the read row.
    base_ts: Option<Ts>,
    /// The new row of the last pass.
    last: Applied,
    /// An error a `recheck` could not report through the `Epq` trait
    /// (the trait has no `Result`); surfaced by the caller.
    error: Option<DbError>,
}

impl<'a> UpdateEpq<'a> {
    fn new(
        t: &'a TableHandle,
        st: &'a Stmt<'a>,
        pk: &'a [Option<Value>],
        set: &'a [(u16, Option<Datum>)],
        read_bytes: Vec<u8>,
    ) -> Result<UpdateEpq<'a>, DbError> {
        let last = apply_set(t, st, pk, set, &read_bytes)?;
        Ok(UpdateEpq {
            t,
            st,
            pk,
            set,
            base_bytes: read_bytes,
            base_ts: None,
            last,
            error: None,
        })
    }
}

impl Epq for UpdateEpq<'_> {
    fn recheck(&mut self, newest: &CommittedVersion) -> EpqDecision {
        let payload = match &newest.value {
            VersionValue::Live { payload, .. } => payload.clone(),
            // §5.2: the row is gone; §5.1's pre-rules usually answer
            // before the callback runs, this is the same verdict.
            VersionValue::Tombstone { .. } => return EpqDecision::Skip,
        };
        self.base_bytes = payload;
        self.base_ts = Some(newest.ts);
        match apply_set(self.t, self.st, self.pk, self.set, &self.base_bytes) {
            Ok(applied) => {
                let decision = EpqDecision::Apply(RowOp::Update {
                    value: applied.new_bytes.clone(),
                    key_cols_changed: applied.key_cols_changed,
                });
                self.last = applied;
                decision
            }
            Err(err) => {
                self.error = Some(err);
                EpqDecision::Skip
            }
        }
    }
}

/// The DELETE row-level EPQ: the newest committed version is the base the
/// row op (and entry maintenance) acts on; a tombstone skips.
struct DeleteEpq {
    base_bytes: Vec<u8>,
    /// The commit ts of the version a pass re-based on; `None` while the
    /// base is the read row.
    base_ts: Option<Ts>,
}

impl Epq for DeleteEpq {
    fn recheck(&mut self, newest: &CommittedVersion) -> EpqDecision {
        match &newest.value {
            VersionValue::Live { payload, .. } => {
                self.base_bytes = payload.clone();
                self.base_ts = Some(newest.ts);
                EpqDecision::Apply(RowOp::Delete)
            }
            VersionValue::Tombstone { .. } => EpqDecision::Skip,
        }
    }
}

/// Whether two pk value lists name different rows (representation-faithful
/// for floats, NULL-aware).
fn pks_differ(a: &[Option<Value>], b: &[Option<Value>]) -> bool {
    a.len() != b.len()
        || a.iter().zip(b).any(|(x, y)| match (x, y) {
            (Some(x), Some(y)) => !value_repr_eq(x, y),
            (None, None) => false,
            _ => true,
        })
}

/// One statement's position (§4, §5): the txn, its snapshot and the
/// statement's `seq0`, plus the statement's **single ordinal counter**
/// (§3: one per `(txn, seq0)`, shared by every rowid and toast id the
/// statement assigns, across calls — never reset per call).
///
/// Built only by [`Stmt::new`], which draws `seq0 = txn.next_seq()`, so no
/// two statements of one txn share a `(txn, seq0)`. The caller owns
/// snapshot registration (see the module docs).
pub struct Stmt<'a> {
    txn: &'a Txn,
    snapshot: Ts,
    seq0: Seq,
    ord: Cell<u32>,
}

impl<'a> Stmt<'a> {
    pub fn new(txn: &'a Txn, snapshot: Ts) -> Result<Stmt<'a>, DbError> {
        Ok(Stmt {
            txn,
            snapshot,
            seq0: txn.next_seq().map_err(DbError::Txn)?,
            ord: Cell::new(0),
        })
    }

    pub fn txn(&self) -> &Txn {
        self.txn
    }

    pub fn snapshot(&self) -> Ts {
        self.snapshot
    }

    pub fn seq0(&self) -> Seq {
        self.seq0
    }

    /// Hands out the statement's next ordinal (§3/§5): unique within the
    /// statement forever, no I/O, no lock. Exhaustion at `u32::MAX` is
    /// 54000, never a wrap.
    pub fn next_ord(&self) -> Result<u32, DbError> {
        let ord = self.ord.get();
        let next = ord.checked_add(1).ok_or(DbError::ProgramLimitExceeded)?;
        self.ord.set(next);
        Ok(ord)
    }

    /// The statement's write ctx (§5).
    pub fn ctx(&self) -> StmtCtx {
        StmtCtx::new(self.snapshot, self.seq0, self.seq0)
    }

    /// The statement's read ctx (§4).
    fn read_ctx(&self) -> ReadCtx {
        ReadCtx {
            txn: self.txn.id,
            snapshot: self.snapshot,
            stmt_seq: self.seq0,
        }
    }
}

// ---- keys (§3, §5) -----------------------------------------------------------

/// The lower bound of storage `sid`'s table range: every logical row key
/// of the table starts with these bytes.
pub fn table_lo(sid: u64) -> Key {
    table_range(sid).0
}

/// `lo ‖ 0xFF`: the exclusive upper bound of a row scan (§5) — the first
/// byte any large-value chunk of the storage starts with, and no encoded
/// key ever starts with `0xFF` (C-Q3s §2 markers 0x00–0x02).
pub(crate) fn chunk_prefix(lo: &[u8]) -> Key {
    let mut hi = lo.to_vec();
    hi.push(0xFF);
    hi
}

/// The row key of a primary-key row: `table_lo ‖ encode_key(pk ASC NULLS
/// LAST)` (§3). The encoded primary key (the bytes past `table_lo`) is
/// limited to [`KEY_LIMIT`] bytes; over it is 54000.
fn pk_key(sid: u64, schema: &TableSchema, pk: &[Option<Value>]) -> Result<Key, DbError> {
    if pk.len() != schema.pk.len() {
        return Err(DbError::Row(RowError::Arity {
            expected: schema.pk.len(),
            got: pk.len(),
        }));
    }
    let mut cols = Vec::with_capacity(schema.pk.len());
    for &attnum in &schema.pk {
        let idx = usize::from(attnum).checked_sub(1);
        let Some(col) = idx.and_then(|i| schema.columns.get(i)) else {
            return Err(DbError::Row(RowError::Corrupt(
                "primary key names a missing attnum",
            )));
        };
        cols.push(KeyColumn::asc(col.ty.clone()));
    }
    let mut key = table_lo(sid);
    let start = key.len();
    encode_key(&cols, pk, &mut key)
        .map_err(|_| DbError::Row(RowError::Corrupt("primary key does not encode")))?;
    if key.len() - start > KEY_LIMIT {
        return Err(DbError::ProgramLimitExceeded);
    }
    Ok(key)
}

/// The row key of a hidden-rowid row (§3): `table_lo ‖ encode_key([bytea
/// ASC], rowid)` with `rowid = be32(epoch) be64(txn n) be32(seq0)
/// be32(ord)` — unique forever (§7.2), in the key only, never a column.
fn rowid_key(sid: u64, txn: &Txn, seq0: Seq, ord: u32) -> Result<Key, DbError> {
    let mut id = Vec::with_capacity(20);
    id.extend_from_slice(&txn.id.epoch.to_be_bytes());
    id.extend_from_slice(&txn.id.n.to_be_bytes());
    id.extend_from_slice(&seq0.to_be_bytes());
    id.extend_from_slice(&ord.to_be_bytes());
    let mut key = table_lo(sid);
    encode_key(
        &[KeyColumn::asc(KeyType::Bytea)],
        &[Some(Value::Bytea(id))],
        &mut key,
    )
    .map_err(|_| DbError::Row(RowError::Corrupt("rowid does not encode")))?;
    Ok(key)
}

/// The pk values of a row about to be written: one per pk column, in key
/// order, from the `live` datums (§2's attnum order).
fn pk_values(schema: &TableSchema, live: &[Option<Datum>]) -> Result<Vec<Option<Value>>, DbError> {
    let mut out = Vec::with_capacity(schema.pk.len());
    for &attnum in &schema.pk {
        let pos = schema.live_columns().position(|c| c.attnum == attnum);
        let Some(pos) = pos else {
            return Err(DbError::Row(RowError::Corrupt(
                "primary key names a dropped column",
            )));
        };
        match live.get(pos) {
            Some(None) => out.push(None),
            Some(Some(Datum::Inline(v))) => out.push(Some(v.clone())),
            Some(Some(Datum::Toast(_))) => {
                return Err(DbError::Row(RowError::Corrupt(
                    "primary-key column is out of line",
                )))
            }
            None => {
                return Err(DbError::Row(RowError::Arity {
                    expected: schema.live_columns().count(),
                    got: live.len(),
                }))
            }
        }
    }
    Ok(out)
}

/// [`pk_values`] for a row whose pk column may be **carried** out of line
/// (§5: the row's stored form points at its chunks, but the key does not
/// change). The row at that key encodes exactly the caller's pk values
/// (§3 — I-ROWFMT's own check), so a carried pointer resolves to them;
/// the value the caller read the row by is the value the new row keeps.
fn pk_values_resolved(
    schema: &TableSchema,
    live: &[Option<Datum>],
    caller_pk: &[Option<Value>],
) -> Result<Vec<Option<Value>>, DbError> {
    if caller_pk.len() != schema.pk.len() {
        return Err(DbError::Row(RowError::Arity {
            expected: schema.pk.len(),
            got: caller_pk.len(),
        }));
    }
    let mut out = Vec::with_capacity(schema.pk.len());
    for (i, &attnum) in schema.pk.iter().enumerate() {
        let pos = schema.live_columns().position(|c| c.attnum == attnum);
        let Some(pos) = pos else {
            return Err(DbError::Row(RowError::Corrupt(
                "primary key names a dropped column",
            )));
        };
        match live.get(pos) {
            Some(None) => out.push(None),
            Some(Some(Datum::Inline(v))) => out.push(Some(v.clone())),
            Some(Some(Datum::Toast(_))) => out.push(caller_pk[i].clone()),
            None => {
                return Err(DbError::Row(RowError::Arity {
                    expected: schema.live_columns().count(),
                    got: live.len(),
                }))
            }
        }
    }
    Ok(out)
}

/// A reader id this process never allocates (status ids start at 1, as
/// `rebuild_storage_map`'s `n: 0`): the ghost of §4's own-writes-ignoring
/// catalog read owns no intents, so the own-write branch of the read rule
/// never fires for it.
pub(crate) fn ghost_id<K: OrderedKv>(core: &Core<K>) -> TxnId {
    TxnId {
        epoch: core.epoch(),
        n: 0,
    }
}
