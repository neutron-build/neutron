//! The typed table layer over `nucleus-txn` (C-T6 §2-§4): the [`Db`]
//! handle, one statement's position ([`Stmt`], the §3 ordinal counter),
//! `open_table` with the §4 relation rule, and the first DML/reads —
//! insert (hidden-rowid keys, the §3 2704-byte key limit), point get and
//! scan (§5's `[table_lo, table_lo ‖ 0xFF)` bound, toast chunks excluded).
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

use nucleus_codec::row::{Datum, RowError, TableSchema};
use nucleus_codec::{encode_key, KeyColumn, KeyType, Value};
use nucleus_kv::{Key, OrderedKv};
use nucleus_txn::boot::Core;
use nucleus_txn::catalog::{table_range, Catalog, IdxStorage};
use nucleus_txn::locks::{LockManager, RelLockMode};
use nucleus_txn::read::{read_key, scan, NoSsi};
use nucleus_txn::ssi::Ssi;
use nucleus_txn::txn::{Isolation, Txn};
use nucleus_txn::visibility::ReadCtx;
use nucleus_txn::write::{LockWait, StmtCtx, UniqueRule};
use nucleus_txn::{Seq, Ts, TxnError, TxnId};

use crate::defs::IndexDef;
use crate::error::DbError;

/// The encoded primary key is limited to this many bytes (§3); the insert
/// that exceeds it fails with 54000 ([`DbError::ProgramLimitExceeded`]).
pub const KEY_LIMIT: usize = 2704;

/// A table opened by [`Db::open_table`]: the catalog as this statement sees
/// it (§4: the latest committed catalog plus the txn's own earlier
/// statements), pinned for the statement's DML and reads.
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

    /// INSERT: RowExclusive on the relation (§4), the row encoded under
    /// the handle's (latest) definition, then a key-existence insert of
    /// the row key with the primary-key unique rule. A table with any
    /// index on the handle refuses with 0A000 — index maintenance is
    /// C-T6b2a. Hidden-rowid tables draw `ord` from the statement's
    /// counter ([`Stmt::next_ord`]), so two inserts of one statement can
    /// never collide.
    ///
    /// §4 expects the writing statement's catalog read to happen under
    /// RowExclusive or stronger: open the handle with
    /// [`RelLockMode::RowExclusive`] in the same statement. The RowExclusive
    /// taken here upgrades a weaker handle's lock too, but the handle's
    /// catalog read is the one its statement made.
    pub fn insert(
        &self,
        st: &Stmt<'_>,
        t: &TableHandle,
        live: &[Option<Datum>],
    ) -> Result<(), DbError> {
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
        if !t.indexes.is_empty() {
            return Err(DbError::FeatureNotSupported("index maintenance: C-T6b2a"));
        }
        let value = t.schema.encode_row(live)?;
        let key = if t.schema.hidden_rowid {
            let ord = st.next_ord()?;
            rowid_key(t.storage_id, st.txn, st.seq0, ord)?
        } else {
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
        Ok(())
    }

    /// Point read of the primary-key row `pk` (one value per pk column,
    /// `None` = NULL) at the statement's snapshot, decoded under the
    /// handle's definition. SERIALIZABLE registers its SIREAD
    /// (§8.1); other levels read through a plain registered view.
    /// A hidden-rowid table has no callable key: 0A000 (scan it instead).
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
            Some(b) => Ok(Some(t.schema.decode_row(&b)?)),
            None => Ok(None),
        }
    }

    /// Every row of the table at the statement's snapshot, in key order,
    /// decoded under the handle's definition. The scan is bounded to
    /// `[table_lo, table_lo ‖ 0xFF)` (§5): `0xFF` never starts an encoded
    /// key, so large-value chunks (`/t/{sid}/0xFF…`) are never rows.
    /// SERIALIZABLE registers the range SIREAD (§8.1).
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
            out.push((key, t.schema.decode_row(&bytes)?));
        }
        Ok(out)
    }
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
fn chunk_prefix(lo: &[u8]) -> Key {
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

/// A reader id this process never allocates (status ids start at 1, as
/// `rebuild_storage_map`'s `n: 0`): the ghost of §4's own-writes-ignoring
/// catalog read owns no intents, so the own-write branch of the read rule
/// never fires for it.
fn ghost_id<K: OrderedKv>(core: &Core<K>) -> TxnId {
    TxnId {
        epoch: core.epoch(),
        n: 0,
    }
}
