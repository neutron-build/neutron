//! C-T0 §10: the catalog — relation and index rows in `/sys/catalog`,
//! transactional DDL, storage ids.
//!
//! **Rows are versioned rows like any table** (§10): placed through the §5
//! write path (`insert_key`, `row_op`) and read through the §4 read path at
//! the caller's snapshot, with no special casing beyond the prefix. DDL is
//! thereby transactional — a DDL txn's catalog changes commit atomically
//! with it, `abort` and `ROLLBACK TO` restore them like any row, and the
//! catalog-snapshot rule falls out of versioning: a table created after a
//! reader's snapshot is invisible to it, the reader's reads of the old
//! storage still work, and a new txn sees the new table.
//!
//! **Storage ids and oids** (§10) come from persisted counters
//! (`/sys/next_storage_id`, `/sys/next_oid`), reserved in blocks of
//! [`ID_BLOCK`] and written `Durability::Yes` **before any id of a new
//! block is handed out** — a crash may skip ids, never repeat one. Each
//! counter's in-memory block position sits behind a leaf mutex; no latch,
//! registry, SSI or graph mutex is ever taken while holding it (§3.1).
//!
//! **DDL execution order** (the card's item 3): every operation first takes
//! `lock_relation(oid, AccessExclusive)` — a wait parks through the §6
//! protocol unchanged, so concurrent DML blocks and unblocks exactly as the
//! wait-for graph decides — then reads the catalog at the **latest
//! committed** state (§6: after acquiring, catalog lookups use the latest
//! committed catalog; once the lock is acquired every earlier DDL on the
//! relation is a visible commit), then drives the SSI hooks, then writes
//! the catalog rows through the write path. TRUNCATE follows the seed-60
//! order, now driven by the catalog: allocate the new storage id,
//! `unmap_storage(old)`, `note_retired(txn, oid, [old])`,
//! `map_storage(new, oid)`, and — at execution, while the txn can still be
//! chosen as a victim — `on_ddl_execute(txn, oid)`.
//!
//! **Not this card:** DROP's data-prefix deletion (the §9.2
//! retired-prefix `DeleteRange` once `W` passes the DDL) is GC/Drop-range
//! work; the catalog only notes the retirement. The `/sys/catalog/`
//! compaction-filter carve-out of §10 is also open: `TxnGcFilter` lives in
//! `gc/filter.rs`, which is outside this card's Touch-only scope, so the
//! filter still keeps every `/sys/` key — see the note in [`crate::gc`].
//!
//! **Boot rebuild** (§10, the card's item 6): [`Catalog::open`] scans the
//! committed catalog at `visible_ts` and rebuilds the SSI storage map
//! (`map_storage` per relation row's table range, and per index row's
//! entry ranges mapped to the owning relation, §12 Q7). It runs in the boot
//! sequence — after `Core::open` (which wrote the epoch and loaded every
//! persisted `Committed` record) and `Ssi::install`, before the first txn.
//! The guarantee relied on: §7.2 makes an older-epoch intent without a
//! record Aborted, and boot loads every persisted record, so the §4 read
//! path decides every catalog row from its intent plus its owner's status
//! — a committed-but-unresolved DDL reads as its own newest version and a
//! lost DDL is invisible. The scan is therefore resolution-order
//! insensitive and the eager rebuild is safe; resolution (§7.3, on
//! encounter or the sweep) only turns intents into the same bytes.
//!
//! **Abort and `ROLLBACK TO` of executed DDL:** the seed-60 hooks run at
//! execution, so the map changes are journalled per DDL statement and
//! undone when the txn aborts or rolls back to a savepoint below the
//! statement — through the [`ReleaseHook`] chain [`Catalog::open`]
//! installs around the lock manager's hook (§7.1 sets `Aborted` before the
//! hook runs, and commit step 5 runs it after `Committed`, so the status
//! tells the two apart; both precede the wake, so no waiter re-runs
//! against a half-undone map). A commit drops the journal entry and keeps
//! the map; the undo restores exactly the pre-statement map (a `Map` is
//! unmapped, an `Unmap` re-mapped).
//!
//! Two naming notes. The card writes these as `Core::catalog_create_table`
//! etc.; they live on [`Catalog`] because their state (the id counters and
//! the installed `Ssi`/`LockManager` handles) cannot be added to `Core`
//! within this card's file scope. And SSI's relation oids are `u32`
//! (`RelOid`, §8.1) while catalog oids are u64: a relation oid at or above
//! 2^32 is refused when it reaches SSI (an invariant error), which the
//! counter discipline makes unreachable in practice.

mod rows;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::Duration;

use nucleus_kv::{Batch, Durability, OrderedKv};

use crate::boot::Core;
use crate::commit::ReleaseHook;
use crate::locks::{LockManager, RelLockMode};
use crate::read::{self, NoSsi};
use crate::registry::ViewGuard;
use crate::ssi::{RelOid, Ssi};
use crate::status::Remembered;
use crate::txn::Txn;
use crate::visibility::ReadCtx;
use crate::write::{
    CommittedVersion, Epq, EpqDecision, LockWait, RowOp, RowOutcome, SsiHook, StmtCtx, UniqueRule,
};
use crate::{Seq, TxnError, TxnId};

pub use rows::{
    idx_key, idx_prefix, idx_prefix_end, index_ranges, next_oid_key, next_storage_id_key,
    parse_idx_key, parse_rel_key, rel_key, rel_prefix, rel_prefix_end, table_range, IdxRow,
    IdxStorage, KeyRange, RelKind, RelRow, SYS_CATALOG_PREFIX,
};

/// Ids are reserved in blocks of this many (§10: "reserved in blocks,
/// persisted before any id of a new block is used").
pub const ID_BLOCK: u64 = 64;

/// One DDL statement's position (§10, the card's `(txn, ctx, …)`
/// signatures): the core, txn and the statement ctx the row writes run
/// at, plus the §6 wait policy of the AccessExclusive acquisition. Build
/// with [`Ddl::new`] (blocking wait, no timeout) and `.wait(..)` /
/// `.lock_timeout(..)` to override.
pub struct Ddl<'a, K: OrderedKv> {
    /// The store the DDL runs on.
    pub core: &'a Core<K>,
    /// The DDL txn.
    pub txn: &'a Txn,
    /// The DDL statement's ctx (§5): the row writes' snapshot and seqs.
    pub ctx: StmtCtx,
    /// How `lock_relation(oid, AccessExclusive)` handles a conflict (§6).
    pub wait: LockWait,
    /// A blocking wait's `lock_timeout` (§6); `None` waits indefinitely.
    /// Wall-clock bounded — production configuration, never a test's.
    pub lock_timeout: Option<Duration>,
}

impl<'a, K: OrderedKv> Ddl<'a, K> {
    /// A DDL that blocks through the wait-for graph with no timeout.
    pub fn new(core: &'a Core<K>, txn: &'a Txn, ctx: StmtCtx) -> Ddl<'a, K> {
        Ddl {
            core,
            txn,
            ctx,
            wait: LockWait::Block,
            lock_timeout: None,
        }
    }

    /// Overrides the wait policy (§6: NOWAIT raises 55P03 instead of
    /// waiting; SKIP LOCKED skips).
    pub fn wait(mut self, wait: LockWait) -> Ddl<'a, K> {
        self.wait = wait;
        self
    }

    /// Bounds a blocking wait (§6 `lock_timeout`).
    pub fn lock_timeout(mut self, timeout: Duration) -> Ddl<'a, K> {
        self.lock_timeout = Some(timeout);
        self
    }
}

// ---- the DDL map journal (abort / ROLLBACK TO of executed DDL) -----------

/// One execution-time SSI map change, recorded before it runs so an abort
/// can undo it. Undoing a [`MapUndo::Map`] unmaps; undoing an
/// [`MapUndo::Unmap`] re-maps — together they restore the pre-statement
/// map exactly.
#[derive(Debug, Clone, PartialEq, Eq)]
enum MapUndo {
    Map {
        lo: Vec<u8>,
        hi: Vec<u8>,
        rel: RelOid,
    },
    Unmap {
        lo: Vec<u8>,
        hi: Vec<u8>,
        rel: RelOid,
    },
}

/// Per DDL txn, the map changes of each statement (tagged with the
/// statement's `seq0`, so `ROLLBACK TO` undoes exactly those taken at or
/// after the savepoint). Dropped at release: kept-map on commit, undone on
/// abort. A leaf mutex (§3.1): nothing later in the order is taken while
/// it is held (the undo takes the SSI mutex only after releasing it).
#[derive(Default)]
struct MapJournal {
    entries: Mutex<BTreeMap<TxnId, Vec<(Seq, MapUndo)>>>,
}

impl MapJournal {
    fn lock(&self) -> MutexGuard<'_, BTreeMap<TxnId, Vec<(Seq, MapUndo)>>> {
        self.entries.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn record(&self, txn: TxnId, seq: Seq, undo: MapUndo) {
        self.lock().entry(txn).or_default().push((seq, undo));
    }

    /// §7.1 abort: undo every entry of `txn` (last recorded first — the
    /// inverse order of the statement sequence), then drop the entry.
    fn undo_all(&self, txn: TxnId, ssi: &Ssi) {
        let undos = self.lock().remove(&txn).unwrap_or_default();
        for (_, undo) in undos.into_iter().rev() {
            apply_undo(ssi, &undo);
        }
    }

    /// Commit: keep the map, drop the journal entry.
    fn keep(&self, txn: TxnId) {
        self.lock().remove(&txn);
    }

    /// §5.5 `ROLLBACK TO s`: undo the entries tagged `>= s`, keep the rest.
    fn undo_from(&self, txn: TxnId, from: Seq, ssi: &Ssi) {
        let mut map = self.lock();
        let Some(entries) = map.get_mut(&txn) else {
            return;
        };
        let split = entries.partition_point(|(seq, _)| *seq < from);
        let undone = entries.split_off(split);
        for (_, undo) in undone.into_iter().rev() {
            apply_undo(ssi, &undo);
        }
    }
}

fn apply_undo(ssi: &Ssi, undo: &MapUndo) {
    match *undo {
        MapUndo::Map { ref lo, ref hi, .. } => ssi.unmap_storage(lo, hi),
        MapUndo::Unmap {
            ref lo,
            ref hi,
            rel,
        } => ssi.map_storage(lo, hi, rel),
    }
}

/// What [`CatalogRelease`] needs from the core: the txn's status, read as a
/// leaf (§3.1) to tell an abort's `release_all` from a commit's. The Ssi's
/// `RegistryHost` pattern.
trait ReleaseHost: Send + Sync {
    fn abort_status(&self, txn: TxnId) -> bool;
}

impl<K: OrderedKv> ReleaseHost for Core<K> {
    fn abort_status(&self, txn: TxnId) -> bool {
        matches!(
            self.status.lookup_remembered(txn),
            Remembered::Live(crate::TxnStatus::Aborted, _)
        )
    }
}

/// The [`ReleaseHook`] chain [`Catalog::open`] installs: every release
/// first runs the previous hook (the lock manager's — relation locks go
/// first), then the journal is settled — undone for an abort or a
/// `ROLLBACK TO`, kept for a commit. Both callers run it before the wake
/// (§7.1, §3 step 5), so a waiter that re-runs never sees a half-undone
/// map.
struct CatalogRelease {
    inner: Arc<dyn ReleaseHook>,
    journal: Weak<MapJournal>,
    ssi: Weak<Ssi>,
    host: Weak<dyn ReleaseHost>,
}

impl ReleaseHook for CatalogRelease {
    fn release_all(&self, txn: TxnId) {
        self.inner.release_all(txn);
        let (Some(journal), Some(ssi)) = (self.journal.upgrade(), self.ssi.upgrade()) else {
            return;
        };
        let aborted = self
            .host
            .upgrade()
            .is_none_or(|host| host.abort_status(txn));
        if aborted {
            journal.undo_all(txn, &ssi);
        } else {
            journal.keep(txn);
        }
    }

    fn release_from(&self, txn: TxnId, from_seq: Seq) {
        self.inner.release_from(txn, from_seq);
        let (Some(journal), Some(ssi)) = (self.journal.upgrade(), self.ssi.upgrade()) else {
            return;
        };
        journal.undo_from(txn, from_seq, &ssi);
    }
}

// ---- the persisted id counters ----------------------------------------------

/// The in-memory position inside the reserved block.
#[derive(Debug)]
struct IdBlock {
    /// The next id to hand out.
    next: u64,
    /// The exclusive end of the reserved block (`next == limit` = none
    /// left).
    limit: u64,
}

/// A persisted u64 counter plus its reserved block (§10). The mutex is a
/// leaf: nothing later in the §3.1 order is taken while it is held.
struct IdCounter {
    key: Vec<u8>,
    block: Mutex<IdBlock>,
}

impl IdCounter {
    fn new(key: Vec<u8>) -> IdCounter {
        IdCounter {
            key,
            block: Mutex::new(IdBlock { next: 0, limit: 0 }),
        }
    }

    fn lock(&self) -> MutexGuard<'_, IdBlock> {
        self.block.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Hands out the next id. When the block is exhausted, first reads the
    /// persisted counter (the end of the last reserved block), then writes
    /// `persisted + ID_BLOCK` with `Durability::Yes`, and only then moves
    /// the in-memory block — so every handed-out id is below a value that
    /// is already durable (a crash may skip ids, never repeat one).
    ///
    /// The persisted read is `latest_get` under the leaf mutex: no intent
    /// ever exists on a `/sys/next_*` key (no removal path can touch it)
    /// and the mutex serializes every writer, so the latest state is exact
    /// — the registered-view discipline of §3.1 exists for data keys with
    /// intents, not for this single-writer system key.
    fn alloc<K: OrderedKv>(&self, core: &Core<K>) -> Result<u64, TxnError> {
        let mut b = self.lock();
        if b.next == b.limit {
            let base = match core.latest_get(&self.key)? {
                Some(v) if v.len() == 8 => {
                    let mut n = [0u8; 8];
                    n.copy_from_slice(&v);
                    u64::from_be_bytes(n)
                }
                Some(v) => {
                    return Err(TxnError::Corrupt(format!(
                        "{} of {} bytes, expected 8",
                        String::from_utf8_lossy(&self.key),
                        v.len()
                    )))
                }
                None => 0,
            };
            // The persisted value is the end of the last reserved block;
            // ids below it may have been used, so reservation continues
            // upward from it even if this Catalog's block was lost.
            let base = base.max(b.limit);
            let limit = base.checked_add(ID_BLOCK).ok_or_else(|| {
                TxnError::Invariant(format!(
                    "{} space exhausted (C-T0 §10)",
                    String::from_utf8_lossy(&self.key)
                ))
            })?;
            let mut batch = Batch::default();
            batch.put(self.key.clone(), limit.to_be_bytes().to_vec());
            // §10: persisted (synced) before any id of the new block is
            // used.
            core.write(batch, Durability::Yes)?;
            b.limit = limit;
            b.next = base;
        }
        let id = b.next;
        b.next += 1;
        Ok(id)
    }
}

// ---- the catalog ------------------------------------------------------------

/// The catalog (§10): the id counters plus weak handles to the `Ssi` and
/// `LockManager` [`Catalog::open`] verified. Not tied to a `Core` — every
/// method takes the core it acts on, so one catalog serves a reopened store
/// only through a fresh `Catalog::open` (which re-reads the counters and
/// rebuilds the SSI map).
pub struct Catalog {
    ssi: Weak<Ssi>,
    locks: Weak<LockManager>,
    /// The execution-time map changes of live DDL txns (undone at abort /
    /// `ROLLBACK TO` through the [`CatalogRelease`] chain).
    journal: Arc<MapJournal>,
    oids: IdCounter,
    storage_ids: IdCounter,
}

impl Catalog {
    /// Opens the catalog: rebuilds the SSI storage map from the committed
    /// catalog (the card's item 6; see the module docs for the guarantee
    /// this relies on), chains the journal-settling [`ReleaseHook`] around
    /// whatever hook the core carries (the lock manager's, §7.1/§3 step 5),
    /// and starts the id counters at their persisted values. Call once per
    /// boot, after `Ssi::install` and `LockManager::install` on the core,
    /// before the first txn.
    pub fn open<K: OrderedKv>(
        core: &Arc<Core<K>>,
        ssi: &Arc<Ssi>,
        locks: &Arc<LockManager>,
    ) -> Result<Catalog, TxnError> {
        let installed = core.ssi_hook();
        if !Arc::ptr_eq(&installed, &(Arc::clone(ssi) as Arc<dyn SsiHook>)) {
            return Err(TxnError::Invariant(
                "Catalog::open: the Ssi given is not the core's installed SsiHook".into(),
            ));
        }
        rebuild_storage_map(core, ssi)?;
        let journal = Arc::new(MapJournal::default());
        let host: Arc<dyn ReleaseHost> = Arc::clone(core) as Arc<dyn ReleaseHost>;
        core.set_release_hook(Arc::new(CatalogRelease {
            inner: core.release_hook(),
            journal: Arc::downgrade(&journal),
            ssi: Arc::downgrade(ssi),
            host: Arc::downgrade(&host),
        }));
        Ok(Catalog {
            ssi: Arc::downgrade(ssi),
            locks: Arc::downgrade(locks),
            journal,
            oids: IdCounter::new(next_oid_key()),
            storage_ids: IdCounter::new(next_storage_id_key()),
        })
    }

    fn ssi(&self) -> Result<Arc<Ssi>, TxnError> {
        self.ssi
            .upgrade()
            .ok_or_else(|| TxnError::Invariant("the Ssi opened with this catalog is gone".into()))
    }

    fn locks(&self) -> Result<Arc<LockManager>, TxnError> {
        self.locks.upgrade().ok_or_else(|| {
            TxnError::Invariant("the LockManager opened with this catalog is gone".into())
        })
    }

    /// Allocates one storage id (§10) — the primitive `CREATE INDEX` with
    /// an explicit [`IdxStorage::Id`] and any table-rewriting ALTER composes
    /// from. Reserved in blocks, persisted before use, never reused.
    pub fn alloc_storage_id<K: OrderedKv>(&self, core: &Core<K>) -> Result<u64, TxnError> {
        self.storage_ids.alloc(core)
    }

    /// Allocates one catalog oid (§10), same discipline as
    /// [`Catalog::alloc_storage_id`].
    pub fn alloc_oid<K: OrderedKv>(&self, core: &Core<K>) -> Result<u64, TxnError> {
        self.oids.alloc(core)
    }

    // ---- reads (§4 at the caller's snapshot) --------------------------------

    /// Relation `oid`'s catalog row at the reader's snapshot, from one
    /// registered view (§3.1, §4) — the catalog-snapshot rule. A
    /// SERIALIZABLE reader must instead go through `Ssi::read_key` on
    /// [`rel_key`] (the SIREAD registers before the view opens, §8.1);
    /// this function registers no SIREAD.
    pub fn read_rel<K: OrderedKv>(
        core: &Core<K>,
        view: &ViewGuard<'_, K::Snap>,
        ctx: &ReadCtx,
        oid: u64,
    ) -> Result<Option<RelRow>, TxnError> {
        match read::read_key(core, view, &rel_key(oid), ctx, &mut NoSsi)? {
            Some(bytes) => Ok(Some(RelRow::decode(&bytes)?)),
            None => Ok(None),
        }
    }

    /// Index `oid`'s catalog row at the reader's snapshot ([`Catalog::read_rel`]).
    pub fn read_idx<K: OrderedKv>(
        core: &Core<K>,
        view: &ViewGuard<'_, K::Snap>,
        ctx: &ReadCtx,
        oid: u64,
    ) -> Result<Option<IdxRow>, TxnError> {
        match read::read_key(core, view, &idx_key(oid), ctx, &mut NoSsi)? {
            Some(bytes) => Ok(Some(IdxRow::decode(&bytes)?)),
            None => Ok(None),
        }
    }

    /// Every visible relation row at `ctx.snapshot`, in oid order. The boot
    /// rebuild and the SQL layer's name resolution walk this.
    pub fn scan_rels<K: OrderedKv>(
        core: &Core<K>,
        view: &ViewGuard<'_, K::Snap>,
        ctx: &ReadCtx,
    ) -> Result<Vec<(u64, RelRow)>, TxnError> {
        let mut out = Vec::new();
        let mut no_ssi = NoSsi;
        let iter = read::scan(
            core,
            view,
            (
                std::ops::Bound::Included(rel_prefix().as_slice()),
                std::ops::Bound::Excluded(rel_prefix_end().as_slice()),
            ),
            ctx,
            &mut no_ssi,
        );
        for (key, value) in iter.collect::<Result<Vec<_>, _>>()? {
            match parse_rel_key(&key) {
                Some(oid) => out.push((oid, RelRow::decode(&value)?)),
                None => {
                    return Err(TxnError::Corrupt(format!(
                        "catalog rel key {key:?} outside the oid layout"
                    )))
                }
            }
        }
        Ok(out)
    }

    /// Every visible index row owned by `rel_oid` at `ctx.snapshot` (the
    /// whole `idx` family scanned and filtered; a by-owner sub-index is an
    /// engine concern, not a 2.0 catalog one).
    pub fn scan_idxs_of_rel<K: OrderedKv>(
        core: &Core<K>,
        view: &ViewGuard<'_, K::Snap>,
        ctx: &ReadCtx,
        rel_oid: u64,
    ) -> Result<Vec<IdxRow>, TxnError> {
        let mut out = Vec::new();
        let mut no_ssi = NoSsi;
        let iter = read::scan(
            core,
            view,
            (
                std::ops::Bound::Included(idx_prefix().as_slice()),
                std::ops::Bound::Excluded(idx_prefix_end().as_slice()),
            ),
            ctx,
            &mut no_ssi,
        );
        for (key, value) in iter.collect::<Result<Vec<_>, _>>()? {
            let row = IdxRow::decode(&value)?;
            match parse_idx_key(&key) {
                Some(oid) if oid == row.oid => {}
                Some(_) => {
                    return Err(TxnError::Corrupt(format!(
                        "catalog idx key {key:?} disagrees with its row's oid"
                    )))
                }
                None => {
                    return Err(TxnError::Corrupt(format!(
                        "catalog idx key {key:?} outside the oid layout"
                    )))
                }
            }
            if row.owning_rel == rel_oid {
                out.push(row);
            }
        }
        Ok(out)
    }

    // ---- DDL (§6, §10): AE lock first, then the SSI hooks, then rows ----

    /// `CREATE TABLE`-shaped DDL (the card's `Core::catalog_create_table`):
    /// allocates the oid and the initial table storage id, takes
    /// `lock_relation(oid, AccessExclusive)`, maps the new storage range,
    /// and inserts the `/sys/catalog/rel/{oid}` row through the write path
    /// — transactional, so an abort leaves no row. Returns `(oid,
    /// storage_id)`. No `on_ddl_execute`: a fresh oid has no SIREADs to
    /// conflict with (§8.2's DDL check is for DROP/TRUNCATE/rewrite).
    pub fn catalog_create_table<K: OrderedKv>(
        &self,
        ddl: &Ddl<'_, K>,
        name: &[u8],
        kind: RelKind,
        def: &[u8],
    ) -> Result<(u64, u64), TxnError> {
        let Ddl {
            core,
            txn,
            ctx,
            wait,
            lock_timeout,
        } = ddl;
        let oid = self.oids.alloc(core)?;
        let storage_id = self.storage_ids.alloc(core)?;
        let ssi = self.ssi()?;
        self.locks()?.lock_relation(
            core,
            txn,
            oid,
            RelLockMode::AccessExclusive,
            *wait,
            *lock_timeout,
        )?;
        let (lo, hi) = table_range(storage_id);
        let rel = rel_oid_of(oid)?;
        self.journal.record(
            txn.id,
            ctx.seq0(),
            MapUndo::Map {
                lo: lo.clone(),
                hi: hi.clone(),
                rel,
            },
        );
        ssi.map_storage(&lo, &hi, rel);
        let row = RelRow {
            oid,
            kind,
            storage_id,
            name: name.to_vec(),
            def: def.to_vec(),
        };
        core.insert_key(
            txn,
            &rel_key(oid),
            None,
            row.encode()?,
            (*ctx).clone(),
            UniqueRule::None,
        )?;
        Ok((oid, storage_id))
    }

    /// `DROP TABLE`-shaped DDL (the card's `Core::catalog_drop_table`):
    /// AccessExclusive first, then the latest committed catalog row and its
    /// indexes; every storage range of the relation (table and each
    /// [`IdxStorage`] index) is unmapped and noted retired, `on_ddl_execute`
    /// runs at execution (§8.2, seed 60), and the rel and idx rows are
    /// deleted through the write path — transactional. **The data-prefix
    /// `DeleteRange` is not here**: §9.2 retires once `W` passes the DDL,
    /// which is GC/Drop-range work; this only records the retirement.
    pub fn catalog_drop_table<K: OrderedKv>(
        &self,
        ddl: &Ddl<'_, K>,
        oid: u64,
    ) -> Result<(), TxnError> {
        let Ddl {
            core,
            txn,
            ctx,
            wait,
            lock_timeout,
        } = ddl;
        let ssi = self.ssi()?;
        self.locks()?.lock_relation(
            core,
            txn,
            oid,
            RelLockMode::AccessExclusive,
            *wait,
            *lock_timeout,
        )?;
        let rel = rel_oid_of(oid)?;
        let row = Self::latest_rel_row(core, txn, ctx, oid)?
            .ok_or_else(|| catalog_miss("relation", oid))?;
        let idxs = Self::latest_idx_rows_of(core, txn, ctx, oid)?;
        let mut retired = vec![table_range(row.storage_id)];
        self.journal.record(
            txn.id,
            ctx.seq0(),
            MapUndo::Unmap {
                lo: retired[0].0.clone(),
                hi: retired[0].1.clone(),
                rel,
            },
        );
        ssi.unmap_storage(&retired[0].0, &retired[0].1);
        for idx in &idxs {
            for (lo, hi) in idx.storage.ranges() {
                self.journal.record(
                    txn.id,
                    ctx.seq0(),
                    MapUndo::Unmap {
                        lo: lo.clone(),
                        hi: hi.clone(),
                        rel,
                    },
                );
                ssi.unmap_storage(&lo, &hi);
                retired.push((lo, hi));
            }
        }
        ssi.note_retired(txn.id, rel, retired);
        ssi.on_ddl_execute(txn.id, txn.isolation, rel)?;
        write_catalog_row(core, txn, ctx, &rel_key(oid), RowOp::Delete)?;
        for idx in &idxs {
            write_catalog_row(core, txn, ctx, &idx_key(idx.oid), RowOp::Delete)?;
        }
        Ok(())
    }

    /// `TRUNCATE`-shaped DDL (the card's `Core::catalog_truncate_table`):
    /// AccessExclusive first, then the seed-60 order — allocate the new
    /// table storage id, `unmap_storage(old)`, `note_retired(txn, oid,
    /// [old])`, `map_storage(new, oid)`, `on_ddl_execute` at execution —
    /// then the rel row's update through the write path. Every
    /// [`IdxStorage::Id`] index of the relation is retired and re-allocated
    /// with it (§9.2 retires table and every index); an explicit-range
    /// index keeps its range (no storage id embeds its prefix). Returns the
    /// new table storage id. As with DROP, the retired prefixes' data
    /// deletion is GC/Drop-range work, not this card.
    pub fn catalog_truncate_table<K: OrderedKv>(
        &self,
        ddl: &Ddl<'_, K>,
        oid: u64,
    ) -> Result<u64, TxnError> {
        let Ddl {
            core,
            txn,
            ctx,
            wait,
            lock_timeout,
        } = ddl;
        let ssi = self.ssi()?;
        self.locks()?.lock_relation(
            core,
            txn,
            oid,
            RelLockMode::AccessExclusive,
            *wait,
            *lock_timeout,
        )?;
        let rel = rel_oid_of(oid)?;
        let row = Self::latest_rel_row(core, txn, ctx, oid)?
            .ok_or_else(|| catalog_miss("relation", oid))?;
        let idxs = Self::latest_idx_rows_of(core, txn, ctx, oid)?;
        let new_storage = self.storage_ids.alloc(core)?;

        let (old_lo, old_hi) = table_range(row.storage_id);
        self.journal.record(
            txn.id,
            ctx.seq0(),
            MapUndo::Unmap {
                lo: old_lo.clone(),
                hi: old_hi.clone(),
                rel,
            },
        );
        ssi.unmap_storage(&old_lo, &old_hi);
        ssi.note_retired(txn.id, rel, vec![(old_lo.clone(), old_hi.clone())]);
        let (new_lo, new_hi) = table_range(new_storage);
        self.journal.record(
            txn.id,
            ctx.seq0(),
            MapUndo::Map {
                lo: new_lo.clone(),
                hi: new_hi.clone(),
                rel,
            },
        );
        ssi.map_storage(&new_lo, &new_hi, rel);

        // Id-storage indexes follow the table: retire the old ranges,
        // re-allocate, map the new ones. The row rewrites happen after the
        // SSI hooks, like the rel row's.
        let mut idx_updates: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for idx in &idxs {
            let IdxStorage::Id(old_sid) = idx.storage else {
                continue; // an explicit range: no storage id to retire
            };
            let mut retired = Vec::new();
            for (lo, hi) in index_ranges(old_sid) {
                self.journal.record(
                    txn.id,
                    ctx.seq0(),
                    MapUndo::Unmap {
                        lo: lo.clone(),
                        hi: hi.clone(),
                        rel,
                    },
                );
                ssi.unmap_storage(&lo, &hi);
                retired.push((lo, hi));
            }
            let new_sid = self.storage_ids.alloc(core)?;
            for (lo, hi) in index_ranges(new_sid) {
                self.journal.record(
                    txn.id,
                    ctx.seq0(),
                    MapUndo::Map {
                        lo: lo.clone(),
                        hi: hi.clone(),
                        rel,
                    },
                );
                ssi.map_storage(&lo, &hi, rel);
            }
            ssi.note_retired(txn.id, rel, retired);
            idx_updates.push((idx_key(idx.oid), idx.with_storage_id(new_sid).encode()?));
        }

        // At execution (seed 60): with the retirement noted and the new
        // storage mapped, while the txn can still be a victim.
        ssi.on_ddl_execute(txn.id, txn.isolation, rel)?;

        write_catalog_row(
            core,
            txn,
            ctx,
            &rel_key(oid),
            RowOp::Update {
                value: row.with_storage(new_storage).encode()?,
                key_cols_changed: false,
            },
        )?;
        for (key, value) in &idx_updates {
            write_catalog_row(
                core,
                txn,
                ctx,
                key,
                RowOp::Update {
                    value: value.clone(),
                    key_cols_changed: false,
                },
            )?;
        }
        Ok(new_storage)
    }

    /// `CREATE INDEX`-shaped DDL (the card's `Core::catalog_create_index`,
    /// "likewise"): AccessExclusive on the **owning relation**, the row
    /// inserted through the write path, and every entry range of the index
    /// mapped to the owner (§12 Q7). Returns `(index oid, the mapped
    /// ranges)`. No `on_ddl_execute`: nothing is retired and a new index's
    /// ranges had no readers.
    pub fn catalog_create_index<K: OrderedKv>(
        &self,
        ddl: &Ddl<'_, K>,
        owning_rel: u64,
        storage: IdxStorage,
        def: &[u8],
    ) -> Result<(u64, Vec<KeyRange>), TxnError> {
        let Ddl {
            core,
            txn,
            ctx,
            wait,
            lock_timeout,
        } = ddl;
        let ssi = self.ssi()?;
        self.locks()?.lock_relation(
            core,
            txn,
            owning_rel,
            RelLockMode::AccessExclusive,
            *wait,
            *lock_timeout,
        )?;
        let rel = rel_oid_of(owning_rel)?;
        // §6: after acquiring, the latest committed catalog decides — the
        // owner must exist.
        Self::latest_rel_row(core, txn, ctx, owning_rel)?
            .ok_or_else(|| catalog_miss("relation", owning_rel))?;
        let oid = self.oids.alloc(core)?;
        let ranges = storage.ranges();
        for (lo, hi) in &ranges {
            self.journal.record(
                txn.id,
                ctx.seq0(),
                MapUndo::Map {
                    lo: lo.clone(),
                    hi: hi.clone(),
                    rel,
                },
            );
            ssi.map_storage(lo, hi, rel);
        }
        let row = IdxRow {
            oid,
            owning_rel,
            storage,
            def: def.to_vec(),
        };
        core.insert_key(
            txn,
            &idx_key(oid),
            None,
            row.encode()?,
            (*ctx).clone(),
            UniqueRule::None,
        )?;
        Ok((oid, ranges))
    }

    /// `DROP INDEX`-shaped DDL (the card's `Core::catalog_drop_index`):
    /// reads the index row to learn its owner, takes AccessExclusive on the
    /// owner, re-reads the row under the lock (§6 freshness), retires and
    /// unmaps the entry ranges (noted against the owner — §8.6's promotion
    /// is keyed by relation oid), runs `on_ddl_execute` at execution
    /// (§8.2's "any storage of the relation" covers a reader that scanned
    /// the index), and deletes the row through the write path.
    pub fn catalog_drop_index<K: OrderedKv>(
        &self,
        ddl: &Ddl<'_, K>,
        idx_oid: u64,
    ) -> Result<(), TxnError> {
        let Ddl {
            core,
            txn,
            ctx,
            wait,
            lock_timeout,
        } = ddl;
        let ssi = self.ssi()?;
        let owner = match Self::latest_idx_row(core, txn, ctx, idx_oid)? {
            Some(row) => row.owning_rel,
            None => return Err(catalog_miss("index", idx_oid)),
        };
        self.locks()?.lock_relation(
            core,
            txn,
            owner,
            RelLockMode::AccessExclusive,
            *wait,
            *lock_timeout,
        )?;
        let rel = rel_oid_of(owner)?;
        // Re-read under the lock: a concurrent DROP INDEX of the same index
        // may have committed while we waited for the owner's AE.
        let Some(row) = Self::latest_idx_row(core, txn, ctx, idx_oid)? else {
            return Ok(()); // dropped by the txn that held the lock before us
        };
        let ranges = row.storage.ranges();
        for (lo, hi) in &ranges {
            self.journal.record(
                txn.id,
                ctx.seq0(),
                MapUndo::Unmap {
                    lo: lo.clone(),
                    hi: hi.clone(),
                    rel,
                },
            );
            ssi.unmap_storage(lo, hi);
        }
        ssi.note_retired(txn.id, rel, ranges);
        ssi.on_ddl_execute(txn.id, txn.isolation, rel)?;
        write_catalog_row(core, txn, ctx, &idx_key(idx_oid), RowOp::Delete)?;
        Ok(())
    }

    // ---- latest-committed catalog reads (§6) --------------------------------

    /// The relation row at the latest committed catalog, seen through a
    /// registered view at `visible_ts` with the DDL txn's own earlier
    /// statements visible (`stmt_seq = ctx.seq0()`).
    fn latest_rel_row<K: OrderedKv>(
        core: &Core<K>,
        txn: &Txn,
        ctx: &StmtCtx,
        oid: u64,
    ) -> Result<Option<RelRow>, TxnError> {
        let view = core.open_view();
        let rc = latest_ctx(core, txn, ctx);
        Catalog::read_rel(core, &view, &rc, oid)
    }

    fn latest_idx_row<K: OrderedKv>(
        core: &Core<K>,
        txn: &Txn,
        ctx: &StmtCtx,
        idx_oid: u64,
    ) -> Result<Option<IdxRow>, TxnError> {
        let view = core.open_view();
        let rc = latest_ctx(core, txn, ctx);
        Catalog::read_idx(core, &view, &rc, idx_oid)
    }

    fn latest_idx_rows_of<K: OrderedKv>(
        core: &Core<K>,
        txn: &Txn,
        ctx: &StmtCtx,
        rel_oid: u64,
    ) -> Result<Vec<IdxRow>, TxnError> {
        let view = core.open_view();
        let rc = latest_ctx(core, txn, ctx);
        Catalog::scan_idxs_of_rel(core, &view, &rc, rel_oid)
    }
}

// ---- helpers ----------------------------------------------------------------

/// The catalog's place in the §3.1 lock order is none at all: this leaf
/// read only builds a `ReadCtx`.
fn latest_ctx<K: OrderedKv>(core: &Core<K>, txn: &Txn, ctx: &StmtCtx) -> ReadCtx {
    ReadCtx {
        txn: txn.id,
        snapshot: core.visible_ts(),
        stmt_seq: ctx.seq0(),
    }
}

/// SSI's relation oids are u32 (§8.1); catalog oids are u64.
fn rel_oid_of(oid: u64) -> Result<RelOid, TxnError> {
    u32::try_from(oid).map_err(|_| {
        TxnError::Invariant(format!(
            "relation oid {oid} does not fit SSI's u32 RelOid (C-T0 §8.1)"
        ))
    })
}

fn catalog_miss(what: &str, oid: u64) -> TxnError {
    // 42P01/42704 are the SQL layer's mapping; the txn layer has no such
    // variant, so the invariant carries the name.
    TxnError::Invariant(format!(
        "catalog: {what} {oid} not found (undefined_object)"
    ))
}

/// One row-op write on a catalog row through the §5 path. A skip cannot
/// happen under the AccessExclusive the DDL holds — it would mean the row
/// vanished — so it is an invariant, not a silent success.
fn write_catalog_row<K: OrderedKv>(
    core: &Core<K>,
    txn: &Txn,
    ctx: &StmtCtx,
    key: &[u8],
    op: RowOp,
) -> Result<(), TxnError> {
    let mut epq = DdlEpq { op: op.clone() };
    match core.row_op(txn, key, None, op, ctx.clone(), &mut epq)? {
        RowOutcome::Applied => Ok(()),
        RowOutcome::Skipped(reason) => Err(TxnError::Invariant(format!(
            "catalog row {key:?} skipped under AccessExclusive ({reason:?})"
        ))),
    }
}

/// The DDL statements' EvalPlanQual (§5.2): DDL has no user quals — a row
/// still live is re-applied, a row gone (tombstone) skips. The
/// AccessExclusive lock makes the skip unreachable in practice; it exists
/// so RC DDL composes with §5.2 like any row op.
struct DdlEpq {
    op: RowOp,
}

impl Epq for DdlEpq {
    fn recheck(&mut self, newest: &CommittedVersion) -> EpqDecision {
        if newest.is_tombstone() {
            EpqDecision::Skip
        } else {
            EpqDecision::Apply(self.op.clone())
        }
    }
}

// ---- the boot rebuild (§10, item 6) -----------------------------------------

/// Scans the committed catalog at `visible_ts` and rebuilds the SSI storage
/// map: every visible relation row maps its table range to its oid, every
/// visible index row maps its entry ranges to the owning relation (§12 Q7).
/// See [`Catalog::open`]'s docs and the module docs for the resolution
/// guarantee that makes the eager rebuild safe.
fn rebuild_storage_map<K: OrderedKv>(core: &Arc<Core<K>>, ssi: &Arc<Ssi>) -> Result<(), TxnError> {
    let view = core.open_view();
    // A TxnId this process never allocates (status ids start at 1): the
    // rebuild owns no intents, so §4's own-write branch never fires.
    let ctx = ReadCtx {
        txn: TxnId {
            epoch: core.epoch(),
            n: 0,
        },
        snapshot: core.visible_ts(),
        stmt_seq: 0,
    };
    for (oid, row) in Catalog::scan_rels(core, &view, &ctx)? {
        let rel = rel_oid_of(oid)?;
        let (lo, hi) = table_range(row.storage_id);
        ssi.map_storage(&lo, &hi, rel);
    }
    // Index rows: the whole family (no owner filter at boot).
    let mut no_ssi = NoSsi;
    let iter = read::scan(
        core,
        &view,
        (
            std::ops::Bound::Included(idx_prefix().as_slice()),
            std::ops::Bound::Excluded(idx_prefix_end().as_slice()),
        ),
        &ctx,
        &mut no_ssi,
    );
    for (key, value) in iter.collect::<Result<Vec<_>, _>>()? {
        let row = IdxRow::decode(&value)?;
        if parse_idx_key(&key) != Some(row.oid) {
            return Err(TxnError::Corrupt(format!(
                "catalog idx key {key:?} disagrees with its row's oid"
            )));
        }
        let rel = rel_oid_of(row.owning_rel)?;
        for (lo, hi) in row.storage.ranges() {
            ssi.map_storage(&lo, &hi, rel);
        }
    }
    Ok(())
}
