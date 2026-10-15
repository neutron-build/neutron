//! C-T6 §7's online (non-unique) index build driver: `CREATE INDEX`
//! outside a transaction block and `CREATE INDEX CONCURRENTLY`. Each step
//! is its own ReadCommitted txn holding `ShareUpdateExclusive` on the
//! relation from its entry to its catalog write (§7: the lock is taken
//! right after `find` names the owner, so the re-read, the wait, the scan
//! and the ingest all run under it — the catalog call's own SUE
//! re-acquisition is a re-entrant no-op for the same txn; never
//! SERIALIZABLE), every re-read runs at the latest committed catalog, and
//! a step that finds the index row absent or in another state than it
//! expects fails the build with 55000 ("index was dropped or changed
//! concurrently").
//!
//! The ladder ([`create_index_online`]):
//!
//! - **T0** — [`Catalog::catalog_create_index_with`] at
//!   `ShareUpdateExclusive`: the index row lands `DeleteOnly` with a
//!   fresh storage id and its entry ranges mapped; commit
//!   `SyncCommit::On`, wait for the ack.
//! - **T1** — lock, re-read (expect `DeleteOnly`),
//!   [`LockManager::wait_for_lockers`](nucleus_txn::locks::LockManager::wait_for_lockers)`(rel, Share)`
//!   (§6: waits out every writer that read an older state), rewrite the
//!   state to `WriteOnly`; commit, ack. After this step's ack every
//!   writer maintains at least deletes (§7.3).
//! - **T2 (fence)** — lock, re-read (expect `WriteOnly`),
//!   `wait_for_lockers(rel, Share)`, rewrite the row with its def
//!   **unchanged**; commit, ack; that commit ts is `f`. After it every
//!   txn that commits at or after `f` read `WriteOnly` or later.
//! - **T3** — lock, re-read (expect `WriteOnly`, using the **current**
//!   storage ids — a TRUNCATE between steps re-allocates them), register
//!   the snapshot `S` (so `S >= f`: the fence's ack preceded it), scan
//!   `[table_lo, table_lo ‖ 0xFF)` at `S` with a ghost ctx, compute each
//!   live row's §6 entry (indexed toasted columns detoasted at `S`, the
//!   §6 key limit 54000), sort by logical key, and ingest every entry at
//!   exactly `S` through
//!   [`Core::ingest_versions`](nucleus_txn::boot::Core::ingest_versions)
//!   in sorted chunks of [`BUILD_CHUNK`] (C-T0 §10: the committed and
//!   aborted intents of each chunk's key span are resolved and the WAL
//!   synced first); drop the guard, rewrite the row `Public { built_at =
//!   S }`, commit.
//!
//! [`resume_build`] restarts the ladder from the committed state:
//! `DeleteOnly` at T1, `WriteOnly` at T2 (the fence runs again — its wait
//! is what makes the resumed build's own `S >= f'` carry the same
//! guarantee), `Public` is a no-op.
//!
//! Failure (§7): any error after T0 — a plain build drops its own index
//! in a fresh txn (best effort, through
//! [`Catalog::catalog_drop_index`]); a CONCURRENTLY build leaves the
//! non-public index (PostgreSQL's INVALID) for [`resume_build`] or DROP
//! INDEX. After a crash, [`Db::open`](crate::Db::open) runs
//! [`Db::cleanup_unfinished_builds`](crate::Db::cleanup_unfinished_builds)
//! — every non-`Public` index with `concurrently == false` is dropped
//! before the handle is returned (§9.5); a CONCURRENTLY one is left.
//!
//! [`BuildHooks`] are blocking callbacks the tests (C-T6e2a) use to
//! observe and to force interleavings between the steps. They are not a
//! production seam: the SQL layer calls [`create_index_online`] outside a
//! transaction block only, and routes `CREATE INDEX CONCURRENTLY` inside a
//! block to
//! [`ddl::create_index_concurrently_in_block`](crate::ddl::create_index_concurrently_in_block)
//! (25001) instead.

use std::ops::Bound;

use nucleus_codec::row::TableSchema;
use nucleus_kv::{Key, OrderedKv};
use nucleus_txn::catalog::{index_ranges, Catalog, Ddl, IdxRow, IdxStorage, RelRow};
use nucleus_txn::commit::SyncCommit;
use nucleus_txn::locks::RelLockMode;
use nucleus_txn::read::{scan, NoSsi};
use nucleus_txn::txn::{Isolation, Txn};
use nucleus_txn::visibility::ReadCtx;
use nucleus_txn::write::LockWait;
use nucleus_txn::Ts;

use crate::db::{chunk_prefix, ghost_id, table_lo, Db, IndexHandle, Stmt, TableHandle};
use crate::defs::{IndexDef, IndexState};
use crate::error::DbError;
use crate::{index, toast};

/// §7 step 6: the entries are ingested in sorted chunks of this many;
/// [`BuildHooks::chunk`] overrides it for tests. Never 0.
pub const BUILD_CHUNK: usize = 4096;

/// §7's 55000 message: a step found the index row absent, or not in the
/// state it expected.
const CHANGED: &str = "index was dropped or changed concurrently";

/// Blocking observation points around the ladder's steps (§7), default
/// none. Every hook runs on the driver's thread, inside the named step's
/// txn where one is open, and may itself drive the store (the C-T6e2a
/// interleaving tests do); a hook that blocks blocks the build.
#[derive(Default)]
pub struct BuildHooks {
    /// After T0's commit was acked (no txn open).
    pub after_t0: Option<Box<dyn Fn() + Send + Sync>>,
    /// In T1, after the re-read and the `DeleteOnly` check, before
    /// `wait_for_lockers`.
    pub before_t1_wait: Option<Box<dyn Fn() + Send + Sync>>,
    /// In T1, after `wait_for_lockers` returned, before the catalog write
    /// (T1's txn still open, still holding ShareUpdateExclusive).
    pub before_t1_commit: Option<Box<dyn Fn() + Send + Sync>>,
    /// After T1's commit was acked (no txn open).
    pub after_t1: Option<Box<dyn Fn() + Send + Sync>>,
    /// In T2, after the re-read and the `WriteOnly` check, before
    /// `wait_for_lockers`.
    pub before_fence_wait: Option<Box<dyn Fn() + Send + Sync>>,
    /// After T2's commit was acked (no txn open); its commit ts was `f`.
    pub after_fence: Option<Box<dyn Fn() + Send + Sync>>,
    /// In T3, right after the snapshot `S` was registered, with `S` itself.
    pub after_register: Option<Box<dyn Fn(Ts) + Send + Sync>>,
    /// In T3, after the scan (every entry computed, nothing ingested yet);
    /// the `gc_guard` test drives a WriteOnly-aware delete and a GC round
    /// here, before the ingest.
    pub after_scan: Option<Box<dyn Fn() + Send + Sync>>,
    /// In T3, after the ingest and the guard drop, before the `Public`
    /// catalog write.
    pub before_publish: Option<Box<dyn Fn() + Send + Sync>>,
    /// Overrides [`BUILD_CHUNK`] for tests (clamped to >= 1).
    pub chunk: Option<usize>,
}

fn run(h: &Option<Box<dyn Fn() + Send + Sync>>) {
    if let Some(f) = h {
        f();
    }
}

fn run_ts(h: &Option<Box<dyn Fn(Ts) + Send + Sync>>, ts: Ts) {
    if let Some(f) = h {
        f(ts);
    }
}

/// The §7 online build driver. Runs its own RC txns (one per step) and
/// blocks for each commit's ack, so the core must have a commit pipeline
/// running. The SQL layer calls this **outside a transaction block only**;
/// `CREATE INDEX CONCURRENTLY` inside a block is
/// [`ddl::create_index_concurrently_in_block`](crate::ddl::create_index_concurrently_in_block)'s
/// 25001.
///
/// `def`'s `state` and `built_at` are the ladder's business and are
/// overridden; `def.unique` is refused with 0A000 **before T0** (§7.2:
/// the online path never builds a unique index — plain CREATE UNIQUE
/// INDEX outside a block routes to
/// [`ddl::create_index_own_txn`](crate::ddl::create_index_own_txn),
/// CONCURRENTLY is 0A000 either way). `concurrently` sets the def's flag
/// bit (§6), which is what makes a failed or crashed build survive
/// [`Db::cleanup_unfinished_builds`](crate::Db::cleanup_unfinished_builds).
///
/// Returns the index oid. On any error after T0 the failure rule of §7
/// runs: a plain build drops the half-built index (best effort) and
/// returns the error; a CONCURRENTLY build leaves it non-public for
/// [`resume_build`] or DROP INDEX.
pub fn create_index_online<K: OrderedKv>(
    db: &Db<K>,
    rel_oid: u64,
    def: &IndexDef,
    concurrently: bool,
    hooks: &BuildHooks,
) -> Result<u64, DbError> {
    if def.unique {
        return Err(DbError::FeatureNotSupported(
            "CREATE UNIQUE INDEX: the online path never builds unique indexes (C-T6 §7.2)",
        ));
    }
    // T0: the DeleteOnly row, committed and acked before anything else.
    // A failure here leaves nothing committed, so there is nothing to undo.
    let idx_oid = t0(db, rel_oid, def, concurrently, hooks)?;
    match ladder(db, idx_oid, hooks, Stage::Fresh) {
        Ok(()) => Ok(idx_oid),
        Err(e) => {
            // §7 Failure: a plain build drops its own index; a
            // CONCURRENTLY one is left non-public.
            if !concurrently {
                stop(db, idx_oid);
            }
            Err(e)
        }
    }
}

/// Resumes a non-public build (§7 Resume): `DeleteOnly` restarts at T1,
/// `WriteOnly` at T2 (the fence is re-run, so the resumed build's `S`
/// carries the same ordering guarantee), `Public` is a no-op. The
/// committed def's `concurrently` flag picks the failure rule if the
/// resume fails. Returns the index oid.
pub fn resume_build<K: OrderedKv>(
    db: &Db<K>,
    idx_oid: u64,
    hooks: &BuildHooks,
) -> Result<u64, DbError> {
    let (_, idx) = find(db, idx_oid)?;
    let def = IndexDef::decode(&idx.def)?;
    let stage = match def.state {
        IndexState::Public => return Ok(idx_oid),
        IndexState::DeleteOnly => Stage::FromDeleteOnly,
        IndexState::WriteOnly => Stage::FromWriteOnly,
    };
    match ladder(db, idx_oid, hooks, stage) {
        Ok(()) => Ok(idx_oid),
        Err(e) => {
            if !def.concurrently {
                stop(db, idx_oid);
            }
            Err(e)
        }
    }
}

/// Where [`ladder`] picks up: a fresh build runs T1 and T2; a resume
/// enters at the state it found.
enum Stage {
    Fresh,
    FromDeleteOnly,
    FromWriteOnly,
}

fn ladder<K: OrderedKv>(
    db: &Db<K>,
    idx_oid: u64,
    hooks: &BuildHooks,
    stage: Stage,
) -> Result<(), DbError> {
    match stage {
        Stage::Fresh | Stage::FromDeleteOnly => {
            t1(db, idx_oid, hooks)?;
            t2(db, idx_oid, hooks)?;
        }
        Stage::FromWriteOnly => {
            t2(db, idx_oid, hooks)?;
        }
    }
    t3(db, idx_oid, hooks)
}

// ---- the steps ------------------------------------------------------------

/// T0: create the `DeleteOnly` row in its own RC txn (the
/// ShareUpdateExclusive is taken inside
/// [`Catalog::catalog_create_index_with`], before its catalog reads), then
/// commit `SyncCommit::On` and wait for the ack.
fn t0<K: OrderedKv>(
    db: &Db<K>,
    rel_oid: u64,
    def: &IndexDef,
    concurrently: bool,
    hooks: &BuildHooks,
) -> Result<u64, DbError> {
    let txn = db.core.begin(Isolation::ReadCommitted);
    let body = (|| -> Result<u64, DbError> {
        let st = Stmt::new(&txn, db.core.visible_ts())?;
        let storage_id = db
            .catalog
            .alloc_storage_id(&db.core)
            .map_err(DbError::Txn)?;
        let def = IndexDef {
            state: IndexState::DeleteOnly,
            built_at: 0,
            concurrently,
            ..def.clone()
        };
        let ddl = Ddl::new(&db.core, &txn, st.ctx());
        let (idx_oid, _) = db
            .catalog
            .catalog_create_index_with(
                &ddl,
                rel_oid,
                IdxStorage::Id(storage_id),
                &def.encode()?,
                RelLockMode::ShareUpdateExclusive,
            )
            .map_err(DbError::Txn)?;
        Ok(idx_oid)
    })();
    let idx_oid = finish(db, txn, body)?;
    run(&hooks.after_t0);
    Ok(idx_oid)
}

/// T1: re-read at the latest committed catalog (expect `DeleteOnly`), wait
/// the old lockers out at `Share`, rewrite the state to `WriteOnly`.
/// §7's step ordering: the ShareUpdateExclusive is taken at the step's
/// entry (right after `find`) and held to the catalog write below.
fn t1<K: OrderedKv>(db: &Db<K>, idx_oid: u64, hooks: &BuildHooks) -> Result<(), DbError> {
    let txn = db.core.begin(Isolation::ReadCommitted);
    let body = (|| -> Result<(), DbError> {
        let st = Stmt::new(&txn, db.core.visible_ts())?;
        let owner = find(db, idx_oid)?.0;
        lock_sue(db, &txn, owner)?;
        let (_, def, _) = reread(db, &st, owner, idx_oid, IndexState::DeleteOnly)?;
        run(&hooks.before_t1_wait);
        db.locks
            .wait_for_lockers(&db.core, &txn, owner, RelLockMode::Share, None)
            .map_err(DbError::Txn)?;
        run(&hooks.before_t1_commit);
        let next = IndexDef {
            state: IndexState::WriteOnly,
            ..def
        };
        let ddl = Ddl::new(&db.core, &txn, st.ctx());
        db.catalog
            .catalog_alter_idx_def(
                &ddl,
                idx_oid,
                &next.encode()?,
                RelLockMode::ShareUpdateExclusive,
            )
            .map_err(DbError::Txn)?;
        Ok(())
    })();
    finish(db, txn, body)?;
    run(&hooks.after_t1);
    Ok(())
}

/// T2, the fence: re-read (expect `WriteOnly`), wait at `Share`, rewrite
/// the row with its def unchanged; the commit ts is `f`. T3's snapshot is
/// taken after this call's ack returned, so `S >= f` — the §7.3 argument
/// that every txn committed at `S` is WriteOnly-aware rests on it.
/// §7's step ordering: the ShareUpdateExclusive is taken at the step's
/// entry (right after `find`) and held to the catalog write below.
fn t2<K: OrderedKv>(db: &Db<K>, idx_oid: u64, hooks: &BuildHooks) -> Result<(), DbError> {
    let txn = db.core.begin(Isolation::ReadCommitted);
    let body = (|| -> Result<(), DbError> {
        let st = Stmt::new(&txn, db.core.visible_ts())?;
        let owner = find(db, idx_oid)?.0;
        lock_sue(db, &txn, owner)?;
        let (_, def, _) = reread(db, &st, owner, idx_oid, IndexState::WriteOnly)?;
        run(&hooks.before_fence_wait);
        db.locks
            .wait_for_lockers(&db.core, &txn, owner, RelLockMode::Share, None)
            .map_err(DbError::Txn)?;
        // The rewrite is the point, not the bytes: a new version of an
        // unchanged def, committed at its own ts `f` after the wait.
        let ddl = Ddl::new(&db.core, &txn, st.ctx());
        db.catalog
            .catalog_alter_idx_def(
                &ddl,
                idx_oid,
                &def.encode()?,
                RelLockMode::ShareUpdateExclusive,
            )
            .map_err(DbError::Txn)?;
        Ok(())
    })();
    finish(db, txn, body)?;
    run(&hooks.after_fence);
    Ok(())
}

/// T3: the scan at `S`, the chunked ingest, the publish. See the module
/// docs; the S guard stays registered until the last ingest returned.
/// §7's step ordering: the ShareUpdateExclusive is taken at the step's
/// entry (right after `find`), so the re-read that captures the storage
/// ids, the scan and the ingest all run under it, through the publish.
fn t3<K: OrderedKv>(db: &Db<K>, idx_oid: u64, hooks: &BuildHooks) -> Result<(), DbError> {
    let txn = db.core.begin(Isolation::ReadCommitted);
    let body = (|| -> Result<(), DbError> {
        let st = Stmt::new(&txn, db.core.visible_ts())?;
        let owner = find(db, idx_oid)?.0;
        lock_sue(db, &txn, owner)?;
        // The current storage ids: a TRUNCATE between steps
        // re-allocated them, and the scan must use the new ones.
        let (rel, def, isid) = reread(db, &st, owner, idx_oid, IndexState::WriteOnly)?;
        let schema = TableSchema::decode(&rel.def)?;
        // §7 step 4: register S (>= f by construction — the fence's
        // ack preceded this txn), and keep it registered until the
        // ingest below returns.
        let guard = db.core.registry.take_snapshot();
        let s = guard.ts();
        run_ts(&hooks.after_register, s);
        // Step 5: every row live at S, its §6 entry computed.
        let ghost = ReadCtx {
            txn: ghost_id(&db.core),
            snapshot: s,
            stmt_seq: 0,
        };
        let mut entries = scan_entries(db, &rel, &schema, &def, idx_oid, isid, &ghost)?;
        // §7 step 5: sort by stored key.
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        run(&hooks.after_scan);
        // Step 6: the chunked ingest, every entry at S. The guard is
        // still registered, so W <= S and GC never removes a version
        // the scan derived an entry from (§7.3).
        let chunk_len = hooks.chunk_len();
        let ranges: Vec<(Key, Key)> = index_ranges(isid);
        for part in entries.chunks(chunk_len) {
            let mut chunk = part
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect::<Vec<(Key, Vec<u8>)>>()
                .into_iter();
            db.core
                .ingest_versions(&guard, &ranges, &mut chunk)
                .map_err(DbError::Txn)?;
        }
        drop(guard);
        run(&hooks.before_publish);
        // Step 7: publish. `built_at = S` makes the index serve every
        // snapshot at or above the one the entries were ingested at.
        let public = IndexDef {
            state: IndexState::Public,
            built_at: s.0,
            ..def
        };
        let ddl = Ddl::new(&db.core, &txn, st.ctx());
        db.catalog
            .catalog_alter_idx_def(
                &ddl,
                idx_oid,
                &public.encode()?,
                RelLockMode::ShareUpdateExclusive,
            )
            .map_err(DbError::Txn)?;
        Ok(())
    })();
    finish(db, txn, body)
}

// ---- the pieces every step shares ------------------------------------------

/// §7's step entry: take `ShareUpdateExclusive` on the owning relation
/// for the step's txn, blocking (no timeout). Held to the step's catalog
/// write: the re-read, the wait, the scan and the ingest all run under
/// it, so a concurrent DROP INDEX or another build's step on the same
/// relation cannot land inside the step's window — it parks, and the
/// next step's `find`/`reread` turns it into the 55000. The step's
/// catalog call re-acquires SUE re-entrantly (a no-op for the same
/// txn), and the lock releases with the txn (commit §3 step 5 / the
/// §7.1 abort).
fn lock_sue<K: OrderedKv>(db: &Db<K>, txn: &Txn, owner: u64) -> Result<(), DbError> {
    db.locks
        .lock_relation(
            &db.core,
            txn,
            owner,
            RelLockMode::ShareUpdateExclusive,
            LockWait::Block,
            None,
        )
        .map(|_| ())
        .map_err(DbError::Txn)
}

/// Commits `txn` (`SyncCommit::On`, waiting for the ack) when the body
/// succeeded, aborts it (§7.1) when it failed. The ack is what makes the
/// next step's re-read see this step's row (I-ACK), and — for the fence —
/// what orders `S >= f`.
fn finish<K: OrderedKv, T>(db: &Db<K>, txn: Txn, body: Result<T, DbError>) -> Result<T, DbError> {
    match body {
        Ok(out) => {
            db.core.commit(txn, SyncCommit::On).map_err(DbError::Txn)?;
            Ok(out)
        }
        Err(e) => {
            db.core.abort(txn).map_err(DbError::Txn)?;
            Err(e)
        }
    }
}

/// The index row's owner, read at the latest committed catalog through a
/// ghost ctx (a build txn owns no intents before its first write). An
/// absent row is §7's 55000, not 42704: the row existed when the build
/// started, so its absence means a concurrent DROP INDEX.
fn find<K: OrderedKv>(db: &Db<K>, idx_oid: u64) -> Result<(u64, IdxRow), DbError> {
    let ctx = ReadCtx {
        txn: ghost_id(&db.core),
        snapshot: db.core.visible_ts(),
        stmt_seq: 0,
    };
    let row = {
        let view = db.core.open_view();
        Catalog::read_idx(&db.core, &view, &ctx, idx_oid).map_err(DbError::Txn)?
    };
    match row {
        Some(row) => Ok((row.owning_rel, row)),
        None => Err(changed()),
    }
}

/// The step's post-lock re-read: both rows at the latest committed
/// catalog (§6 freshness under the ShareUpdateExclusive the step holds),
/// the def decoded, the state checked (§7's 55000), the index's **current**
/// storage id returned. An absent relation row is 42P01 (the whole
/// relation was dropped); an absent index row is 55000.
fn reread<K: OrderedKv>(
    db: &Db<K>,
    st: &Stmt<'_>,
    owner: u64,
    idx_oid: u64,
    expect: IndexState,
) -> Result<(RelRow, IndexDef, u64), DbError> {
    let ctx = ReadCtx {
        txn: st.txn().id,
        snapshot: db.core.visible_ts(),
        stmt_seq: st.seq0(),
    };
    let rel = {
        let view = db.core.open_view();
        Catalog::read_rel(&db.core, &view, &ctx, owner).map_err(DbError::Txn)?
    }
    .ok_or(DbError::UndefinedTable)?;
    let idx = {
        let view = db.core.open_view();
        Catalog::read_idx(&db.core, &view, &ctx, idx_oid).map_err(DbError::Txn)?
    }
    .ok_or_else(changed)?;
    let def = IndexDef::decode(&idx.def)?;
    if def.state != expect {
        return Err(changed());
    }
    let IdxStorage::Id(isid) = idx.storage else {
        return Err(DbError::FeatureNotSupported("index storage ranges: C-T6b2"));
    };
    Ok((rel, def, isid))
}

/// §7 step 5: every row live at `ctx.snapshot` (§5's
/// `[table_lo, table_lo ‖ 0xFF)` bound — chunks are never rows), decoded
/// under the latest def, its indexed toasted columns detoasted at the same
/// snapshot, and its §6 entry computed with the insert-side key limit
/// (over 2704 → 54000: "a build that meets one fails with 54000").
fn scan_entries<K: OrderedKv>(
    db: &Db<K>,
    rel: &RelRow,
    schema: &TableSchema,
    def: &IndexDef,
    idx_oid: u64,
    isid: u64,
    ctx: &ReadCtx,
) -> Result<Vec<(Key, Vec<u8>)>, DbError> {
    let lo = table_lo(rel.storage_id);
    let hi = chunk_prefix(&lo);
    let view = db.core.open_view();
    let rows = scan(
        &db.core,
        &view,
        (
            Bound::Included(lo.as_slice()),
            Bound::Excluded(hi.as_slice()),
        ),
        ctx,
        &mut NoSsi,
    )
    .collect::<Result<Vec<(Key, Vec<u8>)>, _>>()
    .map_err(DbError::Txn)?;
    drop(view);
    // The §6 entry computer's handle shapes (ghost-owned: they borrow no
    // txn state).
    let table = TableHandle {
        txn: ghost_id(&db.core),
        seq0: 0,
        lock: RelLockMode::AccessShare,
        oid: rel.oid,
        storage_id: rel.storage_id,
        schema: schema.clone(),
        def_version: schema.version,
        indexes: Vec::new(),
    };
    let handle = IndexHandle {
        oid: idx_oid,
        storage_id: isid,
        def: def.clone(),
    };
    let mut out = Vec::with_capacity(rows.len());
    for (row_key, bytes) in rows {
        let mut live = schema.decode_row(&bytes)?;
        toast::detoast_row_at(&db.core, schema, rel.storage_id, &mut live, ctx)?;
        let (key, value, _rule) = index::entry_for_insert(&table, &handle, &row_key, &live)?;
        out.push((key, value));
    }
    Ok(out)
}

/// §7 Failure: a plain build drops its half-built index in a fresh txn,
/// best effort (a concurrent DROP INDEX that won the race makes
/// `catalog_drop_index` a no-op; any failure of the drop itself is
/// swallowed — the caller sees the build's error, §7's remedy).
fn stop<K: OrderedKv>(db: &Db<K>, idx_oid: u64) {
    let txn = db.core.begin(Isolation::ReadCommitted);
    let dropped = (|| -> Result<(), DbError> {
        let st = Stmt::new(&txn, db.core.visible_ts())?;
        let ddl = Ddl::new(&db.core, &txn, st.ctx());
        db.catalog
            .catalog_drop_index(&ddl, idx_oid)
            .map_err(DbError::Txn)?;
        Ok(())
    })();
    match dropped {
        Ok(()) => {
            let _ = db.core.commit(txn, SyncCommit::On);
        }
        Err(_) => {
            let _ = db.core.abort(txn);
        }
    }
}

fn changed() -> DbError {
    DbError::ObjectNotInPrerequisiteState(CHANGED)
}

impl BuildHooks {
    /// The ingest chunk length: [`BuildHooks::chunk`] when set (at least
    /// 1), else [`BUILD_CHUNK`].
    fn chunk_len(&self) -> usize {
        self.chunk.unwrap_or(BUILD_CHUNK).max(1)
    }
}
