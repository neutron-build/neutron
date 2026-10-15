//! C-T6e2b: the §7.3 correctness argument bound by deterministic,
//! hook-driven interleaving tests (blocking `BuildHooks` callbacks; the
//! tests react to whichever hook — or `WaitHook` — fires first; no
//! wall-clock verdicts, every wait bounded).
//!
//! - `c1_interleaving`: a writer whose handle predates the whole index
//!   (empty index list, no maintenance at all) commits its update between
//!   T1's re-read and T1's wait — T1's wait never collects it — and a
//!   WriteOnly-aware writer re-shapes the row after T1; the scan at S
//!   covers the committed version and the published index is exact
//!   (mutant G7: T0 lands WriteOnly and the ladder skips T1 — the
//!   WriteOnly-aware writer's entry outlives the row version it was
//!   computed from).
//! - `g3_interleaving`: a DeleteOnly-aware writer V that opens inside
//!   T1's window (after the wait) is the fence's business: the build's
//!   wait parks on V and V commits from the wait hook at `c_V < S` (the
//!   scan covers its row); with the fence's wait skipped (mutant G3) the
//!   row commits above S with no entry and the published index is wrong.
//! - `delete_reinsert_in_deleteonly` / `shadowing_older_intent`: a
//!   committed DeleteOnly-era intent on the entry key — same-txn
//!   delete+re-insert, or an older writer's delete under a younger
//!   re-insert — would shadow `E@S` (§4 step 1) unless the ingest's
//!   resolution pass removes it first (mutant G10).
//! - `sync_off_crash`: a `synchronous_commit = off` writer the scan saw;
//!   the ingest's WAL sync makes its commit durable before any derived
//!   entry, so the crash at `before_publish` leaves no entry without its
//!   row (mutant G8: the sync skipped — the row dies, the ingested entry
//!   survives).
//! - `delete_between_scan_and_ingest`: a WriteOnly-aware delete of the
//!   scanned row between the scan and the ingest lands its blind
//!   tombstone above `E@S` (mutant G6: a non-blind entry delete skips —
//!   the ingested entry dangles).
//! - `drop_column_vs_online_t0`: a DROP COLUMN that drops the half-built
//!   index while the build is at `after_t0` surfaces as the build's own
//!   55000; amcheck stays clean.
//! - `truncate_mid_t3` (e2a-gate rider): a TRUNCATE attempting to commit
//!   inside T3 after the step's re-read — it parks on the step's
//!   ShareUpdateExclusive; the pinned invariant is the surviving state's
//!   soundness (amcheck clean, `index_scan == scan`), whichever mechanism
//!   produced it.
//! - `late_sue_acquirer` (e2a-gate rider): a ShareUpdateExclusive
//!   acquirer starting after the fence's re-read — the build's outcome
//!   stays sound and the acquirer's commit lands at or above the fence's
//!   `f`.

mod t6b_support;

use std::ops::Bound;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use t6b_support::build_rig::BuildRig;
use t6b_support::{i32_d, i32_v, idx_def, ok, set_col, wait_until, SharedFault};

use nucleus_catalog::{
    create_index_online, BuildHooks, Db, IndexDef, IndexState, ScannedRow, Stmt,
};
use nucleus_codec::row::{ColumnDef, Datum, TableSchema};
use nucleus_codec::{KeyType, Value};
use nucleus_txn::catalog::{Catalog, Ddl, IdxStorage};
use nucleus_txn::commit::SyncCommit;
use nucleus_txn::jobs::{BootSweep, JobOutcome};
use nucleus_txn::locks::RelLockMode;
use nucleus_txn::resolver::Resolver;
use nucleus_txn::txn::{Isolation, Txn};
use nucleus_txn::visibility::ReadCtx;
use nucleus_txn::wait::WaitHook;
use nucleus_txn::write::StmtCtx;
use nucleus_txn::{Ts, TxnId};

/// Everything a thread may wait on is bounded by this; the joins use it
/// too (a hang is a failure, never a pass).
const BOUND: Duration = Duration::from_secs(30);

// ---- fixtures and read helpers ----------------------------------------------

/// `CREATE TABLE t (id int4 PRIMARY KEY, a int4)` — the card's `a` is the
/// indexed column (attnum 2), int-valued so `a = 3/5/6/7/9` is literal.
fn a_schema() -> TableSchema {
    ok(TableSchema::new(
        vec![
            ColumnDef {
                attnum: 1,
                name: "id".into(),
                ty: KeyType::Int4,
                not_null: true,
                dropped: false,
                missing: None,
            },
            ColumnDef {
                attnum: 2,
                name: "a".into(),
                ty: KeyType::Int4,
                not_null: false,
                dropped: false,
                missing: None,
            },
        ],
        vec![1],
    ))
}

/// The plain non-unique online-built index on `a` (attnum 2); the driver
/// overrides state and built_at.
fn a_def() -> IndexDef {
    idx_def(2, false, IndexState::DeleteOnly)
}

/// The table plus one committed row per `(id, a)` pair, each its own txn
/// (distinct, single commit ts per row).
fn table_a(rig: &BuildRig, name: &[u8], rows: &[(i32, i32)]) -> u64 {
    let t = rig.rc();
    let s = rig.core().visible_ts();
    let st = rig.stmt(&t, s);
    let (oid, _) = ok(rig.db.create_table(&st, name, &a_schema()));
    ok(rig.commit_rc(t));
    for &(id, a) in rows {
        insert_a(&rig.db, oid, id, a);
    }
    oid
}

/// One committed insert in its own txn (the writer-thread shape), then a
/// resolver pass.
fn insert_a(db: &Db<SharedFault>, oid: u64, id: i32, a: i32) {
    let t = db.core.begin(Isolation::ReadCommitted);
    let st = ok(Stmt::new(&t, db.core.visible_ts()));
    let h = ok(db.open_table(&st, oid, RelLockMode::RowExclusive));
    ok(db.insert_with(&st, &h, &[i32_d(id), i32_d(a)]));
    ok(db.core.commit(t, SyncCommit::On));
    ok(Resolver::run_once(&db.core));
}

/// `core.commit(SyncCommit::On)` plus a resolver pass — the shape every
/// hook-driven writer on the build thread uses.
fn commit_and_settle(db: &Db<SharedFault>, t: Txn) -> Ts {
    let ts = ok(db.core.commit(t, SyncCommit::On));
    ok(Resolver::run_once(&db.core));
    ts
}

/// A fresh RC reader over the rig: runs `f` with its statement (the
/// statement borrows the txn, so both stay inside this call) and commits.
fn with_reader<T>(
    rig: &BuildRig,
    f: impl FnOnce(&Stmt<'_>) -> Result<T, nucleus_catalog::DbError>,
) -> T {
    let t = rig.rc();
    let st = rig.stmt(&t, rig.core().visible_ts());
    let out = ok(f(&st));
    ok(rig.commit_rc(t));
    out
}

/// The `(id, a)` projection of scanned rows, sorted (order-independent).
fn ids_a_of(rows: &[ScannedRow]) -> Vec<(i32, i32)> {
    let mut out = Vec::with_capacity(rows.len());
    for (_, live) in rows {
        let id = match live.first() {
            Some(Some(Datum::Inline(Value::Int4(n)))) => *n,
            other => panic!("id datum: {other:?}"),
        };
        let a = match live.get(1) {
            Some(Some(Datum::Inline(Value::Int4(n)))) => *n,
            other => panic!("a datum: {other:?}"),
        };
        out.push((id, a));
    }
    out.sort();
    out
}

/// Every row of the table at the latest state, as `(id, a)` pairs.
fn scan_a(rig: &BuildRig, oid: u64) -> Vec<(i32, i32)> {
    let rows = with_reader(rig, |st| {
        let h = ok(rig.db.open_table(st, oid, RelLockMode::AccessShare));
        rig.db.scan(st, &h)
    });
    ids_a_of(&rows)
}

/// `index_scan` for exactly `a = value` on `idx_oid` at the latest state
/// (an included lower and upper bound over the one key column).
fn iscan_a(rig: &BuildRig, idx_oid: u64, value: i32) -> Vec<(i32, i32)> {
    let rows = with_reader(rig, |st| {
        rig.db.index_scan(
            st,
            idx_oid,
            Bound::Included(vec![i32_v(value)]),
            Bound::Included(vec![i32_v(value)]),
        )
    });
    ids_a_of(&rows)
}

/// `index_scan` over the whole index at the latest state.
fn iscan_all(rig: &BuildRig, idx_oid: u64) -> Vec<(i32, i32)> {
    let rows = with_reader(rig, |st| {
        rig.db
            .index_scan(st, idx_oid, Bound::Unbounded, Bound::Unbounded)
    });
    ids_a_of(&rows)
}

/// The relation's single visible index oid at the latest committed
/// catalog (a ghost reader), for hooks that must name the just-created
/// index before the driver has returned its oid.
fn only_idx_of(db: &Db<SharedFault>, oid: u64) -> u64 {
    let core = &db.core;
    let view = core.open_view();
    let ctx = ReadCtx {
        txn: TxnId {
            epoch: core.epoch(),
            n: u64::MAX,
        },
        snapshot: core.visible_ts(),
        stmt_seq: 1,
    };
    let idxs = ok(Catalog::scan_idxs_of_rel(core, &view, &ctx, oid));
    assert_eq!(idxs.len(), 1, "exactly one index on the relation");
    idxs[0].oid
}

/// The committed def of an index row read from `db` (hooks run on the
/// build thread, where the rig itself is not reachable).
fn def_of_db(db: &Db<SharedFault>, idx_oid: u64) -> IndexDef {
    let core = &db.core;
    let view = core.open_view();
    let ctx = ReadCtx {
        txn: TxnId {
            epoch: core.epoch(),
            n: u64::MAX,
        },
        snapshot: core.visible_ts(),
        stmt_seq: 1,
    };
    let row = ok(Catalog::read_idx(core, &view, &ctx, idx_oid)).expect("idx row committed");
    ok(IndexDef::decode(&row.def))
}

/// Runs `f` on a thread and returns the receiver for a bounded join.
fn spawn_run<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> mpsc::Receiver<T> {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let _ = tx.send(f());
    });
    rx
}

/// One release kit: the go sender that releases a parked writer and the
/// done receiver its commit acks on. Whichever hook (or `WaitHook`) fires
/// first takes the kit from under the mutex, sends the go and waits for
/// the done — a `Receiver` is `!Sync`, so it lives here, behind the lock.
type ReleaseKit<M> = Arc<Mutex<Option<(mpsc::Sender<M>, mpsc::Receiver<()>)>>>;

/// The by-handle writer M of `c1_interleaving`: opens its own handle
/// (so it reads the index's current state — WriteOnly-aware), updates
/// row 1's `a` to `to`, commits and settles.
fn m_update_a(db: &Db<SharedFault>, oid: u64, to: i32) {
    let t = db.core.begin(Isolation::ReadCommitted);
    let st = ok(Stmt::new(&t, db.core.visible_ts()));
    let h = ok(db.open_table(&st, oid, RelLockMode::RowExclusive));
    assert_eq!(h.indexes.len(), 1, "M sees the index");
    assert_eq!(
        h.indexes[0].def.state,
        IndexState::WriteOnly,
        "M is WriteOnly-aware"
    );
    let n = ok(db.update_with(&st, &h, &[i32_v(1)], &[set_col(2, i32_d(to))]));
    assert!(n, "M's update applied");
    ok(db.core.commit(t, SyncCommit::On));
    ok(Resolver::run_once(&db.core));
}

/// The boot sweep driven to `Finished` plus one resolver pass — the
/// reopen sequence that settles every older-epoch intent (§7.2).
fn sweep(rig: &BuildRig) {
    let mut sweep = BootSweep::new(Arc::clone(rig.core()));
    // The walk is over a finite store; a bound only turns a livelock
    // into a test failure.
    for _ in 0..100_000 {
        match ok(sweep.run_once()) {
            JobOutcome::Finished => break,
            JobOutcome::Progress | JobOutcome::Idle => continue,
        }
    }
    ok(Resolver::run_once(rig.core()));
}

// ---- 1: the C1 interleaving (a pre-index writer plus a WriteOnly one) -------

/// §7.3's first argument: a writer U whose statement read the catalog
/// before the index existed (its handle's index list is empty — it
/// maintains nothing, not even deletes) commits **between T1's re-read
/// and T1's `wait_for_lockers`**, so T1 never collects it. Sound because
/// U's commit precedes the fence, so `S > c_U` and the scan at S covers
/// the version U wrote; and a WriteOnly-aware M afterwards re-shapes the
/// row with full maintenance. The published index is exact.
///
/// Mutant G7 (T0 creates `WriteOnly` directly, the driver starts at the
/// fence, no T1): M runs WriteOnly-aware *before* U's un-maintained
/// update, its entry `E(5)` is committed at `c_M1`, and U's update
/// (a 5→6, no maintenance) leaves it dangling — the scan at S sees
/// `a = 6`, so `E(5)` outlives the row version it was computed from:
/// amcheck reports it and `index_scan(a=5)` fails its verification.
#[test]
fn c1_interleaving() {
    let rig = BuildRig::new();
    let oid = table_a(&rig, b"c1", &[(1, 3)]);

    // Which hook released U (the branch the build's own shape picked —
    // asserted at the end: the real ladder runs T1).
    #[derive(Debug, PartialEq, Eq)]
    enum Branch {
        MutantAtT0,
        BeforeT1Wait,
    }
    let branch: Arc<Mutex<Option<Branch>>> = Arc::new(Mutex::new(None));

    // U: opens the relation (RowExclusive) before T0 — the index does
    // not exist yet, so its handle's index list is empty — then updates
    // through that legally-stale handle when a hook releases it (the
    // handle and its statement live on U's own thread: a `_with` handle
    // is single-statement, §4).
    enum Go {
        _Mutant,
        Correct,
    }
    let (u_go_tx, u_go_rx) = mpsc::channel::<Go>();
    let (u_done_tx, u_done_rx) = mpsc::channel::<()>();
    let (u_open_tx, u_open_rx) = mpsc::channel::<()>();
    // The release token: whichever hook fires first takes it, sends U
    // its go, and waits for U's commit.
    let go_slot: ReleaseKit<Go> = Arc::new(Mutex::new(Some((u_go_tx, u_done_rx))));
    let u_db = Arc::clone(&rig.db);
    let u = spawn_run(move || {
        let t = u_db.core.begin(Isolation::ReadCommitted);
        let st = ok(Stmt::new(&t, u_db.core.visible_ts()));
        let h = ok(u_db.open_table(&st, oid, RelLockMode::RowExclusive));
        assert!(h.indexes.is_empty(), "U's handle predates the index");
        let _ = u_open_tx.send(());
        let _go = u_go_rx.recv_timeout(BOUND).expect("a hook released U");
        let n = ok(u_db.update_with(
            &st,
            &h,
            &[i32_v(1)],
            &[set_col(2, i32_d(6))], // a 3→6 (correct build) / 5→6 (mutant)
        ));
        assert!(n, "the stale-handle update applied (§7.3)");
        ok(u_db.core.commit(t, SyncCommit::On));
        ok(Resolver::run_once(&u_db.core));
        let _ = u_done_tx.send(());
    });
    match u_open_rx.recv_timeout(BOUND) {
        Ok(()) => {}
        Err(_) => panic!("U never opened before T0"),
    }

    // M: a by-handle writer on the build thread that opens its own
    // handle, so it reads the index's current state.
    let m_update = move |db: &Db<SharedFault>, to: i32| m_update_a(db, oid, to);

    let hooks = BuildHooks {
        after_t0: {
            let db = Arc::clone(&rig.db);
            let go = Arc::clone(&go_slot);
            let branch = Arc::clone(&branch);
            Some(Box::new(move || {
                // Which state did T0 land? DeleteOnly is the ladder §7
                // specifies; WriteOnly is mutant G7's world.
                let idx_oid = only_idx_of(&db, oid);
                match def_of_db(&db, idx_oid).state {
                    IndexState::WriteOnly => {
                        // Mutant G7: M first (a 3→5), then U (a 5→6,
                        // un-maintained), both before the fence.
                        m_update(&db, 5);
                        let (go, done) = go
                            .lock()
                            .expect("go")
                            .take()
                            .expect("the go token is U's once");
                        ok(go.send(Go::_Mutant));
                        match done.recv_timeout(BOUND) {
                            Ok(()) => {}
                            Err(_) => panic!("U never committed at after_t0"),
                        }
                        *branch.lock().expect("branch") = Some(Branch::MutantAtT0);
                    }
                    IndexState::DeleteOnly => {
                        // The correct ladder: release (do nothing); U's
                        // update is before_t1_wait's business.
                        *branch.lock().expect("branch") = Some(Branch::BeforeT1Wait);
                    }
                    other => panic!("unexpected state after T0: {other:?}"),
                }
            }))
        },
        before_t1_wait: {
            let go = Arc::clone(&go_slot);
            Some(Box::new(move || {
                // T1's wait has not run yet: U updates and commits here,
                // so the wait that follows finds nobody to wait for.
                let (go, done) = match go.lock().expect("go").take() {
                    Some(pair) => pair,
                    None => return, // the mutant branch already released U
                };
                ok(go.send(Go::Correct));
                match done.recv_timeout(BOUND) {
                    Ok(()) => {}
                    Err(_) => panic!("U never committed at before_t1_wait"),
                }
            }))
        },
        after_t1: {
            let db = Arc::clone(&rig.db);
            Some(Box::new(move || {
                // WriteOnly is committed: M re-shapes the row with full
                // maintenance (a 6→5 in the correct build).
                m_update(&db, 5);
            }))
        },
        ..Default::default()
    };

    let idx_oid = ok(create_index_online(&rig.db, oid, &a_def(), false, &hooks));

    // The real ladder ran T1 (U released inside T1's window); under
    // mutant G7 the after_t0 branch records MutantAtT0 and the verdicts
    // below fail instead.
    assert_eq!(
        *branch.lock().expect("branch"),
        Some(Branch::BeforeT1Wait),
        "T0 lands DeleteOnly and the ladder runs T1"
    );
    match u.recv_timeout(BOUND) {
        Ok(()) => {}
        Err(_) => panic!("U never finished"),
    }

    let def = rig.def_of(idx_oid);
    assert_eq!(def.state, IndexState::Public, "the ladder published");
    assert!(def.built_at > 0);

    // The exact index: a=5 (M's version) present, a=6 (U's un-maintained
    // version, blind-deleted by M) absent, amcheck clean.
    let report = rig.amcheck(oid);
    assert!(report.is_clean(), "amcheck: {report:?}");
    assert_eq!(iscan_a(&rig, idx_oid, 5), vec![(1, 5)], "index_scan(a=5)");
    assert_eq!(iscan_a(&rig, idx_oid, 6), vec![], "index_scan(a=6)");
    assert_eq!(scan_a(&rig, oid), vec![(1, 5)], "the surviving row");
    rig.shutdown();
}

// ---- 2: the G3 interleaving (the fence waits the straggler out) -------------

/// §7.3's fence argument: V opens its handle (DeleteOnly-aware) inside
/// T1's window **after** T1's wait, so only the fence can collect it. The
/// test reacts to whichever comes first: the build's wait parking on V
/// (the correct build — V inserts and commits from the wait hook, so
/// `c_V < f <= S` and the scan covers its row), or the scan finishing
/// with V still open (mutant G3 — the fence's wait skipped — V commits
/// above S with no entry, and the published index misses the row).
#[test]
fn g3_interleaving() {
    let rig = BuildRig::new();
    let oid = table_a(&rig, b"g3", &[(1, 1)]);

    // Which release fired (asserted at the end: the fence's wait).
    #[derive(Debug, PartialEq, Eq)]
    enum Release {
        FenceWait,
        AfterScan,
    }
    let release: Arc<Mutex<Option<Release>>> = Arc::new(Mutex::new(None));
    let s_slot: Arc<Mutex<Option<Ts>>> = Arc::new(Mutex::new(None));
    let v_id_slot: Arc<Mutex<Option<TxnId>>> = Arc::new(Mutex::new(None));

    // The one release kit: the go sender (releases V) and the done
    // receiver (V's commit ack). The WaitHook (the fence parks on V) and
    // the after_scan hook (V still open — the mutant's world) race for
    // it; whichever takes it releases V and waits for its commit.
    let (go_tx, go_rx) = mpsc::channel::<()>();
    let go_for_kit = go_tx.clone();
    let (done_tx, done_rx) = mpsc::channel::<()>();
    let kit: ReleaseKit<()> = Arc::new(Mutex::new(Some((go_for_kit, done_rx))));
    let (open_tx, open_rx) = mpsc::channel::<()>();
    let (opened_tx, opened_rx) = mpsc::channel::<()>();
    // The ack receiver lives under the mutex the hook owns (a Receiver is
    // !Sync, and a hook must be `Fn() + Send + Sync`).
    let opened_rx = Arc::new(Mutex::new(opened_rx));

    // The WaitHook: when a build txn first parks on V, release it and
    // wait for its commit ack (the ack runs on the build thread before
    // the wait registers; V's commit is served by the pipeline thread,
    // so the two cannot deadlock).
    struct FenceWaitsV {
        v_id: Arc<Mutex<Option<TxnId>>>,
        kit: ReleaseKit<()>,
        release: Arc<Mutex<Option<Release>>>,
    }
    impl WaitHook for FenceWaitsV {
        fn on_wait_start(&self, waiter: TxnId, target: TxnId) {
            let Some(v_id) = *self.v_id.lock().expect("v_id") else {
                return;
            };
            if target != v_id || waiter == v_id {
                return;
            }
            if let Some((go, done)) = self.kit.lock().expect("kit").take() {
                *self.release.lock().expect("release") = Some(Release::FenceWait);
                let _ = go.send(());
                match done.recv_timeout(BOUND) {
                    Ok(()) => {}
                    Err(_) => panic!("V never committed from the fence's wait"),
                }
            }
        }
        fn on_wait_end(&self, _waiter: TxnId, _target: TxnId) {}
    }
    rig.core().waits.set_hook(Arc::new(FenceWaitsV {
        v_id: Arc::clone(&v_id_slot),
        kit: Arc::clone(&kit),
        release: Arc::clone(&release),
    }) as Arc<dyn WaitHook>);

    // V: opens (DeleteOnly-aware) when before_t1_commit says so — and
    // acknowledges the open only after its handle read the state, so T1
    // cannot commit WriteOnly under it — then inserts (2, 9) through
    // that handle when released, commits, acknowledges.
    let v_db = Arc::clone(&rig.db);
    let v = spawn_run(move || -> Ts {
        let t = v_db.core.begin(Isolation::ReadCommitted);
        *v_id_slot.lock().expect("v_id") = Some(t.id);
        match open_rx.recv_timeout(BOUND) {
            Ok(()) => {}
            Err(_) => panic!("V never got the open signal"),
        }
        let st = ok(Stmt::new(&t, v_db.core.visible_ts()));
        let h = ok(v_db.open_table(&st, oid, RelLockMode::RowExclusive));
        assert_eq!(h.indexes.len(), 1, "V sees the index");
        assert_eq!(
            h.indexes[0].def.state,
            IndexState::DeleteOnly,
            "V is DeleteOnly-aware (T1 has not committed)"
        );
        let _ = opened_tx.send(());
        match go_rx.recv_timeout(BOUND) {
            Ok(()) => {}
            Err(_) => panic!("V never got the go signal"),
        }
        ok(v_db.insert_with(&st, &h, &[i32_d(2), i32_d(9)]));
        let c = ok(v_db.core.commit(t, SyncCommit::On));
        ok(Resolver::run_once(&v_db.core));
        let _ = done_tx.send(());
        c
    });

    let hooks = BuildHooks {
        before_t1_commit: Some(Box::new(move || {
            // T1's wait has returned: open V now (and wait for the open
            // to complete — its RowExclusive is held and its catalog
            // read saw DeleteOnly before T1 commits WriteOnly).
            let _ = open_tx.send(());
            match opened_rx.lock().expect("opened").recv_timeout(BOUND) {
                Ok(()) => {}
                Err(_) => panic!("V never opened inside T1's window"),
            }
        })),
        after_register: {
            let s_slot = Arc::clone(&s_slot);
            Some(Box::new(move |s| {
                *s_slot.lock().expect("s") = Some(s);
            }))
        },
        after_scan: {
            let kit = Arc::clone(&kit);
            let release = Arc::clone(&release);
            Some(Box::new(move || {
                // Mutant G3's world: the fence never waited, so V is
                // still open when the scan ends — release it here.
                if let Some((go, done)) = kit.lock().expect("kit").take() {
                    *release.lock().expect("release") = Some(Release::AfterScan);
                    let _ = go.send(());
                    match done.recv_timeout(BOUND) {
                        Ok(()) => {}
                        Err(_) => panic!("V never committed from after_scan"),
                    }
                }
            }))
        },
        ..Default::default()
    };

    // The build runs on its own thread so that even a wedged wait is a
    // bounded test failure, never a hung one (a hang is a failure, never
    // a pass).
    let build_db = Arc::clone(&rig.db);
    let build = spawn_run(move || create_index_online(&build_db, oid, &a_def(), false, &hooks));
    let idx_oid = match build.recv_timeout(BOUND) {
        Ok(r) => ok(r),
        Err(_) => panic!("the build never finished"),
    };

    // The fence's wait released V (mutant G3's after_scan release fails
    // this, and the verdicts below fail too).
    assert_eq!(
        *release.lock().expect("release"),
        Some(Release::FenceWait),
        "the fence's wait parked on V and released it"
    );
    let c_v = match v.recv_timeout(BOUND) {
        Ok(c) => c,
        Err(_) => panic!("V never finished"),
    };
    let s = s_slot.lock().expect("s").expect("after_register fired");
    assert!(
        s > c_v,
        "the fence orders S ({s:?}) strictly above c_V ({c_v:?})"
    );

    let def = rig.def_of(idx_oid);
    assert_eq!(def.state, IndexState::Public);
    let report = rig.amcheck(oid);
    assert!(report.is_clean(), "amcheck: {report:?}");
    assert_eq!(iscan_a(&rig, idx_oid, 9), vec![(2, 9)], "index_scan(a=9)");
    assert_eq!(scan_a(&rig, oid), vec![(1, 1), (2, 9)]);
    rig.shutdown();
}

// ---- 3: delete + re-insert under DeleteOnly, resolved by the ingest ---------

/// §7.3's "entries are not emitted at the row version's ts" shadow: X
/// (DeleteOnly-aware) deletes and re-inserts (1, 3) in one txn and
/// commits; its row is live at S (through the committed intent), and its
/// blind entry-delete intent on `E(3)` is a **committed intent** that §4
/// step 1 would serve over every version — the ingest's resolution pass
/// (C-T0 §10 step 2) must resolve it before ingesting `E@S`. No resolver
/// runs before the publish (the rig drives the resolver by hand).
/// Mutant G10 (the resolution pass skipped): the tombstone intent
/// shadows `E@S`, `index_scan(a=3)` returns `{}` and amcheck reports the
/// missing entry.
#[test]
fn delete_reinsert_in_deleteonly() {
    let rig = BuildRig::new();
    let oid = table_a(&rig, b"dri", &[(1, 3)]);

    let hooks = BuildHooks {
        after_t0: {
            let db = Arc::clone(&rig.db);
            Some(Box::new(move || {
                let idx_oid = only_idx_of(&db, oid);
                assert_eq!(
                    def_of_db(&db, idx_oid).state,
                    IndexState::DeleteOnly,
                    "X is DeleteOnly-aware"
                );
                let t = db.core.begin(Isolation::ReadCommitted);
                let st = ok(Stmt::new(&t, db.core.visible_ts()));
                let h = ok(db.open_table(&st, oid, RelLockMode::RowExclusive));
                // One txn, one statement: delete, then re-insert.
                assert!(ok(db.delete_with(&st, &h, &[i32_v(1)])));
                ok(db.insert_with(&st, &h, &[i32_d(1), i32_d(3)]));
                // Committed with no resolver pass: X's intents stay for
                // the ingest to resolve (the rig drives the resolver).
                ok(db.core.commit(t, SyncCommit::On));
            }))
        },
        ..Default::default()
    };

    let idx_oid = ok(create_index_online(&rig.db, oid, &a_def(), false, &hooks));
    assert_eq!(rig.def_of(idx_oid).state, IndexState::Public);

    // Before any resolver pass: the ingest resolved the entry intent, so
    // the entry is visible at S above X's same-ts tombstone.
    assert_eq!(
        iscan_a(&rig, idx_oid, 3),
        vec![(1, 3)],
        "index_scan(a=3) before Resolver::run_once"
    );

    // And after the resolver settles X's row intent: same answer, clean.
    ok(Resolver::run_once(rig.core()));
    assert_eq!(
        iscan_a(&rig, idx_oid, 3),
        vec![(1, 3)],
        "index_scan(a=3) after Resolver::run_once"
    );
    let report = rig.amcheck(oid);
    assert!(report.is_clean(), "amcheck: {report:?}");

    // Repeat under the fault: crash after the publish (every build
    // commit is SyncCommit::On and the ingest synced the resolutions),
    // reopen through Db::open, and the state is still sound.
    let rig = rig.crash_reopen(0);
    sweep(&rig);
    let idxs = rig.idxs_of(oid);
    assert_eq!(idxs.len(), 1, "the published index survived the open");
    let def = rig.def_of(idxs[0].oid);
    assert_eq!(def.state, IndexState::Public);
    let report = rig.amcheck(oid);
    assert!(report.is_clean(), "amcheck after reopen: {report:?}");
    assert_eq!(iscan_a(&rig, idxs[0].oid, 3), vec![(1, 3)]);
    rig.shutdown();
}

// ---- 4: an older writer's entry intent shadows E@S unless resolved ----------

/// §7.3's shadowing-intent case split across two writers: X (DeleteOnly
/// aware) deletes pk 1 and commits — its blind delete leaves a committed
/// intent on `E(3)` — then Y (still DeleteOnly-aware) re-inserts (1, 3)
/// and commits. Y writes nothing on `E`, so X's intent stays unresolved
/// until the ingest's resolution pass removes it; the scan at S sees Y's
/// row (the newest committed intent at the row key) and `E@S` lands
/// above X's resolved tombstone. Mutant G10: X's intent shadows `E@S`
/// and `index_scan(a=3)` returns `{}`.
#[test]
fn shadowing_older_intent() {
    let rig = BuildRig::new();
    let oid = table_a(&rig, b"shd", &[(1, 3)]);

    let hooks = BuildHooks {
        after_t0: {
            let db = Arc::clone(&rig.db);
            Some(Box::new(move || {
                let idx_oid = only_idx_of(&db, oid);
                assert_eq!(def_of_db(&db, idx_oid).state, IndexState::DeleteOnly);
                // X: delete pk 1, commit (no resolver pass).
                let x = db.core.begin(Isolation::ReadCommitted);
                {
                    let st = ok(Stmt::new(&x, db.core.visible_ts()));
                    let h = ok(db.open_table(&st, oid, RelLockMode::RowExclusive));
                    assert!(ok(db.delete_with(&st, &h, &[i32_v(1)])));
                }
                ok(db.core.commit(x, SyncCommit::On));
                // Y: still DeleteOnly-aware, re-inserts (1, 3) and
                // commits — writing nothing on E (a DeleteOnly insert
                // maintains nothing), so X's committed Delete intent on
                // E stays in place.
                let y = db.core.begin(Isolation::ReadCommitted);
                {
                    let st = ok(Stmt::new(&y, db.core.visible_ts()));
                    let h = ok(db.open_table(&st, oid, RelLockMode::RowExclusive));
                    assert_eq!(h.indexes[0].def.state, IndexState::DeleteOnly);
                    ok(db.insert_with(&st, &h, &[i32_d(1), i32_d(3)]));
                }
                ok(db.core.commit(y, SyncCommit::On));
            }))
        },
        ..Default::default()
    };

    let idx_oid = ok(create_index_online(&rig.db, oid, &a_def(), false, &hooks));
    assert_eq!(rig.def_of(idx_oid).state, IndexState::Public);

    // No resolver ran before the publish; the ingest itself resolved
    // X's entry intent, so the entry is served.
    assert_eq!(
        iscan_a(&rig, idx_oid, 3),
        vec![(1, 3)],
        "index_scan(a=3) with no resolver pass"
    );
    assert_eq!(scan_a(&rig, oid), vec![(1, 3)]);

    ok(Resolver::run_once(rig.core()));
    assert_eq!(iscan_a(&rig, idx_oid, 3), vec![(1, 3)]);
    let report = rig.amcheck(oid);
    assert!(report.is_clean(), "amcheck: {report:?}");
    rig.shutdown();
}

// ---- 5: synchronous_commit = off across the crash ---------------------------

/// §7.3's WAL argument: a `synchronous_commit = off` writer W commits
/// after the fence (WriteOnly-aware, so it maintains fully — its entry
/// intent is resolved by the ingest itself); the scan sees its row and
/// the ingest's **sync before the ingest** makes W's commit record
/// durable, so the crash at `before_publish` (after the last ingest
/// returned) leaves no entry without its row: W's row survives the
/// reopen and amcheck is clean. Mutant G8 (the sync skipped): the crash
/// drops W's unsynced commit record, the boot sweep discards its row —
/// and the durably-ingested `E(7)@S` dangles (I-IDX-WO).
#[test]
fn sync_off_crash() {
    let rig = BuildRig::new();
    let oid = table_a(&rig, b"soc", &[(1, 1)]);

    let s_slot: Arc<Mutex<Option<Ts>>> = Arc::new(Mutex::new(None));
    let c_w_slot: Arc<Mutex<Option<Ts>>> = Arc::new(Mutex::new(None));
    let (crashed_tx, crashed_rx) = mpsc::channel::<()>();

    let kv_for_hook = Arc::clone(&rig.kv);
    let hooks = BuildHooks {
        after_register: {
            let s_slot = Arc::clone(&s_slot);
            Some(Box::new(move |s| {
                *s_slot.lock().expect("s") = Some(s);
            }))
        },
        after_fence: {
            // W: WriteOnly-aware (the fence acked), inserts (3, 7) and
            // commits with synchronous_commit = off — its WAL tail stays
            // unsynced until the ingest's own sync.
            let db = Arc::clone(&rig.db);
            let c_w_slot = Arc::clone(&c_w_slot);
            Some(Box::new(move || {
                let t = db.core.begin(Isolation::ReadCommitted);
                let st = ok(Stmt::new(&t, db.core.visible_ts()));
                let h = ok(db.open_table(&st, oid, RelLockMode::RowExclusive));
                assert_eq!(h.indexes.len(), 1);
                assert_eq!(
                    h.indexes[0].def.state,
                    IndexState::WriteOnly,
                    "W is WriteOnly-aware"
                );
                ok(db.insert_with(&st, &h, &[i32_d(3), i32_d(7)]));
                // SyncCommit::Off, and no resolver pass: only the
                // ingest's sync and resolution touch W's footprint.
                let c = ok(db.core.commit(t, SyncCommit::Off));
                *c_w_slot.lock().expect("c_w") = Some(c);
            }))
        },
        before_publish: {
            // The last ingest has returned and its sync made everything
            // before it durable: crash now, keeping no unsynced tail, and
            // park the build so the publish never runs.
            Some(Box::new(move || {
                ok(kv_for_hook.crash(0));
                let _ = crashed_tx.send(());
                loop {
                    thread::park();
                }
            }))
        },
        ..Default::default()
    };

    // The build runs on its own thread (it never returns: it parks in
    // before_publish after the crash).
    let db = Arc::clone(&rig.db);
    let build = spawn_run(move || create_index_online(&db, oid, &a_def(), true, &hooks));
    match crashed_rx.recv_timeout(BOUND) {
        Ok(()) => {}
        Err(_) => panic!("the build never crashed at before_publish"),
    }
    let s = s_slot.lock().expect("s").expect("after_register fired");
    let c_w = c_w_slot.lock().expect("c_w").expect("W committed");
    assert!(
        s >= c_w,
        "W's commit ({c_w:?}) is visible at S ({s:?}) — the scan saw its row"
    );

    // Reopen: crash_reopen shuts the commit thread down first, then
    // crashes again (nothing unsynced is left to drop) and reopens
    // through Db::open — whose cleanup must leave the CONCURRENTLY
    // index alone (that is why concurrently = true).
    let rig = rig.crash_reopen(0);
    sweep(&rig);
    let idxs = rig.idxs_of(oid);
    assert_eq!(idxs.len(), 1, "Db::open kept the CONCURRENTLY build");
    let idx_oid = idxs[0].oid;
    let def = rig.def_of(idx_oid);
    assert_eq!(def.state, IndexState::WriteOnly, "the publish never ran");
    assert!(def.concurrently);

    // The row survived the crash — the ingest's sync made W's commit
    // durable before any entry derived from it — so no entry is without
    // its row: amcheck (I-IDX-WO over the WriteOnly index) is clean.
    assert_eq!(
        scan_a(&rig, oid),
        vec![(1, 1), (3, 7)],
        "W's row survived because the ingest synced first"
    );
    let report = rig.amcheck(oid);
    assert!(report.is_clean(), "amcheck after reopen: {report:?}");
    // And the ingested entry itself is there, live at exactly S.
    let isid = rig.isid_of(idx_oid);
    let raw = rig.raw_idx_versions(isid);
    assert!(
        raw.iter()
            .any(|(_, ts, header, _)| *ts == Some(s) && *header == 0x00),
        "the ingest survived the crash at S: {raw:?}"
    );
    drop(build); // the parked thread stays parked; its sender is gone
    rig.shutdown();
}

// ---- 5a: a WriteOnly-aware delete between the scan and the ingest -----------

/// §6's blind entry delete across the build's scan→ingest gap: the scan
/// already emitted `E(3)@S` for row 1 (live at S); Z (WriteOnly-aware)
/// deletes the row at `c_Z > S` — `E` does not exist yet, but the blind
/// tombstone lands anyway and sorts above the ingested `E@S`, so the
/// entry is gone at every snapshot that sees the delete. Mutant G6 (a
/// non-blind entry delete): the tombstone is skipped (no visible entry
/// at Z's read), the ingest resurrects the entry, and it dangles above
/// the row's tombstone — amcheck and `index_scan`'s verification catch
/// it.
#[test]
fn delete_between_scan_and_ingest() {
    let rig = BuildRig::new();
    let oid = table_a(&rig, b"dbs", &[(1, 3)]);

    let hooks = BuildHooks {
        after_scan: {
            let db = Arc::clone(&rig.db);
            Some(Box::new(move || {
                let t = db.core.begin(Isolation::ReadCommitted);
                let st = ok(Stmt::new(&t, db.core.visible_ts()));
                let h = ok(db.open_table(&st, oid, RelLockMode::RowExclusive));
                assert_eq!(h.indexes.len(), 1);
                assert_eq!(
                    h.indexes[0].def.state,
                    IndexState::WriteOnly,
                    "Z is WriteOnly-aware"
                );
                assert!(ok(db.delete_with(&st, &h, &[i32_v(1)])));
                commit_and_settle(&db, t);
            }))
        },
        ..Default::default()
    };

    let idx_oid = ok(create_index_online(&rig.db, oid, &a_def(), false, &hooks));
    assert_eq!(rig.def_of(idx_oid).state, IndexState::Public);

    let report = rig.amcheck(oid);
    assert!(report.is_clean(), "amcheck: {report:?}");
    assert_eq!(iscan_a(&rig, idx_oid, 3), vec![], "index_scan(a=3)");
    assert_eq!(scan_a(&rig, oid), vec![], "the row is gone");
    rig.shutdown();
}

// ---- 6: DROP COLUMN vs a build parked at after_t0 ---------------------------

/// §3's DROP COLUMN drops every index using the column, in any state: a
/// build parked at `after_t0` (its index DeleteOnly) loses its index row
/// to a committed `drop_column(a)`; on release, T1's re-read finds the
/// row absent and the build fails with its own 55000 — the failure
/// path's best-effort drop is the no-op (a concurrent drop won) — and
/// the table is left clean.
#[test]
fn drop_column_vs_online_t0() {
    let rig = BuildRig::new();
    let oid = table_a(&rig, b"dcx", &[(1, 3)]);

    let hooks = BuildHooks {
        after_t0: {
            let db = Arc::clone(&rig.db);
            Some(Box::new(move || {
                // The parked prefix: the index is DeleteOnly on `a`.
                let idx_oid = only_idx_of(&db, oid);
                let def = def_of_db(&db, idx_oid);
                assert_eq!(def.state, IndexState::DeleteOnly);
                assert_eq!(def.columns[0].attnum, 2, "the index is on `a`");
                // drop_column(a) commits: AccessExclusive (no build txn
                // is open at after_t0), the index goes with the column.
                let t = db.core.begin(Isolation::ReadCommitted);
                let st = ok(Stmt::new(&t, db.core.visible_ts()));
                ok(db.drop_column(&st, oid, 2));
                commit_and_settle(&db, t);
            }))
        },
        ..Default::default()
    };

    let err = create_index_online(&rig.db, oid, &a_def(), false, &hooks)
        .expect_err("the index vanished under the build");
    assert_eq!(err.sqlstate(), "55000", "{err:?}");
    assert!(
        rig.idxs_of(oid).is_empty(),
        "no index row survived (drop_column dropped it; the failure path's retry was the no-op)"
    );
    let report = rig.amcheck(oid);
    assert!(report.is_clean(), "amcheck: {report:?}");
    // The table itself keeps its row (with `a` dropped).
    assert_eq!(scan_a_len(&rig, oid), 1, "the row survived the drop_column");
    rig.shutdown();
}

/// How many rows the table has at the latest state (the drop_column
/// test's `a` is gone, so the `(id, a)` projection is not usable).
fn scan_a_len(rig: &BuildRig, oid: u64) -> usize {
    with_reader(rig, |st| {
        let h = ok(rig.db.open_table(st, oid, RelLockMode::AccessShare));
        rig.db.scan(st, &h).map(|rows| rows.len())
    })
}

// ---- 6a: a TRUNCATE inside T3's window (e2a-gate rider) ---------------------

/// A TRUNCATE attempting to commit inside T3 after the step's re-read:
/// with §7's ShareUpdateExclusive-first step ordering it parks on the
/// step's lock and lands after the publish; without it, the gate
/// verified a self-healing outcome. The pinned invariant is the
/// surviving state either way: after both complete, amcheck is clean and
/// `index_scan == scan`.
#[test]
fn truncate_mid_t3() {
    let rig = BuildRig::new();
    let oid = table_a(&rig, b"tr3", &[(1, 1), (2, 2), (3, 3)]);
    let old_sid = rig.rel_row(oid).expect("rel row").storage_id;

    // The TRUNCATE thread, released inside T3's window (after the
    // re-read, before the scan): its AccessExclusive either parks on the
    // step's ShareUpdateExclusive or — in a world without the step
    // ordering — commits inside the window.
    let (trunc_go_tx, trunc_go_rx) = mpsc::channel::<()>();
    let trunc_done = Arc::new(AtomicBool::new(false));
    let trunc_db = Arc::clone(&rig.db);
    let trunc_done_w = Arc::clone(&trunc_done);
    let truncate = spawn_run(move || -> Result<u64, nucleus_txn::TxnError> {
        match trunc_go_rx.recv_timeout(BOUND) {
            Ok(()) => {}
            Err(_) => panic!("the TRUNCATE was never released"),
        }
        let t = trunc_db.core.begin(Isolation::ReadCommitted);
        let s = trunc_db.core.visible_ts();
        let seq = ok(t.next_seq());
        let ddl = Ddl::new(&trunc_db.core, &t, StmtCtx::new(s, seq, seq));
        let new_sid = trunc_db.catalog.catalog_truncate_table(&ddl, oid)?;
        trunc_db.core.commit(t, SyncCommit::On)?;
        ok(Resolver::run_once(&trunc_db.core));
        trunc_done_w.store(true, Ordering::SeqCst);
        Ok(new_sid)
    });

    let hooks = BuildHooks {
        after_register: {
            // Fires inside T3 right after S was registered — after the
            // step's re-read, before the scan. Release the TRUNCATE and
            // hold the window open until it has parked (or completed on
            // its own).
            let core = Arc::clone(rig.core());
            let done = Arc::clone(&trunc_done);
            Some(Box::new(move |_s| {
                let _ = trunc_go_tx.send(());
                wait_until(BOUND, || {
                    done.load(Ordering::SeqCst) || !core.wait_edges().is_empty()
                });
            }))
        },
        ..Default::default()
    };

    let build = create_index_online(&rig.db, oid, &a_def(), false, &hooks);
    let new_sid = match truncate.recv_timeout(BOUND) {
        Ok(Ok(sid)) => sid,
        Ok(Err(e)) => panic!("the TRUNCATE failed: {e:?}"),
        Err(_) => panic!("the TRUNCATE never finished"),
    };
    assert_ne!(old_sid, new_sid, "the TRUNCATE really re-allocated storage");

    // The pinned invariant, whichever mechanism produced the surviving
    // state: amcheck clean, and the (possibly failed) build left no
    // index that disagrees with the table.
    match build {
        Ok(idx_oid) => {
            let def = rig.def_of(idx_oid);
            assert_eq!(def.state, IndexState::Public);
            let report = rig.amcheck(oid);
            assert!(report.is_clean(), "amcheck: {report:?}");
            assert_eq!(
                iscan_all(&rig, idx_oid),
                scan_a(&rig, oid),
                "index_scan == scan on the surviving state"
            );
        }
        Err(e) => {
            // A self-healing outcome may fail the build; a plain failure
            // drops its own index, and the state must still be sound.
            assert_eq!(e.sqlstate(), "55000", "only a changed-index failure: {e:?}");
            assert!(
                rig.idxs_of(oid).is_empty(),
                "the plain failure dropped its index"
            );
            let report = rig.amcheck(oid);
            assert!(report.is_clean(), "amcheck: {report:?}");
            assert_eq!(scan_a(&rig, oid), Vec::<(i32, i32)>::new());
        }
    }
    rig.shutdown();
}

// ---- 6b: a late ShareUpdateExclusive acquirer (e2a-gate rider) --------------

/// A writer acquiring ShareUpdateExclusive on the relation after the
/// fence's re-read but before the scan completes (here: another index's
/// T0, which takes SUE inside `catalog_create_index_with`):
/// ShareUpdateExclusive self-conflicts, so it parks on the fence's (or
/// T3's) step lock and its commit lands at or above the fence's `f` —
/// the build's outcome stays sound: amcheck clean (the second index is
/// DeleteOnly and empty — I-IDX-DO) and `index_scan == scan`.
#[test]
fn late_sue_acquirer() {
    let rig = BuildRig::new();
    let oid = table_a(&rig, b"lsu", &[(1, 1), (2, 2)]);

    let f_slot: Arc<Mutex<Option<Ts>>> = Arc::new(Mutex::new(None));
    let c_a_slot: Arc<Mutex<Option<Ts>>> = Arc::new(Mutex::new(None));

    // The acquirer: a second index's T0 (SUE), released inside the
    // fence's window, committed with its ack.
    let (acq_go_tx, acq_go_rx) = mpsc::channel::<()>();
    let acq_done = Arc::new(AtomicBool::new(false));
    let acq_db = Arc::clone(&rig.db);
    let c_a_w = Arc::clone(&c_a_slot);
    let acq_done_w = Arc::clone(&acq_done);
    let acquirer = spawn_run(move || {
        match acq_go_rx.recv_timeout(BOUND) {
            Ok(()) => {}
            Err(_) => panic!("the acquirer was never released"),
        }
        let t = acq_db.core.begin(Isolation::ReadCommitted);
        let s = acq_db.core.visible_ts();
        let seq = ok(t.next_seq());
        let ddl = Ddl::new(&acq_db.core, &t, StmtCtx::new(s, seq, seq));
        let isid = ok(acq_db.catalog.alloc_storage_id(&acq_db.core));
        let def = idx_def(1, false, IndexState::DeleteOnly); // on `id`
        ok(acq_db.catalog.catalog_create_index_with(
            &ddl,
            oid,
            IdxStorage::Id(isid),
            &ok(def.encode()),
            RelLockMode::ShareUpdateExclusive,
        ));
        let c = ok(acq_db.core.commit(t, SyncCommit::On));
        ok(Resolver::run_once(&acq_db.core));
        *c_a_w.lock().expect("c_a") = Some(c);
        acq_done_w.store(true, Ordering::SeqCst);
    });

    let hooks = BuildHooks {
        before_fence_wait: {
            // Inside the fence's window, after its re-read: release the
            // acquirer (its SUE parks on the fence's) and hold the
            // window until it parked or completed.
            let core = Arc::clone(rig.core());
            let done = Arc::clone(&acq_done);
            Some(Box::new(move || {
                let _ = acq_go_tx.send(());
                wait_until(BOUND, || {
                    done.load(Ordering::SeqCst) || !core.wait_edges().is_empty()
                });
            }))
        },
        after_fence: {
            // Record a lower bound of f (visible_ts right after the
            // fence's ack) for the c_A >= f pin.
            let db = Arc::clone(&rig.db);
            let f_slot = Arc::clone(&f_slot);
            Some(Box::new(move || {
                *f_slot.lock().expect("f") = Some(db.core.visible_ts());
            }))
        },
        ..Default::default()
    };

    let idx_oid = ok(create_index_online(&rig.db, oid, &a_def(), false, &hooks));
    match acquirer.recv_timeout(BOUND) {
        Ok(()) => {}
        Err(_) => panic!("the acquirer never finished"),
    }

    let def = rig.def_of(idx_oid);
    assert_eq!(def.state, IndexState::Public);
    // The acquirer committed at or above the fence (its visible floor):
    // every txn committing at or above f maintains fully (§7.3).
    let f = f_slot.lock().expect("f").expect("after_fence fired");
    let c_a = c_a_slot
        .lock()
        .expect("c_a")
        .expect("the acquirer committed");
    assert!(
        c_a >= f,
        "the acquirer's commit ({c_a:?}) is at or above the fence's floor ({f:?})"
    );

    let report = rig.amcheck(oid);
    assert!(report.is_clean(), "amcheck (both indexes): {report:?}");
    assert_eq!(
        iscan_all(&rig, idx_oid),
        scan_a(&rig, oid),
        "index_scan == scan"
    );
    // The second index is DeleteOnly (its own build is a later card's).
    assert_eq!(rig.idxs_of(oid).len(), 2);
    rig.shutdown();
}
