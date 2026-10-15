//! C-T6e2a item 5: the §7 online (non-unique) index build driver, end to
//! end over the threaded `build_rig` (the driver blocks for each step's
//! ack, so the pipeline runs on a real thread; every join and wait is
//! bounded — a hang is a failure, never a pass).
//!
//! - `builds_online_amcheck_clean_with_concurrent_writers`: a plain build
//!   under concurrent insert/update/delete threads (plus one write forced
//!   after `S` and before the ingest through `after_register`), amcheck
//!   clean at `S` and later, `index_scan` equals `scan` after publish.
//! - `fence_orders_s`: the fence's wait orders `S > c_X` for a
//!   DeleteOnly-aware writer X that T1's wait never collected (mutant
//!   G11: no fence, T3 waits instead — `S == c_X`).
//! - `resume_after_crash_from_delete_only_and_write_only`: a CONCURRENTLY
//!   build parked (hook) at a committed step, `crash(0)`, reopen through
//!   `Db::open`, `resume_build` completes; amcheck clean.
//! - `plain_failure_drops_index_concurrently_leaves_it`: a corrupt row
//!   the scan cannot decode fails T3 after the fence; the plain build's
//!   index row is gone, the CONCURRENTLY one stays `WriteOnly`.
//! - `db_open_drops_crashed_plain_build`: `Db::open` drops a crashed
//!   plain build before the handle returns, leaves a CONCURRENTLY one,
//!   and a fresh plain build on the returned handle completes (mutant
//!   G14: cleanup callable later instead).
//! - `in_block_concurrently_is_25001`, `unique_refused_before_t0`
//!   (no catalog write at all), `concurrent_drop_index_is_55000` (mutant
//!   G15: the abandoned build must report the failure, not `Ok`).
//! - `ingest_ts_is_s`: every entry landed at exactly `S` (the driver
//!   level of the C-T6e1 primitive; chunks of 2 — mutant G1: entries at
//!   the row version's ts).
//! - `gc_guard`: a WriteOnly-aware delete at `c > S` and a GC round
//!   (retention 0) between scan and ingest, with the S guard the only
//!   registered snapshot: GC is held at `S`, amcheck clean (mutant G2:
//!   the guard dropped before the ingest — a resurrected entry).
//! - `index_scan_matches_scan_and_old_snapshot_refused`: a reader at
//!   `S' < built_at` is 0A000.
//! - `mid_step_drop_is_55000`: a DROP INDEX racing **inside** a step's
//!   open window (`before_t1_commit`) surfaces as the build's 55000, not
//!   XX000 — the step's entry ShareUpdateExclusive parks it out of the
//!   window (the C-T6e2a gate's M-2).
//! - `concurrent_second_build_same_index`: a CONCURRENTLY build parked
//!   mid-T3 plus a concurrent `resume_build` serialize — the resume
//!   parks on the parked build's SUE and, once the publish lands, fails
//!   the build's own 55000 (the gate's M-1 interleaving, now ordered).

mod t6b_support;

use std::ops::Bound;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use t6b_support::build_rig::{table_with_rows, BuildRig};
use t6b_support::{
    i32_d, i32_v, i_entry_key, idx_def, ok, pk_schema, row_key_of, text_d, text_v, wait_until,
};

use nucleus_catalog::ddl::create_index_concurrently_in_block;
use nucleus_catalog::{create_index_online, resume_build, BuildHooks, Db, IndexState, BUILD_CHUNK};
use nucleus_codec::row::Datum;
use nucleus_codec::Value;
use nucleus_codec::{Collation, KeyColumn, KeyType};
use nucleus_kv::{Batch, Durability};
use nucleus_txn::catalog::{Catalog, Ddl};
use nucleus_txn::commit::SyncCommit;
use nucleus_txn::gc::{GcConfig, GcJob};
use nucleus_txn::locks::RelLockMode;
use nucleus_txn::resolver::Resolver;
use nucleus_txn::txn::{Isolation, Txn};
use nucleus_txn::visibility::ReadCtx;
use nucleus_txn::wait::WaitHook;
use nucleus_txn::{Ts, TxnId};

/// Everything a thread may wait on is bounded by this; the joins use it
/// too.
const BOUND: Duration = Duration::from_secs(30);

/// The plain non-unique index definition on `payload` (attnum 2) the
/// tests build; the driver overrides state and built_at.
fn payload_def() -> nucleus_catalog::IndexDef {
    idx_def(2, false, IndexState::DeleteOnly)
}

fn text_col() -> KeyColumn {
    KeyColumn::asc(KeyType::Text(Collation::C))
}

/// Runs `f` on a thread and returns the receiver for a bounded join.
fn spawn_run<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> mpsc::Receiver<T> {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let _ = tx.send(f());
    });
    rx
}

/// A hook that signals it arrived and then parks forever: the build
/// thread stays inside the step (its committed prefix survives a
/// `crash(0)` — every build commit is `SyncCommit::On`), and never touches
/// the store again, so the crashed state is exactly the parked step's.
fn park_hook(tx: mpsc::Sender<()>) -> Option<Box<dyn Fn() + Send + Sync>> {
    Some(Box::new(move || {
        let _ = tx.send(());
        loop {
            thread::park();
        }
    }))
}

/// The relation's single visible index oid at the latest committed
/// catalog (a ghost reader), for hooks that must name the just-created
/// index before the driver has returned its oid.
fn only_idx_of(db: &Db<t6b_support::SharedFault>, oid: u64) -> u64 {
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

/// A (id, payload) projection of scanned rows, order-independent.
fn ids_of(rows: &[(Vec<u8>, Vec<Option<Datum>>)]) -> Vec<(i32, String)> {
    let mut out = Vec::with_capacity(rows.len());
    for (_, live) in rows {
        let id = match live.first() {
            Some(Some(Datum::Inline(Value::Int4(n)))) => *n,
            other => panic!("id datum: {other:?}"),
        };
        let payload = match live.get(1) {
            Some(Some(Datum::Inline(Value::Text(s)))) => s.clone(),
            other => panic!("payload datum: {other:?}"),
        };
        out.push((id, payload));
    }
    out.sort();
    out
}

/// One committed insert in its own txn (the writer-thread shape).
fn insert_row(db: &Db<t6b_support::SharedFault>, oid: u64, id: i32, payload: &str) {
    let t = db.core.begin(Isolation::ReadCommitted);
    let st = ok(nucleus_catalog::Stmt::new(&t, db.core.visible_ts()));
    let h = ok(db.open_table(&st, oid, RelLockMode::RowExclusive));
    ok(db.insert_with(&st, &h, &[i32_d(id), text_d(payload)]));
    ok(db.core.commit(t, SyncCommit::On));
    ok(Resolver::run_once(&db.core));
}

/// A plain build under concurrent insert/update/delete threads. The
/// assertions are order-independent — whichever way the writers interleave
/// with the steps, amcheck must be clean at `S` and later, and the
/// published index must agree with a scan.
#[test]
fn builds_online_amcheck_clean_with_concurrent_writers() {
    let rig = BuildRig::new();
    let oid = table_with_rows(
        &rig,
        b"cw",
        &[
            (1, "p1"),
            (2, "p2"),
            (3, "p3"),
            (4, "p4"),
            (5, "p5"),
            (6, "p6"),
        ],
    );

    // One committed insert forced after `S` and before the ingest
    // (deterministic: its entry must come from the writer at its own ts,
    // never from the build's scan at S).
    let s_slot: Arc<Mutex<Option<Ts>>> = Arc::new(Mutex::new(None));
    let s_for_hook = Arc::clone(&s_slot);
    let db_for_hook = Arc::clone(&rig.db);
    let hooks = BuildHooks {
        after_register: Some(Box::new(move |s| {
            *s_for_hook.lock().expect("s") = Some(s);
            insert_row(&db_for_hook, oid, 200, "postS");
        })),
        chunk: Some(3),
        ..Default::default()
    };

    // The three writer threads: fixed workloads over distinct rows (no
    // writer-writer conflicts; RowExclusive never conflicts with the
    // build's ShareUpdateExclusive, so nothing blocks).
    let ins = {
        let db = Arc::clone(&rig.db);
        spawn_run(move || {
            for i in 0..8i32 {
                insert_row(&db, oid, 100 + i, &format!("w{i}"));
            }
        })
    };
    let upd = {
        let db = Arc::clone(&rig.db);
        spawn_run(move || {
            for &(id, new) in &[(2i32, "p2x"), (3, "p3x")] {
                let t = db.core.begin(Isolation::ReadCommitted);
                let st = ok(nucleus_catalog::Stmt::new(&t, db.core.visible_ts()));
                ok(db.update(&st, oid, &[i32_v(id)], &[(2u16, text_d(new))]));
                ok(db.core.commit(t, SyncCommit::On));
                ok(Resolver::run_once(&db.core));
            }
        })
    };
    let del = {
        let db = Arc::clone(&rig.db);
        spawn_run(move || {
            for &id in &[4i32, 5] {
                let t = db.core.begin(Isolation::ReadCommitted);
                let st = ok(nucleus_catalog::Stmt::new(&t, db.core.visible_ts()));
                ok(db.delete(&st, oid, &[i32_v(id)]));
                ok(db.core.commit(t, SyncCommit::On));
                ok(Resolver::run_once(&db.core));
            }
        })
    };

    let idx_oid = ok(create_index_online(
        &rig.db,
        oid,
        &payload_def(),
        false,
        &hooks,
    ));

    for (name, rx) in [("ins", ins), ("upd", upd), ("del", del)] {
        match rx.recv_timeout(BOUND) {
            Ok(()) => {}
            Err(_) => panic!("writer thread {name} did not finish within {BOUND:?}"),
        }
    }
    let s = s_slot.lock().expect("s").expect("after_register fired");

    let def = rig.def_of(idx_oid);
    assert_eq!(def.state, IndexState::Public, "the ladder published");
    assert_eq!(def.built_at, s.0, "built_at is the registered snapshot");
    assert!(!def.concurrently);

    // amcheck clean at S (the publish ts) and at the latest state.
    let at_s = rig.amcheck_at(oid, s);
    assert!(at_s.is_clean(), "amcheck at S ({s:?}): {at_s:?}");
    let later = rig.amcheck(oid);
    assert!(later.is_clean(), "amcheck later: {later:?}");

    // index_scan equals scan after publish (both at a fresh snapshot),
    // and returns rows in index order.
    let t = rig.rc();
    let st = rig.stmt(&t, rig.core().visible_ts());
    let via_idx = ok(rig
        .db
        .index_scan(&st, idx_oid, Bound::Unbounded, Bound::Unbounded));
    let h = ok(rig.db.open_table(&st, oid, RelLockMode::AccessShare));
    let via_scan = ok(rig.db.scan(&st, &h));
    ok(rig.commit_rc(t));
    assert_eq!(ids_of(&via_idx), ids_of(&via_scan), "index_scan == scan");
    let payloads: Vec<String> = via_idx
        .iter()
        .map(|(_, live)| match &live[1] {
            Some(Datum::Inline(Value::Text(s))) => s.clone(),
            other => panic!("payload datum: {other:?}"),
        })
        .collect();
    let mut want = payloads.clone();
    want.sort();
    assert_eq!(payloads, want, "index_scan returns rows in index order");
    rig.shutdown();
}

/// The fence orders `S` past every DeleteOnly-aware writer (§7.3): X
/// opens its handle (reads DeleteOnly) between T1's wait and T1's commit,
/// so T1 never collects it; X inserts (5, "1") there and stays open; the
/// fence's `wait_for_lockers(Share)` — observed through a `WaitHook` —
/// waits X out, X commits at `c_X` from the hook, and T3's `S` (recorded
/// by `after_register`) is strictly above `c_X`. Nothing else commits.
/// Mutant G11 (no fence; T3 waits instead): the snapshot is taken after
/// X's commit with nothing in between — `S == c_X` — and the strict
/// ordering assertion fails.
#[test]
fn fence_orders_s() {
    let rig = BuildRig::new();
    let oid = table_with_rows(&rig, b"fence", &[(1, "a")]);

    // X: begun now, its handle opened and (5, "1") inserted at
    // `before_t1_commit` (one statement: the `_with` contract), committed
    // from the wait hook when a build txn first parks on it.
    let x = rig.rc();
    let x_id = x.id;
    let x_slot = Arc::new(Mutex::new(Some(x)));

    let s_slot: Arc<Mutex<Option<Ts>>> = Arc::new(Mutex::new(None));
    let s_hook = Arc::clone(&s_slot);
    let x_open = Arc::clone(&x_slot);
    let db_open = Arc::clone(&rig.db);
    let hooks = BuildHooks {
        after_register: Some(Box::new(move |s| {
            *s_hook.lock().expect("s") = Some(s);
        })),
        before_t1_commit: Some(Box::new(move || {
            let mut slot = x_open.lock().expect("x");
            let x = slot.as_mut().expect("X still open");
            let st = ok(nucleus_catalog::Stmt::new(x, db_open.core.visible_ts()));
            let h = ok(db_open.open_table(&st, oid, RelLockMode::RowExclusive));
            // The DeleteOnly read the whole test is about: X's statement
            // maintains deletes only, so its insert writes no entry.
            assert_eq!(h.indexes.len(), 1);
            assert_eq!(h.indexes[0].def.state, IndexState::DeleteOnly);
            ok(db_open.insert_with(&st, &h, &[i32_d(5), text_d("1")]));
        })),
        ..Default::default()
    };

    /// Commits X from `on_wait_start(_, X)` (the hook runs on the build
    /// thread before it parks, so X's commit — served by the separate
    /// pipeline thread — cannot deadlock with it).
    struct CommitX {
        x_id: TxnId,
        x: Arc<Mutex<Option<Txn>>>,
        db: Arc<Db<t6b_support::SharedFault>>,
        c_x: Mutex<Option<Ts>>,
        done: AtomicBool,
    }
    impl WaitHook for CommitX {
        fn on_wait_start(&self, _waiter: TxnId, target: TxnId) {
            if target != self.x_id || self.done.swap(true, Ordering::SeqCst) {
                return;
            }
            let x = self.x.lock().expect("x").take().expect("X alive");
            let c = ok(self.db.core.commit(x, SyncCommit::On));
            ok(Resolver::run_once(&self.db.core));
            *self.c_x.lock().expect("c_x") = Some(c);
        }
        fn on_wait_end(&self, _waiter: TxnId, _target: TxnId) {}
    }
    let committer = Arc::new(CommitX {
        x_id,
        x: Arc::clone(&x_slot),
        db: Arc::clone(&rig.db),
        c_x: Mutex::new(None),
        done: AtomicBool::new(false),
    });
    rig.core()
        .waits
        .set_hook(Arc::clone(&committer) as Arc<dyn WaitHook>);

    let idx_oid = ok(create_index_online(
        &rig.db,
        oid,
        &payload_def(),
        false,
        &hooks,
    ));

    let s = s_slot.lock().expect("s").expect("after_register fired");
    let c_x = committer
        .c_x
        .lock()
        .expect("c_x")
        .expect("the fence waited X out");
    assert!(
        s > c_x,
        "the fence's wait orders S ({s:?}) strictly above c_X ({c_x:?})"
    );

    // The build completed over X's row: X's insert wrote no entry
    // (DeleteOnly-aware), the scan at S (> c_X) saw the row and ingested
    // its entry at S.
    assert_eq!(rig.def_of(idx_oid).state, IndexState::Public);
    let report = rig.amcheck(oid);
    assert!(report.is_clean(), "{report:?}");
    rig.shutdown();
}

/// Resume (§7): a CONCURRENTLY build parked after a committed step,
/// `crash(0)`, reopen through `Db::open` (its cleanup must leave the
/// CONCURRENTLY index alone), then `resume_build` — from DeleteOnly
/// (restart at T1) and from WriteOnly (restart at the fence) — completes
/// and publishes; amcheck is clean and every entry sits at the resumed
/// build's own S.
#[test]
fn resume_after_crash_from_delete_only_and_write_only() {
    for parked_at_delete_only in [true, false] {
        let rig = BuildRig::new();
        let oid = table_with_rows(&rig, b"res", &[(1, "r1"), (2, "r2"), (3, "r3")]);

        let (tx, rx) = mpsc::channel();
        let mut hooks = BuildHooks::default();
        if parked_at_delete_only {
            hooks.after_t0 = park_hook(tx); // T0 acked: state DeleteOnly
        } else {
            hooks.after_fence = park_hook(tx); // fence acked: WriteOnly
        }
        let db = Arc::clone(&rig.db);
        let build = spawn_run(move || create_index_online(&db, oid, &payload_def(), true, &hooks));
        match rx.recv_timeout(BOUND) {
            Ok(()) => {}
            Err(_) => panic!("the build never reached its parking hook"),
        }

        // Every build commit was SyncCommit::On, so crash(0) keeps the
        // parked prefix exactly.
        let rig = rig.crash_reopen(0);
        let idxs = rig.idxs_of(oid);
        assert_eq!(idxs.len(), 1, "the CONCURRENTLY index survived the open");
        let idx_oid = idxs[0].oid;
        let def = rig.def_of(idx_oid);
        assert_eq!(
            def.state,
            if parked_at_delete_only {
                IndexState::DeleteOnly
            } else {
                IndexState::WriteOnly
            },
            "the crashed prefix is the parked step's committed state"
        );
        assert!(def.concurrently, "the def carries the CONCURRENTLY flag");

        let hooks = BuildHooks {
            chunk: Some(2),
            ..Default::default()
        };
        assert_eq!(ok(resume_build(&rig.db, idx_oid, &hooks)), idx_oid);
        let def = rig.def_of(idx_oid);
        assert_eq!(def.state, IndexState::Public);
        assert!(def.built_at > 0, "built_at is the resumed build's S");

        // The index is exact: one live entry per surviving row, every one
        // at the resumed S, and amcheck clean.
        let isid = rig.isid_of(idx_oid);
        let raw = rig.raw_idx_versions(isid);
        assert_eq!(raw.len(), 3, "one entry per row");
        for (_, ts, header, _) in &raw {
            assert_eq!(*ts, Some(Ts(def.built_at)), "every entry at S");
            assert_eq!(*header, 0x00, "live");
        }
        let report = rig.amcheck(oid);
        assert!(report.is_clean(), "{report:?}");

        // Resuming a Public index is the no-op.
        assert_eq!(ok(resume_build(&rig.db, idx_oid, &hooks)), idx_oid);
        drop(build); // the parked thread stays parked; its sender is gone
        rig.shutdown();
    }
}

/// Failure (§7): a corrupt row version the T3 scan cannot decode fails
/// the build after the fence; a plain build's index row is gone (dropped,
/// best effort) and a CONCURRENTLY one's stays — WriteOnly, the def's
/// flag set.
#[test]
fn plain_failure_drops_index_concurrently_leaves_it() {
    let rig = BuildRig::new();
    let oid = table_with_rows(&rig, b"pf", &[(1, "a"), (2, "b")]);
    let sid = rig.rel_row(oid).expect("rel row").storage_id;

    // A live row version whose payload no definition decodes, planted at
    // the fence's ack: T3's scan (after it) fails on it — deterministically
    // before any entry was ingested, with the index left WriteOnly.
    let mut hooks = BuildHooks::default();
    let plant_core = Arc::clone(rig.core());
    hooks.after_fence = Some(Box::new(move || {
        let key = row_key_of(sid, &pk_schema(), &[i32_v(999)]);
        let ts = plant_core.visible_ts();
        let mut value = vec![0x00u8]; // §2.2 live header
        value.extend_from_slice(b"not a row");
        let mut batch = Batch::default();
        batch.put(nucleus_txn::encoding::version_key(&key, ts), value);
        ok(plant_core.write(batch, Durability::No));
    }));

    // Plain: the failure path drops the half-built index.
    let err = create_index_online(&rig.db, oid, &payload_def(), false, &hooks)
        .expect_err("the scan cannot decode the planted row");
    assert_eq!(err.sqlstate(), "XX001", "{err:?}");
    assert!(
        rig.idxs_of(oid).is_empty(),
        "the plain build dropped its index row"
    );

    // CONCURRENTLY: the same failure leaves it — WriteOnly, flagged.
    let err = create_index_online(&rig.db, oid, &payload_def(), true, &hooks)
        .expect_err("the scan still cannot decode the planted row");
    assert_eq!(err.sqlstate(), "XX001", "{err:?}");
    let idxs = rig.idxs_of(oid);
    assert_eq!(idxs.len(), 1, "the CONCURRENTLY build left its index");
    let def = rig.def_of(idxs[0].oid);
    assert_eq!(def.state, IndexState::WriteOnly);
    assert!(def.concurrently);
    // Nothing was ingested before the failure.
    assert!(
        rig.raw_idx_versions(rig.isid_of(idxs[0].oid)).is_empty(),
        "the failed scan ingested nothing"
    );
    rig.shutdown();
}

/// `Db::open` runs `cleanup_unfinished_builds` before the handle returns:
/// a crashed plain build's WriteOnly index is gone, a CONCURRENTLY one's
/// survives, and a plain build started on the returned handle completes
/// and stays. Mutant G14 (the cleanup callable later, `Db::open` plain):
/// the plain WriteOnly row is still there the moment the handle returns.
#[test]
fn db_open_drops_crashed_plain_build() {
    let rig = BuildRig::new();
    let oid_p = table_with_rows(&rig, b"plain", &[(1, "a")]);
    let oid_c = table_with_rows(&rig, b"conc", &[(1, "b")]);

    // Park both builds after T1's ack (state WriteOnly: the crashed
    // prefix ends there).
    let (tx_p, rx_p) = mpsc::channel();
    let hooks_p = BuildHooks {
        after_t1: park_hook(tx_p),
        ..Default::default()
    };
    let db_p = Arc::clone(&rig.db);
    let build_p =
        spawn_run(move || create_index_online(&db_p, oid_p, &payload_def(), false, &hooks_p));
    match rx_p.recv_timeout(BOUND) {
        Ok(()) => {}
        Err(_) => panic!("the plain build never parked"),
    }
    let (tx_c, rx_c) = mpsc::channel();
    let hooks_c = BuildHooks {
        after_t1: park_hook(tx_c),
        ..Default::default()
    };
    let db_c = Arc::clone(&rig.db);
    let build_c =
        spawn_run(move || create_index_online(&db_c, oid_c, &payload_def(), true, &hooks_c));
    match rx_c.recv_timeout(BOUND) {
        Ok(()) => {}
        Err(_) => panic!("the concurrently build never parked"),
    }

    let rig = rig.crash_reopen(0);

    // The plain build's index was dropped before the handle returned.
    assert!(
        rig.idxs_of(oid_p).is_empty(),
        "Db::open dropped the crashed plain build"
    );
    // The CONCURRENTLY one was left, still WriteOnly.
    let idxs = rig.idxs_of(oid_c);
    assert_eq!(idxs.len(), 1);
    let idx_c = idxs[0].oid;
    let def = rig.def_of(idx_c);
    assert_eq!(def.state, IndexState::WriteOnly);
    assert!(def.concurrently);

    // A plain build started on the returned handle completes and must not
    // be dropped by it.
    let idx_new = ok(create_index_online(
        &rig.db,
        oid_p,
        &payload_def(),
        false,
        &BuildHooks::default(),
    ));
    let def = rig.def_of(idx_new);
    assert_eq!(def.state, IndexState::Public);
    assert!(!def.concurrently);
    let report = rig.amcheck(oid_p);
    assert!(report.is_clean(), "{report:?}");

    drop((build_p, build_c));
    rig.shutdown();
}

/// CREATE INDEX CONCURRENTLY inside a transaction block is 25001 (the SQL
/// layer routes it to the refusal instead of the driver).
#[test]
fn in_block_concurrently_is_25001() {
    let err = create_index_concurrently_in_block().expect_err("always refused");
    assert_eq!(err.sqlstate(), "25001");
}

/// `def.unique` is refused with 0A000 before T0 — no catalog row, no
/// storage, nothing — for plain and CONCURRENTLY alike (§7.2).
#[test]
fn unique_refused_before_t0() {
    let rig = BuildRig::new();
    let oid = table_with_rows(&rig, b"uq", &[(1, "a")]);
    let unique = idx_def(2, true, IndexState::DeleteOnly);
    for concurrently in [false, true] {
        let err = create_index_online(&rig.db, oid, &unique, concurrently, &BuildHooks::default())
            .expect_err("unique is never built online");
        assert_eq!(err.sqlstate(), "0A000", "{err:?}");
        assert!(
            rig.idxs_of(oid).is_empty(),
            "no index row was written (concurrently = {concurrently})"
        );
    }
    rig.shutdown();
}

/// A concurrent DROP INDEX between the steps (at `after_t0`) makes the
/// next step's re-read find the row absent: 55000, the failure path's own
/// drop is the best-effort no-op, and the driver reports the failure.
/// Mutant G15 (the abandoned build returns `Ok`): the `expect_err` fails.
#[test]
fn concurrent_drop_index_is_55000() {
    let rig = BuildRig::new();
    let oid = table_with_rows(&rig, b"drop", &[(1, "a")]);

    let mut hooks = BuildHooks::default();
    let dropper_db = Arc::clone(&rig.db);
    hooks.after_t0 = Some(Box::new(move || {
        // The just-created index is the relation's only one.
        let idx_oid = only_idx_of(&dropper_db, oid);
        let t = dropper_db.core.begin(Isolation::ReadCommitted);
        let st = ok(nucleus_catalog::Stmt::new(&t, dropper_db.core.visible_ts()));
        let ddl = Ddl::new(&dropper_db.core, &t, st.ctx());
        ok(dropper_db.catalog.catalog_drop_index(&ddl, idx_oid));
        ok(dropper_db.core.commit(t, SyncCommit::On));
        ok(Resolver::run_once(&dropper_db.core));
    }));

    let err = create_index_online(&rig.db, oid, &payload_def(), false, &hooks)
        .expect_err("the index vanished between the steps");
    assert_eq!(err.sqlstate(), "55000", "{err:?}");
    assert!(
        rig.idxs_of(oid).is_empty(),
        "no index row survived (the hook dropped it; the failure path's retry was the best-effort no-op)"
    );
    rig.shutdown();
}

/// `ingest_ts_is_s` at the driver level: every entry of a quiet build
/// lands at exactly `version_key(E, S)` with a live header, nothing else
/// touches the range, and the def publishes `built_at = S`. Chunks of 2
/// over 5 rows (the card's `BUILD_CHUNK` is pinned too) exercise the
/// chunked ingest.
/// Mutant G1 (entries emitted at the row version's ts): the version keys
/// disagree with S and this fails.
#[test]
fn ingest_ts_is_s() {
    assert_eq!(BUILD_CHUNK, 4096, "the §7 step-6 chunk size");
    let rig = BuildRig::new();
    let rows = [(1, "e1"), (2, "e2"), (3, "e3"), (4, "e4"), (5, "e5")];
    let oid = table_with_rows(&rig, b"ts", &rows);
    let sid = rig.rel_row(oid).expect("rel row").storage_id;

    let s_slot: Arc<Mutex<Option<Ts>>> = Arc::new(Mutex::new(None));
    let s_hook = Arc::clone(&s_slot);
    let hooks = BuildHooks {
        after_register: Some(Box::new(move |s| {
            *s_hook.lock().expect("s") = Some(s);
        })),
        chunk: Some(2),
        ..Default::default()
    };
    let idx_oid = ok(create_index_online(
        &rig.db,
        oid,
        &payload_def(),
        false,
        &hooks,
    ));

    let s = s_slot.lock().expect("s").expect("after_register fired");
    let def = rig.def_of(idx_oid);
    assert_eq!(def.state, IndexState::Public);
    assert_eq!(def.built_at, s.0, "built_at is the ingest ts");

    let isid = rig.isid_of(idx_oid);
    let raw = rig.raw_idx_versions(isid);
    // Every entry: a live version at exactly S, empty /i/ payload.
    let mut logicals: Vec<Vec<u8>> = Vec::with_capacity(raw.len());
    for (logical, ts, header, payload) in &raw {
        assert_eq!(*ts, Some(s), "entry {logical:?} landed at S");
        assert_eq!(*header, 0x00, "live header");
        assert!(payload.is_empty(), "/i/ entries carry no payload");
        logicals.push(logical.clone());
    }
    // Exactly the five §6 entries, in sorted order.
    let mut want: Vec<Vec<u8>> = rows
        .iter()
        .map(|&(id, p)| {
            i_entry_key(
                isid,
                &[text_col()],
                &[text_v(p.into())],
                &row_key_of(sid, &pk_schema(), &[i32_v(id)]),
            )
        })
        .collect();
    want.sort();
    logicals.sort();
    assert_eq!(logicals, want, "the index holds exactly the scanned rows");

    let report = rig.amcheck(oid);
    assert!(report.is_clean(), "{report:?}");
    rig.shutdown();
}

/// The S guard holds GC back across the ingest (§7.3 "S stays registered
/// until the ingest returns"): between the scan and the ingest a
/// WriteOnly-aware writer deletes a row at `c > S` and a GC round (no AS
/// OF window) runs — with the guard the only registered snapshot, W is
/// held at S, no DeleteRange fires, the ingest lands, and amcheck is
/// clean at S and later.
/// Mutant G2 (the guard dropped before the ingest): W passes the delete,
/// GC's DeleteRanges remove the row and its entry tombstone, and the
/// post-GC ingest resurrects the entry — amcheck reports it (or the
/// ingest itself fails 72000 re-registering S below the new W).
#[test]
fn gc_guard() {
    let rig = BuildRig::new();
    let oid = table_with_rows(&rig, b"gc", &[(1, "g1"), (2, "g2"), (3, "g3")]);

    // The GC job, installed before the build (one publisher per core) and
    // shared with the hook.
    let gc = Arc::new(ok(GcJob::install(
        rig.core(),
        GcConfig {
            as_of_window_secs: None,
        },
    )));

    let s_slot: Arc<Mutex<Option<Ts>>> = Arc::new(Mutex::new(None));
    let s_hook = Arc::clone(&s_slot);
    let writer_db = Arc::clone(&rig.db);
    let writer_gc = Arc::clone(&gc);
    let hooks = BuildHooks {
        after_register: Some(Box::new(move |s| {
            *s_hook.lock().expect("s") = Some(s);
        })),
        after_scan: Some(Box::new(move || {
            // (a) a WriteOnly-aware delete of row 2, committed at c > S (the
            // build's guard is registered; this txn is the next commit).
            let t = writer_db.core.begin(Isolation::ReadCommitted);
            let st = ok(nucleus_catalog::Stmt::new(&t, writer_db.core.visible_ts()));
            ok(writer_db.delete(&st, oid, &[i32_v(2)]));
            ok(writer_db.core.commit(t, SyncCommit::On));
            ok(Resolver::run_once(&writer_db.core));
            // (b) a full GC round: publish (W = min(S, visible_ts) = S while
            // the guard is registered) + drop_tombstones.
            ok(writer_gc.run_once(0));
        })),
        ..Default::default()
    };

    let idx_oid = ok(create_index_online(
        &rig.db,
        oid,
        &payload_def(),
        false,
        &hooks,
    ));
    let s = s_slot.lock().expect("s").expect("after_register fired");

    // GC really ran, and was held at exactly S.
    assert_eq!(
        gc.durable_w(),
        s,
        "the guard held W at S across the GC round"
    );

    let def = rig.def_of(idx_oid);
    assert_eq!(def.state, IndexState::Public);
    // Row 2 is gone (the delete at c > S); rows 1 and 3 keep their
    // entries; row 2's entry is invisible under the tombstone at c.
    assert!(
        rig.amcheck_at(oid, s).is_clean(),
        "amcheck at S: {:?}",
        rig.amcheck_at(oid, s)
    );
    let later = rig.amcheck(oid);
    assert!(later.is_clean(), "amcheck later: {later:?}");
    rig.shutdown();
}

/// A reader at `S' < built_at` is refused (§4: an index serves a read iff
/// Public and built_at <= S); after publish, `index_scan` equals the
/// filtered scan and returns rows in index order.
#[test]
fn index_scan_matches_scan_and_old_snapshot_refused() {
    let rig = BuildRig::new();
    let oid = table_with_rows(&rig, b"use", &[(1, "a"), (2, "b"), (3, "c")]);

    // A registered snapshot from before the build: strictly below
    // built_at (the ladder commits T0..T3 above it).
    let s_pre = rig.core().registry.take_snapshot();

    let idx_oid = ok(create_index_online(
        &rig.db,
        oid,
        &payload_def(),
        false,
        &BuildHooks::default(),
    ));
    let def = rig.def_of(idx_oid);
    assert_eq!(def.state, IndexState::Public);
    assert!(
        def.built_at > s_pre.ts().0,
        "built_at is above the old snapshot"
    );

    // The old-snapshot reader: 0A000.
    let t = rig.rc();
    let st = rig.stmt(&t, s_pre.ts());
    let err = rig
        .db
        .index_scan(&st, idx_oid, Bound::Unbounded, Bound::Unbounded)
        .expect_err("an index never serves a snapshot below built_at");
    assert_eq!(err.sqlstate(), "0A000", "{err:?}");
    ok(rig.core().abort(t));
    drop(s_pre);

    // A current reader: full agreement with the scan, in index order.
    let t = rig.rc();
    let st = rig.stmt(&t, rig.core().visible_ts());
    let via_idx = ok(rig
        .db
        .index_scan(&st, idx_oid, Bound::Unbounded, Bound::Unbounded));
    let h = ok(rig.db.open_table(&st, oid, RelLockMode::AccessShare));
    let via_scan = ok(rig.db.scan(&st, &h));
    ok(rig.commit_rc(t));
    assert_eq!(ids_of(&via_idx), ids_of(&via_scan));
    let ids: Vec<i32> = via_idx
        .iter()
        .map(|(_, live)| match live[0] {
            Some(Datum::Inline(Value::Int4(n))) => n,
            ref other => panic!("id datum: {other:?}"),
        })
        .collect();
    assert_eq!(ids, vec![1, 2, 3], "index order (payload a, b, c)");
    rig.shutdown();
}

/// A DROP INDEX racing **inside** a step's open window — begun while T1
/// is between its re-read and its catalog write (`before_t1_commit`) —
/// must surface as the build's 55000, never XX000 (the C-T6e2a gate's
/// M-2). With §7's restored ordering T1 holds ShareUpdateExclusive from
/// its entry: the drop's AccessExclusive parks on it, the drop lands
/// only after T1's commit, and T2's `find`/`reread` is the 55000. With
/// the entry lock removed (mutant ORDER-DROP) the drop commits inside
/// the window and T1's own catalog write misses the row — XX000.
#[test]
fn mid_step_drop_is_55000() {
    let rig = BuildRig::new();
    let oid = table_with_rows(&rig, b"msd", &[(1, "a")]);

    // The DROP INDEX thread. Under the restored ordering it parks
    // acquiring AccessExclusive against T1's ShareUpdateExclusive — a
    // wait-for edge appears while T1 is still inside its window; with
    // the entry lock gone it commits straight away and sets `done`. The
    // `before_t1_commit` hook returns once either is observed — never
    // waiting for the park to clear, which under the restored ordering
    // cannot happen before T1's own commit (that would be a
    // self-deadlock). `after_t1` then holds the ladder in the gap until
    // the drop has landed, so the next step observes it deterministically.
    let done = Arc::new(AtomicBool::new(false));
    let dropper: Arc<Mutex<Option<thread::JoinHandle<()>>>> = Arc::new(Mutex::new(None));
    let mut hooks = BuildHooks::default();
    {
        let done_h = Arc::clone(&done);
        let dropper_h = Arc::clone(&dropper);
        let db_h = Arc::clone(&rig.db);
        hooks.before_t1_commit = Some(Box::new(move || {
            let db_t = Arc::clone(&db_h);
            let done_t = Arc::clone(&done_h);
            let handle = thread::spawn(move || {
                let idx_oid = only_idx_of(&db_t, oid);
                let t = db_t.core.begin(Isolation::ReadCommitted);
                let st = ok(nucleus_catalog::Stmt::new(&t, db_t.core.visible_ts()));
                let ddl = Ddl::new(&db_t.core, &t, st.ctx());
                ok(db_t.catalog.catalog_drop_index(&ddl, idx_oid));
                ok(db_t.core.commit(t, SyncCommit::On));
                ok(Resolver::run_once(&db_t.core));
                done_t.store(true, Ordering::SeqCst);
            });
            *dropper_h.lock().expect("dropper") = Some(handle);
            wait_until(BOUND, || {
                done_h.load(Ordering::SeqCst) || !db_h.core.wait_edges().is_empty()
            });
        }));
    }
    {
        let done_h = Arc::clone(&done);
        hooks.after_t1 = Some(Box::new(move || {
            // T1's txn is gone (its SUE released it): the drop that
            // parked on it now runs to its commit before T2 begins.
            wait_until(BOUND, || done_h.load(Ordering::SeqCst));
        }));
    }

    let err = create_index_online(&rig.db, oid, &payload_def(), false, &hooks)
        .expect_err("a drop inside the step's window fails the build");
    assert_eq!(err.sqlstate(), "55000", "{err:?}");
    // The drop really landed (after T1's commit, under the restored
    // ordering); the failure path's own drop is the documented no-op.
    assert!(rig.idxs_of(oid).is_empty(), "no index row survived");
    // The dropper completed (the build observed the row gone); join it
    // before the rig shuts its commit thread down.
    let handle = dropper.lock().expect("dropper").take().expect("spawned");
    handle.join().expect("dropper thread");
    rig.shutdown();
}

/// The gate's M-1 interleaving, now serialized: a live CONCURRENTLY build
/// parked mid-T3 (after its ingest, at `before_publish`) plus a
/// concurrent `resume_build` on the same index. With §7's restored
/// ordering the parked build's T3 holds ShareUpdateExclusive, the
/// resume's first step parks acquiring its own SUE (observed as a
/// wait-for edge) until the publish commits; its re-read then finds
/// `Public` where it expected `WriteOnly` and fails the build's own
/// 55000. The published `built_at` stays the parked build's `S` — no
/// second scan, no regression. Mutant ORDER-DROP (the entry lock
/// removed): the resume never parks, runs its whole ladder under the
/// parked build and returns `Ok`. Mutant ORDER-SWAP (the lock taken
/// after the re-read): the resume parks, but its state check already
/// passed before the wait — it clobbers the publish back to `WriteOnly`
/// and also returns `Ok`.
#[test]
fn concurrent_second_build_same_index() {
    let rig = BuildRig::new();
    let oid = table_with_rows(&rig, b"csb", &[(1, "a"), (2, "b")]);

    // Build A: CONCURRENTLY, parked inside T3 right before its publish
    // (its S registered, its entries ingested, its txn — and therefore
    // its ShareUpdateExclusive — still open). The release latch is an
    // `AtomicBool` (a hook must be `Sync`, and an mpsc receiver is not).
    let (parked_tx, parked_rx) = mpsc::channel::<()>();
    let release = Arc::new(AtomicBool::new(false));
    let release_h = Arc::clone(&release);
    let s_a: Arc<Mutex<Option<Ts>>> = Arc::new(Mutex::new(None));
    let s_hook = Arc::clone(&s_a);
    let hooks_a = BuildHooks {
        after_register: Some(Box::new(move |s| {
            *s_hook.lock().expect("s_a") = Some(s);
        })),
        before_publish: Some(Box::new(move || {
            let _ = parked_tx.send(());
            // Bounded even if the release never comes (a hang is a
            // failure, never a pass).
            wait_until(BOUND, || release_h.load(Ordering::SeqCst));
        })),
        ..Default::default()
    };
    let db_a = Arc::clone(&rig.db);
    let build_a =
        spawn_run(move || create_index_online(&db_a, oid, &payload_def(), true, &hooks_a));
    match parked_rx.recv_timeout(BOUND) {
        Ok(()) => {}
        Err(_) => panic!("build A never parked mid-T3"),
    }

    // The second build: `resume_build` on the same parked index.
    let idx_oid = only_idx_of(&rig.db, oid);
    let r_done = Arc::new(AtomicBool::new(false));
    let r_done_w = Arc::clone(&r_done);
    let db_r = Arc::clone(&rig.db);
    let resume = spawn_run(move || {
        let out = resume_build(&db_r, idx_oid, &BuildHooks::default());
        r_done_w.store(true, Ordering::SeqCst);
        out
    });

    // The serialized shape: the resume is parked on A's open T3 (the
    // wait-for edge) and has not finished when A is released. Under the
    // mutants no edge ever appears — the resume runs past the parked
    // build and finishes on its own.
    wait_until(BOUND, || {
        !rig.core().wait_edges().is_empty() || r_done.load(Ordering::SeqCst)
    });
    let serialized = !rig.core().wait_edges().is_empty();
    release.store(true, Ordering::SeqCst);

    // A completes and publishes its own S.
    let idx = match build_a.recv_timeout(BOUND) {
        Ok(Ok(idx)) => idx,
        Ok(Err(e)) => panic!("build A failed: {e:?}"),
        Err(_) => panic!("build A never finished"),
    };
    // The resume: serialized to the build's own 55000.
    let err = match resume.recv_timeout(BOUND) {
        Ok(r) => r.expect_err("the second build must not run past the parked one"),
        Err(_) => panic!("the resume never finished"),
    };
    assert_eq!(err.sqlstate(), "55000", "{err:?}");
    assert!(
        serialized,
        "the resume parked on A's ShareUpdateExclusive before A was released"
    );

    // The publish is A's and only A's: Public at A's own S, amcheck clean.
    let def = rig.def_of(idx);
    assert_eq!(def.state, IndexState::Public);
    let s_a = s_a.lock().expect("s_a").expect("A registered S");
    assert_eq!(def.built_at, s_a.0, "built_at is A's S, never rewritten");
    assert!(def.concurrently);
    let report = rig.amcheck(oid);
    assert!(report.is_clean(), "{report:?}");
    rig.shutdown();
}
