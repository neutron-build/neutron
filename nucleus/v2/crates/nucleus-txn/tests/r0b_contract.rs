//! C-R0ba tests: the blocking contract (`rt`) — thread roles and guard
//! nesting, every checked funnel refusing on an executor thread with the
//! right class and site, the N-class entry points running normally on an
//! executor thread, `allow_blocking` reopening them, and the refusal
//! firing before any state change.
//!
//! C-R0bFIX adds: reads through a held view refusing on the executor
//! (M-1), a refused shutdown leaving the pipeline running and a later
//! off-executor shutdown succeeding (M-2), and the ordering pins — a
//! refused `open_view` registers nothing (O1), a refused
//! `wait_on_any_deadline` fires no wait-hook events (O9), and dropping a
//! live commit handle refuses (O2).
//!
//! Setup discipline: every `Core`, txn and pipeline is set up off-executor;
//! `on_executor` marks the thread only around the call under test.

use std::ops::Bound;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use nucleus_kv::{Batch, Durability, MemKv};

use nucleus_txn::boot::Core;
use nucleus_txn::commit::{spawn_commit_thread, CommitPipeline, SyncCommit};
use nucleus_txn::locks::{LockManager, RelLockMode};
use nucleus_txn::read::{read_key, scan, NoSsi};
use nucleus_txn::rt::{self, Blocking};
use nucleus_txn::seq::sys_seq_key;
use nucleus_txn::txn::Isolation;
use nucleus_txn::visibility::ReadCtx;
use nucleus_txn::wait::{WaitBegin, WaitHook};
use nucleus_txn::write::{LockWait, StmtCtx, UniqueRule};
use nucleus_txn::{Ts, TxnError, TxnId};

fn ok<T, E: std::fmt::Debug>(r: Result<T, E>) -> T {
    match r {
        Ok(v) => v,
        Err(e) => panic!("unexpected error: {e:?}"),
    }
}

/// Marks the current thread as an executor thread, runs `f`, and catches
/// the panic a refused blocking call raises, turning its payload into the
/// `Err`.
fn on_executor<R>(f: impl FnOnce() -> R) -> Result<R, String> {
    let _guard = rt::enter_executor();
    assert!(rt::is_executor_thread(), "the guard marks the thread");
    std::panic::catch_unwind(AssertUnwindSafe(f)).map_err(|payload| {
        if let Some(s) = payload.downcast_ref::<String>() {
            s.clone()
        } else if let Some(s) = payload.downcast_ref::<&'static str>() {
            (*s).to_string()
        } else {
            "panic with a non-string payload".to_string()
        }
    })
}

/// Asserts `r` is a refusal whose message names the violation, the class
/// and the site.
fn assert_blocked<R>(r: Result<R, String>, kind: Blocking, site: &str) {
    let msg = match r {
        Err(msg) => msg,
        Ok(_) => panic!("expected a {kind:?} refusal at {site}, got a normal return"),
    };
    assert!(msg.contains(rt::VIOLATION), "not a C-R0b refusal: {msg:?}");
    assert!(
        msg.contains(&format!("{kind:?} at")),
        "wrong class in {msg:?}, wanted {kind:?}"
    );
    assert!(msg.contains(site), "wrong site in {msg:?}, wanted {site}");
}

/// A fresh core over a fresh `MemKv` (off-executor, like every setup
/// here).
fn fresh_core() -> Core<MemKv> {
    ok(Core::open(MemKv::new()))
}

/// The current wake generation of `id` on `core`.
fn gen_of(core: &Core<MemKv>, id: TxnId) -> u64 {
    core.status.entry(id).map(|e| e.gen).unwrap_or(0)
}

// (a)
#[test]
fn roles_default_unmarked_and_guards_nest_and_restore() {
    // A fresh thread is unmarked.
    let fresh = std::thread::spawn(rt::is_executor_thread)
        .join()
        .expect("fresh thread join");
    assert!(!fresh, "a fresh thread is unmarked");

    // enter → marked; nested allow_blocking → unmarked; drop it → marked;
    // drop the outer guard → unmarked.
    assert!(!rt::is_executor_thread());
    {
        let _e = rt::enter_executor();
        assert!(rt::is_executor_thread(), "enter_executor marks");
        {
            let _a = rt::allow_blocking();
            assert!(!rt::is_executor_thread(), "allow_blocking unmarks");
        }
        assert!(
            rt::is_executor_thread(),
            "dropping allow_blocking restores the executor mark"
        );
    }
    assert!(
        !rt::is_executor_thread(),
        "dropping enter_executor restores unmarked"
    );

    // mark_executor_thread persists across calls on that thread and does
    // not leak to other threads.
    let persists = std::thread::spawn(|| {
        rt::mark_executor_thread();
        fn nested_probe() -> bool {
            rt::is_executor_thread()
        }
        nested_probe() && nested_probe() && rt::is_executor_thread()
    })
    .join()
    .expect("marked thread join");
    assert!(persists, "mark_executor_thread persists across calls");
    assert!(
        !rt::is_executor_thread(),
        "the mark did not leak to this thread"
    );
}

// (b)
#[test]
fn fsync_funnels_refuse_on_executor() {
    let core = fresh_core();
    let mut batch = Batch::default();
    batch.put(b"b/k".to_vec(), b"v".to_vec());
    assert_blocked(
        on_executor(|| core.sync_wal()),
        Blocking::Fsync,
        "Core::sync_wal",
    );
    assert_blocked(
        on_executor(|| core.write(batch.clone(), Durability::Yes)),
        Blocking::Fsync,
        "Core::write",
    );
    assert_blocked(
        on_executor(|| Core::open(MemKv::new())),
        Blocking::Fsync,
        "Core::open",
    );
}

// (c)
#[test]
fn kv_funnels_refuse_on_executor() {
    let core = fresh_core();
    let mut batch = Batch::default();
    batch.put(b"c/k".to_vec(), b"v".to_vec());
    assert_blocked(
        on_executor(|| core.write(batch.clone(), Durability::No)),
        Blocking::KvIo,
        "Core::write",
    );
    assert_blocked(
        on_executor(|| core.latest_get(b"c/k")),
        Blocking::KvIo,
        "Core::latest_get",
    );
    // `Core::open_view` delegates to `Registry::open_view` — the one
    // funnel — so both report its site.
    assert_blocked(
        on_executor(|| core.open_view()),
        Blocking::KvIo,
        "Registry::open_view",
    );
    // The registry funnel reached directly, through its own KV handle.
    let kv = MemKv::new();
    assert_blocked(
        on_executor(|| core.registry.open_view(&kv)),
        Blocking::KvIo,
        "Registry::open_view",
    );
    let id = TxnId {
        epoch: core.epoch(),
        n: 1,
    };
    assert_blocked(
        on_executor(|| core.truncate_status(id)),
        Blocking::KvIo,
        "Core::truncate_status",
    );
}

// (d)
#[test]
fn park_funnels_refuse_on_executor() {
    let core = fresh_core();
    let target = core.begin(Isolation::ReadCommitted);
    let waiter = core.begin(Isolation::ReadCommitted);
    let g = gen_of(&core, target.id);

    // The blocking wait, before the hooks and any registration.
    assert_blocked(
        on_executor(|| core.wait_on_any_deadline(&waiter, &[(target.id, g)], None)),
        Blocking::Park,
        "Core::wait_on_any_deadline",
    );

    // The step API's begin is N-class on an executor thread; its park is
    // Park-class.
    let handle = match on_executor(|| match core.wait_begin(&waiter, &[(target.id, g)]) {
        WaitBegin::Registered(h) => h,
        other => panic!("wait_begin must register on a live target, got {other:?}"),
    }) {
        Ok(h) => h,
        Err(msg) => panic!("wait_begin is N-class, refused: {msg}"),
    };
    assert_blocked(
        on_executor(move || handle.park(Duration::ZERO)),
        Blocking::Park,
        "WaitHandle::park",
    );

    // The lock manager's Block policy refuses even when the lock is free.
    let core2 = Arc::new(fresh_core());
    let mgr = LockManager::install(&core2);
    let t = core2.begin(Isolation::ReadCommitted);
    assert_blocked(
        on_executor(|| {
            mgr.lock_relation(
                &core2,
                &t,
                7,
                RelLockMode::AccessExclusive,
                LockWait::Block,
                None,
            )
        }),
        Blocking::Park,
        "LockManager::lock_relation",
    );
    assert_blocked(
        on_executor(|| mgr.advisory_xact_lock(&core2, &t, 9, false, LockWait::Block, None)),
        Blocking::Park,
        "LockManager::advisory_xact_lock",
    );
}

// (e)
#[test]
fn commit_and_seq_refuse_on_executor() {
    let core = Arc::new(fresh_core());

    // A read-only commit has a no-I/O fast path; the entry check still
    // fires, so misuse is detected deterministically.
    let ro = core.begin(Isolation::ReadCommitted);
    assert_blocked(
        on_executor(|| core.commit(ro, SyncCommit::On)),
        Blocking::Fsync,
        "Core::commit",
    );

    // An unanswered ticket's wait blocks on recv for an ack whose duration
    // includes the group's fsync. Built with CommitPipeline::new and never
    // processed.
    let _pipeline = ok(CommitPipeline::new(Arc::clone(&core)));
    let w = core.begin(Isolation::ReadCommitted);
    let seq = ok(w.next_seq());
    w.log_write(seq, b"e/tk", None);
    let ticket = ok(core.commit_submit(w, SyncCommit::On));
    assert_blocked(
        on_executor(move || ticket.wait()),
        Blocking::Fsync,
        "CommitTicket::wait",
    );

    // A warmed sequence's cached path does no KV call; the entry check is
    // what refuses.
    let id = 42u64;
    assert_eq!(ok(core.seq_next(id)), 1, "warm the cache off-executor");
    assert_blocked(
        on_executor(|| core.seq_next(id)),
        Blocking::Fsync,
        "Core::seq_next",
    );
}

// (f)
#[test]
fn join_refuses_on_executor() {
    let core = Arc::new(fresh_core());
    // spawn_commit_thread runs off-executor (this thread is unmarked).
    let handle = ok(spawn_commit_thread(Arc::clone(&core)));
    assert_blocked(
        on_executor(move || handle.shutdown()),
        Blocking::Join,
        "CommitThreadHandle::shutdown",
    );
}

// (g)
#[test]
fn non_blocking_entry_points_run_on_executor() {
    let core = fresh_core();
    let mgr = LockManager::install(&core);

    // begin / next_seq / cancel / take_snapshot / visible_ts.
    let txn = match on_executor(|| {
        let txn = core.begin(Isolation::ReadCommitted);
        ok(txn.next_seq());
        txn.cancel_handle().cancel();
        {
            let snap = core.registry.take_snapshot();
            let _ = snap.ts();
        }
        let _ = core.visible_ts();
        let _ = core.epoch();
        let _ = core.ts_hwm();
        txn
    }) {
        Ok(txn) => txn,
        Err(msg) => panic!("begin and friends are N-class, refused: {msg}"),
    };

    // commit_submit of a read-only txn, try_ack, and wait on the
    // already-answered ticket: Ts::ZERO, no panic.
    match on_executor(|| {
        let ticket = ok(core.commit_submit(txn, SyncCommit::On));
        match ticket.try_ack() {
            Some(Ok(ts)) => assert_eq!(ts, Ts::ZERO, "read-only commit ts"),
            other => panic!("try_ack: {other:?}"),
        }
        ticket.wait()
    }) {
        Ok(Ok(ts)) => assert_eq!(ts, Ts::ZERO, "wait on an answered ticket"),
        Ok(Err(e)) => panic!("wait on an answered ticket: {e:?}"),
        Err(msg) => panic!("commit_submit/try_ack/wait are N-class, refused: {msg}"),
    }

    // abort of a txn with no writes.
    match on_executor(|| {
        let t = core.begin(Isolation::ReadCommitted);
        core.abort(t)
    }) {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("abort: {e:?}"),
        Err(msg) => panic!("abort is N-class, refused: {msg}"),
    }

    // The wait steps on a live target.
    let target = core.begin(Isolation::ReadCommitted);
    let waiter = core.begin(Isolation::ReadCommitted);
    let g = gen_of(&core, target.id);
    match on_executor(|| match core.wait_begin(&waiter, &[(target.id, g)]) {
        WaitBegin::Registered(h) => {
            assert!(
                core.wait_poll(&h).is_none(),
                "the target is still live: the wait is not over"
            );
            core.wait_end(h);
        }
        other => panic!("wait_begin must register on a live target, got {other:?}"),
    }) {
        Ok(()) => {}
        Err(msg) => panic!("the wait steps are N-class, refused: {msg}"),
    }

    // lock_relation with NoWait: Ok(true) on a free lock, 55P03 on a held
    // one.
    let holder = core.begin(Isolation::ReadCommitted);
    assert!(
        ok(mgr.lock_relation(
            &core,
            &holder,
            7,
            RelLockMode::AccessExclusive,
            LockWait::NoWait,
            None
        )),
        "holder acquires off-executor"
    );
    let u = core.begin(Isolation::ReadCommitted);
    match on_executor(|| {
        assert!(
            ok(mgr.lock_relation(
                &core,
                &u,
                8,
                RelLockMode::AccessExclusive,
                LockWait::NoWait,
                None
            )),
            "NoWait on a free lock acquires"
        );
        match mgr.lock_relation(
            &core,
            &u,
            7,
            RelLockMode::AccessExclusive,
            LockWait::NoWait,
            None,
        ) {
            Err(TxnError::LockNotAvailable) => {}
            other => panic!("NoWait on a held lock must be 55P03, got {other:?}"),
        }
    }) {
        Ok(()) => {}
        Err(msg) => panic!("NoWait lock_relation is N-class, refused: {msg}"),
    }
}

// (h)
#[test]
fn allow_blocking_section_permits_blocking_calls() {
    let core = fresh_core();
    let mut batch = Batch::default();
    batch.put(b"h/k".to_vec(), b"v".to_vec());
    match on_executor(|| {
        {
            let _a = rt::allow_blocking();
            ok(core.sync_wal());
            ok(core.write(batch.clone(), Durability::Yes));
        }
        // The allow_blocking guard is dropped; the executor mark must be
        // back while the outer enter_executor scope is still open.
        rt::is_executor_thread()
    }) {
        Ok(still_marked) => assert!(
            still_marked,
            "dropping allow_blocking restores the executor mark inside the scope"
        ),
        Err(msg) => panic!("allow_blocking must permit blocking calls: {msg}"),
    }
    assert!(
        !rt::is_executor_thread(),
        "the outer enter_executor guard is gone with the scope"
    );
}

// (i) — all on one thread.
#[test]
fn refusal_happens_before_state_change() {
    let core = Arc::new(fresh_core());
    let _commit = ok(spawn_commit_thread(Arc::clone(&core)));

    // insert_key of a fresh key: the refusal fires inside the first view
    // open, before the write-set log, the placement count or any write.
    let txn = core.begin(Isolation::ReadCommitted);
    let seq = ok(txn.next_seq());
    let ctx = StmtCtx::new(core.visible_ts(), seq, seq);
    let key = b"t/1/7";
    assert_blocked(
        on_executor(|| {
            core.insert_key(
                &txn,
                key,
                None,
                b"v".to_vec(),
                ctx.clone(),
                UniqueRule::Unique { same_row: None },
            )
        }),
        Blocking::KvIo,
        "Registry::open_view",
    );

    // Off-executor, on the same thread: the key has no intent, the latch
    // flag is clear (a new latch succeeds without the debug assert
    // firing), and the same txn can still insert and commit that key.
    {
        let view = core.open_view();
        assert!(
            matches!(view.get(&nucleus_txn::encoding::intent_key(key)), Ok(None)),
            "the refused insert left no intent"
        );
    }
    {
        let _latch = core.latches.lock(b"any-other-key");
    }
    ok(core.insert_key(
        &txn,
        key,
        None,
        b"v".to_vec(),
        ctx,
        UniqueRule::Unique { same_row: None },
    ));
    let ts = ok(core.commit(txn, SyncCommit::On));
    assert!(ts > Ts::ZERO, "the txn committed after the refusal");

    // seq_next on a fresh id: the refusal fires before the KV read and the
    // synced create, so nothing of the sequence exists.
    let fresh = 7u64;
    assert_blocked(
        on_executor(|| core.seq_next(fresh)),
        Blocking::Fsync,
        "Core::seq_next",
    );
    assert!(
        matches!(core.latest_get(&sys_seq_key(fresh)), Ok(None)),
        "/sys/seq/{fresh} is absent after the refusal"
    );
    assert_eq!(
        core.seq_reservations_for_tests(fresh),
        None,
        "no reservation was counted after the refusal"
    );
}

// (j) — M-1: the held-view read funnel.
#[test]
fn held_view_reads_refuse_on_executor() {
    let core = fresh_core();
    // The view is opened off-executor (opening one there is already
    // refused); every read below happens on the executor thread. This is
    // the async misuse shape: open in a blocking section, read on the
    // executor.
    let view = core.open_view();
    let ctx = ReadCtx {
        txn: TxnId {
            epoch: core.epoch(),
            n: 0,
        },
        snapshot: core.visible_ts(),
        stmt_seq: 0,
    };
    assert_blocked(
        on_executor(|| view.get(b"m/k")),
        Blocking::KvIo,
        "ViewGuard::get",
    );
    // `read::read_key` reaches the KV through `view.as_snap()`.
    assert_blocked(
        on_executor(|| read_key(&core, &view, b"m/k", &ctx, &mut NoSsi)),
        Blocking::KvIo,
        "ViewGuard::as_snap",
    );
    // `read::scan` opens its iterator through `view.scan(..)`; the panic
    // fires while the scan is being built, before `.next()` runs.
    assert_blocked(
        on_executor(|| {
            scan(
                &core,
                &view,
                (Bound::Included(&b"m"[..]), Bound::Excluded(&b"n"[..])),
                &ctx,
                &mut NoSsi,
            )
            .next()
        }),
        Blocking::KvIo,
        "ViewGuard::scan",
    );
    // Off-executor, the same held view reads normally: a refusal must
    // not poison the guard.
    assert!(
        matches!(view.get(b"m/k"), Ok(None)),
        "the held view still reads off-executor"
    );
}

// (k) — M-2: a refused shutdown has no side effects.
#[test]
fn refused_shutdown_leaves_pipeline_running() {
    let core = Arc::new(fresh_core());
    let handle = ok(spawn_commit_thread(Arc::clone(&core)));

    // The refused shutdown panics at its check; the unwind drops the
    // consumed handle. That drop must neither send Stop nor join —
    // otherwise the blocking Join the check refused runs anyway, on the
    // executor thread, and the pipeline is stopped as a side effect of
    // the panic.
    assert_blocked(
        on_executor(move || handle.shutdown()),
        Blocking::Join,
        "CommitThreadHandle::shutdown",
    );

    // The pipeline is still attached and running: a write txn commits
    // (before the fix, the unwind's Stop had stopped it and this
    // returned `Invariant("commit pipeline stopped")`).
    let txn = core.begin(Isolation::ReadCommitted);
    let seq = ok(txn.next_seq());
    txn.log_write(seq, b"n/k", None);
    let ts = ok(core.commit(txn, SyncCommit::On));
    assert!(ts > Ts::ZERO, "the pipeline committed the write txn");

    // An off-executor shutdown then succeeds. The refused handle was
    // consumed by the unwind and one core carries one pipeline, so this
    // must be a fresh pipeline; what it pins is that the refusal left
    // the shutdown path itself working on this thread.
    let core2 = Arc::new(fresh_core());
    let handle2 = ok(spawn_commit_thread(Arc::clone(&core2)));
    ok(handle2.shutdown());
}

// (l) — O1 pin: the open_view check is before the registration.
#[test]
fn refused_open_view_registers_nothing() {
    let core = fresh_core();
    assert_eq!(
        core.registry.min_view_counter(),
        u64::MAX,
        "no view is open on a fresh core"
    );
    assert_blocked(
        on_executor(|| core.open_view()),
        Blocking::KvIo,
        "Registry::open_view",
    );
    // A refused open must not leave a registered view behind: a leaked
    // counter would pin `min_view_counter` and hold back GC's W (§9.1)
    // forever.
    assert_eq!(
        core.registry.min_view_counter(),
        u64::MAX,
        "the refused open_view registered no view"
    );
}

// (m) — O9 pin: the wait check is before the hooks.
#[test]
fn refused_wait_records_no_hook_events() {
    /// Records every `on_wait_start` / `on_wait_end` pair.
    #[derive(Default)]
    struct Rec {
        starts: Mutex<Vec<(TxnId, TxnId)>>,
        ends: Mutex<Vec<(TxnId, TxnId)>>,
    }
    impl WaitHook for Rec {
        fn on_wait_start(&self, waiter: TxnId, target: TxnId) {
            self.starts.lock().unwrap().push((waiter, target));
        }
        fn on_wait_end(&self, waiter: TxnId, target: TxnId) {
            self.ends.lock().unwrap().push((waiter, target));
        }
    }

    let core = fresh_core();
    let target = core.begin(Isolation::ReadCommitted);
    let waiter = core.begin(Isolation::ReadCommitted);
    let g = gen_of(&core, target.id);
    let rec = Arc::new(Rec::default());
    core.waits.set_hook(Arc::clone(&rec) as Arc<dyn WaitHook>);
    assert_blocked(
        on_executor(|| core.wait_on_any_deadline(&waiter, &[(target.id, g)], None)),
        Blocking::Park,
        "Core::wait_on_any_deadline",
    );
    // A refused wait must fire no hook events: an `on_wait_start` without
    // its `on_wait_end` (what a check placed after the hook loop would
    // produce) is an unbalanced observation.
    assert!(
        rec.starts.lock().unwrap().is_empty(),
        "the refused wait fired no on_wait_start"
    );
    assert!(
        rec.ends.lock().unwrap().is_empty(),
        "the refused wait fired no on_wait_end"
    );
    assert!(
        core.wait_edges().is_empty(),
        "the refused wait inserted no edge"
    );
}

// (n) — O2 pin: the drop check.
#[test]
fn commit_handle_drop_refuses_on_executor() {
    let core = Arc::new(fresh_core());
    let handle = ok(spawn_commit_thread(Arc::clone(&core)));
    assert_blocked(
        on_executor(move || drop(handle)),
        Blocking::Join,
        "CommitThreadHandle::drop",
    );
}
