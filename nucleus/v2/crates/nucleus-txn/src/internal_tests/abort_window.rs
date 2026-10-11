//! C-T5-ABORT: the abort-window write skew (draft 7.6's gate finding,
//! ported from the reviewer repro). `Core::abort` used to call
//! `set_aborted` first and run the release hooks afterwards, and §6 says
//! an `Aborted` holder never conflicts — so in the window between the
//! two, a concurrent NOWAIT acquirer was granted the relation while the
//! SSI storage map still reflected the aborting DDL, its writes resolved
//! rw-edges against the unmapped range, and a serialization cycle passed
//! undetected. The fix settles the catalog map journal in
//! [`crate::commit::ReleaseHook::before_abort`], before `set_aborted`.
//!
//! An internal test because the wrapper hook needs
//! [`Core::release_hook`](crate::boot::Core::release_hook) (crate-private)
//! to layer itself outermost, above the catalog's
//! [`crate::catalog::CatalogRelease`], freezing the abort exactly at the
//! moment the lock manager has dropped the target's locks — the earliest
//! a concurrent session can acquire the relation.
//!
//! Mutants (the card's binding evidence): M-A — `before_abort` not
//! called (the old order) — fails `abort_window_settles_the_map_before_aborted`
//! (no X->W edge); M-B — `before_abort` called after `set_aborted` —
//! fails it too (`before_abort` must fire while the txn is still
//! `Pending`). The control keeps the post-abort behaviour: the edge is
//! recorded and the skew is detected.

use std::sync::mpsc;
use std::sync::{Arc, Mutex};

use nucleus_kv::MemKv;

use crate::boot::Core;
use crate::catalog::{table_range, Catalog, Ddl, RelKind};
use crate::commit::{CommitConfig, CommitPipeline, ReleaseHook, SyncCommit};
use crate::locks::{LockManager, RelLockMode};
use crate::ssi::Ssi;
use crate::status::Remembered;
use crate::txn::{Isolation, Txn};
use crate::visibility::ReadCtx;
use crate::write::{LockWait, StmtCtx, UniqueRule};
use crate::{Seq, Ts, TxnError, TxnId, TxnStatus};

fn ok<T, E: std::fmt::Debug>(r: Result<T, E>) -> T {
    match r {
        Ok(v) => v,
        Err(e) => panic!("unexpected error: {e:?}"),
    }
}

/// The outermost [`ReleaseHook`] of the window test: layered above the
/// catalog's wrapper (installed after `Catalog::open`, chaining around
/// whatever the core carried), it freezes the armed target's
/// `release_all` at the AE-free moment — after the lock manager has
/// dropped the target's locks, before anything later in the abort runs.
/// `before_abort` records whether the target was still `Pending` when it
/// fired (the M-B discriminator: the settle must precede `set_aborted`).
struct Outer {
    inner: Arc<dyn ReleaseHook>,
    core: Arc<Core<MemKv>>,
    target: Mutex<Option<TxnId>>,
    fired: Mutex<Option<mpsc::Sender<bool>>>,
    proceed: Mutex<Option<mpsc::Receiver<()>>>,
    /// `Some(was_pending)` once `before_abort` fired for the target.
    before_pending: Mutex<Option<bool>>,
}

impl ReleaseHook for Outer {
    fn before_abort(&self, txn: TxnId) {
        if *self.target.lock().expect("gate target") == Some(txn) {
            *self.before_pending.lock().expect("before_pending") = Some(matches!(
                self.core.status.lookup_remembered(txn),
                Remembered::Live(TxnStatus::Pending, _)
            ));
        }
        self.inner.before_abort(txn);
    }

    fn release_all(&self, txn: TxnId) {
        if *self.target.lock().expect("gate target") == Some(txn) {
            let aborted =
                matches!(self.core.status.lookup_for_intent(txn), Ok(TxnStatus::Aborted));
            if let Some(tx) = self.fired.lock().expect("fired").take() {
                ok(tx.send(aborted));
                let rx = self
                    .proceed
                    .lock()
                    .expect("proceed")
                    .take()
                    .expect("proceed channel");
                ok(rx.recv());
            }
        }
        self.inner.release_all(txn);
    }

    fn release_from(&self, txn: TxnId, s: Seq) -> bool {
        self.inner.release_from(txn, s)
    }
}

fn commit(
    core: &Core<MemKv>,
    p: &Mutex<CommitPipeline<MemKv>>,
    txn: Txn,
) -> Result<Ts, TxnError> {
    let ticket = core.commit_submit(txn, SyncCommit::On)?;
    {
        let mut p = p.lock().expect("pipeline");
        let g = p.drain_available();
        p.process_group(g);
    }
    ticket.wait()
}

fn ins(core: &Core<MemKv>, txn: &Txn, s: Ts, key: &[u8]) {
    let seq = ok(txn.next_seq());
    ok(core.insert_key(
        txn,
        key,
        None,
        b"v".to_vec(),
        StmtCtx::new(s, seq, seq),
        UniqueRule::None,
    ));
}

/// The gate's scenario. `window`: the abort is frozen at the AE-free
/// moment and W writes there (the anomaly's interleaving); otherwise W
/// acts after the abort finished (the control). Returns (edge X->W
/// recorded, whether `before_abort` fired for S while Pending, X's commit
/// result, W's commit result).
fn run(window: bool) -> (bool, Option<bool>, Result<Ts, TxnError>, Result<Ts, TxnError>) {
    let core = Arc::new(ok(Core::open(MemKv::new())));
    let ssi = Ssi::install(&core);
    let locks = LockManager::install(&core);
    let _catalog = ok(Catalog::open(&core, &ssi, &locks));
    let (ftx, frx) = mpsc::channel();
    let (ptx, prx) = mpsc::channel();
    let outer = Arc::new(Outer {
        inner: core.release_hook(),
        core: Arc::clone(&core),
        target: Mutex::new(None),
        fired: Mutex::new(Some(ftx)),
        proceed: Mutex::new(Some(prx)),
        before_pending: Mutex::new(None),
    });
    core.set_release_hook(Arc::clone(&outer) as Arc<dyn ReleaseHook>);
    let p = Mutex::new(ok(CommitPipeline::with_config(
        Arc::clone(&core),
        CommitConfig::new().with_observer(ssi.commit_observer()),
    )));

    // A committed table t on storage sid.
    let t0 = core.begin(Isolation::ReadCommitted);
    let seq = ok(t0.next_seq());
    let s0 = core.visible_ts();
    let (oid, sid) = ok(_catalog.catalog_create_table(
        &Ddl::new(&core, &t0, StmtCtx::new(s0, seq, seq)),
        b"t",
        RelKind::Table,
        b"",
    ));
    ok(commit(&core, &p, t0));

    // X, W SER and concurrent: W reads /y (absent), X holds a relation
    // SIREAD on t and writes /y — the write gives W -> X.
    let x = core.begin(Isolation::Serializable);
    let gx = ok(ssi.begin(&core, &x, false));
    let w = core.begin(Isolation::Serializable);
    let gw = ok(ssi.begin(&core, &w, false));
    let (sx, sw) = (gx.ts(), gw.ts());

    let ctx = ReadCtx {
        txn: w.id,
        snapshot: sw,
        stmt_seq: ok(w.next_seq()),
    };
    assert_eq!(ok(ssi.read_key(&core, w.id, b"/y", &ctx)), None);
    ok(ssi.lock_relation_read(x.id, oid as u32));
    ins(&core, &x, sx, b"/y");
    assert!(ssi.edges().contains(&(w.id, x.id)));

    // S (RC) drops t (unmaps the range, journalled), then aborts.
    let s = core.begin(Isolation::ReadCommitted);
    let seq = ok(s.next_seq());
    let ss = core.visible_ts();
    ok(_catalog.catalog_drop_table(
        &Ddl::new(&core, &s, StmtCtx::new(ss, seq, seq)),
        oid,
    ));
    let k = {
        let mut k = table_range(sid).0;
        k.extend_from_slice(b"k");
        k
    };
    let w_writes = |w: &Txn| {
        // A fresh DML acquirer: RowExclusive NOWAIT on t, then a write in
        // t's storage range.
        ok(locks.lock_relation(
            &core,
            w,
            oid,
            RelLockMode::RowExclusive,
            LockWait::NoWait,
            None,
        ));
        ins(&core, w, sw, &k);
    };
    if window {
        *outer.target.lock().expect("gate target") = Some(s.id);
        let c2 = Arc::clone(&core);
        let h = std::thread::spawn(move || ok(c2.abort(s)));
        let aborted = ok(frx.recv());
        assert!(aborted, "status is Aborted when the release hook starts");
        w_writes(&w);
        ok(ptx.send(()));
        ok(h.join());
    } else {
        ok(core.abort(s));
        w_writes(&w);
    }
    let edge = ssi.edges().contains(&(x.id, w.id));
    let before = *outer.before_pending.lock().expect("before_pending");
    let rx = commit(&core, &p, x);
    drop(gx);
    let rw = commit(&core, &p, w);
    drop(gw);
    (edge, before, rx, rw)
}

/// Control: W acts after the abort finished — the X->W edge is recorded
/// and the skew is detected (one of X, W gets the 40001).
#[test]
fn abort_window_control_after_the_abort_finished() {
    let (edge, _before, rx, rw) = run(false);
    assert!(edge, "the edge X->W is recorded outside the window");
    assert!(
        rx.is_err() || rw.is_err(),
        "the write skew is detected (X={rx:?} W={rw:?})"
    );
}

/// The window: W writes at the AE-free moment, mid-abort. The journal
/// must already be settled there (`before_abort`, while Pending), so the
/// X->W edge is recorded and the cycle W->X->W is caught.
#[test]
fn abort_window_settles_the_map_before_aborted() {
    let (edge, before, rx, rw) = run(true);
    assert_eq!(
        before,
        Some(true),
        "the journal settles in before_abort, before set_aborted (still Pending)"
    );
    assert!(
        edge,
        "missed X->W in the set_aborted..settle window: W's write in t's range \
         resolved no relation edge because the map still said the DROP's"
    );
    assert!(
        rx.is_err() || rw.is_err(),
        "the write skew must be detected (X={rx:?} W={rw:?})"
    );
    assert!(
        matches!(rw, Err(TxnError::SerializationFailure)),
        "W, the txn that wrote in the window, is the victim (W={rw:?})"
    );
}
