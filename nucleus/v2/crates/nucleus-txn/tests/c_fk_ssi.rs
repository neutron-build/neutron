//! C-FK-SSI: the SERIALIZABLE FK child check records its reader-side rw
//! edges (§8.2), closing the ESCALATE-2 write skew G2 found. The cycle
//! (both txns SERIALIZABLE, parent `p0` exists, child `c0` does not):
//!
//! 1. A reads `c0` → absent (an SSI point read: SIREAD on `c0`).
//! 2. A updates `p0` **non-key** (`key_cols_changed: false`): a
//!    `NoKeyUpdate` intent. A has no SIREAD on `p0` yet, so the writer
//!    side gives no edge.
//! 3. B inserts `c0`. The writer-side check (`on_data_placed`) finds A's
//!    SIREAD on `c0`: edge **A → B**.
//! 4. B's FK check registers a SIREAD on `p0`, opens a view, reads `p0`
//!    at `S(B)` and skips A's Pending intent (or, once A committed, A's
//!    version `> S(B)`). That is edge **B → A**; before this card the
//!    read ran with `NoSsi` and dropped it.
//! 5. B takes KEY SHARE on `p0`: compatible with A's `NoKeyUpdate`, and
//!    the version above `S` is no tombstone, move or key change, so no
//!    40001 (§5.1) — correctly: KEY SHARE protects the FK, not
//!    serializability.
//! 6. `A → B → A` is a cycle (rw, rw): write skew unless SSI aborts one.
//!
//! Each test names the mutant it kills.

mod ssi_support;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use nucleus_txn::commit::SyncCommit;
use nucleus_txn::locks::LockManager;
use nucleus_txn::read::{read_key, NoSsi};
use nucleus_txn::resolver::Resolver;
use nucleus_txn::ssi::{Siread, Ssi};
use nucleus_txn::txn::{Isolation, Txn};
use nucleus_txn::visibility::{ReadCtx, RwEdge};
use nucleus_txn::write::{RowOp, SsiHook, StmtCtx, UniqueRule};
use nucleus_txn::{Ts, TxnError, TxnId};

use ssi_support::{NoEpq, Rig, Ser};

const P0: &[u8] = b"/t/p0";
const C0: &[u8] = b"/t/c0";
const C1: &[u8] = b"/t/c1";

fn is_k1(v: &[u8]) -> bool {
    v == b"k1"
}

/// A rig with the row-lock table the child check's KEY SHARE needs
/// (`ssi_support::Rig` installs none).
fn lock_rig() -> Rig {
    let rig = Rig::new();
    LockManager::install(&rig.core);
    rig
}

/// The child-side FK check as an internal command at `snapshot` (§5.3: a
/// fresh seq above the statement's).
fn fk_at(rig: &Rig, txn: &Txn, snapshot: Ts, parent: &[u8]) -> Result<(), TxnError> {
    let seq0 = txn.next_seq().expect("seq");
    let ctx = StmtCtx::new(snapshot, seq0, txn.next_seq().expect("seq")).internal();
    rig.core.fk_check_child(txn, &ctx, parent, &is_k1)
}

/// [`fk_at`] for a SER txn, at its registered snapshot.
fn fk(rig: &Rig, t: &Ser<'_>, parent: &[u8]) -> Result<(), TxnError> {
    fk_at(rig, &t.txn, t.s, parent)
}

/// Drains the resolver, so a committed writer's intent becomes a version.
fn resolve(rig: &Rig) {
    while Resolver::run_once(&rig.core).expect("resolve") > 0 {}
}

/// Submit + process + ack for a txn begun with `core.begin` (no `Ser`).
fn commit_plain(rig: &Rig, txn: Txn) -> Result<Ts, TxnError> {
    let ticket = rig.core.commit_submit(txn, SyncCommit::On).expect("submit");
    rig.process();
    ticket.wait()
}

// ---- helpers for the non-SER (RC/RR) interleaving --------------------------

fn stmt_ctx(txn: &Txn, snapshot: Ts) -> StmtCtx {
    let seq = txn.next_seq().expect("seq");
    StmtCtx::new(snapshot, seq, seq)
}

/// A point read at `snapshot` without SSI (the §4 path a non-SER txn uses).
fn plain_read(rig: &Rig, txn: &Txn, snapshot: Ts, key: &[u8]) -> Option<Vec<u8>> {
    let view = rig.core.open_view();
    read_key(
        &rig.core,
        &view,
        key,
        &ReadCtx {
            txn: txn.id,
            snapshot,
            stmt_seq: txn.next_seq().expect("seq"),
        },
        &mut NoSsi,
    )
    .expect("plain read")
}

/// A non-key update of `key` (a `NoKeyUpdate` intent).
fn plain_update(rig: &Rig, txn: &Txn, snapshot: Ts, key: &[u8], value: &[u8]) {
    rig.core
        .row_op(
            txn,
            key,
            None,
            RowOp::Update {
                value: value.to_vec(),
                key_cols_changed: false,
            },
            stmt_ctx(txn, snapshot),
            &mut NoEpq,
        )
        .expect("plain update");
}

/// An insert of `key` (a `NoKeyUpdate` intent).
fn plain_insert(rig: &Rig, txn: &Txn, snapshot: Ts, key: &[u8], value: &[u8]) {
    rig.core
        .insert_key(
            txn,
            key,
            None,
            value.to_vec(),
            stmt_ctx(txn, snapshot),
            UniqueRule::Unique { same_row: None },
        )
        .expect("plain insert");
}

// ---- the SER gate counter ---------------------------------------------------

/// A wrapper around the installed SSI that counts `on_read_edges` calls:
/// the SER gate of the FK reader-side path (zero below SERIALIZABLE, at
/// least one under it).
struct CountingSsi {
    inner: Arc<Ssi>,
    on_read_edges_calls: AtomicUsize,
}

impl CountingSsi {
    fn wrap(rig: &Rig) -> Arc<CountingSsi> {
        let hook = Arc::new(CountingSsi {
            inner: Arc::clone(&rig.ssi),
            on_read_edges_calls: AtomicUsize::new(0),
        });
        rig.core.set_ssi_hook(hook.clone());
        hook
    }

    fn on_read_edges_calls(&self) -> usize {
        self.on_read_edges_calls.load(Ordering::SeqCst)
    }
}

impl SsiHook for CountingSsi {
    fn covers(&self, txn: TxnId, key: &[u8]) -> bool {
        self.inner.covers(txn, key)
    }

    fn before_point_read(&self, txn: TxnId, key: &[u8]) {
        self.inner.before_point_read(txn, key)
    }

    fn on_read_edges(&self, txn: TxnId, edges: &[RwEdge], view_counter: u64) {
        self.on_read_edges_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.on_read_edges(txn, edges, view_counter)
    }

    fn on_data_placed(
        &self,
        writer: TxnId,
        isolation: Isolation,
        key: &[u8],
    ) -> Result<(), TxnError> {
        self.inner.on_data_placed(writer, isolation, key)
    }

    fn pre_commit(
        &self,
        txn: TxnId,
        isolation: Isolation,
        enqueue: &mut dyn FnMut() -> Result<(), TxnError>,
    ) -> Result<(), TxnError> {
        self.inner.pre_commit(txn, isolation, enqueue)
    }

    fn on_abort(&self, txn: TxnId) {
        self.inner.on_abort(txn)
    }
}

// ---- tests ------------------------------------------------------------------

/// Steps 1-6 of the module doc, A still Pending at B's FK check: both
/// edges exist before any commit and exactly one txn aborts — B, the
/// second committer (A's pre-commit dooms it as the pivot of the cycle).
/// Mutants killed: M1 (the SER parent read back to `NoSsi`) and M2
/// (`Ssi::on_read_edges` a no-op): the `B -> A` edge goes missing and
/// both commit.
#[test]
fn fk_parent_pending_writer_write_skew_aborts_one() {
    let rig = lock_rig();
    rig.preload(P0, b"k1");
    let a = rig.ser(false);
    let b = rig.ser(false);

    assert_eq!(rig.read(&a, C0), None, "c0 does not exist yet");
    rig.update(&a, P0, b"k1v2");
    rig.insert(&b, C0, b"k1");
    fk(&rig, &b, P0).expect("the FK check passes (a non-key update)");

    let (ia, ib) = (a.id(), b.id());
    assert!(rig.has_edge(ia, ib), "writer-side edge A -> B");
    assert!(rig.has_edge(ib, ia), "reader-side edge B -> A");

    rig.commit(a).expect("A commits");
    assert_eq!(
        rig.commit(b),
        Err(TxnError::SerializationFailure),
        "exactly one of the pair aborts, and it is B"
    );
}

/// The same cycle with A committed (and resolved) before B's FK check:
/// the reader-side edge now comes from the skipped version `> S(B)`
/// through the writer map (`ToVersionWriter`). The FK check itself still
/// succeeds (non-key update: KEY SHARE passes), and B's commit is 40001.
/// Mutants killed: M1/M2 (missing `B -> A`, both commit).
#[test]
fn fk_parent_committed_writer_write_skew_aborts_one() {
    let rig = lock_rig();
    rig.preload(P0, b"k1");
    let a = rig.ser(false);
    let b = rig.ser(false);

    assert_eq!(rig.read(&a, C0), None, "c0 does not exist yet");
    rig.update(&a, P0, b"k1v2");
    rig.insert(&b, C0, b"k1");
    let (ia, ib) = (a.id(), b.id());
    rig.commit(a).expect("A commits before B's FK check");
    resolve(&rig);

    fk(&rig, &b, P0).expect("the FK check passes (a non-key update)");
    assert!(rig.has_edge(ia, ib), "writer-side edge A -> B");
    assert!(
        rig.has_edge(ib, ia),
        "the ToVersionWriter edge through the writer map"
    );

    assert_eq!(
        rig.commit(b),
        Err(TxnError::SerializationFailure),
        "B aborts on the cycle A -> B -> A"
    );
}

/// No false positive: a lone `B -> A` (A updates the parent without
/// reading the child) is not a dangerous structure, so both commit. The
/// same holds for the mirror order — A's update after B's check, the
/// writer-side edge today — and for either commit order.
/// Mutant killed: recording a writer-side edge where none exists (A's
/// update alone must not give `A -> B`), or a single edge aborting.
#[test]
fn fk_parent_non_key_update_alone_commits_both() {
    // A's update before B's check: exactly one edge, B -> A (reader side).
    let rig = lock_rig();
    rig.preload(P0, b"k1");
    let a = rig.ser(false);
    let b = rig.ser(false);
    rig.update(&a, P0, b"k1v2");
    rig.insert(&b, C0, b"k1");
    fk(&rig, &b, P0).expect("the FK check passes");
    assert!(!rig.has_edge(a.id(), b.id()), "A read no child key");
    assert!(rig.has_edge(b.id(), a.id()));
    assert_eq!(rig.ssi.edges(), vec![(b.id(), a.id())]);
    rig.commit(a).expect("A commits");
    rig.commit(b).expect("B commits");

    // A's update after B's check: the writer-side edge (handled before
    // this card), exactly one edge again, both commit.
    let rig = lock_rig();
    rig.preload(P0, b"k1");
    let a = rig.ser(false);
    let b = rig.ser(false);
    rig.insert(&b, C0, b"k1");
    fk(&rig, &b, P0).expect("the FK check passes");
    rig.update(&a, P0, b"k1v2");
    assert_eq!(rig.ssi.edges(), vec![(b.id(), a.id())]);
    rig.commit(b).expect("B commits");
    rig.commit(a).expect("A commits");
}

/// Two SER txns inserting different children of the same parent: no edges
/// at all (KEY SHARE vs KEY SHARE, no data write on `p0`), both commit.
/// Mutant killed: the KEY SHARE (or the SIREAD registration of the child
/// check) treated as a data-changing placement that checks SIREADs (§8.2
/// forbids it) — `B -> C` would appear and one would abort.
#[test]
fn fk_concurrent_children_same_parent_no_edges() {
    let rig = lock_rig();
    rig.preload(P0, b"k1");
    let b = rig.ser(false);
    let c = rig.ser(false);
    rig.insert(&b, C0, b"k1");
    fk(&rig, &b, P0).expect("B's FK check");
    rig.insert(&c, C1, b"k1");
    fk(&rig, &c, P0).expect("C's FK check");
    assert!(rig.ssi.edges().is_empty(), "{:?}", rig.ssi.edges());
    rig.commit(b).expect("B commits");
    rig.commit(c).expect("C commits");
}

/// A 23503 still records the reader-side edge: B's FK snapshot predates
/// the parent's insertion, so the read skips the writer's version
/// `> S(B)` (an edge) and then finds nothing at `S(B)`. The edge must be
/// recorded even though the check errors.
/// Mutant killed: M4a (`on_read_edges` moved after the `found`/`matches`
/// test, so a 23503 outcome drops the edge).
#[test]
fn fk_missing_parent_23503_still_records_reader_edge() {
    let rig = lock_rig();
    let b = rig.ser(false);
    // The parent appears only after S(B): a committed, resolved version
    // above B's snapshot.
    let w = rig.ser(false);
    let iw = w.id();
    rig.insert(&w, P0, b"k1");
    rig.commit(w).expect("the parent writer commits");
    resolve(&rig);

    rig.insert(&b, C0, b"k1");
    assert_eq!(fk(&rig, &b, P0), Err(TxnError::ForeignKeyViolation));
    assert!(
        rig.has_edge(b.id(), iw),
        "the 23503 outcome must not drop the reader-side edge"
    );
}

/// The FK check's read hands its **own view's** counter to
/// `on_read_edges` (§8.2): the SIREAD registers first (stamped with the
/// registry's view counter), the view opens after it (the counter it was
/// handed is strictly greater), and `last_read_view_counter` reports that
/// view — the monotone relation I-SSI-ORDER is asserted on.
/// Mutant killed: R3 (`fk_check_child` passing `0` instead of
/// `view.counter()`): `last_read_view_counter` reports a counter at or
/// below the SIREAD's registration stamp and the assertion fails.
/// Diagnostic-only (the edges themselves are unaffected), so this is the
/// only observable seam.
#[test]
fn fk_child_read_view_counter_is_past_the_siread_stamp() {
    let rig = lock_rig();
    rig.preload(P0, b"k1");
    let a = rig.ser(false);
    let b = rig.ser(false);
    assert_eq!(rig.read(&a, C0), None);
    rig.update(&a, P0, b"k1v2");
    rig.insert(&b, C0, b"k1");
    // The FK check is B's last SSI read, so `last_read_view_counter`
    // names exactly the view that read the parent.
    fk(&rig, &b, P0).expect("the FK check passes");

    let stamp = rig
        .ssi
        .sireads_of(b.id())
        .into_iter()
        .find_map(|(sr, c)| match sr {
            Siread::Point { ref key } if key.as_slice() == P0 => Some(c),
            _ => None,
        })
        .expect("the FK check registered a point SIREAD on the parent");
    let view = rig
        .ssi
        .last_read_view_counter(b.id())
        .expect("the FK check recorded its read");
    assert!(
        view > stamp,
        "the read ran through a view opened after the SIREAD registered \
         (view counter {view} must be past the stamp {stamp})"
    );
    // The stamp itself was a real registration (the counter handed out
    // at least one view since the registry opened).
    assert!(stamp > 0, "the registry handed out views before the check");
}

/// RC/RR keep the no-op path: the same interleaving with both txns RR,
/// then both RC, commits both (write skew is allowed below SER), and the
/// counting hook sees zero `on_read_edges` calls — while the SERIALIZABLE
/// run of the same interleaving makes at least one.
/// Mutant killed: M3 (the SER gate dropped: the hook called for every
/// isolation level, so the RC/RR counts are non-zero).
#[test]
fn fk_rc_rr_unchanged() {
    // Both RR, snapshots taken at begin and held to commit.
    {
        let rig = lock_rig();
        let hook = CountingSsi::wrap(&rig);
        rig.preload(P0, b"k1");
        let a = rig.core.begin(Isolation::RepeatableRead);
        let ga = rig.core.registry.take_snapshot();
        let b = rig.core.begin(Isolation::RepeatableRead);
        let gb = rig.core.registry.take_snapshot();
        assert_eq!(plain_read(&rig, &a, ga.ts(), C0), None);
        plain_update(&rig, &a, ga.ts(), P0, b"k1v2");
        plain_insert(&rig, &b, gb.ts(), C0, b"k1");
        fk_at(&rig, &b, gb.ts(), P0).expect("RR FK check");
        commit_plain(&rig, a).expect("A commits under RR");
        commit_plain(&rig, b).expect("B commits under RR");
        drop((ga, gb));
        assert_eq!(hook.on_read_edges_calls(), 0, "RR never calls the hook");
    }

    // Both RC, a fresh visible_ts as the FK snapshot.
    {
        let rig = lock_rig();
        let hook = CountingSsi::wrap(&rig);
        rig.preload(P0, b"k1");
        let a = rig.core.begin(Isolation::ReadCommitted);
        let b = rig.core.begin(Isolation::ReadCommitted);
        assert_eq!(plain_read(&rig, &a, rig.core.visible_ts(), C0), None);
        plain_update(&rig, &a, rig.core.visible_ts(), P0, b"k1v2");
        plain_insert(&rig, &b, rig.core.visible_ts(), C0, b"k1");
        fk_at(&rig, &b, rig.core.visible_ts(), P0).expect("RC FK check");
        commit_plain(&rig, a).expect("A commits under RC");
        commit_plain(&rig, b).expect("B commits under RC");
        assert_eq!(hook.on_read_edges_calls(), 0, "RC never calls the hook");
    }

    // The SERIALIZABLE gate itself: the pending-writer interleaving
    // through the same wrapper makes at least one call (and still aborts
    // exactly one txn).
    {
        let rig = lock_rig();
        let hook = CountingSsi::wrap(&rig);
        rig.preload(P0, b"k1");
        let a = rig.ser(false);
        let b = rig.ser(false);
        assert_eq!(rig.read(&a, C0), None);
        rig.update(&a, P0, b"k1v2");
        rig.insert(&b, C0, b"k1");
        fk(&rig, &b, P0).expect("SER FK check");
        assert!(hook.on_read_edges_calls() >= 1, "SER must call the hook");
        rig.commit(a).expect("A commits");
        assert_eq!(rig.commit(b), Err(TxnError::SerializationFailure));
    }
}
