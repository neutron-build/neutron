//! C-T2c stress: 8 threads upsert 4 arbiter keys with `DO UPDATE SET v =
//! v + 1` (RC), one txn per upsert, each proposing a fresh row; one thread
//! inserts and deletes children of 2 parents (FK child check), another
//! deletes and re-inserts the parents (FK parent check, RESTRICT or NO
//! ACTION); a checker thread verifies I-FK at registered snapshots while
//! they run. Afterwards: each arbiter key has exactly one live row whose
//! value is (committed upserts on the key) - 1 — no lost upsert, never two
//! rows for one key — no committed child without its parent, no intent
//! remains once the jobs drain, and every surviving status entry has count
//! 0 (I-COUNT).

mod c_t2c_support;

use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use c_t2c_support::{count_intents, Fixed, TestRowLocks};
use nucleus_kv::MemKv;
use nucleus_txn::boot::Core;
use nucleus_txn::commit::{spawn_commit_thread, SyncCommit};
use nucleus_txn::read::{read_key, scan, NoSsi};
use nucleus_txn::resolver::{spawn_background, Resolver};
use nucleus_txn::txn::{CancelHandle, Isolation, Txn};
use nucleus_txn::visibility::ReadCtx;
use nucleus_txn::write::{
    FkParentMode, IndexEntry, OnConflictAction, OnConflictResult, ProposedRow, RowOp, StmtCtx,
    UniqueRule,
};
use nucleus_txn::{Ts, TxnError};

const UPSERT_THREADS: usize = 8;
const UPSERTS_PER_THREAD: usize = 250;
const KEYS: usize = 4;
const PARENTS: usize = 2;
const FK_TXNS: usize = 300;
const WATCHDOG_AFTER: Duration = Duration::from_secs(15);

type Cancels = Arc<Mutex<Vec<CancelHandle>>>;

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 33
    }
}

fn row_key(pk: &[u8]) -> Vec<u8> {
    let mut k = b"/t/r/".to_vec();
    k.extend_from_slice(pk);
    k
}

fn arb_key(k: usize) -> Vec<u8> {
    format!("/u/a/{k}").into_bytes()
}

fn parent_key(p: usize) -> Vec<u8> {
    format!("/t/p/{p}").into_bytes()
}

fn child_lo(p: usize) -> Vec<u8> {
    format!("/t/c/{p}/").into_bytes()
}

fn child_hi(p: usize) -> Vec<u8> {
    // '/' + 1 == '0': every `/t/c/{p}/...` sorts below `/t/c/{p}0`.
    format!("/t/c/{p}0").into_bytes()
}

/// Row value: the arbiter key index, then the counter.
fn row_value(k: usize, v: u64) -> Vec<u8> {
    let mut b = vec![k as u8];
    b.extend_from_slice(&v.to_be_bytes());
    b
}

fn parse_row(b: &[u8]) -> (usize, u64) {
    let v = u64::from_be_bytes(b[1..9].try_into().expect("row value"));
    (b[0] as usize, v)
}

fn begin(core: &Core<MemKv>, cancels: &Cancels) -> Txn {
    let t = core.begin(Isolation::ReadCommitted);
    cancels.lock().expect("cancels").push(t.cancel_handle());
    t
}

/// One upsert txn. Returns the result (committed).
fn upsert_txn(
    core: &Core<MemKv>,
    cancels: &Cancels,
    k: usize,
    pk: &str,
) -> Result<OnConflictResult, TxnError> {
    let txn = begin(core, cancels);
    let snap = core.registry.take_snapshot();
    let seq0 = txn.next_seq()?;
    let row = ProposedRow {
        t_key: row_key(pk.as_bytes()),
        value: row_value(k, 0),
        entries: vec![IndexEntry {
            key: arb_key(k),
            value: pk.as_bytes().to_vec(),
            arbiter: true,
        }],
        pk_arbiter: false,
    };
    let mut f = |v: &[u8]| {
        let (kk, n) = parse_row(v);
        Some((row_value(kk, n + 1), false))
    };
    let r = core.insert_on_conflict(
        &txn,
        StmtCtx::new(snap.ts(), seq0, seq0),
        row,
        &row_key,
        &mut |_| {},
        OnConflictAction::DoUpdate(&mut f),
    );
    drop(snap);
    match r {
        Ok(out) => {
            core.commit(txn, SyncCommit::Off)?;
            Ok(out.result)
        }
        Err(e) => {
            core.abort(txn)?;
            Err(e)
        }
    }
}

/// Runs `body` in a fresh RC txn with its own statement snapshot; commits on
/// `Ok(true)`, aborts on `Ok(false)` or a 23503, propagates anything else.
fn fk_txn(
    core: &Core<MemKv>,
    cancels: &Cancels,
    body: impl FnOnce(&Txn, Ts, u32) -> Result<(), TxnError>,
) -> Result<bool, TxnError> {
    let txn = begin(core, cancels);
    let snap = core.registry.take_snapshot();
    let seq0 = txn.next_seq()?;
    let r = body(&txn, snap.ts(), seq0);
    drop(snap);
    match r {
        Ok(()) => {
            core.commit(txn, SyncCommit::Off)?;
            Ok(true)
        }
        Err(TxnError::ForeignKeyViolation) => {
            core.abort(txn)?;
            Ok(false)
        }
        Err(e) => {
            core.abort(txn)?;
            Err(e)
        }
    }
}

fn child_thread(core: Arc<Core<MemKv>>, cancels: Cancels) -> Result<usize, TxnError> {
    let mut rng = Rng(0xc0ffee);
    let mut live: Vec<(usize, u64)> = Vec::new();
    let mut next_id = 0u64;
    let mut inserted = 0;
    for _ in 0..FK_TXNS {
        if !live.is_empty() && rng.next().is_multiple_of(3) {
            let i = (rng.next() as usize) % live.len();
            let (p, id) = live[i];
            let key = format!("/t/c/{p}/{id}").into_bytes();
            let done = fk_txn(&core, &cancels, |t, s, seq0| {
                core.row_op(
                    t,
                    &key,
                    None,
                    RowOp::Delete,
                    StmtCtx::new(s, seq0, seq0),
                    &mut Fixed(RowOp::Delete),
                )?;
                Ok(())
            })?;
            if done {
                live.swap_remove(i);
            }
        } else {
            let p = (rng.next() as usize) % PARENTS;
            next_id += 1;
            let id = next_id;
            let key = format!("/t/c/{p}/{id}").into_bytes();
            let pk = parent_key(p);
            let done = fk_txn(&core, &cancels, |t, s, seq0| {
                core.insert_key(
                    t,
                    &key,
                    None,
                    vec![p as u8],
                    StmtCtx::new(s, seq0, seq0),
                    UniqueRule::Unique { same_row: None },
                )?;
                // End-of-statement FK check: an internal command at a fresh
                // seq, reading at a fresh snapshot (RC).
                let fresh = core.registry.take_snapshot();
                let ctx = StmtCtx::new(fresh.ts(), seq0, t.next_seq()?).internal();
                core.fk_check_child(t, &ctx, &pk, &|_| true)
            })?;
            if done {
                live.push((p, id));
                inserted += 1;
            }
        }
    }
    Ok(inserted)
}

fn parent_thread(core: Arc<Core<MemKv>>, cancels: Cancels) -> Result<usize, TxnError> {
    let mut rng = Rng(0xbadcafe);
    let mut live = [true; PARENTS];
    let mut deletes = 0;
    for _ in 0..FK_TXNS {
        let p = (rng.next() as usize) % PARENTS;
        let pk = parent_key(p);
        if live[p] {
            let restrict = rng.next().is_multiple_of(2);
            let done = fk_txn(&core, &cancels, |t, s, seq0| {
                core.row_op(
                    t,
                    &pk,
                    None,
                    RowOp::Delete,
                    StmtCtx::new(s, seq0, seq0),
                    &mut Fixed(RowOp::Delete),
                )?;
                let ctx = StmtCtx::new(s, seq0, t.next_seq()?).internal();
                let mut hi = pk.clone();
                hi.push(0xff);
                let mode = if restrict {
                    FkParentMode::Restrict
                } else {
                    FkParentMode::NoAction {
                        parent_lo: &pk,
                        parent_hi: &hi,
                    }
                };
                core.fk_check_parent(t, &ctx, &child_lo(p), &child_hi(p), mode)
            })?;
            if done {
                live[p] = false;
                deletes += 1;
            }
        } else {
            let done = fk_txn(&core, &cancels, |t, s, seq0| {
                core.insert_key(
                    t,
                    &pk,
                    None,
                    vec![p as u8],
                    StmtCtx::new(s, seq0, seq0),
                    UniqueRule::Unique { same_row: None },
                )
            })?;
            assert!(done, "re-inserting a parent never violates an FK");
            live[p] = true;
        }
    }
    Ok(deletes)
}

/// Live rows of `[lo, hi)` at a registered snapshot of the latest state.
fn live_rows(core: &Core<MemKv>, s: Ts, lo: &[u8], hi: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
    let view = core.open_view();
    let ctx = ReadCtx {
        txn: nucleus_txn::TxnId { epoch: 0, n: 0 },
        snapshot: s,
        stmt_seq: 0,
    };
    let mut obs = NoSsi;
    scan(
        core,
        &view,
        (Bound::Included(lo), Bound::Excluded(hi)),
        &ctx,
        &mut obs,
    )
    .collect::<Result<Vec<_>, TxnError>>()
    .expect("scan")
}

/// I-FK at one registered snapshot: every live child's parent is live.
fn check_ifk(core: &Core<MemKv>) {
    let snap = core.registry.take_snapshot();
    let s = snap.ts();
    for p in 0..PARENTS {
        let children = live_rows(core, s, &child_lo(p), &child_hi(p));
        if children.is_empty() {
            continue;
        }
        let view = core.open_view();
        let parent = read_key(
            core,
            &view,
            &parent_key(p),
            &ReadCtx {
                txn: nucleus_txn::TxnId { epoch: 0, n: 0 },
                snapshot: s,
                stmt_seq: 0,
            },
            &mut NoSsi,
        )
        .expect("read parent");
        assert!(
            parent.is_some(),
            "I-FK at S={s:?}: {} live children of parent {p}, which is not live",
            children.len()
        );
    }
}

#[test]
fn stress_upserts_and_fk() {
    let core = Arc::new(Core::open(MemKv::new()).expect("core"));
    core.set_row_locks(TestRowLocks::new());
    let commit_handle = spawn_commit_thread(Arc::clone(&core)).expect("commit thread");
    let bg = spawn_background(Arc::clone(&core)).expect("resolver");
    let cancels: Cancels = Arc::new(Mutex::new(Vec::new()));
    let start = Instant::now();

    // Parents start live.
    for p in 0..PARENTS {
        let t = core.begin(Isolation::ReadCommitted);
        let seq = t.next_seq().expect("seq");
        core.insert_key(
            &t,
            &parent_key(p),
            None,
            vec![p as u8],
            StmtCtx::new(core.visible_ts(), seq, seq),
            UniqueRule::Unique { same_row: None },
        )
        .expect("parent");
        core.commit(t, SyncCommit::On).expect("commit parent");
    }

    let (wd_tx, wd_rx) = std::sync::mpsc::channel::<()>();
    let watchdog = {
        let cancels = Arc::clone(&cancels);
        std::thread::spawn(move || {
            if wd_rx.recv_timeout(WATCHDOG_AFTER).is_err() {
                for c in cancels.lock().expect("cancels").iter() {
                    c.cancel();
                }
            }
        })
    };

    let committed: Arc<Mutex<HashMap<usize, u64>>> = Arc::new(Mutex::new(HashMap::new()));
    let mut upserters = Vec::new();
    for th in 0..UPSERT_THREADS {
        let core = Arc::clone(&core);
        let cancels = Arc::clone(&cancels);
        let committed = Arc::clone(&committed);
        upserters.push(std::thread::spawn(move || {
            let mut rng = Rng(0x9e3779b97f4a7c15 ^ (th as u64 + 7));
            for i in 0..UPSERTS_PER_THREAD {
                let k = (rng.next() as usize) % KEYS;
                let pk = format!("{th}-{i}");
                match upsert_txn(&core, &cancels, k, &pk) {
                    Ok(OnConflictResult::Inserted | OnConflictResult::Updated) => {
                        *committed.lock().expect("committed").entry(k).or_default() += 1;
                    }
                    Ok(other) => panic!("unexpected upsert result {other:?}"),
                    Err(TxnError::QueryCanceled) => panic!("upserter cancelled: watchdog fired"),
                    Err(e) => panic!("unexpected upsert error {e:?}"),
                }
            }
        }));
    }
    let children = {
        let core = Arc::clone(&core);
        let cancels = Arc::clone(&cancels);
        std::thread::spawn(move || child_thread(core, cancels))
    };
    let parents = {
        let core = Arc::clone(&core);
        let cancels = Arc::clone(&cancels);
        std::thread::spawn(move || parent_thread(core, cancels))
    };
    let stop_checker = Arc::new(AtomicBool::new(false));
    let checker = {
        let core = Arc::clone(&core);
        let stop = Arc::clone(&stop_checker);
        std::thread::spawn(move || {
            let mut checks = 0usize;
            while !stop.load(Ordering::SeqCst) {
                check_ifk(&core);
                checks += 1;
                std::thread::sleep(Duration::from_millis(2));
            }
            checks
        })
    };

    for h in upserters {
        if let Err(e) = h.join() {
            std::panic::resume_unwind(e);
        }
    }
    let inserted = match children.join() {
        Ok(r) => r.expect("child thread"),
        Err(e) => std::panic::resume_unwind(e),
    };
    let deletes = match parents.join() {
        Ok(r) => r.expect("parent thread"),
        Err(e) => std::panic::resume_unwind(e),
    };
    stop_checker.store(true, Ordering::SeqCst);
    let checks = match checker.join() {
        Ok(n) => n,
        Err(e) => std::panic::resume_unwind(e),
    };
    wd_tx.send(()).expect("watchdog alive");
    watchdog.join().expect("watchdog");
    bg.stop().expect("resolver stop");
    commit_handle.shutdown().expect("shutdown");
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_secs(20),
        "bounded run: {elapsed:?}"
    );
    assert!(inserted > 0 && deletes > 0, "the FK workload made progress");
    assert!(checks > 0);

    // Drain the jobs: no intent remains; I-COUNT for every surviving entry.
    for _ in 0..500 {
        let n = Resolver::run_once(&core).expect("resolve");
        if n == 0 && count_intents(&core) == 0 {
            break;
        }
    }
    assert_eq!(
        count_intents(&core),
        0,
        "every intent resolved or discarded"
    );
    for id in core.status.truncation_candidates() {
        if let Some(e) = core.status.entry(id) {
            assert_eq!(e.intent_count, 0, "I-COUNT for {id:?}");
        }
    }

    // The upsert oracle: per arbiter key exactly one live row, value =
    // committed upserts - 1, and the entry names it.
    let s = core.visible_ts();
    let rows = live_rows(&core, s, b"/t/r/", b"/t/r0");
    let mut by_key: BTreeMap<usize, Vec<(Vec<u8>, u64)>> = BTreeMap::new();
    for (key, value) in rows {
        let (k, v) = parse_row(&value);
        by_key.entry(k).or_default().push((key, v));
    }
    let committed = committed.lock().expect("committed").clone();
    let total: u64 = committed.values().sum();
    assert_eq!(total as usize, UPSERT_THREADS * UPSERTS_PER_THREAD);
    for k in 0..KEYS {
        let n = committed.get(&k).copied().unwrap_or(0);
        let rows = by_key.get(&k).cloned().unwrap_or_default();
        if n == 0 {
            assert!(rows.is_empty());
            continue;
        }
        assert_eq!(
            rows.len(),
            1,
            "key {k}: rows {rows:?} (two rows for one key)"
        );
        assert_eq!(rows[0].1, n - 1, "key {k}: lost upsert ({n} committed)");
        let entry = live_rows(&core, s, &arb_key(k), &[arb_key(k), vec![0xff]].concat());
        assert_eq!(entry.len(), 1);
        assert_eq!(row_key(&entry[0].1), rows[0].0, "the entry names the row");
    }
    check_ifk(&core);
}

const _: () = assert!(UPSERT_THREADS * UPSERTS_PER_THREAD + 2 * FK_TXNS >= 2000);
