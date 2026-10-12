//! C-R0bb: the PLAN C-R0b gate "Latency under fsync load", run on std
//! threads over a modelled slow fsync ([`SlowSyncKv`], D = 100 ms).
//! Contract rule R3 (`rt.rs`): no lock that another session's N- or
//! KvIo-class operation can need is held across an fsync.
//!
//! # What each probe class proves
//!
//! - **P1 read** (`begin` → `take_snapshot` → `open_view` → `read_key` →
//!   `abort`): the registry mutex, the status table and the read path
//!   never wait behind someone else's fsync. This is R3's core claim —
//!   the registry is the one lock every fast operation needs.
//! - **P2 write_abort** (`begin` → `insert_key` → `abort`): intent
//!   placement and abort are `Durability::No` writes (KvIo); they never
//!   couple to a non-commit sync. They also exercise the backend model's
//!   rule R4 (the write path is not held across the fsync) — a `No`
//!   write that queued behind a stalled sync would blow the bound.
//! - **P3 seq_cached** (`seq_next(1)`, recorded only when no reservation
//!   happened during the call): the C-R0bb fix itself. A cached nextval
//!   takes the module mutex for a lookup and id 1's slot; before the fix
//!   every id sat behind the one module mutex held across other ids'
//!   synced writes, and this class measured 803 ms p99 under load.
//! - **P4 commit_off** (`insert_key` → `commit(SyncCommit::Off)`): an
//!   off commit's wait for an in-flight fsync of an *earlier on* commit.
//!
//! # Why P4 has a spec bound and not a fast bound (notes, D9)
//!
//! `visible_ts` is a contiguous prefix (§3 step 4): the commit thread
//! acks the off prefix of a group **before** its one fsync, and every
//! request after the first `on` request of the group after it. An off
//! commit that lands behind an `on` request therefore legitimately waits
//! for that group's single fsync — the wait is the durability contract,
//! not a lock-coupling bug. Its bound is fsync-scale (`3·D`), and it
//! guards against **more than one fsync per group**: two full stalls
//! (one inherited in flight, one own-group) already fit inside `3·D`.
//! In phase A (no sync commits anywhere) the same class must meet the
//! fast `D/4` bound: off-only groups never fsync at all.
//!
//! # Scope
//!
//! The load here is std threads over the modelled backend; the re-run on
//! a real backend behind the async facade belongs to the facade card.
//! The C-B1 framework joins the L-load once it has merged.

mod r0b_support;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Duration, Instant};

use nucleus_txn::boot::Core;
use nucleus_txn::commit::{spawn_commit_thread, SyncCommit};
use nucleus_txn::read::{read_key, NoSsi};
use nucleus_txn::txn::Isolation;
use nucleus_txn::visibility::ReadCtx;
use nucleus_txn::write::{StmtCtx, UniqueRule};
use nucleus_txn::TxnError;

use r0b_support::{ok, Lat, SlowSyncKv};

/// The modelled fsync cost, D.
const D: Duration = Duration::from_millis(100);

/// One phase's load window, W.
const W: Duration = Duration::from_secs(2);

/// How long the main thread waits for one thread's report: a deadlock
/// fails the test instead of hanging it.
const COLLECT: Duration = Duration::from_secs(30);

/// The pause between probe ops.
const PROBE_SLEEP: Duration = Duration::from_millis(1);

/// The fast bound: a quarter of one fsync.
const FAST: Duration = Duration::from_millis(25); // D/4

/// The spec bound for P4 under sync-commit load: three fsyncs' worth.
const SPEC: Duration = Duration::from_millis(300); // 3·D

/// Samples each probe class needs per phase.
const MIN_PROBE_SAMPLES: usize = 200;

/// L2 commit samples needed (the anti-vacuity half of the gate).
const MIN_L2_SAMPLES: usize = 5;

/// The stalls the armed window must accumulate (anti-vacuity).
const MIN_STALL_GROWTH: u64 = 10;

/// What one thread sends the coordinator when its window ends. `err` is
/// `None` unless an op failed — the coordinator turns it into a failure
/// with the message, so a probe never dies in a detached thread where its
/// panic would only surface as a missing report.
struct Report {
    name: &'static str,
    lat: Lat,
    err: Option<String>,
}

/// A fresh-key insert in its own statement context (the `insert_key` /
/// commit idiom of `tests/write_path.rs`'s `Rig`).
fn insert_fresh(
    core: &Core<SlowSyncKv>,
    txn: &nucleus_txn::txn::Txn,
    key: &[u8],
) -> Result<(), TxnError> {
    let seq = txn.next_seq()?;
    let ctx = StmtCtx::new(core.visible_ts(), seq, seq);
    core.insert_key(
        txn,
        key,
        None,
        b"v".to_vec(),
        ctx,
        UniqueRule::Unique { same_row: None },
    )
}

// ---- the load threads -------------------------------------------------------

/// L1: `seq_next` on a fresh id each iteration — its boot is a read plus
/// two synced writes (creation + reservation), the heaviest seq load.
fn load_seq_fresh(
    tx: mpsc::Sender<Report>,
    core: Arc<Core<SlowSyncKv>>,
    stop: Arc<AtomicBool>,
    base: u64,
) {
    let mut err = None;
    let mut id = base;
    while !stop.load(Ordering::SeqCst) {
        if let Err(e) = core.seq_next(id) {
            err = Some(format!("L1 seq_next({id}) failed: {e:?}"));
            break;
        }
        id += 1;
    }
    let _ = tx.send(Report {
        name: "L1",
        lat: Lat::new(),
        err,
    });
}

/// L2: insert a fresh key, then commit `SyncCommit::On` — the on-commit
/// whose group fsync the window runs on. Only the commit call is timed;
/// its latency is the anti-vacuity oracle (each must cost at least one
/// full stall, D).
fn load_commit_on(
    tx: mpsc::Sender<Report>,
    core: Arc<Core<SlowSyncKv>>,
    stop: Arc<AtomicBool>,
    key_base: String,
) {
    let mut lat = Lat::new();
    let mut err = None;
    let mut n = 0u64;
    while !stop.load(Ordering::SeqCst) {
        let txn = core.begin(Isolation::ReadCommitted);
        let key = format!("{key_base}/{n}").into_bytes();
        if let Err(e) = insert_fresh(&core, &txn, &key) {
            err = Some(format!("L2 insert failed: {e:?}"));
            break;
        }
        let t0 = Instant::now();
        let r = core.commit(txn, SyncCommit::On);
        let dt = t0.elapsed();
        match r {
            Ok(_) => lat.record(dt),
            Err(e) => {
                err = Some(format!("L2 commit failed: {e:?}"));
                break;
            }
        }
        n += 1;
    }
    let _ = tx.send(Report { name: "L2", lat, err });
}

/// L3: a bare `sync_wal` loop — the purest fsync load, no seq or commit
/// state near it.
fn load_sync_wal(tx: mpsc::Sender<Report>, core: Arc<Core<SlowSyncKv>>, stop: Arc<AtomicBool>) {
    let mut err = None;
    while !stop.load(Ordering::SeqCst) {
        if let Err(e) = core.sync_wal() {
            err = Some(format!("L3 sync_wal failed: {e:?}"));
            break;
        }
    }
    let _ = tx.send(Report {
        name: "L3",
        lat: Lat::new(),
        err,
    });
}

// ---- the probe threads ------------------------------------------------------

/// P1 read: `begin`, `take_snapshot`, `open_view`, `read_key("/t/pre")`
/// (must be `Some(b"v")`), `abort` — the whole op is one sample.
fn probe_read(tx: mpsc::Sender<Report>, core: Arc<Core<SlowSyncKv>>, stop: Arc<AtomicBool>) {
    let mut lat = Lat::new();
    let mut err = None;
    while !stop.load(Ordering::SeqCst) {
        thread::sleep(PROBE_SLEEP);
        let t0 = Instant::now();
        let txn = core.begin(Isolation::ReadCommitted);
        let seq = txn.next_seq().expect("next_seq");
        let read = {
            let snap = core.registry.take_snapshot();
            let view = core.open_view();
            read_key(
                &core,
                &view,
                b"t/pre",
                &ReadCtx {
                    txn: txn.id,
                    snapshot: snap.ts(),
                    stmt_seq: seq,
                },
                &mut NoSsi,
            )
        };
        let aborted = core.abort(txn);
        let dt = t0.elapsed();
        match (read, aborted) {
            (Ok(Some(v)), Ok(())) if v == b"v" => lat.record(dt),
            (Ok(other), Ok(())) => {
                err = Some(format!("P1 read of /t/pre returned {other:?}, wanted Some(b\"v\")"));
                break;
            }
            (Err(e), _) => {
                err = Some(format!("P1 read failed: {e:?}"));
                break;
            }
            (_, Err(e)) => {
                err = Some(format!("P1 abort failed: {e:?}"));
                break;
            }
        }
    }
    let _ = tx.send(Report { name: "P1", lat, err });
}

/// P2 write_abort: `begin`, `insert_key` on a fresh key, `abort` — the
/// KvIo write path with no commit and no sync.
fn probe_write_abort(
    tx: mpsc::Sender<Report>,
    core: Arc<Core<SlowSyncKv>>,
    stop: Arc<AtomicBool>,
    key_base: String,
) {
    let mut lat = Lat::new();
    let mut err = None;
    let mut n = 0u64;
    while !stop.load(Ordering::SeqCst) {
        thread::sleep(PROBE_SLEEP);
        let t0 = Instant::now();
        let txn = core.begin(Isolation::ReadCommitted);
        let key = format!("{key_base}/{n}").into_bytes();
        let r = insert_fresh(&core, &txn, &key).and_then(|()| core.abort(txn));
        let dt = t0.elapsed();
        match r {
            Ok(()) => lat.record(dt),
            Err(e) => {
                err = Some(format!("P2 insert/abort failed: {e:?}"));
                break;
            }
        }
        n += 1;
    }
    let _ = tx.send(Report { name: "P2", lat, err });
}

/// P3 seq_cached: `seq_next(1)`. The sample is recorded **only when no
/// reservation happened during the call** (`seq_reservations_for_tests`
/// unchanged across it): once per block the call legitimately pays its
/// own synced write, and that in-family wait is not what this class
/// measures.
fn probe_seq_cached(tx: mpsc::Sender<Report>, core: Arc<Core<SlowSyncKv>>, stop: Arc<AtomicBool>) {
    let mut lat = Lat::new();
    let mut err = None;
    while !stop.load(Ordering::SeqCst) {
        thread::sleep(PROBE_SLEEP);
        let before = core.seq_reservations_for_tests(1);
        let t0 = Instant::now();
        let v = core.seq_next(1);
        let dt = t0.elapsed();
        let after = core.seq_reservations_for_tests(1);
        match v {
            Ok(_) => {}
            Err(e) => {
                err = Some(format!("P3 seq_next(1) failed: {e:?}"));
                break;
            }
        }
        if before == after {
            lat.record(dt);
        }
    }
    let _ = tx.send(Report { name: "P3", lat, err });
}

/// P4 commit_off: `insert_key` on a fresh key, then `commit(Off)`. The
/// spec-bound class — see the module docs.
fn probe_commit_off(
    tx: mpsc::Sender<Report>,
    core: Arc<Core<SlowSyncKv>>,
    stop: Arc<AtomicBool>,
    key_base: String,
) {
    let mut lat = Lat::new();
    let mut err = None;
    let mut n = 0u64;
    while !stop.load(Ordering::SeqCst) {
        thread::sleep(PROBE_SLEEP);
        let t0 = Instant::now();
        let txn = core.begin(Isolation::ReadCommitted);
        let key = format!("{key_base}/{n}").into_bytes();
        let r = insert_fresh(&core, &txn, &key).and_then(|()| core.commit(txn, SyncCommit::Off));
        let dt = t0.elapsed();
        match r {
            Ok(_) => lat.record(dt),
            Err(e) => {
                err = Some(format!("P4 insert/commit(Off) failed: {e:?}"));
                break;
            }
        }
        n += 1;
    }
    let _ = tx.send(Report { name: "P4", lat, err });
}

// ---- the phases -------------------------------------------------------------

/// Runs one window: spawns the phase's loads and all four probes, sleeps
/// W, stops them, and collects every report (bounded by [`COLLECT`], so
/// a deadlock fails instead of hanging). Returns the reports in spawn
/// order and how many stalls the window accumulated.
fn run_phase(
    tag: &'static str,
    core: &Arc<Core<SlowSyncKv>>,
    kv: &SlowSyncKv,
    with_l2: bool,
) -> (Vec<Report>, u64) {
    let stop = Arc::new(AtomicBool::new(false));
    let (tx, rx) = mpsc::channel::<Report>();

    // Load: L1 (fresh ids, phase-distinct base) and L3 always; L2 in
    // phase B only.
    {
        let (tx, core, stop) = (tx.clone(), Arc::clone(core), Arc::clone(&stop));
        thread::spawn(move || load_seq_fresh(tx, core, stop, 1_000_000 * phase_of(tag)));
    }
    if with_l2 {
        let (tx, core, stop) = (tx.clone(), Arc::clone(core), Arc::clone(&stop));
        let key_base = format!("t/g/{tag}/l2");
        thread::spawn(move || load_commit_on(tx, core, stop, key_base));
    }
    {
        let (tx, core, stop) = (tx.clone(), Arc::clone(core), Arc::clone(&stop));
        thread::spawn(move || load_sync_wal(tx, core, stop));
    }

    // Probes: one thread per class, 1 ms between ops.
    {
        let (tx, core, stop) = (tx.clone(), Arc::clone(core), Arc::clone(&stop));
        thread::spawn(move || probe_read(tx, core, stop));
    }
    {
        let (tx, core, stop) = (tx.clone(), Arc::clone(core), Arc::clone(&stop));
        let key_base = format!("t/g/{tag}/p2");
        thread::spawn(move || probe_write_abort(tx, core, stop, key_base));
    }
    {
        let (tx, core, stop) = (tx.clone(), Arc::clone(core), Arc::clone(&stop));
        thread::spawn(move || probe_seq_cached(tx, core, stop));
    }
    {
        let (tx, core, stop) = (tx.clone(), Arc::clone(core), Arc::clone(&stop));
        let key_base = format!("t/g/{tag}/p4");
        thread::spawn(move || probe_commit_off(tx, core, stop, key_base));
    }

    let expected = 6 + usize::from(with_l2);
    let stalls0 = kv.stalls();
    thread::sleep(W);
    stop.store(true, Ordering::SeqCst);
    let mut reports = Vec::with_capacity(expected);
    for i in 0..expected {
        match rx.recv_timeout(COLLECT) {
            Ok(r) => reports.push(r),
            Err(_) => panic!(
                "phase {tag}: {} of {expected} threads reported within {COLLECT:?} — \
                  a thread is deadlocked",
                report_names(&reports, i)
            ),
        }
    }
    (reports, kv.stalls() - stalls0)
}

/// `1` for phase "A", `2` for phase "B" — the L1 id base multiplier, so
/// the two phases never share a sequence id.
fn phase_of(tag: &str) -> u64 {
    match tag {
        "B" => 2,
        _ => 1,
    }
}

/// The names of the first `n` reports, for the deadlock message.
fn report_names(reports: &[Report], n: usize) -> String {
    let names: Vec<&str> = reports.iter().take(n).map(|r| r.name).collect();
    if names.is_empty() {
        "none".into()
    } else {
        names.join(", ")
    }
}

/// Finds one class's latency bag among the reports.
fn lat_of<'a>(reports: &'a [Report], name: &str) -> &'a Lat {
    &reports
        .iter()
        .find(|r| r.name == name)
        .unwrap_or_else(|| panic!("no {name} report arrived"))
        .lat
}

/// Asserts every thread left its error slot empty.
fn assert_no_errors(tag: &str, reports: &[Report]) {    for r in reports {
        if let Some(e) = &r.err {
            panic!("phase {tag}: {} failed mid-window: {e}", r.name);
        }
    }
}

/// Asserts the sample count and p99 bound of one class, after printing
/// its evidence line.
fn assert_class(tag: &str, name: &str, lat: &Lat, bound: Duration) {
    assert_samples(tag, name, lat, MIN_PROBE_SAMPLES);
    assert_p99(tag, name, lat, bound);
}

/// Asserts only the sample count of one class.
fn assert_samples(tag: &str, name: &str, lat: &Lat, min: usize) {
    let line = lat.summary(&format!("{name} phase {tag}"));
    eprintln!("{line}");
    assert!(
        lat.len() >= min,
        "phase {tag} {name}: {} samples, need {min} — the probe starved ({line})",
        lat.len()
    );
}

/// Asserts only the p99 bound of one class (its evidence line is printed
/// by whoever checked the count).
fn assert_p99(tag: &str, name: &str, lat: &Lat, bound: Duration) {
    assert!(
        lat.p99() <= bound,
        "phase {tag} {name}: p99 {:?} exceeds the {:?} bound ({}) — \
          a lock this class needs is held across an fsync (R3)",
        lat.p99(),
        bound,
        lat.summary(&format!("{name} phase {tag}"))
    );
}

#[test]
fn latency_under_fsync_load() {
    let kv = SlowSyncKv::new(D);
    let core = Arc::new(ok(Core::open(kv.clone())));
    let commit_thread = ok(spawn_commit_thread(Arc::clone(&core)));

    // Setup, all while disarmed: the preload row, then id 1's boot (its
    // two synced writes answer at once). Only then arm the stall.
    let pre = core.begin(Isolation::ReadCommitted);
    ok(insert_fresh(&core, &pre, b"t/pre"));
    ok(core.commit(pre, SyncCommit::On));
    assert_eq!(ok(core.seq_next(1)), 1, "boot id 1 and hand out its first value");
    kv.arm();

    // Phase A: L1 + L3 only — fsync load with no sync commits anywhere.
    // Every class, P4 included, meets the fast bound: an off-only group
    // never fsyncs (§3 step 4).
    let (reports_a, growth_a) = run_phase("A", &core, &kv, false);
    assert_no_errors("A", &reports_a);
    for name in ["P1", "P2", "P3", "P4"] {
        assert_class("A", name, lat_of(&reports_a, name), FAST);
    }
    assert!(
        growth_a >= MIN_STALL_GROWTH,
        "phase A: only {growth_a} stalls in the window, need {MIN_STALL_GROWTH} — \
          the fsync load never ran (vacuous gate)"
    );

    // Phase B: L2 joins — on commits whose group fsync is in flight while
    // the probes run. P1–P3 keep the fast bound and the sample floor;
    // P4 moves to the spec bound (module docs, D9) with no sample floor
    // beyond one — an off commit ordered behind an on request pays the
    // group's fsync every time, so the class measures in tens of samples
    // per window, not hundreds; L2's own commits each cost at least D.
    let (reports_b, growth_b) = run_phase("B", &core, &kv, true);
    assert_no_errors("B", &reports_b);
    for name in ["P1", "P2", "P3"] {
        assert_class("B", name, lat_of(&reports_b, name), FAST);
    }
    assert_samples("B", "P4", lat_of(&reports_b, "P4"), 1);
    assert_p99("B", "P4", lat_of(&reports_b, "P4"), SPEC);
    let l2 = lat_of(&reports_b, "L2");
    let l2_line = l2.summary("L2 commit(On) phase B");
    eprintln!("{l2_line}");
    assert!(
        l2.len() >= MIN_L2_SAMPLES,
        "phase B L2: {} commit samples, need {MIN_L2_SAMPLES} ({l2_line})",
        l2.len()
    );
    assert!(
        l2.min() >= D,
        "phase B L2: cheapest commit {:?} is below D {D:?} ({l2_line}) — \
          the on commits never paid a stall (vacuous gate)",
        l2.min()
    );
    assert!(
        growth_b >= MIN_STALL_GROWTH,
        "phase B: only {growth_b} stalls in the window, need {MIN_STALL_GROWTH} — \
          the fsync load never ran (vacuous gate)"
    );

    ok(commit_thread.shutdown());
}
