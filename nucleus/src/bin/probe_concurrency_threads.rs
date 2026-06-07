//! Real OS-thread MVCC concurrency invariant checker.
//!
//! One `Executor` backed by the MVCC storage adapter is shared (Arc) across N
//! genuine OS worker threads. Each worker owns its own per-connection session
//! (`Executor::create_session`) and its own single-thread tokio runtime, and
//! drives `BEGIN … reads + UPDATE/INSERT … COMMIT|ROLLBACK` through the real
//! wire-protocol entry point `execute_with_session` (which scopes the
//! `CURRENT_SESSION` / `STORAGE_SESSION_ID` task-locals just like a live pgwire
//! connection). This produces true contention on the MVCC version chains, not a
//! single-threaded simulation.
//!
//! Four invariants are asserted under contention, over many rounds:
//!   (1) LOST UPDATE: N workers each do `read; v=v+1; commit` with
//!       retry-on-serialization-failure. Final counter MUST equal the number of
//!       successful commits (no update silently lost).
//!   (2) DIRTY READ: while a writer holds an uncommitted change, no other txn
//!       (snapshot/serializable) may observe that uncommitted value.
//!   (3) TORN READ / ATOMICITY: a writer flips two columns keeping a+b ==
//!       CONST. Concurrent readers must ALWAYS see a+b == CONST (never a
//!       partial/torn transaction).
//!   (4) WRITE-CONFLICT DETECTION: when two active txns both UPDATE the same
//!       row, the engine must reject one with a serialization error — it must
//!       not silently let both "succeed" and lose one.
//!
//! All invariants are derived from MVCC snapshot-isolation / serializable
//! semantics (Postgres "first-updater-wins" write-conflict model). Any
//! violation is reported with the parameters needed to reproduce; timing-
//! dependent findings are flagged as such. Exits non-zero on any violation.
//!
//! Build: `cargo build --release --features "server rusqlite" --bin probe_concurrency_threads`
//! (rusqlite is unused here but the workflow builds with that feature set.)
//! Run:   `cargo run   --release --features "server rusqlite" --bin probe_concurrency_threads -- --seed 1 --rounds 200`
#![cfg(feature = "server")]
#![allow(unused)]
#![allow(clippy::all)] // internal fuzz harness

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};

use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::storage::MvccStorageAdapter;
use nucleus::types::Value;

// ─── Deterministic PRNG (xorshift) ────────────────────────────────────────────
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: usize) -> usize { (self.next() % n as u64) as usize }
    fn pick<'a, T>(&mut self, xs: &'a [T]) -> &'a T { &xs[self.below(xs.len())] }
}

// ─── Per-worker runtime + session helpers ─────────────────────────────────────

/// Run `sql` in this worker's session. Returns Ok(results) or Err(message).
fn run(ex: &Executor, rt: &tokio::runtime::Runtime, sid: u64, sql: &str) -> Result<Vec<ExecResult>, String> {
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        rt.block_on(ex.execute_with_session(sid, sql))
    }));
    match res {
        Ok(Ok(r)) => Ok(r),
        Ok(Err(e)) => Err(format!("{e}")),
        Err(_) => Err("PANIC".to_string()),
    }
}

/// True if an error string is a (legitimate) MVCC serialization/conflict abort.
fn is_serialization_err(e: &str) -> bool {
    let l = e.to_lowercase();
    l.contains("could not serialize")
        || l.contains("serializ") // SerializationFailure / serialization
        || l.contains("write conflict")
        || l.contains("concurrent update")
}

/// Read the single integer cell of `SELECT <col> FROM counter WHERE id=<id>`.
fn read_int(ex: &Executor, rt: &tokio::runtime::Runtime, sid: u64, sql: &str) -> Result<Option<i64>, String> {
    let mut r = run(ex, rt, sid, sql)?;
    match r.pop() {
        Some(ExecResult::Select { rows, .. }) => {
            let v = rows.get(0).and_then(|row| row.get(0));
            match v {
                Some(Value::Int64(n)) => Ok(Some(*n)),
                Some(Value::Int32(n)) => Ok(Some(*n as i64)),
                Some(Value::Null) | None => Ok(None),
                Some(other) => Err(format!("non-int cell: {other:?}")),
            }
        }
        other => Err(format!("non-select result: {other:?}")),
    }
}

fn new_rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("worker runtime")
}

// ─── Fresh executor per round (in-memory MVCC) ────────────────────────────────
fn fresh_executor() -> Arc<Executor> {
    Arc::new(Executor::new(Arc::new(Catalog::new()), Arc::new(MvccStorageAdapter::new())))
}

/// DDL + one auto-committed setup statement, run on a throwaway session.
fn setup(ex: &Executor, stmts: &[&str]) -> Result<(), String> {
    let rt = new_rt();
    let sid = ex.create_session();
    for s in stmts {
        run(ex, &rt, sid, s)?;
    }
    ex.drop_session(sid);
    Ok(())
}

// ─── Violation reporting ──────────────────────────────────────────────────────
struct Report {
    divergences: usize,
    max: usize,
}
impl Report {
    fn fail(&mut self, title: &str, detail: String) {
        self.divergences += 1;
        if self.divergences <= self.max {
            println!("─── INVARIANT VIOLATION #{} : {title} ───", self.divergences);
            println!("{detail}\n");
        }
    }
}

// ─── Test 1: no lost updates ──────────────────────────────────────────────────
//
// N workers each: BEGIN; SELECT v; UPDATE v=v+1; COMMIT — retry on
// serialization failure until committed. Each worker performs exactly K
// successful increments. Final v MUST equal N*K. A lower value means an update
// was lost (read-modify-write race not detected); a higher value is impossible
// unless reads/writes are corrupted.
fn test_lost_update(rng: &mut Rng, report: &mut Report) {
    let iso = *rng.pick(&["SERIALIZABLE", "REPEATABLE READ"]);
    let workers = 2 + rng.below(5); // 2..=6
    let per_worker = 3 + rng.below(6); // 3..=8

    let ex = fresh_executor();
    if let Err(e) = setup(&ex, &[
        "CREATE TABLE counter (id INTEGER PRIMARY KEY, v INTEGER NOT NULL)",
        "INSERT INTO counter (id, v) VALUES (1, 0)",
    ]) {
        report.fail("lost-update setup failed", format!("  {e}"));
        return;
    }

    let commits = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(workers));
    let mut handles = Vec::new();
    for _ in 0..workers {
        let ex = ex.clone();
        let commits = commits.clone();
        let barrier = barrier.clone();
        let iso = iso.to_string();
        handles.push(std::thread::spawn(move || {
            let rt = new_rt();
            let sid = ex.create_session();
            barrier.wait();
            for _ in 0..per_worker {
                // retry until the increment commits
                loop {
                    if run(&ex, &rt, sid, &format!("BEGIN ISOLATION LEVEL {iso}")).is_err() {
                        let _ = run(&ex, &rt, sid, "ROLLBACK");
                        continue;
                    }
                    let cur = match read_int(&ex, &rt, sid, "SELECT v FROM counter WHERE id=1") {
                        Ok(Some(n)) => n,
                        _ => { let _ = run(&ex, &rt, sid, "ROLLBACK"); continue; }
                    };
                    if let Err(e) = run(&ex, &rt, sid, &format!("UPDATE counter SET v={} WHERE id=1", cur + 1)) {
                        let _ = run(&ex, &rt, sid, "ROLLBACK");
                        if is_serialization_err(&e) { continue; }
                        continue; // any error → retry
                    }
                    match run(&ex, &rt, sid, "COMMIT") {
                        Ok(_) => { commits.fetch_add(1, Ordering::Relaxed); break; }
                        Err(_) => { let _ = run(&ex, &rt, sid, "ROLLBACK"); continue; }
                    }
                }
            }
            ex.drop_session(sid);
        }));
    }
    for h in handles { let _ = h.join(); }

    let n_commits = commits.load(Ordering::Relaxed);
    let rt = new_rt();
    let sid = ex.create_session();
    let final_v = read_int(&ex, &rt, sid, "SELECT v FROM counter WHERE id=1");
    ex.drop_session(sid);

    match final_v {
        Ok(Some(v)) => {
            if v != n_commits as i64 {
                report.fail(
                    "LOST UPDATE (final counter != successful commits)",
                    format!(
                        "  isolation={iso} workers={workers} per_worker={per_worker}\n  \
                         successful COMMITs = {n_commits}\n  final counter value = {v}\n  \
                         (expected {n_commits}; a lower value => a committed increment was lost — \
                         read-modify-write race not serialized). TIMING-DEPENDENT.",
                    ),
                );
            }
        }
        other => report.fail("lost-update final read failed", format!("  {other:?}")),
    }
}

// ─── Test 2: no dirty reads ───────────────────────────────────────────────────
//
// One writer takes a txn, updates v to a SENTINEL, and holds it uncommitted
// while M readers (each its own txn) repeatedly read v. No reader may ever see
// SENTINEL. Then the writer ROLLs BACK and the committed value must be unchanged.
fn test_dirty_read(rng: &mut Rng, report: &mut Report) {
    const BASE: i64 = 100;
    const SENTINEL: i64 = 999_999;
    let readers = 2 + rng.below(4);
    let reads_each = 30 + rng.below(60);
    let writer_iso = *rng.pick(&["SERIALIZABLE", "REPEATABLE READ", "READ COMMITTED"]);
    let reader_iso = *rng.pick(&["SERIALIZABLE", "REPEATABLE READ", "READ COMMITTED"]);

    let ex = fresh_executor();
    if let Err(e) = setup(&ex, &[
        "CREATE TABLE counter (id INTEGER PRIMARY KEY, v INTEGER NOT NULL)",
        &format!("INSERT INTO counter (id, v) VALUES (1, {BASE})"),
    ]) {
        report.fail("dirty-read setup failed", format!("  {e}"));
        return;
    }

    let dirty_seen = Arc::new(AtomicUsize::new(0));
    let writer_ready = Arc::new(Barrier::new(readers + 1));
    let readers_done = Arc::new(Barrier::new(readers + 1));

    // Writer thread: open txn, write sentinel, wait for readers, rollback.
    let writer = {
        let ex = ex.clone();
        let wr = writer_ready.clone();
        let rd = readers_done.clone();
        let iso = writer_iso.to_string();
        std::thread::spawn(move || {
            let rt = new_rt();
            let sid = ex.create_session();
            let _ = run(&ex, &rt, sid, &format!("BEGIN ISOLATION LEVEL {iso}"));
            let _ = run(&ex, &rt, sid, &format!("UPDATE counter SET v={SENTINEL} WHERE id=1"));
            wr.wait(); // signal readers: uncommitted write is in place
            rd.wait(); // wait for readers to finish hammering
            let _ = run(&ex, &rt, sid, "ROLLBACK");
            ex.drop_session(sid);
        })
    };

    let mut handles = Vec::new();
    for _ in 0..readers {
        let ex = ex.clone();
        let seen = dirty_seen.clone();
        let wr = writer_ready.clone();
        let rd = readers_done.clone();
        let iso = reader_iso.to_string();
        handles.push(std::thread::spawn(move || {
            let rt = new_rt();
            let sid = ex.create_session();
            wr.wait(); // start only after writer staged its uncommitted write
            for _ in 0..reads_each {
                // Each read in its own short txn so READ COMMITTED re-snapshots.
                let _ = run(&ex, &rt, sid, &format!("BEGIN ISOLATION LEVEL {iso}"));
                if let Ok(Some(v)) = read_int(&ex, &rt, sid, "SELECT v FROM counter WHERE id=1") {
                    if v == SENTINEL {
                        seen.fetch_add(1, Ordering::Relaxed);
                    } else if v != BASE {
                        // some other committed value would be a different bug
                        seen.fetch_add(1, Ordering::Relaxed);
                    }
                }
                let _ = run(&ex, &rt, sid, "COMMIT");
            }
            rd.wait();
            ex.drop_session(sid);
        }));
    }
    let _ = writer.join();
    for h in handles { let _ = h.join(); }

    let n = dirty_seen.load(Ordering::Relaxed);
    if n > 0 {
        report.fail(
            "DIRTY READ (uncommitted write observed by another txn)",
            format!(
                "  writer_iso={writer_iso} reader_iso={reader_iso} readers={readers}\n  \
                 readers observed the uncommitted sentinel/non-base value {n} time(s).\n  \
                 Committed value was {BASE}; writer rolled back. TIMING-DEPENDENT.",
            ),
        );
    }

    // After rollback the committed value must be exactly BASE.
    let rt = new_rt();
    let sid = ex.create_session();
    match read_int(&ex, &rt, sid, "SELECT v FROM counter WHERE id=1") {
        Ok(Some(v)) if v != BASE => report.fail(
            "ROLLBACK DID NOT RESTORE (committed value changed after aborted txn)",
            format!("  expected {BASE}, got {v}  writer_iso={writer_iso}"),
        ),
        Ok(_) => {}
        other => report.fail("dirty-read post read failed", format!("  {other:?}")),
    }
    ex.drop_session(sid);
}

// ─── Test 3: no torn reads (atomicity of multi-row/column commit) ─────────────
//
// Invariant a+b == CONST. Writers repeatedly, in one txn, set a=x and b=CONST-x
// then COMMIT (so any committed state satisfies a+b==CONST). Readers, each in a
// single txn, read both columns and verify a+b==CONST. A reader that ever sees
// a+b != CONST observed a partial/torn transaction.
fn test_torn_read(rng: &mut Rng, report: &mut Report) {
    const CONST: i64 = 1000;
    let writers = 1 + rng.below(3);
    let readers = 2 + rng.below(4);
    let iters = 40 + rng.below(80);
    let w_iso = *rng.pick(&["SERIALIZABLE", "REPEATABLE READ", "READ COMMITTED"]);
    let r_iso = *rng.pick(&["SERIALIZABLE", "REPEATABLE READ", "READ COMMITTED"]);

    let ex = fresh_executor();
    if let Err(e) = setup(&ex, &[
        "CREATE TABLE acct (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, b INTEGER NOT NULL)",
        &format!("INSERT INTO acct (id, a, b) VALUES (1, 0, {CONST})"),
    ]) {
        report.fail("torn-read setup failed", format!("  {e}"));
        return;
    }

    let torn = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(writers + readers));

    let mut handles = Vec::new();
    for w in 0..writers {
        let ex = ex.clone();
        let stop = stop.clone();
        let barrier = barrier.clone();
        let iso = w_iso.to_string();
        let seed = (w as u64).wrapping_add(0xABCD);
        handles.push(std::thread::spawn(move || {
            let rt = new_rt();
            let sid = ex.create_session();
            let mut r = Rng(seed | 1);
            barrier.wait();
            while !stop.load(Ordering::Relaxed) {
                let x = (r.next() % (CONST as u64 + 1)) as i64;
                if run(&ex, &rt, sid, &format!("BEGIN ISOLATION LEVEL {iso}")).is_err() { continue; }
                let ok1 = run(&ex, &rt, sid, &format!("UPDATE acct SET a={x} WHERE id=1")).is_ok();
                let ok2 = run(&ex, &rt, sid, &format!("UPDATE acct SET b={} WHERE id=1", CONST - x)).is_ok();
                if ok1 && ok2 {
                    let _ = run(&ex, &rt, sid, "COMMIT");
                } else {
                    let _ = run(&ex, &rt, sid, "ROLLBACK");
                }
            }
            ex.drop_session(sid);
        }));
    }
    for _ in 0..readers {
        let ex = ex.clone();
        let torn = torn.clone();
        let stop = stop.clone();
        let barrier = barrier.clone();
        let iso = r_iso.to_string();
        handles.push(std::thread::spawn(move || {
            let rt = new_rt();
            let sid = ex.create_session();
            barrier.wait();
            for _ in 0..iters {
                let _ = run(&ex, &rt, sid, &format!("BEGIN ISOLATION LEVEL {iso}"));
                let mut r = match run(&ex, &rt, sid, "SELECT a, b FROM acct WHERE id=1") {
                    Ok(x) => x,
                    Err(_) => { let _ = run(&ex, &rt, sid, "ROLLBACK"); continue; }
                };
                if let Some(ExecResult::Select { rows, .. }) = r.pop() {
                    if let Some(row) = rows.get(0) {
                        let a = as_int(row.get(0));
                        let b = as_int(row.get(1));
                        if let (Some(a), Some(b)) = (a, b) {
                            if a + b != CONST {
                                torn.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                }
                let _ = run(&ex, &rt, sid, "COMMIT");
            }
            ex.drop_session(sid);
        }));
    }
    // Let readers finish first, then stop writers.
    // Readers loop a fixed count; writers loop until stop. Join readers via
    // sleep-free coordination: writers are the last `writers` handles? They are
    // first. Simpler: join all, but signal stop after a brief spin so writers exit.
    // We spin a bounded number of yields proportional to reader work.
    {
        // Busy-wait until readers likely done: poll torn/iters by yielding.
        // To avoid blocked sleep, yield many times.
        for _ in 0..(iters * readers * 200) {
            std::thread::yield_now();
        }
        stop.store(true, Ordering::Relaxed);
    }
    for h in handles { let _ = h.join(); }

    // Final committed invariant must hold.
    let rt = new_rt();
    let sid = ex.create_session();
    if let Ok(mut r) = run(&ex, &rt, sid, "SELECT a, b FROM acct WHERE id=1") {
        if let Some(ExecResult::Select { rows, .. }) = r.pop() {
            if let Some(row) = rows.get(0) {
                if let (Some(a), Some(b)) = (as_int(row.get(0)), as_int(row.get(1))) {
                    if a + b != CONST {
                        report.fail(
                            "FINAL STATE VIOLATES INVARIANT (a+b != CONST after all commits)",
                            format!("  a={a} b={b} a+b={} expected {CONST}  w_iso={w_iso}", a + b),
                        );
                    }
                }
            }
        }
    }
    ex.drop_session(sid);

    let n = torn.load(Ordering::Relaxed);
    if n > 0 {
        report.fail(
            "TORN READ (reader saw partial transaction: a+b != CONST)",
            format!(
                "  w_iso={w_iso} r_iso={r_iso} writers={writers} readers={readers}\n  \
                 invariant a+b={CONST} violated on {n} read(s).\n  \
                 A committed snapshot must never expose both columns mid-update. TIMING-DEPENDENT.",
            ),
        );
    }
}

fn as_int(v: Option<&Value>) -> Option<i64> {
    match v {
        Some(Value::Int64(n)) => Some(*n),
        Some(Value::Int32(n)) => Some(*n as i64),
        _ => None,
    }
}

// ─── Test 4: write-conflict detection (no silent lost write) ──────────────────
//
// Two workers both: BEGIN; UPDATE the SAME row to their own distinct value;
// then COMMIT. With "first-updater-wins", at most one such overlapping pair may
// both commit ONLY if the second re-read; here neither re-reads, so the engine
// must reject one of the two concurrent writers. We assert: it is never the case
// that both report a clean COMMIT while the final value matches neither writer,
// and that when both "commit", the survivor's value is one of the two written
// (no corruption). The strong check: across many trials, at least some pairs are
// rejected (conflicts ARE detected at all); and the final value is always a
// value that some committed writer wrote.
fn test_write_conflict(rng: &mut Rng, report: &mut Report, stats: &mut ConflictStats) {
    let iso = *rng.pick(&["SERIALIZABLE", "REPEATABLE READ"]);
    let ex = fresh_executor();
    if let Err(e) = setup(&ex, &[
        "CREATE TABLE counter (id INTEGER PRIMARY KEY, v INTEGER NOT NULL)",
        "INSERT INTO counter (id, v) VALUES (1, 0)",
    ]) {
        report.fail("write-conflict setup failed", format!("  {e}"));
        return;
    }

    let val_a = 11i64;
    let val_b = 22i64;
    let barrier = Arc::new(Barrier::new(2));
    let outcome = Arc::new([AtomicUsize::new(0), AtomicUsize::new(0)]); // [committed, rejected] per writer index encoded below
    let committed_vals = Arc::new(std::sync::Mutex::new(Vec::<i64>::new()));

    let mut handles = Vec::new();
    for (idx, val) in [(0usize, val_a), (1usize, val_b)] {
        let ex = ex.clone();
        let barrier = barrier.clone();
        let outcome = outcome.clone();
        let committed_vals = committed_vals.clone();
        let iso = iso.to_string();
        let _ = idx;
        handles.push(std::thread::spawn(move || {
            let rt = new_rt();
            let sid = ex.create_session();
            barrier.wait();
            let _ = run(&ex, &rt, sid, &format!("BEGIN ISOLATION LEVEL {iso}"));
            let upd = run(&ex, &rt, sid, &format!("UPDATE counter SET v={val} WHERE id=1"));
            barrier.wait(); // both attempt update before either commits
            if upd.is_err() {
                let _ = run(&ex, &rt, sid, "ROLLBACK");
                outcome[1].fetch_add(1, Ordering::Relaxed);
            } else {
                match run(&ex, &rt, sid, "COMMIT") {
                    Ok(_) => {
                        outcome[0].fetch_add(1, Ordering::Relaxed);
                        committed_vals.lock().unwrap().push(val);
                    }
                    Err(_) => {
                        let _ = run(&ex, &rt, sid, "ROLLBACK");
                        outcome[1].fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            ex.drop_session(sid);
        }));
    }
    for h in handles { let _ = h.join(); }

    let committed = outcome[0].load(Ordering::Relaxed);
    let rejected = outcome[1].load(Ordering::Relaxed);
    stats.trials += 1;
    if rejected > 0 { stats.conflicts_detected += 1; }
    if committed == 2 { stats.both_committed += 1; }

    // Final value sanity: must be a value some committed writer wrote.
    let rt = new_rt();
    let sid = ex.create_session();
    let final_v = read_int(&ex, &rt, sid, "SELECT v FROM counter WHERE id=1").ok().flatten();
    ex.drop_session(sid);
    let cvals = committed_vals.lock().unwrap().clone();

    if let Some(fv) = final_v {
        if committed >= 1 {
            // final must equal a committed writer's value
            if !cvals.contains(&fv) {
                report.fail(
                    "CORRUPT FINAL VALUE (final != any committed write)",
                    format!(
                        "  iso={iso} committed_writers={cvals:?} final={fv}\n  \
                         final value matches neither committed writer — write got corrupted.",
                    ),
                );
            }
        } else {
            // nobody committed → must still be initial 0
            if fv != 0 {
                report.fail(
                    "GHOST WRITE (value changed though no writer committed)",
                    format!("  iso={iso} both writers rejected/rolled back but final={fv} (expected 0)"),
                );
            }
        }
    }

    // Hard violation: BOTH committed without either re-reading the other's write.
    // Under first-updater-wins this is impossible for blind overlapping writes
    // to the SAME row started before either committed — one must be rejected.
    if committed == 2 {
        report.fail(
            "WRITE-WRITE CONFLICT NOT DETECTED (both blind writers committed same row)",
            format!(
                "  iso={iso}\n  both writers updated counter.v in overlapping txns and both \
                 COMMITted — one write was silently lost (first-updater-wins violated).\n  \
                 final value = {final_v:?}. TIMING-DEPENDENT (both must overlap).",
            ),
        );
    }
}

struct ConflictStats {
    trials: usize,
    conflicts_detected: usize,
    both_committed: usize,
}

fn main_impl() {
    let mut seed: u64 = 0xC0FFEE_77;
    let mut rounds = 150usize;
    let mut max_report = 12usize;
    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--seed" => { i += 1; seed = args[i].parse().unwrap(); }
            "--rounds" => { i += 1; rounds = args[i].parse().unwrap(); }
            "--max-report" => { i += 1; max_report = args[i].parse().unwrap(); }
            _ => {}
        }
        i += 1;
    }
    // Silence per-call panics (we catch_unwind around executor calls).
    std::panic::set_hook(Box::new(|_| {}));

    println!("Nucleus real-thread MVCC concurrency invariant checker");
    println!("seed={seed} rounds={rounds} (4 invariants/round, genuine OS-thread contention)\n");

    let mut rng = Rng(seed | 1);
    let mut report = Report { divergences: 0, max: max_report };
    let mut cstats = ConflictStats { trials: 0, conflicts_detected: 0, both_committed: 0 };

    for round in 0..rounds {
        if round % 25 == 0 && round > 0 {
            println!("  …round {round}/{rounds} (violations so far: {})", report.divergences);
        }
        test_lost_update(&mut rng, &mut report);
        test_dirty_read(&mut rng, &mut report);
        test_torn_read(&mut rng, &mut report);
        test_write_conflict(&mut rng, &mut report, &mut cstats);
    }

    println!("\n════ SUMMARY ════");
    println!("rounds                       : {rounds}");
    println!("invariant violations         : {}", report.divergences);
    println!(
        "write-conflict trials        : {} (conflicts detected in {}, both-committed in {})",
        cstats.trials, cstats.conflicts_detected, cstats.both_committed
    );
    if cstats.trials > 0 && cstats.conflicts_detected == 0 {
        println!(
            "NOTE: no write-write conflict was ever observed across {} trials — either contention \
             never overlapped (timing) or detection is absent; inspect if persistent across seeds.",
            cstats.trials
        );
    }
    if report.divergences == 0 {
        println!("\nAll MVCC concurrency invariants held under real-thread contention. 🎯");
    } else {
        std::process::exit(1);
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    tokio::task::spawn_blocking(main_impl).await.unwrap();
}
