//! REAL crash-injection durability fuzzer via subprocess SIGKILL.
//!
//! Unlike `probe_recover` (which "crashes" by dropping the Database in-process,
//! leaving the OS page cache and any pending fsync intact), this harness spawns
//! a *separate OS process* that opens a durable MVCC database and inserts rows
//! one-per-auto-commit-txn, calling `sync()` (fsync) after every commit and
//! printing the last durably-committed id. The parent then sends a real
//! `SIGKILL` at a random instant — the child dies instantly with no unwinding,
//! no Drop, no flush — exactly modeling a `kill -9` / hard process crash.
//!
//! After reaping the corpse the parent reopens the same dir and asserts the
//! recovery invariant:
//!
//!   The recovered rows are EXACTLY a committed prefix id = 1..k for some k,
//!   with no gaps, no rows beyond k, no duplicates, and no corruption.
//!
//! Because the child fsyncs after each insert, every id it *printed* as durable
//! MUST be present after recovery (k >= last_printed). A row may also appear for
//! the in-flight insert that had committed+fsynced but whose id the child was
//! killed before printing — that is still a valid larger prefix. The only
//! failures are: reopen panic/error, a gap in 1..k, a row with id > k where some
//! id <= k is missing (torn/over-recovery), duplicate ids, or corrupted values.
//!
//! Self-spawning: `--child <dir> <n>` runs the inserter; default mode is parent.
//! Build/run: `cargo run --release --features "server rusqlite" --bin probe_crash_subprocess`
//!   (rusqlite is unused here but the harness builds & runs fine with it on.)
#![cfg(feature = "server")]
#![allow(unused)]
#![allow(clippy::all)] // internal fuzz harness

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use nucleus::embedded::Database;
use nucleus::executor::ExecResult;
use nucleus::types::Value;

// ─── Deterministic PRNG ───────────────────────────────────────────────────────
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: usize) -> usize { (self.next() % n as u64) as usize }
    fn int(&mut self, lo: u64, hi: u64) -> u64 { lo + self.next() % (hi - lo + 1) }
}

// Per-row marker value, derived deterministically from the id so we can detect
// corrupted / cross-row torn writes after recovery.
fn marker_for(id: i64) -> i64 {
    (id.wrapping_mul(2_654_435_761)) & 0x7FFF_FFFF
}

// ─── Child: the process that gets SIGKILLed ─────────────────────────────────────
fn child_main(dir: &str, n: u64) -> ! {
    // No panic noise; if open fails we still must exit so the parent can detect it.
    std::panic::set_hook(Box::new(|_| {}));
    let dir = PathBuf::from(dir);

    let db = match Database::durable_mvcc(&dir) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("CHILD_OPEN_ERR {e:?}");
            std::process::exit(7);
        }
    };
    let rt = tokio::runtime::Runtime::new().expect("child rt");

    // Idempotent schema. id = primary key, m = derived marker, pad = filler text.
    let ddl = "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, m INTEGER NOT NULL, pad TEXT NOT NULL)";
    if let Err(e) = rt.block_on(db.execute(ddl)) {
        eprintln!("CHILD_DDL_ERR {e:?}");
        std::process::exit(8);
    }
    let _ = db.sync();

    // Discover where we should resume (supports multi-crash on the same dir).
    let start: i64 = match rt.block_on(db.query_one("SELECT COALESCE(MAX(id),0) FROM t")) {
        Ok(Some(Value::Int64(v))) => v + 1,
        Ok(Some(Value::Int32(v))) => v as i64 + 1,
        _ => 1,
    };

    let stdout = std::io::stdout();
    for id in start..start + n as i64 {
        let m = marker_for(id);
        let pad = format!("row-{id}-{m}");
        let sql = format!(
            "INSERT INTO t (id, m, pad) VALUES ({id}, {m}, '{pad}')"
        );
        // One auto-commit txn per row.
        if let Err(e) = rt.block_on(db.execute(&sql)) {
            eprintln!("CHILD_INSERT_ERR id={id} {e:?}");
            std::process::exit(9);
        }
        // fsync: make this commit durable against a hard process kill.
        if let Err(e) = db.sync() {
            eprintln!("CHILD_SYNC_ERR id={id} {e:?}");
            std::process::exit(10);
        }
        // Announce the last durably-committed id. The parent does not strictly
        // need this, but it lets us assert k >= last_printed.
        let mut h = stdout.lock();
        let _ = writeln!(h, "{id}");
        let _ = h.flush();
        drop(h);
    }

    // Done inserting all n rows; idle so the parent's random-timed kill can still
    // land (and so a "kill after completion" cycle exercises clean-state reopen).
    loop {
        std::thread::sleep(Duration::from_millis(20));
    }
}

// ─── Parent: spawn, kill, reopen, verify ────────────────────────────────────────

struct TmpDir(PathBuf);
impl TmpDir {
    fn new(tag: &str) -> Self {
        let mut p = std::env::temp_dir();
        p.push(format!("nucleus_crashsub_{tag}"));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).expect("mkdir temp");
        TmpDir(p)
    }
}
impl Drop for TmpDir {
    fn drop(&mut self) { let _ = std::fs::remove_dir_all(&self.0); }
}

/// Hard-kill the child. On Unix, `std::process::Child::kill()` delivers SIGKILL
/// (`libc::kill(pid, SIGKILL)`) — an uncatchable, un-handlable signal: the
/// process dies immediately with no unwinding, no Drop, no flush. Identical
/// crash semantics to `kill -9`. (libc is only a dev-dependency here, so we use
/// the std wrapper, which is precisely SIGKILL on this platform.)
fn sigkill(child: &mut std::process::Child) {
    let _ = child.kill();
}

#[derive(Debug)]
enum Finding {
    ReopenError(String),
    ReadError(String),
    Gap { missing: i64, max: i64 },          // id `missing` absent though `max` present
    DuplicateId(i64),
    BadMarker { id: i64, got: i64, want: i64 },
    BadPad { id: i64, got: String, want: String },
    LostCommitted { last_printed: i64, recovered_max: i64 }, // k < an id the child fsynced+printed
    NonContiguousType(String),
}

/// Reopen `dir`, read all rows, and verify the committed-prefix invariant.
/// `last_printed` is the highest id the child announced as durably committed
/// (0 if none observed).
fn verify(dir: &Path, last_printed: i64) -> Result<i64, Finding> {
    let db = Database::durable_mvcc(dir).map_err(|e| Finding::ReopenError(format!("{e:?}")))?;
    let rt = tokio::runtime::Runtime::new().expect("verify rt");

    let rows = match rt.block_on(db.execute("SELECT id, m, pad FROM t ORDER BY id ASC")) {
        Ok(mut r) => match r.pop() {
            Some(ExecResult::Select { rows, .. }) => rows,
            other => return Err(Finding::ReadError(format!("non-select: {other:?}"))),
        },
        Err(e) => {
            let es = format!("{e:?}");
            // The child can be killed before the CREATE TABLE commit lands. An
            // uninitialized DB is a *valid* committed prefix of length 0 — but
            // only if the child never reported a durable row. If it printed rows
            // yet the table is gone on reopen, that IS data loss / corruption.
            if es.contains("TableNotFound") && last_printed == 0 {
                return Ok(0);
            }
            return Err(Finding::ReadError(es));
        }
    };

    // Extract (id, m, pad) with type checks; detect duplicates and corruption.
    let mut ids: Vec<i64> = Vec::with_capacity(rows.len());
    let mut prev: Option<i64> = None;
    for row in &rows {
        let id = match &row[0] {
            Value::Int64(v) => *v,
            Value::Int32(v) => *v as i64,
            other => return Err(Finding::NonContiguousType(format!("id not int: {other:?}"))),
        };
        let m = match &row[1] {
            Value::Int64(v) => *v,
            Value::Int32(v) => *v as i64,
            other => return Err(Finding::NonContiguousType(format!("m not int: {other:?}"))),
        };
        let pad = match &row[2] {
            Value::Text(s) => s.clone(),
            other => return Err(Finding::NonContiguousType(format!("pad not text: {other:?}"))),
        };
        if let Some(p) = prev {
            if p == id { return Err(Finding::DuplicateId(id)); }
        }
        prev = Some(id);

        // Cross-field consistency: a torn / partial write would show a marker or
        // pad that doesn't match the id.
        let wm = marker_for(id);
        if m != wm { return Err(Finding::BadMarker { id, got: m, want: wm }); }
        let wpad = format!("row-{id}-{wm}");
        if pad != wpad { return Err(Finding::BadPad { id, got: pad, want: wpad }); }

        ids.push(id);
    }

    // Must be exactly the prefix 1..k.
    let k = ids.len() as i64;
    for (i, &id) in ids.iter().enumerate() {
        let expect = i as i64 + 1;
        if id != expect {
            // Either a gap (id jumped ahead) or out-of-prefix id.
            let max = *ids.last().unwrap();
            return Err(Finding::Gap { missing: expect, max });
        }
    }

    // Durability: everything the child fsynced + printed must be present.
    if k < last_printed {
        return Err(Finding::LostCommitted { last_printed, recovered_max: k });
    }
    Ok(k)
}

fn main_impl() {
    // ── Child dispatch ──
    let raw: Vec<String> = std::env::args().collect();
    if raw.len() >= 4 && raw[1] == "--child" {
        let dir = raw[2].clone();
        let n: u64 = raw[3].parse().unwrap_or(1000);
        child_main(&dir, n);
    }

    // ── Parent ──
    let mut seed: u64 = 0xC0FF_EE17;
    let mut cycles = 120usize;
    let mut rows_per: u64 = 800;
    let mut max_report = 15usize;
    let mut i = 1;
    while i < raw.len() {
        match raw[i].as_str() {
            "--seed" => { i += 1; seed = raw[i].parse().unwrap(); }
            "--cycles" => { i += 1; cycles = raw[i].parse().unwrap(); }
            "--rows" => { i += 1; rows_per = raw[i].parse().unwrap(); }
            "--max-report" => { i += 1; max_report = raw[i].parse().unwrap(); }
            _ => {}
        }
        i += 1;
    }
    std::panic::set_hook(Box::new(|_| {}));

    let exe = std::env::current_exe().expect("current_exe");
    println!("Nucleus subprocess SIGKILL crash-injection fuzzer");
    println!("seed={seed} cycles={cycles} rows/child={rows_per}\nexe={}\n", exe.display());

    let mut total = 0usize;
    let mut findings = 0usize;
    let mut last_k = 0i64;
    let mut max_k = 0i64;
    let mut sum_k = 0i64;
    let mut verified = 0usize;

    'outer: for cycle in 0..cycles {
        let mut rng = Rng(seed.wrapping_add(cycle as u64).wrapping_mul(0x100000001B3));
        // Most cycles use a fresh dir; some reuse the previous dir to exercise
        // recovery-then-more-inserts-then-crash (multi-crash on one WAL).
        let reuse = cycle > 0 && rng.below(3) == 0;
        let tag = if reuse {
            format!("{seed}_{}", cycle - 1)
        } else {
            format!("{seed}_{cycle}")
        };
        // Keep a TmpDir alive for cleanup; for reuse we don't wipe.
        let mut p = std::env::temp_dir();
        p.push(format!("nucleus_crashsub_{tag}"));
        let _tmp_guard;
        if !reuse {
            let _ = std::fs::remove_dir_all(&p);
            std::fs::create_dir_all(&p).expect("mkdir");
            _tmp_guard = Some(TmpDir(p.clone()));
        } else {
            _tmp_guard = None;
        }
        let dir = p;

        // Spawn the inserter child.
        let mut child = match Command::new(&exe)
            .arg("--child")
            .arg(&dir)
            .arg(rows_per.to_string())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
        {
            Ok(c) => c,
            Err(e) => { eprintln!("spawn failed: {e}"); continue; }
        };
        // Read the child's progress on a background thread so we know the highest
        // durably-committed id at kill time (best-effort lower bound on k).
        let stdout = child.stdout.take().unwrap();
        let last_printed = std::sync::Arc::new(std::sync::atomic::AtomicI64::new(0));
        let lp2 = last_printed.clone();
        let reader = std::thread::spawn(move || {
            use std::io::BufRead;
            let r = std::io::BufReader::new(stdout);
            for line in r.lines().flatten() {
                if let Ok(v) = line.trim().parse::<i64>() {
                    lp2.store(v, std::sync::atomic::Ordering::Relaxed);
                }
            }
        });

        // Arm the kill relative to *observed durable progress*, not raw spawn
        // time: a fresh process must cold-start a tokio runtime, open the DB and
        // commit the CREATE TABLE before the first insert — easily tens of ms.
        // We wait (bounded) until the child has fsynced+printed at least one row,
        // THEN sleep a random extra slice and kill. This guarantees most kills
        // land mid-insert-stream (the interesting window), while a fraction
        // (timeout path / tiny extra-slice) still hit early/very-late states.
        let arm_deadline = std::time::Instant::now() + Duration::from_millis(1500);
        loop {
            if last_printed.load(std::sync::atomic::Ordering::Relaxed) > 0 { break; }
            // Child may have already finished all rows and be idling, or died.
            match child.try_wait() {
                Ok(Some(_)) => break, // exited on its own (shouldn't, but be safe)
                _ => {}
            }
            if std::time::Instant::now() >= arm_deadline { break; }
            std::thread::sleep(Duration::from_micros(200));
        }
        // Random extra slice. fsync-per-row makes each commit ~milliseconds, so
        // we spread the kill across a wide range: most kills land in the first
        // handful of rows (small prefix), but a third run long enough to commit
        // tens–hundreds of rows (large WAL, deeper replay) and some run past
        // completion (clean-state reopen). This varies k widely across cycles.
        let micros = if rng.below(3) == 0 {
            rng.int(40_000, 1_200_000) // long: deep prefix / sometimes past completion
        } else {
            rng.int(0, 60_000) // short: torn region near the kill instant
        };
        std::thread::sleep(Duration::from_micros(micros));
        sigkill(&mut child);

        // Reap. (After SIGKILL the process is gone; wait reaps the zombie.)
        let _ = child.wait();
        let _ = reader.join();
        let printed = last_printed.load(std::sync::atomic::Ordering::Relaxed);

        // Reopen + verify in a fresh process-local runtime (parent process).
        total += 1;
        let res = std::panic::catch_unwind(|| verify(&dir, printed));
        match res {
            Ok(Ok(k)) => { last_k = k; if k > max_k { max_k = k; } sum_k += k; verified += 1; }
            Ok(Err(f)) => {
                findings += 1;
                if findings <= max_report {
                    println!("─── FINDING #{findings} (cycle {cycle}, reuse={reuse}, kill@{micros}us, printed_max={printed}) ───");
                    println!("  dir   : {}", dir.display());
                    println!("  detail: {f:?}\n");
                }
                if !reuse { continue 'outer; }
            }
            Err(_) => {
                findings += 1;
                if findings <= max_report {
                    println!("─── FINDING #{findings} (cycle {cycle}) ── PANIC during reopen/verify (kill@{micros}us)\n");
                }
                continue 'outer;
            }
        }
    }

    println!("\n════ SUMMARY ════");
    println!("crash/recover cycles: {total}");
    println!("findings            : {findings}");
    println!("recovered prefix k  : last={last_k} max={max_k} avg={:.1}",
        if verified > 0 { sum_k as f64 / verified as f64 } else { 0.0 });
    if findings == 0 {
        println!("\nEvery SIGKILL recovered an exact committed prefix; no gaps/over-recovery/corruption. 🎯");
    } else {
        std::process::exit(1);
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    tokio::task::spawn_blocking(main_impl).await.unwrap();
}
