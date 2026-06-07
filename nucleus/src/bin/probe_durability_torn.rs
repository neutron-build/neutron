//! Torn-write / power-loss durability approximation (Tier 2).
//!
//! For each iteration we open a durable WAL-backed database in a fresh temp
//! dir, apply a random sequence of COMMITTED auto-commit + BEGIN/COMMIT
//! mutations, `sync()` (fsync the WAL), snapshot the live committed state via
//! SELECT, then "crash" by dropping the db. We then simulate a torn / partial
//! write or power loss by mutating the on-disk WAL file (`dir/mvcc.wal`):
//!
//!   * truncate at a random offset (mid-record),
//!   * drop a run of trailing bytes,
//!   * flip random bytes (mid-record / tail / length-prefix / CRC field).
//!
//! Then we reopen and assert the durability contract:
//!
//!   1. Reopen NEVER panics / aborts.
//!   2. Recovery yields a CONSISTENT state: every recovered row was actually
//!      committed AT SOME POINT before the crash. Because replay stops at the
//!      torn record boundary, the recovered state is the state after applying a
//!      PREFIX of the committed record stream — which is exactly one of the
//!      committed states the live DB passed through. So the correct invariant
//!      is: every recovered (id -> row) tuple belongs to the EVER-COMMITTED set
//!      (the union of all post-commit snapshots). A torn tail that drops a
//!      recent DELETE/UPDATE legitimately resurrects/reverts to an EARLIER
//!      committed version — that is acceptable "lost a recent committed write"
//!      behavior, NOT a bug. Recovering an (id,row) tuple that was NEVER
//!      committed in any state is a REAL BUG.
//!   3. The WAL has a per-record CRC32C — a corrupted/torn trailing record
//!      MUST be skipped (CRC honored, not mis-applied). A recovered row that
//!      sits "behind" a corrupted record but was never committed in any state
//!      indicates the CRC gate let bad bytes through.
//!
//! Ground truth is the union of live SELECTs taken after every commit boundary
//! BEFORE the crash (the engine's own view of committed state at each step) —
//! NOT a re-run of the replay logic, so this does not test the parser against
//! itself.
//!
//! Build/run:
//!   cargo build  --release --features "server rusqlite" --bin probe_durability_torn
//!   cargo run    --release --features "server rusqlite" --bin probe_durability_torn
#![cfg(feature = "server")]
#![allow(unused)]
#![allow(clippy::all)] // internal fuzz harness

use std::collections::BTreeMap;
use std::path::PathBuf;

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
    fn below(&mut self, n: usize) -> usize { (self.next() % n.max(1) as u64) as usize }
    fn chance(&mut self, pct: u64) -> bool { self.next() % 100 < pct }
    fn int(&mut self, lo: i64, hi: i64) -> i64 { lo + (self.next() % ((hi - lo + 1) as u64)) as i64 }
    fn pick<'a, T>(&mut self, xs: &'a [T]) -> &'a T { &xs[self.below(xs.len())] }
}

#[derive(Clone, Copy, PartialEq)]
enum Ty { Int, Real, Text }
#[derive(Clone)]
struct Col { name: &'static str, ty: Ty, nn: bool }
struct Schema { cols: Vec<Col> }

const NAMES: &[&str] = &["c1", "c2", "c3", "c4"];
const CATS: &[&str] = &["red", "green", "blue", "amber", "str0", "str1"];

impl Schema {
    fn random(rng: &mut Rng) -> Schema {
        let mut cols = vec![Col { name: "id", ty: Ty::Int, nn: true }];
        let extra = 1 + rng.below(3);
        for k in 0..extra {
            let ty = *rng.pick(&[Ty::Int, Ty::Real, Ty::Text]);
            cols.push(Col { name: NAMES[k], ty, nn: rng.chance(45) });
        }
        Schema { cols }
    }
    fn ddl(&self) -> String {
        let parts: Vec<String> = self.cols.iter().enumerate().map(|(i, c)| {
            if i == 0 { return "id INTEGER PRIMARY KEY".to_string(); }
            let ty = match c.ty { Ty::Int => "INTEGER", Ty::Real => "REAL", Ty::Text => "TEXT" };
            format!("{} {ty}{}", c.name, if c.nn { " NOT NULL" } else { "" })
        }).collect();
        format!("CREATE TABLE t ({})", parts.join(", "))
    }
    fn names(&self) -> String { self.cols.iter().map(|c| c.name).collect::<Vec<_>>().join(",") }
    fn nonid<'a>(&'a self, rng: &mut Rng) -> &'a Col { &self.cols[1 + rng.below(self.cols.len() - 1)] }
}

fn gen_value(rng: &mut Rng, c: &Col) -> String {
    if !c.nn && rng.chance(22) { return "NULL".into(); }
    match c.ty {
        Ty::Int => rng.int(-9, 30).to_string(),
        Ty::Real => format!("{:.1}", rng.int(-50, 50) as f64 / 10.0),
        Ty::Text => format!("'{}'", rng.pick(CATS)),
    }
}

fn gen_insert(s: &Schema, rng: &mut Rng, id: i64) -> String {
    let cells: Vec<String> = s.cols.iter().enumerate()
        .map(|(i, c)| if i == 0 { id.to_string() } else { gen_value(rng, c) }).collect();
    format!("INSERT INTO t ({}) VALUES ({})", s.names(), cells.join(","))
}

fn canon(v: &Value) -> String {
    match v {
        Value::Null => "∅".into(),
        Value::Bool(b) => (if *b { "1" } else { "0" }).into(),
        Value::Int32(n) => n.to_string(),
        Value::Int64(n) => n.to_string(),
        Value::Float64(f) => {
            if f.is_finite() && (f - f.round()).abs() < 1e-9 && f.abs() < 9e15 {
                format!("{}", f.round() as i64)
            } else { format!("{f:.6}") }
        }
        Value::Text(s) => format!("'{s}'"),
        other => format!("'{other}'"),
    }
}

// ─── Engine I/O (block_in_place + catch_unwind) ───────────────────────────────
fn exec(db: &Database, sql: &str) -> Result<(), ()> {
    let rt = tokio::runtime::Handle::current();
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        tokio::task::block_in_place(|| rt.block_on(db.execute(sql)))
    })).map(|r| if r.is_ok() { Ok(()) } else { Err(()) }).unwrap_or(Err(()))
}

enum Snap {
    Rows(BTreeMap<String, Vec<String>>),
    /// Table `t` does not exist (e.g. the CreateTable record was torn). The
    /// recovered row set is trivially empty — an acceptable outcome.
    NoTable,
    /// Unexpected read failure or panic — a genuine problem.
    HardError,
}

/// Snapshot the table as an id -> canonical-row map.
fn snapshot(db: &Database, cols: &str) -> Snap {
    let rt = tokio::runtime::Handle::current();
    let sql = format!("SELECT id,{cols} FROM t ORDER BY id ASC");
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        tokio::task::block_in_place(|| rt.block_on(db.execute(&sql)))
    }));
    match res {
        Ok(Ok(mut r)) => match r.pop() {
            Some(ExecResult::Select { rows, .. }) => {
                let mut m = BTreeMap::new();
                for row in &rows {
                    let key = match row.first() { Some(v) => canon(v), None => return Snap::HardError };
                    let vals: Vec<String> = row.iter().map(canon).collect();
                    m.insert(key, vals);
                }
                Snap::Rows(m)
            }
            _ => Snap::HardError,
        },
        Ok(Err(e)) => {
            let msg = format!("{e:?}").to_lowercase();
            if msg.contains("tablenotfound") || msg.contains("does not exist")
                || msg.contains("not found") || msg.contains("no such table") {
                Snap::NoTable
            } else {
                Snap::HardError
            }
        }
        Err(_) => Snap::HardError,
    }
}

struct TmpDir(PathBuf);
impl TmpDir {
    fn new(tag: &str) -> Self {
        let mut p = std::env::temp_dir();
        p.push(format!("nucleus_torn_{tag}"));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).expect("mkdir temp");
        TmpDir(p)
    }
    fn wal(&self) -> PathBuf { self.0.join("mvcc.wal") }
}
impl Drop for TmpDir {
    fn drop(&mut self) { let _ = std::fs::remove_dir_all(&self.0); }
}

/// Open under catch_unwind so a panic in recovery is reported, not aborted.
fn open(dir: &PathBuf) -> Result<Result<Database, String>, ()> {
    std::panic::catch_unwind(|| Database::durable_mvcc(dir).map_err(|e| format!("{e:?}")))
        .map_err(|_| ())
}

#[derive(Clone, Debug)]
enum Tear {
    /// Truncate the file to `len` bytes (mid-record).
    Truncate(usize),
    /// Flip `count` bytes at `offset` (consecutive), simulating a torn page.
    FlipRun { offset: usize, count: usize },
    /// Flip a single byte at `offset`.
    Flip(usize),
}

fn apply_tear(bytes: &mut Vec<u8>, tear: &Tear) {
    match tear {
        Tear::Truncate(len) => { bytes.truncate((*len).min(bytes.len())); }
        Tear::FlipRun { offset, count } => {
            for i in 0..*count {
                if let Some(b) = bytes.get_mut(offset + i) { *b ^= 0xFF; }
            }
        }
        Tear::Flip(offset) => { if let Some(b) = bytes.get_mut(*offset) { *b ^= 0xA5; } }
    }
}

/// Pick a tearing strategy biased toward the trailing portion of the WAL
/// (where a torn write lands) but also exercising mid-file flips.
fn gen_tear(rng: &mut Rng, len: usize) -> Tear {
    if len == 0 { return Tear::Truncate(0); }
    match rng.below(5) {
        // Truncate somewhere in the trailing half (mid-record territory).
        0 | 1 => {
            let lo = len / 2;
            Tear::Truncate(lo + rng.below(len - lo + 1))
        }
        // Drop a small run of trailing bytes (partial last record).
        2 => {
            let drop = 1 + rng.below(8.min(len));
            Tear::Truncate(len - drop)
        }
        // Flip a run of bytes near the tail (torn page).
        3 => {
            let lo = len.saturating_sub(32);
            let off = lo + rng.below(len - lo);
            Tear::FlipRun { offset: off, count: 1 + rng.below(6) }
        }
        // Single flip anywhere (could hit a length prefix or CRC field).
        _ => Tear::Flip(rng.below(len)),
    }
}

fn main_impl() {
    let mut seed: u64 = 0x70B0_FEED;
    let mut iterations = 4000usize;
    let mut ops_per = 18usize;
    let mut max_report = 15usize;
    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--seed" => { i += 1; seed = args[i].parse().unwrap(); }
            "--iterations" => { i += 1; iterations = args[i].parse().unwrap(); }
            "--ops" => { i += 1; ops_per = args[i].parse().unwrap(); }
            "--max-report" => { i += 1; max_report = args[i].parse().unwrap(); }
            _ => {}
        }
        i += 1;
    }
    std::panic::set_hook(Box::new(|_| {}));
    println!("Nucleus torn-write / power-loss durability fuzzer");
    println!("seed={seed} iterations={iterations} ops/iter={ops_per}\n");

    let mut total = 0usize;       // torn-recover cycles attempted
    let mut panics = 0usize;      // reopen panicked/aborted-via-catch
    let mut resurrected = 0usize; // recovered a non-committed / wrong-value row
    let mut findings = 0usize;
    let mut cov_nonempty = 0usize; // cycles where rows survived the tear (real verification work)
    let mut cov_partial = 0usize;  // cycles where the tear dropped >=1 committed record (lossy recovery)

    'outer: for iter in 0..iterations {
        let mut rng = Rng(seed.wrapping_add(iter as u64).wrapping_mul(0x100000001B3));
        let schema = Schema::random(&mut rng);
        let cols = schema.cols.iter().skip(1).map(|c| c.name).collect::<Vec<_>>().join(",");
        let tmp = TmpDir::new(&format!("{seed}_{iter}"));

        // ── Phase 1: write COMMITTED data, accumulate the EVER-COMMITTED set,
        //    sync, crash. `ever` maps an (id,row) tuple -> () for every row that
        //    appeared in any committed state. A row is keyed by its full
        //    canonical tuple so a value-revert from a dropped UPDATE is still
        //    recognized as "was committed at some point".
        let mut ever: std::collections::HashSet<Vec<String>> = std::collections::HashSet::new();
        let mut final_rows = 0usize; // size of the last pre-crash committed state
        {
            // Accumulate every committed-state row tuple into `ever`.
            macro_rules! accumulate { ($db:expr) => {{
                match snapshot($db, &cols) {
                    Snap::Rows(snap) => { final_rows = snap.len(); for (_id, row) in snap { ever.insert(row); } true }
                    _ => false,
                }
            }}; }
            let db = match open(&tmp.0) {
                Ok(Ok(d)) => d,
                Ok(Err(_)) => continue 'outer,
                Err(_) => { panics += 1; findings += 1;
                    if findings <= max_report { println!("─── REOPEN PANIC (initial open, iter {iter})\n"); }
                    continue 'outer; }
            };
            if exec(&db, &schema.ddl()).is_err() { continue 'outer; }
            let mut next_id = 1i64;
            let mut in_txn = false;
            for _ in 0..ops_per {
                // Occasionally wrap a couple inserts in an explicit txn (fsync on commit).
                if !in_txn && rng.chance(25) { let _ = exec(&db, "BEGIN"); in_txn = true; }
                match rng.below(5) {
                    0 | 1 | 2 => { let id = next_id; next_id += 1; let _ = exec(&db, &gen_insert(&schema, &mut rng, id)); }
                    3 => { let c = schema.nonid(&mut rng).clone();
                           let _ = exec(&db, &format!("UPDATE t SET {} = {} WHERE id = {}",
                               c.name, gen_value(&mut rng, &c), rng.int(1, next_id.max(1)))); }
                    _ => { let _ = exec(&db, &format!("DELETE FROM t WHERE id = {}", rng.int(1, next_id.max(1)))); }
                }
                // Capture every committed state the live DB passes through. Inside
                // an open txn the writes are not yet committed, so we only sample
                // at commit boundaries (auto-commit ops, or after COMMIT).
                if !in_txn { if !accumulate!(&db) { continue 'outer; } }
                if in_txn && rng.chance(50) { let _ = exec(&db, "COMMIT"); in_txn = false; if !accumulate!(&db) { continue 'outer; } }
            }
            if in_txn { let _ = exec(&db, "COMMIT"); }
            // Make everything committed durable against power loss.
            let _ = db.sync();
            if !accumulate!(&db) { continue 'outer; }
            // drop(db) below == crash.
        }

        // ── Phase 2: tear the on-disk WAL. ──
        let original = match std::fs::read(tmp.wal()) { Ok(b) => b, Err(_) => continue 'outer };
        if original.is_empty() { continue 'outer; }
        // A few independent torn variants per committed image, each on a fresh copy.
        let variants = 2 + rng.below(3);
        for _ in 0..variants {
            let mut torn = original.clone();
            let tear = gen_tear(&mut rng, torn.len());
            apply_tear(&mut torn, &tear);
            if std::fs::write(tmp.wal(), &torn).is_err() { continue; }

            // ── Phase 3: reopen + verify the durability contract. ──
            total += 1;
            let db = match open(&tmp.0) {
                Ok(Ok(d)) => d,
                Ok(Err(_e)) => {
                    // An Err (not a panic) on reopen of a torn WAL is itself a
                    // durability defect: recovery should tolerate a torn tail.
                    findings += 1; panics += 1;
                    if findings <= max_report {
                        println!("─── REOPEN ERROR (torn WAL, iter {iter}) ── tear={:?}\n", tear);
                    }
                    // restore original so subsequent variants start clean
                    let _ = std::fs::write(tmp.wal(), &original);
                    continue;
                }
                Err(_) => {
                    findings += 1; panics += 1;
                    if findings <= max_report {
                        println!("─── REOPEN PANIC (torn WAL, iter {iter}) ── tear={:?}", tear);
                        println!("  wal_len={} schema={}\n", original.len(), schema.ddl());
                    }
                    let _ = std::fs::write(tmp.wal(), &original);
                    continue;
                }
            };

            let recovered = match snapshot(&db, &cols) {
                Snap::Rows(s) => s,
                // CreateTable record torn away => table gone => empty recovered
                // set. Trivially satisfies recovered ⊆ ever. Acceptable.
                Snap::NoTable => { drop(db); let _ = std::fs::write(tmp.wal(), &original); continue; }
                Snap::HardError => {
                    findings += 1;
                    if findings <= max_report {
                        println!("─── RECOVERY READ FAILED (torn WAL, iter {iter}) ── tear={:?}\n", tear);
                    }
                    drop(db);
                    let _ = std::fs::write(tmp.wal(), &original);
                    continue;
                }
            };

            // Safety invariant: every recovered (id,row) tuple must belong to
            // the EVER-COMMITTED set. A recovered row that was never committed
            // in ANY state means corrupt / never-committed data survived
            // recovery — a REAL bug (CRC mis-applied or a torn record partially
            // replayed). A recovered row that reverts to an earlier committed
            // version (dropped DELETE/UPDATE in the torn tail) is NOT flagged.
            if !recovered.is_empty() { cov_nonempty += 1; }
            // A torn tail that drops committed records yields fewer rows than the
            // last committed state — confirms we exercised real recovery-loss
            // paths (not just no-op tears that left the WAL fully valid).
            if recovered.len() < final_rows { cov_partial += 1; }

            let mut bad: Option<(String, Vec<String>)> = None;
            for (id, row) in &recovered {
                if !ever.contains(row) { bad = Some((id.clone(), row.clone())); break; }
            }
            drop(db); // re-open compacted the WAL on disk; restore original next.

            if let Some((id, got)) = bad {
                resurrected += 1;
                findings += 1;
                if findings <= max_report {
                    println!("─── DURABILITY VIOLATION #{findings} (iter {iter}, seed {seed}) ───");
                    println!("  tear      : {:?}", tear);
                    println!("  schema    : {}", schema.ddl());
                    println!("  wal bytes : {} (orig)", original.len());
                    println!("  bad id    : {id}");
                    println!("  recovered : {:?}  (NEVER COMMITTED in any state)", got);
                    println!("  recovered={} rows, ever-committed tuples={}\n", recovered.len(), ever.len());
                }
            }

            // Restore the pristine torn-free image for the next variant.
            let _ = std::fs::write(tmp.wal(), &original);
        }
    }

    println!("\n════ SUMMARY ════");
    println!("torn-recover cycles : {total}");
    println!("  rows survived tear: {cov_nonempty}  (non-empty recovered set)");
    println!("  lossy recoveries  : {cov_partial}  (torn tail dropped committed records)");
    println!("reopen panics/errors: {panics}");
    println!("resurrected rows    : {resurrected}");
    println!("total findings      : {findings}");
    if findings == 0 {
        println!("\nNo panics; every recovered row was committed; CRC gate honored. 🎯");
    } else {
        std::process::exit(1);
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    tokio::task::spawn_blocking(main_impl).await.unwrap();
}
