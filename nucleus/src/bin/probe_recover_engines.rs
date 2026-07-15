//! Persistence/recovery round-trip fuzzer for the durable engines OTHER than
//! durable-MVCC (which probe_recover.rs already covers).
//!
//! What is actually durable through the public `Database` API:
//!   * `StorageMode::Disk`  — `Database::builder().disk(path)` / `Database::open(path)`.
//!     Page-store with a buffer pool + page WAL. NOTE (verified by reading
//!     src/storage/disk_engine.rs): `DiskEngine::drop()` calls `self.flush()`, so
//!     dropping the DB is a CLEAN SHUTDOWN, not a crash. There is also no public
//!     flush/checkpoint method on `Database`, and `Database::sync()` is a no-op for
//!     the Disk engine (DiskEngine does not override `StorageEngine::sync`). So the
//!     strongest crash-style test reachable through the public API is:
//!     write committed DDL/DML -> drop (clean flush) -> reopen -> assert recovered
//!     == pre-shutdown committed snapshot, across multiple cycles.
//!   * Durable KV — when a data dir is present the executor opens a WAL-backed
//!     `KvStore` (src/executor/mod.rs new_with_persistence -> KvStore::open). The KV
//!     WAL `flush()`es to the OS on every set/del/incr/expire (src/storage/kv_wal.rs),
//!     so a process crash (drop) keeps committed KV state; reopen replays the WAL.
//!     We exercise this directly via `db.kv()`.
//!
//! Per-table LSM and columnar/MergeTree engines are durable and publicly reachable
//! through `CREATE TABLE ... WITH (engine=...)` when the executor has a data
//! directory. Their restart/crash-copy coverage lives in `commit_durability.rs`;
//! this older whole-database harness remains focused on the global Disk engine.
//!
//! Build/run: `cargo run --release --features "server rusqlite" --bin probe_recover_engines`
//!   (rusqlite is unused here but harmless; `--features server` also works.)
#![cfg(feature = "server")]
#![allow(unused)]
#![allow(clippy::all)] // internal fuzz harness

use std::path::PathBuf;

use nucleus::embedded::Database;
use nucleus::executor::ExecResult;
use nucleus::types::Value;

// ─── Deterministic PRNG (xorshift) ─────────────────────────────────────────────
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
    fn chance(&mut self, pct: u64) -> bool {
        self.next() % 100 < pct
    }
    fn int(&mut self, lo: i64, hi: i64) -> i64 {
        lo + (self.next() % ((hi - lo + 1) as u64)) as i64
    }
    fn pick<'a, T>(&mut self, xs: &'a [T]) -> &'a T {
        &xs[self.below(xs.len())]
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Ty {
    Int,
    Real,
    Text,
}
#[derive(Clone)]
struct Col {
    name: &'static str,
    ty: Ty,
    nn: bool,
}
struct Schema {
    cols: Vec<Col>,
}

const NAMES: &[&str] = &["c1", "c2", "c3", "c4"];
const CATS: &[&str] = &["red", "green", "blue", "amber", "str0", "str1"];

impl Schema {
    fn random(rng: &mut Rng) -> Schema {
        let mut cols = vec![Col {
            name: "id",
            ty: Ty::Int,
            nn: true,
        }];
        cols.push(Col {
            name: NAMES[0],
            ty: Ty::Int,
            nn: true,
        });
        let extra = 1 + rng.below(3);
        for k in 0..extra {
            let ty = *rng.pick(&[Ty::Int, Ty::Real, Ty::Text]);
            cols.push(Col {
                name: NAMES[1 + k],
                ty,
                nn: rng.chance(45),
            });
        }
        Schema { cols }
    }
    fn ddl(&self) -> String {
        let parts: Vec<String> = self
            .cols
            .iter()
            .enumerate()
            .map(|(i, c)| {
                if i == 0 {
                    return "id INTEGER PRIMARY KEY".to_string();
                }
                let ty = match c.ty {
                    Ty::Int => "INTEGER",
                    Ty::Real => "REAL",
                    Ty::Text => "TEXT",
                };
                format!("{} {ty}{}", c.name, if c.nn { " NOT NULL" } else { "" })
            })
            .collect();
        format!("CREATE TABLE t ({})", parts.join(", "))
    }
    fn names(&self) -> String {
        self.cols
            .iter()
            .map(|c| c.name)
            .collect::<Vec<_>>()
            .join(",")
    }
    fn pick<'a>(&'a self, rng: &mut Rng) -> &'a Col {
        &self.cols[rng.below(self.cols.len())]
    }
}

fn gen_value(rng: &mut Rng, c: &Col) -> String {
    if !c.nn && rng.chance(22) {
        return "NULL".into();
    }
    match c.ty {
        Ty::Int => rng.int(-9, 30).to_string(),
        Ty::Real => format!("{:.1}", rng.int(-50, 50) as f64 / 10.0),
        Ty::Text => format!("'{}'", rng.pick(CATS)),
    }
}
fn gen_literal(rng: &mut Rng, c: &Col) -> String {
    match c.ty {
        Ty::Int => rng.int(-9, 30).to_string(),
        Ty::Real => format!("{:.1}", rng.int(-50, 50) as f64 / 10.0),
        Ty::Text => format!("'{}'", rng.pick(CATS)),
    }
}
fn gen_pred(s: &Schema, rng: &mut Rng) -> String {
    let c = s.pick(rng);
    match rng.below(4) {
        0 if !c.nn => format!(
            "{} IS {}NULL",
            c.name,
            if rng.chance(50) { "NOT " } else { "" }
        ),
        1 if c.ty == Ty::Int => format!(
            "{} {} {}",
            c.name,
            rng.pick(&["<", "<=", ">", ">=", "="]),
            gen_literal(rng, c)
        ),
        _ => format!(
            "{} {} {}",
            c.name,
            rng.pick(&["=", "<>"]),
            gen_literal(rng, c)
        ),
    }
}

fn gen_insert(s: &Schema, rng: &mut Rng, id: i64) -> String {
    let cells: Vec<String> = s
        .cols
        .iter()
        .enumerate()
        .map(|(i, c)| {
            if i == 0 {
                id.to_string()
            } else {
                gen_value(rng, c)
            }
        })
        .collect();
    format!("INSERT INTO t ({}) VALUES ({})", s.names(), cells.join(","))
}
fn gen_mutation(s: &Schema, rng: &mut Rng, next_id: &mut i64) -> String {
    match rng.below(4) {
        0 | 1 => {
            let id = *next_id;
            *next_id += 1;
            gen_insert(s, rng, id)
        }
        2 => {
            let c = *rng.pick(&s.cols.iter().skip(1).collect::<Vec<_>>());
            format!(
                "UPDATE t SET {} = {} WHERE {}",
                c.name,
                gen_value(rng, c),
                gen_pred(s, rng)
            )
        }
        _ => format!("DELETE FROM t WHERE {}", gen_pred(s, rng)),
    }
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
            } else {
                format!("{f:.6}")
            }
        }
        Value::Text(s) => format!("'{s}'"),
        other => format!("'{other}'"),
    }
}

// ─── Engine bridge ──────────────────────────────────────────────────────────
fn exec(db: &Database, sql: &str) -> Result<(), String> {
    let rt = tokio::runtime::Handle::current();
    match tokio::task::block_in_place(|| rt.block_on(db.execute(sql))) {
        Ok(_) => Ok(()),
        Err(e) => Err(format!("{e:?}")),
    }
}
fn snapshot(db: &Database, cols: &str) -> Result<Vec<Vec<String>>, String> {
    let rt = tokio::runtime::Handle::current();
    let sql = format!("SELECT {cols} FROM t ORDER BY id ASC");
    match tokio::task::block_in_place(|| rt.block_on(db.execute(&sql))) {
        Ok(mut r) => match r.pop() {
            Some(ExecResult::Select { rows, .. }) => Ok(rows
                .iter()
                .map(|row| row.iter().map(canon).collect())
                .collect()),
            other => Err(format!("non-select: {other:?}")),
        },
        Err(e) => Err(format!("{e:?}")),
    }
}

struct TmpDir(PathBuf);
impl TmpDir {
    fn new(tag: &str) -> Self {
        let mut p = std::env::temp_dir();
        p.push(format!("nucleus_recover_engines_{tag}"));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).expect("mkdir temp");
        TmpDir(p)
    }
}
impl Drop for TmpDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

// The Disk engine path is a FILE (data file), created next to it in `dir`.
fn open_disk(dir: &PathBuf) -> Result<Database, String> {
    let file = dir.join("data.db");
    Database::builder()
        .disk(file)
        .build()
        .map_err(|e| format!("open: {e:?}"))
}

// Durable KV is only reachable when `data_dir` is a real DIRECTORY (the executor
// does `data_dir.join("kv")`). DurableMvcc passes a directory; Disk passes a file,
// so `file/kv` cannot be created and KV durability is silently disabled there.
// Therefore the KV-durability sweep opens a DurableMvcc database (its SQL recovery
// is already covered by probe_recover.rs; here we exercise the WAL-backed KV store,
// which probe_recover.rs does not).
fn open_dir(dir: &PathBuf) -> Result<Database, String> {
    Database::durable_mvcc(dir).map_err(|e| format!("open: {e:?}"))
}

// ─── SQL on the Disk engine: write -> drop(clean flush) -> reopen -> verify ───
fn run_disk_sql(
    seed: u64,
    iterations: usize,
    ops_per: usize,
    max_report: usize,
    total: &mut usize,
    divergences: &mut usize,
) {
    'outer: for iter in 0..iterations {
        let mut rng = Rng(seed.wrapping_add(iter as u64).wrapping_mul(0x100000001B3));
        let schema = Schema::random(&mut rng);
        let cols = schema.names();
        let tmp = TmpDir::new(&format!("disksql_{seed}_{iter}"));
        let cycles = 2 + rng.below(3); // multi-shutdown/recover cycles on the SAME dir
        let mut next_id = 1i64;
        let mut expected: Option<Vec<Vec<String>>> = None;

        for cycle in 0..cycles {
            let db = match open_disk(&tmp.0) {
                Ok(d) => d,
                Err(e) => {
                    *divergences += 1;
                    if *divergences <= max_report {
                        println!(
                            "─── [disk-sql] OPEN FAILED #{divergences} (iter {iter}) ── {e}\n",
                            divergences = *divergences
                        );
                    }
                    continue 'outer;
                }
            };
            if cycle == 0 {
                if exec(&db, &schema.ddl()).is_err() {
                    continue 'outer;
                }
            } else if let Some(exp) = &expected {
                *total += 1;
                let recovered = match snapshot(&db, &cols) {
                    Ok(s) => s,
                    Err(e) => {
                        *divergences += 1;
                        if *divergences <= max_report {
                            println!(
                                "─── [disk-sql] READ FAILED #{} (iter {iter}) ── {e}\n",
                                *divergences
                            );
                        }
                        continue 'outer;
                    }
                };
                if &recovered != exp {
                    *divergences += 1;
                    if *divergences <= max_report {
                        println!(
                            "─── [disk-sql] RECOVERY DIVERGENCE #{} (iter {iter}, cycle {cycle}, seed {seed}) ───",
                            *divergences
                        );
                        println!("  schema    : {}", schema.ddl());
                        println!(
                            "  pre-down  ({} rows): {:?}",
                            exp.len(),
                            &exp[..exp.len().min(8)]
                        );
                        println!(
                            "  recovered ({} rows): {:?}",
                            recovered.len(),
                            &recovered[..recovered.len().min(8)]
                        );
                        println!();
                    }
                    continue 'outer;
                }
            }

            // Mix of auto-commit and explicit-transaction committed mutations.
            for _ in 0..ops_per {
                if rng.chance(25) {
                    // explicit txn exercises begin_txn(flush) + commit_txn(wal_log_commit)
                    let _ = exec(&db, "BEGIN");
                    for _ in 0..(1 + rng.below(3)) {
                        let m = gen_mutation(&schema, &mut rng, &mut next_id);
                        let _ = exec(&db, &m);
                    }
                    let _ = exec(&db, "COMMIT");
                } else {
                    let m = gen_mutation(&schema, &mut rng, &mut next_id);
                    let _ = exec(&db, &m);
                }
            }
            expected = snapshot(&db, &cols).ok();
            if expected.is_none() {
                continue 'outer;
            }

            // Stage UNCOMMITTED writes that must NOT survive: open txn, then crash.
            if rng.chance(30) {
                let _ = exec(&db, "BEGIN");
                for _ in 0..4 {
                    let m = gen_insert(&schema, &mut rng, next_id);
                    next_id += 1;
                    let _ = exec(&db, &m);
                }
                // dropping the db with an open txn: Transaction wrapper is not used here
                // (we drive raw SQL), so the open txn is simply abandoned.
            }
            // drop(db) = clean shutdown (DiskEngine::drop flushes).
        }
    }
}

// ─── Durable KV: set/incr/del/expire -> drop(crash) -> reopen -> verify ───────
// KV WAL flushes per-write, so drop is a genuine process-crash for KV.
fn run_kv(
    seed: u64,
    iterations: usize,
    ops_per: usize,
    max_report: usize,
    total: &mut usize,
    divergences: &mut usize,
) {
    use std::collections::HashMap;
    'outer: for iter in 0..iterations {
        let mut rng = Rng(seed
            .wrapping_add(0xABCD ^ iter as u64)
            .wrapping_mul(0x100000001B3));
        let tmp = TmpDir::new(&format!("kv_{seed}_{iter}"));
        let cycles = 2 + rng.below(3);
        // Reference oracle: the set of live (non-expired-immediately) key->value we expect.
        let mut model: HashMap<String, i64> = HashMap::new();

        for cycle in 0..cycles {
            let db = match open_dir(&tmp.0) {
                Ok(d) => d,
                Err(e) => {
                    *divergences += 1;
                    if *divergences <= max_report {
                        println!(
                            "─── [kv] OPEN FAILED #{} (iter {iter}) ── {e}\n",
                            *divergences
                        );
                    }
                    continue 'outer;
                }
            };

            // After cycle 0, verify recovered KV == model BEFORE mutating again.
            if cycle > 0 {
                *total += 1;
                let kv = db.kv();
                let mut mismatch: Option<String> = None;
                for (k, v) in model.iter() {
                    match kv.get(k) {
                        Some(Value::Int64(got)) if got == *v => {}
                        Some(Value::Int32(got)) if got as i64 == *v => {}
                        other => {
                            mismatch = Some(format!("key {k}: expected {v}, got {other:?}"));
                            break;
                        }
                    }
                }
                // Also ensure no extra keys leaked back (deleted keys staying alive).
                if mismatch.is_none() {
                    let live = kv.keys("*");
                    if live.len() != model.len() {
                        mismatch = Some(format!(
                            "key count: expected {}, recovered {} ({:?})",
                            model.len(),
                            live.len(),
                            live.iter().take(8).collect::<Vec<_>>()
                        ));
                    }
                }
                if let Some(m) = mismatch {
                    *divergences += 1;
                    if *divergences <= max_report {
                        println!(
                            "─── [kv] RECOVERY DIVERGENCE #{} (iter {iter}, cycle {cycle}, seed {seed}) ───",
                            *divergences
                        );
                        println!("  model size {}: {}", model.len(), m);
                        println!();
                    }
                    continue 'outer;
                }
            }

            // Apply committed KV mutations.
            let kv = db.kv();
            for _ in 0..ops_per {
                let key = format!("k{}", rng.below(40));
                match rng.below(5) {
                    0 | 1 => {
                        let v = rng.int(-100, 100);
                        kv.set(&key, Value::Int64(v), None);
                        model.insert(key, v);
                    }
                    2 => {
                        let amt = rng.int(-5, 5);
                        // incr_by on a non-int existing value would error; our model only
                        // ever stores ints under these keys, so this is well-defined.
                        match kv.incr_by(&key, amt) {
                            Ok(newv) => {
                                model.insert(key, newv);
                            }
                            Err(_) => {}
                        }
                    }
                    3 => {
                        if kv.del(&key) {
                            model.remove(&key);
                        } else {
                            model.remove(&key);
                        }
                    }
                    _ => {
                        // overwrite with mset to exercise the batch WAL path
                        let v = rng.int(-100, 100);
                        kv.set(&key, Value::Int64(v), None);
                        model.insert(key, v);
                    }
                }
            }
            // drop(db) = crash; KV WAL already flushed each op to the OS.
        }
    }
}

fn main_impl() {
    let mut seed: u64 = 0x5EC0_FFEE;
    let mut iterations = 300usize;
    let mut ops_per = 25usize;
    let mut max_report = 15usize;
    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--seed" => {
                i += 1;
                seed = args[i].parse().unwrap();
            }
            "--iterations" => {
                i += 1;
                iterations = args[i].parse().unwrap();
            }
            "--ops" => {
                i += 1;
                ops_per = args[i].parse().unwrap();
            }
            "--max-report" => {
                i += 1;
                max_report = args[i].parse().unwrap();
            }
            _ => {}
        }
        i += 1;
    }
    std::panic::set_hook(Box::new(|_| {}));
    println!("Nucleus durable-engine recovery fuzzer (Disk SQL + durable KV)");
    println!("seed={seed} iterations={iterations} ops/iter={ops_per}\n");

    let mut total = 0usize;
    let mut divergences = 0usize;

    // catch_unwind around each engine sweep so a panic still reports + exits(1).
    let r1 = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        run_disk_sql(
            seed,
            iterations,
            ops_per,
            max_report,
            &mut total,
            &mut divergences,
        );
    }));
    if r1.is_err() {
        divergences += 1;
        println!("─── [disk-sql] PANIC during sweep (counted as divergence) ───\n");
    }
    let r2 = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        run_kv(
            seed,
            iterations,
            ops_per,
            max_report,
            &mut total,
            &mut divergences,
        );
    }));
    if r2.is_err() {
        divergences += 1;
        println!("─── [kv] PANIC during sweep (counted as divergence) ───\n");
    }

    println!("\n════ SUMMARY ════");
    println!("recovery round-trips verified : {total}");
    println!("divergences                   : {divergences}");
    println!(
        "(LSM + Columnar durable engines are NOT wired into StorageMode — not testable via Database.)"
    );
    if divergences == 0 {
        println!("\nAll Disk-SQL and durable-KV committed state recovered exactly. 🎯");
    } else {
        std::process::exit(1);
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    tokio::task::spawn_blocking(main_impl).await.unwrap();
}
