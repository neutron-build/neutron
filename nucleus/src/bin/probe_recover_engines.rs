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
//! ── S35 sections (2026-08-17): durable-log honesty and corrupt-state refusal ──
//!
//! Three further sections, one per confirmed bug class from the Phase C fix
//! campaign, each with the negative-control discipline `probe_streams_oracle`
//! established:
//!
//!   * `datalog` (class A / NU-013): random DATALOG_ASSERT / RETRACT / CLEAR /
//!     RULE through the SQL surface over three predicates, then drop -> reopen
//!     and compare the FULL fact set per predicate (and rule-derived closure)
//!     against the model. A mutation acknowledged by the statement must be
//!     reflected after restart; a fact that silently vanished is the exact
//!     shape NU-013 shipped when the WAL was opened and never written.
//!   * `vector` (class A / NU-048): an HNSW-indexed VECTOR table under random
//!     INSERT / DELETE / checkpoint cycles, reopened and compared through
//!     `hnsw_index_live_ids` — the index itself, not a SQL KNN query, which
//!     falls back to a base-table scan and would mask an index that lost or
//!     resurrected vectors.
//!   * `catalog` (class C / NU-163 + NU-165): corrupt `sequences.json` /
//!     `catalog.json` / `meta.json` must be REFUSED — poisoned sequence
//!     surface, failed open, refused write-back — never treated as empty
//!     state that the next write then persists over the original.
//!
//! `--negative-control <datalog|vector|catalog>` runs the three sections twice
//! at one seed, clean then with that section's model perturbed the way the
//! original bug perturbed the engine state (a dropped fact, a resurrected
//! vector, an expected silent-reset). It passes only if the perturbation adds
//! divergences to that section and none to the other two — a comparison nobody
//! has watched fail is not a comparison.
//!
//! Build/run: `cargo run --release --features "server rusqlite" --bin probe_recover_engines`
//!   (rusqlite is unused here but harmless; `--features server` also works.)
//!   `... --bin probe_recover_engines -- --engine buffered-disk`
//!   `... --bin probe_recover_engines -- --negative-control datalog`
#![cfg(feature = "server")]
#![allow(unused)]
#![allow(clippy::all)] // internal fuzz harness

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use nucleus::embedded::Database;
use nucleus::executor::ExecResult;
use nucleus::metrics::harness::{EngineConfig, EngineKind, HarnessDb};
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

// ═════════════════════════════════════════════════════════════════════════════
// S35 sections — durable-log honesty (class A) and corrupt-state refusal (C)
// ═════════════════════════════════════════════════════════════════════════════

/// Per-section divergence counts for the S35 sections, so a negative control
/// can require "gained here, unchanged elsewhere".
#[derive(Default)]
struct Sections {
    counts: BTreeMap<&'static str, usize>,
    findings: Vec<(&'static str, String)>,
}

impl Sections {
    fn push(&mut self, section: &'static str, detail: String) {
        *self.counts.entry(section).or_insert(0) += 1;
        if self.findings.len() < 40 {
            self.findings.push((section, detail));
        }
    }
    fn count(&self, section: &str) -> usize {
        self.counts.get(section).copied().unwrap_or(0)
    }
    fn total(&self) -> usize {
        self.counts.values().sum()
    }
}

/// `HarnessDb::open` is async; the sections run inside `spawn_blocking`.
fn open_harness(kind: EngineKind, dir: &std::path::Path) -> Result<HarnessDb, String> {
    let rt = tokio::runtime::Handle::current();
    tokio::task::block_in_place(|| rt.block_on(HarnessDb::open(kind, dir, EngineConfig::default())))
        .map_err(|e| format!("open: {e:?}"))
}

fn harness_exec(db: &HarnessDb, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let rt = tokio::runtime::Handle::current();
    let res = tokio::task::block_in_place(|| rt.block_on(db.executor().execute(sql)));
    let mut results = res.map_err(|e| format!("{e:?}"))?;
    match results.pop() {
        Some(ExecResult::Select { rows, .. }) => Ok(rows
            .iter()
            .map(|r| r.iter().map(|v| v.to_string()).collect())
            .collect()),
        Some(ExecResult::Command { rows_affected, .. }) => {
            Ok(vec![vec![rows_affected.to_string()]])
        }
        other => Err(format!("unexpected result shape: {other:?}")),
    }
}

fn db_exec(db: &Database, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let rt = tokio::runtime::Handle::current();
    let mut results = tokio::task::block_in_place(|| rt.block_on(db.execute(sql)))
        .map_err(|e| format!("{e:?}"))?;
    match results.pop() {
        Some(ExecResult::Select { rows, .. }) => Ok(rows
            .iter()
            .map(|r| r.iter().map(|v| v.to_string()).collect())
            .collect()),
        Some(ExecResult::Command { rows_affected, .. }) => {
            Ok(vec![vec![rows_affected.to_string()]])
        }
        other => Err(format!("unexpected result shape: {other:?}")),
    }
}

/// Parse the `[["v1","v2"], ...]` JSON text DATALOG_QUERY emits into a set of
/// tuples — with serde, because a hand-rolled bracket scanner is how the
/// first version of this section mis-parsed nested arrays into one bogus
/// tuple and reported six divergences the engine had not caused.
fn parse_fact_tuples(s: &str) -> BTreeSet<Vec<String>> {
    if let Ok(parsed) = serde_json::from_str::<Vec<Vec<String>>>(s.trim()) {
        return parsed.into_iter().collect();
    }
    // Fallback: strip a surrounding Value-Display wrapper and retry.
    let trimmed = s
        .trim()
        .trim_start_matches("Text(")
        .trim_end_matches(')')
        .trim()
        .trim_matches('"');
    serde_json::from_str::<Vec<Vec<String>>>(trimmed)
        .unwrap_or_default()
        .into_iter()
        .collect()
}

/// Naive transitive closure for the rule-derived predicate, so a rule that
/// stopped deriving after a restart is visible, not just missing base facts.
fn closure(edges: &BTreeSet<(String, String)>) -> BTreeSet<(String, String)> {
    let mut path: BTreeSet<(String, String)> = edges.clone();
    loop {
        let mut new_pairs = Vec::new();
        for (ex, ey) in edges.iter() {
            for (px, py) in path.iter() {
                if px == ey {
                    new_pairs.push((ex.clone(), py.clone()));
                }
            }
        }
        let before = path.len();
        for pair in new_pairs {
            path.insert(pair);
        }
        if path.len() == before {
            return path;
        }
    }
}

const DL_PREDS: &[&str] = &["edge", "link", "tag"];
const DL_NODES: &[&str] = &["a", "b", "c", "d"];

/// Class A / NU-013: every acknowledged Datalog mutation survives a reopen,
/// per predicate and for rule-derived facts.
fn run_datalog(
    seed: u64,
    iterations: usize,
    ops_per: usize,
    kind: EngineKind,
    perturb: bool,
    sec: &mut Sections,
) {
    'outer: for iter in 0..iterations {
        let mut rng = Rng(seed.wrapping_add(iter as u64).wrapping_mul(0xD10_10C));
        let tmp = TmpDir::new(&format!("datalog_{seed}_{iter}"));
        let cycles = 2 + rng.below(3);
        // Model: predicate -> set of (x, y) facts. Rules are installed once,
        // deterministically, on cycle 0.
        let mut model: BTreeMap<&str, BTreeSet<(String, String)>> = BTreeMap::new();
        for p in DL_PREDS {
            model.insert(p, BTreeSet::new());
        }

        for cycle in 0..cycles {
            let db = match open_harness(kind, tmp.0.as_path()) {
                Ok(d) => d,
                Err(e) => {
                    sec.push("datalog", format!("iter {iter}: OPEN FAILED: {e}"));
                    continue 'outer;
                }
            };
            if cycle == 0 {
                for sql in [
                    "SELECT DATALOG_RULE('path(X,Y) :- edge(X,Y)')",
                    "SELECT DATALOG_RULE('path(X,Z) :- edge(X,Y), path(Y,Z)')",
                ] {
                    if let Err(e) = harness_exec(&db, sql) {
                        sec.push(
                            "datalog",
                            format!("iter {iter}: rule registration failed: {sql}: {e}"),
                        );
                        continue 'outer;
                    }
                }
            } else {
                // The reopen comparison: full fact set per predicate, plus the
                // rule-derived closure. A wrong count alone would cancel a
                // vanish against a resurrect.
                let mut expected: BTreeMap<&str, BTreeSet<Vec<String>>> = model
                    .iter()
                    .map(|(p, facts)| {
                        (
                            *p,
                            facts
                                .iter()
                                .map(|(x, y)| vec![x.clone(), y.clone()])
                                .collect(),
                        )
                    })
                    .collect();
                if perturb {
                    // NU-013's shape: one acknowledged fact silently absent
                    // after restart. Deterministic: smallest fact of `edge`.
                    if let Some(facts) = model.get("edge")
                        && let Some(victim) = facts.iter().next().cloned()
                    {
                        let set = expected.get_mut("edge").unwrap();
                        set.remove(&vec![victim.0.clone(), victim.1.clone()]);
                    }
                }
                for p in DL_PREDS {
                    let sql = format!("SELECT DATALOG_QUERY('{p}(X,Y)')");
                    match harness_exec(&db, &sql) {
                        Ok(rows) => {
                            let got: BTreeSet<Vec<String>> = rows
                                .iter()
                                .flat_map(|r| parse_fact_tuples(&r.join("")))
                                .collect();
                            let want = expected.get(p).unwrap();
                            if &got != want {
                                sec.push(
                                    "datalog",
                                    format!(
                                        "iter {iter} cycle {cycle}: predicate {p} recovered \
                                         {} facts {got:?}, model has {} {want:?}",
                                        got.len(),
                                        want.len()
                                    ),
                                );
                                continue 'outer;
                            }
                        }
                        Err(e) => {
                            sec.push(
                                "datalog",
                                format!(
                                    "iter {iter} cycle {cycle}: DATALOG_QUERY({p}) failed: {e}"
                                ),
                            );
                            continue 'outer;
                        }
                    }
                }
                // Rule-derived: path = closure(edge).
                match harness_exec(&db, "SELECT DATALOG_QUERY('path(X,Y)')") {
                    Ok(rows) => {
                        let got: BTreeSet<Vec<String>> = rows
                            .iter()
                            .flat_map(|r| parse_fact_tuples(&r.join("")))
                            .collect();
                        let want: BTreeSet<Vec<String>> = closure(model.get("edge").unwrap())
                            .iter()
                            .map(|(x, y)| vec![x.clone(), y.clone()])
                            .collect();
                        if got != want {
                            sec.push(
                                "datalog",
                                format!(
                                    "iter {iter} cycle {cycle}: rule-derived path recovered {} \
                                     tuples, model derives {} — a rule or its derivation was \
                                     lost across restart",
                                    got.len(),
                                    want.len()
                                ),
                            );
                            continue 'outer;
                        }
                    }
                    Err(e) => {
                        sec.push(
                            "datalog",
                            format!("iter {iter} cycle {cycle}: DATALOG_QUERY(path) failed: {e}"),
                        );
                        continue 'outer;
                    }
                }
            }

            // Apply acknowledged mutations.
            for _ in 0..ops_per {
                let x = DL_NODES[rng.below(DL_NODES.len())].to_string();
                let y = DL_NODES[rng.below(DL_NODES.len())].to_string();
                let p = DL_PREDS[rng.below(DL_PREDS.len())];
                match rng.below(10) {
                    0..=5 => {
                        let sql = format!("SELECT DATALOG_ASSERT('{p}({x},{y})')");
                        if let Err(e) = harness_exec(&db, &sql) {
                            sec.push("datalog", format!("iter {iter}: {sql} failed: {e}"));
                            continue 'outer;
                        }
                        model.get_mut(p).unwrap().insert((x, y));
                    }
                    6..=7 => {
                        let sql = format!("SELECT DATALOG_RETRACT('{p}({x},{y})')");
                        if let Err(e) = harness_exec(&db, &sql) {
                            sec.push("datalog", format!("iter {iter}: {sql} failed: {e}"));
                            continue 'outer;
                        }
                        model.get_mut(p).unwrap().remove(&(x, y));
                    }
                    _ => {
                        let sql = format!("SELECT DATALOG_CLEAR('{p}')");
                        if let Err(e) = harness_exec(&db, &sql) {
                            sec.push("datalog", format!("iter {iter}: {sql} failed: {e}"));
                            continue 'outer;
                        }
                        model.get_mut(p).unwrap().clear();
                    }
                }
            }
        }
    }
}

/// Class A / NU-048: an HNSW index recovers from its WAL with exactly the
/// live vectors the statements acknowledged — none lost, none resurrected.
/// Compared through `hnsw_index_live_ids` because a SQL KNN query falls back
/// to a base-table scan and would mask both.
///
/// `control_shape` predates the two fixes that made the honest shape clean
/// (serialized HNSW tombstones, F1a; the persisted PK registry, F1b) and is
/// kept for the negative control's clean-baseline arm: it uses the
/// insert-only, one-reopen, no-checkpoints shape. The honest shape runs in
/// normal gating.
/// GDL-3 fixed-case section: quoted datalog constants must survive a
/// checkpoint → reopen → replay cycle exactly. The checkpoint REGENERATES
/// Datalog text from parsed args, so any argument that is not a bare
/// lowercase atom or number was either silently DROPPED by replay's
/// `if let Ok` parse or re-parsed with a different arity (silently wrong
/// data). Deterministic — the section's domain is fixed cases, no rng.
///
/// The checkpoint is forced the way production forces it: a datalog-touching
/// ROLLBACK rewrites the log to the restored state (cross-model compensation).
fn run_datalog_quoting(kind: EngineKind, perturb: bool, sec: &mut Sections) {
    let tmp = TmpDir::new("datalog_quoting");
    // (query, expected tuples as (pred, args) pairs)
    let cases: &[(&str, &str, Vec<Vec<String>>)] = &[
        (
            "city(N, P)",
            "city",
            vec![vec!["New York".into(), "8.4".into()]],
        ),
        ("tag(X)", "tag", vec![vec!["a, b".into()]]),
        ("note(X)", "note", vec![vec!["it's here".into()]]),
        ("ny(P)", "ny", vec![vec!["8.4".into()]]),
    ];

    let db = match open_harness(kind, tmp.0.as_path()) {
        Ok(d) => d,
        Err(e) => {
            sec.push("datalog-quoting", format!("OPEN FAILED: {e}"));
            return;
        }
    };
    for sql in [
        "SELECT DATALOG_ASSERT('city(\"New York\", 8.4)')",
        "SELECT DATALOG_ASSERT('tag(\"a, b\")')",
        "SELECT DATALOG_ASSERT('note(\"it''s here\")')",
        "SELECT DATALOG_RULE('ny(P) :- city(\"New York\", P)')",
        // Force the checkpoint: BEGIN + a datalog touch + ROLLBACK rewrites
        // the WAL to the (still-quoted) restored state.
        "BEGIN",
        "SELECT DATALOG_ASSERT('scratch(x)')",
        "ROLLBACK",
    ] {
        if let Err(e) = harness_exec(&db, sql) {
            sec.push("datalog-quoting", format!("setup failed ({sql}): {e}"));
            return;
        }
    }
    drop(db);

    // Reopen: replay must reproduce every quoted constant exactly.
    let db = match open_harness(kind, tmp.0.as_path()) {
        Ok(d) => d,
        Err(e) => {
            sec.push("datalog-quoting", format!("REOPEN FAILED: {e}"));
            return;
        }
    };
    for (q, pred, want) in cases {
        // Negative control: model the pre-fix corruption — every quoted
        // fact/rule was dropped or arity-garbled on replay, i.e. absent.
        let want = if perturb { Vec::new() } else { want.clone() };
        let sql = format!("SELECT DATALOG_QUERY('{q}')");
        match harness_exec(&db, &sql) {
            Ok(rows) => {
                let got: BTreeSet<Vec<String>> = rows
                    .iter()
                    .flat_map(|r| parse_fact_tuples(&r.join("")))
                    .collect();
                let want: BTreeSet<Vec<String>> = want.into_iter().collect();
                if got != want {
                    sec.push(
                        "datalog-quoting",
                        format!(
                            "predicate {pred} recovered {got:?} after checkpoint+reopen, \
                             model has {want:?} — a quoted constant was dropped or \
                             re-parsed with a different arity"
                        ),
                    );
                }
            }
            Err(e) => {
                sec.push("datalog-quoting", format!("DATALOG_QUERY({q}) failed: {e}"));
            }
        }
    }
}

fn run_vector(
    seed: u64,
    iterations: usize,
    ops_per: usize,
    kind: EngineKind,
    perturb: bool,
    control_shape: bool,
    sec: &mut Sections,
) {
    'outer: for iter in 0..iterations {
        let mut rng = Rng(seed.wrapping_add(0xACD ^ iter as u64).wrapping_mul(0x5EED));
        let tmp = TmpDir::new(&format!("vector_{seed}_{iter}"));
        // Control shape: exactly one reopen, and only inserts before it.
        let cycles = if control_shape { 2 } else { 2 + rng.below(3) };
        let mut model: BTreeSet<i64> = BTreeSet::new();
        let mut next_fresh: i64 = 1;

        for cycle in 0..cycles {
            let db = match open_harness(kind, tmp.0.as_path()) {
                Ok(d) => d,
                Err(e) => {
                    sec.push("vector", format!("iter {iter}: OPEN FAILED: {e}"));
                    continue 'outer;
                }
            };
            if cycle == 0 {
                for sql in [
                    "CREATE TABLE vp (id INT PRIMARY KEY, x VECTOR(4))",
                    "CREATE INDEX vidx ON vp USING HNSW (x)",
                ] {
                    if let Err(e) = harness_exec(&db, sql) {
                        sec.push("vector", format!("iter {iter}: DDL failed ({sql}): {e}"));
                        continue 'outer;
                    }
                }
            } else {
                // Reopen comparison. The recovered live set is compared by
                // COUNT, not by id: PK-keyed HNSW logs its internal monotonic
                // node ids to the WAL, and the node→PK registry is persisted
                // now (F1b) but a full rebuild renumbers the space, so ids are
                // not comparable across a restart that rebuilt. The count is
                // the stable observable — it catches the NU-048 shapes in
                // aggregate: acknowledged inserts lost shrink it, deleted
                // vectors resurrecting grow it.
                let mut want = model.len();
                if perturb && want > 0 {
                    // Model of the bug: one acknowledged insert silently
                    // vanishes. Any fixed engine diverges from that.
                    want -= 1;
                }
                match db.executor().hnsw_index_live_ids("vidx") {
                    Some(live) => {
                        let got = live.len();
                        if got != want {
                            // Record and continue with this cycle's ops rather
                            // than skipping the iteration: the negative
                            // control needs per-cycle counts to prove a −1
                            // shift in the model ADDS a divergence even while
                            // the engine itself is red in this section.
                            sec.push(
                                "vector",
                                format!(
                                    "iter {iter} cycle {cycle}: recovered HNSW index holds {got} \
                                     live vectors, {want} were acknowledged ({} vs {want}) — \
                                     inserts lost or deletes resurrected across restart (NU-048)",
                                    got as i64 - want as i64
                                ),
                            );
                        }
                    }
                    None => {
                        sec.push(
                            "vector",
                            format!(
                                "iter {iter} cycle {cycle}: HNSW index vidx did not survive reopen"
                            ),
                        );
                        continue 'outer;
                    }
                }
            }

            for _ in 0..ops_per {
                // Control shape: inserts only — the faithful subset.
                let op = if control_shape { 0 } else { rng.below(10) };
                match op {
                    0..=5 => {
                        // Insert a fresh id (duplicate-PK semantics differ per
                        // engine kind, so only ever insert ids the model does
                        // not hold).
                        let id = next_fresh;
                        next_fresh += 1;
                        let dims: Vec<String> =
                            (0..4).map(|_| format!("{}", rng.below(9))).collect();
                        let sql = format!(
                            "INSERT INTO vp (id, x) VALUES ({id}, VECTOR('[{}]'))",
                            dims.join(",")
                        );
                        if let Err(e) = harness_exec(&db, &sql) {
                            sec.push("vector", format!("iter {iter}: {sql} failed: {e}"));
                            continue 'outer;
                        }
                        model.insert(id);
                    }
                    6..=7 => {
                        if model.is_empty() {
                            continue;
                        }
                        let live: Vec<i64> = model.iter().copied().collect();
                        let id = live[rng.below(live.len())];
                        let sql = format!("DELETE FROM vp WHERE id = {id}");
                        if let Err(e) = harness_exec(&db, &sql) {
                            sec.push("vector", format!("iter {iter}: {sql} failed: {e}"));
                            continue 'outer;
                        }
                        model.remove(&id);
                    }
                    _ => {
                        // The server checkpoints the vector WAL on a recurring
                        // task; a probe that never checkpoints would not cover
                        // snapshot+delta recovery.
                        if control_shape {
                            continue;
                        }
                        if let Err(e) = db.executor().checkpoint_vector_wal() {
                            sec.push(
                                "vector",
                                format!("iter {iter}: checkpoint_vector_wal failed: {e}"),
                            );
                            continue 'outer;
                        }
                    }
                }
            }
        }
    }
}

/// Class C / NU-163 + NU-165: persisted catalog state that cannot be read
/// must be refused — never treated as empty state that the next write then
/// persists over the original.
fn run_catalog_refusal(
    seed: u64,
    iterations: usize,
    kind: EngineKind,
    perturb: bool,
    sec: &mut Sections,
) {
    for iter in 0..iterations {
        let tag = format!("catalog_{seed}_{iter}");
        let tmp = TmpDir::new(&tag);

        // ── sequences.json: corrupt bytes poison NEXTVAL (NU-165) ──
        {
            let dir = tmp.0.join("seq");
            let _ = std::fs::create_dir_all(&dir);
            let db = match Database::durable_mvcc(&dir) {
                Ok(d) => d,
                Err(e) => {
                    sec.push("catalog", format!("seq arm: open failed: {e:?}"));
                    continue;
                }
            };
            if let Err(e) = db_exec(&db, "CREATE SEQUENCE sq") {
                sec.push("catalog", format!("seq arm: CREATE SEQUENCE failed: {e}"));
                continue;
            }
            for _ in 0..3 {
                if let Err(e) = db_exec(&db, "SELECT NEXTVAL('sq')") {
                    sec.push(
                        "catalog",
                        format!("seq arm: NEXTVAL failed pre-corruption: {e}"),
                    );
                    continue;
                }
            }
            drop(db);
            let seq_path = dir.join("sequences.json");
            for (variant, bytes) in [
                ("truncated", &b"{\"name\": \"sq\""[..]),
                ("garbage", b"\x00\x01\x02not json at all"),
                ("wrong-shape", b"{\"name\":\"sq\"}"),
            ] {
                let _ = std::fs::write(&seq_path, bytes);
                let db = match Database::durable_mvcc(&dir) {
                    Ok(d) => d,
                    Err(e) => {
                        if perturb {
                            // The old bug: reopen succeeded, NEXTVAL silently
                            // restarted from the catalog default.
                            continue;
                        }
                        sec.push(
                            "catalog",
                            format!("seq arm ({variant}): corrupt sequences.json refused the entire open: {e:?}"),
                        );
                        continue;
                    }
                };
                let next = db_exec(&db, "SELECT NEXTVAL('sq')");
                if perturb {
                    // Model of the bug: expect silent reset to 1. The fixed
                    // engine errors, which is the divergence the control needs.
                    if !matches!(&next, Ok(rows) if rows.first().map(|r| r.first().cloned()) == Some(Some("1".to_string())))
                    {
                        sec.push(
                            "catalog",
                            format!("seq arm ({variant}): NEXTVAL did not silently reset to 1 (perturbed model expects the NU-165 bug)"),
                        );
                    }
                } else {
                    match next {
                        Ok(rows) => {
                            sec.push(
                                "catalog",
                                format!(
                                    "seq arm ({variant}): NEXTVAL returned {rows:?} against corrupt \
                                     sequences.json instead of refusing — values already handed \
                                     out can be reissued (NU-165)"
                                ),
                            );
                        }
                        Err(_) => {} // refused: correct
                    }
                }
            }
            // Boundary: a VALID empty array is a legitimate empty sequence
            // file, not corruption — the surface must not be poisoned by it.
            let _ = std::fs::write(&seq_path, b"[]");
            let db = match Database::durable_mvcc(&dir) {
                Ok(d) => d,
                Err(e) => {
                    if !perturb {
                        sec.push(
                            "catalog",
                            format!("seq arm (valid-empty): open refused: {e:?}"),
                        );
                    }
                    continue;
                }
            };
            if !perturb {
                if let Err(e) = db_exec(&db, "SELECT NEXTVAL('sq')") {
                    let msg = format!("{e:?}");
                    if msg.contains("does not exist") {
                        // The sequence DEFINITION lives in meta.json, and the
                        // embedded builder never loads meta.json — a separate
                        // live defect (see the meta arm), not sequence
                        // poisoning. Attribute it where it belongs.
                        sec.push(
                            "catalog",
                            "seq arm (valid-empty): the sequence DEFINITION is gone after an \
                             embedded reopen — meta.json is not loaded by the embedded builder, \
                             so sequences (views/triggers/roles/RLS with it) silently vanish"
                                .to_string(),
                        );
                    } else {
                        sec.push(
                            "catalog",
                            format!("seq arm (valid-empty): NEXTVAL refused a valid empty sequences.json: {e}"),
                        );
                    }
                }
            }
        }

        // ── catalog.json: corruption refuses the open (NU-163's sibling gate) ──
        {
            let dir = tmp.0.join("cat");
            let _ = std::fs::create_dir_all(&dir);
            let db = match Database::durable_mvcc(&dir) {
                Ok(d) => d,
                Err(e) => {
                    sec.push("catalog", format!("cat arm: open failed: {e:?}"));
                    continue;
                }
            };
            if let Err(e) = db_exec(&db, "CREATE TABLE keepme (id INT PRIMARY KEY)") {
                sec.push("catalog", format!("cat arm: DDL failed: {e}"));
                continue;
            }
            drop(db);
            let original = std::fs::read(dir.join("catalog.json")).unwrap_or_default();
            let _ = std::fs::write(dir.join("catalog.json"), b"definitely not json");
            match Database::durable_mvcc(&dir) {
                Ok(_) => {
                    if perturb {
                        // The old bug: corrupt catalog treated as empty, open
                        // succeeds. The fixed engine refuses.
                        sec.push("catalog", "cat arm: corrupt catalog.json refused the open (perturbed model expects the bug)".to_string());
                    } else {
                        sec.push(
                            "catalog",
                            "cat arm: corrupt catalog.json OPENED — the next write persists the \
                             emptied catalog over the original (NU-163)"
                                .to_string(),
                        );
                    }
                }
                Err(_) => {
                    let after = std::fs::read(dir.join("catalog.json")).unwrap_or_default();
                    if after != b"definitely not json" && !perturb {
                        sec.push(
                            "catalog",
                            "cat arm: corrupt catalog.json was rewritten during a refused open"
                                .to_string(),
                        );
                    }
                    let _ = original;
                }
            }
        }

        // ── meta.json: the policy catalog. Two openers, because there are two
        //    in the tree and they disagree: `main.rs` loads meta at startup
        //    (HarnessDb mirrors it), the embedded `Database` builder does not.
        {
            let dir = tmp.0.join("meta");
            let _ = std::fs::create_dir_all(&dir);
            // Seed through the server-shaped path (HarnessDb loads meta).
            let db = match open_harness(kind, &dir) {
                Ok(d) => d,
                Err(e) => {
                    sec.push("catalog", format!("meta arm: seed open failed: {e}"));
                    continue;
                }
            };
            for sql in [
                "CREATE TABLE mdocs (id INT, owner TEXT)",
                "CREATE ROLE meta_reader LOGIN PASSWORD 'x'",
                "CREATE POLICY mpol ON mdocs FOR SELECT USING (owner = CURRENT_USER)",
                "ALTER TABLE mdocs ENABLE ROW LEVEL SECURITY",
            ] {
                if let Err(e) = harness_exec(&db, sql) {
                    sec.push("catalog", format!("meta arm: seed DDL failed ({sql}): {e}"));
                    continue;
                }
            }
            drop(db);
            let meta_path = dir.join("meta.json");
            let corrupt = b"{ this is not valid json";
            let _ = std::fs::write(&meta_path, corrupt);

            // (a) Server-shaped reopen: DDL must be refused, file untouched.
            let db = match open_harness(kind, &dir) {
                Ok(d) => d,
                Err(e) => {
                    if !perturb {
                        sec.push(
                            "catalog",
                            format!("meta arm (server): open failed on corrupt meta.json: {e}"),
                        );
                    }
                    continue;
                }
            };
            let ddl = harness_exec(&db, "CREATE TABLE mdocs2 (id INT)");
            let bytes_now = std::fs::read(&meta_path).unwrap_or_default();
            if perturb {
                if ddl.is_ok() || bytes_now == corrupt.to_vec() {
                    // Model of the bug: DDL succeeded and/or the emptied
                    // catalog was written back.
                    if ddl.is_ok() {
                        sec.push("catalog", "meta arm (server): corrupt meta.json refused write-back (perturbed model expects the NU-163 bug)".to_string());
                    }
                }
            } else {
                match ddl {
                    Ok(_) => {
                        sec.push(
                            "catalog",
                            "meta arm (server): DDL succeeded against a corrupt meta.json — the \
                             emptied policy catalog can be persisted over the original (NU-163)"
                                .to_string(),
                        );
                    }
                    Err(_) => {
                        if bytes_now != corrupt.to_vec() {
                            sec.push(
                                "catalog",
                                "meta arm (server): meta.json was rewritten despite refusing the DDL".to_string(),
                            );
                        }
                    }
                }
            }
            drop(db);

            // (b) Embedded reopen (`Database::durable_mvcc`): the same policy
            // catalog, the same corrupt file — but a builder that never loads
            // meta.json at all. Expectation: still refused / not overwritten.
            let corrupt2 = b"{ this is not valid json";
            let _ = std::fs::write(&meta_path, corrupt2);
            let db = match Database::durable_mvcc(&dir) {
                Ok(d) => d,
                Err(e) => {
                    if !perturb {
                        sec.push(
                            "catalog",
                            format!("meta arm (embedded): open failed on corrupt meta.json: {e:?}"),
                        );
                    }
                    continue;
                }
            };
            let ddl = db_exec(&db, "CREATE TABLE mdocs3 (id INT)");
            let bytes_now = std::fs::read(&meta_path).unwrap_or_default();
            if !perturb {
                match ddl {
                    Ok(_) if bytes_now != corrupt2.to_vec() => {
                        sec.push(
                            "catalog",
                            "meta arm (embedded): DDL through the embedded API succeeded and \
                             REWROTE a corrupt meta.json it never read — the write-back NU-163 \
                             closed for the server, reachable through Database::durable_mvcc"
                                .to_string(),
                        );
                    }
                    Ok(_) => {
                        sec.push(
                            "catalog",
                            "meta arm (embedded): DDL succeeded against corrupt meta.json through the embedded API".to_string(),
                        );
                    }
                    Err(_) => {}
                }
            }
        }
    }
}

fn main_impl() {
    let mut seed: u64 = 0x5EC0_FFEE;
    let mut iterations = 300usize;
    let mut ops_per = 25usize;
    let mut max_report = 15usize;
    let mut engine: EngineKind = EngineKind::BufferedDisk;
    let mut negative: Option<String> = None;
    let mut skip: Vec<String> = Vec::new();
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
            // Repeatable. Exists so a section with a KNOWN live finding can be
            // held out of a gate run without deleting the section or muting the
            // finding — the skip is announced loudly below, because a probe that
            // quietly covers less than it claims is the failure this whole file
            // exists to prevent.
            "--skip-section" => {
                i += 1;
                let name = args[i].clone();
                if !matches!(
                    name.as_str(),
                    "datalog" | "datalog-quoting" | "vector" | "catalog"
                ) {
                    eprintln!(
                        "--skip-section takes one of: datalog, datalog-quoting, vector, catalog (got {name:?})"
                    );
                    std::process::exit(2);
                }
                skip.push(name);
            }
            "--engine" => {
                i += 1;
                match EngineKind::parse(&args[i]) {
                    Some(k) => engine = k,
                    None => {
                        eprintln!(
                            "unknown --engine {:?}; expected one of {:?}",
                            args[i],
                            EngineKind::ALL
                        );
                        std::process::exit(2);
                    }
                }
            }
            "--negative-control" => {
                i += 1;
                let section = args[i].clone();
                if !["datalog", "datalog-quoting", "vector", "catalog"].contains(&section.as_str())
                {
                    eprintln!(
                        "--negative-control takes one of: datalog, datalog-quoting, vector, catalog (got {section:?})"
                    );
                    std::process::exit(2);
                }
                negative = Some(section);
            }
            other => {
                eprintln!("unknown argument {other:?}");
                std::process::exit(2);
            }
        }
        i += 1;
    }
    std::panic::set_hook(Box::new(|_| {}));

    // ── Negative control: prove the S35 sections can discriminate ──
    if let Some(section) = &negative {
        println!(
            "NEGATIVE CONTROL: the {section} model is deliberately wrong; that section MUST report"
        );
        // One iteration is enough to prove a check fires, and keeps "the other
        // sections stay clean" a statement about this run, not an average.
        let vcs = section == "vector";
        let base = run_s35_sections(seed, 1, ops_per, engine, None, vcs, &[]);
        let pert = run_s35_sections(seed, 1, ops_per, engine, Some(section.as_str()), vcs, &[]);
        println!("\n════ SUMMARY (control, 1 iteration) ════");
        for s in ["datalog", "datalog-quoting", "vector", "catalog"] {
            println!(
                "{s:<9}: {} divergence(s)  (clean baseline: {})",
                pert.count(s),
                base.count(s)
            );
        }
        let gained = pert.count(section) as i64 - base.count(section) as i64;
        let spilled: i64 = ["datalog", "datalog-quoting", "vector", "catalog"]
            .iter()
            .filter(|s| **s != section.as_str())
            .map(|s| pert.count(s) as i64 - base.count(s) as i64)
            .sum();
        if gained > 0 && spilled == 0 {
            println!(
                "\nNEGATIVE CONTROL PASSED: perturbing the {section} model added {gained} \
                 divergence(s) to {section} and none to the other sections."
            );
            std::process::exit(0);
        }
        println!(
            "\nNEGATIVE CONTROL FAILED: perturbing the {section} model changed {section} by \
             {gained} and the other sections by {spilled}. A check that cannot fail is not a \
             check, and a check that fires for something else is worse."
        );
        std::process::exit(1);
    }

    println!("Nucleus durable-engine recovery fuzzer (Disk SQL + durable KV)");
    println!(
        "seed={seed} iterations={iterations} ops/iter={ops_per} s35-engine={}",
        engine.name()
    );
    println!();

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

    for name in &skip {
        println!(
            "─── [{name}] SECTION SKIPPED (--skip-section). This run does NOT cover it. \
             See nucleus/docs/PROBES.md and _internal/OPEN_WORK.md for the open finding."
        );
    }
    let mut sec = run_s35_sections(
        seed,
        iterations.min(40),
        ops_per.min(30),
        engine,
        None,
        false,
        &skip,
    );
    for (section, detail) in &sec.findings {
        println!("─── [{section}] {detail}");
    }
    if sec.total() > 0 {
        divergences += sec.total();
    }

    println!("\n════ SUMMARY ════");
    println!("recovery round-trips verified : {total}");
    println!("divergences                   : {divergences}");
    for s in ["datalog", "datalog-quoting", "vector", "catalog"] {
        if skip.iter().any(|k| k == s) {
            println!("s35/{s:<9}            : SKIPPED (not covered by this run)");
        } else {
            println!("s35/{s:<9}            : {} divergence(s)", sec.count(s));
        }
    }
    println!(
        "(LSM + Columnar durable engines are NOT wired into StorageMode — not testable via Database.)"
    );
    if divergences == 0 {
        println!("\nAll Disk-SQL, durable-KV and S35 section state recovered exactly.");
    } else {
        std::process::exit(1);
    }
}

/// The three S35 sections, once. `perturb` names the section whose model is
/// deliberately wrong (negative control), or `None` for the honest run.
///
/// `vector_control_shape` runs the vector section in its control shape
/// (insert-only, one reopen, no checkpoints) for BOTH arms of the vector
/// negative control: the checkpoint path and post-reopen incremental
/// maintenance currently diverge on their own (live findings the probe
/// reports in normal runs), and a control must compare like against like
/// against a clean baseline.
fn run_s35_sections(
    seed: u64,
    iterations: usize,
    ops_per: usize,
    engine: EngineKind,
    perturb: Option<&str>,
    vector_control_shape: bool,
    skip: &[String],
) -> Sections {
    let mut sec = Sections::default();
    let skipped = |name: &str| skip.iter().any(|s| s == name);
    if !skipped("datalog") {
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run_datalog(
                seed,
                iterations,
                ops_per,
                engine,
                perturb == Some("datalog"),
                &mut sec,
            );
        }));
        if r.is_err() {
            sec.push(
                "datalog",
                "PANIC during section (counted as divergence)".to_string(),
            );
        }
    }
    if !skipped("datalog-quoting") {
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run_datalog_quoting(engine, perturb == Some("datalog-quoting"), &mut sec);
        }));
        if r.is_err() {
            sec.push(
                "datalog-quoting",
                "PANIC during section (counted as divergence)".to_string(),
            );
        }
    }
    if !skipped("vector") {
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run_vector(
                seed,
                iterations,
                ops_per,
                engine,
                perturb == Some("vector"),
                vector_control_shape,
                &mut sec,
            );
        }));
        if r.is_err() {
            sec.push(
                "vector",
                "PANIC during section (counted as divergence)".to_string(),
            );
        }
    }
    if !skipped("catalog") {
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run_catalog_refusal(
                seed,
                iterations,
                engine,
                perturb == Some("catalog"),
                &mut sec,
            );
        }));
        if r.is_err() {
            sec.push(
                "catalog",
                "PANIC during section (counted as divergence)".to_string(),
            );
        }
    }
    sec
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    tokio::task::spawn_blocking(main_impl).await.unwrap();
}
