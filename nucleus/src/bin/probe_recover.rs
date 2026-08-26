//! WAL recovery round-trip fuzzer. For each iteration it opens a durable
//! (WAL-backed) database in a fresh temp dir, applies a random sequence of
//! auto-committed DDL/DML, snapshots the live committed state, "crashes" by
//! dropping the database, reopens from the same dir, and asserts the recovered
//! state is byte-identical to the pre-crash snapshot. A second mode verifies
//! that an UNCOMMITTED transaction's writes are rolled back on crash.
//!
//! Auto-commit writes fsync on commit (MvccWal::log_commit), and we call
//! `Database::sync()` before crashing, so any mismatch is a real WAL
//! replay/recovery bug — not a buffering-timing artifact.
//!
//! Build/run: `cargo run --release --features server --bin probe_recover`
#![cfg(feature = "server")]
#![allow(clippy::all)] // internal fuzz harness

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
        p.push(format!("nucleus_recover_{tag}"));
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

fn open(dir: &PathBuf) -> Result<Database, String> {
    Database::durable_mvcc(dir).map_err(|e| format!("open: {e:?}"))
}

fn main_impl() {
    let mut seed: u64 = 0x5EC0_FFEE;
    let mut iterations = 400usize;
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
    println!(
        "Nucleus WAL recovery round-trip fuzzer\nseed={seed} iterations={iterations} ops/iter={ops_per}\n"
    );

    let mut total = 0usize;
    let mut divergences = 0usize;

    'outer: for iter in 0..iterations {
        let mut rng = Rng(seed.wrapping_add(iter as u64).wrapping_mul(0x100000001B3));
        let schema = Schema::random(&mut rng);
        let cols = schema.names();
        let tmp = TmpDir::new(&format!("{seed}_{iter}"));
        // Several crash/recover cycles on the SAME dir, to exercise multi-crash
        // WAL accumulation (where a fresh run's version indices must not collide
        // with a survivor's old ones).
        let cycles = 1 + rng.below(3);
        let mut next_id = 1i64;
        let mut expected: Option<Vec<Vec<String>>> = None; // committed state to verify after a crash

        for cycle in 0..cycles {
            let db = match open(&tmp.0) {
                Ok(d) => d,
                Err(e) => {
                    divergences += 1;
                    if divergences <= max_report {
                        println!("─── RECOVERY OPEN FAILED #{divergences} (iter {iter}) ── {e}\n");
                    }
                    continue 'outer;
                }
            };
            if cycle == 0 {
                if exec(&db, &schema.ddl()).is_err() {
                    continue 'outer;
                }
            } else if let Some(exp) = &expected {
                // Verify the prior crash recovered exactly the committed state.
                total += 1;
                let recovered = match snapshot(&db, &cols) {
                    Ok(s) => s,
                    Err(e) => {
                        divergences += 1;
                        if divergences <= max_report {
                            println!(
                                "─── RECOVERY READ FAILED #{divergences} (iter {iter}) ── {e}\n"
                            );
                        }
                        continue 'outer;
                    }
                };
                if &recovered != exp {
                    divergences += 1;
                    if divergences <= max_report {
                        println!(
                            "─── RECOVERY DIVERGENCE #{divergences} (iter {iter}, cycle {cycle}, seed {seed}) ───"
                        );
                        println!("  schema   : {}", schema.ddl());
                        println!(
                            "  pre-crash ({} rows): {:?}",
                            exp.len(),
                            &exp[..exp.len().min(6)]
                        );
                        println!(
                            "  recovered ({} rows): {:?}",
                            recovered.len(),
                            &recovered[..recovered.len().min(6)]
                        );
                        println!();
                    }
                    continue 'outer;
                }
            }

            // Apply committed mutations for this cycle.
            for _ in 0..ops_per {
                let m = gen_mutation(&schema, &mut rng, &mut next_id);
                let _ = exec(&db, &m); // tolerate occasional rejects (e.g. NOT NULL)
            }
            let _ = db.sync();
            expected = snapshot(&db, &cols).ok();
            if expected.is_none() {
                continue 'outer;
            }

            // Stage writes that must NOT survive the crash: an open txn, no commit.
            if rng.chance(35) {
                let _ = exec(&db, "BEGIN");
                for _ in 0..5 {
                    let m = gen_insert(&schema, &mut rng, next_id);
                    next_id += 1;
                    let _ = exec(&db, &m);
                }
            }
            // drop db = crash (committed WAL already fsynced).
        }
    }

    println!("\n════ SUMMARY ════");
    println!("recovery round-trips: {total}");
    println!("divergences         : {divergences}");
    if divergences == 0 {
        println!("\nAll committed state recovered exactly; rollbacks honored.");
    } else {
        std::process::exit(1);
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    tokio::task::spawn_blocking(main_impl).await.unwrap();
}
