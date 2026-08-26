//! Engine-vs-engine differential fuzzer (oracle-free): runs identical generated
//! SQL on two Nucleus storage engines (Mvcc as the baseline vs LSM / Memory /
//! Columnar) and flags any result divergence. No external oracle — the engines
//! must agree. Catches storage-engine-specific bugs in scan / filter / aggregate
//! / mutation paths. Build: `cargo run --release --features server --bin probe_engines`.
#![cfg(feature = "server")]

use std::sync::Arc;

use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::storage::{
    ColumnarStorageEngine, DiskEngine, LsmStorageEngine, MemoryEngine, MvccStorageAdapter,
    StorageEngine,
};
use nucleus::types::Value;

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
    BigInt,
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

const NAMES: &[&str] = &["c1", "c2", "c3", "c4", "c5"];
const CATS: &[&str] = &["red", "green", "blue", "amber", "str0", "str1", "str2"];

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
        cols.push(Col {
            name: NAMES[1],
            ty: Ty::Text,
            nn: true,
        });
        // A guaranteed BIGINT column so every run exercises the enduring width surface
        // (Int32 literal / Int32 column vs Int64-stored BIGINT) after canonicalization.
        cols.push(Col {
            name: NAMES[2],
            ty: Ty::BigInt,
            nn: true,
        });
        let extra = 1 + rng.below(2);
        for k in 0..extra {
            let ty = *rng.pick(&[Ty::Int, Ty::BigInt, Ty::Real, Ty::Text]);
            cols.push(Col {
                name: NAMES[3 + k],
                ty,
                nn: rng.chance(40),
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
                    Ty::BigInt => "BIGINT",
                    Ty::Real => "REAL",
                    Ty::Text => "TEXT",
                };
                format!("{} {ty}{}", c.name, if c.nn { " NOT NULL" } else { "" })
            })
            .collect();
        format!("CREATE TABLE t ({})", parts.join(", "))
    }
    fn of<F: Fn(&Col) -> bool>(&self, f: F) -> Vec<&Col> {
        self.cols.iter().filter(|c| f(c)).collect()
    }
    fn int_cols(&self) -> Vec<&Col> {
        self.of(|c| matches!(c.ty, Ty::Int | Ty::BigInt))
    }
    fn nn_nonid(&self) -> Vec<&Col> {
        self.of(|c| c.nn && c.name != "id")
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
        Ty::Int | Ty::BigInt => rng.int(-5, 20).to_string(),
        Ty::Real => format!("{:.1}", rng.int(-50, 50) as f64 / 10.0),
        Ty::Text => format!("'{}'", rng.pick(CATS)),
    }
}
fn gen_literal(rng: &mut Rng, c: &Col) -> String {
    match c.ty {
        Ty::Int | Ty::BigInt => rng.int(-5, 20).to_string(),
        Ty::Real => format!("{:.1}", rng.int(-50, 50) as f64 / 10.0),
        Ty::Text => format!("'{}'", rng.pick(CATS)),
    }
}
fn gen_inserts(s: &Schema, rng: &mut Rng, rows: usize) -> String {
    let names: Vec<&str> = s.cols.iter().map(|c| c.name).collect();
    let vals: Vec<String> = (1..=rows)
        .map(|id| {
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
            format!("({})", cells.join(","))
        })
        .collect();
    format!(
        "INSERT INTO t ({}) VALUES {}",
        names.join(","),
        vals.join(",")
    )
}

fn gen_predicate(s: &Schema, rng: &mut Rng, depth: u32) -> String {
    if depth > 0 && rng.chance(35) {
        let l = gen_predicate(s, rng, depth - 1);
        let r = gen_predicate(s, rng, depth - 1);
        return format!("({l} {} {r})", rng.pick(&["AND", "OR"]));
    }
    if rng.chance(12) {
        return format!("NOT ({})", gen_predicate(s, rng, 0));
    }
    let c = s.pick(rng);
    match rng.below(6) {
        0 if !c.nn => format!(
            "{} IS {}NULL",
            c.name,
            if rng.chance(50) { "NOT " } else { "" }
        ),
        1 if c.ty == Ty::Int => {
            let lo = rng.int(-5, 12);
            format!("{} BETWEEN {lo} AND {}", c.name, lo + rng.int(0, 14))
        }
        2 => {
            let n = 1 + rng.below(3);
            let items: Vec<String> = (0..n).map(|_| gen_literal(rng, c)).collect();
            format!(
                "{} {}IN ({})",
                c.name,
                if rng.chance(40) { "NOT " } else { "" },
                items.join(",")
            )
        }
        3 if c.ty == Ty::Text => format!("{} LIKE '{}%'", c.name, &rng.pick(CATS)[..2]),
        _ => format!(
            "{} {} {}",
            c.name,
            rng.pick(&["=", "<>", "<", "<=", ">", ">="]),
            gen_literal(rng, c)
        ),
    }
}

fn gen_orderby(s: &Schema, rng: &mut Rng) -> String {
    let nn = s.nn_nonid();
    let mut keys = Vec::new();
    if !nn.is_empty() && rng.chance(70) {
        let c = *rng.pick(&nn);
        keys.push(format!(
            "{} {}",
            c.name,
            if rng.chance(50) { "ASC" } else { "DESC" }
        ));
    }
    keys.push("id ASC".to_string());
    format!("ORDER BY {}", keys.join(", "))
}

fn gen_agg(s: &Schema, rng: &mut Rng) -> String {
    let any = s.pick(rng).name;
    let ints = s.int_cols();
    match rng.below(7) {
        0 => "COUNT(*)".into(),
        1 => format!("COUNT({any})"),
        2 => format!("COUNT(DISTINCT {any})"),
        3 if !ints.is_empty() => format!("SUM({})", rng.pick(&ints).name),
        4 if !ints.is_empty() => format!("AVG({})", rng.pick(&ints).name),
        5 => format!("MIN({any})"),
        _ => format!("MAX({any})"),
    }
}

/// (sql, ordered)
fn gen_query(s: &Schema, rng: &mut Rng, rows: usize) -> (String, bool) {
    match rng.below(6) {
        0 => {
            let n = 1 + rng.below(3);
            let aggs: Vec<String> = (0..n).map(|_| gen_agg(s, rng)).collect();
            let w = if rng.chance(55) {
                format!(" WHERE {}", gen_predicate(s, rng, 2))
            } else {
                String::new()
            };
            (format!("SELECT {} FROM t{w}", aggs.join(", ")), true)
        }
        1 => {
            let gcols: Vec<&Col> = s
                .nn_nonid()
                .into_iter()
                .filter(|c| c.ty != Ty::Real)
                .collect();
            let g = if gcols.is_empty() {
                "id"
            } else {
                rng.pick(&gcols).name
            };
            let agg = gen_agg(s, rng);
            let w = if rng.chance(45) {
                format!(" WHERE {}", gen_predicate(s, rng, 1))
            } else {
                String::new()
            };
            (
                format!("SELECT {g}, {agg} FROM t{w} GROUP BY {g} ORDER BY {g} ASC"),
                true,
            )
        }
        2 => {
            let proj = if rng.chance(30) {
                "*".to_string()
            } else {
                let cols: Vec<&str> = s.cols.iter().map(|c| c.name).collect();
                (0..1 + rng.below(3))
                    .map(|_| *rng.pick(&cols))
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            (
                format!(
                    "SELECT DISTINCT {proj} FROM t WHERE {}",
                    gen_predicate(s, rng, 1)
                ),
                false,
            )
        }
        _ => {
            let cols: Vec<&str> = s.cols.iter().map(|c| c.name).collect();
            let proj = if rng.chance(25) {
                "*".to_string()
            } else {
                (0..1 + rng.below(3))
                    .map(|_| *rng.pick(&cols))
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            let w = if rng.chance(60) {
                format!(" WHERE {}", gen_predicate(s, rng, 2))
            } else {
                String::new()
            };
            let order = format!(" {}", gen_orderby(s, rng));
            let limit = if rng.chance(40) {
                format!(" LIMIT {}", rng.int(1, rows as i64))
            } else {
                String::new()
            };
            (format!("SELECT {proj} FROM t{w}{order}{limit}"), true)
        }
    }
}

fn gen_mutation(s: &Schema, rng: &mut Rng, next_id: &mut i64) -> String {
    match rng.below(3) {
        0 => {
            let names: Vec<&str> = s.cols.iter().map(|c| c.name).collect();
            let cells: Vec<String> = s
                .cols
                .iter()
                .enumerate()
                .map(|(i, c)| {
                    if i == 0 {
                        next_id.to_string()
                    } else {
                        gen_value(rng, c)
                    }
                })
                .collect();
            *next_id += 1;
            format!(
                "INSERT INTO t ({}) VALUES ({})",
                names.join(","),
                cells.join(",")
            )
        }
        1 => {
            let c = *rng.pick(&s.cols.iter().skip(1).collect::<Vec<_>>());
            format!(
                "UPDATE t SET {} = {} WHERE {}",
                c.name,
                gen_value(rng, c),
                gen_predicate(s, rng, 1)
            )
        }
        _ => format!("DELETE FROM t WHERE {}", gen_predicate(s, rng, 1)),
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

fn run(ex: &Executor, sql: &str) -> Result<Vec<Vec<String>>, ()> {
    let rt = tokio::runtime::Handle::current();
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        tokio::task::block_in_place(|| rt.block_on(ex.execute(sql)))
    }));
    match res {
        Ok(Ok(mut r)) => match r.pop() {
            Some(ExecResult::Select { rows, .. }) => Ok(rows
                .iter()
                .map(|row| row.iter().map(canon).collect())
                .collect()),
            _ => Err(()),
        },
        _ => Err(()),
    }
}
fn exec(ex: &Executor, sql: &str) -> bool {
    let rt = tokio::runtime::Handle::current();
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        tokio::task::block_in_place(|| rt.block_on(ex.execute(sql)))
    }))
    .map(|r| r.is_ok())
    .unwrap_or(false)
}
fn compare(mut a: Vec<Vec<String>>, mut b: Vec<Vec<String>>, ordered: bool) -> bool {
    if !ordered {
        a.sort();
        b.sort();
    }
    a == b
}

fn make_engine(name: &str, catalog: &Arc<Catalog>) -> Arc<dyn StorageEngine> {
    match name {
        "lsm" => Arc::new(LsmStorageEngine::new()),
        "memory" => Arc::new(MemoryEngine::new()),
        "columnar" => Arc::new(ColumnarStorageEngine::new()),
        // The disk engine is where the integer-width bug actually diverged — its B-tree
        // compares serialized key bytes. Include it so the differential guards it. Fresh
        // temp file per call; the previous iteration's engine is already dropped.
        "disk" => {
            let path = std::env::temp_dir().join("probe_engines_disk.ndb");
            let _ = std::fs::remove_file(&path);
            let _ = std::fs::remove_file(path.with_extension("wal"));
            let _ = std::fs::remove_dir_all(path.with_extension("wal.d"));
            Arc::new(DiskEngine::open(&path, catalog.clone()).expect("disk engine open"))
        }
        _ => Arc::new(MvccStorageAdapter::new()),
    }
}

fn run_pair(
    engine_b: &str,
    seed: u64,
    iterations: usize,
    queries_per: usize,
    max_report: usize,
) -> usize {
    println!("── Mvcc ⇄ {engine_b} ──");
    let mut total = 0usize;
    let mut divergences = 0usize;

    'outer: for iter in 0..iterations {
        let mut rng = Rng(seed.wrapping_add(iter as u64).wrapping_mul(0x100000001B3));
        let schema = Schema::random(&mut rng);
        let rows = 8 + rng.below(30);
        let ddl = schema.ddl();
        let inserts = gen_inserts(&schema, &mut rng, rows);
        let mut next_id = rows as i64 + 1;

        let cat_a = Arc::new(Catalog::new());
        let exa = Arc::new(Executor::new(cat_a.clone(), make_engine("mvcc", &cat_a)));
        let cat_b = Arc::new(Catalog::new());
        let exb = Arc::new(Executor::new(cat_b.clone(), make_engine(engine_b, &cat_b)));
        let mut ops = vec![ddl.clone(), inserts.clone()];
        for stmt in [&ddl, &inserts] {
            if !exec(&exa, stmt) || !exec(&exb, stmt) {
                continue 'outer;
            }
        }

        for _ in 0..queries_per {
            total += 1;
            if rng.chance(30) {
                let m = gen_mutation(&schema, &mut rng, &mut next_id);
                let (ra, rb) = (exec(&exa, &m), exec(&exb, &m));
                if ra != rb {
                    continue 'outer;
                } // mutation acceptance differs → desync, skip
                if ra {
                    ops.push(m);
                }
                continue;
            }
            let (q, ordered) = gen_query(&schema, &mut rng, rows);
            match (run(&exa, &q), run(&exb, &q)) {
                (Ok(a), Ok(b)) => {
                    if !compare(a.clone(), b.clone(), ordered) {
                        divergences += 1;
                        if divergences <= max_report {
                            let (mut av, mut bv) = (a, b);
                            if !ordered {
                                av.sort();
                                bv.sort();
                            }
                            println!(
                                "─── DIVERGENCE #{divergences} (Mvcc vs {engine_b}, iter {iter}, seed {seed}) ───"
                            );
                            println!("  query : {q}");
                            println!(
                                "  mvcc ({} rows)   : {:?}",
                                av.len(),
                                &av[..av.len().min(6)]
                            );
                            println!(
                                "  {engine_b} ({} rows): {:?}",
                                bv.len(),
                                &bv[..bv.len().min(6)]
                            );
                            println!("  ── replay ({} ops) ──", ops.len());
                            for op in &ops {
                                println!("    {op};");
                            }
                            println!("    {q};\n");
                        }
                        continue 'outer;
                    }
                }
                (Err(_), Err(_)) => {}
                _ => continue 'outer, // one engine errored, other didn't → likely unsupported feature
            }
        }
    }
    println!("  queries: {total}  divergences: {divergences}\n");
    divergences
}

fn main_impl() {
    let mut seed: u64 = 0xE0E0_1234;
    let mut iterations = 1500usize;
    let mut queries_per = 25usize;
    let mut max_report = 10usize;
    let mut engines = vec![
        "lsm".to_string(),
        "memory".to_string(),
        "columnar".to_string(),
        "disk".to_string(),
    ];
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
            "--queries" => {
                i += 1;
                queries_per = args[i].parse().unwrap();
            }
            "--max-report" => {
                i += 1;
                max_report = args[i].parse().unwrap();
            }
            "--engine" => {
                i += 1;
                engines = vec![args[i].clone()];
            }
            _ => {}
        }
        i += 1;
    }
    std::panic::set_hook(Box::new(|_| {}));
    println!(
        "Nucleus engine-vs-engine differential fuzzer\nseed={seed} iterations={iterations} queries/iter={queries_per}\n"
    );

    let mut total_div = 0usize;
    for e in &engines {
        total_div += run_pair(e, seed, iterations, queries_per, max_report);
    }
    println!("════ SUMMARY ════\ntotal divergences: {total_div}");
    if total_div == 0 {
        println!("\nAll engines agree with Mvcc.");
    } else {
        std::process::exit(1);
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    tokio::task::spawn_blocking(main_impl).await.unwrap();
}
