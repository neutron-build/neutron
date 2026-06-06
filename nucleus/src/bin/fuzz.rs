//! Differential fuzzer: Nucleus vs SQLite.
//!
//! Generates random schemas, data, and SQL queries, runs each query against
//! both Nucleus (embedded executor) and SQLite (a battle-tested oracle), and
//! reports any result divergence with a fully reproducible seed. SQLite is the
//! reference because it is correct and rigorous; any disagreement is a Nucleus
//! bug (or a documented dialect difference).
//!
//! This is how the bugs found by hand this cycle (comma-join row loss, ORDER BY
//! by a non-projected column, etc.) get found automatically — and how future
//! regressions get caught before a consumer hits them.
//!
//! Usage:
//!   cargo run --release --features "server rusqlite" --bin fuzz
//!   cargo run --release --features "server rusqlite" --bin fuzz -- --seed 42 --iterations 5000 --max-report 20
//!   cargo run --release --features "server rusqlite" --bin fuzz -- --seed 123   # reproduce one iteration
//!
//! Methodology notes (to avoid false positives):
//!   - ORDER BY uses only NOT NULL columns and always ends with the unique `id`
//!     tiebreaker, so the output order is deterministic and comparable.
//!   - LIMIT is only emitted with an ORDER BY (otherwise which rows survive is
//!     undefined and the two engines may legally differ).
//!   - Numbers are compared semantically (5 == 5.0); floats rounded to 6 dp.
//!   - GROUP BY keys are NOT NULL (NULL-group + NULL-order conventions differ).

use std::sync::Arc;

use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::storage::{MvccStorageAdapter, StorageEngine};
use nucleus::types::Value;
use rusqlite::Connection;

// ─── Deterministic PRNG (so any divergence reproduces from its seed) ──────────
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        // SplitMix64
        self.0 = self.0.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: usize) -> usize {
        if n == 0 {
            0
        } else {
            (self.next() % n as u64) as usize
        }
    }
    fn chance(&mut self, pct: u64) -> bool {
        self.next() % 100 < pct
    }
    fn pick<'a, T>(&mut self, xs: &'a [T]) -> &'a T {
        &xs[self.below(xs.len())]
    }
    fn int(&mut self, lo: i64, hi: i64) -> i64 {
        lo + (self.next() % ((hi - lo + 1) as u64)) as i64
    }
}

// ─── Fixed schema (varied data per seed) ──────────────────────────────────────
// id: unique NOT NULL (tiebreaker). d,e: NOT NULL ints (safe to ORDER BY).
// a,b: nullable ints (aggregates / WHERE). g: NOT NULL category text (GROUP BY).
// s: nullable text.
const SCHEMA: &str = "CREATE TABLE t (\
    id INTEGER PRIMARY KEY, \
    d INTEGER NOT NULL, \
    e INTEGER NOT NULL, \
    a INTEGER, \
    b INTEGER, \
    g TEXT NOT NULL, \
    s TEXT)";

const ORDER_COLS: &[&str] = &["d", "e", "g", "id"];
const NUM_COLS: &[&str] = &["id", "d", "e", "a", "b"];
const ALL_COLS: &[&str] = &["id", "d", "e", "a", "b", "g", "s"];
const CATS: &[&str] = &["red", "green", "blue", "amber"];

fn gen_inserts(rng: &mut Rng, rows: usize) -> String {
    let mut vals = Vec::with_capacity(rows);
    for id in 1..=rows {
        let d = rng.int(0, 9);
        let e = rng.int(0, 4);
        let a = if rng.chance(20) {
            "NULL".to_string()
        } else {
            rng.int(-5, 20).to_string()
        };
        let b = if rng.chance(20) {
            "NULL".to_string()
        } else {
            rng.int(0, 100).to_string()
        };
        let g = rng.pick(CATS);
        let s = if rng.chance(25) {
            "NULL".to_string()
        } else {
            format!("'str{}'", rng.int(0, 5))
        };
        vals.push(format!("({id},{d},{e},{a},{b},'{g}',{s})"));
    }
    format!("INSERT INTO t (id,d,e,a,b,g,s) VALUES {}", vals.join(","))
}

// ─── Predicate / query generation ─────────────────────────────────────────────
fn gen_literal(rng: &mut Rng, col: &str) -> String {
    match col {
        "g" => format!("'{}'", rng.pick(CATS)),
        "s" => format!("'str{}'", rng.int(0, 5)),
        "a" => rng.int(-5, 20).to_string(),
        "b" => rng.int(0, 100).to_string(),
        _ => rng.int(0, 9).to_string(),
    }
}

fn gen_predicate(rng: &mut Rng, depth: u32) -> String {
    if depth > 0 && rng.chance(45) {
        let l = gen_predicate(rng, depth - 1);
        let r = gen_predicate(rng, depth - 1);
        let op = rng.pick(&["AND", "OR"]);
        return format!("({l} {op} {r})");
    }
    if rng.chance(12) {
        return format!("NOT ({})", gen_predicate(rng, 0));
    }
    let col = *rng.pick(ALL_COLS);
    match rng.below(6) {
        0 if col == "a" || col == "b" || col == "s" => {
            let n = if rng.chance(50) { "NOT " } else { "" };
            format!("{col} IS {n}NULL")
        }
        1 if NUM_COLS.contains(&col) => {
            let lo = rng.int(-5, 10);
            let hi = lo + rng.int(0, 15);
            format!("{col} BETWEEN {lo} AND {hi}")
        }
        2 => {
            let n = 1 + rng.below(3);
            let items: Vec<String> = (0..n).map(|_| gen_literal(rng, col)).collect();
            format!("{col} IN ({})", items.join(","))
        }
        _ => {
            let op = *rng.pick(&["=", "<>", "<", "<=", ">", ">="]);
            format!("{col} {op} {}", gen_literal(rng, col))
        }
    }
}

fn gen_orderby(rng: &mut Rng) -> String {
    // 1-2 NOT NULL keys, then `id` as a unique tiebreaker → fully deterministic.
    let n = 1 + rng.below(2);
    let mut keys = Vec::new();
    for _ in 0..n {
        let c = *rng.pick(&ORDER_COLS[..3]); // d,e,g (exclude id; appended below)
        let dir = if rng.chance(50) { "ASC" } else { "DESC" };
        keys.push(format!("{c} {dir}"));
    }
    keys.push("id ASC".to_string());
    format!("ORDER BY {}", keys.join(", "))
}

fn gen_agg(rng: &mut Rng) -> String {
    let aggs = [
        "COUNT(*)", "COUNT(a)", "SUM(a)", "AVG(a)", "MIN(a)", "MAX(a)", "SUM(b)", "AVG(b)",
        "MIN(d)", "MAX(e)", "MIN(g)", "MAX(g)",
    ];
    rng.pick(&aggs).to_string()
}

/// Returns (sql, ordered) — `ordered` true means compare rows in order.
fn gen_query(rng: &mut Rng, rows: usize) -> (String, bool) {
    match rng.below(5) {
        // Aggregate, no GROUP BY → single row.
        0 => {
            let n = 1 + rng.below(3);
            let aggs: Vec<String> = (0..n).map(|_| gen_agg(rng)).collect();
            let where_c = if rng.chance(55) {
                format!(" WHERE {}", gen_predicate(rng, 2))
            } else {
                String::new()
            };
            (format!("SELECT {} FROM t{where_c}", aggs.join(", ")), true)
        }
        // GROUP BY.
        1 => {
            let gcol = *rng.pick(&["d", "e", "g"]);
            let n = 1 + rng.below(2);
            let aggs: Vec<String> = (0..n).map(|_| gen_agg(rng)).collect();
            let where_c = if rng.chance(45) {
                format!(" WHERE {}", gen_predicate(rng, 1))
            } else {
                String::new()
            };
            (
                format!(
                    "SELECT {gcol}, {} FROM t{where_c} GROUP BY {gcol} ORDER BY {gcol} ASC",
                    aggs.join(", ")
                ),
                true,
            )
        }
        // Comma self-join (the class that hid the row-loss bug).
        2 | 3 => {
            let jcol = *rng.pick(&["d", "e", "a", "g"]);
            let explicit = rng.chance(50);
            let from = if explicit {
                format!("t x1 JOIN t x2 ON x1.{jcol} = x2.{jcol}")
            } else {
                format!("t x1, t x2 WHERE x1.{jcol} = x2.{jcol}")
            };
            let extra = if rng.chance(50) {
                let conj = if explicit { "WHERE" } else { "AND" };
                format!(" {conj} x1.id < {}", rng.int(2, rows as i64))
            } else {
                String::new()
            };
            let proj = *rng.pick(&["x1.id, x2.id", "x1.g, x2.d, x1.id, x2.id", "x1.id, x2.g"]);
            // Deterministic order across both engines.
            (
                format!("SELECT {proj} FROM {from}{extra} ORDER BY x1.id ASC, x2.id ASC"),
                true,
            )
        }
        // Projection with WHERE / ORDER BY / LIMIT.
        _ => {
            let proj = if rng.chance(30) {
                "*".to_string()
            } else {
                let n = 1 + rng.below(3);
                let cols: Vec<&str> = (0..n).map(|_| *rng.pick(ALL_COLS)).collect();
                cols.join(", ")
            };
            let where_c = if rng.chance(55) {
                format!(" WHERE {}", gen_predicate(rng, 2))
            } else {
                String::new()
            };
            let has_order = rng.chance(65);
            let order = if has_order {
                format!(" {}", gen_orderby(rng))
            } else {
                String::new()
            };
            // LIMIT only with ORDER BY (else which rows survive is undefined).
            let limit = if has_order && rng.chance(40) {
                format!(" LIMIT {}", rng.int(1, rows as i64))
            } else {
                String::new()
            };
            (
                format!("SELECT {proj} FROM t{where_c}{order}{limit}"),
                has_order,
            )
        }
    }
}

// ─── Value canonicalization (semantic equality) ───────────────────────────────
fn canon_num(f: f64) -> String {
    if f.is_finite() && (f - f.round()).abs() < 1e-9 && f.abs() < 9e15 {
        format!("{}", f.round() as i64)
    } else {
        format!("{f:.6}")
    }
}
fn canon_nucleus(v: &Value) -> String {
    match v {
        Value::Null => "∅".into(),
        Value::Bool(b) => (if *b { "1" } else { "0" }).into(),
        Value::Int32(n) => n.to_string(),
        Value::Int64(n) => n.to_string(),
        Value::Float64(f) => canon_num(*f),
        Value::Text(s) => format!("'{s}'"),
        other => format!("'{other}'"),
    }
}
fn canon_sqlite(v: &rusqlite::types::Value) -> String {
    use rusqlite::types::Value as Sv;
    match v {
        Sv::Null => "∅".into(),
        Sv::Integer(i) => i.to_string(),
        Sv::Real(f) => canon_num(*f),
        Sv::Text(s) => format!("'{s}'"),
        Sv::Blob(b) => format!("blob:{}", b.len()),
    }
}

fn run_nucleus(ex: &Executor, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let rt = tokio::runtime::Handle::current();
    let res = tokio::task::block_in_place(|| rt.block_on(ex.execute(sql)));
    match res {
        Ok(mut results) => match results.pop() {
            Some(ExecResult::Select { rows, .. }) => Ok(rows
                .iter()
                .map(|r| r.iter().map(canon_nucleus).collect())
                .collect()),
            _ => Err("non-select result".into()),
        },
        Err(e) => Err(format!("{e:?}")),
    }
}
fn run_sqlite(conn: &Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let ncol = stmt.column_count();
    let rows_iter = stmt
        .query_map([], |row| {
            let mut cells = Vec::with_capacity(ncol);
            for i in 0..ncol {
                let v: rusqlite::types::Value = row.get(i)?;
                cells.push(canon_sqlite(&v));
            }
            Ok(cells)
        })
        .map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    for r in rows_iter {
        out.push(r.map_err(|e| e.to_string())?);
    }
    Ok(out)
}

fn compare(mut nuc: Vec<Vec<String>>, mut sql: Vec<Vec<String>>, ordered: bool) -> bool {
    if !ordered {
        nuc.sort();
        sql.sort();
    }
    nuc == sql
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    let mut seed: u64 = 0x1234_5678;
    let mut iterations: usize = 2000;
    let mut max_report: usize = 15;
    let mut queries_per: usize = 25;
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
            "--max-report" => {
                i += 1;
                max_report = args[i].parse().unwrap();
            }
            "--queries" => {
                i += 1;
                queries_per = args[i].parse().unwrap();
            }
            _ => {}
        }
        i += 1;
    }

    println!("Nucleus ⇄ SQLite differential fuzzer");
    println!("seed={seed} iterations={iterations} queries/iter={queries_per}\n");

    let mut total_queries = 0usize;
    let mut divergences = 0usize;
    let mut nuc_errors = 0usize; // Nucleus errored where SQLite succeeded (possible gap)

    'outer: for iter in 0..iterations {
        let mut rng = Rng(seed.wrapping_add(iter as u64).wrapping_mul(0x100000001B3));
        let rows = 8 + rng.below(40);

        // Fresh Nucleus + SQLite with identical schema + data.
        let catalog = Arc::new(Catalog::new());
        let storage: Arc<dyn StorageEngine> = Arc::new(MvccStorageAdapter::new());
        let ex = Arc::new(Executor::new(catalog, storage));
        let sqlite = Connection::open_in_memory().unwrap();

        let inserts = gen_inserts(&mut rng, rows);
        for ddl in [SCHEMA, &inserts] {
            if let Err(e) = run_nucleus(&ex, ddl) {
                // DDL/INSERT must succeed on both; if Nucleus can't, that's notable.
                if !e.contains("non-select") {
                    eprintln!("[setup] nucleus failed: {e}\n  {ddl}");
                    continue 'outer;
                }
            }
            sqlite.execute_batch(ddl).unwrap();
        }

        for _ in 0..queries_per {
            let (q, ordered) = gen_query(&mut rng, rows);
            total_queries += 1;
            let n = run_nucleus(&ex, &q);
            let s = run_sqlite(&sqlite, &q);
            match (n, s) {
                (Ok(nr), Ok(sr)) => {
                    if !compare(nr.clone(), sr.clone(), ordered) {
                        divergences += 1;
                        if divergences <= max_report {
                            println!(
                                "─── DIVERGENCE #{divergences} (iter {iter}, seed {seed}) ───"
                            );
                            println!("  query : {q}");
                            println!("  setup : {SCHEMA}");
                            println!("          {inserts}");
                            let (mut a, mut b) = (nr, sr);
                            if !ordered {
                                a.sort();
                                b.sort();
                            }
                            println!("  nucleus({} rows): {}", a.len(), preview(&a));
                            println!("  sqlite ({} rows): {}", b.len(), preview(&b));
                            println!();
                        }
                    }
                }
                (Err(ne), Ok(_)) => {
                    // SQLite accepts it, Nucleus errors — possible missing feature/bug.
                    nuc_errors += 1;
                    if nuc_errors <= max_report {
                        println!("─── NUCLEUS-ERROR (iter {iter}) ─── {q}\n    err: {ne}\n");
                    }
                }
                _ => {} // both error, or Nucleus-only success (rare) → ignore
            }
        }
    }

    println!("\n════ SUMMARY ════");
    println!("queries run        : {total_queries}");
    println!("RESULT divergences : {divergences}");
    println!("nucleus-only errors: {nuc_errors} (SQLite accepted; may be unsupported features)");
    if divergences == 0 {
        println!("\nNo result divergences vs SQLite. 🎯");
    } else {
        println!("\nReproduce a divergence with: --seed {seed} --iterations <iter+1>");
        std::process::exit(1);
    }
}

fn preview(rows: &[Vec<String>]) -> String {
    let shown: Vec<String> = rows
        .iter()
        .take(8)
        .map(|r| format!("[{}]", r.join(",")))
        .collect();
    let more = if rows.len() > 8 {
        format!(" …(+{})", rows.len() - 8)
    } else {
        String::new()
    };
    format!("{}{more}", shown.join(" "))
}
