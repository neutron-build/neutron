//! Differential fuzzer: Nucleus vs SQLite.
//!
//! Generates random schemas, data, queries, AND mutations, runs each against
//! both Nucleus (embedded executor) and SQLite (a battle-tested oracle), and
//! reports any divergence with a fully reproducible seed. SQLite is the
//! reference because it is correct and rigorous; any disagreement is a Nucleus
//! bug (or a documented dialect difference we deliberately avoid generating).
//!
//! Coverage (each axis is a place bugs hide):
//!   - Random schemas: varied column count, types (INT/REAL/TEXT), nullability.
//!   - Reads: aggregates (incl COUNT(DISTINCT)), GROUP BY + HAVING, INNER/LEFT
//!     self-joins, projection w/ CASE/COALESCE/ABS/arithmetic, LIKE, IS NULL,
//!     nested AND/OR/NOT, DISTINCT, ORDER BY, LIMIT/OFFSET, UNION/INTERSECT/
//!     EXCEPT, IN/NOT IN/EXISTS/scalar subqueries.
//!   - Mutations: INSERT/UPDATE/DELETE interleaved with reads; after every
//!     mutation the FULL table state must match SQLite (catches visibility /
//!     MVCC / index-maintenance bugs).
//!   - Robustness: every execution runs under catch_unwind — any panic on any
//!     input is a bug, reported and fatal.
//!
//! Usage:
//!   cargo run --release --features "server rusqlite" --bin fuzz
//!   cargo run --release --features "server rusqlite" --bin fuzz -- --seed 42 --iterations 5000
//!   cargo run --release --features "server rusqlite" --bin fuzz -- --engine buffered-disk
//!
//! `--engine` selects which storage engine the oracle runs against. It defaults
//! to `mvcc` (RAM-resident, no WAL) purely for speed, and that default is a
//! coverage hole worth stating plainly: every paged-storage defect found so far
//! — scan-ordinal row addressing, missing buffer-pool frame latches, VACUUM slot
//! renumbering — lives in `DiskEngine`, which `mvcc` does not exercise at all.
//! `MvccStorageAdapter` passed the 8-way soak that both paged engines failed. To
//! aim this oracle at what `nucleus serve` actually runs, pass
//! `--engine buffered-disk`; `--engine disk` isolates paged storage from the
//! buffering layer. Paged engines open a fresh temp directory per iteration and
//! fsync on commit, so they are far slower — lower `--iterations` accordingly.
//!
//! Methodology notes (to avoid false positives — dialect differences, not bugs):
//!   - ORDER BY uses only NOT NULL columns + the unique `id` tiebreaker, so row
//!     order is deterministic (SQLite/Nucleus differ on NULL ordering default).
//!   - LIMIT/OFFSET only with an ORDER BY (else surviving rows are undefined).
//!   - SUM/AVG only over INTEGER columns (float accumulation order differs).
//!   - No `/` or `%` (SQLite returns NULL on div-by-zero; Nucleus errors).
//!   - LIKE patterns and text data are lowercase (SQLite LIKE is ASCII-case-
//!     insensitive; equal case keeps it moot).
//!   - Set operations compared unordered (set semantics, no ORDER BY needed).

use std::panic::AssertUnwindSafe;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::metrics::harness::{EngineConfig, EngineKind, HarnessDb};
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

// ─── Random schema ────────────────────────────────────────────────────────────
#[derive(Clone, Copy, PartialEq, Eq)]
enum Ty {
    Int,
    Real,
    Text,
}

#[derive(Clone)]
struct Col {
    name: &'static str,
    ty: Ty,
    nn: bool, // NOT NULL
}

struct Schema {
    cols: Vec<Col>, // cols[0] is always `id INTEGER PRIMARY KEY`
}

const NAMES: &[&str] = &["c1", "c2", "c3", "c4", "c5", "c6", "c7"];
const CATS: &[&str] = &[
    "red", "green", "blue", "amber", "str0", "str1", "str2", "str3",
];

impl Schema {
    fn random(rng: &mut Rng) -> Schema {
        let mut cols = vec![Col {
            name: "id",
            ty: Ty::Int,
            nn: true,
        }];
        // Guarantee a NOT NULL int and a NOT NULL text so ORDER BY / GROUP BY
        // always have safe (non-NULL) targets besides id.
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
        let extra = 2 + rng.below(4); // total non-id cols: 4..7
        for k in 0..extra {
            let ty = *rng.pick(&[Ty::Int, Ty::Int, Ty::Real, Ty::Text]);
            cols.push(Col {
                name: NAMES[2 + k],
                ty,
                nn: rng.chance(35),
            });
        }
        Schema { cols }
    }

    fn ddl(&self) -> String {
        let mut parts = Vec::new();
        for (i, c) in self.cols.iter().enumerate() {
            if i == 0 {
                parts.push("id INTEGER PRIMARY KEY".to_string());
                continue;
            }
            let ty = match c.ty {
                Ty::Int => "INTEGER",
                Ty::Real => "REAL",
                Ty::Text => "TEXT",
            };
            let nn = if c.nn { " NOT NULL" } else { "" };
            parts.push(format!("{} {ty}{nn}", c.name));
        }
        format!("CREATE TABLE t ({})", parts.join(", "))
    }

    fn of<F: Fn(&Col) -> bool>(&self, f: F) -> Vec<&Col> {
        self.cols.iter().filter(|c| f(c)).collect()
    }
    fn nn_cols(&self) -> Vec<&Col> {
        self.of(|c| c.nn)
    }
    fn int_cols(&self) -> Vec<&Col> {
        self.of(|c| c.ty == Ty::Int)
    }
    fn nullable(&self) -> Vec<&Col> {
        self.of(|c| !c.nn)
    }
    fn pick<'a>(&'a self, rng: &mut Rng) -> &'a Col {
        let i = rng.below(self.cols.len());
        &self.cols[i]
    }
}

// ─── Value / literal generation ───────────────────────────────────────────────
fn gen_value(rng: &mut Rng, c: &Col) -> String {
    let null_pct = if c.ty == Ty::Text { 25 } else { 20 };
    if !c.nn && rng.chance(null_pct) {
        return "NULL".into();
    }
    match c.ty {
        Ty::Int => rng.int(-5, 20).to_string(),
        // One-decimal reals keep f64 sums/compares exact for small row counts.
        Ty::Real => format!("{:.1}", rng.int(-50, 50) as f64 / 10.0),
        Ty::Text => format!("'{}'", rng.pick(CATS)),
    }
}

fn gen_literal(rng: &mut Rng, c: &Col) -> String {
    match c.ty {
        Ty::Int => rng.int(-5, 20).to_string(),
        Ty::Real => format!("{:.1}", rng.int(-50, 50) as f64 / 10.0),
        Ty::Text => format!("'{}'", rng.pick(CATS)),
    }
}

fn gen_inserts(schema: &Schema, rng: &mut Rng, rows: usize) -> String {
    let names: Vec<&str> = schema.cols.iter().map(|c| c.name).collect();
    let mut vals = Vec::with_capacity(rows);
    for id in 1..=rows {
        let mut cells = Vec::with_capacity(schema.cols.len());
        for (i, c) in schema.cols.iter().enumerate() {
            cells.push(if i == 0 {
                id.to_string()
            } else {
                gen_value(rng, c)
            });
        }
        vals.push(format!("({})", cells.join(",")));
    }
    format!(
        "INSERT INTO t ({}) VALUES {}",
        names.join(","),
        vals.join(",")
    )
}

// ─── Predicates ───────────────────────────────────────────────────────────────
fn gen_predicate(schema: &Schema, rng: &mut Rng, depth: u32) -> String {
    if depth > 0 && rng.chance(42) {
        let l = gen_predicate(schema, rng, depth - 1);
        let r = gen_predicate(schema, rng, depth - 1);
        let op = rng.pick(&["AND", "OR"]);
        return format!("({l} {op} {r})");
    }
    if rng.chance(12) {
        return format!("NOT ({})", gen_predicate(schema, rng, 0));
    }
    let c = schema.pick(rng);
    match rng.below(7) {
        0 if !c.nn => {
            let n = if rng.chance(50) { "NOT " } else { "" };
            format!("{} IS {n}NULL", c.name)
        }
        1 if c.ty == Ty::Int => {
            let lo = rng.int(-5, 12);
            let hi = lo + rng.int(0, 14);
            format!("{} BETWEEN {lo} AND {hi}", c.name)
        }
        2 => {
            let n = 1 + rng.below(3);
            let items: Vec<String> = (0..n).map(|_| gen_literal(rng, c)).collect();
            let neg = if rng.chance(40) { "NOT " } else { "" };
            format!("{} {neg}IN ({})", c.name, items.join(","))
        }
        3 if c.ty == Ty::Text => {
            // Lowercase patterns over lowercase data → LIKE case-folding moot.
            let pat = match rng.below(4) {
                0 => format!("'{}'", rng.pick(CATS)),
                1 => format!("'{}%'", &rng.pick(CATS)[..2]),
                2 => "'%r%'".to_string(),
                _ => "'str_'".to_string(),
            };
            let neg = if rng.chance(35) { "NOT " } else { "" };
            format!("{} {neg}LIKE {pat}", c.name)
        }
        _ => {
            let op = *rng.pick(&["=", "<>", "<", "<=", ">", ">="]);
            format!("{} {op} {}", c.name, gen_literal(rng, c))
        }
    }
}

// ─── Scalar projection expressions ────────────────────────────────────────────
fn gen_scalar(schema: &Schema, rng: &mut Rng) -> String {
    match rng.below(6) {
        0 => {
            // CASE WHEN <pred> THEN <int> ELSE <int> END
            let p = gen_predicate(schema, rng, 1);
            format!(
                "CASE WHEN {p} THEN {} ELSE {} END",
                rng.int(0, 9),
                rng.int(0, 9)
            )
        }
        1 if !schema.nullable().is_empty() => {
            let c = *rng.pick(&schema.nullable());
            format!("COALESCE({}, {})", c.name, gen_literal(rng, c))
        }
        2 if !schema.int_cols().is_empty() => {
            let c = *rng.pick(&schema.int_cols());
            format!("ABS({})", c.name)
        }
        3 if !schema.int_cols().is_empty() => {
            // Bounded arithmetic (operands tiny → no overflow; no '/' or '%').
            let c = *rng.pick(&schema.int_cols());
            let op = *rng.pick(&["+", "-", "*"]);
            let k = if op == "*" {
                rng.int(0, 3)
            } else {
                rng.int(0, 10)
            };
            format!("({} {op} {k})", c.name)
        }
        4 => {
            // Boolean expression → 0/1 in both engines.
            format!("({})", gen_predicate(schema, rng, 0))
        }
        _ => schema.pick(rng).name.to_string(),
    }
}

// ─── ORDER BY (NOT NULL keys + id tiebreaker = deterministic) ─────────────────
fn gen_orderby(schema: &Schema, rng: &mut Rng) -> String {
    let nn: Vec<&Col> = schema
        .nn_cols()
        .into_iter()
        .filter(|c| c.name != "id")
        .collect();
    let mut keys = Vec::new();
    if !nn.is_empty() {
        let n = 1 + rng.below(2.min(nn.len()));
        for _ in 0..n {
            let c = *rng.pick(&nn);
            let dir = if rng.chance(50) { "ASC" } else { "DESC" };
            keys.push(format!("{} {dir}", c.name));
        }
    }
    keys.push("id ASC".to_string());
    format!("ORDER BY {}", keys.join(", "))
}

// ─── Aggregates ───────────────────────────────────────────────────────────────
fn gen_agg(schema: &Schema, rng: &mut Rng) -> String {
    let any = schema.pick(rng).name;
    let ints = schema.int_cols();
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

// ─── Query generation. Returns (sql, ordered). ────────────────────────────────
fn gen_query(schema: &Schema, rng: &mut Rng, rows: usize) -> (String, bool) {
    match rng.below(8) {
        // Aggregate, no GROUP BY → single row.
        0 => {
            let n = 1 + rng.below(3);
            let aggs: Vec<String> = (0..n).map(|_| gen_agg(schema, rng)).collect();
            let w = if rng.chance(55) {
                format!(" WHERE {}", gen_predicate(schema, rng, 2))
            } else {
                String::new()
            };
            (format!("SELECT {} FROM t{w}", aggs.join(", ")), true)
        }
        // GROUP BY [HAVING].
        1 => {
            // Group by a NOT NULL column (NULL-group conventions differ).
            let gcols: Vec<&Col> = schema
                .nn_cols()
                .into_iter()
                .filter(|c| c.ty != Ty::Real)
                .collect();
            let g = if gcols.is_empty() {
                "id"
            } else {
                rng.pick(&gcols).name
            };
            let n = 1 + rng.below(2);
            let aggs: Vec<String> = (0..n).map(|_| gen_agg(schema, rng)).collect();
            let w = if rng.chance(45) {
                format!(" WHERE {}", gen_predicate(schema, rng, 1))
            } else {
                String::new()
            };
            let having = if rng.chance(30) {
                let op = *rng.pick(&[">", ">=", "<", "<=", "="]);
                format!(" HAVING COUNT(*) {op} {}", rng.int(0, 3))
            } else {
                String::new()
            };
            (
                format!(
                    "SELECT {g}, {} FROM t{w} GROUP BY {g}{having} ORDER BY {g} ASC",
                    aggs.join(", ")
                ),
                true,
            )
        }
        // Self-join: INNER or LEFT.
        2 | 3 => {
            let jcols = schema.of(|c| c.ty == Ty::Int || c.ty == Ty::Text);
            let jcol = rng.pick(&jcols).name;
            let kind = if rng.chance(50) { "JOIN" } else { "LEFT JOIN" };
            let extra = if rng.chance(50) {
                format!(" AND x1.id < {}", rng.int(2, rows as i64))
            } else {
                String::new()
            };
            // x2.id is NULL only for unmatched LEFT singletons (unique per x1.id),
            // so ORDER BY x1.id, x2.id stays deterministic across both engines.
            (
                format!(
                    "SELECT x1.id, x2.id, x1.{jcol} FROM t x1 {kind} t x2 ON x1.{jcol} = x2.{jcol}{extra} ORDER BY x1.id ASC, x2.id ASC"
                ),
                true,
            )
        }
        // Set operation (compared unordered).
        4 => {
            let op = *rng.pick(&["UNION", "UNION ALL", "INTERSECT", "EXCEPT"]);
            let c1 = schema.cols[1].name; // guaranteed NN int
            let c2 = schema.cols[2].name; // guaranteed NN text
            let p1 = gen_predicate(schema, rng, 1);
            let p2 = gen_predicate(schema, rng, 1);
            (
                format!(
                    "SELECT id, {c1}, {c2} FROM t WHERE {p1} {op} SELECT id, {c1}, {c2} FROM t WHERE {p2}"
                ),
                false,
            )
        }
        // Subquery: IN / NOT IN / EXISTS / scalar.
        5 => {
            let pred = match rng.below(4) {
                0 => {
                    let c = schema.pick(rng).name;
                    let neg = if rng.chance(50) { "NOT " } else { "" };
                    format!(
                        "{c} {neg}IN (SELECT {c} FROM t WHERE {})",
                        gen_predicate(schema, rng, 1)
                    )
                }
                1 => {
                    let neg = if rng.chance(50) { "NOT " } else { "" };
                    let c = schema.cols[1].name;
                    format!(
                        "{neg}EXISTS (SELECT 1 FROM t x2 WHERE x2.{c} = t.{c} AND {})",
                        gen_predicate(schema, rng, 0)
                    )
                }
                2 => {
                    let c = schema.cols[1].name; // NN int
                    let op = *rng.pick(&["=", "<", ">", "<=", ">="]);
                    format!("{c} {op} (SELECT MAX({c}) FROM t)")
                }
                _ => {
                    let c = schema.cols[1].name;
                    format!("{c} > (SELECT AVG({c}) FROM t)")
                }
            };
            (
                format!("SELECT id FROM t WHERE {pred} ORDER BY id ASC"),
                true,
            )
        }
        // Projection (+ DISTINCT) with WHERE / ORDER BY / LIMIT / OFFSET.
        _ => {
            let distinct = rng.chance(25);
            let proj = if rng.chance(25) && !distinct {
                "*".to_string()
            } else {
                let n = 1 + rng.below(3);
                let items: Vec<String> = (0..n).map(|_| gen_scalar(schema, rng)).collect();
                items.join(", ")
            };
            let w = if rng.chance(55) {
                format!(" WHERE {}", gen_predicate(schema, rng, 2))
            } else {
                String::new()
            };
            // DISTINCT rows are a set → compare unordered by default. BUT a DISTINCT
            // over NOT-NULL columns ordered by *all* of those columns has no ties, so
            // the order (and a following LIMIT) is deterministic and can be compared
            // ORDERED against SQLite — exercising the SELECT → DISTINCT → ORDER BY →
            // LIMIT pipeline (dedup must precede ORDER BY/LIMIT), a combo the plain
            // arm below never generates.
            if distinct {
                let nn = schema.nn_cols();
                if !nn.is_empty() && rng.chance(60) {
                    let take = (1 + rng.below(3)).min(nn.len());
                    let mut names: Vec<String> = nn.iter().map(|c| c.name.to_string()).collect();
                    for i in 0..take {
                        let j = i + rng.below(names.len() - i);
                        names.swap(i, j);
                    }
                    names.truncate(take);
                    let dproj = names.join(", ");
                    let order_keys: Vec<String> = names
                        .iter()
                        .map(|nm| format!("{nm} {}", if rng.chance(50) { "ASC" } else { "DESC" }))
                        .collect();
                    let limit = if rng.chance(50) {
                        format!(" LIMIT {}", rng.int(1, rows.max(1) as i64))
                    } else {
                        String::new()
                    };
                    return (
                        format!(
                            "SELECT DISTINCT {dproj} FROM t{w} ORDER BY {}{limit}",
                            order_keys.join(", ")
                        ),
                        true, // ordered comparison
                    );
                }
                return (format!("SELECT DISTINCT {proj} FROM t{w}"), false);
            }
            let has_order = rng.chance(70);
            let order = if has_order {
                format!(" {}", gen_orderby(schema, rng))
            } else {
                String::new()
            };
            let limit = if has_order && rng.chance(45) {
                let off = if rng.chance(40) {
                    format!(" OFFSET {}", rng.int(0, rows as i64))
                } else {
                    String::new()
                };
                format!(" LIMIT {}{off}", rng.int(1, rows as i64))
            } else {
                String::new()
            };
            (format!("SELECT {proj} FROM t{w}{order}{limit}"), has_order)
        }
    }
}

// ─── Mutations ────────────────────────────────────────────────────────────────
fn gen_mutation(schema: &Schema, rng: &mut Rng, next_id: &mut i64) -> String {
    match rng.below(3) {
        0 => {
            // INSERT a fresh row (unique id, NOT NULL respected by gen_value).
            let names: Vec<&str> = schema.cols.iter().map(|c| c.name).collect();
            let mut cells = Vec::with_capacity(schema.cols.len());
            for (i, c) in schema.cols.iter().enumerate() {
                cells.push(if i == 0 {
                    next_id.to_string()
                } else {
                    gen_value(rng, c)
                });
            }
            *next_id += 1;
            format!(
                "INSERT INTO t ({}) VALUES ({})",
                names.join(","),
                cells.join(",")
            )
        }
        1 => {
            // UPDATE a non-id column (respecting NOT NULL) on a predicate subset.
            let targets: Vec<&Col> = schema.cols.iter().skip(1).collect();
            let c = *rng.pick(&targets);
            format!(
                "UPDATE t SET {} = {} WHERE {}",
                c.name,
                gen_value(rng, c),
                gen_predicate(schema, rng, 1)
            )
        }
        _ => format!("DELETE FROM t WHERE {}", gen_predicate(schema, rng, 1)),
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

// ─── Execution wrappers (panic-safe) ──────────────────────────────────────────
/// Run a SELECT on Nucleus. Errors are returned as `Err`; panics are returned as
/// `Err("PANIC: ..")` so the caller can treat them as always-fatal bugs.
fn run_nucleus(ex: &Executor, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let rt = tokio::runtime::Handle::current();
    let res = std::panic::catch_unwind(AssertUnwindSafe(|| {
        tokio::task::block_in_place(|| rt.block_on(ex.execute(sql)))
    }));
    match res {
        Ok(Ok(mut results)) => match results.pop() {
            Some(ExecResult::Select { rows, .. }) => Ok(rows
                .iter()
                .map(|r| r.iter().map(canon_nucleus).collect())
                .collect()),
            _ => Err("non-select result".into()),
        },
        Ok(Err(e)) => Err(format!("{e:?}")),
        Err(p) => Err(format!("PANIC: {}", panic_msg(&p))),
    }
}

/// Run a non-SELECT (DML/DDL) on Nucleus. `Ok(())` on any success.
fn exec_nucleus(ex: &Executor, sql: &str) -> Result<(), String> {
    let rt = tokio::runtime::Handle::current();
    let res = std::panic::catch_unwind(AssertUnwindSafe(|| {
        tokio::task::block_in_place(|| rt.block_on(ex.execute(sql)))
    }));
    match res {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => Err(format!("{e:?}")),
        Err(p) => Err(format!("PANIC: {}", panic_msg(&p))),
    }
}

fn panic_msg(p: &(dyn std::any::Any + Send)) -> String {
    p.downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| p.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown".into())
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

/// Full table snapshot, ordered by id — the post-mutation invariant.
fn snapshot(schema: &Schema) -> String {
    let cols: Vec<&str> = schema.cols.iter().map(|c| c.name).collect();
    format!("SELECT {} FROM t ORDER BY id ASC", cols.join(", "))
}

/// Reconstruct a replayable INSERT from a full-table snapshot (canon cells in
/// schema column order, ordered by id). The original `inserts` no longer
/// describes the live table once mutations have run, so a faithful repro must
/// dump the *current* rows. canon text cells are already valid `'..'` literals
/// and numbers are bare; only NULL (`∅`) needs translating.
fn dump_inserts(schema: &Schema, rows: &[Vec<String>]) -> String {
    if rows.is_empty() {
        return "-- (table empty after mutations)".into();
    }
    let names: Vec<&str> = schema.cols.iter().map(|c| c.name).collect();
    let tuples: Vec<String> = rows
        .iter()
        .map(|r| {
            let cells: Vec<String> = r
                .iter()
                .map(|c| {
                    if c == "∅" {
                        "NULL".to_string()
                    } else {
                        c.clone()
                    }
                })
                .collect();
            format!("({})", cells.join(","))
        })
        .collect();
    format!(
        "INSERT INTO t ({}) VALUES {}",
        names.join(","),
        tuples.join(",")
    )
}

/// Monotonic suffix so concurrently-live temp directories never collide.
static DIR_SEQ: AtomicU64 = AtomicU64::new(0);

/// One Nucleus instance under test, owning any on-disk directory it needs.
///
/// Dropping this closes the engine *before* removing the directory — a paged
/// engine still holds WAL and data-file handles until its `HarnessDb` is gone.
struct NucleusUnderTest {
    ex: Arc<Executor>,
    db: Option<HarnessDb>,
    dir: Option<PathBuf>,
}

impl Drop for NucleusUnderTest {
    fn drop(&mut self) {
        self.db.take();
        if let Some(dir) = &self.dir {
            let _ = std::fs::remove_dir_all(dir);
        }
    }
}

/// Open a fresh Nucleus for one candidate.
///
/// `None` keeps the historical path — a bare in-process `MvccStorageAdapter`
/// with no persistence — so an unflagged run is byte-for-byte the oracle it has
/// always been. `Some(kind)` routes through the same [`HarnessDb`] the scale
/// harnesses use, which is what makes `--engine buffered-disk` measure the
/// engine `nucleus serve` constructs rather than a RAM stand-in for it.
fn open_nucleus(kind: Option<EngineKind>) -> Option<NucleusUnderTest> {
    let Some(kind) = kind else {
        let cat = Arc::new(Catalog::new());
        let st: Arc<dyn StorageEngine> = Arc::new(MvccStorageAdapter::new());
        return Some(NucleusUnderTest {
            ex: Arc::new(Executor::new(cat, st)),
            db: None,
            dir: None,
        });
    };
    let dir = std::env::temp_dir().join(format!(
        "nucleus-fuzz-{}-{}",
        std::process::id(),
        DIR_SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let rt = tokio::runtime::Handle::current();
    let db = tokio::task::block_in_place(|| {
        rt.block_on(HarnessDb::open(kind, &dir, EngineConfig::default()))
    })
    .ok()?;
    Some(NucleusUnderTest {
        ex: db.executor().clone(),
        db: Some(db),
        dir: Some(dir),
    })
}

/// Replay an op sequence on a fresh Nucleus + fresh SQLite, run `q` on both,
/// and report whether they diverge. `None` if the candidate is structurally
/// invalid (some op errors on either engine) — such a candidate is unusable for
/// minimization. SELECTs in `ops` are run for side effects (cache priming).
fn replay_diverges(
    ops: &[String],
    q: &str,
    ordered: bool,
    kind: Option<EngineKind>,
) -> Option<bool> {
    let nut = open_nucleus(kind)?;
    let ex = nut.ex.clone();
    let sqlite = Connection::open_in_memory().ok()?;
    for op in ops {
        let is_select = op.trim_start().to_uppercase().starts_with("SELECT");
        let nr = if is_select {
            run_nucleus(&ex, op).map(|_| ())
        } else {
            exec_nucleus(&ex, op)
        };
        if nr.is_err() {
            return None;
        }
        if sqlite.execute_batch(op).is_err() {
            // SELECTs aren't valid for execute_batch on some shapes; ignore
            // read failures, but a failed DDL/DML invalidates the candidate.
            if !is_select {
                return None;
            }
        }
    }
    match (run_nucleus(&ex, q), run_sqlite(&sqlite, q)) {
        (Ok(x), Ok(y)) => Some(!compare(x, y, ordered)),
        _ => None,
    }
}

/// Delta-debug an op sequence down to a minimal subsequence that still makes
/// `q` diverge. Op 0 (CREATE TABLE) is always kept. Greedy single-op removal to
/// a fixpoint — small op counts make this cheap and the result is a drop-in repro.
fn minimize(ops: &[String], q: &str, ordered: bool, kind: Option<EngineKind>) -> Vec<String> {
    let mut cur: Vec<String> = ops.to_vec();
    let mut changed = true;
    while changed {
        changed = false;
        let mut i = 1;
        while i < cur.len() {
            let mut cand = cur.clone();
            cand.remove(i);
            if replay_diverges(&cand, q, ordered, kind) == Some(true) {
                cur = cand;
                changed = true;
            } else {
                i += 1;
            }
        }
    }
    cur
}

fn main_impl() {
    let mut seed: u64 = 0x1234_5678;
    let mut iterations: usize = 2000;
    let mut max_report: usize = 15;
    let mut queries_per: usize = 25;
    // 0 = default random (8..47 rows). Set high (e.g. 9000) to cross the
    // 8192-row zone-map granule boundary and exercise multi-granule pruning.
    let mut fixed_rows: usize = 0;
    // None = the historical in-process MvccStorageAdapter (RAM, no WAL).
    let mut engine: Option<EngineKind> = None;
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
            "--rows" => {
                i += 1;
                fixed_rows = args[i].parse().unwrap();
            }
            "--engine" => {
                i += 1;
                match EngineKind::parse(&args[i]) {
                    Some(k) => engine = Some(k),
                    None => {
                        let names: Vec<&str> = EngineKind::ALL.iter().map(|k| k.name()).collect();
                        println!(
                            "unknown --engine {:?}; expected one of: {}",
                            args[i],
                            names.join(", ")
                        );
                        std::process::exit(2);
                    }
                }
            }
            _ => {}
        }
        i += 1;
    }

    // Silence the default panic printer; catch_unwind captures the message.
    std::panic::set_hook(Box::new(|_| {}));

    let engine_label = match engine {
        Some(k) => k.name(),
        None => "mvcc (in-process, no WAL)",
    };
    println!("Nucleus ⇄ SQLite differential fuzzer (schema+DML+expr coverage)");
    println!("seed={seed} iterations={iterations} queries/iter={queries_per}");
    println!("engine={engine_label}");
    if engine.is_none_or(|k| !k.has_buffer_pool()) {
        println!(
            "NOTE: this engine has no buffer pool or paged storage, so nothing \
             below covers DiskEngine.\n      Pass --engine buffered-disk to \
             aim the oracle at what `nucleus serve` runs."
        );
    }
    println!();

    let mut total = 0usize;
    let mut divergences = 0usize;
    let mut panics = 0usize;
    let mut nuc_errors = 0usize;

    'outer: for iter in 0..iterations {
        let mut rng = Rng(seed.wrapping_add(iter as u64).wrapping_mul(0x100000001B3));
        let schema = Schema::random(&mut rng);
        let rows = if fixed_rows > 0 {
            fixed_rows
        } else {
            8 + rng.below(40)
        };
        let ddl = schema.ddl();
        let inserts = gen_inserts(&schema, &mut rng, rows);
        let mut next_id = rows as i64 + 1;

        let Some(nut) = open_nucleus(engine) else {
            println!("FAIL: could not open engine {engine_label} for iteration {iter}");
            std::process::exit(1);
        };
        let ex = nut.ex.clone();
        let sqlite = Connection::open_in_memory().unwrap();

        // Replayable op-log for faithful repros. WHERE-filtered scans can go
        // wrong only after a *sequence* of mutations (the bug lives in
        // incrementally-maintained MVCC/index state, not the final rows), so a
        // dump of final state alone does not reproduce — we must replay every
        // applied mutation in order.
        let mut ops: Vec<String> = vec![ddl.clone(), inserts.clone()];

        // Setup must succeed on both.
        for stmt in [&ddl, &inserts] {
            if let Err(e) = exec_nucleus(&ex, stmt) {
                if e.starts_with("PANIC:") {
                    report_panic(&mut panics, max_report, iter, stmt, &e, &ddl, &inserts);
                }
                eprintln!("[setup] nucleus failed: {e}\n  {stmt}");
                continue 'outer;
            }
            if sqlite.execute_batch(stmt).is_err() {
                continue 'outer;
            }
        }

        for _ in 0..queries_per {
            total += 1;

            // ~30% of steps mutate, then assert full state matches.
            if rng.chance(30) {
                let m = gen_mutation(&schema, &mut rng, &mut next_id);
                let nr = exec_nucleus(&ex, &m);
                if let Err(e) = &nr
                    && e.starts_with("PANIC:")
                {
                    report_panic(&mut panics, max_report, iter, &m, e, &ddl, &inserts);
                    if panics > max_report {
                        std::process::exit(1);
                    }
                    continue 'outer;
                }
                let sr = sqlite.execute_batch(&m);
                match (nr, sr) {
                    (Ok(()), Ok(())) => {
                        // Both applied — record it so a later read divergence
                        // can be replayed exactly, then assert state matches.
                        ops.push(m.clone());
                        let snap = snapshot(&schema);
                        let ns = run_nucleus(&ex, &snap);
                        let ss = run_sqlite(&sqlite, &snap);
                        if let (Ok(a), Ok(b)) = (ns, ss)
                            && !compare(a.clone(), b.clone(), true)
                        {
                            divergences += 1;
                            if divergences <= max_report {
                                println!(
                                    "─── STATE DIVERGENCE after mutation #{divergences} (iter {iter}, seed {seed}) ───"
                                );
                                println!("  schema : {ddl}");
                                println!("  data   : {inserts}");
                                println!("  mutation: {m}");
                                println!("  nucleus({} rows): {}", a.len(), preview(&a));
                                println!("  sqlite ({} rows): {}", b.len(), preview(&b));
                                println!();
                            }
                        }
                    }
                    (Err(_), Err(_)) => { /* both rejected — state unchanged */ }
                    (Err(_e), Ok(())) => {
                        // SQLite applied, Nucleus didn't → state now divergent. Stop.
                        nuc_errors += 1;
                        continue 'outer;
                    }
                    (Ok(()), Err(_)) => continue 'outer,
                }
                continue;
            }

            let (q, ordered) = gen_query(&schema, &mut rng, rows);
            let n = run_nucleus(&ex, &q);
            let s = run_sqlite(&sqlite, &q);
            // A read can leave behind cache state that a later mutation fails to
            // invalidate, so intervening queries are part of a faithful repro,
            // not just mutations. Record every query that ran cleanly on nucleus.
            let n_ok = n.is_ok();
            match (n, s) {
                (Ok(nr), Ok(sr)) => {
                    if !compare(nr.clone(), sr.clone(), ordered) {
                        divergences += 1;
                        if divergences <= max_report {
                            // Is the LIVE table identical at query time? If yes, the
                            // divergence is a read-path bug, not a desynced mutation.
                            let snap = snapshot(&schema);
                            let (live_n, live_s) =
                                (run_nucleus(&ex, &snap), run_sqlite(&sqlite, &snap));
                            let live_match = matches!((&live_n, &live_s), (Ok(x), Ok(y)) if compare(x.clone(), y.clone(), true));
                            let live_data = match (&live_s, &live_n) {
                                (Ok(rows), _) => dump_inserts(&schema, rows),
                                (_, Ok(rows)) => dump_inserts(&schema, rows),
                                _ => inserts.clone(),
                            };
                            // Re-run the exact query: does it still diverge (persistent)
                            // or was it transient (cache)?
                            let rerun_match = match (run_nucleus(&ex, &q), run_sqlite(&sqlite, &q))
                            {
                                (Ok(x), Ok(y)) => compare(x, y, ordered),
                                _ => false,
                            };
                            // Path-dependence probe: rebuild a fresh table from the
                            // *final* rows and run q. If that matches sqlite, the
                            // bug needs the mutation history (MVCC/index state),
                            // not just the rows — so the op-log below is the repro.
                            let fresh_match = {
                                let fresh = open_nucleus(engine);
                                let fx = match &fresh {
                                    Some(n) => n.ex.clone(),
                                    None => break 'outer,
                                };
                                let ok = exec_nucleus(&fx, &ddl).is_ok()
                                    && exec_nucleus(&fx, &live_data).is_ok();
                                if ok {
                                    match (run_nucleus(&fx, &q), run_sqlite(&sqlite, &q)) {
                                        (Ok(x), Ok(y)) => Some(compare(x, y, ordered)),
                                        _ => None,
                                    }
                                } else {
                                    None
                                }
                            };
                            // Op-log replay: a fresh executor fed the exact recorded
                            // op sequence, then q. If this MATCHES sqlite while the
                            // live executor does not, the trigger is something the
                            // op-log omits (e.g. the post-mutation snapshot probes).
                            let oplog_match = {
                                let replay = open_nucleus(engine);
                                let rx = match &replay {
                                    Some(n) => n.ex.clone(),
                                    None => break 'outer,
                                };
                                let mut ok = true;
                                for op in &ops {
                                    let is_select =
                                        op.trim_start().to_uppercase().starts_with("SELECT");
                                    let r = if is_select {
                                        run_nucleus(&rx, op).map(|_| ())
                                    } else {
                                        exec_nucleus(&rx, op)
                                    };
                                    if r.is_err() {
                                        ok = false;
                                        break;
                                    }
                                }
                                if ok {
                                    match (run_nucleus(&rx, &q), run_sqlite(&sqlite, &q)) {
                                        (Ok(x), Ok(y)) => Some(compare(x, y, ordered)),
                                        _ => None,
                                    }
                                } else {
                                    None
                                }
                            };
                            println!(
                                "─── DIVERGENCE #{divergences} (iter {iter}, seed {seed}) ───"
                            );
                            println!("  query  : {q}");
                            println!("  schema : {ddl}");
                            println!("  data   : {live_data}");
                            let (mut a, mut b) = (nr, sr);
                            if !ordered {
                                a.sort();
                                b.sort();
                            }
                            println!("  nucleus({} rows): {}", a.len(), preview(&a));
                            println!("  sqlite ({} rows): {}", b.len(), preview(&b));
                            println!(
                                "  live-state match: {live_match} | rerun match: {rerun_match} | fresh-rebuild: {} | oplog-replay: {}",
                                match fresh_match {
                                    Some(true) => "match",
                                    Some(false) => "diverge",
                                    None => "n/a",
                                },
                                match oplog_match {
                                    Some(true) => "MATCH (op-log INSUFFICIENT)",
                                    Some(false) => "diverge (op-log reproduces)",
                                    None => "n/a",
                                }
                            );
                            // Minimal replayable sequence — delta-debugged when the
                            // op-log reproduces, else the full sequence.
                            let repro = if oplog_match == Some(false) {
                                minimize(&ops, &q, ordered, engine)
                            } else {
                                ops.clone()
                            };
                            println!(
                                "  ── repro ({} ops, minimized from {}) ──",
                                repro.len(),
                                ops.len()
                            );
                            for op in &repro {
                                println!("    {op};");
                            }
                            println!("    {q};");
                            println!();
                        }
                    }
                }
                (Err(ne), Ok(_)) => {
                    if ne.starts_with("PANIC:") {
                        report_panic(&mut panics, max_report, iter, &q, &ne, &ddl, &inserts);
                        if panics > max_report {
                            std::process::exit(1);
                        }
                    } else {
                        nuc_errors += 1;
                        if nuc_errors <= max_report {
                            println!("─── NUCLEUS-ERROR (iter {iter}) ─── {q}\n    err: {ne}\n");
                        }
                    }
                }
                _ => {}
            }
            if n_ok {
                ops.push(q.clone());
            }
        }
    }

    println!("\n════ SUMMARY ════");
    println!("queries/mutations  : {total}");
    println!("RESULT divergences : {divergences}");
    println!("PANICS             : {panics}");
    println!("nucleus-only errors: {nuc_errors} (SQLite accepted; may be unsupported features)");
    if divergences == 0 && panics == 0 {
        println!("\nNo divergences, no panics vs SQLite. 🎯");
    } else {
        println!("\nReproduce with: --seed {seed} --iterations <iter+1>");
        std::process::exit(1);
    }
}

#[allow(clippy::too_many_arguments)]
fn report_panic(
    panics: &mut usize,
    max_report: usize,
    iter: usize,
    sql: &str,
    err: &str,
    ddl: &str,
    inserts: &str,
) {
    *panics += 1;
    if *panics <= max_report {
        println!("─── PANIC #{panics} (iter {iter}) ───");
        println!("  sql    : {sql}");
        println!("  schema : {ddl}");
        println!("  data   : {inserts}");
        println!("  {err}\n");
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

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    main_impl();
}
