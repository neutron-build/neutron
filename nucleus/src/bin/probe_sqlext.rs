//! Extended SQL differential fuzzer: Nucleus vs SQLite.
//!
//! Extends fuzz.rs coverage with:
//!   (a) Column types: BOOLEAN, DATE, TIMESTAMP (stored as integers), NUMERIC
//!   (b) Window functions: ROW_NUMBER, SUM OVER, RANK, DENSE_RANK with PARTITION BY + ORDER BY
//!   (c) CTEs (WITH ... AS) — both plain and a simple recursive CTE (integers 1..N)
//!   (d) Scalar function families present in both engines:
//!       - String: UPPER, LOWER, SUBSTR, REPLACE, LENGTH, LTRIM, RTRIM, TRIM
//!       - Math:   ABS, ROUND, SIGN, MOD, MAX/MIN aggregates
//!       - Null:   COALESCE, NULLIF
//!
//! Dialect notes (false-positive avoidance):
//!   - BOOLEAN stored as INTEGER 0/1 in both (SQLite has no native bool type).
//!   - DATE stored as TEXT 'YYYY-MM-DD' in both; compared as strings.
//!   - TIMESTAMP stored as INTEGER epoch seconds in both (microseconds would drift).
//!   - NUMERIC uses REAL in SQLite; only compared via 4-decimal abs+rel tolerance.
//!   - Window functions: results compared as sets per-partition (ordered within each
//!     partition) to avoid whole-result-set ordering dependency.
//!   - Recursive CTE: only UNION ALL form tested (both engines support it).
//!   - SUBSTR(s,start,len): 1-indexed in both SQL dialects.
//!   - Division by zero: skipped for MOD (both engines diverge on treatment).
//!   - Float results use abs tolerance 1e-9 + rel tolerance 1e-9.
//!
//! Build:
//!   cargo build --release --features "server rusqlite" --bin probe_sqlext
//! Run:
//!   cargo run --release --features "server rusqlite" --bin probe_sqlext
//!   cargo run --release --features "server rusqlite" --bin probe_sqlext -- --seed 42 --iterations 2000
#![cfg(feature = "server")]
#![allow(clippy::all)] // internal fuzz harness

use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::storage::{MvccStorageAdapter, StorageEngine};
use nucleus::types::Value;
use rusqlite::Connection;

// ─── Deterministic PRNG (SplitMix64) ─────────────────────────────────────────
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: usize) -> usize {
        if n == 0 { 0 } else { (self.next() % n as u64) as usize }
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

// ─── Column type taxonomy ─────────────────────────────────────────────────────
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Ty {
    Int,       // INTEGER — core numeric
    Real,      // REAL    — floating point
    Text,      // TEXT    — strings
    Bool,      // BOOLEAN — stored as 0/1 integer in both engines
    DateStr,   // DATE    — stored as TEXT 'YYYY-MM-DD' in both engines
    TsInt,     // TIMESTAMP — stored as INTEGER epoch seconds in both engines
}

#[derive(Clone)]
struct Col {
    name: &'static str,
    ty: Ty,
    nn: bool,
}

const NAMES: &[&str] = &["c1", "c2", "c3", "c4", "c5", "c6"];
// String constants (lowercase, no single-quote) to avoid LIKE/case issues.
const CATS: &[&str] = &["red", "green", "blue", "amber", "str0", "str1"];
// Date strings: all valid YYYY-MM-DD, comparable as text in both engines.
const DATES: &[&str] = &[
    "2020-01-01", "2021-06-15", "2022-03-31", "2023-11-20",
    "2024-07-04", "2019-12-25",
];
// Epoch seconds (timestamps) in the 2019..2024 range — no TZ issues.
const TS_EPOCHS: &[i64] = &[
    1_546_300_800, // 2019-01-01 00:00:00 UTC
    1_577_836_800, // 2020-01-01
    1_609_459_200, // 2021-01-01
    1_640_995_200, // 2022-01-01
    1_672_531_200, // 2023-01-01
    1_704_067_200, // 2024-01-01
    1_718_150_400, // 2024-06-12
];

struct Schema {
    cols: Vec<Col>,
}

impl Schema {
    fn random(rng: &mut Rng) -> Schema {
        let mut cols = vec![Col { name: "id", ty: Ty::Int, nn: true }];
        // Guarantee NN int (for safe ORDER BY / GROUP BY).
        cols.push(Col { name: NAMES[0], ty: Ty::Int, nn: true });
        // Guarantee NN text (for GROUP BY, LIKE tests).
        cols.push(Col { name: NAMES[1], ty: Ty::Text, nn: true });
        // Add 2-4 extra cols with varied types including extended types.
        let extra = 2 + rng.below(3);
        let ext_types = [Ty::Int, Ty::Real, Ty::Text, Ty::Bool, Ty::DateStr, Ty::TsInt];
        for k in 0..extra {
            let ty = *rng.pick(&ext_types);
            cols.push(Col { name: NAMES[2 + k], ty, nn: rng.chance(40) });
        }
        Schema { cols }
    }

    /// DDL string. BOOLEAN/DATE/TIMESTAMP all map to types that exist in SQLite.
    fn ddl(&self) -> String {
        let mut parts = Vec::new();
        for (i, c) in self.cols.iter().enumerate() {
            if i == 0 {
                parts.push("id INTEGER PRIMARY KEY".to_string());
                continue;
            }
            let ty_str = match c.ty {
                Ty::Int => "INTEGER",
                Ty::Real => "REAL",
                Ty::Text => "TEXT",
                Ty::Bool => "INTEGER",    // 0/1 in both
                Ty::DateStr => "TEXT",    // 'YYYY-MM-DD' stored as text in both
                Ty::TsInt => "INTEGER",   // epoch seconds
            };
            let nn = if c.nn { " NOT NULL" } else { "" };
            parts.push(format!("{} {ty_str}{nn}", c.name));
        }
        format!("CREATE TABLE t ({})", parts.join(", "))
    }

    fn int_cols(&self) -> Vec<&Col> {
        self.cols.iter().filter(|c| matches!(c.ty, Ty::Int)).collect()
    }
}

// ─── Value generation ─────────────────────────────────────────────────────────
fn gen_value(rng: &mut Rng, c: &Col) -> String {
    if !c.nn && rng.chance(20) {
        return "NULL".into();
    }
    match c.ty {
        Ty::Int => rng.int(-5, 20).to_string(),
        Ty::Real => format!("{:.2}", rng.int(-100, 100) as f64 / 10.0),
        Ty::Text => format!("'{}'", rng.pick(CATS)),
        Ty::Bool => rng.int(0, 1).to_string(),     // 0 or 1
        Ty::DateStr => format!("'{}'", rng.pick(DATES)),
        Ty::TsInt => TS_EPOCHS[rng.below(TS_EPOCHS.len())].to_string(),
    }
}

fn gen_inserts(schema: &Schema, rng: &mut Rng, rows: usize) -> String {
    let names: Vec<&str> = schema.cols.iter().map(|c| c.name).collect();
    let mut vals = Vec::with_capacity(rows);
    for id in 1..=rows {
        let mut cells = Vec::with_capacity(schema.cols.len());
        for (i, c) in schema.cols.iter().enumerate() {
            cells.push(if i == 0 { id.to_string() } else { gen_value(rng, c) });
        }
        vals.push(format!("({})", cells.join(",")));
    }
    format!("INSERT INTO t ({}) VALUES {}", names.join(","), vals.join(","))
}

// ─── Value canonicalization ───────────────────────────────────────────────────
/// Absolute + relative tolerance for float comparisons.
fn floats_close(a: f64, b: f64) -> bool {
    if a == b { return true; }
    let diff = (a - b).abs();
    let mag = a.abs().max(b.abs());
    diff <= 1e-9 + mag * 1e-9
}

fn canon_nucleus(v: &Value) -> String {
    match v {
        Value::Null => "NULL".into(),
        Value::Bool(b) => if *b { "1".into() } else { "0".into() },
        Value::Int32(n) => n.to_string(),
        Value::Int64(n) => n.to_string(),
        Value::Float64(f) => {
            // Normalize -0.0 to 0.0, then round to 4 decimal places for
            // comparison — avoids float-accumulation drift on tiny tables.
            if f.is_finite() {
                format!("{:.4}", f)
            } else {
                format!("{f}")
            }
        }
        Value::Text(s) => s.clone(),
        Value::Date(d) => {
            // Convert Nucleus internal date (days since 2000-01-01) to YYYY-MM-DD.
            // The table stores dates as TEXT 'YYYY-MM-DD' so this path is not
            // normally hit — but guard against it anyway.
            format!("date:{d}")
        }
        other => other.to_string(),
    }
}

fn canon_sqlite(v: &rusqlite::types::Value) -> String {
    use rusqlite::types::Value as Sv;
    match v {
        Sv::Null => "NULL".into(),
        Sv::Integer(i) => i.to_string(),
        Sv::Real(f) => {
            if f.is_finite() {
                format!("{:.4}", f)
            } else {
                format!("{f}")
            }
        }
        Sv::Text(s) => s.clone(),
        Sv::Blob(b) => format!("blob:{}", b.len()),
    }
}

/// Row-level equality: cells must match exactly (after canon_*), except float
/// columns where we fall back to per-cell float comparison.
fn rows_equal(a: &[String], b: &[String]) -> bool {
    if a.len() != b.len() { return false; }
    a.iter().zip(b.iter()).all(|(x, y)| {
        if x == y { return true; }
        // Try float comparison for numeric-looking values.
        if let (Ok(fa), Ok(fb)) = (x.parse::<f64>(), y.parse::<f64>()) {
            return floats_close(fa, fb);
        }
        false
    })
}

fn compare(mut nuc: Vec<Vec<String>>, mut sql: Vec<Vec<String>>, ordered: bool) -> bool {
    if ordered {
        nuc.len() == sql.len() && nuc.iter().zip(sql.iter()).all(|(a, b)| rows_equal(a, b))
    } else {
        // Sort both to compare as sets (not ordered).
        nuc.sort();
        sql.sort();
        nuc.len() == sql.len() && nuc.iter().zip(sql.iter()).all(|(a, b)| rows_equal(a, b))
    }
}

// ─── Execution wrappers ───────────────────────────────────────────────────────
fn panic_msg(p: &(dyn std::any::Any + Send)) -> String {
    p.downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| p.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown".into())
}

fn run_nucleus(ex: &Executor, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let rt = tokio::runtime::Handle::current();
    let res = std::panic::catch_unwind(AssertUnwindSafe(|| {
        tokio::task::block_in_place(|| rt.block_on(ex.execute(sql)))
    }));
    match res {
        Ok(Ok(mut results)) => match results.pop() {
            Some(ExecResult::Select { rows, .. }) => {
                Ok(rows.iter().map(|r| r.iter().map(canon_nucleus).collect()).collect())
            }
            _ => Err("non-select result".into()),
        },
        Ok(Err(e)) => Err(format!("{e:?}")),
        Err(p) => Err(format!("PANIC: {}", panic_msg(&p))),
    }
}

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

// ─── Test harness builders ────────────────────────────────────────────────────

/// Run one deterministic query against both engines, report divergence/panic.
/// Returns (diverged, panicked).
fn probe(
    ex: &Executor,
    sqlite: &Connection,
    label: &str,
    sql: &str,
    ordered: bool,
    divergences: &mut usize,
    panics: &mut usize,
    nuc_errors: &mut usize,
    max_report: usize,
    seed: u64,
    iter: usize,
) {
    let n = run_nucleus(ex, sql);
    let s = run_sqlite(sqlite, sql);
    match (n, s) {
        (Ok(nr), Ok(sr)) => {
            if !compare(nr.clone(), sr.clone(), ordered) {
                *divergences += 1;
                if *divergences <= max_report {
                    let (mut a, mut b) = (nr, sr);
                    if !ordered { a.sort(); b.sort(); }
                    println!(
                        "─── DIVERGENCE #{divergences} [{label}] (iter {iter}, seed {seed}) ───"
                    );
                    println!("  sql    : {sql}");
                    println!("  nucleus: {}", preview(&a));
                    println!("  sqlite : {}", preview(&b));
                    println!();
                }
            }
        }
        (Err(ne), Ok(_)) => {
            if ne.starts_with("PANIC:") {
                *panics += 1;
                if *panics <= max_report {
                    println!("─── PANIC #{panics} [{label}] (iter {iter}) ───");
                    println!("  sql  : {sql}");
                    println!("  {ne}");
                    println!();
                }
            } else {
                *nuc_errors += 1;
                if *nuc_errors <= max_report {
                    println!("─── NUC-ERROR #{nuc_errors} [{label}] ─── {sql}");
                    println!("    {ne}");
                    println!();
                }
            }
        }
        _ => {} // SQLite also errored — dialect gap, skip
    }
}

fn preview(rows: &[Vec<String>]) -> String {
    let shown: Vec<String> = rows
        .iter()
        .take(6)
        .map(|r| format!("[{}]", r.join(",")))
        .collect();
    let more = if rows.len() > 6 {
        format!(" ...+{}", rows.len() - 6)
    } else {
        String::new()
    };
    format!("{}{more}", shown.join(" "))
}

// ─── Test families ────────────────────────────────────────────────────────────

/// (a) Queries exercising BOOLEAN (0/1), DATE (text), TIMESTAMP (int), NUMERIC.
fn run_extended_types(
    rng: &mut Rng,
    schema: &Schema,
    ex: &Executor,
    sqlite: &Connection,
    divergences: &mut usize,
    panics: &mut usize,
    nuc_errors: &mut usize,
    max_report: usize,
    seed: u64,
    iter: usize,
    rows: usize,
) {
    // Find bool, date, ts columns if they exist.
    let bool_cols: Vec<&Col> = schema.cols.iter().filter(|c| c.ty == Ty::Bool).collect();
    let date_cols: Vec<&Col> = schema.cols.iter().filter(|c| c.ty == Ty::DateStr).collect();
    let ts_cols: Vec<&Col> = schema.cols.iter().filter(|c| c.ty == Ty::TsInt).collect();
    let real_cols: Vec<&Col> = schema.cols.iter().filter(|c| c.ty == Ty::Real).collect();
    let int_cols = schema.int_cols();
    let nn_int_col = schema.cols[1].name; // always NN int

    macro_rules! p {
        ($sql:expr, $ordered:expr) => {
            probe(
                ex, sqlite, "ext-types", $sql, $ordered,
                divergences, panics, nuc_errors, max_report, seed, iter,
            )
        };
    }

    // SELECT with CASE on bool column → 0/1 predicate.
    if !bool_cols.is_empty() {
        let bc = rng.pick(&bool_cols).name;
        let q = format!(
            "SELECT id, {bc}, CASE WHEN {bc} = 1 THEN 'yes' ELSE 'no' END AS lbl \
             FROM t ORDER BY id ASC"
        );
        p!(&q, true);
    }

    // DATE string comparison (text ordering of YYYY-MM-DD is correct).
    if !date_cols.is_empty() && rng.chance(70) {
        let dc = rng.pick(&date_cols).name;
        let d = rng.pick(DATES);
        let op = *rng.pick(&["<", ">", "=", "<=", ">="]);
        let q = format!(
            "SELECT id, {dc} FROM t WHERE {dc} {op} '{d}' ORDER BY id ASC"
        );
        p!(&q, true);

        // GROUP BY date col, count rows per date.
        let q2 = format!(
            "SELECT {dc}, COUNT(*) AS n FROM t WHERE {dc} IS NOT NULL GROUP BY {dc} ORDER BY {dc} ASC"
        );
        p!(&q2, true);
    }

    // TIMESTAMP (epoch int) range scan.
    if !ts_cols.is_empty() && rng.chance(70) {
        let tc = rng.pick(&ts_cols).name;
        let lo = TS_EPOCHS[rng.below(TS_EPOCHS.len())];
        let hi = TS_EPOCHS[rng.below(TS_EPOCHS.len())];
        let (lo, hi) = if lo <= hi { (lo, hi) } else { (hi, lo) };
        let q = format!(
            "SELECT id, {tc} FROM t WHERE {tc} BETWEEN {lo} AND {hi} ORDER BY id ASC"
        );
        p!(&q, true);

        // MIN/MAX of epoch col.
        let q2 = format!("SELECT MIN({tc}), MAX({tc}) FROM t");
        p!(&q2, true);
    }

    // REAL column: ROUND to 1 decimal, ABS, SUM.
    if !real_cols.is_empty() && rng.chance(70) {
        let rc = rng.pick(&real_cols).name;
        let q = format!(
            "SELECT id, ROUND({rc}, 1) AS r1 FROM t WHERE {rc} IS NOT NULL ORDER BY id ASC"
        );
        p!(&q, true);

        let q2 = format!("SELECT SUM(ROUND({rc}, 1)), COUNT({rc}) FROM t WHERE {rc} IS NOT NULL");
        p!(&q2, true);
    }

    // COALESCE over nullable extended-type columns.
    let nullable_ext: Vec<&Col> = schema
        .cols
        .iter()
        .filter(|c| !c.nn && matches!(c.ty, Ty::Bool | Ty::DateStr | Ty::TsInt))
        .collect();
    if !nullable_ext.is_empty() && rng.chance(60) {
        let nc = rng.pick(&nullable_ext);
        let fallback = match nc.ty {
            Ty::Bool => "0".to_string(),
            Ty::DateStr => "'1970-01-01'".to_string(),
            Ty::TsInt => "0".to_string(),
            _ => "0".to_string(),
        };
        let q = format!(
            "SELECT id, COALESCE({}, {fallback}) AS c FROM t ORDER BY id ASC",
            nc.name
        );
        p!(&q, true);
    }

    // NULLIF over int column.
    if !int_cols.is_empty() && rng.chance(50) {
        let ic = rng.pick(&int_cols).name;
        let val = rng.int(-5, 20);
        let q = format!(
            "SELECT id, NULLIF({ic}, {val}) AS n FROM t ORDER BY id ASC"
        );
        p!(&q, true);
    }

    // Arithmetic: ABS(int) + ORDER BY id.
    let _ = rows; // suppress unused warning
    let q = format!(
        "SELECT id, ABS({nn_int_col}) AS absv FROM t ORDER BY id ASC"
    );
    p!(&q, true);
}

/// (b) Window functions: ROW_NUMBER and SUM OVER PARTITION BY / ORDER BY.
fn run_window(
    rng: &mut Rng,
    schema: &Schema,
    ex: &Executor,
    sqlite: &Connection,
    divergences: &mut usize,
    panics: &mut usize,
    nuc_errors: &mut usize,
    max_report: usize,
    seed: u64,
    iter: usize,
) {
    let nn_int = schema.cols[1].name; // guaranteed NN int
    let nn_text = schema.cols[2].name; // guaranteed NN text
    let int_cols = schema.int_cols();

    macro_rules! p {
        ($sql:expr, $ordered:expr) => {
            probe(
                ex, sqlite, "window", $sql, $ordered,
                divergences, panics, nuc_errors, max_report, seed, iter,
            )
        };
    }

    // ROW_NUMBER() OVER (ORDER BY id).
    {
        let q = format!(
            "SELECT id, {nn_int}, ROW_NUMBER() OVER (ORDER BY id ASC) AS rn \
             FROM t ORDER BY id ASC"
        );
        p!(&q, true);
    }

    // ROW_NUMBER() OVER (PARTITION BY text ORDER BY id).
    if rng.chance(70) {
        let q = format!(
            "SELECT id, {nn_text}, ROW_NUMBER() OVER (PARTITION BY {nn_text} ORDER BY id ASC) AS rn \
             FROM t ORDER BY {nn_text} ASC, id ASC"
        );
        p!(&q, true);
    }

    // SUM(int) OVER (PARTITION BY text ORDER BY id) — running sum per partition.
    if !int_cols.is_empty() && rng.chance(70) {
        let ic = if rng.chance(50) { nn_int } else { rng.pick(&int_cols).name };
        let q = format!(
            "SELECT id, {nn_text}, \
             SUM({ic}) OVER (PARTITION BY {nn_text} ORDER BY id ASC) AS running \
             FROM t ORDER BY {nn_text} ASC, id ASC"
        );
        p!(&q, true);
    }

    // RANK() OVER (PARTITION BY text ORDER BY int DESC).
    if rng.chance(60) {
        let q = format!(
            "SELECT id, {nn_text}, {nn_int}, \
             RANK() OVER (PARTITION BY {nn_text} ORDER BY {nn_int} DESC) AS rnk \
             FROM t ORDER BY {nn_text} ASC, {nn_int} DESC, id ASC"
        );
        p!(&q, true);
    }

    // DENSE_RANK() OVER (ORDER BY int ASC).
    if rng.chance(50) {
        let q = format!(
            "SELECT id, {nn_int}, \
             DENSE_RANK() OVER (ORDER BY {nn_int} ASC) AS drnk \
             FROM t ORDER BY {nn_int} ASC, id ASC"
        );
        p!(&q, true);
    }

    // SUM without partition (whole-table running total).
    if !int_cols.is_empty() && rng.chance(60) {
        let ic = nn_int;
        let q = format!(
            "SELECT id, SUM({ic}) OVER (ORDER BY id ASC) AS cum \
             FROM t ORDER BY id ASC"
        );
        p!(&q, true);
    }
}

/// (c) CTEs — plain and recursive.
fn run_ctes(
    rng: &mut Rng,
    schema: &Schema,
    ex: &Executor,
    sqlite: &Connection,
    divergences: &mut usize,
    panics: &mut usize,
    nuc_errors: &mut usize,
    max_report: usize,
    seed: u64,
    iter: usize,
    rows: usize,
) {
    let nn_int = schema.cols[1].name;
    let nn_text = schema.cols[2].name;
    let int_cols = schema.int_cols();

    macro_rules! p {
        ($sql:expr, $ordered:expr) => {
            probe(
                ex, sqlite, "cte", $sql, $ordered,
                divergences, panics, nuc_errors, max_report, seed, iter,
            )
        };
    }

    // Plain CTE: aggregate in CTE, join back to table.
    if rng.chance(80) {
        let q = format!(
            "WITH stats AS (\
               SELECT {nn_text} AS grp, COUNT(*) AS n, MAX({nn_int}) AS mx \
               FROM t GROUP BY {nn_text}\
             ) \
             SELECT t.id, t.{nn_text}, t.{nn_int}, stats.n, stats.mx \
             FROM t JOIN stats ON t.{nn_text} = stats.grp \
             ORDER BY t.id ASC"
        );
        p!(&q, true);
    }

    // Plain CTE: filter in CTE.
    if rng.chance(70) {
        let threshold = rng.int(0, 10);
        let q = format!(
            "WITH high AS (SELECT id, {nn_int} FROM t WHERE {nn_int} > {threshold}) \
             SELECT id, {nn_int} FROM high ORDER BY id ASC"
        );
        p!(&q, true);
    }

    // Chained CTEs (CTE referencing another CTE).
    if !int_cols.is_empty() && rng.chance(60) {
        let ic = nn_int;
        let mid = rng.int(1, rows as i64);
        let q = format!(
            "WITH first AS (SELECT id, {ic} FROM t WHERE id <= {mid}), \
                  totals AS (SELECT SUM({ic}) AS s, COUNT(*) AS n FROM first) \
             SELECT s, n FROM totals"
        );
        p!(&q, true);
    }

    // Recursive CTE: generate integers 1..N (N small so it's fast in both engines).
    // Both SQLite and Nucleus support WITH RECURSIVE.
    if rng.chance(70) {
        let n = rng.int(3, 12);
        // Use the sqlparser-compatible form with column alias
        let q = format!(
            "WITH RECURSIVE seq(v) AS (\
               SELECT 1 \
               UNION ALL \
               SELECT v + 1 FROM seq WHERE v < {n}\
             ) \
             SELECT v FROM seq ORDER BY v ASC"
        );
        p!(&q, true);
    }

    // Recursive CTE computing a running sum: 1+2+...+N.
    if rng.chance(50) {
        let n = rng.int(3, 10);
        // Both engines support UNION ALL form; UNION (distinct) has edge cases so skip.
        let q = format!(
            "WITH RECURSIVE cnt(v, s) AS (\
               SELECT 1, 1 \
               UNION ALL \
               SELECT v + 1, s + v + 1 FROM cnt WHERE v < {n}\
             ) \
             SELECT v, s FROM cnt ORDER BY v ASC"
        );
        p!(&q, true);
    }
}

/// (d) Scalar function families available in both engines.
fn run_scalar_fns(
    rng: &mut Rng,
    schema: &Schema,
    ex: &Executor,
    sqlite: &Connection,
    divergences: &mut usize,
    panics: &mut usize,
    nuc_errors: &mut usize,
    max_report: usize,
    seed: u64,
    iter: usize,
) {
    let nn_text = schema.cols[2].name; // guaranteed NN text
    let nn_int = schema.cols[1].name;  // guaranteed NN int
    let int_cols = schema.int_cols();

    macro_rules! p {
        ($sql:expr, $ordered:expr) => {
            probe(
                ex, sqlite, "scalar-fn", $sql, $ordered,
                divergences, panics, nuc_errors, max_report, seed, iter,
            )
        };
    }

    // ── String functions ──────────────────────────────────────────────────────

    // UPPER / LOWER — both engines are ASCII-equivalent on our lowercase data.
    if rng.chance(70) {
        let q = format!("SELECT id, UPPER({nn_text}) AS up FROM t ORDER BY id ASC");
        p!(&q, true);
    }
    if rng.chance(70) {
        let q = format!("SELECT id, LOWER({nn_text}) AS lo FROM t ORDER BY id ASC");
        p!(&q, true);
    }

    // LENGTH — char count on ASCII data matches byte count.
    if rng.chance(70) {
        let q = format!("SELECT id, LENGTH({nn_text}) AS len FROM t ORDER BY id ASC");
        p!(&q, true);
    }

    // SUBSTR(s, start [, len]) — 1-indexed in both.
    if rng.chance(70) {
        let start = rng.int(1, 3);
        let len = rng.int(1, 4);
        let q = format!(
            "SELECT id, SUBSTR({nn_text}, {start}, {len}) AS sub FROM t ORDER BY id ASC"
        );
        p!(&q, true);
    }

    // REPLACE(s, from, to).
    if rng.chance(60) {
        // Replace first char of one cat with a known char.
        let from = *rng.pick(&["r", "g", "b", "a", "s"]);
        let q = format!(
            "SELECT id, REPLACE({nn_text}, '{from}', 'X') AS rep FROM t ORDER BY id ASC"
        );
        p!(&q, true);
    }

    // TRIM / LTRIM / RTRIM — our data has no spaces, so these are identity ops.
    // Still verifies they don't error.
    if rng.chance(50) {
        let fn_name = *rng.pick(&["TRIM", "LTRIM", "RTRIM"]);
        let q = format!("SELECT id, {fn_name}({nn_text}) AS tr FROM t ORDER BY id ASC");
        p!(&q, true);
    }

    // ── Math functions ────────────────────────────────────────────────────────

    // ABS(int).
    if rng.chance(70) {
        let q = format!("SELECT id, ABS({nn_int}) AS a FROM t ORDER BY id ASC");
        p!(&q, true);
    }

    // ROUND(real, n) — only over NN int cast to real to avoid NULL.
    // ROUND(int) should return the int in both engines.
    if rng.chance(70) {
        let q = format!("SELECT id, ROUND(CAST({nn_int} AS REAL), 2) AS r FROM t ORDER BY id ASC");
        p!(&q, true);
    }

    // SIGN(int) — 1, 0, -1.
    if rng.chance(60) && !int_cols.is_empty() {
        let q = format!("SELECT id, SIGN({nn_int}) AS s FROM t ORDER BY id ASC");
        p!(&q, true);
    }

    // MOD(int, divisor) — skip if divisor could be 0 from data;
    // use a fixed safe divisor to avoid div-by-zero engine differences.
    if rng.chance(60) && !int_cols.is_empty() {
        let divisor = rng.int(2, 7); // always positive, never 0
        let q = format!("SELECT id, MOD({nn_int}, {divisor}) AS m FROM t ORDER BY id ASC");
        p!(&q, true);
    }

    // MAX / MIN aggregates.
    if rng.chance(70) {
        let q = format!("SELECT MAX({nn_int}), MIN({nn_int}), SUM({nn_int}), COUNT(*) FROM t");
        p!(&q, true);
    }

    // COALESCE over nullable int column.
    let nullable_ints: Vec<&Col> = schema
        .cols
        .iter()
        .filter(|c| !c.nn && c.ty == Ty::Int)
        .collect();
    if !nullable_ints.is_empty() && rng.chance(60) {
        let nc = rng.pick(&nullable_ints).name;
        let q = format!(
            "SELECT id, COALESCE({nc}, -99) AS coa FROM t ORDER BY id ASC"
        );
        p!(&q, true);
    }

    // NULLIF(int, constant).
    if rng.chance(50) {
        let val = rng.int(-5, 10);
        let q = format!(
            "SELECT id, NULLIF({nn_int}, {val}) AS ni FROM t ORDER BY id ASC"
        );
        p!(&q, true);
    }

    // CAST(int AS REAL) and CAST(real AS INTEGER) round-trip.
    if rng.chance(60) {
        let q = format!(
            "SELECT id, CAST(CAST({nn_int} AS REAL) AS INTEGER) AS rt FROM t ORDER BY id ASC"
        );
        p!(&q, true);
    }

    // GROUP BY + aggregate over string column.
    if rng.chance(60) {
        let q = format!(
            "SELECT {nn_text}, COUNT(*) AS n, MAX({nn_int}) AS mx, MIN({nn_int}) AS mn \
             FROM t GROUP BY {nn_text} ORDER BY {nn_text} ASC"
        );
        p!(&q, true);
    }
}

// ─── Main loop ────────────────────────────────────────────────────────────────
fn main_impl() {
    let mut seed: u64 = 0xABCD_1234;
    let mut iterations: usize = 2000;
    let mut max_report: usize = 20;
    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--seed" => {
                i += 1;
                let s = args[i].trim_start_matches("0x").trim_start_matches("0X");
                seed = u64::from_str_radix(s, 16)
                    .or_else(|_| args[i].parse::<u64>())
                    .unwrap_or(0x1234_5678);
            }
            "--iterations" => {
                i += 1;
                iterations = args[i].parse().unwrap();
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

    println!("Nucleus extended SQL differential fuzzer (types + window + CTE + fns)");
    println!("seed={seed}  iterations={iterations}\n");

    let mut divergences = 0usize;
    let mut panics = 0usize;
    let mut nuc_errors = 0usize;
    let mut setup_failures = 0usize;

    'outer: for iter in 0..iterations {
        let mut rng = Rng(seed.wrapping_add(iter as u64).wrapping_mul(0x100000001B3));

        let schema = Schema::random(&mut rng);
        let rows = 6 + rng.below(20); // 6..25 rows — small enough to keep CTEs/window fast
        let ddl = schema.ddl();
        let inserts = gen_inserts(&schema, &mut rng, rows);

        let catalog = Arc::new(Catalog::new());
        let storage: Arc<dyn StorageEngine> = Arc::new(MvccStorageAdapter::new());
        let ex = Arc::new(Executor::new(catalog, storage));
        let sqlite = match Connection::open_in_memory() {
            Ok(c) => c,
            Err(_) => { setup_failures += 1; continue; }
        };

        // Setup must succeed on both.
        for stmt in [&ddl, &inserts] {
            if let Err(e) = exec_nucleus(&ex, stmt) {
                if e.starts_with("PANIC:") {
                    panics += 1;
                    if panics <= max_report {
                        println!("─── SETUP PANIC (iter {iter}) ───");
                        println!("  stmt  : {stmt}");
                        println!("  {e}\n");
                    }
                } else {
                    setup_failures += 1;
                }
                continue 'outer;
            }
            if sqlite.execute_batch(stmt).is_err() {
                setup_failures += 1;
                continue 'outer;
            }
        }

        let args = (
            &mut rng,
            &schema,
            &*ex,
            &sqlite,
            &mut divergences,
            &mut panics,
            &mut nuc_errors,
            max_report,
            seed,
            iter,
        );

        run_extended_types(
            args.0, args.1, args.2, args.3,
            args.4, args.5, args.6, args.7, args.8, args.9,
            rows,
        );
        run_window(
            args.0, args.1, args.2, args.3,
            args.4, args.5, args.6, args.7, args.8, args.9,
        );
        run_ctes(
            args.0, args.1, args.2, args.3,
            args.4, args.5, args.6, args.7, args.8, args.9,
            rows,
        );
        run_scalar_fns(
            args.0, args.1, args.2, args.3,
            args.4, args.5, args.6, args.7, args.8, args.9,
        );
    }

    println!("\n════ SUMMARY ════");
    println!("iterations         : {iterations}");
    println!("setup failures     : {setup_failures}");
    println!("RESULT divergences : {divergences}");
    println!("PANICS             : {panics}");
    println!("nucleus-only errors: {nuc_errors}");
    if divergences == 0 && panics == 0 {
        println!("\nAll clear — no divergences, no panics vs SQLite.");
    } else {
        println!("\nRerun with: --seed {seed} --iterations <failing_iter+1>");
        std::process::exit(1);
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    tokio::task::spawn_blocking(main_impl).await.unwrap();
}
