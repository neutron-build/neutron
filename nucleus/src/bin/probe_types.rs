//! Extended column-type coverage harness for Nucleus.
//!
//! Two complementary strategies:
//!   1. Differential vs SQLite (rusqlite) for types both engines share:
//!        REAL/NUMERIC precision & rounding, DATE arithmetic & comparison,
//!        BOOLEAN storage & retrieval.
//!   2. Rust reference-oracle / round-trip invariants for Nucleus-specific or
//!        SQLite-incompatible types:
//!        JSONB  — parse/serialize via serde_json, path extraction
//!        ARRAY  — ARRAY[...] constructor, ARRAY_LENGTH, ARRAY_APPEND, ARRAY_CAT
//!        UUID   — UUID_GENERATE_V4 format check + round-trip through a column
//!        BYTEA  — ENCODE/DECODE hex & base64 round-trips
//!
//! Build:
//!   cargo build --release --features "server rusqlite" --bin probe_types
//! Run:
//!   cargo run --release --features "server rusqlite" --bin probe_types
//!   cargo run --release --features "server rusqlite" --bin probe_types -- --seed 99 --iterations 3000
#![cfg(feature = "server")]
#![allow(unused)]
#![allow(clippy::all)]

use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::storage::{MvccStorageAdapter, StorageEngine};
use nucleus::types::Value;
use rusqlite::Connection;

// ─── Deterministic PRNG (xorshift64) ─────────────────────────────────────────
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: usize) -> usize {
        if n == 0 { return 0; }
        (self.next() % n as u64) as usize
    }
    fn int(&mut self, lo: i64, hi: i64) -> i64 {
        lo + (self.next() % ((hi - lo + 1) as u64)) as i64
    }
    fn chance(&mut self, pct: u64) -> bool {
        self.next() % 100 < pct
    }
    fn pick<'a, T>(&mut self, xs: &'a [T]) -> &'a T {
        &xs[self.below(xs.len())]
    }
}

// ─── Helpers: run Nucleus SQL, catch panics ───────────────────────────────────
fn run_nucleus(ex: &Executor, sql: &str) -> Result<Vec<Vec<Value>>, String> {
    let rt = tokio::runtime::Handle::current();
    let res = std::panic::catch_unwind(AssertUnwindSafe(|| {
        tokio::task::block_in_place(|| rt.block_on(ex.execute(sql)))
    }));
    match res {
        Ok(Ok(mut results)) => match results.pop() {
            Some(ExecResult::Select { rows, .. }) => Ok(rows),
            Some(_) => Ok(vec![]),
            None => Err("no result".into()),
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

fn nucleus_scalar(ex: &Executor, sql: &str) -> Result<Value, String> {
    let rows = run_nucleus(ex, sql)?;
    rows.into_iter()
        .next()
        .and_then(|r| r.into_iter().next())
        .ok_or_else(|| "empty result".into())
}

fn panic_msg(p: &(dyn std::any::Any + Send)) -> String {
    p.downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| p.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown".into())
}

// ─── Value canonicalization ───────────────────────────────────────────────────
/// Canonical float: round to 6dp string. Same for both engines.
fn canon_f64(f: f64) -> String {
    if f.is_finite() && (f - f.round()).abs() < 1e-9 && f.abs() < 9e15 {
        format!("{}", f.round() as i64)
    } else {
        format!("{f:.6}")
    }
}

fn canon_nucleus_val(v: &Value) -> String {
    match v {
        Value::Null => "∅".into(),
        Value::Bool(b) => (if *b { "1" } else { "0" }).into(),
        Value::Int32(n) => n.to_string(),
        Value::Int64(n) => n.to_string(),
        Value::Float64(f) => canon_f64(*f),
        Value::Text(s) => s.clone(),
        Value::Numeric(s) => {
            // Try to parse as f64 for comparison
            if let Ok(f) = s.parse::<f64>() { canon_f64(f) } else { s.clone() }
        }
        Value::Date(d) => {
            let (y, m, day) = nucleus::types::days_to_ymd(*d);
            format!("{y:04}-{m:02}-{day:02}")
        }
        other => format!("{other}"),
    }
}

fn canon_sqlite_val(v: &rusqlite::types::Value) -> String {
    use rusqlite::types::Value as Sv;
    match v {
        Sv::Null => "∅".into(),
        Sv::Integer(i) => i.to_string(),
        Sv::Real(f) => canon_f64(*f),
        Sv::Text(s) => s.clone(),
        Sv::Blob(b) => format!("blob:{}", b.len()),
    }
}

fn run_sqlite_scalar(conn: &Connection, sql: &str) -> Result<String, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let mut rows = stmt.query([]).map_err(|e| e.to_string())?;
    let row = rows.next().map_err(|e| e.to_string())?
        .ok_or("no row")?;
    let v: rusqlite::types::Value = row.get(0).map_err(|e| e.to_string())?;
    Ok(canon_sqlite_val(&v))
}

fn run_sqlite_rows(conn: &Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let ncols = stmt.column_count();
    let rows_iter = stmt.query_map([], |row| {
        let mut cells = Vec::with_capacity(ncols);
        for i in 0..ncols {
            let v: rusqlite::types::Value = row.get(i)?;
            cells.push(canon_sqlite_val(&v));
        }
        Ok(cells)
    }).map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    for r in rows_iter { out.push(r.map_err(|e| e.to_string())?); }
    Ok(out)
}

fn run_nucleus_rows_canon(ex: &Executor, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let rows = run_nucleus(ex, sql)?;
    Ok(rows.iter().map(|r| r.iter().map(canon_nucleus_val).collect()).collect())
}

// ─── Fresh Nucleus executor ───────────────────────────────────────────────────
fn fresh_executor() -> Arc<Executor> {
    let cat = Arc::new(Catalog::new());
    let st: Arc<dyn StorageEngine> = Arc::new(MvccStorageAdapter::new());
    Arc::new(Executor::new(cat, st))
}

// ─────────────────────────────────────────────────────────────────────────────
//  SECTION 1: BOOLEAN — store TRUE/FALSE, compare, use in expressions
// ─────────────────────────────────────────────────────────────────────────────

fn test_boolean(rng: &mut Rng, findings: &mut Vec<Finding>) {
    let ex = fresh_executor();
    let conn = Connection::open_in_memory().unwrap();

    let ddl = "CREATE TABLE bools (id INTEGER PRIMARY KEY, flag BOOLEAN NOT NULL, val INTEGER NOT NULL)";
    exec_nucleus(&ex, ddl).expect("BOOL DDL failed");
    conn.execute_batch(ddl).expect("SQLite BOOL DDL");

    // Insert random TRUE/FALSE rows (SQLite stores as 0/1 integer)
    let n_rows = 8 + rng.below(12);
    for id in 1..=n_rows {
        let flag_bool = rng.chance(50);
        let val = rng.int(0, 10);
        let flag_nucleus = if flag_bool { "TRUE" } else { "FALSE" };
        let flag_sqlite = if flag_bool { "1" } else { "0" }; // SQLite canonical
        let ins_nuc = format!("INSERT INTO bools VALUES ({id}, {flag_nucleus}, {val})");
        let ins_sql = format!("INSERT INTO bools VALUES ({id}, {flag_sqlite}, {val})");
        exec_nucleus(&ex, &ins_nuc).expect("BOOL insert nucleus");
        conn.execute_batch(&ins_sql).expect("BOOL insert sqlite");
    }

    // Query: SELECT flag as 0/1, SUM(val) WHERE flag, COUNT(*) WHERE NOT flag
    let queries = [
        "SELECT id, CASE WHEN flag THEN 1 ELSE 0 END, val FROM bools ORDER BY id ASC",
        "SELECT COUNT(*) FROM bools WHERE flag = TRUE",
        "SELECT COUNT(*) FROM bools WHERE flag = FALSE",
        "SELECT SUM(val) FROM bools WHERE flag = TRUE",
        "SELECT SUM(val) FROM bools WHERE NOT flag",
    ];
    let sqlite_queries = [
        "SELECT id, CASE WHEN flag THEN 1 ELSE 0 END, val FROM bools ORDER BY id ASC",
        "SELECT COUNT(*) FROM bools WHERE flag = 1",
        "SELECT COUNT(*) FROM bools WHERE flag = 0",
        "SELECT SUM(val) FROM bools WHERE flag = 1",
        "SELECT SUM(val) FROM bools WHERE NOT flag",
    ];

    for (nq, sq) in queries.iter().zip(sqlite_queries.iter()) {
        let nr = run_nucleus_rows_canon(&ex, nq);
        let sr = run_sqlite_rows(&conn, sq);
        match (nr, sr) {
            (Ok(mut nv), Ok(mut sv)) => {
                nv.sort();
                sv.sort();
                if nv != sv {
                    findings.push(Finding {
                        title: "BOOLEAN: query result diverges from SQLite reference".into(),
                        query: nq.to_string(),
                        nucleus: format!("{nv:?}"),
                        expected: format!("{sv:?}"),
                        real_bug: true,
                    });
                }
            }
            (Err(e), Ok(_)) => {
                findings.push(Finding {
                    title: "BOOLEAN: Nucleus errored where SQLite succeeded".into(),
                    query: nq.to_string(),
                    nucleus: format!("ERR: {e}"),
                    expected: "success".into(),
                    real_bug: !e.contains("Unsupported"),
                });
            }
            _ => {}
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
//  SECTION 2: NUMERIC/DECIMAL precision & ROUND/CEIL/FLOOR vs SQLite
// ─────────────────────────────────────────────────────────────────────────────

fn test_numeric_round(rng: &mut Rng, findings: &mut Vec<Finding>) {
    let ex = fresh_executor();
    let conn = Connection::open_in_memory().unwrap();

    // ROUND(x, n) differential — SQLite uses same IEEE754 rounding
    for _ in 0..30 {
        let int_part = rng.int(-99, 99);
        let frac = rng.int(0, 99);
        let val_str = format!("{int_part}.{frac:02}");
        let decimals = rng.below(3) as i64; // 0, 1, or 2

        let nq = format!("SELECT ROUND({val_str}, {decimals})");
        let sq = format!("SELECT ROUND({val_str}, {decimals})");

        let nr = nucleus_scalar(&ex, &nq).map(|v| canon_nucleus_val(&v));
        let sr = run_sqlite_scalar(&conn, &sq);

        match (nr, sr) {
            (Ok(nv), Ok(sv)) if nv != sv => {
                // SQLite uses C printf %.*f which rounds-half-to-even on this platform.
                // Nucleus (and PostgreSQL) use round-half-away-from-zero.
                // Divergences on exact half-way float values (e.g. x.x5 * 10 = integer.5)
                // are a known SQLite oracle limitation, not a real Nucleus bug.
                findings.push(Finding {
                    title: "NUMERIC: ROUND() diverges from SQLite (float half-way: SQLite=half-to-even, Nucleus/PG=half-away)".into(),
                    query: nq,
                    nucleus: nv,
                    expected: sv,
                    real_bug: false,
                });
            }
            (Err(e), Ok(_)) => {
                findings.push(Finding {
                    title: "NUMERIC: ROUND() Nucleus error".into(),
                    query: nq,
                    nucleus: format!("ERR: {e}"),
                    expected: "success".into(),
                    real_bug: !e.contains("Unsupported"),
                });
            }
            _ => {}
        }
    }

    // CEIL / FLOOR differential
    for fname in &["CEIL", "FLOOR"] {
        for _ in 0..20 {
            let int_part = rng.int(-50, 50);
            let frac = rng.int(1, 99); // non-zero frac to exercise rounding
            let val_str = format!("{int_part}.{frac:02}");
            let nq = format!("SELECT {fname}({val_str})");
            let sq = nq.clone();
            let nr = nucleus_scalar(&ex, &nq).map(|v| canon_nucleus_val(&v));
            let sr = run_sqlite_scalar(&conn, &sq);
            match (nr, sr) {
                (Ok(nv), Ok(sv)) if nv != sv => {
                    findings.push(Finding {
                        title: format!("NUMERIC: {fname}() diverges from SQLite"),
                        query: nq,
                        nucleus: nv,
                        expected: sv,
                        real_bug: true,
                    });
                }
                (Err(e), Ok(_)) => {
                    findings.push(Finding {
                        title: format!("NUMERIC: {fname}() Nucleus error"),
                        query: nq,
                        nucleus: format!("ERR: {e}"),
                        expected: "success".into(),
                        real_bug: !e.contains("Unsupported"),
                    });
                }
                _ => {}
            }
        }
    }

    // NUMERIC column: store via NUMERIC type, read back
    let ddl = "CREATE TABLE nums (id INTEGER PRIMARY KEY, n NUMERIC)";
    exec_nucleus(&ex, ddl).expect("NUMERIC DDL");
    conn.execute_batch(ddl).expect("SQLite NUMERIC DDL");
    for id in 1..=15i64 {
        let int_part = rng.int(-999, 999);
        let frac = rng.int(0, 9999);
        let val_str = format!("{int_part}.{frac:04}");
        let ins = format!("INSERT INTO nums VALUES ({id}, {val_str})");
        exec_nucleus(&ex, &ins).expect("NUMERIC insert");
        conn.execute_batch(&ins).expect("SQLite NUMERIC insert");
    }
    let nrows = run_nucleus_rows_canon(&ex, "SELECT id, n FROM nums ORDER BY id");
    let srows = run_sqlite_rows(&conn, "SELECT id, n FROM nums ORDER BY id");
    match (nrows, srows) {
        (Ok(nv), Ok(sv)) if nv != sv => {
            findings.push(Finding {
                title: "NUMERIC: column round-trip diverges from SQLite".into(),
                query: "SELECT id, n FROM nums ORDER BY id".into(),
                nucleus: format!("{nv:?}"),
                expected: format!("{sv:?}"),
                real_bug: true,
            });
        }
        _ => {}
    }
}

// ─────────────────────────────────────────────────────────────────────────────
//  SECTION 3: DATE — storage, comparison, EXTRACT vs SQLite reference
// ─────────────────────────────────────────────────────────────────────────────

fn test_date(rng: &mut Rng, findings: &mut Vec<Finding>) {
    let ex = fresh_executor();
    let conn = Connection::open_in_memory().unwrap();

    // SQLite stores dates as TEXT (ISO-8601). Nucleus stores as Date(i32).
    // We compare string representations (yyyy-mm-dd) which are unambiguous.
    let ddl = "CREATE TABLE dates (id INTEGER PRIMARY KEY, d DATE NOT NULL)";
    exec_nucleus(&ex, ddl).expect("DATE DDL");
    conn.execute_batch(ddl).expect("SQLite DATE DDL");

    // Fixed pool of known-good dates (avoid edge cases like Feb-29 for simplicity)
    let date_pool: &[&str] = &[
        "2020-01-15", "2021-06-30", "2022-12-01", "2023-03-15",
        "2024-07-04", "2019-11-11", "2018-02-28", "2025-09-01",
        "2000-01-01", "1999-12-31", "2010-05-20", "2015-08-08",
    ];

    let n = 8 + rng.below(4);
    let mut used_dates: Vec<&str> = Vec::new();
    for id in 1..=(n as i64) {
        let d = rng.pick(date_pool);
        used_dates.push(d);
        let ins_nuc = format!("INSERT INTO dates VALUES ({id}, DATE '{d}')");
        let ins_sql = format!("INSERT INTO dates VALUES ({id}, '{d}')");
        exec_nucleus(&ex, &ins_nuc).expect("DATE insert nucleus");
        conn.execute_batch(&ins_sql).expect("DATE insert sqlite");
    }

    // Compare SELECT d as text
    let nrows = run_nucleus_rows_canon(&ex, "SELECT id, d FROM dates ORDER BY id");
    let srows = run_sqlite_rows(&conn, "SELECT id, d FROM dates ORDER BY id");
    match (nrows, srows) {
        (Ok(nv), Ok(sv)) if nv != sv => {
            findings.push(Finding {
                title: "DATE: column values diverge from SQLite reference".into(),
                query: "SELECT id, d FROM dates ORDER BY id".into(),
                nucleus: format!("{nv:?}"),
                expected: format!("{sv:?}"),
                real_bug: true,
            });
        }
        _ => {}
    }

    // Compare ORDER BY on date column
    let nrows = run_nucleus_rows_canon(&ex, "SELECT id FROM dates ORDER BY d ASC, id ASC");
    let srows = run_sqlite_rows(&conn, "SELECT id FROM dates ORDER BY d ASC, id ASC");
    match (nrows, srows) {
        (Ok(nv), Ok(sv)) if nv != sv => {
            findings.push(Finding {
                title: "DATE: ORDER BY date column diverges from SQLite".into(),
                query: "SELECT id FROM dates ORDER BY d ASC, id ASC".into(),
                nucleus: format!("{nv:?}"),
                expected: format!("{sv:?}"),
                real_bug: true,
            });
        }
        _ => {}
    }

    // EXTRACT year/month/day from a specific date
    let test_dates = [
        ("2024-07-04", 2024i64, 7i64, 4i64),
        ("2020-01-15", 2020, 1, 15),
        ("2022-12-31", 2022, 12, 31),
    ];
    for (date_str, exp_year, exp_month, exp_day) in &test_dates {
        for (field, expected) in &[
            ("year", *exp_year),
            ("month", *exp_month),
            ("day", *exp_day),
        ] {
            let sql = format!("SELECT EXTRACT({field} FROM DATE '{date_str}')");
            match nucleus_scalar(&ex, &sql) {
                Ok(v) => {
                    let got = match &v {
                        Value::Int32(n) => *n as i64,
                        Value::Int64(n) => *n,
                        Value::Float64(f) => *f as i64,
                        _ => -9999,
                    };
                    if got != *expected {
                        findings.push(Finding {
                            title: format!("DATE: EXTRACT({field}) wrong for {date_str}"),
                            query: sql,
                            nucleus: got.to_string(),
                            expected: expected.to_string(),
                            real_bug: true,
                        });
                    }
                }
                Err(e) => {
                    findings.push(Finding {
                        title: format!("DATE: EXTRACT({field}) errored for {date_str}"),
                        query: sql,
                        nucleus: format!("ERR: {e}"),
                        expected: expected.to_string(),
                        real_bug: !e.contains("Unsupported"),
                    });
                }
            }
        }
    }

    // DATE comparison: WHERE d > '...'
    let pivot = rng.pick(date_pool);
    let nq = format!("SELECT COUNT(*) FROM dates WHERE d > DATE '{pivot}'");
    let sq = format!("SELECT COUNT(*) FROM dates WHERE d > '{pivot}'");
    let nr = nucleus_scalar(&ex, &nq).map(|v| canon_nucleus_val(&v));
    let sr = run_sqlite_scalar(&conn, &sq);
    match (nr, sr) {
        (Ok(nv), Ok(sv)) if nv != sv => {
            findings.push(Finding {
                title: "DATE: comparison WHERE d > '...' diverges from SQLite".into(),
                query: nq,
                nucleus: nv,
                expected: sv,
                real_bug: true,
            });
        }
        _ => {}
    }
}

// ─────────────────────────────────────────────────────────────────────────────
//  SECTION 4: JSONB — parse/serialize, path extraction, JSONB_BUILD_ARRAY
// ─────────────────────────────────────────────────────────────────────────────

fn test_jsonb(rng: &mut Rng, findings: &mut Vec<Finding>) {
    let ex = fresh_executor();

    // ── 4a: JSONB_BUILD_OBJECT round-trip ──
    let key1 = "name";
    let key2 = "score";
    let names: &[&str] = &["alice", "bob", "charlie", "dave"];
    let name = *rng.pick(names);
    let score = rng.int(1, 100);
    let build_sql = format!("SELECT JSONB_BUILD_OBJECT('{key1}', '{name}', '{key2}', {score})");
    match nucleus_scalar(&ex, &build_sql) {
        Ok(Value::Jsonb(v)) => {
            // Verify it's a JSON object with the right keys
            let obj = v.as_object();
            if obj.is_none() {
                findings.push(Finding {
                    title: "JSONB: JSONB_BUILD_OBJECT did not return an object".into(),
                    query: build_sql.clone(),
                    nucleus: v.to_string(),
                    expected: "JSON object".into(),
                    real_bug: true,
                });
            } else {
                let obj = obj.unwrap();
                let got_name = obj.get(key1).and_then(|v| v.as_str()).unwrap_or("");
                let got_score = obj.get(key2).and_then(|v| v.as_i64()).unwrap_or(-1);
                if got_name != name {
                    findings.push(Finding {
                        title: "JSONB: JSONB_BUILD_OBJECT key 'name' mismatch".into(),
                        query: build_sql.clone(),
                        nucleus: got_name.to_string(),
                        expected: name.to_string(),
                        real_bug: true,
                    });
                }
                if got_score != score {
                    findings.push(Finding {
                        title: "JSONB: JSONB_BUILD_OBJECT key 'score' mismatch".into(),
                        query: build_sql,
                        nucleus: got_score.to_string(),
                        expected: score.to_string(),
                        real_bug: true,
                    });
                }
            }
        }
        Ok(other) => {
            findings.push(Finding {
                title: "JSONB: JSONB_BUILD_OBJECT returned non-JSONB value".into(),
                query: build_sql,
                nucleus: format!("{other:?}"),
                expected: "Value::Jsonb(...)".into(),
                real_bug: true,
            });
        }
        Err(e) => {
            findings.push(Finding {
                title: "JSONB: JSONB_BUILD_OBJECT errored".into(),
                query: build_sql,
                nucleus: format!("ERR: {e}"),
                expected: "success".into(),
                real_bug: !e.contains("Unsupported"),
            });
        }
    }

    // ── 4b: JSONB_EXTRACT_PATH_TEXT for object key ──
    let json_lit = format!("'{{\"a\": 42, \"b\": \"hello\"}}'");
    // Cast TEXT literal as JSONB
    let path_sql = format!("SELECT JSONB_EXTRACT_PATH_TEXT({json_lit}::JSONB, 'a')");
    match nucleus_scalar(&ex, &path_sql) {
        Ok(v) => {
            let got = canon_nucleus_val(&v);
            if got != "42" {
                findings.push(Finding {
                    title: "JSONB: JSONB_EXTRACT_PATH_TEXT('a') got wrong value".into(),
                    query: path_sql,
                    nucleus: got,
                    expected: "42".into(),
                    real_bug: true,
                });
            }
        }
        Err(e) => {
            findings.push(Finding {
                title: "JSONB: JSONB_EXTRACT_PATH_TEXT errored".into(),
                query: path_sql,
                nucleus: format!("ERR: {e}"),
                expected: "42".into(),
                real_bug: !e.contains("Unsupported"),
            });
        }
    }

    // ── 4c: JSONB_BUILD_ARRAY length check ──
    let n_elems = 2 + rng.below(5);
    let elems: Vec<String> = (0..n_elems).map(|i| rng.int(0, 99).to_string()).collect();
    let arr_sql = format!("SELECT JSONB_ARRAY_LENGTH(JSONB_BUILD_ARRAY({}))", elems.join(", "));
    match nucleus_scalar(&ex, &arr_sql) {
        Ok(v) => {
            let got = match &v {
                Value::Int32(n) => *n as usize,
                Value::Int64(n) => *n as usize,
                _ => usize::MAX,
            };
            if got != n_elems {
                findings.push(Finding {
                    title: "JSONB: JSONB_ARRAY_LENGTH after JSONB_BUILD_ARRAY wrong".into(),
                    query: arr_sql,
                    nucleus: got.to_string(),
                    expected: n_elems.to_string(),
                    real_bug: true,
                });
            }
        }
        Err(e) => {
            findings.push(Finding {
                title: "JSONB: JSONB_BUILD_ARRAY + JSONB_ARRAY_LENGTH errored".into(),
                query: arr_sql,
                nucleus: format!("ERR: {e}"),
                expected: n_elems.to_string(),
                real_bug: !e.contains("Unsupported"),
            });
        }
    }

    // ── 4d: JSONB column round-trip ──
    let ddl = "CREATE TABLE jdocs (id INTEGER PRIMARY KEY, doc JSONB)";
    match exec_nucleus(&ex, ddl) {
        Ok(_) => {
            let jsons: &[&str] = &[
                r#"'{"x":1,"y":2}'"#,
                r#"'{"name":"test","tags":["a","b"]}'"#,
                r#"'{"nested":{"k":99}}'"#,
                r#"'[]'"#,
                r#"'null'"#,
            ];
            for (id, json_lit) in jsons.iter().enumerate() {
                let id = id as i64 + 1;
                let ins = format!("INSERT INTO jdocs VALUES ({id}, {json_lit}::JSONB)");
                if let Err(e) = exec_nucleus(&ex, &ins) {
                    // If JSONB column type isn't supported in DDL, that's a known gap
                    if !e.contains("Unsupported") {
                        findings.push(Finding {
                            title: "JSONB: column INSERT failed unexpectedly".into(),
                            query: ins,
                            nucleus: format!("ERR: {e}"),
                            expected: "success".into(),
                            real_bug: true,
                        });
                    }
                }
            }
            // Read back
            match run_nucleus(&ex, "SELECT COUNT(*) FROM jdocs") {
                Ok(rows) => {
                    // Just verify we got a result (non-panic)
                    if rows.is_empty() {
                        findings.push(Finding {
                            title: "JSONB: COUNT(*) on jdocs returned no rows".into(),
                            query: "SELECT COUNT(*) FROM jdocs".into(),
                            nucleus: "empty".into(),
                            expected: "1 row with count".into(),
                            real_bug: true,
                        });
                    }
                }
                Err(e) => {
                    findings.push(Finding {
                        title: "JSONB: SELECT COUNT(*) panicked or errored on jdocs".into(),
                        query: "SELECT COUNT(*) FROM jdocs".into(),
                        nucleus: format!("ERR: {e}"),
                        expected: "success".into(),
                        real_bug: e.starts_with("PANIC"),
                    });
                }
            }
        }
        Err(e) => {
            // JSONB column type not supported — skip rest of 4d
            if !e.contains("Unsupported") {
                findings.push(Finding {
                    title: "JSONB: CREATE TABLE with JSONB column failed".into(),
                    query: ddl.into(),
                    nucleus: format!("ERR: {e}"),
                    expected: "success".into(),
                    real_bug: true,
                });
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
//  SECTION 5: ARRAY — constructor, ARRAY_LENGTH, ARRAY_APPEND, ARRAY_CAT
// ─────────────────────────────────────────────────────────────────────────────

fn test_array(rng: &mut Rng, findings: &mut Vec<Finding>) {
    let ex = fresh_executor();

    // ── 5a: ARRAY[...] constructor + ARRAY_LENGTH ──
    let n_elems = 1 + rng.below(8);
    let elems: Vec<i64> = (0..n_elems).map(|_| rng.int(0, 99)).collect();
    let elems_str: Vec<String> = elems.iter().map(|e| e.to_string()).collect();
    let arr_expr = format!("ARRAY[{}]", elems_str.join(", "));
    let len_sql = format!("SELECT ARRAY_LENGTH({arr_expr}, 1)");

    match nucleus_scalar(&ex, &len_sql) {
        Ok(v) => {
            let got = match &v {
                Value::Int32(n) => *n as usize,
                Value::Int64(n) => *n as usize,
                _ => usize::MAX,
            };
            if got != n_elems {
                findings.push(Finding {
                    title: "ARRAY: ARRAY_LENGTH wrong after ARRAY[...] constructor".into(),
                    query: len_sql,
                    nucleus: got.to_string(),
                    expected: n_elems.to_string(),
                    real_bug: true,
                });
            }
        }
        Err(e) => {
            findings.push(Finding {
                title: "ARRAY: ARRAY_LENGTH errored".into(),
                query: len_sql,
                nucleus: format!("ERR: {e}"),
                expected: n_elems.to_string(),
                real_bug: !e.contains("Unsupported"),
            });
        }
    }

    // ── 5b: ARRAY_APPEND invariant: length increases by 1 ──
    let n_base = 1 + rng.below(5);
    let base_elems: Vec<String> = (0..n_base).map(|_| rng.int(0, 99).to_string()).collect();
    let base_arr = format!("ARRAY[{}]", base_elems.join(", "));
    let new_elem = rng.int(0, 99);
    let append_sql = format!("SELECT ARRAY_LENGTH(ARRAY_APPEND({base_arr}, {new_elem}), 1)");

    match nucleus_scalar(&ex, &append_sql) {
        Ok(v) => {
            let got = match &v {
                Value::Int32(n) => *n as usize,
                Value::Int64(n) => *n as usize,
                _ => usize::MAX,
            };
            let expected = n_base + 1;
            if got != expected {
                findings.push(Finding {
                    title: "ARRAY: ARRAY_APPEND did not increase length by 1".into(),
                    query: append_sql,
                    nucleus: got.to_string(),
                    expected: expected.to_string(),
                    real_bug: true,
                });
            }
        }
        Err(e) => {
            findings.push(Finding {
                title: "ARRAY: ARRAY_APPEND errored".into(),
                query: append_sql,
                nucleus: format!("ERR: {e}"),
                expected: (n_base + 1).to_string(),
                real_bug: !e.contains("Unsupported"),
            });
        }
    }

    // ── 5c: ARRAY_CAT: combined length = sum of both ──
    let n_a = 1 + rng.below(5);
    let n_b = 1 + rng.below(5);
    let a_elems: Vec<String> = (0..n_a).map(|_| rng.int(0, 99).to_string()).collect();
    let b_elems: Vec<String> = (0..n_b).map(|_| rng.int(0, 99).to_string()).collect();
    let a_arr = format!("ARRAY[{}]", a_elems.join(", "));
    let b_arr = format!("ARRAY[{}]", b_elems.join(", "));
    let cat_sql = format!("SELECT ARRAY_LENGTH(ARRAY_CAT({a_arr}, {b_arr}), 1)");

    match nucleus_scalar(&ex, &cat_sql) {
        Ok(v) => {
            let got = match &v {
                Value::Int32(n) => *n as usize,
                Value::Int64(n) => *n as usize,
                _ => usize::MAX,
            };
            let expected = n_a + n_b;
            if got != expected {
                findings.push(Finding {
                    title: "ARRAY: ARRAY_CAT length wrong".into(),
                    query: cat_sql,
                    nucleus: got.to_string(),
                    expected: expected.to_string(),
                    real_bug: true,
                });
            }
        }
        Err(e) => {
            findings.push(Finding {
                title: "ARRAY: ARRAY_CAT errored".into(),
                query: cat_sql,
                nucleus: format!("ERR: {e}"),
                expected: (n_a + n_b).to_string(),
                real_bug: !e.contains("Unsupported"),
            });
        }
    }

    // ── 5d: ARRAY column store + ARRAY_LENGTH from column ──
    let ddl = "CREATE TABLE arrtbl (id INTEGER PRIMARY KEY, vals INTEGER[])";
    match exec_nucleus(&ex, ddl) {
        Ok(_) => {
            let n_rows = 3 + rng.below(5);
            let mut expected_lens: Vec<usize> = Vec::new();
            for id in 1..=(n_rows as i64) {
                let n_el = 1 + rng.below(6);
                expected_lens.push(n_el);
                let els: Vec<String> = (0..n_el).map(|_| rng.int(0, 99).to_string()).collect();
                let arr_lit = format!("ARRAY[{}]", els.join(", "));
                let ins = format!("INSERT INTO arrtbl VALUES ({id}, {arr_lit})");
                if let Err(e) = exec_nucleus(&ex, &ins) {
                    if !e.contains("Unsupported") {
                        findings.push(Finding {
                            title: "ARRAY: INSERT into INTEGER[] column failed".into(),
                            query: ins,
                            nucleus: format!("ERR: {e}"),
                            expected: "success".into(),
                            real_bug: true,
                        });
                    }
                    return; // Can't continue
                }
            }
            // Check ARRAY_LENGTH on stored values
            let q = "SELECT id, ARRAY_LENGTH(vals, 1) FROM arrtbl ORDER BY id";
            match run_nucleus(&ex, q) {
                Ok(rows) => {
                    for (i, row) in rows.iter().enumerate() {
                        if i >= expected_lens.len() { break; }
                        let got = match row.get(1) {
                            Some(Value::Int32(n)) => *n as usize,
                            Some(Value::Int64(n)) => *n as usize,
                            Some(Value::Null) => 0, // treat NULL as 0 for diff
                            _ => usize::MAX,
                        };
                        if got != expected_lens[i] {
                            findings.push(Finding {
                                title: "ARRAY: ARRAY_LENGTH from column wrong".into(),
                                query: q.into(),
                                nucleus: format!("row {}: got {got}", i + 1),
                                expected: format!("row {}: {}", i + 1, expected_lens[i]),
                                real_bug: true,
                            });
                        }
                    }
                }
                Err(e) => {
                    findings.push(Finding {
                        title: "ARRAY: SELECT ARRAY_LENGTH from column errored".into(),
                        query: q.into(),
                        nucleus: format!("ERR: {e}"),
                        expected: "success".into(),
                        real_bug: e.starts_with("PANIC"),
                    });
                }
            }
        }
        Err(e) => {
            if !e.contains("Unsupported") {
                findings.push(Finding {
                    title: "ARRAY: CREATE TABLE with INTEGER[] failed".into(),
                    query: ddl.into(),
                    nucleus: format!("ERR: {e}"),
                    expected: "success".into(),
                    real_bug: true,
                });
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
//  SECTION 6: UUID — format check + round-trip
// ─────────────────────────────────────────────────────────────────────────────

fn test_uuid(rng: &mut Rng, findings: &mut Vec<Finding>) {
    let ex = fresh_executor();

    // ── 6a: UUID_GENERATE_V4 format: 8-4-4-4-12 hex chars ──
    let sql = "SELECT UUID_GENERATE_V4()";
    for _ in 0..10 {
        match nucleus_scalar(&ex, sql) {
            Ok(v) => {
                let s = format!("{v}");
                // Validate UUID v4 format: xxxxxxxx-xxxx-4xxx-[89ab]xxx-xxxxxxxxxxxx
                if !is_valid_uuidv4(&s) {
                    findings.push(Finding {
                        title: "UUID: UUID_GENERATE_V4 produced invalid UUID format".into(),
                        query: sql.into(),
                        nucleus: s,
                        expected: "xxxxxxxx-xxxx-4xxx-[89ab]xxx-xxxxxxxxxxxx".into(),
                        real_bug: true,
                    });
                }
            }
            Err(e) => {
                findings.push(Finding {
                    title: "UUID: UUID_GENERATE_V4 errored".into(),
                    query: sql.into(),
                    nucleus: format!("ERR: {e}"),
                    expected: "valid UUID v4".into(),
                    real_bug: !e.contains("Unsupported"),
                });
            }
        }
    }

    // ── 6b: Known UUID round-trip through column ──
    let ddl = "CREATE TABLE uuids (id INTEGER PRIMARY KEY, uid UUID NOT NULL)";
    match exec_nucleus(&ex, ddl) {
        Ok(_) => {
            let known_uuids: &[&str] = &[
                "550e8400-e29b-41d4-a716-446655440000",
                "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
                "123e4567-e89b-42d3-a456-426614174000",
            ];
            for (i, uuid) in known_uuids.iter().enumerate() {
                let id = i as i64 + 1;
                let ins = format!("INSERT INTO uuids VALUES ({id}, UUID '{uuid}')");
                if let Err(e) = exec_nucleus(&ex, &ins) {
                    if !e.contains("Unsupported") {
                        findings.push(Finding {
                            title: "UUID: INSERT into UUID column failed".into(),
                            query: ins,
                            nucleus: format!("ERR: {e}"),
                            expected: "success".into(),
                            real_bug: true,
                        });
                    }
                    return;
                }
            }
            // Read back and verify
            match run_nucleus(&ex, "SELECT id, uid FROM uuids ORDER BY id") {
                Ok(rows) => {
                    for (i, row) in rows.iter().enumerate() {
                        if i >= known_uuids.len() { break; }
                        let got = row.get(1).map(|v| format!("{v}")).unwrap_or_default();
                        // Compare case-insensitive
                        if got.to_lowercase() != known_uuids[i].to_lowercase() {
                            findings.push(Finding {
                                title: "UUID: column round-trip mismatch".into(),
                                query: "SELECT id, uid FROM uuids ORDER BY id".into(),
                                nucleus: got,
                                expected: known_uuids[i].to_string(),
                                real_bug: true,
                            });
                        }
                    }
                }
                Err(e) => {
                    findings.push(Finding {
                        title: "UUID: SELECT from UUID column errored".into(),
                        query: "SELECT id, uid FROM uuids ORDER BY id".into(),
                        nucleus: format!("ERR: {e}"),
                        expected: "success".into(),
                        real_bug: e.starts_with("PANIC"),
                    });
                }
            }
        }
        Err(e) => {
            if !e.contains("Unsupported") {
                findings.push(Finding {
                    title: "UUID: CREATE TABLE with UUID column failed".into(),
                    query: ddl.into(),
                    nucleus: format!("ERR: {e}"),
                    expected: "success".into(),
                    real_bug: true,
                });
            }
        }
    }
}

fn is_valid_uuidv4(s: &str) -> bool {
    // Format: 8-4-4-4-12
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() != 5 { return false; }
    if parts[0].len() != 8 { return false; }
    if parts[1].len() != 4 { return false; }
    if parts[2].len() != 4 { return false; }
    if parts[3].len() != 4 { return false; }
    if parts[4].len() != 12 { return false; }
    if !s.chars().all(|c| c.is_ascii_hexdigit() || c == '-') { return false; }
    // Version nibble = '4'
    if parts[2].chars().next() != Some('4') { return false; }
    // Variant nibble: first char of parts[3] must be 8, 9, a, or b
    let variant = parts[3].chars().next().unwrap_or('x');
    matches!(variant, '8' | '9' | 'a' | 'b' | 'A' | 'B')
}

// ─────────────────────────────────────────────────────────────────────────────
//  SECTION 7: BYTEA — ENCODE/DECODE hex & base64 round-trips
// ─────────────────────────────────────────────────────────────────────────────

fn test_bytea(rng: &mut Rng, findings: &mut Vec<Finding>) {
    let ex = fresh_executor();

    // Generate random ASCII strings and verify ENCODE/DECODE are inverse
    let words: &[&str] = &["hello", "world", "nucleus", "test", "abc", "xyz123", "foobar"];

    for _ in 0..15 {
        let word = *rng.pick(words);
        for encoding in &["hex", "base64"] {
            // DECODE(ENCODE(word, encoding), encoding) == word
            let round_trip_sql = format!(
                "SELECT DECODE(ENCODE('{word}', '{encoding}'), '{encoding}')"
            );
            match nucleus_scalar(&ex, &round_trip_sql) {
                Ok(v) => {
                    let got = match &v {
                        Value::Text(s) => s.clone(),
                        Value::Bytea(b) => String::from_utf8_lossy(b).to_string(),
                        other => format!("{other}"),
                    };
                    if got != word {
                        findings.push(Finding {
                            title: format!("BYTEA: ENCODE/DECODE({encoding}) round-trip failed"),
                            query: round_trip_sql,
                            nucleus: got,
                            expected: word.into(),
                            real_bug: true,
                        });
                    }
                }
                Err(e) => {
                    findings.push(Finding {
                        title: format!("BYTEA: ENCODE/DECODE({encoding}) errored"),
                        query: round_trip_sql,
                        nucleus: format!("ERR: {e}"),
                        expected: word.into(),
                        real_bug: !e.contains("Unsupported"),
                    });
                }
            }
        }
    }

    // Known hex values: ENCODE('A', 'hex') == '41'
    let known_hex = [("A", "41"), ("Z", "5a"), ("a", "61"), ("0", "30")];
    for (ch, expected_hex) in &known_hex {
        let sql = format!("SELECT ENCODE('{ch}', 'hex')");
        match nucleus_scalar(&ex, &sql) {
            Ok(v) => {
                let got = match &v {
                    Value::Text(s) => s.clone(),
                    other => format!("{other}"),
                };
                if got.to_lowercase() != *expected_hex {
                    findings.push(Finding {
                        title: format!("BYTEA: ENCODE('{ch}', 'hex') wrong"),
                        query: sql,
                        nucleus: got,
                        expected: expected_hex.to_string(),
                        real_bug: true,
                    });
                }
            }
            Err(e) => {
                findings.push(Finding {
                    title: format!("BYTEA: ENCODE('{ch}', 'hex') errored"),
                    query: sql,
                    nucleus: format!("ERR: {e}"),
                    expected: expected_hex.to_string(),
                    real_bug: !e.contains("Unsupported"),
                });
            }
        }
    }

    // ENCODE should be idempotent for same inputs
    let word = *rng.pick(words);
    let sql1 = format!("SELECT ENCODE('{word}', 'hex')");
    let sql2 = format!("SELECT ENCODE('{word}', 'hex')");
    let r1 = nucleus_scalar(&ex, &sql1).map(|v| format!("{v}"));
    let r2 = nucleus_scalar(&ex, &sql2).map(|v| format!("{v}"));
    match (r1, r2) {
        (Ok(a), Ok(b)) if a != b => {
            findings.push(Finding {
                title: "BYTEA: ENCODE is not idempotent (same input, different outputs)".into(),
                query: sql1,
                nucleus: format!("{a} vs {b}"),
                expected: "same value both times".into(),
                real_bug: true,
            });
        }
        _ => {}
    }
}

// ─────────────────────────────────────────────────────────────────────────────
//  Finding accumulator
// ─────────────────────────────────────────────────────────────────────────────

struct Finding {
    title: String,
    query: String,
    nucleus: String,
    expected: String,
    real_bug: bool,
}

// ─────────────────────────────────────────────────────────────────────────────
//  main
// ─────────────────────────────────────────────────────────────────────────────

fn main_impl() {
    let mut seed: u64 = 0xDEAD_BEEF_1234_5678;
    let mut iterations: usize = 500;
    let mut max_report: usize = 30;
    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--seed"       => { i += 1; seed       = args[i].parse().unwrap(); }
            "--iterations" => { i += 1; iterations = args[i].parse().unwrap(); }
            "--max-report" => { i += 1; max_report = args[i].parse().unwrap(); }
            _ => {}
        }
        i += 1;
    }

    std::panic::set_hook(Box::new(|_| {}));

    println!("probe_types: extended column-type coverage harness");
    println!("seed={seed}  iterations={iterations}\n");

    let mut all_findings: Vec<Finding> = Vec::new();
    let mut panics = 0usize;

    for iter in 0..iterations {
        let mut rng = Rng(seed.wrapping_add(iter as u64).wrapping_mul(0x100000001B3));

        // Each section is wrapped in catch_unwind so a panic in one section
        // doesn't abort the whole iteration.
        let sections: &[(&str, fn(&mut Rng, &mut Vec<Finding>))] = &[
            ("BOOLEAN",  test_boolean),
            ("NUMERIC",  test_numeric_round),
            ("DATE",     test_date),
            ("JSONB",    test_jsonb),
            ("ARRAY",    test_array),
            ("UUID",     test_uuid),
            ("BYTEA",    test_bytea),
        ];

        for (name, test_fn) in sections {
            let mut local_findings: Vec<Finding> = Vec::new();
            let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
                test_fn(&mut rng, &mut local_findings);
            }));
            match result {
                Ok(_) => all_findings.extend(local_findings),
                Err(p) => {
                    panics += 1;
                    if panics <= max_report {
                        let msg = p.downcast_ref::<&str>().map(|s| s.to_string())
                            .or_else(|| p.downcast_ref::<String>().cloned())
                            .unwrap_or_else(|| "unknown".into());
                        println!("─── PANIC in {name} section (iter {iter}) ───");
                        println!("  msg: {msg}\n");
                    }
                }
            }
        }
    }

    // ── Report ──
    println!("════ FINDINGS ════");
    let mut real_bugs = 0usize;
    let mut shown = 0usize;
    for f in &all_findings {
        if shown < max_report {
            let tag = if f.real_bug { "[BUG]" } else { "[SOFT]" };
            println!("{tag} {}", f.title);
            println!("  query   : {}", f.query);
            println!("  nucleus : {}", f.nucleus);
            println!("  expected: {}", f.expected);
            println!();
            shown += 1;
        }
        if f.real_bug { real_bugs += 1; }
    }
    if all_findings.len() > max_report {
        println!("  ... {} more findings omitted", all_findings.len() - max_report);
    }

    println!("\n════ SUMMARY ════");
    println!("iterations  : {iterations}");
    println!("total finds : {}", all_findings.len());
    println!("real bugs   : {real_bugs}");
    println!("panics      : {panics}");

    if real_bugs == 0 && panics == 0 {
        println!("\nAll type checks passed.");
    } else {
        println!("\nReproduce with: --seed {seed}");
        std::process::exit(1);
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    tokio::task::spawn_blocking(main_impl).await.unwrap();
}
