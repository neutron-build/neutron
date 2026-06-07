//! Join-shape differential fuzzer: Nucleus vs SQLite.
//!
//! Generates TWO or THREE related tables and fuzzes:
//!   - INNER / LEFT / RIGHT / CROSS joins with multi-predicate ON clauses
//!   - Correlated subqueries (WHERE x IN/EXISTS (SELECT ... WHERE inner.k = outer.k))
//!   - Derived/nested tables (FROM (SELECT ...) AS s)
//!   - Scalar subqueries in the SELECT list
//!   - Combinations with GROUP BY / HAVING / ORDER BY / LIMIT
//!
//! SQLite is the oracle. Any result divergence (given identical inputs) is
//! reported with a minimal reproducible sequence.
//!
//! Build:
//!   cargo build --release --features "server rusqlite" --bin probe_joins
//! Run:
//!   cargo run --release --features "server rusqlite" --bin probe_joins
//!   cargo run --release --features "server rusqlite" --bin probe_joins -- --seed 42 --iterations 3000
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

// ─── Schema (one table) ───────────────────────────────────────────────────────
#[derive(Clone, Copy, PartialEq, Eq)]
enum Ty { Int, Text }

#[derive(Clone)]
struct Col {
    name: &'static str,
    ty: Ty,
    nn: bool,
}

struct Schema {
    tname: &'static str,
    cols: Vec<Col>, // cols[0] always `id INTEGER PRIMARY KEY`
}

const COL_NAMES: &[&str] = &["c1", "c2", "c3", "c4"];
// Small integer and text value domains for high join-hit rate.
const INT_VALS: &[i64] = &[1, 2, 3, 4, 5, 6, 7, 8];
// Lowercase text only — keeps LIKE behaviour identical between engines.
const TEXT_VALS: &[&str] = &["alpha", "beta", "gamma", "delta", "eta"];

impl Schema {
    fn new(tname: &'static str, rng: &mut Rng) -> Self {
        let mut cols = vec![Col { name: "id", ty: Ty::Int, nn: true }];
        // Guarantee a NOT NULL int join-key column (col index 1).
        cols.push(Col { name: COL_NAMES[0], ty: Ty::Int, nn: true });
        // Guarantee a NOT NULL text column (col index 2).
        cols.push(Col { name: COL_NAMES[1], ty: Ty::Text, nn: true });
        // 0–2 extra nullable columns for variety.
        let extra = rng.below(3);
        for k in 0..extra {
            let ty = if rng.chance(60) { Ty::Int } else { Ty::Text };
            cols.push(Col { name: COL_NAMES[2 + k], ty, nn: false });
        }
        Schema { tname, cols }
    }

    fn ddl(&self) -> String {
        let parts: Vec<String> = self.cols.iter().enumerate().map(|(i, c)| {
            if i == 0 { return "id INTEGER PRIMARY KEY".to_string(); }
            let ty = match c.ty { Ty::Int => "INTEGER", Ty::Text => "TEXT" };
            let nn = if c.nn { " NOT NULL" } else { "" };
            format!("{} {ty}{nn}", c.name)
        }).collect();
        format!("CREATE TABLE {} ({})", self.tname, parts.join(", "))
    }

    fn int_join_col(&self) -> &'static str { COL_NAMES[0] } // always c1
    fn text_col(&self) -> &'static str { COL_NAMES[1] }     // always c2

    fn gen_inserts(&self, rng: &mut Rng, rows: usize) -> String {
        let names: Vec<&str> = self.cols.iter().map(|c| c.name).collect();
        let mut vals = Vec::with_capacity(rows);
        for id in 1..=rows {
            let mut cells = Vec::with_capacity(self.cols.len());
            for (i, c) in self.cols.iter().enumerate() {
                cells.push(if i == 0 {
                    id.to_string()
                } else {
                    gen_cell(rng, c)
                });
            }
            vals.push(format!("({})", cells.join(",")));
        }
        format!("INSERT INTO {} ({}) VALUES {}", self.tname, names.join(","), vals.join(","))
    }
}

fn gen_cell(rng: &mut Rng, c: &Col) -> String {
    if !c.nn && rng.chance(20) { return "NULL".into(); }
    match c.ty {
        Ty::Int => rng.pick(INT_VALS).to_string(),
        Ty::Text => format!("'{}'", rng.pick(TEXT_VALS)),
    }
}

// ─── Query generation ─────────────────────────────────────────────────────────

/// Generate a WHERE predicate involving a single table aliased as `alias`.
fn gen_pred(rng: &mut Rng, alias: &str, s: &Schema) -> String {
    let c = s.cols.get(1 + rng.below(s.cols.len() - 1)).unwrap_or(&s.cols[1]);
    let (cref, lit) = (format!("{alias}.{}", c.name), gen_lit(rng, c));
    let op = rng.pick(&["=", "<>", "<", "<=", ">", ">="]);
    format!("{cref} {op} {lit}")
}

fn gen_lit(rng: &mut Rng, c: &Col) -> String {
    match c.ty {
        Ty::Int => rng.pick(INT_VALS).to_string(),
        Ty::Text => format!("'{}'", rng.pick(TEXT_VALS)),
    }
}

/// ORDER BY that is always deterministic:
///   - primary key from each contributing table prefix (always NOT NULL, unique)
fn det_orderby_two(a_alias: &str, b_alias: &str) -> String {
    format!("ORDER BY {a_alias}.id ASC, {b_alias}.id ASC")
}
#[allow(dead_code)]
fn det_orderby_three(a_alias: &str, b_alias: &str, c_alias: &str) -> String {
    format!("ORDER BY {a_alias}.id ASC, {b_alias}.id ASC, {c_alias}.id ASC")
}

/// (sql, ordered) — ordered means rows must match positionally.
fn gen_join_query(
    rng: &mut Rng,
    sa: &Schema, // table a
    sb: &Schema, // table b
    sc: Option<&Schema>, // optional table c
    rows_a: usize,
    rows_b: usize,
) -> (String, bool) {
    match rng.below(12) {
        // ── INNER JOIN (2 tables, multi-predicate ON) ──────────────────────
        0 | 1 => {
            let (aa, ba) = ("a", "b");
            let jcol = sa.int_join_col();
            // Multi-predicate ON: equality + optional extra bound
            let extra = if rng.chance(40) {
                let lo = *rng.pick(INT_VALS);
                format!(" AND {aa}.{jcol} >= {lo}")
            } else {
                String::new()
            };
            let proj = if rng.chance(50) {
                format!("{aa}.id, {ba}.id, {aa}.{jcol}", aa=aa, ba=ba, jcol=jcol)
            } else {
                format!("{aa}.id, {ba}.id, {aa}.{}, {ba}.{}", sa.text_col(), sb.text_col(), aa=aa, ba=ba)
            };
            let w = if rng.chance(40) {
                format!(" WHERE {}", gen_pred(rng, aa, sa))
            } else { String::new() };
            (
                format!(
                    "SELECT {proj} FROM {ta} {aa} JOIN {tb} {ba} ON {aa}.{jcol} = {ba}.{jcol}{extra}{w} {ob}",
                    ta=sa.tname, tb=sb.tname, ob=det_orderby_two(aa, ba)
                ),
                true,
            )
        }
        // ── LEFT JOIN ──────────────────────────────────────────────────────
        2 | 3 => {
            let (aa, ba) = ("a", "b");
            let jcol = sa.int_join_col();
            let extra_on = if rng.chance(35) {
                let k = *rng.pick(INT_VALS);
                format!(" AND {ba}.{jcol} < {k}", ba=ba, jcol=jcol)
            } else { String::new() };
            (
                format!(
                    "SELECT {aa}.id, {ba}.id, {aa}.{jc} FROM {ta} {aa} LEFT JOIN {tb} {ba} ON {aa}.{jc} = {ba}.{jc}{ext} {ob}",
                    ta=sa.tname, tb=sb.tname, aa=aa, ba=ba, jc=jcol, ext=extra_on,
                    ob=det_orderby_two(aa, ba)
                ),
                true,
            )
        }
        // ── RIGHT JOIN ──────────────────────────────────────────────────────
        4 => {
            let (aa, ba) = ("a", "b");
            let jcol = sa.int_join_col();
            // RIGHT JOIN: only columns from the right (b) side are guaranteed
            // non-NULL in unmatched rows. Use b.id as tiebreaker.
            (
                format!(
                    "SELECT {ba}.id, {aa}.id, {ba}.{jc} FROM {ta} {aa} RIGHT JOIN {tb} {ba} ON {aa}.{jc} = {ba}.{jc} ORDER BY {ba}.id ASC, {aa}.id ASC",
                    ta=sa.tname, tb=sb.tname, aa=aa, ba=ba, jc=jcol
                ),
                true,
            )
        }
        // ── CROSS JOIN ─────────────────────────────────────────────────────
        5 => {
            let (aa, ba) = ("a", "b");
            // Small row counts to keep result manageable.
            let limit = 1 + rng.below(rows_a.max(1) * rows_b.max(1)).min(50);
            (
                format!(
                    "SELECT {aa}.id, {ba}.id FROM {ta} {aa} CROSS JOIN {tb} {ba} ORDER BY {aa}.id ASC, {ba}.id ASC LIMIT {limit}",
                    ta=sa.tname, tb=sb.tname, aa=aa, ba=ba
                ),
                true,
            )
        }
        // ── Correlated subquery: IN ────────────────────────────────────────
        6 => {
            let (aa, ba) = ("a", "b");
            let jcol = sa.int_join_col();
            let neg = if rng.chance(40) { "NOT " } else { "" };
            let inner_pred = if rng.chance(40) {
                format!(" AND {ba}.{jc} > {v}", ba=ba, jc=jcol, v=*rng.pick(INT_VALS))
            } else { String::new() };
            (
                format!(
                    "SELECT {aa}.id FROM {ta} {aa} WHERE {neg}EXISTS (SELECT 1 FROM {tb} {ba} WHERE {ba}.{jc} = {aa}.{jc}{ip}) ORDER BY {aa}.id ASC",
                    ta=sa.tname, tb=sb.tname, aa=aa, ba=ba, jc=jcol, ip=inner_pred
                ),
                true,
            )
        }
        // ── Correlated subquery: EXISTS ────────────────────────────────────
        7 => {
            let (aa, ba) = ("a", "b");
            let jcol = sa.int_join_col();
            let neg = if rng.chance(40) { "NOT " } else { "" };
            let extra_pred = if rng.chance(40) {
                format!(" AND {}", gen_pred(rng, ba, sb))
            } else { String::new() };
            (
                format!(
                    "SELECT {aa}.id FROM {ta} {aa} WHERE {aa}.{jc} {neg}IN (SELECT {ba}.{jc} FROM {tb} {ba} WHERE {ba}.{jc} = {aa}.{jc}{ep}) ORDER BY {aa}.id ASC",
                    ta=sa.tname, tb=sb.tname, aa=aa, ba=ba, jc=jcol, ep=extra_pred
                ),
                true,
            )
        }
        // ── Derived/nested table ───────────────────────────────────────────
        8 => {
            let (aa, da) = ("a", "d");
            let jcol = sa.int_join_col();
            let inner_where = if rng.chance(50) {
                let k = *rng.pick(INT_VALS);
                format!(" WHERE {jc} > {k}", jc=jcol)
            } else { String::new() };
            let inner_limit = if rng.chance(40) {
                format!(" ORDER BY id ASC LIMIT {}", 2 + rng.below(rows_b.max(1)))
            } else { String::new() };
            (
                format!(
                    "SELECT {aa}.id, {da}.{jc} FROM {ta} {aa} JOIN (SELECT id, {jc} FROM {tb}{iw}{il}) AS {da} ON {aa}.{jc} = {da}.{jc} ORDER BY {aa}.id ASC, {da}.id ASC",
                    ta=sa.tname, tb=sb.tname, aa=aa, da=da, jc=jcol, iw=inner_where, il=inner_limit
                ),
                true,
            )
        }
        // ── Scalar subquery in SELECT ──────────────────────────────────────
        9 => {
            let (aa, ba) = ("a", "b");
            let jcol = sa.int_join_col();
            let agg = rng.pick(&["COUNT(*)", "MAX(id)", "MIN(id)"]);
            let w = if rng.chance(40) { format!(" WHERE {}", gen_pred(rng, aa, sa)) } else { String::new() };
            (
                format!(
                    "SELECT {aa}.id, (SELECT {agg} FROM {tb} {ba} WHERE {ba}.{jc} = {aa}.{jc}) AS sub FROM {ta} {aa}{w} ORDER BY {aa}.id ASC",
                    ta=sa.tname, tb=sb.tname, aa=aa, ba=ba, jc=jcol
                ),
                true,
            )
        }
        // ── Three-table join with GROUP BY / HAVING ────────────────────────
        10 => {
            if let Some(sc) = sc {
                let (aa, ba, ca) = ("a", "b", "c");
                let jcol = sa.int_join_col();
                let inner_join = if rng.chance(50) { "JOIN" } else { "LEFT JOIN" };
                let having = if rng.chance(40) {
                    let k = rng.int(0, 3);
                    format!(" HAVING COUNT(*) >= {k}")
                } else { String::new() };
                (
                    format!(
                        "SELECT {aa}.{jc}, COUNT(*) AS cnt FROM {ta} {aa} {ij} {tb} {ba} ON {aa}.{jc} = {ba}.{jc} {ij} {tc} {ca} ON {aa}.{jc} = {ca}.{jc} GROUP BY {aa}.{jc}{hv} ORDER BY {aa}.{jc} ASC",
                        ta=sa.tname, tb=sb.tname, tc=sc.tname, aa=aa, ba=ba, ca=ca,
                        jc=jcol, ij=inner_join, hv=having
                    ),
                    true,
                )
            } else {
                // Fallback: INNER JOIN + GROUP BY on 2 tables.
                let (aa, ba) = ("a", "b");
                let jcol = sa.int_join_col();
                (
                    format!(
                        "SELECT {aa}.{jc}, COUNT(*) FROM {ta} {aa} JOIN {tb} {ba} ON {aa}.{jc} = {ba}.{jc} GROUP BY {aa}.{jc} ORDER BY {aa}.{jc} ASC",
                        ta=sa.tname, tb=sb.tname, aa=aa, ba=ba, jc=jcol
                    ),
                    true,
                )
            }
        }
        // ── JOIN + ORDER BY + LIMIT ────────────────────────────────────────
        _ => {
            let (aa, ba) = ("a", "b");
            let jcol = sa.int_join_col();
            let kind = rng.pick(&["JOIN", "LEFT JOIN"]);
            let limit = 1 + rng.below(rows_a.max(1) + rows_b.max(1)).min(40);
            let w = if rng.chance(40) { format!(" WHERE {}", gen_pred(rng, aa, sa)) } else { String::new() };
            (
                format!(
                    "SELECT {aa}.id, {ba}.id, {aa}.{jc} FROM {ta} {aa} {kind} {tb} {ba} ON {aa}.{jc} = {ba}.{jc}{w} {ob} LIMIT {limit}",
                    ta=sa.tname, tb=sb.tname, aa=aa, ba=ba, jc=jcol,
                    ob=det_orderby_two(aa, ba)
                ),
                true,
            )
        }
    }
}

// ─── Value canonicalization ───────────────────────────────────────────────────
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
        Value::Bool(b) => if *b { "1" } else { "0" }.into(),
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

// ─── Executor wrappers ────────────────────────────────────────────────────────
fn panic_msg(p: &(dyn std::any::Any + Send)) -> String {
    p.downcast_ref::<&str>().map(|s| s.to_string())
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
    let rows_iter = stmt.query_map([], |row| {
        let mut cells = Vec::with_capacity(ncol);
        for i in 0..ncol {
            let v: rusqlite::types::Value = row.get(i)?;
            cells.push(canon_sqlite(&v));
        }
        Ok(cells)
    }).map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    for r in rows_iter {
        out.push(r.map_err(|e| e.to_string())?);
    }
    Ok(out)
}

fn compare(mut nuc: Vec<Vec<String>>, mut sq: Vec<Vec<String>>, ordered: bool) -> bool {
    if !ordered { nuc.sort(); sq.sort(); }
    nuc == sq
}

fn preview(rows: &[Vec<String>]) -> String {
    let shown: Vec<String> = rows.iter().take(6).map(|r| format!("[{}]", r.join(","))).collect();
    let more = if rows.len() > 6 { format!(" …(+{})", rows.len() - 6) } else { String::new() };
    format!("{}{more}", shown.join(" "))
}

// ─── Main loop ────────────────────────────────────────────────────────────────
fn main_impl() {
    let mut seed: u64 = 0xABCD_EF01;
    let mut iterations: usize = 3000;
    let mut queries_per: usize = 20;
    let mut max_report: usize = 15;

    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    // Accept both decimal and 0x… hex seeds.
    fn parse_u64(s: &str) -> u64 {
        if let Some(hex) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
            u64::from_str_radix(hex, 16).unwrap_or_else(|_| s.parse().unwrap())
        } else {
            s.parse().unwrap()
        }
    }
    while i < args.len() {
        match args[i].as_str() {
            "--seed"       => { i += 1; seed       = parse_u64(&args[i]); }
            "--iterations" => { i += 1; iterations = args[i].parse().unwrap(); }
            "--queries"    => { i += 1; queries_per = args[i].parse().unwrap(); }
            "--max-report" => { i += 1; max_report  = args[i].parse().unwrap(); }
            _ => {}
        }
        i += 1;
    }

    std::panic::set_hook(Box::new(|_| {}));

    println!("Nucleus ⇄ SQLite join-shape differential fuzzer");
    println!("seed={seed} iterations={iterations} queries/iter={queries_per}\n");

    let mut total       = 0usize;
    let mut divergences = 0usize;
    let mut panics      = 0usize;
    let mut nuc_errors  = 0usize;

    'outer: for iter in 0..iterations {
        let mut rng = Rng(seed.wrapping_add(iter as u64).wrapping_mul(0x100000001B3));

        // ── Build two (occasionally three) related schemas ─────────────────
        let use_three = rng.chance(30);
        let sa = Schema::new("ta", &mut rng);
        let sb = Schema::new("tb", &mut rng);
        let sc_opt: Option<Schema> = if use_three { Some(Schema::new("tc", &mut rng)) } else { None };

        let rows_a = 4 + rng.below(10);
        let rows_b = 4 + rng.below(10);
        let rows_c = 4 + rng.below(8);

        let ddl_a   = sa.ddl();
        let ddl_b   = sb.ddl();
        let ins_a   = sa.gen_inserts(&mut rng, rows_a);
        let ins_b   = sb.gen_inserts(&mut rng, rows_b);
        let ddl_c   = sc_opt.as_ref().map(|s| s.ddl());
        let ins_c   = sc_opt.as_ref().map(|s| s.gen_inserts(&mut rng, rows_c));

        // ── Fresh engines ──────────────────────────────────────────────────
        let catalog: Arc<_> = Arc::new(Catalog::new());
        let storage: Arc<dyn StorageEngine> = Arc::new(MvccStorageAdapter::new());
        let ex = Arc::new(Executor::new(catalog, storage));
        let sqlite = Connection::open_in_memory().unwrap();

        // Setup — must succeed on both.
        let mut setup_stmts = vec![ddl_a.clone(), ins_a.clone(), ddl_b.clone(), ins_b.clone()];
        if let (Some(dc), Some(ic)) = (&ddl_c, &ins_c) {
            setup_stmts.push(dc.clone());
            setup_stmts.push(ic.clone());
        }

        let mut ok = true;
        for stmt in &setup_stmts {
            if exec_nucleus(&ex, stmt).is_err() || sqlite.execute_batch(stmt).is_err() {
                ok = false;
                break;
            }
        }
        if !ok { continue 'outer; }

        // ── Query loop ─────────────────────────────────────────────────────
        for _ in 0..queries_per {
            total += 1;

            let (q, ordered) = gen_join_query(
                &mut rng, &sa, &sb, sc_opt.as_ref(), rows_a, rows_b,
            );

            let n = run_nucleus(&ex, &q);
            let s = run_sqlite(&sqlite, &q);

            match (n, s) {
                (Ok(nr), Ok(sr)) => {
                    if !compare(nr.clone(), sr.clone(), ordered) {
                        divergences += 1;
                        if divergences <= max_report {
                            let (mut a, mut b) = (nr, sr);
                            if !ordered { a.sort(); b.sort(); }
                            println!("─── DIVERGENCE #{divergences} (iter {iter}, seed {seed}) ───");
                            println!("  query   : {q}");
                            println!("  nucleus ({} rows): {}", a.len(), preview(&a));
                            println!("  sqlite  ({} rows): {}", b.len(), preview(&b));
                            println!("  ── setup ({} stmts) ──", setup_stmts.len());
                            for s in &setup_stmts { println!("    {s};"); }
                            println!("    {q};");
                            println!();
                        }
                    }
                }
                (Err(ne), Ok(_)) => {
                    if ne.starts_with("PANIC:") {
                        panics += 1;
                        if panics <= max_report {
                            println!("─── PANIC #{panics} (iter {iter}) ───");
                            println!("  sql : {q}");
                            println!("  {ne}");
                            println!("  ── setup ──");
                            for s in &setup_stmts { println!("    {s};"); }
                            println!();
                        }
                        if panics > max_report { std::process::exit(1); }
                        continue 'outer;
                    } else {
                        nuc_errors += 1;
                        if nuc_errors <= max_report {
                            println!("─── NUCLEUS-ERROR (iter {iter}) ───");
                            println!("  sql: {q}");
                            println!("  err: {ne}\n");
                        }
                    }
                }
                _ => {} // both errored or SQLite errored — skip
            }
        }
    }

    println!("\n════ SUMMARY ════");
    println!("queries run        : {total}");
    println!("RESULT divergences : {divergences}");
    println!("PANICS             : {panics}");
    println!("nucleus-only errors: {nuc_errors}");
    if divergences == 0 && panics == 0 {
        println!("\nNo divergences and no panics. probe_joins clean.");
    } else {
        println!("\nReproduce with: --seed {seed} --iterations <iter+1>");
        std::process::exit(1);
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    tokio::task::spawn_blocking(main_impl).await.unwrap();
}
