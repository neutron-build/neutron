//! Resource-exhaustion / DoS guard fuzzer.
//!
//! Invariant (oracle-free): the executor MUST NEVER panic, abort, or hang on
//! any adversarial SQL input.  Graceful `Ok` or `Err` is always acceptable.
//!
//! Adversarial classes exercised:
//!   1. Deeply nested parenthesized / arithmetic expressions (→ expr depth guard)
//!   2. Very long IN lists (→ in-list linear scan; no O(n²) blow-up)
//!   3. Deeply nested subqueries  (→ subquery depth guard: MAX 64)
//!   4. Simple recursive CTEs with bounded depth (→ MAX_RECURSION guard: 1000)
//!   5. Catastrophic-backtracking LIKE patterns on long text
//!   6. Large (but bounded) string arguments to SQL functions
//!   7. Deeply nested AND/OR chains
//!   8. Deeply nested CASE expressions
//!
//! Build: `cargo build --release --features server --bin probe_security`
//! Run:   `cargo run  --release --features server --bin probe_security`
#![cfg(feature = "server")]

use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::{Duration, Instant};

use nucleus::catalog::Catalog;
use nucleus::executor::Executor;
use nucleus::storage::{MvccStorageAdapter, StorageEngine};

// ---------------------------------------------------------------------------
// Deterministic xorshift-64 PRNG (same structure as other probe bins)
// ---------------------------------------------------------------------------
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
}

// ---------------------------------------------------------------------------
// Safety cap: all generated inputs stay within these limits so the *harness
// process* itself cannot OOM or spin.  The guards under test are much lower.
// ---------------------------------------------------------------------------
/// Max number of items in an IN-list.
const MAX_IN_LIST: usize = 50_000;
/// Max nesting depth (kept for documentation; actual probes cap lower to stay in parse-budget).
#[allow(dead_code)]
const MAX_NEST_DEPTH: usize = 512;
/// Max nesting depth for subquery generation.
const MAX_SUBQUERY_DEPTH: usize = 80;
/// Max number of CTE recursion steps (seed the base case as a small integer).
const MAX_CTE_STEPS: usize = 1500;
/// Max length for adversarial string literals (bytes).
const MAX_STR_LEN: usize = 64 * 1024; // 64 KB
/// Execution time budget per individual query (wall clock).
const QUERY_TIMEOUT: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------------------
// Panic message extraction (mirrors probe_crash)
// ---------------------------------------------------------------------------
fn panic_msg(p: &(dyn std::any::Any + Send)) -> String {
    p.downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| p.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic payload".into())
}

// ---------------------------------------------------------------------------
// Run one SQL string; return None = ok/graceful-err; Some(msg) = panic
// ---------------------------------------------------------------------------
fn check(ex: &Executor, sql: &str) -> Option<String> {
    if std::env::var("NUC_TRACE").is_ok() {
        eprintln!("RUN: {sql}");
    }
    let rt = tokio::runtime::Handle::current();
    let res = std::panic::catch_unwind(AssertUnwindSafe(|| {
        tokio::task::block_in_place(|| rt.block_on(ex.execute(sql)))
    }));
    match res {
        Err(p) => Some(panic_msg(&*p)),
        Ok(_) => None,
    }
}

// ---------------------------------------------------------------------------
// Generator 1: deeply nested arithmetic expression (left-associative chain)
// ---------------------------------------------------------------------------
/// Non-exponential nested arithmetic: builds a left-associative chain.
fn gen_nested_arith_linear(depth: usize) -> String {
    let ops = ["+", "-", "*"];
    let mut expr = "1".to_string();
    for i in 0..depth {
        let op = ops[i % ops.len()];
        expr = format!("({} {op} 1)", expr);
    }
    expr
}

// ---------------------------------------------------------------------------
// Generator 2: long IN list
// ---------------------------------------------------------------------------
fn gen_in_list(n: usize, rng: &mut Rng) -> String {
    let vals: Vec<String> = (0..n)
        .map(|_| (rng.next() as i64 % 10_000).to_string())
        .collect();
    format!("SELECT 42 IN ({})", vals.join(","))
}

// ---------------------------------------------------------------------------
// Generator 3: deeply nested subquery
// ---------------------------------------------------------------------------
fn gen_nested_subquery(depth: usize) -> String {
    if depth == 0 {
        return "SELECT 1".into();
    }
    format!("SELECT * FROM ({}) AS sub_{depth}", gen_nested_subquery(depth - 1))
}

// ---------------------------------------------------------------------------
// Generator 4: recursive CTE
// ---------------------------------------------------------------------------
fn gen_recursive_cte(steps: usize) -> String {
    // WITH RECURSIVE counter(n) AS (
    //   SELECT 1
    //   UNION ALL
    //   SELECT n + 1 FROM counter WHERE n < <steps>
    // ) SELECT MAX(n) FROM counter
    format!(
        "WITH RECURSIVE counter(n) AS (\
          SELECT 1 \
          UNION ALL \
          SELECT n + 1 FROM counter WHERE n < {steps}\
        ) SELECT MAX(n) FROM counter"
    )
}

// ---------------------------------------------------------------------------
// Generator 5: catastrophic-backtracking LIKE patterns
// A naively implemented NFA/backtracking matcher blows up on patterns like
// '%a%a%a%a%a%a%a%a%a%' against a string of 'a...a' with a mismatched tail.
// The DP matcher should handle this in O(|text| * |pattern|).
// ---------------------------------------------------------------------------

/// Build a pattern: repeated `%a` segments followed by a mismatch 'b'.
fn gen_catastrophic_like(segments: usize) -> (String, String) {
    // text: "aaa...a" (segments chars) without 'b' at the end → mismatch
    let text = "a".repeat(segments.min(2000));
    let pattern_body = "%a".repeat(segments.min(200));
    let pattern = format!("{pattern_body}b"); // trailing 'b' never matches
    (text, pattern)
}

/// All-percent pattern: '%' repeated n times. Should be O(|text| * |pattern|)
/// not O(|text|^n).
fn gen_all_percent_like(n: usize, text_len: usize) -> (String, String) {
    let text = "x".repeat(text_len.min(4096));
    let pattern = "%".repeat(n.min(500));
    (text, pattern)
}

/// Alternating percent-underscore catastrophic pattern.
fn gen_pct_under_like(n: usize) -> (String, String) {
    let text = "ab".repeat(n.min(500));
    let pattern = "%_".repeat(n.min(200));
    (text, pattern)
}

// ---------------------------------------------------------------------------
// Generator 6: large string arguments
// ---------------------------------------------------------------------------
fn gen_large_string_query(len: usize, rng: &mut Rng) -> String {
    // Repeat a safe char so SQL is valid.
    let c = if rng.chance(50) { 'a' } else { ' ' };
    let s = c.to_string().repeat(len);
    let funcs = [
        format!("SELECT LENGTH('{s}')"),
        format!("SELECT UPPER('{s}')"),
        format!("SELECT LOWER('{s}')"),
        format!("SELECT REVERSE('{s}')"),
        format!("SELECT MD5('{s}')"),
        format!("SELECT LTRIM('{s}')"),
        format!("SELECT RTRIM('{s}')"),
        format!("SELECT TRIM('{s}')"),
    ];
    funcs[rng.below(funcs.len())].clone()
}

// ---------------------------------------------------------------------------
// Generator 7: deeply nested AND/OR chains
// ---------------------------------------------------------------------------
fn gen_nested_and_or(depth: usize) -> String {
    if depth == 0 {
        return "1=1".into();
    }
    let inner = gen_nested_and_or(depth - 1);
    if depth % 2 == 0 {
        format!("({inner} AND 1=1)")
    } else {
        format!("({inner} OR 1=0)")
    }
}

// ---------------------------------------------------------------------------
// Generator 8: deeply nested CASE expressions
// ---------------------------------------------------------------------------
fn gen_nested_case(depth: usize) -> String {
    if depth == 0 {
        return "1".into();
    }
    let inner = gen_nested_case(depth - 1);
    format!("CASE WHEN 1=1 THEN {inner} ELSE 0 END")
}

// ---------------------------------------------------------------------------
// Timed check — returns (panicked, suspected_hang)
// ---------------------------------------------------------------------------
fn timed_check(ex: &Executor, sql: &str) -> (Option<String>, bool) {
    let t0 = Instant::now();
    let result = check(ex, sql);
    let elapsed = t0.elapsed();
    let suspected_hang = elapsed > QUERY_TIMEOUT;
    (result, suspected_hang)
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
fn main_impl() {
    // ── Parse CLI ──────────────────────────────────────────────────────────
    let mut seed: u64 = 0xBAD_C0FFEE;
    let mut max_report = 40usize;
    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--seed" => {
                i += 1;
                seed = args[i].parse().expect("seed must be u64");
            }
            "--max-report" => {
                i += 1;
                max_report = args[i].parse().expect("max-report must be usize");
            }
            _ => {}
        }
        i += 1;
    }

    std::panic::set_hook(Box::new(|_| {}));

    println!("Nucleus security/DoS fuzzer (resource-exhaustion guards)");
    println!("seed={seed}  timeout_per_query={QUERY_TIMEOUT:?}\n");

    // ── Shared executor (stateless SQL; no model data needed for most tests) ──
    let catalog = Arc::new(Catalog::new());
    let storage: Arc<dyn StorageEngine> = Arc::new(MvccStorageAdapter::new());
    let ex = Arc::new(Executor::new(catalog, storage));

    let rt = tokio::runtime::Handle::current();
    // Seed a table so FROM-clause subqueries have something to scan.
    for sql in [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
        "INSERT INTO t VALUES (1,10),(2,20),(3,30)",
    ] {
        let _ = tokio::task::block_in_place(|| rt.block_on(ex.execute(sql)));
    }

    let mut total = 0usize;
    let mut panics = 0usize;
    let mut hangs = 0usize;
    let mut seen_panics: std::collections::HashSet<String> = Default::default();
    let mut seen_hangs: std::collections::HashSet<String> = Default::default();

    macro_rules! probe {
        ($sql:expr) => {{
            total += 1;
            let sql: String = $sql;
            let (panic_opt, hang) = timed_check(&ex, &sql);
            if let Some(msg) = panic_opt {
                let key = format!("{:.80}|{:.80}", sql, msg);
                if seen_panics.insert(key) {
                    panics += 1;
                    if panics <= max_report {
                        println!("=== PANIC #{panics} (total iter {total}) ===");
                        println!("  SQL  : {}", &sql[..sql.len().min(300)]);
                        println!("  panic: {msg}\n");
                    }
                }
            }
            if hang {
                let key = format!("{:.120}", sql);
                if seen_hangs.insert(key) {
                    hangs += 1;
                    if hangs <= max_report {
                        println!("=== SUSPECTED HANG #{hangs} (total iter {total}) ===");
                        println!("  SQL: {}", &sql[..sql.len().min(300)]);
                        println!("  (query took > {QUERY_TIMEOUT:?})\n");
                    }
                }
            }
        }};
    }

    // ────────────────────────────────────────────────────────────────────────
    // CATEGORY 1: nested arithmetic expressions
    // The expr-depth guard fires at 256; test values above and below.
    // ────────────────────────────────────────────────────────────────────────
    println!("--- Category 1: nested arithmetic expressions ---");
    // The expr-depth guard fires at 256; test at/below/above that limit.
    // Cap at 280 to keep parse time sane — the guard behaviour is what we test.
    for depth in [1, 10, 50, 100, 200, 256, 260, 270, 280] {
        let expr = gen_nested_arith_linear(depth);
        probe!(format!("SELECT {expr}"));
    }
    // CAST chains: sqlparser (the parser used by Nucleus) has confirmed exponential
    // backtracking at depth ~47. Testing above that depth would hang THIS harness,
    // not just Nucleus — so we stay well below the blowup point (45 is safe).
    // The finding is documented as a suspected hang in the FINDINGS list.
    {
        let mut expr = "1".to_string();
        for d in 0..45 {
            let t = if d % 3 == 0 { "INTEGER" } else if d % 3 == 1 { "REAL" } else { "TEXT" };
            expr = format!("CAST({expr} AS {t})");
        }
        probe!(format!("SELECT {expr}"));
    }
    // NOTE: CAST depth >=47 was verified to hang sqlparser (exponential parse time).
    // This is a real pre-executor DoS: any client can send ~50 nested CASTs and
    // freeze the Nucleus parser thread.  Documented as finding; not run here.

    // ────────────────────────────────────────────────────────────────────────
    // CATEGORY 2: long IN lists
    // ────────────────────────────────────────────────────────────────────────
    println!("--- Category 2: long IN lists ---");
    let mut rng = Rng(seed);
    for n in [100, 1_000, 10_000, 50_000] {
        probe!(gen_in_list(n, &mut rng));
    }
    // IN list of strings (needs SQL quoting; use simple identifiers)
    {
        let items: Vec<String> = (0..5000).map(|i| format!("'{}'", i)).collect();
        probe!(format!("SELECT 'hello' IN ({})", items.join(",")));
    }
    // Very large IN with NULLs mixed in
    {
        let items: Vec<String> = (0..10_000)
            .map(|i| if i % 100 == 0 { "NULL".to_string() } else { i.to_string() })
            .collect();
        probe!(format!("SELECT 42 IN ({})", items.join(",")));
    }

    // ────────────────────────────────────────────────────────────────────────
    // CATEGORY 3: deeply nested subqueries
    // The subquery depth guard fires at 64.
    // ────────────────────────────────────────────────────────────────────────
    println!("--- Category 3: deeply nested subqueries ---");
    for depth in [1, 10, 30, 64, 65, 70, 80] {
        probe!(gen_nested_subquery(depth));
    }
    // Nested scalar subquery in WHERE
    {
        let mut sq = "SELECT 1".to_string();
        for _d in 0..70 {
            sq = format!("SELECT ({})", sq);
        }
        probe!(format!("SELECT {sq}"));
    }
    // Correlated-style: always references t but stays shallow enough to parse
    {
        probe!("SELECT (SELECT (SELECT (SELECT (SELECT (SELECT (SELECT (SELECT (SELECT (SELECT 1)))))))))) AS x".to_string());
    }

    // ────────────────────────────────────────────────────────────────────────
    // CATEGORY 4: recursive CTEs
    // MAX_RECURSION = 1000 inside the engine; test above and below.
    // ────────────────────────────────────────────────────────────────────────
    println!("--- Category 4: recursive CTEs ---");
    for steps in [0, 1, 10, 100, 500, 999, 1000, 1001, 1500] {
        probe!(gen_recursive_cte(steps));
    }
    // Recursive CTE that produces many rows (bounded)
    {
        probe!(
            "WITH RECURSIVE fib(a,b) AS (\
              SELECT 0,1 \
              UNION ALL \
              SELECT b, a+b FROM fib WHERE a < 1000000\
            ) SELECT COUNT(*) FROM fib".to_string()
        );
    }
    // Non-terminating-looking CTE that converges (the WHERE stops it)
    {
        probe!(
            "WITH RECURSIVE nums(n) AS (\
              SELECT 1 \
              UNION ALL \
              SELECT n+1 FROM nums WHERE n < 2000\
            ) SELECT SUM(n) FROM nums".to_string()
        );
    }

    // ────────────────────────────────────────────────────────────────────────
    // CATEGORY 5: catastrophic LIKE patterns
    // ────────────────────────────────────────────────────────────────────────
    println!("--- Category 5: catastrophic LIKE patterns ---");

    // 5a: %a%a%...%a b pattern against long 'a' string (classic catastrophic)
    for segs in [5, 10, 20, 30, 50, 100, 150, 200] {
        let (text, pattern) = gen_catastrophic_like(segs);
        probe!(format!("SELECT '{text}' LIKE '{pattern}'"));
    }

    // 5b: All-percent patterns — '%%%%%...%%%' against medium text
    for n_pct in [1, 10, 50, 100, 200, 500] {
        let (text, pattern) = gen_all_percent_like(n_pct, 1000);
        probe!(format!("SELECT '{text}' LIKE '{pattern}'"));
    }

    // 5c: Alternating %_ pattern
    for n in [10, 50, 100, 200] {
        let (text, pattern) = gen_pct_under_like(n);
        probe!(format!("SELECT '{text}' LIKE '{pattern}'"));
    }

    // 5d: Very long pattern with leading %
    {
        let pattern = format!("%{}", "a".repeat(2000));
        let text = "a".repeat(2000);
        probe!(format!("SELECT '{text}' LIKE '{pattern}'"));
    }

    // 5e: Pattern longer than text (should fast-reject gracefully)
    {
        let text = "ab";
        let pattern = format!("{}ab{}", "a".repeat(1000), "b".repeat(1000));
        probe!(format!("SELECT '{text}' LIKE '{pattern}'"));
    }

    // 5f: ILIKE versions of the worst cases
    {
        let (text, pattern) = gen_catastrophic_like(100);
        probe!(format!("SELECT '{text}' ILIKE '{pattern}'"));
    }

    // 5g: Random adversarial LIKE patterns via RNG
    let like_chars = ['%', '_', 'a', 'b', '%', '%', '_'];
    for iter in 0..200 {
        let mut rng2 = Rng(seed.wrapping_add(iter).wrapping_mul(0x100000001B3));
        let plen = 10 + rng2.below(100);
        let tlen = 10 + rng2.below(500);
        let pattern: String = (0..plen)
            .map(|_| like_chars[rng2.below(like_chars.len())])
            .collect();
        let text: String = (0..tlen)
            .map(|_| ['a', 'b', 'c'][rng2.below(3)])
            .collect();
        // Escape any single quotes inside the strings to avoid SQL injection into our own query
        let safe_pattern = pattern.replace('\'', "''");
        let safe_text = text.replace('\'', "''");
        probe!(format!("SELECT '{safe_text}' LIKE '{safe_pattern}'"));
    }

    // ────────────────────────────────────────────────────────────────────────
    // CATEGORY 6: large string arguments to scalar functions
    // ────────────────────────────────────────────────────────────────────────
    println!("--- Category 6: large string arguments ---");
    let mut rng6 = Rng(seed ^ 0x6666);
    for sz in [1024, 4 * 1024, 16 * 1024, 64 * 1024] {
        probe!(gen_large_string_query(sz, &mut rng6));
    }
    // REPEAT function: large output (cap at 100_000 to stay safe — above that is OOM territory)
    for n in [1, 1000, 10_000, 100_000] {
        probe!(format!("SELECT LENGTH(REPEAT('ab', {n}))"));
    }
    // LPAD / RPAD: large target width (cap at 100_000)
    for w in [0, 1, 100, 10_000, 100_000] {
        probe!(format!("SELECT LENGTH(LPAD('x', {w}, 'y'))"));
        probe!(format!("SELECT LENGTH(RPAD('x', {w}, 'y'))"));
    }
    // CONCAT of many pieces
    {
        let pieces: Vec<String> = (0..1000).map(|i| format!("'{}'", i % 10)).collect();
        probe!(format!("SELECT LENGTH(CONCAT({}))", pieces.join(",")));
    }
    // CONCAT_WS
    {
        let pieces: Vec<String> = (0..500).map(|_| "'hello'".to_string()).collect();
        probe!(format!("SELECT LENGTH(CONCAT_WS(',', {}))", pieces.join(",")));
    }
    // TRANSLATE with large mapping
    {
        let from_chars = "abcdefghij";
        let to_chars = "0123456789";
        let s = "abcdefghij".repeat(1000);
        probe!(format!("SELECT LENGTH(TRANSLATE('{s}', '{from_chars}', '{to_chars}'))"));
    }

    // ────────────────────────────────────────────────────────────────────────
    // CATEGORY 7: deeply nested AND/OR chains
    // The expr depth guard fires at 256.
    // ────────────────────────────────────────────────────────────────────────
    println!("--- Category 7: deeply nested AND/OR chains ---");
    // Cap at 280 — the expr-depth guard (256) is what we're testing.
    for depth in [10, 50, 100, 200, 256, 260, 280] {
        let expr = gen_nested_and_or(depth);
        probe!(format!("SELECT CASE WHEN {expr} THEN 1 ELSE 0 END"));
    }

    // ────────────────────────────────────────────────────────────────────────
    // CATEGORY 8: deeply nested CASE expressions
    // ────────────────────────────────────────────────────────────────────────
    println!("--- Category 8: deeply nested CASE expressions ---");
    for depth in [10, 50, 100, 200, 256, 270] {
        let expr = gen_nested_case(depth);
        probe!(format!("SELECT {expr}"));
    }

    // ────────────────────────────────────────────────────────────────────────
    // CATEGORY 9: mixed / compound adversarial queries
    // Combine multiple stress axes in a single statement.
    // ────────────────────────────────────────────────────────────────────────
    println!("--- Category 9: compound adversarial queries ---");

    // Long IN inside a subquery
    {
        let items: Vec<String> = (0..5000).map(|i| i.to_string()).collect();
        probe!(format!(
            "SELECT * FROM t WHERE id IN (SELECT id FROM t WHERE v IN ({}))",
            items.join(",")
        ));
    }
    // Deep AND inside a recursive CTE WHERE clause
    {
        let cond = gen_nested_and_or(50);
        probe!(format!(
            "WITH RECURSIVE r(n) AS (\
              SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 100 AND ({cond})\
            ) SELECT MAX(n) FROM r"
        ));
    }
    // LIKE inside recursive CTE
    {
        probe!(
            "WITH RECURSIVE r(s) AS (\
              SELECT 'aaaa' UNION ALL SELECT s || 'a' FROM r WHERE LENGTH(s) < 20\
            ) SELECT COUNT(*) FROM r WHERE s LIKE '%a%a%a%'".to_string()
        );
    }
    // Nested arithmetic inside IN list (keep short — 50-item IN with depth-10 arith)
    {
        let expr = gen_nested_arith_linear(10);
        let items: Vec<String> = (0..50).map(|i| format!("{} + {i}", expr)).collect();
        probe!(format!("SELECT 1 IN ({})", items.join(",")));
    }
    // Large CASE nesting inside a SELECT with many columns
    {
        let case_expr = gen_nested_case(80);
        let cols: Vec<String> = (0..50).map(|i| format!("{case_expr} AS c{i}")).collect();
        probe!(format!("SELECT {}", cols.join(",")));
    }

    // ────────────────────────────────────────────────────────────────────────
    // CATEGORY 10: edge-case scalar / arithmetic inputs for DoS
    // ────────────────────────────────────────────────────────────────────────
    println!("--- Category 10: edge-case arithmetic / overflow inputs ---");
    for sql in [
        // Integer overflow corner cases
        "SELECT 9223372036854775807 + 1",
        "SELECT -9223372036854775808 - 1",
        "SELECT 9223372036854775807 * 2",
        "SELECT 1 / 0",
        "SELECT 0 / 0",
        "SELECT 1 % 0",
        // Float edge cases
        "SELECT 1e308 * 1e308",
        "SELECT -1e308 * 1e308",
        "SELECT 1e-308 / 1e308",
        "SELECT SQRT(-1.0)",
        "SELECT LOG(-1.0)",
        "SELECT LOG(0.0)",
        "SELECT ASIN(2.0)",
        "SELECT ACOS(-2.0)",
        // Large ROUND/TRUNCATE arguments
        "SELECT ROUND(1.5, 2147483647)",
        "SELECT ROUND(1.5, -2147483648)",
        "SELECT TRUNCATE(1.5, 2147483647)",
        // String extremes
        "SELECT SUBSTR('hello', 2147483647, 2147483647)",
        "SELECT SUBSTR('hello', -2147483648, 2147483647)",
        "SELECT LEFT('hello', -1)",
        "SELECT RIGHT('hello', -1)",
        "SELECT LEFT('hello', 2147483647)",
        "SELECT RIGHT('hello', 2147483647)",
        "SELECT LPAD('x', -1, 'y')",
        "SELECT RPAD('x', -1, 'y')",
        // Avoid 2^31-1 width on LPAD/RPAD — would OOM the harness too.
        "SELECT REPEAT('a', -1)",
        // Note: REPEAT with very large N (e.g. 2^31-1) would OOM this process too,
        // so we test only that negative/zero values are handled gracefully.
        "SELECT REPEAT('a', 0)",
        "SELECT SPLIT_PART('a:b:c', ':', 0)",
        "SELECT SPLIT_PART('a:b:c', ':', 2147483647)",
        // NULL propagation
        "SELECT NULL + NULL",
        "SELECT NULL / NULL",
        "SELECT NULL IN (NULL, NULL, NULL)",
        "SELECT NOT NULL",
        "SELECT NULL LIKE NULL",
        "SELECT NULL LIKE '%'",
        "SELECT 'a' LIKE NULL",
        // Type coercion extremes
        "SELECT CAST(9223372036854775807 AS REAL)",
        "SELECT CAST(-9223372036854775808 AS REAL)",
        "SELECT CAST('NaN' AS REAL)",
        "SELECT CAST('Inf' AS REAL)",
        "SELECT CAST('-Inf' AS REAL)",
        "SELECT CAST('' AS INTEGER)",
        "SELECT CAST('' AS REAL)",
        "SELECT CAST('' AS BOOLEAN)",
        "SELECT CAST('true' AS BOOLEAN)",
        "SELECT CAST('false' AS BOOLEAN)",
        "SELECT CAST('1' AS BOOLEAN)",
        // Empty string function inputs
        "SELECT LENGTH('')",
        "SELECT UPPER('')",
        "SELECT REVERSE('')",
        "SELECT SPLIT_PART('', ':', 1)",
        // Zero-length / degenerate array
        "SELECT ARRAY_LENGTH(ARRAY[]::INTEGER[], 1)",
        // ABS overflow
        "SELECT ABS(-9223372036854775808)",
        // MOD extremes
        "SELECT MOD(9223372036854775807, 1)",
        "SELECT MOD(1, 9223372036854775807)",
    ] {
        probe!(sql.to_string());
    }

    // ────────────────────────────────────────────────────────────────────────
    // CATEGORY 11: parser-level stress (very long identifiers, deep SQL)
    // ────────────────────────────────────────────────────────────────────────
    println!("--- Category 11: parser-level stress ---");

    // Very long identifier (alias)
    {
        let long_name = "a".repeat(10_000);
        probe!(format!("SELECT 1 AS {long_name}"));
    }
    // Very long table alias in subquery
    {
        let long_alias = "x".repeat(5_000);
        probe!(format!("SELECT * FROM (SELECT 1) AS {long_alias}"));
    }
    // SELECT with many columns
    {
        let cols: Vec<String> = (0..2000).map(|i| format!("{i}")).collect();
        probe!(format!("SELECT {}", cols.join(",")));
    }
    // INSERT with many values (one row, many columns)
    {
        // This tests the parser; the table won't match, graceful error expected.
        let vals: Vec<String> = (0..500).map(|i| i.to_string()).collect();
        probe!(format!("INSERT INTO fake_table VALUES ({})", vals.join(",")));
    }
    // WHERE with many OR'd conditions
    {
        let conds: Vec<String> = (0..500).map(|i| format!("id = {i}")).collect();
        probe!(format!("SELECT * FROM t WHERE {}", conds.join(" OR ")));
    }
    // ORDER BY many columns
    {
        let cols: Vec<String> = (0..100).map(|i| format!("{} ASC", (i % 2) + 1)).collect();
        probe!(format!("SELECT 1, 2 ORDER BY {}", cols.join(",")));
    }

    // ────────────────────────────────────────────────────────────────────────
    // CATEGORY 12: random adversarial iterations seeded by CLI seed
    // Generates random combinations of the above stress classes.
    // ────────────────────────────────────────────────────────────────────────
    println!("--- Category 12: random adversarial iterations ---");
    let mut rng12 = Rng(seed.wrapping_mul(0x517CC1B727220A95));
    for _ in 0..2000 {
        let sql = match rng12.below(12) {
            0 => {
                // Cap at 280 — beyond is slow to parse, guard fires at 256
                let d = 1 + rng12.below(280);
                format!("SELECT {}", gen_nested_arith_linear(d))
            }
            1 => {
                let n = 1 + rng12.below(MAX_IN_LIST);
                gen_in_list(n, &mut rng12)
            }
            2 => {
                let d = 1 + rng12.below(MAX_SUBQUERY_DEPTH);
                gen_nested_subquery(d)
            }
            3 => {
                let s = rng12.below(MAX_CTE_STEPS);
                gen_recursive_cte(s)
            }
            4 => {
                let segs = 1 + rng12.below(200);
                let (text, pattern) = gen_catastrophic_like(segs);
                let st = text.replace('\'', "''");
                let sp = pattern.replace('\'', "''");
                format!("SELECT '{st}' LIKE '{sp}'")
            }
            5 => {
                let sz = 1 + rng12.below(MAX_STR_LEN);
                gen_large_string_query(sz, &mut rng12)
            }
            6 => {
                // Cap at 280 — beyond is slow to parse
                let d = 1 + rng12.below(280);
                let expr = gen_nested_and_or(d);
                format!("SELECT CASE WHEN {expr} THEN 1 ELSE 0 END")
            }
            7 => {
                let d = 1 + rng12.below(270);
                let expr = gen_nested_case(d);
                format!("SELECT {expr}")
            }
            8 => {
                // All-percent LIKE
                let n = 1 + rng12.below(500);
                let t = 1 + rng12.below(4096);
                let (text, pattern) = gen_all_percent_like(n, t);
                format!("SELECT '{text}' LIKE '{pattern}'")
            }
            9 => {
                // REPEAT stress — cap at 100_000 to avoid OOM in harness
                let n = rng12.below(100_001) as i64;
                format!("SELECT LENGTH(REPEAT('a', {n}))")
            }
            10 => {
                // Deep CASE in subquery (cap at 200 — 270 would be ok but slower)
                let d = 1 + rng12.below(200);
                let expr = gen_nested_case(d);
                format!("SELECT * FROM (SELECT {expr} AS x) AS sub WHERE x IS NOT NULL")
            }
            _ => {
                // Random % / _ pattern mix
                let plen = 1 + rng12.below(300);
                let tlen = 1 + rng12.below(2000);
                let chars = ['%', '_', 'a', 'b'];
                let pattern: String = (0..plen).map(|_| chars[rng12.below(chars.len())]).collect();
                let text: String = (0..tlen).map(|_| ['a', 'b'][rng12.below(2)]).collect();
                format!("SELECT '{text}' LIKE '{pattern}'")
            }
        };
        probe!(sql);
    }

    // ────────────────────────────────────────────────────────────────────────
    // Summary
    // ────────────────────────────────────────────────────────────────────────
    println!("\n========== SUMMARY ==========");
    println!("total probes       : {total}");
    println!("distinct panics    : {panics}");
    println!("suspected hangs    : {hangs}  (wall > {QUERY_TIMEOUT:?})");
    if panics == 0 && hangs == 0 {
        println!("\nAll adversarial inputs handled gracefully (Ok or Err, no panics).");
    } else {
        std::process::exit(1);
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    tokio::task::spawn_blocking(main_impl).await.unwrap();
}
