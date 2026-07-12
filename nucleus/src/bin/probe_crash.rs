//! Oracle-free crash/panic fuzzer. Throws random + adversarial arguments at
//! every registered SQL/model function (and nested expressions) and asserts the
//! executor never PANICS — graceful `Err` is fine, an unwind is a bug. Catches
//! unchecked indexing, arithmetic overflow, bad casts, slice panics, etc.
//! Build: `cargo run --release --features server --bin probe_crash`.
#![cfg(feature = "server")]

use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use nucleus::catalog::Catalog;
use nucleus::executor::Executor;
use nucleus::storage::{MvccStorageAdapter, StorageEngine};

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
    fn pick<'a, T>(&mut self, xs: &'a [T]) -> &'a T {
        &xs[self.below(xs.len())]
    }
}

// Every dispatchable function name (from scalar_fns.rs).
const FUNCS: &[&str] = &[
    "ABS",
    "ACOS",
    "AGE",
    "ARRAY_APPEND",
    "ARRAY_CAT",
    "ARRAY_LENGTH",
    "ARRAY_LOWER",
    "ARRAY_UPPER",
    "ASCII",
    "ASIN",
    "ATAN",
    "ATAN2",
    "BIT_LENGTH",
    "BLOB_COUNT",
    "BLOB_DEDUP_RATIO",
    "BLOB_DELETE",
    "BLOB_GET",
    "BLOB_LIST",
    "BLOB_META",
    "BLOB_STORE",
    "BLOB_TAG",
    "CARDINALITY",
    "CDC_COUNT",
    "CDC_READ",
    "CDC_TABLE_READ",
    "CEILING",
    "CHARACTER_LENGTH",
    "CHR",
    "CLASSIFY",
    "CLOCK_TIMESTAMP",
    "COALESCE",
    "COL_DESCRIPTION",
    "COLUMNAR_AVG",
    "COLUMNAR_COUNT",
    "COLUMNAR_INSERT",
    "COLUMNAR_MAX",
    "COLUMNAR_MIN",
    "COLUMNAR_SUM",
    "CONCAT",
    "CONCAT_WS",
    "COS",
    "COSINE_DISTANCE",
    "CURRENT_DATABASE",
    "CURRENT_DATE",
    "CURRENT_SCHEMA",
    "CURRENT_TIME",
    "CURRENT_TIMESTAMP",
    "CURRVAL",
    "CYPHER",
    "DATALOG_ASSERT",
    "DATALOG_CLEAR",
    "DATALOG_IMPORT",
    "DATALOG_IMPORT_GRAPH",
    "DATALOG_IMPORT_NODES",
    "DATALOG_QUERY",
    "DATALOG_RETRACT",
    "DATALOG_RULE",
    "DATE_BIN",
    "DATE_PART",
    "DATE_TRUNC",
    "DB_BRANCH_CREATE",
    "DB_BRANCH_DELETE",
    "DB_BRANCH_DIFF",
    "DB_BRANCH_LIST",
    "DB_BRANCH_MERGE",
    "DECODE",
    "DEGREES",
    "DOC_COUNT",
    "DOC_GET",
    "DOC_INSERT",
    "DOC_PATH",
    "DOC_QUERY",
    "EMBED",
    "ENCODE",
    "ENCRYPTED_LOOKUP",
    "ENDS_WITH",
    "EXP",
    "FLOOR",
    "FORMAT_TYPE",
    "FTS_DOC_COUNT",
    "FTS_FUZZY_SEARCH",
    "FTS_INDEX",
    "FTS_INDEX_FACETED",
    "FTS_MATCH",
    "FTS_RANK",
    "FTS_REMOVE",
    "FTS_SEARCH",
    "FTS_SEARCH_FILTER",
    "FTS_TERM_COUNT",
    "GCD",
    "GDPR_DELETE_PLAN",
    "GENERATE_SERIES",
    "GRAPH_ADD_EDGE",
    "GRAPH_ADD_NODE",
    "GRAPH_DELETE_EDGE",
    "GRAPH_DELETE_NODE",
    "GRAPH_EDGE_COUNT",
    "GRAPH_NEIGHBORS",
    "GRAPH_NODE_COUNT",
    "GRAPH_NODE_DEGREE",
    "GRAPH_QUERY",
    "GRAPH_SHORTEST_PATH",
    "GRAPH_SHORTEST_PATH_LENGTH",
    "GREATEST",
    "HAS_SCHEMA_PRIVILEGE",
    "HAS_TABLE_PRIVILEGE",
    "INITCAP",
    "INNER_PRODUCT",
    "JSON_EXTRACT_PATH",
    "JSON_EXTRACT_PATH_TEXT",
    "JSON_OBJECT_KEYS",
    "JSON_PRETTY",
    "JSON_SET",
    "JSON_STRIP_NULLS",
    "JSONB_ARRAY_LENGTH",
    "JSONB_BUILD_ARRAY",
    "JSONB_BUILD_OBJECT",
    "JSONB_TYPEOF",
    "KV_CDEL",
    "KV_CEXPIRE",
    "KV_DBSIZE",
    "KV_DEL",
    "KV_EXISTS",
    "KV_EXPIRE",
    "KV_FLUSHDB",
    "KV_GET",
    "KV_HDEL",
    "KV_HEXISTS",
    "KV_HGET",
    "KV_HGETALL",
    "KV_HLEN",
    "KV_HSET",
    "KV_INCR",
    "KV_LINDEX",
    "KV_LLEN",
    "KV_LPOP",
    "KV_LPUSH",
    "KV_LRANGE",
    "KV_PFADD",
    "KV_PFCOUNT",
    "KV_PFMERGE",
    "KV_RPOP",
    "KV_RPUSH",
    "KV_SADD",
    "KV_SCARD",
    "KV_SET",
    "KV_SETNX",
    "KV_SISMEMBER",
    "KV_SMEMBERS",
    "KV_SREM",
    "KV_TTL",
    "KV_ZADD",
    "KV_ZCARD",
    "KV_ZRANGE",
    "KV_ZRANGEBYSCORE",
    "KV_ZREM",
    "L2_DISTANCE",
    "LCM",
    "LEAST",
    "LEFT",
    "LEVENSHTEIN",
    "LOG",
    "LOG10",
    "LOWER",
    "LPAD",
    "LTRIM",
    "MAKE_DATE",
    "MD5",
    "MEM_AVAILABLE",
    "MEM_BUDGET",
    "MEM_PEAK",
    "MEM_PRESSURE_EVENTS",
    "MEM_STATS",
    "MEM_USAGE",
    "MEM_UTILIZATION",
    "MOD",
    "NEXTVAL",
    "NORMALIZE",
    "NULLIF",
    "OBJ_DESCRIPTION",
    "PG_BACKEND_PID",
    "PG_ENCODING_TO_CHAR",
    "PG_GET_CONSTRAINTDEF",
    "PG_GET_EXPR",
    "PG_GET_INDEXDEF",
    "PG_GET_USERBYID",
    "PG_POSTMASTER_START_TIME",
    "PG_TABLE_IS_VISIBLE",
    "PG_TYPEOF",
    "PII_DETECT",
    "PII_DETECT_CATEGORY",
    "PLAINTO_TSQUERY",
    "POW",
    "PREDICT",
    "PROC_DROP",
    "PROC_LIST",
    "PROC_REGISTER",
    "PUBSUB_CHANNELS",
    "PUBSUB_PUBLISH",
    "PUBSUB_SUBSCRIBERS",
    "QUOTE_IDENT",
    "RADIANS",
    "RANDOM",
    "REGEXP_MATCHES",
    "REGEXP_REPLACE",
    "REPEAT",
    "REPLACE",
    "RETENTION_CHECK",
    "RETENTION_SET",
    "REVERSE",
    "RIGHT",
    "ROUND",
    "ROW_TO_JSON",
    "RPAD",
    "RTRIM",
    "SESSION_USER",
    "SETVAL",
    "SIGN",
    "SIN",
    "SPARSE_DOC_COUNT",
    "SPARSE_DOT_PRODUCT",
    "SPARSE_INSERT",
    "SPARSE_REMOVE",
    "SPARSE_SEARCH",
    "SPARSE_WAND",
    "SPLIT_PART",
    "SQRT",
    "ST_AREA",
    "ST_CONTAINS",
    "ST_DISTANCE",
    "ST_DISTANCE_EUCLIDEAN",
    "ST_DWITHIN",
    "ST_MAKEPOINT",
    "ST_X",
    "ST_Y",
    "STARTS_WITH",
    "STREAM_XACK",
    "STREAM_XADD",
    "STREAM_XGROUP_CREATE",
    "STREAM_XLEN",
    "STREAM_XRANGE",
    "STREAM_XREAD",
    "STREAM_XREADGROUP",
    "STRPOS",
    "SUBSCRIBE",
    "SUBSCRIPTION_COUNT",
    "SUBSTR",
    "TAN",
    "TENSOR_COUNT",
    "TENSOR_LIST_VERSIONS",
    "TENSOR_SHAPE",
    "TENSOR_SIZE_BYTES",
    "TENSOR_STORE",
    "TENSOR_VERSIONS",
    "TIME_BUCKET",
    "TO_CHAR",
    "TO_DATE",
    "TO_TIMESTAMP",
    "TO_TSQUERY",
    "TO_TSVECTOR",
    "TRANSLATE",
    "TRIM",
    "TRUNCATE",
    "TS_COUNT",
    "TS_HEADLINE",
    "TS_INSERT",
    "TS_LAST",
    "TS_MATCH",
    "TS_RANGE_AVG",
    "TS_RANGE_COUNT",
    "TS_RETENTION",
    "UNNEST",
    "UPPER",
    "UUID_GENERATE_V4",
    "VECTOR",
    "VECTOR_COSINE_DISTANCE",
    "VECTOR_DIMS",
    "VECTOR_DISTANCE",
    "VECTOR_INNER_PRODUCT",
    "VECTOR_L2_DISTANCE",
    "VERSION",
    "VERSION_BRANCH",
    "VERSION_BRANCHES",
    "VERSION_COMMIT",
    "VERSION_LOG",
];

// Adversarial literal arguments. Counts/sizes are bounded so the fuzzer itself
// can't OOM/hang (true DoS testing is a separate, sandboxed harness).
const INTS: &[&str] = &[
    "0",
    "1",
    "-1",
    "2",
    "-3",
    "7",
    "255",
    "-255",
    "2147483647",
    "-2147483648",
    "9223372036854775807",
    "-9223372036854775808",
    "100",
    "-100",
    "1000",
];
const FLOATS: &[&str] = &[
    "0.0", "-0.0", "1.5", "-3.14", "1e30", "-1e30", "1e-30", "0.000001",
];
const TEXTS: &[&str] = &[
    "''",
    "'a'",
    "'hello world'",
    "'日本語🎉'",
    "'with '' quote'",
    "'%_pat%'",
    "'[1,2,3]'",
    "'{\"k\":1}'",
    "'(a+)+b'",
    "'123'",
    "'-45.6'",
    "'NaN'",
    "'  '",
    "'\\x00'",
    "'2020-13-99'",
    "'1,2,3'",
    "'a:b:c'",
    "'k v:1 w:2'",
];

fn gen_arg(rng: &mut Rng, depth: u32) -> String {
    // Occasionally nest a function call to fuzz composition.
    if depth > 0 && rng.chance(15) {
        return gen_call(rng, depth - 1);
    }
    match rng.below(12) {
        0 => "NULL".into(),
        1 => "TRUE".into(),
        2 => "FALSE".into(),
        3 | 4 => rng.pick(INTS).to_string(),
        5 => rng.pick(FLOATS).to_string(),
        6 => format!("VECTOR('[{}]')", gen_vec_body(rng)),
        7 => format!("ARRAY[{},{}]", rng.pick(INTS), rng.pick(INTS)),
        _ => rng.pick(TEXTS).to_string(),
    }
}

fn gen_vec_body(rng: &mut Rng) -> String {
    let n = rng.below(5); // 0..4 dims, including empty
    (0..n)
        .map(|_| rng.pick(FLOATS).trim_end_matches("e30").to_string())
        .collect::<Vec<_>>()
        .join(",")
}

fn gen_call(rng: &mut Rng, depth: u32) -> String {
    let f = rng.pick(FUNCS);
    let argc = rng.below(6); // 0..5 args → exercises arity checks too
    let args: Vec<String> = (0..argc).map(|_| gen_arg(rng, depth)).collect();
    format!("{f}({})", args.join(","))
}

/// Standalone expression fuzzing: arithmetic with extremes, casts, nesting.
fn gen_expr(rng: &mut Rng, depth: u32) -> String {
    if depth == 0 {
        return rng.pick(INTS).to_string();
    }
    match rng.below(7) {
        0 => {
            let op = rng.pick(&["+", "-", "*", "/", "%"]);
            format!(
                "({} {op} {})",
                gen_expr(rng, depth - 1),
                gen_expr(rng, depth - 1)
            )
        }
        1 => format!(
            "CAST({} AS {})",
            gen_expr(rng, depth - 1),
            rng.pick(&[
                "INTEGER",
                "BIGINT",
                "REAL",
                "TEXT",
                "BOOLEAN",
                "DATE",
                "TIMESTAMP"
            ])
        ),
        2 => format!("-{}", gen_expr(rng, depth - 1)),
        3 => format!("ABS({})", gen_expr(rng, depth - 1)),
        4 => gen_call(rng, depth - 1),
        5 => rng.pick(FLOATS).to_string(),
        _ => rng.pick(TEXTS).to_string(),
    }
}

fn panic_msg(p: &(dyn std::any::Any + Send)) -> String {
    p.downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| p.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown".into())
}

/// Returns Some(panic_message) iff the executor unwound; None otherwise
/// (success OR graceful Err — both acceptable).
fn check_panic(ex: &Executor, sql: &str) -> Option<String> {
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

fn main_impl() {
    let mut seed: u64 = 0xDEAD_BEEF;
    let mut iterations = 200_000usize;
    let mut max_report = 40usize;
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
            _ => {}
        }
        i += 1;
    }
    std::panic::set_hook(Box::new(|_| {}));

    println!("Nucleus crash/panic fuzzer ({} functions)", FUNCS.len());
    println!("seed={seed} iterations={iterations}\n");

    // One executor with a small table + some seeded model state, so functions
    // exercise both empty and populated paths.
    let catalog = Arc::new(Catalog::new());
    let storage: Arc<dyn StorageEngine> = Arc::new(MvccStorageAdapter::new());
    let ex = Arc::new(Executor::new(catalog, storage));
    let rt = tokio::runtime::Handle::current();
    for setup in [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, txt TEXT)",
        "INSERT INTO t VALUES (1,10,'alpha'),(2,20,'beta'),(3,30,'gamma')",
    ] {
        let _ = tokio::task::block_in_place(|| rt.block_on(ex.execute(setup)));
    }

    let mut panics = 0usize;
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();

    for iter in 0..iterations {
        let mut rng = Rng(seed.wrapping_add(iter as u64).wrapping_mul(0x100000001B3));
        let sql = if rng.chance(70) {
            format!("SELECT {}", gen_call(&mut rng, 2))
        } else {
            format!("SELECT {}", gen_expr(&mut rng, 3))
        };
        if let Some(msg) = check_panic(&ex, &sql) {
            // Dedup by function name + message so we report distinct root causes.
            let key = format!("{}|{}", sql.split('(').next().unwrap_or(""), msg);
            if seen.insert(key) {
                panics += 1;
                if panics <= max_report {
                    println!("─── PANIC #{panics} (iter {iter}, seed {seed}) ───");
                    println!("  sql  : {sql}");
                    println!("  panic: {msg}\n");
                }
            }
        }
    }

    println!("\n════ SUMMARY ════");
    println!("iterations         : {iterations}");
    println!("distinct panics    : {panics}");
    if panics == 0 {
        println!("\nNo panics across all functions. 🎯");
    } else {
        std::process::exit(1);
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    tokio::task::spawn_blocking(main_impl).await.unwrap();
}
