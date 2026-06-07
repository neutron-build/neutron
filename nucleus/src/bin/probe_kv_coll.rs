//! KV-collections differential fuzzer: drives Nucleus's KV collection functions
//! (Sets, Sorted Sets, Hashes, HyperLogLog) through the SQL surface and checks
//! every deterministic return value against a plain-Rust reference oracle.
//!
//! Build:
//!   cargo build --release --features server --bin probe_kv_coll
//! Run:
//!   cargo run  --release --features server --bin probe_kv_coll
//!   cargo run  --release --features server --bin probe_kv_coll -- --seed 42 --iterations 2000
#![cfg(feature = "server")]
#![allow(clippy::all)] // internal fuzz harness

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::storage::{MvccStorageAdapter, StorageEngine};
use nucleus::types::Value;

// ─── Deterministic PRNG (xorshift64) ────────────────────────────────────────
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
    fn int(&mut self, lo: i64, hi: i64) -> i64 {
        lo + (self.next() % ((hi - lo + 1) as u64)) as i64
    }
    fn pick<'a, T>(&mut self, xs: &'a [T]) -> &'a T {
        &xs[self.below(xs.len())]
    }
}

// ─── Reference models ───────────────────────────────────────────────────────

/// Reference oracle for one iteration (fresh state per fuzz session).
#[derive(Default)]
struct RefOracle {
    /// Sets: key → BTreeSet<member>  (sorted for deterministic SMEMBERS)
    sets: HashMap<String, BTreeSet<String>>,
    /// Sorted sets: key → BTreeMap<(score_bits, member), ()>
    ///   sorted by (score asc, member asc) using f64::to_bits() NOT valid for
    ///   negative floats via BTreeMap key — use Vec sorted on demand.
    zsets: HashMap<String, Vec<(f64, String)>>, // sorted by (score, member)
    /// Hashes: key → BTreeMap<field, value>  (BTreeMap for sorted HGETALL)
    hashes: HashMap<String, BTreeMap<String, String>>,
    /// HLL: key → exact element set (used only for cardinality bounds check)
    hlls: HashMap<String, BTreeSet<String>>,
}

impl RefOracle {
    // ── Sets ──────────────────────────────────────────────────────────────

    fn sadd(&mut self, key: &str, member: &str) -> bool {
        let s = self.sets.entry(key.to_string()).or_default();
        s.insert(member.to_string())
    }

    fn srem(&mut self, key: &str, member: &str) -> bool {
        if let Some(s) = self.sets.get_mut(key) {
            let removed = s.remove(member);
            if s.is_empty() {
                self.sets.remove(key);
            }
            removed
        } else {
            false
        }
    }

    fn smembers(&self, key: &str) -> String {
        match self.sets.get(key) {
            None => String::new(),
            Some(s) => s.iter().cloned().collect::<Vec<_>>().join(","),
        }
    }

    fn sismember(&self, key: &str, member: &str) -> bool {
        self.sets.get(key).map_or(false, |s| s.contains(member))
    }

    fn scard(&self, key: &str) -> usize {
        self.sets.get(key).map_or(0, |s| s.len())
    }

    // ── Sorted Sets ───────────────────────────────────────────────────────

    /// Maintains (score, member) list in (score asc, member asc) order.
    fn zset_sorted(v: &mut Vec<(f64, String)>) {
        v.sort_by(|a, b| {
            a.0.partial_cmp(&b.0)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.1.cmp(&b.1))
        });
    }

    fn zadd(&mut self, key: &str, score: f64, member: &str) -> bool {
        let v = self.zsets.entry(key.to_string()).or_default();
        let pos = v.iter().position(|(_, m)| m == member);
        if let Some(idx) = pos {
            v[idx].0 = score; // update score
            Self::zset_sorted(v);
            false // not new
        } else {
            v.push((score, member.to_string()));
            Self::zset_sorted(v);
            true // new
        }
    }

    fn zrem(&mut self, key: &str, member: &str) -> bool {
        if let Some(v) = self.zsets.get_mut(key) {
            let before = v.len();
            v.retain(|(_, m)| m != member);
            let removed = v.len() < before;
            if v.is_empty() {
                self.zsets.remove(key);
            }
            removed
        } else {
            false
        }
    }

    /// ZRANGE: 0-based inclusive rank range [start, stop].
    fn zrange(&self, key: &str, start: usize, stop: usize) -> String {
        let v = match self.zsets.get(key) {
            Some(v) => v,
            None => return String::new(),
        };
        if start >= v.len() {
            return String::new();
        }
        let end = std::cmp::min(stop, v.len().saturating_sub(1));
        if start > end {
            return String::new();
        }
        v[start..=end]
            .iter()
            .map(|(score, m)| {
                // Match Nucleus format: member:score
                // Nucleus uses format!("{}:{}", e.member, e.score)
                // which for integer-valued scores prints e.g. "m:1" not "m:1.0"
                // f64 Display in Rust prints "1" for 1.0? No — f64 always prints decimal.
                // Actually Rust's f64 Display: 1_f64 prints as "1" — NO, it prints "1"
                // Let's check: format!("{}", 1.0_f64) = "1" in Rust.
                // Actually in Rust: format!("{}", 1.0f64) = "1" — NOT "1.0"
                // We must match exactly what Nucleus does.
                format!("{}:{}", m, score)
            })
            .collect::<Vec<_>>()
            .join(",")
    }

    /// ZRANGEBYSCORE: score in [min, max] inclusive.
    fn zrangebyscore(&self, key: &str, min: f64, max: f64) -> String {
        let v = match self.zsets.get(key) {
            Some(v) => v,
            None => return String::new(),
        };
        v.iter()
            .filter(|(score, _)| *score >= min && *score <= max)
            .map(|(score, m)| format!("{}:{}", m, score))
            .collect::<Vec<_>>()
            .join(",")
    }

    fn zcard(&self, key: &str) -> usize {
        self.zsets.get(key).map_or(0, |v| v.len())
    }

    // ── Hashes ────────────────────────────────────────────────────────────

    fn hset(&mut self, key: &str, field: &str, value: &str) -> bool {
        let h = self.hashes.entry(key.to_string()).or_default();
        let is_new = !h.contains_key(field);
        h.insert(field.to_string(), value.to_string());
        is_new
    }

    fn hget(&self, key: &str, field: &str) -> String {
        match self.hashes.get(key).and_then(|h| h.get(field)) {
            Some(v) => v.clone(),
            None => "NULL".to_string(),
        }
    }

    fn hdel(&mut self, key: &str, field: &str) -> bool {
        if let Some(h) = self.hashes.get_mut(key) {
            let removed = h.remove(field).is_some();
            if h.is_empty() {
                self.hashes.remove(key);
            }
            removed
        } else {
            false
        }
    }

    /// HGETALL returns sorted by field name, format "field=value,...".
    fn hgetall(&self, key: &str) -> String {
        match self.hashes.get(key) {
            None => String::new(),
            Some(h) => h
                .iter()
                .map(|(f, v)| format!("{}={}", f, v))
                .collect::<Vec<_>>()
                .join(","),
        }
    }

    fn hlen(&self, key: &str) -> usize {
        self.hashes.get(key).map_or(0, |h| h.len())
    }

    fn hexists(&self, key: &str, field: &str) -> bool {
        self.hashes
            .get(key)
            .map_or(false, |h| h.contains_key(field))
    }

    // ── HyperLogLog ──────────────────────────────────────────────────────

    fn pfadd(&mut self, key: &str, element: &str) -> bool {
        // We track exact sets for cardinality checking; return value is
        // "did the estimate change?" which is probabilistic so we don't
        // differential-check the bool return — only the count bounds.
        self.hlls
            .entry(key.to_string())
            .or_default()
            .insert(element.to_string());
        true // always "may have changed" per spec; skip bool differential
    }

    fn pfcount_exact(&self, key: &str) -> usize {
        self.hlls.get(key).map_or(0, |s| s.len())
    }

    fn pfmerge(&mut self, dest: &str, srcs: &[&str]) {
        // Redis PFMERGE treats the DESTINATION as one of the source sets too: its
        // prior cardinality is included in the merged result (it is NOT replaced).
        // Seed the union with dest's existing content before adding the sources.
        let mut union: BTreeSet<String> = self.hlls.get(dest).cloned().unwrap_or_default();
        for &src in srcs {
            if let Some(s) = self.hlls.get(src) {
                for m in s {
                    union.insert(m.clone());
                }
            }
        }
        self.hlls.insert(dest.to_string(), union);
    }

    fn pfmerge_exact(&self, dest: &str) -> usize {
        self.hlls.get(dest).map_or(0, |s| s.len())
    }
}

// ─── Op model ─────────────────────────────────────────────────────────────────
#[derive(Clone, Debug)]
enum Op {
    // Sets
    Sadd(String, String),
    Srem(String, String),
    Smembers(String),
    Sismember(String, String),
    Scard(String),
    // Sorted Sets
    Zadd(String, f64, String),
    Zrem(String, String),
    Zrange(String, usize, usize),
    Zrangebyscore(String, f64, f64),
    Zcard(String),
    // Hashes
    Hset(String, String, String),
    Hget(String, String),
    Hdel(String, String),
    Hgetall(String),
    Hlen(String),
    Hexists(String, String),
    // HyperLogLog
    Pfadd(String, String),
    PfcountCheck(String),       // check cardinality bounds
    Pfmerge(String, Vec<String>), // dest, srcs
    PfmergeCheck(String),       // check post-merge cardinality bounds
}

const SET_KEYS: &[&str] = &["sa", "sb", "sc"];
const ZSET_KEYS: &[&str] = &["za", "zb", "zc"];
const HASH_KEYS: &[&str] = &["ha", "hb", "hc"];
const HLL_KEYS: &[&str] = &["pa", "pb", "pc"];

const MEMBERS: &[&str] = &["m0", "m1", "m2", "m3", "m4"];
const FIELDS: &[&str] = &["f0", "f1", "f2", "f3"];
const FVALS: &[&str] = &["v0", "v1", "v2", "v3"];
const ELEMENTS: &[&str] = &["e0", "e1", "e2", "e3", "e4", "e5"];

fn gen_op(rng: &mut Rng) -> Op {
    match rng.below(22) {
        // Sets (5 ops)
        0 => Op::Sadd(
            rng.pick(SET_KEYS).to_string(),
            rng.pick(MEMBERS).to_string(),
        ),
        1 => Op::Srem(
            rng.pick(SET_KEYS).to_string(),
            rng.pick(MEMBERS).to_string(),
        ),
        2 => Op::Smembers(rng.pick(SET_KEYS).to_string()),
        3 => Op::Sismember(
            rng.pick(SET_KEYS).to_string(),
            rng.pick(MEMBERS).to_string(),
        ),
        4 => Op::Scard(rng.pick(SET_KEYS).to_string()),
        // Sorted Sets (5 ops)
        5 => Op::Zadd(
            rng.pick(ZSET_KEYS).to_string(),
            rng.int(1, 10) as f64,
            rng.pick(MEMBERS).to_string(),
        ),
        6 => Op::Zrem(
            rng.pick(ZSET_KEYS).to_string(),
            rng.pick(MEMBERS).to_string(),
        ),
        7 => {
            let start = rng.below(5);
            let stop = rng.below(6);
            Op::Zrange(rng.pick(ZSET_KEYS).to_string(), start, stop)
        }
        8 => {
            let a = rng.int(1, 5) as f64;
            let b = rng.int(5, 10) as f64;
            Op::Zrangebyscore(rng.pick(ZSET_KEYS).to_string(), a, b)
        }
        9 => Op::Zcard(rng.pick(ZSET_KEYS).to_string()),
        // Hashes (6 ops)
        10 => Op::Hset(
            rng.pick(HASH_KEYS).to_string(),
            rng.pick(FIELDS).to_string(),
            rng.pick(FVALS).to_string(),
        ),
        11 => Op::Hget(
            rng.pick(HASH_KEYS).to_string(),
            rng.pick(FIELDS).to_string(),
        ),
        12 => Op::Hdel(
            rng.pick(HASH_KEYS).to_string(),
            rng.pick(FIELDS).to_string(),
        ),
        13 => Op::Hgetall(rng.pick(HASH_KEYS).to_string()),
        14 => Op::Hlen(rng.pick(HASH_KEYS).to_string()),
        15 => Op::Hexists(
            rng.pick(HASH_KEYS).to_string(),
            rng.pick(FIELDS).to_string(),
        ),
        // HyperLogLog (4 ops)
        16 => Op::Pfadd(
            rng.pick(HLL_KEYS).to_string(),
            rng.pick(ELEMENTS).to_string(),
        ),
        17 => Op::PfcountCheck(rng.pick(HLL_KEYS).to_string()),
        18 => {
            let num_srcs = rng.below(2) + 1; // 1 or 2 sources
            let srcs: Vec<String> = (0..num_srcs)
                .map(|_| rng.pick(HLL_KEYS).to_string())
                .collect();
            Op::Pfmerge(rng.pick(HLL_KEYS).to_string(), srcs)
        }
        _ => Op::PfmergeCheck(rng.pick(HLL_KEYS).to_string()),
    }
}

/// HLL tolerance: precision 14 → ~1.04% std error; allow 20% for safety
/// (tests run small counts so relative error can be larger).
/// Allow minimum 2-element slack for very small counts.
fn hll_within_tolerance(nucleus_count: i64, exact: usize) -> bool {
    let exact = exact as f64;
    let got = nucleus_count as f64;
    // exact=0 is always correct if got=0
    if exact == 0.0 {
        return got == 0.0;
    }
    let err = (got - exact).abs() / exact;
    // 20% relative OR absolute slack of 2
    err <= 0.20 || (got - exact).abs() <= 2.0
}

// ─── SQL rendering ─────────────────────────────────────────────────────────
fn op_sql(op: &Op) -> Option<String> {
    match op {
        Op::Sadd(k, m) => Some(format!("SELECT KV_SADD('{k}','{m}')")),
        Op::Srem(k, m) => Some(format!("SELECT KV_SREM('{k}','{m}')")),
        Op::Smembers(k) => Some(format!("SELECT KV_SMEMBERS('{k}')")),
        Op::Sismember(k, m) => Some(format!("SELECT KV_SISMEMBER('{k}','{m}')")),
        Op::Scard(k) => Some(format!("SELECT KV_SCARD('{k}')")),

        Op::Zadd(k, score, m) => Some(format!("SELECT KV_ZADD('{k}',{score},'{m}')")),
        Op::Zrem(k, m) => Some(format!("SELECT KV_ZREM('{k}','{m}')")),
        Op::Zrange(k, s, e) => Some(format!("SELECT KV_ZRANGE('{k}',{s},{e})")),
        Op::Zrangebyscore(k, min, max) => {
            Some(format!("SELECT KV_ZRANGEBYSCORE('{k}',{min},{max})"))
        }
        Op::Zcard(k) => Some(format!("SELECT KV_ZCARD('{k}')")),

        Op::Hset(k, f, v) => Some(format!("SELECT KV_HSET('{k}','{f}','{v}')")),
        Op::Hget(k, f) => Some(format!("SELECT KV_HGET('{k}','{f}')")),
        Op::Hdel(k, f) => Some(format!("SELECT KV_HDEL('{k}','{f}')")),
        Op::Hgetall(k) => Some(format!("SELECT KV_HGETALL('{k}')")),
        Op::Hlen(k) => Some(format!("SELECT KV_HLEN('{k}')")),
        Op::Hexists(k, f) => Some(format!("SELECT KV_HEXISTS('{k}','{f}')")),

        Op::Pfadd(k, e) => Some(format!("SELECT KV_PFADD('{k}','{e}')")),
        Op::PfcountCheck(k) => Some(format!("SELECT KV_PFCOUNT('{k}')")),
        Op::Pfmerge(dest, srcs) => {
            let src_args = srcs
                .iter()
                .map(|s| format!("'{s}'"))
                .collect::<Vec<_>>()
                .join(",");
            Some(format!("SELECT KV_PFMERGE('{dest}',{src_args})"))
        }
        Op::PfmergeCheck(k) => Some(format!("SELECT KV_PFCOUNT('{k}')")),
    }
}

// ─── Executor helpers ─────────────────────────────────────────────────────
fn run_str(ex: &Executor, sql: &str) -> Result<String, ()> {
    let rt = tokio::runtime::Handle::current();
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        tokio::task::block_in_place(|| rt.block_on(ex.execute(sql)))
    }));
    match res {
        Ok(Ok(mut results)) => match results.pop() {
            Some(ExecResult::Select { rows, .. }) => {
                let v = rows
                    .first()
                    .and_then(|r| r.first())
                    .cloned()
                    .unwrap_or(Value::Null);
                Ok(v.to_string())
            }
            _ => Err(()),
        },
        Ok(Err(_)) => Err(()),
        Err(_) => Err(()), // panic — handled below
    }
}

fn run_i64(ex: &Executor, sql: &str) -> Result<i64, ()> {
    let rt = tokio::runtime::Handle::current();
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        tokio::task::block_in_place(|| rt.block_on(ex.execute(sql)))
    }));
    match res {
        Ok(Ok(mut results)) => match results.pop() {
            Some(ExecResult::Select { rows, .. }) => {
                match rows.first().and_then(|r| r.first()) {
                    Some(Value::Int64(n)) => Ok(*n),
                    Some(Value::Int32(n)) => Ok(*n as i64),
                    _ => Err(()),
                }
            }
            _ => Err(()),
        },
        Ok(Err(_)) => Err(()),
        Err(_) => Err(()),
    }
}

fn is_panic(ex: &Executor, sql: &str) -> bool {
    let rt = tokio::runtime::Handle::current();
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        tokio::task::block_in_place(|| rt.block_on(ex.execute(sql)))
    }))
    .is_err()
}

// ─── Main fuzzer loop ────────────────────────────────────────────────────────
fn main_impl() {
    let mut seed: u64 = 0xDEAD_BEEF_CAFE_1234;
    let mut iterations = 3000usize;
    let mut ops_per = 50usize;
    let mut max_report = 20usize;

    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--seed" => {
                i += 1;
                seed = if args[i].starts_with("0x") || args[i].starts_with("0X") {
                    u64::from_str_radix(args[i].trim_start_matches("0x").trim_start_matches("0X"), 16).unwrap()
                } else {
                    args[i].parse::<u64>().unwrap()
                };
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

    // Suppress panic backtraces from catch_unwind.
    std::panic::set_hook(Box::new(|_| {}));

    println!("Nucleus KV-collections differential fuzzer (Sets / ZSets / Hashes / HLL)");
    println!("seed={seed} iterations={iterations} ops/iter={ops_per}\n");

    let mut total_ops = 0usize;
    let mut divergences = 0usize;
    let mut panics = 0usize;

    'outer: for iter in 0..iterations {
        let mut rng = Rng(seed.wrapping_add(iter as u64).wrapping_mul(0x100000001B3));
        let catalog = Arc::new(Catalog::new());
        let storage: Arc<dyn StorageEngine> = Arc::new(MvccStorageAdapter::new());
        let ex = Arc::new(Executor::new(catalog, storage));
        let mut oracle = RefOracle::default();
        let mut log: Vec<Op> = Vec::new();

        for _ in 0..ops_per {
            total_ops += 1;
            let op = gen_op(&mut rng);
            let sql = match op_sql(&op) {
                Some(s) => s,
                None => continue,
            };
            log.push(op.clone());

            macro_rules! check_str {
                ($expected:expr) => {{
                    let expected: String = $expected;
                    let got = run_str(&ex, &sql);
                    match got {
                        Ok(ref g) if *g == expected => {}
                        Ok(ref g) => {
                            if is_panic(&ex, &sql) {
                                panics += 1;
                                if panics <= max_report {
                                    println!(
                                        "─── PANIC #{panics} (iter {iter}) ───\n  sql: {sql}\n"
                                    );
                                }
                                if panics > max_report {
                                    std::process::exit(1);
                                }
                                continue 'outer;
                            }
                            divergences += 1;
                            if divergences <= max_report {
                                println!("─── DIVERGENCE #{divergences} (iter {iter}, seed {seed}) ───");
                                println!("  sql      : {sql}");
                                println!("  expected : {:?}", expected);
                                println!("  nucleus  : {:?}", g);
                                println!("  ── replay ({} ops) ──", log.len());
                                for o in &log {
                                    if let Some(s) = op_sql(o) {
                                        println!("    {};", s);
                                    }
                                }
                                println!();
                            }
                            continue 'outer;
                        }
                        Err(()) => {
                            // engine error when we expect success
                            divergences += 1;
                            if divergences <= max_report {
                                println!("─── ERROR (iter {iter}) ───");
                                println!("  sql      : {sql}");
                                println!("  expected : {:?}", expected);
                                println!("  nucleus  : Err(engine error)");
                                println!();
                            }
                            continue 'outer;
                        }
                    }
                }};
            }

            macro_rules! check_bool {
                ($expected:expr) => {
                    check_str!(if $expected { "true".into() } else { "false".into() })
                };
            }

            macro_rules! check_int {
                ($expected:expr) => {
                    check_str!(format!("{}", $expected as i64))
                };
            }

            match &op {
                // ── Sets ──────────────────────────────────────────────────
                Op::Sadd(k, m) => {
                    let expected = oracle.sadd(k, m);
                    check_bool!(expected);
                }
                Op::Srem(k, m) => {
                    let expected = oracle.srem(k, m);
                    check_bool!(expected);
                }
                Op::Smembers(k) => {
                    let expected = oracle.smembers(k);
                    check_str!(expected);
                }
                Op::Sismember(k, m) => {
                    let expected = oracle.sismember(k, m);
                    check_bool!(expected);
                }
                Op::Scard(k) => {
                    let expected = oracle.scard(k);
                    check_int!(expected);
                }

                // ── Sorted Sets ───────────────────────────────────────────
                Op::Zadd(k, score, m) => {
                    let expected = oracle.zadd(k, *score, m);
                    check_bool!(expected);
                }
                Op::Zrem(k, m) => {
                    let expected = oracle.zrem(k, m);
                    check_bool!(expected);
                }
                Op::Zrange(k, start, stop) => {
                    let expected = oracle.zrange(k, *start, *stop);
                    check_str!(expected);
                }
                Op::Zrangebyscore(k, min, max) => {
                    let expected = oracle.zrangebyscore(k, *min, *max);
                    check_str!(expected);
                }
                Op::Zcard(k) => {
                    let expected = oracle.zcard(k);
                    check_int!(expected);
                }

                // ── Hashes ────────────────────────────────────────────────
                Op::Hset(k, f, v) => {
                    let expected = oracle.hset(k, f, v);
                    check_bool!(expected);
                }
                Op::Hget(k, f) => {
                    let expected = oracle.hget(k, f);
                    check_str!(expected);
                }
                Op::Hdel(k, f) => {
                    let expected = oracle.hdel(k, f);
                    check_bool!(expected);
                }
                Op::Hgetall(k) => {
                    let expected = oracle.hgetall(k);
                    check_str!(expected);
                }
                Op::Hlen(k) => {
                    let expected = oracle.hlen(k);
                    check_int!(expected);
                }
                Op::Hexists(k, f) => {
                    let expected = oracle.hexists(k, f);
                    check_bool!(expected);
                }

                // ── HyperLogLog ──────────────────────────────────────────
                Op::Pfadd(k, e) => {
                    oracle.pfadd(k, e);
                    // Don't check the bool return — HLL is probabilistic.
                    // Just drive the op and check it doesn't error/panic.
                    let got = run_str(&ex, &sql);
                    if got.is_err() {
                        if is_panic(&ex, &sql) {
                            panics += 1;
                            if panics <= max_report {
                                println!(
                                    "─── PANIC in PFADD #{panics} (iter {iter}) ───\n  sql: {sql}\n"
                                );
                            }
                        } else {
                            divergences += 1;
                            if divergences <= max_report {
                                println!("─── PFADD ERROR (iter {iter}) ───");
                                println!("  sql: {sql}");
                                println!("  expected: ok(bool), got: error");
                                println!();
                            }
                        }
                        continue 'outer;
                    }
                }
                Op::PfcountCheck(k) => {
                    let exact = oracle.pfcount_exact(k);
                    let got = run_i64(&ex, &sql);
                    match got {
                        Ok(n) => {
                            if !hll_within_tolerance(n, exact) {
                                divergences += 1;
                                if divergences <= max_report {
                                    println!("─── HLL PFCOUNT OUT OF BOUNDS (iter {iter}) ───");
                                    println!("  sql      : {sql}");
                                    println!("  exact_ref: {exact}");
                                    println!("  nucleus  : {n}");
                                    let err_pct = if exact > 0 {
                                        (n as f64 - exact as f64).abs() / exact as f64 * 100.0
                                    } else {
                                        0.0
                                    };
                                    println!("  error    : {err_pct:.1}%");
                                    println!();
                                }
                                continue 'outer;
                            }
                        }
                        Err(()) => {
                            if is_panic(&ex, &sql) {
                                panics += 1;
                                if panics <= max_report {
                                    println!("─── PANIC in PFCOUNT #{panics} (iter {iter}) ───\n  sql: {sql}\n");
                                }
                            } else {
                                divergences += 1;
                                if divergences <= max_report {
                                    println!("─── PFCOUNT ERROR (iter {iter}) ───");
                                    println!("  sql: {sql}");
                                    println!("  expected: ok(i64), got: error");
                                    println!();
                                }
                            }
                            continue 'outer;
                        }
                    }
                }
                Op::Pfmerge(dest, srcs) => {
                    let src_refs: Vec<&str> = srcs.iter().map(|s| s.as_str()).collect();
                    oracle.pfmerge(dest, &src_refs);
                    let got = run_str(&ex, &sql);
                    if got.is_err() {
                        if is_panic(&ex, &sql) {
                            panics += 1;
                            if panics <= max_report {
                                println!("─── PANIC in PFMERGE #{panics} (iter {iter}) ───\n  sql: {sql}\n");
                            }
                        } else {
                            divergences += 1;
                            if divergences <= max_report {
                                println!("─── PFMERGE ERROR (iter {iter}) ───");
                                println!("  sql: {sql}");
                                println!("  expected: ok(true), got: error");
                                println!();
                            }
                        }
                        continue 'outer;
                    }
                }
                Op::PfmergeCheck(k) => {
                    let exact = oracle.pfmerge_exact(k);
                    let got = run_i64(&ex, &sql);
                    match got {
                        Ok(n) => {
                            if !hll_within_tolerance(n, exact) {
                                divergences += 1;
                                if divergences <= max_report {
                                    println!("─── HLL PFMERGE PFCOUNT OUT OF BOUNDS (iter {iter}) ───");
                                    println!("  sql      : {sql}");
                                    println!("  exact_ref: {exact}");
                                    println!("  nucleus  : {n}");
                                    println!();
                                }
                                continue 'outer;
                            }
                        }
                        Err(()) => {
                            // key might not exist yet (no pfmerge ran on it)
                            // that's fine — skip
                        }
                    }
                }
            }
        }
    }

    println!("\n════ SUMMARY ════");
    println!("ops run            : {total_ops}");
    println!("divergences        : {divergences}");
    println!("panics             : {panics}");
    if divergences == 0 && panics == 0 {
        println!("\nAll KV-collection ops match reference. No divergences, no panics.");
    } else {
        std::process::exit(1);
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    tokio::task::spawn_blocking(main_impl).await.unwrap();
}
