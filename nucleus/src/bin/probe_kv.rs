//! KV-model differential fuzzer: drives Nucleus's KV functions (strings + lists)
//! through the SQL surface and checks every return value against a small,
//! known-correct reference (Redis semantics). Disjoint key namespaces per type
//! keep this focused on each type's logic (WRONGTYPE behavior is out of scope
//! here). Build: `cargo run --release --features "server" --bin probe_kv`.
#![cfg(feature = "server")]

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::storage::{MvccStorageAdapter, StorageEngine};
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
    fn int(&mut self, lo: i64, hi: i64) -> i64 {
        lo + (self.next() % ((hi - lo + 1) as u64)) as i64
    }
    fn pick<'a, T>(&mut self, xs: &'a [T]) -> &'a T {
        &xs[self.below(xs.len())]
    }
}

// ─── Reference KV model (Redis semantics) ─────────────────────────────────────
#[derive(Default)]
struct Ref {
    strings: HashMap<String, String>,
    lists: HashMap<String, VecDeque<String>>,
}

/// Redis index normalization for LINDEX (single index → Option<usize>).
fn norm_index(i: i64, len: usize) -> Option<usize> {
    let len = len as i64;
    let idx = if i < 0 { len + i } else { i };
    if idx < 0 || idx >= len {
        None
    } else {
        Some(idx as usize)
    }
}

/// Redis LRANGE normalization → inclusive [start,stop] clamped to bounds.
fn norm_range(start: i64, stop: i64, len: usize) -> (usize, usize, bool) {
    let len = len as i64;
    let mut s = if start < 0 { len + start } else { start };
    let mut e = if stop < 0 { len + stop } else { stop };
    if s < 0 {
        s = 0;
    }
    if e >= len {
        e = len - 1;
    }
    if s > e || s >= len || len == 0 {
        (0, 0, true) // empty
    } else {
        (s as usize, e as usize, false)
    }
}

impl Ref {
    /// Compute the expected result of an op. `Ok(s)` = a value whose Display is
    /// `s`; `Err(())` = the op should error (type/parse). NULL is the string
    /// "NULL" (matching Value::Null's Display).
    fn apply(&mut self, op: &Op) -> Result<String, ()> {
        match op {
            Op::Set(k, v) => {
                self.strings.insert(k.clone(), v.clone());
                Ok("OK".into())
            }
            Op::SetNx(k, v) => {
                if self.strings.contains_key(k) {
                    Ok("false".into())
                } else {
                    self.strings.insert(k.clone(), v.clone());
                    Ok("true".into())
                }
            }
            Op::Get(k) => Ok(self
                .strings
                .get(k)
                .cloned()
                .unwrap_or_else(|| "NULL".into())),
            Op::Del(k) => Ok(if self.strings.remove(k).is_some() {
                "true"
            } else {
                "false"
            }
            .into()),
            Op::Exists(k) => Ok(if self.strings.contains_key(k) {
                "true"
            } else {
                "false"
            }
            .into()),
            Op::Incr(k, amt) => {
                let cur = match self.strings.get(k) {
                    None => 0i64,
                    Some(s) => match s.parse::<i64>() {
                        Ok(n) => n,
                        Err(_) => return Err(()),
                    },
                };
                let n = cur + amt;
                self.strings.insert(k.clone(), n.to_string());
                Ok(n.to_string())
            }
            Op::LPush(k, v) => {
                let l = self.lists.entry(k.clone()).or_default();
                l.push_front(v.clone());
                Ok(l.len().to_string())
            }
            Op::RPush(k, v) => {
                let l = self.lists.entry(k.clone()).or_default();
                l.push_back(v.clone());
                Ok(l.len().to_string())
            }
            Op::LPop(k) => Ok(self
                .lists
                .get_mut(k)
                .and_then(|l| l.pop_front())
                .unwrap_or_else(|| "NULL".into())),
            Op::RPop(k) => Ok(self
                .lists
                .get_mut(k)
                .and_then(|l| l.pop_back())
                .unwrap_or_else(|| "NULL".into())),
            Op::LLen(k) => Ok(self.lists.get(k).map_or(0, |l| l.len()).to_string()),
            Op::LIndex(k, i) => {
                let l = match self.lists.get(k) {
                    Some(l) => l,
                    None => return Ok("NULL".into()),
                };
                Ok(match norm_index(*i, l.len()) {
                    Some(idx) => l[idx].clone(),
                    None => "NULL".into(),
                })
            }
            Op::LRange(k, s, e) => {
                let l = match self.lists.get(k) {
                    Some(l) => l,
                    None => return Ok(String::new()),
                };
                let (lo, hi, empty) = norm_range(*s, *e, l.len());
                if empty {
                    Ok(String::new())
                } else {
                    Ok(l.iter()
                        .skip(lo)
                        .take(hi - lo + 1)
                        .cloned()
                        .collect::<Vec<_>>()
                        .join(","))
                }
            }
        }
    }
}

// ─── Op model ─────────────────────────────────────────────────────────────────
#[derive(Clone)]
enum Op {
    Set(String, String),
    SetNx(String, String),
    Get(String),
    Del(String),
    Exists(String),
    Incr(String, i64),
    LPush(String, String),
    RPush(String, String),
    LPop(String),
    RPop(String),
    LLen(String),
    LIndex(String, i64),
    LRange(String, i64, i64),
}

impl Op {
    /// SQL rendering for the executor.
    fn sql(&self) -> String {
        match self {
            Op::Set(k, v) => format!("SELECT KV_SET('{k}','{v}')"),
            Op::SetNx(k, v) => format!("SELECT KV_SETNX('{k}','{v}')"),
            Op::Get(k) => format!("SELECT KV_GET('{k}')"),
            Op::Del(k) => format!("SELECT KV_DEL('{k}')"),
            Op::Exists(k) => format!("SELECT KV_EXISTS('{k}')"),
            Op::Incr(k, a) => format!("SELECT KV_INCR('{k}',{a})"),
            Op::LPush(k, v) => format!("SELECT KV_LPUSH('{k}','{v}')"),
            Op::RPush(k, v) => format!("SELECT KV_RPUSH('{k}','{v}')"),
            Op::LPop(k) => format!("SELECT KV_LPOP('{k}')"),
            Op::RPop(k) => format!("SELECT KV_RPOP('{k}')"),
            Op::LLen(k) => format!("SELECT KV_LLEN('{k}')"),
            Op::LIndex(k, i) => format!("SELECT KV_LINDEX('{k}',{i})"),
            Op::LRange(k, s, e) => format!("SELECT KV_LRANGE('{k}',{s},{e})"),
        }
    }
}

const SKEYS: &[&str] = &["s0", "s1", "s2"];
const LKEYS: &[&str] = &["l0", "l1", "l2"];
// Mix of integer-parseable and non-numeric values so INCR errors sometimes.
const SVALS: &[&str] = &["0", "5", "-3", "12", "a", "xx"];
const LVALS: &[&str] = &["a", "b", "c", "1", "2"];

fn gen_op(rng: &mut Rng) -> Op {
    match rng.below(13) {
        0 => Op::Set(rng.pick(SKEYS).to_string(), rng.pick(SVALS).to_string()),
        1 => Op::SetNx(rng.pick(SKEYS).to_string(), rng.pick(SVALS).to_string()),
        2 => Op::Get(rng.pick(SKEYS).to_string()),
        3 => Op::Del(rng.pick(SKEYS).to_string()),
        4 => Op::Exists(rng.pick(SKEYS).to_string()),
        5 => Op::Incr(rng.pick(SKEYS).to_string(), rng.int(-3, 5)),
        6 => Op::LPush(rng.pick(LKEYS).to_string(), rng.pick(LVALS).to_string()),
        7 => Op::RPush(rng.pick(LKEYS).to_string(), rng.pick(LVALS).to_string()),
        8 => Op::LPop(rng.pick(LKEYS).to_string()),
        9 => Op::RPop(rng.pick(LKEYS).to_string()),
        10 => Op::LLen(rng.pick(LKEYS).to_string()),
        11 => Op::LIndex(rng.pick(LKEYS).to_string(), rng.int(-4, 4)),
        _ => Op::LRange(rng.pick(LKEYS).to_string(), rng.int(-4, 4), rng.int(-4, 4)),
    }
}

// ─── Executor runner ──────────────────────────────────────────────────────────
fn run(ex: &Executor, sql: &str) -> Result<String, ()> {
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
        Err(_) => Err(()), // PANIC: surfaced separately below
    }
}

fn is_panic(ex: &Executor, sql: &str) -> bool {
    let rt = tokio::runtime::Handle::current();
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        tokio::task::block_in_place(|| rt.block_on(ex.execute(sql)))
    }))
    .is_err()
}

fn main_impl() {
    let mut seed: u64 = 0x9E37_79B9;
    let mut iterations = 4000usize;
    let mut ops_per = 40usize;
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

    println!("Nucleus KV differential fuzzer (strings + lists vs reference)");
    println!("seed={seed} iterations={iterations} ops/iter={ops_per}\n");

    let mut total = 0usize;
    let mut divergences = 0usize;
    let mut panics = 0usize;

    'outer: for iter in 0..iterations {
        let mut rng = Rng(seed.wrapping_add(iter as u64).wrapping_mul(0x100000001B3));
        let catalog = Arc::new(Catalog::new());
        let storage: Arc<dyn StorageEngine> = Arc::new(MvccStorageAdapter::new());
        let ex = Arc::new(Executor::new(catalog, storage));
        let mut reference = Ref::default();
        let mut log: Vec<Op> = Vec::new();

        for _ in 0..ops_per {
            total += 1;
            let op = gen_op(&mut rng);
            let sql = op.sql();
            let expected = reference.apply(&op);
            let got = run(&ex, &sql);
            log.push(op.clone());

            match (&expected, &got) {
                (Ok(a), Ok(b)) if a == b => {}
                (Err(_), Err(_)) => {} // both reject — agree
                _ => {
                    // Distinguish a panic from a plain divergence.
                    if is_panic(&ex, &sql) {
                        panics += 1;
                        if panics <= max_report {
                            println!("─── PANIC #{panics} (iter {iter}) ───\n  op: {sql}\n");
                        }
                        if panics > max_report {
                            std::process::exit(1);
                        }
                        continue 'outer;
                    }
                    divergences += 1;
                    if divergences <= max_report {
                        println!("─── KV DIVERGENCE #{divergences} (iter {iter}, seed {seed}) ───");
                        println!("  op       : {sql}");
                        println!("  expected : {expected:?}");
                        println!("  nucleus  : {got:?}");
                        println!("  ── replay ({} ops) ──", log.len());
                        for o in &log {
                            println!("    {};", o.sql());
                        }
                        println!();
                    }
                    continue 'outer;
                }
            }
        }
    }

    println!("\n════ SUMMARY ════");
    println!("ops run            : {total}");
    println!("KV divergences     : {divergences}");
    println!("PANICS             : {panics}");
    if divergences == 0 && panics == 0 {
        println!("\nNo KV divergences, no panics vs reference. 🎯");
    } else {
        std::process::exit(1);
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    tokio::task::spawn_blocking(main_impl).await.unwrap();
}
