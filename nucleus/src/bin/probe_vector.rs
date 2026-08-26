//! Vector-model differential fuzzer: checks every distance metric and KNN
//! ordering against a brute-force f32 reference. Small integer components keep
//! the intermediate sums exact regardless of SIMD summation order; a tolerance
//! absorbs the final sqrt/division rounding. Build:
//! `cargo run --release --features server --bin probe_vector`.
#![cfg(feature = "server")]

use std::sync::Arc;

use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::storage::{MvccStorageAdapter, StorageEngine};
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
    fn comp(&mut self) -> i32 {
        (self.next() % 11) as i32 - 5
    } // -5..5
}

// ── Brute-force reference (f32, matches the engine's element type) ──
fn l2(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b)
        .map(|(x, y)| (x - y) * (x - y))
        .sum::<f32>()
        .sqrt()
}
fn dot(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| x * y).sum()
}
fn cosine(a: &[f32], b: &[f32]) -> f32 {
    let na = dot(a, a).sqrt();
    let nb = dot(b, b).sqrt();
    1.0 - dot(a, b) / (na * nb)
}

/// A random non-zero vector (non-zero so cosine norms never divide by zero).
fn gen_vec(rng: &mut Rng, dim: usize) -> Vec<f32> {
    loop {
        let v: Vec<f32> = (0..dim).map(|_| rng.comp() as f32).collect();
        if v.iter().any(|&x| x != 0.0) {
            return v;
        }
    }
}

fn lit(v: &[f32]) -> String {
    let body: Vec<String> = v.iter().map(|x| format!("{}", *x as i64)).collect();
    format!("VECTOR('[{}]')", body.join(","))
}
fn json_lit(v: &[f32]) -> String {
    let body: Vec<String> = v.iter().map(|x| format!("{}", *x as i64)).collect();
    format!("'[{}]'", body.join(","))
}

fn run_f64(ex: &Executor, sql: &str) -> Result<f64, ()> {
    let rt = tokio::runtime::Handle::current();
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        tokio::task::block_in_place(|| rt.block_on(ex.execute(sql)))
    }));
    match res {
        Ok(Ok(mut r)) => match r.pop() {
            Some(ExecResult::Select { rows, .. }) => match rows.first().and_then(|r| r.first()) {
                Some(Value::Float64(f)) => Ok(*f),
                Some(Value::Int32(n)) => Ok(*n as f64),
                Some(Value::Int64(n)) => Ok(*n as f64),
                _ => Err(()),
            },
            _ => Err(()),
        },
        _ => Err(()),
    }
}

fn run_ids(ex: &Executor, sql: &str) -> Result<Vec<i64>, ()> {
    let rt = tokio::runtime::Handle::current();
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        tokio::task::block_in_place(|| rt.block_on(ex.execute(sql)))
    }));
    match res {
        Ok(Ok(mut r)) => match r.pop() {
            Some(ExecResult::Select { rows, .. }) => Ok(rows
                .iter()
                .filter_map(|row| match row.first() {
                    Some(Value::Int32(n)) => Some(*n as i64),
                    Some(Value::Int64(n)) => Some(*n),
                    _ => None,
                })
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

fn dist_metric(a: &[f32], b: &[f32], metric: &str) -> f32 {
    match metric {
        "cosine" => cosine(a, b),
        "inner" => -dot(a, b),
        _ => l2(a, b),
    }
}

/// Tolerant float compare (absolute + relative).
fn close(a: f64, b: f64) -> bool {
    (a - b).abs() <= 1e-3 + 1e-3 * a.abs().max(b.abs())
}

fn main_impl() {
    let mut seed: u64 = 0x5151_2727;
    let mut iterations = 60_000usize;
    let mut max_report = 20usize;
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

    println!("Nucleus vector differential fuzzer (metrics + KNN vs brute force)");
    println!("seed={seed} iterations={iterations}\n");

    let catalog = Arc::new(Catalog::new());
    let storage: Arc<dyn StorageEngine> = Arc::new(MvccStorageAdapter::new());
    let ex = Arc::new(Executor::new(catalog, storage));

    let mut total = 0usize;
    let mut divergences = 0usize;
    let report = |d: &mut usize, what: &str, sql: &str, exp: f64, got: Result<f64, ()>| {
        *d += 1;
        if *d <= max_report {
            println!("─── VECTOR DIVERGENCE #{d} ({what}) ───");
            println!("  sql      : {sql}");
            println!("  expected : {exp}");
            println!("  nucleus  : {got:?}\n");
        }
    };

    for iter in 0..iterations {
        let mut rng = Rng(seed.wrapping_add(iter as u64).wrapping_mul(0x100000001B3));
        let dim = 1 + rng.below(6);
        let a = gen_vec(&mut rng, dim);
        let b = gen_vec(&mut rng, dim);
        let (va, vb) = (lit(&a), lit(&b));

        // Each (description, sql, expected) tuple.
        let cases: Vec<(&str, String, f64)> = vec![
            (
                "default(l2)",
                format!("SELECT VECTOR_DISTANCE({va},{vb})"),
                l2(&a, &b) as f64,
            ),
            (
                "l2",
                format!("SELECT VECTOR_DISTANCE({va},{vb},'l2')"),
                l2(&a, &b) as f64,
            ),
            (
                "cosine",
                format!("SELECT VECTOR_DISTANCE({va},{vb},'cosine')"),
                cosine(&a, &b) as f64,
            ),
            (
                "inner(neg)",
                format!("SELECT VECTOR_DISTANCE({va},{vb},'inner')"),
                -dot(&a, &b) as f64,
            ),
            (
                "l2_fn",
                format!(
                    "SELECT VECTOR_L2_DISTANCE({},{})",
                    json_lit(&a),
                    json_lit(&b)
                ),
                l2(&a, &b) as f64,
            ),
            (
                "cosine_fn",
                format!(
                    "SELECT VECTOR_COSINE_DISTANCE({},{})",
                    json_lit(&a),
                    json_lit(&b)
                ),
                cosine(&a, &b) as f64,
            ),
            (
                "inner_fn(pos)",
                format!(
                    "SELECT VECTOR_INNER_PRODUCT({},{})",
                    json_lit(&a),
                    json_lit(&b)
                ),
                dot(&a, &b) as f64,
            ),
            ("dims", format!("SELECT VECTOR_DIMS({va})"), dim as f64),
        ];

        for (what, sql, exp) in cases {
            total += 1;
            let got = run_f64(&ex, &sql);
            match got {
                Ok(g) if close(g, exp) => {}
                _ => report(&mut divergences, what, &sql, exp, got),
            }
        }
    }

    // ── KNN ordering: top-k by distance vs brute-force, per fresh table ──
    let mut knn_total = 0usize;
    let mut knn_div = 0usize;
    let knn_iters = (iterations / 20).max(1);
    for iter in 0..knn_iters {
        let mut rng = Rng((seed ^ 0x4B4E_4E00)
            .wrapping_add(iter as u64)
            .wrapping_mul(0x100000001B3));
        let dim = 1 + rng.below(6);
        let n = 6 + rng.below(10);
        let vecs: Vec<Vec<f32>> = (0..n).map(|_| gen_vec(&mut rng, dim)).collect();
        let q = gen_vec(&mut rng, dim);
        let metric = *["l2", "cosine", "inner"].get(rng.below(3)).unwrap();
        let k = 1 + rng.below(n);

        let cat = Arc::new(Catalog::new());
        let st: Arc<dyn StorageEngine> = Arc::new(MvccStorageAdapter::new());
        let kex = Arc::new(Executor::new(cat, st));
        if !exec(
            &kex,
            &format!("CREATE TABLE t (id INTEGER PRIMARY KEY, v VECTOR({dim}))"),
        ) {
            continue;
        }
        let mut ok = true;
        for (i, v) in vecs.iter().enumerate() {
            if !exec(
                &kex,
                &format!("INSERT INTO t VALUES ({}, {})", i + 1, lit(v)),
            ) {
                ok = false;
                break;
            }
        }
        if !ok {
            continue;
        }
        knn_total += 1;

        let sql = format!(
            "SELECT id FROM t ORDER BY VECTOR_DISTANCE(v, {}, '{metric}') ASC LIMIT {k}",
            lit(&q)
        );
        let got = match run_ids(&kex, &sql) {
            Ok(ids) => ids,
            Err(_) => {
                knn_div += 1;
                if knn_div <= max_report {
                    println!("─── KNN DIVERGENCE #{knn_div} (errored) ───\n  {sql}\n");
                }
                continue;
            }
        };
        // Reference: distance per id (1-based), the k smallest sorted ascending.
        let mut ref_d: Vec<f32> = vecs.iter().map(|v| dist_metric(v, &q, metric)).collect();
        ref_d.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let expected: Vec<f32> = ref_d.into_iter().take(k).collect();
        // Distances of the rows nucleus returned, sorted ascending.
        let mut got_d: Vec<f32> = got
            .iter()
            .filter_map(|&id| {
                vecs.get((id - 1) as usize)
                    .map(|v| dist_metric(v, &q, metric))
            })
            .collect();
        got_d.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let matches = got.len() == k
            && got_d.len() == expected.len()
            && got_d
                .iter()
                .zip(&expected)
                .all(|(g, e)| close(*g as f64, *e as f64));
        if !matches {
            knn_div += 1;
            if knn_div <= max_report {
                println!("─── KNN DIVERGENCE #{knn_div} (iter {iter}, metric {metric}, k={k}) ───");
                println!("  query    : {}", lit(&q));
                println!("  vectors  : {vecs:?}");
                println!("  nucleus  : ids {got:?} -> dists {got_d:?}");
                println!("  expected : k-smallest dists {expected:?}\n");
            }
        }
    }

    println!("\n════ SUMMARY ════");
    println!("scalar checks      : {total}");
    println!("scalar divergences : {divergences}");
    println!("knn queries        : {knn_total}");
    println!("knn divergences    : {knn_div}");
    divergences += knn_div;
    if divergences == 0 {
        println!("\nNo vector divergences vs brute force.");
    } else {
        std::process::exit(1);
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    tokio::task::spawn_blocking(main_impl).await.unwrap();
}
