//! Vector recall-regression harness.
//!
//! The coherence oracle (probe_index_coherence) proves the vector index never
//! returns stale/duplicate/wrong rows, but it deliberately does NOT check
//! recall (how many of the true k nearest the index actually finds). An
//! incremental-maintenance refactor could silently degrade recall without
//! tripping any coherence check, so this harness closes that gap: it compares
//! the indexed KNN path against a brute-force top-k reference and asserts
//! average recall@k stays above a per-index-type floor — both on a freshly
//! built index AND after sustained churn (deletes + updates), which is where a
//! bad incremental posting update would show up.
//!
//! `cargo run --release --features server --bin probe_vector_recall`
#![cfg(feature = "server")]
#![allow(clippy::unusual_byte_groupings)]

use std::collections::BTreeMap;
use std::sync::Arc;

use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::storage::{MvccStorageAdapter, StorageEngine};
use nucleus::types::Value;

const DIM: usize = 16;

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: usize) -> usize {
        if n == 0 {
            0
        } else {
            (self.next() % n as u64) as usize
        }
    }
    fn unit(&mut self) -> f32 {
        (self.below(20000) as f32) / 10000.0 - 1.0
    }
}

fn rand_vec(rng: &mut Rng) -> Vec<f32> {
    (0..DIM).map(|_| rng.unit()).collect()
}

fn vec_lit(v: &[f32]) -> String {
    let body: Vec<String> = v.iter().map(|x| format!("{x}")).collect();
    format!("VECTOR('[{}]')", body.join(","))
}

fn l2_sq(a: &[f32], b: &[f32]) -> f64 {
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| {
            let d = (*x as f64) - (*y as f64);
            d * d
        })
        .sum()
}

fn exec(ex: &Executor, sql: &str) -> bool {
    let rt = tokio::runtime::Handle::current();
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        tokio::task::block_in_place(|| rt.block_on(ex.execute(sql)))
    }))
    .map(|r| r.is_ok())
    .unwrap_or(false)
}

fn query_ids(ex: &Executor, sql: &str) -> Vec<i64> {
    let rt = tokio::runtime::Handle::current();
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        tokio::task::block_in_place(|| rt.block_on(ex.execute(sql)))
    }));
    match res {
        Ok(Ok(mut r)) => match r.pop() {
            Some(ExecResult::Select { rows, .. }) => rows
                .iter()
                .filter_map(|row| match row.first() {
                    Some(Value::Int32(n)) => Some(*n as i64),
                    Some(Value::Int64(n)) => Some(*n),
                    _ => None,
                })
                .collect(),
            _ => Vec::new(),
        },
        _ => Vec::new(),
    }
}

/// True top-k ids for `q` by brute force over the reference model.
fn brute_topk(model: &BTreeMap<i64, Vec<f32>>, q: &[f32], k: usize) -> Vec<i64> {
    let mut scored: Vec<(f64, i64)> = model.iter().map(|(id, v)| (l2_sq(q, v), *id)).collect();
    scored.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
    scored.into_iter().take(k).map(|(_, id)| id).collect()
}

/// Average recall@k of the indexed path vs brute force over `queries` probes.
fn measure_recall(
    ex: &Executor,
    model: &BTreeMap<i64, Vec<f32>>,
    metric: &str,
    k: usize,
    queries: usize,
    rng: &mut Rng,
) -> f64 {
    if model.len() < k {
        return 1.0;
    }
    let mut sum = 0.0;
    for _ in 0..queries {
        let q = rand_vec(rng);
        let truth: std::collections::HashSet<i64> = brute_topk(model, &q, k).into_iter().collect();
        let sql = format!(
            "SELECT id FROM vr ORDER BY VECTOR_DISTANCE(v, {}, '{metric}') ASC LIMIT {k}",
            vec_lit(&q)
        );
        let got = query_ids(ex, &sql);
        let hit = got.iter().filter(|id| truth.contains(id)).count();
        sum += hit as f64 / k as f64;
    }
    sum / queries as f64
}

struct Case {
    kind: &'static str,
    metric: &'static str,
    fresh_floor: f64,
    churn_floor: f64,
}

fn run_case(case: &Case, n: usize, k: usize, queries: usize, seed: u64) -> Option<String> {
    let mut rng = Rng(seed | 1);
    let catalog = Arc::new(Catalog::new());
    let storage: Arc<dyn StorageEngine> = Arc::new(MvccStorageAdapter::new());
    let ex = Arc::new(Executor::new(catalog, storage));

    if !exec(&ex, "CREATE TABLE vr (id INT PRIMARY KEY, v VECTOR(16))") {
        return Some(format!("{}: CREATE TABLE failed", case.kind));
    }
    let mut model: BTreeMap<i64, Vec<f32>> = BTreeMap::new();
    let mut next_id = 1i64;
    for _ in 0..n {
        let id = next_id;
        next_id += 1;
        let v = rand_vec(&mut rng);
        if exec(
            &ex,
            &format!("INSERT INTO vr VALUES ({id}, {})", vec_lit(&v)),
        ) {
            model.insert(id, v);
        }
    }
    if !exec(
        &ex,
        &format!("CREATE INDEX vr_v ON vr USING {} (v)", case.kind),
    ) {
        return Some(format!("{}: CREATE INDEX failed", case.kind));
    }

    let fresh = measure_recall(&ex, &model, case.metric, k, queries, &mut rng);
    println!(
        "  {:>8} {:>6}  fresh recall@{k} = {:.3}  (floor {:.2})",
        case.kind, case.metric, fresh, case.fresh_floor
    );
    if fresh < case.fresh_floor {
        return Some(format!(
            "{} {}: fresh recall {:.3} below floor {:.2}",
            case.kind, case.metric, fresh, case.fresh_floor
        ));
    }

    // Churn: delete ~30% and update ~30% of the rows, then re-measure. This is
    // where an incremental-maintenance refactor would regress recall.
    let ids: Vec<i64> = model.keys().copied().collect();
    for (i, id) in ids.iter().enumerate() {
        if i % 10 < 3 {
            if exec(&ex, &format!("DELETE FROM vr WHERE id = {id}")) {
                model.remove(id);
            }
        } else if i % 10 < 6 {
            let v = rand_vec(&mut rng);
            if exec(
                &ex,
                &format!("UPDATE vr SET v = {} WHERE id = {id}", vec_lit(&v)),
            ) {
                model.insert(*id, v);
            }
        }
    }
    // Add fresh rows post-churn to exercise insert-after-delete maintenance.
    for _ in 0..(n / 4) {
        let id = next_id;
        next_id += 1;
        let v = rand_vec(&mut rng);
        if exec(
            &ex,
            &format!("INSERT INTO vr VALUES ({id}, {})", vec_lit(&v)),
        ) {
            model.insert(id, v);
        }
    }

    let churned = measure_recall(&ex, &model, case.metric, k, queries, &mut rng);
    println!(
        "  {:>8} {:>6}  churn recall@{k} = {:.3}  (floor {:.2})",
        case.kind, case.metric, churned, case.churn_floor
    );
    if churned < case.churn_floor {
        return Some(format!(
            "{} {}: post-churn recall {:.3} below floor {:.2}",
            case.kind, case.metric, churned, case.churn_floor
        ));
    }
    None
}

fn main_impl() {
    std::panic::set_hook(Box::new(|_| {}));
    let args: Vec<String> = std::env::args().collect();
    let mut seed: u64 = 0x5EED_1234_ABCD;
    let mut n: usize = 600;
    let mut k: usize = 10;
    let mut queries: usize = 40;
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--seed" => {
                i += 1;
                seed = args.get(i).and_then(|s| s.parse().ok()).unwrap_or(seed);
            }
            "--n" => {
                i += 1;
                n = args.get(i).and_then(|s| s.parse().ok()).unwrap_or(n);
            }
            "--k" => {
                i += 1;
                k = args.get(i).and_then(|s| s.parse().ok()).unwrap_or(k);
            }
            "--queries" => {
                i += 1;
                queries = args.get(i).and_then(|s| s.parse().ok()).unwrap_or(queries);
            }
            _ => {}
        }
        i += 1;
    }

    // Floors sit well below observed values (hnsw fresh ~1.0 / churn ~0.72-0.76;
    // ivfflat fresh ~0.78-0.83 / churn ~0.68-0.78 across seeds) so the gate is
    // robust to seed variance while still catching a real regression — an
    // incremental-maintenance bug drops recall far more than this headroom, or
    // trips the coherence oracle outright.
    let cases = [
        Case {
            kind: "hnsw",
            metric: "l2",
            fresh_floor: 0.80,
            churn_floor: 0.55,
        },
        Case {
            kind: "hnsw",
            metric: "cosine",
            fresh_floor: 0.75,
            churn_floor: 0.55,
        },
        Case {
            kind: "ivfflat",
            metric: "l2",
            fresh_floor: 0.45,
            churn_floor: 0.25,
        },
    ];

    println!("probe_vector_recall: n={n} k={k} queries={queries} seed={seed:#x}");
    let mut failures = Vec::new();
    for (ci, case) in cases.iter().enumerate() {
        if let Some(f) = run_case(
            case,
            n,
            k,
            queries,
            seed.wrapping_add(ci as u64 * 0x9E3779B1),
        ) {
            failures.push(f);
        }
    }

    println!("\n════ SUMMARY ════");
    if failures.is_empty() {
        println!("All vector recall floors met (fresh + post-churn). 🎯");
    } else {
        for f in &failures {
            println!("FAIL: {f}");
        }
        std::process::exit(1);
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    tokio::task::spawn_blocking(main_impl).await.unwrap();
}
