//! Serializability oracle: does the outcome match SOME serial order?
//!
//! Every other isolation test in the tree checks a NAMED anomaly — write skew,
//! lost update, phantom. That is necessary and not sufficient: it can only find
//! the anomalies someone thought to write down, and the definition of
//! serializable is not "these six things do not happen", it is "the result is
//! one a serial execution could have produced".
//!
//! So this probe checks the definition directly. It runs N concurrent
//! SERIALIZABLE transactions with randomly generated, data-dependent scripts,
//! records which ones committed, then replays EVERY permutation of the
//! committed set serially against the initial state. If the database's actual
//! final state matches no permutation, the schedule was not serializable and
//! the probe prints the counterexample.
//!
//! Because the check is factorial, transactions per round are kept small
//! (default 4 → 24 orders). That is not a limitation on what can be caught: a
//! serializability violation needs only two transactions, and small rounds run
//! many more times.
//!
//! Scripts are data-dependent on purpose (`set b = read(a) + delta`). A script
//! of blind writes is serializable under almost any schedule and would make the
//! oracle vacuous — the read-modify-write is what makes order observable.
//!
//! Run:
//!   cargo run --release --features server --bin probe_serializable
//!   cargo run --release --features server --bin probe_serializable -- --rounds 500 --seed 7
//!   cargo run --release --features server --bin probe_serializable -- --engine mvcc
#![cfg(feature = "server")]
#![allow(clippy::all)] // internal probe harness

use std::sync::Arc;

use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::storage::StorageEngine;
use nucleus::storage::buffered_engine::BufferedDiskEngine;
use nucleus::storage::disk_engine::DiskEngine;
use nucleus::types::Value;

/// Rows per table.
const ACCOUNTS: i64 = 4;
/// Tables. Transactions pick their read and write table independently, so a
/// round contains a mix of conflicting and disjoint pairs.
///
/// This matters more than it looks. With a SINGLE table, table-level 2PL makes
/// every transaction in a round conflict no matter which row it touches, and
/// wait-die then kills all but one: a first version of this probe measured 600
/// aborts in 800 transactions, and an oracle where one transaction commits per
/// round is checking almost nothing. Multiple tables restore the case that
/// actually needs checking — partial overlap, where some transactions commit
/// concurrently and the serial order is not forced.
const TABLES: usize = 4;
const INITIAL: i64 = 100;

/// Deterministic xorshift — `rand` is not a dependency of this crate's probes
/// and a seeded generator is what makes a failure reproducible.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// One transaction's script: read `read_id`, then set `write_id` to the value
/// read plus `delta`. Deterministic given the state it observes, which is what
/// lets the serial replay reproduce it exactly.
#[derive(Clone, Copy, Debug)]
struct Script {
    read_tbl: usize,
    read_id: i64,
    write_tbl: usize,
    write_id: i64,
    delta: i64,
}

impl Script {
    fn random(rng: &mut Rng) -> Self {
        Self {
            read_tbl: rng.below(TABLES as u64) as usize,
            read_id: rng.below(ACCOUNTS as u64) as i64 + 1,
            write_tbl: rng.below(TABLES as u64) as usize,
            write_id: rng.below(ACCOUNTS as u64) as i64 + 1,
            delta: rng.below(9) as i64 + 1,
        }
    }

    /// Apply this script to an in-memory state, exactly as the SQL would.
    fn apply(&self, state: &mut Vec<Vec<i64>>) {
        let seen = state[self.read_tbl][(self.read_id - 1) as usize];
        state[self.write_tbl][(self.write_id - 1) as usize] = seen + self.delta;
    }
}

fn engine_of(kind: &str, dir: &std::path::Path) -> (Arc<Executor>, &'static str) {
    let catalog = Arc::new(Catalog::new());
    match kind {
        "mvcc" => {
            let storage: Arc<dyn StorageEngine> =
                Arc::new(nucleus::storage::MvccStorageAdapter::new());
            (Arc::new(Executor::new(catalog, storage)), "mvcc")
        }
        _ => {
            let disk = Arc::new(DiskEngine::open(&dir.join("t.db"), catalog.clone()).unwrap());
            let storage: Arc<dyn StorageEngine> = Arc::new(BufferedDiskEngine::new(disk));
            (Arc::new(Executor::new(catalog, storage)), "buffered-disk")
        }
    }
}

async fn read_all(ex: &Executor) -> Vec<Vec<i64>> {
    let mut all = Vec::new();
    for t in 0..TABLES {
        let res = ex
            .execute(&format!("SELECT id, balance FROM acct{t} ORDER BY id"))
            .await
            .expect("read back");
        let mut out = vec![0i64; ACCOUNTS as usize];
        for r in res {
            if let ExecResult::Select { rows, .. } = r {
                for row in rows {
                    let id = match row[0] {
                        Value::Int64(v) => v,
                        Value::Int32(v) => v as i64,
                        _ => panic!("bad id"),
                    };
                    let bal = match row[1] {
                        Value::Int64(v) => v,
                        Value::Int32(v) => v as i64,
                        _ => panic!("bad balance"),
                    };
                    out[(id - 1) as usize] = bal;
                }
            }
        }
        all.push(out);
    }
    all
}

/// Every permutation of `items`, as index vectors.
fn permutations(n: usize) -> Vec<Vec<usize>> {
    let mut out = Vec::new();
    let mut idx: Vec<usize> = (0..n).collect();
    fn go(k: usize, idx: &mut Vec<usize>, out: &mut Vec<Vec<usize>>) {
        if k == idx.len() {
            out.push(idx.clone());
            return;
        }
        for i in k..idx.len() {
            idx.swap(k, i);
            go(k + 1, idx, out);
            idx.swap(k, i);
        }
    }
    go(0, &mut idx, &mut out);
    out
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let arg = |name: &str, default: &str| -> String {
        args.iter()
            .position(|a| a == name)
            .and_then(|i| args.get(i + 1))
            .cloned()
            .unwrap_or_else(|| default.to_string())
    };
    let rounds: usize = arg("--rounds", "300").parse().unwrap();
    let per_round: usize = arg("--txns", "4").parse().unwrap();
    let seed: u64 = arg("--seed", "0x2545F4914F6CDD1D").parse().unwrap_or(0x2545_F491_4F6C_DD1D);
    let engine_kind = arg("--engine", "buffered-disk");
    // `--isolation read-committed` is the oracle's own control: the same
    // workload at a level that is NOT serializable must produce violations. An
    // oracle that reports zero failures without ever having been shown a
    // failure is measuring nothing.
    let isolation = arg("--isolation", "serializable");
    let begin_stmt = match isolation.as_str() {
        "read-committed" => "BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED".to_string(),
        other => format!("BEGIN TRANSACTION ISOLATION LEVEL {}", other.to_uppercase()),
    };

    println!("== serializability oracle ==");
    println!(
        "engine: {engine_kind}  isolation: {isolation}  rounds: {rounds}  \
         txns/round: {per_round}  seed: {seed:#x}"
    );
    println!(
        "checking the DEFINITION (result == some serial order), not a list of named anomalies\n"
    );

    let mut rng = Rng(seed);
    let orders = permutations(per_round.min(6));
    let mut violations: Vec<String> = Vec::new();
    let mut committed_total = 0usize;
    let mut aborted_total = 0usize;
    let mut all_committed_rounds = 0usize;

    let root = std::env::temp_dir().join(format!("nucleus-serializable-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&root);

    for round in 0..rounds {
        let dir = root.join(format!("r{round}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let (ex, _) = engine_of(&engine_kind, &dir);
        for t in 0..TABLES {
            ex.execute(&format!("CREATE TABLE acct{t} (id INT, balance INT)"))
                .await
                .unwrap();
            for i in 1..=ACCOUNTS {
                ex.execute(&format!("INSERT INTO acct{t} VALUES ({i}, {INITIAL})"))
                    .await
                    .unwrap();
            }
        }

        let scripts: Vec<Script> = (0..per_round).map(|_| Script::random(&mut rng)).collect();
        let gate = Arc::new(tokio::sync::Barrier::new(per_round));

        let mut handles = Vec::new();
        for (i, script) in scripts.iter().copied().enumerate() {
            let ex = ex.clone();
            let gate = gate.clone();
            let s = ex.create_session();
            let begin_stmt = begin_stmt.clone();
            handles.push(tokio::spawn(async move {
                let r = async {
                    ex.execute_with_session(s, &begin_stmt).await?;
                    let res = ex
                        .execute_with_session(
                            s,
                            &format!(
                                "SELECT balance FROM acct{} WHERE id = {}",
                                script.read_tbl, script.read_id
                            ),
                        )
                        .await?;
                    let seen = match &res[0] {
                        ExecResult::Select { rows, .. } => match rows[0][0] {
                            Value::Int64(v) => v,
                            Value::Int32(v) => v as i64,
                            _ => panic!("bad balance"),
                        },
                        _ => panic!("expected Select"),
                    };
                    // Force the overlap. Without it the tasks frequently run
                    // one after another and the oracle checks nothing.
                    gate.wait().await;
                    ex.execute_with_session(
                        s,
                        &format!(
                            "UPDATE acct{} SET balance = {} WHERE id = {}",
                            script.write_tbl,
                            seen + script.delta,
                            script.write_id
                        ),
                    )
                    .await?;
                    ex.execute_with_session(s, "COMMIT").await
                }
                .await;
                if r.is_err() {
                    let _ = ex.execute_with_session(s, "ROLLBACK").await;
                }
                (i, r.is_ok())
            }));
        }

        let mut committed: Vec<usize> = Vec::new();
        for h in handles {
            let (i, ok) = h.await.unwrap();
            if ok {
                committed.push(i);
                committed_total += 1;
            } else {
                aborted_total += 1;
            }
        }
        committed.sort_unstable();
        if committed.len() == per_round {
            all_committed_rounds += 1;
        }

        let actual = read_all(&ex).await;

        // Replay every serial order of the COMMITTED transactions. An aborted
        // transaction left no trace, so it takes no part in the equivalent
        // serial history.
        let mut matched = false;
        for order in &orders {
            let seq: Vec<usize> = order.iter().copied().filter(|i| committed.contains(i)).collect();
            if seq.len() != committed.len() {
                continue;
            }
            let mut state = vec![vec![INITIAL; ACCOUNTS as usize]; TABLES];
            for &i in &seq {
                scripts[i].apply(&mut state);
            }
            if state == actual {
                matched = true;
                break;
            }
        }

        let _ = std::fs::remove_dir_all(&dir);

        if !matched {
            violations.push(format!(
                "round {round}: final state {actual:?} matches no serial order of the \
                 committed transactions {committed:?}\n         scripts: {scripts:?}"
            ));
            if violations.len() >= 5 {
                break;
            }
        }
    }

    let _ = std::fs::remove_dir_all(&root);

    println!("rounds run              : {rounds}");
    println!("transactions committed  : {committed_total}");
    println!("transactions aborted    : {aborted_total}");
    println!("rounds with no abort    : {all_committed_rounds}");
    println!("serializability failures: {}", violations.len());
    for v in &violations {
        println!("    {v}");
    }

    if violations.is_empty() {
        println!(
            "\nEvery round's final state was reproducible by a serial execution of the \
             transactions that committed."
        );
    } else {
        println!("\nSERIALIZABILITY VIOLATIONS PRESENT");
        std::process::exit(1);
    }
}
