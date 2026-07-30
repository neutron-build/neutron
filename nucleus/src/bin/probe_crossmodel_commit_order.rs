//! Crash-injection proof for cross-model commit ordering (R3).
//!
//! A transaction that writes both a SQL row and a specialty-store value (KV,
//! timeseries, vector, graph, streams) forces two separate WALs durable at
//! COMMIT, with a crash window between them. `executor::mod::execute_statement`
//! forces the specialty stores FIRST and the SQL WAL LAST specifically so that
//! a crash in that window leaves the specialty write an ORPHAN (durable, but
//! unreferenced by anything the SQL side durably committed) rather than a
//! durable SQL row DANGLING a reference to a specialty write that was never
//! made durable.
//!
//! This harness dies at the named boundary, `commit.after_specialty_before_sql`
//! (`storage::crashpoint::ALL_POINTS`), and checks the only invariant that
//! actually distinguishes the two failure modes: for every row recovered in
//! `t`, its KV counterpart must also be recovered. The reverse — an orphaned
//! KV key with no matching row — is expected and fine.
//!
//! Run: `cargo run --release --features server --bin probe_crossmodel_commit_order`
#![cfg(feature = "server")]
#![allow(clippy::all)] // internal probe harness

use std::path::Path;
use std::process::{Command, Stdio};

use nucleus::executor::ExecResult;
use nucleus::types::Value;

const POINT: &str = "commit.after_specialty_before_sql";
/// Transactions attempted per run. Each is BEGIN; INSERT; SELECT KV_SET; COMMIT.
const ROWS_PER_RUN: i64 = 30;
/// Arrivals to let pass before dying: early, mid, and deep steady state.
const SKIPS: &[u64] = &[0, 3, 12];

fn marker_for(id: i64) -> i64 {
    id.wrapping_mul(2_654_435_761) % 1_000_003
}

fn kv_value_for(id: i64) -> String {
    format!("v{id}")
}

fn child_main(dir: &str) -> ! {
    use nucleus::embedded::Database;
    std::panic::set_hook(Box::new(|_| {}));

    let db = match Database::durable_mvcc(Path::new(dir)) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("CHILD_OPEN_ERR {e:?}");
            std::process::exit(7);
        }
    };
    let rt = tokio::runtime::Runtime::new().expect("child rt");

    let ddl = "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, m INTEGER NOT NULL)";
    if let Err(e) = rt.block_on(db.execute(ddl)) {
        eprintln!("CHILD_DDL_ERR {e:?}");
        std::process::exit(8);
    }
    let _ = db.sync();

    for id in 1..=ROWS_PER_RUN {
        let ins = format!("INSERT INTO t (id, m) VALUES ({id}, {})", marker_for(id));
        let kv = format!("SELECT KV_SET('xm{id}', '{}')", kv_value_for(id));
        let ok = rt.block_on(db.execute("BEGIN")).is_ok()
            && rt.block_on(db.execute(&ins)).is_ok()
            && rt.block_on(db.execute(&kv)).is_ok()
            && rt.block_on(db.execute("COMMIT")).is_ok();
        if !ok {
            std::process::exit(9);
        }
        if db.sync().is_err() {
            std::process::exit(10);
        }
        println!("DURABLE {id}");
        use std::io::Write;
        let _ = std::io::stdout().flush();
    }
    // Never reached when the crashpoint is armed and reachable.
    std::process::exit(0);
}

/// Reopen and read back both stores.
fn recover(dir: &Path) -> Result<(Vec<(i64, i64)>, std::collections::HashSet<i64>), String> {
    use nucleus::embedded::Database;
    let db = Database::durable_mvcc(dir).map_err(|e| format!("reopen: {e:?}"))?;
    let rt = tokio::runtime::Runtime::new().map_err(|e| format!("rt: {e}"))?;

    let mut rows = Vec::new();
    match rt.block_on(db.execute("SELECT id, m FROM t ORDER BY id")) {
        Ok(res) => {
            for r in res {
                if let ExecResult::Select { rows: rs, .. } = r {
                    for row in rs {
                        let id = match row.first() {
                            Some(Value::Int64(v)) => *v,
                            Some(Value::Int32(v)) => *v as i64,
                            other => return Err(format!("bad id cell: {other:?}")),
                        };
                        let m = match row.get(1) {
                            Some(Value::Int64(v)) => *v,
                            Some(Value::Int32(v)) => *v as i64,
                            other => return Err(format!("bad m cell: {other:?}")),
                        };
                        rows.push((id, m));
                    }
                }
            }
        }
        Err(e) => {
            let msg = format!("{e:?}");
            if !msg.contains("TableNotFound") {
                return Err(format!("select t: {msg}"));
            }
        }
    }

    // Which ids have a durably recovered KV counterpart.
    let mut kv_present = std::collections::HashSet::new();
    for id in 1..=ROWS_PER_RUN {
        let sql = format!("SELECT KV_GET('xm{id}')");
        match rt.block_on(db.execute(&sql)) {
            Ok(res) => {
                for r in res {
                    if let ExecResult::Select { rows: rs, .. } = r
                        && let Some(row) = rs.first()
                        && let Some(Value::Text(v)) = row.first()
                        && v == &kv_value_for(id)
                    {
                        kv_present.insert(id);
                    }
                }
            }
            Err(e) => return Err(format!("kv_get {id}: {e:?}")),
        }
    }

    Ok((rows, kv_present))
}

struct Finding(String);

fn main() {
    let raw: Vec<String> = std::env::args().collect();
    if raw.len() >= 3 && raw[1] == "--child" {
        child_main(&raw[2]);
    }

    let exe = std::env::current_exe().expect("current_exe");
    let root = std::env::temp_dir().join(format!("nucleus-xmodel-order-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&root);

    println!("== cross-model commit-order crash proof ==");
    println!("point: {POINT}  skips: {SKIPS:?}  txns/run: {ROWS_PER_RUN}\n");

    let mut findings: Vec<Finding> = Vec::new();
    let mut hit = 0usize;

    for &skip in SKIPS {
        let label = format!("{POINT}[skip={skip}]");
        let dir = root.join(format!("run-{skip}"));
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::create_dir_all(&dir);

        let out = Command::new(&exe)
            .arg("--child")
            .arg(dir.to_str().unwrap())
            .env("NUCLEUS_CRASHPOINT", POINT)
            .env("NUCLEUS_CRASHPOINT_SKIP", skip.to_string())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .expect("spawn child");

        let stderr = String::from_utf8_lossy(&out.stderr);
        let crashed = stderr.contains("NUCLEUS_CRASHPOINT_HIT");
        if !crashed {
            findings.push(Finding(format!(
                "{label}: never reached — the workload does not exercise this boundary \
                 at this skip depth (coverage gap, not a durability finding)"
            )));
            continue;
        }
        hit += 1;

        let (rows, kv_present) = match recover(&dir) {
            Ok(v) => v,
            Err(e) => {
                findings.push(Finding(format!("{label}: recovery FAILED: {e}")));
                continue;
            }
        };

        // The one invariant that matters: no SQL row may durably reference a
        // specialty write that was never made durable. Every recovered row's
        // KV counterpart must also be recovered.
        for (id, m) in &rows {
            if *m != marker_for(*id) {
                findings.push(Finding(format!(
                    "{label}: CORRUPT payload for id {id}: m={m}, expected {}",
                    marker_for(*id)
                )));
            }
            if !kv_present.contains(id) {
                findings.push(Finding(format!(
                    "{label}: DANGLING REFERENCE — row id {id} is durable in SQL but its \
                     KV counterpart xm{id} is NOT durable. Cross-model commit ordering \
                     is backwards."
                )));
            }
        }

        // The converse is the whole point: an orphaned KV key with no SQL row
        // is the expected, SAFE half of this crash window. Report it as
        // informational, not a finding.
        let sql_ids: std::collections::HashSet<i64> = rows.iter().map(|(id, _)| *id).collect();
        let orphans: Vec<i64> = kv_present
            .iter()
            .copied()
            .filter(|id| !sql_ids.contains(id))
            .collect();
        if !orphans.is_empty() {
            println!("{label}: {} orphaned specialty write(s) — expected, harmless", orphans.len());
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    let _ = std::fs::remove_dir_all(&root);

    println!("\nruns crashed at the boundary: {hit}/{}", SKIPS.len());
    println!("findings: {}", findings.len());
    for f in &findings {
        println!("    {}", f.0);
    }

    if findings.is_empty() {
        println!(
            "\nEvery crash at {POINT} recovered with no SQL row dangling a reference \
             to an un-durable specialty write."
        );
    } else {
        println!("\nCROSS-MODEL COMMIT ORDERING FINDINGS PRESENT");
        std::process::exit(1);
    }
}
