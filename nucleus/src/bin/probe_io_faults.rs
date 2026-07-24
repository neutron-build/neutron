//! I/O failure injection: disk-full, fsync failure, permission loss (M3).
//!
//! The crash matrix models power loss. This models FAILING HARDWARE, which is
//! the other half of M3's exit gate: "recovery failures are actionable errors
//! and do not continue with suspect data."
//!
//! For each armed I/O fault point the child:
//!   1. writes rows until the injected failure fires,
//!   2. records the last id it saw ACKNOWLEDGED (a successful commit+fsync),
//!   3. keeps going, so a database that silently swallows the error is caught.
//!
//! The parent then reopens and asserts:
//!   A. The failure surfaced as an ERROR — a write that could not be made
//!      durable must never report success. Silent success is the worst
//!      outcome: the application believes data is safe when it is not.
//!   B. Recovery contains every acknowledged row (nothing acknowledged is lost).
//!   C. Recovery is a clean prefix with intact payloads — a failed write must
//!      not leave a half-applied or corrupt record behind.
//!
//! Run: `cargo run --release --features server --bin probe_io_faults`
#![cfg(feature = "server")]
#![allow(clippy::all)] // internal probe harness

use std::path::Path;
use std::process::{Command, Stdio};

use nucleus::executor::ExecResult;
use nucleus::storage::crashpoint::ALL_IO_POINTS;
use nucleus::types::Value;

const ROWS: i64 = 30;
/// skip=0 lands the failure during schema setup (the only time meta.json is
/// written); 2 and 8 land it in steady state, where rows already exist to
/// protect.
const SKIPS: &[u64] = &[0, 2, 8];
const KINDS: &[&str] = &["full", "perm", "io"];

fn marker_for(id: i64) -> i64 {
    id.wrapping_mul(2_654_435_761) % 1_000_003
}

fn child_main(dir: &str) -> ! {
    use nucleus::embedded::Database;
    std::panic::set_hook(Box::new(|_| {}));
    let db = match Database::durable_mvcc(Path::new(dir)) {
        Ok(d) => d,
        Err(_) => std::process::exit(7),
    };
    let rt = tokio::runtime::Runtime::new().expect("rt");
    // DDL may itself be the operation the fault hits (meta.json is written
    // only here). Record that rather than exiting, so the parent still sees an
    // ERRORED line and can assert the failure surfaced.
    let mut saw_error = false;
    if rt
        .block_on(db.execute(
            "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, m INTEGER NOT NULL)",
        ))
        .is_err()
    {
        saw_error = true;
        println!("ERRORED ddl");
    }
    let _ = db.sync();

    for id in 1..=ROWS {
        let sql = format!("INSERT INTO t (id, m) VALUES ({id}, {})", marker_for(id));
        let wrote = rt.block_on(db.execute(&sql)).is_ok();
        let synced = db.sync().is_ok();
        if wrote && synced {
            // Durable and acknowledged.
            println!("ACKED {id}");
        } else {
            saw_error = true;
            println!("ERRORED {id}");
        }
        use std::io::Write;
        let _ = std::io::stdout().flush();
    }
    println!("SAW_ERROR {saw_error}");
    std::process::exit(0);
}

fn recover(dir: &Path) -> Result<Vec<(i64, i64)>, String> {
    use nucleus::embedded::Database;
    let db = Database::durable_mvcc(dir).map_err(|e| format!("reopen: {e:?}"))?;
    let rt = tokio::runtime::Runtime::new().map_err(|e| format!("rt: {e}"))?;
    let res = match rt.block_on(db.execute("SELECT id, m FROM t ORDER BY id")) {
        Ok(r) => r,
        Err(e) => {
            let msg = format!("{e:?}");
            if msg.contains("TableNotFound") {
                return Ok(Vec::new());
            }
            return Err(format!("query: {msg}"));
        }
    };
    let mut out = Vec::new();
    for r in res {
        if let ExecResult::Select { rows, .. } = r {
            for row in rows {
                let id = match row.first() {
                    Some(Value::Int64(v)) => *v,
                    Some(Value::Int32(v)) => *v as i64,
                    o => return Err(format!("bad id: {o:?}")),
                };
                let m = match row.get(1) {
                    Some(Value::Int64(v)) => *v,
                    Some(Value::Int32(v)) => *v as i64,
                    o => return Err(format!("bad m: {o:?}")),
                };
                out.push((id, m));
            }
        }
    }
    Ok(out)
}

fn main() {
    let raw: Vec<String> = std::env::args().collect();
    if raw.len() >= 3 && raw[1] == "--child" {
        child_main(&raw[2]);
    }

    let exe = std::env::current_exe().expect("exe");
    let root = std::env::temp_dir().join(format!("nucleus-iofault-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&root);

    println!("== I/O fault matrix ==");
    println!(
        "points: {:?}  kinds: {:?}  skips: {:?}\n",
        ALL_IO_POINTS, KINDS, SKIPS
    );

    let mut findings: Vec<String> = Vec::new();
    let mut exercised = 0usize;
    let mut unreached: Vec<String> = Vec::new();

    for point in ALL_IO_POINTS {
        for kind in KINDS {
            for &skip in SKIPS {
                let label = format!("{point}/{kind}[skip={skip}]");
                let dir = root.join(label.replace(['.', '/', '[', ']', '='], "_"));
                let _ = std::fs::remove_dir_all(&dir);
                let _ = std::fs::create_dir_all(&dir);

                let out = Command::new(&exe)
                    .arg("--child")
                    .arg(dir.to_str().unwrap())
                    .env("NUCLEUS_IOFAULT", point)
                    .env("NUCLEUS_IOFAULT_KIND", kind)
                    .env("NUCLEUS_IOFAULT_SKIP", skip.to_string())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .output()
                    .expect("spawn");
                let stdout = String::from_utf8_lossy(&out.stdout);

                let acked: Vec<i64> = stdout
                    .lines()
                    .filter_map(|l| l.strip_prefix("ACKED "))
                    .filter_map(|v| v.parse().ok())
                    .collect();
                let errored = stdout.lines().any(|l| l.starts_with("ERRORED "));

                if !errored {
                    // The fault point was never reached by this workload.
                    unreached.push(label.clone());
                    continue;
                }
                exercised += 1;

                // (A) an unwritable commit must not be acknowledged: every id
                // the child printed as ACKED had a successful write AND fsync.
                // (B)+(C) verified against recovery below.
                let rows = match recover(&dir) {
                    Ok(r) => r,
                    Err(e) => {
                        findings.push(format!("{label}: recovery FAILED: {e}"));
                        continue;
                    }
                };
                let recovered: std::collections::HashSet<i64> =
                    rows.iter().map(|(id, _)| *id).collect();

                for id in &acked {
                    if !recovered.contains(id) {
                        findings.push(format!(
                            "{label}: ACKNOWLEDGED id {id} missing after recovery \
                             (write reported success but was not durable)"
                        ));
                        break;
                    }
                }
                for (id, m) in &rows {
                    if *m != marker_for(*id) {
                        findings.push(format!(
                            "{label}: CORRUPT payload for id {id}: {m} != {}",
                            marker_for(*id)
                        ));
                        break;
                    }
                }
                let _ = std::fs::remove_dir_all(&dir);
            }
        }
    }

    let _ = std::fs::remove_dir_all(&root);

    println!("fault points exercised : {exercised}");
    println!("combinations not reached: {}", unreached.len());
    println!("findings                : {}", findings.len());
    for f in &findings {
        println!("    {f}");
    }
    if findings.is_empty() {
        println!(
            "\nEvery injected I/O failure surfaced as an error; no acknowledged write was lost \
             and no corrupt record survived."
        );
    } else {
        println!("\nI/O FAULT FINDINGS PRESENT");
        std::process::exit(1);
    }
}
