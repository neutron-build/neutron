//! Deterministic crash-point durability matrix (M3).
//!
//! `probe_crash_subprocess` kills a child at a RANDOM instant. That is good
//! evidence in aggregate but proves nothing about any SPECIFIC durability
//! window: a window that is only entered for a few microseconds may never be
//! sampled, and a passing run cannot distinguish "safe" from "never hit".
//!
//! This harness inverts that. For every crashpoint the engine declares
//! (`storage::crashpoint::ALL_POINTS`), and for a range of skip counts, it
//! spawns a child that dies EXACTLY at that boundary via `process::abort()`
//! (no unwinding, no Drop, no flush — power-loss equivalent), then reopens the
//! database and asserts the recovery contract:
//!
//!   1. Reopen never panics and never errors.
//!   2. The recovered rows are exactly a committed PREFIX `id = 1..k`: no
//!      gaps, no duplicates, no rows past `k`, no corrupted payloads.
//!   3. Every id the child printed as durably committed is present
//!      (`k >= last_printed`) — a fsynced commit must survive.
//!   4. Recovery is IDEMPOTENT: reopening a second and third time yields the
//!      identical row set.
//!
//! Invariant 3 is the sharp one: it is what separates "we didn't lose the
//! file" from "we honored the durability contract we advertise".
//!
//! Run: `cargo run --release --features server --bin probe_crash_points`
//!      `... --bin probe_crash_points -- --point wal.after_fsync`  (one point)
#![cfg(feature = "server")]
#![allow(clippy::all)] // internal probe harness

use std::path::Path;
use std::process::{Command, Stdio};

use nucleus::executor::ExecResult;
use nucleus::storage::crashpoint::ALL_POINTS;
use nucleus::types::Value;

/// Rows the child attempts per run.
const ROWS_PER_RUN: u64 = 40;
/// Skip counts to walk per point: dying on the 1st, 4th, and 13th arrival
/// exercises the boundary during schema setup, early steady state, and deep
/// steady state respectively.
const SKIPS: &[u64] = &[0, 3, 12];

fn marker_for(id: i64) -> i64 {
    id.wrapping_mul(2_654_435_761) % 1_000_003
}

// ─────────────────────────────────────────────────────────────────────────────
// Child: insert rows, fsync each commit, print durable ids, die at the point
// ─────────────────────────────────────────────────────────────────────────────

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

    // Resume where a previous phase left off, so the two-phase (populate then
    // crash-on-reopen) runs build on each other.
    let start: i64 = match rt.block_on(db.execute("SELECT COALESCE(MAX(id),0) FROM t")) {
        Ok(res) => res
            .into_iter()
            .find_map(|r| match r {
                ExecResult::Select { rows, .. } => rows.first().and_then(|row| match row.first() {
                    Some(Value::Int64(v)) => Some(*v + 1),
                    Some(Value::Int32(v)) => Some(*v as i64 + 1),
                    _ => None,
                }),
                _ => None,
            })
            .unwrap_or(1),
        Err(_) => 1,
    };

    for id in start..start + ROWS_PER_RUN as i64 {
        // Alternate commit shapes: auto-commit exercises the plain append/fsync
        // path, an explicit transaction exercises the COMMIT-record path. Both
        // must be durable once fsynced.
        let sql = format!("INSERT INTO t (id, m) VALUES ({id}, {})", marker_for(id));
        let ok = if id % 2 == 0 {
            rt.block_on(db.execute("BEGIN")).is_ok()
                && rt.block_on(db.execute(&sql)).is_ok()
                && rt.block_on(db.execute("COMMIT")).is_ok()
        } else {
            rt.block_on(db.execute(&sql)).is_ok()
        };
        if !ok {
            std::process::exit(9);
        }
        if db.sync().is_err() {
            std::process::exit(10);
        }
        // Only after a successful fsync may we claim this id is durable.
        println!("DURABLE {id}");
        use std::io::Write;
        let _ = std::io::stdout().flush();
    }
    // Never reached when a crashpoint is armed and reachable.
    std::process::exit(0);
}

// ─────────────────────────────────────────────────────────────────────────────
// Parent: recovery assertions
// ─────────────────────────────────────────────────────────────────────────────

/// Read the recovered table. Returns `Err` when reopen or the query fails.
fn recover(dir: &Path) -> Result<Vec<(i64, i64)>, String> {
    use nucleus::embedded::Database;
    let db = Database::durable_mvcc(dir).map_err(|e| format!("reopen: {e:?}"))?;
    let rt = tokio::runtime::Runtime::new().map_err(|e| format!("rt: {e}"))?;
    let res = match rt.block_on(db.execute("SELECT id, m FROM t ORDER BY id")) {
        Ok(r) => r,
        Err(e) => {
            // Dying at the very first WAL append kills the child during
            // CREATE TABLE, so the table legitimately does not exist. That is
            // an empty (k=0) recovery, not a failure — the prefix and
            // durability checks below still apply, and they are what decides
            // whether losing it was allowed.
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
                    other => return Err(format!("bad id cell: {other:?}")),
                };
                let m = match row.get(1) {
                    Some(Value::Int64(v)) => *v,
                    Some(Value::Int32(v)) => *v as i64,
                    other => return Err(format!("bad m cell: {other:?}")),
                };
                out.push((id, m));
            }
        }
    }
    Ok(out)
}

struct Finding(String);

fn check_recovery(dir: &Path, last_durable: i64, label: &str) -> Vec<Finding> {
    let mut findings = Vec::new();

    let rows = match recover(dir) {
        Ok(r) => r,
        Err(e) => {
            findings.push(Finding(format!("{label}: recovery FAILED: {e}")));
            return findings;
        }
    };

    // (2) exact committed prefix, no gaps / dupes / corruption
    let k = rows.len() as i64;
    for (i, (id, m)) in rows.iter().enumerate() {
        let expect_id = i as i64 + 1;
        if *id != expect_id {
            findings.push(Finding(format!(
                "{label}: not a prefix — position {i} holds id {id}, expected {expect_id}"
            )));
            break;
        }
        if *m != marker_for(*id) {
            findings.push(Finding(format!(
                "{label}: CORRUPT payload for id {id}: m={m}, expected {}",
                marker_for(*id)
            )));
            break;
        }
    }

    // (3) every fsynced commit survived
    if k < last_durable {
        findings.push(Finding(format!(
            "{label}: DURABILITY VIOLATION — child fsynced id {last_durable} but only {k} recovered"
        )));
    }

    // (4) replay idempotency across repeated recovery cycles
    for cycle in 2..=3 {
        match recover(dir) {
            Ok(again) => {
                if again != rows {
                    findings.push(Finding(format!(
                        "{label}: recovery NOT idempotent — cycle {cycle} yielded {} rows vs {}",
                        again.len(),
                        rows.len()
                    )));
                    break;
                }
            }
            Err(e) => {
                findings.push(Finding(format!(
                    "{label}: recovery cycle {cycle} FAILED: {e}"
                )));
                break;
            }
        }
    }

    findings
}

fn main() {
    let raw: Vec<String> = std::env::args().collect();
    if raw.len() >= 3 && raw[1] == "--child" {
        child_main(&raw[2]);
    }

    let only: Option<&str> = raw
        .iter()
        .position(|a| a == "--point")
        .and_then(|i| raw.get(i + 1))
        .map(|s| s.as_str());

    let exe = std::env::current_exe().expect("current_exe");
    let root = std::env::temp_dir().join(format!("nucleus-crashpoints-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&root);

    let points: Vec<&str> = match only {
        Some(p) => vec![p],
        None => ALL_POINTS.to_vec(),
    };

    println!("== deterministic crash-point matrix ==");
    println!(
        "points: {}  skips: {:?}  rows/run: {ROWS_PER_RUN}\n",
        points.len(),
        SKIPS
    );

    let mut findings: Vec<Finding> = Vec::new();
    let mut hit = 0usize;
    let mut never_reached: Vec<String> = Vec::new();
    let mut reached_points: std::collections::HashSet<String> = std::collections::HashSet::new();

    for point in &points {
        for &skip in SKIPS {
            let label = format!("{point}[skip={skip}]");
            let dir = root.join(format!("{}-{skip}", point.replace('.', "_")));
            let _ = std::fs::remove_dir_all(&dir);
            let _ = std::fs::create_dir_all(&dir);

            // WAL compaction only runs when reopening a dir that already holds
            // recovered state, so checkpoint points need a clean populate pass
            // first. Its durable ids carry into the crashing phase.
            let mut last_durable: i64 = 0;
            if point.starts_with("checkpoint.") {
                let pre = Command::new(&exe)
                    .arg("--child")
                    .arg(dir.to_str().unwrap())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .output()
                    .expect("spawn populate child");
                last_durable = String::from_utf8_lossy(&pre.stdout)
                    .lines()
                    .filter_map(|l| l.strip_prefix("DURABLE "))
                    .filter_map(|v| v.parse().ok())
                    .max()
                    .unwrap_or(0);
            }

            let out = Command::new(&exe)
                .arg("--child")
                .arg(dir.to_str().unwrap())
                .env("NUCLEUS_CRASHPOINT", point)
                .env("NUCLEUS_CRASHPOINT_SKIP", skip.to_string())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .output()
                .expect("spawn child");

            let stdout = String::from_utf8_lossy(&out.stdout);
            let stderr = String::from_utf8_lossy(&out.stderr);

            last_durable = last_durable.max(
                stdout
                    .lines()
                    .filter_map(|l| l.strip_prefix("DURABLE "))
                    .filter_map(|v| v.parse().ok())
                    .max()
                    .unwrap_or(0),
            );

            let crashed = stderr.contains("NUCLEUS_CRASHPOINT_HIT");
            if !crashed {
                // Either the point is unreachable by this workload (a real
                // coverage gap) or it simply fires fewer times than this skip
                // count. The distinction is made after the loop, from whether
                // ANY skip reached it.
                never_reached.push(label.clone());
                continue;
            }
            hit += 1;
            reached_points.insert(point.to_string());

            findings.extend(check_recovery(&dir, last_durable, &label));
            let _ = std::fs::remove_dir_all(&dir);
        }
    }

    let _ = std::fs::remove_dir_all(&root);

    let unreachable: Vec<&&str> = points
        .iter()
        .filter(|p| !reached_points.contains(**p))
        .collect();
    let low_frequency = never_reached.len() - unreachable.len() * SKIPS.len();

    println!("crash points exercised : {hit}");
    println!(
        "points UNREACHABLE     : {} {:?}",
        unreachable.len(),
        unreachable
    );
    println!("runs skipped (point fires fewer times than skip): {low_frequency}");
    println!("findings               : {}", findings.len());
    for f in &findings {
        println!("    {}", f.0);
    }

    if findings.is_empty() {
        println!(
            "\nEvery exercised crash point recovered to a committed prefix, \
             kept all fsynced commits, and replayed idempotently."
        );
    } else {
        println!("\nDURABILITY FINDINGS PRESENT");
        std::process::exit(1);
    }
}
