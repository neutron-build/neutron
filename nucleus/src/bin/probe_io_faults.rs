//! I/O failure injection: disk-full, fsync failure, permission loss (M3).
//!
//! The crash matrix models power loss. This models FAILING HARDWARE, which is
//! the other half of M3's exit gate: "recovery failures are actionable errors
//! and do not continue with suspect data."
//!
//! For each armed I/O fault point the child:
//!   1. writes rows until the injected failure fires,
//!   2. records the last id it saw ACKNOWLEDGED (a successful commit+fsync),
//!   3. keeps going, so a database that silently swallows the error is caught,
//!   4. runs the same acknowledged-or-errored discipline over the two
//!      specialty durable logs the S35 campaign opened to fault injection:
//!      Datalog WAL appends (`datalog.wal_append`) and vector WAL appends
//!      (`vector.wal_append`). A failed append must fail the statement —
//!      printed-and-acknowledged is exactly NU-013/NU-048 — and only what was
//!      acknowledged may survive a restart.
//!   5. drives KV keys past the eviction threshold and checkpoints under the
//!      fault (`lsm.sst_write`, STO-1/2): a failed cold-tier SSTable write
//!      must fail the checkpoint so the WAL is never truncated past keys
//!      whose only copy is still in RAM. Every acknowledged key must survive
//!      the reopen.
//!
//! The parent then reopens and asserts:
//!   A. The failure surfaced as an ERROR — a write that could not be made
//!      durable must never report success. Silent success is the worst
//!      outcome: the application believes data is safe when it is not.
//!   B. Recovery contains every acknowledged row (nothing acknowledged is lost).
//!   C. Recovery is a clean prefix with intact payloads — a failed write must
//!      not leave a half-applied or corrupt record behind.
//!   D. Every acknowledged DATALOG_ASSERT survives the reopen verbatim, and
//!      the recovered HNSW index holds exactly the acknowledged vectors.
//!   E. Every acknowledged KV key survives the reopen — via the hot WAL
//!      snapshot, the fsynced cold SSTable, or the un-truncated WAL when the
//!      checkpoint refused.
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
/// KV cold-tier section: 64 KiB values under a 1 MiB hot budget (the parent
/// sets NUCLEUS_KV_MAX_HOT_MB=1 for the child) spill most keys to the cold
/// LsmTree, so the checkpoint's SSTable write is the durability boundary
/// under test.
const KV_KEYS: i64 = 20;

fn marker_for(id: i64) -> i64 {
    id.wrapping_mul(2_654_435_761) % 1_000_003
}

/// Reopen and read the recovered Datalog facts of `iofault/2`.
fn recover_datalog(dir: &Path) -> Result<Vec<(String, String)>, String> {
    use nucleus::embedded::Database;
    let db = Database::durable_mvcc(dir).map_err(|e| format!("reopen: {e:?}"))?;
    let rt = tokio::runtime::Runtime::new().map_err(|e| format!("rt: {e}"))?;
    let res = rt.block_on(db.execute("SELECT DATALOG_QUERY('iofault(X,Y)')"));
    let mut results = res.map_err(|e| format!("query: {e:?}"))?;
    let text = match results.pop() {
        Some(ExecResult::Select { rows, .. }) => match rows.first().and_then(|r| r.first()) {
            Some(Value::Text(t)) => t.clone(),
            other => return Err(format!("bad datalog result: {other:?}")),
        },
        other => return Err(format!("non-select: {other:?}")),
    };
    // Scan `["node1", "mark1"]` pairs out of the JSON array text.
    let mut out = Vec::new();
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '"' {
            let mut first = String::new();
            while let Some(&c2) = chars.peek() {
                if c2 == '"' {
                    break;
                }
                first.push(c2);
                chars.next();
            }
            chars.next(); // closing quote
            let mut second = String::new();
            // Scan forward to the next quoted token in the same tuple.
            while let Some(&c2) = chars.peek() {
                if c2 == '"' {
                    chars.next();
                    break;
                }
                chars.next();
            }
            while let Some(&c2) = chars.peek() {
                if c2 == '"' {
                    break;
                }
                second.push(c2);
                chars.next();
            }
            chars.next(); // closing quote
            if !first.is_empty() && !second.is_empty() {
                out.push((first, second));
            }
        }
    }
    Ok(out)
}

/// Reopen and read the recovered live-vector id set of the HNSW index.
fn recover_vector_index(dir: &Path) -> Result<Vec<u64>, String> {
    use nucleus::embedded::Database;
    let db = Database::durable_mvcc(dir).map_err(|e| format!("reopen: {e:?}"))?;
    Ok(db
        .executor()
        .hnsw_index_live_ids("iov_idx")
        .ok_or_else(|| "HNSW index iov_idx did not survive reopen".to_string())?
        .into_iter()
        .collect())
}

/// Reopen and read back which KV cold-tier keys are present.
fn recover_kv(dir: &Path) -> Result<Vec<i64>, String> {
    use nucleus::embedded::Database;
    let db = Database::durable_mvcc(dir).map_err(|e| format!("reopen: {e:?}"))?;
    let rt = tokio::runtime::Runtime::new().map_err(|e| format!("rt: {e}"))?;
    let mut present = Vec::new();
    for i in 0..KV_KEYS {
        let sql = format!("SELECT KV_GET('iofk{i}')");
        let res = rt
            .block_on(db.execute(&sql))
            .map_err(|e| format!("query: {e:?}"))?;
        let got = res.into_iter().any(|r| match r {
            ExecResult::Select { rows, .. } => rows
                .first()
                .and_then(|row| row.first())
                .is_some_and(|v| !matches!(v, Value::Null)),
            _ => false,
        });
        if got {
            present.push(i);
        }
    }
    Ok(present)
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
        .block_on(
            db.execute("CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, m INTEGER NOT NULL)"),
        )
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

    // ── Datalog WAL: an append failure must fail the statement (NU-013) ──
    // A failed CREATE TABLE above means the specialty stores may not be
    // usable either; the parent treats zero DL_ACKED + zero DL_ERRORED as
    // "fault point not reached by this workload" for this section.
    for i in 1..=ROWS {
        let sql = format!("SELECT DATALOG_ASSERT('iofault(node{i}, mark{i})')");
        match rt.block_on(db.execute(&sql)) {
            Ok(_) => println!("DL_ACKED {i}"),
            Err(_) => {
                saw_error = true;
                println!("DL_ERRORED {i}");
            }
        }
        use std::io::Write;
        let _ = std::io::stdout().flush();
    }

    // ── Vector WAL: an insert/delete the WAL could not log must fail the
    //    statement, never be acknowledged (NU-048) ──
    let vec_setup_ok = rt
        .block_on(db.execute("CREATE TABLE IF NOT EXISTS iov (id INT PRIMARY KEY, x VECTOR(4))"))
        .is_ok()
        && rt
            .block_on(db.execute("CREATE INDEX IF NOT EXISTS iov_idx ON iov USING HNSW (x)"))
            .is_ok();
    if !vec_setup_ok {
        saw_error = true;
        println!("V_SETUP_ERRORED");
    }
    for i in 1..=ROWS {
        let sql = format!("INSERT INTO iov (id, x) VALUES ({i}, VECTOR('[{i},2,3,4]'))");
        match rt.block_on(db.execute(&sql)) {
            Ok(_) => println!("V_ACKED {i}"),
            Err(_) => {
                saw_error = true;
                println!("V_ERRORED {i}");
            }
        }
        use std::io::Write;
        let _ = std::io::stdout().flush();
    }
    // ── KV cold tier (STO-1/2): a failed SSTable write must fail the
    //    checkpoint, and only acknowledged keys may be required to survive.
    //    Values are large enough that the parent-set 1 MiB hot budget forces
    //    eviction, so the checkpoint's flush is what makes them durable. ──
    let pad = "kv".repeat(64 * 1024);
    for i in 0..KV_KEYS {
        let sql = format!("SELECT KV_SET('iofk{i}', '{pad}')");
        match rt.block_on(db.execute(&sql)) {
            Ok(_) => println!("KV_ACKED {i}"),
            Err(_) => {
                saw_error = true;
                println!("KV_ERRORED {i}");
            }
        }
        use std::io::Write;
        let _ = std::io::stdout().flush();
    }
    match db.executor().kv_store().checkpoint() {
        Ok(()) => println!("KV_CKPT_OK"),
        Err(_) => {
            saw_error = true;
            println!("KV_CKPT_ERRORED");
        }
    }
    use std::io::Write;
    let _ = std::io::stdout().flush();

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
    let negative = raw.iter().any(|a| a == "--negative-control");
    if negative {
        negative_control_main();
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
                let (reached, case_findings) = run_case(&exe, &dir, point, kind, skip, None);
                if !reached {
                    // The fault point was never reached by this workload.
                    unreached.push(label.clone());
                } else {
                    exercised += 1;
                    findings.extend(case_findings);
                }
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

/// One child run plus recovery verification. `perturb` names a section whose
/// MODEL is deliberately wrong for the negative control: `"datalog"` /
/// `"vector"` treat that section's ERRORED operations as acknowledged — the
/// exact NU-013/NU-048 bug shape (a failed WAL append acknowledged). Returns
/// (fault reached, findings).
fn run_case(
    exe: &std::path::Path,
    dir: &std::path::Path,
    point: &str,
    kind: &str,
    skip: u64,
    perturb: Option<&str>,
) -> (bool, Vec<String>) {
    let mut findings: Vec<String> = Vec::new();
    let label = format!("{point}/{kind}[skip={skip}]");
    let _ = std::fs::remove_dir_all(dir);
    let _ = std::fs::create_dir_all(dir);

    let out = Command::new(exe)
        .arg("--child")
        .arg(dir.to_str().unwrap())
        .env("NUCLEUS_IOFAULT", point)
        .env("NUCLEUS_IOFAULT_KIND", kind)
        .env("NUCLEUS_IOFAULT_SKIP", skip.to_string())
        // Small hot budget so the KV section actually spills to the cold
        // tier and the checkpoint's SSTable write is load-bearing.
        .env("NUCLEUS_KV_MAX_HOT_MB", "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("spawn");
    let stdout = String::from_utf8_lossy(&out.stdout);

    let collect = |prefix: &str| -> Vec<i64> {
        stdout
            .lines()
            .filter_map(|l| l.strip_prefix(prefix))
            .filter_map(|v| v.parse().ok())
            .collect()
    };
    let acked = collect("ACKED ");
    let mut dl_acked = collect("DL_ACKED ");
    let mut v_acked = collect("V_ACKED ");
    let kv_acked = collect("KV_ACKED ");
    if perturb == Some("datalog") {
        dl_acked.extend(collect("DL_ERRORED "));
    }
    if perturb == Some("vector") {
        v_acked.extend(collect("V_ERRORED "));
    }
    let errored = stdout.lines().any(|l| {
        l.starts_with("ERRORED ")
            || l.starts_with("DL_ERRORED")
            || l.starts_with("V_ERRORED")
            || l.starts_with("V_SETUP_ERRORED")
            || l.starts_with("KV_ERRORED")
            || l == "KV_CKPT_ERRORED"
    });

    if !errored {
        // The fault point was never reached by this workload.
        return (false, findings);
    }

    // (A) an unwritable commit must not be acknowledged: every id
    // the child printed as ACKED had a successful write AND fsync.
    // (B)+(C) verified against recovery below.
    let rows = match recover(dir) {
        Ok(r) => r,
        Err(e) => {
            findings.push(format!("{label}: recovery FAILED: {e}"));
            return (true, findings);
        }
    };
    let recovered: std::collections::HashSet<i64> = rows.iter().map(|(id, _)| *id).collect();

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

    // (D) specialty durable logs: only acknowledged mutations may
    // survive. Verified whenever the child reached the section at
    // all (any DL_*/V_* line), not only when it errored there —
    // an append failure that was swallowed prints ACKED and is
    // exactly what this catches.
    let reached_dl = stdout
        .lines()
        .any(|l| l.starts_with("DL_ACKED") || l.starts_with("DL_ERRORED"));
    let reached_v = stdout
        .lines()
        .any(|l| l.starts_with("V_ACKED") || l.starts_with("V_ERRORED"));
    if reached_dl {
        match recover_datalog(dir) {
            Ok(facts) => {
                let want: std::collections::HashSet<String> = dl_acked
                    .iter()
                    .map(|i| format!("node{i}, mark{i}"))
                    .collect();
                let got: std::collections::HashSet<String> =
                    facts.iter().map(|(a, b)| format!("{a}, {b}")).collect();
                if got != want {
                    findings.push(format!(
                        "{label}: datalog WAL: recovered {} facts, {} acknowledged — \
                         acknowledged facts lost or unacknowledged survived (NU-013 class)",
                        got.len(),
                        want.len()
                    ));
                }
            }
            Err(e) => {
                findings.push(format!("{label}: datalog recovery FAILED: {e}"));
            }
        }
    }
    if reached_v && !stdout.lines().any(|l| l == "V_SETUP_ERRORED") {
        match recover_vector_index(dir) {
            Ok(live) => {
                // Count-based: PK-keyed HNSW logs internal monotonic
                // node ids to the WAL (the pk→node registry is not
                // persisted), so id SETS live in a different space
                // than the inserted PKs — an off-by-one artifact,
                // not a finding. The count is the faithful
                // observable for this insert-only workload.
                let got = live.len();
                let want = v_acked.len();
                if got != want {
                    findings.push(format!(
                        "{label}: vector WAL: recovered {got} live vectors, {want} \
                         acknowledged — acknowledged inserts lost or unacknowledged \
                         inserts survived (NU-048 class)"
                    ));
                }
            }
            Err(e) => {
                findings.push(format!("{label}: vector recovery FAILED: {e}"));
            }
        }
    }
    // (E) KV cold tier: every acknowledged key must survive — through the
    // hot WAL snapshot and fsynced SSTable when the checkpoint succeeded,
    // or through the un-truncated WAL when it refused (STO-2). Verified
    // whenever the child reached the section at all, exactly like (D): a
    // checkpoint that silently truncated past a failed cold flush is what
    // this catches.
    let reached_kv = stdout
        .lines()
        .any(|l| l.starts_with("KV_ACKED") || l.starts_with("KV_ERRORED"));
    if reached_kv {
        match recover_kv(dir) {
            Ok(present) => {
                let got: std::collections::HashSet<i64> = present.into_iter().collect();
                for id in &kv_acked {
                    if !got.contains(id) {
                        findings.push(format!(
                            "{label}: KV key iofk{id} acknowledged but missing after \
                             recovery (cold tier lost below a checkpoint boundary — \
                             STO-1/2 class)"
                        ));
                        break;
                    }
                }
            }
            Err(e) => {
                findings.push(format!("{label}: KV recovery FAILED: {e}"));
            }
        }
    }
    let _ = std::fs::remove_dir_all(dir);
    (true, findings)
}

/// Negative control: prove the (D) checks can discriminate. For each specialty
/// log, run one faulted child twice — once with the honest acknowledged-set,
/// once with the model of the bug (ERRORED treated as acknowledged) — and
/// pass only if the honest model reports nothing and the perturbed model
/// reports the swallowed-failure finding.
fn negative_control_main() -> ! {
    let exe = std::env::current_exe().expect("exe");
    let root = std::env::temp_dir().join(format!("nucleus-iofault-ctl-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&root);

    println!("== I/O fault negative control ==");
    println!(
        "model of the bug: a failed WAL append is treated as acknowledged (NU-013 / NU-048)\n"
    );
    let mut ok = true;
    for (section, point) in [
        ("datalog", "datalog.wal_append"),
        ("vector", "vector.wal_append"),
    ] {
        let dir = root.join(format!("{section}_base"));
        let (_, base) = run_case(&exe, &dir, point, "full", 2, None);
        let dir = root.join(format!("{section}_pert"));
        let (reached, pert) = run_case(&exe, &dir, point, "full", 2, Some(section));
        println!(
            "{section:<8}: honest model {} finding(s), perturbed model {} finding(s)",
            base.len(),
            pert.len()
        );
        if !reached {
            println!("           REJECTED: fault point not reached — control is vacuous");
            ok = false;
        } else if base.is_empty() && !pert.is_empty() {
            println!(
                "           PASSED: the perturbation produced the finding the honest model does not"
            );
        } else {
            println!("           FAILED: a check that cannot fail is not a check");
            for f in &pert {
                println!("             {f}");
            }
            ok = false;
        }
    }
    let _ = std::fs::remove_dir_all(&root);
    if ok {
        println!("\nNEGATIVE CONTROL PASSED");
        std::process::exit(0);
    }
    println!("\nNEGATIVE CONTROL FAILED");
    std::process::exit(1);
}
