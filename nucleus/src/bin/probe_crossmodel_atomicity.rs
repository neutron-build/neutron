//! Crash proof for cross-model atomicity (S63): SQL + Streams (slice 1),
//! SQL + the KV strings WAL (slice 2), SQL + the document WAL (slice 3), and
//! SQL + the property-graph WAL (slice 4).
//!
//! The claim under test is the discard half of Option D: a transaction that
//! spans the SQL engine and a specialty model is atomic across a crash in the
//! window between its specialty records and its SQL COMMIT record. The child
//! runs `BEGIN; INSERT (SQL); STREAM_XADD / KV_SET / DOC_INSERT /
//! GRAPH_ADD_NODE; COMMIT` and dies at `crossmodel.before_commit_record` —
//! after the specialty record was flushed to the WAL, before the COMMIT
//! record (with the coordinating id in its body) exists. Recovery must
//! discard BOTH halves: the specialty write because no commit record vouches
//! for its id, the INSERT because it is a loser.
//!
//! The converse is asserted in the same run: a transaction that completes its
//! COMMIT, plus an autocommit specialty write beside it, must survive the
//! same reopen — a filter that passed the first half by dropping everything
//! would pass the first assertion and fail this one.
//!
//! This harness runs the SERVED stack (segmented DiskEngine wrapped in
//! BufferedDiskEngine), not the embedded durable_mvcc one, because that is
//! the configuration whose COMMIT records carry the S63 body.
//!
//! Run: `cargo run --release --features server --bin probe_crossmodel_atomicity`
#![cfg(feature = "server")]
#![allow(clippy::all)] // internal probe harness

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;

use nucleus::catalog::Catalog;
use nucleus::executor::{ExecError, ExecResult, Executor};
use nucleus::storage::buffered_engine::BufferedDiskEngine;
use nucleus::storage::persistence::CatalogPersistence;
use nucleus::storage::wal::SyncMode;
use nucleus::storage::{DiskEngine, StorageEngine};
use nucleus::types::Value;

const POINT: &str = "crossmodel.before_commit_record";

fn build_executor(dir: &Path) -> Result<Executor, String> {
    let catalog_path = dir.join("catalog.json");
    let db_path = dir.join("nucleus.db");
    let catalog = Arc::new(Catalog::new());

    let cp = CatalogPersistence::new(&catalog_path);
    cp.load_catalog_sync(&catalog)
        .map_err(|e| format!("catalog load: {e}"))?;

    let engine = Arc::new(
        DiskEngine::open_segmented_with_sync(&db_path, catalog.clone(), 64, 1, SyncMode::Fsync)
            .map_err(|e| format!("engine open: {e}"))?,
    );
    let storage: Arc<dyn StorageEngine> = Arc::new(BufferedDiskEngine::new(engine));
    Ok(Executor::new_with_persistence(
        catalog,
        storage,
        Some(catalog_path),
        Some(dir),
    ))
}

fn scalar_i64(res: &[ExecResult], sql: &str) -> Result<i64, String> {
    let r = res
        .first()
        .ok_or_else(|| format!("{sql} returned nothing"))?;
    let ExecResult::Select { rows, .. } = r else {
        return Err(format!("{sql} did not return rows"));
    };
    match rows.first().and_then(|row| row.first()) {
        Some(Value::Int64(v)) => Ok(*v),
        Some(Value::Int32(v)) => Ok(*v as i64),
        other => Err(format!("{sql} returned {other:?}")),
    }
}

/// One child run. `mode` selects the model and the outcome:
/// - "crash" arms the crashpoint on the streams transaction's COMMIT (the
///   child dies there);
/// - "commit" lets the streams transaction finish, then adds an autocommit
///   XADD;
/// - "kv_crash"/"kv_commit" are the KV_SET twins of the same two shapes;
/// - "doc_crash"/"doc_commit" are the DOC_INSERT twins;
/// - "graph_crash"/"graph_commit" are the GRAPH_ADD_NODE twins.
fn child_main(dir: &str, mode: &str) -> ! {
    std::panic::set_hook(Box::new(|_| {}));
    let rt = tokio::runtime::Runtime::new().expect("child rt");
    let ex = match build_executor(Path::new(dir)) {
        Ok(ex) => ex,
        Err(e) => {
            eprintln!("CHILD_OPEN_ERR {e:?}");
            std::process::exit(7);
        }
    };

    let run = async {
        let step = |sql: &'static str| ex.execute(sql);
        step("CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, v TEXT)")
            .await
            .map_err(|e: ExecError| e.to_string())?;
        step("BEGIN").await.map_err(|e| e.to_string())?;
        step("INSERT INTO t (id, v) VALUES (1, 'xm')")
            .await
            .map_err(|e| e.to_string())?;
        match mode {
            "kv_crash" | "kv_commit" => {
                step("SELECT KV_SET('xm', 'txn')")
                    .await
                    .map_err(|e| e.to_string())?;
            }
            "doc_crash" | "doc_commit" => {
                step("SELECT DOC_INSERT('{\"kind\": \"txn\"}')")
                    .await
                    .map_err(|e| e.to_string())?;
            }
            "graph_crash" | "graph_commit" => {
                step("SELECT GRAPH_ADD_NODE('Txn')")
                    .await
                    .map_err(|e| e.to_string())?;
            }
            _ => {
                step("SELECT STREAM_XADD('s', 'kind', 'txn')")
                    .await
                    .map_err(|e| e.to_string())?;
            }
        }
        // Crash modes: the crashpoint fires inside COMMIT handling, before
        // the COMMIT record — the process dies right here.
        step("COMMIT").await.map_err(|e| e.to_string())?;
        match mode {
            "commit" => {
                step("SELECT STREAM_XADD('s', 'kind', 'auto')")
                    .await
                    .map_err(|e| e.to_string())?;
            }
            "kv_commit" => {
                step("SELECT KV_SET('xm_auto', 'auto')")
                    .await
                    .map_err(|e| e.to_string())?;
            }
            "doc_commit" => {
                step("SELECT DOC_INSERT('{\"kind\": \"auto\"}')")
                    .await
                    .map_err(|e| e.to_string())?;
            }
            "graph_commit" => {
                step("SELECT GRAPH_ADD_NODE('Auto')")
                    .await
                    .map_err(|e| e.to_string())?;
            }
            _ => {}
        }
        Ok::<(), String>(())
    };
    if let Err(e) = rt.block_on(run) {
        eprintln!("CHILD_SQL_ERR {e}");
        std::process::exit(9);
    }
    std::process::exit(0);
}

fn recover_and_check(dir: &Path, expect_rows: i64, expect_len: i64) -> Result<(), String> {
    let rt = tokio::runtime::Runtime::new().map_err(|e| format!("rt: {e}"))?;
    let ex = build_executor(dir)?;
    let rows = rt.block_on(execute(&ex, "SELECT COUNT(*) FROM t"))?;
    let rows = scalar_i64(&rows, "COUNT(*)")?;
    let len = rt.block_on(execute(&ex, "SELECT STREAM_XLEN('s')"))?;
    let len = scalar_i64(&len, "STREAM_XLEN")?;
    if rows != expect_rows {
        return Err(format!(
            "SQL rows: expected {expect_rows}, recovered {rows}"
        ));
    }
    if len != expect_len {
        return Err(format!(
            "stream entries: expected {expect_len}, recovered {len}"
        ));
    }
    Ok(())
}

/// The KV twin of `recover_and_check`: `expect_kv` is `(key, expected)`,
/// `absent` names keys that must NOT have recovered.
fn recover_and_check_kv(
    dir: &Path,
    expect_rows: i64,
    expect_kv: &[(&str, &str)],
    absent: &[&str],
) -> Result<(), String> {
    let rt = tokio::runtime::Runtime::new().map_err(|e| format!("rt: {e}"))?;
    let ex = build_executor(dir)?;
    let rows = rt.block_on(execute(&ex, "SELECT COUNT(*) FROM t"))?;
    let rows = scalar_i64(&rows, "COUNT(*)")?;
    if rows != expect_rows {
        return Err(format!(
            "SQL rows: expected {expect_rows}, recovered {rows}"
        ));
    }
    for (key, want) in expect_kv {
        let res = rt.block_on(execute(&ex, &format!("SELECT KV_GET('{key}')")))?;
        match res.first() {
            Some(ExecResult::Select { rows, .. }) => match rows.first().and_then(|r| r.first()) {
                Some(Value::Text(v)) if v == want => {}
                other => {
                    return Err(format!(
                        "KV_GET('{key}'): expected {want:?}, recovered {other:?}"
                    ));
                }
            },
            other => return Err(format!("KV_GET('{key}') returned {other:?}")),
        }
    }
    for key in absent {
        let res = rt.block_on(execute(&ex, &format!("SELECT KV_GET('{key}')")))?;
        match res.first() {
            Some(ExecResult::Select { rows, .. }) => match rows.first().and_then(|r| r.first()) {
                Some(Value::Null) | None => {}
                other => {
                    return Err(format!(
                        "KV_GET('{key}'): expected absence after an uncommitted \
                         transaction, recovered {other:?}"
                    ));
                }
            },
            other => return Err(format!("KV_GET('{key}') returned {other:?}")),
        }
    }
    Ok(())
}

/// The document twin of `recover_and_check`: `expect` maps doc ids to a
/// substring their recovered JSON must contain; every other id in 1..=2
/// must be absent.
fn recover_and_check_doc(
    dir: &Path,
    expect_rows: i64,
    expect: &[(i64, &str)],
) -> Result<(), String> {
    let rt = tokio::runtime::Runtime::new().map_err(|e| format!("rt: {e}"))?;
    let ex = build_executor(dir)?;
    let rows = rt.block_on(execute(&ex, "SELECT COUNT(*) FROM t"))?;
    let rows = scalar_i64(&rows, "COUNT(*)")?;
    if rows != expect_rows {
        return Err(format!(
            "SQL rows: expected {expect_rows}, recovered {rows}"
        ));
    }
    for id in 1i64..=2 {
        let res = rt.block_on(execute(&ex, &format!("SELECT DOC_GET({id})")))?;
        let got = match res.first() {
            Some(ExecResult::Select { rows, .. }) => match rows.first().and_then(|r| r.first()) {
                Some(Value::Text(v)) => Some(v.clone()),
                Some(Value::Null) | None => None,
                other => return Err(format!("DOC_GET({id}) returned {other:?}")),
            },
            other => return Err(format!("DOC_GET({id}) returned {other:?}")),
        };
        match (expect.iter().find(|(eid, _)| *eid == id), got) {
            (Some((_, want)), Some(json)) => {
                if !json.contains(want) {
                    return Err(format!("DOC_GET({id}): expected {want:?} inside {json:?}"));
                }
            }
            (Some((_, want)), None) => {
                return Err(format!("DOC_GET({id}): expected {want:?}, recovered NULL"));
            }
            (None, Some(json)) => {
                return Err(format!(
                    "DOC_GET({id}): expected absence after an uncommitted \
                     transaction, recovered {json:?}"
                ));
            }
            (None, None) => {}
        }
    }
    Ok(())
}

async fn execute(ex: &Executor, sql: &str) -> Result<Vec<ExecResult>, String> {
    ex.execute(sql).await.map_err(|e| e.to_string())
}

/// The graph twin of `recover_and_check`: `expect` maps labels to the node
/// count that must have recovered; labels mapped through `absent` are
/// asserted at zero.
fn recover_and_check_graph(
    dir: &Path,
    expect_rows: i64,
    expect: &[(&str, i64)],
) -> Result<(), String> {
    let rt = tokio::runtime::Runtime::new().map_err(|e| format!("rt: {e}"))?;
    let ex = build_executor(dir)?;
    let rows = rt.block_on(execute(&ex, "SELECT COUNT(*) FROM t"))?;
    let rows = scalar_i64(&rows, "COUNT(*)")?;
    if rows != expect_rows {
        return Err(format!(
            "SQL rows: expected {expect_rows}, recovered {rows}"
        ));
    }
    for (label, want) in expect {
        let res = rt.block_on(execute(
            &ex,
            &format!("SELECT GRAPH_QUERY('MATCH (n:{label}) RETURN COUNT(*)')"),
        ))?;
        let json = match res.first() {
            Some(ExecResult::Select { rows, .. }) => match rows.first().and_then(|r| r.first()) {
                Some(Value::Text(v)) => v.clone(),
                other => return Err(format!("GRAPH_QUERY({label}) returned {other:?}")),
            },
            other => return Err(format!("GRAPH_QUERY({label}) returned {other:?}")),
        };
        let v: serde_json::Value = serde_json::from_str(&json)
            .map_err(|e| format!("GRAPH_QUERY({label}) unparseable {json:?}: {e}"))?;
        let got = v["rows"][0][0]
            .as_i64()
            .or_else(|| v["rows"][0][0].as_str().and_then(|s| s.parse().ok()))
            .ok_or_else(|| format!("no count in {json:?}"))?;
        if got != *want {
            return Err(format!(
                "GRAPH_QUERY({label}): expected {want} nodes, recovered {got}"
            ));
        }
    }
    Ok(())
}

fn main() {
    let raw: Vec<String> = std::env::args().collect();
    if raw.len() >= 4 && raw[1] == "--child" {
        child_main(&raw[2], &raw[3]);
    }

    let exe = std::env::current_exe().expect("current_exe");
    let root: PathBuf =
        std::env::temp_dir().join(format!("nucleus-xmodel-s63-{}", std::process::id()));

    println!("== cross-model atomicity crash proof (S63 slices 1+2+3+4) ==");
    println!("point: {POINT}\n");

    let mut findings: Vec<String> = Vec::new();

    // One scenario: a child mode to run and the recovery assertion for it.
    // The crash modes must die at the boundary; the commit modes must finish
    // cleanly. Both directions are asserted for both models.
    struct Scenario {
        mode: &'static str,
        label: &'static str,
        check: Box<dyn Fn(&Path) -> Result<(), String>>,
        pass_msg: &'static str,
    }
    let scenarios = vec![
        Scenario {
            mode: "crash",
            label: "streams",
            check: Box::new(|dir| recover_and_check(dir, 0, 0)),
            pass_msg: "crash before the commit record -> SQL row gone AND stream \
                 entry gone (atomic discard)",
        },
        Scenario {
            mode: "commit",
            label: "streams",
            check: Box::new(|dir| recover_and_check(dir, 1, 2)),
            pass_msg: "commit direction: committed txn (row + tagged XADD) and autocommit \
                 XADD all survive reopen",
        },
        Scenario {
            mode: "kv_crash",
            label: "kv",
            check: Box::new(|dir| recover_and_check_kv(dir, 0, &[], &["xm"])),
            pass_msg: "crash before the commit record -> SQL row gone AND kv.wal's \
                 tagged record discarded (atomic discard)",
        },
        Scenario {
            mode: "kv_commit",
            label: "kv",
            check: Box::new(|dir| {
                recover_and_check_kv(dir, 1, &[("xm", "txn"), ("xm_auto", "auto")], &[])
            }),
            pass_msg: "commit direction: committed txn (row + tagged KV_SET) and \
                 autocommit KV_SET all survive reopen",
        },
        Scenario {
            mode: "doc_crash",
            label: "doc",
            check: Box::new(|dir| recover_and_check_doc(dir, 0, &[])),
            pass_msg: "crash before the commit record -> SQL row gone AND doc.wal's \
                 tagged record discarded (atomic discard)",
        },
        Scenario {
            mode: "doc_commit",
            label: "doc",
            check: Box::new(|dir| recover_and_check_doc(dir, 1, &[(1, "txn"), (2, "auto")])),
            pass_msg: "commit direction: committed txn (row + tagged DOC_INSERT) and \
                 autocommit DOC_INSERT all survive reopen",
        },
        Scenario {
            mode: "graph_crash",
            label: "graph",
            check: Box::new(|dir| recover_and_check_graph(dir, 0, &[("Txn", 0), ("Auto", 0)])),
            pass_msg: "crash before the commit record -> SQL row gone AND graph.wal's \
                 tagged record discarded (atomic discard)",
        },
        Scenario {
            mode: "graph_commit",
            label: "graph",
            check: Box::new(|dir| recover_and_check_graph(dir, 1, &[("Txn", 1), ("Auto", 1)])),
            pass_msg: "commit direction: committed txn (row + tagged GRAPH_ADD_NODE) and \
                 autocommit GRAPH_ADD_NODE all survive reopen",
        },
    ];

    for sc in scenarios {
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("mkdir");
        let mut cmd = Command::new(&exe);
        cmd.arg("--child")
            .arg(root.to_str().unwrap())
            .arg(sc.mode)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if sc.mode.ends_with("crash") {
            cmd.env("NUCLEUS_CRASHPOINT", POINT);
        }
        let out = cmd.output().expect("spawn child");
        let stderr = String::from_utf8_lossy(&out.stderr);
        if sc.mode.ends_with("crash") {
            if !stderr.contains("NUCLEUS_CRASHPOINT_HIT") {
                findings.push(format!(
                    "{POINT}[{}]: never reached — the crash boundary is not exercised \
                     (coverage gap, not an atomicity finding)",
                    sc.label
                ));
                continue;
            }
        } else if !out.status.success() && stderr.contains("CHILD_") {
            findings.push(format!(
                "{} commit-direction child failed: {stderr}",
                sc.label
            ));
            continue;
        }
        match (sc.check)(&root) {
            Ok(()) => println!("{POINT}[{}]: {}", sc.label, sc.pass_msg),
            Err(e) => findings.push(format!(
                "{POINT}[{}]: {} — {e}",
                sc.label,
                if sc.mode.ends_with("crash") {
                    "DISCARD FAILED"
                } else {
                    "KEEP FAILED"
                }
            )),
        }
    }

    let _ = std::fs::remove_dir_all(&root);

    println!("\nfindings: {}", findings.len());
    for f in &findings {
        println!("    {f}");
    }
    if findings.is_empty() {
        println!(
            "\nCross-model transactions are atomic across the pre-commit-record \
             crash window: discard is total, and committed work survives."
        );
    } else {
        println!("\nCROSS-MODEL ATOMICITY FINDINGS PRESENT");
        std::process::exit(1);
    }
}
