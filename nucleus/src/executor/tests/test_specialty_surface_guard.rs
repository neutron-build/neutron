//! The specialty fail-closed guard, audited against the dispatcher itself.
//!
//! M5/N15 allows either implementing native ownership boundaries for the
//! specialty stores or keeping each surface unavailable while protected
//! relational state exists. Nucleus does the second: `is_specialty_surface`
//! refuses those functions while any RLS policy is active for the principal.
//!
//! The problem with that guard is its shape. It is a list of NAME PREFIXES,
//! and a prefix list cannot tell you what it missed — a new function whose
//! name does not happen to start with one of them is silently allowed, and
//! looks exactly like a function that was deliberately allowed. That is not
//! hypothetical: this test found `RETENTION_SET` and `RETENTION_CHECK`, which
//! reach the compliance retention engine (registering a deletion policy
//! against a named table, and enumerating every table with an estimated row
//! count) and matched no prefix, so both were callable by any principal with
//! RLS active.
//!
//! So the audit reads the dispatcher's own source: every `match` arm in
//! `scalar_fns.rs` that touches a store field must be classified as a
//! specialty surface. A new specialty function is then a failing test rather
//! than a hole, which is the only version of this guard that stays true.

use super::*;
use crate::executor::scalar_fns::is_specialty_surface;

/// The dispatcher's source, read at compile time.
const DISPATCHER: &str = include_str!("../scalar_fns.rs");
/// The executor's source, to prove the field names below still exist.
const EXECUTOR: &str = include_str!("../mod.rs");

/// Fields on `Executor` that ARE a specialty store — a function body
/// mentioning one is reaching data the secured relational path does not cover.
///
/// Adding a store to the executor means adding it here. The completeness check
/// below fails if a name in this list no longer exists, so a rename cannot
/// quietly disable the audit (a guard that cannot fire is the failure mode
/// this whole file exists to prevent).
const STORE_FIELDS: [&str; 15] = [
    "kv_store",
    "doc_store",
    "graph_store",
    "blob_store",
    "columnar_store",
    "datalog_store",
    "ts_store",
    "streams",
    "cdc_log",
    "fts_index",
    "vector_indexes",
    "tensor_store",
    "sparse_index",
    "version_store",
    "retention_engine",
];

/// Every `"NAME" => …` (and `"A" | "B" => …`) arm of the scalar dispatcher,
/// as (names, body).
///
/// Arms of the top-level dispatch begin at exactly 12 spaces with a quoted
/// SQL-shaped name; nested matches inside an arm are indented deeper, so the
/// depth is the delimiter. A head may span several lines (`"A"\n| "B" => {`),
/// so names are read from the start line up to the one carrying `=>`.
fn dispatch_arms() -> Vec<(Vec<String>, String)> {
    let lines: Vec<&str> = DISPATCHER.lines().collect();
    let is_start = |line: &str| {
        let Some(rest) = line.strip_prefix("            \"") else {
            return false;
        };
        if line.starts_with("             ") {
            return false; // deeper indentation: a nested match
        }
        let name = rest.split('"').next().unwrap_or("");
        name.len() >= 3
            && name
                .chars()
                .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
    };
    let starts: Vec<usize> = lines
        .iter()
        .enumerate()
        .filter(|(_, l)| is_start(l))
        .map(|(i, _)| i)
        .collect();

    let mut out = Vec::new();
    for (idx, start) in starts.iter().enumerate() {
        let end = starts.get(idx + 1).copied().unwrap_or(lines.len());
        // Head: up to and including the line carrying `=>`.
        let mut head = String::new();
        for line in lines.iter().take(end.min(start + 8)).skip(*start) {
            head.push_str(line);
            head.push('\n');
            if line.contains("=>") {
                break;
            }
        }
        let names = quoted_upper_names(&head);
        if !names.is_empty() {
            out.push((names, lines[*start..end].join("\n")));
        }
    }
    out
}

/// Every `"UPPER_NAME"` literal in `text`.
fn quoted_upper_names(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest = text;
    while let Some(open) = rest.find('"') {
        rest = &rest[open + 1..];
        let Some(close) = rest.find('"') else { break };
        let candidate = &rest[..close];
        rest = &rest[close + 1..];
        if candidate.len() >= 3
            && candidate
                .chars()
                .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
        {
            out.push(candidate.to_string());
        }
    }
    out
}

/// The audit must be able to find something. A parser that silently matched
/// nothing would make every assertion below vacuously true — the exact shape
/// that has passed for months elsewhere in this repo.
#[test]
fn the_audit_can_see_the_dispatcher() {
    let arms = dispatch_arms();
    assert!(
        arms.len() > 200,
        "only parsed {} dispatch arms; the parser has drifted from the source \
         and every assertion in this file would pass vacuously",
        arms.len()
    );
    let touching = arms
        .iter()
        .filter(|(_, body)| {
            STORE_FIELDS
                .iter()
                .any(|f| body.contains(&format!("self.{f}")))
        })
        .count();
    assert!(
        touching > 50,
        "only {touching} arms appear to touch a store; the field names have drifted"
    );
    for field in STORE_FIELDS {
        assert!(
            EXECUTOR.contains(&format!("{field}:")),
            "`{field}` is no longer a field on Executor — this audit is checking \
             for something that does not exist, so it cannot fire"
        );
    }
}

/// Every dispatch arm that reaches a store must be refused while RLS is active.
#[test]
fn every_store_touching_function_is_a_specialty_surface() {
    let mut unguarded: Vec<String> = Vec::new();
    for (names, body) in dispatch_arms() {
        let hits: Vec<&str> = STORE_FIELDS
            .iter()
            .copied()
            .filter(|f| body.contains(&format!("self.{f}")))
            .collect();
        if hits.is_empty() {
            continue;
        }
        for name in names {
            if !is_specialty_surface(&name) {
                unguarded.push(format!("{name} (touches {})", hits.join(", ")));
            }
        }
    }
    assert!(
        unguarded.is_empty(),
        "these functions reach a specialty store and are NOT refused while RLS \
         is active, so they are an alternate channel around the secured path:\n  {}",
        unguarded.join("\n  ")
    );
}

/// The reverse direction: functions that only compute over their arguments
/// must NOT be gated, or RLS silently removes ordinary SQL.
#[test]
fn pure_computations_are_not_gated() {
    for name in [
        "TS_MATCH",
        "TS_RANK",
        "TS_HEADLINE",
        "FTS_RANK",
        "UPPER",
        "ABS",
        "COALESCE",
        "GEO_DISTANCE",
        "TIME_BUCKET",
        "BM25",
        "VECTOR_DISTANCE",
    ] {
        assert!(
            !is_specialty_surface(name),
            "{name} computes over its arguments and must stay available under RLS"
        );
    }
}

/// And the guard actually fires, end to end, rather than merely classifying.
#[tokio::test]
async fn a_specialty_call_is_refused_once_a_policy_is_active() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE t (id INT PRIMARY KEY, owner TEXT)").await;
    exec(&ex, "CREATE ROLE reader LOGIN PASSWORD 'p'").await;

    // Control: before any policy exists, the specialty surface works.
    exec(&ex, "SELECT KV_SET('k', 'v')").await;
    exec(&ex, "SELECT RETENTION_SET('t', 30, 'created_at')").await;

    exec(
        &ex,
        "CREATE POLICY p ON t FOR SELECT TO reader USING (owner = 'ada')",
    )
    .await;
    exec(&ex, "ALTER TABLE t ENABLE ROW LEVEL SECURITY").await;

    // As a non-superuser: `any_rls_active` is deliberately false for a
    // superuser, so running this on the default session would prove nothing.
    let sid = ex.create_session();
    ex.bind_authenticated_session(sid, "reader").await.unwrap();

    for sql in [
        "SELECT KV_GET('k')",
        "SELECT DOC_GET(1)",
        "SELECT RETENTION_CHECK()",
        "SELECT RETENTION_SET('t', 30, 'created_at')",
    ] {
        let err = ex
            .execute_with_session(sid, sql)
            .await
            .expect_err(&format!("`{sql}` must be refused while RLS is active"));
        assert!(
            err.to_string()
                .contains("unavailable while row-level security"),
            "`{sql}` failed for the wrong reason: {err}"
        );
    }
    // Control again: ordinary SQL still works for that same principal.
    ex.execute_with_session(sid, "SELECT UPPER('still works')")
        .await
        .expect("a pure function must stay available under RLS");
}
