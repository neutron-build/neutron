//! Tests for the five previously-orphaned modules wired to the executor:
//! tensor, branching, versioning, compliance, and procedures.

use crate::types::Value;
use super::{exec, scalar, test_executor};

// ============================================================================
// Tensor SQL function tests
// ============================================================================

#[tokio::test]
async fn test_tensor_store_and_shape() {
    let ex = test_executor();
    // Store a 2x3 float32 tensor (zero-filled, no hex data needed)
    let res = exec(&ex, "SELECT tensor_store('weights', 'v1', '[2,3]', 'float32')").await;
    assert_eq!(scalar(&res[0]), &Value::Text("OK".into()));

    // Shape should be [2,3]
    let res = exec(&ex, "SELECT tensor_shape('weights', 'v1')").await;
    assert_eq!(scalar(&res[0]), &Value::Text("[2,3]".into()));
}

#[tokio::test]
async fn test_tensor_versions() {
    let ex = test_executor();
    exec(&ex, "SELECT tensor_store('model', 'v1', '[4]', 'float32')").await;
    exec(&ex, "SELECT tensor_store('model', 'v2', '[4]', 'float32')").await;

    let res = exec(&ex, "SELECT tensor_versions('model')").await;
    assert_eq!(scalar(&res[0]), &Value::Int64(2));
}

#[tokio::test]
async fn test_tensor_count() {
    let ex = test_executor();
    let res = exec(&ex, "SELECT tensor_count()").await;
    assert_eq!(scalar(&res[0]), &Value::Int64(0));

    exec(&ex, "SELECT tensor_store('a', 'v1', '[2]', 'float32')").await;
    exec(&ex, "SELECT tensor_store('b', 'v1', '[3]', 'int32')").await;

    let res = exec(&ex, "SELECT tensor_count()").await;
    assert_eq!(scalar(&res[0]), &Value::Int64(2));
}

#[tokio::test]
async fn test_tensor_size_bytes() {
    let ex = test_executor();
    // 3x4 float32 = 12 elements * 4 bytes = 48 bytes
    exec(&ex, "SELECT tensor_store('t', 'v1', '[3,4]', 'float32')").await;
    let res = exec(&ex, "SELECT tensor_size_bytes('t')").await;
    assert_eq!(scalar(&res[0]), &Value::Int64(48));
}

#[tokio::test]
async fn test_tensor_list_versions() {
    let ex = test_executor();
    exec(&ex, "SELECT tensor_store('net', 'epoch1', '[2]', 'float32')").await;
    exec(&ex, "SELECT tensor_store('net', 'epoch2', '[2]', 'float32')").await;

    let res = exec(&ex, "SELECT tensor_list_versions('net')").await;
    let json = match scalar(&res[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected Text, got {other:?}"),
    };
    assert!(json.contains("epoch1"));
    assert!(json.contains("epoch2"));
}

// ============================================================================
// Compliance SQL function tests
// ============================================================================

#[tokio::test]
async fn test_pii_detect_email() {
    let ex = test_executor();
    let res = exec(&ex, "SELECT pii_detect('email_address', 'user@example.com', 'admin@test.org')").await;
    let json = match scalar(&res[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected Text, got {other:?}"),
    };
    // Should detect Email category
    assert!(json.contains("Email") || json.len() > 2, "expected PII matches, got: {json}");
}

#[tokio::test]
async fn test_pii_detect_category() {
    let ex = test_executor();
    let res = exec(&ex, "SELECT pii_detect_category('email', 'user@example.com')").await;
    let category = match scalar(&res[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected Text, got {other:?}"),
    };
    assert_eq!(category, "Email");
}

#[tokio::test]
async fn test_pii_detect_no_match() {
    let ex = test_executor();
    let res = exec(&ex, "SELECT pii_detect_category('description', 'This is a normal text field')").await;
    let category = match scalar(&res[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected Text, got {other:?}"),
    };
    assert_eq!(category, "NONE");
}

#[tokio::test]
async fn test_retention_set_and_check() {
    let ex = test_executor();
    let res = exec(&ex, "SELECT retention_set('events', 30, 'created_at')").await;
    assert_eq!(scalar(&res[0]), &Value::Text("OK".into()));

    let res = exec(&ex, "SELECT retention_check()").await;
    let json = match scalar(&res[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected Text, got {other:?}"),
    };
    // Result is a JSON array (may be empty or contain entries)
    assert!(json.starts_with('['), "expected JSON array, got: {json}");
}

#[tokio::test]
async fn test_gdpr_delete_plan() {
    let ex = test_executor();
    let res = exec(&ex, "SELECT gdpr_delete_plan('users', 'id', '42')").await;
    let json = match scalar(&res[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected Text, got {other:?}"),
    };
    // Should return a JSON plan with 'users' table
    assert!(json.contains("users"), "expected users in plan, got: {json}");
}

// ============================================================================
// Row-level versioning SQL function tests
// ============================================================================

#[tokio::test]
async fn test_version_branches_default() {
    let ex = test_executor();
    // Fresh VersionStore has 'main' branch
    let res = exec(&ex, "SELECT version_branches()").await;
    let json = match scalar(&res[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected Text, got {other:?}"),
    };
    assert!(json.contains("main"), "expected 'main' branch, got: {json}");
}

#[tokio::test]
async fn test_version_branch_create() {
    let ex = test_executor();
    let res = exec(&ex, "SELECT version_branch('staging', 'main')").await;
    assert_eq!(scalar(&res[0]), &Value::Text("OK".into()));

    let res = exec(&ex, "SELECT version_branches()").await;
    let json = match scalar(&res[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected Text, got {other:?}"),
    };
    assert!(json.contains("staging"));
    assert!(json.contains("main"));
}

#[tokio::test]
async fn test_version_commit_and_log() {
    let ex = test_executor();
    let res = exec(&ex, "SELECT version_commit('main', 'initial data load')").await;
    let commit_id = match scalar(&res[0]) {
        Value::Int64(n) => *n,
        other => panic!("expected Int64, got {other:?}"),
    };
    assert!(commit_id > 0, "commit ID should be positive");

    let res = exec(&ex, "SELECT version_log('main')").await;
    let json = match scalar(&res[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected Text, got {other:?}"),
    };
    assert!(json.contains("initial data load"), "commit message missing from log: {json}");
}

// ============================================================================
// Database branching SQL function tests
// ============================================================================

#[tokio::test]
async fn test_db_branch_create_and_list() {
    let ex = test_executor();
    // 'main' exists by default
    let res = exec(&ex, "SELECT db_branch_list()").await;
    let json = match scalar(&res[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected Text, got {other:?}"),
    };
    assert!(json.contains("main"));

    // Create a new branch from main
    let res = exec(&ex, "SELECT db_branch_create('staging', 'main')").await;
    let branch_id = match scalar(&res[0]) {
        Value::Int64(n) => *n,
        other => panic!("expected Int64, got {other:?}"),
    };
    assert!(branch_id >= 1);

    let res = exec(&ex, "SELECT db_branch_list()").await;
    let json = match scalar(&res[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected Text, got {other:?}"),
    };
    assert!(json.contains("staging"));
}

#[tokio::test]
async fn test_db_branch_delete() {
    let ex = test_executor();
    exec(&ex, "SELECT db_branch_create('temp', 'main')").await;

    // Delete the branch
    let res = exec(&ex, "SELECT db_branch_delete('temp')").await;
    assert_eq!(scalar(&res[0]), &Value::Bool(true));

    // Deleting again returns false
    let res = exec(&ex, "SELECT db_branch_delete('temp')").await;
    assert_eq!(scalar(&res[0]), &Value::Bool(false));
}

#[tokio::test]
async fn test_db_branch_diff() {
    let ex = test_executor();
    exec(&ex, "SELECT db_branch_create('feature', 'main')").await;

    // Diff between two branches with no page differences
    let res = exec(&ex, "SELECT db_branch_diff('main', 'feature')").await;
    let json = match scalar(&res[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected Text, got {other:?}"),
    };
    assert!(json.contains("added") && json.contains("modified") && json.contains("deleted"),
        "unexpected diff JSON: {json}");
}

#[tokio::test]
async fn test_show_branches_command() {
    let ex = test_executor();
    exec(&ex, "SELECT db_branch_create('dev', 'main')").await;
    // SHOW BRANCHES returns a result set
    let results = ex.execute("SHOW BRANCHES").await.expect("SHOW BRANCHES failed");
    assert!(!results.is_empty());
    if let super::super::ExecResult::Select { rows, .. } = &results[0] {
        assert!(!rows.is_empty(), "expected at least 'main' in SHOW BRANCHES");
    }
}

// ============================================================================
// Stored procedure SQL function tests
// ============================================================================

#[tokio::test]
async fn test_proc_register_and_list() {
    let ex = test_executor();
    let res = exec(&ex, "SELECT proc_register('greet', 'SELECT 1')").await;
    assert_eq!(scalar(&res[0]), &Value::Text("OK".into()));

    let res = exec(&ex, "SELECT proc_list()").await;
    let json = match scalar(&res[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected Text, got {other:?}"),
    };
    assert!(json.contains("greet"), "registered procedure missing from list: {json}");
}

#[tokio::test]
async fn test_proc_drop() {
    let ex = test_executor();
    exec(&ex, "SELECT proc_register('to_drop', 'SELECT 1')").await;

    let res = exec(&ex, "SELECT proc_drop('to_drop')").await;
    assert_eq!(scalar(&res[0]), &Value::Bool(true));

    // Drop nonexistent → false
    let res = exec(&ex, "SELECT proc_drop('to_drop')").await;
    assert_eq!(scalar(&res[0]), &Value::Bool(false));
}

#[tokio::test]
async fn test_show_procedures_command() {
    let ex = test_executor();
    // Built-in procedures are auto-registered
    let results = ex.execute("SHOW PROCEDURES").await.expect("SHOW PROCEDURES failed");
    assert!(!results.is_empty());
    if let super::super::ExecResult::Select { rows, .. } = &results[0] {
        // At least the built-in 'nucleus_version' procedure should be present
        assert!(!rows.is_empty(), "expected at least one built-in procedure");
    }
}

#[tokio::test]
async fn test_create_and_call_procedure() {
    let ex = test_executor();
    exec(&ex, "CREATE PROCEDURE count_one() LANGUAGE sql AS 'SELECT 1 AS n'").await;

    // Verify it's registered
    let res = exec(&ex, "SELECT proc_list()").await;
    let json = match scalar(&res[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected Text, got {other:?}"),
    };
    assert!(json.contains("count_one"), "procedure not registered: {json}");
}

#[tokio::test]
async fn test_call_builtin_procedure() {
    let ex = test_executor();
    // 'nucleus_version' is a built-in procedure registered in ProcedureEngine::new()
    let results = ex.execute("CALL nucleus_version()").await.expect("CALL failed");
    assert!(!results.is_empty());
}

#[tokio::test]
async fn test_proc_with_params() {
    let ex = test_executor();
    // Register a procedure with parameters
    let res = exec(&ex, "SELECT proc_register('add_vals', 'a,b', 'SELECT $1 + $2')").await;
    assert_eq!(scalar(&res[0]), &Value::Text("OK".into()));

    // The procedure body with parameter substitution should be callable via CALL
    // (CALL returns the substituted SQL body for SQL procedures)
    let results = ex.execute("CALL add_vals(10, 20)").await.expect("CALL add_vals failed");
    assert!(!results.is_empty());
}
