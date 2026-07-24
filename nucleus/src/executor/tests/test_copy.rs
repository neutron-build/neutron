//! `COPY ... FROM STDIN` payload reconstruction.
//!
//! sqlparser hands the executor a flat token list, not lines (see
//! `Executor::copy_payload_rows`). These tests pin both halves of that contract:
//! the shapes the parser actually produces, and the rows the executor must
//! rebuild from them.

use super::*;

/// Guard on the upstream parser. If a sqlparser upgrade ever starts returning
/// line-oriented values (or stops emitting the empty-field artifacts), the
/// reconstruction in `copy_payload_rows` becomes wrong and must be revisited —
/// this test is what will say so.
#[test]
fn sqlparser_copy_payload_is_a_flat_field_list() {
    use sqlparser::{ast::Statement, dialect::PostgreSqlDialect, parser::Parser};
    let payload = |sql: &str| -> Vec<Option<String>> {
        let stmts = Parser::parse_sql(&PostgreSqlDialect {}, sql).expect("parse failed");
        for stmt in stmts {
            if let Statement::Copy { values, .. } = stmt {
                return values;
            }
        }
        panic!("no COPY statement parsed");
    };

    // Two 2-column rows arrive as five entries: a leading empty artifact from
    // the newline after the semicolon, then four bare fields. Row boundaries
    // are gone.
    assert_eq!(
        payload("COPY t FROM STDIN;\n1\ta\n2\tb\n\\."),
        vec![
            Some(String::new()),
            Some("1".into()),
            Some("a".into()),
            Some("2".into()),
            Some("b".into()),
        ]
    );

    // `\N` yields None plus a trailing empty artifact.
    assert_eq!(
        payload("COPY t FROM STDIN;\n1\t\\N\n\\."),
        vec![
            Some(String::new()),
            Some("1".into()),
            None,
            Some(String::new()),
        ]
    );

    // A non-tab delimiter is not tokenizer-significant, so entries stay whole lines.
    assert_eq!(
        payload("COPY t FROM STDIN WITH (DELIMITER '|');\n1|a\n2|b\n\\."),
        vec![
            Some(String::new()),
            Some("1|a".into()),
            Some("2|b".into()),
        ]
    );
}

#[tokio::test]
async fn copy_from_stdin_text_reconstructs_rows() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT, qty INT)").await;
    let out = exec(
        &ex,
        "COPY t FROM STDIN;\n1\talice\t10\n2\tbob\t20\n3\tcarol\t30\n\\.",
    )
    .await;
    match &out[0] {
        ExecResult::Command { rows_affected, .. } => assert_eq!(
            *rows_affected, 3,
            "COPY must report the number of rows, not the number of fields"
        ),
        other => panic!("expected a command tag, got {other:?}"),
    }

    let got = exec(&ex, "SELECT id, name, qty FROM t ORDER BY id").await;
    assert_eq!(
        rows(&got[0]),
        &vec![
            vec![Value::Int32(1), Value::Text("alice".into()), Value::Int32(10)],
            vec![Value::Int32(2), Value::Text("bob".into()), Value::Int32(20)],
            vec![Value::Int32(3), Value::Text("carol".into()), Value::Int32(30)],
        ],
        "each payload line must become exactly one row with its fields in place"
    );
}

#[tokio::test]
async fn copy_from_stdin_text_honours_null_marker() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT, qty INT)").await;
    exec(
        &ex,
        "COPY t FROM STDIN;\n1\t\\N\t10\n2\tbob\t\\N\n\\N\tcarol\t30\n\\.",
    )
    .await;

    let got = exec(&ex, "SELECT id, name, qty FROM t ORDER BY qty").await;
    assert_eq!(
        rows(&got[0]),
        &vec![
            vec![Value::Int32(1), Value::Null, Value::Int32(10)],
            vec![Value::Null, Value::Text("carol".into()), Value::Int32(30)],
            vec![Value::Int32(2), Value::Text("bob".into()), Value::Null],
        ],
        "\\N must land as SQL NULL in its own column without shifting the row"
    );
}

#[tokio::test]
async fn copy_from_stdin_text_preserves_empty_and_spaced_fields() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)").await;
    exec(&ex, "COPY t FROM STDIN;\n1\ta b\n2\t\n\\.").await;

    let got = exec(&ex, "SELECT id, name FROM t ORDER BY id").await;
    // NOTE: the second row's empty field lands as NULL rather than ''. That is
    // `parse_field`'s blanket empty-string-is-NULL rule, a separate (and in text
    // format, non-Postgres) behaviour — not the reconstruction under test. What
    // matters here is that the field still occupies its own column and does not
    // shift or drop the row.
    assert_eq!(
        rows(&got[0]),
        &vec![
            vec![Value::Int32(1), Value::Text("a b".into())],
            vec![Value::Int32(2), Value::Null],
        ],
        "an embedded space must not split a field, and an empty trailing field \
         must not collapse the row"
    );
}

#[tokio::test]
async fn copy_from_stdin_csv_and_custom_delimiter() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE c (id INT PRIMARY KEY, name TEXT)").await;
    exec(
        &ex,
        "COPY c FROM STDIN WITH (FORMAT CSV);\n1,alice\n2,\"bob, jr\"\n\\.",
    )
    .await;
    let got = exec(&ex, "SELECT id, name FROM c ORDER BY id").await;
    assert_eq!(
        rows(&got[0]),
        &vec![
            vec![Value::Int32(1), Value::Text("alice".into())],
            vec![Value::Int32(2), Value::Text("bob, jr".into())],
        ]
    );

    let ex = test_executor();
    exec(&ex, "CREATE TABLE p (id INT PRIMARY KEY, name TEXT)").await;
    exec(
        &ex,
        "COPY p FROM STDIN WITH (DELIMITER '|');\n1|alice\n2|bob\n\\.",
    )
    .await;
    let got = exec(&ex, "SELECT id, name FROM p ORDER BY id").await;
    assert_eq!(
        rows(&got[0]),
        &vec![
            vec![Value::Int32(1), Value::Text("alice".into())],
            vec![Value::Int32(2), Value::Text("bob".into())],
        ]
    );
}

/// With HEADER, the row skipped must be the header line — not the leading
/// empty-field artifact, which would silently eat the header as data and drop
/// a real row instead.
#[tokio::test]
async fn copy_from_stdin_csv_header_skips_the_header_line() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE h (id INT PRIMARY KEY, name TEXT)").await;
    exec(
        &ex,
        "COPY h FROM STDIN WITH (FORMAT CSV, HEADER);\nid,name\n1,alice\n2,bob\n\\.",
    )
    .await;
    let got = exec(&ex, "SELECT id, name FROM h ORDER BY id").await;
    assert_eq!(
        rows(&got[0]),
        &vec![
            vec![Value::Int32(1), Value::Text("alice".into())],
            vec![Value::Int32(2), Value::Text("bob".into())],
        ]
    );
}

/// HEADER used to be honoured only for CSV, so a text-format header line was
/// inserted as data. Now that rows are reconstructed properly the skip applies
/// to both formats, matching PostgreSQL 16+.
#[tokio::test]
async fn copy_from_stdin_text_header_skips_the_header_line() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE th (id INT PRIMARY KEY, name TEXT)").await;
    exec(
        &ex,
        "COPY th FROM STDIN WITH (HEADER);\nid\tname\n1\talice\n\\.",
    )
    .await;
    let got = exec(&ex, "SELECT id, name FROM th ORDER BY id").await;
    assert_eq!(
        rows(&got[0]),
        &vec![vec![Value::Int32(1), Value::Text("alice".into())]],
        "the text-format header line must not be inserted as a row"
    );
}

/// COPY is a bulk INSERT and gets no exemption from declared constraints.
/// Because it writes through a bare storage append rather than the INSERT path,
/// it is the one write that can silently duplicate a primary key.
#[tokio::test]
async fn copy_from_enforces_unique_constraints() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE u (id INT PRIMARY KEY, name TEXT)").await;
    exec(&ex, "INSERT INTO u VALUES (1, 'first')").await;

    let clash = ex.execute("COPY u FROM STDIN;\n1\tsecond\n\\.").await;
    assert!(
        clash.is_err(),
        "COPY duplicated an existing primary key without error"
    );
    let got = exec(&ex, "SELECT id, name FROM u ORDER BY id").await;
    assert_eq!(
        rows(&got[0]),
        &vec![vec![Value::Int32(1), Value::Text("first".into())]],
        "the rejected COPY row must not be visible"
    );

    // ...including a key duplicated inside a single payload.
    let ex = test_executor();
    exec(&ex, "CREATE TABLE u (id INT PRIMARY KEY, name TEXT)").await;
    let self_clash = ex.execute("COPY u FROM STDIN;\n7\ta\n7\tb\n\\.").await;
    assert!(
        self_clash.is_err(),
        "COPY duplicated a primary key within its own payload without error"
    );
}

/// An empty payload must insert nothing at all — the leading artifact alone
/// used to become a phantom all-NULL row.
#[tokio::test]
async fn copy_from_stdin_empty_payload_inserts_nothing() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE e (id INT PRIMARY KEY, name TEXT)").await;
    let out = exec(&ex, "COPY e FROM STDIN;\n\\.").await;
    match &out[0] {
        ExecResult::Command { rows_affected, .. } => assert_eq!(*rows_affected, 0),
        other => panic!("expected a command tag, got {other:?}"),
    }
    let got = exec(&ex, "SELECT COUNT(*) FROM e").await;
    assert_eq!(scalar(&got[0]), &Value::Int64(0));
}
