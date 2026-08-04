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
        vec![Some(String::new()), Some("1|a".into()), Some("2|b".into()),]
    );
}

#[tokio::test]
async fn copy_from_stdin_text_reconstructs_rows() {
    let ex = test_executor();
    exec(
        &ex,
        "CREATE TABLE t (id INT PRIMARY KEY, name TEXT, qty INT)",
    )
    .await;
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
            vec![
                Value::Int32(1),
                Value::Text("alice".into()),
                Value::Int32(10)
            ],
            vec![Value::Int32(2), Value::Text("bob".into()), Value::Int32(20)],
            vec![
                Value::Int32(3),
                Value::Text("carol".into()),
                Value::Int32(30)
            ],
        ],
        "each payload line must become exactly one row with its fields in place"
    );
}

#[tokio::test]
async fn copy_from_stdin_text_honours_null_marker() {
    let ex = test_executor();
    // No PRIMARY KEY: `id` must be nullable for the `\N`-in-the-first-column
    // case below. (A NULL primary key is rejected — see
    // `copy_from_enforces_not_null`.)
    exec(&ex, "CREATE TABLE t (id INT, name TEXT, qty INT)").await;
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
    // The second row's empty field is the empty STRING, not NULL: in
    // PostgreSQL's text format only `\N` is NULL. (This used to land as NULL —
    // `parse_field` applied a blanket empty-string-is-NULL rule that belongs to
    // CSV, not text.)
    assert_eq!(
        rows(&got[0]),
        &vec![
            vec![Value::Int32(1), Value::Text("a b".into())],
            vec![Value::Int32(2), Value::Text(String::new())],
        ],
        "an embedded space must not split a field, and an empty trailing field \
         must not collapse the row"
    );
}

/// PostgreSQL's text format distinguishes an empty field from `\N`: the first is
/// the empty string, the second is NULL. Collapsing them (the old behaviour)
/// silently rewrites data on the way in and makes the two indistinguishable on
/// the way back out.
#[tokio::test]
async fn copy_from_text_empty_field_is_empty_string_not_null() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)").await;
    exec(&ex, "COPY t FROM STDIN;\n1\t\n2\t\\N\n\\.").await;

    let got = exec(&ex, "SELECT id, name FROM t ORDER BY id").await;
    assert_eq!(
        rows(&got[0]),
        &vec![
            vec![Value::Int32(1), Value::Text(String::new())],
            vec![Value::Int32(2), Value::Null],
        ],
        "an empty text-format field is '', only \\N is NULL"
    );

    // And the difference must be observable through SQL, not just by inspecting
    // the stored value.
    let got = exec(&ex, "SELECT COUNT(*) FROM t WHERE name IS NULL").await;
    assert_eq!(scalar(&got[0]), &Value::Int64(1));
    let got = exec(&ex, "SELECT COUNT(*) FROM t WHERE name = ''").await;
    assert_eq!(scalar(&got[0]), &Value::Int64(1));
}

/// CSV is the other way round: an unquoted empty field IS NULL (PostgreSQL's
/// default CSV NULL string is the empty string), while a quoted `""` is the
/// empty string.
#[tokio::test]
async fn copy_from_csv_distinguishes_empty_from_quoted_empty() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE c (id INT PRIMARY KEY, name TEXT)").await;
    exec(&ex, "COPY c FROM STDIN WITH (FORMAT CSV);\n1,\n2,\"\"\n\\.").await;

    let got = exec(&ex, "SELECT id, name FROM c ORDER BY id").await;
    assert_eq!(
        rows(&got[0]),
        &vec![
            vec![Value::Int32(1), Value::Null],
            vec![Value::Int32(2), Value::Text(String::new())],
        ],
        "an unquoted empty CSV field is NULL; a quoted one is ''"
    );
}

// ── Constraint enforcement (COPY is a bulk INSERT, not a bypass) ────────────

/// COPY is the loader most likely to ingest untrusted data, so it is the worst
/// possible place to skip validation. It used to write through a bare storage
/// append that never consulted NOT NULL or CHECK at all.
#[tokio::test]
async fn copy_from_enforces_not_null() {
    let ex = test_executor();
    exec(
        &ex,
        "CREATE TABLE n (id INT PRIMARY KEY, name TEXT NOT NULL)",
    )
    .await;

    let bad = ex.execute("COPY n FROM STDIN;\n1\t\\N\n\\.").await;
    assert!(bad.is_err(), "COPY accepted NULL in a NOT NULL column");

    let got = exec(&ex, "SELECT COUNT(*) FROM n").await;
    assert_eq!(scalar(&got[0]), &Value::Int64(0));
}

#[tokio::test]
async fn copy_from_enforces_check_constraints() {
    let ex = test_executor();
    exec(
        &ex,
        "CREATE TABLE ck (id INT PRIMARY KEY, qty INT CHECK (qty > 0))",
    )
    .await;

    let bad = ex.execute("COPY ck FROM STDIN;\n1\t-5\n\\.").await;
    assert!(
        bad.is_err(),
        "COPY accepted a row violating CHECK (qty > 0)"
    );

    let got = exec(&ex, "SELECT COUNT(*) FROM ck").await;
    assert_eq!(scalar(&got[0]), &Value::Int64(0));
}

/// PostgreSQL's COPY is all-or-nothing within the statement. A violation on the
/// third row must leave the table exactly as it was — not two rows heavier.
#[tokio::test]
async fn copy_from_is_atomic_across_the_whole_payload() {
    let ex = test_executor();
    exec(
        &ex,
        "CREATE TABLE a (id INT PRIMARY KEY, qty INT CHECK (qty > 0))",
    )
    .await;

    let bad = ex
        .execute("COPY a FROM STDIN;\n1\t10\n2\t20\n3\t-1\n4\t40\n\\.")
        .await;
    assert!(bad.is_err(), "the third row violates CHECK (qty > 0)");

    let got = exec(&ex, "SELECT COUNT(*) FROM a").await;
    assert_eq!(
        scalar(&got[0]),
        &Value::Int64(0),
        "a failed COPY must leave ZERO rows — rows 1 and 2 must not survive"
    );

    // The same holds when the failure is a unique violation inside the payload.
    let ex = test_executor();
    exec(&ex, "CREATE TABLE a (id INT PRIMARY KEY, qty INT)").await;
    let bad = ex
        .execute("COPY a FROM STDIN;\n1\t10\n2\t20\n1\t30\n\\.")
        .await;
    assert!(bad.is_err(), "id 1 appears twice in one payload");
    let got = exec(&ex, "SELECT COUNT(*) FROM a").await;
    assert_eq!(
        scalar(&got[0]),
        &Value::Int64(0),
        "a payload that duplicates a key inside itself must insert nothing"
    );

    // ...and when it clashes with a row already in the table.
    let ex = test_executor();
    exec(&ex, "CREATE TABLE a (id INT PRIMARY KEY, qty INT)").await;
    exec(&ex, "INSERT INTO a VALUES (9, 1)").await;
    let bad = ex
        .execute("COPY a FROM STDIN;\n1\t10\n2\t20\n9\t30\n\\.")
        .await;
    assert!(bad.is_err(), "id 9 already exists");
    let got = exec(&ex, "SELECT COUNT(*) FROM a").await;
    assert_eq!(
        scalar(&got[0]),
        &Value::Int64(1),
        "only the pre-existing row may remain"
    );
}

/// A COPY field must land in its column's declared type, exactly as the same
/// literal would through INSERT. The old `parse_field` only understood
/// INT/BIGINT/DOUBLE/BOOLEAN and left everything else as raw `Text`, so a DATE
/// column silently stored a string.
#[tokio::test]
async fn copy_from_coerces_fields_to_the_declared_column_type() {
    let ex = test_executor();
    exec(
        &ex,
        "CREATE TABLE ty (id INT PRIMARY KEY, d DATE, ts TIMESTAMP, flag BOOLEAN, tags TEXT[])",
    )
    .await;
    exec(
        &ex,
        "COPY ty FROM STDIN;\n1\t2024-03-26\t2024-03-26 12:34:56\tt\t{a,b}\n\\.",
    )
    .await;
    exec(
        &ex,
        "INSERT INTO ty VALUES (2, '2024-03-26', '2024-03-26 12:34:56', true, ARRAY['a','b'])",
    )
    .await;

    let copied = exec(&ex, "SELECT d, ts, flag, tags FROM ty WHERE id = 1").await;
    let inserted = exec(&ex, "SELECT d, ts, flag, tags FROM ty WHERE id = 2").await;
    assert_eq!(
        rows(&copied[0]),
        rows(&inserted[0]),
        "COPY and INSERT of the same literals must store identical values"
    );

    // A field that cannot be the declared type is an error, not a silent Text.
    let bad = ex
        .execute("COPY ty FROM STDIN;\n3\tnot-a-date\t\\N\t\\N\t\\N\n\\.")
        .await;
    assert!(bad.is_err(), "COPY stored an unparseable DATE as text");
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

/// COPY now shares the INSERT write path, so it inherits INSERT's guarantees —
/// and INSERT had a hole. `check_unique_constraints` runs per row against
/// *storage*, but a multi-row statement stages every row and writes them
/// afterwards, so rows in the same statement are invisible to one another. The
/// only backstop was `StorageEngine::insert_unique`, whose trait default is a
/// plain insert — so on any engine without atomic unique support (MemoryEngine,
/// which is what `nucleus start --memory` runs) a single statement could
/// silently store two rows with the same PRIMARY KEY.
#[tokio::test]
async fn one_statement_cannot_duplicate_a_primary_key() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE z (id INT PRIMARY KEY, q INT)").await;

    let bad = ex.execute("INSERT INTO z VALUES (1, 10), (1, 20)").await;
    assert!(
        bad.is_err(),
        "one INSERT statement stored the same primary key twice"
    );
    let got = exec(&ex, "SELECT COUNT(*) FROM z").await;
    assert_eq!(
        scalar(&got[0]),
        &Value::Int64(0),
        "the rejected statement must not leave its first row behind"
    );

    // Multiple NULLs are still allowed in a plain UNIQUE column.
    exec(&ex, "CREATE TABLE zu (id INT PRIMARY KEY, tag TEXT UNIQUE)").await;
    exec(&ex, "INSERT INTO zu VALUES (1, NULL), (2, NULL)").await;
    let got = exec(&ex, "SELECT COUNT(*) FROM zu").await;
    assert_eq!(scalar(&got[0]), &Value::Int64(2));
}
