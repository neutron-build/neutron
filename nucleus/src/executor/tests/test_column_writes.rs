//! X10: stored generated columns, identity columns, deferrable foreign keys,
//! `INSERT ... DEFAULT VALUES` and `varchar(n)` / `char(n)` length limits.
//! Expected results were taken from PostgreSQL 17.

use super::*;
use crate::wire::error_codec::{ErrorCodec, PgWireErrorCodec};

async fn sqlstate(ex: &Executor, sql: &str) -> String {
    let err = ex.execute(sql).await.expect_err(sql);
    let codec = PgWireErrorCodec;
    codec.code_to_string(codec.encode(&err).code)
}

async fn ints(ex: &Executor, sql: &str) -> Vec<Vec<i64>> {
    let results = exec(ex, sql).await;
    rows(&results[0])
        .iter()
        .map(|r| {
            r.iter()
                .map(|v| match v {
                    Value::Int32(n) => *n as i64,
                    Value::Int64(n) => *n,
                    other => panic!("expected int, got {other:?}"),
                })
                .collect()
        })
        .collect()
}

#[tokio::test]
async fn stored_generated_column_is_computed_on_insert_and_update() {
    let ex = test_executor();
    exec(
        &ex,
        "CREATE TABLE g (a INT NOT NULL, b INT GENERATED ALWAYS AS (a * 2) STORED)",
    )
    .await;
    exec(&ex, "INSERT INTO g (a) VALUES (3)").await;
    exec(&ex, "INSERT INTO g VALUES (4, DEFAULT)").await;
    assert_eq!(
        ints(&ex, "SELECT a, b FROM g ORDER BY a").await,
        [[3, 6], [4, 8]]
    );
    exec(&ex, "UPDATE g SET a = 10 WHERE a = 3").await;
    assert_eq!(
        ints(&ex, "SELECT a, b FROM g ORDER BY a").await,
        [[4, 8], [10, 20]]
    );
    exec(&ex, "UPDATE g SET b = DEFAULT WHERE a = 4").await;
    assert_eq!(ints(&ex, "SELECT b FROM g WHERE a = 4").await, [[8]]);
    let r = exec(&ex, "INSERT INTO g (a) VALUES (5) RETURNING b").await;
    assert_eq!(rows(&r[0])[0][0], Value::Int32(10));
    exec(&ex, "INSERT INTO g SELECT 6").await;
    assert_eq!(ints(&ex, "SELECT b FROM g WHERE a = 6").await, [[12]]);
}

#[tokio::test]
async fn generated_column_rejects_explicit_values_with_428c9() {
    let ex = test_executor();
    exec(
        &ex,
        "CREATE TABLE g (a INT NOT NULL, b INT GENERATED ALWAYS AS (a + 1) STORED)",
    )
    .await;
    exec(&ex, "INSERT INTO g (a) VALUES (1)").await;
    assert_eq!(
        sqlstate(&ex, "INSERT INTO g (a, b) VALUES (1, 2)").await,
        "428C9"
    );
    assert_eq!(sqlstate(&ex, "INSERT INTO g VALUES (1, 2)").await, "428C9");
    assert_eq!(sqlstate(&ex, "UPDATE g SET b = 1").await, "428C9");
    // Not even OVERRIDING SYSTEM VALUE may write a generated column.
    assert_eq!(
        sqlstate(
            &ex,
            "INSERT INTO g (a, b) OVERRIDING SYSTEM VALUE VALUES (1, 2)"
        )
        .await,
        "428C9"
    );
    assert_eq!(ints(&ex, "SELECT COUNT(*) FROM g").await, [[1]]);
}

#[tokio::test]
async fn generation_expression_rules() {
    let ex = test_executor();
    let err = ex
        .execute(
            "CREATE TABLE ge (a INT, b INT GENERATED ALWAYS AS (a + 1) STORED, \
             c INT GENERATED ALWAYS AS (b + 1) STORED)",
        )
        .await
        .expect_err("a generated column may not read another");
    assert!(err.to_string().contains("generated column"), "{err}");
    assert!(
        ex.execute("CREATE TABLE gn (a INT, b INT GENERATED ALWAYS AS (a + random()) STORED)")
            .await
            .is_err()
    );
    exec(
        &ex,
        "CREATE TABLE gd (a INT, b INT GENERATED ALWAYS AS (a + 1) STORED)",
    )
    .await;
    assert!(ex.execute("ALTER TABLE gd DROP COLUMN a").await.is_err());
    assert!(
        ex.execute("ALTER TABLE gd RENAME COLUMN a TO z")
            .await
            .is_err()
    );
}

#[tokio::test]
async fn add_column_generated_backfills_existing_rows() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE ag (a INT)").await;
    exec(&ex, "INSERT INTO ag VALUES (2), (3)").await;
    exec(
        &ex,
        "ALTER TABLE ag ADD COLUMN b INT GENERATED ALWAYS AS (a * 10) STORED",
    )
    .await;
    assert_eq!(
        ints(&ex, "SELECT a, b FROM ag ORDER BY a").await,
        [[2, 20], [3, 30]]
    );
}

#[tokio::test]
async fn identity_always_rejects_explicit_values_unless_overridden() {
    let ex = test_executor();
    exec(
        &ex,
        "CREATE TABLE i (id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, v TEXT)",
    )
    .await;
    let r = exec(&ex, "INSERT INTO i (v) VALUES ('a') RETURNING id").await;
    assert_eq!(rows(&r[0])[0][0], Value::Int32(1));
    assert_eq!(
        sqlstate(&ex, "INSERT INTO i (id, v) VALUES (50, 'b')").await,
        "428C9"
    );
    assert_eq!(
        sqlstate(&ex, "INSERT INTO i VALUES (50, 'b')").await,
        "428C9"
    );
    assert_eq!(sqlstate(&ex, "UPDATE i SET id = 9").await, "428C9");
    // DEFAULT is always allowed and draws from the sequence.
    let r = exec(
        &ex,
        "INSERT INTO i (id, v) VALUES (DEFAULT, 'c') RETURNING id",
    )
    .await;
    assert_eq!(rows(&r[0])[0][0], Value::Int32(2));
    exec(&ex, "UPDATE i SET id = DEFAULT WHERE id = 1").await;
    // OVERRIDING SYSTEM VALUE keeps the explicit value, USER VALUE discards it.
    let r = exec(
        &ex,
        "INSERT INTO i OVERRIDING SYSTEM VALUE VALUES (50, 'd') RETURNING id",
    )
    .await;
    assert_eq!(rows(&r[0])[0][0], Value::Int32(50));
    let r = exec(
        &ex,
        "INSERT INTO i (id, v) OVERRIDING USER VALUE VALUES (77, 'e') RETURNING id",
    )
    .await;
    assert_eq!(rows(&r[0])[0][0], Value::Int32(4));
}

#[tokio::test]
async fn identity_by_default_accepts_explicit_values() {
    let ex = test_executor();
    exec(
        &ex,
        "CREATE TABLE d (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, v TEXT)",
    )
    .await;
    exec(&ex, "INSERT INTO d VALUES (5, 'x')").await;
    let r = exec(&ex, "INSERT INTO d (v) VALUES ('y') RETURNING id").await;
    assert_eq!(rows(&r[0])[0][0], Value::Int32(1));
    let r = exec(
        &ex,
        "INSERT INTO d OVERRIDING USER VALUE VALUES (100, 'z') RETURNING id",
    )
    .await;
    assert_eq!(rows(&r[0])[0][0], Value::Int32(2));
    let r = exec(
        &ex,
        "INSERT INTO d OVERRIDING SYSTEM VALUE VALUES (101, 'w') RETURNING id",
    )
    .await;
    assert_eq!(rows(&r[0])[0][0], Value::Int32(101));
}

#[tokio::test]
async fn generation_is_reported_by_the_catalog() {
    let ex = test_executor();
    exec(
        &ex,
        "CREATE TABLE cg (id INT GENERATED ALWAYS AS IDENTITY, a INT, \
         b INT GENERATED ALWAYS AS (a * 2) STORED, s VARCHAR(3))",
    )
    .await;
    let r = exec(
        &ex,
        "SELECT column_name, column_default, is_generated, is_identity, identity_generation, \
         character_maximum_length FROM information_schema.columns WHERE table_name = 'cg' \
         ORDER BY ordinal_position",
    )
    .await;
    let r = rows(&r[0]);
    assert_eq!(
        r[0][1],
        Value::Null,
        "identity backing sequence is not a default"
    );
    assert_eq!(r[0][3], Value::Text("YES".into()));
    assert_eq!(r[0][4], Value::Text("ALWAYS".into()));
    assert_eq!(r[2][2], Value::Text("ALWAYS".into()));
    assert_eq!(r[3][5], Value::Int32(3));
    let r = exec(
        &ex,
        "SELECT attname, attidentity, attgenerated FROM pg_attribute WHERE attname IN ('id', 'b')",
    )
    .await;
    let flags: Vec<(String, String)> = rows(&r[0])
        .iter()
        .map(|r| (format!("{:?}", r[1]), format!("{:?}", r[2])))
        .collect();
    assert!(
        flags.contains(&("Text(\"a\")".into(), "Text(\"\")".into())),
        "{flags:?}"
    );
    assert!(
        flags.contains(&("Text(\"\")".into(), "Text(\"s\")".into())),
        "{flags:?}"
    );
}

#[tokio::test]
async fn varchar_and_char_overflow_is_22001() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE v (s VARCHAR(3), c CHAR(3), t TEXT)").await;
    assert_eq!(
        sqlstate(&ex, "INSERT INTO v (s) VALUES ('abcd')").await,
        "22001"
    );
    assert_eq!(
        sqlstate(&ex, "INSERT INTO v (c) VALUES ('abcd')").await,
        "22001"
    );
    // Trailing spaces past the limit are cut, not an error; other text is free.
    exec(
        &ex,
        "INSERT INTO v VALUES ('abc   ', 'x', 'a long unbounded text')",
    )
    .await;
    let r = exec(&ex, "SELECT s FROM v").await;
    assert_eq!(*scalar(&r[0]), Value::Text("abc".into()));
    exec(&ex, "INSERT INTO v (s) VALUES ('ab')").await;
    assert_eq!(sqlstate(&ex, "UPDATE v SET s = 'abcdef'").await, "22001");
    // Characters, not bytes.
    exec(&ex, "INSERT INTO v (s) VALUES ('\u{e9}\u{e9}\u{e9}')").await;
    assert_eq!(
        sqlstate(&ex, "INSERT INTO v (s) VALUES ('\u{e9}\u{e9}\u{e9}\u{e9}')").await,
        "22001"
    );
    // Existing data longer than the new limit blocks the ALTER.
    exec(&ex, "CREATE TABLE v2 (s TEXT)").await;
    exec(&ex, "INSERT INTO v2 VALUES ('abcdef')").await;
    assert!(
        ex.execute("ALTER TABLE v2 ALTER COLUMN s TYPE VARCHAR(3)")
            .await
            .is_err()
    );
    exec(&ex, "ALTER TABLE v2 ALTER COLUMN s TYPE VARCHAR(10)").await;
    assert_eq!(
        sqlstate(&ex, "INSERT INTO v2 VALUES ('01234567890')").await,
        "22001"
    );
}

#[tokio::test]
async fn insert_default_values() {
    let ex = test_executor();
    exec(
        &ex,
        "CREATE TABLE dv (id SERIAL PRIMARY KEY, t TEXT DEFAULT 'q', n INT)",
    )
    .await;
    let r = exec(&ex, "INSERT INTO dv DEFAULT VALUES RETURNING id, t, n").await;
    assert_eq!(
        rows(&r[0])[0],
        vec![Value::Int32(1), Value::Text("q".into()), Value::Null]
    );
    exec(&ex, "INSERT INTO dv DEFAULT VALUES").await;
    assert_eq!(ints(&ex, "SELECT COUNT(*) FROM dv").await, [[2]]);
}

async fn deferred_fk_tables(ex: &Executor, clause: &str) {
    exec(ex, "CREATE TABLE pa (id INT PRIMARY KEY)").await;
    exec(
        ex,
        &format!(
            "CREATE TABLE ch (id INT PRIMARY KEY, pa INT NOT NULL REFERENCES pa (id) {clause})"
        ),
    )
    .await;
}

#[tokio::test]
async fn deferred_foreign_key_is_checked_at_commit() {
    let ex = test_executor();
    deferred_fk_tables(&ex, "DEFERRABLE INITIALLY DEFERRED").await;
    // Child before parent inside one transaction: fine at COMMIT.
    exec(&ex, "BEGIN").await;
    exec(&ex, "INSERT INTO ch VALUES (1, 5)").await;
    exec(&ex, "INSERT INTO pa VALUES (5)").await;
    exec(&ex, "COMMIT").await;
    assert_eq!(ints(&ex, "SELECT COUNT(*) FROM ch").await, [[1]]);
    // A violation is reported at COMMIT, and the transaction is rolled back.
    exec(&ex, "BEGIN").await;
    exec(&ex, "INSERT INTO ch VALUES (2, 6)").await;
    assert_eq!(sqlstate(&ex, "COMMIT").await, "23503");
    assert_eq!(ints(&ex, "SELECT COUNT(*) FROM ch").await, [[1]]);
    // Autocommit statements are their own transaction: still immediate.
    assert_eq!(sqlstate(&ex, "INSERT INTO ch VALUES (3, 7)").await, "23503");
    // Repaired inside the transaction: no violation.
    exec(&ex, "BEGIN").await;
    exec(&ex, "INSERT INTO ch VALUES (4, 8)").await;
    exec(&ex, "INSERT INTO pa VALUES (8)").await;
    exec(&ex, "COMMIT").await;
    // Deleting a referenced parent is deferred the same way.
    exec(&ex, "BEGIN").await;
    exec(&ex, "DELETE FROM pa WHERE id = 8").await;
    exec(&ex, "DELETE FROM ch WHERE pa = 8").await;
    exec(&ex, "COMMIT").await;
    exec(&ex, "BEGIN").await;
    exec(&ex, "DELETE FROM pa WHERE id = 5").await;
    assert_eq!(sqlstate(&ex, "COMMIT").await, "23503");
    // Parent 8 was deleted with its child; failed COMMIT restores parent 5.
    assert_eq!(ints(&ex, "SELECT id FROM pa ORDER BY id").await, [[5]]);
    assert_eq!(
        ints(&ex, "SELECT id, pa FROM ch ORDER BY id").await,
        [[1, 5]]
    );
}

#[tokio::test]
async fn set_constraints_moves_the_check() {
    let ex = test_executor();
    deferred_fk_tables(&ex, "DEFERRABLE INITIALLY IMMEDIATE").await;
    // Initially immediate: the violation is refused at the statement.
    exec(&ex, "BEGIN").await;
    assert_eq!(sqlstate(&ex, "INSERT INTO ch VALUES (1, 5)").await, "23503");
    exec(&ex, "ROLLBACK").await;
    exec(&ex, "BEGIN").await;
    exec(&ex, "SET CONSTRAINTS ALL DEFERRED").await;
    exec(&ex, "INSERT INTO ch VALUES (1, 5)").await;
    exec(&ex, "INSERT INTO pa VALUES (5)").await;
    exec(&ex, "COMMIT").await;
    // SET CONSTRAINTS ... IMMEDIATE checks what is already pending.
    exec(&ex, "BEGIN").await;
    exec(&ex, "SET CONSTRAINTS ch_pa_fkey DEFERRED").await;
    exec(&ex, "INSERT INTO ch VALUES (2, 6)").await;
    assert_eq!(
        sqlstate(&ex, "SET CONSTRAINTS ALL IMMEDIATE").await,
        "23503"
    );
    exec(&ex, "ROLLBACK").await;
    // The setting ends with the transaction.
    exec(&ex, "BEGIN").await;
    assert_eq!(sqlstate(&ex, "INSERT INTO ch VALUES (3, 9)").await, "23503");
    exec(&ex, "ROLLBACK").await;
    // A constraint that is not deferrable cannot be deferred by name.
    exec(&ex, "CREATE TABLE ch2 (id INT, pa INT REFERENCES pa (id))").await;
    exec(&ex, "BEGIN").await;
    assert!(
        ex.execute("SET CONSTRAINTS ch2_pa_fkey DEFERRED")
            .await
            .is_err()
    );
    exec(&ex, "ROLLBACK").await;
}

#[tokio::test]
async fn deferrable_unique_is_refused_not_silently_immediate() {
    let ex = test_executor();
    let err = ex
        .execute("CREATE TABLE u (a INT UNIQUE DEFERRABLE INITIALLY DEFERRED)")
        .await
        .expect_err("deferrable UNIQUE");
    assert!(
        err.to_string().contains("FOREIGN KEY constraints only"),
        "{err}"
    );
    assert!(
        ex.execute("CREATE TABLE u2 (a INT, PRIMARY KEY (a) DEFERRABLE)")
            .await
            .is_err()
    );
}

#[tokio::test]
async fn generated_and_identity_columns_survive_a_logical_dump_restore() {
    let ex = test_executor();
    exec(
        &ex,
        "CREATE TABLE dg (id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, a INT, \
         b INT GENERATED ALWAYS AS (a * 2) STORED, s VARCHAR(4))",
    )
    .await;
    exec(&ex, "INSERT INTO dg (a, s) VALUES (1, 'x'), (2, 'y')").await;
    let dump = ex.dump_logical().await.expect("dump");
    let restored = test_executor();
    restored.restore_logical(&dump).await.expect("restore");
    assert_eq!(
        ints(&restored, "SELECT id, a, b FROM dg ORDER BY id").await,
        [[1, 1, 2], [2, 2, 4]]
    );
    // Identity stays ALWAYS, the sequence continues, the length limit holds.
    let r = exec(&restored, "INSERT INTO dg (a) VALUES (9) RETURNING id, b").await;
    assert_eq!(rows(&r[0])[0], vec![Value::Int32(3), Value::Int32(18)]);
    assert_eq!(
        sqlstate(&restored, "INSERT INTO dg (id, a) VALUES (10, 1)").await,
        "428C9"
    );
    assert_eq!(
        sqlstate(&restored, "INSERT INTO dg (a, s) VALUES (1, 'toolong')").await,
        "22001"
    );
}

#[tokio::test]
async fn x10_deferred_checks_survive_set_constraints_and_savepoint_rollback() {
    for repair_before_switch in [false, true] {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE parent (id int PRIMARY KEY)").await;
        exec(
            &ex,
            "CREATE TABLE child (id int REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)",
        )
        .await;
        exec(&ex, "BEGIN").await;
        exec(&ex, "INSERT INTO child VALUES (99)").await;
        exec(&ex, "SAVEPOINT s").await;
        if repair_before_switch {
            exec(&ex, "INSERT INTO parent VALUES (99)").await;
        }
        let result = ex.execute("SET CONSTRAINTS ALL IMMEDIATE").await;
        assert_eq!(result.is_ok(), repair_before_switch);
        exec(&ex, "ROLLBACK TO s").await;
        assert_eq!(sqlstate(&ex, "COMMIT").await, "23503");
        assert_eq!(ints(&ex, "SELECT count(*) FROM child").await, [[0]]);
    }
}

#[tokio::test]
async fn x10_constraint_modes_restore_at_savepoint() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE parent (id int PRIMARY KEY)").await;
    exec(
        &ex,
        "CREATE TABLE child (id int REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)",
    )
    .await;
    exec(&ex, "BEGIN").await;
    exec(&ex, "SAVEPOINT s").await;
    exec(&ex, "SET CONSTRAINTS ALL IMMEDIATE").await;
    exec(&ex, "ROLLBACK TO s").await;
    exec(&ex, "INSERT INTO child VALUES (99)").await;
    exec(&ex, "INSERT INTO parent VALUES (99)").await;
    exec(&ex, "COMMIT").await;
    assert_eq!(ints(&ex, "SELECT count(*) FROM child").await, [[1]]);
}

#[tokio::test]
async fn x10_schema_changes_cannot_bypass_pending_foreign_keys() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE parent (id int PRIMARY KEY)").await;
    exec(
        &ex,
        "CREATE TABLE child (id int REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)",
    )
    .await;
    exec(&ex, "BEGIN").await;
    exec(&ex, "INSERT INTO child VALUES (99)").await;
    assert_eq!(
        sqlstate(&ex, "ALTER TABLE child RENAME TO renamed").await,
        "0A000"
    );
    exec(&ex, "ROLLBACK").await;
    assert_eq!(ints(&ex, "SELECT count(*) FROM child").await, [[0]]);
    assert!(ex.catalog.get_table("renamed").await.is_none());
}

#[tokio::test]
async fn x10_generated_children_follow_foreign_key_actions() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE parent (id int PRIMARY KEY)").await;
    exec(&ex, "CREATE TABLE child (p int REFERENCES parent(id) ON UPDATE CASCADE ON DELETE SET NULL, g int GENERATED ALWAYS AS (p*2) STORED)").await;
    exec(&ex, "INSERT INTO parent VALUES (1)").await;
    exec(&ex, "INSERT INTO child(p) VALUES (1)").await;
    exec(&ex, "UPDATE parent SET id=2").await;
    assert_eq!(ints(&ex, "SELECT p,g FROM child").await, [[2, 4]]);
    exec(&ex, "DELETE FROM parent").await;
    let result = exec(&ex, "SELECT p,g FROM child").await;
    assert_eq!(rows(&result[0])[0], vec![Value::Null, Value::Null]);
}

#[tokio::test]
async fn x10_generated_checks_guard_cascades_before_any_write() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE parent (id int PRIMARY KEY)").await;
    exec(&ex, "CREATE TABLE child (p int REFERENCES parent(id) ON UPDATE CASCADE, g int GENERATED ALWAYS AS (p*2) STORED CHECK(g<4))").await;
    exec(&ex, "INSERT INTO parent VALUES (1)").await;
    exec(&ex, "INSERT INTO child(p) VALUES (1)").await;
    assert_eq!(sqlstate(&ex, "UPDATE parent SET id=2").await, "23514");
    assert_eq!(ints(&ex, "SELECT id FROM parent").await, [[1]]);
    assert_eq!(ints(&ex, "SELECT p,g FROM child").await, [[1, 2]]);
}

#[tokio::test]
async fn x10_generated_expressions_refuse_unverified_functions() {
    let ex = test_executor();
    exec(&ex, "CREATE SEQUENCE generated_side_effect_seq").await;
    exec(&ex, "CREATE FUNCTION generated_side_effect() RETURNS BIGINT LANGUAGE SQL AS $$ SELECT nextval('generated_side_effect_seq') $$").await;
    for (name, expr) in [
        ("udf", "generated_side_effect()"),
        ("pid", "pg_backend_pid()"),
        ("role", "current_role"),
        ("qualified", "pg_catalog.random()"),
        ("kv", "kv_incr('generated_side_effect_key')"),
    ] {
        let sql = format!(
            "CREATE TABLE generated_bad_{name} (a INT, b TEXT GENERATED ALWAYS AS ({expr}) STORED)"
        );
        assert!(ex.execute(&sql).await.is_err(), "accepted {sql}");
        assert!(
            ex.catalog
                .get_table(&format!("generated_bad_{name}"))
                .await
                .is_none()
        );
    }
    exec(&ex, "CREATE TABLE generated_good (a INT, s TEXT, b INT GENERATED ALWAYS AS (abs(a)) STORED, t TEXT GENERATED ALWAYS AS (upper(s)) STORED)").await;
    exec(&ex, "INSERT INTO generated_good(a,s) VALUES (-3,'ok')").await;
    assert_eq!(ints(&ex, "SELECT a,b FROM generated_good").await, [[-3, 3]]);
    assert_eq!(
        rows(&exec(&ex, "SELECT t FROM generated_good").await[0])[0],
        vec![Value::Text("OK".into())]
    );
}

#[tokio::test]
async fn x10_default_errors_are_not_replaced_with_null_or_wrapped_integers() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE default_zero (a INT DEFAULT (1/0))").await;
    assert_eq!(
        sqlstate(&ex, "INSERT INTO default_zero DEFAULT VALUES").await,
        "22012"
    );
    assert_eq!(ints(&ex, "SELECT count(*) FROM default_zero").await, [[0]]);
    exec(&ex, "CREATE TABLE default_wide (a INT DEFAULT 2147483648)").await;
    assert_eq!(
        sqlstate(&ex, "INSERT INTO default_wide DEFAULT VALUES").await,
        "22003"
    );
    assert_eq!(ints(&ex, "SELECT count(*) FROM default_wide").await, [[0]]);
}

#[tokio::test]
async fn x10_identity_dump_preserves_sequence_definition_and_position() {
    let ex = test_executor();
    exec(
        &ex,
        "CREATE TABLE identity_dump (id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY)",
    )
    .await;
    // Exercise the complete sequence definition, including the restart default.
    {
        let seqs = ex.sequences.read();
        let mut seq = seqs.get("identity_dump_id_seq").unwrap().lock();
        seq.increment = 3;
        seq.min_value = 2;
        seq.max_value = 200;
        seq.start = 11;
        seq.current = 8;
    }
    exec(&ex, "INSERT INTO identity_dump DEFAULT VALUES").await;
    let dump = ex.dump_logical().await.unwrap();
    let restored = test_executor();
    restored.restore_logical(&dump).await.unwrap();
    {
        let seqs = restored.sequences.read();
        let seq = seqs.get("identity_dump_id_seq").unwrap().lock();
        assert_eq!(
            (
                seq.increment,
                seq.min_value,
                seq.max_value,
                seq.start,
                seq.current
            ),
            (3, 2, 200, 11, 11)
        );
    }
    assert_eq!(
        ints(
            &restored,
            "INSERT INTO identity_dump DEFAULT VALUES RETURNING id"
        )
        .await,
        [[14]]
    );
    exec(&restored, "ALTER SEQUENCE identity_dump_id_seq RESTART").await;
    assert_eq!(
        ints(&restored, "SELECT nextval('identity_dump_id_seq')").await,
        [[11]]
    );
}

#[tokio::test]
async fn x10_cascade_duplicate_parent_batch_leaves_both_tables_unchanged() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE atomic_parent(id INT PRIMARY KEY)").await;
    exec(&ex, "CREATE TABLE atomic_child(p INT REFERENCES atomic_parent(id) ON UPDATE CASCADE, g INT GENERATED ALWAYS AS(p*2) STORED UNIQUE)").await;
    exec(&ex, "INSERT INTO atomic_parent VALUES (1),(2)").await;
    exec(&ex, "INSERT INTO atomic_child(p) VALUES (1),(2)").await;
    assert_eq!(
        sqlstate(&ex, "UPDATE atomic_parent SET id=3").await,
        "23505"
    );
    assert_eq!(
        ints(&ex, "SELECT id FROM atomic_parent ORDER BY id").await,
        [[1], [2]]
    );
    assert_eq!(
        ints(&ex, "SELECT p,g FROM atomic_child ORDER BY p").await,
        [[1, 2], [2, 4]]
    );
}
