use super::*;

#[tokio::test]
async fn x12_dropped_declaration_does_not_attach_to_new_column() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE x12_reuse (a INT, b VARCHAR(20))").await;
    exec(&ex, "ALTER TABLE x12_reuse DROP COLUMN b").await;
    exec(&ex, "ALTER TABLE x12_reuse ADD COLUMN c TEXT").await;
    let result = exec(&ex, "SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid = 'x12_reuse'::regclass AND attname = 'c'").await;
    assert_eq!(scalar(&result[0]), &Value::Text("text".into()));
}

#[tokio::test]
async fn x12_independent_unique_index_retains_its_own_catalog_entry() {
    for missing_backing_index in [false, true] {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE x12_index (id INT PRIMARY KEY)").await;
        if missing_backing_index {
            // The catalog supports constraint metadata without a physical
            // index. Its synthetic backing entry must not consume a user's
            // independent index with the same columns.
            ex.catalog.drop_index("x12_index_pkey").await.unwrap();
        }
        exec(&ex, "CREATE UNIQUE INDEX x12_independent ON x12_index (id)").await;
        let result = exec(&ex, "SELECT relname FROM pg_class WHERE relname IN ('x12_index_pkey', 'x12_independent') ORDER BY relname").await;
        assert_eq!(
            rows(&result[0]),
            &vec![
                vec![Value::Text("x12_independent".into())],
                vec![Value::Text("x12_index_pkey".into())]
            ]
        );
    }
}

#[tokio::test]
async fn x12_array_declarations_preserve_element_typmods() {
    let ex = test_executor();
    exec(
        &ex,
        "CREATE TABLE x12_array_types (v VARCHAR(20)[], n NUMERIC(10,2)[])",
    )
    .await;
    let result = exec(&ex, "SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid = 'x12_array_types'::regclass AND attnum > 0 ORDER BY attnum").await;
    assert_eq!(
        rows(&result[0]),
        &vec![
            vec![Value::Text("character varying(20)[]".into())],
            vec![Value::Text("numeric(10,2)[]".into())]
        ]
    );
}

#[tokio::test]
async fn x12_check_literals_do_not_create_column_dependencies() {
    let ex = test_executor();
    exec(
        &ex,
        "CREATE TABLE x12_check (id INT, CONSTRAINT x12_literal CHECK ('id' <> ''))",
    )
    .await;
    let result = exec(
        &ex,
        "SELECT conkey FROM pg_constraint WHERE conname = 'x12_literal'",
    )
    .await;
    assert_eq!(scalar(&result[0]), &Value::Array(vec![]));
}

#[tokio::test]
async fn x12_column_privileges_answer_for_the_named_principal() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE x12_secret (id INT)").await;
    exec(&ex, "CREATE ROLE x12_reader LOGIN PASSWORD 'test-only'").await;
    let result = exec(
        &ex,
        "SELECT has_column_privilege('x12_reader', 'x12_secret', 'id', 'SELECT')",
    )
    .await;
    assert_eq!(scalar(&result[0]), &Value::Bool(false));
    exec(&ex, "GRANT SELECT ON x12_secret TO x12_reader").await;
    let result = exec(
        &ex,
        "SELECT has_column_privilege('x12_reader', 'x12_secret', 'id', 'SELECT')",
    )
    .await;
    assert_eq!(scalar(&result[0]), &Value::Bool(true));
}

#[tokio::test]
async fn x12_generation_metadata_matches_real_column_behavior() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE x12_generation (id BIGINT GENERATED ALWAYS AS IDENTITY, amount INT, doubled INT GENERATED ALWAYS AS (amount * 2) STORED, regular INT DEFAULT 7)").await;
    let result = exec(&ex, "SELECT attname, attidentity, attgenerated, atthasdef FROM pg_attribute WHERE attrelid = 'x12_generation'::regclass AND attnum > 0 ORDER BY attnum").await;
    assert_eq!(
        rows(&result[0]),
        &vec![
            vec![
                Value::Text("id".into()),
                Value::Text("a".into()),
                Value::Text("".into()),
                Value::Bool(false)
            ],
            vec![
                Value::Text("amount".into()),
                Value::Text("".into()),
                Value::Text("".into()),
                Value::Bool(false)
            ],
            vec![
                Value::Text("doubled".into()),
                Value::Text("".into()),
                Value::Text("s".into()),
                Value::Bool(true)
            ],
            vec![
                Value::Text("regular".into()),
                Value::Text("".into()),
                Value::Text("".into()),
                Value::Bool(true)
            ],
        ]
    );
    let result = exec(&ex, "SELECT column_name, is_generated, is_identity, identity_generation, column_default, generation_expression FROM information_schema.columns WHERE table_name = 'x12_generation' ORDER BY ordinal_position").await;
    let metadata = rows(&result[0]);
    assert_eq!(
        &metadata[0][1..],
        &[
            Value::Text("NEVER".into()),
            Value::Text("YES".into()),
            Value::Text("ALWAYS".into()),
            Value::Null,
            Value::Null
        ]
    );
    assert_eq!(metadata[2][1], Value::Text("ALWAYS".into()));
    assert_eq!(metadata[2][4], Value::Null);
    assert!(
        matches!(&metadata[2][5], Value::Text(expr) if expr.contains("amount") && expr.contains("* 2"))
    );
    let result = exec(
        &ex,
        "SELECT count(*) FROM pg_attrdef WHERE adrelid = 'x12_generation'::regclass",
    )
    .await;
    assert_eq!(scalar(&result[0]), &Value::Int64(2));
}

#[tokio::test]
async fn x12_foreign_key_modes_match_catalog_and_deparsed_definition() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE x12_fk_parent (id INT PRIMARY KEY)").await;
    for (name, clause, can_defer, starts_deferred, suffix) in [
        ("x12_fk_fixed", "NOT DEFERRABLE", false, false, ""),
        (
            "x12_fk_immediate",
            "DEFERRABLE INITIALLY IMMEDIATE",
            true,
            false,
            " DEFERRABLE",
        ),
        (
            "x12_fk_deferred",
            "DEFERRABLE INITIALLY DEFERRED",
            true,
            true,
            " DEFERRABLE INITIALLY DEFERRED",
        ),
    ] {
        exec(&ex, &format!("CREATE TABLE {name} (id INT, CONSTRAINT {name}_ref FOREIGN KEY (id) REFERENCES x12_fk_parent (id) {clause})")).await;
        let result = exec(&ex, &format!("SELECT condeferrable, condeferred, pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = '{name}_ref'")).await;
        assert_eq!(
            rows(&result[0]),
            &vec![vec![
                Value::Bool(can_defer),
                Value::Bool(starts_deferred),
                Value::Text(format!(
                    "FOREIGN KEY (id) REFERENCES x12_fk_parent(id){suffix}"
                )),
            ]]
        );
    }
}

#[tokio::test]
async fn x12_unsupported_index_ordering_is_refused_before_catalog_mutation() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE x12_ordering (id INT)").await;
    for (name, keys) in [
        ("x12_desc", "id DESC"),
        ("x12_null_first", "id NULLS FIRST"),
        ("x12_null_last", "id ASC NULLS LAST"),
    ] {
        let error = ex
            .execute(&format!("CREATE INDEX {name} ON x12_ordering ({keys})"))
            .await
            .unwrap_err();
        assert!(matches!(error, ExecError::Unsupported(_)), "{error:?}");
        let result = exec(
            &ex,
            &format!("SELECT count(*) FROM pg_class WHERE relname = '{name}'"),
        )
        .await;
        assert_eq!(scalar(&result[0]), &Value::Int64(0));
    }
    exec(&ex, "CREATE INDEX x12_ascending ON x12_ordering (id ASC)").await;
    let result = exec(
        &ex,
        "SELECT pg_get_indexdef(oid) FROM pg_class WHERE relname = 'x12_ascending'",
    )
    .await;
    assert_eq!(
        scalar(&result[0]),
        &Value::Text("CREATE INDEX x12_ascending ON public.x12_ordering USING btree (id)".into())
    );
}

#[tokio::test]
async fn x12_unnest_ordinality_has_bigint_position_and_preserves_nulls() {
    let ex = test_executor();
    let result = exec(&ex, "SELECT value, position FROM UNNEST(ARRAY[4,NULL,9]) WITH ORDINALITY AS k(value,position) ORDER BY position").await;
    assert_eq!(
        rows(&result[0]),
        &vec![
            vec![Value::Int32(4), Value::Int64(1)],
            vec![Value::Null, Value::Int64(2)],
            vec![Value::Int32(9), Value::Int64(3)]
        ]
    );
    let result = exec(
        &ex,
        "SELECT * FROM UNNEST(ARRAY[1,2], ARRAY[7]) WITH ORDINALITY AS k(a,b,n) ORDER BY n",
    )
    .await;
    assert_eq!(
        rows(&result[0]),
        &vec![
            vec![Value::Int32(1), Value::Int32(7), Value::Int64(1)],
            vec![Value::Int32(2), Value::Null, Value::Int64(2)]
        ]
    );
}

#[tokio::test]
async fn x12_catalog_ordinality_and_studio_lateral_queries_run() {
    let ex = test_executor();
    exec(
        &ex,
        "CREATE TABLE x12_ordinal (id INT PRIMARY KEY, amount INT)",
    )
    .await;
    let result = exec(&ex, "SELECT c.conname, (SELECT array_agg(k.attnum ORDER BY k.ord) FROM unnest(c.conkey) WITH ORDINALITY AS k(attnum,ord)) FROM pg_constraint c WHERE c.conrelid='x12_ordinal'::regclass").await;
    assert_eq!(
        rows(&result[0]),
        &vec![vec![
            Value::Text("x12_ordinal_pkey".into()),
            Value::Array(vec![Value::Int32(1)])
        ]]
    );
    let result = exec(&ex, "SELECT a.attname, COALESCE((SELECT u.ord FROM pg_catalog.pg_index i CROSS JOIN LATERAL pg_catalog.unnest(i.indkey) WITH ORDINALITY AS u(k,ord) WHERE i.indrelid=c.oid AND i.indisprimary AND i.indisvalid AND u.k=a.attnum),0) AS key_pos FROM pg_catalog.pg_class c JOIN pg_catalog.pg_attribute a ON a.attrelid=c.oid AND a.attnum>0 WHERE c.relname='x12_ordinal' ORDER BY a.attnum").await;
    assert_eq!(
        rows(&result[0]),
        &vec![
            vec![Value::Text("id".into()), Value::Int64(1)],
            vec![Value::Text("amount".into()), Value::Int32(0)]
        ]
    );
}
