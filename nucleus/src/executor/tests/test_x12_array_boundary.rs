use super::*;

fn column_type(result: &ExecResult) -> &DataType {
    match result {
        ExecResult::Select { columns, .. } => &columns[0].1,
        _ => panic!("expected SELECT"),
    }
}

#[tokio::test]
async fn catalog_constraint_refcols_keeps_nullable_array_type() {
    let e = test_executor();
    exec(&e, "CREATE TABLE x12_array_pk (id integer PRIMARY KEY)").await;
    let query = "SELECT (SELECT array_agg(ta.attname ORDER BY k.ord) FROM unnest(rc.confkey) WITH ORDINALITY AS k(attnum, ord) JOIN pg_attribute ta ON ta.attrelid = rc.confrelid AND ta.attnum = k.attnum) AS ref_cols FROM pg_constraint rc WHERE rc.contype = 'p'";
    let result = exec(&e, query).await;
    assert_eq!(
        column_type(&result[0]),
        &DataType::Array(Box::new(DataType::Text))
    );
    assert_eq!(rows(&result[0]), &vec![vec![Value::Null]]);
}

#[tokio::test]
async fn catalog_index_subqueries_describe_arrays_for_empty_and_populated_rows() {
    let e = test_executor();
    exec(&e, "CREATE TABLE x12_array_idx (id integer); CREATE INDEX x12_array_idx_id ON x12_array_idx (id)").await;
    for (expression, ty) in [
        ("oc.opcdefault", DataType::Bool),
        ("oc.opcname::text", DataType::Text),
    ] {
        for suffix in ["", " WHERE false", " LIMIT 0"] {
            let query = format!(
                "SELECT (SELECT array_agg({expression} ORDER BY k.ord) FROM unnest(i.indclass) WITH ORDINALITY AS k(opclass, ord) JOIN pg_opclass oc ON oc.oid = k.opclass) AS values FROM pg_index i{suffix}"
            );
            let result = exec(&e, &query).await;
            assert_eq!(
                column_type(&result[0]),
                &DataType::Array(Box::new(ty.clone()))
            );
            if suffix.is_empty() {
                assert_eq!(rows(&result[0]).len(), 1);
                assert!(matches!(rows(&result[0])[0][0], Value::Array(_)));
            } else {
                assert!(rows(&result[0]).is_empty());
            }
        }
    }
}

#[tokio::test]
async fn correlated_unnest_preserves_empty_all_null_and_null_arrays() {
    let e = test_executor();
    exec(&e, "CREATE TABLE x12_array_values (v integer[]); INSERT INTO x12_array_values VALUES (NULL), ('{}'::integer[]), ('{NULL,NULL}'::integer[])").await;
    let result = exec(
        &e,
        "SELECT (SELECT count(*) FROM unnest(o.v) AS k(v)) FROM x12_array_values o",
    )
    .await;
    assert_eq!(
        rows(&result[0]),
        &vec![
            vec![Value::Int64(0)],
            vec![Value::Int64(0)],
            vec![Value::Int64(2)]
        ]
    );
    assert!(
        e.execute("SELECT (SELECT count(*) FROM unnest(o.v) AS k(v)) FROM (SELECT NULL AS v) o")
            .await
            .is_err()
    );
}

#[tokio::test]
async fn subquery_type_uses_inner_alias_when_it_shadows_outer_alias() {
    let e = test_executor();
    exec(
        &e,
        "CREATE TABLE x12_outer_shadow (v integer); CREATE TABLE x12_inner_shadow (v boolean)",
    )
    .await;
    let result = exec(
        &e,
        "SELECT (SELECT array_agg(o.v) FROM x12_inner_shadow o) FROM x12_outer_shadow o LIMIT 0",
    )
    .await;
    assert_eq!(
        column_type(&result[0]),
        &DataType::Array(Box::new(DataType::Bool))
    );
}
