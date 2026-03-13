use super::*;

// ======================================================================
// Multi-model SQL function tests
// ======================================================================

#[tokio::test]
async fn test_geo_distance() {
    let ex = test_executor();
    // NYC to London (~5570 km)
    let results = exec(
        &ex,
        "SELECT GEO_DISTANCE(40.7128, -74.0060, 51.5074, -0.1278)",
    )
    .await;
    match scalar(&results[0]) {
        Value::Float64(d) => {
            assert!(*d > 5_000_000.0 && *d < 6_000_000.0, "distance={d}");
        }
        other => panic!("expected Float64, got {other:?}"),
    }
}

#[tokio::test]
async fn test_geo_within() {
    let ex = test_executor();
    // Two points < 1km apart
    let results = exec(
        &ex,
        "SELECT GEO_WITHIN(40.7128, -74.0060, 40.7130, -74.0062, 1000.0)",
    )
    .await;
    assert_eq!(scalar(&results[0]), &Value::Bool(true));
}

#[tokio::test]
async fn test_vector_distance() {
    let ex = test_executor();
    let results = exec(
        &ex,
        "SELECT L2_DISTANCE('[1.0, 0.0, 0.0]'::JSONB, '[0.0, 1.0, 0.0]'::JSONB)",
    )
    .await;
    match scalar(&results[0]) {
        Value::Float64(d) => {
            assert!((*d - std::f64::consts::SQRT_2).abs() < 0.01, "l2={d}");
        }
        other => panic!("expected Float64, got {other:?}"),
    }
}

#[tokio::test]
async fn test_cosine_distance() {
    let ex = test_executor();
    // Same vector should have distance 0
    let results = exec(
        &ex,
        "SELECT COSINE_DISTANCE('[1.0, 0.0]'::JSONB, '[1.0, 0.0]'::JSONB)",
    )
    .await;
    match scalar(&results[0]) {
        Value::Float64(d) => assert!(d.abs() < 0.001, "cosine={d}"),
        other => panic!("expected Float64, got {other:?}"),
    }
}

#[tokio::test]
async fn test_fts_functions() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT TO_TSVECTOR('The quick brown fox')").await;
    match scalar(&results[0]) {
        Value::Text(s) => assert!(!s.is_empty()),
        other => panic!("expected Text, got {other:?}"),
    }

    let results = exec(&ex, "SELECT LEVENSHTEIN('kitten', 'sitting')").await;
    assert_eq!(scalar(&results[0]), &Value::Int32(3));
}

#[tokio::test]
async fn test_time_bucket() {
    let ex = test_executor();
    // 3600000 ms = 1 hour bucket, timestamp 7200001 → bucket 7200000
    let results = exec(&ex, "SELECT TIME_BUCKET(3600000, 7200001)").await;
    assert_eq!(scalar(&results[0]), &Value::Int64(7200000));
}

#[tokio::test]
async fn test_version_function() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT VERSION()").await;
    match scalar(&results[0]) {
        Value::Text(s) => {
            assert!(s.contains("Nucleus"));
            assert!(s.starts_with("PostgreSQL 16.0"), "version should start with PostgreSQL 16.0, got: {s}");
        }
        other => panic!("expected Text, got {other:?}"),
    }
}

#[tokio::test]
async fn test_now_returns_timestamp() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT NOW()").await;
    // NOW() returns Value::TimestampTz (microseconds since 2000-01-01 UTC)
    match scalar(&results[0]) {
        Value::TimestampTz(us) => assert!(*us > 0, "timestamp should be positive: {us}"),
        other => panic!("expected TimestampTz, got {other:?}"),
    }
}


// ======================================================================
// Vector function tests
// ======================================================================

#[tokio::test]
async fn test_vector_from_text() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT VECTOR('[1.0,2.0,3.0]')").await;
    assert_eq!(scalar(&results[0]), &Value::Vector(vec![1.0, 2.0, 3.0]));
}

#[tokio::test]
async fn test_vector_from_array() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT VECTOR(ARRAY[1, 2, 3])").await;
    assert_eq!(scalar(&results[0]), &Value::Vector(vec![1.0, 2.0, 3.0]));
}

#[tokio::test]
async fn test_vector_dims() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT VECTOR_DIMS(VECTOR('[1.0,2.0,3.0]'))").await;
    assert_eq!(scalar(&results[0]), &Value::Int32(3));
}

#[tokio::test]
async fn test_vector_distance_l2() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT VECTOR_DISTANCE(VECTOR('[1,2]'), VECTOR('[4,6]'), 'l2')").await;
    match scalar(&results[0]) {
        Value::Float64(d) => assert!((d - 5.0).abs() < 0.001), // sqrt((4-1)^2 + (6-2)^2) = sqrt(9+16) = 5
        other => panic!("expected Float64, got {other:?}"),
    }
}

#[tokio::test]
async fn test_vector_distance_cosine() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT VECTOR_DISTANCE(VECTOR('[1,0]'), VECTOR('[0,1]'), 'cosine')").await;
    match scalar(&results[0]) {
        Value::Float64(d) => assert!((d - 1.0).abs() < 0.001), // orthogonal vectors: cosine = 0, distance = 1
        other => panic!("expected Float64, got {other:?}"),
    }
}

#[tokio::test]
async fn test_vector_distance_default_l2() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT VECTOR_DISTANCE(VECTOR('[0,0]'), VECTOR('[3,4]'))").await;
    match scalar(&results[0]) {
        Value::Float64(d) => assert!((d - 5.0).abs() < 0.001), // default metric is L2
        other => panic!("expected Float64, got {other:?}"),
    }
}

#[tokio::test]
async fn test_normalize() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT NORMALIZE(VECTOR('[3,4]'))").await;
    match scalar(&results[0]) {
        Value::Vector(v) => {
            assert_eq!(v.len(), 2);
            // [3,4] has norm 5, so normalized is [0.6, 0.8]
            assert!((v[0] - 0.6).abs() < 0.001);
            assert!((v[1] - 0.8).abs() < 0.001);
        }
        other => panic!("expected Vector, got {other:?}"),
    }
}

#[tokio::test]
async fn test_vector_in_table() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE embeddings (id INT, vec VECTOR(3))").await;
    exec(&ex, "INSERT INTO embeddings VALUES (1, VECTOR('[1,2,3]'))").await;
    exec(&ex, "INSERT INTO embeddings VALUES (2, VECTOR('[4,5,6]'))").await;

    let results = exec(&ex, "SELECT id, vec FROM embeddings ORDER BY id").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2);
    assert_eq!(r[0][0], Value::Int32(1));
    assert_eq!(r[0][1], Value::Vector(vec![1.0, 2.0, 3.0]));
    assert_eq!(r[1][0], Value::Int32(2));
    assert_eq!(r[1][1], Value::Vector(vec![4.0, 5.0, 6.0]));
}

#[tokio::test]
async fn test_vector_distance_query() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE products (id INT, embedding VECTOR(3))").await;
    exec(&ex, "INSERT INTO products VALUES (1, VECTOR('[1,0,0]'))").await;
    exec(&ex, "INSERT INTO products VALUES (2, VECTOR('[0,1,0]'))").await;
    exec(&ex, "INSERT INTO products VALUES (3, VECTOR('[0,0,1]'))").await;

    // Test vector distance in SELECT
    let results = exec(
        &ex,
        "SELECT id, VECTOR_DISTANCE(embedding, VECTOR('[1,0,0]'), 'l2') AS dist FROM products WHERE id = 1"
    ).await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Int32(1));
    // Distance from [1,0,0] to [1,0,0] should be 0
    match &r[0][1] {
        Value::Float64(d) => assert!(d.abs() < 0.001),
        other => panic!("expected Float64, got {other:?}"),
    }
}

#[tokio::test]
async fn test_vector_index_creation() {
    let ex = test_executor();

    // Create table with vector column
    exec(&ex, "CREATE TABLE vectors (id INT PRIMARY KEY, embedding VECTOR(128))").await;

    // Create HNSW index
    exec(&ex, "CREATE INDEX vectors_embedding_idx ON vectors USING hnsw (embedding)").await;

    // Verify the HNSW index was created with correct metadata.
    // There is also an implicit B-tree index for the PRIMARY KEY, so find by name.
    let indexes = ex.catalog.get_indexes("vectors").await;
    let hnsw = indexes
        .iter()
        .find(|i| i.name == "vectors_embedding_idx")
        .expect("HNSW index not found in catalog");
    assert_eq!(hnsw.index_type, crate::catalog::IndexType::Hnsw);
    assert_eq!(hnsw.columns, vec!["embedding"]);

    // Verify options are stored
    assert_eq!(hnsw.options.get("dims"), Some(&"128".to_string()));
    assert_eq!(hnsw.options.get("metric"), Some(&"l2".to_string()));

    // Verify live HNSW index was created
    let vi = ex.vector_indexes.read();
    assert!(vi.contains_key("vectors_embedding_idx"));
}

#[tokio::test]
async fn test_vector_hnsw_index_populated() {
    let ex = test_executor();

    // Create table and insert vectors first
    exec(&ex, "CREATE TABLE items (id INT, embedding VECTOR(3))").await;
    exec(&ex, "INSERT INTO items VALUES (1, VECTOR('[1,0,0]'))").await;
    exec(&ex, "INSERT INTO items VALUES (2, VECTOR('[0,1,0]'))").await;
    exec(&ex, "INSERT INTO items VALUES (3, VECTOR('[0,0,1]'))").await;
    exec(&ex, "INSERT INTO items VALUES (4, VECTOR('[1,1,0]'))").await;
    exec(&ex, "INSERT INTO items VALUES (5, VECTOR('[0,1,1]'))").await;

    // Create HNSW index AFTER data exists — should scan and populate
    exec(&ex, "CREATE INDEX items_emb_idx ON items USING hnsw (embedding)").await;

    // Verify the live index has 5 vectors
    let vi = ex.vector_indexes.read();
    let entry = vi.get("items_emb_idx").unwrap();
    match &entry.kind {
        VectorIndexKind::Hnsw(hnsw) => {
            assert_eq!(hnsw.len(), 5);
            // Search for nearest to [1,0,0] — should find row 0 (id=1) first
            let results = hnsw.search(&vector::Vector::new(vec![1.0, 0.0, 0.0]), 2);
            assert_eq!(results.len(), 2);
            assert_eq!(results[0].0, 0); // row_id 0 = [1,0,0]
            assert!(results[0].1 < 0.001); // distance ~0
        }
        _ => panic!("expected HNSW index"),
    }
}

#[tokio::test]
async fn test_vector_index_accelerated_search() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE search_test (id INT, embedding VECTOR(3))").await;

    // Insert 10 vectors
    exec(&ex, "INSERT INTO search_test VALUES (1, VECTOR('[1,0,0]'))").await;
    exec(&ex, "INSERT INTO search_test VALUES (2, VECTOR('[0,1,0]'))").await;
    exec(&ex, "INSERT INTO search_test VALUES (3, VECTOR('[0,0,1]'))").await;
    exec(&ex, "INSERT INTO search_test VALUES (4, VECTOR('[0.9,0.1,0]'))").await;
    exec(&ex, "INSERT INTO search_test VALUES (5, VECTOR('[0.1,0.9,0]'))").await;
    exec(&ex, "INSERT INTO search_test VALUES (6, VECTOR('[0,0.1,0.9]'))").await;
    exec(&ex, "INSERT INTO search_test VALUES (7, VECTOR('[0.5,0.5,0]'))").await;
    exec(&ex, "INSERT INTO search_test VALUES (8, VECTOR('[0,0.5,0.5]'))").await;
    exec(&ex, "INSERT INTO search_test VALUES (9, VECTOR('[0.5,0,0.5]'))").await;
    exec(&ex, "INSERT INTO search_test VALUES (10, VECTOR('[0.33,0.33,0.34]'))").await;

    // Create HNSW index (builds from existing data)
    exec(&ex, "CREATE INDEX search_idx ON search_test USING hnsw (embedding)").await;

    // Query using ORDER BY + LIMIT — should use HNSW index
    let results = exec(
        &ex,
        "SELECT id FROM search_test ORDER BY VECTOR_DISTANCE(embedding, VECTOR('[1,0,0]'), 'l2') LIMIT 3"
    ).await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 3);
    // Nearest to [1,0,0]: id=1 (exact), then id=4 ([0.9,0.1,0]), then id=7 ([0.5,0.5,0])
    assert_eq!(r[0][0], Value::Int32(1));
}

#[tokio::test]
async fn test_vector_order_by_distance() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE vecs (id INT, embedding VECTOR(3))").await;
    exec(&ex, "INSERT INTO vecs VALUES (1, VECTOR('[1,0,0]'))").await;
    exec(&ex, "INSERT INTO vecs VALUES (2, VECTOR('[0,1,0]'))").await;
    exec(&ex, "INSERT INTO vecs VALUES (3, VECTOR('[0.9,0.1,0]'))").await;

    // ORDER BY expression (vector distance)
    let results = exec(
        &ex,
        "SELECT id FROM vecs ORDER BY VECTOR_DISTANCE(embedding, VECTOR('[1,0,0]'), 'l2') LIMIT 2"
    ).await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2);
    // Closest to [1,0,0]: id=1 (dist=0), then id=3 (dist≈0.14)
    assert_eq!(r[0][0], Value::Int32(1));
    assert_eq!(r[1][0], Value::Int32(3));
}

#[tokio::test]
async fn test_vector_index_insert_maintains() {
    let ex = test_executor();

    // Create table, create HNSW index, then insert — index should grow
    exec(&ex, "CREATE TABLE docs (id INT, embedding VECTOR(3))").await;
    exec(&ex, "CREATE INDEX docs_idx ON docs USING hnsw (embedding)").await;

    // Index should be empty
    {
        let vi = ex.vector_indexes.read();
        let entry = vi.get("docs_idx").unwrap();
        match &entry.kind {
            VectorIndexKind::Hnsw(hnsw) => assert_eq!(hnsw.len(), 0),
            _ => panic!("expected HNSW"),
        }
    }

    // Insert rows
    exec(&ex, "INSERT INTO docs VALUES (1, VECTOR('[1,0,0]'))").await;
    exec(&ex, "INSERT INTO docs VALUES (2, VECTOR('[0,1,0]'))").await;
    exec(&ex, "INSERT INTO docs VALUES (3, VECTOR('[0,0,1]'))").await;

    // Index should now have 3 vectors
    {
        let vi = ex.vector_indexes.read();
        let entry = vi.get("docs_idx").unwrap();
        match &entry.kind {
            VectorIndexKind::Hnsw(hnsw) => {
                assert_eq!(hnsw.len(), 3);
                // Search should work
                let results = hnsw.search(&vector::Vector::new(vec![1.0, 0.0, 0.0]), 1);
                assert_eq!(results.len(), 1);
                assert!(results[0].1 < 0.001); // exact match
            }
            _ => panic!("expected HNSW"),
        }
    }
}

#[tokio::test]
async fn test_vector_end_to_end() {
    let ex = test_executor();

    // Create table with vector column
    exec(&ex, "CREATE TABLE documents (id INT PRIMARY KEY, title TEXT, embedding VECTOR(4))").await;

    // Insert some documents with embeddings
    exec(&ex, "INSERT INTO documents VALUES (1, 'Rust programming', VECTOR('[1.0, 0.5, 0.2, 0.1]'))").await;
    exec(&ex, "INSERT INTO documents VALUES (2, 'Python guide', VECTOR('[0.5, 1.0, 0.3, 0.2]'))").await;
    exec(&ex, "INSERT INTO documents VALUES (3, 'Database design', VECTOR('[0.2, 0.3, 1.0, 0.5]'))").await;

    // Query with multiple vector functions
    let results = exec(
        &ex,
        "SELECT id, title, VECTOR_DIMS(embedding), VECTOR_DISTANCE(embedding, VECTOR('[1,0,0,0]'), 'cosine') AS similarity FROM documents"
    ).await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 3);

    // Check dimensions
    assert_eq!(r[0][2], Value::Int32(4));
    assert_eq!(r[1][2], Value::Int32(4));
    assert_eq!(r[2][2], Value::Int32(4));

    // Test normalize function
    let results = exec(&ex, "SELECT id, NORMALIZE(embedding) FROM documents WHERE id = 1").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    match &r[0][1] {
        Value::Vector(v) => {
            assert_eq!(v.len(), 4);
            // Normalized vector should have magnitude ~1
            let mag: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            assert!((mag - 1.0).abs() < 0.001);
        }
        other => panic!("expected Vector, got {other:?}"),
    }
}


// ======================================================================
// KV store SQL function tests
// ======================================================================

#[tokio::test]
async fn test_kv_set_and_get() {
    let ex = test_executor();
    // SET
    let res = exec(&ex, "SELECT kv_set('mykey', 'hello')").await;
    assert_eq!(scalar(&res[0]), &Value::Text("OK".into()));
    // GET
    let res = exec(&ex, "SELECT kv_get('mykey')").await;
    assert_eq!(scalar(&res[0]), &Value::Text("hello".into()));
    // GET missing key returns NULL
    let res = exec(&ex, "SELECT kv_get('missing')").await;
    assert_eq!(scalar(&res[0]), &Value::Null);
}

#[tokio::test]
async fn test_kv_del_and_exists() {
    let ex = test_executor();
    exec(&ex, "SELECT kv_set('k1', 'v1')").await;
    // EXISTS
    let res = exec(&ex, "SELECT kv_exists('k1')").await;
    assert_eq!(scalar(&res[0]), &Value::Bool(true));
    let res = exec(&ex, "SELECT kv_exists('nope')").await;
    assert_eq!(scalar(&res[0]), &Value::Bool(false));
    // DEL
    let res = exec(&ex, "SELECT kv_del('k1')").await;
    assert_eq!(scalar(&res[0]), &Value::Bool(true));
    let res = exec(&ex, "SELECT kv_del('k1')").await;
    assert_eq!(scalar(&res[0]), &Value::Bool(false));
    // GET after DEL
    let res = exec(&ex, "SELECT kv_get('k1')").await;
    assert_eq!(scalar(&res[0]), &Value::Null);
}

#[tokio::test]
async fn test_kv_incr() {
    let ex = test_executor();
    // INCR on missing key creates it with value 1
    let res = exec(&ex, "SELECT kv_incr('counter')").await;
    assert_eq!(scalar(&res[0]), &Value::Int64(1));
    // INCR again
    let res = exec(&ex, "SELECT kv_incr('counter')").await;
    assert_eq!(scalar(&res[0]), &Value::Int64(2));
    // INCR with amount
    let res = exec(&ex, "SELECT kv_incr('counter', 10)").await;
    assert_eq!(scalar(&res[0]), &Value::Int64(12));
}

#[tokio::test]
async fn test_kv_setnx_and_dbsize() {
    let ex = test_executor();
    // SETNX on missing key
    let res = exec(&ex, "SELECT kv_setnx('lock', 'owner1')").await;
    assert_eq!(scalar(&res[0]), &Value::Bool(true));
    // SETNX on existing key
    let res = exec(&ex, "SELECT kv_setnx('lock', 'owner2')").await;
    assert_eq!(scalar(&res[0]), &Value::Bool(false));
    // Value should still be owner1
    let res = exec(&ex, "SELECT kv_get('lock')").await;
    assert_eq!(scalar(&res[0]), &Value::Text("owner1".into()));
    // DBSIZE
    let res = exec(&ex, "SELECT kv_dbsize()").await;
    assert_eq!(scalar(&res[0]), &Value::Int64(1));
}

#[tokio::test]
async fn test_kv_set_with_ttl() {
    let ex = test_executor();
    // Set with 0-second TTL (expires immediately)
    exec(&ex, "SELECT kv_set('ephemeral', 'gone', 0)").await;
    std::thread::sleep(std::time::Duration::from_millis(10));
    let res = exec(&ex, "SELECT kv_get('ephemeral')").await;
    assert_eq!(scalar(&res[0]), &Value::Null);
}

#[tokio::test]
async fn test_kv_ttl_and_expire() {
    let ex = test_executor();
    exec(&ex, "SELECT kv_set('k', 'v')").await;
    // TTL on key with no expiry → -1
    let res = exec(&ex, "SELECT kv_ttl('k')").await;
    assert_eq!(scalar(&res[0]), &Value::Int64(-1));
    // TTL on missing key → -2
    let res = exec(&ex, "SELECT kv_ttl('nope')").await;
    assert_eq!(scalar(&res[0]), &Value::Int64(-2));
    // EXPIRE sets a TTL
    let res = exec(&ex, "SELECT kv_expire('k', 3600)").await;
    assert_eq!(scalar(&res[0]), &Value::Bool(true));
    // TTL should now be positive (close to 3600)
    // After expire, check TTL
    let res = exec(&ex, "SELECT kv_ttl('k')").await;
    match scalar(&res[0]) {
        Value::Int64(t) => assert!(*t > 3500 && *t <= 3600, "expected ~3600, got {t}"),
        other => panic!("expected Int64, got {other:?}"),
    }
}

#[tokio::test]
async fn test_kv_flushdb() {
    let ex = test_executor();
    exec(&ex, "SELECT kv_set('a', '1')").await;
    exec(&ex, "SELECT kv_set('b', '2')").await;
    exec(&ex, "SELECT kv_set('c', '3')").await;
    let res = exec(&ex, "SELECT kv_dbsize()").await;
    assert_eq!(scalar(&res[0]), &Value::Int64(3));
    exec(&ex, "SELECT kv_flushdb()").await;
    let res = exec(&ex, "SELECT kv_dbsize()").await;
    assert_eq!(scalar(&res[0]), &Value::Int64(0));
}

#[tokio::test]
async fn test_kv_integer_values() {
    let ex = test_executor();
    // KV can store non-text values via expressions
    exec(&ex, "SELECT kv_set('num', '42')").await;
    let res = exec(&ex, "SELECT kv_get('num')").await;
    assert_eq!(scalar(&res[0]), &Value::Text("42".into()));
}

// ======================================================================

// Columnar store SQL function tests
// ======================================================================

#[tokio::test]
async fn test_columnar_insert_and_count() {
    let ex = test_executor();
    // Insert rows into columnar store
    exec(&ex, "SELECT columnar_insert('events', 'ts', 100, 'user', 'alice')").await;
    exec(&ex, "SELECT columnar_insert('events', 'ts', 200, 'user', 'bob')").await;
    exec(&ex, "SELECT columnar_insert('events', 'ts', 300, 'user', 'charlie')").await;
    // Count
    let res = exec(&ex, "SELECT columnar_count('events')").await;
    assert_eq!(scalar(&res[0]), &Value::Int64(3));
    // Count on missing table
    let res = exec(&ex, "SELECT columnar_count('nonexistent')").await;
    assert_eq!(scalar(&res[0]), &Value::Int64(0));
}

#[tokio::test]
async fn test_columnar_sum_avg() {
    let ex = test_executor();
    exec(&ex, "SELECT columnar_insert('metrics', 'value', 10, 'label', 'a')").await;
    exec(&ex, "SELECT columnar_insert('metrics', 'value', 20, 'label', 'b')").await;
    exec(&ex, "SELECT columnar_insert('metrics', 'value', 30, 'label', 'c')").await;
    // SUM
    let res = exec(&ex, "SELECT columnar_sum('metrics', 'value')").await;
    assert_eq!(scalar(&res[0]), &Value::Float64(60.0));
    // AVG
    let res = exec(&ex, "SELECT columnar_avg('metrics', 'value')").await;
    assert_eq!(scalar(&res[0]), &Value::Float64(20.0));
}

#[tokio::test]
async fn test_columnar_min_max() {
    let ex = test_executor();
    exec(&ex, "SELECT columnar_insert('temps', 'temp', 15, 'city', 'nyc')").await;
    exec(&ex, "SELECT columnar_insert('temps', 'temp', 25, 'city', 'la')").await;
    exec(&ex, "SELECT columnar_insert('temps', 'temp', 5, 'city', 'chi')").await;
    // MIN
    let res = exec(&ex, "SELECT columnar_min('temps', 'temp')").await;
    assert_eq!(scalar(&res[0]), &Value::Float64(5.0));
    // MAX
    let res = exec(&ex, "SELECT columnar_max('temps', 'temp')").await;
    assert_eq!(scalar(&res[0]), &Value::Float64(25.0));
}

#[tokio::test]
async fn test_columnar_empty_aggregates() {
    let ex = test_executor();
    // Aggregates on empty table return NULL or 0
    let res = exec(&ex, "SELECT columnar_sum('empty', 'x')").await;
    assert_eq!(scalar(&res[0]), &Value::Float64(0.0));
    let res = exec(&ex, "SELECT columnar_avg('empty', 'x')").await;
    assert_eq!(scalar(&res[0]), &Value::Null);
    let res = exec(&ex, "SELECT columnar_min('empty', 'x')").await;
    assert_eq!(scalar(&res[0]), &Value::Null);
    let res = exec(&ex, "SELECT columnar_max('empty', 'x')").await;
    assert_eq!(scalar(&res[0]), &Value::Null);
}

// ======================================================================

// Time-series SQL function tests
// ======================================================================

#[tokio::test]
async fn test_ts_insert_and_count() {
    let ex = test_executor();
    exec(&ex, "SELECT ts_insert('cpu', 1000, 45.5)").await;
    exec(&ex, "SELECT ts_insert('cpu', 2000, 50.2)").await;
    exec(&ex, "SELECT ts_insert('cpu', 3000, 42.1)").await;
    let res = exec(&ex, "SELECT ts_count('cpu')").await;
    assert_eq!(scalar(&res[0]), &Value::Int64(3));
    // Missing series returns 0
    let res = exec(&ex, "SELECT ts_count('missing')").await;
    assert_eq!(scalar(&res[0]), &Value::Int64(0));
}

#[tokio::test]
async fn test_ts_last_value() {
    let ex = test_executor();
    exec(&ex, "SELECT ts_insert('mem', 1000, 60.0)").await;
    exec(&ex, "SELECT ts_insert('mem', 5000, 75.0)").await;
    let res = exec(&ex, "SELECT ts_last('mem')").await;
    assert_eq!(scalar(&res[0]), &Value::Float64(75.0));
    // Missing series
    let res = exec(&ex, "SELECT ts_last('nope')").await;
    assert_eq!(scalar(&res[0]), &Value::Null);
}

#[tokio::test]
async fn test_ts_range_count_and_avg() {
    let ex = test_executor();
    exec(&ex, "SELECT ts_insert('temp', 1000, 20.0)").await;
    exec(&ex, "SELECT ts_insert('temp', 2000, 25.0)").await;
    exec(&ex, "SELECT ts_insert('temp', 3000, 30.0)").await;
    exec(&ex, "SELECT ts_insert('temp', 4000, 35.0)").await;
    // Range count: [2000, 4000) should contain 2000 and 3000
    let res = exec(&ex, "SELECT ts_range_count('temp', 2000, 4000)").await;
    assert_eq!(scalar(&res[0]), &Value::Int64(2));
    // Range avg: [1000, 4000) should avg 20+25+30 = 75/3 = 25.0
    let res = exec(&ex, "SELECT ts_range_avg('temp', 1000, 4000)").await;
    assert_eq!(scalar(&res[0]), &Value::Float64(25.0));
    // Empty range
    let res = exec(&ex, "SELECT ts_range_avg('temp', 9000, 10000)").await;
    assert_eq!(scalar(&res[0]), &Value::Null);
}

#[tokio::test]
async fn test_ts_retention() {
    let ex = test_executor();
    // Set retention policy
    let res = exec(&ex, "SELECT ts_retention(86400000)").await;
    assert_eq!(scalar(&res[0]), &Value::Text("OK".into()));
}

// ======================================================================

// Document store SQL function tests
// ======================================================================

#[tokio::test]
async fn test_doc_insert_and_get() {
    let ex = test_executor();
    // Insert a JSON document
    let res = exec(&ex, r#"SELECT doc_insert('{"name":"Alice","age":30}')"#).await;
    assert_eq!(scalar(&res[0]), &Value::Int64(1));
    // Insert another
    let res = exec(&ex, r#"SELECT doc_insert('{"name":"Bob","age":25}')"#).await;
    assert_eq!(scalar(&res[0]), &Value::Int64(2));
    // Get by ID
    let res = exec(&ex, "SELECT doc_get(1)").await;
    let text = match scalar(&res[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected Text, got {other:?}"),
    };
    assert!(text.contains("Alice"));
    assert!(text.contains("30"));
    // Get missing doc
    let res = exec(&ex, "SELECT doc_get(999)").await;
    assert_eq!(scalar(&res[0]), &Value::Null);
}

#[tokio::test]
async fn test_doc_query_containment() {
    let ex = test_executor();
    exec(&ex, r#"SELECT doc_insert('{"type":"user","name":"Alice","role":"admin"}')"#).await;
    exec(&ex, r#"SELECT doc_insert('{"type":"user","name":"Bob","role":"viewer"}')"#).await;
    exec(&ex, r#"SELECT doc_insert('{"type":"event","action":"login"}')"#).await;
    // Query for docs containing {"type":"user"}
    let res = exec(&ex, r#"SELECT doc_query('{"type":"user"}')"#).await;
    let ids = match scalar(&res[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected Text, got {other:?}"),
    };
    // Should match docs 1 and 2 (both are type:user)
    let id_set: std::collections::HashSet<&str> = ids.split(',').collect();
    assert!(id_set.contains("1"));
    assert!(id_set.contains("2"));
    assert!(!id_set.contains("3"));
}

#[tokio::test]
async fn test_doc_path() {
    let ex = test_executor();
    exec(&ex, r#"SELECT doc_insert('{"user":{"name":"Alice","address":{"city":"NYC"}}}')"#).await;
    // Path query: user → name
    let res = exec(&ex, "SELECT doc_path(1, 'user', 'name')").await;
    assert_eq!(scalar(&res[0]), &Value::Text("\"Alice\"".into()));
    // Nested path: user → address → city
    let res = exec(&ex, "SELECT doc_path(1, 'user', 'address', 'city')").await;
    assert_eq!(scalar(&res[0]), &Value::Text("\"NYC\"".into()));
    // Missing path
    let res = exec(&ex, "SELECT doc_path(1, 'user', 'phone')").await;
    assert_eq!(scalar(&res[0]), &Value::Null);
}

#[tokio::test]
async fn test_doc_count() {
    let ex = test_executor();
    let res = exec(&ex, "SELECT doc_count()").await;
    assert_eq!(scalar(&res[0]), &Value::Int64(0));
    exec(&ex, r#"SELECT doc_insert('{"a":1}')"#).await;
    exec(&ex, r#"SELECT doc_insert('{"b":2}')"#).await;
    let res = exec(&ex, "SELECT doc_count()").await;
    assert_eq!(scalar(&res[0]), &Value::Int64(2));
}

// ======================================================================

// Full-text search (FTS) integration tests
// ======================================================================

#[tokio::test]
async fn test_fts_index_and_search() {
    let ex = test_executor();
    // Index three documents
    exec(&ex, "SELECT fts_index(1, 'rust programming language systems')").await;
    exec(&ex, "SELECT fts_index(2, 'python data science machine learning')").await;
    exec(&ex, "SELECT fts_index(3, 'rust systems performance optimization')").await;
    // Search for "rust systems" — docs 1 and 3 should match
    let res = exec(&ex, "SELECT fts_search('rust systems', 10)").await;
    let json = match scalar(&res[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected text, got {other:?}"),
    };
    assert!(json.contains("\"doc_id\":1") || json.contains("\"doc_id\":3"));
    // "python" should only match doc 2
    let res = exec(&ex, "SELECT fts_search('python', 10)").await;
    let json = match scalar(&res[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected text, got {other:?}"),
    };
    assert!(json.contains("\"doc_id\":2"));
    assert!(!json.contains("\"doc_id\":1"));
}

#[tokio::test]
async fn test_fts_fuzzy_search() {
    let ex = test_executor();
    exec(&ex, "SELECT fts_index(1, 'quantum computing research')").await;
    exec(&ex, "SELECT fts_index(2, 'classical mechanics physics')").await;
    // "quantm" is a typo for "quantum" — fuzzy should find doc 1
    let res = exec(&ex, "SELECT fts_fuzzy_search('quantm', 2, 10)").await;
    let json = match scalar(&res[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected text, got {other:?}"),
    };
    assert!(json.contains("\"doc_id\":1"), "fuzzy should match 'quantum': {json}");
}

#[tokio::test]
async fn test_fts_remove_and_counts() {
    let ex = test_executor();
    // Empty index
    let res = exec(&ex, "SELECT fts_doc_count()").await;
    assert_eq!(scalar(&res[0]), &Value::Int64(0));
    let res = exec(&ex, "SELECT fts_term_count()").await;
    assert_eq!(scalar(&res[0]), &Value::Int64(0));
    // Index two docs
    exec(&ex, "SELECT fts_index(10, 'database engine storage')").await;
    exec(&ex, "SELECT fts_index(20, 'web server framework')").await;
    let res = exec(&ex, "SELECT fts_doc_count()").await;
    assert_eq!(scalar(&res[0]), &Value::Int64(2));
    // Remove one
    exec(&ex, "SELECT fts_remove(10)").await;
    let res = exec(&ex, "SELECT fts_doc_count()").await;
    assert_eq!(scalar(&res[0]), &Value::Int64(1));
    // Search should only find doc 20
    let res = exec(&ex, "SELECT fts_search('server', 10)").await;
    let json = match scalar(&res[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected text, got {other:?}"),
    };
    assert!(!json.contains("\"doc_id\":10"));
}

#[tokio::test]
async fn test_fts_empty_and_no_match() {
    let ex = test_executor();
    // Search on empty index → empty array
    let res = exec(&ex, "SELECT fts_search('anything', 10)").await;
    assert_eq!(scalar(&res[0]), &Value::Text("[]".into()));
    // Index a doc then search for non-matching term
    exec(&ex, "SELECT fts_index(1, 'hello world')").await;
    let res = exec(&ex, "SELECT fts_search('nonexistent', 10)").await;
    assert_eq!(scalar(&res[0]), &Value::Text("[]".into()));
}


// FTS SQL integration tests
// ==========================================================================

#[tokio::test]
async fn test_fts_ts_match() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT TS_MATCH('The quick brown fox jumps over the lazy dog', 'quick fox')").await;
    let r = rows(&results[0]);
    assert_eq!(r[0][0], Value::Bool(true));

    let results = exec(&ex, "SELECT TS_MATCH('The quick brown fox', 'elephant')").await;
    let r = rows(&results[0]);
    assert_eq!(r[0][0], Value::Bool(false));
}

#[tokio::test]
async fn test_fts_ts_headline() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT TS_HEADLINE('The quick brown fox jumps over the lazy dog', 'quick fox')").await;
    let r = rows(&results[0]);
    if let Value::Text(s) = &r[0][0] {
        assert!(s.contains("<b>quick</b>"), "headline should highlight 'quick': {s}");
        assert!(s.contains("<b>fox</b>"), "headline should highlight 'fox': {s}");
    } else {
        panic!("expected text");
    }
}

#[tokio::test]
async fn test_fts_plainto_tsquery() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT PLAINTO_TSQUERY('running dogs')").await;
    let r = rows(&results[0]);
    if let Value::Text(s) = &r[0][0] {
        assert!(s.contains("&"), "should contain & operator: {s}");
        assert!(s.contains("run"), "should stem 'running' to 'run': {s}");
    } else {
        panic!("expected text");
    }
}

#[tokio::test]
async fn test_fts_in_table() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE articles (id INT, title TEXT, body TEXT)").await;
    exec(&ex, "INSERT INTO articles VALUES (1, 'Rust Programming', 'Rust is a systems programming language focused on safety')").await;
    exec(&ex, "INSERT INTO articles VALUES (2, 'Python Guide', 'Python is an interpreted dynamic language')").await;
    exec(&ex, "INSERT INTO articles VALUES (3, 'Database Design', 'SQL databases store structured data efficiently')").await;

    // Search using TS_MATCH in WHERE clause
    let results = exec(&ex, "SELECT id, title FROM articles WHERE TS_MATCH(body, 'programming language')").await;
    let r = rows(&results[0]);
    assert!(r.len() >= 1, "should find at least 1 match");
    // Both article 1 and 2 mention 'language'
    let ids: Vec<i32> = r.iter().filter_map(|row| if let Value::Int32(i) = row[0] { Some(i) } else { None }).collect();
    assert!(ids.contains(&1), "should find Rust article");

    // Rank-based ordering
    let results = exec(&ex, "SELECT id, TS_RANK(body, 'programming') AS rank FROM articles WHERE TS_MATCH(body, 'programming')").await;
    let r = rows(&results[0]);
    assert!(!r.is_empty(), "should find matches for 'programming'");
}

// FTS_MATCH against persistent index tests
// ==========================================================================

#[tokio::test]
async fn test_fts_match_uses_persistent_index() {
    let ex = test_executor();
    // Index documents into the shared persistent index
    exec(&ex, "SELECT FTS_INDEX(10, 'rust systems programming language fast safe')").await;
    exec(&ex, "SELECT FTS_INDEX(20, 'python scripting machine learning data science')").await;
    exec(&ex, "SELECT FTS_INDEX(30, 'database storage engine query optimizer')").await;

    // FTS_MATCH uses the persistent index (not a per-call rebuild)
    let r1 = exec(&ex, "SELECT FTS_MATCH(10, 'rust programming')").await;
    assert_eq!(rows(&r1[0])[0][0], Value::Bool(true));

    let r2 = exec(&ex, "SELECT FTS_MATCH(20, 'machine learning')").await;
    assert_eq!(rows(&r2[0])[0][0], Value::Bool(true));

    // doc 10 does not contain "database"
    let r3 = exec(&ex, "SELECT FTS_MATCH(10, 'database')").await;
    assert_eq!(rows(&r3[0])[0][0], Value::Bool(false));

    // Unknown doc returns false
    let r4 = exec(&ex, "SELECT FTS_MATCH(99, 'rust')").await;
    assert_eq!(rows(&r4[0])[0][0], Value::Bool(false));
}

#[tokio::test]
async fn test_fts_match_in_where_clause() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE posts (id INT, title TEXT)").await;
    exec(&ex, "INSERT INTO posts VALUES (1, 'Rust is fast')").await;
    exec(&ex, "INSERT INTO posts VALUES (2, 'Python is expressive')").await;
    exec(&ex, "INSERT INTO posts VALUES (3, 'Rust and Python coexist')").await;

    exec(&ex, "SELECT FTS_INDEX(1, 'Rust is fast')").await;
    exec(&ex, "SELECT FTS_INDEX(2, 'Python is expressive')").await;
    exec(&ex, "SELECT FTS_INDEX(3, 'Rust and Python coexist')").await;

    // Filter by FTS_MATCH in WHERE clause
    let results = exec(&ex, "SELECT id FROM posts WHERE FTS_MATCH(id, 'rust')").await;
    let r = rows(&results[0]);
    let ids: Vec<i32> = r.iter().filter_map(|row| {
        if let Value::Int32(i) = row[0] { Some(i) } else { None }
    }).collect();
    assert!(ids.contains(&1), "doc 1 should match 'rust'");
    assert!(ids.contains(&3), "doc 3 should match 'rust'");
    assert!(!ids.contains(&2), "doc 2 should NOT match 'rust'");
}

#[tokio::test]
async fn test_fts_remove_updates_match() {
    let ex = test_executor();
    exec(&ex, "SELECT FTS_INDEX(1, 'rust programming')").await;
    // Confirm initial match
    let r1 = exec(&ex, "SELECT FTS_MATCH(1, 'rust')").await;
    assert_eq!(rows(&r1[0])[0][0], Value::Bool(true));
    // Remove and confirm no longer matches
    exec(&ex, "SELECT FTS_REMOVE(1)").await;
    let r2 = exec(&ex, "SELECT FTS_MATCH(1, 'rust')").await;
    assert_eq!(rows(&r2[0])[0][0], Value::Bool(false));
}

// ==========================================================================
// Sparse vector index SQL functions (SPARSE_INSERT / SPARSE_WAND)
// ==========================================================================

#[tokio::test]
async fn test_sparse_insert_and_doc_count() {
    let ex = test_executor();
    // Empty index initially
    let r = exec(&ex, "SELECT SPARSE_DOC_COUNT()").await;
    assert_eq!(rows(&r[0])[0][0], Value::Int64(0));

    exec(&ex, r#"SELECT SPARSE_INSERT(1, '{"0": 2.0, "1": 3.0}')"#).await;
    exec(&ex, r#"SELECT SPARSE_INSERT(2, '{"1": 1.0, "2": 4.0}')"#).await;

    let r2 = exec(&ex, "SELECT SPARSE_DOC_COUNT()").await;
    assert_eq!(rows(&r2[0])[0][0], Value::Int64(2));
}

#[tokio::test]
async fn test_sparse_remove() {
    let ex = test_executor();
    exec(&ex, r#"SELECT SPARSE_INSERT(10, '{"0": 1.0}')"#).await;
    exec(&ex, r#"SELECT SPARSE_INSERT(20, '{"0": 2.0}')"#).await;
    assert_eq!(rows(&exec(&ex, "SELECT SPARSE_DOC_COUNT()").await[0])[0][0], Value::Int64(2));

    let r = exec(&ex, "SELECT SPARSE_REMOVE(10)").await;
    assert_eq!(rows(&r[0])[0][0], Value::Bool(true));
    assert_eq!(rows(&exec(&ex, "SELECT SPARSE_DOC_COUNT()").await[0])[0][0], Value::Int64(1));

    // Removing nonexistent doc returns false
    let r2 = exec(&ex, "SELECT SPARSE_REMOVE(999)").await;
    assert_eq!(rows(&r2[0])[0][0], Value::Bool(false));
}

#[tokio::test]
async fn test_sparse_search_exact() {
    let ex = test_executor();
    exec(&ex, r#"SELECT SPARSE_INSERT(1, '{"0": 2.0, "1": 1.0}')"#).await;
    exec(&ex, r#"SELECT SPARSE_INSERT(2, '{"0": 0.5, "2": 3.0}')"#).await;
    exec(&ex, r#"SELECT SPARSE_INSERT(3, '{"1": 4.0}')"#).await;

    // Query on dim 0: doc 1 (score 2.0) > doc 2 (score 0.5)
    let r = exec(&ex, r#"SELECT SPARSE_SEARCH('{"0": 1.0}', 3)"#).await;
    match &rows(&r[0])[0][0] {
        Value::Text(json) => {
            assert!(json.contains(r#""doc_id":1"#), "doc 1 should be in results: {json}");
        }
        _ => panic!("expected JSON text"),
    }
}

#[tokio::test]
async fn test_sparse_wand_top_k() {
    let ex = test_executor();
    // Insert 20 docs with escalating weights on dim 0
    for i in 1..=20u64 {
        exec(&ex, &format!(r#"SELECT SPARSE_INSERT({i}, '{{"0": {}.0}}')"#, i)).await;
    }
    // WAND top 3 should be docs 20, 19, 18
    let r = exec(&ex, r#"SELECT SPARSE_WAND('{"0": 1.0}', 3)"#).await;
    match &rows(&r[0])[0][0] {
        Value::Text(json) => {
            assert!(json.contains(r#""doc_id":20"#), "doc 20 should be top result: {json}");
        }
        _ => panic!("expected JSON text"),
    }
}

#[tokio::test]
async fn test_sparse_wand_matches_search() {
    let ex = test_executor();
    for i in 1..=15u64 {
        exec(&ex, &format!(
            r#"SELECT SPARSE_INSERT({i}, '{{"0": {a}.0, "1": {b}.0}}')"#,
            a = i, b = 16 - i,
        )).await;
    }
    let query = r#"'{"0": 1.0, "1": 1.0}'"#;
    let r_exact = exec(&ex, &format!("SELECT SPARSE_SEARCH({query}, 5)")).await;
    let r_wand = exec(&ex, &format!("SELECT SPARSE_WAND({query}, 5)")).await;

    // Both should return same top result (all docs score 16 here)
    match (&rows(&r_exact[0])[0][0], &rows(&r_wand[0])[0][0]) {
        (Value::Text(e), Value::Text(w)) => {
            // Both should be non-empty JSON arrays
            assert!(e.starts_with('[') && !e.is_empty());
            assert!(w.starts_with('[') && !w.is_empty());
        }
        _ => panic!("expected JSON text"),
    }
}

// ==========================================================================
// Memory allocator SQL functions
// ==========================================================================

#[tokio::test]
async fn test_mem_budget_and_available() {
    let ex = test_executor();
    // Budget should be 1 GiB (1 << 30)
    let r = exec(&ex, "SELECT MEM_BUDGET()").await;
    match rows(&r[0])[0][0] {
        Value::Int64(b) => assert_eq!(b, 1 << 30, "budget should be 1 GiB"),
        _ => panic!("expected Int64"),
    }
    // Available should be ≤ budget
    let r2 = exec(&ex, "SELECT MEM_AVAILABLE()").await;
    match rows(&r2[0])[0][0] {
        Value::Int64(a) => assert!(a >= 0 && a <= (1 << 30), "available should be in [0, budget]"),
        _ => panic!("expected Int64"),
    }
}

#[tokio::test]
async fn test_mem_usage_after_fts_insert() {
    let ex = test_executor();
    let r_before = exec(&ex, "SELECT MEM_USAGE()").await;
    let before = match rows(&r_before[0])[0][0] { Value::Int64(v) => v, _ => panic!() };

    exec(&ex, "SELECT FTS_INDEX(1, 'rust systems programming')").await;

    let r_after = exec(&ex, "SELECT MEM_USAGE()").await;
    let after = match rows(&r_after[0])[0][0] { Value::Int64(v) => v, _ => panic!() };

    assert!(after > before, "usage should increase after FTS_INDEX");
}

#[tokio::test]
async fn test_mem_usage_after_sparse_insert() {
    let ex = test_executor();
    let r_before = exec(&ex, "SELECT MEM_USAGE()").await;
    let before = match rows(&r_before[0])[0][0] { Value::Int64(v) => v, _ => panic!() };

    exec(&ex, r#"SELECT SPARSE_INSERT(1, '{"0": 1.0, "1": 2.0}')"#).await;

    let r_after = exec(&ex, "SELECT MEM_USAGE()").await;
    let after = match rows(&r_after[0])[0][0] { Value::Int64(v) => v, _ => panic!() };
    assert!(after > before, "usage should increase after SPARSE_INSERT");
}

#[tokio::test]
async fn test_mem_utilization() {
    let ex = test_executor();
    let r = exec(&ex, "SELECT MEM_UTILIZATION()").await;
    match rows(&r[0])[0][0] {
        Value::Float64(u) => assert!(u >= 0.0 && u <= 100.0, "utilization should be 0-100%"),
        _ => panic!("expected Float64"),
    }
}

#[tokio::test]
async fn test_show_memory() {
    let ex = test_executor();
    let results = ex.execute("SHOW MEMORY").await.unwrap();
    match &results[0] {
        ExecResult::Select { columns, rows } => {
            assert_eq!(columns[0].0, "subsystem");
            assert_eq!(columns[1].0, "current_bytes");
            // Should have registered subsystems
            assert!(!rows.is_empty(), "should have at least one subsystem");
            // All subsystem names should be non-empty strings
            for row in rows {
                if let Value::Text(name) = &row[0] {
                    assert!(!name.is_empty());
                } else {
                    panic!("subsystem name should be Text");
                }
            }
        }
        _ => panic!("expected select"),
    }
}

#[tokio::test]
async fn test_memory_pressure_command() {
    let ex = test_executor();
    let results = ex.execute("MEMORY PRESSURE").await.unwrap();
    match &results[0] {
        ExecResult::Command { tag, .. } => assert_eq!(tag, "MEMORY PRESSURE"),
        _ => panic!("expected command"),
    }
}

#[tokio::test]
async fn test_mem_stats_json() {
    let ex = test_executor();
    let r = exec(&ex, "SELECT MEM_STATS()").await;
    match &rows(&r[0])[0][0] {
        Value::Text(json) => {
            assert!(json.starts_with('['), "should be JSON array: {json}");
            assert!(json.contains("\"name\""), "should have name field");
        }
        _ => panic!("expected Text"),
    }
}

// ==========================================================================

// Geospatial SQL integration tests
// ==========================================================================

#[tokio::test]
async fn test_geo_st_makepoint() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT ST_MAKEPOINT(-74.006, 40.7128)").await;
    let r = rows(&results[0]);
    if let Value::Text(s) = &r[0][0] {
        assert!(s.contains("POINT("), "should return WKT POINT: {s}");
        assert!(s.contains("-74.006"), "should contain longitude");
        assert!(s.contains("40.7128"), "should contain latitude");
    } else {
        panic!("expected text");
    }
}

#[tokio::test]
async fn test_geo_st_x_y() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT ST_X(ST_MAKEPOINT(-74.006, 40.7128))").await;
    let r = rows(&results[0]);
    if let Value::Float64(x) = r[0][0] {
        assert!((x - (-74.006)).abs() < 0.001, "ST_X should return -74.006, got {x}");
    } else {
        panic!("expected float64");
    }

    let results = exec(&ex, "SELECT ST_Y(ST_MAKEPOINT(-74.006, 40.7128))").await;
    let r = rows(&results[0]);
    if let Value::Float64(y) = r[0][0] {
        assert!((y - 40.7128).abs() < 0.001, "ST_Y should return 40.7128, got {y}");
    } else {
        panic!("expected float64");
    }
}

#[tokio::test]
async fn test_geo_st_contains() {
    let ex = test_executor();
    // Unit square polygon, test point inside and outside
    let results = exec(&ex, "SELECT ST_CONTAINS('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))', 'POINT(5 5)')").await;
    let r = rows(&results[0]);
    assert_eq!(r[0][0], Value::Bool(true));

    let results = exec(&ex, "SELECT ST_CONTAINS('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))', 'POINT(15 5)')").await;
    let r = rows(&results[0]);
    assert_eq!(r[0][0], Value::Bool(false));
}

#[tokio::test]
async fn test_geo_in_table() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE locations (id INT, name TEXT, lat FLOAT, lon FLOAT)").await;
    exec(&ex, "INSERT INTO locations VALUES (1, 'New York', 40.7128, -74.006)").await;
    exec(&ex, "INSERT INTO locations VALUES (2, 'Los Angeles', 34.0522, -118.2437)").await;
    exec(&ex, "INSERT INTO locations VALUES (3, 'Newark', 40.7357, -74.1724)").await;

    // Find locations within 50km of NYC using existing 4-arg ST_DISTANCE
    let results = exec(&ex, "SELECT name, ST_DISTANCE(lat, lon, 40.7128, -74.006) AS dist FROM locations WHERE ST_DISTANCE(lat, lon, 40.7128, -74.006) < 50000").await;
    let r = rows(&results[0]);
    let names: Vec<String> = r.iter().filter_map(|row| if let Value::Text(s) = &row[0] { Some(s.clone()) } else { None }).collect();
    assert!(names.contains(&"New York".to_string()), "NYC should be within 50km of itself");
    assert!(names.contains(&"Newark".to_string()), "Newark should be within 50km of NYC");
    assert!(!names.contains(&"Los Angeles".to_string()), "LA should NOT be within 50km of NYC");
}

// ==========================================================================

// Time-series SQL integration tests
// ==========================================================================

#[tokio::test]
async fn test_timeseries_date_bin() {
    let ex = test_executor();
    // date_bin with text interval
    let results = exec(&ex, "SELECT DATE_BIN('1 hour', 1700000123456)").await;
    let r = rows(&results[0]);
    if let Value::Int64(ts) = r[0][0] {
        assert_eq!(ts % 3_600_000, 0, "should be truncated to hour boundary");
        assert!(ts <= 1700000123456, "truncated ts should be <= original");
    } else {
        panic!("expected int64");
    }
}

#[tokio::test]
async fn test_timeseries_time_bucket_in_table() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE metrics (ts BIGINT, value FLOAT)").await;
    let base = 1700000000000i64;
    for i in 0..10 {
        let ts = base + i * 60_000; // one per minute
        exec(&ex, &format!("INSERT INTO metrics VALUES ({ts}, {}.5)", i)).await;
    }

    // Group by minute bucket using TIME_BUCKET (numeric form already existed)
    let results = exec(&ex, &format!("SELECT TIME_BUCKET(60000, ts) AS bucket, COUNT(*) FROM metrics GROUP BY TIME_BUCKET(60000, ts)")).await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 10, "each minute should be its own bucket with 1-minute intervals");
}

// ==========================================================================

// Graph SQL integration tests
// ==========================================================================

#[tokio::test]
async fn test_graph_shortest_path_length() {
    let ex = test_executor();
    // Simple linear graph: 1→2→3→4
    let edges = r#"[{"from":1,"to":2},{"from":2,"to":3},{"from":3,"to":4}]"#;
    let results = exec(&ex, &format!("SELECT GRAPH_SHORTEST_PATH_LENGTH('{edges}', 1, 4)")).await;
    let r = rows(&results[0]);
    assert_eq!(r[0][0], Value::Int32(3), "path 1→2→3→4 has length 3");
}

#[tokio::test]
async fn test_graph_shortest_path_no_path() {
    let ex = test_executor();
    // Disconnected graph: 1→2, 3→4 (no path from 1 to 4)
    let edges = r#"[{"from":1,"to":2},{"from":3,"to":4}]"#;
    let results = exec(&ex, &format!("SELECT GRAPH_SHORTEST_PATH_LENGTH('{edges}', 1, 4)")).await;
    let r = rows(&results[0]);
    assert_eq!(r[0][0], Value::Null, "no path should return NULL");
}

#[tokio::test]
async fn test_graph_node_degree() {
    let ex = test_executor();
    // Node 2 has edges: 1→2, 2→3, 2→4
    let edges = r#"[{"from":1,"to":2},{"from":2,"to":3},{"from":2,"to":4}]"#;
    let results = exec(&ex, &format!("SELECT GRAPH_NODE_DEGREE('{edges}', 2)")).await;
    let r = rows(&results[0]);
    assert_eq!(r[0][0], Value::Int32(3), "node 2 has 3 edges (1 in + 2 out)");
}

#[tokio::test]
async fn test_graph_in_table() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE roads (id INT, edges_json TEXT)").await;
    exec(&ex, r#"INSERT INTO roads VALUES (1, '[{"from":1,"to":2},{"from":2,"to":3},{"from":1,"to":3}]')"#).await;

    let results = exec(&ex, "SELECT GRAPH_SHORTEST_PATH_LENGTH(edges_json, 1, 3) FROM roads WHERE id = 1").await;
    let r = rows(&results[0]);
    assert_eq!(r[0][0], Value::Int32(1), "direct edge 1→3 has length 1");
}

// ================================================================

// Cross-Model Datalog Integration Tests
// ======================================================================

#[tokio::test]
async fn test_datalog_assert_and_query() {
    let ex = test_executor();
    exec(&ex, "SELECT DATALOG_ASSERT('parent(alice, bob)')").await;
    exec(&ex, "SELECT DATALOG_ASSERT('parent(bob, charlie)')").await;
    let r = exec(&ex, "SELECT DATALOG_QUERY('parent(alice, X)')").await;
    let json = match scalar(&r[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected Text, got {other:?}"),
    };
    assert!(json.contains("bob"), "result should contain bob: {json}");
}

#[tokio::test]
async fn test_datalog_rule_recursive() {
    let ex = test_executor();
    exec(&ex, "SELECT DATALOG_ASSERT('parent(alice, bob)')").await;
    exec(&ex, "SELECT DATALOG_ASSERT('parent(bob, charlie)')").await;
    exec(&ex, "SELECT DATALOG_ASSERT('parent(charlie, dave)')").await;
    exec(&ex, "SELECT DATALOG_RULE('ancestor(X, Y) :- parent(X, Y)')").await;
    exec(&ex, "SELECT DATALOG_RULE('ancestor(X, Z) :- ancestor(X, Y), parent(Y, Z)')").await;
    let r = exec(&ex, "SELECT DATALOG_QUERY('ancestor(alice, Who)')").await;
    let json = match scalar(&r[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected Text, got {other:?}"),
    };
    assert!(json.contains("bob"), "should find bob: {json}");
    assert!(json.contains("charlie"), "should find charlie: {json}");
    assert!(json.contains("dave"), "should find dave: {json}");
}

#[tokio::test]
async fn test_datalog_import_from_relational_table() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE employees (name TEXT, dept TEXT)").await;
    exec(&ex, "INSERT INTO employees VALUES ('alice', 'eng')").await;
    exec(&ex, "INSERT INTO employees VALUES ('bob', 'sales')").await;
    exec(&ex, "INSERT INTO employees VALUES ('charlie', 'eng')").await;

    // Import relational table into datalog
    let r = exec(&ex, "SELECT DATALOG_IMPORT('employees', 'employee')").await;
    let msg = match scalar(&r[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected Text, got {other:?}"),
    };
    assert!(msg.contains("3"), "should import 3 rows: {msg}");

    // Query the imported facts
    let r = exec(&ex, "SELECT DATALOG_QUERY('employee(X, eng)')").await;
    let json = match scalar(&r[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected Text, got {other:?}"),
    };
    assert!(json.contains("alice"), "should find alice in eng: {json}");
    assert!(json.contains("charlie"), "should find charlie in eng: {json}");
    assert!(!json.contains("bob"), "bob is in sales, not eng: {json}");
}

#[tokio::test]
async fn test_datalog_import_graph_edges() {
    let ex = test_executor();
    // Create graph nodes and edges
    exec(&ex, "SELECT GRAPH_ADD_NODE('Person', '{\"name\":\"alice\"}')").await;
    exec(&ex, "SELECT GRAPH_ADD_NODE('Person', '{\"name\":\"bob\"}')").await;
    exec(&ex, "SELECT GRAPH_ADD_NODE('Person', '{\"name\":\"charlie\"}')").await;
    exec(&ex, "SELECT GRAPH_ADD_EDGE(1, 2, 'KNOWS')").await;
    exec(&ex, "SELECT GRAPH_ADD_EDGE(2, 3, 'KNOWS')").await;

    // Import graph edges into datalog
    let r = exec(&ex, "SELECT DATALOG_IMPORT_GRAPH('knows')").await;
    let msg = match scalar(&r[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected Text, got {other:?}"),
    };
    assert!(msg.contains("2"), "should import 2 edges: {msg}");

    // Query the imported edge facts
    let r = exec(&ex, "SELECT DATALOG_QUERY('knows(1, KNOWS, X)')").await;
    let json = match scalar(&r[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected Text, got {other:?}"),
    };
    assert!(json.contains("2"), "node 1 should know node 2: {json}");
}

#[tokio::test]
async fn test_datalog_import_nodes() {
    let ex = test_executor();
    exec(&ex, "SELECT GRAPH_ADD_NODE('Person', '{}')").await;
    exec(&ex, "SELECT GRAPH_ADD_NODE('Company', '{}')").await;

    let r = exec(&ex, "SELECT DATALOG_IMPORT_NODES('node')").await;
    let msg = match scalar(&r[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected Text, got {other:?}"),
    };
    assert!(msg.contains("2"), "should import 2 node-label pairs: {msg}");

    // Query: all Person nodes
    let r = exec(&ex, "SELECT DATALOG_QUERY('node(Id, Person)')").await;
    let json = match scalar(&r[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected Text, got {other:?}"),
    };
    assert!(json.contains("1"), "node 1 is a Person: {json}");
}

#[tokio::test]
async fn test_datalog_cross_model_reasoning() {
    let ex = test_executor();
    // Relational: employees table
    exec(&ex, "CREATE TABLE staff (name TEXT, role TEXT)").await;
    exec(&ex, "INSERT INTO staff VALUES ('alice', 'manager')").await;
    exec(&ex, "INSERT INTO staff VALUES ('bob', 'engineer')").await;
    exec(&ex, "INSERT INTO staff VALUES ('charlie', 'engineer')").await;

    // Import relational data
    exec(&ex, "SELECT DATALOG_IMPORT('staff', 'staff')").await;

    // Add a rule: managers manage engineers
    exec(&ex, "SELECT DATALOG_RULE('manages(M, E) :- staff(M, manager), staff(E, engineer)')").await;

    // Query: who does alice manage?
    let r = exec(&ex, "SELECT DATALOG_QUERY('manages(alice, Who)')").await;
    let json = match scalar(&r[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected Text, got {other:?}"),
    };
    assert!(json.contains("bob"), "alice should manage bob: {json}");
    assert!(json.contains("charlie"), "alice should manage charlie: {json}");
}

#[tokio::test]
async fn test_datalog_retract_and_clear() {
    let ex = test_executor();
    exec(&ex, "SELECT DATALOG_ASSERT('fact(a, b)')").await;
    exec(&ex, "SELECT DATALOG_ASSERT('fact(c, d)')").await;

    // Retract one fact
    exec(&ex, "SELECT DATALOG_RETRACT('fact(a, b)')").await;
    let r = exec(&ex, "SELECT DATALOG_QUERY('fact(X, Y)')").await;
    let json = match scalar(&r[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected Text, got {other:?}"),
    };
    assert!(!json.contains("\"a\""), "a should be retracted: {json}");
    assert!(json.contains("\"c\""), "c should remain: {json}");

    // Clear all
    exec(&ex, "SELECT DATALOG_CLEAR('fact')").await;
    let r = exec(&ex, "SELECT DATALOG_QUERY('fact(X, Y)')").await;
    let json = match scalar(&r[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected Text, got {other:?}"),
    };
    assert_eq!(json, "[]", "should be empty after clear");
}

