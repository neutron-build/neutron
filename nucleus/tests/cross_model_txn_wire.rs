//! Cross-model transaction regressions, exercised over the real pgwire path.
//!
//! Every case here reproduced against a live server before the M8 fix. They are
//! deliberately end-to-end (tokio-postgres over TCP, sessions created and torn
//! down by the wire handler) because the failures live in session lifecycle and
//! in process-global store state, neither of which a direct `Executor::execute`
//! call exercises faithfully: `execute` runs on the default session, so a
//! library-level test cannot even express "session A rolls back while session B
//! watches".
//!
//! The three failure modes under test:
//!
//! 1. One session's ROLLBACK destroyed every other session's committed,
//!    acknowledged non-SQL writes, because `ROLLBACK` assigned a whole-store
//!    clone taken at `BEGIN` back over the live store.
//! 2. ROLLBACK reverted memory but left the specialty WAL records in place, so
//!    recovery resurrected the rolled-back writes.
//! 3. A client disconnect rolled back the SQL half of a transaction and kept
//!    the non-SQL half, permanently.

#![cfg(feature = "server")]

use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio_postgres::NoTls;

use nucleus::executor::open_persistent_executor;
use nucleus::wire::{NucleusHandler, NucleusServer};

// ============================================================================
// Harness
// ============================================================================

struct Server {
    port: u16,
    accept: tokio::task::JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.accept.abort();
    }
}

/// Boot a durable Nucleus server (segmented SQL WAL plus every specialty WAL)
/// rooted at `data_dir`, listening on an OS-assigned port.
///
/// The accept loop mirrors `main.rs`, including the `cleanup_session` call
/// after `process_socket` returns — that call is the disconnect path under test,
/// so a harness that omitted it would test nothing.
async fn start(data_dir: &Path) -> Server {
    let executor = open_persistent_executor(data_dir)
        .await
        .expect("open persistent executor");
    let handler = Arc::new(NucleusHandler::new(executor));
    let server = Arc::new(NucleusServer::new(handler.clone()));

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().unwrap().port();

    let accept = tokio::spawn(async move {
        loop {
            let Ok((socket, peer)) = listener.accept().await else {
                break;
            };
            let srv = server.clone();
            let cleanup = handler.clone();
            let peer_addr = peer.to_string();
            tokio::spawn(async move {
                let _ =
                    pgwire::tokio::process_socket(socket, None::<pgwire::tokio::TlsAcceptor>, srv)
                        .await;
                cleanup.cleanup_session(&peer_addr);
            });
        }
    });

    Server { port, accept }
}

async fn connect(port: u16) -> tokio_postgres::Client {
    let connstr = format!("host=127.0.0.1 port={port} user=nucleus dbname=test");
    let (client, connection) = tokio_postgres::connect(&connstr, NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

/// Run a statement whose result is a single text-ish scalar, returned as a
/// string. Simple-query protocol so the value arrives already rendered.
async fn scalar(client: &tokio_postgres::Client, sql: &str) -> Option<String> {
    let messages = client.simple_query(sql).await.expect(sql);
    for message in messages {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = message {
            return row.get(0).map(|v| v.to_string());
        }
    }
    None
}

async fn run(client: &tokio_postgres::Client, sql: &str) {
    client.simple_query(sql).await.expect(sql);
}

/// Copy a live data directory. This is exactly what a `kill -9` leaves behind:
/// whatever reached the filesystem, with no clean-shutdown flush.
fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let target = dst.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).unwrap();
        }
    }
}

fn tmpdir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "nucleus-m8-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

// ============================================================================
// 1. One session's ROLLBACK must not destroy another session's committed writes
// ============================================================================

/// Session B sets a key, autocommitted and acknowledged, while session A holds
/// an open transaction. A's ROLLBACK must not touch B's key.
///
/// Before the fix `KvStore::txn_restore` cleared every shard and reinstalled
/// A's BEGIN-time clone, so `b_key` vanished — an acknowledged, fsynced write
/// destroyed by an unrelated session.
#[tokio::test]
async fn rollback_keeps_other_sessions_committed_kv_writes() {
    let dir = tmpdir("kv-clobber");
    let server = start(&dir).await;
    let a = connect(server.port).await;
    let b = connect(server.port).await;

    run(&a, "BEGIN").await;
    run(&a, "SELECT kv_set('a_key', 'uncommitted')").await;

    // B writes outside any transaction: acknowledged before A rolls back.
    run(&b, "SELECT kv_set('b_key', 'committed')").await;
    assert_eq!(
        scalar(&b, "SELECT kv_get('b_key')").await.as_deref(),
        Some("committed")
    );

    run(&a, "ROLLBACK").await;

    assert_eq!(
        scalar(&b, "SELECT kv_get('b_key')").await.as_deref(),
        Some("committed"),
        "session A's ROLLBACK destroyed session B's committed KV write"
    );
    // A's own write is still correctly reverted.
    assert_eq!(
        scalar(&b, "SELECT kv_get('a_key')").await.as_deref(),
        None,
        "the rolling-back session's own KV write survived"
    );
}

/// Same shape for the document store, whose rollback was also a whole-store
/// assignment.
#[tokio::test]
async fn rollback_keeps_other_sessions_committed_doc_writes() {
    let dir = tmpdir("doc-clobber");
    let server = start(&dir).await;
    let a = connect(server.port).await;
    let b = connect(server.port).await;

    run(&a, "BEGIN").await;
    run(&a, "SELECT doc_insert('{\"owner\":\"a\"}')").await;

    let b_id = scalar(&b, "SELECT doc_insert('{\"owner\":\"b\"}')")
        .await
        .expect("doc id");

    run(&a, "ROLLBACK").await;

    let got = scalar(&b, &format!("SELECT doc_get({b_id})")).await;
    assert!(
        got.is_some_and(|v| v.contains("\"b\"")),
        "session A's ROLLBACK destroyed session B's committed document"
    );
}

/// And for the graph store. B's node must outlive A's rollback, and A's own
/// node must still disappear.
#[tokio::test]
async fn rollback_keeps_other_sessions_committed_graph_writes() {
    let dir = tmpdir("graph-clobber");
    let server = start(&dir).await;
    let a = connect(server.port).await;
    let b = connect(server.port).await;

    run(&a, "BEGIN").await;
    run(&a, "SELECT graph_add_node('A', '{}')").await;
    run(&b, "SELECT graph_add_node('B', '{}')").await;
    assert_eq!(
        scalar(&b, "SELECT graph_node_count()").await.as_deref(),
        Some("2")
    );

    run(&a, "ROLLBACK").await;

    // Exactly B's node must remain: A's own node reverted, B's untouched.
    assert_eq!(
        scalar(&b, "SELECT graph_node_count()").await.as_deref(),
        Some("1"),
        "ROLLBACK must revert only the rolling-back session's graph node"
    );
    let labelled_b = scalar(&b, "SELECT graph_query('MATCH (n:B) RETURN n')").await;
    assert!(
        labelled_b.is_some_and(|v| !v.contains("\"rows\":[]")),
        "session A's ROLLBACK destroyed session B's committed graph node"
    );
}

// ============================================================================
// 2. ROLLBACK must be durable
// ============================================================================

/// A crash after a successful ROLLBACK must not resurrect the rolled-back
/// writes. Before the fix `txn_restore` reverted memory only and left the
/// `SET` record in `kv/kv.wal`, so replay brought the key back.
///
/// The copy is taken while the first server is still running, so it contains
/// exactly what a `kill -9` would leave on disk.
#[tokio::test]
async fn rolled_back_kv_write_does_not_come_back_after_recovery() {
    let dir = tmpdir("kv-durable-rollback");
    let server = start(&dir).await;
    let client = connect(server.port).await;

    run(&client, "SELECT kv_set('survivor', 'keep')").await;
    run(&client, "BEGIN").await;
    run(&client, "SELECT kv_set('ghost', 'should-not-survive')").await;
    run(&client, "ROLLBACK").await;

    // Snapshot the on-disk state as a crash would leave it, then recover.
    let crashed = tmpdir("kv-durable-rollback-recovered");
    copy_dir(&dir, &crashed);

    let recovered = open_persistent_executor(&crashed).await.expect("recover");
    let ghost = recovered
        .execute("SELECT kv_get('ghost')")
        .await
        .expect("query");
    let rendered = format!("{ghost:?}");
    assert!(
        !rendered.contains("should-not-survive"),
        "WAL replay resurrected a rolled-back KV write: {rendered}"
    );

    let survivor = recovered
        .execute("SELECT kv_get('survivor')")
        .await
        .expect("query");
    assert!(
        format!("{survivor:?}").contains("keep"),
        "recovery lost a committed KV write while reverting the rolled-back one"
    );
}

/// Same invariant for the document store, whose WAL also kept the rolled-back
/// insert.
#[tokio::test]
async fn rolled_back_document_does_not_come_back_after_recovery() {
    let dir = tmpdir("doc-durable-rollback");
    let server = start(&dir).await;
    let client = connect(server.port).await;

    run(&client, "BEGIN").await;
    let id = scalar(&client, "SELECT doc_insert('{\"ghost\":true}')")
        .await
        .expect("doc id");
    run(&client, "ROLLBACK").await;

    let crashed = tmpdir("doc-durable-rollback-recovered");
    copy_dir(&dir, &crashed);

    let recovered = open_persistent_executor(&crashed).await.expect("recover");
    let got = recovered
        .execute(&format!("SELECT doc_get({id})"))
        .await
        .expect("query");
    let rendered = format!("{got:?}");
    assert!(
        !rendered.contains("ghost"),
        "WAL replay resurrected a rolled-back document: {rendered}"
    );
}

// ============================================================================
// 3. A disconnect must not split a transaction
// ============================================================================

/// Dropping the client mid-transaction discards the SQL rows (the storage
/// session goes with the connection). The non-SQL writes have to go with them,
/// otherwise a plain TCP close leaves half a transaction committed forever.
#[tokio::test]
async fn disconnect_mid_transaction_reverts_cross_model_writes() {
    let dir = tmpdir("disconnect");
    let server = start(&dir).await;
    let observer = connect(server.port).await;
    run(
        &observer,
        "CREATE TABLE orders (id INT PRIMARY KEY, state TEXT)",
    )
    .await;

    {
        let doomed = connect(server.port).await;
        run(&doomed, "BEGIN").await;
        run(&doomed, "INSERT INTO orders VALUES (1, 'paid')").await;
        run(&doomed, "SELECT kv_set('order:1:state', 'paid')").await;
        run(&doomed, "SELECT doc_insert('{\"order\":1}')").await;
        run(&doomed, "SELECT graph_add_node('Order', '{}')").await;
        // Drop without COMMIT or ROLLBACK: a client that went away.
        drop(doomed);

        // Wait for the server side to notice and run its cleanup path.
        for _ in 0..200 {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            if scalar(&observer, "SELECT kv_get('order:1:state')")
                .await
                .is_none()
            {
                break;
            }
        }
        assert_eq!(
            scalar(&observer, "SELECT graph_node_count()")
                .await
                .as_deref(),
            Some("0"),
            "graph node from an abandoned transaction survived the disconnect"
        );
    }

    assert_eq!(
        scalar(&observer, "SELECT kv_get('order:1:state')")
            .await
            .as_deref(),
        None,
        "KV write from an abandoned transaction survived the disconnect"
    );
    assert_eq!(
        scalar(&observer, "SELECT count(*) FROM orders")
            .await
            .as_deref(),
        Some("0"),
        "SQL side of the abandoned transaction was not discarded"
    );
    assert_eq!(
        scalar(&observer, "SELECT doc_count()").await.as_deref(),
        Some("0"),
        "document from an abandoned transaction survived the disconnect"
    );
}

// ============================================================================
// 4. ROLLBACK TO SAVEPOINT must revert cross-model writes too
// ============================================================================

/// Before the fix `ROLLBACK TO SAVEPOINT` reverted SQL and the security catalog
/// and nothing else, so the KV write below survived a COMMIT that, as far as
/// the client could tell, contained no such write.
#[tokio::test]
async fn rollback_to_savepoint_reverts_cross_model_writes() {
    let dir = tmpdir("savepoint");
    let server = start(&dir).await;
    let client = connect(server.port).await;
    run(&client, "CREATE TABLE t (id INT PRIMARY KEY)").await;

    run(&client, "BEGIN").await;
    run(&client, "SELECT kv_set('before_sp', 'keep')").await;
    run(&client, "SAVEPOINT s1").await;
    run(&client, "INSERT INTO t VALUES (1)").await;
    run(&client, "SELECT kv_set('after_sp', 'discard')").await;
    let doc = scalar(&client, "SELECT doc_insert('{\"after_sp\":true}')")
        .await
        .expect("doc id");
    run(&client, "ROLLBACK TO SAVEPOINT s1").await;
    run(&client, "COMMIT").await;

    assert_eq!(
        scalar(&client, "SELECT kv_get('after_sp')")
            .await
            .as_deref(),
        None,
        "KV write made after the savepoint survived ROLLBACK TO SAVEPOINT"
    );
    let doc_after = scalar(&client, &format!("SELECT doc_get({doc})")).await;
    assert!(
        !doc_after.is_some_and(|v| v.contains("after_sp")),
        "document inserted after the savepoint survived ROLLBACK TO SAVEPOINT"
    );
    assert_eq!(
        scalar(&client, "SELECT kv_get('before_sp')")
            .await
            .as_deref(),
        Some("keep"),
        "ROLLBACK TO SAVEPOINT reverted a write made before the savepoint"
    );
    assert_eq!(
        scalar(&client, "SELECT count(*) FROM t").await.as_deref(),
        Some("0"),
        "SQL row inserted after the savepoint survived ROLLBACK TO SAVEPOINT"
    );
}

// ============================================================================
// 5. A committing session must keep its own cross-model writes
// ============================================================================

/// Guard against the fix over-reverting: COMMIT must leave everything in place,
/// and a concurrent session's rollback must not disturb it either.
#[tokio::test]
async fn commit_keeps_cross_model_writes_under_concurrent_rollback() {
    let dir = tmpdir("commit-keeps");
    let server = start(&dir).await;
    let a = connect(server.port).await;
    let b = connect(server.port).await;

    run(&a, "BEGIN").await;
    run(&a, "SELECT kv_set('a_only', 'x')").await;

    run(&b, "BEGIN").await;
    run(&b, "SELECT kv_set('b_committed', 'y')").await;
    run(&b, "SELECT ts_insert('cpu', 1000, 1.5)").await;
    run(&b, "COMMIT").await;

    run(&a, "ROLLBACK").await;

    assert_eq!(
        scalar(&b, "SELECT kv_get('b_committed')").await.as_deref(),
        Some("y"),
        "a concurrent ROLLBACK destroyed a committed KV write"
    );
    assert_eq!(
        scalar(&b, "SELECT ts_count('cpu')").await.as_deref(),
        Some("1"),
        "a concurrent ROLLBACK destroyed a committed time-series point"
    );
    assert_eq!(scalar(&b, "SELECT kv_get('a_only')").await.as_deref(), None);
}
