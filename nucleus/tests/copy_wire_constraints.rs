//! `COPY ... FROM STDIN` over the real pgwire protocol.
//!
//! The wire path (`on_copy_done` / `parse_copy_rows`) is *different code* from
//! the inline `COPY ... FROM STDIN;` the executor handles, and it is the one
//! `psql \copy`, `pg_dump | psql`, and every bulk loader actually use. Green
//! executor-level tests say nothing about it: the wire handler used to
//! synthesize its own `INSERT` text in 500-row chunks, so a payload that
//! violated a constraint in the second chunk committed the first one.
//!
//! These tests speak the protocol directly. `tokio_postgres::copy_in` is NOT
//! usable here: it prepares the statement over the *extended* protocol, which
//! never reaches the server's COPY interception (that lives in the simple-query
//! path), so every assertion built on it passes for the wrong reason. psql's
//! `\copy` uses simple query, and so does `simple_copy_in` below.
//!
//!     cargo test --test copy_wire_constraints

use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use nucleus::catalog::Catalog;
use nucleus::executor::Executor;
use nucleus::storage::{MvccStorageAdapter, StorageEngine};
use nucleus::wire::{NucleusHandler, NucleusServer};

// ============================================================================
// Minimal pgwire client (simple query protocol only)
// ============================================================================

/// A backend message: type byte plus body (length prefix stripped).
struct Msg {
    tag: u8,
    body: Vec<u8>,
}

struct Conn {
    stream: TcpStream,
}

impl Conn {
    async fn connect(port: u16) -> Self {
        let stream = TcpStream::connect(("127.0.0.1", port)).await.expect("tcp");
        let mut conn = Self { stream };
        conn.startup().await;
        conn
    }

    /// StartupMessage (protocol 3.0) followed by a drain to ReadyForQuery.
    async fn startup(&mut self) {
        let mut params = Vec::new();
        for (k, v) in [("user", "nucleus"), ("database", "test")] {
            params.extend_from_slice(k.as_bytes());
            params.push(0);
            params.extend_from_slice(v.as_bytes());
            params.push(0);
        }
        params.push(0);

        let len = 4 + 4 + params.len();
        let mut out = Vec::with_capacity(len);
        out.extend_from_slice(&(len as i32).to_be_bytes());
        out.extend_from_slice(&196_608i32.to_be_bytes()); // 3.0
        out.extend_from_slice(&params);
        self.stream.write_all(&out).await.expect("startup");

        loop {
            let msg = self.read_msg().await;
            match msg.tag {
                b'Z' => break,                                   // ReadyForQuery
                b'E' => panic!("startup failed: {}", err(&msg)), // ErrorResponse
                _ => {}
            }
        }
    }

    async fn read_msg(&mut self) -> Msg {
        let mut header = [0u8; 5];
        self.stream
            .read_exact(&mut header)
            .await
            .expect("read message header");
        let tag = header[0];
        let len = i32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize;
        let mut body = vec![0u8; len - 4];
        if !body.is_empty() {
            self.stream
                .read_exact(&mut body)
                .await
                .expect("read message body");
        }
        Msg { tag, body }
    }

    async fn send(&mut self, tag: u8, body: &[u8]) {
        let mut out = Vec::with_capacity(5 + body.len());
        out.push(tag);
        out.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
        out.extend_from_slice(body);
        self.stream.write_all(&out).await.expect("send");
    }

    async fn query_raw(&mut self, sql: &str) {
        let mut body = sql.as_bytes().to_vec();
        body.push(0);
        self.send(b'Q', &body).await;
    }

    /// Run a statement, discarding rows. Panics on ErrorResponse.
    async fn exec(&mut self, sql: &str) {
        self.query_raw(sql).await;
        loop {
            let msg = self.read_msg().await;
            match msg.tag {
                b'Z' => return,
                b'E' => panic!("{sql} failed: {}", err(&msg)),
                _ => {}
            }
        }
    }

    /// Run a query returning a single text cell (`DataRow` field 0).
    async fn scalar(&mut self, sql: &str) -> String {
        self.query_raw(sql).await;
        let mut value = None;
        loop {
            let msg = self.read_msg().await;
            match msg.tag {
                b'D' => {
                    // DataRow: i16 field count, then per field i32 len + bytes.
                    let n = i16::from_be_bytes([msg.body[0], msg.body[1]]);
                    assert!(n >= 1, "expected at least one column");
                    let len =
                        i32::from_be_bytes([msg.body[2], msg.body[3], msg.body[4], msg.body[5]]);
                    value = Some(if len < 0 {
                        "NULL".to_string()
                    } else {
                        String::from_utf8_lossy(&msg.body[6..6 + len as usize]).into_owned()
                    });
                }
                b'Z' => return value.expect("no DataRow returned"),
                b'E' => panic!("{sql} failed: {}", err(&msg)),
                _ => {}
            }
        }
    }

    /// The real `\copy`: simple-query `COPY ... FROM STDIN`, then CopyData +
    /// CopyDone. Returns the CommandComplete tag, or the error message.
    async fn simple_copy_in(&mut self, sql: &str, payload: &str) -> Result<String, String> {
        self.query_raw(sql).await;
        // Expect CopyInResponse before streaming anything.
        loop {
            let msg = self.read_msg().await;
            match msg.tag {
                b'G' => break,
                b'E' => {
                    let message = err(&msg);
                    self.drain_to_ready().await;
                    return Err(message);
                }
                b'Z' => return Err("server never entered copy-in mode".into()),
                _ => {}
            }
        }
        self.send(b'd', payload.as_bytes()).await; // CopyData
        self.send(b'c', &[]).await; // CopyDone

        let mut tag = None;
        loop {
            let msg = self.read_msg().await;
            match msg.tag {
                b'C' => {
                    tag = Some(
                        String::from_utf8_lossy(&msg.body[..msg.body.len().saturating_sub(1)])
                            .into_owned(),
                    );
                }
                b'E' => {
                    let message = err(&msg);
                    self.drain_to_ready().await;
                    return Err(message);
                }
                b'Z' => return Ok(tag.unwrap_or_default()),
                _ => {}
            }
        }
    }

    async fn drain_to_ready(&mut self) {
        // A CopyDone that errors leaves no ReadyForQuery pending in some
        // sequences; a short read timeout keeps the helper from hanging.
        loop {
            let read =
                tokio::time::timeout(std::time::Duration::from_millis(500), self.read_msg()).await;
            match read {
                Ok(msg) if msg.tag == b'Z' => return,
                Ok(_) => {}
                Err(_) => return,
            }
        }
    }
}

/// Extract the human-readable text from an ErrorResponse body.
fn err(msg: &Msg) -> String {
    let mut out = String::new();
    let mut i = 0;
    while i < msg.body.len() && msg.body[i] != 0 {
        let field = msg.body[i];
        let start = i + 1;
        let end = msg.body[start..]
            .iter()
            .position(|b| *b == 0)
            .map_or(msg.body.len(), |p| start + p);
        if field == b'M' {
            out = String::from_utf8_lossy(&msg.body[start..end]).into_owned();
        }
        i = end + 1;
    }
    out
}

// ============================================================================
// Server harness
// ============================================================================

async fn start_server() -> (u16, tokio::task::JoinHandle<()>) {
    let catalog = Arc::new(Catalog::new());
    let storage: Arc<dyn StorageEngine> = Arc::new(MvccStorageAdapter::new());
    let executor = Arc::new(Executor::new(catalog, storage));
    let handler = Arc::new(NucleusHandler::new(executor));
    let server = Arc::new(NucleusServer::new(handler));

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().unwrap().port();
    let handle = tokio::spawn(async move {
        while let Ok((socket, _)) = listener.accept().await {
            let srv = server.clone();
            tokio::spawn(async move {
                let _ =
                    pgwire::tokio::process_socket(socket, None::<pgwire::tokio::TlsAcceptor>, srv)
                        .await;
            });
        }
    });
    (port, handle)
}

// ============================================================================
// Tests
// ============================================================================

/// Sanity check on the harness itself: a clean payload must actually load. If
/// this fails, every "must be rejected" assertion below is meaningless.
#[tokio::test]
async fn wire_copy_loads_a_valid_payload() {
    let (port, server) = start_server().await;
    let mut c = Conn::connect(port).await;
    c.exec("CREATE TABLE wok (id INT PRIMARY KEY, qty INT)")
        .await;

    let tag = c
        .simple_copy_in("COPY wok FROM STDIN", "1\t10\n2\t20\n")
        .await
        .expect("a valid COPY must succeed");
    assert_eq!(tag, "COPY 2");
    assert_eq!(c.scalar("SELECT COUNT(*) FROM wok").await, "2");
    server.abort();
}

/// COPY is the loader most likely to ingest untrusted data, so it is the worst
/// place to skip validation.
#[tokio::test]
async fn wire_copy_enforces_check_and_not_null() {
    let (port, server) = start_server().await;
    let mut c = Conn::connect(port).await;
    c.exec("CREATE TABLE wc (id INT PRIMARY KEY, qty INT CHECK (qty > 0))")
        .await;
    c.exec("CREATE TABLE wn (id INT PRIMARY KEY, name TEXT NOT NULL)")
        .await;

    let bad = c.simple_copy_in("COPY wc FROM STDIN", "1\t-5\n").await;
    assert!(
        bad.is_err(),
        "wire COPY accepted a row violating CHECK (qty > 0)"
    );
    assert_eq!(c.scalar("SELECT COUNT(*) FROM wc").await, "0");

    let bad = c.simple_copy_in("COPY wn FROM STDIN", "1\t\\N\n").await;
    assert!(bad.is_err(), "wire COPY accepted NULL in a NOT NULL column");
    assert_eq!(c.scalar("SELECT COUNT(*) FROM wn").await, "0");

    server.abort();
}

/// PostgreSQL's COPY is all-or-nothing within the statement. The failure must
/// land past the old 500-row chunk size: that is exactly where the previous
/// implementation committed one batch and then failed on the next, and a
/// short payload would not have caught it.
#[tokio::test]
async fn wire_copy_is_atomic_past_the_old_chunk_boundary() {
    let (port, server) = start_server().await;
    let mut c = Conn::connect(port).await;
    c.exec("CREATE TABLE wb (id INT PRIMARY KEY, qty INT CHECK (qty > 0))")
        .await;

    let mut payload = String::new();
    for id in 1..=600 {
        payload.push_str(&format!("{id}\t{id}\n"));
    }
    payload.push_str("601\t-1\n");

    let bad = c.simple_copy_in("COPY wb FROM STDIN", &payload).await;
    assert!(bad.is_err(), "row 601 violates CHECK (qty > 0)");
    assert_eq!(
        c.scalar("SELECT COUNT(*) FROM wb").await,
        "0",
        "the first 500-row chunk must not survive a failure in the second"
    );

    server.abort();
}

/// A duplicate key inside one payload must also leave the table untouched.
#[tokio::test]
async fn wire_copy_rejects_a_self_duplicated_key_atomically() {
    let (port, server) = start_server().await;
    let mut c = Conn::connect(port).await;
    c.exec("CREATE TABLE wd (id INT PRIMARY KEY, qty INT)")
        .await;

    let bad = c
        .simple_copy_in("COPY wd FROM STDIN", "1\t10\n2\t20\n1\t30\n")
        .await;
    assert!(bad.is_err(), "id 1 appears twice in one payload");
    assert_eq!(c.scalar("SELECT COUNT(*) FROM wd").await, "0");

    server.abort();
}

/// Text format: an empty field is the empty string, only `\N` is NULL.
#[tokio::test]
async fn wire_copy_text_empty_field_is_empty_string_not_null() {
    let (port, server) = start_server().await;
    let mut c = Conn::connect(port).await;
    c.exec("CREATE TABLE wt (id INT PRIMARY KEY, name TEXT)")
        .await;

    c.simple_copy_in("COPY wt FROM STDIN", "1\t\n2\t\\N\n")
        .await
        .expect("valid COPY");

    assert_eq!(
        c.scalar("SELECT COUNT(*) FROM wt WHERE name = ''").await,
        "1",
        "the empty text-format field must be '', not NULL"
    );
    assert_eq!(
        c.scalar("SELECT COUNT(*) FROM wt WHERE name IS NULL").await,
        "1",
        "only the \\N field may be NULL"
    );

    server.abort();
}

/// CSV is the other way round: an unquoted empty field is NULL (PostgreSQL's
/// default CSV NULL string is the empty string), a quoted `""` is the empty
/// string.
#[tokio::test]
async fn wire_copy_csv_distinguishes_empty_from_quoted_empty() {
    let (port, server) = start_server().await;
    let mut c = Conn::connect(port).await;
    c.exec("CREATE TABLE wcsv (id INT PRIMARY KEY, name TEXT)")
        .await;

    c.simple_copy_in("COPY wcsv FROM STDIN WITH (FORMAT csv)", "1,\n2,\"\"\n")
        .await
        .expect("valid CSV COPY");

    assert_eq!(
        c.scalar("SELECT COUNT(*) FROM wcsv WHERE name IS NULL")
            .await,
        "1"
    );
    assert_eq!(
        c.scalar("SELECT COUNT(*) FROM wcsv WHERE name = ''").await,
        "1"
    );

    server.abort();
}

/// A COPY field must land in its column's declared type, exactly as the same
/// literal would through INSERT.
#[tokio::test]
async fn wire_copy_coerces_to_the_declared_column_type() {
    let (port, server) = start_server().await;
    let mut c = Conn::connect(port).await;
    c.exec("CREATE TABLE wty (id INT PRIMARY KEY, d DATE)")
        .await;

    c.simple_copy_in("COPY wty FROM STDIN", "1\t2024-03-26\n")
        .await
        .expect("valid COPY");
    c.exec("INSERT INTO wty VALUES (2, '2024-03-26')").await;

    assert_eq!(
        c.scalar("SELECT COUNT(*) FROM wty WHERE d = DATE '2024-03-26'")
            .await,
        "2",
        "the COPY'd DATE must compare equal to the INSERT'd one"
    );

    let bad = c
        .simple_copy_in("COPY wty FROM STDIN", "3\tnot-a-date\n")
        .await;
    assert!(bad.is_err(), "COPY stored an unparseable DATE as text");

    server.abort();
}
