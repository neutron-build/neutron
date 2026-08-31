//! A parameterized column's LABEL is provisional until the parameters are
//! bound, and a label that changes on binding must not refuse the read.
//!
//! Found in production on the first 1.0.0 rollout, by nothing in this suite.
//! Teploy Ship polls `SELECT STREAM_XRANGE($1, 0, …)` every few seconds.
//! Portal Describe labelled that column `stream_xrange`; execution labelled it
//! with the substituted expression, `STREAM_XRANGE('ship:run-…', 0, …)`. Same
//! count, same type, rows decodable either way — but the portal path recorded
//! its names as FINAL, so 1.0.0's mismatch guard refused to send the rows and
//! Ship's run-event stream failed on every poll.
//!
//! This test speaks the protocol directly, and it has to. `tokio_postgres`'s
//! `prepare()` issues Describe(STATEMENT), which has always marked its names
//! provisional because parameters are not bound yet — a test built on it
//! passes with the fix reverted, which is how the first version of this test
//! was caught being decorative. Ship's client issues Describe(PORTAL), after
//! Bind. Only the raw frames reach that path.
//!
//!     cargo test --test portal_describe_label

#![cfg(feature = "server")]

use std::path::Path;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use nucleus::executor::open_persistent_executor;
use nucleus::wire::{NucleusHandler, NucleusServer};

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
        out.extend_from_slice(&196_608i32.to_be_bytes());
        out.extend_from_slice(&params);
        self.stream.write_all(&out).await.expect("startup");
        loop {
            let msg = self.read_msg().await;
            match msg.tag {
                b'Z' => break,
                b'E' => panic!("startup failed: {}", err(&msg)),
                _ => {}
            }
        }
    }

    async fn read_msg(&mut self) -> Msg {
        let mut header = [0u8; 5];
        self.stream.read_exact(&mut header).await.expect("header");
        let tag = header[0];
        let len = i32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize;
        let mut body = vec![0u8; len - 4];
        if !body.is_empty() {
            self.stream.read_exact(&mut body).await.expect("body");
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

    async fn simple(&mut self, sql: &str) {
        let mut body = sql.as_bytes().to_vec();
        body.push(0);
        self.send(b'Q', &body).await;
        loop {
            let msg = self.read_msg().await;
            match msg.tag {
                b'Z' => return,
                b'E' => panic!("{sql} failed: {}", err(&msg)),
                _ => {}
            }
        }
    }

    /// Parse, Bind, Describe(PORTAL), Execute, Sync — the sequence a driver
    /// sends when it describes the bound portal rather than the statement.
    /// Returns the RowDescription's column names and the DataRow count, or the
    /// ErrorResponse text.
    async fn portal_roundtrip(
        &mut self,
        sql: &str,
        param: &str,
    ) -> Result<(Vec<String>, usize), String> {
        // Parse (unnamed statement, no declared param types)
        let mut b = vec![0u8];
        b.extend_from_slice(sql.as_bytes());
        b.push(0);
        b.extend_from_slice(&0i16.to_be_bytes());
        self.send(b'P', &b).await;

        // Bind: unnamed portal, unnamed statement, one text parameter.
        let mut b = vec![
            0u8, // portal: unnamed
            0u8, // statement: unnamed
        ];
        b.extend_from_slice(&0i16.to_be_bytes()); // param format codes: all text
        b.extend_from_slice(&1i16.to_be_bytes()); // one parameter
        b.extend_from_slice(&(param.len() as i32).to_be_bytes());
        b.extend_from_slice(param.as_bytes());
        b.extend_from_slice(&0i16.to_be_bytes()); // result format codes: all text
        self.send(b'B', &b).await;

        // Describe the PORTAL ('P'), not the statement ('S'). This is the
        // whole point of the test.
        let b = [b'P', 0];
        self.send(b'D', &b).await;

        // Execute (unnamed portal, no row limit), then Sync.
        let mut b = vec![0u8];
        b.extend_from_slice(&0i32.to_be_bytes());
        self.send(b'E', &b).await;
        self.send(b'S', &[]).await;

        let mut names = Vec::new();
        let mut rows = 0usize;
        loop {
            let msg = self.read_msg().await;
            match msg.tag {
                b'T' => names = row_description_names(&msg.body),
                b'D' => rows += 1,
                b'E' => {
                    let e = err(&msg);
                    // Drain to ReadyForQuery so the connection stays usable.
                    loop {
                        if self.read_msg().await.tag == b'Z' {
                            break;
                        }
                    }
                    return Err(e);
                }
                b'Z' => return Ok((names, rows)),
                _ => {}
            }
        }
    }
}

/// RowDescription: i16 field count, then per field: name\0 + 18 bytes.
fn row_description_names(body: &[u8]) -> Vec<String> {
    let n = i16::from_be_bytes([body[0], body[1]]) as usize;
    let mut out = Vec::with_capacity(n);
    let mut i = 2;
    for _ in 0..n {
        let end = i + body[i..].iter().position(|&c| c == 0).expect("name NUL");
        out.push(String::from_utf8_lossy(&body[i..end]).into_owned());
        i = end + 1 + 18;
    }
    out
}

fn err(msg: &Msg) -> String {
    String::from_utf8_lossy(&msg.body).replace('\0', " ")
}

struct Server {
    port: u16,
    accept: tokio::task::JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.accept.abort();
    }
}

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

#[tokio::test]
async fn a_bound_portals_column_label_may_change_without_refusing_the_rows() {
    let tmp = tempfile::tempdir().unwrap();
    let server = start(tmp.path()).await;
    let mut c = Conn::connect(server.port).await;

    c.simple("SELECT STREAM_XADD('wir7:s', 'f', 'v')").await;

    // Ship's exact shape.
    let out = c
        .portal_roundtrip(
            "SELECT STREAM_XRANGE($1, 0, 9007199254740991, 1000000)",
            "wir7:s",
        )
        .await;

    let (names, rows) = match out {
        Ok(v) => v,
        Err(e) => panic!("a bound portal whose column label changed was refused its rows: {e}"),
    };
    assert_eq!(
        names.len(),
        1,
        "one projection item must be described exactly once, got {names:?}"
    );
    assert_eq!(rows, 1, "the stream's single entry must come back");
}
