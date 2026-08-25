//! S31-10: the RESP stream commands silently discarded an unparseable COUNT
//! (`.parse::<usize>().ok()` in XRANGE/XREVRANGE/XREAD/XREADGROUP — an
//! unparseable COUNT meant "no limit" instead of an error), and XRANGE/
//! XREVRANGE trusted ARG POSITION over the keyword, parsing args[5] whenever
//! six args were present without ever checking args[4] == "COUNT".
//!
//! Driven over a real TCP server (the S31-13 pattern) because the defect is
//! reachable over the wire by any Redis-protocol client.

#![cfg(feature = "server")]

use std::sync::Arc;
use std::time::Duration;

use nucleus::kv::KvStore;
use nucleus::resp::server::{RespServerConfig, start_resp_server_with_config};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

struct RespServer {
    port: u16,
    task: tokio::task::JoinHandle<()>,
    shutdown: Arc<tokio::sync::Notify>,
}

impl Drop for RespServer {
    fn drop(&mut self) {
        self.shutdown.notify_waiters();
        self.task.abort();
    }
}

async fn start() -> RespServer {
    for _ in 0..10 {
        let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = probe.local_addr().unwrap().port();
        drop(probe);

        let shutdown = Arc::new(tokio::sync::Notify::new());
        let addr = format!("127.0.0.1:{port}");
        let kv = Arc::new(KvStore::new());
        let shut = Arc::clone(&shutdown);
        let task = tokio::spawn(async move {
            let _ = start_resp_server_with_config(
                addr,
                kv,
                None,
                shut,
                RespServerConfig {
                    idle_timeout_secs: 300,
                    ..Default::default()
                },
            )
            .await;
        });
        for _ in 0..50 {
            if TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
                return RespServer {
                    port,
                    task,
                    shutdown,
                };
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        task.abort();
    }
    panic!("could not bind a RESP test port");
}

fn resp_array(parts: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", parts.len()).into_bytes();
    for p in parts {
        out.extend_from_slice(format!("${}\r\n{}\r\n", p.len(), p).as_bytes());
    }
    out
}

/// Read one CRLF-terminated reply line with a deadline.
async fn read_line(stream: &mut TcpStream, secs: u64) -> Option<String> {
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    let deadline = tokio::time::Duration::from_secs(secs);
    loop {
        match tokio::time::timeout(deadline, stream.read(&mut byte)).await {
            Ok(Ok(0)) | Err(_) => return None,
            Ok(Ok(_)) => {
                buf.push(byte[0]);
                if buf.ends_with(b"\r\n") {
                    buf.pop();
                    buf.pop();
                    return Some(String::from_utf8_lossy(&buf).into_owned());
                }
                if buf.len() > 1024 * 1024 {
                    return Some(String::from_utf8_lossy(&buf).into_owned());
                }
            }
            Ok(Err(e)) => panic!("read error: {e}"),
        }
    }
}

async fn read_exact_n(stream: &mut TcpStream, n: usize) -> Vec<u8> {
    let mut buf = vec![0u8; n];
    stream.read_exact(&mut buf).await.expect("read_exact");
    buf
}

/// Consume one complete RESP value, returning its first line (the type marker
/// line) so callers can assert on it without leaving bytes buffered.
async fn read_reply_first_line(stream: &mut TcpStream) -> String {
    let first = read_line(stream, 5).await.expect("reply line");
    match first.as_bytes().first() {
        Some(b'$') => {
            let n: usize = first[1..].parse().expect("bulk length");
            let _payload = read_exact_n(stream, n + 2).await;
        }
        Some(b'*') => {
            let n: usize = first[1..].parse().expect("array length");
            for _ in 0..n {
                Box::pin(read_reply_first_line(stream)).await;
            }
        }
        _ => {}
    }
    first
}

async fn command(stream: &mut TcpStream, parts: &[&str]) -> String {
    stream
        .write_all(resp_array(parts).as_slice())
        .await
        .expect("write command");
    read_reply_first_line(stream).await
}

#[tokio::test]
async fn xrange_count_is_keyword_checked_and_validated() {
    let server = start().await;
    let mut c = TcpStream::connect(("127.0.0.1", server.port))
        .await
        .expect("connect");
    for i in 1..=3 {
        let reply = command(&mut c, &["XADD", "s", "*", "f", &format!("v{i}")]).await;
        assert!(reply.starts_with('$'), "XADD reply: {reply}");
    }

    // A valid COUNT limits the reply (first line of the array: *2).
    let reply = command(&mut c, &["XRANGE", "s", "-", "+", "COUNT", "2"]).await;
    assert_eq!(reply, "*2", "XRANGE COUNT 2 must return 2 entries: {reply}");

    // An unparseable COUNT is an error, not "no limit" — pre-fix it was
    // silently discarded and every entry came back.
    let reply = command(&mut c, &["XRANGE", "s", "-", "+", "COUNT", "banana"]).await;
    assert!(
        reply.starts_with("-ERR") && reply.contains("not an integer"),
        "unparseable COUNT must error, got: {reply}"
    );

    // A non-COUNT keyword in the COUNT slot is a syntax error — pre-fix
    // args[5] was parsed as a count regardless of args[4].
    let reply = command(&mut c, &["XRANGE", "s", "-", "+", "FROB", "2"]).await;
    assert!(
        reply.starts_with("-ERR") && reply.contains("syntax"),
        "COUNT must be keyword-checked, got: {reply}"
    );

    // No COUNT: full result.
    let reply = command(&mut c, &["XRANGE", "s", "-", "+"]).await;
    assert_eq!(reply, "*3", "XRANGE without COUNT returns all: {reply}");
}

#[tokio::test]
async fn xrevrange_xread_xreadgroup_validate_count() {
    let server = start().await;
    let mut c = TcpStream::connect(("127.0.0.1", server.port))
        .await
        .expect("connect");
    for i in 1..=3 {
        command(&mut c, &["XADD", "s", "*", "f", &format!("v{i}")]).await;
    }

    let reply = command(&mut c, &["XREVRANGE", "s", "+", "-", "COUNT", "banana"]).await;
    assert!(
        reply.starts_with("-ERR") && reply.contains("not an integer"),
        "XREVRANGE unparseable COUNT must error, got: {reply}"
    );
    let reply = command(&mut c, &["XREVRANGE", "s", "+", "-", "COUNT", "1"]).await;
    assert_eq!(reply, "*1", "XREVRANGE COUNT 1: {reply}");

    let reply = command(&mut c, &["XREAD", "COUNT", "banana", "STREAMS", "s", "0"]).await;
    assert!(
        reply.starts_with("-ERR") && reply.contains("not an integer"),
        "XREAD unparseable COUNT must error, got: {reply}"
    );
    let reply = command(&mut c, &["XREAD", "COUNT", "2", "STREAMS", "s", "0"]).await;
    assert_eq!(reply, "*1", "XREAD COUNT 2 outer array: {reply}");

    // XREADGROUP needs a group first.
    command(&mut c, &["XGROUP", "CREATE", "s", "g", "0"]).await;
    let reply = command(
        &mut c,
        &[
            "XREADGROUP",
            "GROUP",
            "g",
            "c1",
            "COUNT",
            "banana",
            "STREAMS",
            "s",
            ">",
        ],
    )
    .await;
    assert!(
        reply.starts_with("-ERR") && reply.contains("not an integer"),
        "XREADGROUP unparseable COUNT must error, got: {reply}"
    );
}
