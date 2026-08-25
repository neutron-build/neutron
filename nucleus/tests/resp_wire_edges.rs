//! Pre-auth RESP protocol edges, driven over a real TCP server.
//!
//! In-process tests of the parser or handler cannot see these: the parser
//! recursion (WIR-1) aborts the whole process, not a task; the SUBSCRIBE
//! auth bypass (WIR-2) lives in the server loop's dispatch, not in any
//! unit-callable function; the unbounded line read (WIR-8) only manifests
//! against a socket that never sends a newline. All three are reachable
//! before AUTH, so each test connects with no credentials at all.

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

/// Start a RESP server with a password on a loopback port.
async fn start() -> RespServer {
    // Grab a free port by binding then releasing it (the server binds
    // itself). Retry if the race loses.
    for _ in 0..10 {
        let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = probe.local_addr().unwrap().port();
        drop(probe);

        let shutdown = Arc::new(tokio::sync::Notify::new());
        let addr = format!("127.0.0.1:{port}");
        let kv = Arc::new(KvStore::new());
        let pw = Some("sekret".to_string());
        let shut = Arc::clone(&shutdown);
        let task = tokio::spawn(async move {
            let _ = start_resp_server_with_config(
                addr,
                kv,
                pw,
                shut,
                RespServerConfig {
                    idle_timeout_secs: 300,
                    ..Default::default()
                },
            )
            .await;
        });
        // Wait for the listener to come up.
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

async fn connect(port: u16) -> TcpStream {
    TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("connect")
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

fn resp_array(parts: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", parts.len()).into_bytes();
    for p in parts {
        out.extend_from_slice(format!("${}\r\n{}\r\n", p.len(), p).as_bytes());
    }
    out
}

/// Like [`resp_array`] but the last part is sent as raw bytes (binary-safe
/// bulk string).
fn resp_array_with_raw(parts: &[&str], raw: &[u8]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", parts.len() + 1).into_bytes();
    for p in parts {
        out.extend_from_slice(format!("${}\r\n{}\r\n", p.len(), p).as_bytes());
    }
    out.extend_from_slice(format!("${}\r\n", raw.len()).as_bytes());
    out.extend_from_slice(raw);
    out.extend_from_slice(b"\r\n");
    out
}

/// WIR-1: deeply nested arrays must produce a protocol error on ONE
/// connection, not a stack overflow that aborts the server process. No AUTH
/// is attempted — the parser runs before any gate.
#[tokio::test]
async fn deep_nesting_errors_without_killing_the_server() {
    let server = start().await;

    let mut hostile = connect(server.port).await;
    let payload = "*1\r\n".repeat(100_000);
    // The server closes the connection on the depth error, so the write may
    // fail with EPIPE partway — that is fine, the recursion it triggers is
    // the point.
    for chunk in payload.as_bytes().chunks(16 * 1024) {
        if hostile.write_all(chunk).await.is_err() {
            break;
        }
    }
    let mut sink = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(10), hostile.read_to_end(&mut sink)).await;

    // The server must still be alive and serving NEW connections.
    let mut honest = connect(server.port).await;
    honest
        .write_all(resp_array(&["PING"]).as_slice())
        .await
        .expect("server dead after nesting attack");
    let reply = read_line(&mut honest, 5)
        .await
        .expect("no PING reply — server died");
    assert_eq!(reply, "+PONG");
}

/// WIR-8: a prefix line past 64 KiB with no newline must get the connection
/// closed promptly, not buffer the whole stream until the idle timeout.
#[tokio::test]
async fn overlong_line_closes_the_connection() {
    let server = start().await;

    let mut hostile = connect(server.port).await;
    let flood = vec![b'A'; 256 * 1024];
    hostile.write_all(&flood).await.expect("write flood");
    // No newline is ever sent; the read side must see the connection close
    // well before the 300 s idle timeout. Keep the write half open so the
    // close can only come from the server erroring out. The close may
    // surface as clean EOF or as a reset (unread flood bytes are still in
    // flight when the server drops the socket) — either proves the line cap
    // fired; only the idle timeout would leave the read pending.
    let mut sink = Vec::new();
    match tokio::time::timeout(Duration::from_secs(10), hostile.read_to_end(&mut sink)).await {
        Ok(_) => {}
        Err(_) => panic!("connection stayed open — the unbounded line was buffered"),
    }

    let mut honest = connect(server.port).await;
    honest
        .write_all(resp_array(&["PING"]).as_slice())
        .await
        .unwrap();
    assert_eq!(read_line(&mut honest, 5).await.unwrap(), "+PONG");
}

/// WIR-2: SUBSCRIBE before AUTH must be refused; an authenticated PUBLISH
/// must then reach zero subscribers. Pre-fix, the unauthenticated connection
/// entered pub/sub mode and received every subsequent message.
#[tokio::test]
async fn subscribe_before_auth_is_rejected() {
    let server = start().await;

    // Unauthenticated SUBSCRIBE.
    let mut intruder = connect(server.port).await;
    intruder
        .write_all(resp_array(&["SUBSCRIBE", "events"]).as_slice())
        .await
        .unwrap();
    let reply = read_line(&mut intruder, 5)
        .await
        .expect("no reply to SUBSCRIBE");
    assert!(
        reply.starts_with("-NOAUTH"),
        "unauthenticated SUBSCRIBE must be refused, got: {reply}"
    );

    // Authenticated PUBLISH: nobody may be subscribed.
    let mut client = connect(server.port).await;
    client
        .write_all(resp_array(&["AUTH", "sekret"]).as_slice())
        .await
        .unwrap();
    assert_eq!(read_line(&mut client, 5).await.unwrap(), "+OK");
    client
        .write_all(resp_array(&["PUBLISH", "events", "hello"]).as_slice())
        .await
        .unwrap();
    let reply = read_line(&mut client, 5).await.unwrap();
    assert_eq!(
        reply, ":0",
        "the refused SUBSCRIBE must not be counting as a subscriber"
    );

    // And the intruder connection must have received nothing.
    assert!(
        read_line(&mut intruder, 1).await.is_none(),
        "the unauthenticated connection received pub/sub traffic"
    );
}

/// WIR-7: a binary (non-UTF-8) SET value must be refused over the wire with
/// `ERR invalid argument encoding` — the same error keys already get — and
/// leave the key absent. Pre-fix, `from_utf8_lossy` accepted the bytes,
/// stored U+FFFD corruption durably, and replied +OK.
#[tokio::test]
async fn binary_set_value_is_refused_over_the_wire() {
    let server = start().await;

    let mut client = connect(server.port).await;
    client
        .write_all(resp_array(&["AUTH", "sekret"]).as_slice())
        .await
        .unwrap();
    assert_eq!(read_line(&mut client, 5).await.unwrap(), "+OK");

    client
        .write_all(resp_array_with_raw(&["SET", "wir7"], &[0xFF, 0xFE]).as_slice())
        .await
        .unwrap();
    let reply = read_line(&mut client, 5).await.unwrap();
    assert!(
        reply.starts_with("-ERR invalid argument encoding"),
        "binary SET value must be refused, got: {reply}"
    );

    // The key must be absent, not holding U+FFFD corruption.
    client
        .write_all(resp_array(&["GET", "wir7"]).as_slice())
        .await
        .unwrap();
    let reply = read_line(&mut client, 5).await.unwrap();
    assert_eq!(reply, "$-1", "a refused SET must not leave a value behind");

    // Valid multi-byte UTF-8 still round-trips byte-exact.
    let text = "héllo";
    client
        .write_all(resp_array(&["SET", "wir7", text]).as_slice())
        .await
        .unwrap();
    assert_eq!(read_line(&mut client, 5).await.unwrap(), "+OK");
    client
        .write_all(resp_array(&["STRLEN", "wir7"]).as_slice())
        .await
        .unwrap();
    let reply = read_line(&mut client, 5).await.unwrap();
    assert_eq!(
        reply,
        format!(":{}", text.len()),
        "STRLEN must report bytes, not chars"
    );
}
