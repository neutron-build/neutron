//! A malformed frontend frame must close the connection with a protocol
//! error, not unwind the connection task.
//!
//! pgwire 0.36's Bind decoder trusts each parameter's length field and calls
//! `split_to` on it, so a Bind whose parameter length points past the bytes
//! received panics inside the codec. Found by `fuzz/fuzz_targets/wire_protocol.rs`
//! (crash-15c56d76d0b8a8ff70acb9547135c67aa28e56fb). The codec decodes every
//! message type once startup is done, so the frame is accepted while the
//! password exchange is still pending: these tests send it before AUTH.

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::{NucleusHandler, NucleusServer, process_socket_closing_on_terminate};

/// A Bind with no format codes and one parameter claiming 100 bytes, of
/// which none are sent. The frame length covers exactly what is sent.
fn short_bind() -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(b"\0\0"); // portal, statement
    body.extend_from_slice(&0u16.to_be_bytes()); // format codes
    body.extend_from_slice(&1u16.to_be_bytes()); // parameters
    body.extend_from_slice(&100i32.to_be_bytes()); // parameter length
    let mut frame = vec![b'B'];
    frame.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    frame.extend_from_slice(&body);
    frame
}

async fn serve_one(password: Option<&str>) -> (u16, tokio::task::JoinHandle<std::io::Result<()>>) {
    let dir = tempfile::tempdir().unwrap();
    let catalog = Arc::new(crate::catalog::Catalog::new());
    let engine =
        crate::storage::DiskEngine::open(&dir.path().join("nucleus.db"), catalog.clone()).unwrap();
    let storage: Arc<dyn crate::storage::StorageEngine> = Arc::new(engine);
    let ex = Arc::new(crate::executor::Executor::new(catalog, storage));
    let handler = NucleusHandler::with_password(ex, password.map(str::to_owned));
    let server = Arc::new(NucleusServer::new(Arc::new(handler)));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let task = tokio::spawn(async move {
        let _dir = dir;
        let (socket, _) = listener.accept().await.unwrap();
        process_socket_closing_on_terminate(socket, None, server).await
    });
    (port, task)
}

/// Send a StartupMessage and read until the server stops for input: an
/// authentication request ('R' other than AuthenticationOk) or ReadyForQuery.
async fn startup(stream: &mut tokio::net::TcpStream) -> u8 {
    let mut body = Vec::new();
    body.extend_from_slice(&196_608i32.to_be_bytes());
    body.extend_from_slice(b"user\0nucleus\0\0");
    stream
        .write_all(&((body.len() + 4) as i32).to_be_bytes())
        .await
        .unwrap();
    stream.write_all(&body).await.unwrap();
    loop {
        let mut header = [0u8; 5];
        stream.read_exact(&mut header).await.unwrap();
        let len = i32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize;
        let mut payload = vec![0u8; len - 4];
        stream.read_exact(&mut payload).await.unwrap();
        match header[0] {
            b'R' if payload[..4] != [0, 0, 0, 0] => return b'R',
            b'Z' => return b'Z',
            _ => {}
        }
    }
}

async fn send_short_bind_and_expect_close(port: u16, expect_state: u8) -> Vec<u8> {
    let mut client = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    assert_eq!(startup(&mut client).await, expect_state);
    client.write_all(&short_bind()).await.unwrap();
    let mut rest = Vec::new();
    tokio::time::timeout(Duration::from_secs(10), client.read_to_end(&mut rest))
        .await
        .expect("server must close the connection")
        .unwrap();
    rest
}

#[tokio::test]
async fn short_bind_before_auth_closes_without_panicking() {
    let (port, task) = serve_one(Some("sekret")).await;
    let rest = send_short_bind_and_expect_close(port, b'R').await;
    assert_eq!(rest.first(), Some(&b'E'), "expected an ErrorResponse: {rest:?}");
    assert!(
        rest.windows(5).any(|w| w == b"08P01"),
        "expected SQLSTATE 08P01: {rest:?}"
    );
    let res = task.await.expect("connection task must not panic");
    assert_eq!(res.unwrap_err().kind(), std::io::ErrorKind::InvalidData);
}

#[tokio::test]
async fn short_bind_after_startup_closes_without_panicking() {
    let (port, task) = serve_one(None).await;
    let rest = send_short_bind_and_expect_close(port, b'Z').await;
    assert!(
        rest.windows(5).any(|w| w == b"08P01"),
        "expected SQLSTATE 08P01: {rest:?}"
    );
    let res = task.await.expect("connection task must not panic");
    assert!(res.is_err());
}

