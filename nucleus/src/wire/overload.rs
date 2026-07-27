//! Refusing a connection the way a PostgreSQL client understands it.
//!
//! When the server is at `max_connections` the socket used to be dropped on
//! the floor. A client sees that as `server closed the connection
//! unexpectedly` — indistinguishable from a crash, a firewall, or a TLS
//! mismatch, which sends operators debugging the wrong thing.
//!
//! PostgreSQL instead answers with an `ErrorResponse` carrying severity
//! `FATAL` and SQLSTATE `53300` (`too_many_connections`), so `psql` prints
//! `FATAL: sorry, too many clients already`. This module writes that frame.
//!
//! Both pre-startup shapes are handled by the same reply: whether the client's
//! first packet is an `SSLRequest` or a `StartupMessage`, libpq accepts an
//! `ErrorResponse` at that point and surfaces its message.

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// SQLSTATE for "too many clients already".
pub const SQLSTATE_TOO_MANY_CONNECTIONS: &str = "53300";

/// Build a PostgreSQL `ErrorResponse` ('E') frame.
///
/// Layout: `'E'` | int32 length | field(code, value NUL)... | NUL
pub fn error_response_frame(severity: &str, sqlstate: &str, message: &str, hint: &str) -> Vec<u8> {
    let mut body = Vec::with_capacity(64 + message.len() + hint.len());
    let mut field = |code: u8, value: &str| {
        body.push(code);
        body.extend_from_slice(value.as_bytes());
        body.push(0);
    };
    // 'S' is the localised severity, 'V' the non-localised one (PG 9.6+).
    field(b'S', severity);
    field(b'V', severity);
    field(b'C', sqlstate);
    field(b'M', message);
    if !hint.is_empty() {
        field(b'H', hint);
    }
    body.push(0); // terminator

    let mut frame = Vec::with_capacity(body.len() + 5);
    frame.push(b'E');
    // Length covers the int32 itself but not the leading type byte.
    frame.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
    frame.extend_from_slice(&body);
    frame
}

/// The exact frame sent when the connection limit is reached.
pub fn too_many_connections_frame(max_connections: usize) -> Vec<u8> {
    error_response_frame(
        "FATAL",
        SQLSTATE_TOO_MANY_CONNECTIONS,
        "sorry, too many clients already",
        &format!(
            "The server is at its connection limit of {max_connections}. \
             Raise server.max_connections in nucleus.toml (or \
             NUCLEUS_SERVER_MAX_CONNECTIONS), or reduce client pool sizes."
        ),
    )
}

/// Tell a client it was refused for hitting the connection limit, then close.
///
/// Runs on its own task so the accept loop is never delayed by a slow or
/// unresponsive client. The short read before writing drains the client's
/// first packet: closing with unread data queued can surface as a TCP reset
/// on some platforms, which would hide the error message we just wrote.
pub async fn refuse_too_many_connections<S>(mut socket: S, max_connections: usize)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let frame = too_many_connections_frame(max_connections);
    let mut scratch = [0u8; 1024];
    let _ = tokio::time::timeout(Duration::from_millis(250), socket.read(&mut scratch)).await;
    let _ = tokio::time::timeout(Duration::from_millis(250), socket.write_all(&frame)).await;
    let _ = tokio::time::timeout(Duration::from_millis(250), socket.flush()).await;
    let _ = socket.shutdown().await;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Minimal parser for the frame, so the test asserts the wire bytes a
    /// client would actually decode rather than the builder's own structure.
    fn parse_error_frame(buf: &[u8]) -> Vec<(u8, String)> {
        assert_eq!(buf[0], b'E', "not an ErrorResponse frame");
        let len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
        assert_eq!(
            len + 1,
            buf.len(),
            "declared length {len} does not match frame size {}",
            buf.len()
        );
        let mut fields = Vec::new();
        let mut i = 5;
        while i < buf.len() && buf[i] != 0 {
            let code = buf[i];
            i += 1;
            let start = i;
            while i < buf.len() && buf[i] != 0 {
                i += 1;
            }
            fields.push((code, String::from_utf8_lossy(&buf[start..i]).into_owned()));
            i += 1; // skip the NUL
        }
        fields
    }

    #[test]
    fn frame_is_a_wellformed_fatal_53300() {
        let frame = too_many_connections_frame(100);
        let fields = parse_error_frame(&frame);
        let get = |c: u8| {
            fields
                .iter()
                .find(|(code, _)| *code == c)
                .map(|(_, v)| v.clone())
                .unwrap_or_else(|| panic!("missing field {}", c as char))
        };
        assert_eq!(get(b'S'), "FATAL");
        assert_eq!(get(b'V'), "FATAL");
        assert_eq!(get(b'C'), "53300");
        assert_eq!(get(b'M'), "sorry, too many clients already");
        // The hint must be actionable: name the knob and its current value.
        let hint = get(b'H');
        assert!(hint.contains("100"), "{hint}");
        assert!(hint.contains("max_connections"), "{hint}");
        assert_eq!(*frame.last().unwrap(), 0, "frame must be NUL-terminated");
    }

    /// Prove the refusal actually reaches a peer over a real byte stream,
    /// not just that the builder returns bytes.
    #[tokio::test]
    async fn refusal_is_delivered_to_the_peer_and_closes_the_socket() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            refuse_too_many_connections(socket, 7).await;
        });

        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        // An SSLRequest, which is what libpq sends first by default.
        client.write_all(&8u32.to_be_bytes()).await.unwrap();
        client.write_all(&80877103u32.to_be_bytes()).await.unwrap();

        let mut received = Vec::new();
        tokio::time::timeout(
            Duration::from_secs(5),
            client.read_to_end(&mut received),
        )
        .await
        .expect("server should reply and close, not hang")
        .unwrap();

        let fields = parse_error_frame(&received);
        assert!(fields.contains(&(b'C', "53300".to_string())), "{fields:?}");
        assert!(fields.contains(&(b'S', "FATAL".to_string())), "{fields:?}");
        server.await.unwrap();
    }
}
