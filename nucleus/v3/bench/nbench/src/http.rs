//! Minimal HTTP/1.1 client for localhost JSON APIs (Qdrant REST).
//!
//! Invariant owned here: no TLS, no redirects, no compression — only what
//! a same-host Qdrant needs. Response bodies may be length-delimited,
//! connection-close delimited or chunked; all three are decoded, and a
//! truncated response is an error (never a short silent body, which would
//! corrupt vector-search results).

use crate::error::{Error, Result};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

/// A parsed HTTP response.
pub struct Response {
    /// Status code.
    pub status: u16,
    /// Decoded body bytes.
    pub body: Vec<u8>,
}

/// How a response body is framed, decided from the headers.
enum Framing {
    /// Exactly `usize` bytes follow the header block.
    ContentLength(usize),
    /// Chunked transfer coding.
    Chunked,
    /// No length information: the body ends when the peer closes.
    UntilClose,
}

/// Parse the status line and framing headers from a raw header block.
fn parse_head(head_text: &str) -> Result<(u16, Framing)> {
    let mut lines = head_text.lines();
    let status_line = lines.next().ok_or_else(|| Error::Parse {
        what: "http status",
        line: "empty response".into(),
    })?;
    let status: u16 = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| Error::Parse {
            what: "http status",
            line: status_line.to_string(),
        })?;
    let mut framing = Framing::UntilClose;
    for line in lines {
        let Some((k, v)) = line.split_once(':') else {
            continue;
        };
        if k.trim().eq_ignore_ascii_case("content-length")
            && let Some(n) = v.trim().parse::<usize>().ok()
        {
            framing = Framing::ContentLength(n);
        }
        if k.trim().eq_ignore_ascii_case("transfer-encoding")
            && v.trim().eq_ignore_ascii_case("chunked")
        {
            framing = Framing::Chunked;
        }
    }
    Ok((status, framing))
}

/// Perform one request. `body` may be empty for GET.
pub fn request(
    host: &str,
    port: u16,
    method: &str,
    path: &str,
    body: &[u8],
    timeout: Duration,
) -> Result<Response> {
    let mut stream = TcpStream::connect((host, port)).map_err(Error::Io)?;
    stream.set_read_timeout(Some(timeout)).map_err(Error::Io)?;
    stream.set_write_timeout(Some(timeout)).map_err(Error::Io)?;

    let head = format!(
        "{method} {path} HTTP/1.1\r\nHost: {host}:{port}\r\nConnection: close\r\n\
         Content-Length: {}\r\nContent-Type: application/json\r\nAccept: */*\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).map_err(Error::Io)?;
    stream.write_all(body).map_err(Error::Io)?;
    stream.flush().map_err(Error::Io)?;

    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).map_err(Error::Io)?;

    // Split headers from body.
    let sep = find_subsequence(&raw, b"\r\n\r\n").ok_or_else(|| Error::Parse {
        what: "http response",
        line: "no header terminator".into(),
    })?;
    let head_bytes = &raw[..sep];
    let body_bytes = &raw[sep + 4..];

    let head_text = String::from_utf8_lossy(head_bytes).to_string();
    let (status, framing) = parse_head(&head_text)?;

    let decoded;
    let body_bytes = match framing {
        Framing::Chunked => {
            decoded = dechunk(body_bytes)?;
            &decoded
        }
        Framing::ContentLength(_) | Framing::UntilClose => body_bytes,
    };

    Ok(Response {
        status,
        body: body_bytes.to_vec(),
    })
}

/// A persistent HTTP/1.1 connection (keep-alive).
///
/// Invariant owned here (gate nv3-B-0-gate-1 finding 5): the Qdrant
/// search pass times per-query requests, and a one-shot `Connection:
/// close` request pays TCP setup per query while the pgvector arm times
/// one psql session — the arms were not overhead-matched. A
/// [`Connection`] keeps one socket for the whole pass. It never
/// reconnects or retries: a mid-measurement failure surfaces as an
/// error so the row is re-run (the "measurement requests never retry"
/// invariant in `vector.rs`).
pub struct Connection {
    stream: TcpStream,
    host: String,
    port: u16,
    /// Bytes read past the current response frame (none in the
    /// request/response lockstep this client keeps, but carried, never
    /// dropped, if a server ever pipelines).
    pending: Vec<u8>,
    /// Set when the server signalled `Connection: close` and the body
    /// was read to EOF; the next request is an error, not a silent
    /// reconnect.
    closed: bool,
}

impl Connection {
    /// Open a keep-alive connection.
    pub fn connect(host: &str, port: u16, timeout: Duration) -> Result<Connection> {
        let stream = TcpStream::connect((host, port)).map_err(Error::Io)?;
        stream.set_read_timeout(Some(timeout)).map_err(Error::Io)?;
        stream.set_write_timeout(Some(timeout)).map_err(Error::Io)?;
        Ok(Connection {
            stream,
            host: host.to_string(),
            port,
            pending: Vec::new(),
            closed: false,
        })
    }

    /// Perform one request on the open socket. Errors if the server
    /// already closed the connection; callers that need to continue
    /// re-run, they never silently reconnect inside a measurement.
    pub fn request(&mut self, method: &str, path: &str, body: &[u8]) -> Result<Response> {
        if self.closed {
            return Err(Error::Usage(
                "keep-alive connection was closed by the server; reopen explicitly".into(),
            ));
        }
        let head = format!(
            "{method} {path} HTTP/1.1\r\nHost: {}:{}\r\nConnection: keep-alive\r\n\
             Content-Length: {}\r\nContent-Type: application/json\r\nAccept: */*\r\n\r\n",
            self.host,
            self.port,
            body.len()
        );
        self.stream.write_all(head.as_bytes()).map_err(Error::Io)?;
        self.stream.write_all(body).map_err(Error::Io)?;
        self.stream.flush().map_err(Error::Io)?;

        let mut buf = std::mem::take(&mut self.pending);
        // Read until one complete response frame sits in `buf`.
        let (sep, framing) = loop {
            if let Some(sep) = find_subsequence(&buf, b"\r\n\r\n") {
                let head_text = String::from_utf8_lossy(&buf[..sep]).to_string();
                let (_, framing) = parse_head(&head_text)?;
                let complete = match framing {
                    Framing::ContentLength(n) => buf.len() >= sep + 4 + n,
                    Framing::Chunked => try_dechunk(&buf[sep + 4..])?.is_some(),
                    // no length information: only EOF terminates this body
                    Framing::UntilClose => false,
                };
                if complete {
                    break (sep, framing);
                }
            }
            let mut chunk = [0u8; 8192];
            let n = self.stream.read(&mut chunk).map_err(Error::Io)?;
            if n == 0 {
                if let Some(sep) = find_subsequence(&buf, b"\r\n\r\n") {
                    let head_text = String::from_utf8_lossy(&buf[..sep]).to_string();
                    if let (_, Framing::UntilClose) = parse_head(&head_text)? {
                        break (sep, Framing::UntilClose);
                    }
                }
                return Err(Error::Parse {
                    what: "http response",
                    line: "connection closed mid-response".into(),
                });
            }
            buf.extend_from_slice(&chunk[..n]);
        };

        let head_text = String::from_utf8_lossy(&buf[..sep]).to_string();
        let (status, _) = parse_head(&head_text)?;
        let consumed;
        let decoded;
        let body: Vec<u8> = match framing {
            Framing::ContentLength(n) => {
                let start = sep + 4;
                consumed = start + n;
                buf[start..consumed].to_vec()
            }
            Framing::Chunked => {
                let Some((body, body_len)) = try_dechunk(&buf[sep + 4..])? else {
                    // unreachable: the loop only breaks on completeness
                    return Err(Error::Parse {
                        what: "http chunk",
                        line: "incomplete chunked body".into(),
                    });
                };
                decoded = body;
                consumed = sep + 4 + body_len;
                decoded
            }
            Framing::UntilClose => {
                self.closed = true;
                consumed = buf.len();
                buf[sep + 4..].to_vec()
            }
        };
        // bytes past this frame belong to the next response; carry them
        self.pending = buf.split_off(consumed);
        Ok(Response { status, body })
    }
}

fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || haystack.len() < needle.len() {
        return None;
    }
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// Incremental chunked decoding: `Ok(None)` means "need more bytes",
/// any `Err` is a malformed body (never a silent short read).
/// `Ok(Some((body, consumed)))` returns the decoded body and how many
/// bytes of `data` the chunked stream consumed (its 0-chunk included),
/// so a keep-alive connection knows where the next response starts.
fn try_dechunk(data: &[u8]) -> Result<Option<(Vec<u8>, usize)>> {
    let mut out = Vec::new();
    let mut pos = 0;
    loop {
        let Some(rel) = find_subsequence(&data[pos..], b"\r\n") else {
            return Ok(None);
        };
        let line_end = rel + pos;
        let size_text = std::str::from_utf8(&data[pos..line_end])
            .map_err(|_| Error::Parse {
                what: "http chunk",
                line: "bad chunk size".into(),
            })?
            .split(';')
            .next()
            .unwrap_or("")
            .trim()
            .to_string();
        let size = usize::from_str_radix(&size_text, 16).map_err(|_| Error::Parse {
            what: "http chunk",
            line: format!("bad chunk size {size_text:?}"),
        })?;
        pos = line_end + 2;
        if size == 0 {
            // the 0-chunk's trailing CRLF must be present too
            if data.len() < pos + 2 {
                return Ok(None);
            }
            return Ok(Some((out, pos + 2)));
        }
        let end = pos + size;
        if end + 2 > data.len() {
            return Ok(None);
        }
        out.extend_from_slice(&data[pos..end]);
        pos = end + 2; // skip trailing CRLF
    }
}

/// Decode a complete chunked body (a short body is an error).
fn dechunk(data: &[u8]) -> Result<Vec<u8>> {
    match try_dechunk(data)? {
        Some((body, _)) => Ok(body),
        None => Err(Error::Parse {
            what: "http chunk",
            line: "truncated chunked body".into(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn dechunk_joins_bodies() {
        let data = b"4\r\nWiki\r\n5\r\npedia\r\n0\r\n\r\n";
        assert_eq!(dechunk(data).expect("dechunk"), b"Wikipedia");
    }

    #[test]
    fn dechunk_rejects_truncation() {
        assert!(dechunk(b"4\r\nWik").is_err());
        assert!(dechunk(b"zz\r\nxx").is_err());
        assert!(dechunk(b"").is_err());
        // a 0-chunk without its trailing CRLF is not yet complete
        assert!(dechunk(b"0\r\n").is_err());
    }

    #[test]
    fn try_dechunk_reports_consumed_length() {
        // two chunked frames back to back: the first must consume
        // exactly its own bytes, never into the next frame
        let data = b"4\r\nWiki\r\n0\r\n\r\nNEXT-FRAME";
        let (body, used) = try_dechunk(data).expect("complete").expect("some");
        assert_eq!(body, b"Wiki");
        assert_eq!(used, 14); // "4\r\nWiki\r\n" + "0\r\n" + final "\r\n"
        assert_eq!(&data[used..], b"NEXT-FRAME");
        // "0\r\n\r\n" appearing inside a chunk body must not end the
        // stream early (regression: consumed used to be scanned)
        let tricky = b"8\r\nXX0\r\n\r\nY\r\n0\r\n\r\n";
        let (body, used) = try_dechunk(tricky).expect("complete").expect("some");
        assert_eq!(body, b"XX0\r\n\r\nY");
        assert_eq!(used, tricky.len());
        assert!(try_dechunk(b"3\r\nab").expect("err-free").is_none());
    }

    #[test]
    fn find_subsequence_finds_and_rejects() {
        assert_eq!(find_subsequence(b"abc\r\n\r\ndef", b"\r\n\r\n"), Some(3));
        assert_eq!(find_subsequence(b"abc", b"\r\n\r\n"), None);
        assert_eq!(find_subsequence(b"abc", b""), None);
    }

    #[test]
    fn connect_refused_is_an_error_not_a_panic() {
        // Port 1 on localhost: nothing listens there in test environments.
        let r = request("127.0.0.1", 1, "GET", "/", b"", Duration::from_secs(2));
        assert!(r.is_err());
    }

    /// A keep-alive test server: accepts exactly one connection and
    /// serves `script.len()` Content-Length-framed responses on it,
    /// each with body `script[i]`. Returns (port, accepts, requests).
    fn keepalive_server(script: Vec<&'static str>) -> (u16, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let port = listener.local_addr().expect("addr").port();
        let accepts = Arc::new(AtomicUsize::new(0));
        let requests = Arc::new(AtomicUsize::new(0));
        let (a, r) = (Arc::clone(&accepts), Arc::clone(&requests));
        std::thread::spawn(move || {
            let Ok((mut conn, _)) = listener.accept() else {
                return;
            };
            a.fetch_add(1, Ordering::SeqCst);
            let mut buf = [0u8; 4096];
            for body in script {
                // wait for one full request head (ends with the blank line)
                loop {
                    let n = conn.read(&mut buf).expect("read request");
                    let got = &buf[..n];
                    if find_subsequence(got, b"\r\n\r\n").is_some() {
                        break;
                    }
                }
                r.fetch_add(1, Ordering::SeqCst);
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}",
                    body.len(),
                    body
                );
                conn.write_all(resp.as_bytes()).expect("write response");
            }
        });
        (port, accepts, requests)
    }

    #[test]
    fn keepalive_connection_serves_many_requests_on_one_socket() {
        // Finding 5 (gate nv3-B-0-gate-1): the qdrant search pass must
        // not pay TCP setup per query. One Connection, N requests, one
        // accept on the server side.
        let (port, accepts, requests) = keepalive_server(vec!["a", "b", "c", "d"]);
        let mut conn =
            Connection::connect("127.0.0.1", port, Duration::from_secs(5)).expect("connect");
        for expected in ["a", "b", "c", "d"] {
            let resp = conn.request("POST", "/points/search", b"{}").expect("req");
            assert_eq!(resp.status, 200);
            assert_eq!(resp.body, expected.as_bytes());
        }
        assert_eq!(accepts.load(Ordering::SeqCst), 1, "one socket expected");
        assert_eq!(requests.load(Ordering::SeqCst), 4);
    }

    #[test]
    fn keepalive_survives_bodies_containing_header_like_bytes() {
        // a body containing \r\n\r\n and "0\r\n" sequences must not
        // confuse the framing (Content-Length governs)
        let body = "pre\r\n\r\n0\r\npost";
        let (port, _accepts, _requests) = keepalive_server(vec![body, "next"]);
        let mut conn =
            Connection::connect("127.0.0.1", port, Duration::from_secs(5)).expect("connect");
        let r1 = conn.request("POST", "/x", b"{}").expect("req1");
        assert_eq!(r1.body, body.as_bytes());
        let r2 = conn.request("POST", "/x", b"{}").expect("req2");
        assert_eq!(r2.body, b"next");
    }

    #[test]
    fn server_initiated_close_ends_the_connection_loudly() {
        // The server answers once with Connection: close; the response
        // is delivered (close-delimited body) and the *next* request is
        // an explicit error — never a silent reconnect, which would
        // hide transport stalls inside a timed measurement.
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let port = listener.local_addr().expect("addr").port();
        std::thread::spawn(move || {
            use std::io::Write as _;
            let Ok((mut conn, _)) = listener.accept() else {
                return;
            };
            let mut buf = [0u8; 1024];
            let _ = conn.read(&mut buf);
            let resp = "HTTP/1.1 200 OK\r\nConnection: close\r\n\r\nbye";
            let _ = conn.write_all(resp.as_bytes());
            // socket closes when the thread ends
        });
        let mut conn =
            Connection::connect("127.0.0.1", port, Duration::from_secs(5)).expect("connect");
        let r = conn
            .request("GET", "/", b"")
            .expect("close-delimited response");
        assert_eq!(r.status, 200);
        assert_eq!(r.body, b"bye");
        assert!(conn.request("GET", "/", b"").is_err());
    }

    #[test]
    fn truncated_keepalive_response_is_an_error() {
        // server sends headers promising 10 bytes, then closes
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let port = listener.local_addr().expect("addr").port();
        std::thread::spawn(move || {
            use std::io::Write as _;
            let Ok((mut conn, _)) = listener.accept() else {
                return;
            };
            let mut buf = [0u8; 1024];
            let _ = conn.read(&mut buf);
            let _ = conn.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nshort");
            // closes -> promised length never arrives
        });
        let mut conn =
            Connection::connect("127.0.0.1", port, Duration::from_secs(5)).expect("connect");
        assert!(conn.request("GET", "/", b"").is_err());
    }
}
