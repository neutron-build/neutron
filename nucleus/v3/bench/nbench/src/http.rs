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
    let mut body_bytes = &raw[sep + 4..];

    let head_text = String::from_utf8_lossy(head_bytes).to_string();
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

    let mut chunked = false;
    for line in lines {
        if let Some((k, v)) = line.split_once(':')
            && k.trim().eq_ignore_ascii_case("transfer-encoding")
            && v.trim().eq_ignore_ascii_case("chunked")
        {
            chunked = true;
        }
    }

    let decoded;
    if chunked {
        decoded = dechunk(body_bytes)?;
        body_bytes = &decoded;
    }

    Ok(Response {
        status,
        body: body_bytes.to_vec(),
    })
}

fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || haystack.len() < needle.len() {
        return None;
    }
    haystack.windows(needle.len()).position(|w| w == needle)
}

fn dechunk(data: &[u8]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    let mut pos = 0;
    loop {
        let line_end = find_subsequence(&data[pos..], b"\r\n").ok_or_else(|| Error::Parse {
            what: "http chunk",
            line: format!("truncated chunk header at {pos}"),
        })? + pos;
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
            return Ok(out);
        }
        let end = pos + size;
        if end > data.len() {
            return Err(Error::Parse {
                what: "http chunk",
                line: format!("truncated chunk body (need {size} at {pos})"),
            });
        }
        out.extend_from_slice(&data[pos..end]);
        pos = end + 2; // skip trailing CRLF
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
