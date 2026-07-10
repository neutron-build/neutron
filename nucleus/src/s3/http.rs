//! Minimal HTTP/1.1 request/response handling for the S3 gateway.
//!
//! Hand-rolled like the other Nucleus protocol servers (pgwire, RESP,
//! binary) — no web framework. Supports Content-Length bodies (S3 clients
//! use `Content-Encoding: aws-chunked` *with* Content-Length for streaming
//! uploads; bare `Transfer-Encoding: chunked` is rejected).

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

use super::sigv4::percent_decode;

/// Hard cap on the header block (request line + headers).
const MAX_HEADER_BYTES: usize = 64 * 1024;

#[derive(Debug)]
pub struct Request {
    pub method: String,
    /// Path portion of the request target, still percent-encoded.
    pub raw_path: String,
    /// Decoded query parameters in arrival order.
    pub query: Vec<(String, String)>,
    /// Lowercased header name → value, in arrival order.
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    pub keep_alive: bool,
}

impl Request {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }

    pub fn query_param(&self, name: &str) -> Option<&str> {
        self.query
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }

    pub fn has_query(&self, name: &str) -> bool {
        self.query.iter().any(|(k, _)| k == name)
    }
}

/// A parsed request head — body not yet read (so the server can emit
/// `100 Continue` before the client streams the payload).
pub struct RequestHead {
    pub method: String,
    pub raw_path: String,
    pub query: Vec<(String, String)>,
    pub headers: Vec<(String, String)>,
    pub content_length: usize,
    pub expects_continue: bool,
    pub keep_alive: bool,
}

/// Outcome of reading one request head off the wire.
pub enum ReadOutcome {
    Head(Box<RequestHead>),
    /// Connection closed cleanly between requests.
    Closed,
    /// Protocol violation — the receiver should respond (if given a message)
    /// and drop the connection.
    Bad(&'static str),
}

pub async fn read_request_head<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut BufReader<R>,
    max_body: usize,
) -> std::io::Result<ReadOutcome> {
    // Request line
    let mut line = String::new();
    let n = reader.read_line(&mut line).await?;
    if n == 0 {
        return Ok(ReadOutcome::Closed);
    }
    let line = line.trim_end();
    let mut parts = line.split(' ');
    let (Some(method), Some(target), Some(version)) = (parts.next(), parts.next(), parts.next())
    else {
        return Ok(ReadOutcome::Bad("malformed request line"));
    };
    if !version.starts_with("HTTP/1.") {
        return Ok(ReadOutcome::Bad("unsupported HTTP version"));
    }
    let mut keep_alive = version == "HTTP/1.1";

    let (raw_path, raw_query) = match target.split_once('?') {
        Some((p, q)) => (p.to_string(), q.to_string()),
        None => (target.to_string(), String::new()),
    };

    // Headers
    let mut headers: Vec<(String, String)> = Vec::new();
    let mut header_bytes = 0usize;
    loop {
        let mut hline = String::new();
        let n = reader.read_line(&mut hline).await?;
        if n == 0 {
            return Ok(ReadOutcome::Bad("connection closed mid-headers"));
        }
        header_bytes += n;
        if header_bytes > MAX_HEADER_BYTES {
            return Ok(ReadOutcome::Bad("header block too large"));
        }
        let hline = hline.trim_end();
        if hline.is_empty() {
            break;
        }
        let Some((name, value)) = hline.split_once(':') else {
            return Ok(ReadOutcome::Bad("malformed header"));
        };
        headers.push((name.trim().to_lowercase(), value.trim().to_string()));
    }

    // Connection semantics
    if let Some(conn) = headers
        .iter()
        .find(|(k, _)| k == "connection")
        .map(|(_, v)| v.to_lowercase())
    {
        if conn.contains("close") {
            keep_alive = false;
        } else if conn.contains("keep-alive") {
            keep_alive = true;
        }
    }

    let te = headers
        .iter()
        .find(|(k, _)| k == "transfer-encoding")
        .map(|(_, v)| v.to_lowercase());
    if te.as_deref().is_some_and(|v| v.contains("chunked")) {
        return Ok(ReadOutcome::Bad("Transfer-Encoding: chunked not supported"));
    }
    let content_length = headers
        .iter()
        .find(|(k, _)| k == "content-length")
        .map(|(_, v)| v.parse::<usize>())
        .transpose()
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "bad content-length"))?
        .unwrap_or(0);
    if content_length > max_body {
        return Ok(ReadOutcome::Bad("request body too large"));
    }
    let expects_continue = headers
        .iter()
        .any(|(k, v)| k == "expect" && v.to_lowercase().contains("100-continue"));

    let query = parse_query(&raw_query);

    Ok(ReadOutcome::Head(Box::new(RequestHead {
        method: method.to_string(),
        raw_path,
        query,
        headers,
        content_length,
        expects_continue,
        keep_alive,
    })))
}

/// Read the request body and assemble the full [`Request`].
pub async fn read_request_body<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut BufReader<R>,
    head: RequestHead,
) -> std::io::Result<Request> {
    let mut body = vec![0u8; head.content_length];
    reader.read_exact(&mut body).await?;
    Ok(Request {
        method: head.method,
        raw_path: head.raw_path,
        query: head.query,
        headers: head.headers,
        body,
        keep_alive: head.keep_alive,
    })
}

/// Decode `a=1&b=x%20y` into ordered pairs. Bare keys get empty values
/// (S3 subresources like `?uploads` rely on this).
pub fn parse_query(raw: &str) -> Vec<(String, String)> {
    if raw.is_empty() {
        return Vec::new();
    }
    raw.split('&')
        .filter(|s| !s.is_empty())
        .map(|pair| match pair.split_once('=') {
            Some((k, v)) => (percent_decode(k), percent_decode(v)),
            None => (percent_decode(pair), String::new()),
        })
        .collect()
}

pub struct Response {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    /// HEAD responses advertise the length a GET would return without
    /// carrying (or even reading) the body.
    pub content_length_override: Option<u64>,
}

impl Response {
    pub fn new(status: u16) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body: Vec::new(),
            content_length_override: None,
        }
    }

    pub fn with_body(status: u16, content_type: &str, body: Vec<u8>) -> Self {
        Self {
            status,
            headers: vec![("Content-Type".to_string(), content_type.to_string())],
            body,
            content_length_override: None,
        }
    }

    pub fn header(mut self, name: &str, value: impl Into<String>) -> Self {
        self.headers.push((name.to_string(), value.into()));
        self
    }
}

fn status_text(status: u16) -> &'static str {
    match status {
        200 => "OK",
        204 => "No Content",
        206 => "Partial Content",
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        411 => "Length Required",
        412 => "Precondition Failed",
        413 => "Payload Too Large",
        416 => "Range Not Satisfiable",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        _ => "Unknown",
    }
}

pub async fn write_response<W: tokio::io::AsyncWrite + Unpin>(
    writer: &mut W,
    resp: &Response,
    keep_alive: bool,
    head_only: bool,
) -> std::io::Result<()> {
    let mut out = format!("HTTP/1.1 {} {}\r\n", resp.status, status_text(resp.status));
    for (k, v) in &resp.headers {
        out.push_str(k);
        out.push_str(": ");
        out.push_str(v);
        out.push_str("\r\n");
    }
    let content_length = resp
        .content_length_override
        .unwrap_or(resp.body.len() as u64);
    out.push_str(&format!("Content-Length: {content_length}\r\n"));
    out.push_str("Server: NucleusS3\r\n");
    if !keep_alive {
        out.push_str("Connection: close\r\n");
    }
    out.push_str("\r\n");
    writer.write_all(out.as_bytes()).await?;
    if !head_only {
        writer.write_all(&resp.body).await?;
    }
    writer.flush().await
}

// ── Time formatting ───────────────────────────────────────────────────────────

/// Civil date from days since epoch (Howard Hinnant's algorithm).
fn civil_from_unix(ts: i64) -> (i64, u32, u32, u32, u32, u32, u32) {
    let days = ts.div_euclid(86400);
    let secs = ts.rem_euclid(86400);
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = z.rem_euclid(146097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let year = if m <= 2 { y + 1 } else { y };
    let dow = (days + 4).rem_euclid(7) as u32; // 1970-01-01 was a Thursday (dow 4=Thu? 0=Sun)
    (
        year,
        m,
        d,
        (secs / 3600) as u32,
        ((secs % 3600) / 60) as u32,
        (secs % 60) as u32,
        dow,
    )
}

/// RFC 7231 date for HTTP headers, e.g. `Fri, 24 May 2013 00:00:00 GMT`.
pub fn http_date(unix_ts: i64) -> String {
    const DAYS: [&str; 7] = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    let (y, m, d, hh, mm, ss, dow) = civil_from_unix(unix_ts);
    format!(
        "{}, {:02} {} {} {:02}:{:02}:{:02} GMT",
        DAYS[dow as usize],
        d,
        MONTHS[(m - 1) as usize],
        y,
        hh,
        mm,
        ss
    )
}

/// ISO 8601 timestamp for S3 XML bodies, e.g. `2013-05-24T00:00:00.000Z`.
pub fn iso8601(unix_ts: i64) -> String {
    let (y, m, d, hh, mm, ss, _) = civil_from_unix(unix_ts);
    format!("{y:04}-{m:02}-{d:02}T{hh:02}:{mm:02}:{ss:02}.000Z")
}

// ── XML helpers ───────────────────────────────────────────────────────────────

pub fn xml_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            _ => out.push(c),
        }
    }
    out
}

/// Extract the text of every `<tag>...</tag>` occurrence (no attribute or
/// nesting support — S3 request bodies like CompleteMultipartUpload and
/// Delete are flat).
pub fn xml_extract_all(body: &str, tag: &str) -> Vec<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let mut out = Vec::new();
    let mut rest = body;
    while let Some(start) = rest.find(&open) {
        let after = &rest[start + open.len()..];
        let Some(end) = after.find(&close) else {
            break;
        };
        out.push(xml_unescape(after[..end].trim()));
        rest = &after[end + close.len()..];
    }
    out
}

fn xml_unescape(s: &str) -> String {
    s.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn date_formatting() {
        // 2013-05-24 00:00:00 UTC was a Friday.
        assert_eq!(http_date(1369353600), "Fri, 24 May 2013 00:00:00 GMT");
        assert_eq!(iso8601(1369353600), "2013-05-24T00:00:00.000Z");
        assert_eq!(http_date(0), "Thu, 01 Jan 1970 00:00:00 GMT");
        // Leap day.
        assert_eq!(iso8601(1709208000), "2024-02-29T12:00:00.000Z");
    }

    #[test]
    fn query_parsing() {
        let q = parse_query("list-type=2&prefix=a%2Fb&uploads");
        assert_eq!(q[0], ("list-type".to_string(), "2".to_string()));
        assert_eq!(q[1], ("prefix".to_string(), "a/b".to_string()));
        assert_eq!(q[2], ("uploads".to_string(), String::new()));
    }

    #[test]
    fn xml_roundtrip() {
        assert_eq!(xml_escape("a<b>&\"c\""), "a&lt;b&gt;&amp;&quot;c&quot;");
        let body = "<Delete><Object><Key>a&amp;b</Key></Object>\
                    <Object><Key>c</Key></Object></Delete>";
        assert_eq!(xml_extract_all(body, "Key"), vec!["a&b", "c"]);
    }
}
