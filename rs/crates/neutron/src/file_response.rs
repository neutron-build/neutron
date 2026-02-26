//! Named file response type with streaming, range requests, and conditional support.
//!
//! `NamedFile` opens a file on disk, detects its content type, and streams it
//! as an HTTP response. It supports:
//!
//! - Content-Type detection from file extension
//! - Content-Length header
//! - ETag generation (SHA-256 of size + mtime)
//! - `If-None-Match` → 304 Not Modified
//! - `Accept-Ranges: bytes`
//! - `Range` header → 206 Partial Content (single ranges only)
//! - `Last-Modified` header
//!
//! # Example
//!
//! ```rust,ignore
//! use neutron::file_response::NamedFile;
//!
//! async fn download() -> NamedFile {
//!     NamedFile::open("./files/report.pdf").await.unwrap()
//! }
//! ```

use std::convert::Infallible;
use std::io;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use bytes::Bytes;
use http::StatusCode;
use http_body::Frame;
use http_body_util::StreamBody;
use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;

use tokio_stream::Stream;

use crate::handler::{Body, IntoResponse, Request, Response};

/// Default chunk size for streaming file reads (64 KiB).
const CHUNK_SIZE: usize = 64 * 1024;

/// A file response that streams the file body and sets appropriate headers.
///
/// Use [`NamedFile::open`] to create one, then either call `.into_response()`
/// for an unconditional 200, or `.into_conditional_response(&req)` to handle
/// `If-None-Match` and `Range` headers.
#[derive(Debug)]
pub struct NamedFile {
    path: PathBuf,
    content_type: &'static str,
    file_size: u64,
    etag: String,
    last_modified: Option<String>,
}

impl NamedFile {
    /// Open a file and read its metadata. Returns an `io::Error` if the file
    /// does not exist or cannot be read.
    pub async fn open(path: impl Into<PathBuf>) -> io::Result<Self> {
        let path = path.into();

        let metadata = tokio::fs::metadata(&path).await?;
        if !metadata.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "path is not a file",
            ));
        }

        let file_size = metadata.len();
        let content_type = content_type_for_path(&path);

        let mtime_secs = metadata
            .modified()
            .ok()
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let etag = generate_etag(file_size, mtime_secs);
        let last_modified = format_last_modified(mtime_secs);

        Ok(Self {
            path,
            content_type,
            file_size,
            etag,
            last_modified: Some(last_modified),
        })
    }

    /// The detected content type.
    pub fn content_type(&self) -> &str {
        self.content_type
    }

    /// The file size in bytes.
    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    /// The computed ETag value (quoted).
    pub fn etag(&self) -> &str {
        &self.etag
    }

    /// Build a response that respects conditional (`If-None-Match`) and range
    /// (`Range`) headers from the request.
    ///
    /// - If the ETag matches `If-None-Match`, returns **304 Not Modified**.
    /// - If a valid `Range` header is present, returns **206 Partial Content**
    ///   with the requested byte range.
    /// - If the `Range` is unsatisfiable, returns **416 Range Not Satisfiable**.
    /// - Otherwise returns a full **200 OK** response.
    pub fn into_conditional_response(self, req: &Request) -> Response {
        // --- If-None-Match → 304 ------------------------------------------------
        if let Some(client_etag) = req
            .headers()
            .get("if-none-match")
            .and_then(|v| v.to_str().ok())
        {
            if etags_match(client_etag, &self.etag) {
                return self.not_modified_response();
            }
        }

        // --- Range → 206 / 416 --------------------------------------------------
        if let Some(range_val) = req
            .headers()
            .get("range")
            .and_then(|v| v.to_str().ok())
        {
            return self.range_response(range_val);
        }

        // --- Full 200 -----------------------------------------------------------
        self.full_response()
    }

    // -------------------------------------------------------------------------
    // Private response builders
    // -------------------------------------------------------------------------

    /// Build a full 200 response streaming the entire file.
    fn full_response(self) -> Response {
        let file_size = self.file_size;
        let path = self.path.clone();

        let stream = file_byte_stream(path, 0, file_size);
        let body = Body::stream(StreamBody::new(stream));

        let mut builder = http::Response::builder()
            .status(StatusCode::OK)
            .header("content-type", self.content_type)
            .header("content-length", self.file_size)
            .header("accept-ranges", "bytes")
            .header("etag", &self.etag);

        if let Some(ref lm) = self.last_modified {
            builder = builder.header("last-modified", lm);
        }

        builder.body(body).unwrap()
    }

    /// 304 Not Modified (empty body).
    fn not_modified_response(self) -> Response {
        let mut builder = http::Response::builder()
            .status(StatusCode::NOT_MODIFIED)
            .header("etag", &self.etag);

        if let Some(ref lm) = self.last_modified {
            builder = builder.header("last-modified", lm);
        }

        builder.body(Body::empty()).unwrap()
    }

    /// 206 Partial Content (or 416 if the range is invalid).
    fn range_response(self, range_header: &str) -> Response {
        let file_size = self.file_size;

        match parse_range(range_header, file_size) {
            Some((start, end)) => {
                let content_length = end - start + 1;
                let content_range =
                    format!("bytes {start}-{end}/{file_size}");
                let path = self.path.clone();

                let stream = file_byte_stream(path, start, content_length);
                let body = Body::stream(StreamBody::new(stream));

                let mut builder = http::Response::builder()
                    .status(StatusCode::PARTIAL_CONTENT)
                    .header("content-type", self.content_type)
                    .header("content-length", content_length)
                    .header("content-range", content_range)
                    .header("accept-ranges", "bytes")
                    .header("etag", &self.etag);

                if let Some(ref lm) = self.last_modified {
                    builder = builder.header("last-modified", lm);
                }

                builder.body(body).unwrap()
            }
            None => {
                // 416 Range Not Satisfiable
                let content_range = format!("bytes */{file_size}");
                http::Response::builder()
                    .status(StatusCode::RANGE_NOT_SATISFIABLE)
                    .header("content-range", content_range)
                    .body(Body::empty())
                    .unwrap()
            }
        }
    }
}

// ---------------------------------------------------------------------------
// IntoResponse — unconditional full response
// ---------------------------------------------------------------------------

impl IntoResponse for NamedFile {
    fn into_response(self) -> Response {
        self.full_response()
    }
}

// ---------------------------------------------------------------------------
// Streaming helpers
// ---------------------------------------------------------------------------

/// Create a stream of `Frame<Bytes>` reading `length` bytes starting at
/// `offset` from the file at `path`.
fn file_byte_stream(
    path: PathBuf,
    offset: u64,
    length: u64,
) -> impl Stream<Item = Result<Frame<Bytes>, Infallible>> {
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Frame<Bytes>, Infallible>>(2);

    tokio::spawn(async move {
        let Ok(mut file) = tokio::fs::File::open(&path).await else {
            return;
        };

        if offset > 0 {
            use tokio::io::AsyncSeekExt;
            if file
                .seek(std::io::SeekFrom::Start(offset))
                .await
                .is_err()
            {
                return;
            }
        }

        let mut remaining = length;
        let mut buf = vec![0u8; CHUNK_SIZE];

        while remaining > 0 {
            let to_read = (remaining as usize).min(CHUNK_SIZE);
            match file.read(&mut buf[..to_read]).await {
                Ok(0) => break,
                Ok(n) => {
                    remaining -= n as u64;
                    let frame = Frame::data(Bytes::copy_from_slice(&buf[..n]));
                    if tx.send(Ok(frame)).await.is_err() {
                        break; // receiver dropped
                    }
                }
                Err(_) => break,
            }
        }
    });

    tokio_stream::wrappers::ReceiverStream::new(rx)
}

// ---------------------------------------------------------------------------
// Range header parsing (single byte ranges only)
// ---------------------------------------------------------------------------

/// Parse a `Range: bytes=START-END` header value, returning inclusive
/// `(start, end)`. Returns `None` for invalid or multi-range requests.
fn parse_range(header: &str, file_size: u64) -> Option<(u64, u64)> {
    let header = header.trim();
    let suffix = header.strip_prefix("bytes=")?;

    // We only support a single range (no commas).
    if suffix.contains(',') {
        return None;
    }

    let (start_str, end_str) = suffix.split_once('-')?;
    let start_str = start_str.trim();
    let end_str = end_str.trim();

    if start_str.is_empty() {
        // Suffix range: bytes=-500 means last 500 bytes
        let suffix_len: u64 = end_str.parse().ok()?;
        if suffix_len == 0 || suffix_len > file_size {
            return None;
        }
        let start = file_size - suffix_len;
        Some((start, file_size - 1))
    } else {
        let start: u64 = start_str.parse().ok()?;
        let end = if end_str.is_empty() {
            file_size - 1
        } else {
            end_str.parse::<u64>().ok()?
        };

        if start > end || start >= file_size {
            return None;
        }

        // Clamp end to file_size - 1
        let end = end.min(file_size - 1);
        Some((start, end))
    }
}

// ---------------------------------------------------------------------------
// ETag generation
// ---------------------------------------------------------------------------

/// Generate a quoted ETag from file size and modification time using SHA-256.
///
/// The input `"{size}-{mtime_secs}"` is hashed and the first 16 hex characters
/// are used as the ETag value.
fn generate_etag(size: u64, mtime_secs: u64) -> String {
    let input = format!("{size}-{mtime_secs}");
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    let hash = hasher.finalize();
    let hex: String = hash.iter().take(8).map(|b| format!("{b:02x}")).collect();
    format!("\"{hex}\"")
}

// ---------------------------------------------------------------------------
// ETag comparison
// ---------------------------------------------------------------------------

/// Check whether a client-supplied `If-None-Match` value matches our ETag.
///
/// Handles both `"etag"` and `W/"etag"` forms, as well as the `*` wildcard.
fn etags_match(client_header: &str, server_etag: &str) -> bool {
    let client_header = client_header.trim();

    // Wildcard
    if client_header == "*" {
        return true;
    }

    // The header may contain multiple comma-separated ETags.
    for tag in client_header.split(',') {
        let tag = tag.trim();
        // Strip optional weak validator prefix
        let tag = tag.strip_prefix("W/").unwrap_or(tag);
        let server = server_etag.strip_prefix("W/").unwrap_or(server_etag);
        if tag == server {
            return true;
        }
    }

    false
}

// ---------------------------------------------------------------------------
// Last-Modified formatting
// ---------------------------------------------------------------------------

/// Format a Unix timestamp as an HTTP-date (RFC 7231 §7.1.1.1).
///
/// Example: `Sun, 06 Nov 1994 08:49:37 GMT`
fn format_last_modified(secs: u64) -> String {
    // We do a simple manual formatting to avoid pulling in chrono/time.
    // This implements a minimal RFC 7231 date formatter.
    let days_since_epoch = secs / 86400;
    let time_of_day = secs % 86400;

    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;

    // Day of week (Jan 1 1970 was Thursday = 4)
    let dow = ((days_since_epoch + 4) % 7) as usize;
    let day_names = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];

    // Convert days since epoch to year/month/day using a civil calendar algorithm.
    let (year, month, day) = civil_from_days(days_since_epoch as i64);
    let month_names = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct",
        "Nov", "Dec",
    ];

    format!(
        "{}, {:02} {} {:04} {:02}:{:02}:{:02} GMT",
        day_names[dow],
        day,
        month_names[(month - 1) as usize],
        year,
        hours,
        minutes,
        seconds,
    )
}

/// Convert days since Unix epoch to (year, month [1-12], day [1-31]).
///
/// Uses Howard Hinnant's civil_from_days algorithm.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

// ---------------------------------------------------------------------------
// Content-Type detection
// ---------------------------------------------------------------------------

/// Detect content type from file extension.
fn content_type_for_path(path: &Path) -> &'static str {
    match path
        .extension()
        .and_then(|ext| ext.to_str())
        .unwrap_or("")
    {
        // Text
        "html" | "htm" => "text/html; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "js" | "mjs" => "application/javascript; charset=utf-8",
        "json" => "application/json; charset=utf-8",
        "xml" => "application/xml; charset=utf-8",
        "txt" => "text/plain; charset=utf-8",
        "csv" => "text/csv; charset=utf-8",
        "md" => "text/markdown; charset=utf-8",

        // Images
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "svg" => "image/svg+xml",
        "ico" => "image/x-icon",
        "webp" => "image/webp",
        "avif" => "image/avif",
        "bmp" => "image/bmp",

        // Fonts
        "woff" => "font/woff",
        "woff2" => "font/woff2",
        "ttf" => "font/ttf",
        "otf" => "font/otf",
        "eot" => "application/vnd.ms-fontobject",

        // Media
        "mp4" => "video/mp4",
        "webm" => "video/webm",
        "mp3" => "audio/mpeg",
        "ogg" => "audio/ogg",
        "wav" => "audio/wav",

        // Archives
        "zip" => "application/zip",
        "gz" | "gzip" => "application/gzip",
        "tar" => "application/x-tar",

        // Documents
        "pdf" => "application/pdf",

        // Web
        "wasm" => "application/wasm",
        "map" => "application/json",
        "webmanifest" => "application/manifest+json",

        _ => "application/octet-stream",
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handler::Request;
    use bytes::Bytes;
    use http::{HeaderMap, Method};
    use http_body_util::BodyExt;
    use std::io::Write;
    use tempfile::NamedTempFile;

    /// Collect a response body into bytes.
    async fn body_bytes(resp: Response) -> Vec<u8> {
        resp.into_body().collect().await.unwrap().to_bytes().to_vec()
    }

    /// Create a temp file with the given extension and content.
    fn temp_file_with_ext(ext: &str, content: &[u8]) -> NamedTempFile {
        let suffix = format!(".{ext}");
        let mut f = tempfile::Builder::new()
            .suffix(&suffix)
            .tempfile()
            .unwrap();
        f.write_all(content).unwrap();
        f.flush().unwrap();
        f
    }

    fn make_request(headers: HeaderMap) -> Request {
        Request::new(Method::GET, "/".parse().unwrap(), headers, Bytes::new())
    }

    // 1. Content type detection for known extensions
    #[test]
    fn content_type_known_extensions() {
        assert_eq!(
            content_type_for_path(Path::new("page.html")),
            "text/html; charset=utf-8"
        );
        assert_eq!(
            content_type_for_path(Path::new("style.css")),
            "text/css; charset=utf-8"
        );
        assert_eq!(
            content_type_for_path(Path::new("app.js")),
            "application/javascript; charset=utf-8"
        );
        assert_eq!(
            content_type_for_path(Path::new("data.json")),
            "application/json; charset=utf-8"
        );
        assert_eq!(content_type_for_path(Path::new("photo.png")), "image/png");
        assert_eq!(
            content_type_for_path(Path::new("photo.jpg")),
            "image/jpeg"
        );
        assert_eq!(content_type_for_path(Path::new("anim.gif")), "image/gif");
        assert_eq!(
            content_type_for_path(Path::new("icon.svg")),
            "image/svg+xml"
        );
        assert_eq!(
            content_type_for_path(Path::new("doc.pdf")),
            "application/pdf"
        );
        assert_eq!(
            content_type_for_path(Path::new("module.wasm")),
            "application/wasm"
        );
        assert_eq!(
            content_type_for_path(Path::new("readme.txt")),
            "text/plain; charset=utf-8"
        );
        assert_eq!(
            content_type_for_path(Path::new("feed.xml")),
            "application/xml; charset=utf-8"
        );
    }

    // 2. Content type detection for unknown extension
    #[test]
    fn content_type_unknown_extension() {
        assert_eq!(
            content_type_for_path(Path::new("file.xyz")),
            "application/octet-stream"
        );
        assert_eq!(
            content_type_for_path(Path::new("binary.dat")),
            "application/octet-stream"
        );
    }

    // 3. ETag is consistent for same file
    #[tokio::test]
    async fn etag_consistent_for_same_file() {
        let tmp = temp_file_with_ext("txt", b"hello etag");
        let nf1 = NamedFile::open(tmp.path()).await.unwrap();
        let nf2 = NamedFile::open(tmp.path()).await.unwrap();
        assert_eq!(nf1.etag(), nf2.etag());
        assert!(nf1.etag().starts_with('"'));
        assert!(nf1.etag().ends_with('"'));
    }

    // 4. Full file response has correct headers
    #[tokio::test]
    async fn full_response_has_correct_headers() {
        let content = b"hello world from named file";
        let tmp = temp_file_with_ext("txt", content);
        let nf = NamedFile::open(tmp.path()).await.unwrap();

        let resp = nf.into_response();

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get("content-type").unwrap(),
            "text/plain; charset=utf-8"
        );
        assert_eq!(
            resp.headers()
                .get("content-length")
                .unwrap()
                .to_str()
                .unwrap(),
            content.len().to_string()
        );
        assert_eq!(
            resp.headers().get("accept-ranges").unwrap(),
            "bytes"
        );
        assert!(resp.headers().get("etag").is_some());
        assert!(resp.headers().get("last-modified").is_some());

        let bytes = body_bytes(resp).await;
        assert_eq!(bytes, content);
    }

    // 5. 404 for nonexistent file
    #[tokio::test]
    async fn open_nonexistent_returns_error() {
        let result = NamedFile::open("/tmp/definitely_does_not_exist_12345.txt").await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::NotFound);
    }

    // 6. Range request returns 206 with correct Content-Range
    #[tokio::test]
    async fn range_request_returns_206() {
        let content = b"0123456789ABCDEF";
        let tmp = temp_file_with_ext("txt", content);
        let nf = NamedFile::open(tmp.path()).await.unwrap();

        let mut headers = HeaderMap::new();
        headers.insert("range", "bytes=0-4".parse().unwrap());
        let req = make_request(headers);

        let resp = nf.into_conditional_response(&req);
        assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);

        let content_range = resp
            .headers()
            .get("content-range")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        assert_eq!(content_range, format!("bytes 0-4/{}", content.len()));

        assert_eq!(
            resp.headers()
                .get("content-length")
                .unwrap()
                .to_str()
                .unwrap(),
            "5"
        );

        let bytes = body_bytes(resp).await;
        assert_eq!(bytes, b"01234");
    }

    // 7. Invalid range returns 416 Range Not Satisfiable
    #[tokio::test]
    async fn invalid_range_returns_416() {
        let content = b"short";
        let tmp = temp_file_with_ext("txt", content);
        let nf = NamedFile::open(tmp.path()).await.unwrap();

        let mut headers = HeaderMap::new();
        // Start beyond file size
        headers.insert("range", "bytes=100-200".parse().unwrap());
        let req = make_request(headers);

        let resp = nf.into_conditional_response(&req);
        assert_eq!(resp.status(), StatusCode::RANGE_NOT_SATISFIABLE);
        assert!(resp.headers().get("content-range").is_some());

        let cr = resp
            .headers()
            .get("content-range")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        assert_eq!(cr, format!("bytes */{}", content.len()));
    }

    // 8. If-None-Match with matching ETag returns 304
    #[tokio::test]
    async fn if_none_match_returns_304() {
        let content = b"conditional content";
        let tmp = temp_file_with_ext("html", content);
        let nf = NamedFile::open(tmp.path()).await.unwrap();
        let etag = nf.etag().to_string();

        let mut headers = HeaderMap::new();
        headers.insert("if-none-match", etag.parse().unwrap());
        let req = make_request(headers);

        let resp = nf.into_conditional_response(&req);
        assert_eq!(resp.status(), StatusCode::NOT_MODIFIED);

        let body = body_bytes(resp).await;
        assert!(body.is_empty());
    }

    // Additional: parse_range unit tests
    #[test]
    fn parse_range_normal() {
        assert_eq!(parse_range("bytes=0-9", 100), Some((0, 9)));
        assert_eq!(parse_range("bytes=10-19", 100), Some((10, 19)));
    }

    #[test]
    fn parse_range_open_ended() {
        // bytes=50- means from 50 to end
        assert_eq!(parse_range("bytes=50-", 100), Some((50, 99)));
    }

    #[test]
    fn parse_range_suffix() {
        // bytes=-10 means last 10 bytes
        assert_eq!(parse_range("bytes=-10", 100), Some((90, 99)));
    }

    #[test]
    fn parse_range_invalid_start_beyond_size() {
        assert_eq!(parse_range("bytes=200-300", 100), None);
    }

    #[test]
    fn parse_range_multi_range_rejected() {
        assert_eq!(parse_range("bytes=0-9,20-29", 100), None);
    }

    #[test]
    fn parse_range_invalid_format() {
        assert_eq!(parse_range("pages=0-9", 100), None);
        assert_eq!(parse_range("bytes=abc-def", 100), None);
    }

    #[test]
    fn parse_range_end_clamped() {
        // End beyond file size is clamped
        assert_eq!(parse_range("bytes=0-999", 100), Some((0, 99)));
    }

    #[test]
    fn etag_match_exact() {
        assert!(etags_match("\"abc123\"", "\"abc123\""));
    }

    #[test]
    fn etag_match_weak() {
        assert!(etags_match("W/\"abc123\"", "\"abc123\""));
    }

    #[test]
    fn etag_match_wildcard() {
        assert!(etags_match("*", "\"anything\""));
    }

    #[test]
    fn etag_no_match() {
        assert!(!etags_match("\"different\"", "\"abc123\""));
    }

    #[test]
    fn last_modified_format() {
        // 2024-01-01 00:00:00 UTC = 1704067200
        let formatted = format_last_modified(1704067200);
        assert!(formatted.ends_with(" GMT"));
        assert!(formatted.contains("Jan"));
        assert!(formatted.contains("2024"));
    }

    #[test]
    fn civil_from_days_epoch() {
        // Day 0 = 1970-01-01
        assert_eq!(civil_from_days(0), (1970, 1, 1));
    }

    #[test]
    fn civil_from_days_known_date() {
        // 2024-01-01 is day 19723
        assert_eq!(civil_from_days(19723), (2024, 1, 1));
    }
}
