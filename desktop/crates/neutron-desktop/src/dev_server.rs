//! Lightweight HTTP dev server for debugging `neutron://` protocol routes.
//!
//! When `NEUTRON_DESKTOP_DEV=true`, this spawns a TCP server on
//! `127.0.0.1:{port}` (default 3001) that mirrors the same Router used by
//! the protocol bridge. Requests from curl/Postman/browser dev-tools hit
//! this TCP port and are dispatched through the same handler pipeline.
//!
//! **Production builds never open a TCP port** — the neutron:// protocol
//! remains the sole interface.

use crate::bridge::{Request, Response, Router};
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read as _, Write as _};
use std::net::TcpListener;
use std::sync::Arc;

/// Returns `true` when the user has opted into desktop dev mode.
pub fn is_dev_mode() -> bool {
    std::env::var("NEUTRON_DESKTOP_DEV").unwrap_or_default() == "true"
}

/// Read the configured dev-server port (default 3001).
pub fn dev_port() -> u16 {
    std::env::var("NEUTRON_DESKTOP_DEV_PORT")
        .unwrap_or_else(|_| "3001".to_string())
        .parse::<u16>()
        .unwrap_or(3001)
}

/// Spawn the dev HTTP server on a background thread.
///
/// The server binds to `127.0.0.1:{port}` and forwards every request through
/// the same [`Router`] that the `neutron://` protocol handler uses.
pub fn spawn_dev_server(router: Arc<Router>, port: u16) {
    std::thread::spawn(move || {
        let addr = format!("127.0.0.1:{port}");
        let listener = match TcpListener::bind(&addr) {
            Ok(l) => l,
            Err(e) => {
                tracing::error!("Failed to bind dev server on {addr}: {e}");
                return;
            }
        };

        tracing::info!("Desktop dev server running on http://{addr} (dev mode)");
        println!("Desktop running in DEV MODE \u{2014} HTTP server on :{port}, use curl/Postman to debug");

        for stream in listener.incoming() {
            let stream = match stream {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!("Dev server accept error: {e}");
                    continue;
                }
            };

            let router = Arc::clone(&router);
            std::thread::spawn(move || {
                if let Err(e) = handle_connection(stream, &router) {
                    tracing::debug!("Dev server connection error: {e}");
                }
            });
        }
    });
}

// ---------------------------------------------------------------------------
// Internal HTTP/1.1 request parsing + response writing
// ---------------------------------------------------------------------------

fn handle_connection(
    mut stream: std::net::TcpStream,
    router: &Router,
) -> Result<(), Box<dyn std::error::Error>> {
    // Set a generous read timeout so we don't hang forever on slow clients.
    stream.set_read_timeout(Some(std::time::Duration::from_secs(30)))?;

    let mut reader = BufReader::new(stream.try_clone()?);

    // --- Parse request line (e.g. "GET /api/users HTTP/1.1\r\n") ---
    let mut request_line = String::new();
    reader.read_line(&mut request_line)?;
    let request_line = request_line.trim_end().to_string();

    let parts: Vec<&str> = request_line.split_whitespace().collect();
    if parts.len() < 2 {
        return Ok(()); // Malformed or empty — drop silently
    }

    let method_str = parts[0];
    let raw_path = parts[1];

    let method = method_str.parse::<http::Method>().unwrap_or(http::Method::GET);

    // --- Parse headers ---
    let mut headers: HashMap<String, String> = HashMap::new();
    loop {
        let mut line = String::new();
        reader.read_line(&mut line)?;
        let trimmed = line.trim_end();
        if trimmed.is_empty() {
            break; // End of headers
        }
        if let Some((key, value)) = trimmed.split_once(':') {
            headers.insert(
                key.trim().to_lowercase(),
                value.trim().to_string(),
            );
        }
    }

    // --- Handle CORS preflight ---
    if method == http::Method::OPTIONS {
        let cors_response = build_cors_preflight();
        write_http_response(&mut stream, &cors_response)?;
        return Ok(());
    }

    // --- Read body (if Content-Length present) ---
    let body = if let Some(len_str) = headers.get("content-length") {
        let len: usize = len_str.parse().unwrap_or(0);
        let mut buf = vec![0u8; len];
        reader.read_exact(&mut buf)?;
        buf
    } else {
        Vec::new()
    };

    // --- Parse path and query string ---
    let (path, query) = parse_path_and_query(raw_path);

    // --- Build bridge Request ---
    let request = Request {
        method,
        path,
        headers: headers.clone(),
        body,
        query,
    };

    // --- Dispatch through router ---
    let response = router.handle_direct(request);

    // --- Write response with CORS headers ---
    write_http_response_with_cors(&mut stream, &response)?;

    Ok(())
}

fn parse_path_and_query(raw: &str) -> (String, HashMap<String, String>) {
    if let Some(idx) = raw.find('?') {
        let path = raw[..idx].to_string();
        let qs = &raw[idx + 1..];
        let query: HashMap<String, String> = qs
            .split('&')
            .filter_map(|pair| {
                let mut parts = pair.splitn(2, '=');
                let key = parts.next()?;
                let value = parts.next().unwrap_or("");
                Some((
                    urlencoding_decode(key),
                    urlencoding_decode(value),
                ))
            })
            .collect();
        (path, query)
    } else {
        (raw.to_string(), HashMap::new())
    }
}

/// Minimal percent-decoding (covers the most common cases).
fn urlencoding_decode(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '%' {
            let hex: String = chars.by_ref().take(2).collect();
            if let Ok(byte) = u8::from_str_radix(&hex, 16) {
                result.push(byte as char);
            } else {
                result.push('%');
                result.push_str(&hex);
            }
        } else if c == '+' {
            result.push(' ');
        } else {
            result.push(c);
        }
    }
    result
}

fn build_cors_preflight() -> Response {
    let mut headers = HashMap::new();
    headers.insert(
        "access-control-allow-origin".to_string(),
        "*".to_string(),
    );
    headers.insert(
        "access-control-allow-methods".to_string(),
        "GET, POST, PUT, DELETE, PATCH, OPTIONS".to_string(),
    );
    headers.insert(
        "access-control-allow-headers".to_string(),
        "Content-Type, Authorization".to_string(),
    );
    headers.insert(
        "access-control-max-age".to_string(),
        "86400".to_string(),
    );
    Response {
        status: 204,
        headers,
        body: Vec::new(),
    }
}

fn write_http_response_with_cors(
    stream: &mut std::net::TcpStream,
    response: &Response,
) -> std::io::Result<()> {
    let mut merged = response.headers.clone();
    merged
        .entry("access-control-allow-origin".to_string())
        .or_insert_with(|| "*".to_string());
    merged
        .entry("access-control-allow-methods".to_string())
        .or_insert_with(|| "GET, POST, PUT, DELETE, PATCH, OPTIONS".to_string());
    merged
        .entry("access-control-allow-headers".to_string())
        .or_insert_with(|| "Content-Type, Authorization".to_string());

    let merged_response = Response {
        status: response.status,
        headers: merged,
        body: response.body.clone(),
    };

    write_http_response(stream, &merged_response)
}

fn write_http_response(
    stream: &mut std::net::TcpStream,
    response: &Response,
) -> std::io::Result<()> {
    let reason = status_reason(response.status);
    let mut buf = format!("HTTP/1.1 {} {}\r\n", response.status, reason);

    for (key, value) in &response.headers {
        buf.push_str(&format!("{key}: {value}\r\n"));
    }

    buf.push_str(&format!("content-length: {}\r\n", response.body.len()));
    buf.push_str("connection: close\r\n");
    buf.push_str("\r\n");

    stream.write_all(buf.as_bytes())?;
    stream.write_all(&response.body)?;
    stream.flush()
}

fn status_reason(code: u16) -> &'static str {
    match code {
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        500 => "Internal Server Error",
        503 => "Service Unavailable",
        _ => "OK",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_path_and_query() {
        let (path, query) = parse_path_and_query("/api/users?name=alice&age=30");
        assert_eq!(path, "/api/users");
        assert_eq!(query.get("name").unwrap(), "alice");
        assert_eq!(query.get("age").unwrap(), "30");
    }

    #[test]
    fn test_parse_path_no_query() {
        let (path, query) = parse_path_and_query("/api/users");
        assert_eq!(path, "/api/users");
        assert!(query.is_empty());
    }

    #[test]
    fn test_urlencoding_decode() {
        assert_eq!(urlencoding_decode("hello%20world"), "hello world");
        assert_eq!(urlencoding_decode("a+b"), "a b");
        assert_eq!(urlencoding_decode("normal"), "normal");
    }

    #[test]
    fn test_cors_preflight() {
        let resp = build_cors_preflight();
        assert_eq!(resp.status, 204);
        assert_eq!(
            resp.headers.get("access-control-allow-origin").unwrap(),
            "*"
        );
    }

    #[test]
    fn test_is_dev_mode_default() {
        // Without the env var set, should return false
        std::env::remove_var("NEUTRON_DESKTOP_DEV");
        assert!(!is_dev_mode());
    }

    #[test]
    fn test_dev_port_default() {
        std::env::remove_var("NEUTRON_DESKTOP_DEV_PORT");
        assert_eq!(dev_port(), 3001);
    }
}
