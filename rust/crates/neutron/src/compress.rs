//! Response compression middleware (gzip and brotli).
//!
//! Negotiates encoding via the `Accept-Encoding` header, preferring brotli
//! over gzip. Skips already-compressed content types and small bodies.
//!
//! ```rust,ignore
//! Router::new().middleware(Compress::new())
//! ```

use std::future::Future;
use std::io::Write;
use std::pin::Pin;

use http_body_util::BodyExt;

use crate::handler::{Body, Request, Response};
use crate::middleware::{MiddlewareTrait, Next};

/// Minimum response body size to compress (bytes).
/// Below this threshold, compression overhead exceeds savings.
const MIN_COMPRESS_SIZE: usize = 860;

/// Response compression middleware.
///
/// Negotiates encoding via the `Accept-Encoding` request header, preferring
/// brotli over gzip. Compresses the response body and sets `Content-Encoding`
/// and `Vary` headers.
///
/// Compression is skipped when:
/// - The response body is smaller than 860 bytes
/// - The response already has a `Content-Encoding` header
/// - The content type is inherently compressed (images, video, fonts, etc.)
/// - The client doesn't accept gzip or brotli
///
/// # Example
///
/// ```rust,ignore
/// use neutron::prelude::*;
///
/// let router = Router::new()
///     .middleware(Compress::new())
///     .get("/", handler);
/// ```
#[derive(Clone)]
pub struct Compress {
    min_size: usize,
}

impl Compress {
    /// Create a compression middleware with the default minimum size (860 bytes).
    pub fn new() -> Self {
        Self {
            min_size: MIN_COMPRESS_SIZE,
        }
    }

    /// Create a compression middleware with a custom minimum body size threshold.
    pub fn min_size(min_size: usize) -> Self {
        Self { min_size }
    }
}

impl Default for Compress {
    fn default() -> Self {
        Self::new()
    }
}

/// Encoding preference, ordered by priority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Encoding {
    Brotli,
    Gzip,
}

impl Encoding {
    fn as_str(self) -> &'static str {
        match self {
            Encoding::Brotli => "br",
            Encoding::Gzip => "gzip",
        }
    }
}

/// Parse `Accept-Encoding` header and pick the best supported encoding.
///
/// Prefers brotli > gzip. Respects `q=0` (explicitly disabled).
fn negotiate_encoding(accept: &str) -> Option<Encoding> {
    let mut best = None;
    let mut best_priority = 0u8; // brotli=2, gzip=1

    for part in accept.split(',') {
        let part = part.trim();
        let (name, q) = parse_encoding_part(part);

        if q == 0.0 {
            continue;
        }

        match name {
            "br" if best_priority < 2 => {
                best = Some(Encoding::Brotli);
                best_priority = 2;
            }
            "gzip" if best_priority < 1 => {
                best = Some(Encoding::Gzip);
                best_priority = 1;
            }
            _ => {}
        }
    }

    best
}

/// Parse a single encoding entry like `gzip;q=0.8` into (name, quality).
fn parse_encoding_part(part: &str) -> (&str, f32) {
    if let Some((name, params)) = part.split_once(';') {
        let name = name.trim();
        let q = params
            .split(';')
            .find_map(|p| {
                let p = p.trim();
                p.strip_prefix("q=")
                    .and_then(|v| v.trim().parse::<f32>().ok())
            })
            .unwrap_or(1.0);
        (name, q)
    } else {
        (part.trim(), 1.0)
    }
}

/// Returns true if the content type is already compressed and shouldn't be
/// double-compressed.
fn is_precompressed(content_type: &str) -> bool {
    let ct = content_type.split(';').next().unwrap_or("").trim();
    matches!(
        ct,
        "image/png"
            | "image/jpeg"
            | "image/gif"
            | "image/webp"
            | "image/avif"
            | "video/mp4"
            | "video/webm"
            | "audio/mpeg"
            | "audio/ogg"
            | "application/zip"
            | "application/gzip"
            | "application/x-gzip"
            | "application/x-brotli"
            | "application/zstd"
            | "application/x-tar"
            | "application/x-rar-compressed"
            | "application/x-7z-compressed"
            | "font/woff"
            | "font/woff2"
            | "application/wasm"
    )
}

/// Compress bytes with gzip.
fn compress_gzip(data: &[u8]) -> Option<Vec<u8>> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    encoder.write_all(data).ok()?;
    encoder.finish().ok()
}

/// Compress bytes with brotli.
fn compress_brotli(data: &[u8]) -> Option<Vec<u8>> {
    let mut output = Vec::new();
    // Quality 4 is a good balance of speed and compression for dynamic content.
    // (Brotli quality ranges 0-11; static assets benefit from higher levels.)
    let params = brotli::enc::BrotliEncoderParams {
        quality: 4,
        ..Default::default()
    };
    brotli::BrotliCompress(&mut &data[..], &mut output, &params).ok()?;
    Some(output)
}

impl MiddlewareTrait for Compress {
    fn call(&self, req: Request, next: Next) -> Pin<Box<dyn Future<Output = Response> + Send>> {
        let min_size = self.min_size;

        // Negotiate encoding from the request before passing it to the handler.
        let encoding = req
            .headers()
            .get("accept-encoding")
            .and_then(|v| v.to_str().ok())
            .and_then(negotiate_encoding);

        Box::pin(async move {
            let resp = next.run(req).await;

            // No supported encoding requested — return as-is.
            let encoding = match encoding {
                Some(e) => e,
                None => return resp,
            };

            // Already encoded — don't double-compress.
            if resp.headers().contains_key("content-encoding") {
                return resp;
            }

            // Check content type — skip pre-compressed formats.
            let skip = resp
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .map(is_precompressed)
                .unwrap_or(false);
            if skip {
                return resp;
            }

            // Decompose response to access body bytes.
            let (mut parts, body) = resp.into_parts();

            // Skip streaming responses — can't buffer them for compression.
            if body.is_streaming() {
                parts
                    .headers
                    .append("vary", "accept-encoding".parse().unwrap());
                return http::Response::from_parts(parts, body);
            }

            // For Full<Bytes>, collect() resolves immediately — no actual I/O.
            let original = body.collect().await.unwrap().to_bytes();
            let original_len = original.len();

            // Too small to benefit from compression.
            if original_len < min_size {
                parts
                    .headers
                    .append("vary", "accept-encoding".parse().unwrap());
                return http::Response::from_parts(parts, Body::full(original));
            }

            let compressed = match encoding {
                Encoding::Brotli => compress_brotli(&original),
                Encoding::Gzip => compress_gzip(&original),
            };

            match compressed {
                Some(data) if data.len() < original_len => {
                    parts
                        .headers
                        .insert("content-encoding", encoding.as_str().parse().unwrap());
                    parts
                        .headers
                        .append("vary", "accept-encoding".parse().unwrap());
                    parts
                        .headers
                        .insert("content-length", data.len().to_string().parse().unwrap());
                    http::Response::from_parts(parts, Body::full(data))
                }
                _ => {
                    // Compression didn't help — return original body.
                    parts
                        .headers
                        .append("vary", "accept-encoding".parse().unwrap());
                    http::Response::from_parts(parts, Body::full(original))
                }
            }
        })
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handler::Json;
    use crate::router::Router;
    use crate::testing::TestClient;
    use http::StatusCode;

    /// Helper to generate a string large enough to trigger compression.
    fn large_body() -> String {
        "Hello, this is a reasonably long response body that should be large enough to trigger compression in our middleware. ".repeat(20)
    }

    fn large_json() -> serde_json::Value {
        serde_json::json!({
            "data": large_body(),
            "items": (0..50).map(|i| serde_json::json!({"id": i, "name": format!("item-{i}")})).collect::<Vec<_>>(),
        })
    }

    #[tokio::test]
    async fn gzip_compresses_large_text() {
        let body = large_body();
        let expected_len = body.len();
        let client = TestClient::new(
            Router::new()
                .middleware(Compress::new())
                .get("/", move || {
                    let b = body.clone();
                    async move { b }
                }),
        );

        let resp = client
            .get("/")
            .header("accept-encoding", "gzip")
            .send()
            .await;

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.header("content-encoding").unwrap(), "gzip");
        assert!(resp.header("vary").unwrap().contains("accept-encoding"));

        let compressed_len: usize = resp.header("content-length").unwrap().parse().unwrap();
        assert!(
            compressed_len < expected_len,
            "compressed {compressed_len} should be smaller than original {expected_len}"
        );
    }

    #[tokio::test]
    async fn brotli_compresses_large_text() {
        let body = large_body();
        let expected_len = body.len();
        let client = TestClient::new(
            Router::new()
                .middleware(Compress::new())
                .get("/", move || {
                    let b = body.clone();
                    async move { b }
                }),
        );

        let resp = client
            .get("/")
            .header("accept-encoding", "br")
            .send()
            .await;

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.header("content-encoding").unwrap(), "br");

        let compressed_len: usize = resp.header("content-length").unwrap().parse().unwrap();
        assert!(compressed_len < expected_len);
    }

    #[tokio::test]
    async fn brotli_preferred_over_gzip() {
        let body = large_body();
        let client = TestClient::new(
            Router::new()
                .middleware(Compress::new())
                .get("/", move || {
                    let b = body.clone();
                    async move { b }
                }),
        );

        let resp = client
            .get("/")
            .header("accept-encoding", "gzip, br")
            .send()
            .await;

        assert_eq!(resp.header("content-encoding").unwrap(), "br");
    }

    #[tokio::test]
    async fn brotli_preferred_regardless_of_order() {
        let body = large_body();
        let client = TestClient::new(
            Router::new()
                .middleware(Compress::new())
                .get("/", move || {
                    let b = body.clone();
                    async move { b }
                }),
        );

        let resp = client
            .get("/")
            .header("accept-encoding", "br, gzip, deflate")
            .send()
            .await;

        assert_eq!(resp.header("content-encoding").unwrap(), "br");
    }

    #[tokio::test]
    async fn skips_small_body() {
        let client = TestClient::new(
            Router::new()
                .middleware(Compress::new())
                .get("/", || async { "small" }),
        );

        let resp = client
            .get("/")
            .header("accept-encoding", "gzip, br")
            .send()
            .await;

        assert_eq!(resp.status(), StatusCode::OK);
        assert!(resp.header("content-encoding").is_none());
        assert_eq!(resp.text().await, "small");
    }

    #[tokio::test]
    async fn skips_when_no_accept_encoding() {
        let body = large_body();
        let client = TestClient::new(
            Router::new()
                .middleware(Compress::new())
                .get("/", move || {
                    let b = body.clone();
                    async move { b }
                }),
        );

        let resp = client.get("/").send().await;

        assert_eq!(resp.status(), StatusCode::OK);
        assert!(resp.header("content-encoding").is_none());
    }

    #[tokio::test]
    async fn skips_unsupported_encoding() {
        let body = large_body();
        let client = TestClient::new(
            Router::new()
                .middleware(Compress::new())
                .get("/", move || {
                    let b = body.clone();
                    async move { b }
                }),
        );

        let resp = client
            .get("/")
            .header("accept-encoding", "deflate, zstd")
            .send()
            .await;

        assert!(resp.header("content-encoding").is_none());
    }

    #[tokio::test]
    async fn skips_precompressed_content_type() {
        let body = large_body();
        let client = TestClient::new(Router::new().middleware(Compress::new()).get(
            "/",
            move || {
                let b = body.clone();
                async move {
                    let mut headers = http::HeaderMap::new();
                    headers.insert("content-type", "image/png".parse().unwrap());
                    (headers, b)
                }
            },
        ));

        let resp = client
            .get("/")
            .header("accept-encoding", "gzip, br")
            .send()
            .await;

        assert!(resp.header("content-encoding").is_none());
    }

    #[tokio::test]
    async fn skips_already_encoded() {
        let body = large_body();
        let client = TestClient::new(Router::new().middleware(Compress::new()).get(
            "/",
            move || {
                let b = body.clone();
                async move {
                    let mut headers = http::HeaderMap::new();
                    headers.insert("content-encoding", "br".parse().unwrap());
                    (headers, b)
                }
            },
        ));

        let resp = client
            .get("/")
            .header("accept-encoding", "gzip, br")
            .send()
            .await;

        // Should not be double-encoded — still "br" from the handler.
        assert_eq!(resp.header("content-encoding").unwrap(), "br");
    }

    #[tokio::test]
    async fn respects_q_zero_exclusion() {
        let body = large_body();
        let client = TestClient::new(
            Router::new()
                .middleware(Compress::new())
                .get("/", move || {
                    let b = body.clone();
                    async move { b }
                }),
        );

        // Client supports gzip but explicitly disables brotli.
        let resp = client
            .get("/")
            .header("accept-encoding", "br;q=0, gzip")
            .send()
            .await;

        assert_eq!(resp.header("content-encoding").unwrap(), "gzip");
    }

    #[tokio::test]
    async fn json_response_compressed() {
        let val = large_json();
        let client = TestClient::new(
            Router::new()
                .middleware(Compress::new())
                .get("/", move || {
                    let v = val.clone();
                    async move { Json(v) }
                }),
        );

        let resp = client
            .get("/")
            .header("accept-encoding", "gzip")
            .send()
            .await;

        assert_eq!(resp.header("content-encoding").unwrap(), "gzip");
        assert_eq!(
            resp.headers()
                .get("content-type")
                .unwrap()
                .to_str()
                .unwrap(),
            "application/json"
        );
    }

    #[tokio::test]
    async fn compressed_body_decompresses_correctly_gzip() {
        let body = large_body();
        let original = body.clone();
        let client = TestClient::new(
            Router::new()
                .middleware(Compress::new())
                .get("/", move || {
                    let b = body.clone();
                    async move { b }
                }),
        );

        let resp = client
            .get("/")
            .header("accept-encoding", "gzip")
            .send()
            .await;

        assert_eq!(resp.header("content-encoding").unwrap(), "gzip");

        // Decompress and verify round-trip.
        let compressed = resp.bytes().await;
        let mut decoder = flate2::read::GzDecoder::new(&compressed[..]);
        let mut decompressed = String::new();
        std::io::Read::read_to_string(&mut decoder, &mut decompressed).unwrap();
        assert_eq!(decompressed, original);
    }

    #[tokio::test]
    async fn compressed_body_decompresses_correctly_brotli() {
        let body = large_body();
        let original = body.clone();
        let client = TestClient::new(
            Router::new()
                .middleware(Compress::new())
                .get("/", move || {
                    let b = body.clone();
                    async move { b }
                }),
        );

        let resp = client
            .get("/")
            .header("accept-encoding", "br")
            .send()
            .await;

        assert_eq!(resp.header("content-encoding").unwrap(), "br");

        // Decompress and verify round-trip.
        let compressed = resp.bytes().await;
        let mut decompressed = Vec::new();
        brotli::BrotliDecompress(&mut &compressed[..], &mut decompressed).unwrap();
        assert_eq!(String::from_utf8(decompressed).unwrap(), original);
    }

    #[tokio::test]
    async fn custom_min_size() {
        // Body is 200 bytes — below default 860 threshold but above custom 100.
        // Must be large enough for gzip to actually shrink (repeated text compresses well).
        let body = "abcdefghij".repeat(20);
        let client = TestClient::new(
            Router::new()
                .middleware(Compress::min_size(100))
                .get("/", move || {
                    let b = body.clone();
                    async move { b }
                }),
        );

        let resp = client
            .get("/")
            .header("accept-encoding", "gzip")
            .send()
            .await;

        // 200 bytes of repeated text compresses well with gzip.
        assert_eq!(resp.header("content-encoding").unwrap(), "gzip");
    }

    #[tokio::test]
    async fn vary_header_added_even_without_compression() {
        let client = TestClient::new(
            Router::new()
                .middleware(Compress::new())
                .get("/", || async { "small" }),
        );

        let resp = client
            .get("/")
            .header("accept-encoding", "gzip")
            .send()
            .await;

        // Body too small to compress, but Vary should still be present
        // for correct caching behavior.
        assert!(resp.header("vary").unwrap().contains("accept-encoding"));
    }

    // -----------------------------------------------------------------------
    // Unit tests for negotiate_encoding
    // -----------------------------------------------------------------------

    #[test]
    fn negotiate_prefers_brotli() {
        assert_eq!(
            negotiate_encoding("gzip, br, deflate"),
            Some(Encoding::Brotli)
        );
    }

    #[test]
    fn negotiate_gzip_only() {
        assert_eq!(negotiate_encoding("gzip"), Some(Encoding::Gzip));
    }

    #[test]
    fn negotiate_brotli_only() {
        assert_eq!(negotiate_encoding("br"), Some(Encoding::Brotli));
    }

    #[test]
    fn negotiate_unknown_returns_none() {
        assert_eq!(negotiate_encoding("deflate, zstd"), None);
    }

    #[test]
    fn negotiate_q_zero_excluded() {
        assert_eq!(negotiate_encoding("br;q=0, gzip"), Some(Encoding::Gzip));
    }

    #[test]
    fn negotiate_empty_returns_none() {
        assert_eq!(negotiate_encoding(""), None);
    }

    #[test]
    fn negotiate_identity_only_returns_none() {
        assert_eq!(negotiate_encoding("identity"), None);
    }

    #[test]
    fn negotiate_with_quality_values() {
        // Both have non-zero quality, brotli still preferred.
        assert_eq!(
            negotiate_encoding("gzip;q=1.0, br;q=0.5"),
            Some(Encoding::Brotli)
        );
    }
}
