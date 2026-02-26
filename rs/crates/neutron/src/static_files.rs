//! Static file serving with ETag / conditional-request support.
//!
//! Serves files from a directory, auto-detecting content types from
//! extensions. Generates ETags and returns `304 Not Modified` when
//! `If-None-Match` matches.
//!
//! ```rust,ignore
//! Router::new().static_files("/assets", "./public")
//! ```

use std::future::Future;
use std::path::{Component, Path, PathBuf};
use std::pin::Pin;

use http::StatusCode;

use crate::handler::{Body, Handler, Request, Response};
use crate::router::Router;

/// Serve static files from a directory with ETag/304 support.
///
/// Content types are detected from file extensions. ETags are generated from
/// file metadata (modified time + size). Conditional requests with
/// `If-None-Match` return `304 Not Modified` when the ETag matches.
///
/// # Example
///
/// ```rust,ignore
/// use neutron::prelude::*;
///
/// let router = Router::new()
///     .static_files("/assets", "./public");
/// ```
pub struct StaticFiles {
    root: PathBuf,
}

impl StaticFiles {
    /// Create a static file handler for the given directory.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }
}

impl Handler<()> for StaticFiles {
    fn call(&self, req: Request) -> Pin<Box<dyn Future<Output = Response> + Send>> {
        let root = self.root.clone();

        // Get wildcard capture or default to index.html
        let file_path = req
            .params()
            .iter()
            .find(|(k, _)| k == "rest")
            .map(|(_, v)| v.trim_start_matches('/').to_string())
            .unwrap_or_default();

        // Capture If-None-Match header before moving req
        let if_none_match = req
            .headers()
            .get("if-none-match")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        Box::pin(async move {
            let file_path = if file_path.is_empty() {
                "index.html".to_string()
            } else {
                file_path
            };

            // Sanitize path to prevent directory traversal
            let resolved = match resolve_safe_path(&root, &file_path) {
                Some(p) => p,
                None => return not_found(),
            };

            // Try to serve the resolved path (file or directory/index.html)
            let metadata = match tokio::fs::metadata(&resolved).await {
                Ok(m) => m,
                Err(_) => return not_found(),
            };

            let target = if metadata.is_dir() {
                let index = resolved.join("index.html");
                match tokio::fs::metadata(&index).await {
                    Ok(m) if m.is_file() => index,
                    _ => return not_found(),
                }
            } else {
                resolved
            };

            // Read file metadata for ETag
            let file_meta = match tokio::fs::metadata(&target).await {
                Ok(m) => m,
                Err(_) => return not_found(),
            };

            let etag = generate_etag(&file_meta);

            // Check If-None-Match → 304
            if let Some(ref client_etag) = if_none_match {
                if client_etag.trim_matches('"') == etag.trim_matches('"') {
                    return http::Response::builder()
                        .status(StatusCode::NOT_MODIFIED)
                        .header("etag", &etag)
                        .header("cache-control", "public, max-age=3600")
                        .body(Body::empty())
                        .unwrap();
                }
            }

            // Read file
            let body = match tokio::fs::read(&target).await {
                Ok(data) => data,
                Err(_) => return not_found(),
            };

            let content_type = content_type_for_path(&target);

            http::Response::builder()
                .status(StatusCode::OK)
                .header("content-type", content_type)
                .header("content-length", body.len())
                .header("etag", &etag)
                .header("cache-control", "public, max-age=3600")
                .body(Body::full(body))
                .unwrap()
        })
    }
}

impl Router {
    /// Serve static files from a directory at the given URL prefix.
    ///
    /// Registers a wildcard route at `<prefix>/*` and a root route at `<prefix>`
    /// (serving `index.html`).
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// Router::new()
    ///     .static_files("/assets", "./public")
    ///     .get("/api/hello", handler)
    /// ```
    pub fn static_files(self, prefix: &str, dir: impl Into<PathBuf>) -> Self {
        let root = dir.into();
        let prefix = prefix.trim_end_matches('/');
        let wildcard_path = format!("{prefix}/*");

        let handler_wild = StaticFiles::new(root.clone());
        let handler_root = StaticFiles::new(root);

        self.get(&wildcard_path, handler_wild)
            .get(prefix, handler_root)
    }
}

// ---------------------------------------------------------------------------
// Path safety
// ---------------------------------------------------------------------------

/// Resolve a request path against a root directory, preventing traversal attacks.
///
/// Returns `None` if the path attempts to escape the root (e.g., `../`).
///
/// **Note:** The input `request_path` is expected to be already percent-decoded
/// (the router handles decoding). No further decoding is performed here.
fn resolve_safe_path(root: &Path, request_path: &str) -> Option<PathBuf> {
    let clean = Path::new(request_path);

    // Reject any path with `..` components
    for component in clean.components() {
        match component {
            Component::ParentDir => return None,
            Component::Prefix(_) => return None, // Windows drive letters
            _ => {}
        }
    }

    let resolved = root.join(clean);

    // Double-check: canonical path must be under root.
    // We can't use canonicalize() here (it requires the file to exist),
    // so we rely on the component check above.
    Some(resolved)
}

// ---------------------------------------------------------------------------
// ETag generation
// ---------------------------------------------------------------------------

/// Generate an ETag from file metadata (modified time + size).
///
/// Format: `"<mtime_secs>-<size>"` — same strategy as nginx.
fn generate_etag(metadata: &std::fs::Metadata) -> String {
    let modified = metadata
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let size = metadata.len();
    format!("\"{modified:x}-{size:x}\"")
}

// ---------------------------------------------------------------------------
// Content type detection
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

fn not_found() -> Response {
    http::Response::builder()
        .status(StatusCode::NOT_FOUND)
        .header("content-type", "text/plain; charset=utf-8")
        .body(Body::full("Not Found"))
        .unwrap()
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::TestClient;
    use std::io::Write;
    use tempfile::TempDir;

    /// Create a temp directory with test files.
    fn setup_static_dir() -> TempDir {
        let dir = TempDir::new().unwrap();

        // Create files
        std::fs::write(dir.path().join("index.html"), "<h1>Home</h1>").unwrap();
        std::fs::write(dir.path().join("style.css"), "body { color: red }").unwrap();
        std::fs::write(dir.path().join("app.js"), "console.log('hi')").unwrap();
        std::fs::write(dir.path().join("data.json"), r#"{"key":"value"}"#).unwrap();

        // Create subdirectory
        let sub = dir.path().join("sub");
        std::fs::create_dir(&sub).unwrap();
        std::fs::write(sub.join("page.html"), "<h1>Sub</h1>").unwrap();
        std::fs::write(sub.join("index.html"), "<h1>SubIndex</h1>").unwrap();

        // Binary file
        let mut f = std::fs::File::create(dir.path().join("image.png")).unwrap();
        f.write_all(&[0x89, 0x50, 0x4E, 0x47]).unwrap(); // PNG magic bytes

        dir
    }

    #[tokio::test]
    async fn serves_index_at_root() {
        let dir = setup_static_dir();
        let client = TestClient::new(Router::new().static_files("/", dir.path().to_path_buf()));

        let resp = client.get("/").send().await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(resp
            .header("content-type")
            .unwrap()
            .contains("text/html"));
        assert_eq!(resp.text().await, "<h1>Home</h1>");
    }

    #[tokio::test]
    async fn serves_css_file() {
        let dir = setup_static_dir();
        let client = TestClient::new(Router::new().static_files("/assets", dir.path().to_path_buf()));

        let resp = client.get("/assets/style.css").send().await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(resp.header("content-type").unwrap().contains("text/css"));
        assert_eq!(resp.text().await, "body { color: red }");
    }

    #[tokio::test]
    async fn serves_js_file() {
        let dir = setup_static_dir();
        let client = TestClient::new(Router::new().static_files("/assets", dir.path().to_path_buf()));

        let resp = client.get("/assets/app.js").send().await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(resp
            .header("content-type")
            .unwrap()
            .contains("javascript"));
    }

    #[tokio::test]
    async fn serves_json_file() {
        let dir = setup_static_dir();
        let client = TestClient::new(Router::new().static_files("/static", dir.path().to_path_buf()));

        let resp = client.get("/static/data.json").send().await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(resp
            .header("content-type")
            .unwrap()
            .contains("application/json"));
    }

    #[tokio::test]
    async fn serves_nested_file() {
        let dir = setup_static_dir();
        let client = TestClient::new(Router::new().static_files("/assets", dir.path().to_path_buf()));

        let resp = client.get("/assets/sub/page.html").send().await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.text().await, "<h1>Sub</h1>");
    }

    #[tokio::test]
    async fn serves_subdirectory_index() {
        let dir = setup_static_dir();
        let client = TestClient::new(Router::new().static_files("/assets", dir.path().to_path_buf()));

        let resp = client.get("/assets/sub").send().await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.text().await, "<h1>SubIndex</h1>");
    }

    #[tokio::test]
    async fn returns_404_for_missing_file() {
        let dir = setup_static_dir();
        let client = TestClient::new(Router::new().static_files("/assets", dir.path().to_path_buf()));

        let resp = client.get("/assets/nonexistent.txt").send().await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn prevents_directory_traversal() {
        let dir = setup_static_dir();
        let client = TestClient::new(Router::new().static_files("/assets", dir.path().to_path_buf()));

        let resp = client.get("/assets/../../../etc/passwd").send().await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn has_etag_header() {
        let dir = setup_static_dir();
        let client = TestClient::new(Router::new().static_files("/assets", dir.path().to_path_buf()));

        let resp = client.get("/assets/style.css").send().await;
        assert_eq!(resp.status(), StatusCode::OK);

        let etag = resp.header("etag").unwrap().to_string();
        assert!(etag.starts_with('"'));
        assert!(etag.ends_with('"'));
        assert!(etag.contains('-'));
    }

    #[tokio::test]
    async fn conditional_request_304() {
        let dir = setup_static_dir();
        let client = TestClient::new(Router::new().static_files("/assets", dir.path().to_path_buf()));

        // First request to get the ETag
        let resp1 = client.get("/assets/style.css").send().await;
        let etag = resp1.header("etag").unwrap().to_string();

        // Second request with If-None-Match
        let resp2 = client
            .get("/assets/style.css")
            .header("if-none-match", &etag)
            .send()
            .await;

        assert_eq!(resp2.status(), StatusCode::NOT_MODIFIED);
        assert!(resp2.bytes().await.is_empty());
    }

    #[tokio::test]
    async fn conditional_request_mismatched_etag() {
        let dir = setup_static_dir();
        let client = TestClient::new(Router::new().static_files("/assets", dir.path().to_path_buf()));

        let resp = client
            .get("/assets/style.css")
            .header("if-none-match", "\"wrong-etag\"")
            .send()
            .await;

        assert_eq!(resp.status(), StatusCode::OK);
        assert!(!resp.bytes().await.is_empty());
    }

    #[tokio::test]
    async fn has_cache_control_header() {
        let dir = setup_static_dir();
        let client = TestClient::new(Router::new().static_files("/assets", dir.path().to_path_buf()));

        let resp = client.get("/assets/style.css").send().await;
        assert!(resp
            .header("cache-control")
            .unwrap()
            .contains("max-age=3600"));
    }

    #[tokio::test]
    async fn binary_file_content_type() {
        let dir = setup_static_dir();
        let client = TestClient::new(Router::new().static_files("/assets", dir.path().to_path_buf()));

        let resp = client.get("/assets/image.png").send().await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.header("content-type").unwrap(), "image/png");
        assert_eq!(resp.bytes().await.as_ref(), &[0x89, 0x50, 0x4E, 0x47]);
    }

    #[tokio::test]
    async fn coexists_with_api_routes() {
        let dir = setup_static_dir();
        let client = TestClient::new(
            Router::new()
                .static_files("/assets", dir.path().to_path_buf())
                .get("/api/hello", || async { "hello api" }),
        );

        let asset = client.get("/assets/style.css").send().await;
        assert_eq!(asset.status(), StatusCode::OK);
        assert!(asset.header("content-type").unwrap().contains("text/css"));

        let api = client.get("/api/hello").send().await;
        assert_eq!(api.status(), StatusCode::OK);
        assert_eq!(api.text().await, "hello api");
    }

    // -----------------------------------------------------------------------
    // Unit tests for helpers
    // -----------------------------------------------------------------------

    #[test]
    fn safe_path_simple() {
        let root = PathBuf::from("/srv/static");
        let resolved = resolve_safe_path(&root, "style.css").unwrap();
        assert_eq!(resolved, PathBuf::from("/srv/static/style.css"));
    }

    #[test]
    fn safe_path_nested() {
        let root = PathBuf::from("/srv/static");
        let resolved = resolve_safe_path(&root, "sub/page.html").unwrap();
        assert_eq!(resolved, PathBuf::from("/srv/static/sub/page.html"));
    }

    #[test]
    fn safe_path_blocks_traversal() {
        let root = PathBuf::from("/srv/static");
        assert!(resolve_safe_path(&root, "../etc/passwd").is_none());
        assert!(resolve_safe_path(&root, "sub/../../etc/passwd").is_none());
    }

    #[test]
    fn safe_path_blocks_decoded_traversal() {
        let root = PathBuf::from("/srv/static");
        // Path params arrive already decoded by the router, so `..` is literal
        assert!(resolve_safe_path(&root, "../etc/passwd").is_none());
    }

    #[test]
    fn content_type_detection() {
        assert_eq!(
            content_type_for_path(Path::new("file.html")),
            "text/html; charset=utf-8"
        );
        assert_eq!(
            content_type_for_path(Path::new("file.css")),
            "text/css; charset=utf-8"
        );
        assert_eq!(
            content_type_for_path(Path::new("file.js")),
            "application/javascript; charset=utf-8"
        );
        assert_eq!(content_type_for_path(Path::new("file.png")), "image/png");
        assert_eq!(
            content_type_for_path(Path::new("file.unknown")),
            "application/octet-stream"
        );
    }
}
