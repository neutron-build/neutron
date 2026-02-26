//! Multipart form data parsing for file uploads.
//!
//! # Example
//!
//! ```rust,ignore
//! use neutron::prelude::*;
//! use neutron::multipart::Multipart;
//!
//! async fn upload(mut form: Multipart) -> String {
//!     let mut files = Vec::new();
//!     while let Some(field) = form.next_field().await.unwrap() {
//!         let name = field.name().unwrap_or("unknown").to_string();
//!         if let Some(file_name) = field.file_name() {
//!             let data = field.bytes().await.unwrap();
//!             files.push(format!("{file_name}: {} bytes", data.len()));
//!         } else {
//!             let text = field.text().await.unwrap();
//!             files.push(format!("{name} = {text}"));
//!         }
//!     }
//!     files.join("\n")
//! }
//!
//! let router = Router::new().post("/upload", upload);
//! ```

use bytes::Bytes;
use http::StatusCode;

use crate::extract::FromRequest;
use crate::handler::{IntoResponse, Request, Response};

// ---------------------------------------------------------------------------
// Multipart extractor
// ---------------------------------------------------------------------------

/// Multipart form data extractor.
///
/// Wraps the [`multer`] crate to parse `multipart/form-data` requests.
/// Use [`next_field`](Self::next_field) to iterate over form fields.
pub struct Multipart {
    inner: multer::Multipart<'static>,
}

impl Multipart {
    /// Get the next field from the multipart stream.
    ///
    /// Returns `None` when all fields have been consumed.
    /// Each previous [`Field`] must be fully consumed or dropped before
    /// calling this method again.
    pub async fn next_field(&mut self) -> Result<Option<Field>, MultipartError> {
        self.inner
            .next_field()
            .await
            .map(|opt| opt.map(|f| Field { inner: f }))
            .map_err(MultipartError)
    }
}

impl FromRequest for Multipart {
    fn from_request(req: &Request) -> Result<Self, Response> {
        let content_type = req
            .headers()
            .get(http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");

        let boundary = multer::parse_boundary(content_type).map_err(|_| {
            (
                StatusCode::BAD_REQUEST,
                "Invalid or missing multipart boundary",
            )
                .into_response()
        })?;

        let body = req.body().clone();
        let stream = tokio_stream::once(Ok::<Bytes, std::convert::Infallible>(body));
        let multipart = multer::Multipart::new(stream, boundary);

        Ok(Multipart { inner: multipart })
    }
}

// ---------------------------------------------------------------------------
// Field
// ---------------------------------------------------------------------------

/// A single field in a multipart form.
///
/// A field can be a text value or a file upload. Use [`name`](Self::name)
/// and [`file_name`](Self::file_name) to distinguish between them.
pub struct Field {
    inner: multer::Field<'static>,
}

impl Field {
    /// The field name from the `Content-Disposition` header.
    pub fn name(&self) -> Option<&str> {
        self.inner.name()
    }

    /// The file name from the `Content-Disposition` header, if present.
    ///
    /// A non-`None` value typically indicates a file upload field.
    pub fn file_name(&self) -> Option<&str> {
        self.inner.file_name()
    }

    /// The `Content-Type` of this field, if provided.
    pub fn content_type(&self) -> Option<&str> {
        self.inner.content_type().map(|m| m.as_ref())
    }

    /// The headers for this field.
    pub fn headers(&self) -> &http::HeaderMap {
        self.inner.headers()
    }

    /// Read the entire field data as bytes.
    pub async fn bytes(self) -> Result<Bytes, MultipartError> {
        self.inner.bytes().await.map_err(MultipartError)
    }

    /// Read the entire field as a UTF-8 string.
    pub async fn text(self) -> Result<String, MultipartError> {
        self.inner.text().await.map_err(MultipartError)
    }

    /// Read the next chunk of data from this field.
    ///
    /// Returns `None` when the field data is fully consumed.
    /// Use this for processing large files without buffering the
    /// entire field in memory.
    pub async fn chunk(&mut self) -> Result<Option<Bytes>, MultipartError> {
        self.inner.chunk().await.map_err(MultipartError)
    }
}

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

/// Error type for multipart parsing failures.
#[derive(Debug)]
pub struct MultipartError(multer::Error);

impl std::fmt::Display for MultipartError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "multipart error: {}", self.0)
    }
}

impl std::error::Error for MultipartError {}

impl IntoResponse for MultipartError {
    fn into_response(self) -> Response {
        let status = match &self.0 {
            multer::Error::FieldSizeExceeded { .. }
            | multer::Error::StreamSizeExceeded { .. } => StatusCode::PAYLOAD_TOO_LARGE,
            _ => StatusCode::BAD_REQUEST,
        };
        (status, self.to_string()).into_response()
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::router::Router;
    use crate::testing::TestClient;

    /// Build a multipart body with the given boundary and parts.
    fn build_multipart_body(boundary: &str, parts: &[(&str, Option<&str>, &[u8])]) -> Vec<u8> {
        let mut body = Vec::new();
        for &(name, file_name, data) in parts {
            body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
            if let Some(fname) = file_name {
                body.extend_from_slice(
                    format!(
                        "Content-Disposition: form-data; name=\"{name}\"; filename=\"{fname}\"\r\n"
                    )
                    .as_bytes(),
                );
                body.extend_from_slice(b"Content-Type: application/octet-stream\r\n");
            } else {
                body.extend_from_slice(
                    format!("Content-Disposition: form-data; name=\"{name}\"\r\n").as_bytes(),
                );
            }
            body.extend_from_slice(b"\r\n");
            body.extend_from_slice(data);
            body.extend_from_slice(b"\r\n");
        }
        body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
        body
    }

    #[tokio::test]
    async fn parse_text_field() {
        let client = TestClient::new(
            Router::new().post("/upload", |mut form: Multipart| async move {
                let field = form.next_field().await.unwrap().unwrap();
                assert_eq!(field.name(), Some("greeting"));
                assert!(field.file_name().is_none());
                let text = field.text().await.unwrap();
                text
            }),
        );

        let boundary = "----TestBoundary";
        let body = build_multipart_body(boundary, &[("greeting", None, b"hello world")]);

        let resp = client
            .post("/upload")
            .header(
                "content-type",
                &format!("multipart/form-data; boundary={boundary}"),
            )
            .body(body)
            .send()
            .await;

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.text().await, "hello world");
    }

    #[tokio::test]
    async fn parse_file_upload() {
        let client = TestClient::new(
            Router::new().post("/upload", |mut form: Multipart| async move {
                let field = form.next_field().await.unwrap().unwrap();
                assert_eq!(field.name(), Some("document"));
                assert_eq!(field.file_name(), Some("test.txt"));
                let data = field.bytes().await.unwrap();
                format!("{}:{}", data.len(), String::from_utf8_lossy(&data))
            }),
        );

        let boundary = "----FileBoundary";
        let body = build_multipart_body(
            boundary,
            &[("document", Some("test.txt"), b"file content here")],
        );

        let resp = client
            .post("/upload")
            .header(
                "content-type",
                &format!("multipart/form-data; boundary={boundary}"),
            )
            .body(body)
            .send()
            .await;

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.text().await, "17:file content here");
    }

    #[tokio::test]
    async fn parse_multiple_fields() {
        let client = TestClient::new(
            Router::new().post("/upload", |mut form: Multipart| async move {
                let mut names = Vec::new();
                while let Some(field) = form.next_field().await.unwrap() {
                    let name = field.name().unwrap_or("?").to_string();
                    let is_file = field.file_name().is_some();
                    let data = field.bytes().await.unwrap();
                    names.push(format!("{name}:{is_file}:{}", data.len()));
                }
                names.join(",")
            }),
        );

        let boundary = "----MultiBoundary";
        let body = build_multipart_body(
            boundary,
            &[
                ("name", None, b"Alice"),
                ("avatar", Some("pic.png"), &[0xFF, 0xD8, 0xFF, 0xE0]),
                ("bio", None, b"Hello!"),
            ],
        );

        let resp = client
            .post("/upload")
            .header(
                "content-type",
                &format!("multipart/form-data; boundary={boundary}"),
            )
            .body(body)
            .send()
            .await;

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.text().await,
            "name:false:5,avatar:true:4,bio:false:6"
        );
    }

    #[tokio::test]
    async fn rejects_missing_boundary() {
        let client = TestClient::new(
            Router::new().post("/upload", |mut form: Multipart| async move {
                let _ = form.next_field().await;
                "ok"
            }),
        );

        let resp = client
            .post("/upload")
            .header("content-type", "multipart/form-data")
            .body(b"some data".to_vec())
            .send()
            .await;

        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn rejects_non_multipart_content_type() {
        let client = TestClient::new(
            Router::new().post("/upload", |mut form: Multipart| async move {
                let _ = form.next_field().await;
                "ok"
            }),
        );

        let resp = client
            .post("/upload")
            .header("content-type", "application/json")
            .body(b"{}".to_vec())
            .send()
            .await;

        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn chunk_by_chunk_reading() {
        let client = TestClient::new(
            Router::new().post("/upload", |mut form: Multipart| async move {
                let mut field = form.next_field().await.unwrap().unwrap();
                let mut chunks = 0;
                let mut total = 0;
                while let Some(chunk) = field.chunk().await.unwrap() {
                    chunks += 1;
                    total += chunk.len();
                }
                format!("{chunks} chunks, {total} bytes")
            }),
        );

        let boundary = "----ChunkBoundary";
        let data = vec![0u8; 1024];
        let body = build_multipart_body(boundary, &[("data", Some("big.bin"), &data)]);

        let resp = client
            .post("/upload")
            .header(
                "content-type",
                &format!("multipart/form-data; boundary={boundary}"),
            )
            .body(body)
            .send()
            .await;

        assert_eq!(resp.status(), StatusCode::OK);
        let text = resp.text().await;
        assert!(text.contains("1024 bytes"), "got: {text}");
    }

    #[tokio::test]
    async fn multipart_error_display() {
        let err = MultipartError(multer::Error::NoBoundary);
        let text = format!("{err}");
        assert!(text.contains("multipart error"));
    }

    #[tokio::test]
    async fn multipart_error_into_response() {
        let err = MultipartError(multer::Error::NoBoundary);
        let resp = err.into_response();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }
}
