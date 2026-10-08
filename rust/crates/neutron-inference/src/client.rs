//! HTTP client that talks to a running neutron-mojo inference server.

use std::pin::Pin;

use bytes::Bytes;
use futures_util::{Stream, TryStreamExt};
use http::{Method, Request as HyperRequest};
use http_body_util::{BodyExt, Full};
use hyper_util::client::legacy::{connect::HttpConnector, Client as HyperClient};
use hyper_util::rt::TokioExecutor;
use tokio_stream::StreamExt;

use crate::error::InferError;
use crate::request::InferenceRequest;
use crate::response::{InferenceChunk, InferenceResponse};

// ---------------------------------------------------------------------------
// InferenceClientConfig
// ---------------------------------------------------------------------------

/// Configuration for an [`InferenceClient`].
#[derive(Debug, Clone)]
pub struct InferenceClientConfig {
    /// Base URL of the inference server (e.g. `http://127.0.0.1:8080`).
    pub base_url: String,
    /// Path for non-streaming requests (default: `/inference`).
    pub infer_path: String,
    /// Path for streaming requests (default: `/inference/stream`).
    pub stream_path: String,
}

impl Default for InferenceClientConfig {
    fn default() -> Self {
        Self {
            base_url: "http://127.0.0.1:8080".to_string(),
            infer_path: "/inference".to_string(),
            stream_path: "/inference/stream".to_string(),
        }
    }
}

// ---------------------------------------------------------------------------
// InferenceClient
// ---------------------------------------------------------------------------

/// HTTP client for a neutron-mojo inference server.
///
/// Register as shared state and extract via `State<InferenceClient>`:
///
/// ```rust,ignore
/// let client = InferenceClient::new(InferenceClientConfig::default());
///
/// let router = Router::new()
///     .state(client)
///     .post("/generate", generate);
///
/// async fn generate(
///     State(client): State<InferenceClient>,
///     Json(req): Json<InferenceRequest>,
/// ) -> impl IntoResponse {
///     match req.stream {
///         true  => InferStream::new(client.stream(req).await).into_response(),
///         false => Json(client.complete(req).await?).into_response(),
///     }
/// }
/// ```
#[derive(Clone)]
pub struct InferenceClient {
    config: InferenceClientConfig,
    http: HyperClient<HttpConnector, Full<Bytes>>,
}

impl InferenceClient {
    /// Create a new client with the given configuration.
    pub fn new(config: InferenceClientConfig) -> Self {
        let http = HyperClient::builder(TokioExecutor::new()).build(HttpConnector::new());
        Self { config, http }
    }

    /// Send a non-streaming inference request and collect the full response.
    pub async fn complete(&self, req: InferenceRequest) -> Result<InferenceResponse, InferError> {
        let url = format!("{}{}", self.config.base_url, self.config.infer_path);
        let body = serde_json::to_vec(&req).map_err(InferError::Json)?;

        let http_req = HyperRequest::builder()
            .method(Method::POST)
            .uri(&url)
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(body)))
            .map_err(|e| InferError::Http(e.to_string()))?;

        let resp = self
            .http
            .request(http_req)
            .await
            .map_err(|e| InferError::Http(e.to_string()))?;

        let status = resp.status();
        let bytes = resp
            .into_body()
            .collect()
            .await
            .map_err(|e| InferError::Http(e.to_string()))?
            .to_bytes();

        if !status.is_success() {
            return Err(InferError::Status(
                status.as_u16(),
                String::from_utf8_lossy(&bytes).into_owned(),
            ));
        }

        serde_json::from_slice(&bytes).map_err(InferError::Json)
    }

    /// Send a streaming inference request, returning an async stream of chunks.
    ///
    /// Parses Server-Sent Events from the response body.
    pub async fn stream(
        &self,
        req: InferenceRequest,
    ) -> Pin<Box<dyn Stream<Item = Result<InferenceChunk, InferError>> + Send>> {
        let req = InferenceRequest {
            stream: true,
            ..req
        };
        let url = format!("{}{}", self.config.base_url, self.config.stream_path);
        let body = match serde_json::to_vec(&req) {
            Ok(b) => b,
            Err(e) => {
                return Box::pin(futures_util::stream::once(async move {
                    Err(InferError::Json(e))
                }));
            }
        };

        let http_req = match HyperRequest::builder()
            .method(Method::POST)
            .uri(&url)
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(body)))
        {
            Ok(r) => r,
            Err(e) => {
                return Box::pin(futures_util::stream::once(async move {
                    Err(InferError::Http(e.to_string()))
                }));
            }
        };

        let result = self.http.request(http_req).await;

        let resp = match result {
            Ok(r) => r,
            Err(e) => {
                return Box::pin(futures_util::stream::once(async move {
                    Err(InferError::Http(e.to_string()))
                }));
            }
        };

        if !resp.status().is_success() {
            let status = resp.status().as_u16();
            let bytes = resp
                .into_body()
                .collect()
                .await
                .map(|c| c.to_bytes())
                .unwrap_or_default();
            let msg = String::from_utf8_lossy(&bytes).into_owned();
            return Box::pin(futures_util::stream::once(async move {
                Err(InferError::Status(status, msg))
            }));
        }

        // Convert the body stream into an SSE line-by-line stream.
        let body_stream = resp
            .into_body()
            .into_data_stream()
            .map_err(|e| InferError::Http(e.to_string()));

        let chunk_stream = parse_sse_stream(body_stream);
        Box::pin(chunk_stream)
    }
}

// ---------------------------------------------------------------------------
// SSE parser
// ---------------------------------------------------------------------------

fn parse_sse_stream<S>(
    byte_stream: S,
) -> impl Stream<Item = Result<InferenceChunk, InferError>> + Send
where
    S: Stream<Item = Result<Bytes, InferError>> + Send,
{
    // Buffer incoming BYTES into complete SSE events, decoding UTF-8
    // incrementally. HTTP chunk boundaries carry no Unicode alignment
    // guarantee: a multibyte character split across two transport chunks is
    // valid SSE and previously aborted the stream with "invalid UTF-8".
    // CRLF separators, `data:` without a space, and multi-line data events
    // are handled per the SSE specification; the event buffer is bounded.
    const MAX_EVENT_BYTES: usize = 1024 * 1024;
    async_stream::stream! {
        let mut line_bytes = Vec::new();
        let mut event_bytes = 0usize;
        let mut data_lines: Vec<String> = Vec::new();
        tokio::pin!(byte_stream);
        macro_rules! flush_event {
            () => {
                if !data_lines.is_empty() {
                    let data = data_lines.join("\n");
                    data_lines.clear();
                    if data == "[DONE]" { return; }
                    match serde_json::from_str::<InferenceChunk>(&data) {
                        Ok(chunk) => { let done = chunk.done; yield Ok(chunk); if done { return; } }
                        Err(e) => { yield Err(InferError::Json(e)); return; }
                    }
                }
            };
        }
        macro_rules! take_line {
            () => {{
                // Normalize only AFTER reassembling bytes, including split CRLF.
                if line_bytes.last() == Some(&b'\r') { line_bytes.pop(); }
                let line = match std::str::from_utf8(&line_bytes) {
                    Ok(line) => line,
                    Err(_) => { yield Err(InferError::Protocol("invalid UTF-8 at SSE line/EOF".into())); return; }
                };
                if line.is_empty() { flush_event!(); event_bytes = 0; }
                else if let Some(d) = line.strip_prefix("data:") {
                    data_lines.push(d.strip_prefix(' ').unwrap_or(d).to_owned());
                }
                line_bytes.clear();
            }};
        }
        while let Some(result) = byte_stream.next().await {
            let bytes = match result { Ok(b) => b, Err(e) => { yield Err(e); return; } };
            // Transport chunks may contain arbitrarily many SMALL events.
            // Retain only the current event, counting completed data lines too.
            for byte in bytes {
                event_bytes += 1;
                if event_bytes > MAX_EVENT_BYTES {
                    yield Err(InferError::Protocol("SSE event exceeded 1 MiB".into())); return;
                }
                if byte == b'\n' { take_line!(); } else { line_bytes.push(byte); }
            }
        }
        if !line_bytes.is_empty() { take_line!(); }
        flush_event!();
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_values() {
        let cfg = InferenceClientConfig::default();
        assert!(cfg.base_url.starts_with("http://"));
        assert_eq!(cfg.infer_path, "/inference");
        assert_eq!(cfg.stream_path, "/inference/stream");
    }

    #[test]
    fn client_is_clone() {
        let c = InferenceClient::new(InferenceClientConfig::default());
        let _c2 = c.clone();
    }

    #[tokio::test]
    async fn sse_parser_emits_chunks_then_terminates() {
        use futures_util::stream;

        let frames = concat!(
            "data: {\"delta\":\"Hello\",\"done\":false,\"finish_reason\":null}\n\n",
            "data: {\"delta\":\" world\",\"done\":true,\"finish_reason\":\"stop\"}\n\n",
        );

        let byte_stream = stream::once(async { Ok::<_, InferError>(Bytes::from(frames)) });

        let chunks: Vec<_> = parse_sse_stream(byte_stream).collect::<Vec<_>>().await;

        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].as_ref().unwrap().delta, "Hello");
        assert!(chunks[1].as_ref().unwrap().done);
    }

    #[tokio::test]
    async fn sse_parser_stops_on_done_sentinel() {
        use futures_util::stream;

        let frames = "data: [DONE]\n\n";
        let byte_stream = stream::once(async { Ok::<_, InferError>(Bytes::from(frames)) });
        let chunks: Vec<_> = parse_sse_stream(byte_stream).collect::<Vec<_>>().await;
        assert!(chunks.is_empty());
    }

    /// Regression (RS-19): a multibyte character split across two transport
    /// chunks is valid SSE and must not abort the stream. Every byte offset
    /// of a Unicode event is exercised.
    #[tokio::test]
    async fn sse_parser_survives_utf8_split_at_every_boundary() {
        use futures_util::stream;

        let event = format!(
            "data: {}\n\n",
            serde_json::json!({"delta": "héllo → 🌍", "done": true, "finish_reason": "stop"})
        );
        let bytes = event.as_bytes();
        for split in 1..bytes.len() {
            let a = Bytes::copy_from_slice(&bytes[..split]);
            let b = Bytes::copy_from_slice(&bytes[split..]);
            let byte_stream = stream::iter(vec![Ok::<_, InferError>(a), Ok::<_, InferError>(b)]);
            let chunks: Vec<_> = parse_sse_stream(byte_stream).collect::<Vec<_>>().await;
            assert_eq!(
                chunks.len(),
                1,
                "split at {split} lost or duplicated events: {chunks:?}"
            );
            assert_eq!(chunks[0].as_ref().unwrap().delta, "héllo → 🌍");
            assert!(chunks[0].as_ref().unwrap().done);
        }
    }

    /// CRLF separators, `data:` without the optional space, and multi-line
    /// data events are all part of the SSE spec.
    #[tokio::test]
    async fn sse_parser_handles_crlf_bare_data_and_multiline() {
        use futures_util::stream;

        // Multi-line data is JOINED with \n per the spec — so split the JSON
        // so the reassembly parses.
        let frames = concat!(
            "data:{\"delta\":\"a\",\r\n",
            "data:\"done\":false,\"finish_reason\":null}\r\n",
            "\r\n",
        );
        let byte_stream = stream::once(async { Ok::<_, InferError>(Bytes::from(frames)) });
        let chunks: Vec<_> = parse_sse_stream(byte_stream).collect::<Vec<_>>().await;
        assert_eq!(chunks.len(), 1, "{chunks:?}");
        // The two data lines were joined with \n into one valid JSON event.
        assert_eq!(chunks[0].as_ref().unwrap().delta, "a");
        assert!(!chunks[0].as_ref().unwrap().done);
    }

    /// Truly invalid UTF-8 (not merely a split sequence) still fails with a
    /// protocol error instead of passing mojibake through.
    #[tokio::test]
    async fn sse_parser_rejects_invalid_utf8() {
        use futures_util::stream;

        let byte_stream = stream::iter(vec![Ok::<_, InferError>(Bytes::from_static(
            b"data: \xff\xfe\n\n",
        ))]);
        let chunks: Vec<_> = parse_sse_stream(byte_stream).collect::<Vec<_>>().await;
        assert_eq!(chunks.len(), 1);
        assert!(chunks[0].is_err());
    }

    /// A bounded event buffer: an endless stream with no blank line must
    /// terminate with an error, not buffer forever.
    #[tokio::test]
    async fn sse_parser_bounds_unterminated_events() {
        use futures_util::stream;

        let chunk = Bytes::from(vec![b'a'; 256 * 1024]);
        let byte_stream = stream::iter((0..8).map(move |_| Ok::<_, InferError>(chunk.clone())));
        let chunks: Vec<_> = parse_sse_stream(byte_stream).collect::<Vec<_>>().await;
        assert!(chunks.last().map(|c| c.is_err()).unwrap_or(false));
    }

    #[tokio::test]
    async fn every_split_of_crlf_and_unicode_preserves_events() {
        let event = "data:{\"delta\":\"é文\",\"done\":false,\"finish_reason\":null}\r\n\r\n";
        for split in 0..=event.len() {
            let bytes = event.as_bytes();
            let stream = futures_util::stream::iter(vec![
                Ok(Bytes::copy_from_slice(&bytes[..split])),
                Ok(Bytes::copy_from_slice(&bytes[split..])),
            ]);
            let results: Vec<_> = parse_sse_stream(stream).collect().await;
            assert_eq!(results.len(), 1, "split {split}");
            assert_eq!(results[0].as_ref().unwrap().delta, "é文");
        }
    }
    #[tokio::test]
    async fn accumulated_lines_are_bounded_but_large_multi_event_chunk_is_valid() {
        let event = "data:{\"delta\":\"x\",\"done\":false,\"finish_reason\":null}\n\n";
        let data = event.repeat(20000);
        assert!(data.len() > 1024 * 1024);
        let results: Vec<_> =
            parse_sse_stream(futures_util::stream::iter(vec![Ok(Bytes::from(data))]))
                .collect()
                .await;
        assert_eq!(results.len(), 20000);
        assert!(results.iter().all(Result::is_ok));
        let line = format!("data:{}\n", "x".repeat(1024));
        let results: Vec<_> = parse_sse_stream(futures_util::stream::iter(
            (0..2048).map(|_| Ok(Bytes::from(line.clone()))),
        ))
        .collect()
        .await;
        assert_eq!(results.len(), 1);
        assert!(matches!(results[0], Err(InferError::Protocol(_))));
        let results: Vec<_> = parse_sse_stream(futures_util::stream::iter(vec![Ok(
            Bytes::from_static(b"data:\xe2\x82"),
        )]))
        .collect()
        .await;
        assert!(matches!(results[0], Err(InferError::Protocol(_))));
    }
}
