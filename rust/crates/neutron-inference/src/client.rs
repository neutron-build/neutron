//! HTTP client that talks to a running neutron-mojo inference server.

use std::pin::Pin;

use bytes::Bytes;
use futures_util::{Stream, TryStreamExt};
use http::{Method, Request as HyperRequest};
use http_body_util::{BodyExt, Full};
use hyper_util::client::legacy::{Client as HyperClient, connect::HttpConnector};
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
    pub base_url:    String,
    /// Path for non-streaming requests (default: `/inference`).
    pub infer_path:  String,
    /// Path for streaming requests (default: `/inference/stream`).
    pub stream_path: String,
}

impl Default for InferenceClientConfig {
    fn default() -> Self {
        Self {
            base_url:    "http://127.0.0.1:8080".to_string(),
            infer_path:  "/inference".to_string(),
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
    http:   HyperClient<HttpConnector, Full<Bytes>>,
}

impl InferenceClient {
    /// Create a new client with the given configuration.
    pub fn new(config: InferenceClientConfig) -> Self {
        let http = HyperClient::builder(TokioExecutor::new()).build(HttpConnector::new());
        Self { config, http }
    }

    /// Send a non-streaming inference request and collect the full response.
    pub async fn complete(
        &self,
        req: InferenceRequest,
    ) -> Result<InferenceResponse, InferError> {
        let url  = format!("{}{}", self.config.base_url, self.config.infer_path);
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
        let bytes  = resp
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
        let req  = InferenceRequest { stream: true, ..req };
        let url  = format!("{}{}", self.config.base_url, self.config.stream_path);
        let body = match serde_json::to_vec(&req) {
            Ok(b)  => b,
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
            Ok(r)  => r,
            Err(e) => {
                return Box::pin(futures_util::stream::once(async move {
                    Err(InferError::Http(e.to_string()))
                }));
            }
        };

        let result = self.http.request(http_req).await;

        let resp = match result {
            Ok(r)  => r,
            Err(e) => {
                return Box::pin(futures_util::stream::once(async move {
                    Err(InferError::Http(e.to_string()))
                }));
            }
        };

        if !resp.status().is_success() {
            let status = resp.status().as_u16();
            let bytes = resp.into_body().collect().await
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
    // Buffer incoming bytes into lines, parse "data: ..." SSE frames.
    async_stream::stream! {
        let mut buf = String::new();
        tokio::pin!(byte_stream);

        while let Some(result) = byte_stream.next().await {
            let bytes = match result {
                Ok(b)  => b,
                Err(e) => { yield Err(e); return; }
            };

            let text = match std::str::from_utf8(&bytes) {
                Ok(s)  => s,
                Err(_) => { yield Err(InferError::Protocol("invalid UTF-8".into())); return; }
            };

            buf.push_str(text);

            // Process all complete SSE frames (terminated by \n\n).
            while let Some(idx) = buf.find("\n\n") {
                let frame = buf[..idx].to_string();
                buf.drain(..idx + 2);

                for line in frame.lines() {
                    let data = if let Some(d) = line.strip_prefix("data: ") {
                        d
                    } else {
                        continue;
                    };

                    if data == "[DONE]" {
                        return;
                    }

                    match serde_json::from_str::<InferenceChunk>(data) {
                        Ok(chunk) => {
                            let done = chunk.done;
                            yield Ok(chunk);
                            if done { return; }
                        }
                        Err(e) => {
                            yield Err(InferError::Json(e));
                            return;
                        }
                    }
                }
            }
        }
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

        let byte_stream = stream::once(async {
            Ok::<_, InferError>(Bytes::from(frames))
        });

        let chunks: Vec<_> = parse_sse_stream(byte_stream)
            .collect::<Vec<_>>()
            .await;

        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].as_ref().unwrap().delta, "Hello");
        assert!(chunks[1].as_ref().unwrap().done);
    }

    #[tokio::test]
    async fn sse_parser_stops_on_done_sentinel() {
        use futures_util::stream;

        let frames = "data: [DONE]\n\n";
        let byte_stream = stream::once(async {
            Ok::<_, InferError>(Bytes::from(frames))
        });
        let chunks: Vec<_> = parse_sse_stream(byte_stream).collect::<Vec<_>>().await;
        assert!(chunks.is_empty());
    }
}
