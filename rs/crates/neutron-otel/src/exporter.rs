use std::sync::Arc;
use tokio::sync::Mutex;

use bytes::Bytes;
use http_body_util::Full;
use hyper::client::conn::http1;
use hyper::Request;
use hyper_util::rt::TokioIo;
use serde_json::json;
use tokio::net::TcpStream;

use crate::config::OtelConfig;
use crate::error::OtelError;
use crate::span::SpanData;

/// Batches spans and exports them to an OTLP/JSON endpoint.
///
/// Internally maintains a buffer protected by a `Mutex`. Call
/// [`OtlpExporter::push`] to add spans and [`OtlpExporter::flush`] to export
/// all buffered spans immediately.
#[derive(Clone)]
pub struct OtlpExporter {
    config: Arc<OtelConfig>,
    buffer: Arc<Mutex<Vec<SpanData>>>,
}

impl OtlpExporter {
    /// Create a new exporter with the given configuration.
    pub fn new(config: OtelConfig) -> Result<Self, OtelError> {
        config.validate()?;
        Ok(OtlpExporter {
            config: Arc::new(config),
            buffer: Arc::new(Mutex::new(Vec::new())),
        })
    }

    /// Push a span into the internal buffer.
    ///
    /// If the buffer reaches `config.batch_size`, the spans are automatically
    /// flushed to the collector.
    pub async fn push(&self, span: SpanData) -> Result<(), OtelError> {
        let should_flush = {
            let mut buf = self.buffer.lock().await;
            buf.push(span);
            buf.len() >= self.config.batch_size
        };
        if should_flush {
            self.flush().await?;
        }
        Ok(())
    }

    /// Export all buffered spans to the OTLP endpoint and clear the buffer.
    pub async fn flush(&self) -> Result<(), OtelError> {
        let spans: Vec<SpanData> = {
            let mut buf = self.buffer.lock().await;
            std::mem::take(&mut *buf)
        };
        if spans.is_empty() {
            return Ok(());
        }
        self.export_spans(&spans).await
    }

    /// Number of spans currently buffered (not yet exported).
    pub async fn buffered_count(&self) -> usize {
        self.buffer.lock().await.len()
    }

    async fn export_spans(&self, spans: &[SpanData]) -> Result<(), OtelError> {
        let span_jsons: Vec<_> = spans.iter().map(|s| s.to_otlp_json()).collect();
        let body = json!({
            "resourceSpans": [{
                "resource": {
                    "attributes": [{
                        "key": "service.name",
                        "value": { "stringValue": self.config.service_name }
                    }]
                },
                "scopeSpans": [{
                    "scope": { "name": "neutron-otel", "version": "0.1.0" },
                    "spans": span_jsons
                }]
            }]
        });

        let body_bytes = serde_json::to_vec(&body)
            .map_err(|e| OtelError::Export(e.to_string()))?;

        let traces_url = self.config.traces_url();
        let url: hyper::Uri = traces_url
            .parse()
            .map_err(|e: hyper::http::uri::InvalidUri| OtelError::Config(e.to_string()))?;

        let host = url.host().ok_or_else(|| OtelError::Config("missing host".into()))?.to_string();
        let port = url.port_u16().unwrap_or(80);

        let stream = TcpStream::connect(format!("{host}:{port}"))
            .await
            .map_err(|e| OtelError::Connect(e.to_string()))?;
        let io = TokioIo::new(stream);

        let (mut sender, conn) = http1::handshake::<_, Full<Bytes>>(io).await
            .map_err(|e| OtelError::Export(e.to_string()))?;
        tokio::spawn(async move { let _ = conn.await; });

        let req = Request::builder()
            .method("POST")
            .uri(url)
            .header("content-type", "application/json")
            .header("host", &host)
            .body(Full::<Bytes>::from(body_bytes))
            .map_err(|e| OtelError::Export(e.to_string()))?;

        let resp = sender.send_request(req).await
            .map_err(|e| OtelError::Export(e.to_string()))?;

        if !resp.status().is_success() {
            return Err(OtelError::Export(format!(
                "OTLP endpoint returned {}",
                resp.status()
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::{random_span_id, random_trace_id};
    use crate::span::{AttributeValue, SpanStatus};

    fn make_span(name: &str) -> SpanData {
        SpanData {
            trace_id: random_trace_id(),
            span_id: random_span_id(),
            parent_span_id: None,
            name: name.to_string(),
            start_ns: 1_000_000,
            end_ns: 2_000_000,
            status: SpanStatus::Ok,
            attributes: vec![("svc".to_string(), AttributeValue::String("test".to_string()))],
        }
    }

    #[tokio::test]
    async fn new_with_valid_config() {
        let cfg = OtelConfig::new("svc");
        assert!(OtlpExporter::new(cfg).is_ok());
    }

    #[tokio::test]
    async fn new_with_empty_service_name_fails() {
        let cfg = OtelConfig::new("");
        assert!(OtlpExporter::new(cfg).is_err());
    }

    #[tokio::test]
    async fn push_adds_to_buffer() {
        let exp = OtlpExporter::new(OtelConfig::new("svc")).unwrap();
        exp.push(make_span("s1")).await.unwrap();
        assert_eq!(exp.buffered_count().await, 1);
    }

    #[tokio::test]
    async fn push_multiple_adds_all() {
        let exp = OtlpExporter::new(OtelConfig::new("svc")).unwrap();
        exp.push(make_span("a")).await.unwrap();
        exp.push(make_span("b")).await.unwrap();
        exp.push(make_span("c")).await.unwrap();
        assert_eq!(exp.buffered_count().await, 3);
    }

    #[tokio::test]
    async fn flush_empty_buffer_is_ok() {
        let exp = OtlpExporter::new(OtelConfig::new("svc")).unwrap();
        assert!(exp.flush().await.is_ok());
    }

    #[tokio::test]
    async fn flush_clears_buffer_even_on_network_error() {
        // Export to a non-existent endpoint — flush will fail but buffer is drained
        let cfg = OtelConfig::new("svc").endpoint("http://127.0.0.1:19999");
        let exp = OtlpExporter::new(cfg).unwrap();
        exp.buffer.lock().await.push(make_span("x"));
        // flush will fail (nothing listening on 19999) — but buffer should be cleared
        let _ = exp.flush().await;
        assert_eq!(exp.buffered_count().await, 0);
    }

    #[tokio::test]
    async fn batch_auto_flush_clears_buffer() {
        // Set batch_size=2, push 2 — triggers auto flush (to non-existent endpoint)
        let cfg = OtelConfig::new("svc")
            .endpoint("http://127.0.0.1:19999")
            .batch_size(2);
        let exp = OtlpExporter::new(cfg).unwrap();
        let _ = exp.push(make_span("a")).await;
        // After first push buffer has 1 span — no flush yet
        let _ = exp.push(make_span("b")).await;
        // After second push buffer had 2 (>= batch_size) — flush attempted (fails) but buffer cleared
        assert_eq!(exp.buffered_count().await, 0);
    }

    #[tokio::test]
    async fn exporter_is_clone() {
        let exp = OtlpExporter::new(OtelConfig::new("svc")).unwrap();
        let exp2 = exp.clone();
        exp.push(make_span("a")).await.unwrap();
        // Both clones share the same buffer
        assert_eq!(exp2.buffered_count().await, 1);
    }

    #[tokio::test]
    async fn buffered_count_starts_at_zero() {
        let exp = OtlpExporter::new(OtelConfig::new("svc")).unwrap();
        assert_eq!(exp.buffered_count().await, 0);
    }
}
