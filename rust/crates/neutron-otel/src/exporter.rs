use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
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

/// Type-erased IO stream for scheme-aware transports.
trait MaybeTls: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin {}
impl<T> MaybeTls for T where T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin {}

/// Verified-TLS connector (webpki root store, no client auth).
fn tls_connector() -> tokio_rustls::TlsConnector {
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let cfg = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .expect("supported TLS versions")
    .with_root_certificates(roots)
    .with_no_client_auth();
    tokio_rustls::TlsConnector::from(Arc::new(cfg))
}

/// Batches spans and exports them to an OTLP/JSON endpoint.
///
/// Internally maintains a buffer protected by a `Mutex`. Call
/// [`OtlpExporter::push`] to add spans and [`OtlpExporter::flush`] to export
/// all buffered spans immediately.
struct Worker {
    task: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    stopped: AtomicBool,
    dropped: AtomicU64,
    flush_lock: Mutex<()>,
    shutdown_lock: Mutex<()>,
    stop_flush: tokio::sync::Notify,
    in_flight: AtomicU64,
    exported: AtomicU64,
    wake: Arc<tokio::sync::Notify>,
}
struct BatchGuard {
    buffer: Arc<std::sync::Mutex<Vec<SpanData>>>,
    worker: Arc<Worker>,
    spans: Option<Vec<SpanData>>,
    capacity: usize,
}
impl Drop for BatchGuard {
    fn drop(&mut self) {
        if let Some(spans) = self.spans.take() {
            let mut buffer = self.buffer.lock().unwrap();
            let kept = spans.len().min(self.capacity.saturating_sub(buffer.len()));
            self.worker
                .dropped
                .fetch_add((spans.len() - kept) as u64, Ordering::Relaxed);
            let count = spans.len();
            buffer.splice(0..0, spans.into_iter().take(kept));
            self.worker
                .in_flight
                .fetch_sub(count as u64, Ordering::Relaxed);
        }
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        if let Some(task) = self.task.get_mut().unwrap().take() {
            task.abort();
        }
    }
}

#[derive(Clone)]
pub struct OtlpExporter {
    config: Arc<OtelConfig>,
    buffer: Arc<std::sync::Mutex<Vec<SpanData>>>,
    worker: Arc<Worker>,
}

impl OtlpExporter {
    /// Create a new exporter with the given configuration.
    pub fn new(config: OtelConfig) -> Result<Self, OtelError> {
        config.validate()?;
        Ok(OtlpExporter {
            config: Arc::new(config),
            buffer: Arc::new(std::sync::Mutex::new(Vec::new())),
            worker: Arc::new(Worker {
                task: std::sync::Mutex::new(None),
                stopped: AtomicBool::new(false),
                dropped: AtomicU64::new(0),
                flush_lock: Mutex::new(()),
                shutdown_lock: Mutex::new(()),
                stop_flush: tokio::sync::Notify::new(),
                in_flight: AtomicU64::new(0),
                exported: AtomicU64::new(0),
                wake: Arc::new(tokio::sync::Notify::new()),
            }),
        })
    }

    /// Push a span into the internal buffer.
    ///
    /// If the buffer reaches `config.batch_size`, the spans are automatically
    /// flushed to the collector.
    /// Synchronous bounded producer admission. No per-span task is spawned.
    /// The same lock serializes admission with the shutdown barrier.
    pub fn try_push(&self, span: SpanData) -> Result<(), OtelError> {
        self.start_worker();
        let mut buf = self.buffer.lock().unwrap();
        if self.worker.stopped.load(Ordering::Acquire)
            || buf.len() >= self.config.batch_size.saturating_mul(2)
        {
            self.worker.dropped.fetch_add(1, Ordering::Relaxed);
            return Err(OtelError::Export("span admission closed or full".into()));
        }
        buf.push(span);
        if buf.len() >= self.config.batch_size {
            self.worker.wake.notify_one();
        }
        Ok(())
    }

    pub async fn push(&self, span: SpanData) -> Result<(), OtelError> {
        self.try_push(span)?;
        let should_flush = self.buffer.lock().unwrap().len() >= self.config.batch_size;
        if should_flush {
            self.flush().await?;
        }
        Ok(())
    }

    /// Flush with one five-second deadline covering the public mutex queue
    /// and transport. Failure/cancellation retains the owned batch.
    pub async fn flush(&self) -> Result<(), OtelError> {
        self.flush_until(tokio::time::Instant::now() + Duration::from_secs(5), false)
            .await
    }

    async fn flush_until(
        &self,
        deadline: tokio::time::Instant,
        terminal: bool,
    ) -> Result<(), OtelError> {
        // Register before checking stopped so shutdown cannot be missed.
        let stop = self.worker.stop_flush.notified();
        tokio::pin!(stop);
        stop.as_mut().enable();
        if !terminal && self.worker.stopped.load(Ordering::Acquire) {
            return Err(OtelError::Export("exporter stopped; spans retained".into()));
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(OtelError::Export(
                "flush deadline; spans retained or owned by active export".into(),
            ));
        }
        let _flush = tokio::time::timeout_at(deadline, self.worker.flush_lock.lock())
            .await
            .map_err(|_| {
                OtelError::Export(
                    "flush deadline while queued; spans retained or owned by active export".into(),
                )
            })?;
        if !terminal && self.worker.stopped.load(Ordering::Acquire) {
            return Err(OtelError::Export("exporter stopped; spans retained".into()));
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(OtelError::Export("flush deadline; spans retained".into()));
        }
        let spans = {
            let mut buffer = self.buffer.lock().unwrap();
            let spans = std::mem::take(&mut *buffer);
            self.worker
                .in_flight
                .fetch_add(spans.len() as u64, Ordering::Relaxed);
            spans
        };
        if spans.is_empty() {
            return Ok(());
        }
        let mut batch = BatchGuard {
            buffer: self.buffer.clone(),
            worker: self.worker.clone(),
            spans: Some(spans),
            capacity: self.config.batch_size.saturating_mul(2),
        };
        let export =
            tokio::time::timeout_at(deadline, self.export_spans(batch.spans.as_ref().unwrap()));
        let result = tokio::select! {
            biased;
            _ = &mut stop, if !terminal => Err(OtelError::Export("public export interrupted by shutdown; spans retained".into())),
            result = export => result.unwrap_or_else(|_| Err(OtelError::Export("flush deadline; spans retained".into()))),
        };
        if result.is_ok() {
            let count = batch.spans.take().unwrap().len() as u64;
            // Serialize accounting with buffered/pending snapshots.
            let _buffer = self.buffer.lock().unwrap();
            self.worker.in_flight.fetch_sub(count, Ordering::Relaxed);
            self.worker.exported.fetch_add(count, Ordering::Relaxed);
        }
        // Failure or cancellation returns the batch synchronously through Drop.
        result
    }

    fn start_worker(&self) {
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let mut task = self.worker.task.lock().unwrap();
        if task.is_some() || self.worker.stopped.load(Ordering::Acquire) {
            return;
        }
        let buffer = Arc::downgrade(&self.buffer);
        let worker = Arc::downgrade(&self.worker);
        let config = self.config.clone();
        let wake = self.worker.wake.clone();
        *task = Some(runtime.spawn(async move {
            let mut interval =
                tokio::time::interval(Duration::from_millis(config.export_interval_ms));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            interval.tick().await;
            loop {
                tokio::select! { _ = interval.tick() => {}, _ = wake.notified() => {} }
                let (Some(buffer), Some(worker)) = (buffer.upgrade(), worker.upgrade()) else {
                    break;
                };
                if worker.stopped.load(Ordering::Acquire) {
                    break;
                }
                let exporter = OtlpExporter {
                    config: config.clone(),
                    buffer,
                    worker,
                };
                if let Err(error) = exporter.flush().await {
                    tracing::warn!(%error, "OTLP periodic export failed; retained bounded batch");
                }
            }
        }));
    }

    /// Fence admission and stop/join the worker, then perform a final export.
    /// One absolute five-second deadline includes shutdown/flush mutex queues,
    /// owned-worker joining and transport. Err means pending spans may remain;
    /// pending_count includes a batch still owned by another public call.
    pub async fn shutdown(&self) -> Result<(), OtelError> {
        self.shutdown_until(tokio::time::Instant::now() + Duration::from_secs(5))
            .await
    }

    async fn shutdown_until(&self, deadline: tokio::time::Instant) -> Result<(), OtelError> {
        {
            let _barrier = self.buffer.lock().unwrap();
            self.worker.stopped.store(true, Ordering::Release);
        }
        self.worker.stop_flush.notify_waiters();
        let _shutdown = tokio::time::timeout_at(deadline, self.worker.shutdown_lock.lock())
            .await
            .map_err(|_| {
                OtelError::Export("shutdown deadline while queued; drain incomplete".into())
            })?;
        let task = self.worker.task.lock().unwrap().take();
        if let Some(mut task) = task {
            task.abort();
            if tokio::time::timeout_at(deadline, &mut task).await.is_err() {
                // Retain ownership for a subsequent join; never claim full drain.
                *self.worker.task.lock().unwrap() = Some(task);
                return Err(OtelError::Export(
                    "shutdown deadline joining worker; drain incomplete".into(),
                ));
            }
        }
        self.flush_until(deadline, true).await
    }

    /// Accepted work awaiting export, including a batch owned by an active flush.
    pub fn pending_count(&self) -> usize {
        let buffer = self.buffer.lock().unwrap();
        buffer
            .len()
            .saturating_add(self.worker.in_flight.load(Ordering::Relaxed) as usize)
    }

    pub fn exported_count(&self) -> u64 {
        self.worker.exported.load(Ordering::Relaxed)
    }

    pub fn dropped_count(&self) -> u64 {
        self.worker.dropped.load(Ordering::Relaxed)
    }

    /// Number of spans currently buffered (not yet exported).
    pub async fn buffered_count(&self) -> usize {
        self.buffer.lock().unwrap().len()
    }

    #[cfg(test)]
    pub(crate) fn buffered_spans(&self) -> Vec<SpanData> {
        self.buffer.lock().unwrap().clone()
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

        let body_bytes = serde_json::to_vec(&body).map_err(|e| OtelError::Export(e.to_string()))?;

        let traces_url = self.config.traces_url();
        let url: hyper::Uri = traces_url
            .parse()
            .map_err(|e: hyper::http::uri::InvalidUri| OtelError::Config(e.to_string()))?;

        let host = url
            .host()
            .ok_or_else(|| OtelError::Config("missing host".into()))?
            .to_string();
        // Scheme-aware transport (RS-24): an https endpoint previously got
        // the same raw TcpStream + HTTP/1 treatment as http (default port
        // 80, no TLS), silently exporting trace attributes in plaintext and
        // failing against real TLS collectors. https now goes through a
        // verified rustls connection; unknown schemes are refused.
        let scheme = url.scheme_str().unwrap_or("http");
        let port = url
            .port_u16()
            .unwrap_or(if scheme == "https" { 443 } else { 80 });

        let io: TokioIo<Box<dyn MaybeTls>> = match scheme {
            "https" => {
                let tcp = TcpStream::connect(format!("{host}:{port}"))
                    .await
                    .map_err(|e| OtelError::Connect(e.to_string()))?;
                let server_name = rustls::pki_types::ServerName::try_from(host.clone())
                    .map_err(|e| OtelError::Config(format!("invalid TLS server name: {e}")))?
                    .to_owned();
                let tls = tls_connector()
                    .connect(server_name, tcp)
                    .await
                    .map_err(|e| OtelError::Connect(format!("TLS handshake: {e}")))?;
                TokioIo::new(Box::new(tls) as Box<dyn MaybeTls>)
            }
            "http" => {
                // Plaintext is the documented local-collector path, kept
                // explicit: trace attributes are exported unencrypted.
                let tcp = TcpStream::connect(format!("{host}:{port}"))
                    .await
                    .map_err(|e| OtelError::Connect(e.to_string()))?;
                TokioIo::new(Box::new(tcp) as Box<dyn MaybeTls>)
            }
            other => {
                return Err(OtelError::Config(format!(
                    "unsupported endpoint scheme: {other}"
                )))
            }
        };

        let (mut sender, conn) = http1::handshake::<_, Full<Bytes>>(io)
            .await
            .map_err(|e| OtelError::Export(e.to_string()))?;
        tokio::pin!(conn);

        let req = Request::builder()
            .method("POST")
            .uri(url)
            .header("content-type", "application/json")
            .header("host", &host)
            .body(Full::<Bytes>::from(body_bytes))
            .map_err(|e| OtelError::Export(e.to_string()))?;

        let resp = tokio::select! {
            result = sender.send_request(req) => result.map_err(|e| OtelError::Export(e.to_string()))?,
            result = &mut conn => { return Err(OtelError::Export(format!("collector connection ended before response: {result:?}"))); }
        };

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
            attributes: vec![(
                "svc".to_string(),
                AttributeValue::String("test".to_string()),
            )],
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
    async fn flush_retains_buffer_on_network_error() {
        // Export to a non-existent endpoint — flush will fail but buffer is drained
        let cfg = OtelConfig::new("svc").endpoint("http://127.0.0.1:19999");
        let exp = OtlpExporter::new(cfg).unwrap();
        exp.buffer.lock().unwrap().push(make_span("x"));
        // flush will fail (nothing listening on 19999) — but buffer should be cleared
        let _ = exp.flush().await;
        assert_eq!(exp.buffered_count().await, 1);
    }

    #[tokio::test]
    async fn batch_auto_flush_retains_buffer_on_error() {
        // Set batch_size=2, push 2 — triggers auto flush (to non-existent endpoint)
        let cfg = OtelConfig::new("svc")
            .endpoint("http://127.0.0.1:19999")
            .batch_size(2);
        let exp = OtlpExporter::new(cfg).unwrap();
        let _ = exp.push(make_span("a")).await;
        // After first push buffer has 1 span — no flush yet
        let _ = exp.push(make_span("b")).await;
        // Failed export retains both spans for a bounded retry.
        assert_eq!(exp.buffered_count().await, 2);
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

    /// Regression (RS-24): an https endpoint must actually use TLS — the
    /// exporter previously dialed port 80 with a raw TcpStream for every
    /// scheme. Against a TLS listener with an untrusted certificate the
    /// export must fail closed (handshake error), never falling back to
    /// plaintext.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn https_endpoint_fails_closed_against_untrusted_certificate() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let tls_cfg = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .expect("supported TLS versions")
        .with_no_client_auth()
        .with_single_cert(
            vec![rustls::pki_types::CertificateDer::from(
                cert.cert.der().to_vec(),
            )],
            rustls::pki_types::PrivateKeyDer::try_from(cert.key_pair.serialize_der()).unwrap(),
        )
        .unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(tls_cfg));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let acc = acceptor.clone();
        let saw_plain_http = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let saw2 = Arc::clone(&saw_plain_http);
        tokio::spawn(async move {
            use hyper::service::service_fn;
            use hyper_util::rt::TokioIo;
            if let Ok((sock, _)) = listener.accept().await {
                if let Ok(tls) = acc.accept(sock).await {
                    let svc = service_fn(|_req: hyper::Request<hyper::body::Incoming>| {
                        let h = Arc::clone(&saw2);
                        async move {
                            h.store(true, std::sync::atomic::Ordering::SeqCst);
                            Ok::<_, std::convert::Infallible>(http::Response::new(
                                http_body_util::Full::new(Bytes::new()),
                            ))
                        }
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(tls), svc)
                        .await;
                }
            }
        });

        let config =
            crate::config::OtelConfig::new("svc").endpoint(format!("https://localhost:{port}"));
        let exporter = OtlpExporter::new(config).unwrap();
        exporter.push(make_span("test")).await.unwrap();
        let err = exporter.flush().await.unwrap_err();
        assert!(
            err.to_string().contains("TLS") || err.to_string().contains("handshake"),
            "expected TLS handshake failure, got: {err}"
        );
        assert!(
            !saw_plain_http.load(std::sync::atomic::Ordering::SeqCst),
            "no trace payload may be delivered after a failed handshake"
        );
    }
    #[tokio::test]
    async fn low_volume_is_exported_on_interval_and_shutdown_flushes() {
        use http_body_util::BodyExt;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let received = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let count = received.clone();
        let server = tokio::spawn(async move {
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                let count = count.clone();
                tokio::spawn(async move {
                    let service = hyper::service::service_fn(
                        move |req: hyper::Request<hyper::body::Incoming>| {
                            let count = count.clone();
                            async move {
                                let bytes = req.into_body().collect().await.unwrap().to_bytes();
                                let value: serde_json::Value =
                                    serde_json::from_slice(&bytes).unwrap();
                                count.fetch_add(
                                    value["resourceSpans"][0]["scopeSpans"][0]["spans"]
                                        .as_array()
                                        .unwrap()
                                        .len(),
                                    Ordering::SeqCst,
                                );
                                Ok::<_, std::convert::Infallible>(hyper::Response::new(Full::new(
                                    Bytes::new(),
                                )))
                            }
                        },
                    );
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(socket), service)
                        .await;
                });
            }
        });
        let exp = OtlpExporter::new(
            OtelConfig::new("test")
                .endpoint(format!("http://{addr}"))
                .batch_size(100)
                .export_interval_ms(20),
        )
        .unwrap();
        exp.push(make_span("low-volume")).await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while received.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        exp.push(make_span("shutdown")).await.unwrap();
        exp.shutdown().await.unwrap();
        assert_eq!(received.load(Ordering::SeqCst), 2);
        assert_eq!(exp.buffered_count().await, 0);
        assert!(exp.push(make_span("too-late")).await.is_err());
        server.abort();
    }

    #[tokio::test]
    async fn outage_is_bounded_and_loss_is_counted() {
        let exp = OtlpExporter::new(
            OtelConfig::new("test")
                .endpoint("http://127.0.0.1:1")
                .batch_size(2),
        )
        .unwrap();
        for i in 0..8 {
            let _ = exp.push(make_span(&format!("span-{i}"))).await;
        }
        assert_eq!(exp.buffered_count().await, 4);
        assert_eq!(exp.dropped_count(), 4);
        assert!(exp.shutdown().await.is_err());
        assert_eq!(exp.buffered_count().await, 4);
    }
    #[tokio::test]
    async fn canceled_flush_returns_owned_batch() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (entered, accepted) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (_socket, _) = listener.accept().await.unwrap();
            entered.send(()).unwrap();
            std::future::pending::<()>().await;
        });
        let exp =
            OtlpExporter::new(OtelConfig::new("test").endpoint(format!("http://{addr}"))).unwrap();
        exp.push(make_span("held")).await.unwrap();
        let exporter = exp.clone();
        let flushing = tokio::spawn(async move { exporter.flush().await });
        tokio::time::timeout(Duration::from_secs(1), accepted)
            .await
            .unwrap()
            .unwrap();
        flushing.abort();
        let _ = flushing.await;
        assert_eq!(exp.buffered_count().await, 1);
        assert_eq!(exp.dropped_count(), 0);
        server.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn synchronous_burst_is_bounded_before_worker_poll_and_shutdown_accounts_admission() {
        let exp = OtlpExporter::new(
            OtelConfig::new("test")
                .batch_size(4)
                .endpoint("http://127.0.0.1:1"),
        )
        .unwrap();
        for _ in 0..1000 {
            let _ = exp.try_push(make_span("burst"));
        }
        assert_eq!(exp.buffered_count().await, 8);
        assert_eq!(exp.dropped_count(), 992);
        let _ = exp.shutdown().await;
        assert!(exp.try_push(make_span("late")).is_err());
        assert_eq!(exp.dropped_count(), 993);
        assert_eq!(exp.buffered_count().await, 8); // failed final batch retained/accounted
    }
    #[tokio::test(flavor = "current_thread")]
    async fn shutdown_deadline_includes_contended_public_flush_queue_and_retains_accounting() {
        use std::future::Future;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (entered, accepted) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (first, _) = listener.accept().await.unwrap();
            entered.send(()).unwrap();
            let (second, _) = listener.accept().await.unwrap();
            let _sockets = (first, second); // stalled headers on both exports
            std::future::pending::<()>().await;
        });
        let exp = OtlpExporter::new(
            OtelConfig::new("test")
                .endpoint(format!("http://{addr}"))
                .batch_size(100)
                .export_interval_ms(60_000),
        )
        .unwrap();
        exp.try_push(make_span("held")).unwrap();
        let exporter = exp.clone();
        let owner = tokio::spawn(async move { exporter.flush().await });
        tokio::time::timeout(Duration::from_secs(1), accepted)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(exp.pending_count(), 1);
        assert_eq!(exp.buffered_count().await, 0); // owned, not lost
        let mut queued = tokio::task::JoinSet::new();
        let mut admissions = Vec::new();
        for _ in 0..32 {
            let exporter = exp.clone();
            let (waiting, admitted) = tokio::sync::oneshot::channel();
            admissions.push(admitted);
            queued.spawn(async move {
                let flush = exporter.flush();
                tokio::pin!(flush);
                let mut waiting = Some(waiting);
                std::future::poll_fn(|cx| {
                    let result = flush.as_mut().poll(cx);
                    if result.is_pending() {
                        if let Some(waiting) = waiting.take() {
                            let _ = waiting.send(());
                        }
                    }
                    result
                })
                .await
            });
        }
        for admitted in admissions {
            tokio::time::timeout(Duration::from_secs(1), admitted)
                .await
                .unwrap()
                .unwrap();
        }
        let start = tokio::time::Instant::now();
        let result = exp.shutdown_until(start + Duration::from_millis(80)).await;
        assert!(result.is_err());
        assert!(
            start.elapsed() < Duration::from_millis(500),
            "deadline grew with queue length"
        );
        assert!(tokio::time::timeout(Duration::from_secs(1), owner)
            .await
            .unwrap()
            .unwrap()
            .is_err());
        while let Some(result) = tokio::time::timeout(Duration::from_secs(1), queued.join_next())
            .await
            .unwrap()
        {
            assert!(result.unwrap().is_err());
        }
        assert_eq!(exp.pending_count(), 1);
        assert_eq!(exp.buffered_count().await, 1);
        assert_eq!(exp.exported_count(), 0);
        assert_eq!(exp.dropped_count(), 0);
        assert_eq!(
            exp.pending_count() as u64 + exp.exported_count() + exp.dropped_count(),
            1
        );
        server.abort();
        let _ = server.await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn flush_deadline_covers_mutex_wait_without_taking_the_batch() {
        let exp = OtlpExporter::new(OtelConfig::new("test").batch_size(100)).unwrap();
        exp.try_push(make_span("queued")).unwrap();
        let held = exp.worker.flush_lock.lock().await;
        let start = tokio::time::Instant::now();
        assert!(exp
            .flush_until(start + Duration::from_millis(20), false)
            .await
            .is_err());
        assert!(start.elapsed() < Duration::from_millis(250));
        assert_eq!(exp.pending_count(), 1);
        assert_eq!(exp.dropped_count(), 0);
        drop(held);
        // Stop timer ownership without performing a network export in this case.
        exp.worker.stopped.store(true, Ordering::Release);
        let task = exp.worker.task.lock().unwrap().take();
        if let Some(task) = task {
            task.abort();
            let _ = task.await;
        }
    }
}
