//! HTTP/3 server over QUIC (requires the `http3` feature).
//!
//! Uses [`quinn`] for QUIC transport and [`h3`] for HTTP/3 framing.
//! TLS 1.3 is mandatory for QUIC — pass a [`TlsConfig`] to configure
//! certificates.
//!
//! Mounted via [`Neutron::listen_h3`](crate::app::Neutron::listen_h3).
//!
//! # Example
//!
//! ```rust,ignore
//! use neutron::app::Neutron;
//! use neutron::tls::TlsConfig;
//!
//! let tls = TlsConfig::from_pem("cert.pem", "key.pem").unwrap();
//!
//! Neutron::new()
//!     .router(router)
//!     .listen_h3("0.0.0.0:4433".parse().unwrap(), tls)
//!     .await
//!     .unwrap();
//! ```
//!
//! # Browser support
//!
//! To advertise HTTP/3 support add an `Alt-Svc` header on your HTTP/1+2
//! responses:
//!
//! ```text
//! Alt-Svc: h3=":4433"; ma=2592000
//! ```
//!
//! # Performance notes
//!
//! - Each QUIC connection is handled in a spawned task.
//! - Multiple request streams within one connection are handled concurrently.
//! - Body bytes are buffered from the QUIC stream before dispatch (same as
//!   the TCP path; a streaming extractor can be added later).

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Buf;
use bytes::Bytes;
use futures_util::StreamExt;
use h3::server::RequestStream;
use h3_quinn::quinn;
use http::StatusCode;
use http_body_util::BodyExt;

use crate::app::DispatchChain;
use crate::handler::{Request as NeutronRequest, StateMap};
use crate::tls::TlsConfig;

// ---------------------------------------------------------------------------
// Http3Config
// ---------------------------------------------------------------------------

/// Configuration for the HTTP/3 server.
#[derive(Debug, Clone)]
pub struct Http3Config {
    /// Maximum body size in bytes accepted from a single request.
    /// Default: 2 MiB.
    pub max_body_size: usize,
}

impl Default for Http3Config {
    fn default() -> Self {
        Self {
            max_body_size: 2 * 1024 * 1024,
        }
    }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// Start an HTTP/3 server on `addr`.
///
/// Binds a QUIC UDP endpoint and accepts connections indefinitely until the
/// returned future is dropped (or until `endpoint.close()` is called).
///
/// TLS 1.3 with ALPN `"h3"` is configured automatically from `tls_config`.
pub async fn serve_h3(
    addr: SocketAddr,
    dispatch: DispatchChain,
    state_map: Arc<StateMap>,
    tls_config: TlsConfig,
    config: Http3Config,
) -> Result<(), std::io::Error> {
    serve_h3_with_shutdown(
        addr,
        dispatch,
        state_map,
        tls_config,
        config,
        Box::pin(std::future::pending()),
        std::time::Duration::from_secs(30),
        None,
    )
    .await
}

pub(crate) async fn serve_h3_with_shutdown(
    addr: SocketAddr,
    dispatch: DispatchChain,
    state_map: Arc<StateMap>,
    tls_config: TlsConfig,
    config: Http3Config,
    mut shutdown: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
    deadline: std::time::Duration,
    max_connections: Option<usize>,
) -> Result<(), std::io::Error> {
    // Clone the rustls ServerConfig and replace ALPN protocols with "h3".
    let mut rustls_cfg = (*tls_config.server_config).clone();
    rustls_cfg.alpn_protocols = vec![b"h3".to_vec()];

    let quic_tls: quinn::crypto::rustls::QuicServerConfig =
        rustls_cfg
            .try_into()
            .map_err(|e: quinn::crypto::rustls::NoInitialCipherSuite| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, e.to_string())
            })?;

    let quic_server_config = quinn::ServerConfig::with_crypto(Arc::new(quic_tls));
    let endpoint = quinn::Endpoint::server(quic_server_config, addr)?;

    tracing::info!("HTTP/3 (QUIC) listening on {addr}");

    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let mut tasks = tokio::task::JoinSet::new();
    let semaphore = max_connections.map(|limit| Arc::new(tokio::sync::Semaphore::new(limit)));
    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => break,
            _ = tasks.join_next(), if !tasks.is_empty() => {}
            incoming = endpoint.accept() => {
                let Some(incoming) = incoming else { break; };
                let permit = if let Some(ref semaphore) = semaphore {
                    match semaphore.clone().try_acquire_owned() { Ok(permit) => Some(permit), Err(_) => { incoming.refuse(); continue; } }
                } else { None };
                let dispatch = dispatch.clone();
                let state_map = state_map.clone();
                let config = config.clone();
                let stop = stop_rx.clone();
                tasks.spawn(async move { let _permit = permit; handle_connection(incoming, dispatch, state_map, config, stop).await; });
            }
        }
    }
    let end = tokio::time::Instant::now() + deadline;
    let _ = stop_tx.send(true);
    if tokio::time::timeout_at(end, async { while tasks.join_next().await.is_some() {} })
        .await
        .is_err()
    {
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
    }
    endpoint.close(0u32.into(), b"server shutdown");
    let _ = tokio::time::timeout_at(end, endpoint.wait_idle()).await;

    Ok(())
}

// ---------------------------------------------------------------------------
// Connection handler
// ---------------------------------------------------------------------------

async fn handle_connection(
    incoming: quinn::Incoming,
    dispatch: DispatchChain,
    state_map: Arc<StateMap>,
    config: Http3Config,
    mut stop: tokio::sync::watch::Receiver<bool>,
) {
    let conn = match tokio::select! { _ = stop.changed() => return, result = incoming => result } {
        Ok(c) => c,
        Err(e) => {
            tracing::debug!("QUIC connection failed: {e}");
            return;
        }
    };

    let remote = conn.remote_address();
    tracing::debug!(%remote, "HTTP/3 connection established");

    let h3_conn = h3_quinn::Connection::new(conn);
    let mut h3: h3::server::Connection<_, Bytes> = match h3::server::builder().build(h3_conn).await
    {
        Ok(c) => c,
        Err(e) => {
            tracing::debug!(%remote, "HTTP/3 handshake failed: {e}");
            return;
        }
    };

    let mut requests = futures_util::stream::FuturesUnordered::<
        std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
    >::new();
    loop {
        let accepted = tokio::select! {
            biased;
            _ = stop.changed() => { let _ = h3.shutdown(0).await; break; }
            _ = requests.next(), if !requests.is_empty() => continue,
            result = h3.accept() => result,
        };
        match accepted {
            Ok(Some(resolver)) => {
                let dispatch = Arc::clone(&dispatch);
                let state_map = Arc::clone(&state_map);
                let cfg = config.clone();
                requests.push(Box::pin(async move {
                    match resolver.resolve_request().await {
                        Ok((req, stream)) => {
                            handle_request(req, stream, dispatch, state_map, cfg, remote).await;
                        }
                        Err(e) => {
                            tracing::debug!(%remote, "HTTP/3 resolve_request error: {e}");
                        }
                    }
                }));
            }
            Ok(None) => break, // Connection closed cleanly.
            Err(e) => {
                tracing::debug!(%remote, "HTTP/3 stream accept error: {e}");
                break;
            }
        }
    }
    while requests.next().await.is_some() {}
}

// ---------------------------------------------------------------------------
// Request handler
// ---------------------------------------------------------------------------

async fn handle_request(
    req: http::Request<()>,
    mut stream: RequestStream<h3_quinn::BidiStream<Bytes>, Bytes>,
    dispatch: DispatchChain,
    state_map: Arc<StateMap>,
    config: Http3Config,
    remote: SocketAddr,
) {
    // Collect the request body from the QUIC stream.
    let mut body_bytes: Vec<u8> = Vec::new();
    loop {
        match stream.recv_data().await {
            Ok(Some(mut data)) => {
                while data.has_remaining() {
                    let chunk = data.chunk().to_vec();
                    let len = chunk.len();
                    if len > config.max_body_size.saturating_sub(body_bytes.len()) {
                        tracing::warn!(%remote, "HTTP/3 request body exceeds limit");
                        let _ = send_error(&mut stream, StatusCode::PAYLOAD_TOO_LARGE).await;
                        return;
                    }
                    body_bytes.extend_from_slice(&chunk);
                    data.advance(len);
                }
            }
            Ok(None) => break, // End of body.
            Err(e) => {
                tracing::debug!(%remote, "HTTP/3 recv_data error: {e}");
                return;
            }
        }
    }

    // Convert to a NeutronRequest.
    //
    // P1.2 (deferred true h3 streaming, see P1.9): the h3 recv loop above already
    // accumulates the body with its own per-chunk 413 ceiling. To reach the new
    // streaming dispatch surface without rewriting h3's chunk API, wrap the
    // collected bytes as a single-frame ReqBody and mount it via
    // with_streaming_state. The handler still streams the type; true frame-level
    // h3 backpressure lands with P1.9.
    let (parts, _) = req.into_parts();
    let boxed = crate::handler::full_frame(Bytes::from(body_bytes));
    let mut neutron_req = NeutronRequest::with_streaming_state(
        parts.method,
        parts.uri,
        parts.headers,
        boxed,
        state_map,
    );
    neutron_req.set_remote_addr(remote);

    // Dispatch through the middleware + router chain.
    let response = dispatch(neutron_req).await;

    // Send frames incrementally: an SSE/stream response must not be collected
    // before headers or retained in an unbounded whole-response buffer.
    let (resp_parts, mut resp_body) = response.into_parts();
    let h3_resp = http::Response::from_parts(resp_parts, ());
    if let Err(e) = stream.send_response(h3_resp).await {
        tracing::debug!(%remote, "HTTP/3 send_response error: {e}");
        return;
    }
    while let Some(frame) = resp_body.frame().await {
        let frame = match frame {
            Ok(frame) => frame,
            Err(never) => match never {},
        };
        if let Ok(data) = frame.into_data() {
            if let Err(e) = stream.send_data(data).await {
                tracing::debug!(%remote, "HTTP/3 send_data error: {e}");
                return;
            }
        }
    }

    let _ = stream.finish().await;
}

async fn send_error(
    stream: &mut RequestStream<h3_quinn::BidiStream<Bytes>, Bytes>,
    status: StatusCode,
) -> Result<(), h3::error::StreamError> {
    let resp = http::Response::builder()
        .status(status)
        .header("content-type", "text/plain")
        .body(())
        .unwrap();
    stream.send_response(resp).await?;
    stream
        .send_data(Bytes::from(status.canonical_reason().unwrap_or("Error")))
        .await?;
    stream.finish().await
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http3_config_defaults() {
        let cfg = Http3Config::default();
        assert_eq!(cfg.max_body_size, 2 * 1024 * 1024);
    }

    #[test]
    fn http3_config_clone() {
        let cfg = Http3Config {
            max_body_size: 1024,
        };
        let cfg2 = cfg.clone();
        assert_eq!(cfg.max_body_size, cfg2.max_body_size);
    }
}
