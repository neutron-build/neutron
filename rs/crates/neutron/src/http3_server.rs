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
        Self { max_body_size: 2 * 1024 * 1024 }
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
    addr:       SocketAddr,
    dispatch:   DispatchChain,
    state_map:  Arc<StateMap>,
    tls_config: TlsConfig,
    config:     Http3Config,
) -> Result<(), std::io::Error> {
    // Clone the rustls ServerConfig and replace ALPN protocols with "h3".
    let mut rustls_cfg = (*tls_config.server_config).clone();
    rustls_cfg.alpn_protocols = vec![b"h3".to_vec()];

    let quic_tls: quinn::crypto::rustls::QuicServerConfig = rustls_cfg
        .try_into()
        .map_err(|e: quinn::crypto::rustls::NoInitialCipherSuite| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, e.to_string())
        })?;

    let quic_server_config = quinn::ServerConfig::with_crypto(Arc::new(quic_tls));
    let endpoint = quinn::Endpoint::server(quic_server_config, addr)?;

    tracing::info!("HTTP/3 (QUIC) listening on {addr}");

    while let Some(incoming) = endpoint.accept().await {
        let dispatch   = Arc::clone(&dispatch);
        let state_map  = Arc::clone(&state_map);
        let cfg        = config.clone();
        tokio::spawn(handle_connection(incoming, dispatch, state_map, cfg));
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Connection handler
// ---------------------------------------------------------------------------

async fn handle_connection(
    incoming:  quinn::Incoming,
    dispatch:  DispatchChain,
    state_map: Arc<StateMap>,
    config:    Http3Config,
) {
    let conn = match incoming.await {
        Ok(c) => c,
        Err(e) => {
            tracing::debug!("QUIC connection failed: {e}");
            return;
        }
    };

    let remote = conn.remote_address();
    tracing::debug!(%remote, "HTTP/3 connection established");

    let h3_conn = h3_quinn::Connection::new(conn);
    let mut h3: h3::server::Connection<_, Bytes> =
        match h3::server::builder().build(h3_conn).await {
            Ok(c) => c,
            Err(e) => {
                tracing::debug!(%remote, "HTTP/3 handshake failed: {e}");
                return;
            }
        };

    loop {
        match h3.accept().await {
            Ok(Some(resolver)) => {
                let dispatch   = Arc::clone(&dispatch);
                let state_map  = Arc::clone(&state_map);
                let cfg        = config.clone();
                tokio::spawn(async move {
                    match resolver.resolve_request().await {
                        Ok((req, stream)) => {
                            handle_request(req, stream, dispatch, state_map, cfg, remote).await;
                        }
                        Err(e) => {
                            tracing::debug!(%remote, "HTTP/3 resolve_request error: {e}");
                        }
                    }
                });
            }
            Ok(None) => break,   // Connection closed cleanly.
            Err(e)   => {
                tracing::debug!(%remote, "HTTP/3 stream accept error: {e}");
                break;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Request handler
// ---------------------------------------------------------------------------

async fn handle_request(
    req:       http::Request<()>,
    mut stream: RequestStream<h3_quinn::BidiStream<Bytes>, Bytes>,
    dispatch:  DispatchChain,
    state_map: Arc<StateMap>,
    config:    Http3Config,
    remote:    SocketAddr,
) {
    // Collect the request body from the QUIC stream.
    let mut body_bytes: Vec<u8> = Vec::new();
    loop {
        match stream.recv_data().await {
            Ok(Some(mut data)) => {
                while data.has_remaining() {
                    let chunk = data.chunk().to_vec();
                    let len   = chunk.len();
                    body_bytes.extend_from_slice(&chunk);
                    data.advance(len);

                    if body_bytes.len() > config.max_body_size {
                        tracing::warn!(%remote, "HTTP/3 request body exceeds limit");
                        let _ = send_error(&mut stream, StatusCode::PAYLOAD_TOO_LARGE).await;
                        return;
                    }
                }
            }
            Ok(None) => break,  // End of body.
            Err(e)   => {
                tracing::debug!(%remote, "HTTP/3 recv_data error: {e}");
                return;
            }
        }
    }

    // Convert to a NeutronRequest.
    let (parts, _) = req.into_parts();
    let mut neutron_req = NeutronRequest::with_state(
        parts.method,
        parts.uri,
        parts.headers,
        Bytes::from(body_bytes),
        state_map,
    );
    neutron_req.set_remote_addr(remote);

    // Dispatch through the middleware + router chain.
    let response = dispatch(neutron_req).await;

    // Collect the response body.
    let (resp_parts, resp_body) = response.into_parts();
    let body = resp_body
        .collect()
        .await
        .map(|c| c.to_bytes())
        .unwrap_or_default();

    // Send the response.
    let h3_resp = http::Response::from_parts(resp_parts, ());
    if let Err(e) = stream.send_response(h3_resp).await {
        tracing::debug!(%remote, "HTTP/3 send_response error: {e}");
        return;
    }

    if !body.is_empty() {
        if let Err(e) = stream.send_data(body).await {
            tracing::debug!(%remote, "HTTP/3 send_data error: {e}");
            return;
        }
    }

    let _ = stream.finish().await;
}

async fn send_error(
    stream:  &mut RequestStream<h3_quinn::BidiStream<Bytes>, Bytes>,
    status:  StatusCode,
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
        let cfg  = Http3Config { max_body_size: 1024 };
        let cfg2 = cfg.clone();
        assert_eq!(cfg.max_body_size, cfg2.max_body_size);
    }
}
