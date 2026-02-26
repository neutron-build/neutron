//! Internal HTTPS client for S3-compatible storage requests.
//!
//! Mirrors neutron-oauth's client but supports arbitrary methods + byte bodies.

use std::sync::Arc;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::client::conn::http1;
use hyper::Request;
use hyper_util::rt::TokioIo;
use rustls::ClientConfig;
use rustls::pki_types::ServerName;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

use crate::error::StorageError;

// ---------------------------------------------------------------------------
// TLS root store (lazily initialised)
// ---------------------------------------------------------------------------

fn tls_connector() -> Result<TlsConnector, StorageError> {
    let mut root_store = rustls::RootCertStore::empty();
    root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

    let config = ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();

    Ok(TlsConnector::from(Arc::new(config)))
}

// ---------------------------------------------------------------------------
// Public request function
// ---------------------------------------------------------------------------

/// Execute an HTTPS request and return `(status_code, response_body_bytes)`.
pub async fn https_request(
    method:  &str,
    host:    &str,
    port:    u16,
    path:    &str,
    headers: &[(&str, &str)],
    body:    Vec<u8>,
    use_tls: bool,
) -> Result<(u16, Vec<u8>), StorageError> {
    if use_tls {
        https_inner(method, host, port, path, headers, body).await
    } else {
        http_inner(method, host, port, path, headers, body).await
    }
}

async fn https_inner(
    method:  &str,
    host:    &str,
    port:    u16,
    path:    &str,
    headers: &[(&str, &str)],
    body:    Vec<u8>,
) -> Result<(u16, Vec<u8>), StorageError> {
    let connector = tls_connector()?;
    let addr      = format!("{host}:{port}");

    let tcp  = TcpStream::connect(&addr).await
        .map_err(|e| StorageError::Connect(e.to_string()))?;
    let host_only = host.split(':').next().unwrap_or(host);
    let server_name: ServerName<'static> = ServerName::try_from(host_only.to_string())
        .map_err(|e| StorageError::Connect(e.to_string()))?;
    let tls = connector.connect(server_name, tcp).await
        .map_err(|e| StorageError::Connect(e.to_string()))?;

    let io  = TokioIo::new(tls);
    let (mut sender, conn) = http1::Builder::new()
        .handshake::<_, Full<Bytes>>(io)
        .await
        .map_err(|e| StorageError::Connect(e.to_string()))?;

    tokio::spawn(async move { let _ = conn.await; });

    send_request(&mut sender, method, host, path, headers, body).await
}

async fn http_inner(
    method:  &str,
    host:    &str,
    port:    u16,
    path:    &str,
    headers: &[(&str, &str)],
    body:    Vec<u8>,
) -> Result<(u16, Vec<u8>), StorageError> {
    let addr = format!("{host}:{port}");
    let tcp  = TcpStream::connect(&addr).await
        .map_err(|e| StorageError::Connect(e.to_string()))?;
    let io   = TokioIo::new(tcp);
    let (mut sender, conn) = http1::Builder::new()
        .handshake::<_, Full<Bytes>>(io)
        .await
        .map_err(|e| StorageError::Connect(e.to_string()))?;

    tokio::spawn(async move { let _ = conn.await; });

    send_request(&mut sender, method, host, path, headers, body).await
}

type H1Sender = http1::SendRequest<Full<Bytes>>;

async fn send_request(
    sender:  &mut H1Sender,
    method:  &str,
    host:    &str,
    path:    &str,
    headers: &[(&str, &str)],
    body:    Vec<u8>,
) -> Result<(u16, Vec<u8>), StorageError> {
    let body_len = body.len();
    let mut req  = Request::builder()
        .method(method)
        .uri(path)
        .header("host", host)
        .header("content-length", body_len.to_string());

    for (k, v) in headers {
        req = req.header(*k, *v);
    }

    let request = req
        .body(Full::new(Bytes::from(body)))
        .map_err(|e| StorageError::Sign(e.to_string()))?;

    let resp: hyper::Response<Incoming> = sender.send_request(request).await
        .map_err(|e| StorageError::Io(e.to_string()))?;

    let status = resp.status().as_u16();
    let bytes  = resp.into_body()
        .collect()
        .await
        .map_err(|e| StorageError::Io(e.to_string()))?
        .to_bytes();

    Ok((status, bytes.to_vec()))
}
