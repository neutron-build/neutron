//! Minimal outbound HTTPS client for OAuth token exchange and userinfo requests.
//!
//! Uses hyper (HTTP/1.1) + tokio-rustls + webpki-roots — no reqwest dependency.

use std::sync::Arc;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::client::conn::http1;
use hyper_util::rt::TokioIo;
use rustls::pki_types::ServerName;
use rustls::{ClientConfig, RootCertStore};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

use crate::error::OAuthError;

// ---------------------------------------------------------------------------
// HTTPS helpers
// ---------------------------------------------------------------------------

/// POST `body` (application/x-www-form-urlencoded) to `url` and return the
/// response body as a UTF-8 string.
pub(crate) async fn https_post(url: &str, body: String) -> Result<String, OAuthError> {
    let (host, port, path) = parse_url(url)?;

    let connector = make_connector()?;
    let stream    = TcpStream::connect((host.as_str(), port))
        .await
        .map_err(|e| OAuthError::Connect(e.to_string()))?;

    let server_name = ServerName::try_from(host.clone())
        .map_err(|e| OAuthError::BadUrl(e.to_string()))?
        .to_owned();

    let tls = connector.connect(server_name, stream)
        .await
        .map_err(|e| OAuthError::Connect(e.to_string()))?;

    let io        = TokioIo::new(tls);
    let (mut sender, conn) = http1::Builder::new()
        .handshake(io)
        .await
        .map_err(|e| OAuthError::Http(e.to_string()))?;

    tokio::spawn(async move { let _ = conn.await; });

    let body_len = body.len();
    let req = http::Request::builder()
        .method("POST")
        .uri(&path)
        .header("host",           &host)
        .header("content-type",   "application/x-www-form-urlencoded")
        .header("accept",         "application/json")
        .header("content-length", body_len.to_string())
        .body(Full::new(Bytes::from(body)))
        .map_err(|e| OAuthError::Http(e.to_string()))?;

    let resp = sender.send_request(req)
        .await
        .map_err(|e| OAuthError::Http(e.to_string()))?;

    let bytes = resp.into_body().collect().await
        .map_err(|e| OAuthError::Http(e.to_string()))?
        .to_bytes();

    Ok(String::from_utf8_lossy(&bytes).into_owned())
}

/// GET `url` with `Authorization: Bearer <token>` and return the response body.
pub(crate) async fn https_get(url: &str, bearer: &str) -> Result<String, OAuthError> {
    let (host, port, path) = parse_url(url)?;

    let connector = make_connector()?;
    let stream    = TcpStream::connect((host.as_str(), port))
        .await
        .map_err(|e| OAuthError::Connect(e.to_string()))?;

    let server_name = ServerName::try_from(host.clone())
        .map_err(|e| OAuthError::BadUrl(e.to_string()))?
        .to_owned();

    let tls = connector.connect(server_name, stream)
        .await
        .map_err(|e| OAuthError::Connect(e.to_string()))?;

    let io        = TokioIo::new(tls);
    let (mut sender, conn) = http1::Builder::new()
        .handshake(io)
        .await
        .map_err(|e| OAuthError::Http(e.to_string()))?;

    tokio::spawn(async move { let _ = conn.await; });

    let req = http::Request::builder()
        .method("GET")
        .uri(&path)
        .header("host",          &host)
        .header("authorization", format!("Bearer {bearer}"))
        .header("accept",        "application/json")
        .body(Full::new(Bytes::new()))
        .map_err(|e| OAuthError::Http(e.to_string()))?;

    let resp = sender.send_request(req)
        .await
        .map_err(|e| OAuthError::Http(e.to_string()))?;

    let bytes = resp.into_body().collect().await
        .map_err(|e| OAuthError::Http(e.to_string()))?
        .to_bytes();

    Ok(String::from_utf8_lossy(&bytes).into_owned())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_connector() -> Result<TlsConnector, OAuthError> {
    let mut roots = RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

    let cfg = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();

    Ok(TlsConnector::from(Arc::new(cfg)))
}

/// Returns `(host, port, path_and_query)`.
fn parse_url(url: &str) -> Result<(String, u16, String), OAuthError> {
    let uri: http::Uri = url.parse()
        .map_err(|_| OAuthError::BadUrl(url.to_string()))?;

    let host = uri.host()
        .ok_or_else(|| OAuthError::BadUrl(format!("no host in {url}")))?
        .to_string();

    let port = uri.port_u16().unwrap_or(443);

    let path = uri.path_and_query()
        .map(|p| p.as_str().to_string())
        .unwrap_or_else(|| "/".to_string());

    Ok((host, port, path))
}
