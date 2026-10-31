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
    execute_https(url, "POST", None, body).await
}

/// Fetch genuine userinfo, refusing non-success status before decoding identity.
pub(crate) async fn https_get(url: &str, bearer: &str) -> Result<String, OAuthError> {
    execute_https(url, "GET", Some(bearer), String::new()).await
}

async fn execute_https(
    url: &str,
    method: &str,
    bearer: Option<&str>,
    body: String,
) -> Result<String, OAuthError> {
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        let (host, port, path) = parse_url(url)?;
        let connector = make_connector()?;
        let stream = TcpStream::connect((host.as_str(), port)).await.map_err(|e| OAuthError::Connect(e.to_string()))?;
        let name = ServerName::try_from(host.clone()).map_err(|e| OAuthError::BadUrl(e.to_string()))?.to_owned();
        let tls = connector.connect(name, stream).await.map_err(|e| OAuthError::Connect(e.to_string()))?;
        let (mut sender, connection) = http1::Builder::new().handshake(TokioIo::new(tls)).await.map_err(|e| OAuthError::Http(e.to_string()))?;
        let mut request = http::Request::builder().method(method).uri(path).header("host", host)
            .header("user-agent", "neutron-oauth").header("accept", "application/json");
        if let Some(bearer) = bearer { request = request.header("authorization", format!("Bearer {bearer}")); }
        if method == "POST" { request = request.header("content-type", "application/x-www-form-urlencoded"); }
        let request = request.body(Full::new(Bytes::from(body))).map_err(|e| OAuthError::Http(e.to_string()))?;
        let operation = async {
            let response = sender.send_request(request).await.map_err(|e| OAuthError::Http(e.to_string()))?;
            require_success(response.status())?;
            let mut incoming = response.into_body(); let mut bytes = Vec::new();
            while let Some(frame) = incoming.frame().await {
                let frame = frame.map_err(|e| OAuthError::Http(e.to_string()))?;
                if let Ok(data) = frame.into_data() {
                    if data.len() > (2usize * 1024 * 1024).saturating_sub(bytes.len()) { return Err(OAuthError::Http("provider body exceeds 2 MiB".into())); }
                    bytes.extend_from_slice(&data);
                }
            }
            String::from_utf8(bytes).map_err(|e| OAuthError::Http(e.to_string()))
        };
        tokio::pin!(connection); tokio::pin!(operation);
        tokio::select! { biased;
            result = &mut operation => result,
            result = &mut connection => { result.map_err(|e| OAuthError::Http(e.to_string()))?; operation.await }
        }
    }).await.map_err(|_| OAuthError::Http("provider operation deadline exceeded; token exchange outcome may be unknown".into()))?
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn require_success(status: http::StatusCode) -> Result<(), OAuthError> {
    if status.is_success() {
        Ok(())
    } else {
        Err(OAuthError::Http(format!("provider HTTP status {status}")))
    }
}

fn make_connector() -> Result<TlsConnector, OAuthError> {
    let mut roots = RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

    let cfg = ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .expect("supported TLS versions")
    .with_root_certificates(roots)
    .with_no_client_auth();

    Ok(TlsConnector::from(Arc::new(cfg)))
}

/// Returns `(host, port, path_and_query)`.
fn parse_url(url: &str) -> Result<(String, u16, String), OAuthError> {
    let uri: http::Uri = url
        .parse()
        .map_err(|_| OAuthError::BadUrl(url.to_string()))?;

    if uri.scheme_str() != Some("https") {
        return Err(OAuthError::BadUrl("OAuth endpoints require https".into()));
    }
    let host = uri
        .host()
        .ok_or_else(|| OAuthError::BadUrl(format!("no host in {url}")))?
        .to_string();

    let port = uri.port_u16().unwrap_or(443);

    let path = uri
        .path_and_query()
        .map(|p| p.as_str().to_string())
        .unwrap_or_else(|| "/".to_string());

    Ok((host, port, path))
}

#[cfg(test)]
mod status_tests {
    use super::*;
    #[test]
    fn identity_looking_error_body_cannot_waive_status_failure() {
        let body = r#"{"id":42,"sub":"subject"}"#;
        assert!(serde_json::from_str::<serde_json::Value>(body).is_ok());
        for code in [301, 400, 401, 403, 500] {
            assert!(require_success(http::StatusCode::from_u16(code).unwrap()).is_err());
        }
    }
}
