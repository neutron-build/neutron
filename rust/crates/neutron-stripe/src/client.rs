//! HTTP client for the Stripe REST API.

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::client::conn::http1;
use hyper::Request;
use hyper_util::rt::TokioIo;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::net::TcpStream;

use crate::config::StripeConfig;
use crate::error::StripeError;
use crate::event::PaymentIntent;

/// Parameters for creating a PaymentIntent.
#[derive(Debug, Clone, Default, Serialize)]
pub struct CreatePaymentIntent {
    /// Amount in smallest currency unit (cents for USD).
    pub amount: i64,
    /// ISO currency code, lowercase (e.g. `"usd"`).
    pub currency: String,
    /// Optional customer ID.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub customer: Option<String>,
    /// Human-readable description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Whether to automatically confirm the intent on creation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub confirm: Option<bool>,
}

/// Parameters for creating a Stripe Customer.
#[derive(Debug, Clone, Default, Serialize)]
pub struct CreateCustomer {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// A Stripe Customer object.
#[derive(Debug, Clone, Deserialize)]
pub struct Customer {
    pub id: String,
    pub object: String,
    pub email: Option<String>,
    pub name: Option<String>,
    pub description: Option<String>,
    pub created: u64,
}

/// Thin async HTTP client for the Stripe API.
///
/// Uses the existing workspace hyper stack — no reqwest/ureq dependency.
#[derive(Clone)]
pub struct StripeClient {
    config: std::sync::Arc<StripeConfig>,
}

impl StripeClient {
    pub fn new(config: StripeConfig) -> Self {
        StripeClient {
            config: std::sync::Arc::new(config),
        }
    }

    /// `POST /v1/payment_intents`
    pub async fn create_payment_intent(
        &self,
        params: CreatePaymentIntent,
    ) -> Result<PaymentIntent, StripeError> {
        let body = serde_urlencoded(params)?;
        let resp = self.post("/v1/payment_intents", &body).await?;
        serde_json::from_value(resp).map_err(|e| StripeError::ParseError(e.to_string()))
    }

    /// `GET /v1/payment_intents/{id}`
    pub async fn retrieve_payment_intent(&self, id: &str) -> Result<PaymentIntent, StripeError> {
        let resp = self.get(&format!("/v1/payment_intents/{id}")).await?;
        serde_json::from_value(resp).map_err(|e| StripeError::ParseError(e.to_string()))
    }

    /// `POST /v1/customers`
    pub async fn create_customer(&self, params: CreateCustomer) -> Result<Customer, StripeError> {
        let body = serde_urlencoded_customer(params)?;
        let resp = self.post("/v1/customers", &body).await?;
        serde_json::from_value(resp).map_err(|e| StripeError::ParseError(e.to_string()))
    }

    /// `GET /v1/customers/{id}`
    pub async fn retrieve_customer(&self, id: &str) -> Result<Customer, StripeError> {
        let resp = self.get(&format!("/v1/customers/{id}")).await?;
        serde_json::from_value(resp).map_err(|e| StripeError::ParseError(e.to_string()))
    }

    /// `DELETE /v1/customers/{id}`
    pub async fn delete_customer(&self, id: &str) -> Result<bool, StripeError> {
        let resp = self.delete(&format!("/v1/customers/{id}")).await?;
        Ok(resp
            .get("deleted")
            .and_then(|v| v.as_bool())
            .unwrap_or(false))
    }

    // -----------------------------------------------------------------------
    // HTTP helpers
    // -----------------------------------------------------------------------

    async fn post(&self, path: &str, body: &str) -> Result<Value, StripeError> {
        let url = format!("{}{}", self.config.api_base, path);
        let req = Request::builder()
            .method("POST")
            .uri(url.as_str())
            .header(
                "authorization",
                format!("Bearer {}", self.config.secret_key),
            )
            .header("content-type", "application/x-www-form-urlencoded")
            .header("stripe-version", "2023-10-16")
            .body(Full::<Bytes>::from(body.to_owned()))
            .map_err(|e| StripeError::ApiError(e.to_string()))?;

        self.execute(req).await
    }

    async fn get(&self, path: &str) -> Result<Value, StripeError> {
        let url = format!("{}{}", self.config.api_base, path);
        let req = Request::builder()
            .method("GET")
            .uri(url.as_str())
            .header(
                "authorization",
                format!("Bearer {}", self.config.secret_key),
            )
            .header("stripe-version", "2023-10-16")
            .body(Full::<Bytes>::from(""))
            .map_err(|e| StripeError::ApiError(e.to_string()))?;

        self.execute(req).await
    }

    async fn delete(&self, path: &str) -> Result<Value, StripeError> {
        let url = format!("{}{}", self.config.api_base, path);
        let req = Request::builder()
            .method("DELETE")
            .uri(url.as_str())
            .header(
                "authorization",
                format!("Bearer {}", self.config.secret_key),
            )
            .header("stripe-version", "2023-10-16")
            .body(Full::<Bytes>::from(""))
            .map_err(|e| StripeError::ApiError(e.to_string()))?;

        self.execute(req).await
    }

    async fn execute(&self, req: Request<Full<Bytes>>) -> Result<Value, StripeError> {
        let end = tokio::time::Instant::now() + self.config.operation_timeout;
        tokio::time::timeout_at(end, self.execute_bounded(req, end)).await
            .map_err(|_| StripeError::ApiError("operation deadline exceeded; mutation outcome may be unknown; reconcile before retry".into()))?
    }

    async fn execute_bounded(
        &self,
        req: Request<Full<Bytes>>,
        end: tokio::time::Instant,
    ) -> Result<Value, StripeError> {
        let host = req
            .uri()
            .host()
            .ok_or_else(|| StripeError::ApiError("missing host in URL".into()))?
            .to_string();
        let scheme = req.uri().scheme_str().unwrap_or("https");
        let port = req
            .uri()
            .port_u16()
            .unwrap_or(if scheme == "https" { 443 } else { 80 });
        let addr = format!("{host}:{port}");

        // Scheme-aware transport (RS-23): the default base URL is https, but
        // the connector used to open a RAW TcpStream regardless of scheme,
        // sending the `Authorization: Bearer sk_live_…` header in plaintext
        // on port 80 (and failing against real Stripe anyway). https now
        // goes through a verified rustls TLS connection (webpki roots,
        // hostname-checked); plaintext http is refused unless explicitly
        // enabled for local test doubles.
        let io: TokioIo<Box<dyn MaybeTls>> = match scheme {
            "https" => {
                let tcp = TcpStream::connect(&addr)
                    .await
                    .map_err(|e| StripeError::ApiError(e.to_string()))?;
                let connector = tls_connector()?;
                let server_name = rustls::pki_types::ServerName::try_from(host.clone())
                    .map_err(|e| StripeError::Config(format!("invalid TLS server name: {e}")))?
                    .to_owned();
                let tls = connector
                    .connect(server_name, tcp)
                    .await
                    .map_err(|e| StripeError::ApiError(format!("TLS handshake: {e}")))?;
                TokioIo::new(Box::new(tls) as Box<dyn MaybeTls>)
            }
            "http" => {
                if !self.config.allow_insecure_http {
                    // Refuse BEFORE any connection attempt: opening the
                    // socket already leaks that a Stripe client exists.
                    return Err(StripeError::Config(
                        "refusing to send API credentials over plaintext http (set \
                         allow_insecure_http only for local test doubles)"
                            .into(),
                    ));
                }
                let tcp = TcpStream::connect(&addr)
                    .await
                    .map_err(|e| StripeError::ApiError(e.to_string()))?;
                TokioIo::new(Box::new(tcp) as Box<dyn MaybeTls>)
            }
            other => {
                return Err(StripeError::Config(format!(
                    "unsupported URL scheme: {other}"
                )))
            }
        };

        let (mut sender, conn) = http1::handshake::<_, Full<Bytes>>(io)
            .await
            .map_err(|e| StripeError::ApiError(e.to_string()))?;

        let operation = async {
            let resp: hyper::Response<Incoming> = sender
                .send_request(req)
                .await
                .map_err(|e| StripeError::ApiError(e.to_string()))?;

            let status = resp.status().as_u16();
            let mut incoming = resp.into_body();
            let mut body = Vec::new();
            while let Some(frame) = incoming.frame().await {
                let frame = frame.map_err(|e| StripeError::ApiError(e.to_string()))?;
                if let Ok(data) = frame.into_data() {
                    if data.len() > self.config.max_response_bytes.saturating_sub(body.len()) {
                        return Err(StripeError::ApiError(
                            "response exceeds byte limit; reconcile payment outcome".into(),
                        ));
                    }
                    body.extend_from_slice(&data);
                }
            }

            let value: Value = serde_json::from_slice(&body)
                .map_err(|e| StripeError::ParseError(e.to_string()))?;

            if tokio::time::Instant::now() > end {
                return Err(StripeError::ApiError(
                    "response parsing exceeded operation deadline; reconcile mutation outcome"
                        .into(),
                ));
            }
            if !(200..300).contains(&status) {
                let msg = value
                    .get("error")
                    .and_then(|e| e.get("message"))
                    .and_then(|m| m.as_str())
                    .unwrap_or("unknown error")
                    .to_string();
                return Err(StripeError::StripeApiError {
                    status,
                    message: msg,
                });
            }

            Ok(value)
        };
        tokio::pin!(conn);
        tokio::pin!(operation);
        tokio::select! {
            biased;
            result = &mut operation => result,
            result = &mut conn => {
                result.map_err(|e| StripeError::ApiError(e.to_string()))?;
                operation.await
            }
        }
    }
}

// -----------------------------------------------------------------------
// Minimal form-encoding helpers (no serde_urlencoded dep needed for simple cases)
// -----------------------------------------------------------------------

fn serde_urlencoded(p: CreatePaymentIntent) -> Result<String, StripeError> {
    let mut parts = vec![
        format!("amount={}", p.amount),
        format!("currency={}", url_encode(&p.currency)),
    ];
    if let Some(c) = p.customer {
        parts.push(format!("customer={}", url_encode(&c)));
    }
    if let Some(d) = p.description {
        parts.push(format!("description={}", url_encode(&d)));
    }
    if let Some(c) = p.confirm {
        parts.push(format!("confirm={c}"));
    }
    Ok(parts.join("&"))
}

fn serde_urlencoded_customer(p: CreateCustomer) -> Result<String, StripeError> {
    let mut parts: Vec<String> = Vec::new();
    if let Some(e) = p.email {
        parts.push(format!("email={}", url_encode(&e)));
    }
    if let Some(n) = p.name {
        parts.push(format!("name={}", url_encode(&n)));
    }
    if let Some(d) = p.description {
        parts.push(format!("description={}", url_encode(&d)));
    }
    Ok(parts.join("&"))
}

fn url_encode(s: &str) -> String {
    s.bytes()
        .flat_map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                vec![b as char]
            }
            b => vec!['%', nibble(b >> 4), nibble(b & 0xf)],
        })
        .collect()
}

fn nibble(n: u8) -> char {
    if n < 10 {
        (b'0' + n) as char
    } else {
        (b'a' + n - 10) as char
    }
}

/// Type-erased IO stream for scheme-aware transports.
trait MaybeTls: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin {}
impl<T> MaybeTls for T where T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin {}

/// Test hook: additional trust anchors (test CA certificates). Never set
/// outside `#[cfg(test)]` builds.
#[cfg(test)]
static TEST_ROOTS: std::sync::OnceLock<Vec<rustls::pki_types::CertificateDer<'static>>> =
    std::sync::OnceLock::new();

/// Shared verified-TLS connector (webpki root store, no client auth).
fn tls_connector() -> Result<tokio_rustls::TlsConnector, StripeError> {
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    #[cfg(test)]
    if let Some(extra) = TEST_ROOTS.get() {
        for cert in extra {
            let _ = roots.add(cert.clone());
        }
    }
    let cfg = rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .expect("supported TLS versions")
    .with_root_certificates(roots)
    .with_no_client_auth();
    Ok(tokio_rustls::TlsConnector::from(std::sync::Arc::new(cfg)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_pi_form_encodes_amount_currency() {
        let body = serde_urlencoded(CreatePaymentIntent {
            amount: 2000,
            currency: "usd".to_string(),
            ..Default::default()
        })
        .unwrap();
        assert!(body.contains("amount=2000"));
        assert!(body.contains("currency=usd"));
    }

    #[test]
    fn create_pi_optional_fields() {
        let body = serde_urlencoded(CreatePaymentIntent {
            amount: 500,
            currency: "eur".to_string(),
            customer: Some("cus_123".to_string()),
            description: Some("Test order".to_string()),
            confirm: Some(true),
        })
        .unwrap();
        assert!(body.contains("customer=cus_123"));
        assert!(body.contains("description=Test"));
        assert!(body.contains("confirm=true"));
    }

    #[test]
    fn url_encode_passthrough_simple() {
        assert_eq!(url_encode("hello"), "hello");
    }

    #[test]
    fn url_encode_spaces_and_special() {
        let encoded = url_encode("hello world");
        assert!(encoded.contains("%20"));
    }

    #[test]
    fn url_encode_at_sign() {
        let encoded = url_encode("user@example.com");
        assert!(encoded.contains("%40"));
    }

    #[test]
    fn stripe_client_constructs() {
        let cfg = StripeConfig::new("whsec_abc", "sk_test_xyz");
        let _c = StripeClient::new(cfg);
    }

    #[test]
    fn create_customer_form_encodes() {
        let body = serde_urlencoded_customer(CreateCustomer {
            email: Some("a@b.com".to_string()),
            name: Some("Alice".to_string()),
            description: None,
        })
        .unwrap();
        assert!(body.contains("email=a%40b.com"));
        assert!(body.contains("name=Alice"));
    }

    // ------------------------------------------------------------------
    // RS-23 regressions: scheme-aware, credential-safe transport.
    // ------------------------------------------------------------------

    /// rustls needs a process-level CryptoProvider; tests pin ring.
    fn install_test_provider() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn http_base_is_refused_without_explicit_opt_in() {
        install_test_provider();
        use std::sync::atomic::{AtomicBool, Ordering};

        // A local plaintext listener: nothing should EVER reach it.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let hit = std::sync::Arc::new(AtomicBool::new(false));
        let hit2 = std::sync::Arc::clone(&hit);
        tokio::spawn(async move {
            if listener.accept().await.is_ok() {
                hit2.store(true, Ordering::SeqCst);
            }
        });

        let config =
            StripeConfig::new("whsec_x", "sk_test_x").api_base(format!("http://127.0.0.1:{port}"));
        let client = StripeClient::new(config);
        let err = client.retrieve_customer("cus_test").await.unwrap_err();
        assert!(
            err.to_string().contains("plaintext"),
            "expected plaintext refusal, got: {err}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            !hit.load(Ordering::SeqCst),
            "no connection may be attempted against a plaintext base"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn https_fails_closed_against_an_untrusted_certificate() {
        install_test_provider();
        use std::sync::atomic::{AtomicBool, Ordering};

        // Local TLS server with a self-signed cert the client does NOT trust.
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let tls_cfg = rustls::ServerConfig::builder_with_provider(std::sync::Arc::new(
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
        let acceptor = tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(tls_cfg));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let saw_request = std::sync::Arc::new(AtomicBool::new(false));
        let saw2 = std::sync::Arc::clone(&saw_request);
        let acc = acceptor.clone();
        tokio::spawn(async move {
            if let Ok((sock, _)) = listener.accept().await {
                // Even if the handshake somehow succeeded, nothing readable.
                let _ = acc.accept(sock).await;
                saw2.store(true, Ordering::SeqCst);
            }
        });

        let config =
            StripeConfig::new("whsec_x", "sk_test_x").api_base(format!("https://localhost:{port}"));
        let client = StripeClient::new(config);
        let err = client.retrieve_customer("cus_test").await.unwrap_err();
        assert!(
            err.to_string().contains("TLS"),
            "expected TLS handshake failure, got: {err}"
        );
        assert!(
            !saw_request.load(Ordering::SeqCst),
            "no HTTP request may be sent after a failed handshake"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn https_trusted_certificate_succeeds() {
        install_test_provider();
        use std::sync::atomic::{AtomicBool, Ordering};

        // Self-signed server cert, ADDED to the client's test trust anchors.
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let cert_der = rustls::pki_types::CertificateDer::from(cert.cert.der().to_vec());
        let _ = TEST_ROOTS.set(vec![cert_der.clone()]);

        let tls_cfg = rustls::ServerConfig::builder_with_provider(std::sync::Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .expect("supported TLS versions")
        .with_no_client_auth()
        .with_single_cert(
            vec![cert_der],
            rustls::pki_types::PrivateKeyDer::try_from(cert.key_pair.serialize_der()).unwrap(),
        )
        .unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(tls_cfg));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let got_auth_header = std::sync::Arc::new(AtomicBool::new(false));
        let hdr2 = std::sync::Arc::clone(&got_auth_header);
        let acc = acceptor.clone();
        tokio::spawn(async move {
            use http_body_util::BodyExt;
            use hyper::service::service_fn;
            use hyper_util::rt::TokioIo;
            if let Ok((sock, _)) = listener.accept().await {
                if let Ok(tls) = acc.accept(sock).await {
                    let svc = service_fn(|req: hyper::Request<hyper::body::Incoming>| {
                        let h = std::sync::Arc::clone(&hdr2);
                        async move {
                            if req.headers().contains_key("authorization") {
                                h.store(true, Ordering::SeqCst);
                            }
                            let _ = req.into_body().collect().await;
                            Ok::<_, std::convert::Infallible>(
                                http::Response::new(http_body_util::Full::new(
                    bytes::Bytes::from_static(b"{\"id\":\"cus_test\",\"object\":\"customer\",\"created\":1700000000}")
                                )),
                            )
                        }
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(tls), svc)
                        .await;
                }
            }
        });

        let config =
            StripeConfig::new("whsec_x", "sk_test_x").api_base(format!("https://localhost:{port}"));
        let client = StripeClient::new(config);
        let value = client
            .retrieve_customer("cus_test")
            .await
            .expect("TLS request");
        assert_eq!(value.id, "cus_test");
        assert!(
            got_auth_header.load(Ordering::SeqCst),
            "credentials travelled over the TLS connection"
        );
    }

    #[tokio::test]
    async fn stalled_tls_headers_body_and_oversize_are_bounded_and_drop_owned_io() {
        use std::time::Duration;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        for stage in ["tls", "headers", "body", "oversize"] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let peer = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                if stage != "tls" {
                    let mut request = Vec::new();
                    while !request.ends_with(b"\r\n\r\n") {
                        request.push(stream.read_u8().await.unwrap());
                    }
                    if stage == "body" {
                        stream
                            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n{")
                            .await
                            .unwrap();
                    }
                    if stage == "oversize" {
                        stream
                            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 256\r\n\r\n")
                            .await
                            .unwrap();
                        stream.write_all(&[b'x'; 256]).await.unwrap();
                    }
                }
                let mut bytes = [0u8; 1024];
                loop {
                    match stream.read(&mut bytes).await {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {}
                    }
                }
            });
            let scheme = if stage == "tls" { "https" } else { "http" };
            let url = format!("{scheme}://{addr}/v1/test");
            let config = StripeConfig::new("whsec_test", "sk_test_fixture")
                .api_base(&url)
                .allow_insecure_http(stage != "tls")
                .operation_limits(Duration::from_millis(80), 64);
            let client = StripeClient::new(config);
            let request = Request::builder()
                .uri(url)
                .body(Full::new(Bytes::new()))
                .unwrap();
            let result = tokio::time::timeout(Duration::from_secs(1), client.execute(request))
                .await
                .unwrap();
            assert!(result.is_err(), "{stage} unexpectedly succeeded");
            tokio::time::timeout(Duration::from_secs(1), peer)
                .await
                .expect("client IO survived operation completion")
                .unwrap();
        }
    }
}
