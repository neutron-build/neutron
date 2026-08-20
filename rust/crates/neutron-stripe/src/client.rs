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
    pub id:          String,
    pub object:      String,
    pub email:       Option<String>,
    pub name:        Option<String>,
    pub description: Option<String>,
    pub created:     u64,
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
        StripeClient { config: std::sync::Arc::new(config) }
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
    pub async fn create_customer(
        &self,
        params: CreateCustomer,
    ) -> Result<Customer, StripeError> {
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
        Ok(resp.get("deleted").and_then(|v| v.as_bool()).unwrap_or(false))
    }

    // -----------------------------------------------------------------------
    // HTTP helpers
    // -----------------------------------------------------------------------

    async fn post(&self, path: &str, body: &str) -> Result<Value, StripeError> {
        let url = format!("{}{}", self.config.api_base, path);
        let req = Request::builder()
            .method("POST")
            .uri(url.as_str())
            .header("authorization", format!("Bearer {}", self.config.secret_key))
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
            .header("authorization", format!("Bearer {}", self.config.secret_key))
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
            .header("authorization", format!("Bearer {}", self.config.secret_key))
            .header("stripe-version", "2023-10-16")
            .body(Full::<Bytes>::from(""))
            .map_err(|e| StripeError::ApiError(e.to_string()))?;

        self.execute(req).await
    }

    async fn execute(&self, req: Request<Full<Bytes>>) -> Result<Value, StripeError> {
        let host = req.uri().host()
            .ok_or_else(|| StripeError::ApiError("missing host in URL".into()))?
            .to_string();
        let port = req.uri().port_u16().unwrap_or(80);
        let addr = format!("{host}:{port}");

        let stream = TcpStream::connect(&addr).await
            .map_err(|e| StripeError::ApiError(e.to_string()))?;
        let io = TokioIo::new(stream);

        let (mut sender, conn) = http1::handshake::<_, Full<Bytes>>(io).await
            .map_err(|e| StripeError::ApiError(e.to_string()))?;

        tokio::spawn(async move { let _ = conn.await; });

        let resp: hyper::Response<Incoming> = sender.send_request(req).await
            .map_err(|e| StripeError::ApiError(e.to_string()))?;

        let status = resp.status().as_u16();
        let body = resp.into_body().collect().await
            .map_err(|e| StripeError::ApiError(e.to_string()))?
            .to_bytes();

        let value: Value = serde_json::from_slice(&body)
            .map_err(|e| StripeError::ParseError(e.to_string()))?;

        if !(200..300).contains(&status) {
            let msg = value.get("error")
                .and_then(|e| e.get("message"))
                .and_then(|m| m.as_str())
                .unwrap_or("unknown error")
                .to_string();
            return Err(StripeError::StripeApiError { status, message: msg });
        }

        Ok(value)
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
    if let Some(c) = p.customer { parts.push(format!("customer={}", url_encode(&c))); }
    if let Some(d) = p.description { parts.push(format!("description={}", url_encode(&d))); }
    if let Some(c) = p.confirm { parts.push(format!("confirm={c}")); }
    Ok(parts.join("&"))
}

fn serde_urlencoded_customer(p: CreateCustomer) -> Result<String, StripeError> {
    let mut parts: Vec<String> = Vec::new();
    if let Some(e) = p.email { parts.push(format!("email={}", url_encode(&e))); }
    if let Some(n) = p.name { parts.push(format!("name={}", url_encode(&n))); }
    if let Some(d) = p.description { parts.push(format!("description={}", url_encode(&d))); }
    Ok(parts.join("&"))
}

fn url_encode(s: &str) -> String {
    s.bytes().flat_map(|b| match b {
        b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
            vec![b as char]
        }
        b => vec!['%', nibble(b >> 4), nibble(b & 0xf)],
    }).collect()
}

fn nibble(n: u8) -> char {
    if n < 10 { (b'0' + n) as char } else { (b'a' + n - 10) as char }
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
        }).unwrap();
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
        }).unwrap();
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
        }).unwrap();
        assert!(body.contains("email=a%40b.com"));
        assert!(body.contains("name=Alice"));
    }
}
