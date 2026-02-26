//! JWT (JSON Web Token) authentication middleware using HMAC-SHA256.
//!
//! Provides token generation, validation, and an Actix-style middleware that
//! extracts `Claims` from the `Authorization: Bearer <token>` header and stores
//! them as a request extension.
//!
//! # Example
//!
//! ```rust,ignore
//! use neutron::prelude::*;
//! use neutron::jwt::{JwtAuth, JwtConfig, Claims};
//! use neutron::extract::Extension;
//!
//! let config = JwtConfig::new(b"my-secret-key")
//!     .issuer("my-app")
//!     .audience("my-api")
//!     .leeway(30);
//!
//! let router = Router::new()
//!     .middleware(JwtAuth::new(config.clone()))
//!     .get("/me", |Extension(claims): Extension<Claims>| async move {
//!         format!("Hello, {}", claims.sub.unwrap_or_default())
//!     });
//! ```

use std::fmt;
use std::future::Future;
use std::pin::Pin;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use hmac::{Hmac, Mac};
use http::StatusCode;
use serde::{Deserialize, Serialize};
use sha2::Sha256;

use crate::handler::{Body, Request, Response};
use crate::middleware::{MiddlewareTrait, Next};

type HmacSha256 = Hmac<Sha256>;

// ---------------------------------------------------------------------------
// JwtError
// ---------------------------------------------------------------------------

/// Errors that can occur during JWT encoding or decoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JwtError {
    /// The token format is invalid (wrong number of parts, bad base64, etc.).
    InvalidToken,
    /// The token has expired (current time > exp + leeway).
    ExpiredToken,
    /// The token is not yet valid (current time < nbf - leeway).
    NotYetValid,
    /// The HMAC signature does not match.
    InvalidSignature,
    /// The `iss` claim does not match the expected issuer.
    InvalidIssuer,
    /// The `aud` claim does not match the expected audience.
    InvalidAudience,
    /// The `Authorization` header is missing or not in `Bearer <token>` format.
    MissingHeader,
}

impl fmt::Display for JwtError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JwtError::InvalidToken => write!(f, "invalid token"),
            JwtError::ExpiredToken => write!(f, "token has expired"),
            JwtError::NotYetValid => write!(f, "token is not yet valid"),
            JwtError::InvalidSignature => write!(f, "invalid signature"),
            JwtError::InvalidIssuer => write!(f, "invalid issuer"),
            JwtError::InvalidAudience => write!(f, "invalid audience"),
            JwtError::MissingHeader => write!(f, "missing or malformed Authorization header"),
        }
    }
}

// ---------------------------------------------------------------------------
// Claims
// ---------------------------------------------------------------------------

/// Standard JWT claims plus an `extra` field for arbitrary data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claims {
    /// Subject (who the token is about).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sub: Option<String>,
    /// Issuer (who issued the token).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub iss: Option<String>,
    /// Audience (who the token is intended for).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aud: Option<String>,
    /// Expiration time (Unix timestamp).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exp: Option<u64>,
    /// Not before (Unix timestamp).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nbf: Option<u64>,
    /// Issued at (Unix timestamp).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub iat: Option<u64>,
    /// Arbitrary extra claims (flattened into the payload JSON).
    #[serde(flatten)]
    pub extra: serde_json::Value,
}

impl Default for Claims {
    fn default() -> Self {
        Self {
            sub: None,
            iss: None,
            aud: None,
            exp: None,
            nbf: None,
            iat: None,
            extra: serde_json::Value::Object(serde_json::Map::new()),
        }
    }
}

// ---------------------------------------------------------------------------
// JwtConfig
// ---------------------------------------------------------------------------

/// Configuration for JWT encoding and decoding.
///
/// Uses HMAC-SHA256 for signing and verification.
#[derive(Clone)]
pub struct JwtConfig {
    secret: Vec<u8>,
    issuer: Option<String>,
    audience: Option<String>,
    leeway: u64,
}

impl JwtConfig {
    /// Create a new JWT config with the given HMAC secret key.
    pub fn new(secret: &[u8]) -> Self {
        Self {
            secret: secret.to_vec(),
            issuer: None,
            audience: None,
            leeway: 0,
        }
    }

    /// Set the expected `iss` claim for validation.
    pub fn issuer(mut self, issuer: impl Into<String>) -> Self {
        self.issuer = Some(issuer.into());
        self
    }

    /// Set the expected `aud` claim for validation.
    pub fn audience(mut self, audience: impl Into<String>) -> Self {
        self.audience = Some(audience.into());
        self
    }

    /// Set the leeway in seconds for `exp` and `nbf` validation (default: 0).
    pub fn leeway(mut self, leeway: u64) -> Self {
        self.leeway = leeway;
        self
    }

    /// Encode claims into a signed JWT string.
    pub fn encode(&self, claims: &Claims) -> Result<String, JwtError> {
        let header = r#"{"alg":"HS256","typ":"JWT"}"#;
        let header_b64 = URL_SAFE_NO_PAD.encode(header.as_bytes());

        let payload_json =
            serde_json::to_vec(claims).map_err(|_| JwtError::InvalidToken)?;
        let payload_b64 = URL_SAFE_NO_PAD.encode(&payload_json);

        let signing_input = format!("{header_b64}.{payload_b64}");
        let signature = self.sign(signing_input.as_bytes());
        let signature_b64 = URL_SAFE_NO_PAD.encode(&signature);

        Ok(format!("{signing_input}.{signature_b64}"))
    }

    /// Decode and validate a JWT string, returning the claims.
    pub fn decode(&self, token: &str) -> Result<Claims, JwtError> {
        let parts: Vec<&str> = token.split('.').collect();
        if parts.len() != 3 {
            return Err(JwtError::InvalidToken);
        }

        let header_b64 = parts[0];
        let payload_b64 = parts[1];
        let signature_b64 = parts[2];

        // Verify signature
        let signing_input = format!("{header_b64}.{payload_b64}");
        let expected_signature = self.sign(signing_input.as_bytes());
        let provided_signature = URL_SAFE_NO_PAD
            .decode(signature_b64)
            .map_err(|_| JwtError::InvalidToken)?;

        if !constant_time_eq(&expected_signature, &provided_signature) {
            return Err(JwtError::InvalidSignature);
        }

        // Decode payload
        let payload_bytes = URL_SAFE_NO_PAD
            .decode(payload_b64)
            .map_err(|_| JwtError::InvalidToken)?;
        let claims: Claims =
            serde_json::from_slice(&payload_bytes).map_err(|_| JwtError::InvalidToken)?;

        // Validate time-based claims
        let now = current_timestamp();

        if let Some(exp) = claims.exp {
            if now > exp.saturating_add(self.leeway) {
                return Err(JwtError::ExpiredToken);
            }
        }

        if let Some(nbf) = claims.nbf {
            if now.saturating_add(self.leeway) < nbf {
                return Err(JwtError::NotYetValid);
            }
        }

        // Validate issuer
        if let Some(ref expected_iss) = self.issuer {
            match &claims.iss {
                Some(iss) if iss == expected_iss => {}
                _ => return Err(JwtError::InvalidIssuer),
            }
        }

        // Validate audience
        if let Some(ref expected_aud) = self.audience {
            match &claims.aud {
                Some(aud) if aud == expected_aud => {}
                _ => return Err(JwtError::InvalidAudience),
            }
        }

        Ok(claims)
    }

    /// Compute HMAC-SHA256 of the given message.
    fn sign(&self, message: &[u8]) -> Vec<u8> {
        let mut mac =
            HmacSha256::new_from_slice(&self.secret).expect("HMAC accepts any key length");
        mac.update(message);
        mac.finalize().into_bytes().to_vec()
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Constant-time comparison to avoid timing attacks on signature verification.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Get the current Unix timestamp in seconds.
fn current_timestamp() -> u64 {
    std::time::SystemTime::UNIX_EPOCH
        .elapsed()
        .unwrap()
        .as_secs()
}

// ---------------------------------------------------------------------------
// JwtAuth middleware
// ---------------------------------------------------------------------------

/// JWT authentication middleware.
///
/// Extracts and validates the JWT from the `Authorization: Bearer <token>` header,
/// then stores the decoded [`Claims`] as a request extension.
///
/// # Example
///
/// ```rust,ignore
/// use neutron::jwt::{JwtAuth, JwtConfig};
///
/// let config = JwtConfig::new(b"secret");
/// let router = Router::new()
///     .middleware(JwtAuth::new(config))
///     .get("/protected", handler);
/// ```
pub struct JwtAuth {
    config: JwtConfig,
}

impl JwtAuth {
    /// Create a new JWT authentication middleware with the given config.
    pub fn new(config: JwtConfig) -> Self {
        Self { config }
    }
}

/// Build a 401 Unauthorized JSON error response.
fn unauthorized_response(error: &JwtError) -> Response {
    let body = serde_json::json!({ "error": error.to_string() });
    http::Response::builder()
        .status(StatusCode::UNAUTHORIZED)
        .header("content-type", "application/json")
        .body(Body::full(serde_json::to_vec(&body).unwrap()))
        .unwrap()
}

impl MiddlewareTrait for JwtAuth {
    fn call(&self, req: Request, next: Next) -> Pin<Box<dyn Future<Output = Response> + Send>> {
        let config = self.config.clone();

        Box::pin(async move {
            let mut req = req;

            // Extract token from Authorization header
            let token = match extract_bearer_token(&req) {
                Some(t) => t,
                None => return unauthorized_response(&JwtError::MissingHeader),
            };

            // Decode and validate
            let claims = match config.decode(&token) {
                Ok(c) => c,
                Err(e) => return unauthorized_response(&e),
            };

            // Store claims as a request extension
            req.set_extension(claims);

            next.run(req).await
        })
    }
}

/// Extract the bearer token from the `Authorization` header.
fn extract_bearer_token(req: &Request) -> Option<String> {
    let header_value = req.headers().get("authorization")?;
    let value = header_value.to_str().ok()?;
    let token = value.strip_prefix("Bearer ")?;
    if token.is_empty() {
        return None;
    }
    Some(token.to_string())
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extract::Extension;
    use crate::router::Router;
    use crate::testing::TestClient;

    fn test_config() -> JwtConfig {
        JwtConfig::new(b"test-secret-key-for-jwt")
    }

    fn make_claims(sub: &str) -> Claims {
        Claims {
            sub: Some(sub.to_string()),
            ..Claims::default()
        }
    }

    // -----------------------------------------------------------------------
    // 1. Encode and decode roundtrip
    // -----------------------------------------------------------------------

    #[test]
    fn encode_decode_roundtrip() {
        let config = test_config();
        let claims = Claims {
            sub: Some("user-123".to_string()),
            iss: Some("my-app".to_string()),
            aud: Some("my-api".to_string()),
            iat: Some(1000000),
            extra: serde_json::json!({ "role": "admin" }),
            ..Claims::default()
        };

        let token = config.encode(&claims).unwrap();
        let decoded = config.decode(&token).unwrap();

        assert_eq!(decoded.sub, Some("user-123".to_string()));
        assert_eq!(decoded.iss, Some("my-app".to_string()));
        assert_eq!(decoded.aud, Some("my-api".to_string()));
        assert_eq!(decoded.iat, Some(1000000));
        assert_eq!(decoded.extra["role"], "admin");
    }

    // -----------------------------------------------------------------------
    // 2. Invalid signature rejected
    // -----------------------------------------------------------------------

    #[test]
    fn invalid_signature_rejected() {
        let config = test_config();
        let claims = make_claims("user-1");
        let token = config.encode(&claims).unwrap();

        // Use a different secret to decode
        let bad_config = JwtConfig::new(b"wrong-secret");
        let result = bad_config.decode(&token);

        assert_eq!(result.unwrap_err(), JwtError::InvalidSignature);
    }

    // -----------------------------------------------------------------------
    // 3. Expired token rejected
    // -----------------------------------------------------------------------

    #[test]
    fn expired_token_rejected() {
        let config = test_config();
        let claims = Claims {
            sub: Some("user-1".to_string()),
            exp: Some(1), // expired long ago
            ..Claims::default()
        };

        let token = config.encode(&claims).unwrap();
        let result = config.decode(&token);

        assert_eq!(result.unwrap_err(), JwtError::ExpiredToken);
    }

    // -----------------------------------------------------------------------
    // 4. Not-yet-valid token rejected
    // -----------------------------------------------------------------------

    #[test]
    fn not_yet_valid_rejected() {
        let config = test_config();
        let far_future = current_timestamp() + 3600;
        let claims = Claims {
            sub: Some("user-1".to_string()),
            nbf: Some(far_future),
            ..Claims::default()
        };

        let token = config.encode(&claims).unwrap();
        let result = config.decode(&token);

        assert_eq!(result.unwrap_err(), JwtError::NotYetValid);
    }

    // -----------------------------------------------------------------------
    // 5. Issuer validation
    // -----------------------------------------------------------------------

    #[test]
    fn issuer_validation_passes() {
        let config = test_config().issuer("my-app");
        let claims = Claims {
            sub: Some("user-1".to_string()),
            iss: Some("my-app".to_string()),
            ..Claims::default()
        };

        let token = config.encode(&claims).unwrap();
        let decoded = config.decode(&token).unwrap();
        assert_eq!(decoded.iss, Some("my-app".to_string()));
    }

    #[test]
    fn issuer_validation_fails() {
        let config = test_config().issuer("my-app");
        let claims = Claims {
            sub: Some("user-1".to_string()),
            iss: Some("other-app".to_string()),
            ..Claims::default()
        };

        let token = config.encode(&claims).unwrap();
        let result = config.decode(&token);

        assert_eq!(result.unwrap_err(), JwtError::InvalidIssuer);
    }

    #[test]
    fn issuer_validation_fails_when_missing() {
        let config = test_config().issuer("my-app");
        let claims = Claims {
            sub: Some("user-1".to_string()),
            ..Claims::default()
        };

        let token = config.encode(&claims).unwrap();
        let result = config.decode(&token);

        assert_eq!(result.unwrap_err(), JwtError::InvalidIssuer);
    }

    // -----------------------------------------------------------------------
    // 6. Audience validation
    // -----------------------------------------------------------------------

    #[test]
    fn audience_validation_passes() {
        let config = test_config().audience("my-api");
        let claims = Claims {
            sub: Some("user-1".to_string()),
            aud: Some("my-api".to_string()),
            ..Claims::default()
        };

        let token = config.encode(&claims).unwrap();
        let decoded = config.decode(&token).unwrap();
        assert_eq!(decoded.aud, Some("my-api".to_string()));
    }

    #[test]
    fn audience_validation_fails() {
        let config = test_config().audience("my-api");
        let claims = Claims {
            sub: Some("user-1".to_string()),
            aud: Some("other-api".to_string()),
            ..Claims::default()
        };

        let token = config.encode(&claims).unwrap();
        let result = config.decode(&token);

        assert_eq!(result.unwrap_err(), JwtError::InvalidAudience);
    }

    #[test]
    fn audience_validation_fails_when_missing() {
        let config = test_config().audience("my-api");
        let claims = Claims {
            sub: Some("user-1".to_string()),
            ..Claims::default()
        };

        let token = config.encode(&claims).unwrap();
        let result = config.decode(&token);

        assert_eq!(result.unwrap_err(), JwtError::InvalidAudience);
    }

    // -----------------------------------------------------------------------
    // 7. Middleware passes with valid token
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn middleware_passes_with_valid_token() {
        let config = test_config();
        let claims = make_claims("alice");
        let token = config.encode(&claims).unwrap();

        let client = TestClient::new(
            Router::new()
                .middleware(JwtAuth::new(config))
                .get("/", |Extension(claims): Extension<Claims>| async move {
                    format!("Hello, {}", claims.sub.unwrap_or_default())
                }),
        );

        let resp = client
            .get("/")
            .header("authorization", &format!("Bearer {token}"))
            .send()
            .await;

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.text().await, "Hello, alice");
    }

    // -----------------------------------------------------------------------
    // 8. Middleware rejects missing Authorization header (401)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn middleware_rejects_missing_header() {
        let config = test_config();

        let client = TestClient::new(
            Router::new()
                .middleware(JwtAuth::new(config))
                .get("/", || async { "ok" }),
        );

        let resp = client.get("/").send().await;

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        let body: serde_json::Value = resp.json().await;
        assert_eq!(body["error"], "missing or malformed Authorization header");
    }

    // -----------------------------------------------------------------------
    // 9. Middleware rejects invalid token (401)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn middleware_rejects_invalid_token() {
        let config = test_config();

        let client = TestClient::new(
            Router::new()
                .middleware(JwtAuth::new(config))
                .get("/", || async { "ok" }),
        );

        let resp = client
            .get("/")
            .header("authorization", "Bearer not.a.valid-token")
            .send()
            .await;

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // -----------------------------------------------------------------------
    // 10. Leeway allows slightly expired tokens
    // -----------------------------------------------------------------------

    #[test]
    fn leeway_allows_slightly_expired_token() {
        let now = current_timestamp();
        // Token expired 5 seconds ago
        let claims = Claims {
            sub: Some("user-1".to_string()),
            exp: Some(now - 5),
            ..Claims::default()
        };

        // Without leeway, token is rejected
        let config_no_leeway = test_config();
        let token = config_no_leeway.encode(&claims).unwrap();
        assert_eq!(
            config_no_leeway.decode(&token).unwrap_err(),
            JwtError::ExpiredToken
        );

        // With 10 seconds of leeway, token is accepted
        let config_leeway = test_config().leeway(10);
        let decoded = config_leeway.decode(&token).unwrap();
        assert_eq!(decoded.sub, Some("user-1".to_string()));
    }

    #[test]
    fn leeway_allows_slightly_future_nbf() {
        let now = current_timestamp();
        // Token becomes valid 5 seconds from now
        let claims = Claims {
            sub: Some("user-1".to_string()),
            nbf: Some(now + 5),
            ..Claims::default()
        };

        // Without leeway, token is rejected
        let config_no_leeway = test_config();
        let token = config_no_leeway.encode(&claims).unwrap();
        assert_eq!(
            config_no_leeway.decode(&token).unwrap_err(),
            JwtError::NotYetValid
        );

        // With 10 seconds of leeway, token is accepted
        let config_leeway = test_config().leeway(10);
        let decoded = config_leeway.decode(&token).unwrap();
        assert_eq!(decoded.sub, Some("user-1".to_string()));
    }

    // -----------------------------------------------------------------------
    // Additional edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn token_with_three_parts_required() {
        let config = test_config();
        assert_eq!(config.decode("only.two").unwrap_err(), JwtError::InvalidToken);
        assert_eq!(config.decode("one").unwrap_err(), JwtError::InvalidToken);
        assert_eq!(config.decode("").unwrap_err(), JwtError::InvalidToken);
    }

    #[test]
    fn tampered_payload_detected() {
        let config = test_config();
        let claims = make_claims("user-1");
        let token = config.encode(&claims).unwrap();

        // Tamper with the payload (middle part)
        let parts: Vec<&str> = token.split('.').collect();
        let tampered_payload = URL_SAFE_NO_PAD.encode(
            serde_json::to_vec(&serde_json::json!({"sub": "admin"})).unwrap(),
        );
        let tampered_token = format!("{}.{}.{}", parts[0], tampered_payload, parts[2]);

        assert_eq!(
            config.decode(&tampered_token).unwrap_err(),
            JwtError::InvalidSignature
        );
    }

    #[test]
    fn default_claims_roundtrip() {
        let config = test_config();
        let claims = Claims::default();
        let token = config.encode(&claims).unwrap();
        let decoded = config.decode(&token).unwrap();

        assert_eq!(decoded.sub, None);
        assert_eq!(decoded.iss, None);
        assert_eq!(decoded.aud, None);
        assert_eq!(decoded.exp, None);
        assert_eq!(decoded.nbf, None);
        assert_eq!(decoded.iat, None);
    }

    #[tokio::test]
    async fn middleware_rejects_expired_token() {
        let config = test_config();
        let claims = Claims {
            sub: Some("user-1".to_string()),
            exp: Some(1), // long expired
            ..Claims::default()
        };
        let token = config.encode(&claims).unwrap();

        let client = TestClient::new(
            Router::new()
                .middleware(JwtAuth::new(config))
                .get("/", || async { "ok" }),
        );

        let resp = client
            .get("/")
            .header("authorization", &format!("Bearer {token}"))
            .send()
            .await;

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        let body: serde_json::Value = resp.json().await;
        assert_eq!(body["error"], "token has expired");
    }
}
