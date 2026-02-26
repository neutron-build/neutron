//! CSRF (Cross-Site Request Forgery) protection middleware.
//!
//! Uses the double-submit cookie pattern: a random token is set as a signed
//! cookie and must be echoed back in a request header or form field for
//! state-changing (non-safe) methods.
//!
//! # Example
//!
//! ```rust,ignore
//! use neutron::prelude::*;
//! use neutron::csrf::{CsrfLayer, CsrfToken};
//! use neutron::cookie::Key;
//!
//! let key = Key::generate();
//!
//! let router = Router::new()
//!     .middleware(CsrfLayer::new(key))
//!     .get("/form", |token: CsrfToken| async move {
//!         format!(r#"<input type="hidden" name="_csrf" value="{}">"#, token.0)
//!     })
//!     .post("/submit", || async { "ok" });
//! ```
//!
//! ## How It Works
//!
//! 1. On every request, the middleware generates a CSRF token and sets it as
//!    a signed cookie (if not already present).
//! 2. The [`CsrfToken`] extractor provides the token value for embedding in
//!    HTML forms or JavaScript.
//! 3. For non-safe methods (POST, PUT, DELETE, PATCH), the middleware verifies
//!    that the request includes the token via the `X-CSRF-Token` header or
//!    `_csrf` form field, matching the signed cookie value.
//! 4. Safe methods (GET, HEAD, OPTIONS, TRACE) skip verification.

use std::future::Future;
use std::pin::Pin;

use http::{Method, StatusCode};
use rand::RngCore;

use crate::cookie::Key;
use crate::extract::FromRequest;
use crate::handler::{Body, IntoResponse, Request, Response};
use crate::middleware::{MiddlewareTrait, Next};

// ---------------------------------------------------------------------------
// CsrfToken extractor
// ---------------------------------------------------------------------------

/// CSRF token extractor.
///
/// Provides the current CSRF token value for embedding in forms.
/// Requires [`CsrfLayer`] middleware to be active.
///
/// ```rust,ignore
/// async fn form(token: CsrfToken) -> String {
///     format!(r#"<input type="hidden" name="_csrf" value="{}">"#, token.0)
/// }
/// ```
#[derive(Clone, Debug)]
pub struct CsrfToken(pub String);

impl FromRequest for CsrfToken {
    fn from_request(req: &Request) -> Result<Self, Response> {
        req.get_extension::<CsrfToken>()
            .cloned()
            .ok_or_else(|| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "CsrfLayer middleware not configured",
                )
                    .into_response()
            })
    }
}

// ---------------------------------------------------------------------------
// CsrfLayer middleware
// ---------------------------------------------------------------------------

/// CSRF protection middleware using the double-submit cookie pattern.
///
/// # Example
///
/// ```rust,ignore
/// use neutron::prelude::*;
/// use neutron::csrf::CsrfLayer;
/// use neutron::cookie::Key;
///
/// let router = Router::new()
///     .middleware(CsrfLayer::new(Key::generate()))
///     .post("/api/action", handler);
/// ```
pub struct CsrfLayer {
    key: Key,
    cookie_name: String,
    header_name: String,
    form_field: String,
    cookie_path: String,
    secure: bool,
}

impl CsrfLayer {
    /// Create a CSRF layer with the given signing key.
    pub fn new(key: Key) -> Self {
        Self {
            key,
            cookie_name: "_csrf".to_string(),
            header_name: "x-csrf-token".to_string(),
            form_field: "_csrf".to_string(),
            cookie_path: "/".to_string(),
            secure: false,
        }
    }

    /// Set the CSRF cookie name (default: `"_csrf"`).
    pub fn cookie_name(mut self, name: impl Into<String>) -> Self {
        self.cookie_name = name.into();
        self
    }

    /// Set the header name for token submission (default: `"x-csrf-token"`).
    pub fn header_name(mut self, name: impl Into<String>) -> Self {
        self.header_name = name.into();
        self
    }

    /// Set the form field name for token submission (default: `"_csrf"`).
    pub fn form_field(mut self, name: impl Into<String>) -> Self {
        self.form_field = name.into();
        self
    }

    /// Set the cookie path (default: `"/"`).
    pub fn cookie_path(mut self, path: impl Into<String>) -> Self {
        self.cookie_path = path.into();
        self
    }

    /// Require HTTPS for the CSRF cookie (default: `false`).
    pub fn secure(mut self, secure: bool) -> Self {
        self.secure = secure;
        self
    }
}

fn generate_token() -> String {
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn is_safe_method(method: &Method) -> bool {
    matches!(
        *method,
        Method::GET | Method::HEAD | Method::OPTIONS | Method::TRACE
    )
}

fn parse_cookie_value(headers: &http::HeaderMap, cookie_name: &str) -> Option<String> {
    headers
        .get_all("cookie")
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|s| s.split(';'))
        .find_map(|pair| {
            let pair = pair.trim();
            let (name, value) = pair.split_once('=')?;
            if name.trim() == cookie_name {
                Some(value.trim().to_string())
            } else {
                None
            }
        })
}

fn extract_form_field(body: &[u8], field_name: &str) -> Option<String> {
    let body_str = std::str::from_utf8(body).ok()?;
    serde_urlencoded::from_str::<Vec<(String, String)>>(body_str)
        .ok()?
        .into_iter()
        .find(|(k, _)| k == field_name)
        .map(|(_, v)| v)
}

impl MiddlewareTrait for CsrfLayer {
    fn call(&self, req: Request, next: Next) -> Pin<Box<dyn Future<Output = Response> + Send>> {
        let key = self.key.clone();
        let cookie_name = self.cookie_name.clone();
        let header_name = self.header_name.clone();
        let form_field = self.form_field.clone();
        let cookie_path = self.cookie_path.clone();
        let secure = self.secure;

        Box::pin(async move {
            let mut req = req;

            // Try to get existing token from signed cookie
            let existing_token =
                parse_cookie_value(req.headers(), &cookie_name).and_then(|v| key.verify(&v));

            let token = existing_token.unwrap_or_else(generate_token);

            // For non-safe methods, verify the token
            if !is_safe_method(req.method()) {
                // Get the submitted token from header or form field
                let submitted = req
                    .headers()
                    .get(&header_name)
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.to_string())
                    .or_else(|| extract_form_field(req.body(), &form_field));

                match submitted {
                    Some(ref submitted_token) if submitted_token == &token => {
                        // Token matches — proceed
                    }
                    _ => {
                        return http::Response::builder()
                            .status(StatusCode::FORBIDDEN)
                            .header("content-type", "text/plain; charset=utf-8")
                            .body(Body::full("CSRF token missing or invalid"))
                            .unwrap();
                    }
                }
            }

            // Set token as extension for CsrfToken extractor
            req.set_extension(CsrfToken(token.clone()));

            let mut resp = next.run(req).await;

            // Set/refresh the signed CSRF cookie
            let signed_token = key.sign(&token);
            let mut cookie_parts = vec![
                format!("{cookie_name}={signed_token}"),
                format!("Path={cookie_path}"),
                "SameSite=Strict".to_string(),
            ];
            if secure {
                cookie_parts.push("Secure".to_string());
            }
            resp.headers_mut().append(
                "set-cookie",
                cookie_parts.join("; ").parse().unwrap(),
            );

            resp
        })
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cookie::Key;
    use crate::router::Router;
    use crate::testing::TestClient;

    fn test_client() -> (TestClient, Key) {
        let key = Key::generate();
        let client = TestClient::new(
            Router::new()
                .middleware(CsrfLayer::new(key.clone()))
                .get("/form", |token: CsrfToken| async move { token.0 })
                .post("/submit", || async { "ok" })
                .put("/update", || async { "updated" })
                .delete("/remove", || async { "deleted" }),
        );
        (client, key)
    }

    #[tokio::test]
    async fn get_request_passes_without_token() {
        let (client, _key) = test_client();

        let resp = client.get("/form").send().await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(resp.header("set-cookie").is_some());
    }

    #[tokio::test]
    async fn get_returns_csrf_token() {
        let (client, _key) = test_client();

        let resp = client.get("/form").send().await;
        let token = resp.text().await;
        assert_eq!(token.len(), 64); // 32 bytes hex-encoded
    }

    #[tokio::test]
    async fn post_without_token_rejected() {
        let (client, _key) = test_client();

        let resp = client.post("/submit").send().await;
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
        assert_eq!(resp.text().await, "CSRF token missing or invalid");
    }

    #[tokio::test]
    async fn post_with_header_token_accepted() {
        let (client, _key) = test_client();

        // Get a token first — read header before consuming body
        let resp = client.get("/form").send().await;
        let set_cookie = resp.header("set-cookie").unwrap().to_string();
        let token = resp.text().await;

        // Extract cookie from Set-Cookie header
        let cookie_val = set_cookie.split(';').next().unwrap().trim();

        // Submit with the token in header
        let resp = client
            .post("/submit")
            .header("cookie", cookie_val)
            .header("x-csrf-token", &token)
            .send()
            .await;

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.text().await, "ok");
    }

    #[tokio::test]
    async fn post_with_form_field_accepted() {
        let (client, _key) = test_client();

        // Get a token — read header before consuming body
        let resp = client.get("/form").send().await;
        let set_cookie = resp.header("set-cookie").unwrap().to_string();
        let token = resp.text().await;
        let cookie_val = set_cookie.split(';').next().unwrap().trim();

        // Submit with token as form field
        let form_body = format!("_csrf={token}&data=hello");
        let resp = client
            .post("/submit")
            .header("cookie", cookie_val)
            .header("content-type", "application/x-www-form-urlencoded")
            .body(form_body)
            .send()
            .await;

        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn post_with_wrong_token_rejected() {
        let (client, _key) = test_client();

        // Get a valid cookie
        let resp = client.get("/form").send().await;
        let set_cookie = resp.header("set-cookie").unwrap().to_string();
        let cookie_val = set_cookie.split(';').next().unwrap().trim();

        // Submit with wrong token
        let resp = client
            .post("/submit")
            .header("cookie", cookie_val)
            .header("x-csrf-token", "wrong-token")
            .send()
            .await;

        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn put_requires_token() {
        let (client, _key) = test_client();

        let resp = client.put("/update").send().await;
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn delete_requires_token() {
        let (client, _key) = test_client();

        let resp = client.delete("/remove").send().await;
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn safe_methods_skip_verification() {
        let (client, _key) = test_client();

        // GET
        let resp = client.get("/form").send().await;
        assert_eq!(resp.status(), StatusCode::OK);

        // HEAD (will hit /form route as GET)
        let resp = client.request(Method::OPTIONS, "/form").send().await;
        // OPTIONS returns 405 because no OPTIONS handler, but that's
        // the router returning 405, not CSRF blocking
        // The point is CSRF doesn't block it
        assert_ne!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn csrf_cookie_is_samesite_strict() {
        let (client, _key) = test_client();

        let resp = client.get("/form").send().await;
        let set_cookie = resp.header("set-cookie").unwrap();
        assert!(set_cookie.contains("SameSite=Strict"));
    }

    #[tokio::test]
    async fn custom_cookie_and_header_names() {
        let key = Key::generate();
        let client = TestClient::new(
            Router::new()
                .middleware(
                    CsrfLayer::new(key.clone())
                        .cookie_name("my-csrf")
                        .header_name("x-my-csrf"),
                )
                .get("/form", |token: CsrfToken| async move { token.0 })
                .post("/submit", || async { "ok" }),
        );

        // Get token with custom cookie name
        let resp = client.get("/form").send().await;
        let set_cookie = resp.header("set-cookie").unwrap().to_string();
        let token = resp.text().await;
        assert!(set_cookie.contains("my-csrf="));

        let cookie_val = set_cookie.split(';').next().unwrap().trim();

        // Submit with custom header name
        let resp = client
            .post("/submit")
            .header("cookie", cookie_val)
            .header("x-my-csrf", &token)
            .send()
            .await;

        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn without_middleware_returns_500() {
        let client = TestClient::new(
            Router::new().get("/", |token: CsrfToken| async move { token.0 }),
        );

        let resp = client.get("/").send().await;
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn token_reused_across_requests() {
        let (client, _key) = test_client();

        // Get initial token and cookie
        let resp = client.get("/form").send().await;
        let set_cookie = resp.header("set-cookie").unwrap().to_string();
        let token1 = resp.text().await;
        let cookie_val = set_cookie.split(';').next().unwrap().trim();

        // Second request with the cookie returns the same token
        let resp = client
            .get("/form")
            .header("cookie", cookie_val)
            .send()
            .await;
        let token2 = resp.text().await;

        assert_eq!(token1, token2);
    }
}
