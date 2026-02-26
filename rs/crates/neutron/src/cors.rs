//! Cross-Origin Resource Sharing (CORS) middleware.
//!
//! Handles preflight `OPTIONS` requests and adds `Access-Control-*` headers
//! to all responses. Supports origin allowlists, credential mode, and max-age.
//!
//! ```rust,ignore
//! Router::new().middleware(
//!     Cors::new().allow_any_origin().allow_any_header().max_age(3600),
//! )
//! ```

use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;

use http::{HeaderValue, Method, StatusCode};

use crate::handler::{Body, Request, Response};
use crate::middleware::{MiddlewareTrait, Next};

// ---------------------------------------------------------------------------
// Configuration types
// ---------------------------------------------------------------------------

#[derive(Clone)]
enum AllowOrigin {
    /// Respond with `*` (or echo origin when credentials are enabled).
    Any,
    /// Only allow origins in this set; echo the matching origin back.
    List(HashSet<HeaderValue>),
}

#[derive(Clone)]
enum AllowMethods {
    /// Echo back the request's `Access-Control-Request-Method`.
    Any,
    /// Only these methods are allowed.
    List(Vec<Method>),
}

#[derive(Clone)]
enum AllowHeaders {
    /// Echo back the request's `Access-Control-Request-Headers`.
    Any,
    /// Only these headers are allowed.
    List(Vec<HeaderValue>),
}

// ---------------------------------------------------------------------------
// Cors builder
// ---------------------------------------------------------------------------

/// Configurable CORS middleware.
///
/// Handles preflight `OPTIONS` requests automatically and adds the
/// appropriate `Access-Control-*` headers to all responses.
///
/// # Example
///
/// ```rust,ignore
/// use neutron::prelude::*;
/// use neutron::cors::Cors;
///
/// let cors = Cors::new()
///     .allow_any_origin()
///     .allow_methods([Method::GET, Method::POST])
///     .allow_any_header()
///     .max_age(3600);
///
/// let router = Router::new()
///     .middleware(cors)
///     .get("/", || async { "hello" });
/// ```
#[derive(Clone)]
pub struct Cors {
    origins: AllowOrigin,
    methods: AllowMethods,
    headers: AllowHeaders,
    expose_headers: Vec<HeaderValue>,
    max_age: Option<u64>,
    credentials: bool,
}

impl Cors {
    pub fn new() -> Self {
        Self {
            origins: AllowOrigin::List(HashSet::new()),
            methods: AllowMethods::List(vec![Method::GET, Method::HEAD, Method::OPTIONS]),
            headers: AllowHeaders::List(Vec::new()),
            expose_headers: Vec::new(),
            max_age: None,
            credentials: false,
        }
    }

    /// Allow any origin (`Access-Control-Allow-Origin: *`).
    ///
    /// When combined with `allow_credentials()`, the request's `Origin`
    /// header is echoed back instead of `*` (per the CORS spec).
    pub fn allow_any_origin(mut self) -> Self {
        self.origins = AllowOrigin::Any;
        self
    }

    /// Allow a specific origin.
    pub fn allow_origin(mut self, origin: &str) -> Self {
        match &mut self.origins {
            AllowOrigin::Any => {
                let mut set = HashSet::new();
                set.insert(HeaderValue::from_str(origin).expect("invalid origin header value"));
                self.origins = AllowOrigin::List(set);
            }
            AllowOrigin::List(set) => {
                set.insert(HeaderValue::from_str(origin).expect("invalid origin header value"));
            }
        }
        self
    }

    /// Allow multiple specific origins.
    pub fn allow_origins<I, S>(mut self, origins: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        for origin in origins {
            self = self.allow_origin(origin.as_ref());
        }
        self
    }

    /// Allow any HTTP method in preflight requests.
    pub fn allow_any_method(mut self) -> Self {
        self.methods = AllowMethods::Any;
        self
    }

    /// Allow specific HTTP methods.
    pub fn allow_methods<I>(mut self, methods: I) -> Self
    where
        I: IntoIterator<Item = Method>,
    {
        self.methods = AllowMethods::List(methods.into_iter().collect());
        self
    }

    /// Allow any request header.
    pub fn allow_any_header(mut self) -> Self {
        self.headers = AllowHeaders::Any;
        self
    }

    /// Allow specific request headers.
    pub fn allow_headers<I, S>(mut self, headers: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.headers = AllowHeaders::List(
            headers
                .into_iter()
                .map(|h| HeaderValue::from_str(h.as_ref()).expect("invalid header value"))
                .collect(),
        );
        self
    }

    /// Expose specific response headers to the browser.
    pub fn expose_headers<I, S>(mut self, headers: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.expose_headers = headers
            .into_iter()
            .map(|h| HeaderValue::from_str(h.as_ref()).expect("invalid header value"))
            .collect();
        self
    }

    /// Set `Access-Control-Allow-Credentials: true`.
    pub fn allow_credentials(mut self) -> Self {
        self.credentials = true;
        self
    }

    /// Set the `Access-Control-Max-Age` header (seconds).
    pub fn max_age(mut self, seconds: u64) -> Self {
        self.max_age = Some(seconds);
        self
    }

    // -- Internal helpers ---------------------------------------------------

    fn origin_header(&self, request_origin: Option<&HeaderValue>) -> Option<HeaderValue> {
        match &self.origins {
            AllowOrigin::Any => {
                if self.credentials {
                    // Spec: credentials + wildcard → echo the origin.
                    request_origin.cloned()
                } else {
                    Some(HeaderValue::from_static("*"))
                }
            }
            AllowOrigin::List(set) => request_origin
                .filter(|o| set.contains(*o))
                .cloned(),
        }
    }

    fn methods_header(&self, request_method: Option<&HeaderValue>) -> HeaderValue {
        match &self.methods {
            AllowMethods::Any => request_method
                .cloned()
                .unwrap_or_else(|| HeaderValue::from_static("*")),
            AllowMethods::List(methods) => {
                let s: String = methods
                    .iter()
                    .map(|m| m.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                HeaderValue::from_str(&s).unwrap()
            }
        }
    }

    fn headers_header(&self, request_headers: Option<&HeaderValue>) -> HeaderValue {
        match &self.headers {
            AllowHeaders::Any => request_headers
                .cloned()
                .unwrap_or_else(|| HeaderValue::from_static("*")),
            AllowHeaders::List(headers) => {
                let s: String = headers
                    .iter()
                    .map(|h| h.to_str().unwrap_or(""))
                    .collect::<Vec<_>>()
                    .join(", ");
                HeaderValue::from_str(&s).unwrap()
            }
        }
    }

    /// Add CORS headers common to both preflight and normal responses.
    fn apply_common_headers(&self, resp: &mut Response, request_origin: Option<&HeaderValue>) {
        if let Some(origin_val) = self.origin_header(request_origin) {
            resp.headers_mut()
                .insert("access-control-allow-origin", origin_val);
        }

        if self.credentials {
            resp.headers_mut().insert(
                "access-control-allow-credentials",
                HeaderValue::from_static("true"),
            );
        }

        if !self.expose_headers.is_empty() {
            let s = self
                .expose_headers
                .iter()
                .map(|h| h.to_str().unwrap_or(""))
                .collect::<Vec<_>>()
                .join(", ");
            resp.headers_mut().insert(
                "access-control-expose-headers",
                HeaderValue::from_str(&s).unwrap(),
            );
        }

        // Vary: Origin when origin is list-based or credentials are enabled,
        // so caches key responses correctly per-origin.
        if matches!(self.origins, AllowOrigin::List(_)) || self.credentials {
            resp.headers_mut()
                .append("vary", HeaderValue::from_static("origin"));
        }
    }

    /// Build a complete preflight (204 No Content) response.
    fn preflight_response(&self, req: &Request) -> Response {
        let request_origin = req.headers().get("origin");
        let request_method = req.headers().get("access-control-request-method");
        let request_headers = req.headers().get("access-control-request-headers");

        let mut resp = http::Response::builder()
            .status(StatusCode::NO_CONTENT)
            .body(Body::empty())
            .unwrap();

        self.apply_common_headers(&mut resp, request_origin);

        resp.headers_mut().insert(
            "access-control-allow-methods",
            self.methods_header(request_method),
        );

        resp.headers_mut().insert(
            "access-control-allow-headers",
            self.headers_header(request_headers),
        );

        if let Some(max_age) = self.max_age {
            resp.headers_mut().insert(
                "access-control-max-age",
                HeaderValue::from_str(&max_age.to_string()).unwrap(),
            );
        }

        resp
    }
}

impl Default for Cors {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// MiddlewareTrait implementation
// ---------------------------------------------------------------------------

impl MiddlewareTrait for Cors {
    fn call(&self, req: Request, next: Next) -> Pin<Box<dyn Future<Output = Response> + Send>> {
        let cors = self.clone();

        Box::pin(async move {
            // Preflight: OPTIONS with an Origin header → respond immediately.
            if *req.method() == Method::OPTIONS && req.headers().contains_key("origin") {
                return cors.preflight_response(&req);
            }

            // Capture the origin before forwarding to the next handler.
            let request_origin = req.headers().get("origin").cloned();

            let mut resp = next.run(req).await;
            cors.apply_common_headers(&mut resp, request_origin.as_ref());
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
    use bytes::Bytes;
    use http::HeaderMap;
    use std::sync::Arc;

    fn ok_response() -> Response {
        http::Response::builder()
            .status(StatusCode::OK)
            .body(Body::full("ok"))
            .unwrap()
    }

    fn make_next() -> Next {
        Next::new(Arc::new(|_req| {
            Box::pin(async { ok_response() }) as Pin<Box<dyn Future<Output = Response> + Send>>
        }))
    }

    fn make_request(method: Method, headers: &[(&str, &str)]) -> Request {
        let mut hmap = HeaderMap::new();
        for &(k, v) in headers {
            hmap.insert(
                http::header::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                HeaderValue::from_str(v).unwrap(),
            );
        }
        Request::new(method, "/test".parse().unwrap(), hmap, Bytes::new())
    }

    // -----------------------------------------------------------------------
    // Preflight
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn preflight_any_origin() {
        let cors = Cors::new().allow_any_origin().allow_any_method().allow_any_header();
        let req = make_request(
            Method::OPTIONS,
            &[
                ("origin", "http://example.com"),
                ("access-control-request-method", "POST"),
            ],
        );

        let resp = cors.call(req, make_next()).await;
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
        assert_eq!(
            resp.headers().get("access-control-allow-origin").unwrap(),
            "*"
        );
        assert_eq!(
            resp.headers().get("access-control-allow-methods").unwrap(),
            "POST"
        );
    }

    #[tokio::test]
    async fn preflight_specific_origin_allowed() {
        let cors = Cors::new()
            .allow_origin("http://localhost:3000")
            .allow_methods([Method::GET, Method::POST]);

        let req = make_request(
            Method::OPTIONS,
            &[
                ("origin", "http://localhost:3000"),
                ("access-control-request-method", "POST"),
            ],
        );

        let resp = cors.call(req, make_next()).await;
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
        assert_eq!(
            resp.headers().get("access-control-allow-origin").unwrap(),
            "http://localhost:3000"
        );
    }

    #[tokio::test]
    async fn preflight_specific_origin_rejected() {
        let cors = Cors::new()
            .allow_origin("http://localhost:3000")
            .allow_any_method();

        let req = make_request(
            Method::OPTIONS,
            &[
                ("origin", "http://evil.com"),
                ("access-control-request-method", "POST"),
            ],
        );

        let resp = cors.call(req, make_next()).await;
        // No Allow-Origin header when origin is not allowed.
        assert!(resp.headers().get("access-control-allow-origin").is_none());
    }

    #[tokio::test]
    async fn preflight_max_age() {
        let cors = Cors::new().allow_any_origin().allow_any_method().max_age(3600);

        let req = make_request(
            Method::OPTIONS,
            &[
                ("origin", "http://example.com"),
                ("access-control-request-method", "GET"),
            ],
        );

        let resp = cors.call(req, make_next()).await;
        assert_eq!(
            resp.headers().get("access-control-max-age").unwrap(),
            "3600"
        );
    }

    #[tokio::test]
    async fn preflight_allow_headers_echoed() {
        let cors = Cors::new().allow_any_origin().allow_any_method().allow_any_header();

        let req = make_request(
            Method::OPTIONS,
            &[
                ("origin", "http://example.com"),
                ("access-control-request-method", "POST"),
                ("access-control-request-headers", "content-type, authorization"),
            ],
        );

        let resp = cors.call(req, make_next()).await;
        assert_eq!(
            resp.headers().get("access-control-allow-headers").unwrap(),
            "content-type, authorization"
        );
    }

    #[tokio::test]
    async fn preflight_specific_headers() {
        let cors = Cors::new()
            .allow_any_origin()
            .allow_any_method()
            .allow_headers(["content-type"]);

        let req = make_request(
            Method::OPTIONS,
            &[
                ("origin", "http://example.com"),
                ("access-control-request-method", "POST"),
                ("access-control-request-headers", "content-type, authorization"),
            ],
        );

        let resp = cors.call(req, make_next()).await;
        assert_eq!(
            resp.headers().get("access-control-allow-headers").unwrap(),
            "content-type"
        );
    }

    #[tokio::test]
    async fn preflight_specific_methods() {
        let cors = Cors::new()
            .allow_any_origin()
            .allow_methods([Method::GET, Method::POST]);

        let req = make_request(
            Method::OPTIONS,
            &[
                ("origin", "http://example.com"),
                ("access-control-request-method", "DELETE"),
            ],
        );

        let resp = cors.call(req, make_next()).await;
        // Reports configured methods, not the requested one.
        let methods = resp
            .headers()
            .get("access-control-allow-methods")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(methods.contains("GET"));
        assert!(methods.contains("POST"));
    }

    // -----------------------------------------------------------------------
    // Normal (non-preflight) requests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn normal_request_gets_cors_headers() {
        let cors = Cors::new().allow_any_origin();

        let req = make_request(Method::GET, &[("origin", "http://example.com")]);
        let resp = cors.call(req, make_next()).await;

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get("access-control-allow-origin").unwrap(),
            "*"
        );
    }

    #[tokio::test]
    async fn normal_request_no_origin_no_cors_headers() {
        let cors = Cors::new().allow_origin("http://localhost:3000");

        let req = make_request(Method::GET, &[]);
        let resp = cors.call(req, make_next()).await;

        assert_eq!(resp.status(), StatusCode::OK);
        // No Origin header → no CORS headers added.
        assert!(resp.headers().get("access-control-allow-origin").is_none());
    }

    #[tokio::test]
    async fn normal_request_specific_origin() {
        let cors = Cors::new().allow_origin("http://localhost:3000");

        let req = make_request(Method::GET, &[("origin", "http://localhost:3000")]);
        let resp = cors.call(req, make_next()).await;

        assert_eq!(
            resp.headers().get("access-control-allow-origin").unwrap(),
            "http://localhost:3000"
        );
    }

    #[tokio::test]
    async fn normal_request_wrong_origin_rejected() {
        let cors = Cors::new().allow_origin("http://localhost:3000");

        let req = make_request(Method::GET, &[("origin", "http://evil.com")]);
        let resp = cors.call(req, make_next()).await;

        // Request still succeeds (CORS is enforced by the browser), but no
        // Allow-Origin header means the browser will block the response.
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(resp.headers().get("access-control-allow-origin").is_none());
    }

    // -----------------------------------------------------------------------
    // Credentials
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn credentials_echoes_origin() {
        let cors = Cors::new().allow_any_origin().allow_credentials();

        let req = make_request(Method::GET, &[("origin", "http://example.com")]);
        let resp = cors.call(req, make_next()).await;

        // With credentials, must echo origin instead of *.
        assert_eq!(
            resp.headers().get("access-control-allow-origin").unwrap(),
            "http://example.com"
        );
        assert_eq!(
            resp.headers()
                .get("access-control-allow-credentials")
                .unwrap(),
            "true"
        );
    }

    #[tokio::test]
    async fn credentials_vary_header() {
        let cors = Cors::new().allow_any_origin().allow_credentials();

        let req = make_request(Method::GET, &[("origin", "http://example.com")]);
        let resp = cors.call(req, make_next()).await;

        let vary = resp.headers().get("vary").unwrap().to_str().unwrap();
        assert!(vary.contains("origin"), "Vary must include origin with credentials");
    }

    // -----------------------------------------------------------------------
    // Expose headers
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn expose_headers_set() {
        let cors = Cors::new()
            .allow_any_origin()
            .expose_headers(["x-request-id", "x-total-count"]);

        let req = make_request(Method::GET, &[("origin", "http://example.com")]);
        let resp = cors.call(req, make_next()).await;

        let exposed = resp
            .headers()
            .get("access-control-expose-headers")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(exposed.contains("x-request-id"));
        assert!(exposed.contains("x-total-count"));
    }

    // -----------------------------------------------------------------------
    // Multiple origins
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn multiple_allowed_origins() {
        let cors = Cors::new().allow_origins(["http://a.com", "http://b.com"]);

        // Origin A → allowed
        let req = make_request(Method::GET, &[("origin", "http://a.com")]);
        let resp = cors.call(req, make_next()).await;
        assert_eq!(
            resp.headers().get("access-control-allow-origin").unwrap(),
            "http://a.com"
        );

        // Origin B → allowed
        let req = make_request(Method::GET, &[("origin", "http://b.com")]);
        let resp = cors.call(req, make_next()).await;
        assert_eq!(
            resp.headers().get("access-control-allow-origin").unwrap(),
            "http://b.com"
        );

        // Origin C → rejected
        let req = make_request(Method::GET, &[("origin", "http://c.com")]);
        let resp = cors.call(req, make_next()).await;
        assert!(resp.headers().get("access-control-allow-origin").is_none());
    }

    // -----------------------------------------------------------------------
    // Vary header on list-based origins
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn list_origins_add_vary() {
        let cors = Cors::new().allow_origin("http://example.com");

        let req = make_request(Method::GET, &[("origin", "http://example.com")]);
        let resp = cors.call(req, make_next()).await;

        assert!(resp.headers().get("vary").is_some());
    }

    #[tokio::test]
    async fn any_origin_no_vary_without_credentials() {
        let cors = Cors::new().allow_any_origin();

        let req = make_request(Method::GET, &[("origin", "http://example.com")]);
        let resp = cors.call(req, make_next()).await;

        // Wildcard origin without credentials → no Vary needed.
        assert!(resp.headers().get("vary").is_none());
    }

    // -----------------------------------------------------------------------
    // Non-OPTIONS with Origin still gets headers (e.g. POST with Origin)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn post_with_origin_gets_cors_headers() {
        let cors = Cors::new().allow_any_origin();

        let req = make_request(Method::POST, &[("origin", "http://example.com")]);
        let resp = cors.call(req, make_next()).await;

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get("access-control-allow-origin").unwrap(),
            "*"
        );
    }

    // -----------------------------------------------------------------------
    // OPTIONS without Origin is NOT treated as preflight
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn options_without_origin_passes_through() {
        let cors = Cors::new().allow_any_origin();

        let req = make_request(Method::OPTIONS, &[]);
        let resp = cors.call(req, make_next()).await;

        // No Origin → not a CORS preflight → forwarded to next handler.
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
