//! Security headers middleware (Helmet).
//!
//! Sets common security headers on all responses. Provides sensible defaults
//! that harden any application against common web vulnerabilities.
//!
//! # Example
//!
//! ```rust,ignore
//! use neutron::prelude::*;
//!
//! // Use all defaults — one-liner hardening
//! let router = Router::new()
//!     .middleware(Helmet::default())
//!     .get("/", handler);
//!
//! // Customize specific headers
//! let router = Router::new()
//!     .middleware(
//!         Helmet::new()
//!             .content_security_policy("default-src 'self'")
//!             .hsts_max_age(31536000)
//!             .referrer_policy("strict-origin-when-cross-origin")
//!     )
//!     .get("/", handler);
//! ```
//!
//! ## Default Headers
//!
//! | Header | Default Value |
//! |--------|--------------|
//! | `X-Content-Type-Options` | `nosniff` |
//! | `X-Frame-Options` | `DENY` |
//! | `X-XSS-Protection` | `0` |
//! | `Strict-Transport-Security` | `max-age=15552000; includeSubDomains` |
//! | `Content-Security-Policy` | *(not set by default)* |
//! | `Referrer-Policy` | `no-referrer` |
//! | `Permissions-Policy` | *(not set by default)* |
//! | `X-DNS-Prefetch-Control` | `off` |
//! | `X-Download-Options` | `noopen` |
//! | `X-Permitted-Cross-Domain-Policies` | `none` |
//! | `Cross-Origin-Opener-Policy` | `same-origin` |
//! | `Cross-Origin-Resource-Policy` | `same-origin` |
//! | `Cross-Origin-Embedder-Policy` | `require-corp` |

use std::future::Future;
use std::pin::Pin;

use crate::handler::{Request, Response};
use crate::middleware::{MiddlewareTrait, Next};

/// Security headers middleware.
///
/// Applies common security headers to all responses. Use [`Helmet::new()`]
/// for full control, or [`Helmet::default()`] for sensible defaults.
pub struct Helmet {
    x_content_type_options: Option<String>,
    x_frame_options: Option<String>,
    x_xss_protection: Option<String>,
    strict_transport_security: Option<String>,
    content_security_policy: Option<String>,
    referrer_policy: Option<String>,
    permissions_policy: Option<String>,
    x_dns_prefetch_control: Option<String>,
    x_download_options: Option<String>,
    x_permitted_cross_domain_policies: Option<String>,
    cross_origin_opener_policy: Option<String>,
    cross_origin_resource_policy: Option<String>,
    cross_origin_embedder_policy: Option<String>,
}

impl Helmet {
    /// Create a Helmet with all default security headers enabled.
    pub fn new() -> Self {
        Self::default()
    }

    /// Disable all headers, allowing you to enable only what you need.
    pub fn empty() -> Self {
        Self {
            x_content_type_options: None,
            x_frame_options: None,
            x_xss_protection: None,
            strict_transport_security: None,
            content_security_policy: None,
            referrer_policy: None,
            permissions_policy: None,
            x_dns_prefetch_control: None,
            x_download_options: None,
            x_permitted_cross_domain_policies: None,
            cross_origin_opener_policy: None,
            cross_origin_resource_policy: None,
            cross_origin_embedder_policy: None,
        }
    }

    /// Set `X-Content-Type-Options` (default: `nosniff`).
    ///
    /// Prevents browsers from MIME-sniffing the content type.
    pub fn x_content_type_options(mut self, value: impl Into<String>) -> Self {
        self.x_content_type_options = Some(value.into());
        self
    }

    /// Set `X-Frame-Options` (default: `DENY`).
    ///
    /// Controls whether the page can be embedded in iframes.
    /// Use `"DENY"`, `"SAMEORIGIN"`, or `None` to disable.
    pub fn x_frame_options(mut self, value: impl Into<String>) -> Self {
        self.x_frame_options = Some(value.into());
        self
    }

    /// Set `X-XSS-Protection` (default: `0`).
    ///
    /// Set to `"0"` to disable the legacy XSS filter (recommended — the
    /// filter itself can introduce vulnerabilities).
    pub fn x_xss_protection(mut self, value: impl Into<String>) -> Self {
        self.x_xss_protection = Some(value.into());
        self
    }

    /// Set `Strict-Transport-Security` max-age in seconds
    /// (default: `15552000` = 180 days).
    ///
    /// Pass `0` to disable HSTS.
    pub fn hsts_max_age(mut self, seconds: u64) -> Self {
        if seconds == 0 {
            self.strict_transport_security = None;
        } else {
            self.strict_transport_security =
                Some(format!("max-age={seconds}; includeSubDomains"));
        }
        self
    }

    /// Set a custom `Strict-Transport-Security` value.
    pub fn hsts(mut self, value: impl Into<String>) -> Self {
        self.strict_transport_security = Some(value.into());
        self
    }

    /// Set `Content-Security-Policy` (not set by default).
    ///
    /// ```rust,ignore
    /// Helmet::new().content_security_policy("default-src 'self'; script-src 'self'")
    /// ```
    pub fn content_security_policy(mut self, value: impl Into<String>) -> Self {
        self.content_security_policy = Some(value.into());
        self
    }

    /// Set `Referrer-Policy` (default: `no-referrer`).
    pub fn referrer_policy(mut self, value: impl Into<String>) -> Self {
        self.referrer_policy = Some(value.into());
        self
    }

    /// Set `Permissions-Policy` (not set by default).
    ///
    /// ```rust,ignore
    /// Helmet::new().permissions_policy("geolocation=(), camera=()")
    /// ```
    pub fn permissions_policy(mut self, value: impl Into<String>) -> Self {
        self.permissions_policy = Some(value.into());
        self
    }

    /// Set `X-DNS-Prefetch-Control` (default: `off`).
    pub fn x_dns_prefetch_control(mut self, value: impl Into<String>) -> Self {
        self.x_dns_prefetch_control = Some(value.into());
        self
    }

    /// Set `Cross-Origin-Opener-Policy` (default: `same-origin`).
    pub fn cross_origin_opener_policy(mut self, value: impl Into<String>) -> Self {
        self.cross_origin_opener_policy = Some(value.into());
        self
    }

    /// Set `Cross-Origin-Resource-Policy` (default: `same-origin`).
    pub fn cross_origin_resource_policy(mut self, value: impl Into<String>) -> Self {
        self.cross_origin_resource_policy = Some(value.into());
        self
    }

    /// Set `Cross-Origin-Embedder-Policy` (default: `require-corp`).
    pub fn cross_origin_embedder_policy(mut self, value: impl Into<String>) -> Self {
        self.cross_origin_embedder_policy = Some(value.into());
        self
    }

    /// Disable a specific header by setting it to `None`.
    pub fn disable_x_content_type_options(mut self) -> Self {
        self.x_content_type_options = None;
        self
    }

    /// Disable `X-Frame-Options`.
    pub fn disable_x_frame_options(mut self) -> Self {
        self.x_frame_options = None;
        self
    }

    /// Disable `Strict-Transport-Security`.
    pub fn disable_hsts(mut self) -> Self {
        self.strict_transport_security = None;
        self
    }

    /// Disable `Cross-Origin-Embedder-Policy`.
    pub fn disable_cross_origin_embedder_policy(mut self) -> Self {
        self.cross_origin_embedder_policy = None;
        self
    }
}

impl Default for Helmet {
    fn default() -> Self {
        Self {
            x_content_type_options: Some("nosniff".to_string()),
            x_frame_options: Some("DENY".to_string()),
            x_xss_protection: Some("0".to_string()),
            strict_transport_security: Some(
                "max-age=15552000; includeSubDomains".to_string(),
            ),
            content_security_policy: None,
            referrer_policy: Some("no-referrer".to_string()),
            permissions_policy: None,
            x_dns_prefetch_control: Some("off".to_string()),
            x_download_options: Some("noopen".to_string()),
            x_permitted_cross_domain_policies: Some("none".to_string()),
            cross_origin_opener_policy: Some("same-origin".to_string()),
            cross_origin_resource_policy: Some("same-origin".to_string()),
            cross_origin_embedder_policy: Some("require-corp".to_string()),
        }
    }
}

impl MiddlewareTrait for Helmet {
    fn call(&self, req: Request, next: Next) -> Pin<Box<dyn Future<Output = Response> + Send>> {
        // Pre-build header list to avoid cloning strings in the async block
        let headers: Vec<(&'static str, String)> = [
            ("x-content-type-options", &self.x_content_type_options),
            ("x-frame-options", &self.x_frame_options),
            ("x-xss-protection", &self.x_xss_protection),
            (
                "strict-transport-security",
                &self.strict_transport_security,
            ),
            ("content-security-policy", &self.content_security_policy),
            ("referrer-policy", &self.referrer_policy),
            ("permissions-policy", &self.permissions_policy),
            ("x-dns-prefetch-control", &self.x_dns_prefetch_control),
            ("x-download-options", &self.x_download_options),
            (
                "x-permitted-cross-domain-policies",
                &self.x_permitted_cross_domain_policies,
            ),
            (
                "cross-origin-opener-policy",
                &self.cross_origin_opener_policy,
            ),
            (
                "cross-origin-resource-policy",
                &self.cross_origin_resource_policy,
            ),
            (
                "cross-origin-embedder-policy",
                &self.cross_origin_embedder_policy,
            ),
        ]
        .into_iter()
        .filter_map(|(name, opt)| opt.as_ref().map(|v| (name, v.clone())))
        .collect();

        Box::pin(async move {
            let mut resp = next.run(req).await;
            for (name, value) in headers {
                resp.headers_mut()
                    .insert(name, value.parse().unwrap());
            }
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
    use crate::router::Router;
    use crate::testing::TestClient;
    use http::StatusCode;

    #[tokio::test]
    async fn default_headers() {
        let client = TestClient::new(
            Router::new()
                .middleware(Helmet::default())
                .get("/", || async { "ok" }),
        );

        let resp = client.get("/").send().await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.header("x-content-type-options").unwrap(), "nosniff");
        assert_eq!(resp.header("x-frame-options").unwrap(), "DENY");
        assert_eq!(resp.header("x-xss-protection").unwrap(), "0");
        assert_eq!(
            resp.header("strict-transport-security").unwrap(),
            "max-age=15552000; includeSubDomains"
        );
        assert_eq!(resp.header("referrer-policy").unwrap(), "no-referrer");
        assert_eq!(resp.header("x-dns-prefetch-control").unwrap(), "off");
        assert_eq!(resp.header("x-download-options").unwrap(), "noopen");
        assert_eq!(
            resp.header("x-permitted-cross-domain-policies").unwrap(),
            "none"
        );
        assert_eq!(
            resp.header("cross-origin-opener-policy").unwrap(),
            "same-origin"
        );
        assert_eq!(
            resp.header("cross-origin-resource-policy").unwrap(),
            "same-origin"
        );
        assert_eq!(
            resp.header("cross-origin-embedder-policy").unwrap(),
            "require-corp"
        );
        // CSP and Permissions-Policy are not set by default
        assert!(resp.header("content-security-policy").is_none());
        assert!(resp.header("permissions-policy").is_none());
    }

    #[tokio::test]
    async fn custom_csp() {
        let client = TestClient::new(
            Router::new()
                .middleware(Helmet::new().content_security_policy("default-src 'self'"))
                .get("/", || async { "ok" }),
        );

        let resp = client.get("/").send().await;
        assert_eq!(
            resp.header("content-security-policy").unwrap(),
            "default-src 'self'"
        );
    }

    #[tokio::test]
    async fn custom_permissions_policy() {
        let client = TestClient::new(
            Router::new()
                .middleware(Helmet::new().permissions_policy("geolocation=(), camera=()"))
                .get("/", || async { "ok" }),
        );

        let resp = client.get("/").send().await;
        assert_eq!(
            resp.header("permissions-policy").unwrap(),
            "geolocation=(), camera=()"
        );
    }

    #[tokio::test]
    async fn custom_hsts_max_age() {
        let client = TestClient::new(
            Router::new()
                .middleware(Helmet::new().hsts_max_age(31536000))
                .get("/", || async { "ok" }),
        );

        let resp = client.get("/").send().await;
        assert_eq!(
            resp.header("strict-transport-security").unwrap(),
            "max-age=31536000; includeSubDomains"
        );
    }

    #[tokio::test]
    async fn disable_hsts() {
        let client = TestClient::new(
            Router::new()
                .middleware(Helmet::new().hsts_max_age(0))
                .get("/", || async { "ok" }),
        );

        let resp = client.get("/").send().await;
        assert!(resp.header("strict-transport-security").is_none());
    }

    #[tokio::test]
    async fn custom_x_frame_options() {
        let client = TestClient::new(
            Router::new()
                .middleware(Helmet::new().x_frame_options("SAMEORIGIN"))
                .get("/", || async { "ok" }),
        );

        let resp = client.get("/").send().await;
        assert_eq!(resp.header("x-frame-options").unwrap(), "SAMEORIGIN");
    }

    #[tokio::test]
    async fn disable_x_frame_options() {
        let client = TestClient::new(
            Router::new()
                .middleware(Helmet::new().disable_x_frame_options())
                .get("/", || async { "ok" }),
        );

        let resp = client.get("/").send().await;
        assert!(resp.header("x-frame-options").is_none());
    }

    #[tokio::test]
    async fn custom_referrer_policy() {
        let client = TestClient::new(
            Router::new()
                .middleware(
                    Helmet::new().referrer_policy("strict-origin-when-cross-origin"),
                )
                .get("/", || async { "ok" }),
        );

        let resp = client.get("/").send().await;
        assert_eq!(
            resp.header("referrer-policy").unwrap(),
            "strict-origin-when-cross-origin"
        );
    }

    #[tokio::test]
    async fn empty_helmet_sets_no_headers() {
        let client = TestClient::new(
            Router::new()
                .middleware(Helmet::empty())
                .get("/", || async { "ok" }),
        );

        let resp = client.get("/").send().await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(resp.header("x-content-type-options").is_none());
        assert!(resp.header("x-frame-options").is_none());
        assert!(resp.header("strict-transport-security").is_none());
        assert!(resp.header("referrer-policy").is_none());
    }

    #[tokio::test]
    async fn empty_with_selective_enable() {
        let client = TestClient::new(
            Router::new()
                .middleware(
                    Helmet::empty()
                        .x_content_type_options("nosniff")
                        .referrer_policy("origin"),
                )
                .get("/", || async { "ok" }),
        );

        let resp = client.get("/").send().await;
        assert_eq!(resp.header("x-content-type-options").unwrap(), "nosniff");
        assert_eq!(resp.header("referrer-policy").unwrap(), "origin");
        // Others should not be set
        assert!(resp.header("x-frame-options").is_none());
        assert!(resp.header("strict-transport-security").is_none());
    }

    #[tokio::test]
    async fn cross_origin_policies() {
        let client = TestClient::new(
            Router::new()
                .middleware(
                    Helmet::new()
                        .cross_origin_opener_policy("same-origin-allow-popups")
                        .cross_origin_resource_policy("cross-origin")
                        .cross_origin_embedder_policy("unsafe-none"),
                )
                .get("/", || async { "ok" }),
        );

        let resp = client.get("/").send().await;
        assert_eq!(
            resp.header("cross-origin-opener-policy").unwrap(),
            "same-origin-allow-popups"
        );
        assert_eq!(
            resp.header("cross-origin-resource-policy").unwrap(),
            "cross-origin"
        );
        assert_eq!(
            resp.header("cross-origin-embedder-policy").unwrap(),
            "unsafe-none"
        );
    }

    #[tokio::test]
    async fn does_not_affect_response_body() {
        let client = TestClient::new(
            Router::new()
                .middleware(Helmet::default())
                .get("/", || async { "hello" }),
        );

        let resp = client.get("/").send().await;
        assert_eq!(resp.text().await, "hello");
    }
}
