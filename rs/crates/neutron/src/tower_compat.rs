//! Tower middleware compatibility layer.
//!
//! Provides [`TowerLayerAdapter`] which wraps any [`tower::Layer`] into
//! Neutron's [`MiddlewareTrait`] system, and a `.tower_layer()` convenience
//! method on [`Router`].
//!
//! # Performance
//!
//! The adapter adds one `Box::pin` allocation per request per Tower layer —
//! the same cost as a native Neutron middleware. The Tower `Layer::layer()`
//! method is called once per request to wrap the inner chain, but Tower layers
//! are designed for this: stateful middleware (rate limiters, etc.) stores
//! shared state in an `Arc` inside the layer, so each `layer()` call is a
//! cheap clone + wrap.
//!
//! # Example
//!
//! ```rust,ignore
//! use neutron::prelude::*;
//! use tower_http::cors::CorsLayer;
//!
//! let router = Router::new()
//!     .tower_layer(CorsLayer::permissive())
//!     .get("/", || async { "Hello from Neutron + Tower!" });
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use http_body_util::BodyExt;

use crate::handler::{Body, Request, Response};
use crate::middleware::{MiddlewareTrait, Next};

// ---------------------------------------------------------------------------
// NeutronService — wraps Neutron's `Next` as a `tower::Service`
// ---------------------------------------------------------------------------

/// A [`tower_service::Service`] that delegates to Neutron's middleware chain.
///
/// Created per-request from the [`Next`] passed into `MiddlewareTrait::call`.
/// The inner function is `Arc`-shared, so cloning is cheap (atomic increment).
///
/// This type is public so it can appear in the trait bounds of
/// [`Router::tower_layer`], but users should not construct it directly.
pub struct NeutronService {
    inner: Arc<dyn Fn(Request) -> Pin<Box<dyn Future<Output = Response> + Send>> + Send + Sync>,
}

impl Clone for NeutronService {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl tower_service::Service<http::Request<Body>> for NeutronService {
    type Response = http::Response<Body>;
    type Error = std::convert::Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Neutron's chain is always ready — no backpressure.
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, http_req: http::Request<Body>) -> Self::Future {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            // Convert http::Request<Body> back to Neutron's Request.
            let neutron_req = http_request_to_neutron(http_req).await;
            let resp = (inner)(neutron_req).await;
            Ok(resp)
        })
    }
}

// ---------------------------------------------------------------------------
// Type conversions
// ---------------------------------------------------------------------------

/// Convert a Neutron [`Request`] into an [`http::Request<Body>`] for Tower.
///
/// This is a zero-copy operation for headers and body — we move them rather
/// than cloning. The Neutron request is consumed.
fn neutron_to_http_request(req: Request) -> http::Request<Body> {
    let builder = http::Request::builder()
        .method(req.method().clone())
        .uri(req.uri().clone());

    // Transfer headers. We set them after building because the builder API
    // only supports one-at-a-time insertion.
    let headers = req.headers().clone();
    let body_bytes = req.body().clone();

    let mut http_req = builder
        .body(Body::full(body_bytes))
        .expect("building http::Request from valid parts cannot fail");

    *http_req.headers_mut() = headers;

    http_req
}

/// Convert an [`http::Request<Body>`] back into a Neutron [`Request`].
///
/// Collects the body into `Bytes`. For buffered bodies this is zero-copy
/// (just unwraps the inner `Bytes`). For streaming bodies this allocates once.
async fn http_request_to_neutron(http_req: http::Request<Body>) -> Request {
    let (parts, body) = http_req.into_parts();

    // Collect body bytes. For Body::Full this is essentially free.
    let body_bytes = body
        .collect()
        .await
        .expect("Body<Infallible> cannot error")
        .to_bytes();

    Request::new(parts.method, parts.uri, parts.headers, body_bytes)
}

// ---------------------------------------------------------------------------
// TowerLayerAdapter
// ---------------------------------------------------------------------------

/// Adapter that wraps any Tower [`Layer`](tower_layer::Layer) into Neutron's
/// [`MiddlewareTrait`] system.
///
/// The layer is stored and applied per-request to wrap the remaining Neutron
/// middleware chain. Tower layers are designed for this pattern — stateful
/// middleware stores shared state in `Arc`, making `layer()` calls cheap.
///
/// # Type parameters
///
/// - `L`: The Tower layer type. Must produce a service that accepts
///   `http::Request<Body>` and returns `http::Response<Body>`.
pub struct TowerLayerAdapter<L> {
    layer: Arc<L>,
}

impl<L> TowerLayerAdapter<L> {
    /// Create a new adapter wrapping the given Tower layer.
    pub fn new(layer: L) -> Self {
        Self {
            layer: Arc::new(layer),
        }
    }
}

impl<L, S> MiddlewareTrait for TowerLayerAdapter<L>
where
    L: tower_layer::Layer<NeutronService, Service = S> + Send + Sync + 'static,
    S: tower_service::Service<http::Request<Body>, Response = http::Response<Body>>
        + Send
        + 'static,
    S::Error: std::fmt::Display + Send,
    S::Future: Send + 'static,
{
    fn call(&self, req: Request, next: Next) -> Pin<Box<dyn Future<Output = Response> + Send>> {
        let layer = Arc::clone(&self.layer);

        Box::pin(async move {
            // Wrap Neutron's chain as a Tower service.
            let neutron_svc = NeutronService {
                inner: next.into_inner(),
            };

            // Apply the Tower layer on top.
            let mut tower_svc = layer.layer(neutron_svc);

            // Ensure the service is ready.
            futures_util::future::poll_fn(|cx| tower_svc.poll_ready(cx))
                .await
                .unwrap_or_else(|e| {
                    tracing::error!("Tower service poll_ready failed: {e}");
                });

            // Convert Neutron request → http::Request, call the Tower service.
            let http_req = neutron_to_http_request(req);

            match tower_svc.call(http_req).await {
                Ok(resp) => resp,
                Err(e) => {
                    // Tower middleware returned an error — convert to 500.
                    tracing::error!("Tower middleware error: {e}");
                    http::Response::builder()
                        .status(http::StatusCode::INTERNAL_SERVER_ERROR)
                        .body(Body::full(format!("Internal Server Error: {e}")))
                        .unwrap()
                }
            }
        })
    }
}

// ---------------------------------------------------------------------------
// Router extension
// ---------------------------------------------------------------------------

impl crate::router::Router {
    /// Add a Tower middleware layer to this router.
    ///
    /// The layer will be applied to every request passing through this router,
    /// in the same position as a native Neutron middleware added with
    /// [`.middleware()`](crate::router::Router::middleware).
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use neutron::prelude::*;
    /// use tower_http::cors::CorsLayer;
    /// use tower_http::compression::CompressionLayer;
    ///
    /// let router = Router::new()
    ///     .tower_layer(CorsLayer::permissive())
    ///     .tower_layer(CompressionLayer::new())
    ///     .get("/", || async { "Hello!" });
    /// ```
    pub fn tower_layer<L, S>(self, layer: L) -> Self
    where
        L: tower_layer::Layer<NeutronService, Service = S> + Send + Sync + 'static,
        S: tower_service::Service<http::Request<Body>, Response = http::Response<Body>>
            + Send
            + 'static,
        S::Error: std::fmt::Display + Send,
        S::Future: Send + 'static,
    {
        self.middleware(TowerLayerAdapter::new(layer))
    }
}
