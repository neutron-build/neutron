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
//! method is called once to wrap the inner chain, but Tower layers
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
#[derive(Clone)]
pub struct NeutronService;

#[derive(Clone)]
struct NextContext(
    Arc<dyn Fn(Request) -> Pin<Box<dyn Future<Output = Response> + Send>> + Send + Sync>,
);

impl tower_service::Service<http::Request<Body>> for NeutronService {
    type Response = http::Response<Body>;
    type Error = std::convert::Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Neutron's chain is always ready — no backpressure.
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, mut http_req: http::Request<Body>) -> Self::Future {
        let inner = http_req
            .extensions_mut()
            .remove::<NextContext>()
            .expect("adapter dispatch context")
            .0;
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
#[derive(Clone)]
struct NeutronContext(Arc<std::sync::Mutex<Option<Request>>>);

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
        .extensions_mut()
        .insert(NeutronContext(Arc::new(std::sync::Mutex::new(Some(req)))));

    http_req
}

/// Convert an [`http::Request<Body>`] back into a Neutron [`Request`].
///
/// Collects the body into `Bytes`. For buffered bodies this is zero-copy
/// (just unwraps the inner `Bytes`). For streaming bodies this allocates once.
async fn http_request_to_neutron(http_req: http::Request<Body>) -> Request {
    let (mut parts, body) = http_req.into_parts();

    // Collect body bytes. For Body::Full this is essentially free.
    let body_bytes = body
        .collect()
        .await
        .expect("Body<Infallible> cannot error")
        .to_bytes();

    if let Some(context) = parts.extensions.remove::<NeutronContext>() {
        if let Some(mut req) = context.0.lock().unwrap().take() {
            req.replace_http_parts(parts.method, parts.uri, parts.headers, body_bytes);
            return req;
        }
    }
    Request::new(parts.method, parts.uri, parts.headers, body_bytes)
}

// ---------------------------------------------------------------------------
// TowerLayerAdapter
// ---------------------------------------------------------------------------

/// Adapter that wraps any Tower [`Layer`](tower_layer::Layer) into Neutron's
/// [`MiddlewareTrait`] system.
///
/// The layer is applied once and its service persists to wrap the remaining Neutron
/// middleware chain. Tower layers are designed for this pattern — stateful
/// middleware stores shared state in `Arc`, making `layer()` calls cheap.
///
/// # Type parameters
///
/// - `L`: The Tower layer type. Must produce a service that accepts
///   `http::Request<Body>` and returns `http::Response<Body>`.
pub struct TowerLayerAdapter<L: tower_layer::Layer<NeutronService>> {
    service: Arc<tokio::sync::Mutex<L::Service>>,
}

impl<L: tower_layer::Layer<NeutronService>> TowerLayerAdapter<L> {
    /// Apply the layer once. Its readiness and limiter state persist between requests.
    pub fn new(layer: L) -> Self {
        Self {
            service: Arc::new(tokio::sync::Mutex::new(layer.layer(NeutronService))),
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
        let service = self.service.clone();

        Box::pin(async move {
            let mut tower_svc = service.lock().await;

            // Ensure the service is ready.
            if let Err(e) = futures_util::future::poll_fn(|cx| tower_svc.poll_ready(cx)).await {
                tracing::error!("Tower service poll_ready failed: {e}");
                return http::Response::builder()
                    .status(503)
                    .body(Body::empty())
                    .unwrap();
            }
            let mut req = req;
            if let Err(rejection) = req.buffer_body(crate::app::DEFAULT_MAX_BODY_SIZE).await {
                return rejection;
            }
            let mut http_req = neutron_to_http_request(req);
            http_req
                .extensions_mut()
                .insert(NextContext(next.into_inner()));
            let future = tower_svc.call(http_req);
            drop(tower_svc);
            match future.await {
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

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn owned_context_survives_http_round_trip() {
        let mut req = Request::new(
            http::Method::POST,
            "/original".parse().unwrap(),
            http::HeaderMap::new(),
            Bytes::from_static(b"body"),
        );
        req.set_state(crate::handler::StateMapBuilder::new().insert(42u32).build());
        req.set_extension("authenticated".to_string());
        req.set_remote_addr("127.0.0.1:1234".parse().unwrap());
        assert!(req.buffer_body(100).await.is_ok());
        let mut request = neutron_to_http_request(req);
        *request.uri_mut() = "/rewritten".parse().unwrap();
        let req = http_request_to_neutron(request).await;
        assert_eq!(req.get_state::<u32>(), Some(&42));
        assert_eq!(req.get_extension::<String>().unwrap(), "authenticated");
        assert_eq!(req.remote_addr().unwrap().port(), 1234);
        assert_eq!(req.uri().path(), "/rewritten");
        assert_eq!(req.body(), &Bytes::from_static(b"body"));
    }

    struct CountingLayer(Arc<AtomicUsize>);
    impl tower_layer::Layer<NeutronService> for CountingLayer {
        type Service = NeutronService;
        fn layer(&self, inner: NeutronService) -> Self::Service {
            self.0.fetch_add(1, Ordering::SeqCst);
            inner
        }
    }

    #[tokio::test]
    async fn layer_is_constructed_once_and_body_rejections_stop_dispatch() {
        let layers = Arc::new(AtomicUsize::new(0));
        let calls = Arc::new(AtomicUsize::new(0));
        let adapter = TowerLayerAdapter::new(CountingLayer(layers.clone()));
        for size in [0, 1, crate::app::DEFAULT_MAX_BODY_SIZE + 1] {
            let called = calls.clone();
            let next = Next::new(Arc::new(move |_| {
                called.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { http::Response::new(Body::empty()) })
            }));
            let req = Request::new(
                http::Method::POST,
                "/".parse().unwrap(),
                http::HeaderMap::new(),
                Bytes::from(vec![0; size]),
            );
            let response = adapter.call(req, next).await;
            assert_eq!(
                response.status().as_u16(),
                if size > crate::app::DEFAULT_MAX_BODY_SIZE {
                    413
                } else {
                    200
                }
            );
        }
        assert_eq!(layers.load(Ordering::SeqCst), 1);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    struct NotReady;
    impl tower_layer::Layer<NeutronService> for NotReady {
        type Service = FailedService;
        fn layer(&self, _: NeutronService) -> FailedService {
            FailedService
        }
    }
    struct FailedService;
    impl tower_service::Service<http::Request<Body>> for FailedService {
        type Response = Response;
        type Error = &'static str;
        type Future = std::future::Ready<Result<Response, &'static str>>;
        fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Err("unavailable"))
        }
        fn call(&mut self, _: http::Request<Body>) -> Self::Future {
            panic!("call after failed readiness")
        }
    }
    #[tokio::test]
    async fn readiness_error_does_not_call_service() {
        let adapter = TowerLayerAdapter::new(NotReady);
        let next = Next::new(Arc::new(|_| panic!("downstream must not run")));
        let req = Request::new(
            http::Method::GET,
            "/".parse().unwrap(),
            http::HeaderMap::new(),
            Bytes::new(),
        );
        assert_eq!(adapter.call(req, next).await.status(), 503);
    }
}
