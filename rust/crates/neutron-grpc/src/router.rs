//! gRPC router — thin wrapper over neutron's `Router` with gRPC-specific ergonomics.

use neutron::handler::Handler;
use neutron::middleware::MiddlewareTrait;
use neutron::router::Router;

/// gRPC router — registers RPC methods as POST routes on a neutron `Router`.
///
/// gRPC always uses HTTP POST to `/{package}.{Service}/{Method}`.
/// Use `.method(path, handler)` to register each RPC, then call `.into_router()`
/// to integrate with the main application router.
///
/// ```rust,ignore
/// let grpc = GrpcRouter::new()
///     .method("/helloworld.Greeter/SayHello", say_hello)
///     .method("/helloworld.Greeter/SayBye",   say_bye)
///     .state(AppState { db });
///
/// let app = Router::new()
///     .get("/healthz", health)
///     .nest("/", grpc.into_router());
/// ```
pub struct GrpcRouter(Router);

impl GrpcRouter {
    pub fn new() -> Self {
        Self(Router::new())
    }

    /// Register a handler for a gRPC RPC method.
    ///
    /// `path` follows the gRPC convention: `/{package}.{Service}/{Method}`
    /// (e.g., `/helloworld.Greeter/SayHello`). The route is always a POST.
    pub fn method<H, T>(self, path: &str, handler: H) -> Self
    where
        H: Handler<T>,
        T: 'static,
    {
        Self(self.0.post(path, handler))
    }

    /// Register shared state accessible via `State<T>` in RPC handlers.
    pub fn state<T: Send + Sync + 'static>(self, value: T) -> Self {
        Self(self.0.state(value))
    }

    /// Add middleware applied to all routes in this gRPC router.
    pub fn middleware<M: MiddlewareTrait + 'static>(self, mw: M) -> Self {
        Self(self.0.middleware(mw))
    }

    /// Nest another `GrpcRouter` under a path prefix.
    pub fn nest(self, prefix: &str, other: GrpcRouter) -> Self {
        Self(self.0.nest(prefix, other.0))
    }

    /// Convert into a plain neutron `Router` for use with `Neutron::new()`.
    pub fn into_router(self) -> Router {
        self.0
    }
}

impl Default for GrpcRouter {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{GrpcRequest, GrpcResponse};
    use http::Method;

    async fn echo(GrpcRequest(payload): GrpcRequest) -> GrpcResponse {
        GrpcResponse::ok(payload)
    }

    async fn ping() -> GrpcResponse {
        GrpcResponse::ok(b"pong".as_slice())
    }

    #[test]
    fn method_registers_post_route() {
        let mut router = GrpcRouter::new()
            .method("/test.Svc/Echo", echo)
            .into_router();

        router.ensure_built();

        // Must resolve as POST
        assert!(router.resolve(&Method::POST, "/test.Svc/Echo").is_ok());
        // GET should not be registered
        assert!(router.resolve(&Method::GET, "/test.Svc/Echo").is_err());
    }

    #[test]
    fn multiple_methods_on_same_service() {
        let mut router = GrpcRouter::new()
            .method("/pkg.Svc/MethodA", echo)
            .method("/pkg.Svc/MethodB", ping)
            .into_router();

        router.ensure_built();

        assert!(router.resolve(&Method::POST, "/pkg.Svc/MethodA").is_ok());
        assert!(router.resolve(&Method::POST, "/pkg.Svc/MethodB").is_ok());
        assert!(router.resolve(&Method::POST, "/pkg.Svc/MethodC").is_err());
    }

    #[test]
    fn state_can_be_registered() {
        // Verify .state() builds without panicking — state internals are tested in neutron
        let _router = GrpcRouter::new()
            .method("/s/M", ping)
            .state(42u64)
            .into_router();
    }
}
