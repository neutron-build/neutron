//! High-performance HTTP router powered by a compressed radix tree (matchit).
//!
//! Routes are registered with `:param` and `*` wildcard syntax. Internally
//! these are translated to matchit's `{param}` / `{*rest}` format for
//! zero-allocation path matching.
//!
//! ```rust,ignore
//! let router = Router::new()
//!     .get("/", || async { "index" })
//!     .get("/users/:id", get_user)
//!     .nest("/api", api_router);
//! ```

use std::any::TypeId;
use std::collections::HashMap;
#[cfg(feature = "openapi")]
use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use http::Method;
use smallvec::SmallVec;

use crate::handler::{into_boxed, AnyState, BoxedHandler, ErasedHandler, Handler, Request, Response, StateMap};
use crate::middleware::{self, MiddlewareTrait};

// ---------------------------------------------------------------------------
// Method dispatch — array-indexed, no hashing per request
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum MethodKind {
    Get = 0,
    Post = 1,
    Put = 2,
    Delete = 3,
    Patch = 4,
    Head = 5,
    Options = 6,
}

const METHOD_COUNT: usize = 7;

impl MethodKind {
    fn from_http(method: &Method) -> Option<Self> {
        match *method {
            Method::GET => Some(MethodKind::Get),
            Method::POST => Some(MethodKind::Post),
            Method::PUT => Some(MethodKind::Put),
            Method::DELETE => Some(MethodKind::Delete),
            Method::PATCH => Some(MethodKind::Patch),
            Method::HEAD => Some(MethodKind::Head),
            Method::OPTIONS => Some(MethodKind::Options),
            _ => None,
        }
    }
}

struct MethodMap {
    handlers: [Option<BoxedHandler>; METHOD_COUNT],
}

impl Default for MethodMap {
    fn default() -> Self {
        Self {
            handlers: std::array::from_fn(|_| None),
        }
    }
}

impl MethodMap {
    fn insert(&mut self, kind: MethodKind, handler: BoxedHandler) {
        self.handlers[kind as usize] = Some(handler);
    }

    fn get(&self, kind: MethodKind) -> Option<&BoxedHandler> {
        self.handlers[kind as usize].as_ref()
    }

    fn has_any(&self) -> bool {
        self.handlers.iter().any(|h| h.is_some())
    }
}

// ---------------------------------------------------------------------------
// Route resolution result
// ---------------------------------------------------------------------------

/// A successful route match with the handler and extracted path params.
pub struct RouteMatch<'a> {
    pub(crate) handler: &'a BoxedHandler,
    pub(crate) params: SmallVec<[(String, String); 4]>,
}

impl<'a> RouteMatch<'a> {
    /// Call the matched handler with the given request.
    pub async fn call(self, req: Request) -> Response {
        self.handler.call(req).await
    }
}

/// Error during route resolution.
#[derive(Debug)]
pub enum RouteError {
    /// No route matched the path.
    NotFound,
    /// Route matched but the HTTP method is not allowed.
    MethodNotAllowed,
}

// ---------------------------------------------------------------------------
// Handler wrapping for nested middleware
// ---------------------------------------------------------------------------

/// A handler that forwards to a shared `Arc<BoxedHandler>` (used by `.on()`/`.any()`).
struct ForwardingHandler {
    inner: Arc<BoxedHandler>,
}

impl ErasedHandler for ForwardingHandler {
    fn call(&self, req: Request) -> Pin<Box<dyn Future<Output = Response> + Send>> {
        self.inner.call(req)
    }
}

/// A handler that runs a pre-built middleware chain ending with the original handler.
struct ChainedHandler {
    chain: Arc<dyn Fn(Request) -> Pin<Box<dyn Future<Output = Response> + Send>> + Send + Sync>,
}

impl ErasedHandler for ChainedHandler {
    fn call(&self, req: Request) -> Pin<Box<dyn Future<Output = Response> + Send>> {
        (self.chain)(req)
    }
}

/// Wrap a handler with a middleware chain, producing a new `BoxedHandler`.
fn wrap_handler_with_chain(
    handler: BoxedHandler,
    middlewares: &[Arc<dyn MiddlewareTrait>],
) -> BoxedHandler {
    let handler: Arc<BoxedHandler> = Arc::new(handler);
    let final_handler: Arc<
        dyn Fn(Request) -> Pin<Box<dyn Future<Output = Response> + Send>> + Send + Sync,
    > = Arc::new(move |req: Request| handler.call(req));

    let chain = middleware::build_chain(middlewares, final_handler);

    Box::new(ChainedHandler { chain })
}

// ---------------------------------------------------------------------------
// Path syntax conversion
// ---------------------------------------------------------------------------

/// Convert user-facing path syntax (`:param`, `*`) to matchit syntax (`{param}`, `{*rest}`).
#[cfg(feature = "openapi")]
fn method_kind_to_str(kind: MethodKind) -> &'static str {
    match kind {
        MethodKind::Get     => "get",
        MethodKind::Post    => "post",
        MethodKind::Put     => "put",
        MethodKind::Delete  => "delete",
        MethodKind::Patch   => "patch",
        MethodKind::Head    => "head",
        MethodKind::Options => "options",
    }
}

fn to_matchit_path(path: &str) -> String {
    if path.is_empty() || path == "/" {
        return "/".to_string();
    }

    let mut result = String::with_capacity(path.len() + 8);
    for segment in path.split('/') {
        if segment.is_empty() {
            continue;
        }
        result.push('/');
        if segment == "*" {
            result.push_str("{*rest}");
        } else if let Some(name) = segment.strip_prefix(':') {
            result.push('{');
            result.push_str(name);
            result.push('}');
        } else {
            result.push_str(segment);
        }
    }

    if result.is_empty() {
        result.push('/');
    }

    result
}

// ---------------------------------------------------------------------------
// Pending route storage (pre-build)
// ---------------------------------------------------------------------------

/// Before `build()` is called, routes are stored as pending entries.
struct PendingRoute {
    method: MethodKind,
    handler: BoxedHandler,
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

/// High-performance router backed by matchit's compressed radix tree.
///
/// Routes are registered with a builder API, then compiled into a matchit
/// router when the server starts. Method dispatch uses an array-indexed
/// MethodMap for O(1) lookup.
pub struct Router {
    /// Routes pending compilation, keyed by matchit path.
    pending: HashMap<String, Vec<PendingRoute>>,
    /// Compiled matchit router (built lazily on first resolve or on `build()`).
    inner: Option<matchit::Router<MethodMap>>,
    pub(crate) middlewares: Vec<Arc<dyn MiddlewareTrait>>,
    pub(crate) state_map: StateMap,
    pub(crate) fallback: Option<BoxedHandler>,
    /// Sub-routers waiting to be nested (prefix, sub-router).
    pending_nests: Vec<(String, Router)>,
    /// All registered (lowercase_method, original_path) pairs — for OpenAPI discovery.
    #[cfg(feature = "openapi")]
    registered_routes: Vec<(String, String)>,
    /// Explicitly documented [`ApiRoute`]s attached via [`.doc()`].
    #[cfg(feature = "openapi")]
    api_docs: Vec<crate::openapi::ApiRoute>,
}

impl Router {
    pub fn new() -> Self {
        Self {
            pending: HashMap::new(),
            inner: None,
            middlewares: Vec::new(),
            state_map: HashMap::new(),
            fallback: None,
            pending_nests: Vec::new(),
            #[cfg(feature = "openapi")]
            registered_routes: Vec::new(),
            #[cfg(feature = "openapi")]
            api_docs: Vec::new(),
        }
    }

    // -- Route registration helpers -----------------------------------------

    fn route<H, T>(mut self, method: MethodKind, path: &str, handler: H) -> Self
    where
        H: Handler<T>,
        T: 'static,
    {
        let matchit_path = to_matchit_path(path);
        let boxed = into_boxed(handler);
        self.pending
            .entry(matchit_path)
            .or_default()
            .push(PendingRoute { method, handler: boxed });
        #[cfg(feature = "openapi")]
        self.registered_routes
            .push((method_kind_to_str(method).to_string(), path.to_string()));
        self
    }

    pub fn get<H, T>(self, path: &str, handler: H) -> Self
    where
        H: Handler<T>,
        T: 'static,
    {
        self.route(MethodKind::Get, path, handler)
    }

    pub fn post<H, T>(self, path: &str, handler: H) -> Self
    where
        H: Handler<T>,
        T: 'static,
    {
        self.route(MethodKind::Post, path, handler)
    }

    pub fn put<H, T>(self, path: &str, handler: H) -> Self
    where
        H: Handler<T>,
        T: 'static,
    {
        self.route(MethodKind::Put, path, handler)
    }

    pub fn delete<H, T>(self, path: &str, handler: H) -> Self
    where
        H: Handler<T>,
        T: 'static,
    {
        self.route(MethodKind::Delete, path, handler)
    }

    pub fn patch<H, T>(self, path: &str, handler: H) -> Self
    where
        H: Handler<T>,
        T: 'static,
    {
        self.route(MethodKind::Patch, path, handler)
    }

    pub fn head<H, T>(self, path: &str, handler: H) -> Self
    where
        H: Handler<T>,
        T: 'static,
    {
        self.route(MethodKind::Head, path, handler)
    }

    pub fn options<H, T>(self, path: &str, handler: H) -> Self
    where
        H: Handler<T>,
        T: 'static,
    {
        self.route(MethodKind::Options, path, handler)
    }

    /// Register a handler for multiple HTTP methods on the same path.
    ///
    /// ```rust,ignore
    /// Router::new()
    ///     .on("/resource", &[Method::GET, Method::HEAD], handler)
    /// ```
    pub fn on<H, T>(mut self, path: &str, methods: &[Method], handler: H) -> Self
    where
        H: Handler<T>,
        T: 'static,
    {
        let matchit_path = to_matchit_path(path);
        let boxed = Arc::new(into_boxed(handler));
        for method in methods {
            if let Some(kind) = MethodKind::from_http(method) {
                let inner = Arc::clone(&boxed);
                let forwarding: BoxedHandler = Box::new(ForwardingHandler { inner });
                self.pending
                    .entry(matchit_path.clone())
                    .or_default()
                    .push(PendingRoute { method: kind, handler: forwarding });
                #[cfg(feature = "openapi")]
                self.registered_routes
                    .push((method_kind_to_str(kind).to_string(), path.to_string()));
            }
        }
        self
    }

    /// Register a handler that matches any HTTP method.
    ///
    /// ```rust,ignore
    /// Router::new()
    ///     .any("/health", || async { "ok" })
    /// ```
    pub fn any<H, T>(self, path: &str, handler: H) -> Self
    where
        H: Handler<T>,
        T: 'static,
    {
        self.on(
            path,
            &[
                Method::GET,
                Method::POST,
                Method::PUT,
                Method::DELETE,
                Method::PATCH,
                Method::HEAD,
                Method::OPTIONS,
            ],
            handler,
        )
    }

    // -- State --------------------------------------------------------------

    /// Register shared state that handlers can extract via [`State<T>`].
    pub fn state<T: Send + Sync + 'static>(mut self, value: T) -> Self {
        self.state_map
            .insert(TypeId::of::<T>(), Arc::new(value) as Arc<dyn AnyState>);
        self
    }

    // -- Nesting ------------------------------------------------------------

    /// Mount a sub-router under the given prefix.
    ///
    /// All routes in `sub` are merged into this router at `prefix`.
    /// If the sub-router has its own middleware, each of its handlers is
    /// wrapped with that middleware chain (scoped, not applied globally).
    /// State from the sub-router is merged (parent takes precedence on conflict).
    pub fn nest(mut self, prefix: &str, sub: Router) -> Self {
        self.pending_nests.push((prefix.to_string(), sub));
        self
    }

    // -- Middleware ----------------------------------------------------------

    pub fn middleware<M: MiddlewareTrait + 'static>(mut self, mw: M) -> Self {
        self.middlewares.push(Arc::new(mw));
        self
    }

    // -- Fallback -----------------------------------------------------------

    /// Set a custom handler for unmatched routes (404).
    ///
    /// Without a fallback, unmatched routes return a plain-text "Not Found" response.
    ///
    /// ```rust,ignore
    /// Router::new()
    ///     .get("/", index)
    ///     .fallback(|| async { (StatusCode::NOT_FOUND, "custom 404 page") })
    /// ```
    pub fn fallback<H, T>(mut self, handler: H) -> Self
    where
        H: Handler<T>,
        T: 'static,
    {
        self.fallback = Some(into_boxed(handler));
        self
    }

    // -- OpenAPI ------------------------------------------------------------

    #[cfg(feature = "openapi")]
    /// Attach OpenAPI documentation to the previously registered route.
    ///
    /// Call immediately after `.get()`, `.post()`, etc. The given [`ApiRoute`]
    /// is collected and will appear in the spec returned by [`Router::openapi()`].
    ///
    /// ```rust,ignore
    /// use neutron::openapi::{ApiRoute, Schema};
    ///
    /// let router = Router::new()
    ///     .get("/users", list_users)
    ///     .doc(
    ///         ApiRoute::get("/users")
    ///             .summary("List all users")
    ///             .tag("users")
    ///             .response(200, "application/json", Schema::array(Schema::ref_to("User"))),
    ///     );
    /// ```
    pub fn doc(mut self, route: crate::openapi::ApiRoute) -> Self {
        self.api_docs.push(route);
        self
    }

    #[cfg(feature = "openapi")]
    /// Build an [`OpenApi`] spec from all routes registered on this router.
    ///
    /// Routes documented via [`.doc()`] use their full [`ApiRoute`] metadata.
    /// Any undocumented routes get a minimal auto-stub (method + path only).
    /// Nested sub-routers (added with [`.nest()`]) are traversed recursively.
    ///
    /// ```rust,ignore
    /// let spec = Router::new()
    ///     .get("/users", list_users)
    ///     .doc(ApiRoute::get("/users").summary("List users"))
    ///     .post("/users", create_user)      // auto-stub
    ///     .get("/health", health_check)     // auto-stub
    ///     .openapi("My API", "1.0.0");
    /// ```
    pub fn openapi(&self, title: &str, version: &str) -> crate::openapi::OpenApi {
        let mut all_registered: Vec<(String, String)> = Vec::new();
        let mut all_docs: Vec<crate::openapi::ApiRoute>  = Vec::new();
        self.collect_openapi_routes("", &mut all_registered, &mut all_docs);

        let mut spec = crate::openapi::OpenApi::new(title, version);

        // Build a lookup of explicitly documented (method, path) pairs.
        let documented: HashSet<(String, String)> = all_docs.iter()
            .map(|r| (r.method().to_string(), r.path().to_string()))
            .collect();

        for doc in all_docs {
            spec = spec.route(doc);
        }

        // Auto-stubs for routes that have no explicit documentation.
        for (method, path) in &all_registered {
            if !documented.contains(&(method.clone(), path.clone())) {
                spec = spec.route(crate::openapi::ApiRoute::for_method(method, path));
            }
        }

        spec
    }

    #[cfg(feature = "openapi")]
    /// Recursively collect all registered routes and api_docs,
    /// prepending `prefix` to every path.
    fn collect_openapi_routes(
        &self,
        prefix: &str,
        registered: &mut Vec<(String, String)>,
        docs: &mut Vec<crate::openapi::ApiRoute>,
    ) {
        for (method, path) in &self.registered_routes {
            registered.push((method.clone(), format!("{prefix}{path}")));
        }
        for doc in &self.api_docs {
            docs.push(doc.with_prefix(prefix));
        }
        for (sub_prefix, sub_router) in &self.pending_nests {
            let full = format!("{prefix}{sub_prefix}");
            sub_router.collect_openapi_routes(&full, registered, docs);
        }
    }

    // -- Build (compile pending routes into matchit router) -----------------

    /// Compile all pending routes into the matchit router.
    /// Called automatically on first `resolve()`.
    fn build(&mut self) {
        // First, flatten nested sub-routers into self.pending
        self.flatten_nests();

        // Now compile all pending routes into a matchit router
        let mut matchit_router = matchit::Router::new();
        let pending = std::mem::take(&mut self.pending);

        for (path, routes) in pending {
            let mut method_map = MethodMap::default();
            for route in routes {
                method_map.insert(route.method, route.handler);
            }
            if method_map.has_any() {
                matchit_router.insert(path, method_map).unwrap_or_else(|e| {
                    panic!("Failed to insert route: {e}");
                });
            }
        }

        self.inner = Some(matchit_router);
    }

    /// Recursively flatten nested sub-routers into `self.pending`.
    fn flatten_nests(&mut self) {
        let nests = std::mem::take(&mut self.pending_nests);
        for (prefix, mut sub) in nests {
            // Recursively flatten sub-router's own nests first
            if !sub.pending_nests.is_empty() {
                sub.flatten_nests();
            }

            let sub_middlewares = std::mem::take(&mut sub.middlewares);
            let sub_state = std::mem::take(&mut sub.state_map);

            // Merge state from sub-router (parent wins on conflict)
            for (k, v) in sub_state {
                self.state_map.entry(k).or_insert(v);
            }

            // Merge sub-router's pending routes with prefix
            let prefix_matchit = to_matchit_path(&prefix);
            let prefix_str = prefix_matchit.trim_end_matches('/');

            let sub_pending = std::mem::take(&mut sub.pending);
            for (path, routes) in sub_pending {
                let full_path = if path == "/" {
                    if prefix_str.is_empty() { "/".to_string() } else { prefix_str.to_string() }
                } else {
                    format!("{prefix_str}{path}")
                };

                // Apply sub-middleware to each handler (consumes + rebuilds to avoid placeholder)
                let routes = if !sub_middlewares.is_empty() {
                    routes
                        .into_iter()
                        .map(|route| PendingRoute {
                            method: route.method,
                            handler: wrap_handler_with_chain(route.handler, &sub_middlewares),
                        })
                        .collect()
                } else {
                    routes
                };

                self.pending.entry(full_path).or_default().extend(routes);
            }

            // Merge sub fallback
            if self.fallback.is_none() {
                self.fallback = sub.fallback;
            }
        }
    }

    // -- Resolution ---------------------------------------------------------

    /// Resolve a request path and method to a handler + path params.
    pub fn resolve(&self, method: &Method, path: &str) -> Result<RouteMatch<'_>, RouteError> {
        let router = self.inner.as_ref().expect(
            "Router not built — call Neutron::listen() or Router::build() before resolving"
        );

        // Normalize path: ensure it starts with /
        let normalized: String;
        let path = if path.is_empty() {
            "/"
        } else if !path.starts_with('/') {
            normalized = format!("/{path}");
            &normalized
        } else {
            path
        };

        // Try exact match first
        if let Ok(matched) = router.at(path) {
            return Self::resolve_matched(method, matched);
        }

        // Fallback: try stripping trailing slash (/users/ → /users)
        if path.len() > 1 && path.ends_with('/') {
            let trimmed = &path[..path.len() - 1];
            if let Ok(matched) = router.at(trimmed) {
                return Self::resolve_matched(method, matched);
            }
        }

        // Fallback: try adding trailing slash (/users → /users/)
        // Stack buffer avoids heap allocation on every 404.
        if !path.ends_with('/') && path.len() < 256 {
            let mut buf = [0u8; 257];
            buf[..path.len()].copy_from_slice(path.as_bytes());
            buf[path.len()] = b'/';
            if let Ok(with_slash) = std::str::from_utf8(&buf[..path.len() + 1]) {
                if let Ok(matched) = router.at(with_slash) {
                    return Self::resolve_matched(method, matched);
                }
            }
        }

        Err(RouteError::NotFound)
    }

    /// Extract handler and params from a matchit match result.
    fn resolve_matched<'a>(
        method: &Method,
        matched: matchit::Match<'_, '_, &'a MethodMap>,
    ) -> Result<RouteMatch<'a>, RouteError> {
        let method_map = matched.value;
        let kind = MethodKind::from_http(method).ok_or(RouteError::MethodNotAllowed)?;

        // SmallVec<4> avoids any heap allocation for routes with ≤4 params.
        // Static routes (no params) cost nothing — SmallVec::new() is zero-size on stack.
        let params: SmallVec<[(String, String); 4]> = matched
            .params
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();

        match method_map.get(kind) {
            Some(handler) => Ok(RouteMatch { handler, params }),
            None => {
                // HEAD falls back to GET if no explicit HEAD handler
                if kind == MethodKind::Head {
                    if let Some(handler) = method_map.get(MethodKind::Get) {
                        return Ok(RouteMatch { handler, params });
                    }
                }
                Err(RouteError::MethodNotAllowed)
            }
        }
    }

    /// Force-build the internal matchit router.
    ///
    /// Called automatically by `Neutron::listen()` and `TestClient::new()`.
    /// Call this manually if you need to call `resolve()` directly (e.g. benchmarks).
    pub fn ensure_built(&mut self) {
        if self.inner.is_none() {
            self.build();
        }
    }
}

impl Default for Router {
    fn default() -> Self {
        Self::new()
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handler::Request;
    use bytes::Bytes;
    use http::HeaderMap;
    use http_body_util::BodyExt;

    /// Build a minimal request (only used for calling resolved handlers).
    fn test_req() -> Request {
        Request::new(
            Method::GET,
            "/".parse().unwrap(),
            HeaderMap::new(),
            Bytes::new(),
        )
    }

    /// Call a resolved handler and return the response body as a string.
    async fn body_of(handler: &BoxedHandler) -> String {
        let resp = handler.call(test_req()).await;
        let collected = resp.into_body().collect().await.unwrap();
        String::from_utf8(collected.to_bytes().to_vec()).unwrap()
    }

    /// Helper to create a built router from a builder.
    fn build(mut r: Router) -> Router {
        r.ensure_built();
        r
    }

    // -----------------------------------------------------------------------
    // Basic resolution
    // -----------------------------------------------------------------------

    #[test]
    fn root_path() {
        let r = build(Router::new().get("/", || async { "root" }));
        let m = r.resolve(&Method::GET, "/").unwrap();
        assert!(m.params.is_empty());
    }

    #[test]
    fn single_static_segment() {
        let r = build(Router::new().get("/users", || async { "users" }));
        assert!(r.resolve(&Method::GET, "/users").is_ok());
    }

    #[test]
    fn multi_segment_static_path() {
        let r = build(Router::new().get("/api/v1/users", || async { "v1" }));
        let m = r.resolve(&Method::GET, "/api/v1/users").unwrap();
        assert!(m.params.is_empty());
    }

    #[test]
    fn trailing_slash_normalized() {
        let r = build(Router::new().get("/users", || async { "u" }));
        assert!(r.resolve(&Method::GET, "/users").is_ok());
        assert!(r.resolve(&Method::GET, "/users/").is_ok());
    }

    #[test]
    fn trailing_slash_added_when_route_has_it() {
        // Route registered as /users/ — request to /users should still match
        let r = build(Router::new().get("/users/", || async { "u" }));
        assert!(r.resolve(&Method::GET, "/users/").is_ok());
        assert!(r.resolve(&Method::GET, "/users").is_ok());
    }

    #[test]
    fn root_with_and_without_slash() {
        let r = build(Router::new().get("/", || async { "root" }));
        // "/" produces a match
        assert!(r.resolve(&Method::GET, "/").is_ok());
        // "" is normalized to "/" by resolve
        assert!(r.resolve(&Method::GET, "").is_ok());
    }

    // -----------------------------------------------------------------------
    // HTTP methods
    // -----------------------------------------------------------------------

    #[test]
    fn each_method_resolves() {
        let r = build(
            Router::new()
                .get("/g", || async { "g" })
                .post("/p", || async { "p" })
                .put("/u", || async { "u" })
                .delete("/d", || async { "d" })
                .patch("/a", || async { "a" }),
        );

        assert!(r.resolve(&Method::GET, "/g").is_ok());
        assert!(r.resolve(&Method::POST, "/p").is_ok());
        assert!(r.resolve(&Method::PUT, "/u").is_ok());
        assert!(r.resolve(&Method::DELETE, "/d").is_ok());
        assert!(r.resolve(&Method::PATCH, "/a").is_ok());
    }

    #[tokio::test]
    async fn same_path_different_methods_dispatch_correctly() {
        let r = build(
            Router::new()
                .get("/res", || async { "GET" })
                .post("/res", || async { "POST" })
                .put("/res", || async { "PUT" })
                .delete("/res", || async { "DELETE" })
                .patch("/res", || async { "PATCH" }),
        );

        assert_eq!(body_of(r.resolve(&Method::GET, "/res").unwrap().handler).await, "GET");
        assert_eq!(body_of(r.resolve(&Method::POST, "/res").unwrap().handler).await, "POST");
        assert_eq!(body_of(r.resolve(&Method::PUT, "/res").unwrap().handler).await, "PUT");
        assert_eq!(body_of(r.resolve(&Method::DELETE, "/res").unwrap().handler).await, "DELETE");
        assert_eq!(body_of(r.resolve(&Method::PATCH, "/res").unwrap().handler).await, "PATCH");
    }

    #[test]
    fn method_not_allowed_on_existing_path() {
        let r = build(
            Router::new()
                .get("/users", || async { "g" })
                .post("/users", || async { "p" }),
        );

        assert!(matches!(
            r.resolve(&Method::DELETE, "/users"),
            Err(RouteError::MethodNotAllowed)
        ));
    }

    #[tokio::test]
    async fn head_falls_back_to_get() {
        let r = build(Router::new().get("/", || async { "hello" }));
        // HEAD should resolve to the GET handler
        let m = r.resolve(&Method::HEAD, "/").unwrap();
        assert_eq!(body_of(m.handler).await, "hello");
    }

    #[test]
    fn head_returns_method_not_allowed_without_get() {
        let r = build(Router::new().post("/", || async { "p" }));
        assert!(matches!(
            r.resolve(&Method::HEAD, "/"),
            Err(RouteError::MethodNotAllowed)
        ));
    }

    // -----------------------------------------------------------------------
    // Path parameters
    // -----------------------------------------------------------------------

    #[test]
    fn single_param_extracted() {
        let r = build(Router::new().get("/users/:id", || async { "u" }));
        let m = r.resolve(&Method::GET, "/users/42").unwrap();
        assert_eq!(&*m.params, &[("id".into(), "42".into())]);
    }

    #[test]
    fn multiple_params_extracted_in_order() {
        let r = build(Router::new().get("/users/:uid/posts/:pid", || async { "p" }));
        let m = r.resolve(&Method::GET, "/users/5/posts/99").unwrap();
        assert_eq!(
            &*m.params,
            &[("uid".into(), "5".into()), ("pid".into(), "99".into())]
        );
    }

    #[test]
    fn param_preserves_names() {
        let r = build(Router::new().get("/teams/:team_id/members/:member_id", || async { "m" }));
        let m = r.resolve(&Method::GET, "/teams/alpha/members/42").unwrap();
        assert_eq!(m.params[0], ("team_id".into(), "alpha".into()));
        assert_eq!(m.params[1], ("member_id".into(), "42".into()));
    }

    #[test]
    fn param_captures_any_string() {
        let r = build(Router::new().get("/search/:q", || async { "s" }));
        // Percent-encoded values pass through as-is (matchit does not decode)
        let m = r.resolve(&Method::GET, "/search/hello%20world").unwrap();
        assert_eq!(m.params[0].1, "hello%20world");
        // Plain strings pass through unchanged
        let m = r.resolve(&Method::GET, "/search/plain-text").unwrap();
        assert_eq!(m.params[0].1, "plain-text");
    }

    #[test]
    fn param_at_root_level() {
        let r = build(Router::new().get("/:org/repos", || async { "repos" }));
        let m = r.resolve(&Method::GET, "/github/repos").unwrap();
        assert_eq!(&*m.params, &[("org".into(), "github".into())]);
    }

    // -----------------------------------------------------------------------
    // Priority: static > param > wildcard
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn static_wins_over_param() {
        let r = build(
            Router::new()
                .get("/users/me", || async { "STATIC" })
                .get("/users/:id", || async { "PARAM" }),
        );

        let m = r.resolve(&Method::GET, "/users/me").unwrap();
        assert!(m.params.is_empty());
        assert_eq!(body_of(m.handler).await, "STATIC");

        let m = r.resolve(&Method::GET, "/users/123").unwrap();
        assert_eq!(&*m.params, &[("id".into(), "123".into())]);
        assert_eq!(body_of(m.handler).await, "PARAM");
    }

    #[tokio::test]
    async fn static_wins_over_param_regardless_of_registration_order() {
        // Register param first, static second — priority must still hold.
        let r = build(
            Router::new()
                .get("/users/:id", || async { "PARAM" })
                .get("/users/me", || async { "STATIC" }),
        );

        let m = r.resolve(&Method::GET, "/users/me").unwrap();
        assert!(m.params.is_empty());
        assert_eq!(body_of(m.handler).await, "STATIC");

        let m = r.resolve(&Method::GET, "/users/999").unwrap();
        assert_eq!(body_of(m.handler).await, "PARAM");
    }

    #[tokio::test]
    async fn static_wins_over_wildcard() {
        let r = build(
            Router::new()
                .get("/files/readme", || async { "STATIC" })
                .get("/files/*", || async { "WILD" }),
        );

        let m = r.resolve(&Method::GET, "/files/readme").unwrap();
        assert_eq!(body_of(m.handler).await, "STATIC");
    }

    // Note: matchit does not support both :param and *wildcard at the same
    // path prefix. This is the same limitation as Axum/actix. Use either a
    // named param or a wildcard, not both.

    #[tokio::test]
    async fn static_and_param_priorities() {
        let r = build(
            Router::new()
                .get("/x/known", || async { "STATIC" })
                .get("/x/:id", || async { "PARAM" }),
        );

        assert_eq!(body_of(r.resolve(&Method::GET, "/x/known").unwrap().handler).await, "STATIC");
        assert_eq!(body_of(r.resolve(&Method::GET, "/x/other").unwrap().handler).await, "PARAM");
    }

    // -----------------------------------------------------------------------
    // Wildcard catch-all
    // -----------------------------------------------------------------------

    #[test]
    fn wildcard_catches_single_segment() {
        let r = build(Router::new().get("/files/*", || async { "w" }));
        assert!(r.resolve(&Method::GET, "/files/a").is_ok());
    }

    #[test]
    fn wildcard_catches_deep_path() {
        let r = build(Router::new().get("/files/*", || async { "w" }));
        // Wildcard should catch any remaining depth.
        assert!(r.resolve(&Method::GET, "/files/a/b/c").is_ok());
    }

    // -----------------------------------------------------------------------
    // Error cases
    // -----------------------------------------------------------------------

    #[test]
    fn not_found_empty_router() {
        let r = build(Router::new());
        assert!(matches!(
            r.resolve(&Method::GET, "/anything"),
            Err(RouteError::NotFound)
        ));
    }

    #[test]
    fn not_found_root_when_empty() {
        let r = build(Router::new());
        assert!(matches!(
            r.resolve(&Method::GET, "/"),
            Err(RouteError::NotFound)
        ));
    }

    #[test]
    fn not_found_unmatched_path() {
        let r = build(Router::new().get("/users", || async { "u" }));
        assert!(matches!(
            r.resolve(&Method::GET, "/posts"),
            Err(RouteError::NotFound)
        ));
    }

    #[test]
    fn not_found_partial_prefix() {
        // Intermediate nodes without handlers must not match.
        let r = build(Router::new().get("/api/v1/users", || async { "u" }));
        assert!(matches!(
            r.resolve(&Method::GET, "/api/v1"),
            Err(RouteError::NotFound)
        ));
        assert!(matches!(
            r.resolve(&Method::GET, "/api"),
            Err(RouteError::NotFound)
        ));
    }

    #[test]
    fn not_found_deeper_than_registered() {
        let r = build(Router::new().get("/users", || async { "u" }));
        assert!(matches!(
            r.resolve(&Method::GET, "/users/1/posts/2"),
            Err(RouteError::NotFound)
        ));
    }

    #[test]
    fn method_not_allowed_vs_not_found() {
        let r = build(Router::new().get("/items", || async { "i" }));
        // Wrong method on existing path → 405
        assert!(matches!(
            r.resolve(&Method::POST, "/items"),
            Err(RouteError::MethodNotAllowed)
        ));
        // Non-existent path → 404
        assert!(matches!(
            r.resolve(&Method::POST, "/nope"),
            Err(RouteError::NotFound)
        ));
    }

    // -----------------------------------------------------------------------
    // Edge cases & stress
    // -----------------------------------------------------------------------

    #[test]
    fn deeply_nested_static() {
        let r = build(Router::new().get("/a/b/c/d/e/f/g", || async { "deep" }));
        assert!(r.resolve(&Method::GET, "/a/b/c/d/e/f/g").is_ok());
        assert!(matches!(
            r.resolve(&Method::GET, "/a/b/c/d/e/f"),
            Err(RouteError::NotFound)
        ));
    }

    #[tokio::test]
    async fn many_static_siblings() {
        let r = build(
            Router::new()
                .get("/a", || async { "a" })
                .get("/b", || async { "b" })
                .get("/c", || async { "c" })
                .get("/d", || async { "d" })
                .get("/e", || async { "e" }),
        );

        for (path, expected) in [("/a","a"),("/b","b"),("/c","c"),("/d","d"),("/e","e")] {
            let m = r.resolve(&Method::GET, path).unwrap();
            assert_eq!(body_of(m.handler).await, expected, "mismatch for {path}");
        }
    }

    #[tokio::test]
    async fn multiple_static_children_under_same_parent() {
        let r = build(
            Router::new()
                .get("/api/users", || async { "users" })
                .get("/api/posts", || async { "posts" })
                .get("/api/health", || async { "health" }),
        );

        assert_eq!(body_of(r.resolve(&Method::GET, "/api/users").unwrap().handler).await, "users");
        assert_eq!(body_of(r.resolve(&Method::GET, "/api/posts").unwrap().handler).await, "posts");
        assert_eq!(body_of(r.resolve(&Method::GET, "/api/health").unwrap().handler).await, "health");
    }

    #[test]
    fn root_and_deeper_coexist() {
        let r = build(
            Router::new()
                .get("/", || async { "root" })
                .get("/users", || async { "users" }),
        );

        assert!(r.resolve(&Method::GET, "/").is_ok());
        assert!(r.resolve(&Method::GET, "/users").is_ok());
    }

    #[test]
    fn param_and_its_child_both_have_handlers() {
        let r = build(
            Router::new()
                .get("/users/:id", || async { "user" })
                .get("/users/:id/posts", || async { "posts" }),
        );

        let m = r.resolve(&Method::GET, "/users/1").unwrap();
        assert_eq!(&*m.params, &[("id".into(), "1".into())]);

        let m = r.resolve(&Method::GET, "/users/1/posts").unwrap();
        assert_eq!(&*m.params, &[("id".into(), "1".into())]);
    }

    #[test]
    fn param_value_with_dots_and_dashes() {
        let r = build(Router::new().get("/files/:name", || async { "f" }));

        let m = r.resolve(&Method::GET, "/files/my-file.tar.gz").unwrap();
        assert_eq!(m.params[0].1, "my-file.tar.gz");

        let m = r.resolve(&Method::GET, "/files/hello_world-2024").unwrap();
        assert_eq!(m.params[0].1, "hello_world-2024");
    }

    #[test]
    fn shared_param_node_across_methods() {
        // GET and DELETE on same param path share the trie node.
        let r = build(
            Router::new()
                .get("/users/:id", || async { "get" })
                .delete("/users/:id", || async { "del" }),
        );

        let m = r.resolve(&Method::GET, "/users/1").unwrap();
        assert_eq!(&*m.params, &[("id".into(), "1".into())]);

        let m = r.resolve(&Method::DELETE, "/users/1").unwrap();
        assert_eq!(&*m.params, &[("id".into(), "1".into())]);
    }

    // -----------------------------------------------------------------------
    // State
    // -----------------------------------------------------------------------

    #[derive(Clone)]
    struct TestConfig {
        name: String,
    }

    #[test]
    fn state_stored_and_retrievable() {
        let r = Router::new().state(TestConfig {
            name: "test".into(),
        });
        assert!(r.state_map.contains_key(&TypeId::of::<TestConfig>()));
    }

    #[test]
    fn multiple_state_types() {
        let r = Router::new()
            .state(TestConfig {
                name: "app".into(),
            })
            .state(42u64);

        assert!(r.state_map.contains_key(&TypeId::of::<TestConfig>()));
        assert!(r.state_map.contains_key(&TypeId::of::<u64>()));
    }

    #[test]
    fn get_state_step_by_step() {
        use crate::handler::AnyState;

        let mut map = crate::handler::StateMap::new();
        map.insert(
            TypeId::of::<TestConfig>(),
            Arc::new(TestConfig {
                name: "direct".into(),
            }) as Arc<dyn AnyState>,
        );
        assert_eq!(map.len(), 1, "map should have 1 entry");

        let state = Arc::new(map);
        assert_eq!(state.len(), 1, "state should have 1 entry after Arc wrap");

        // Step 1: key lookup
        let found = state.get(&TypeId::of::<TestConfig>());
        assert!(found.is_some(), "key not found in HashMap");

        // Step 2: as_any + downcast (must deref through Arc to avoid blanket impl)
        let arc = found.unwrap();
        let any_ref = (**arc).as_any();
        let result = any_ref.downcast_ref::<TestConfig>();
        assert!(result.is_some(), "downcast_ref::<TestConfig> failed");
        assert_eq!(result.unwrap().name, "direct");

        // Step 3: through Request
        let mut req = test_req();
        req.set_state(state);
        let got = req.get_state::<TestConfig>();
        assert!(got.is_some(), "get_state returned None");
        assert_eq!(got.unwrap().name, "direct");
    }

    #[tokio::test]
    async fn state_injected_into_handler() {
        use crate::extract::State;

        let cfg = TestConfig {
            name: "neutron".into(),
        };
        let r = build(
            Router::new()
                .state(cfg)
                .get("/", |State(c): State<TestConfig>| async move { c.name }),
        );

        let m = r.resolve(&Method::GET, "/").unwrap();

        // Build a request with the state injected (mimics what app.rs does).
        let state_arc = Arc::new(
            r.state_map
                .iter()
                .map(|(k, v)| (*k, Arc::clone(v)))
                .collect::<crate::handler::StateMap>(),
        );
        let mut req = test_req();
        req.set_state(state_arc);

        let resp = m.handler.call(req).await;
        let collected = resp.into_body().collect().await.unwrap();
        let body = String::from_utf8(collected.to_bytes().to_vec()).unwrap();
        assert_eq!(body, "neutron");
    }

    #[tokio::test]
    async fn missing_state_returns_500() {
        use crate::extract::State;
        use http::StatusCode;

        // No state registered — extraction should fail with 500.
        let r = build(Router::new().get("/", |State(_c): State<TestConfig>| async { "nope" }));

        let m = r.resolve(&Method::GET, "/").unwrap();
        let req = test_req(); // no state injected
        let resp = m.handler.call(req).await;
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn arc_state_shared_cheaply() {
        use crate::extract::State;

        let shared = Arc::new(TestConfig {
            name: "shared".into(),
        });

        let r = build(
            Router::new()
                .state(shared)
                .get("/", |State(c): State<Arc<TestConfig>>| async move {
                    c.name.clone()
                }),
        );

        let m = r.resolve(&Method::GET, "/").unwrap();

        let state_arc = Arc::new(
            r.state_map
                .iter()
                .map(|(k, v)| (*k, Arc::clone(v)))
                .collect::<crate::handler::StateMap>(),
        );
        let mut req = test_req();
        req.set_state(state_arc);

        let resp = m.handler.call(req).await;
        let collected = resp.into_body().collect().await.unwrap();
        let body = String::from_utf8(collected.to_bytes().to_vec()).unwrap();
        assert_eq!(body, "shared");
    }

    // -----------------------------------------------------------------------
    // Nested routers
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn nest_basic_routes() {
        let api = Router::new()
            .get("/users", || async { "list_users" })
            .post("/users", || async { "create_user" });

        let r = build(
            Router::new()
                .get("/", || async { "root" })
                .nest("/api", api),
        );

        assert_eq!(body_of(r.resolve(&Method::GET, "/").unwrap().handler).await, "root");
        assert_eq!(body_of(r.resolve(&Method::GET, "/api/users").unwrap().handler).await, "list_users");
        assert_eq!(body_of(r.resolve(&Method::POST, "/api/users").unwrap().handler).await, "create_user");
    }

    #[tokio::test]
    async fn nest_with_params() {
        let sub = Router::new()
            .get("/:id", || async { "get_item" })
            .delete("/:id", || async { "delete_item" });

        let r = build(Router::new().nest("/items", sub));

        let m = r.resolve(&Method::GET, "/items/42").unwrap();
        assert_eq!(&*m.params, &[("id".into(), "42".into())]);
        assert_eq!(body_of(m.handler).await, "get_item");

        let m = r.resolve(&Method::DELETE, "/items/99").unwrap();
        assert_eq!(&*m.params, &[("id".into(), "99".into())]);
        assert_eq!(body_of(m.handler).await, "delete_item");
    }

    #[test]
    fn nest_prefix_with_param() {
        let sub = Router::new().get("/posts", || async { "posts" });

        let r = build(Router::new().nest("/users/:uid", sub));

        let m = r.resolve(&Method::GET, "/users/5/posts").unwrap();
        assert_eq!(&*m.params, &[("uid".into(), "5".into())]);
    }

    #[tokio::test]
    async fn nest_deep_prefix() {
        let sub = Router::new().get("/health", || async { "ok" });

        let r = build(Router::new().nest("/api/v1", sub));

        assert_eq!(
            body_of(r.resolve(&Method::GET, "/api/v1/health").unwrap().handler).await,
            "ok"
        );
    }

    #[tokio::test]
    async fn nest_overlapping_routes() {
        // Parent has /api/status, sub has /status under /api prefix.
        // Sub-router's handler should win (last write wins in merge).
        let sub = Router::new().get("/status", || async { "from_sub" });

        let r = build(
            Router::new()
                .get("/api/status", || async { "from_parent" })
                .nest("/api", sub),
        );

        assert_eq!(
            body_of(r.resolve(&Method::GET, "/api/status").unwrap().handler).await,
            "from_sub"
        );
    }

    #[tokio::test]
    async fn nest_multiple_sub_routers() {
        let users = Router::new()
            .get("/", || async { "list_users" })
            .get("/:id", || async { "get_user" });

        let posts = Router::new()
            .get("/", || async { "list_posts" })
            .post("/", || async { "create_post" });

        let r = build(
            Router::new()
                .nest("/users", users)
                .nest("/posts", posts),
        );

        assert_eq!(body_of(r.resolve(&Method::GET, "/users").unwrap().handler).await, "list_users");
        assert_eq!(body_of(r.resolve(&Method::GET, "/users/5").unwrap().handler).await, "get_user");
        assert_eq!(body_of(r.resolve(&Method::GET, "/posts").unwrap().handler).await, "list_posts");
        assert_eq!(body_of(r.resolve(&Method::POST, "/posts").unwrap().handler).await, "create_post");
    }

    #[tokio::test]
    async fn nest_preserves_parent_routes() {
        let sub = Router::new().get("/items", || async { "items" });

        let r = build(
            Router::new()
                .get("/", || async { "root" })
                .get("/health", || async { "ok" })
                .nest("/api", sub),
        );

        assert_eq!(body_of(r.resolve(&Method::GET, "/").unwrap().handler).await, "root");
        assert_eq!(body_of(r.resolve(&Method::GET, "/health").unwrap().handler).await, "ok");
        assert_eq!(body_of(r.resolve(&Method::GET, "/api/items").unwrap().handler).await, "items");
    }

    #[test]
    fn nest_sub_not_found() {
        let sub = Router::new().get("/items", || async { "items" });
        let r = build(Router::new().nest("/api", sub));

        assert!(matches!(
            r.resolve(&Method::GET, "/api/nope"),
            Err(RouteError::NotFound)
        ));
    }

    #[test]
    fn nest_sub_method_not_allowed() {
        let sub = Router::new().get("/items", || async { "items" });
        let r = build(Router::new().nest("/api", sub));

        assert!(matches!(
            r.resolve(&Method::POST, "/api/items"),
            Err(RouteError::MethodNotAllowed)
        ));
    }

    #[test]
    fn nest_merges_state() {
        let sub = Router::new().state(42u64);

        let r = build(
            Router::new()
                .state(TestConfig { name: "app".into() })
                .nest("/api", sub),
        );

        assert!(r.state_map.contains_key(&TypeId::of::<TestConfig>()));
        assert!(r.state_map.contains_key(&TypeId::of::<u64>()));
    }

    #[test]
    fn nest_parent_state_wins_on_conflict() {
        let sub = Router::new().state(99u64);

        let r = build(Router::new().state(42u64).nest("/api", sub));

        let arc = r.state_map.get(&TypeId::of::<u64>()).unwrap();
        let val = (**arc).as_any().downcast_ref::<u64>().unwrap();
        assert_eq!(*val, 42, "parent state should take precedence");
    }

    #[tokio::test]
    async fn nest_with_sub_middleware() {
        use crate::middleware::Next;

        // Sub-router middleware adds a header
        async fn add_header(req: Request, next: Next) -> Response {
            let mut resp = next.run(req).await;
            resp.headers_mut().insert("x-sub", "yes".parse().unwrap());
            resp
        }

        let sub = Router::new()
            .middleware(add_header)
            .get("/items", || async { "items" });

        let r = build(
            Router::new()
                .get("/", || async { "root" })
                .nest("/api", sub),
        );

        // Sub-router route should have the middleware header
        let state_arc = Arc::new(crate::handler::StateMap::new());
        let mut req = test_req();
        req.set_state(state_arc.clone());
        let m = r.resolve(&Method::GET, "/api/items").unwrap();
        let resp = m.handler.call(req).await;
        assert_eq!(resp.headers().get("x-sub").unwrap(), "yes");

        // Parent route should NOT have the sub middleware header
        let mut req2 = test_req();
        req2.set_state(state_arc);
        let m2 = r.resolve(&Method::GET, "/").unwrap();
        let resp2 = m2.handler.call(req2).await;
        assert!(resp2.headers().get("x-sub").is_none());
    }

    #[tokio::test]
    async fn nest_root_handler_in_sub() {
        // Sub-router has a handler at "/" which should mount at the prefix itself
        let sub = Router::new().get("/", || async { "sub_root" });

        let r = build(Router::new().nest("/api", sub));

        assert_eq!(
            body_of(r.resolve(&Method::GET, "/api").unwrap().handler).await,
            "sub_root"
        );
    }

    #[tokio::test]
    async fn nest_wildcard_in_sub() {
        let sub = Router::new().get("/*", || async { "catch_all" });
        let r = build(Router::new().nest("/files", sub));

        assert!(r.resolve(&Method::GET, "/files/a/b/c").is_ok());
        assert_eq!(
            body_of(r.resolve(&Method::GET, "/files/anything").unwrap().handler).await,
            "catch_all"
        );
    }

    // -----------------------------------------------------------------------
    // Fallback
    // -----------------------------------------------------------------------

    #[test]
    fn fallback_is_stored() {
        let r = Router::new()
            .get("/", || async { "root" })
            .fallback(|| async { "custom 404" });

        assert!(r.fallback.is_some());
    }

    #[tokio::test]
    async fn fallback_handler_is_callable() {
        let r = build(
            Router::new()
                .get("/", || async { "root" })
                .fallback(|| async { "custom 404" }),
        );

        // Route still resolves normally
        assert_eq!(
            body_of(r.resolve(&Method::GET, "/").unwrap().handler).await,
            "root"
        );

        // Unmatched path → resolve returns NotFound → app.rs would call fallback
        assert!(matches!(
            r.resolve(&Method::GET, "/nope"),
            Err(RouteError::NotFound)
        ));

        // Verify fallback handler produces the right response
        let resp = r.fallback.as_ref().unwrap().call(test_req()).await;
        let collected = resp.into_body().collect().await.unwrap();
        let body = String::from_utf8(collected.to_bytes().to_vec()).unwrap();
        assert_eq!(body, "custom 404");
    }

    #[test]
    fn fallback_not_set_by_default() {
        let r = Router::new().get("/", || async { "root" });
        assert!(r.fallback.is_none());
    }
}
