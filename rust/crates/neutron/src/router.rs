//! High-performance HTTP router powered by a compressed radix tree (matchit).
//!
//! Routes are registered with `:param` and `*` wildcard syntax. Internally
//! these are translated to matchit's `{param}` / `{*rest}` format for
//! zero-allocation path matching.
//!
//! ```rust,ignore
//! let router = Router::<()>::new()
//!     .get("/", || async { "index" })
//!     .get("/users/:id", get_user)
//!     .nest("/api", api_router);
//! ```

use std::any::TypeId;
use std::collections::HashMap;
use std::marker::PhantomData;
#[cfg(feature = "openapi")]
use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use http::Method;
use http_body_util::BodyExt;
use smallvec::SmallVec;

use crate::handler::{
    into_boxed, AnyState, Body, BoxedHandler, ErasedHandler, Handler, IntoResponse, ReqBody,
    Request, Response, StateMap,
};
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

    fn to_http(self) -> Method {
        match self {
            MethodKind::Get => Method::GET,
            MethodKind::Post => Method::POST,
            MethodKind::Put => Method::PUT,
            MethodKind::Delete => Method::DELETE,
            MethodKind::Patch => Method::PATCH,
            MethodKind::Head => Method::HEAD,
            MethodKind::Options => Method::OPTIONS,
        }
    }

    /// All method kinds in declaration order.
    const ALL: [MethodKind; METHOD_COUNT] = [
        MethodKind::Get,
        MethodKind::Post,
        MethodKind::Put,
        MethodKind::Delete,
        MethodKind::Patch,
        MethodKind::Head,
        MethodKind::Options,
    ];
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

    /// The set of methods allowed on this path, for the 405 `Allow` header.
    /// Includes every method with a handler, plus implied `HEAD` (when `GET`
    /// is present) and `OPTIONS`.
    fn allowed(&self) -> SmallVec<[Method; METHOD_COUNT]> {
        let mut out: SmallVec<[Method; METHOD_COUNT]> = SmallVec::new();
        for kind in MethodKind::ALL {
            if self.get(kind).is_some() {
                out.push(kind.to_http());
            }
        }
        // HEAD is implicitly served by a GET handler.
        if self.get(MethodKind::Get).is_some() && self.get(MethodKind::Head).is_none() {
            out.push(Method::HEAD);
        }
        // OPTIONS is always answerable for a matched path.
        if self.get(MethodKind::Options).is_none() {
            out.push(Method::OPTIONS);
        }
        out
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
    /// Route matched but the HTTP method is not allowed. Carries the set of
    /// methods that ARE allowed for this path, for the RFC 7231 `Allow` header.
    MethodNotAllowed {
        /// Methods with a handler on this path (plus implied `HEAD`/`OPTIONS`).
        allow: SmallVec<[Method; METHOD_COUNT]>,
    },
    /// `resolve()` was called before `build()`/`ensure_built()`. Production
    /// always force-builds before serving, so this is unreachable on the
    /// request path; returning it (instead of panicking) keeps direct/benchmark
    /// callers unwind-free.
    NotBuilt,
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
pub struct Router<S = ()> {
    /// Routes pending compilation, keyed by matchit path.
    pending: HashMap<String, Vec<PendingRoute>>,
    /// Compiled matchit router (built lazily on first resolve or on `build()`).
    inner: Option<matchit::Router<MethodMap>>,
    pub(crate) middlewares: Vec<Arc<dyn MiddlewareTrait>>,
    pub(crate) state_map: StateMap,
    pub(crate) fallback: Option<BoxedHandler>,
    /// Sub-routers waiting to be nested (prefix, sub-router). Nests carry the
    /// same `S` until the state is bound with [`with_state`](Router::with_state).
    pending_nests: Vec<(String, Router<S>)>,
    /// All registered (lowercase_method, original_path) pairs — for OpenAPI discovery.
    #[cfg(feature = "openapi")]
    registered_routes: Vec<(String, String)>,
    /// Explicitly documented [`ApiRoute`]s attached via [`.doc()`].
    #[cfg(feature = "openapi")]
    api_docs: Vec<crate::openapi::ApiRoute>,
    /// Compile-time tag for the application state this router still expects.
    /// Erased to `()` once [`with_state`](Router::with_state) binds the state.
    _state: PhantomData<fn() -> S>,
}

impl<S> Router<S> {
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
            _state: PhantomData,
        }
    }

    /// Move every field into a `Router<S2>` — the data is `S`-independent; only
    /// the compile-time `PhantomData` tag changes. Used by `with_state` to erase
    /// the state obligation to `()`, and to re-tag nests during `merge`.
    fn retag<S2>(self) -> Router<S2> {
        Router {
            pending: self.pending,
            inner: self.inner,
            middlewares: self.middlewares,
            state_map: self.state_map,
            fallback: self.fallback,
            // Sub-routers carried the same `S`; re-tag each to `S2`.
            pending_nests: self
                .pending_nests
                .into_iter()
                .map(|(p, sub)| (p, sub.retag::<S2>()))
                .collect(),
            #[cfg(feature = "openapi")]
            registered_routes: self.registered_routes,
            #[cfg(feature = "openapi")]
            api_docs: self.api_docs,
            _state: PhantomData,
        }
    }

    /// Bind the application state, erasing the `S` obligation to `()`.
    ///
    /// After this the router is `Router<()>` and `State<T>` extraction reads `T`
    /// from the dynamic state map exactly as before — the state is materialized
    /// into the map here so the runtime lookup always succeeds.
    pub fn with_state(self, state: S) -> Router<()>
    where
        S: Send + Sync + 'static,
    {
        let mut r: Router<()> = self.retag();
        r.state_map
            .insert(TypeId::of::<S>(), Arc::new(state) as Arc<dyn AnyState>);
        r
    }

    /// Merge another router with the **same** state type `S` into this one.
    /// Routes, nests, middleware, state, and (when enabled) OpenAPI metadata are
    /// folded in; on a state-key conflict, `self` wins.
    pub fn merge(mut self, other: Router<S>) -> Self {
        for (path, routes) in other.pending {
            self.pending.entry(path).or_default().extend(routes);
        }
        self.pending_nests.extend(other.pending_nests);
        self.middlewares.extend(other.middlewares);
        for (k, v) in other.state_map {
            self.state_map.entry(k).or_insert(v);
        }
        if self.fallback.is_none() {
            self.fallback = other.fallback;
        }
        #[cfg(feature = "openapi")]
        {
            self.registered_routes.extend(other.registered_routes);
            self.api_docs.extend(other.api_docs);
        }
        self
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
    /// Router::<()>::new()
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
    /// Router::<()>::new()
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
    pub fn nest(mut self, prefix: &str, sub: Router<S>) -> Self {
        self.pending_nests.push((prefix.to_string(), sub));
        self
    }

    /// Register a pre-boxed, type-erased handler for every HTTP method at `path`.
    fn route_all_boxed(mut self, path: &str, boxed: Arc<BoxedHandler>) -> Self {
        let matchit_path = to_matchit_path(path);
        for kind in MethodKind::ALL {
            let forwarding: BoxedHandler =
                Box::new(ForwardingHandler { inner: Arc::clone(&boxed) });
            self.pending
                .entry(matchit_path.clone())
                .or_default()
                .push(PendingRoute { method: kind, handler: forwarding });
            #[cfg(feature = "openapi")]
            self.registered_routes
                .push((method_kind_to_str(kind).to_string(), path.to_string()));
        }
        self
    }

    /// Mount an arbitrary `tower::Service` under a path prefix.
    ///
    /// Unlike [`nest`](Self::nest) (which merges another [`Router`]'s route
    /// table into this one), `nest_service` mounts an opaque
    /// `tower::Service<http::Request<Body>, Response = Response, Error = Infallible>`
    /// — another [`RouterService`], a `tower-http` `ServeDir`, a gRPC handler, etc.
    /// All requests under `prefix` (and `prefix` itself) are forwarded to the
    /// service. The request body is buffered before hand-off.
    ///
    /// ```rust,ignore
    /// let assets = ServeDir::new("./public"); // any tower::Service
    /// let api = Router::<()>::new().get("/users", list).into_service();
    /// let app = Router::<()>::new()
    ///     .nest_service("/assets", assets)
    ///     .nest_service("/api", api);
    /// ```
    pub fn nest_service<Svc>(self, prefix: &str, service: Svc) -> Self
    where
        Svc: tower_service::Service<
                http::Request<Body>,
                Response = Response,
                Error = std::convert::Infallible,
            > + Clone
            + Send
            + Sync
            + 'static,
        Svc::Future: Send,
    {
        let prefix = prefix.trim_end_matches('/');
        let boxed: Arc<BoxedHandler> = Arc::new(Box::new(ServiceHandler {
            service,
            body_limit: crate::app::DEFAULT_MAX_BODY_SIZE,
            prefix: prefix.to_string(),
        }));
        // Mount at both the bare prefix and the catch-all below it.
        let wildcard = if prefix.is_empty() {
            "/*".to_string()
        } else {
            format!("{prefix}/*")
        };
        let bare = if prefix.is_empty() { "/" } else { prefix };
        self.route_all_boxed(bare, Arc::clone(&boxed))
            .route_all_boxed(&wildcard, boxed)
    }

    // -- Middleware ----------------------------------------------------------

    pub fn middleware<M: MiddlewareTrait + 'static>(mut self, mw: M) -> Self {
        self.middlewares.push(Arc::new(mw));
        self
    }

    /// Install the standard Neutron middleware stack in the contract-mandated
    /// order, matching the cross-language `FRAMEWORK_CONTRACT.md` spec:
    ///
    /// ```text
    /// RequestID → Logging → Recovery → CORS → Compression
    ///   → RateLimit → Auth → Timeout → OpenTelemetry
    /// ```
    ///
    /// Each layer is gated on its feature flag, so the stack only includes the
    /// middleware your build actually compiled in. `Auth` is application-defined
    /// (no universal default exists) and is intentionally omitted — add it with
    /// `.middleware(..)` after `default_stack()` at the Auth position if needed.
    /// `OpenTelemetry` is represented by the `tracing-mw` [`TracingLayer`].
    ///
    /// Middleware added *before* `default_stack()` runs outermost; middleware
    /// added *after* runs innermost (closest to the handler). For the canonical
    /// ordering, call `default_stack()` first.
    ///
    /// ```rust,ignore
    /// let router = Router::<()>::new()
    ///     .default_stack(std::time::Duration::from_secs(30))
    ///     .get("/", || async { "ok" });
    /// ```
    pub fn default_stack(mut self, request_timeout: std::time::Duration) -> Self {
        // 1. Request ID — first so every downstream layer can log/propagate it.
        #[cfg(feature = "request-id")]
        self.middlewares
            .push(Arc::new(crate::request_id::RequestId::new()));

        // 2. Logging.
        #[cfg(feature = "logging")]
        self.middlewares.push(Arc::new(crate::logger::Logger::new()));

        // 3. Recovery — catch panics and convert to a 500 problem+json.
        #[cfg(feature = "catch-panic")]
        self.middlewares
            .push(Arc::new(crate::catch_panic::CatchPanic::new()));

        // 4. CORS — permissive defaults; tighten via `.middleware(Cors::new()..)`.
        #[cfg(feature = "cors")]
        self.middlewares.push(Arc::new(crate::cors::Cors::new()));

        // 5. Compression.
        #[cfg(feature = "compress")]
        self.middlewares
            .push(Arc::new(crate::compress::Compress::new()));

        // 6. Rate limit — conservative default (1000 req / 60s per client).
        #[cfg(feature = "rate-limit")]
        self.middlewares
            .push(Arc::new(crate::rate_limit::RateLimiter::new(
                1000,
                std::time::Duration::from_secs(60),
            )));

        // 7. Auth — application-defined; intentionally not installed here.

        // 8. Timeout.
        #[cfg(feature = "timeout")]
        self.middlewares
            .push(Arc::new(crate::timeout::Timeout::new(request_timeout)));

        // 9. OpenTelemetry — tracing span per request.
        #[cfg(feature = "tracing-mw")]
        self.middlewares
            .push(Arc::new(crate::tracing_mw::TracingLayer));

        let _ = request_timeout; // silence unused warning when timeout feature is off
        self
    }

    // -- Fallback -----------------------------------------------------------

    /// Set a custom handler for unmatched routes (404).
    ///
    /// Without a fallback, unmatched routes return a plain-text "Not Found" response.
    ///
    /// ```rust,ignore
    /// Router::<()>::new()
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
    /// let router = Router::<()>::new()
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
    /// let spec = Router::<()>::new()
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
        // Production force-builds in `listen()`/`ensure_built()`, so this is
        // unreachable on the request path. Returning an error (rather than
        // panicking — the old code `.expect()`ed here) keeps benchmarks and
        // direct callers completely unwind-free.
        let router = match self.inner.as_ref() {
            Some(r) => r,
            None => return Err(RouteError::NotBuilt),
        };

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

        // SmallVec<4> avoids any heap allocation for routes with ≤4 params.
        // Static routes (no params) cost nothing — SmallVec::new() is zero-size on stack.
        let params: SmallVec<[(String, String); 4]> = matched
            .params
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();

        if let Some(kind) = MethodKind::from_http(method) {
            if let Some(handler) = method_map.get(kind) {
                return Ok(RouteMatch { handler, params });
            }
            // HEAD falls back to GET if no explicit HEAD handler.
            if kind == MethodKind::Head {
                if let Some(handler) = method_map.get(MethodKind::Get) {
                    return Ok(RouteMatch { handler, params });
                }
            }
        }

        // Path matched but the method has no handler (or is an unknown method):
        // 405 with the set of methods that ARE allowed.
        Err(RouteError::MethodNotAllowed {
            allow: method_map.allowed(),
        })
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

impl<S> Default for Router<S> {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// ServiceHandler — mount an arbitrary `tower::Service` at a path (P1.3)
// ---------------------------------------------------------------------------

/// Type-erased handler that forwards a [`Request`] to an inner `tower::Service`.
///
/// Used by [`Router::nest_service`] to mount any
/// `tower::Service<http::Request<Body>, Response = Response, Error = Infallible>`
/// (e.g. another [`RouterService`], a `tower-http` `ServeDir`, a gRPC service)
/// under a path prefix. The request body is buffered (bounded by `body_limit`)
/// before hand-off so the inner service receives a complete `http::Request<Body>`.
struct ServiceHandler<S> {
    service: S,
    body_limit: usize,
    /// The mount prefix (e.g. `/api`), stripped from the request path before the
    /// inner service sees it — so a service mounted at `/api` receives `/users`
    /// for a request to `/api/users`, matching Axum's `nest_service` semantics.
    prefix: String,
}

impl<S> ServiceHandler<S> {
    /// Strip the mount prefix from `path`, always returning a leading-slash path.
    fn strip_prefix<'a>(&self, path: &'a str) -> std::borrow::Cow<'a, str> {
        if self.prefix.is_empty() {
            return std::borrow::Cow::Borrowed(path);
        }
        match path.strip_prefix(&self.prefix) {
            // Exact prefix match (e.g. "/api" -> "/").
            Some("") => std::borrow::Cow::Borrowed("/"),
            // "/api/users" -> "/users".
            Some(rest) if rest.starts_with('/') => std::borrow::Cow::Borrowed(rest),
            // "/apixyz" — prefix matched as a substring, not a path boundary;
            // forward unchanged.
            _ => std::borrow::Cow::Borrowed(path),
        }
    }
}

impl<S> ErasedHandler for ServiceHandler<S>
where
    S: tower_service::Service<
            http::Request<Body>,
            Response = Response,
            Error = std::convert::Infallible,
        > + Clone
        + Send
        + Sync
        + 'static,
    S::Future: Send,
{
    fn call(&self, mut req: Request) -> Pin<Box<dyn Future<Output = Response> + Send>> {
        let mut service = self.service.clone();
        let body_limit = self.body_limit;

        // Rewrite the path to strip the mount prefix, preserving any query.
        // Computed here (while `self` is borrowed) so the `'static` future below
        // doesn't capture `self`.
        let original = req.uri().clone();
        let stripped_path = self.strip_prefix(original.path());
        let new_path_and_query = match original.query() {
            Some(q) => format!("{stripped_path}?{q}"),
            None => stripped_path.into_owned(),
        };
        let mut uri_parts = original.clone().into_parts();
        uri_parts.path_and_query = new_path_and_query.parse().ok();
        let uri = http::Uri::from_parts(uri_parts).unwrap_or(original);

        Box::pin(async move {
            // Buffer the streaming body so the inner service gets a complete,
            // Infallible-bodied `http::Request<Body>`.
            let body = match req.collect_body(body_limit).await {
                Ok(b) => b,
                Err(resp) => return resp,
            };
            let mut builder = http::Request::builder()
                .method(req.method().clone())
                .uri(uri);
            if let Some(headers) = builder.headers_mut() {
                *headers = req.headers().clone();
            }
            let http_req = match builder.body(Body::full(body)) {
                Ok(r) => r,
                Err(_) => return crate::error::AppError::internal(
                    "Failed to reconstruct request for nested service.",
                )
                .into_response(),
            };
            // poll_ready then call; Error is Infallible so unwrap is total.
            std::future::poll_fn(|cx| service.poll_ready(cx))
                .await
                .unwrap_or_else(|e| match e {});
            service.call(http_req).await.unwrap_or_else(|e| match e {})
        })
    }
}

// ---------------------------------------------------------------------------
// RouterService — Router as a `tower::Service` (P1.1, the keystone)
// ---------------------------------------------------------------------------

/// A compiled, cloneable [`tower::Service`] produced by [`Router::into_service`].
///
/// This is the composable, testable form of a [`Router`]: it implements
/// `tower_service::Service<http::Request<Body>>` with `Error = Infallible`
/// (like Axum), so it composes with `tower` / `tower-http` layers and supports
/// `ServiceExt::oneshot` testing.
///
/// The request body is passed through as a lazy stream (P1.2) — nothing is
/// buffered at the boundary; body extractors enforce per-frame size limits.
#[derive(Clone)]
pub struct RouterService {
    dispatch: crate::app::DispatchChain,
    state: Arc<StateMap>,
}

impl Router {
    /// Freeze this router into a [`RouterService`] — a `tower::Service` carrying
    /// the full middleware chain, route table, state, and fallback.
    pub fn into_service(mut self) -> RouterService {
        self.ensure_built();
        // State is injected into each request at the boundary (handlers extract
        // it from the request's state map); `build_dispatch` itself does not read
        // `state_map`, so moving it out here is sound.
        let state = Arc::new(std::mem::take(&mut self.state_map));
        let dispatch = crate::app::build_dispatch(Arc::new(self));
        RouterService { dispatch, state }
    }
}

impl RouterService {
    /// The compiled dispatch chain (middleware → routing → handler). The
    /// production server (`Neutron::listen`) drives requests through this exact
    /// chain so the server and the `tower::Service` share one dispatch path.
    pub(crate) fn dispatch_chain(&self) -> crate::app::DispatchChain {
        Arc::clone(&self.dispatch)
    }

    /// The application state map injected into every request at the boundary.
    pub(crate) fn state(&self) -> Arc<StateMap> {
        Arc::clone(&self.state)
    }
}

impl tower_service::Service<http::Request<Body>> for RouterService {
    type Response = Response;
    type Error = std::convert::Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // The router leaf is always ready. Backpressure-capable layers
        // (rate-limit, load-shed, buffer) propagate readiness above it once the
        // middleware chain is Tower-native (P1.M).
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<Body>) -> Self::Future {
        let dispatch = Arc::clone(&self.dispatch);
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            let (parts, body) = req.into_parts();
            // P1.2: pass the body through as a lazy stream. The response `Body`'s
            // error type is `Infallible`, so the boxed `ReqBody` error is the
            // never type (the map closure is never invoked).
            let boxed: ReqBody = Box::pin(
                body.map_err(|e: std::convert::Infallible| match e {}),
            );
            let neutron_req = Request::with_streaming_state(
                parts.method,
                parts.uri,
                parts.headers,
                boxed,
                state,
            );
            Ok(dispatch(neutron_req).await)
        })
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
        let r = build(Router::<()>::new().get("/", || async { "root" }));
        let m = r.resolve(&Method::GET, "/").unwrap();
        assert!(m.params.is_empty());
    }

    #[test]
    fn single_static_segment() {
        let r = build(Router::<()>::new().get("/users", || async { "users" }));
        assert!(r.resolve(&Method::GET, "/users").is_ok());
    }

    #[test]
    fn multi_segment_static_path() {
        let r = build(Router::<()>::new().get("/api/v1/users", || async { "v1" }));
        let m = r.resolve(&Method::GET, "/api/v1/users").unwrap();
        assert!(m.params.is_empty());
    }

    #[test]
    fn trailing_slash_normalized() {
        let r = build(Router::<()>::new().get("/users", || async { "u" }));
        assert!(r.resolve(&Method::GET, "/users").is_ok());
        assert!(r.resolve(&Method::GET, "/users/").is_ok());
    }

    #[test]
    fn trailing_slash_added_when_route_has_it() {
        // Route registered as /users/ — request to /users should still match
        let r = build(Router::<()>::new().get("/users/", || async { "u" }));
        assert!(r.resolve(&Method::GET, "/users/").is_ok());
        assert!(r.resolve(&Method::GET, "/users").is_ok());
    }

    #[test]
    fn root_with_and_without_slash() {
        let r = build(Router::<()>::new().get("/", || async { "root" }));
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
            Router::<()>::new()
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
            Router::<()>::new()
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
            Router::<()>::new()
                .get("/users", || async { "g" })
                .post("/users", || async { "p" }),
        );

        assert!(matches!(
            r.resolve(&Method::DELETE, "/users"),
            Err(RouteError::MethodNotAllowed { .. })
        ));
    }

    // P0.3: 405 carries the allowed-method set for the `Allow` header.
    #[test]
    fn method_not_allowed_includes_allow_set() {
        let r = build(
            Router::<()>::new()
                .get("/users", || async { "g" })
                .post("/users", || async { "p" }),
        );

        let allow = match r.resolve(&Method::DELETE, "/users") {
            Err(RouteError::MethodNotAllowed { allow }) => allow,
            Ok(_) => panic!("expected 405, got a route match"),
            Err(other) => panic!("expected MethodNotAllowed, got {other:?}"),
        };
        // GET + POST registered; HEAD implied by GET; OPTIONS always implied.
        for m in [Method::GET, Method::POST, Method::HEAD, Method::OPTIONS] {
            assert!(allow.contains(&m), "Allow set {allow:?} missing {m}");
        }
        // DELETE was the rejected method — it must not be advertised as allowed.
        assert!(!allow.contains(&Method::DELETE));
    }

    // P0.2: resolve() before build() returns an error and never unwinds.
    #[test]
    fn resolve_before_build_returns_not_built() {
        let r = Router::<()>::new().get("/", || async { "x" }); // NOT built
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            matches!(r.resolve(&Method::GET, "/"), Err(RouteError::NotBuilt))
        }));
        assert_eq!(
            result.ok(),
            Some(true),
            "resolve() before build() must return Err(NotBuilt) without panicking"
        );
    }

    // P1.1: RouterService is a tower::Service with the contract associated types.
    #[test]
    fn router_is_tower_service() {
        fn assert_service<S>()
        where
            S: tower_service::Service<
                http::Request<Body>,
                Response = Response,
                Error = std::convert::Infallible,
            >,
        {
        }
        assert_service::<RouterService>();
    }

    // P1.1: dispatch through into_service() + ServiceExt::oneshot runs the real
    // route table + middleware chain and returns the handler's response.
    #[tokio::test]
    async fn router_oneshot_dispatch() {
        use tower::ServiceExt;

        let svc = Router::<()>::new()
            .get("/hi", || async { "hello" })
            .into_service();

        let req = http::Request::builder()
            .method(Method::GET)
            .uri("/hi")
            .body(Body::empty())
            .unwrap();

        let resp = svc.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), http::StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], b"hello");
    }

    // P1.1: a 405 routed through the service still carries the Allow header.
    #[tokio::test]
    async fn router_service_405_has_allow() {
        use tower::ServiceExt;

        let svc = Router::<()>::new()
            .get("/x", || async { "g" })
            .into_service();
        let req = http::Request::builder()
            .method(Method::DELETE)
            .uri("/x")
            .body(Body::empty())
            .unwrap();

        let resp = svc.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), http::StatusCode::METHOD_NOT_ALLOWED);
        let allow = resp.headers().get(http::header::ALLOW).unwrap().to_str().unwrap();
        assert!(allow.contains("GET"), "Allow header was {allow:?}");
    }

    // P1.3: nest_service mounts another RouterService under a prefix and
    // forwards every request below it to that inner service.
    #[tokio::test]
    async fn nest_service_forwards_to_inner_router() {
        use tower::ServiceExt;

        let inner = Router::<()>::new()
            .get("/users", || async { "inner-users" })
            .get("/", || async { "inner-root" })
            .into_service();

        let app = Router::<()>::new()
            .get("/", || async { "outer-root" })
            .nest_service("/api", inner)
            .into_service();

        // Request under the prefix hits the inner service's deep route.
        let req = http::Request::builder()
            .method(Method::GET)
            .uri("/api/users")
            .body(Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), http::StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], b"inner-users");

        // The outer route is unaffected.
        let req = http::Request::builder()
            .method(Method::GET)
            .uri("/")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], b"outer-root");
    }

    // P1.3: a nested service's POST body is buffered and handed through intact.
    #[tokio::test]
    async fn nest_service_passes_body_through() {
        use tower::ServiceExt;

        let inner = Router::<()>::new()
            .post("/echo", |body: String| async move { body })
            .into_service();
        let app = Router::<()>::new().nest_service("/svc", inner).into_service();

        let req = http::Request::builder()
            .method(Method::POST)
            .uri("/svc/echo")
            .body(Body::full("payload-123"))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), http::StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], b"payload-123");
    }

    // P1.4: default_stack installs the contract middleware and dispatch works.
    #[tokio::test]
    async fn default_stack_dispatches() {
        use tower::ServiceExt;

        let svc = Router::<()>::new()
            .default_stack(std::time::Duration::from_secs(5))
            .get("/ok", || async { "stacked" })
            .into_service();

        let req = http::Request::builder()
            .method(Method::GET)
            .uri("/ok")
            .body(Body::empty())
            .unwrap();
        let resp = svc.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), http::StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], b"stacked");
    }

    // P1.4: default_stack installs middleware in the contract order. When the
    // relevant features are on, request-id is outermost (its header is present).
    #[cfg(feature = "request-id")]
    #[tokio::test]
    async fn default_stack_sets_request_id() {
        use tower::ServiceExt;

        let svc = Router::<()>::new()
            .default_stack(std::time::Duration::from_secs(5))
            .get("/", || async { "ok" })
            .into_service();
        let req = http::Request::builder()
            .method(Method::GET)
            .uri("/")
            .body(Body::empty())
            .unwrap();
        let resp = svc.oneshot(req).await.unwrap();
        assert!(
            resp.headers().contains_key("x-request-id"),
            "default_stack should install the RequestId layer"
        );
    }

    // P1.6/B.4: typed state binds via with_state and extracts at dispatch time.
    // The whole composite state extracts as State<AppState>; sub-states are
    // reachable through the derived FromRef impls.
    #[tokio::test]
    async fn typed_state_extracts() {
        use crate::extract::State;
        use crate::from_ref::FromRef;
        use tower::ServiceExt;

        #[derive(Clone, Debug, PartialEq)]
        struct Db(u32);

        #[derive(Clone, crate::FromRef)]
        struct AppState {
            db: Db,
        }

        // Compile-time: the derive produced FromRef<AppState> for Db.
        let app = AppState { db: Db(99) };
        assert_eq!(<Db as FromRef<AppState>>::from_ref(&app), Db(99));

        // Runtime: Router::<AppState>::new(...).with_state(app) -> Router<()>,
        // and State<AppState> resolves from the bound state map.
        let svc = Router::<AppState>::new()
            .get("/db", |State(s): State<AppState>| async move {
                s.db.0.to_string()
            })
            .with_state(app)
            .into_service();

        let req = http::Request::builder()
            .method(Method::GET)
            .uri("/db")
            .body(Body::empty())
            .unwrap();
        let resp = svc.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), http::StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], b"99");
    }

    // P2.1: built-in 404/405 are RFC 7807 problem+json with `instance` set.
    #[tokio::test]
    async fn not_found_is_problem_json() {
        use tower::ServiceExt;

        let svc = Router::<()>::new().get("/x", || async { "x" }).into_service();
        let req = http::Request::builder()
            .method(Method::GET)
            .uri("/nope")
            .body(Body::empty())
            .unwrap();

        let resp = svc.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), http::StatusCode::NOT_FOUND);
        assert_eq!(
            resp.headers().get("content-type").unwrap(),
            "application/problem+json"
        );
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["status"], 404);
        assert_eq!(v["instance"], "/nope");
    }

    #[tokio::test]
    async fn method_not_allowed_is_problem_json_with_allow() {
        use tower::ServiceExt;

        let svc = Router::<()>::new().get("/x", || async { "x" }).into_service();
        let req = http::Request::builder()
            .method(Method::DELETE)
            .uri("/x")
            .body(Body::empty())
            .unwrap();

        let resp = svc.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), http::StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(
            resp.headers().get("content-type").unwrap(),
            "application/problem+json"
        );
        assert!(resp.headers().get(http::header::ALLOW).is_some());
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["status"], 405);
    }

    // P1.M: closure middleware (the blanket `MiddlewareTrait for Fn(Request, Next)`)
    // runs as part of the chain — Axum `from_fn` ergonomics with no boilerplate.
    #[tokio::test]
    async fn closure_middleware_runs_in_chain() {
        use crate::middleware::Next;
        use tower::ServiceExt;

        let svc = Router::<()>::new()
            .middleware(|req: Request, next: Next| async move {
                let mut resp = next.run(req).await;
                resp.headers_mut()
                    .insert("x-mw", http::HeaderValue::from_static("1"));
                resp
            })
            .get("/", || async { "ok" })
            .into_service();

        let req = http::Request::builder().uri("/").body(Body::empty()).unwrap();
        let resp = svc.oneshot(req).await.unwrap();
        assert_eq!(resp.headers().get("x-mw").unwrap(), "1");
    }

    // P1.M: RouterService composes under a real `tower` layer stack — the basis
    // for inheriting the tower-http ecosystem (enabled by P1.1).
    #[tokio::test]
    async fn router_service_composes_with_tower_layer() {
        use tower::{ServiceBuilder, ServiceExt};

        let router = Router::<()>::new().get("/", || async { "ok" }).into_service();
        let svc = ServiceBuilder::new()
            .map_response(|mut resp: Response| {
                resp.headers_mut()
                    .insert("x-wrapped", http::HeaderValue::from_static("1"));
                resp
            })
            .service(router);

        let req = http::Request::builder().uri("/").body(Body::empty()).unwrap();
        let resp = svc.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), http::StatusCode::OK);
        assert_eq!(resp.headers().get("x-wrapped").unwrap(), "1");
    }

    #[tokio::test]
    async fn head_falls_back_to_get() {
        let r = build(Router::<()>::new().get("/", || async { "hello" }));
        // HEAD should resolve to the GET handler
        let m = r.resolve(&Method::HEAD, "/").unwrap();
        assert_eq!(body_of(m.handler).await, "hello");
    }

    #[test]
    fn head_returns_method_not_allowed_without_get() {
        let r = build(Router::<()>::new().post("/", || async { "p" }));
        assert!(matches!(
            r.resolve(&Method::HEAD, "/"),
            Err(RouteError::MethodNotAllowed { .. })
        ));
    }

    // -----------------------------------------------------------------------
    // Path parameters
    // -----------------------------------------------------------------------

    #[test]
    fn single_param_extracted() {
        let r = build(Router::<()>::new().get("/users/:id", || async { "u" }));
        let m = r.resolve(&Method::GET, "/users/42").unwrap();
        assert_eq!(&*m.params, &[("id".into(), "42".into())]);
    }

    #[test]
    fn multiple_params_extracted_in_order() {
        let r = build(Router::<()>::new().get("/users/:uid/posts/:pid", || async { "p" }));
        let m = r.resolve(&Method::GET, "/users/5/posts/99").unwrap();
        assert_eq!(
            &*m.params,
            &[("uid".into(), "5".into()), ("pid".into(), "99".into())]
        );
    }

    #[test]
    fn param_preserves_names() {
        let r = build(Router::<()>::new().get("/teams/:team_id/members/:member_id", || async { "m" }));
        let m = r.resolve(&Method::GET, "/teams/alpha/members/42").unwrap();
        assert_eq!(m.params[0], ("team_id".into(), "alpha".into()));
        assert_eq!(m.params[1], ("member_id".into(), "42".into()));
    }

    #[test]
    fn param_captures_any_string() {
        let r = build(Router::<()>::new().get("/search/:q", || async { "s" }));
        // Percent-encoded values pass through as-is (matchit does not decode)
        let m = r.resolve(&Method::GET, "/search/hello%20world").unwrap();
        assert_eq!(m.params[0].1, "hello%20world");
        // Plain strings pass through unchanged
        let m = r.resolve(&Method::GET, "/search/plain-text").unwrap();
        assert_eq!(m.params[0].1, "plain-text");
    }

    #[test]
    fn param_at_root_level() {
        let r = build(Router::<()>::new().get("/:org/repos", || async { "repos" }));
        let m = r.resolve(&Method::GET, "/github/repos").unwrap();
        assert_eq!(&*m.params, &[("org".into(), "github".into())]);
    }

    // -----------------------------------------------------------------------
    // Priority: static > param > wildcard
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn static_wins_over_param() {
        let r = build(
            Router::<()>::new()
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
            Router::<()>::new()
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
            Router::<()>::new()
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
            Router::<()>::new()
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
        let r = build(Router::<()>::new().get("/files/*", || async { "w" }));
        assert!(r.resolve(&Method::GET, "/files/a").is_ok());
    }

    #[test]
    fn wildcard_catches_deep_path() {
        let r = build(Router::<()>::new().get("/files/*", || async { "w" }));
        // Wildcard should catch any remaining depth.
        assert!(r.resolve(&Method::GET, "/files/a/b/c").is_ok());
    }

    // -----------------------------------------------------------------------
    // Error cases
    // -----------------------------------------------------------------------

    #[test]
    fn not_found_empty_router() {
        let r = build(Router::<()>::new());
        assert!(matches!(
            r.resolve(&Method::GET, "/anything"),
            Err(RouteError::NotFound)
        ));
    }

    #[test]
    fn not_found_root_when_empty() {
        let r = build(Router::<()>::new());
        assert!(matches!(
            r.resolve(&Method::GET, "/"),
            Err(RouteError::NotFound)
        ));
    }

    #[test]
    fn not_found_unmatched_path() {
        let r = build(Router::<()>::new().get("/users", || async { "u" }));
        assert!(matches!(
            r.resolve(&Method::GET, "/posts"),
            Err(RouteError::NotFound)
        ));
    }

    #[test]
    fn not_found_partial_prefix() {
        // Intermediate nodes without handlers must not match.
        let r = build(Router::<()>::new().get("/api/v1/users", || async { "u" }));
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
        let r = build(Router::<()>::new().get("/users", || async { "u" }));
        assert!(matches!(
            r.resolve(&Method::GET, "/users/1/posts/2"),
            Err(RouteError::NotFound)
        ));
    }

    #[test]
    fn method_not_allowed_vs_not_found() {
        let r = build(Router::<()>::new().get("/items", || async { "i" }));
        // Wrong method on existing path → 405
        assert!(matches!(
            r.resolve(&Method::POST, "/items"),
            Err(RouteError::MethodNotAllowed { .. })
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
        let r = build(Router::<()>::new().get("/a/b/c/d/e/f/g", || async { "deep" }));
        assert!(r.resolve(&Method::GET, "/a/b/c/d/e/f/g").is_ok());
        assert!(matches!(
            r.resolve(&Method::GET, "/a/b/c/d/e/f"),
            Err(RouteError::NotFound)
        ));
    }

    #[tokio::test]
    async fn many_static_siblings() {
        let r = build(
            Router::<()>::new()
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
            Router::<()>::new()
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
            Router::<()>::new()
                .get("/", || async { "root" })
                .get("/users", || async { "users" }),
        );

        assert!(r.resolve(&Method::GET, "/").is_ok());
        assert!(r.resolve(&Method::GET, "/users").is_ok());
    }

    #[test]
    fn param_and_its_child_both_have_handlers() {
        let r = build(
            Router::<()>::new()
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
        let r = build(Router::<()>::new().get("/files/:name", || async { "f" }));

        let m = r.resolve(&Method::GET, "/files/my-file.tar.gz").unwrap();
        assert_eq!(m.params[0].1, "my-file.tar.gz");

        let m = r.resolve(&Method::GET, "/files/hello_world-2024").unwrap();
        assert_eq!(m.params[0].1, "hello_world-2024");
    }

    #[test]
    fn shared_param_node_across_methods() {
        // GET and DELETE on same param path share the trie node.
        let r = build(
            Router::<()>::new()
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
        let r = Router::<()>::new().state(TestConfig {
            name: "test".into(),
        });
        assert!(r.state_map.contains_key(&TypeId::of::<TestConfig>()));
    }

    #[test]
    fn multiple_state_types() {
        let r = Router::<()>::new()
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
            Router::<()>::new()
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
        let r = build(Router::<()>::new().get("/", |State(_c): State<TestConfig>| async { "nope" }));

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
            Router::<()>::new()
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
        let api = Router::<()>::new()
            .get("/users", || async { "list_users" })
            .post("/users", || async { "create_user" });

        let r = build(
            Router::<()>::new()
                .get("/", || async { "root" })
                .nest("/api", api),
        );

        assert_eq!(body_of(r.resolve(&Method::GET, "/").unwrap().handler).await, "root");
        assert_eq!(body_of(r.resolve(&Method::GET, "/api/users").unwrap().handler).await, "list_users");
        assert_eq!(body_of(r.resolve(&Method::POST, "/api/users").unwrap().handler).await, "create_user");
    }

    #[tokio::test]
    async fn nest_with_params() {
        let sub = Router::<()>::new()
            .get("/:id", || async { "get_item" })
            .delete("/:id", || async { "delete_item" });

        let r = build(Router::<()>::new().nest("/items", sub));

        let m = r.resolve(&Method::GET, "/items/42").unwrap();
        assert_eq!(&*m.params, &[("id".into(), "42".into())]);
        assert_eq!(body_of(m.handler).await, "get_item");

        let m = r.resolve(&Method::DELETE, "/items/99").unwrap();
        assert_eq!(&*m.params, &[("id".into(), "99".into())]);
        assert_eq!(body_of(m.handler).await, "delete_item");
    }

    #[test]
    fn nest_prefix_with_param() {
        let sub = Router::<()>::new().get("/posts", || async { "posts" });

        let r = build(Router::<()>::new().nest("/users/:uid", sub));

        let m = r.resolve(&Method::GET, "/users/5/posts").unwrap();
        assert_eq!(&*m.params, &[("uid".into(), "5".into())]);
    }

    #[tokio::test]
    async fn nest_deep_prefix() {
        let sub = Router::<()>::new().get("/health", || async { "ok" });

        let r = build(Router::<()>::new().nest("/api/v1", sub));

        assert_eq!(
            body_of(r.resolve(&Method::GET, "/api/v1/health").unwrap().handler).await,
            "ok"
        );
    }

    #[tokio::test]
    async fn nest_overlapping_routes() {
        // Parent has /api/status, sub has /status under /api prefix.
        // Sub-router's handler should win (last write wins in merge).
        let sub = Router::<()>::new().get("/status", || async { "from_sub" });

        let r = build(
            Router::<()>::new()
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
        let users = Router::<()>::new()
            .get("/", || async { "list_users" })
            .get("/:id", || async { "get_user" });

        let posts = Router::<()>::new()
            .get("/", || async { "list_posts" })
            .post("/", || async { "create_post" });

        let r = build(
            Router::<()>::new()
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
        let sub = Router::<()>::new().get("/items", || async { "items" });

        let r = build(
            Router::<()>::new()
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
        let sub = Router::<()>::new().get("/items", || async { "items" });
        let r = build(Router::<()>::new().nest("/api", sub));

        assert!(matches!(
            r.resolve(&Method::GET, "/api/nope"),
            Err(RouteError::NotFound)
        ));
    }

    #[test]
    fn nest_sub_method_not_allowed() {
        let sub = Router::<()>::new().get("/items", || async { "items" });
        let r = build(Router::<()>::new().nest("/api", sub));

        assert!(matches!(
            r.resolve(&Method::POST, "/api/items"),
            Err(RouteError::MethodNotAllowed { .. })
        ));
    }

    #[test]
    fn nest_merges_state() {
        let sub = Router::<()>::new().state(42u64);

        let r = build(
            Router::<()>::new()
                .state(TestConfig { name: "app".into() })
                .nest("/api", sub),
        );

        assert!(r.state_map.contains_key(&TypeId::of::<TestConfig>()));
        assert!(r.state_map.contains_key(&TypeId::of::<u64>()));
    }

    #[test]
    fn nest_parent_state_wins_on_conflict() {
        let sub = Router::<()>::new().state(99u64);

        let r = build(Router::<()>::new().state(42u64).nest("/api", sub));

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

        let sub = Router::<()>::new()
            .middleware(add_header)
            .get("/items", || async { "items" });

        let r = build(
            Router::<()>::new()
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
        let sub = Router::<()>::new().get("/", || async { "sub_root" });

        let r = build(Router::<()>::new().nest("/api", sub));

        assert_eq!(
            body_of(r.resolve(&Method::GET, "/api").unwrap().handler).await,
            "sub_root"
        );
    }

    #[tokio::test]
    async fn nest_wildcard_in_sub() {
        let sub = Router::<()>::new().get("/*", || async { "catch_all" });
        let r = build(Router::<()>::new().nest("/files", sub));

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
        let r = Router::<()>::new()
            .get("/", || async { "root" })
            .fallback(|| async { "custom 404" });

        assert!(r.fallback.is_some());
    }

    #[tokio::test]
    async fn fallback_handler_is_callable() {
        let r = build(
            Router::<()>::new()
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
        let r = Router::<()>::new().get("/", || async { "root" });
        assert!(r.fallback.is_none());
    }
}
