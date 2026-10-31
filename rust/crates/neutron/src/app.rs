//! Application entry point and server lifecycle.
//!
//! The [`Neutron`] builder configures a router, optional HTTP/2 settings, and
//! graceful shutdown, then starts the server with [`Neutron::listen`] or
//! [`Neutron::serve`].
//!
//! ```rust,ignore
//! Neutron::new().router(router).serve(3000).await?;
//! ```

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder;
use tokio::net::TcpListener;
use tokio::sync::watch;
#[cfg(feature = "tls")]
use tokio_rustls::TlsAcceptor;

use crate::error::AppError;
use crate::handler::{Body, IntoResponse, Request as NeutronRequest, Response, StateMap};
use crate::http2::Http2Config;
use crate::middleware;
use crate::router::{RouteError, Router};
#[cfg(feature = "tls")]
use crate::tls::TlsConfig;

// ---------------------------------------------------------------------------
// Pre-computed static response bodies — zero-copy, no allocation per request.
// ---------------------------------------------------------------------------

// P2.1: every built-in failure is RFC 7807 `application/problem+json` via
// `AppError`, so the framework honors its own error contract (not plain text).

#[inline]
fn resp_payload_too_large() -> Response {
    AppError::payload_too_large("The request body exceeds the configured limit.").into_response()
}

#[inline]
fn resp_not_found(path: &str) -> Response {
    AppError::not_found("No route matches the requested path.")
        .with_instance(path)
        .into_response()
}

#[inline]
fn resp_method_not_allowed(allow: &[http::Method], path: &str) -> Response {
    let mut resp =
        AppError::method_not_allowed("The request method is not supported for this resource.")
            .with_instance(path)
            .into_response();
    // RFC 7231 §6.5.5: a 405 response MUST carry an `Allow` header.
    let allow_value = allow
        .iter()
        .map(|m| m.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    if let Ok(hv) = http::HeaderValue::from_str(&allow_value) {
        resp.headers_mut().insert(http::header::ALLOW, hv);
    }
    resp
}

#[inline]
fn resp_internal_error(path: &str) -> Response {
    AppError::internal("The server was not ready to handle the request.")
        .with_instance(path)
        .into_response()
}

/// Apply HTTP/2 configuration to a hyper-util auto builder.
fn apply_http2_config(builder: &mut Builder<TokioExecutor>, config: &Http2Config) {
    let mut h2 = builder.http2();
    h2.initial_stream_window_size(config.initial_stream_window_size)
        .initial_connection_window_size(config.initial_connection_window_size)
        .max_concurrent_streams(config.max_concurrent_streams)
        .max_frame_size(config.max_frame_size)
        .max_header_list_size(config.max_header_list_size)
        .adaptive_window(config.adaptive_window)
        .keep_alive_interval(config.keep_alive_interval)
        .keep_alive_timeout(config.keep_alive_timeout);
    if config.enable_connect_protocol {
        h2.enable_connect_protocol();
    }
}

/// Type alias for the fully-built dispatch chain.
pub(crate) type DispatchChain =
    Arc<dyn Fn(NeutronRequest) -> Pin<Box<dyn Future<Output = Response> + Send>> + Send + Sync>;

/// Build the complete dispatch chain from a router: middleware -> route resolution -> handler.
///
/// Shared by both `Neutron::listen` (production server) and `TestClient` (testing).
/// State is injected into the request before calling the chain via `Request::with_state()`,
/// so this function no longer needs to own the state map.
pub(crate) fn build_dispatch(router: Arc<Router>) -> DispatchChain {
    let final_handler: DispatchChain = {
        let router = Arc::clone(&router);
        Arc::new(
            move |mut req: NeutronRequest| -> Pin<Box<dyn Future<Output = Response> + Send>> {
                let router = Arc::clone(&router);
                Box::pin(async move {
                    let is_head = *req.method() == http::Method::HEAD;
                    match router.resolve(req.method(), req.uri().path()) {
                        Ok(route_match) => {
                            req.set_params(route_match.params);
                            let resp = route_match.handler.call(req).await;
                            if is_head {
                                let (parts, _) = resp.into_parts();
                                http::Response::from_parts(parts, Body::empty())
                            } else {
                                resp
                            }
                        }
                        Err(RouteError::NotFound) => {
                            if let Some(ref fallback) = router.fallback {
                                fallback.call(req).await
                            } else {
                                resp_not_found(req.uri().path())
                            }
                        }
                        Err(RouteError::MethodNotAllowed { allow }) => {
                            resp_method_not_allowed(&allow, req.uri().path())
                        }
                        // Unreachable on the request path (server force-builds
                        // before serving); handled for exhaustiveness.
                        Err(RouteError::NotBuilt) => resp_internal_error(req.uri().path()),
                    }
                })
            },
        )
    };

    middleware::build_chain(&router.middlewares, final_handler)
}

/// Type alias for shutdown hook functions.
type ShutdownHook = Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>;

/// TCP-level configuration for the server socket.
#[derive(Clone, Debug)]
pub struct TcpConfig {
    /// Enable `TCP_NODELAY` (disable Nagle's algorithm). Default: `true`.
    pub nodelay: bool,
    /// Set TCP keepalive interval. `None` to disable. Default: `None`.
    pub keepalive: Option<Duration>,
}

impl Default for TcpConfig {
    fn default() -> Self {
        Self {
            nodelay: true,
            keepalive: None,
        }
    }
}

/// Default global body size limit: 2 MiB.
pub(crate) const DEFAULT_MAX_BODY_SIZE: usize = 2 * 1024 * 1024;

// ---------------------------------------------------------------------------
// Thread-per-core helpers
// ---------------------------------------------------------------------------

/// Create `n` standard `TcpListener`s bound to `addr`.
///
/// On Linux (`SO_REUSEPORT` available) each socket binds independently so the
/// kernel distributes incoming connections across all sockets with no
/// cross-socket locking — true thread-per-core behaviour.
///
/// On other platforms (Windows, macOS) we return a single socket shared by all
/// workers via tokio's multi-task accept; the OS still uses the TCP backlog to
/// buffer incoming connections.
fn create_worker_listeners(
    addr: SocketAddr,
    n: usize,
) -> Result<Vec<std::net::TcpListener>, Box<dyn std::error::Error + Send + Sync>> {
    use socket2::{Domain, Protocol, Socket, Type};

    let domain = if addr.is_ipv6() {
        Domain::IPV6
    } else {
        Domain::IPV4
    };

    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        // Linux / Android: create N independent sockets with SO_REUSEPORT.
        let mut listeners = Vec::with_capacity(n);
        for _ in 0..n {
            let sock = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
            sock.set_reuse_address(true)?;
            #[cfg(any(target_os = "linux", target_os = "android"))]
            sock.set_reuse_port(true)?;
            sock.set_nonblocking(true)?;
            sock.bind(&addr.into())?;
            sock.listen(1024)?;
            listeners.push(std::net::TcpListener::from(sock));
        }
        Ok(listeners)
    }

    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    {
        // This platform lacks SO_REUSEPORT: create one socket and try_clone() N-1
        // times.  Each worker gets its own handle to the same accept queue; the
        // OS distributes concurrent accept() calls across handles.
        let sock = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
        sock.set_reuse_address(true)?;
        sock.set_nonblocking(true)?;
        sock.bind(&addr.into())?;
        sock.listen(1024)?;
        let first: std::net::TcpListener = sock.into();
        let mut listeners = Vec::with_capacity(n);
        for i in 0..n {
            if i == 0 {
                listeners.push(first.try_clone()?);
            } else {
                listeners.push(listeners[0].try_clone()?);
            }
        }
        Ok(listeners)
    }
}

/// One worker's accept loop, running inside a `current_thread` tokio runtime.
async fn worker_accept_loop(
    listener_std: std::net::TcpListener,
    chain: DispatchChain,
    state_map: Arc<StateMap>,
    http2_config: Option<crate::http2::Http2Config>,
    tcp_config: TcpConfig,
    max_body_size: usize,
    mut stop_rx: tokio::sync::broadcast::Receiver<()>,
    semaphore: Option<Arc<tokio::sync::Semaphore>>,
    shutdown_timeout: Duration,
) {
    let listener = match TcpListener::from_std(listener_std) {
        Ok(l) => l,
        Err(e) => {
            tracing::error!("Worker failed to create listener: {e}");
            return;
        }
    };

    #[cfg(feature = "ws")]
    let upgrade_tasks = crate::task_tracker::UpgradeTasks::new();
    #[cfg(feature = "ws")]
    let _upgrade_scope = upgrade_tasks.scope();
    #[cfg(feature = "ws")]
    let chain = crate::task_tracker::attach(chain, upgrade_tasks.clone());
    let mut tasks = tokio::task::JoinSet::new();
    loop {
        let mut conn_stop = stop_rx.resubscribe();
        let (stream, remote_addr) = tokio::select! {
            biased;
            _ = stop_rx.recv() => {
                #[cfg(feature = "ws")] upgrade_tasks.stop();
                break;
            },
            _ = tasks.join_next(), if !tasks.is_empty() => continue,
            res = listener.accept() => match res {
                Ok(pair) => pair,
                Err(e) => {
                    tracing::error!("Accept error: {e}");
                    continue;
                }
            },
        };

        // Apply TCP options.
        let _ = stream.set_nodelay(tcp_config.nodelay);
        if let Some(ka) = tcp_config.keepalive {
            let sock_ref = socket2::SockRef::from(&stream);
            let keepalive = socket2::TcpKeepalive::new().with_time(ka);
            let _ = sock_ref.set_tcp_keepalive(&keepalive);
        }

        let permit = if let Some(ref sem) = semaphore {
            match sem.clone().try_acquire_owned() {
                Ok(permit) => Some(permit),
                Err(_) => continue,
            }
        } else {
            None
        };
        let chain = Arc::clone(&chain);
        let state_conn = Arc::clone(&state_map);
        let h2_config = http2_config.clone();

        tasks.spawn(async move {
            let _permit = permit;
            let service = service_fn(move |mut req: http::Request<Incoming>| {
                let chain = Arc::clone(&chain);
                let state = Arc::clone(&state_conn);
                let remote = remote_addr;
                let body_limit = max_body_size;
                async move {
                    if let Some(cl) = req.headers().get(http::header::CONTENT_LENGTH) {
                        if let Ok(len) = cl.to_str().unwrap_or("0").parse::<usize>() {
                            if len > body_limit {
                                return Ok::<_, std::convert::Infallible>(resp_payload_too_large());
                            }
                        }
                    }

                    let needs_upgrade = req.headers().contains_key(http::header::UPGRADE);
                    let on_upgrade = needs_upgrade.then(|| hyper::upgrade::on(&mut req));

                    let (parts, body) = req.into_parts();
                    // P1.2: pass the body through as a lazy stream — no pre-collect.
                    // The Content-Length early-413 above is kept; the per-frame
                    // ceiling in collect_body enforces the limit for chunked bodies.
                    let raw: crate::handler::ReqBody = Box::pin(
                        body.map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>),
                    );
                    // Transport-level ceiling (RS-05): the configured
                    // max_body_size applies to EVERY consumer — including
                    // chunked bodies (no Content-Length to pre-check) and
                    // streaming handlers — with breaches classified as 413.
                    let boxed = crate::handler::limit_body_stream(raw, body_limit);

                    let mut neutron_req = crate::handler::Request::with_streaming_state(
                        parts.method,
                        parts.uri,
                        parts.headers,
                        boxed,
                        state,
                    );
                    if let Some(upgrade) = on_upgrade {
                        neutron_req.set_on_upgrade(upgrade);
                    }
                    neutron_req.set_remote_addr(remote);

                    let response = chain(neutron_req).await;
                    Ok::<_, std::convert::Infallible>(response)
                }
            });

            let mut builder = Builder::new(TokioExecutor::new());
            if let Some(ref config) = h2_config {
                apply_http2_config(&mut builder, config);
            }
            let conn = builder.serve_connection_with_upgrades(TokioIo::new(stream), service);
            tokio::pin!(conn);
            tokio::select! {
                result = conn.as_mut() => { if let Err(e) = result { tracing::debug!("Worker connection error: {e}"); } }
                _ = conn_stop.recv() => { conn.as_mut().graceful_shutdown(); let _ = conn.await; }
            }
        });
    }
    drop(listener);
    #[cfg(feature = "ws")]
    {
        let end = tokio::time::Instant::now() + shutdown_timeout;
        tokio::join!(
            drain_connections(&mut tasks, shutdown_timeout),
            upgrade_tasks.drain(end)
        );
    }
    #[cfg(not(feature = "ws"))]
    drain_connections(&mut tasks, shutdown_timeout).await;
}

async fn drain_connections(tasks: &mut tokio::task::JoinSet<()>, timeout: Duration) {
    if tokio::time::timeout(timeout, async {
        while tasks.join_next().await.is_some() {}
    })
    .await
    .is_err()
    {
        tracing::warn!("Connection drain deadline exceeded; aborting owned tasks");
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
    }
}

/// The Neutron application builder.
pub struct Neutron {
    router: Router,
    shutdown_timeout: Duration,
    http2_config: Option<Http2Config>,
    shutdown_hooks: Vec<ShutdownHook>,
    custom_shutdown: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
    max_connections: Option<usize>,
    tcp_config: TcpConfig,
    max_body_size: usize,
    /// Number of worker threads. `0` = use the calling tokio runtime (default).
    /// `> 1` = spawn N dedicated current-thread runtimes, each with its own
    /// listener socket.  On Linux these sockets use `SO_REUSEPORT` so the OS
    /// distributes connections at the kernel level with no cross-core locking.
    worker_threads: usize,
}

impl Neutron {
    pub fn new() -> Self {
        Self {
            router: Router::new(),
            shutdown_timeout: Duration::from_secs(30),
            http2_config: None,
            shutdown_hooks: Vec::new(),
            custom_shutdown: None,
            max_connections: None,
            tcp_config: TcpConfig::default(),
            max_body_size: DEFAULT_MAX_BODY_SIZE,
            worker_threads: 0,
        }
    }

    /// Set the application router.
    pub fn router(mut self, router: Router) -> Self {
        self.router = router;
        self
    }

    /// Set HTTP/2 configuration.
    ///
    /// ```rust,ignore
    /// use neutron::http2::Http2Config;
    ///
    /// Neutron::new()
    ///     .http2(Http2Config::new().max_concurrent_streams(100))
    ///     .router(router)
    ///     .listen(addr)
    ///     .await;
    /// ```
    pub fn http2(mut self, config: Http2Config) -> Self {
        self.http2_config = Some(config);
        self
    }

    /// Set the graceful shutdown timeout.
    ///
    /// When a shutdown signal is received, the server stops accepting new connections
    /// and waits up to this duration for active connections to complete.
    /// Defaults to 30 seconds.
    pub fn shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = timeout;
        self
    }

    /// Register a shutdown hook (async closure).
    ///
    /// Hooks run in reverse registration order after active connections drain, before
    /// active connections begin draining. Use them for cleanup tasks like
    /// flushing metrics, closing database pools, or notifying services.
    ///
    /// ```rust,ignore
    /// Neutron::new()
    ///     .on_shutdown(|| async {
    ///         tracing::info!("Flushing metrics...");
    ///     })
    ///     .router(router)
    ///     .listen(addr)
    ///     .await;
    /// ```
    pub fn on_shutdown<F, Fut>(mut self, hook: F) -> Self
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.shutdown_hooks.push(Box::new(move || Box::pin(hook())));
        self
    }

    /// Provide a custom shutdown signal future instead of the default Ctrl-C.
    ///
    /// The server will stop accepting connections when this future completes.
    ///
    /// ```rust,ignore
    /// use tokio::sync::oneshot;
    ///
    /// let (tx, rx) = oneshot::channel::<()>();
    /// Neutron::new()
    ///     .shutdown_signal(async move { rx.await.ok(); })
    ///     .router(router)
    ///     .listen(addr)
    ///     .await;
    ///
    /// // Later, trigger shutdown:
    /// tx.send(()).ok();
    /// ```
    pub fn shutdown_signal<F>(mut self, signal: F) -> Self
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.custom_shutdown = Some(Box::pin(signal));
        self
    }

    /// Set the maximum number of concurrent connections.
    ///
    /// When the limit is reached, new connections are held in the TCP accept
    /// backlog until an existing connection closes. `None` (the default) means
    /// no limit.
    ///
    /// ```rust,ignore
    /// Neutron::new().max_connections(10_000).router(router).listen(addr).await;
    /// ```
    pub fn max_connections(mut self, max: usize) -> Self {
        self.max_connections = Some(max);
        self
    }

    /// Enable `TCP_NODELAY` on accepted sockets.
    ///
    /// When `true` (the default), disables Nagle's algorithm for lower latency.
    pub fn tcp_nodelay(mut self, nodelay: bool) -> Self {
        self.tcp_config.nodelay = nodelay;
        self
    }

    /// Set TCP keepalive interval on accepted sockets.
    ///
    /// `None` (the default) disables TCP keepalive.
    pub fn tcp_keepalive(mut self, interval: Option<Duration>) -> Self {
        self.tcp_config.keepalive = interval;
        self
    }

    /// Set the number of worker threads for thread-per-core operation.
    ///
    /// When set to `n > 0`, Neutron spawns `n` OS threads each running a
    /// dedicated single-thread tokio runtime.  On Linux each worker binds its
    /// own socket with `SO_REUSEPORT` so the kernel distributes incoming
    /// connections at the packet level — no cross-core lock contention.
    /// On Windows/macOS a single listener is shared across workers via
    /// in-process connection dispatch.
    ///
    /// `0` (the default) uses the calling tokio runtime, typically
    /// `#[tokio::main]`'s multi-thread work-stealing scheduler.
    ///
    /// Use [`std::thread::available_parallelism`] to auto-detect CPU count:
    ///
    /// ```rust,ignore
    /// let cpus = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4);
    /// Neutron::new().workers(cpus).router(router).listen(addr).await?;
    /// ```
    pub fn workers(mut self, n: usize) -> Self {
        self.worker_threads = n;
        self
    }

    /// Set the global maximum request body size in bytes.
    ///
    /// Requests with a `Content-Length` header exceeding this limit are rejected
    /// immediately with `413 Payload Too Large` **before** the body is read into
    /// memory. Requests without `Content-Length` are limited during streaming.
    ///
    /// Defaults to 2 MiB. Use [`BodyLimit`](crate::body_limit::BodyLimit)
    /// middleware for per-route limits stricter than this global cap.
    pub fn max_body_size(mut self, max: usize) -> Self {
        self.max_body_size = max;
        self
    }

    /// Start the server on the given port, binding to `127.0.0.1`.
    ///
    /// For custom host binding, use [`listen`](Self::listen) with a `SocketAddr`.
    pub async fn serve(self, port: u16) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.listen(SocketAddr::from(([127, 0, 0, 1], port))).await
    }

    /// Start the server on the given socket address.
    ///
    /// Supports graceful shutdown: on Ctrl-C the server stops accepting new connections,
    /// signals all active connections to finish their current request, and waits for them
    /// to drain (up to the configured shutdown timeout).
    ///
    /// ```rust,ignore
    /// let config = Config::from_env();
    /// Neutron::new().router(router).listen(config.socket_addr()).await
    /// ```
    pub async fn listen(
        self,
        addr: SocketAddr,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.worker_threads > 0 {
            return self.listen_workers(addr).await;
        }
        let listener = TcpListener::bind(addr).await?;

        tracing::info!("Neutron listening on http://{addr}");

        // P1.3: dispatch through the same `RouterService` artifacts the
        // `tower::Service` impl uses — one compiled chain, one state map.
        let service = self.router.into_service();
        let chain = service.dispatch_chain();
        #[cfg(feature = "ws")]
        let upgrade_tasks = crate::task_tracker::UpgradeTasks::new();
        #[cfg(feature = "ws")]
        let _upgrade_scope = upgrade_tasks.scope();
        #[cfg(feature = "ws")]
        let chain = crate::task_tracker::attach(chain, upgrade_tasks.clone());
        let state_map = service.state();

        // Connection limit semaphore
        let conn_semaphore = self
            .max_connections
            .map(|max| Arc::new(tokio::sync::Semaphore::new(max)));

        // Shutdown coordination
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let active_count = Arc::new(AtomicUsize::new(0));
        let shutdown_timeout = self.shutdown_timeout;
        let http2_config = self.http2_config.clone();
        let shutdown_hooks = self.shutdown_hooks;
        let tcp_config = self.tcp_config;
        let max_body_size = self.max_body_size;

        // Build the shutdown signal
        let mut shutdown_signal: Pin<Box<dyn Future<Output = ()> + Send>> =
            if let Some(custom) = self.custom_shutdown {
                custom
            } else {
                Box::pin(async {
                    default_shutdown_signal().await;
                })
            };

        let mut tasks = tokio::task::JoinSet::new();
        // Accept loop
        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown_signal => {
                    #[cfg(feature = "ws")] upgrade_tasks.stop();
                    let _ = shutdown_tx.send(true);
                    break;
                }
                _ = tasks.join_next(), if !tasks.is_empty() => {}
                result = listener.accept() => {
                    let (stream, remote_addr) = result?;

                    // Apply TCP options
                    let _ = stream.set_nodelay(tcp_config.nodelay);
                    if let Some(ka) = tcp_config.keepalive {
                        let sock_ref = socket2::SockRef::from(&stream);
                        let keepalive = socket2::TcpKeepalive::new().with_time(ka);
                        let _ = sock_ref.set_tcp_keepalive(&keepalive);
                    }

                    // Acquire connection permit (if limit set)
                    let permit = if let Some(ref sem) = conn_semaphore {
                        match sem.clone().try_acquire_owned() {
                            Ok(permit) => Some(permit),
                            Err(_) => {
                                tracing::warn!("Max connections reached, rejecting");
                                drop(stream);
                                continue;
                            }
                        }
                    } else {
                        None
                    };

                    let chain = Arc::clone(&chain);
                    let state_map = Arc::clone(&state_map);
                    let mut conn_shutdown_rx = shutdown_rx.clone();
                    let active = Arc::clone(&active_count);
                    let h2_config = http2_config.clone();

                    active.fetch_add(1, Ordering::Relaxed);

                    tasks.spawn(async move {
                        // Hold permit for lifetime of the connection
                        let _permit = permit;

                        let service = service_fn(move |mut req: http::Request<Incoming>| {
                            let chain = Arc::clone(&chain);
                            let state = Arc::clone(&state_map);
                            let remote = remote_addr;
                            let body_limit = max_body_size;
                            async move {
                                // Reject early if Content-Length exceeds the global body limit.
                                if let Some(cl) = req.headers().get(http::header::CONTENT_LENGTH) {
                                    if let Ok(len) = cl.to_str().unwrap_or("0").parse::<usize>() {
                                        if len > body_limit {
                                            return Ok::<_, std::convert::Infallible>(
                                                resp_payload_too_large(),
                                            );
                                        }
                                    }
                                }

                                // Only register the WebSocket upgrade future when the request
                                // actually has an `Upgrade` header — saves ~5-10 µs on every
                                // non-WebSocket request by skipping hyper's upgrade machinery.
                                let needs_upgrade = req
                                    .headers()
                                    .contains_key(http::header::UPGRADE);
                                let on_upgrade = needs_upgrade
                                    .then(|| hyper::upgrade::on(&mut req));

                                let (parts, body) = req.into_parts();

                                // P1.2: pass the body through as a lazy stream — no
                                // pre-collect. The Content-Length early-413 above is
                                // kept; the per-frame ceiling in collect_body enforces
                                // the limit for chunked bodies.
                                let raw: crate::handler::ReqBody =
                                    Box::pin(body.map_err(|e| {
                                        Box::new(e)
                                            as Box<dyn std::error::Error + Send + Sync>
                                    }));
                                // RS-05: transport ceiling for chunked /
                                // length-less bodies (see worker path note).
                                let boxed =
                                    crate::handler::limit_body_stream(raw, body_limit);

                                let mut neutron_req = NeutronRequest::with_streaming_state(
                                    parts.method,
                                    parts.uri,
                                    parts.headers,
                                    boxed,
                                    state,
                                );
                                if let Some(upgrade) = on_upgrade {
                                    neutron_req.set_on_upgrade(upgrade);
                                }
                                neutron_req.set_remote_addr(remote);

                                let response = chain(neutron_req).await;
                                Ok::<_, std::convert::Infallible>(response)
                            }
                        });

                        let mut builder = Builder::new(TokioExecutor::new());
                        if let Some(ref config) = h2_config {
                            apply_http2_config(&mut builder, config);
                        }
                        let conn = builder
                            .serve_connection_with_upgrades(TokioIo::new(stream), service);
                        tokio::pin!(conn);

                        // Drive the connection, watching for shutdown signal
                        let mut shutdown_received = false;
                        tokio::select! {
                            result = conn.as_mut() => {
                                if let Err(e) = result {
                                    tracing::error!("Connection error: {e}");
                                }
                            }
                            _ = conn_shutdown_rx.changed() => {
                                shutdown_received = true;
                                conn.as_mut().graceful_shutdown();
                            }
                        }

                        // If shutdown was signaled, drive connection to completion
                        if shutdown_received {
                            if let Err(e) = conn.as_mut().await {
                                tracing::error!("Connection error during drain: {e}");
                            }
                        }

                        active.fetch_sub(1, Ordering::Relaxed);
                    });
                }
            }
        }

        drop(listener);
        #[cfg(feature = "ws")]
        {
            let end = tokio::time::Instant::now() + shutdown_timeout;
            tokio::join!(
                drain_connections(&mut tasks, shutdown_timeout),
                upgrade_tasks.drain(end)
            );
        }
        #[cfg(not(feature = "ws"))]
        drain_connections(&mut tasks, shutdown_timeout).await;
        for hook in shutdown_hooks.into_iter().rev() {
            hook().await;
        }

        tracing::info!("Server stopped");
        Ok(())
    }

    /// Internal: multi-worker thread-per-core accept loop.
    ///
    /// Spawns `self.worker_threads` OS threads, each running an independent
    /// `current_thread` tokio runtime.  On Linux each thread opens its own
    /// listener socket with `SO_REUSEPORT`; on other platforms a single
    /// listener is shared and connections are distributed via a channel.
    async fn listen_workers(
        self,
        addr: SocketAddr,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let n = self.worker_threads;
        let semaphore = self
            .max_connections
            .map(|max| Arc::new(tokio::sync::Semaphore::new(max)));
        let shutdown_timeout = self.shutdown_timeout;
        tracing::info!("Neutron listening on http://{addr} ({n} worker threads)");

        // P1.3: single dispatch path shared with the `tower::Service` impl.
        let service = self.router.into_service();
        let chain = service.dispatch_chain();
        let state_map = service.state();

        let http2_config = self.http2_config;
        let tcp_config = self.tcp_config;
        let max_body_size = self.max_body_size;

        // Shutdown coordination: a oneshot-style channel from Ctrl-C → workers.
        let (stop_tx, _stop_rx) = tokio::sync::broadcast::channel::<()>(1);

        // --- Per-OS listener construction ---
        //
        // Linux/Android: SO_REUSEPORT — N independent sockets, kernel distributes
        // connections at packet level (no shared accept lock, best cache locality).
        //
        // Windows/macOS: one shared socket dispatched via a channel to workers.
        // The channel overhead is minimal compared to the per-connection cost.

        let listeners = create_worker_listeners(addr, n)?;

        // Spawn worker threads.
        let mut thread_handles = Vec::with_capacity(n);
        for listener_std in listeners {
            let chain = Arc::clone(&chain);
            let state_map = Arc::clone(&state_map);
            let h2_config = http2_config.clone();
            let tcp_cfg = tcp_config.clone();
            let stop_rx = stop_tx.subscribe();
            let semaphore = semaphore.clone();

            let handle = std::thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("worker runtime");

                rt.block_on(worker_accept_loop(
                    listener_std,
                    chain,
                    state_map,
                    h2_config,
                    tcp_cfg,
                    max_body_size,
                    stop_rx,
                    semaphore,
                    shutdown_timeout,
                ));
            });
            thread_handles.push(handle);
        }

        // Wait for Ctrl-C (or custom shutdown).
        if let Some(signal) = self.custom_shutdown {
            signal.await;
        } else {
            default_shutdown_signal().await;
        }

        let _ = stop_tx.send(());
        tokio::task::spawn_blocking(move || {
            for handle in thread_handles {
                let _ = handle.join();
            }
        })
        .await?;
        for hook in self.shutdown_hooks.into_iter().rev() {
            hook().await;
        }

        tracing::info!("Server stopped");
        Ok(())
    }

    /// Start the server with TLS on the given socket address.
    ///
    /// ```rust,ignore
    /// use neutron::tls::TlsConfig;
    ///
    /// let tls = TlsConfig::from_pem("cert.pem", "key.pem").unwrap();
    /// Neutron::new()
    ///     .router(router)
    ///     .listen_tls("0.0.0.0:443".parse().unwrap(), tls)
    ///     .await
    ///     .unwrap();
    /// ```
    #[cfg(feature = "tls")]
    pub async fn listen_tls(
        self,
        addr: SocketAddr,
        tls_config: TlsConfig,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind(addr).await?;
        let tls_acceptor = TlsAcceptor::from(tls_config.server_config);

        tracing::info!("Neutron listening on https://{addr}");

        // P1.3: single dispatch path shared with the `tower::Service` impl.
        let service = self.router.into_service();
        let chain = service.dispatch_chain();
        #[cfg(feature = "ws")]
        let upgrade_tasks = crate::task_tracker::UpgradeTasks::new();
        #[cfg(feature = "ws")]
        let _upgrade_scope = upgrade_tasks.scope();
        #[cfg(feature = "ws")]
        let chain = crate::task_tracker::attach(chain, upgrade_tasks.clone());
        let state_map = service.state();

        let conn_semaphore = self
            .max_connections
            .map(|max| Arc::new(tokio::sync::Semaphore::new(max)));

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let active_count = Arc::new(AtomicUsize::new(0));
        let shutdown_timeout = self.shutdown_timeout;
        let http2_config = self.http2_config.clone();
        let shutdown_hooks = self.shutdown_hooks;
        let tcp_config = self.tcp_config;
        let max_body_size = self.max_body_size;

        let mut shutdown_signal: Pin<Box<dyn Future<Output = ()> + Send>> =
            if let Some(custom) = self.custom_shutdown {
                custom
            } else {
                Box::pin(async {
                    default_shutdown_signal().await;
                })
            };

        let mut tasks = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown_signal => {
                    #[cfg(feature = "ws")] upgrade_tasks.stop();
                    let _ = shutdown_tx.send(true);
                    break;
                }
                _ = tasks.join_next(), if !tasks.is_empty() => {}
                result = listener.accept() => {
                    let (stream, remote_addr) = result?;

                    let _ = stream.set_nodelay(tcp_config.nodelay);
                    if let Some(ka) = tcp_config.keepalive {
                        let sock_ref = socket2::SockRef::from(&stream);
                        let keepalive = socket2::TcpKeepalive::new().with_time(ka);
                        let _ = sock_ref.set_tcp_keepalive(&keepalive);
                    }

                    let permit = if let Some(ref sem) = conn_semaphore {
                        match sem.clone().try_acquire_owned() {
                            Ok(permit) => Some(permit),
                            Err(_) => {
                                tracing::warn!("Max connections reached, rejecting");
                                drop(stream);
                                continue;
                            }
                        }
                    } else {
                        None
                    };

                    let chain = Arc::clone(&chain);
                    let state_map = Arc::clone(&state_map);
                    let mut conn_shutdown_rx = shutdown_rx.clone();
                    let active = Arc::clone(&active_count);
                    let acceptor = tls_acceptor.clone();
                    let h2_config = http2_config.clone();

                    active.fetch_add(1, Ordering::Relaxed);

                    tasks.spawn(async move {
                        let _permit = permit;
                        // TLS handshake
                        let tls_stream = match tokio::select! {
                            _ = conn_shutdown_rx.changed() => { active.fetch_sub(1, Ordering::Relaxed); return; }
                            result = tokio::time::timeout(Duration::from_secs(10), acceptor.accept(stream)) => result,
                        } {
                            Ok(Ok(s)) => s,
                            error => {
                                tracing::debug!("TLS handshake failed or timed out: {error:?}");
                                active.fetch_sub(1, Ordering::Relaxed);
                                return;
                            }
                        };

                        let service = service_fn(move |mut req: http::Request<Incoming>| {
                            let chain = Arc::clone(&chain);
                            let state = Arc::clone(&state_map);
                            let remote = remote_addr;
                            let body_limit = max_body_size;
                            async move {
                                if let Some(cl) = req.headers().get(http::header::CONTENT_LENGTH) {
                                    if let Ok(len) = cl.to_str().unwrap_or("0").parse::<usize>() {
                                        if len > body_limit {
                                            return Ok::<_, std::convert::Infallible>(
                                                resp_payload_too_large(),
                                            );
                                        }
                                    }
                                }

                                let needs_upgrade = req
                                    .headers()
                                    .contains_key(http::header::UPGRADE);
                                let on_upgrade = needs_upgrade
                                    .then(|| hyper::upgrade::on(&mut req));

                                let (parts, body) = req.into_parts();

                                // P1.2: lazy streaming body, no pre-collect (TLS path).
                                let raw: crate::handler::ReqBody =
                                    Box::pin(body.map_err(|e| {
                                        Box::new(e)
                                            as Box<dyn std::error::Error + Send + Sync>
                                    }));
                                // RS-05: transport ceiling for chunked /
                                // length-less bodies (see worker path note).
                                let boxed =
                                    crate::handler::limit_body_stream(raw, body_limit);

                                let mut neutron_req = NeutronRequest::with_streaming_state(
                                    parts.method,
                                    parts.uri,
                                    parts.headers,
                                    boxed,
                                    state,
                                );
                                if let Some(upgrade) = on_upgrade {
                                    neutron_req.set_on_upgrade(upgrade);
                                }
                                neutron_req.set_remote_addr(remote);

                                let response = chain(neutron_req).await;
                                Ok::<_, std::convert::Infallible>(response)
                            }
                        });

                        let mut builder = Builder::new(TokioExecutor::new());
                        if let Some(ref config) = h2_config {
                            apply_http2_config(&mut builder, config);
                        }
                        let conn = builder
                            .serve_connection_with_upgrades(TokioIo::new(tls_stream), service);
                        tokio::pin!(conn);

                        let mut shutdown_received = false;
                        tokio::select! {
                            result = conn.as_mut() => {
                                if let Err(e) = result {
                                    tracing::error!("Connection error: {e}");
                                }
                            }
                            _ = conn_shutdown_rx.changed() => {
                                shutdown_received = true;
                                conn.as_mut().graceful_shutdown();
                            }
                        }

                        if shutdown_received {
                            if let Err(e) = conn.as_mut().await {
                                tracing::error!("Connection error during drain: {e}");
                            }
                        }

                        active.fetch_sub(1, Ordering::Relaxed);
                    });
                }
            }
        }

        drop(listener);
        #[cfg(feature = "ws")]
        {
            let end = tokio::time::Instant::now() + shutdown_timeout;
            tokio::join!(
                drain_connections(&mut tasks, shutdown_timeout),
                upgrade_tasks.drain(end)
            );
        }
        #[cfg(not(feature = "ws"))]
        drain_connections(&mut tasks, shutdown_timeout).await;
        for hook in shutdown_hooks.into_iter().rev() {
            hook().await;
        }

        tracing::info!("Server stopped");
        Ok(())
    }

    /// Start an HTTP/3 server on the given socket address.
    ///
    /// Binds a QUIC UDP endpoint using the supplied TLS configuration
    /// (TLS 1.3 is required by the QUIC specification).  Accepts connections
    /// and serves HTTP/3 requests using the same middleware + router chain as
    /// [`listen`](Self::listen) and [`listen_tls`](Self::listen_tls).
    ///
    /// To advertise HTTP/3 availability to browsers, include an `Alt-Svc`
    /// header in your HTTP/1.1 or HTTP/2 responses pointing to this port:
    ///
    /// ```text
    /// Alt-Svc: h3=":4433"; ma=2592000
    /// ```
    ///
    /// Requires the `http3` Cargo feature.
    ///
    /// ```rust,ignore
    /// use neutron::tls::TlsConfig;
    ///
    /// let tls = TlsConfig::from_pem("cert.pem", "key.pem").unwrap();
    /// Neutron::new()
    ///     .router(router)
    ///     .listen_h3("0.0.0.0:4433".parse().unwrap(), tls)
    ///     .await
    ///     .unwrap();
    /// ```
    #[cfg(feature = "http3")]
    pub async fn listen_h3(
        self,
        addr: SocketAddr,
        tls_config: TlsConfig,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use crate::http3_server::{serve_h3_with_shutdown, Http3Config};

        // P1.3: single dispatch path shared with the `tower::Service` impl.
        let service = self.router.into_service();
        let chain = service.dispatch_chain();
        let state_map = service.state();

        let h3_cfg = Http3Config {
            max_body_size: self.max_body_size,
        };

        let signal = self
            .custom_shutdown
            .unwrap_or_else(|| Box::pin(default_shutdown_signal()));
        serve_h3_with_shutdown(
            addr,
            chain,
            state_map,
            tls_config,
            h3_cfg,
            signal,
            self.shutdown_timeout,
            self.max_connections,
        )
        .await?;
        for hook in self.shutdown_hooks.into_iter().rev() {
            hook().await;
        }
        Ok(())
    }
}

impl Default for Neutron {
    fn default() -> Self {
        Self::new()
    }
}

/// Resolves on SIGINT or, on Unix, SIGTERM (FRAMEWORK_CONTRACT §8): process
/// managers and orchestrators stop services with SIGTERM, not Ctrl-C.
async fn default_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut terminate) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = terminate.recv() => {}
                }
            }
            Err(_) => {
                tokio::signal::ctrl_c().await.ok();
            }
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await.ok();
    }
}
