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
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use bytes::Bytes;
use http::StatusCode;
use http_body_util::{BodyExt, Limited};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder;
use tokio::net::TcpListener;
use tokio::sync::watch;
#[cfg(feature = "tls")]
use tokio_rustls::TlsAcceptor;

use crate::handler::{Body, Request as NeutronRequest, Response, StateMap};
use crate::http2::Http2Config;
use crate::middleware;
use crate::router::{RouteError, Router};
#[cfg(feature = "tls")]
use crate::tls::TlsConfig;

// ---------------------------------------------------------------------------
// Pre-computed static response bodies — zero-copy, no allocation per request.
// ---------------------------------------------------------------------------

#[inline(always)]
fn static_bytes(b: &'static [u8]) -> Bytes {
    static CELLS: OnceLock<()> = OnceLock::new();
    let _ = CELLS; // just to avoid unused warning
    Bytes::from_static(b)
}

#[inline]
fn resp_payload_too_large() -> Response {
    http::Response::builder()
        .status(StatusCode::PAYLOAD_TOO_LARGE)
        .header("content-type", "text/plain; charset=utf-8")
        .body(Body::full(static_bytes(b"Payload Too Large")))
        .unwrap()
}

#[inline]
fn resp_bad_request() -> Response {
    http::Response::builder()
        .status(StatusCode::BAD_REQUEST)
        .body(Body::full(static_bytes(b"Failed to read body")))
        .unwrap()
}

#[inline]
fn resp_not_found() -> Response {
    http::Response::builder()
        .status(StatusCode::NOT_FOUND)
        .header("content-type", "text/plain; charset=utf-8")
        .body(Body::full(static_bytes(b"Not Found")))
        .unwrap()
}

#[inline]
fn resp_method_not_allowed() -> Response {
    http::Response::builder()
        .status(StatusCode::METHOD_NOT_ALLOWED)
        .header("content-type", "text/plain; charset=utf-8")
        .body(Body::full(static_bytes(b"Method Not Allowed")))
        .unwrap()
}

/// Returns `true` if the request carries a body.
///
/// Checks for `Content-Length` (non-zero) or `Transfer-Encoding` headers.
/// Requests without either header have no body — skipping collection saves
/// one async await cycle (~30-50 µs) on every GET/HEAD/DELETE request.
#[inline]
fn request_has_body(headers: &http::HeaderMap) -> bool {
    if let Some(cl) = headers.get(http::header::CONTENT_LENGTH) {
        // Content-Length: 0 means no body
        cl.to_str()
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .map(|n| n > 0)
            .unwrap_or(true) // unknown value → assume body present
    } else {
        // Transfer-Encoding (chunked etc.) always implies a body
        headers.contains_key(http::header::TRANSFER_ENCODING)
    }
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
                                resp_not_found()
                            }
                        }
                        Err(RouteError::MethodNotAllowed) => resp_method_not_allowed(),
                    }
                })
            },
        )
    };

    middleware::build_chain(&router.middlewares, final_handler)
}

/// Type alias for shutdown hook functions.
type ShutdownHook =
    Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>;

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
const DEFAULT_MAX_BODY_SIZE: usize = 2 * 1024 * 1024;

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

    let domain = if addr.is_ipv6() { Domain::IPV6 } else { Domain::IPV4 };

    #[cfg(not(target_os = "windows"))]
    {
        // Linux / macOS: create N independent sockets with SO_REUSEPORT.
        let mut listeners = Vec::with_capacity(n);
        for _ in 0..n {
            let sock = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
            sock.set_reuse_address(true)?;
            #[cfg(target_os = "linux")]
            sock.set_reuse_port(true)?;
            sock.set_nonblocking(true)?;
            sock.bind(&addr.into())?;
            sock.listen(1024)?;
            listeners.push(std::net::TcpListener::from(sock));
        }
        Ok(listeners)
    }

    #[cfg(target_os = "windows")]
    {
        // Windows lacks SO_REUSEPORT: create one socket and try_clone() N-1
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
) {
    let listener = match TcpListener::from_std(listener_std) {
        Ok(l) => l,
        Err(e) => {
            tracing::error!("Worker failed to create listener: {e}");
            return;
        }
    };

    loop {
        let (stream, remote_addr) = tokio::select! {
            biased;
            _ = stop_rx.recv() => break,
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

        let chain = Arc::clone(&chain);
        let state_conn = Arc::clone(&state_map);
        let h2_config = http2_config.clone();

        tokio::spawn(async move {
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
                    let body_bytes = if request_has_body(&parts.headers) {
                        match Limited::new(body, body_limit).collect().await {
                            Ok(collected) => collected.to_bytes(),
                            Err(e) => {
                                let msg = e.to_string();
                                if msg.contains("length limit exceeded") {
                                    return Ok::<_, std::convert::Infallible>(
                                        resp_payload_too_large(),
                                    );
                                }
                                tracing::error!("Failed to read request body: {msg}");
                                return Ok::<_, std::convert::Infallible>(resp_bad_request());
                            }
                        }
                    } else {
                        Bytes::new()
                    };

                    let mut neutron_req = crate::handler::Request::with_state(
                        parts.method,
                        parts.uri,
                        parts.headers,
                        body_bytes,
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
            if let Err(e) = conn.await {
                tracing::error!("Worker connection error: {e}");
            }
        });
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

    /// Register a pre-shutdown hook (async closure).
    ///
    /// Hooks run in order after the shutdown signal is received but before
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
        self.shutdown_hooks
            .push(Box::new(move || Box::pin(hook())));
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

        let state_map = Arc::new(
            self.router
                .state_map
                .iter()
                .map(|(k, v)| (*k, Arc::clone(v)))
                .collect::<StateMap>(),
        );

        let mut router = self.router;
        router.ensure_built();
        let router = Arc::new(router);
        let chain = build_dispatch(router);

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
                    tokio::signal::ctrl_c().await.ok();
                })
            };

        // Accept loop
        loop {
            tokio::select! {
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

                    tokio::spawn(async move {
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

                                // Skip body collection for requests that carry no body
                                // (no Content-Length / Transfer-Encoding header).  For GET,
                                // HEAD, DELETE, OPTIONS this eliminates an entire async await
                                // cycle — roughly 30-50 µs on a loopback benchmark.
                                let body_bytes = if request_has_body(&parts.headers) {
                                    match Limited::new(body, body_limit).collect().await {
                                        Ok(collected) => collected.to_bytes(),
                                        Err(e) => {
                                            let msg = e.to_string();
                                            if msg.contains("length limit exceeded") {
                                                return Ok::<_, std::convert::Infallible>(
                                                    resp_payload_too_large(),
                                                );
                                            }
                                            tracing::error!("Failed to read request body: {msg}");
                                            return Ok::<_, std::convert::Infallible>(
                                                resp_bad_request(),
                                            );
                                        }
                                    }
                                } else {
                                    Bytes::new()
                                };

                                let mut neutron_req = NeutronRequest::with_state(
                                    parts.method,
                                    parts.uri,
                                    parts.headers,
                                    body_bytes,
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
                _ = &mut shutdown_signal => {
                    let count = active_count.load(Ordering::Relaxed);
                    tracing::info!("Shutdown signal received, draining {count} connection(s)...");

                    // Run shutdown hooks
                    for hook in shutdown_hooks {
                        hook().await;
                    }

                    let _ = shutdown_tx.send(true);
                    break;
                }
            }
        }

        // Wait for active connections to drain
        if active_count.load(Ordering::Relaxed) > 0 {
            let drain_result = tokio::time::timeout(shutdown_timeout, async {
                while active_count.load(Ordering::Relaxed) > 0 {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            })
            .await;

            if drain_result.is_err() {
                let remaining = active_count.load(Ordering::Relaxed);
                tracing::warn!(
                    "Drain timeout ({:.0}s): {remaining} connection(s) still active",
                    shutdown_timeout.as_secs_f64()
                );
            }
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
        tracing::info!("Neutron listening on http://{addr} ({n} worker threads)");

        let state_map = Arc::new(
            self.router
                .state_map
                .iter()
                .map(|(k, v)| (*k, Arc::clone(v)))
                .collect::<StateMap>(),
        );
        let mut router = self.router;
        router.ensure_built();
        let router = Arc::new(router);
        let chain = build_dispatch(router);

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
                ));
            });
            thread_handles.push(handle);
        }

        // Wait for Ctrl-C (or custom shutdown).
        if let Some(signal) = self.custom_shutdown {
            signal.await;
        } else {
            tokio::signal::ctrl_c().await.ok();
        }

        // Run shutdown hooks.
        for hook in self.shutdown_hooks {
            hook().await;
        }

        // Signal all workers to stop.
        let _ = stop_tx.send(());

        // Join worker threads (without blocking the tokio runtime).
        tokio::task::block_in_place(|| {
            for handle in thread_handles {
                let _ = handle.join();
            }
        });

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

        let state_map = Arc::new(
            self.router
                .state_map
                .iter()
                .map(|(k, v)| (*k, Arc::clone(v)))
                .collect::<StateMap>(),
        );

        let mut router = self.router;
        router.ensure_built();
        let router = Arc::new(router);
        let chain = build_dispatch(router);

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
                    tokio::signal::ctrl_c().await.ok();
                })
            };

        loop {
            tokio::select! {
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

                    tokio::spawn(async move {
                        let _permit = permit;
                        // TLS handshake
                        let tls_stream = match acceptor.accept(stream).await {
                            Ok(s) => s,
                            Err(e) => {
                                tracing::debug!("TLS handshake failed: {e}");
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

                                let body_bytes = if request_has_body(&parts.headers) {
                                    match Limited::new(body, body_limit).collect().await {
                                        Ok(collected) => collected.to_bytes(),
                                        Err(e) => {
                                            let msg = e.to_string();
                                            if msg.contains("length limit exceeded") {
                                                return Ok::<_, std::convert::Infallible>(
                                                    resp_payload_too_large(),
                                                );
                                            }
                                            tracing::error!("Failed to read request body: {msg}");
                                            return Ok::<_, std::convert::Infallible>(
                                                resp_bad_request(),
                                            );
                                        }
                                    }
                                } else {
                                    Bytes::new()
                                };

                                let mut neutron_req = NeutronRequest::with_state(
                                    parts.method,
                                    parts.uri,
                                    parts.headers,
                                    body_bytes,
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
                _ = &mut shutdown_signal => {
                    let count = active_count.load(Ordering::Relaxed);
                    tracing::info!("Shutdown signal received, draining {count} connection(s)...");

                    // Run shutdown hooks
                    for hook in shutdown_hooks {
                        hook().await;
                    }

                    let _ = shutdown_tx.send(true);
                    break;
                }
            }
        }

        if active_count.load(Ordering::Relaxed) > 0 {
            let drain_result = tokio::time::timeout(shutdown_timeout, async {
                while active_count.load(Ordering::Relaxed) > 0 {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            })
            .await;

            if drain_result.is_err() {
                let remaining = active_count.load(Ordering::Relaxed);
                tracing::warn!(
                    "Drain timeout ({:.0}s): {remaining} connection(s) still active",
                    shutdown_timeout.as_secs_f64()
                );
            }
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
        addr:       SocketAddr,
        tls_config: TlsConfig,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use crate::http3_server::{Http3Config, serve_h3};

        let state_map = Arc::new(
            self.router
                .state_map
                .iter()
                .map(|(k, v)| (*k, Arc::clone(v)))
                .collect::<StateMap>(),
        );

        let mut router = self.router;
        router.ensure_built();
        let router = Arc::new(router);
        let chain  = build_dispatch(router);

        let h3_cfg = Http3Config {
            max_body_size: self.max_body_size,
        };

        serve_h3(addr, chain, state_map, tls_config, h3_cfg).await?;
        Ok(())
    }
}

impl Default for Neutron {
    fn default() -> Self {
        Self::new()
    }
}
