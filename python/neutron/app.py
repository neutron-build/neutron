"""Application class — the ASGI entry point."""

from __future__ import annotations

import asyncio
import os
import signal
import sys
import threading
import time
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Callable, Coroutine

import structlog
from starlette.applications import Starlette
from starlette.datastructures import State
from starlette.exceptions import HTTPException
from starlette.middleware import Middleware
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, Response
from starlette.routing import Route
from starlette.types import Receive, Scope, Send

from neutron.error import (
    AppError,
    handle_app_error,
    handle_http_exception,
    internal_error,
)
from neutron.middleware import TrailingSlashMiddleware, _NeutronMiddleware
from neutron.openapi import SecurityScheme, generate_openapi
from neutron.router import Router

logger = structlog.get_logger("neutron.lifecycle")

_STOP_SIGNALS = (signal.SIGTERM, signal.SIGINT)

_DUPLICATE_SIGNAL_WINDOW = 1.0
"""Seconds after the first stop signal during which further signals are the
same stop request, not a demand to force. A terminal Ctrl-C reaches every
process in the group, so under ``neutron dev`` the worker gets SIGINT from the
tty and then SIGTERM from uvicorn's reloader a few milliseconds later; that
pair must still shut down gracefully."""


class _StopSignals:
    """Graceful-stop bookkeeping chained in front of the server's own handlers.

    Contract §8. The server (uvicorn) owns the shutdown sequence: it stops
    accepting, waits for open connections, sends lifespan shutdown and returns.
    Neutron used to replace uvicorn's handlers with one that only set a flag, so
    uvicorn never learned a signal had arrived and the process could only be
    stopped with SIGKILL. This wraps instead of replacing:

    * First signal: mark the app as stopping (new requests on open connections
      get 503), arm the drain deadline, then hand the signal to the previous
      handler so the server runs its own graceful shutdown. Lifespan shutdown
      then runs the OnStop hooks and closes the database.
    * Later signals (outside the duplicate window): exit immediately.

    The previous handler is always told SIGINT. uvicorn re-raises the signal
    it captured once it has finished shutting down, under the handler that was
    installed before it started; for SIGTERM that is SIG_DFL, which turns a
    completed graceful shutdown into death-by-signal (status 143). A re-raised
    SIGINT becomes KeyboardInterrupt, which uvicorn's ``run()`` and its reload
    / worker subprocess entry points already swallow, so the process exits 0.
    Both signals mean the same thing to uvicorn's ``handle_exit`` on first
    delivery.

    Only installed on the main thread, and only over a Python-level handler:
    a signal left at SIG_DFL/SIG_IGN is not claimed on the server's behalf.
    """

    def __init__(self, app: "App", loop: asyncio.AbstractEventLoop) -> None:
        self._app = app
        self._loop = loop
        self._previous: dict[int, Callable[..., Any]] = {}
        self._first_at: float | None = None

    def install(self) -> None:
        if threading.current_thread() is not threading.main_thread():
            return
        for sig in _STOP_SIGNALS:
            previous = signal.getsignal(sig)
            if not callable(previous):
                continue
            self._previous[sig] = previous
            signal.signal(sig, self._handle)

    def uninstall(self) -> None:
        for sig, previous in self._previous.items():
            if signal.getsignal(sig) == self._handle:
                signal.signal(sig, previous)
        self._previous.clear()

    def _handle(self, signum: int, frame: Any) -> None:
        name = signal.Signals(signum).name
        now = time.monotonic()
        if self._first_at is None:
            self._first_at = now
            logger.info("shutdown_signal_received", signal=name)
            self._app._begin_stop(self._loop)
            self._previous[signum](signal.SIGINT, frame)
            return
        if now - self._first_at < _DUPLICATE_SIGNAL_WINDOW:
            return
        logger.warning("forced_exit", signal=name, inflight=self._app._inflight)
        for stream in (sys.stdout, sys.stderr):
            try:
                stream.flush()
            except Exception:
                pass
        os._exit(128 + signum)


_SWAGGER_HTML = """<!DOCTYPE html>
<html>
<head>
    <title>{title} — API Docs</title>
    <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css">
</head>
<body>
    <div id="swagger-ui"></div>
    <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
    <script>SwaggerUIBundle({{url: "/openapi.json", dom_id: "#swagger-ui"}})</script>
</body>
</html>"""



async def _dependency_reachable(db: Any, *, timeout: float = 2.0) -> bool:
    """Is the configured database actually answering?

    Contract §7 distinguishes connected from disconnected by health, which
    means something has to ask. A cheap SELECT 1 under a short timeout is that
    question; any failure is unhealthy rather than an exception, because a
    health endpoint that raises cannot report degradation.
    """
    pool = getattr(db, "pool", None)
    if pool is None:
        sql = getattr(db, "sql", None)
        pool = getattr(sql, "_pool", None)
    if pool is None:
        # Configured with something that exposes no pool. It was handed to us
        # as the dependency, so treat its presence as the only signal we have.
        return True
    try:
        await asyncio.wait_for(pool.fetchval("SELECT 1"), timeout)
    except Exception:
        return False
    return True


def _middleware_type_error(entry: Any) -> TypeError:
    """Error for a ``middleware=`` entry App cannot use.

    Bare middleware classes are rejected rather than auto-instantiated:
    zero-argument construction is not guaranteed for every middleware
    (e.g. JWTMiddleware requires a secret), so adapting would just move the
    failure into a confusing ``__init__`` traceback. Every documented
    example and ``default_stack`` passes configured instances — keep that
    contract explicit.
    """
    if isinstance(entry, type) and issubclass(entry, _NeutronMiddleware):
        return TypeError(
            f"Invalid middleware entry: {entry.__name__} is a class, not an "
            f"instance. Construct it with your configuration, e.g. "
            f"{entry.__name__}()."
        )
    return TypeError(
        f"Invalid middleware entry: {entry!r}. Expected a Neutron middleware "
        f"instance (e.g. RequestIDMiddleware()) or a "
        f"starlette.middleware.Middleware instance."
    )


class App:
    """Neutron application.

    Wraps Starlette, adds typed routing, OpenAPI generation, and
    RFC 7807 error handling.

    Usage::

        from neutron import App, Router

        app = App(title="My API", version="1.0.0")
        router = Router()

        @router.get("/hello")
        async def hello() -> dict:
            return {"message": "Hello, world!"}

        app.include_router(router)
    """

    _DRAIN_TIMEOUT: float = 30.0
    """Maximum seconds to wait for in-flight requests during shutdown."""

    def __init__(
        self,
        title: str = "Neutron App",
        version: str = "0.1.0",
        middleware: list[Any] | None = None,
        lifespan: Callable | None = None,
        debug: bool = False,
        security_schemes: dict[str, SecurityScheme] | None = None,
        security: list[dict[str, list[str]]] | None = None,
        trailing_slash: str | None = None,
        drain_timeout: float = 30.0,
    ) -> None:
        self.router = Router()
        self._title = title
        self._version = version
        self._middleware_config: list[Any] = list(middleware or [])
        self._user_lifespan = lifespan
        self._debug = debug
        self._starlette: Starlette | None = None
        self._routers: list[tuple[Router, str]] = []
        self.state = State()
        self.db: Any = None  # Set after NucleusClient.connect()
        self._security_schemes = security_schemes
        self._security = security
        self._drain_timeout = drain_timeout

        # Shutdown state
        self._on_stop_hooks: list[Callable[[], Coroutine[Any, Any, None]]] = []
        self._shutting_down = False
        self._inflight = 0
        self._inflight_zero: asyncio.Event | None = None
        self._inflight_tasks: set[asyncio.Task[Any]] = set()
        self._stop_deadline: float | None = None
        self._drain_timer: asyncio.TimerHandle | None = None

        # Auto-add trailing slash middleware when configured
        if trailing_slash is not None:
            self._middleware_config.insert(
                0, TrailingSlashMiddleware(action=trailing_slash)
            )

        # Reject unrecognised middleware at construction. _build_app used to
        # silently drop anything it did not recognise, so a typo'd or bare
        # class entry vanished with no error while the app served without it.
        for mw in self._middleware_config:
            if not isinstance(mw, (_NeutronMiddleware, Middleware)):
                raise _middleware_type_error(mw)

    def on_stop(
        self, func: Callable[[], Coroutine[Any, Any, None]],
    ) -> Callable[[], Coroutine[Any, Any, None]]:
        """Register an async shutdown hook. Hooks run in reverse order.

        Can be used as a decorator::

            @app.on_stop
            async def cleanup():
                await some_resource.close()
        """
        self._on_stop_hooks.append(func)
        return func

    def _begin_stop(self, loop: asyncio.AbstractEventLoop) -> None:
        """Enter draining: refuse new requests and bound the drain.

        Called from a signal handler, so it only sets plain attributes and
        hands the timer to the loop through the signal-safe
        ``call_soon_threadsafe``.
        """
        self._shutting_down = True
        if self._stop_deadline is None:
            self._stop_deadline = time.monotonic() + self._drain_timeout
        try:
            loop.call_soon_threadsafe(self._arm_drain_deadline)
        except RuntimeError:
            pass  # loop already closed

    def _arm_drain_deadline(self) -> None:
        if self._drain_timer is not None or self._stop_deadline is None:
            return
        remaining = max(0.0, self._stop_deadline - time.monotonic())
        loop = asyncio.get_running_loop()
        self._drain_timer = loop.call_later(remaining, self._drain_expired)

    def _drain_expired(self) -> None:
        """The drain timeout ran out: cancel what is still in flight.

        The server will not finish its own shutdown while a request is open
        (uvicorn's graceful timeout is unbounded by default), so the app
        enforces ``drain_timeout`` itself.
        """
        if not self._inflight_tasks:
            return
        logger.warning(
            "drain_timeout_exceeded",
            remaining=len(self._inflight_tasks),
            timeout=self._drain_timeout,
        )
        for task in list(self._inflight_tasks):
            task.cancel("neutron drain timeout exceeded")

    def include_router(self, router: Router, prefix: str = "") -> None:
        """Mount an external Router with an optional path prefix."""
        self._routers.append((router, prefix))

    @property
    def openapi(self) -> dict[str, Any]:
        """Auto-generated OpenAPI 3.1 spec."""
        handler_info = list(self.router.get_handler_info())
        for router, prefix in self._routers:
            handler_info.extend(router.get_handler_info(prefix))
        return generate_openapi(
            self._title,
            self._version,
            handler_info,
            security_schemes=self._security_schemes,
            security=self._security,
        )

    def _build_app(self) -> Starlette:
        if self._starlette is not None:
            return self._starlette

        # Collect routes
        routes = list(self.router.get_routes())
        for router, prefix in self._routers:
            routes.extend(router.get_routes(prefix))

        # Built-in routes
        neutron_app = self

        async def health_endpoint(request: Request) -> JSONResponse:
            db = neutron_app.db
            if db is None and hasattr(neutron_app.state, "db"):
                db = neutron_app.state.db
            # Contract §7: nucleus reflects the HEALTH of the dependency, and
            # says plainly that feature detection -- Nucleus versus plain
            # Postgres -- is §1 and not this endpoint. Reporting
            # "disconnected" for a healthy Postgres both violates that and
            # makes the field useless for monitoring: it pages for a working
            # system, and a genuinely unreachable database is indistinguishable
            # from the normal case. It also left "disconnected" unreachable in
            # practice, since nothing here ever probed the connection.
            status = "ok"
            if db is None:
                nucleus = "unconfigured"
            elif await _dependency_reachable(db):
                nucleus = "connected"
            else:
                nucleus = "disconnected"
                status = "degraded"
            return JSONResponse(
                {
                    "status": status,
                    "nucleus": nucleus,
                    "version": neutron_app._version,
                }
            )

        async def openapi_endpoint(request: Request) -> JSONResponse:
            return JSONResponse(neutron_app.openapi)

        async def docs_endpoint(request: Request) -> HTMLResponse:
            return HTMLResponse(
                _SWAGGER_HTML.format(title=neutron_app._title)
            )

        routes.append(Route("/health", endpoint=health_endpoint, methods=["GET"]))
        routes.append(Route("/openapi.json", endpoint=openapi_endpoint, methods=["GET"]))
        routes.append(Route("/docs", endpoint=docs_endpoint, methods=["GET"]))

        # Convert middleware
        starlette_middleware: list[Middleware] = []
        for mw in self._middleware_config:
            if isinstance(mw, _NeutronMiddleware):
                starlette_middleware.append(mw.as_starlette_middleware())
            elif isinstance(mw, Middleware):
                starlette_middleware.append(mw)
            else:
                raise _middleware_type_error(mw)

        # Exception handlers
        exception_handlers: dict[Any, Callable] = {
            AppError: handle_app_error,
            HTTPException: handle_http_exception,
        }
        if not self._debug:

            async def handle_500(request: Request, exc: Exception) -> Response:
                err = internal_error("An internal error occurred")
                return err.to_response(instance=str(request.url.path))

            exception_handlers[500] = handle_500

        # Build lifespan with graceful shutdown support
        user_lifespan = self._user_lifespan

        @asynccontextmanager
        async def lifespan_wrapper(
            starlette_app: Starlette,
        ) -> AsyncGenerator[None, None]:
            neutron_app._inflight_zero = asyncio.Event()
            neutron_app._inflight_zero.set()  # No requests yet
            neutron_app._shutting_down = False
            neutron_app._stop_deadline = None
            neutron_app._drain_timer = None

            stop_signals = _StopSignals(neutron_app, asyncio.get_running_loop())
            stop_signals.install()
            try:
                if user_lifespan is not None:
                    async with user_lifespan(neutron_app):
                        yield
                else:
                    yield
                await neutron_app._shutdown_sequence()
            finally:
                stop_signals.uninstall()

        lifespan = lifespan_wrapper

        self._starlette = Starlette(
            routes=routes,
            middleware=starlette_middleware,
            exception_handlers=exception_handlers,
            lifespan=lifespan,
            debug=self._debug,
        )
        return self._starlette

    async def _shutdown_sequence(self) -> None:
        """Contract §8 steps 3-5, run from lifespan shutdown."""
        logger.info("shutdown_started", drain_timeout=self._drain_timeout)
        self._shutting_down = True
        if self._stop_deadline is None:
            self._stop_deadline = time.monotonic() + self._drain_timeout

        # 1. Wait for in-flight requests, within what is left of the drain
        # budget. Under uvicorn this is normally already zero: the server
        # waits for open connections before sending lifespan shutdown.
        if self._inflight > 0 and self._inflight_zero is not None:
            logger.info("draining_inflight_requests", count=self._inflight)
            remaining = max(0.0, self._stop_deadline - time.monotonic())
            try:
                await asyncio.wait_for(self._inflight_zero.wait(), timeout=remaining)
                logger.info("inflight_requests_drained")
            except asyncio.TimeoutError:
                self._drain_expired()
        if self._drain_timer is not None:
            self._drain_timer.cancel()
            self._drain_timer = None

        # 2. Run on_stop hooks in reverse registration order
        for hook in reversed(self._on_stop_hooks):
            hook_name = getattr(hook, "__name__", repr(hook))
            try:
                logger.info("running_stop_hook", hook=hook_name)
                await hook()
            except Exception:
                logger.exception("stop_hook_failed", hook=hook_name)

        # 3. Close database connection pool
        db = self.db
        if db is None and hasattr(self.state, "db"):
            db = self.state.db
        if db is not None and hasattr(db, "close"):
            try:
                logger.info("closing_database_pool")
                await db.close()
            except Exception:
                logger.exception("database_close_failed")

        logger.info("server_stopped")

    async def __call__(
        self, scope: Scope, receive: Receive, send: Send
    ) -> None:
        """ASGI interface with in-flight request tracking."""
        app = self._build_app()
        app.state = self.state

        # Only track HTTP/WebSocket requests
        if scope["type"] in ("http", "websocket"):
            # Reject new connections during shutdown
            if self._shutting_down:
                if scope["type"] == "http":
                    resp = JSONResponse(
                        status_code=503,
                        content={
                            "type": "https://neutron.dev/errors/shutting-down",
                            "title": "Service Unavailable",
                            "status": 503,
                            "detail": "Server is shutting down",
                        },
                        media_type="application/problem+json",
                        headers={"Connection": "close", "Retry-After": "5"},
                    )
                    await resp(scope, receive, send)
                    return

            # Track in-flight requests
            self._inflight += 1
            if self._inflight_zero is not None:
                self._inflight_zero.clear()
            task = asyncio.current_task()
            if task is not None:
                self._inflight_tasks.add(task)
            try:
                await app(scope, receive, send)
            finally:
                if task is not None:
                    self._inflight_tasks.discard(task)
                self._inflight -= 1
                if self._inflight <= 0 and self._inflight_zero is not None:
                    self._inflight_zero.set()
        else:
            await app(scope, receive, send)

    def run(
        self,
        host: str | None = None,
        port: int | None = None,
        *,
        server: str = "uvicorn",
        workers: int | None = None,
        **kwargs: Any,
    ) -> None:
        """Run the application server.

        Args:
            host: Bind address. Defaults to ``NEUTRON_HOST``, then ``0.0.0.0``.
            port: Bind port. Defaults to ``NEUTRON_PORT``, then ``8000``.
            server: Server backend — ``"uvicorn"`` (default) or ``"granian"``
                (Rust/Tokio, faster, HTTP/2 support).
            workers: Number of worker processes. Defaults to
                ``NEUTRON_WORKERS``, then ``1``.
            **kwargs: Passed through to the server.

        Precedence is explicit argument > environment (``NEUTRON_`` prefix)
        > default. Logging is configured from ``NEUTRON_LOG_LEVEL`` /
        ``NEUTRON_LOG_FORMAT`` via :func:`neutron.config.configure_logging`.
        """
        from neutron.config import configure_logging, server_settings

        config = server_settings()
        if host is None:
            host = config.host
        if port is None:
            port = config.port
        if workers is None:
            workers = config.workers
        configure_logging(config)

        if server == "granian":
            try:
                from granian import Granian
                from granian.constants import Interfaces

                g = Granian(
                    self,
                    address=host,
                    port=port,
                    workers=workers,
                    interface=Interfaces.ASGI,
                    **kwargs,
                )
                g.serve()
            except ImportError:
                raise ImportError(
                    "Granian server not installed. Install it: pip install granian"
                )
        else:
            import uvicorn

            uvicorn.run(self, host=host, port=port, workers=workers, **kwargs)
