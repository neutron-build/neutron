"""Application class — the ASGI entry point."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Callable

from starlette.applications import Starlette
from starlette.datastructures import State
from starlette.middleware import Middleware
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, Response
from starlette.routing import Route

from neutron.error import AppError, handle_app_error, internal_error
from neutron.middleware import TrailingSlashMiddleware, _NeutronMiddleware
from neutron.openapi import SecurityScheme, generate_openapi
from neutron.router import Router

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

        # Auto-add trailing slash middleware when configured
        if trailing_slash is not None:
            self._middleware_config.insert(
                0, TrailingSlashMiddleware(action=trailing_slash)
            )

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
            is_nucleus = False
            db = neutron_app.db
            if db is None and hasattr(neutron_app.state, "db"):
                db = neutron_app.state.db
            if db is not None and hasattr(db, "features"):
                is_nucleus = db.features.is_nucleus
            return JSONResponse(
                {
                    "status": "ok",
                    "nucleus": is_nucleus,
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

        # Exception handlers
        exception_handlers: dict[Any, Callable] = {
            AppError: handle_app_error,
        }
        if not self._debug:

            async def handle_500(request: Request, exc: Exception) -> Response:
                err = internal_error("An internal error occurred")
                return err.to_response(instance=str(request.url.path))

            exception_handlers[500] = handle_500

        # Wrap lifespan to pass Neutron App instead of Starlette app
        lifespan = None
        if self._user_lifespan:
            user_lifespan = self._user_lifespan

            @asynccontextmanager
            async def lifespan_wrapper(
                starlette_app: Starlette,
            ) -> AsyncGenerator[None, None]:
                async with user_lifespan(neutron_app):
                    yield

            lifespan = lifespan_wrapper

        self._starlette = Starlette(
            routes=routes,
            middleware=starlette_middleware,
            exception_handlers=exception_handlers,
            lifespan=lifespan,
            debug=self._debug,
        )
        return self._starlette

    async def __call__(
        self, scope: dict, receive: Callable, send: Callable
    ) -> None:
        """ASGI interface."""
        # Share state between Neutron App and Starlette
        app = self._build_app()
        app.state = self.state
        await app(scope, receive, send)

    def run(
        self,
        host: str = "0.0.0.0",
        port: int = 8000,
        *,
        server: str = "uvicorn",
        workers: int = 1,
        **kwargs: Any,
    ) -> None:
        """Run the application server.

        Args:
            host: Bind address.
            port: Bind port.
            server: Server backend — ``"uvicorn"`` (default) or ``"granian"``
                (Rust/Tokio, faster, HTTP/2 support).
            workers: Number of worker processes.
            **kwargs: Passed through to the server.
        """
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
