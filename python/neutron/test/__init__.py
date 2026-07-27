"""Testing helpers for Neutron applications.

Two clients are provided:

- :class:`TestClient` — **async**. An ``httpx.AsyncClient`` over
  ``ASGITransport``. Use as ``async with TestClient(app) as client:`` and
  ``await client.get(...)``. Matches the existing async ergonomics.
- :class:`SyncTestClient` — **synchronous**. An ``httpx.Client`` that drives
  the same ``ASGITransport`` through an anyio blocking portal. Use as
  ``with SyncTestClient(app) as client:`` and ``client.get(...)`` — no
  ``await``, no anyio/pytest markers. Mirrors Starlette/FastAPI's
  ``TestClient`` so test suites ported from FastAPI do not need to be
  rewritten as async.

Both clients are public API. The async client is unchanged.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import httpx
from anyio import from_thread
from httpx import ASGITransport, AsyncClient

if TYPE_CHECKING:
    from neutron import App


class TestClient:  # noqa: PT023
    __test__ = False  # Prevent pytest collection
    """Async HTTP test client for Neutron apps.

    Usage::

        async with TestClient(app) as client:
            resp = await client.get("/health")
            assert resp.status_code == 200
    """

    def __init__(self, app: App, base_url: str = "http://test") -> None:
        self._app = app
        self._base_url = base_url
        self._client: AsyncClient | None = None

    async def __aenter__(self) -> AsyncClient:
        transport = ASGITransport(app=self._app)
        self._client = AsyncClient(transport=transport, base_url=self._base_url)
        return self._client

    async def __aexit__(self, *args: object) -> None:
        if self._client:
            await self._client.aclose()


class _SyncASGITransport(httpx.BaseTransport):
    """Drive an :class:`httpx.ASGITransport` from sync code via a portal.

    Composes the stock httpx ASGI transport with an anyio blocking portal;
    it does not reimplement ASGI handling. Each sync request is delegated to
    the async transport's ``handle_async_request`` on the portal's event loop.
    The portal itself is owned and torn down by :class:`SyncTestClient`, so
    ``close`` here is a no-op (the underlying async transport's ``aclose``
    is a no-op as well).
    """

    def __init__(self, app: App, portal: from_thread.BlockingPortal) -> None:
        self._async_transport = ASGITransport(app=app)
        self._portal = portal

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        async_response = self._portal.call(
            self._async_transport.handle_async_request, request
        )
        # The async transport returns a response backed by an
        # ``AsyncByteStream``; a sync ``httpx.Client`` requires a
        # ``SyncByteStream``. Drain the body on the portal's loop and rebuild
        # the response so everything is usable from sync code.
        content = self._portal.call(async_response.aread)
        return httpx.Response(
            status_code=async_response.status_code,
            headers=async_response.headers,
            content=content,
            request=request,
            extensions=async_response.extensions,
        )


class SyncTestClient:  # noqa: PT023
    __test__ = False  # Prevent pytest collection
    """Synchronous HTTP test client for Neutron apps.

    Mirrors Starlette/FastAPI's ``TestClient``: an ``httpx.Client`` whose
    requests are driven through the ASGI app on a background event loop via
    an anyio blocking portal. No ``await``, no async test markers required.

    Usage::

        with SyncTestClient(app) as client:
            resp = client.get("/health")
            assert resp.status_code == 200
    """

    def __init__(self, app: App, base_url: str = "http://test") -> None:
        self._app = app
        self._base_url = base_url
        self._portal_cm: Any = None
        self._client: httpx.Client | None = None

    def __enter__(self) -> httpx.Client:
        self._portal_cm = from_thread.start_blocking_portal()
        portal = self._portal_cm.__enter__()
        transport = _SyncASGITransport(self._app, portal)
        self._client = httpx.Client(
            transport=transport, base_url=self._base_url
        )
        return self._client

    def __exit__(self, *args: object) -> None:
        if self._client:
            self._client.close()
            self._client = None
        if self._portal_cm is not None:
            self._portal_cm.__exit__(None, None, None)
            self._portal_cm = None


__all__ = ["SyncTestClient", "TestClient"]
