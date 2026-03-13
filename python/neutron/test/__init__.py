"""Testing helpers for Neutron applications."""

from __future__ import annotations

from typing import TYPE_CHECKING

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


__all__ = ["TestClient"]
