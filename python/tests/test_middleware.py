"""Tests for built-in middleware."""

import pytest

from neutron import App, Router
from neutron.middleware import (
    CORSMiddleware,
    CompressionMiddleware,
    LoggingMiddleware,
    RateLimitMiddleware,
    RequestIDMiddleware,
    TimeoutMiddleware,
)
from neutron.test import TestClient


def _make_app(*middleware):
    app = App(title="MW Test", middleware=list(middleware))
    router = Router()

    @router.get("/ping")
    async def ping() -> dict:
        return {"ok": True}

    app.include_router(router)
    return app


@pytest.mark.asyncio
async def test_request_id_added():
    app = _make_app(RequestIDMiddleware())
    async with TestClient(app) as client:
        resp = await client.get("/ping")
        assert resp.status_code == 200
        assert "x-request-id" in resp.headers
        # UUID format
        rid = resp.headers["x-request-id"]
        assert len(rid) == 36


@pytest.mark.asyncio
async def test_cors_headers():
    app = _make_app(CORSMiddleware(allow_origins=["http://example.com"]))
    async with TestClient(app) as client:
        resp = await client.options(
            "/ping",
            headers={
                "origin": "http://example.com",
                "access-control-request-method": "GET",
            },
        )
        assert "access-control-allow-origin" in resp.headers


@pytest.mark.asyncio
async def test_logging_middleware_runs():
    """Smoke test: logging middleware doesn't crash."""
    app = _make_app(LoggingMiddleware())
    async with TestClient(app) as client:
        resp = await client.get("/ping")
        assert resp.status_code == 200


@pytest.mark.asyncio
async def test_compression_middleware():
    app = _make_app(CompressionMiddleware(minimum_size=1))
    async with TestClient(app) as client:
        resp = await client.get(
            "/ping", headers={"accept-encoding": "gzip"}
        )
        assert resp.status_code == 200


@pytest.mark.asyncio
async def test_rate_limit_middleware():
    app = _make_app(RateLimitMiddleware(rps=1.0, burst=2))
    async with TestClient(app) as client:
        # First two should succeed (burst=2)
        r1 = await client.get("/ping")
        r2 = await client.get("/ping")
        assert r1.status_code == 200
        assert r2.status_code == 200

        # Third should be rate limited
        r3 = await client.get("/ping")
        assert r3.status_code == 429
        data = r3.json()
        assert data["status"] == 429


@pytest.mark.asyncio
async def test_timeout_middleware():
    import asyncio

    timeout_app = App(title="TO Test", middleware=[TimeoutMiddleware(timeout=0.01)])
    router = Router()

    @router.get("/slow")
    async def slow() -> dict:
        await asyncio.sleep(1.0)
        return {"done": True}

    timeout_app.include_router(router)

    async with TestClient(timeout_app) as client:
        resp = await client.get("/slow")
        assert resp.status_code == 504


@pytest.mark.asyncio
async def test_stacked_middleware():
    """Multiple middleware work together."""
    app = _make_app(RequestIDMiddleware(), LoggingMiddleware())
    async with TestClient(app) as client:
        resp = await client.get("/ping")
        assert resp.status_code == 200
        assert "x-request-id" in resp.headers
