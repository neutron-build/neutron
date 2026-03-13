"""Tests for neutron/cache — tiered cache and HTTP cache middleware."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from pydantic import BaseModel
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route
from starlette.middleware import Middleware

from neutron.cache.tiered import TieredCache
from neutron.cache.http import HTTPCacheMiddleware


# ============================================================================
# Tiered Cache
# ============================================================================


class TestTieredCache:
    async def test_set_and_get(self):
        cache = TieredCache()
        await cache.set("key", "value")
        assert await cache.get("key") == "value"

    async def test_get_missing(self):
        cache = TieredCache()
        assert await cache.get("nonexistent") is None

    async def test_invalidate(self):
        cache = TieredCache()
        await cache.set("key", "value")
        await cache.invalidate("key")
        assert await cache.get("key") is None

    async def test_clear(self):
        cache = TieredCache()
        await cache.set("a", 1)
        await cache.set("b", 2)
        await cache.clear()
        assert cache.l1_size == 0

    async def test_l1_size(self):
        cache = TieredCache()
        await cache.set("a", 1)
        await cache.set("b", 2)
        assert cache.l1_size == 2

    async def test_l1_eviction(self):
        cache = TieredCache(l1_max_size=2)
        await cache.set("a", 1)
        await cache.set("b", 2)
        await cache.set("c", 3)
        assert cache.l1_size == 2
        # "a" should be evicted (LRU)
        assert await cache.get("a") is None
        assert await cache.get("b") == 2
        assert await cache.get("c") == 3

    async def test_dict_value(self):
        cache = TieredCache()
        await cache.set("data", {"x": 1, "y": [2, 3]})
        result = await cache.get("data")
        assert result == {"x": 1, "y": [2, 3]}

    async def test_pydantic_model_value(self):
        class Item(BaseModel):
            name: str
            price: float

        cache = TieredCache()
        await cache.set("item", Item(name="Widget", price=9.99))
        result = await cache.get("item", model=Item)
        assert isinstance(result, Item)
        assert result.name == "Widget"
        assert result.price == 9.99

    async def test_l2_fallback(self):
        """When L1 misses, L2 is checked and value promoted to L1."""
        mock_kv = MagicMock()
        mock_kv.get = AsyncMock(return_value='{"name": "from-l2"}')
        mock_kv.set = AsyncMock()

        cache = TieredCache(l2=mock_kv)

        # Not in L1, should fetch from L2
        result = await cache.get("key")
        assert result == {"name": "from-l2"}
        mock_kv.get.assert_called_once_with("cache:key")

        # Should now be promoted to L1
        mock_kv.get.reset_mock()
        result = await cache.get("key")
        assert result == {"name": "from-l2"}
        mock_kv.get.assert_not_called()  # Served from L1

    async def test_l2_write_through(self):
        """Setting a value writes to both L1 and L2."""
        mock_kv = MagicMock()
        mock_kv.set = AsyncMock()

        cache = TieredCache(l2=mock_kv)
        await cache.set("key", {"x": 1}, ttl=60)

        mock_kv.set.assert_called_once()
        call_args = mock_kv.set.call_args
        assert "cache:key" in call_args[0]

    async def test_l2_invalidate(self):
        mock_kv = MagicMock()
        mock_kv.set = AsyncMock()
        mock_kv.delete = AsyncMock()

        cache = TieredCache(l2=mock_kv)
        await cache.set("key", "val")
        await cache.invalidate("key")
        mock_kv.delete.assert_called_once_with("cache:key")

    async def test_custom_prefix(self):
        mock_kv = MagicMock()
        mock_kv.set = AsyncMock()
        mock_kv.get = AsyncMock(return_value=None)

        cache = TieredCache(l2=mock_kv, prefix="myapp:")
        await cache.set("key", "val")
        call_args = mock_kv.set.call_args
        assert "myapp:key" in call_args[0]


# ============================================================================
# HTTP Cache Middleware
# ============================================================================


class TestHTTPCacheMiddleware:
    async def test_cache_miss_then_hit(self):
        call_count = 0

        async def endpoint(request: Request) -> JSONResponse:
            nonlocal call_count
            call_count += 1
            return JSONResponse({"count": call_count})

        cache = TieredCache()
        mw = HTTPCacheMiddleware(cache=cache, ttl=60).as_starlette_middleware()
        app = Starlette(
            routes=[Route("/data", endpoint)],
            middleware=[mw],
        )

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            # First request — MISS
            resp1 = await client.get("/data")
            assert resp1.status_code == 200
            assert resp1.json()["count"] == 1
            assert resp1.headers.get("x-cache") == "MISS"

            # Second request — HIT
            resp2 = await client.get("/data")
            assert resp2.status_code == 200
            assert resp2.json()["count"] == 1  # Same cached response
            assert resp2.headers.get("x-cache") == "HIT"

        assert call_count == 1  # Only one actual handler call

    async def test_no_cache_for_post(self):
        call_count = 0

        async def endpoint(request: Request) -> JSONResponse:
            nonlocal call_count
            call_count += 1
            return JSONResponse({"count": call_count})

        cache = TieredCache()
        mw = HTTPCacheMiddleware(cache=cache).as_starlette_middleware()
        app = Starlette(
            routes=[Route("/data", endpoint, methods=["POST"])],
            middleware=[mw],
        )

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            await client.post("/data")
            await client.post("/data")
        assert call_count == 2

    async def test_excluded_paths(self):
        call_count = 0

        async def endpoint(request: Request) -> JSONResponse:
            nonlocal call_count
            call_count += 1
            return JSONResponse({"count": call_count})

        cache = TieredCache()
        mw = HTTPCacheMiddleware(
            cache=cache, exclude_paths=["/no-cache"]
        ).as_starlette_middleware()
        app = Starlette(
            routes=[Route("/no-cache", endpoint)],
            middleware=[mw],
        )

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            await client.get("/no-cache")
            await client.get("/no-cache")
        assert call_count == 2

    async def test_different_query_strings_separate_cache(self):
        async def endpoint(request: Request) -> JSONResponse:
            q = request.query_params.get("q", "")
            return JSONResponse({"q": q})

        cache = TieredCache()
        mw = HTTPCacheMiddleware(cache=cache).as_starlette_middleware()
        app = Starlette(
            routes=[Route("/search", endpoint)],
            middleware=[mw],
        )

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            resp1 = await client.get("/search?q=foo")
            resp2 = await client.get("/search?q=bar")
            assert resp1.json()["q"] == "foo"
            assert resp2.json()["q"] == "bar"


# ============================================================================
# Cache __init__ exports
# ============================================================================


class TestCacheExports:
    def test_all_exports(self):
        from neutron.cache import TieredCache, HTTPCacheMiddleware
        assert TieredCache is not None
        assert HTTPCacheMiddleware is not None
