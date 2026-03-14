"""Tests for built-in middleware."""

import asyncio
import time

import pytest

from neutron import App, Router
from neutron.auth.csrf import CSRFMiddleware
from neutron.middleware import (
    CORSMiddleware,
    CompressionMiddleware,
    LoggingMiddleware,
    RateLimitMiddleware,
    RequestIDMiddleware,
    TimeoutMiddleware,
    _default_key_func,
)
from neutron.test import TestClient


def _make_app(*middleware):
    app = App(title="MW Test", middleware=list(middleware))
    router = Router()

    @router.get("/ping")
    async def ping() -> dict:
        return {"ok": True}

    @router.post("/submit")
    async def submit() -> dict:
        return {"submitted": True}

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


# --------------------------------------------------------------------------
# Per-IP Rate Limiting Tests
# --------------------------------------------------------------------------


class TestPerIPRateLimit:
    """Verify per-IP token bucket semantics."""

    @pytest.mark.asyncio
    async def test_different_ips_have_separate_buckets(self):
        """Two different IPs should each get their own burst budget."""
        app = _make_app(RateLimitMiddleware(rps=1.0, burst=1))
        async with TestClient(app) as client:
            # IP 1 sends one request — should succeed
            r1 = await client.get(
                "/ping", headers={"x-forwarded-for": "10.0.0.1"}
            )
            assert r1.status_code == 200

            # IP 2 sends one request — should also succeed (separate bucket)
            r2 = await client.get(
                "/ping", headers={"x-forwarded-for": "10.0.0.2"}
            )
            assert r2.status_code == 200

            # IP 1 sends another — should be rate-limited (burst=1 exhausted)
            r3 = await client.get(
                "/ping", headers={"x-forwarded-for": "10.0.0.1"}
            )
            assert r3.status_code == 429

    @pytest.mark.asyncio
    async def test_x_real_ip_extraction(self):
        """X-Real-IP header is respected when X-Forwarded-For is absent."""
        app = _make_app(RateLimitMiddleware(rps=1.0, burst=1))
        async with TestClient(app) as client:
            r1 = await client.get(
                "/ping", headers={"x-real-ip": "192.168.1.1"}
            )
            assert r1.status_code == 200

            # Different IP via X-Real-IP should get a fresh bucket
            r2 = await client.get(
                "/ping", headers={"x-real-ip": "192.168.1.2"}
            )
            assert r2.status_code == 200

    @pytest.mark.asyncio
    async def test_xff_takes_first_hop(self):
        """X-Forwarded-For extracts the leftmost (client) IP."""
        app = _make_app(RateLimitMiddleware(rps=1.0, burst=1))
        async with TestClient(app) as client:
            r1 = await client.get(
                "/ping",
                headers={"x-forwarded-for": "1.2.3.4, 10.0.0.1, 10.0.0.2"},
            )
            assert r1.status_code == 200

            # Same client IP (first hop) — should be limited
            r2 = await client.get(
                "/ping",
                headers={"x-forwarded-for": "1.2.3.4, 10.0.0.99"},
            )
            assert r2.status_code == 429

    @pytest.mark.asyncio
    async def test_custom_key_func(self):
        """Custom key function groups requests differently."""
        # Key by a custom header instead of IP
        def key_by_tenant(scope):
            for name, value in scope.get("headers", []):
                if name.lower() == b"x-tenant-id":
                    return value.decode()
            return "default"

        app = _make_app(
            RateLimitMiddleware(rps=1.0, burst=1, key_func=key_by_tenant)
        )
        async with TestClient(app) as client:
            r1 = await client.get(
                "/ping", headers={"x-tenant-id": "tenant-a"}
            )
            assert r1.status_code == 200

            # Different tenant — separate bucket
            r2 = await client.get(
                "/ping", headers={"x-tenant-id": "tenant-b"}
            )
            assert r2.status_code == 200

            # Same tenant — limited
            r3 = await client.get(
                "/ping", headers={"x-tenant-id": "tenant-a"}
            )
            assert r3.status_code == 429

    @pytest.mark.asyncio
    async def test_rate_limit_rfc7807_format(self):
        """429 response uses RFC 7807 application/problem+json."""
        app = _make_app(RateLimitMiddleware(rps=1.0, burst=1))
        async with TestClient(app) as client:
            await client.get("/ping")  # exhaust burst
            r = await client.get("/ping")
            assert r.status_code == 429
            data = r.json()
            assert data["type"] == "https://neutron.dev/errors/rate-limited"
            assert data["title"] == "Rate Limited"
            assert data["status"] == 429
            assert data["detail"] == "Too many requests"

    @pytest.mark.asyncio
    async def test_stale_bucket_cleanup(self):
        """Stale buckets are cleaned up after the configured interval."""
        app = _make_app(
            RateLimitMiddleware(
                rps=1.0,
                burst=1,
                cleanup_interval=0.0,  # clean up every call
                stale_after=0.0,       # immediately stale
            )
        )
        async with TestClient(app) as client:
            r1 = await client.get(
                "/ping", headers={"x-forwarded-for": "10.0.0.1"}
            )
            assert r1.status_code == 200

            # Without cleanup the bucket would be exhausted; with stale_after=0
            # and cleanup_interval=0, the bucket is removed between requests
            r2 = await client.get(
                "/ping", headers={"x-forwarded-for": "10.0.0.1"}
            )
            assert r2.status_code == 200


def test_default_key_func_xff():
    """_default_key_func extracts first X-Forwarded-For IP."""
    scope = {
        "type": "http",
        "headers": [
            (b"x-forwarded-for", b"1.2.3.4, 10.0.0.1"),
        ],
    }
    assert _default_key_func(scope) == "1.2.3.4"


def test_default_key_func_x_real_ip():
    """_default_key_func falls back to X-Real-IP."""
    scope = {
        "type": "http",
        "headers": [
            (b"x-real-ip", b"5.6.7.8"),
        ],
    }
    assert _default_key_func(scope) == "5.6.7.8"


def test_default_key_func_client():
    """_default_key_func falls back to ASGI client tuple."""
    scope = {
        "type": "http",
        "headers": [],
        "client": ("192.168.0.1", 12345),
    }
    assert _default_key_func(scope) == "192.168.0.1"


def test_default_key_func_unknown():
    """_default_key_func returns 'unknown' when nothing is available."""
    scope = {"type": "http", "headers": []}
    assert _default_key_func(scope) == "unknown"


# --------------------------------------------------------------------------
# CSRF Middleware Tests
# --------------------------------------------------------------------------


class TestCSRFMiddleware:
    """Tests for the CSRF double-submit cookie middleware."""

    @pytest.mark.asyncio
    async def test_get_sets_csrf_cookie(self):
        """GET request should set _csrf cookie."""
        app = _make_app(CSRFMiddleware())
        async with TestClient(app) as client:
            resp = await client.get("/ping")
            assert resp.status_code == 200
            cookies = resp.headers.get_list("set-cookie")
            csrf_cookies = [c for c in cookies if c.startswith("_csrf=")]
            assert len(csrf_cookies) == 1
            cookie_value = csrf_cookies[0]
            assert "SameSite=Strict" in cookie_value
            assert "HttpOnly" in cookie_value

    @pytest.mark.asyncio
    async def test_get_does_not_reset_existing_cookie(self):
        """GET with existing _csrf cookie should not set a new one."""
        app = _make_app(CSRFMiddleware())
        async with TestClient(app) as client:
            # First GET — sets cookie
            r1 = await client.get("/ping")
            cookie_header = [
                c for c in r1.headers.get_list("set-cookie")
                if c.startswith("_csrf=")
            ][0]
            token = cookie_header.split("=", 1)[1].split(";")[0]

            # Second GET with cookie — should not set a new cookie
            r2 = await client.get(
                "/ping", headers={"cookie": f"_csrf={token}"}
            )
            assert r2.status_code == 200
            new_csrf_cookies = [
                c for c in r2.headers.get_list("set-cookie")
                if c.startswith("_csrf=")
            ]
            assert len(new_csrf_cookies) == 0

    @pytest.mark.asyncio
    async def test_post_with_valid_header_token(self):
        """POST with matching X-CSRF-Token header should succeed."""
        app = _make_app(CSRFMiddleware())
        async with TestClient(app) as client:
            # Get the CSRF token
            r1 = await client.get("/ping")
            cookie_header = [
                c for c in r1.headers.get_list("set-cookie")
                if c.startswith("_csrf=")
            ][0]
            token = cookie_header.split("=", 1)[1].split(";")[0]

            # POST with both cookie and header
            r2 = await client.post(
                "/submit",
                headers={
                    "cookie": f"_csrf={token}",
                    "X-CSRF-Token": token,
                },
                json={"data": "test"},
            )
            assert r2.status_code == 201

    @pytest.mark.asyncio
    async def test_post_without_csrf_cookie_returns_403(self):
        """POST without CSRF cookie should be rejected."""
        app = _make_app(CSRFMiddleware())
        async with TestClient(app) as client:
            r = await client.post("/submit", json={"data": "test"})
            assert r.status_code == 403
            data = r.json()
            assert data["type"] == "https://neutron.dev/errors/csrf-failed"
            assert "cookie missing" in data["detail"].lower()

    @pytest.mark.asyncio
    async def test_post_without_csrf_header_returns_403(self):
        """POST with cookie but no header/form token should be rejected."""
        app = _make_app(CSRFMiddleware())
        async with TestClient(app) as client:
            r = await client.post(
                "/submit",
                headers={"cookie": "_csrf=some-token"},
                json={"data": "test"},
            )
            assert r.status_code == 403
            data = r.json()
            assert "missing" in data["detail"].lower()

    @pytest.mark.asyncio
    async def test_post_with_mismatched_token_returns_403(self):
        """POST with mismatched cookie and header tokens should be rejected."""
        app = _make_app(CSRFMiddleware())
        async with TestClient(app) as client:
            r = await client.post(
                "/submit",
                headers={
                    "cookie": "_csrf=token-a",
                    "X-CSRF-Token": "token-b",
                },
                json={"data": "test"},
            )
            assert r.status_code == 403
            data = r.json()
            assert "mismatch" in data["detail"].lower()

    @pytest.mark.asyncio
    async def test_exempt_paths_skip_csrf(self):
        """Exempt paths bypass CSRF validation."""
        app = _make_app(CSRFMiddleware(exempt_paths=["/submit"]))
        async with TestClient(app) as client:
            # POST to exempt path without any CSRF token
            r = await client.post("/submit", json={"data": "test"})
            assert r.status_code == 201

    @pytest.mark.asyncio
    async def test_custom_cookie_and_header_names(self):
        """Custom cookie/header names are respected."""
        app = _make_app(
            CSRFMiddleware(cookie_name="my_csrf", header_name="X-My-CSRF")
        )
        async with TestClient(app) as client:
            # GET to get token
            r1 = await client.get("/ping")
            cookies = r1.headers.get_list("set-cookie")
            csrf_cookies = [c for c in cookies if c.startswith("my_csrf=")]
            assert len(csrf_cookies) == 1
            token = csrf_cookies[0].split("=", 1)[1].split(";")[0]

            # POST with custom names
            r2 = await client.post(
                "/submit",
                headers={
                    "cookie": f"my_csrf={token}",
                    "X-My-CSRF": token,
                },
                json={"data": "test"},
            )
            assert r2.status_code == 201

    @pytest.mark.asyncio
    async def test_head_and_options_are_safe(self):
        """HEAD and OPTIONS requests don't require CSRF validation."""
        app = _make_app(CSRFMiddleware())
        async with TestClient(app) as client:
            r_head = await client.head("/ping")
            assert r_head.status_code == 200

            r_options = await client.options("/ping")
            # OPTIONS may return 200 or 405 depending on route config,
            # but it should NOT return 403
            assert r_options.status_code != 403

    @pytest.mark.asyncio
    async def test_default_exempt_paths_include_health(self):
        """Health/docs/openapi endpoints are exempt by default."""
        app = _make_app(CSRFMiddleware())
        async with TestClient(app) as client:
            r = await client.get("/health")
            assert r.status_code == 200

    @pytest.mark.asyncio
    async def test_csrf_response_is_rfc7807(self):
        """CSRF error responses use RFC 7807 format."""
        app = _make_app(CSRFMiddleware())
        async with TestClient(app) as client:
            r = await client.post("/submit", json={})
            assert r.status_code == 403
            data = r.json()
            assert data["type"] == "https://neutron.dev/errors/csrf-failed"
            assert data["title"] == "Forbidden"
            assert data["status"] == 403


# --------------------------------------------------------------------------
# Graceful Shutdown Tests
# --------------------------------------------------------------------------


class TestGracefulShutdown:
    """Tests for the graceful shutdown lifecycle."""

    @pytest.mark.asyncio
    async def test_on_stop_hooks_registered(self):
        """on_stop decorator registers hooks."""
        app = App(title="Shutdown Test")
        calls: list[str] = []

        @app.on_stop
        async def hook_a():
            calls.append("a")

        @app.on_stop
        async def hook_b():
            calls.append("b")

        assert len(app._on_stop_hooks) == 2

    @pytest.mark.asyncio
    async def test_shutdown_rejects_new_requests(self):
        """When shutting_down is True, new HTTP requests get 503."""
        app = App(title="Shutdown Test")
        router = Router()

        @router.get("/ping")
        async def ping() -> dict:
            return {"ok": True}

        app.include_router(router)

        async with TestClient(app) as client:
            # Normal request works
            r1 = await client.get("/ping")
            assert r1.status_code == 200

            # Simulate shutdown flag
            app._shutting_down = True

            r2 = await client.get("/ping")
            assert r2.status_code == 503
            data = r2.json()
            assert data["type"] == "https://neutron.dev/errors/shutting-down"
            assert data["status"] == 503
            assert "retry-after" in r2.headers

    @pytest.mark.asyncio
    async def test_inflight_tracking(self):
        """In-flight request counter increments and decrements."""
        app = App(title="Flight Test")
        router = Router()

        @router.get("/ping")
        async def ping() -> dict:
            return {"ok": True}

        app.include_router(router)

        assert app._inflight == 0

        async with TestClient(app) as client:
            r = await client.get("/ping")
            assert r.status_code == 200

        # After request completes, inflight should be back to 0
        assert app._inflight == 0

    @pytest.mark.asyncio
    async def test_drain_timeout_configurable(self):
        """drain_timeout parameter is stored correctly."""
        app = App(title="Drain Test", drain_timeout=15.0)
        assert app._drain_timeout == 15.0

    @pytest.mark.asyncio
    async def test_on_stop_decorator_returns_function(self):
        """on_stop can be used as a decorator and returns the function."""
        app = App(title="Dec Test")

        @app.on_stop
        async def my_hook():
            pass

        assert my_hook is not None
        assert callable(my_hook)
        assert app._on_stop_hooks[-1] is my_hook
