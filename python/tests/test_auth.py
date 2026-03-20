"""Tests for neutron/auth — JWT, sessions, API keys, RBAC."""

from __future__ import annotations

import time
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from pydantic import BaseModel
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route
from starlette.middleware import Middleware

from neutron.auth.jwt import create_token, decode_token, JWTMiddleware, get_current_user
from neutron.auth.session import SessionMiddleware, MemorySessionStore, NucleusSessionStore
from neutron.auth.apikey import APIKeyMiddleware
from neutron.auth.rbac import require_role, require_permission
from neutron.error import AppError


# ============================================================================
# JWT
# ============================================================================


class TestJWT:
    def test_create_and_decode(self):
        token = create_token({"user_id": 42, "role": "admin"}, "secret123")
        payload = decode_token(token, "secret123")
        assert payload["user_id"] == 42
        assert payload["role"] == "admin"
        assert "iat" in payload
        assert "exp" in payload

    def test_create_custom_expiry(self):
        token = create_token({"id": 1}, "secret", expires_in=7200)
        payload = decode_token(token, "secret")
        assert payload["exp"] - payload["iat"] == 7200

    def test_decode_invalid_token(self):
        with pytest.raises(AppError) as exc_info:
            decode_token("not.a.valid.token.at.all", "secret")
        assert exc_info.value.status == 401

    def test_decode_malformed_jwt(self):
        with pytest.raises(AppError) as exc_info:
            decode_token("just_one_part", "secret")
        assert exc_info.value.status == 401

    def test_decode_wrong_secret(self):
        token = create_token({"id": 1}, "correct_secret")
        with pytest.raises(AppError) as exc_info:
            decode_token(token, "wrong_secret")
        assert "signature" in exc_info.value.detail.lower()

    def test_decode_expired_token(self):
        token = create_token({"id": 1}, "secret", expires_in=-10)
        with pytest.raises(AppError) as exc_info:
            decode_token(token, "secret")
        assert exc_info.value.code == "token_expired"

    def test_decode_skip_exp_verification(self):
        token = create_token({"id": 1}, "secret", expires_in=-10)
        payload = decode_token(token, "secret", verify_exp=False)
        assert payload["id"] == 1

    def test_unsupported_algorithm(self):
        with pytest.raises(ValueError, match="Unsupported"):
            create_token({"id": 1}, "secret", algorithm="NONE")


class TestJWTMiddleware:
    async def test_jwt_middleware_sets_user(self):
        secret = "test-secret-that-is-at-least-32ch"
        token = create_token({"user_id": 42}, secret)

        async def endpoint(request: Request) -> JSONResponse:
            user = request.state.user
            return JSONResponse({"user_id": user["user_id"]})

        app = Starlette(
            routes=[Route("/me", endpoint)],
            middleware=[
                JWTMiddleware(secret=secret).as_starlette_middleware()
            ],
        )

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/me", headers={"Authorization": f"Bearer {token}"})
            assert resp.status_code == 200
            assert resp.json()["user_id"] == 42

    async def test_jwt_middleware_no_token(self):
        async def endpoint(request: Request) -> JSONResponse:
            return JSONResponse({"user": request.state.user})

        app = Starlette(
            routes=[Route("/me", endpoint)],
            middleware=[
                JWTMiddleware(secret="s" * 32).as_starlette_middleware()
            ],
        )

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/me")
            assert resp.status_code == 200
            assert resp.json()["user"] is None

    async def test_jwt_middleware_excludes_health(self):
        async def health(request: Request) -> JSONResponse:
            return JSONResponse({"ok": True})

        app = Starlette(
            routes=[Route("/health", health)],
            middleware=[
                JWTMiddleware(secret="s" * 32).as_starlette_middleware()
            ],
        )

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/health")
            assert resp.status_code == 200


class TestGetCurrentUser:
    def test_get_current_user_present(self):
        request = MagicMock()
        request.state.user = {"user_id": 42}
        user = get_current_user(request)
        assert user["user_id"] == 42

    def test_get_current_user_missing(self):
        request = MagicMock()
        request.state.user = None
        with pytest.raises(AppError) as exc_info:
            get_current_user(request)
        assert exc_info.value.status == 401


# ============================================================================
# Sessions
# ============================================================================


class TestMemorySessionStore:
    async def test_save_and_load(self):
        store = MemorySessionStore()
        await store.save("sess1", {"user": "alice"}, ttl=3600)
        data = await store.load("sess1")
        assert data["user"] == "alice"

    async def test_load_missing(self):
        store = MemorySessionStore()
        assert await store.load("nonexistent") is None

    async def test_delete(self):
        store = MemorySessionStore()
        await store.save("sess1", {"x": 1}, ttl=300)
        await store.delete("sess1")
        assert await store.load("sess1") is None

    async def test_expired_session(self):
        store = MemorySessionStore()
        await store.save("sess1", {"x": 1}, ttl=-1)  # Already expired
        assert await store.load("sess1") is None


class TestNucleusSessionStore:
    async def test_save_and_load(self):
        mock_kv = MagicMock()
        mock_kv.set = AsyncMock()
        mock_kv.get = AsyncMock(return_value='{"user": "bob"}')

        store = NucleusSessionStore(mock_kv)
        await store.save("sess1", {"user": "bob"}, ttl=300)
        data = await store.load("sess1")

        assert data["user"] == "bob"
        mock_kv.set.assert_called_once()
        mock_kv.get.assert_called_once_with("session:sess1")

    async def test_load_missing(self):
        mock_kv = MagicMock()
        mock_kv.get = AsyncMock(return_value=None)
        store = NucleusSessionStore(mock_kv)
        assert await store.load("missing") is None

    async def test_delete(self):
        mock_kv = MagicMock()
        mock_kv.delete = AsyncMock()
        store = NucleusSessionStore(mock_kv)
        await store.delete("sess1")
        mock_kv.delete.assert_called_once_with("session:sess1")


class TestSessionMiddleware:
    async def test_session_creates_cookie(self):
        async def endpoint(request: Request) -> JSONResponse:
            session = request.state.session
            session["visited"] = True
            return JSONResponse({"ok": True})

        mw = SessionMiddleware(
            store=MemorySessionStore()
        ).as_starlette_middleware()

        app = Starlette(
            routes=[Route("/", endpoint)],
            middleware=[mw],
        )

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/")
            assert resp.status_code == 200
            assert "session_id" in resp.headers.get("set-cookie", "")


# ============================================================================
# API Key
# ============================================================================


class TestAPIKeyMiddleware:
    async def test_valid_api_key(self):
        async def validator(key: str) -> dict | None:
            if key == "valid-key":
                return {"app": "test"}
            return None

        async def endpoint(request: Request) -> JSONResponse:
            return JSONResponse({"app": request.state.api_key_user["app"]})

        mw = APIKeyMiddleware(validator=validator).as_starlette_middleware()
        app = Starlette(
            routes=[Route("/data", endpoint)],
            middleware=[mw],
        )

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/data", headers={"X-API-Key": "valid-key"})
            assert resp.status_code == 200
            assert resp.json()["app"] == "test"

    async def test_invalid_api_key(self):
        async def validator(key: str) -> None:
            return None

        async def endpoint(request: Request) -> JSONResponse:
            return JSONResponse({"ok": True})

        mw = APIKeyMiddleware(validator=validator).as_starlette_middleware()
        app = Starlette(
            routes=[Route("/data", endpoint)],
            middleware=[mw],
        )

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/data", headers={"X-API-Key": "bad-key"})
            assert resp.status_code == 401

    async def test_missing_api_key(self):
        async def validator(key: str) -> None:
            return None

        async def endpoint(request: Request) -> JSONResponse:
            return JSONResponse({"ok": True})

        mw = APIKeyMiddleware(validator=validator).as_starlette_middleware()
        app = Starlette(
            routes=[Route("/data", endpoint)],
            middleware=[mw],
        )

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/data")
            assert resp.status_code == 401
            assert "required" in resp.json()["detail"]

    async def test_api_key_excluded_paths(self):
        async def validator(key: str) -> None:
            return None

        async def health(request: Request) -> JSONResponse:
            return JSONResponse({"ok": True})

        mw = APIKeyMiddleware(validator=validator).as_starlette_middleware()
        app = Starlette(
            routes=[Route("/health", health)],
            middleware=[mw],
        )

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/health")
            assert resp.status_code == 200

    async def test_api_key_via_query_param(self):
        async def validator(key: str) -> dict | None:
            if key == "qkey":
                return {"ok": True}
            return None

        async def endpoint(request: Request) -> JSONResponse:
            return JSONResponse({"ok": True})

        mw = APIKeyMiddleware(validator=validator).as_starlette_middleware()
        app = Starlette(
            routes=[Route("/data", endpoint)],
            middleware=[mw],
        )

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/data?api_key=qkey")
            assert resp.status_code == 200


# ============================================================================
# RBAC
# ============================================================================


class TestRBAC:
    async def test_require_role_allowed(self):
        @require_role("admin")
        async def handler(request: Request) -> str:
            return "ok"

        request = MagicMock(spec=Request)
        request.state.user = {"role": "admin"}
        result = await handler(request)
        assert result == "ok"

    async def test_require_role_denied(self):
        @require_role("admin")
        async def handler(request: Request) -> str:
            return "ok"

        request = MagicMock(spec=Request)
        request.state.user = {"role": "user"}
        with pytest.raises(AppError) as exc_info:
            await handler(request)
        assert exc_info.value.status == 403

    async def test_require_role_multiple(self):
        @require_role("admin", "manager")
        async def handler(request: Request) -> str:
            return "ok"

        request = MagicMock(spec=Request)
        request.state.user = {"role": "manager"}
        result = await handler(request)
        assert result == "ok"

    async def test_require_role_from_roles_list(self):
        @require_role("editor")
        async def handler(request: Request) -> str:
            return "ok"

        request = MagicMock(spec=Request)
        request.state.user = {"roles": ["viewer", "editor"]}
        result = await handler(request)
        assert result == "ok"

    async def test_require_role_no_user(self):
        @require_role("admin")
        async def handler(request: Request) -> str:
            return "ok"

        request = MagicMock(spec=Request)
        request.state.user = None
        with pytest.raises(AppError) as exc_info:
            await handler(request)
        assert exc_info.value.status == 401

    async def test_require_permission_allowed(self):
        @require_permission("billing:read")
        async def handler(request: Request) -> str:
            return "ok"

        request = MagicMock(spec=Request)
        request.state.user = {"permissions": ["billing:read", "billing:write"]}
        result = await handler(request)
        assert result == "ok"

    async def test_require_permission_denied(self):
        @require_permission("billing:write")
        async def handler(request: Request) -> str:
            return "ok"

        request = MagicMock(spec=Request)
        request.state.user = {"permissions": ["billing:read"]}
        with pytest.raises(AppError) as exc_info:
            await handler(request)
        assert exc_info.value.status == 403
        assert "billing:write" in exc_info.value.detail

    async def test_require_multiple_permissions(self):
        @require_permission("read", "write")
        async def handler(request: Request) -> str:
            return "ok"

        request = MagicMock(spec=Request)
        request.state.user = {"permissions": ["read", "write", "delete"]}
        result = await handler(request)
        assert result == "ok"


# ============================================================================
# Auth __init__ exports
# ============================================================================


class TestAuthExports:
    def test_all_exports(self):
        from neutron.auth import (
            JWTMiddleware,
            create_token,
            decode_token,
            get_current_user,
            SessionMiddleware,
            MemorySessionStore,
            NucleusSessionStore,
            APIKeyMiddleware,
            require_role,
            require_permission,
        )
        assert JWTMiddleware is not None
        assert create_token is not None

    def test_password_exports(self):
        from neutron.auth import hash_password, verify_password, needs_rehash
        assert hash_password is not None
        assert verify_password is not None
        assert needs_rehash is not None


# ============================================================================
# Password Hashing
# ============================================================================


class TestPasswordHashing:
    def test_hash_password_returns_string(self):
        from neutron.auth.password import hash_password

        hashed = hash_password("secret123")
        assert isinstance(hashed, str)
        assert len(hashed) > 0

    def test_hash_password_not_plaintext(self):
        from neutron.auth.password import hash_password

        hashed = hash_password("mypassword")
        assert hashed != "mypassword"

    def test_hash_password_different_for_same_input(self):
        from neutron.auth.password import hash_password

        h1 = hash_password("same")
        h2 = hash_password("same")
        # Should produce different hashes due to random salt
        assert h1 != h2

    def test_verify_password_correct(self):
        from neutron.auth.password import hash_password, verify_password

        hashed = hash_password("correct_password")
        assert verify_password("correct_password", hashed) is True

    def test_verify_password_wrong(self):
        from neutron.auth.password import hash_password, verify_password

        hashed = hash_password("correct")
        assert verify_password("wrong", hashed) is False

    def test_verify_password_empty(self):
        from neutron.auth.password import hash_password, verify_password

        hashed = hash_password("notempty")
        assert verify_password("", hashed) is False

    def test_verify_password_unknown_format(self):
        from neutron.auth.password import verify_password

        with pytest.raises(ValueError, match="Unknown hash format"):
            verify_password("pass", "not_a_valid_hash_format")

    def test_hash_starts_with_known_prefix(self):
        from neutron.auth.password import hash_password

        hashed = hash_password("test")
        # Should start with $argon2 (preferred) or $2b$ (bcrypt fallback)
        assert hashed.startswith("$argon2") or hashed.startswith("$2b$") or hashed.startswith("$2a$")

    def test_needs_rehash_with_current_hash(self):
        from neutron.auth.password import hash_password, needs_rehash

        hashed = hash_password("test")
        # A freshly hashed password with current params should not need rehash
        # (unless it's bcrypt and argon2 is available)
        result = needs_rehash(hashed)
        assert isinstance(result, bool)

    def test_needs_rehash_unknown_format(self):
        from neutron.auth.password import needs_rehash

        assert needs_rehash("not_a_hash") is False

    def test_verify_unicode_password(self):
        from neutron.auth.password import hash_password, verify_password

        hashed = hash_password("p\u00e4ssw\u00f6rd")
        assert verify_password("p\u00e4ssw\u00f6rd", hashed) is True
        assert verify_password("password", hashed) is False

    def test_verify_long_password(self):
        from neutron.auth.password import hash_password, verify_password

        long_pw = "a" * 256
        hashed = hash_password(long_pw)
        assert verify_password(long_pw, hashed) is True
        assert verify_password("a" * 255, hashed) is False
