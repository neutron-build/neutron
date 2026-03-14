"""Tests for neutron/auth/oauth — OAuth2 Authorization Code Flow with PKCE."""

from __future__ import annotations

import base64
import hashlib
import hmac
import time
from typing import Any
from unittest.mock import AsyncMock, patch
from urllib.parse import parse_qs, urlparse

import pytest
from starlette.requests import Request
from starlette.responses import RedirectResponse
from starlette.testclient import TestClient

from neutron.auth.oauth import (
    OAuthProvider,
    OAuthUser,
    _build_auth_url,
    _decode_state_cookie,
    _derive_pkce_challenge,
    _encode_state_cookie,
    _generate_pkce,
    _hmac_sign,
    _normalize_user,
    oauth_callback_handler,
    oauth_redirect_handler,
)
from neutron.error import AppError


_TEST_SECRET = "test-hmac-secret-at-least-32-bytes-long"


def _make_provider(**overrides: Any) -> OAuthProvider:
    """Create a test OAuthProvider with sensible defaults."""
    defaults = dict(
        client_id="test-client-id",
        client_secret="test-client-secret",
        redirect_url="http://localhost:8000/auth/callback",
        auth_url="https://provider.example.com/authorize",
        token_url="https://provider.example.com/token",
        userinfo_url="https://provider.example.com/userinfo",
        scopes=["openid", "profile"],
        secret=_TEST_SECRET,
        provider_name="test",
    )
    defaults.update(overrides)
    return OAuthProvider(**defaults)


# ============================================================================
# PKCE
# ============================================================================


class TestPKCE:
    def test_generate_pkce_returns_pair(self):
        verifier, challenge = _generate_pkce()
        assert isinstance(verifier, str)
        assert isinstance(challenge, str)
        assert len(verifier) > 0
        assert len(challenge) > 0

    def test_generate_pkce_unique(self):
        v1, c1 = _generate_pkce()
        v2, c2 = _generate_pkce()
        assert v1 != v2
        assert c1 != c2

    def test_pkce_challenge_is_s256(self):
        verifier, challenge = _generate_pkce()
        # Manually compute the S256 challenge
        digest = hashlib.sha256(verifier.encode()).digest()
        expected = base64.urlsafe_b64encode(digest).rstrip(b"=").decode()
        assert challenge == expected

    def test_derive_pkce_challenge(self):
        verifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"
        challenge = _derive_pkce_challenge(verifier)
        # Should be base64url(sha256(verifier)) without padding
        digest = hashlib.sha256(verifier.encode()).digest()
        expected = base64.urlsafe_b64encode(digest).rstrip(b"=").decode()
        assert challenge == expected


# ============================================================================
# State cookie
# ============================================================================


class TestStateCookie:
    def test_encode_decode_roundtrip(self):
        state = "abc123"
        verifier = "my-verifier"
        cookie = _encode_state_cookie(state, verifier, _TEST_SECRET)

        decoded_state, decoded_verifier, ok = _decode_state_cookie(
            cookie, _TEST_SECRET
        )
        assert ok is True
        assert decoded_state == state
        assert decoded_verifier == verifier

    def test_cookie_has_four_pipe_separated_parts(self):
        cookie = _encode_state_cookie("state", "verifier", _TEST_SECRET)
        parts = cookie.split("|")
        assert len(parts) == 4
        assert parts[0] == "state"
        assert parts[1] == "verifier"
        # parts[2] is the timestamp (numeric string)
        assert parts[2].isdigit()
        # parts[3] is the HMAC signature
        assert len(parts[3]) > 0

    def test_hmac_signature_is_correct(self):
        state = "mystate"
        verifier = "myverifier"
        cookie = _encode_state_cookie(state, verifier, _TEST_SECRET)
        parts = cookie.split("|")

        payload = f"{parts[0]}|{parts[1]}|{parts[2]}"
        expected_sig = _hmac_sign(payload, _TEST_SECRET)
        assert parts[3] == expected_sig

    def test_cookie_includes_timestamp(self):
        before = int(time.time())
        cookie = _encode_state_cookie("s", "v", _TEST_SECRET)
        after = int(time.time())

        parts = cookie.split("|")
        ts = int(parts[2])
        assert before <= ts <= after

    def test_expired_cookie_rejected(self):
        state = "s"
        verifier = "v"
        # Manually create a cookie with an old timestamp
        old_ts = str(int(time.time()) - 700)  # 700 seconds ago (> 600 max)
        payload = f"{state}|{verifier}|{old_ts}"
        sig = _hmac_sign(payload, _TEST_SECRET)
        cookie = f"{payload}|{sig}"

        _, _, ok = _decode_state_cookie(cookie, _TEST_SECRET)
        assert ok is False

    def test_cookie_within_max_age_accepted(self):
        state = "s"
        verifier = "v"
        # 5 minutes ago — within the 10-minute window
        recent_ts = str(int(time.time()) - 300)
        payload = f"{state}|{verifier}|{recent_ts}"
        sig = _hmac_sign(payload, _TEST_SECRET)
        cookie = f"{payload}|{sig}"

        decoded_state, decoded_verifier, ok = _decode_state_cookie(
            cookie, _TEST_SECRET
        )
        assert ok is True
        assert decoded_state == state
        assert decoded_verifier == verifier

    def test_tampered_state_rejected(self):
        cookie = _encode_state_cookie("original", "verifier", _TEST_SECRET)
        # Tamper with the state
        tampered = cookie.replace("original", "tampered", 1)

        _, _, ok = _decode_state_cookie(tampered, _TEST_SECRET)
        assert ok is False

    def test_tampered_signature_rejected(self):
        cookie = _encode_state_cookie("s", "v", _TEST_SECRET)
        parts = cookie.split("|")
        # Replace signature with garbage
        parts[3] = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        tampered = "|".join(parts)

        _, _, ok = _decode_state_cookie(tampered, _TEST_SECRET)
        assert ok is False

    def test_wrong_secret_rejected(self):
        cookie = _encode_state_cookie("s", "v", _TEST_SECRET)
        _, _, ok = _decode_state_cookie(cookie, "wrong-secret")
        assert ok is False

    def test_malformed_cookie_rejected(self):
        _, _, ok = _decode_state_cookie("no-pipes-here", _TEST_SECRET)
        assert ok is False

    def test_empty_cookie_rejected(self):
        _, _, ok = _decode_state_cookie("", _TEST_SECRET)
        assert ok is False

    def test_non_numeric_timestamp_rejected(self):
        payload = "s|v|notanumber"
        sig = _hmac_sign(payload, _TEST_SECRET)
        cookie = f"{payload}|{sig}"

        _, _, ok = _decode_state_cookie(cookie, _TEST_SECRET)
        assert ok is False

    def test_custom_max_age(self):
        # Create a cookie 30 seconds old, with a 20-second max_age
        old_ts = str(int(time.time()) - 30)
        payload = f"s|v|{old_ts}"
        sig = _hmac_sign(payload, _TEST_SECRET)
        cookie = f"{payload}|{sig}"

        _, _, ok = _decode_state_cookie(cookie, _TEST_SECRET, max_age=20)
        assert ok is False

        # Same cookie with a 60-second max_age should pass
        _, _, ok = _decode_state_cookie(cookie, _TEST_SECRET, max_age=60)
        assert ok is True


# ============================================================================
# User normalization
# ============================================================================


class TestNormalizeUser:
    def test_github_user(self):
        raw = {
            "id": 12345,
            "login": "octocat",
            "name": "The Octocat",
            "email": "octocat@github.com",
            "avatar_url": "https://avatars.githubusercontent.com/u/12345",
        }
        user = _normalize_user("github", raw, "ghp_token123")

        assert user.id == "12345"
        assert user.email == "octocat@github.com"
        assert user.name == "The Octocat"
        assert user.avatar_url == "https://avatars.githubusercontent.com/u/12345"
        assert user.provider == "github"
        assert user.access_token == "ghp_token123"
        assert user.raw is raw

    def test_github_user_string_id(self):
        raw = {"id": "string-id", "login": "user1"}
        user = _normalize_user("github", raw, "token")
        assert user.id == "string-id"

    def test_github_user_login_as_name(self):
        raw = {"id": 1, "login": "mylogin"}
        user = _normalize_user("github", raw, "token")
        assert user.name == "mylogin"

    def test_google_user(self):
        raw = {
            "sub": "1234567890",
            "email": "user@gmail.com",
            "name": "Google User",
            "picture": "https://lh3.googleusercontent.com/photo.jpg",
        }
        user = _normalize_user("google", raw, "ya29.token")

        assert user.id == "1234567890"
        assert user.email == "user@gmail.com"
        assert user.name == "Google User"
        assert user.avatar_url == "https://lh3.googleusercontent.com/photo.jpg"
        assert user.provider == "google"

    def test_google_user_sub_as_id(self):
        raw = {"sub": "oidc-sub-claim"}
        user = _normalize_user("google", raw, "token")
        assert user.id == "oidc-sub-claim"

    def test_discord_user(self):
        raw = {
            "id": "80351110224678912",
            "username": "Nelly",
            "email": "nelly@discord.com",
            "avatar": "8342729096ea3675442027381ff50dfe",
        }
        user = _normalize_user("discord", raw, "discord-token")

        assert user.id == "80351110224678912"
        assert user.email == "nelly@discord.com"
        assert user.name == "Nelly"
        assert "cdn.discordapp.com" in user.avatar_url
        assert "80351110224678912" in user.avatar_url
        assert "8342729096ea3675442027381ff50dfe" in user.avatar_url
        assert user.provider == "discord"

    def test_discord_user_no_avatar(self):
        raw = {"id": "123", "username": "User"}
        user = _normalize_user("discord", raw, "token")
        assert user.avatar_url == ""

    def test_missing_fields_default_to_empty(self):
        raw: dict[str, Any] = {}
        user = _normalize_user("unknown", raw, "token")

        assert user.id == ""
        assert user.email == ""
        assert user.name == ""
        assert user.avatar_url == ""

    def test_numeric_float_id(self):
        raw = {"id": 42.0}
        user = _normalize_user("test", raw, "token")
        assert user.id == "42"

    def test_name_priority_order(self):
        # "name" takes priority over "login" and "username"
        raw = {"id": "1", "name": "Full Name", "login": "loginname", "username": "uname"}
        user = _normalize_user("test", raw, "token")
        assert user.name == "Full Name"

    def test_avatar_priority_order(self):
        # "avatar_url" takes priority over "picture"
        raw = {"id": "1", "avatar_url": "https://a.com/1.png", "picture": "https://b.com/2.png"}
        user = _normalize_user("test", raw, "token")
        assert user.avatar_url == "https://a.com/1.png"


# ============================================================================
# Pre-configured providers
# ============================================================================


class TestProviderFactories:
    def test_github_provider(self):
        p = OAuthProvider.github("cid", "csec", "http://redir", "secret")
        assert p.provider_name == "github"
        assert p.client_id == "cid"
        assert p.client_secret == "csec"
        assert p.redirect_url == "http://redir"
        assert "github.com/login/oauth/authorize" in p.auth_url
        assert "github.com/login/oauth/access_token" in p.token_url
        assert "api.github.com/user" in p.userinfo_url
        assert "read:user" in p.scopes
        assert "user:email" in p.scopes

    def test_google_provider(self):
        p = OAuthProvider.google("cid", "csec", "http://redir", "secret")
        assert p.provider_name == "google"
        assert "accounts.google.com" in p.auth_url
        assert "oauth2.googleapis.com" in p.token_url
        assert "openidconnect.googleapis.com" in p.userinfo_url
        assert "openid" in p.scopes
        assert "profile" in p.scopes
        assert "email" in p.scopes

    def test_discord_provider(self):
        p = OAuthProvider.discord("cid", "csec", "http://redir", "secret")
        assert p.provider_name == "discord"
        assert "discord.com/api/oauth2/authorize" in p.auth_url
        assert "discord.com/api/oauth2/token" in p.token_url
        assert "discord.com/api/users/@me" in p.userinfo_url
        assert "identify" in p.scopes
        assert "email" in p.scopes


# ============================================================================
# Authorization URL builder
# ============================================================================


class TestBuildAuthURL:
    def test_url_contains_required_params(self):
        provider = _make_provider()
        url = _build_auth_url(provider, "mystate", "mychallenge")
        parsed = urlparse(url)
        params = parse_qs(parsed.query)

        assert parsed.scheme == "https"
        assert "provider.example.com" in parsed.netloc
        assert params["response_type"] == ["code"]
        assert params["client_id"] == ["test-client-id"]
        assert params["redirect_uri"] == ["http://localhost:8000/auth/callback"]
        assert params["scope"] == ["openid profile"]
        assert params["state"] == ["mystate"]
        assert params["code_challenge"] == ["mychallenge"]
        assert params["code_challenge_method"] == ["S256"]

    def test_url_uses_provider_auth_url(self):
        provider = _make_provider(auth_url="https://custom.example.com/auth")
        url = _build_auth_url(provider, "s", "c")
        assert url.startswith("https://custom.example.com/auth?")


# ============================================================================
# Redirect handler
# ============================================================================


class TestOAuthRedirectHandler:
    async def test_redirect_returns_302(self):
        provider = _make_provider()
        handler = oauth_redirect_handler(provider)

        # Create a minimal mock request
        scope = {"type": "http", "method": "GET", "path": "/auth/login"}
        request = Request(scope)

        response = await handler(request)
        assert isinstance(response, RedirectResponse)
        assert response.status_code == 302

    async def test_redirect_sets_state_cookie(self):
        provider = _make_provider()
        handler = oauth_redirect_handler(provider)

        scope = {"type": "http", "method": "GET", "path": "/auth/login"}
        request = Request(scope)

        response = await handler(request)

        # Check the set-cookie header
        cookie_headers = [
            v.decode() for k, v in response.raw_headers
            if k == b"set-cookie"
        ]
        assert len(cookie_headers) >= 1
        state_cookie = [h for h in cookie_headers if "__oauth_state=" in h]
        assert len(state_cookie) == 1
        assert "HttpOnly" in state_cookie[0] or "httponly" in state_cookie[0].lower()

    async def test_redirect_url_has_pkce(self):
        provider = _make_provider()
        handler = oauth_redirect_handler(provider)

        scope = {"type": "http", "method": "GET", "path": "/auth/login"}
        request = Request(scope)

        response = await handler(request)

        # Extract the Location header
        location = dict(response.raw_headers).get(b"location", b"").decode()
        parsed = urlparse(location)
        params = parse_qs(parsed.query)

        assert "code_challenge" in params
        assert params["code_challenge_method"] == ["S256"]
        assert "state" in params


# ============================================================================
# Callback handler
# ============================================================================


class TestOAuthCallbackHandler:
    async def test_callback_missing_code(self):
        provider = _make_provider()
        on_success = AsyncMock()
        handler = oauth_callback_handler(provider, on_success)

        scope = {
            "type": "http",
            "method": "GET",
            "path": "/auth/callback",
            "query_string": b"state=abc",
            "headers": [],
        }
        request = Request(scope)

        with pytest.raises(AppError) as exc_info:
            await handler(request)
        assert exc_info.value.status == 400
        assert "code" in exc_info.value.detail.lower()

    async def test_callback_missing_state(self):
        provider = _make_provider()
        on_success = AsyncMock()
        handler = oauth_callback_handler(provider, on_success)

        scope = {
            "type": "http",
            "method": "GET",
            "path": "/auth/callback",
            "query_string": b"code=authcode123",
            "headers": [],
        }
        request = Request(scope)

        with pytest.raises(AppError) as exc_info:
            await handler(request)
        assert exc_info.value.status == 400
        assert "state" in exc_info.value.detail.lower()

    async def test_callback_missing_cookie(self):
        provider = _make_provider()
        on_success = AsyncMock()
        handler = oauth_callback_handler(provider, on_success)

        scope = {
            "type": "http",
            "method": "GET",
            "path": "/auth/callback",
            "query_string": b"code=authcode123&state=mystate",
            "headers": [],
        }
        request = Request(scope)

        with pytest.raises(AppError) as exc_info:
            await handler(request)
        assert exc_info.value.status == 403
        assert "cookie" in exc_info.value.detail.lower()

    async def test_callback_state_mismatch(self):
        provider = _make_provider()
        on_success = AsyncMock()
        handler = oauth_callback_handler(provider, on_success)

        # Create a valid cookie with state "correct-state"
        cookie_val = _encode_state_cookie("correct-state", "verifier", provider.secret)

        scope = {
            "type": "http",
            "method": "GET",
            "path": "/auth/callback",
            "query_string": b"code=authcode123&state=wrong-state",
            "headers": [
                (b"cookie", f"__oauth_state={cookie_val}".encode()),
            ],
        }
        request = Request(scope)

        with pytest.raises(AppError) as exc_info:
            await handler(request)
        assert exc_info.value.status == 403
        assert "mismatch" in exc_info.value.detail.lower()

    async def test_callback_full_flow(self):
        provider = _make_provider()
        state = "test-state-value"
        verifier = "test-verifier"
        cookie_val = _encode_state_cookie(state, verifier, provider.secret)

        on_success = AsyncMock(return_value=RedirectResponse("/dashboard"))
        handler = oauth_callback_handler(provider, on_success)

        scope = {
            "type": "http",
            "method": "GET",
            "path": "/auth/callback",
            "query_string": f"code=authcode123&state={state}".encode(),
            "headers": [
                (b"cookie", f"__oauth_state={cookie_val}".encode()),
            ],
        }
        request = Request(scope)

        # Mock the token exchange and userinfo fetch
        token_resp = {"access_token": "at_12345", "token_type": "bearer"}
        userinfo_resp = {
            "id": "42",
            "name": "Test User",
            "email": "test@example.com",
        }

        with patch("neutron.auth.oauth._exchange_code", new_callable=AsyncMock, return_value=token_resp), \
             patch("neutron.auth.oauth._fetch_userinfo", new_callable=AsyncMock, return_value=userinfo_resp):
            response = await handler(request)

        assert response.status_code == 307  # RedirectResponse default
        on_success.assert_called_once()

        # Verify the OAuthUser passed to on_success
        call_args = on_success.call_args
        user = call_args[0][1]
        assert isinstance(user, OAuthUser)
        assert user.id == "42"
        assert user.email == "test@example.com"
        assert user.name == "Test User"
        assert user.provider == "test"
        assert user.access_token == "at_12345"


# ============================================================================
# Auth __init__ exports
# ============================================================================


class TestOAuthExports:
    def test_exports_from_auth_init(self):
        from neutron.auth import (
            OAuthProvider,
            OAuthUser,
            oauth_callback_handler,
            oauth_redirect_handler,
        )
        assert OAuthProvider is not None
        assert OAuthUser is not None
        assert oauth_redirect_handler is not None
        assert oauth_callback_handler is not None
