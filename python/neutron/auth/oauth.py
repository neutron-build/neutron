"""OAuth2 Authorization Code Flow with PKCE.

Implements the complete OAuth2 authorization code flow with PKCE (RFC 7636)
and HMAC-signed state cookies for CSRF protection.

Includes pre-configured providers for GitHub, Google, and Discord.

Usage::

    from neutron.auth.oauth import OAuthProvider, oauth_redirect_handler, oauth_callback_handler

    github = OAuthProvider.github(
        client_id="...",
        client_secret="...",
        redirect_url="http://localhost:8000/auth/github/callback",
        secret="my-hmac-secret-at-least-32-bytes-long",
    )

    async def on_login(request, user):
        request.state.session["user_id"] = user.id
        return RedirectResponse("/dashboard")

    routes = [
        Route("/auth/github", oauth_redirect_handler(github)),
        Route("/auth/github/callback", oauth_callback_handler(github, on_login)),
    ]
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import logging
import secrets
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Optional
from urllib.parse import urlencode

logger = logging.getLogger("neutron.auth")

import httpx
from starlette.requests import Request
from starlette.responses import JSONResponse, RedirectResponse

from neutron.error import AppError

# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

_OAUTH_STATE_COOKIE = "__oauth_state"
_STATE_MAX_AGE = 600  # 10 minutes


@dataclass
class OAuthUser:
    """Normalized user information from any OAuth provider."""

    id: str
    email: str
    name: str
    avatar_url: str
    provider: str
    access_token: str
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass
class OAuthProvider:
    """Configuration for a single OAuth2 provider.

    Args:
        client_id: Application's OAuth client identifier.
        client_secret: Application's OAuth client secret.
        redirect_url: Registered callback URI.
        auth_url: Provider's authorization endpoint.
        token_url: Provider's token endpoint.
        userinfo_url: Provider's userinfo endpoint.
        scopes: OAuth scopes to request.
        secret: HMAC key for signing the anti-CSRF state cookie (>= 32 chars).
    """

    client_id: str
    client_secret: str
    redirect_url: str
    auth_url: str
    token_url: str
    userinfo_url: str
    scopes: list[str]
    secret: str
    provider_name: str = ""

    def __post_init__(self) -> None:
        if len(self.secret) < 32:
            raise ValueError(
                f"OAuth secret must be at least 32 characters. Got {len(self.secret)}."
            )

    # ------------------------------------------------------------------
    # Factory methods for common providers
    # ------------------------------------------------------------------

    @classmethod
    def github(
        cls,
        client_id: str,
        client_secret: str,
        redirect_url: str,
        secret: str,
    ) -> OAuthProvider:
        """Pre-configured GitHub OAuth provider.

        Default scopes: ``read:user``, ``user:email``.
        """
        return cls(
            client_id=client_id,
            client_secret=client_secret,
            redirect_url=redirect_url,
            auth_url="https://github.com/login/oauth/authorize",
            token_url="https://github.com/login/oauth/access_token",
            userinfo_url="https://api.github.com/user",
            scopes=["read:user", "user:email"],
            secret=secret,
            provider_name="github",
        )

    @classmethod
    def google(
        cls,
        client_id: str,
        client_secret: str,
        redirect_url: str,
        secret: str,
    ) -> OAuthProvider:
        """Pre-configured Google OAuth / OIDC provider.

        Default scopes: ``openid``, ``profile``, ``email``.
        """
        return cls(
            client_id=client_id,
            client_secret=client_secret,
            redirect_url=redirect_url,
            auth_url="https://accounts.google.com/o/oauth2/v2/auth",
            token_url="https://oauth2.googleapis.com/token",
            userinfo_url="https://openidconnect.googleapis.com/v1/userinfo",
            scopes=["openid", "profile", "email"],
            secret=secret,
            provider_name="google",
        )

    @classmethod
    def discord(
        cls,
        client_id: str,
        client_secret: str,
        redirect_url: str,
        secret: str,
    ) -> OAuthProvider:
        """Pre-configured Discord OAuth provider.

        Default scopes: ``identify``, ``email``.
        """
        return cls(
            client_id=client_id,
            client_secret=client_secret,
            redirect_url=redirect_url,
            auth_url="https://discord.com/api/oauth2/authorize",
            token_url="https://discord.com/api/oauth2/token",
            userinfo_url="https://discord.com/api/users/@me",
            scopes=["identify", "email"],
            secret=secret,
            provider_name="discord",
        )


# ---------------------------------------------------------------------------
# PKCE (RFC 7636) — S256 method
# ---------------------------------------------------------------------------


def _generate_pkce() -> tuple[str, str]:
    """Generate a PKCE verifier and S256 challenge pair.

    Returns:
        (verifier, challenge) where challenge = BASE64URL-NoPad(SHA256(verifier)).
    """
    raw = secrets.token_bytes(32)
    verifier = base64.urlsafe_b64encode(raw).rstrip(b"=").decode()
    challenge = _derive_pkce_challenge(verifier)
    return verifier, challenge


def _derive_pkce_challenge(verifier: str) -> str:
    """Derive the S256 PKCE challenge from a verifier string."""
    digest = hashlib.sha256(verifier.encode()).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode()


# ---------------------------------------------------------------------------
# State cookie — HMAC-signed, carries state + PKCE verifier + timestamp
# ---------------------------------------------------------------------------


def _encode_state_cookie(state: str, verifier: str, secret: str) -> str:
    """Produce a signed cookie value: ``state|verifier|timestamp|hmac``.

    The HMAC is computed over ``state|verifier|timestamp`` using SHA-256.
    """
    ts = str(int(time.time()))
    payload = f"{state}|{verifier}|{ts}"
    sig = _hmac_sign(payload, secret)
    return f"{payload}|{sig}"


def _decode_state_cookie(
    cookie: str,
    secret: str,
    max_age: int = _STATE_MAX_AGE,
) -> tuple[str, str, bool]:
    """Verify the HMAC and check the timestamp.

    Args:
        cookie: Raw cookie value.
        secret: HMAC key.
        max_age: Maximum age in seconds (default 600 = 10 minutes).

    Returns:
        (state, verifier, ok) — ``ok`` is False if signature or timestamp
        validation fails.
    """
    parts = cookie.split("|", 3)
    if len(parts) != 4:
        return "", "", False

    state, verifier, ts_str, sig = parts

    payload = f"{state}|{verifier}|{ts_str}"
    expected = _hmac_sign(payload, secret)

    # Constant-time comparison to prevent timing attacks
    if not hmac.compare_digest(sig, expected):
        return "", "", False

    # Verify timestamp freshness
    try:
        ts = int(ts_str)
    except ValueError:
        return "", "", False

    if time.time() - ts > max_age:
        return "", "", False

    return state, verifier, True


def _hmac_sign(payload: str, secret: str) -> str:
    """HMAC-SHA256 sign a payload and return the base64url-encoded signature."""
    sig = hmac.new(
        secret.encode(), payload.encode(), hashlib.sha256
    ).digest()
    return base64.urlsafe_b64encode(sig).rstrip(b"=").decode()


# ---------------------------------------------------------------------------
# Authorization URL builder
# ---------------------------------------------------------------------------


def _build_auth_url(provider: OAuthProvider, state: str, challenge: str) -> str:
    """Build the provider's authorization URL with query parameters."""
    params = {
        "response_type": "code",
        "client_id": provider.client_id,
        "redirect_uri": provider.redirect_url,
        "scope": " ".join(provider.scopes),
        "state": state,
        "code_challenge": challenge,
        "code_challenge_method": "S256",
    }
    return f"{provider.auth_url}?{urlencode(params)}"


# ---------------------------------------------------------------------------
# User normalization
# ---------------------------------------------------------------------------


def _normalize_user(
    provider_name: str,
    raw: dict[str, Any],
    access_token: str,
) -> OAuthUser:
    """Map provider-specific JSON fields to the unified OAuthUser.

    Handles field naming differences across GitHub, Google, and Discord.
    """
    # ID: try "id" then "sub" (OIDC)
    user_id = ""
    raw_id = raw.get("id")
    if isinstance(raw_id, str):
        user_id = raw_id
    elif isinstance(raw_id, (int, float)):
        user_id = str(int(raw_id))
    elif "sub" in raw:
        user_id = str(raw["sub"])

    # Email
    email = raw.get("email", "") or ""

    # Name: try "name", then "login" (GitHub), then "username" (Discord)
    name = ""
    for key in ("name", "login", "username"):
        val = raw.get(key)
        if isinstance(val, str) and val:
            name = val
            break

    # Avatar: try "avatar_url" (GitHub), then "picture" (Google OIDC)
    avatar_url = ""
    for key in ("avatar_url", "picture"):
        val = raw.get(key)
        if isinstance(val, str) and val:
            avatar_url = val
            break

    # Discord constructs avatar URLs from id + avatar hash
    if provider_name == "discord" and not avatar_url:
        avatar_hash = raw.get("avatar")
        if isinstance(avatar_hash, str) and avatar_hash and user_id:
            avatar_url = f"https://cdn.discordapp.com/avatars/{user_id}/{avatar_hash}.png"

    return OAuthUser(
        id=user_id,
        email=email,
        name=name,
        avatar_url=avatar_url,
        provider=provider_name,
        access_token=access_token,
        raw=raw,
    )


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------


def oauth_redirect_handler(
    provider: OAuthProvider,
) -> Callable[[Request], Awaitable[RedirectResponse]]:
    """Return an async handler that initiates the OAuth2 authorization code flow.

    The handler:
      1. Generates a PKCE challenge (S256) and a random anti-CSRF state.
      2. Stores state + PKCE verifier in a signed HttpOnly cookie.
      3. Redirects the browser to the provider's authorization URL.

    Usage::

        Route("/auth/github", oauth_redirect_handler(github_provider))
    """

    async def handler(request: Request) -> RedirectResponse:
        verifier, challenge = _generate_pkce()
        state = secrets.token_hex(32)

        cookie_val = _encode_state_cookie(state, verifier, provider.secret)
        auth_url = _build_auth_url(provider, state, challenge)

        response = RedirectResponse(url=auth_url, status_code=302)
        response.set_cookie(
            key=_OAUTH_STATE_COOKIE,
            value=cookie_val,
            path="/",
            max_age=_STATE_MAX_AGE,
            httponly=True,
            secure=True,
            samesite="lax",
        )
        return response

    return handler


def oauth_callback_handler(
    provider: OAuthProvider,
    on_success: Callable[[Request, OAuthUser], Awaitable[Any]],
) -> Callable[[Request], Awaitable[Any]]:
    """Return an async handler that completes the OAuth2 authorization code flow.

    On success, calls ``on_success(request, user)`` with the normalized
    :class:`OAuthUser`.  The callback controls the final response (e.g.
    create a session, set a cookie, redirect to the app).

    On failure, returns an RFC 7807 error response.

    Usage::

        async def on_login(request: Request, user: OAuthUser):
            request.state.session["user_id"] = user.id
            return RedirectResponse("/dashboard")

        Route("/auth/github/callback", oauth_callback_handler(github, on_login))
    """

    async def handler(request: Request) -> Any:
        # 1. Extract code and state from the query string.
        code = request.query_params.get("code")
        if not code:
            raise AppError(
                status=400,
                code="bad-request",
                title="Bad Request",
                detail="Missing authorization code",
            )

        state = request.query_params.get("state")
        if not state:
            raise AppError(
                status=400,
                code="bad-request",
                title="Bad Request",
                detail="Missing state parameter",
            )

        # 2. Read and verify the signed state cookie.
        cookie_val = request.cookies.get(_OAUTH_STATE_COOKIE)
        if not cookie_val:
            raise AppError(
                status=403,
                code="forbidden",
                title="Forbidden",
                detail="Missing OAuth state cookie",
            )

        stored_state, verifier, ok = _decode_state_cookie(
            cookie_val, provider.secret
        )
        if not ok:
            raise AppError(
                status=403,
                code="forbidden",
                title="Forbidden",
                detail="Invalid or expired OAuth state cookie",
            )

        # 3. Verify anti-CSRF state matches (constant-time).
        if not hmac.compare_digest(state, stored_state):
            raise AppError(
                status=403,
                code="forbidden",
                title="Forbidden",
                detail="CSRF state mismatch",
            )

        # 4. Exchange the authorization code for tokens.
        token_data = await _exchange_code(provider, code, verifier)
        access_token = token_data.get("access_token", "")
        if not access_token:
            raise AppError(
                status=500,
                code="internal",
                title="Internal Server Error",
                detail="Token exchange returned no access_token",
            )

        # 5. Fetch user information from the provider.
        raw_user = await _fetch_userinfo(provider, access_token)

        # 6. Normalize to OAuthUser.
        name = provider.provider_name or "unknown"
        user = _normalize_user(name, raw_user, access_token)

        # 7. Call the success handler, clearing the state cookie.
        response = await on_success(request, user)

        # Clear the state cookie on the response if possible
        if hasattr(response, "delete_cookie"):
            response.delete_cookie(
                key=_OAUTH_STATE_COOKIE,
                path="/",
            )

        return response

    return handler


# ---------------------------------------------------------------------------
# HTTP helpers (httpx)
# ---------------------------------------------------------------------------


async def _exchange_code(
    provider: OAuthProvider,
    code: str,
    code_verifier: str,
) -> dict[str, Any]:
    """Exchange the authorization code for tokens via the provider's token endpoint."""
    data = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": provider.redirect_url,
        "client_id": provider.client_id,
        "client_secret": provider.client_secret,
        "code_verifier": code_verifier,
    }

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            provider.token_url,
            data=data,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
            },
        )

    if resp.status_code < 200 or resp.status_code >= 300:
        logger.error("OAuth token exchange failed: %s %s", resp.status_code, resp.text)
        raise AppError(
            status=500,
            code="internal",
            title="Internal Server Error",
            detail="OAuth authentication failed",
        )

    # Some providers (GitHub) may return form-encoded instead of JSON.
    body = resp.text
    if body.startswith("{"):
        return resp.json()

    # Parse form-encoded response
    from urllib.parse import parse_qs

    parsed = parse_qs(body)
    return {k: v[0] for k, v in parsed.items()}


async def _fetch_userinfo(
    provider: OAuthProvider,
    access_token: str,
) -> dict[str, Any]:
    """Fetch user information from the provider's userinfo endpoint."""
    if not provider.userinfo_url:
        return {"id": access_token[:16]}

    async with httpx.AsyncClient() as client:
        resp = await client.get(
            provider.userinfo_url,
            headers={
                "Authorization": f"Bearer {access_token}",
                "Accept": "application/json",
            },
        )

    if resp.status_code < 200 or resp.status_code >= 300:
        logger.error("OAuth userinfo fetch failed: %s %s", resp.status_code, resp.text)
        raise AppError(
            status=500,
            code="internal",
            title="Internal Server Error",
            detail="OAuth authentication failed",
        )

    return resp.json()
