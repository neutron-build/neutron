"""JWT authentication — create, decode, middleware, and dependency.

Supports HS256 (symmetric), RS256 (RSA), and ES256 (ECDSA).
Uses PyJWT when available for RSA/ECDSA; falls back to stdlib HMAC for HS256.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from typing import Any, Callable

from starlette.requests import Request
from starlette.responses import JSONResponse

from neutron.depends import _Depends
from neutron.error import AppError

_SUPPORTED_ALGORITHMS = {"HS256", "RS256", "ES256"}


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _b64url_decode(s: str) -> bytes:
    padding = 4 - len(s) % 4
    if padding != 4:
        s += "=" * padding
    return base64.urlsafe_b64decode(s)


def create_token(
    payload: dict[str, Any],
    secret: str,
    *,
    expires_in: int = 3600,
    algorithm: str = "HS256",
) -> str:
    """Create a JWT token.

    Args:
        payload: Claims to encode.
        secret: Secret key (HS256) or PEM private key (RS256/ES256).
        expires_in: Token lifetime in seconds (default 1 hour).
        algorithm: HS256, RS256, or ES256.

    Returns:
        Encoded JWT string.
    """
    if algorithm not in _SUPPORTED_ALGORITHMS:
        raise ValueError(
            f"Unsupported algorithm: {algorithm}. "
            f"Supported: {', '.join(sorted(_SUPPORTED_ALGORITHMS))}"
        )

    now = int(time.time())
    claims = {**payload, "iat": now, "exp": now + expires_in}

    if algorithm in ("RS256", "ES256"):
        try:
            import jwt as pyjwt
        except ImportError:
            raise ImportError(
                f"{algorithm} requires PyJWT with cryptography: "
                "pip install PyJWT[crypto]"
            )
        return pyjwt.encode(claims, secret, algorithm=algorithm)

    # HS256 — stdlib implementation (no dependencies)
    header = {"alg": "HS256", "typ": "JWT"}
    segments = [
        _b64url_encode(json.dumps(header).encode()),
        _b64url_encode(json.dumps(claims).encode()),
    ]
    signing_input = f"{segments[0]}.{segments[1]}"
    sig = hmac.new(
        secret.encode(), signing_input.encode(), hashlib.sha256
    ).digest()
    segments.append(_b64url_encode(sig))
    return ".".join(segments)


def decode_token(
    token: str,
    secret: str,
    *,
    algorithms: list[str] | None = None,
    verify_exp: bool = True,
) -> dict[str, Any]:
    """Decode and verify a JWT token.

    Args:
        token: Encoded JWT string.
        secret: Secret key (HS256) or PEM public key (RS256/ES256).
        algorithms: Allowed algorithms (default: ["HS256"]).
        verify_exp: Whether to check expiration.

    Raises:
        AppError: If token is invalid or expired (401).
    """
    allowed = algorithms or ["HS256"]

    parts = token.split(".")
    if len(parts) != 3:
        raise AppError(
            status=401, code="invalid_token",
            title="Invalid Token", detail="Malformed JWT",
        )

    try:
        header = json.loads(_b64url_decode(parts[0]))
    except Exception:
        raise AppError(
            status=401, code="invalid_token",
            title="Invalid Token", detail="Failed to decode JWT header",
        )

    alg = header.get("alg", "HS256")
    if alg not in allowed:
        raise AppError(
            status=401, code="invalid_token",
            title="Invalid Token", detail=f"Unsupported algorithm: {alg}",
        )

    # RS256/ES256 — delegate to PyJWT
    if alg in ("RS256", "ES256"):
        try:
            import jwt as pyjwt
        except ImportError:
            raise AppError(
                status=500, code="config_error",
                title="Configuration Error",
                detail=f"{alg} requires PyJWT: pip install PyJWT[crypto]",
            )
        try:
            return pyjwt.decode(
                token, secret, algorithms=[alg],
                options={"verify_exp": verify_exp},
            )
        except pyjwt.ExpiredSignatureError:
            raise AppError(
                status=401, code="token_expired",
                title="Token Expired", detail="JWT has expired",
            )
        except pyjwt.InvalidTokenError as e:
            raise AppError(
                status=401, code="invalid_token",
                title="Invalid Token", detail=str(e),
            )

    # HS256 — stdlib verification
    try:
        payload = json.loads(_b64url_decode(parts[1]))
        signature = _b64url_decode(parts[2])
    except Exception:
        raise AppError(
            status=401, code="invalid_token",
            title="Invalid Token", detail="Failed to decode JWT",
        )

    signing_input = f"{parts[0]}.{parts[1]}"
    expected = hmac.new(
        secret.encode(), signing_input.encode(), hashlib.sha256
    ).digest()
    if not hmac.compare_digest(signature, expected):
        raise AppError(
            status=401, code="invalid_token",
            title="Invalid Token", detail="Invalid signature",
        )

    if verify_exp and "exp" in payload:
        if time.time() > payload["exp"]:
            raise AppError(
                status=401, code="token_expired",
                title="Token Expired", detail="JWT has expired",
            )

    return payload


class JWTMiddleware:
    """ASGI middleware that validates JWT tokens from the Authorization header.

    Sets ``request.state.user`` with the decoded payload on success.
    Requests without a token pass through (use ``get_current_user`` to enforce).
    """

    def __init__(
        self,
        secret: str,
        *,
        exclude_paths: list[str] | None = None,
        algorithms: list[str] | None = None,
    ) -> None:
        if len(secret) < 32:
            raise ValueError(
                f"JWT secret must be at least 32 characters for HS256 security. Got {len(secret)}."
            )
        self.secret = secret
        self.exclude_paths = set(exclude_paths or ["/health", "/docs", "/openapi.json"])
        self.algorithms = algorithms

    def as_starlette_middleware(self) -> Any:
        from starlette.middleware import Middleware

        return Middleware(
            _JWTMiddlewareImpl,
            secret=self.secret,
            exclude_paths=self.exclude_paths,
            algorithms=self.algorithms,
        )


class _JWTMiddlewareImpl:
    def __init__(self, app: Any, secret: str, exclude_paths: set, algorithms: list | None) -> None:
        self.app = app
        self.secret = secret
        self.exclude_paths = exclude_paths
        self.algorithms = algorithms

    async def __call__(self, scope: dict, receive: Any, send: Any) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "")
        if path in self.exclude_paths:
            await self.app(scope, receive, send)
            return

        request = Request(scope, receive)
        auth_header = request.headers.get("authorization", "")

        if auth_header.startswith("Bearer "):
            token = auth_header[7:]
            try:
                payload = decode_token(token, self.secret, algorithms=self.algorithms)
                scope.setdefault("state", {})["user"] = payload
            except AppError:
                # Invalid token — let it through, get_current_user will raise
                scope.setdefault("state", {})["user"] = None
        else:
            scope.setdefault("state", {})["user"] = None

        await self.app(scope, receive, send)


def get_current_user(request: Request) -> dict[str, Any]:
    """Dependency that extracts the current user from JWT.

    Usage::

        @router.get("/me")
        async def me(user: dict = Depends(get_current_user)) -> dict: ...
    """
    user = getattr(request.state, "user", None)
    if user is None:
        raise AppError(
            status=401,
            code="unauthorized",
            title="Unauthorized",
            detail="Authentication required",
        )
    return user
