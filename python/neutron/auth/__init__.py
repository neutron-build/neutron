"""Neutron Auth — JWT, sessions, API keys, RBAC, and password hashing."""

from neutron.auth.jwt import JWTMiddleware, create_token, decode_token, get_current_user
from neutron.auth.password import hash_password, needs_rehash, verify_password
from neutron.auth.session import SessionMiddleware, MemorySessionStore, NucleusSessionStore
from neutron.auth.apikey import APIKeyMiddleware
from neutron.auth.rbac import require_role, require_permission

__all__ = [
    # JWT
    "JWTMiddleware",
    "create_token",
    "decode_token",
    "get_current_user",
    # Password
    "hash_password",
    "verify_password",
    "needs_rehash",
    # Sessions
    "SessionMiddleware",
    "MemorySessionStore",
    "NucleusSessionStore",
    # API Key
    "APIKeyMiddleware",
    # RBAC
    "require_role",
    "require_permission",
]
