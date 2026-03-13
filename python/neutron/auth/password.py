"""Password hashing — argon2 (preferred) with bcrypt fallback.

Usage::

    from neutron.auth import hash_password, verify_password

    hashed = hash_password("secret123")
    assert verify_password("secret123", hashed)
"""

from __future__ import annotations


def hash_password(password: str) -> str:
    """Hash a password using argon2id (preferred) or bcrypt (fallback).

    Returns:
        A string containing the algorithm identifier and hash.
    """
    try:
        from argon2 import PasswordHasher

        ph = PasswordHasher()
        return ph.hash(password)
    except ImportError:
        pass

    try:
        import bcrypt

        salt = bcrypt.gensalt()
        return bcrypt.hashpw(password.encode(), salt).decode()
    except ImportError:
        pass

    raise ImportError(
        "Password hashing requires argon2-cffi or bcrypt. "
        "Install one: pip install argon2-cffi"
    )


def verify_password(password: str, hashed: str) -> bool:
    """Verify a password against a hash.

    Auto-detects the algorithm from the hash prefix.
    """
    if hashed.startswith("$argon2"):
        try:
            from argon2 import PasswordHasher
            from argon2.exceptions import (
                InvalidHashError,
                VerificationError,
                VerifyMismatchError,
            )

            ph = PasswordHasher()
            try:
                return ph.verify(hashed, password)
            except (VerifyMismatchError, VerificationError, InvalidHashError):
                return False
        except ImportError:
            raise ImportError(
                "argon2-cffi is required to verify argon2 hashes: "
                "pip install argon2-cffi"
            )

    if hashed.startswith("$2b$") or hashed.startswith("$2a$"):
        try:
            import bcrypt

            return bcrypt.checkpw(password.encode(), hashed.encode())
        except ImportError:
            raise ImportError(
                "bcrypt is required to verify bcrypt hashes: "
                "pip install bcrypt"
            )

    raise ValueError(f"Unknown hash format: {hashed[:10]}...")


def needs_rehash(hashed: str) -> bool:
    """Check if a hash should be upgraded (e.g., bcrypt → argon2).

    Returns True if:
    - The hash uses bcrypt and argon2 is available
    - The hash uses outdated argon2 parameters
    """
    if hashed.startswith("$2b$") or hashed.startswith("$2a$"):
        try:
            import argon2  # noqa: F401
            return True  # Upgrade bcrypt → argon2
        except ImportError:
            return False

    if hashed.startswith("$argon2"):
        try:
            from argon2 import PasswordHasher

            ph = PasswordHasher()
            return ph.check_needs_rehash(hashed)
        except ImportError:
            return False

    return False
