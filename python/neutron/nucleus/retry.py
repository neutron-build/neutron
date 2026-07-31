"""Retrying transaction helper.

A ``SERIALIZABLE`` transaction can lose a conflict and must then be re-run from
the beginning. PostgreSQL drivers surface the SQLSTATE and stop there — deciding
to retry is the application's job, and a framework SDK is that layer. Without
this, a serializable transaction simply fails at random under concurrency.
"""

from __future__ import annotations

import asyncio
import random
from typing import TYPE_CHECKING, Any, Awaitable, Callable, TypeVar

if TYPE_CHECKING:  # pragma: no cover - typing only
    from neutron.nucleus.tx import Transaction

T = TypeVar("T")

#: The transaction lost a conflict and MUST be retried from the start. Nucleus
#: raises it from strict 2PL wait-die on the disk engine (the younger
#: transaction is killed to break a potential deadlock) and from SSI on the
#: MVCC engine (a dangerous structure detected at commit).
SERIALIZATION_FAILURE = "40001"

#: ``lock_timeout`` elapsed waiting for a table lock. Deliberately NOT
#: retryable: the holder is still there, so retrying spins against a lock that
#: is not moving. Raise ``lock_timeout`` or find the holder.
LOCK_NOT_AVAILABLE = "55P03"

#: A statement was issued after the transaction had already been aborted. The
#: transaction is dead and only ROLLBACK is accepted, so the whole transaction
#: has to be re-run — which is what :func:`with_tx` does.
IN_FAILED_TRANSACTION = "25P02"


def sqlstate(exc: BaseException) -> str | None:
    """The SQLSTATE of ``exc``, or ``None`` if it does not carry one.

    Reads ``sqlstate``, which asyncpg sets on every ``PostgresError``. Falls
    back to ``pgcode`` so a psycopg-shaped error is understood too.
    """
    for attr in ("sqlstate", "pgcode"):
        code = getattr(exc, attr, None)
        if isinstance(code, str) and code:
            return code
    return None


def is_serialization_failure(exc: BaseException) -> bool:
    """Whether ``exc`` is a conflict the caller should retry.

    Classification is by SQLSTATE, never by message text: the code is the
    contract, the message is free-form and changes. Nucleus itself shipped that
    bug twice — a 2PL kill reported as XX000, then its follow-up error reported
    as XX000 — so the client half is checked explicitly.
    """
    return sqlstate(exc) in (SERIALIZATION_FAILURE, IN_FAILED_TRANSACTION)


def is_lock_not_available(exc: BaseException) -> bool:
    """Whether ``exc`` is a ``lock_timeout`` expiry (55P03).

    Kept distinct from :func:`is_serialization_failure` because the two call for
    opposite responses: a conflict means "someone else won, try again"; a lock
    timeout means "the lock is still held, retrying will not help".
    """
    return sqlstate(exc) == LOCK_NOT_AVAILABLE


class RetryExhausted(Exception):
    """Raised when a transaction kept losing conflicts.

    ``__cause__`` is the last serialization failure.
    """

    def __init__(self, attempts: int) -> None:
        super().__init__(f"transaction did not succeed in {attempts} attempt(s)")
        self.attempts = attempts


async def with_tx(
    db: Any,
    fn: Callable[["Transaction"], Awaitable[T]],
    *,
    max_attempts: int = 5,
    base_delay: float = 0.002,
    max_delay: float = 0.25,
    isolation: str | None = None,
) -> T:
    """Run ``fn`` in a transaction, retrying it on serialization failure.

    ``fn`` **must be idempotent with respect to anything outside the database**:
    it can run more than once. Everything it does through the passed
    ``Transaction`` is rolled back between attempts; anything it does elsewhere
    (sending mail, charging a card, mutating module state) is not.

    A ``lock_timeout`` (55P03) is **not** retried — the lock is still held.

    Backoff is randomised (full jitter). Without it two conflicting
    transactions retry in lockstep and collide again on the same schedule; under
    wait-die the younger one loses every round, so a fixed backoff can starve it
    indefinitely.

    ::

        await with_tx(
            db,
            lambda tx: tx.sql.execute("UPDATE accounts SET balance = balance - 10 WHERE id = $1", 1),
            isolation="SERIALIZABLE",
        )
    """
    attempts = max(1, max_attempts)
    delay = base_delay if base_delay > 0 else 0.002
    cap = max_delay if max_delay >= delay else delay
    last: BaseException | None = None

    for attempt in range(1, attempts + 1):
        try:
            async with db.transaction() as tx:
                if isolation:
                    await tx.sql.execute(
                        f"SET TRANSACTION ISOLATION LEVEL {isolation}"
                    )
                return await fn(tx)
        except BaseException as exc:  # noqa: BLE001 - re-raised below
            if not is_serialization_failure(exc):
                raise
            last = exc
            if attempt == attempts:
                break
            await asyncio.sleep(random.uniform(0, delay))
            delay = min(delay * 2, cap)

    err = RetryExhausted(attempts)
    err.__cause__ = last
    raise err
