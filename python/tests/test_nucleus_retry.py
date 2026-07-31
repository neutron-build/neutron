"""Retrying transaction helper: classification and retry policy.

The SQLSTATE is the contract — a driver surfaces it and stops, and the
application decides whether to retry. These tests pin the decision, because
getting it wrong is silent in both directions: retrying a lock timeout spins
against a lock that is not moving, and not retrying a conflict makes a
serializable transaction fail at random under concurrency.
"""

from __future__ import annotations

import pytest

from neutron.nucleus.retry import (
    IN_FAILED_TRANSACTION,
    LOCK_NOT_AVAILABLE,
    SERIALIZATION_FAILURE,
    RetryExhausted,
    is_lock_not_available,
    is_serialization_failure,
    sqlstate,
    with_tx,
)


class PgErr(Exception):
    """Minimal stand-in for an asyncpg PostgresError."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.sqlstate = code


class _FakeTx:
    def __init__(self) -> None:
        self.sql = self

    async def execute(self, *args: object, **kwargs: object) -> None:
        return None

    async def __aenter__(self) -> "_FakeTx":
        return self

    async def __aexit__(self, *exc: object) -> bool:
        return False


class _FakeDB:
    def transaction(self) -> _FakeTx:
        return _FakeTx()


# ── classification ───────────────────────────────────────────────────────────

@pytest.mark.parametrize(
    "code,retryable",
    [
        (SERIALIZATION_FAILURE, True),
        (IN_FAILED_TRANSACTION, True),
        # The one that must NOT be lumped in with conflicts.
        (LOCK_NOT_AVAILABLE, False),
        ("23505", False),  # unique violation
        ("XX000", False),  # internal — the code Nucleus wrongly used twice
    ],
)
def test_serialization_failure_classification(code: str, retryable: bool) -> None:
    assert is_serialization_failure(PgErr(code)) is retryable


def test_a_plain_exception_is_not_retryable() -> None:
    assert is_serialization_failure(ValueError("boom")) is False
    assert sqlstate(ValueError("boom")) is None


def test_lock_timeout_is_distinguishable() -> None:
    assert is_lock_not_available(PgErr(LOCK_NOT_AVAILABLE))
    assert not is_lock_not_available(PgErr(SERIALIZATION_FAILURE))


# ── retry policy ─────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_retries_a_conflict_then_succeeds() -> None:
    calls = {"n": 0}

    async def fn(tx: object) -> str:
        calls["n"] += 1
        if calls["n"] <= 2:
            raise PgErr(SERIALIZATION_FAILURE)
        return "done"

    assert await with_tx(_FakeDB(), fn, base_delay=0.0001) == "done"
    assert calls["n"] == 3


@pytest.mark.asyncio
async def test_gives_up_after_max_attempts_and_keeps_the_cause() -> None:
    async def always(tx: object) -> None:
        raise PgErr(SERIALIZATION_FAILURE)

    with pytest.raises(RetryExhausted) as excinfo:
        await with_tx(_FakeDB(), always, max_attempts=3, base_delay=0.0001)
    assert excinfo.value.attempts == 3
    # The original failure must survive, or the caller cannot tell WHY.
    assert isinstance(excinfo.value.__cause__, PgErr)


@pytest.mark.asyncio
async def test_a_lock_timeout_is_attempted_exactly_once() -> None:
    """55P03 must not be retried: the holder is still there.

    Retrying it converts one stuck transaction into a busy loop against a lock
    that is not going to move.
    """
    calls = {"n": 0}

    async def locked(tx: object) -> None:
        calls["n"] += 1
        raise PgErr(LOCK_NOT_AVAILABLE)

    with pytest.raises(PgErr):
        await with_tx(_FakeDB(), locked, max_attempts=5, base_delay=0.0001)
    assert calls["n"] == 1


@pytest.mark.asyncio
async def test_a_non_retryable_error_propagates_unchanged() -> None:
    async def dup(tx: object) -> None:
        raise PgErr("23505")

    with pytest.raises(PgErr) as excinfo:
        await with_tx(_FakeDB(), dup, base_delay=0.0001)
    assert excinfo.value.sqlstate == "23505"
