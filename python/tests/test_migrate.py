"""Unit contracts for PostgreSQL schema migration locking."""

from __future__ import annotations

from contextlib import asynccontextmanager

import asyncpg
import pytest

from neutron.nucleus.migrate import Migration, Migrator


class _Connection:
    def __init__(self, applied: set[int], lock_error: Exception | None = None) -> None:
        self.applied = applied
        self.events: list[tuple] = []
        self._lock_error = lock_error

    async def execute(self, sql: str, *args: object) -> None:
        statement = " ".join(sql.split())
        if statement.startswith("SELECT pg_advisory_xact_lock"):
            self.events.append(("execute", statement, *args))
            if self._lock_error is not None:
                raise self._lock_error
            return
        self.events.append(("execute", statement, *args))
        if statement.startswith("INSERT INTO _neutron_migrations"):
            self.applied.add(args[0])

    async def fetch(self, sql: str) -> list[dict[str, int]]:
        self.events.append(("fetch", sql))
        return [{"version": version} for version in self.applied]

    def transaction(self):
        @asynccontextmanager
        async def transaction():
            self.events.append(("transaction", "begin"))
            try:
                yield
            finally:
                self.events.append(("transaction", "end"))

        return transaction()


class _Pool:
    def __init__(self, conn: _Connection) -> None:
        self.conn = conn

    @asynccontextmanager
    async def acquire(self):
        yield self.conn


async def test_run_migrations_locks_then_rereads_and_holds_one_transaction():
    conn = _Connection(applied={1})
    migrations = [
        Migration(3, "third", "up three"),
        Migration(1, "first", "must not run"),
        Migration(2, "second", "up two"),
    ]

    results = await Migrator(_Pool(conn)).run_migrations(migrations)

    assert results == ["Applied: 2_second", "Applied: 3_third"]
    assert conn.applied == {1, 2, 3}
    assert conn.events[0] == ("transaction", "begin")
    assert conn.events[1][0:2] == (
        "execute",
        "SELECT pg_advisory_xact_lock($1)",
    )
    create_index = next(
        index
        for index, event in enumerate(conn.events)
        if event[0] == "execute"
        and event[1].startswith("CREATE TABLE IF NOT EXISTS _neutron_migrations")
    )
    fetch_index = conn.events.index(
        ("fetch", "SELECT version FROM _neutron_migrations")
    )
    up_indexes = [
        conn.events.index(("execute", "up two")),
        conn.events.index(("execute", "up three")),
    ]
    assert 1 < create_index < fetch_index < min(up_indexes)
    assert conn.events[-1] == ("transaction", "end")


async def test_backend_without_advisory_locks_still_migrates():
    conn = _Connection(
        applied=set(),
        lock_error=asyncpg.exceptions.UndefinedFunctionError(
            "function pg_advisory_xact_lock(bigint) does not exist"
        ),
    )
    migrations = [Migration(1, "first", "up one"), Migration(2, "second", "up two")]

    results = await Migrator(_Pool(conn)).run_migrations(migrations)

    assert results == ["Applied: 1_first", "Applied: 2_second"]
    assert conn.applied == {1, 2}
    assert conn.events[0] == ("transaction", "begin")
    assert conn.events[1][0:2] == ("execute", "SELECT pg_advisory_xact_lock($1)")
    assert ("execute", "up one") in conn.events


async def test_a_real_lock_failure_is_not_swallowed():
    conn = _Connection(
        applied=set(), lock_error=asyncpg.exceptions.DeadlockDetectedError("boom")
    )
    migrations = [Migration(1, "first", "up one")]

    with pytest.raises(asyncpg.PostgresError):
        await Migrator(_Pool(conn)).run_migrations(migrations)

    assert conn.applied == set()
    assert ("execute", "up one") not in conn.events
