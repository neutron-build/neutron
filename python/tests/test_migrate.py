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
        if statement.startswith("SELECT pg_catalog.pg_advisory_xact_lock"):
            self.events.append(("execute", statement, *args))
            if self._lock_error is not None:
                raise self._lock_error
            return
        self.events.append(("execute", statement, *args))
        if statement.startswith('INSERT INTO "public"."_neutron_migrations"'):
            self.applied.add(args[0])

    async def fetch(self, sql: str) -> list[dict[str, int]]:
        self.events.append(("fetch", sql))
        return [{"version": version} for version in self.applied]

    async def fetchval(self, sql, *args):
        if sql == "SELECT pg_catalog.version()":
            return "PostgreSQL 17.0"
        if sql == "SELECT pg_catalog.current_schema()":
            return "public"
        if "to_regclass" in sql:
            return None
        raise AssertionError(sql)

    async def fetchrow(self, sql, *args):
        return None

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
        "SELECT pg_catalog.pg_advisory_xact_lock($1)",
    )
    create_index = next(
        index
        for index, event in enumerate(conn.events)
        if event[0] == "execute"
        and event[1].startswith(
            'CREATE TABLE IF NOT EXISTS "public"."_neutron_migrations"'
        )
    )
    fetch_index = conn.events.index(
        ("fetch", 'SELECT version FROM "public"."_neutron_migrations"')
    )
    up_indexes = [
        conn.events.index(("execute", "up two")),
        conn.events.index(("execute", "up three")),
    ]
    assert 1 < create_index < fetch_index < min(up_indexes)
    assert conn.events[-1] == ("transaction", "end")


async def test_backend_without_advisory_locks_refuses_without_mutation():
    conn = _Connection(
        applied=set(),
        lock_error=asyncpg.exceptions.UndefinedFunctionError("missing lock"),
    )
    with pytest.raises(asyncpg.PostgresError):
        await Migrator(_Pool(conn)).run_migrations([Migration(1, "first", "up one")])
    assert conn.applied == set()
    assert not any(
        event[0] == "execute" and event[1].startswith(("CREATE", "up"))
        for event in conn.events
    )


async def test_a_real_lock_failure_is_not_swallowed():
    conn = _Connection(
        applied=set(), lock_error=asyncpg.exceptions.DeadlockDetectedError("boom")
    )
    migrations = [Migration(1, "first", "up one")]

    with pytest.raises(asyncpg.PostgresError):
        await Migrator(_Pool(conn)).run_migrations(migrations)

    assert conn.applied == set()
    assert ("execute", "up one") not in conn.events


@pytest.mark.parametrize(
    "version", ["PostgreSQL 16.0 (Nucleus 1.2.0)", "Unknown pgwire server"]
)
async def test_provider_refusal_precedes_lock_or_history(version):
    class ProviderConnection(_Connection):
        async def fetchval(self, sql, *args):
            if sql == "SELECT pg_catalog.version()":
                return version
            return await super().fetchval(sql, *args)

    conn = ProviderConnection(set())
    with pytest.raises(ValueError, match="require PostgreSQL"):
        await Migrator(_Pool(conn)).run_migrations([Migration(1, "a", "up one")])
    assert not any(e[0] == "execute" for e in conn.events)


@pytest.mark.parametrize("version", [True, 0, -1, 2147483648, 1.5])
async def test_invalid_version_refuses_before_connection(version):
    conn = _Connection(set())
    with pytest.raises(ValueError, match="positive PostgreSQL INTEGER"):
        await Migrator(_Pool(conn)).run_migrations([Migration(version, "a", "up one")])
    assert conn.events == []


async def test_unlock_failure_discards_connection():
    class UnlockConnection(_Connection):
        terminated = False

        async def fetchval(self, sql, *args):
            if "pg_advisory_unlock" in sql:
                raise asyncpg.ConnectionDoesNotExistError("unlock transport failed")
            return await super().fetchval(sql, *args)

        def terminate(self):
            self.terminated = True

    conn = UnlockConnection({1})
    with pytest.raises(asyncpg.ConnectionDoesNotExistError):
        await Migrator(_Pool(conn)).rollback([Migration(1, "a", "", "down one")], 0)
    assert conn.terminated
