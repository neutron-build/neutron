"""SQL file-based schema migrations."""

from __future__ import annotations

import os
from dataclasses import dataclass

import asyncpg


_MIGRATION_LOCK_KEY = 0x6E657574726F6E

# SQLSTATE 42883 undefined_function / 0A000 feature_not_supported: the backend
# has no advisory locks at all. Nucleus is the known case — it accepts
# pg_advisory_unlock_all (asyncpg pool reset) but implements no lock function —
# so an unconditional lock would make every Nucleus migration run fail at the
# first statement. On such a backend the run proceeds unlocked: the claim this
# lock defends, two application replicas booting against one fresh database,
# is a Postgres deployment shape.
_LOCK_UNSUPPORTED_SQLSTATES = frozenset({"42883", "0A000"})


@dataclass
class Migration:
    version: int
    name: str
    up: str
    down: str = ""


class Migrator:
    """Run SQL migrations against a PostgreSQL/Nucleus database."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def _ensure_table(self, conn: asyncpg.Connection) -> None:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS _neutron_migrations (
                version INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                applied_at TIMESTAMPTZ DEFAULT NOW()
            )
            """
        )

    async def _read_applied(self, conn: asyncpg.Connection) -> set[int]:
        rows = await conn.fetch("SELECT version FROM _neutron_migrations")
        return {row["version"] for row in rows}

    async def get_applied(self) -> set[int]:
        async with self._pool.acquire() as conn:
            await self._ensure_table(conn)
            return await self._read_applied(conn)

    async def migrate(self, migrations_dir: str) -> list[str]:
        """Run pending migrations from a directory.

        Files are named ``NNN_description.sql``.  Optionally include
        a ``-- DOWN`` marker to separate up/down SQL.
        """
        migrations = _load_from_dir(migrations_dir)
        return await self.run_migrations(migrations)

    async def rollback(self, migrations: list[Migration], target_version: int) -> list[str]:
        """Roll back applied migrations above ``target_version``, newest first.

        For each applied migration with version > target, runs its ``down`` SQL
        and removes its ``_neutron_migrations`` row in one transaction, so the
        schema and the record of what is applied can never disagree. A
        migration whose ``down`` is empty cannot be reversed: rather than skip
        it (which would leave later migrations rolled back while this one's
        changes linger), the rollback raises ``ValueError`` naming the version.
        An operator who hits that must add a DOWN section or restore a backup --
        the alternative is a silently-inconsistent schema.

        No-op (returns ``[]``) when nothing above ``target_version`` is applied.
        """
        async with self._pool.acquire() as conn:
            await self._ensure_table(conn)
            applied = await self._read_applied(conn)
        results: list[str] = []

        for m in sorted(migrations, key=lambda x: x.version, reverse=True):
            if m.version <= target_version or m.version not in applied:
                continue
            if not m.down:
                raise ValueError(
                    f"migration {m.version}_{m.name} has no DOWN section; "
                    f"cannot roll back without a backup restore"
                )
            async with self._pool.acquire() as conn:
                async with conn.transaction():
                    await conn.execute(m.down)
                    await conn.execute(
                        "DELETE FROM _neutron_migrations WHERE version = $1",
                        m.version,
                    )
            results.append(f"Rolled back: {m.version}_{m.name}")

        return results

    async def rollback_dir(self, migrations_dir: str, target_version: int) -> list[str]:
        """Roll back to ``target_version`` from a migrations directory."""
        migrations = _load_from_dir(migrations_dir)
        return await self.rollback(migrations, target_version)

    async def run_migrations(self, migrations: list[Migration]) -> list[str]:
        """Apply pending migrations, skipping already-applied ones.

        The whole run — lock, versions read, every migration — is one
        transaction behind ``pg_advisory_xact_lock``, so two processes
        booting against the same fresh database serialise: the second
        waits for the first to commit, re-reads the versions table, and
        finds nothing left to do. Without the lock both read an empty
        table, both run migration 001's DDL, and the loser crashes on a
        duplicate object or primary key. The lock is transaction-scoped
        and released automatically at commit or rollback; a run that
        dies mid-migration rolls back to a clean boundary instead of
        leaving partial progress recorded.
        """
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                try:
                    await conn.execute(
                        "SELECT pg_advisory_xact_lock($1)",
                        _MIGRATION_LOCK_KEY,
                    )
                except asyncpg.PostgresError as exc:
                    if getattr(exc, "sqlstate", None) not in _LOCK_UNSUPPORTED_SQLSTATES:
                        raise
                await self._ensure_table(conn)
                applied = await self._read_applied(conn)
                results: list[str] = []

                for m in sorted(migrations, key=lambda x: x.version):
                    if m.version in applied:
                        continue
                    await conn.execute(m.up)
                    await conn.execute(
                        "INSERT INTO _neutron_migrations (version, name) VALUES ($1, $2)",
                        m.version,
                        m.name,
                    )
                    results.append(f"Applied: {m.version}_{m.name}")

        return results


def _load_from_dir(path: str) -> list[Migration]:
    migrations: list[Migration] = []
    if not os.path.isdir(path):
        return migrations
    for filename in sorted(os.listdir(path)):
        if not filename.endswith(".sql"):
            continue
        parts = filename.split("_", 1)
        if len(parts) < 2:
            continue
        try:
            version = int(parts[0])
        except ValueError:
            continue
        name = parts[1].removesuffix(".sql")
        with open(os.path.join(path, filename)) as f:
            sql = f.read()
        sections = sql.split("-- DOWN", 1)
        up = sections[0].strip()
        down = sections[1].strip() if len(sections) > 1 else ""
        migrations.append(Migration(version, name, up, down))
    return migrations
