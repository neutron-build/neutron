"""SQL file-based schema migrations."""

from __future__ import annotations

import asyncio
import os
import re
from dataclasses import dataclass

import asyncpg


# Shared with the canonical CLI, whose TEXT/v2 history remains inadmissible.
_MIGRATION_LOCK_KEY = 7043516000858342567


def _capture_plan(migrations: list[Migration]) -> tuple[Migration, ...]:
    seen: set[int] = set()
    captured = []
    for m in migrations:
        if type(m.version) is not int or not 0 < m.version <= 2147483647:
            raise ValueError("migration version must be a positive PostgreSQL INTEGER")
        if m.version in seen:
            raise ValueError(f"duplicate migration version {m.version}")
        if not all(isinstance(value, str) for value in (m.name, m.up, m.down)):
            raise ValueError(f"migration {m.version}: name and SQL must be strings")
        seen.add(m.version)
        captured.append(Migration(m.version, m.name, m.up, m.down))
    return tuple(sorted(captured, key=lambda m: m.version))


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


@dataclass
class Migration:
    version: int
    name: str
    up: str
    down: str = ""


class Migrator:
    """Legacy PostgreSQL-only migrations for an exclusively Python-owned scope.

    History has no checksums or ownership marker. Identically shaped foreign
    legacy histories cannot be identified; sharing their scope is unsupported.
    Known foreign/v2 histories and Nucleus are refused without adoption.
    """

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def _provider_schema(self, conn: asyncpg.Connection) -> str:
        version = await conn.fetchval("SELECT pg_catalog.version()")
        if (
            not isinstance(version, str)
            or "Nucleus" in version
            or not re.match(r"^PostgreSQL [0-9]+\.", version)
        ):
            raise ValueError(
                "Python migrations require PostgreSQL advisory locks; Nucleus and unknown providers are unsupported"
            )
        schema = await conn.fetchval("SELECT pg_catalog.current_schema()")
        if (
            not schema
            or schema.startswith("pg_temp")
            or schema.startswith("pg_toast")
            or schema == "pg_catalog"
        ):
            raise ValueError(
                "Python migrations require a non-temporary application schema"
            )
        return schema

    async def _admit(self, conn: asyncpg.Connection, schema: str) -> str:
        table = f"{_quote(schema)}.{_quote('_neutron_migrations')}"
        relation = await conn.fetchrow(
            """
            SELECT c.oid, c.relkind::text AS relkind, c.relpersistence::text AS relpersistence
            FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
            WHERE n.nspname=$1 AND c.relname='_neutron_migrations'
        """,
            schema,
        )
        visible = await conn.fetchval(
            "SELECT pg_catalog.to_regclass('_neutron_migrations')::oid"
        )
        if visible is not None and (relation is None or visible != relation["oid"]):
            raise ValueError(
                "migration history is shadowed by another search_path relation; use its designated runner"
            )
        if relation is None:
            return table
        if relation["relkind"] != "r" or relation["relpersistence"] != "p":
            raise ValueError(
                "migration history must be an ordinary persistent legacy Python table"
            )
        columns = await conn.fetch(
            """
            SELECT a.attname, a.atttypid, a.attnotnull, t.typtype::text AS typtype, n.nspname
            FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_type t ON t.oid=a.atttypid
            JOIN pg_catalog.pg_namespace n ON n.oid=t.typnamespace
            WHERE a.attrelid=$1 AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum
        """,
            relation["oid"],
        )
        types = {"version": 23, "name": 25, "applied_at": 1184}
        if (
            len(columns) != 3
            or {c["attname"] for c in columns} != set(types)
            or any(
                c["atttypid"] != types[c["attname"]]
                or c["typtype"] != "b"
                or c["nspname"] != "pg_catalog"
                or (c["attname"] in {"version", "name"} and not c["attnotnull"])
                for c in columns
            )
        ):
            raise ValueError(
                "migration history is not the legacy Python INTEGER shape (foreign/v2 history requires its designated runner; no implicit adoption)"
            )
        primary = await conn.fetchval(
            """
            SELECT count(*) FROM pg_catalog.pg_constraint k
            JOIN pg_catalog.pg_attribute a ON a.attrelid=k.conrelid AND a.attname='version'
            WHERE k.conrelid=$1 AND k.contype='p' AND k.conkey=ARRAY[a.attnum]::smallint[]
        """,
            relation["oid"],
        )
        if primary != 1:
            raise ValueError(
                "legacy Python migration history requires a single-column version primary key"
            )
        return table

    async def _ensure_table(self, conn: asyncpg.Connection, table: str) -> None:
        await conn.execute(f"""CREATE TABLE IF NOT EXISTS {table} (
            version INTEGER PRIMARY KEY, name TEXT NOT NULL,
            applied_at TIMESTAMPTZ DEFAULT NOW())""")

    async def _read_applied(self, conn: asyncpg.Connection, table: str) -> set[int]:
        rows = await conn.fetch(f"SELECT version FROM {table}")
        return {row["version"] for row in rows}

    async def get_applied(self) -> set[int]:
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                schema = await self._provider_schema(conn)
                await conn.execute(
                    "SELECT pg_catalog.pg_advisory_xact_lock($1)", _MIGRATION_LOCK_KEY
                )
                table = await self._admit(conn, schema)
                await self._ensure_table(conn, table)
                return await self._read_applied(conn, table)

    async def migrate(self, migrations_dir: str) -> list[str]:
        """Apply NNN_description.sql files, optionally split with -- DOWN."""
        return await self.run_migrations(_load_from_dir(migrations_dir))

    async def rollback(
        self, migrations: list[Migration], target_version: int
    ) -> list[str]:
        """Rollback newest first, committing each step under one session lock.

        Earlier successful steps remain committed if a later step fails.
        Missing DOWN SQL is validated for every selected step before any down.
        Requires a direct or session-pooled endpoint, not a transaction pooler.
        """
        plan = _capture_plan(migrations)
        if type(target_version) is not int or target_version < 0:
            raise ValueError("target version must be a non-negative integer")
        async with self._pool.acquire() as conn:
            schema = await self._provider_schema(conn)
            # Acquisition can be interrupted after the server grants the lock;
            # attempt unlock on every exit, including uncertain acquisition.
            try:
                await conn.execute(
                    "SELECT pg_catalog.pg_advisory_lock($1)", _MIGRATION_LOCK_KEY
                )
                table = await self._admit(conn, schema)
                # Admission is complete before history creation.
                await self._ensure_table(conn, table)
                applied = await self._read_applied(conn, table)
                selected = [
                    m
                    for m in reversed(plan)
                    if m.version > target_version and m.version in applied
                ]
                for m in selected:
                    if not m.down:
                        raise ValueError(
                            f"migration {m.version}_{m.name} has no DOWN section; cannot roll back without a backup restore"
                        )
                results = []
                for m in selected:
                    async with conn.transaction():
                        await conn.execute(m.down)
                        await conn.execute(
                            f"DELETE FROM {table} WHERE version = $1", m.version
                        )
                    results.append(f"Rolled back: {m.version}_{m.name}")
                return results
            finally:
                try:
                    await asyncio.wait_for(
                        conn.fetchval(
                            "SELECT pg_catalog.pg_advisory_unlock($1)",
                            _MIGRATION_LOCK_KEY,
                        ),
                        timeout=5,
                    )
                except BaseException:
                    # Never return a possibly locked session to the pool.
                    conn.terminate()
                    raise

    async def rollback_dir(self, migrations_dir: str, target_version: int) -> list[str]:
        return await self.rollback(_load_from_dir(migrations_dir), target_version)

    async def run_migrations(self, migrations: list[Migration]) -> list[str]:
        """Apply the entire captured plan/history atomically under a PG xact lock."""
        plan = _capture_plan(migrations)
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                schema = await self._provider_schema(conn)
                await conn.execute(
                    "SELECT pg_catalog.pg_advisory_xact_lock($1)", _MIGRATION_LOCK_KEY
                )
                table = await self._admit(conn, schema)
                await self._ensure_table(conn, table)
                applied = await self._read_applied(conn, table)
                results = []
                for m in plan:
                    if m.version in applied:
                        continue
                    await conn.execute(m.up)
                    await conn.execute(
                        f"INSERT INTO {table} (version, name) VALUES ($1, $2)",
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
