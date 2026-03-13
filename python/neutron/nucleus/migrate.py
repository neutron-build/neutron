"""SQL file-based schema migrations."""

from __future__ import annotations

import os
from dataclasses import dataclass

import asyncpg


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

    async def _ensure_table(self) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS _neutron_migrations (
                    version INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    applied_at TIMESTAMPTZ DEFAULT NOW()
                )
                """
            )

    async def get_applied(self) -> set[int]:
        await self._ensure_table()
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("SELECT version FROM _neutron_migrations")
            return {row["version"] for row in rows}

    async def migrate(self, migrations_dir: str) -> list[str]:
        """Run pending migrations from a directory.

        Files are named ``NNN_description.sql``.  Optionally include
        a ``-- DOWN`` marker to separate up/down SQL.
        """
        migrations = _load_from_dir(migrations_dir)
        return await self.run_migrations(migrations)

    async def run_migrations(self, migrations: list[Migration]) -> list[str]:
        """Run a list of Migration objects, skipping already-applied ones."""
        await self._ensure_table()
        applied = await self.get_applied()
        results: list[str] = []

        for m in sorted(migrations, key=lambda x: x.version):
            if m.version in applied:
                continue
            async with self._pool.acquire() as conn:
                async with conn.transaction():
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
