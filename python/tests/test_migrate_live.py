"""Concurrent migration coverage against a real PostgreSQL server.

    NEUTRON_TEST_DATABASE_URL=postgresql://... pytest tests/test_migrate_live.py
"""

from __future__ import annotations

import asyncio
import os
import uuid

import asyncpg
import pytest

from neutron.nucleus.migrate import Migration, Migrator

DATABASE_URL = os.environ.get("NEUTRON_TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not DATABASE_URL,
    reason="NEUTRON_TEST_DATABASE_URL is not set; migration locking needs PostgreSQL",
)


async def test_two_clients_serialize_the_same_fresh_migration():
    schema = f"neutron_migrate_{uuid.uuid4().hex}"
    admin = await asyncpg.connect(DATABASE_URL)
    pool_a = None
    pool_b = None
    await admin.execute(f"CREATE SCHEMA {schema}")
    try:
        settings = {"search_path": schema}
        pool_a = await asyncpg.create_pool(
            DATABASE_URL, min_size=1, max_size=1, server_settings=settings
        )
        pool_b = await asyncpg.create_pool(
            DATABASE_URL, min_size=1, max_size=1, server_settings=settings
        )
        migration = Migration(
            1,
            "create_concurrent_target",
            """
            SELECT pg_sleep(0.25);
            CREATE TABLE concurrent_target (id INTEGER PRIMARY KEY)
            """,
        )

        results = await asyncio.gather(
            Migrator(pool_a).run_migrations([migration]),
            Migrator(pool_b).run_migrations([migration]),
        )

        assert {tuple(result) for result in results} == {
            (),
            ("Applied: 1_create_concurrent_target",),
        }
        async with pool_a.acquire() as conn:
            assert await conn.fetchval(
                "SELECT COUNT(*) FROM _neutron_migrations WHERE version = 1"
            ) == 1
            assert await conn.fetchval(
                "SELECT to_regclass('concurrent_target') IS NOT NULL"
            ) is True
    finally:
        if pool_a is not None:
            await pool_a.close()
        if pool_b is not None:
            await pool_b.close()
        await admin.execute(f"DROP SCHEMA {schema} CASCADE")
        await admin.close()
