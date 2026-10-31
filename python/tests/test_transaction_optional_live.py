"""Live regression for optional-row parity between pooled and transactional SQL."""
from __future__ import annotations

import json
import os
from pathlib import Path
import secrets
import shutil
import stat
from urllib.parse import urlsplit, urlunsplit

import asyncpg
from pydantic import BaseModel, ConfigDict, ValidationError
import pytest

from neutron.nucleus.client import NucleusClient


class Item(BaseModel):
    model_config = ConfigDict(strict=True)
    id: int
    title: str


class Backend(BaseModel):
    model_config = ConfigDict(strict=True)
    pid: int


class DeliberateRollback(Exception):
    pass


@pytest.mark.asyncio
async def test_transaction_optional_query_retains_session_validation_and_rollback():
    filename = os.environ.get("NEUTRON_TRANSACTION_OPTIONAL_ADMIN_FILE")
    if not filename:
        pytest.skip("explicit private administrative fixture required")
    source = Path(filename)
    if source.is_symlink() or not source.is_file() or stat.S_IMODE(source.stat().st_mode)&0o077:
        raise RuntimeError("private regular administrative file required")
    if shutil.disk_usage(source.parent).free<6*1024**3:
        raise RuntimeError("6GiB disk guard")
    url = json.loads(source.read_text())["postgres_admin_url"]
    parts = urlsplit(url)
    if parts.scheme not in ("postgres", "postgresql") or parts.query or parts.fragment:
        raise RuntimeError("plain PostgreSQL administrative URL required")
    owner = await asyncpg.connect(url)
    database = "neutron_tx_optional_"+secrets.token_hex(8)
    created = False
    db = None
    try:
        version = int(await owner.fetchval("SELECT current_setting('server_version_num')"))
        assert 170000<=version<180000, "fixture is PostgreSQL17 only"
        await owner.execute(f'CREATE DATABASE "{database}"')
        created = True
        db = await NucleusClient.connect(urlunsplit((parts.scheme,parts.netloc,"/"+database,"","")), min_size=2,max_size=2)
        await db.sql.execute("CREATE TABLE optional_items(id INTEGER PRIMARY KEY,title TEXT NOT NULL)")
        await db.sql.execute("INSERT INTO optional_items VALUES(1,'existing')")
        query = "SELECT id,title FROM optional_items WHERE id=$1"
        assert await db.sql.query_one_or_none(Item,query,1)==Item(id=1,title="existing")
        assert await db.sql.query_one_or_none(Item,query,999) is None
        async with db.transaction() as tx:
            pid = (await tx.sql.query_one(Backend,"SELECT pg_backend_pid() AS pid")).pid
            assert await tx.sql.query_one_or_none(Item,query,1)==Item(id=1,title="existing")
            assert await tx.sql.query_one_or_none(Item,query,999) is None
            assert (await tx.sql.query_one_or_none(Backend,"SELECT pg_backend_pid() AS pid")).pid==pid
            assert await tx.sql.execute("INSERT INTO optional_items VALUES(2,'committed')")==1
            assert await tx.sql.query_one_or_none(Item,query,2)==Item(id=2,title="committed")
        assert await db.sql.query_one_or_none(Item,query,2)==Item(id=2,title="committed")
        with pytest.raises(DeliberateRollback):
            async with db.transaction() as tx:
                pid = (await tx.sql.query_one(Backend,"SELECT pg_backend_pid() AS pid")).pid
                assert await tx.sql.execute("INSERT INTO optional_items VALUES(3,'rolled back')")==1
                assert await tx.sql.query_one_or_none(Item,query,3)==Item(id=3,title="rolled back")
                assert (await tx.sql.query_one_or_none(Backend,"SELECT pg_backend_pid() AS pid")).pid==pid
                raise DeliberateRollback()
        assert await db.sql.query_one_or_none(Item,query,3) is None
        # A bad projection must be validated, and exception unwinding must roll back
        # earlier SQL from the same physical transaction.
        with pytest.raises(ValidationError):
            async with db.transaction() as tx:
                await tx.sql.execute("INSERT INTO optional_items VALUES(4,'invalid projection')")
                await tx.sql.query_one_or_none(Item,"SELECT 'bad'::text AS id,'title'::text AS title")
        assert await db.sql.query_one_or_none(Item,query,4) is None
        assert [dict(r) for r in await owner.fetch("SELECT datname FROM pg_database WHERE datname=$1",database)]==[{"datname":database}]
        async with db.pool.acquire() as conn:
            assert [dict(r) for r in await conn.fetch("SELECT id,title FROM optional_items ORDER BY id")]==[{"id":1,"title":"existing"},{"id":2,"title":"committed"}]
    finally:
        try:
            if db is not None:
                await db.close()
            if created:
                await owner.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=$1 AND pid<>pg_backend_pid()",database)
                await owner.execute(f'DROP DATABASE "{database}"')
                assert not await owner.fetchval("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=$1)",database)
        finally:
            await owner.close()
