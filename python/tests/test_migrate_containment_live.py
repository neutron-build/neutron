"""Public migration/CLI containment against an owned PostgreSQL schema.

Run with NEUTRON_TEST_DATABASE_URL; absence is an explicit skip.
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
import subprocess
import sys
import uuid

import asyncpg
import pytest

from neutron.nucleus.migrate import Migration, Migrator, _MIGRATION_LOCK_KEY

URL = os.environ.get("NEUTRON_TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(
    not URL, reason="live containment requires NEUTRON_TEST_DATABASE_URL"
)


@pytest.fixture
async def scope():
    schema = "neutron_py_contain_" + uuid.uuid4().hex
    admin = await asyncpg.connect(URL, statement_cache_size=0)
    await admin.execute(f'CREATE SCHEMA "{schema}"')
    pool = await asyncpg.create_pool(
        URL, min_size=1, max_size=1, server_settings={"search_path": schema}
    )
    try:
        yield admin, pool, schema
    finally:
        await pool.close()
        await admin.execute(f'DROP SCHEMA "{schema}" CASCADE')
        await admin.close()


async def tables(admin, schema):
    return (
        await admin.fetchval(
            "SELECT array_agg(table_name ORDER BY table_name) FROM information_schema.tables WHERE table_schema=$1",
            schema,
        )
        or []
    )


async def wait_for(admin, predicate):
    async with asyncio.timeout(5):
        while not await predicate():
            await asyncio.sleep(
                0.01
            )  # polling observed database state, not a race oracle


@pytest.mark.parametrize(
    "shape",
    ["text", "v2", "v2_populated", "bigint", "domain", "view", "extra", "no_pk"],
)
@pytest.mark.parametrize("operation", ["up", "down", "status"])
async def test_known_foreign_shapes_refused_without_mutation(scope, shape, operation):
    admin, pool, schema = scope
    relation = f'"{schema}"._neutron_migrations'
    if shape == "view":
        await admin.execute(
            f"CREATE VIEW {relation} AS SELECT 1::integer version, 'foreign'::text name, '2026-01-01 00:00:00+00'::timestamptz applied_at"
        )
    else:
        version_type = {"text": "TEXT", "bigint": "BIGINT"}.get(shape, "INTEGER")
        if shape == "domain":
            await admin.execute(f'CREATE DOMAIN "{schema}".version_type AS INTEGER')
            version_type = f'"{schema}".version_type'
        pk = "" if shape == "no_pk" else "PRIMARY KEY"
        extra = (
            ", checksum TEXT, owner TEXT, format TEXT"
            if shape in {"v2", "v2_populated"}
            else ", extra TEXT"
            if shape == "extra"
            else ""
        )
        await admin.execute(
            f"CREATE TABLE {relation}(version {version_type} {pk}, name TEXT NOT NULL, applied_at TIMESTAMPTZ DEFAULT NOW(){extra})"
        )
    if shape == "text":
        await admin.execute(
            f"INSERT INTO {relation}(version,name) VALUES('1','foreign sentinel')"
        )
    if shape == "v2_populated":
        await admin.execute(
            f"INSERT INTO {relation}(version,name,checksum,owner,format) VALUES(1,'foreign sentinel','untouched','sdk','v2')"
        )
    before = await tables(admin, schema)
    rows_before = await admin.fetch(f"SELECT * FROM {relation}")
    migrator = Migrator(pool)
    with pytest.raises(ValueError):
        if operation == "up":
            await migrator.run_migrations(
                [Migration(1, "foreign", "CREATE TABLE unwanted(id INT)")]
            )
        elif operation == "down":
            await migrator.rollback(
                [Migration(1, "foreign", "", "CREATE TABLE unwanted(id INT)")], 0
            )
        else:
            await migrator.get_applied()
    assert await tables(admin, schema) == before
    assert await admin.fetch(f"SELECT * FROM {relation}") == rows_before


async def test_search_path_shadow_does_not_bootstrap_local_ledger(scope):
    admin, pool, schema = scope
    foreign = schema + "_foreign"
    await admin.execute(f'CREATE SCHEMA "{foreign}"')
    try:
        await admin.execute(
            f'CREATE TABLE "{foreign}"._neutron_migrations(version TEXT PRIMARY KEY,name TEXT,applied_at TIMESTAMPTZ)'
        )
        other = await asyncpg.create_pool(
            URL,
            min_size=1,
            max_size=1,
            server_settings={"search_path": f"{schema},{foreign}"},
        )
        try:
            with pytest.raises(ValueError, match="shadowed"):
                await Migrator(other).run_migrations(
                    [Migration(1, "a", "CREATE TABLE unwanted(id INT)")]
                )
        finally:
            await other.close()
        assert await tables(admin, schema) == []
        assert (
            await admin.fetchval(
                f'SELECT count(*) FROM "{foreign}"._neutron_migrations'
            )
            == 0
        )
    finally:
        await admin.execute(f'DROP SCHEMA "{foreign}" CASCADE')


async def test_temp_shadow_refused(scope):
    admin, pool, schema = scope
    async with pool.acquire() as c:
        await c.execute(
            "CREATE TEMP TABLE _neutron_migrations(version INTEGER PRIMARY KEY,name TEXT NOT NULL,applied_at TIMESTAMPTZ)"
        )
    with pytest.raises(ValueError, match="shadowed"):
        await Migrator(pool).get_applied()
    assert await tables(admin, schema) == []


async def test_own_legacy_status_up_down_and_prevalidation(scope):
    admin, pool, schema = scope
    m = Migrator(pool)
    assert await m.get_applied() == set()
    plan = [
        Migration(i, str(i), f"CREATE TABLE t{i}(id INT)", f"DROP TABLE t{i}")
        for i in [1, 2, 3]
    ]
    assert len(await m.run_migrations(plan)) == 3
    assert await m.get_applied() == {1, 2, 3}
    plan[1].down = ""
    with pytest.raises(ValueError, match="no DOWN"):
        await m.rollback(plan, 0)
    assert await tables(admin, schema) == ["_neutron_migrations", "t1", "t2", "t3"]
    plan[1].down = "SELECT missing_down_function()"
    with pytest.raises(asyncpg.UndefinedFunctionError):
        await m.rollback(plan, 0)
    assert await tables(admin, schema) == ["_neutron_migrations", "t1", "t2"]
    assert await m.get_applied() == {1, 2}
    plan[1].down = "DROP TABLE t2"
    assert len(await m.rollback(plan, 0)) == 2
    assert await m.get_applied() == set()


async def test_plan_capture_and_duplicate_refusal(scope):
    admin, pool, schema = scope
    with pytest.raises(ValueError, match="duplicate"):
        await Migrator(pool).run_migrations(
            [Migration(1, "a", "CREATE TABLE unwanted(id INT)"), Migration(1, "b", "")]
        )
    assert await tables(admin, schema) == []
    async with pool.acquire() as c:
        backend_pid = await c.fetchval("SELECT pg_backend_pid()")
    await admin.execute("SELECT pg_advisory_lock($1)", _MIGRATION_LOCK_KEY)
    plan = [Migration(1, "captured", "CREATE TABLE captured(id INT)")]
    task = asyncio.create_task(Migrator(pool).run_migrations(plan))
    try:

        async def waiting():
            return await admin.fetchval(
                "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND NOT granted AND pid=$1)",
                backend_pid,
            )

        await wait_for(admin, waiting)
        plan[0].up = "CREATE TABLE mutated(id INT)"
        plan.append(Migration(2, "added", "CREATE TABLE added(id INT)"))
    finally:
        await admin.execute("SELECT pg_advisory_unlock($1)", _MIGRATION_LOCK_KEY)
    assert await task == ["Applied: 1_captured"]
    assert await tables(admin, schema) == ["_neutron_migrations", "captured"]


async def test_rollback_lock_excludes_up_and_cancel_releases_session(scope):
    admin, pool, schema = scope
    barrier = 73318742
    plan = [
        Migration(
            1,
            "a",
            "CREATE TABLE t1(id INT)",
            f"SELECT pg_advisory_xact_lock({barrier}); DROP TABLE t1",
        )
    ]
    await Migrator(pool).run_migrations(plan)
    await admin.execute("SELECT pg_advisory_lock($1)", barrier)
    other = await asyncpg.create_pool(
        URL, min_size=1, max_size=1, server_settings={"search_path": schema}
    )
    async with pool.acquire() as c:
        down_pid = await c.fetchval("SELECT pg_backend_pid()")
    async with other.acquire() as c:
        up_pid = await c.fetchval("SELECT pg_backend_pid()")
    rollback = asyncio.create_task(Migrator(pool).rollback(plan, 0))
    up = None
    try:

        async def down_waiting():
            return await admin.fetchval(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event='advisory')",
                down_pid,
            )

        await wait_for(admin, down_waiting)
        up = asyncio.create_task(
            Migrator(other).run_migrations(
                [Migration(2, "b", "CREATE TABLE t2(id INT)")]
            )
        )

        async def both_waiting():
            return await admin.fetchval(
                "SELECT count(*)=2 FROM pg_locks WHERE locktype='advisory' AND NOT granted AND pid=ANY($1::int[])",
                [down_pid, up_pid],
            )

        await wait_for(admin, both_waiting)
        assert await tables(admin, schema) == ["_neutron_migrations", "t1"]
        rollback.cancel()
        with pytest.raises(asyncio.CancelledError):
            await rollback
        assert await up == ["Applied: 2_b"]
        assert await Migrator(pool).get_applied() == {1, 2}
        assert await tables(admin, schema) == ["_neutron_migrations", "t1", "t2"]
    finally:
        await admin.execute("SELECT pg_advisory_unlock($1)", barrier)
        for task in [rollback, up]:
            if task is not None and not task.done():
                task.cancel()
        await other.close()


async def test_canceled_lock_wait_has_no_mutation(scope):
    admin, pool, schema = scope
    async with pool.acquire() as c:
        backend_pid = await c.fetchval("SELECT pg_backend_pid()")
    await admin.execute("SELECT pg_advisory_lock($1)", _MIGRATION_LOCK_KEY)
    try:
        task = asyncio.create_task(
            Migrator(pool).rollback([Migration(1, "a", "", "DROP TABLE t1")], 0)
        )

        async def waiting():
            return await admin.fetchval(
                "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND NOT granted AND pid=$1)",
                backend_pid,
            )

        await wait_for(admin, waiting)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert await tables(admin, schema) == []
    finally:
        await admin.execute("SELECT pg_advisory_unlock($1)", _MIGRATION_LOCK_KEY)
    assert await Migrator(pool).get_applied() == set()


@pytest.mark.parametrize("fails", [False, True])
async def test_real_cli_lifespan_opens_and_closes_pool_on_own_loop(
    scope, tmp_path, fails
):
    admin, pool, schema = scope
    (tmp_path / "migrations").mkdir()
    (tmp_path / "migrations/001_create.sql").write_text(
        "SELECT missing_cli_function()" if fails else "CREATE TABLE cli_target(id INT)"
    )
    (tmp_path / "cli_app.py").write_text("""from contextlib import asynccontextmanager
from pathlib import Path
import os,asyncio,asyncpg
from neutron import App
from neutron.nucleus.client import NucleusClient, Features
@asynccontextmanager
async def lifespan(app):
    pool=await asyncpg.create_pool(os.environ['NEUTRON_TEST_DATABASE_URL'],min_size=1,max_size=1,server_settings={'search_path':os.environ['TEST_SCHEMA']})
    app.db=NucleusClient(pool,Features())
    Path('opened').write_text(str(id(asyncio.get_running_loop())))
    try: yield
    finally:
        await app.db.close()
        Path('closed').write_text(str(pool.is_closing()))
app=App(lifespan=lifespan)
""")
    env = dict(
        os.environ, TEST_SCHEMA=schema, PYTHONPATH=str(Path(__file__).parents[1])
    )
    # Real command parser/process; pool is created only by the user lifespan.
    result = await asyncio.to_thread(
        subprocess.run,
        [
            sys.executable,
            "-m",
            "neutron",
            "migrate",
            "cli_app:app",
            "--directory",
            "migrations",
        ],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert (tmp_path / "opened").exists()
    assert (tmp_path / "closed").read_text() == "True"
    if fails:
        assert result.returncode != 0
        assert "missing_cli_function" in result.stdout + result.stderr
        assert await tables(admin, schema) == []
    else:
        assert result.returncode == 0, result.stdout + result.stderr
        assert "Applied: 1_create" in result.stdout
        assert await tables(admin, schema) == ["_neutron_migrations", "cli_target"]
        assert (
            await admin.fetchval(f'SELECT count(*) FROM "{schema}"._neutron_migrations')
            == 1
        )


async def test_real_cli_rejects_preconnected_foreign_loop(scope, tmp_path):
    admin, pool, schema = scope
    (tmp_path / "migrations").mkdir()
    (tmp_path / "migrations/001_create.sql").write_text("CREATE TABLE unwanted(id INT)")
    (tmp_path / "foreign_app.py").write_text("""import asyncio,asyncpg,os
from neutron import App
from neutron.nucleus.client import NucleusClient,Features
async def connect():
    pool=await asyncpg.create_pool(os.environ['NEUTRON_TEST_DATABASE_URL'],min_size=1,max_size=1,server_settings={'search_path':os.environ['TEST_SCHEMA']})
    return NucleusClient(pool,Features())
app=App()
app.db=asyncio.run(connect())
""")
    env = dict(
        os.environ, TEST_SCHEMA=schema, PYTHONPATH=str(Path(__file__).parents[1])
    )
    result = await asyncio.to_thread(
        subprocess.run,
        [
            sys.executable,
            "-m",
            "neutron",
            "migrate",
            "foreign_app:app",
            "--directory",
            "migrations",
        ],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert result.returncode != 0
    assert "belongs to another event loop" in result.stdout + result.stderr
    assert await tables(admin, schema) == []


async def test_lost_rollback_session_is_discarded_and_pool_reconnects(scope):
    admin, pool, schema = scope
    barrier = 73318743
    plan = [
        Migration(
            1,
            "a",
            "CREATE TABLE t1(id INT)",
            f"SELECT pg_advisory_xact_lock({barrier}); DROP TABLE t1",
        )
    ]
    await Migrator(pool).run_migrations(plan)
    async with pool.acquire() as c:
        backend_pid = await c.fetchval("SELECT pg_backend_pid()")
    await admin.execute("SELECT pg_advisory_lock($1)", barrier)
    task = asyncio.create_task(Migrator(pool).rollback(plan, 0))
    try:

        async def blocked():
            return await admin.fetchval(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event='advisory')",
                backend_pid,
            )

        await wait_for(admin, blocked)
        assert await admin.fetchval("SELECT pg_terminate_backend($1)", backend_pid)
        with pytest.raises((asyncpg.PostgresError, asyncpg.InterfaceError)):
            await task
        assert await Migrator(pool).get_applied() == {1}
        assert await tables(admin, schema) == ["_neutron_migrations", "t1"]
        async with pool.acquire() as c:
            assert await c.fetchval("SELECT pg_backend_pid()") != backend_pid
    finally:
        await admin.execute("SELECT pg_advisory_unlock($1)", barrier)
        if not task.done():
            task.cancel()
