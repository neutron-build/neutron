#!/usr/bin/env python3
"""Finite candidate comparison: isolated DB, native oracle, raw timing evidence."""
from __future__ import annotations

import argparse
import asyncio
from collections.abc import Mapping
from decimal import Decimal
import hashlib
from importlib import metadata
import json
import math
import os
from pathlib import Path
import platform
import re
import secrets
import shutil
import stat
import subprocess
import sys
import time
import traceback
from urllib.parse import urlsplit, urlunsplit


GUARD = 6 * 1024**3
SOURCE = Path(__file__).resolve().parent
OWNED_OUTPUT = None
DECIMAL = "1234567890123456789012.123456789012345679"
DDL = """
CREATE TABLE projects(tenant TEXT NOT NULL,id INTEGER NOT NULL,title TEXT NOT NULL,PRIMARY KEY(tenant,id));
CREATE TABLE documents(tenant TEXT NOT NULL,id INTEGER NOT NULL,project_id INTEGER NOT NULL,version BIGINT NOT NULL,amount NUMERIC(40,18) NOT NULL,note TEXT,payload BYTEA NOT NULL,PRIMARY KEY(tenant,id),FOREIGN KEY(tenant,project_id) REFERENCES projects(tenant,id));
CREATE INDEX documents_relation ON documents(tenant,project_id,id);
"""
ORACLE = "SELECT tenant,id,project_id,version::text AS version,amount::text AS amount,note,encode(payload,'hex') AS payload FROM documents"


def disk_guard(path):
    free = shutil.disk_usage(path).free
    if free < GUARD:
        raise RuntimeError("6 GiB disk guard: comparison refused")
    return free


def write_json(path, value):
    with path.open("x", encoding="utf-8") as stream:
        os.chmod(path, 0o600)
        json.dump(value, stream, indent=2)
        stream.write("\n")


def source_identity():
    repo = subprocess.check_output(["git", "rev-parse", "--show-toplevel"], cwd=SOURCE, text=True).strip()
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=SOURCE, text=True).strip()
    files = sorted(SOURCE.glob("*.py")) + sorted(SOURCE.glob("*.txt")) + sorted(SOURCE.glob("*.md"))
    hashes = {str(p.relative_to(Path(repo))): hashlib.sha256(p.read_bytes()).hexdigest() for p in files}
    # SDK import location and hashes distinguish a source candidate from a wheel.
    import neutron
    import neutron.nucleus.client
    import neutron.nucleus.sql
    import neutron.nucleus.tx
    sdk = [Path(m.__file__).resolve() for m in (neutron.nucleus.client, neutron.nucleus.sql, neutron.nucleus.tx)]
    if any(not p.is_relative_to(SOURCE.parents[1] / "neutron") for p in sdk):
        raise RuntimeError("candidate SDK must import from this worktree; set PYTHONPATH explicitly")
    return {"revision": revision, "files": hashes, "sdk_kind": "candidate-source", "sdk_import": neutron.__file__, "sdk_files": {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in sdk}}


def normalize(row, project=False):
    if row is None:
        return None
    fields = ("tenant", "id", "title") if project else ("tenant", "id", "project_id", "version", "amount", "note", "payload")
    result = {k: row[k] if isinstance(row, Mapping) or hasattr(row, "keys") else getattr(row, k) for k in fields}
    if not project:
        if type(result["version"]) is not int or not isinstance(result["amount"], Decimal) or type(result["payload"]) is not bytes:
            raise AssertionError("Lossless native scalar representation required")
        result["version"] = str(result["version"])
        result["amount"] = format(result["amount"], ".18f")
        result["payload"] = result["payload"].hex()
    return result


def normalized_relation(value):
    return None if value is None else {**normalize(value[0], True), "documents": [normalize(d) for d in value[1]]}


def same(actual, expected):
    if actual != expected:
        raise AssertionError("Provider result differs from independent native PostgreSQL oracle")


async def oracle_rows(conn, clause="", *args):
    return [dict(row) for row in await conn.fetch(ORACLE + " " + clause, *args)]


async def snapshot(conn):
    return {"projects": [dict(r) for r in await conn.fetch("SELECT tenant,id,title FROM projects ORDER BY tenant,id")], "documents": await oracle_rows(conn, "ORDER BY tenant,id")}


async def seed_small(conn):
    await conn.execute("TRUNCATE documents,projects")
    await conn.execute("INSERT INTO projects VALUES('a',1,'Alpha'),('b',1,'Other'),('a',2,'Empty')")
    rows = [("a",1,1,9223372036854775807,Decimal(DECIMAL),None,bytes(range(256))), ("a",2,1,-9223372036854775808,Decimal(DECIMAL),"",b""), ("b",1,1,9007199254740993,Decimal(DECIMAL),"Other",b"\x00\xff")]
    await conn.executemany("INSERT INTO documents VALUES($1,$2,$3,$4,$5,$6,$7)", rows)
    actual = await oracle_rows(conn, "ORDER BY tenant,id")
    same([r["version"] for r in actual], ["9223372036854775807", "-9223372036854775808", "9007199254740993"])
    same([r["amount"] for r in actual], [DECIMAL] * 3)
    same([r["payload"] for r in actual], [bytes(range(256)).hex(), "", "00ff"])


async def correctness(provider, oracle):
    await seed_small(oracle)
    initial = await snapshot(oracle)
    for tenant, document_id in (("a",1),("a",2),("b",1),("a",9999)):
        expected = await oracle_rows(oracle, "WHERE tenant=$1 AND id=$2", tenant, document_id)
        same(normalize(await provider.point(tenant,document_id)), expected[0] if expected else None)
    same([normalize(r) for r in await provider.page("a",1)], await oracle_rows(oracle, "WHERE tenant='a' AND id>1 ORDER BY id LIMIT 20"))
    relation = {"tenant":"a","id":1,"title":"Alpha","documents":await oracle_rows(oracle, "WHERE tenant='a' AND project_id=1 ORDER BY id LIMIT 20")}
    same(normalized_relation(await provider.relation("a",1)), relation)
    same(normalized_relation(await provider.relation("a",2)), {"tenant":"a","id":2,"title":"Empty","documents":[]})
    same(normalized_relation(await provider.relation("a",9999)), None)
    capabilities = {"common_relation": "explicit two-query bounded relation"}
    if hasattr(provider, "native_relation") and provider.orm:
        same(normalized_relation(await provider.native_relation("a",1)), relation)
        same(normalized_relation(await provider.native_relation("a",2)), {"tenant":"a","id":2,"title":"Empty","documents":[]})
        capabilities["native_selectinload_relation"] = "PASS: composite tenant FK, exact values, empty relation"
    # Separate mutable row prevents overflow at the int64 boundary fixture.
    row = dict(tenant="a",id=90000,project_id=1,version=0,amount=Decimal(DECIMAL),note=None,payload=bytes(range(256)))
    same(await provider.create(row), 1)
    expected = (await oracle_rows(oracle, "WHERE tenant='a' AND id=90000"))[0]
    same(normalize(await provider.point("a",90000)), expected)
    # Two simultaneous public API calls, separate pool/session leases. Exactly one wins.
    ready = asyncio.Event()
    async def contend(note):
        await ready.wait()
        return await provider.cas("a",90000,0,note)
    contenders = [asyncio.create_task(contend(note)) for note in ("winner-0","winner-1")]
    ready.set()
    winners = await asyncio.gather(*contenders)
    same(sorted(winners), [0,1])
    winner = winners.index(1)
    actual = (await oracle_rows(oracle, "WHERE tenant='a' AND id=90000"))[0]
    same(actual["version"], "1")
    same(actual["note"], f"winner-{winner}")
    same(await provider.cas("a",90000,0,"stale"), 0)
    same((await oracle_rows(oracle, "WHERE tenant='a' AND id=90000"))[0], actual)
    same(await provider.remove("a",90000), 1)
    same(await provider.remove("a",90000), 0)
    from providers import RollbackProbe
    try:
        await provider.rolled_back_create(row)
    except RollbackProbe:
        pass
    else:
        raise AssertionError("rollback control did not throw")
    same(await oracle_rows(oracle, "WHERE tenant='a' AND id=90000"), [])
    # Native SQLSTATE must establish FK refusal, rather than accepting any exception.
    try:
        await provider.create({**row, "project_id":9999})
    except Exception as error:
        states = set()
        cursor = error
        seen = set()
        while cursor is not None and id(cursor) not in seen:
            seen.add(id(cursor))
            states.add(getattr(cursor,"sqlstate",None))
            cursor = getattr(cursor,"orig",None) or cursor.__cause__ or cursor.__context__
        if "23503" not in states:
            raise AssertionError("FK probe did not produce PostgreSQL SQLSTATE23503") from error
    else:
        raise AssertionError("missing FK refusal")
    same(await snapshot(oracle), initial)
    return {"provider": provider.name, "status":"PASS", "checks":["exact native values", "hit/miss", "tenant duplicate IDs", "keyset", "relation/empty/missing", "create/delete", "concurrent CAS one winner", "stale CAS no effect", "rollback", "FK23503 no effect", "unchanged independent full snapshot"], "capabilities":capabilities}


async def seed_measurement(conn):
    await conn.execute("TRUNCATE documents,projects")
    await conn.execute("INSERT INTO projects SELECT t, p, t||'-'||p FROM unnest(ARRAY['a','b']) t CROSS JOIN generate_series(1,100) p")
    await conn.execute("""INSERT INTO documents
    SELECT t, (p-1)*20+d,p,
      CASE WHEN d=1 THEN '9223372036854775807'::bigint WHEN d=2 THEN '-9223372036854775808'::bigint ELSE '9007199254740993'::bigint END,
      $1::numeric(40,18),CASE WHEN d%2=0 THEN '' ELSE NULL END,$2::bytea
    FROM unnest(ARRAY['a','b']) t CROSS JOIN generate_series(1,100) p CROSS JOIN generate_series(1,20) d""", Decimal(DECIMAL), bytes(range(256)))
    await conn.execute("ANALYZE projects; ANALYZE documents")


def percentile(values, p):
    ordered = sorted(values)
    return ordered[max(0, math.ceil(p*len(ordered))-1)]


async def measure(providers, oracle, output, trials, samples, warmups):
    await seed_measurement(oracle)
    initial = await snapshot(oracle)
    expected = {
        "point": (await oracle_rows(oracle, "WHERE tenant='a' AND id=17"))[0],
        "page": await oracle_rows(oracle, "WHERE tenant='a' AND id>20 ORDER BY id LIMIT 20"),
        "relationship_children": await oracle_rows(oracle, "WHERE tenant='a' AND project_id=3 ORDER BY id LIMIT 20"),
    }
    summary = []
    raw = output / "samples.jsonl"
    with raw.open("x", encoding="utf-8") as stream:
        os.chmod(raw,0o600)
        for trial in range(trials):
            # Complete four-trial balanced cyclic order for four providers.
            order = providers[trial%4:] + providers[:trial%4]
            for workload in expected:
                for concurrency in (1,4):
                    for provider in order:
                        method = {"point":provider.point,"page":provider.page,"relationship_children":provider.children}[workload]
                        key = {"point":17,"page":20,"relationship_children":3}[workload]
                        def validate(result):
                            same(normalize(result) if workload=="point" else [normalize(r) for r in result], expected[workload])
                        for _ in range(warmups):
                            validate(await method("a",key))
                        latencies = []
                        async def lane(lane_id):
                            # Round-robin allocation produces exactly samples calls total.
                            for index in range(lane_id,samples,concurrency):
                                before = time.perf_counter_ns()
                                result = await method("a",key)
                                elapsed = time.perf_counter_ns()-before
                                # Assert and serialize strictly outside each operation timer.
                                validate(result)
                                latencies.append(elapsed)
                                stream.write(json.dumps({"provider":provider.name,"trial":trial,"workload":workload,"concurrency":concurrency,"sample":index,"latency_ns":elapsed})+"\n")
                        before_phase = time.perf_counter_ns()
                        await asyncio.gather(*(lane(i) for i in range(concurrency)))
                        phase_ns = time.perf_counter_ns()-before_phase
                        summary.append({"provider":provider.name,"trial":trial,"workload":workload,"concurrency":concurrency,"samples":len(latencies),"phase_ns":phase_ns,"operations_per_second":samples*1e9/phase_ns,"p50_ns":percentile(latencies,.5),"p95_ns":percentile(latencies,.95),"p99_ns":percentile(latencies,.99),"min_ns":min(latencies),"max_ns":max(latencies)})
    same(await snapshot(oracle), initial)
    return {"trials":trials,"samples_per_phase":samples,"warmups_per_phase":warmups,"total_timed_operations":sum(p["samples"] for p in summary),"rows":4000,"summary":summary,"oracle_snapshot_sha256":hashlib.sha256(json.dumps(initial,sort_keys=True).encode()).hexdigest(),"validation":"every timed result exact; entire native database snapshot unchanged", "timing_scope":"public operation including pool/session lease, SQLAlchemy implicit transaction close and mapping; excludes result validation and evidence serialization", "throughput_scope":"whole phase includes dispatch, validation and evidence serialization; not pure database throughput"}


async def run(args):
    global OWNED_OUTPUT
    output = args.output.resolve()
    if not output.parent.is_dir():
        raise RuntimeError("output parent must exist")
    disk_guard(output.parent)
    # No mutation of existing output, no diagnostics to an unowned location.
    output.mkdir(mode=0o700)
    OWNED_OUTPUT = output
    if args.admin_file.is_symlink():
        raise RuntimeError("admin file symlink refused")
    credentials = args.admin_file.resolve(strict=True)
    if not credentials.is_file() or stat.S_IMODE(credentials.stat().st_mode)&0o077:
        raise RuntimeError("admin file must be private regular0600")
    admin_url = json.loads(credentials.read_text())[args.admin_key]
    parts = urlsplit(admin_url)
    if parts.scheme not in ("postgres", "postgresql"):
        raise RuntimeError("PostgreSQL administrative URL required")
    if parts.query or parts.fragment:
        raise RuntimeError("administrative URL query/fragment not supported")
    import asyncpg
    from providers import PROVIDERS
    for name, expected in (("SQLAlchemy","2.0.48"),("greenlet","3.3.2")):
        if metadata.version(name)!=expected:
            raise RuntimeError(f"dependency pin mismatch: {name}")
    source = source_identity()
    write_json(output/"source.json",source)
    owner = await asyncpg.connect(admin_url, timeout=10)
    created = False
    database = "neutron_compare_py_"+secrets.token_hex(8)
    oracle = None
    active = []
    failure = None
    try:
        server = await owner.fetchrow("SELECT version() AS version,current_setting('server_version_num') AS number,current_user AS owner")
        if not 170000<=int(server["number"])<180000:
            raise RuntimeError("This fixture certifies PostgreSQL17 only")
        if not re.fullmatch(r"neutron_compare_py_[a-f0-9]{16}",database):
            raise RuntimeError("invalid owned database identity")
        await owner.execute(f'CREATE DATABASE "{database}"')
        created = True
        url = urlunsplit((parts.scheme,parts.netloc,"/"+database,"",""))
        oracle = await asyncpg.connect(url, timeout=10)
        await oracle.execute(DDL)
        results = []
        for factory in PROVIDERS:
            instance = await factory.open(url)
            active.append(instance)
            results.append(await correctness(instance,oracle))
            await instance.close()
            active.remove(instance)
        write_json(output/"correctness.json",results)
        # SQL audit uses separate provider instances, loggers and fixtures.
        await seed_small(oracle)
        audits = []
        for factory in PROVIDERS:
            statements = []
            instance = await factory.open(url,statements)
            active.append(instance)
            # Startup excluded from per-call deltas (SQLAlchemy dialect discovery etc).
            for name, call in (("point",lambda:instance.point("a",1)),("page",lambda:instance.page("a",0)),("relation",lambda:instance.relation("a",1))):
                await asyncio.sleep(0)
                start = len(statements)
                await call()
                await asyncio.sleep(0)
                observed = statements[start:]
                sql = [s for s in observed if s.lstrip().upper().startswith("SELECT") and ("documents" in s or "projects" in s)]
                same(len(sql),2 if name=="relation" else 1)
                audits.append({"provider":instance.name,"operation":name,"application_selects":len(sql),"statements":observed})
            await instance.close()
            active.remove(instance)
        write_json(output/"sql-audit.json", {"scope":"client execution callbacks, not wire messages or network roundtrips; asyncpg reset commands may appear; SQLAlchemy transaction protocol not fully represented", "operations":audits})
        for factory in PROVIDERS:
            instance = await factory.open(url)
            active.append(instance)
        measurement = await measure(active,oracle,output,args.trials,args.samples,args.warmups)
        write_json(output/"measurement.json",measurement)
        versions = {name:metadata.version(name) for name in ("SQLAlchemy","greenlet","asyncpg","pydantic","typing_extensions","neutron-framework")}
        write_json(output/"environment.json", {"python":sys.version,"executable":sys.executable,"platform":platform.platform(),"machine":platform.machine(),"packages":versions,"database":database,"server":dict(server),"settings":dict(await oracle.fetchrow("SELECT current_setting('shared_buffers') AS shared_buffers,current_setting('max_connections') AS max_connections,current_setting('jit') AS jit")),"pool_max_each":4,"scope":"candidate source, local warm reads; SQLAlchemy ORM and Core separate; no cross-language ranking"})
    except BaseException as error:
        failure = error
    finally:
        cleanup_errors = []
        for instance in active:
            try:
                await instance.close()
            except Exception as error:
                cleanup_errors.append(type(error).__name__)
        if oracle is not None:
            try:
                await oracle.close()
            except Exception as error:
                cleanup_errors.append(type(error).__name__)
        if created:
            try:
                await owner.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=$1 AND pid<>pg_backend_pid()",database)
                await owner.execute(f'DROP DATABASE "{database}"')
                if await owner.fetchval("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=$1)",database):
                    raise RuntimeError("owned database remains")
            except Exception as error:
                cleanup_errors.append(type(error).__name__)
        try:
            await owner.close()
        except Exception as error:
            cleanup_errors.append(type(error).__name__)
        write_json(output/"cleanup.json",{"database":database,"created":created,"removed":created and not cleanup_errors,"errors":cleanup_errors})
        if cleanup_errors and failure is None:
            failure = RuntimeError("Owned database cleanup failed")
    disk_guard(output)
    if failure is not None:
        raise failure
    write_json(output/"result.json",{"status":"PASS","source_revision":source["revision"],"timed_calls":args.trials*args.samples*4*3*2})
    print("PASS: Python correctness, SQL audit, warm-read measurement and owned-database cleanup")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--admin-file",type=Path,required=True)
    parser.add_argument("--admin-key",default="postgres_admin_url")
    parser.add_argument("--output",type=Path,required=True)
    parser.add_argument("--trials",type=int,default=4)
    parser.add_argument("--samples",type=int,default=100)
    parser.add_argument("--warmups",type=int,default=20)
    args = parser.parse_args()
    if args.trials<4 or args.trials%4 or args.samples<100 or args.warmups<1:
        parser.error("trials must be positive multiple of4; samples>=100; warmups>=1")
    try:
        asyncio.run(run(args))
    except BaseException as error:
        # Never print credentials, SQLAlchemy URL/parameters or unowned path diagnostics.
        if OWNED_OUTPUT is not None:
            frames = [{"file":f.filename,"line":f.lineno,"function":f.name} for f in traceback.extract_tb(error.__traceback__)]
            write_json(OWNED_OUTPUT/"failure.json",{"type":type(error).__name__,"message":"Comparison failed; private correctness/audit/cleanup evidence retained. No automatic retry.","frames":frames})
        print(f"FAIL: {type(error).__name__}; see private evidence",file=sys.stderr)
        raise SystemExit(1)


if __name__=="__main__":
    main()
