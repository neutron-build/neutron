"""Native driver investigation; this does not certify Neutron ORM support."""
from __future__ import annotations
import argparse
import asyncio
import datetime
import json
import os
from pathlib import Path
import secrets
import sys
import threading
import uuid
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from oracle import execute as oracle
from protocol import PROTOCOL, redact
from runner import EXPECTED


def normalize(row):
    moment = row[3]
    if not isinstance(moment, datetime.datetime) or moment.tzinfo is None:
        raise ValueError('driver temporal identity lost')
    return [str(row[0]), str(row[1]), str(row[2]), moment.astimezone(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f+00'), row[4] is None, row[5]]

def sync_psycopg(url, scope):
    import psycopg
    with psycopg.connect(url, autocommit=True) as conn:
        conn.execute("SET TIME ZONE 'UTC'")
        rows = conn.execute(f'SELECT id, big, precise, moment, sql_null, document::text FROM "{scope}".values_fixture ORDER BY id').fetchall()
        if [normalize(r) for r in rows] != EXPECTED:
            raise ValueError('psycopg sync value fidelity mismatch')
        timer = threading.Timer(.1, conn.cancel)
        timer.start()
        try:
            try:
                conn.execute('SELECT pg_sleep(10)')
            except psycopg.errors.QueryCanceled:
                pass
            else:
                raise ValueError('sync cancel did not cancel native query')
        finally:
            timer.cancel(); timer.join()
        if conn.execute('SELECT 1').fetchone() != (1,):
            raise ValueError('sync connection unusable after cancellation')

async def async_psycopg(url, scope):
    import psycopg
    async with await psycopg.AsyncConnection.connect(url, autocommit=True) as conn:
        await conn.execute("SET TIME ZONE 'UTC'")
        cur = await conn.execute(f'SELECT id, big, precise, moment, sql_null, document::text FROM "{scope}".values_fixture ORDER BY id')
        rows = await cur.fetchall()
        if [normalize(r) for r in rows] != EXPECTED:
            raise ValueError('psycopg async value fidelity mismatch')
        try:
            await asyncio.wait_for(conn.execute('SELECT pg_sleep(10)'), .1)
        except TimeoutError:
            pass
        else:
            raise ValueError('async cancellation did not time out')
        cur = await conn.execute('SELECT 1')
        if await cur.fetchone() != (1,):
            raise ValueError('async psycopg connection unusable after cancellation')

async def async_asyncpg(url, scope):
    import asyncpg
    conn = await asyncpg.connect(url)
    try:
        await conn.execute("SET TIME ZONE 'UTC'")
        rows = await conn.fetch(f'SELECT id, big, precise, moment, sql_null, document::text FROM "{scope}".values_fixture ORDER BY id')
        if [normalize(r) for r in rows] != EXPECTED:
            raise ValueError('asyncpg value fidelity mismatch')
        try:
            await asyncio.wait_for(conn.execute('SELECT pg_sleep(10)'), .1)
        except TimeoutError:
            pass
        else:
            raise ValueError('asyncpg cancellation did not time out')
        if await conn.fetchval('SELECT 1') != 1:
            raise ValueError('asyncpg connection unusable after cancellation')
    finally:
        await conn.close()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--driver', choices=['psycopg','asyncpg'], required=True)
    parser.add_argument('--mode', choices=['sync','async'], required=True)
    args = parser.parse_args()
    request = {'protocol':PROTOCOL,'case_id':'scalar-extremes','profile':'postgres-direct','schema_scope':'neutron_polyglot_'+uuid.uuid4().hex,'ownership_token':secrets.token_hex(32)}
    url = os.environ.get('NEUTRON_TEST_DATABASE_URL','')
    if not url:
        print(json.dumps({'status':'fail','diagnostics':'NEUTRON_TEST_DATABASE_URL required'})); return 1
    if args.driver == 'asyncpg' and args.mode == 'sync':
        print(json.dumps({'status':'fail','diagnostics':'asyncpg has no native synchronous API'})); return 1
    try:
        oracle({**request,'action':'setup'})
        baseline = oracle({**request,'action':'observe'})
        if baseline['rows'] != EXPECTED:
            raise ValueError('independent native oracle baseline mismatch')
        if args.mode == 'sync':
            sync_psycopg(url,request['schema_scope'])
        elif args.driver == 'psycopg':
            asyncio.run(async_psycopg(url,request['schema_scope']))
        else:
            asyncio.run(async_asyncpg(url,request['schema_scope']))
        if oracle({**request,'action':'observe'})['rows'] != baseline['rows']:
            raise ValueError('cancellation changed persisted fixture state')
        print(json.dumps({'status':'pass','kind':'native-driver-spike','driver':args.driver,'mode':args.mode,'cases':['scalar-read-fidelity','native-query-cancel','same-connection-reuse','persisted-state-after-cancel'],'orm_compliance':'unverified'}))
        return 0
    except Exception as exc:
        print(json.dumps({'status':'fail','diagnostics':redact(str(exc),url)})); return 1
    finally:
        try:
            oracle({**request,'action':'cleanup'})
        except Exception as exc:
            print(json.dumps({'status':'fail','diagnostics':'cleanup failed; manual reconciliation required','schema_scope':request['schema_scope']}),file=sys.stderr)
            # Override successful return: cleanup is mandatory.
            raise SystemExit(1) from exc

if __name__ == '__main__':
    raise SystemExit(main())
