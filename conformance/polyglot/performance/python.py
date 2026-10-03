"""Fresh installed Python consumer. Data dispatch counts come from native cursors.

BEGIN/COMMIT are psycopg transaction controls, reported separately as the frozen
successful-context count; they do not pass through Cursor.execute.
"""
import argparse
import asyncio
import importlib.metadata
import json
import os
from pathlib import Path
import re
import resource
import sys
import time

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--execution', choices=('sync','async'), required=True)
    parser.add_argument('--prefix', type=Path, required=True)
    args = parser.parse_args()
    request = json.load(sys.stdin)
    if request['protocol'] != 'polyglot-performance-v1' or not re.fullmatch('neutron_polyglot_[0-9a-f]{32}', request['schema_scope']): raise ValueError('invalid scope')
    if request['mode'] not in ('orm','raw') or request['workload'] not in ('read','transaction') or (request['warmup'],request['iterations']) != (1024,2048): raise ValueError('invalid frozen workload')
    import neutron.orm as orm
    import psycopg
    from psycopg.rows import dict_row
    prefix = args.prefix.resolve()
    if Path(sys.prefix).resolve() != prefix or any(not Path(m.__file__).resolve().is_relative_to(prefix) for m in (orm,psycopg)): raise ValueError('fresh installed consumer required')
    count = 0
    class Cursor(psycopg.Cursor):
        def execute(self, *a, **kw):
            nonlocal count
            count += 1
            return super().execute(*a, **kw)
    class AsyncCursor(psycopg.AsyncCursor):
        async def execute(self, *a, **kw):
            nonlocal count
            count += 1
            return await super().execute(*a, **kw)
    table = orm.Table('perf_fixture', {'id':orm.ColumnSpec(int,'int4'), 'big':orm.ColumnSpec(int,'int8'), 'body':orm.ColumnSpec(str,'text')},schema=request['schema_scope'])
    idcol = table.column('id',int)
    select_sql = f'SELECT id,big,body FROM "{request["schema_scope"]}".perf_fixture WHERE id=%s'
    update_sql = f'UPDATE "{request["schema_scope"]}".perf_fixture SET body=%s WHERE id=%s'
    def check(row,key):
        if row is None or type(row['big']) is not int or row['id'] != key or row['big'] != 9007199254740993+key or not isinstance(row['body'],str): raise ValueError('point oracle mismatch')
        return row['big']
    url = os.environ['NEUTRON_TEST_DATABASE_URL']
    async def async_run():
        nonlocal count
        conn = await psycopg.AsyncConnection.connect(url,autocommit=True,row_factory=dict_row,cursor_factory=AsyncCursor,prepare_threshold=None)
        await conn.set_isolation_level(psycopg.IsolationLevel.READ_COMMITTED)
        db = orm.AsyncDatabase(conn)
        async def step(i,phase):
            key,marker = i%64+1,phase+':'+str(i)
            async def body():
                if request['mode']=='orm':
                    row = await db.one(orm.select_row(table).where(idcol.eq(key)))
                    if request['workload']=='transaction' and await db.execute(orm.update(table,{'body':marker},where=idcol.eq(key))) != 1: raise ValueError('update cardinality')
                else:
                    async with conn.cursor() as cur:
                        await cur.execute(select_sql,(key,));row=await cur.fetchone()
                        if request['workload']=='transaction':
                            await cur.execute(update_sql,(marker,key))
                            if cur.rowcount != 1: raise ValueError('update cardinality')
                return check(row,key)
            if request['workload']=='transaction':
                async with (db.transaction() if request['mode']=='orm' else conn.transaction()): return await body()
            return await body()
        try:
            for i in range(request['warmup']): await step(i,'warmup')
            count=0;before=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss;start=time.perf_counter_ns();checksum=0
            for i in range(request['iterations']): checksum += await step(i,'measure')
            return checksum,time.perf_counter_ns()-start,before
        finally: await db.close()
    def sync_run():
        nonlocal count
        conn = psycopg.Connection.connect(url,autocommit=True,row_factory=dict_row,cursor_factory=Cursor,prepare_threshold=None)
        conn.isolation_level=psycopg.IsolationLevel.READ_COMMITTED
        db=orm.Database(conn)
        def step(i,phase):
            key,marker=i%64+1,phase+':'+str(i)
            def body():
                if request['mode']=='orm':
                    row=db.one(orm.select_row(table).where(idcol.eq(key)))
                    if request['workload']=='transaction' and db.execute(orm.update(table,{'body':marker},where=idcol.eq(key))) != 1: raise ValueError('update cardinality')
                else:
                    with conn.cursor() as cur:
                        cur.execute(select_sql,(key,));row=cur.fetchone()
                        if request['workload']=='transaction':
                            cur.execute(update_sql,(marker,key))
                            if cur.rowcount != 1: raise ValueError('update cardinality')
                return check(row,key)
            if request['workload']=='transaction':
                with (db.transaction() if request['mode']=='orm' else conn.transaction()): return body()
            return body()
        try:
            for i in range(request['warmup']): step(i,'warmup')
            count=0;before=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss;start=time.perf_counter_ns();checksum=0
            for i in range(request['iterations']): checksum+=step(i,'measure')
            return checksum,time.perf_counter_ns()-start,before
        finally: db.close()
    checksum,elapsed,before=asyncio.run(async_run()) if args.execution=='async' else sync_run()
    expected=request['iterations']*(2 if request['workload']=='transaction' else 1)
    if count != expected: raise ValueError('unexpected native cursor dispatch count')
    controls=2*request['iterations'] if request['workload']=='transaction' else 0
    print(json.dumps({**request,'checksum':str(checksum),'elapsed_ns':elapsed,'query_count':count+controls,
        'query_count_scope':'native cursor dispatches + successful BEGIN/COMMIT contexts',
        'native_data_dispatches':count,'transaction_controls':controls,
        'memory':{'unit':'KiB on Linux; bytes on macOS','peak_rss_before':before,'peak_rss_after':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss},
        'runtime':sys.version,'orm_version':importlib.metadata.version('neutron-framework'),'driver_version':psycopg.__version__}))

if __name__=='__main__':
    try: main()
    except BaseException:
        print(json.dumps({'status':'fail','diagnostics':'installed Python performance consumer failed'}))
        raise SystemExit(1)
