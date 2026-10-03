"""Native cursor lifecycle and native scalar observations, never fake parity."""
import asyncio
import datetime as dt
from decimal import Decimal
import pytest
from neutron.orm import AsyncDatabase,ColumnSpec,Database,JsonDocument,OrmError,Predicate,Table,insert,select,select_row
from .test_orm_live import live_table


def exact_table(live_table):
    url,parent,native=live_table
    table=Table('stream_values',{'id':ColumnSpec(int,'int8'),'amount':ColumnSpec(Decimal,'numeric'),'moment':ColumnSpec(dt.datetime,'timestamptz'),'document':ColumnSpec(JsonDocument,'jsonb',nullable=True)},schema=parent.schema)
    native.execute(f'CREATE TABLE {table.sql} (id bigint PRIMARY KEY,amount numeric NOT NULL,moment timestamptz NOT NULL,document jsonb)')
    amount=Decimal('12345678901234567890.000000001');moment=dt.datetime(2038,1,19,3,14,7,654321,tzinfo=dt.timezone.utc)
    for ident in range(1,8):
        native.execute(f'INSERT INTO {table.sql} VALUES (%s,%s,%s,%s::jsonb)',(ident,amount,moment,None if ident==1 else 'null'))
    return url,table,native,amount,moment


def check_values(rows,amount,moment):
    assert sorted(r['id'] for r in rows)==list(range(1,8))
    assert all(r['amount']==amount and r['moment']==moment and r['moment'].microsecond==654321 for r in rows)
    by_id={r['id']:r for r in rows}
    assert by_id[1]['document'] is None
    assert all(isinstance(by_id[i]['document'],JsonDocument) and by_id[i]['document'].text=='null' for i in range(2,8))


def test_native_stream_exact_values_cursor_and_reuse(live_table):
    url,t,native,amount,moment=exact_table(live_table)
    with Database.connect(url) as db:
        with db.stream(select_row(t),batch_size=2) as rows:
            # Trusted native inspection observes actual server state, bypassing
            # the public lease deliberately; no second ORM operation is admitted.
            assert db._conn.execute('SHOW transaction_read_only').fetchone()=={'transaction_read_only':'on'}
            assert db._conn.execute("SELECT count(*) AS n FROM pg_cursors WHERE name LIKE 'neutron_stream_%'").fetchone()=={'n':1}
            actual=list(rows)
        check_values(actual,amount,moment)
        assert db._conn.execute('SELECT count(*) AS n FROM pg_cursors').fetchone()=={'n':0}
        assert db.one(select(t.column('id',int)).where(t.column('id',int).eq(1)))==1
        with db.stream(select(t.column('id',int)),batch_size=1) as rows: next(rows)
        assert not db.closed
        assert db.one(select(t.column('id',int)).where(t.column('id',int).eq(1)))==1
        with db.transaction():
            db.execute(insert(t,{'id':9,'amount':amount,'moment':moment,'document':None}))
            with db.stream(select(t.column('id',int)),batch_size=1) as rows: next(rows)
        assert native.execute(f'SELECT id FROM {t.sql} WHERE id=9').fetchone()==(9,)


@pytest.mark.asyncio
async def test_native_async_stream_exact_values_cursor_and_reuse(live_table):
    url,t,native,amount,moment=exact_table(live_table)
    async with await AsyncDatabase.connect(url) as db:
        async with db.stream(select_row(t),batch_size=2) as rows:
            inspection=await db._conn.execute('SHOW transaction_read_only')
            assert await inspection.fetchone()=={'transaction_read_only':'on'}
            actual=[r async for r in rows]
        check_values(actual,amount,moment)
        async with db.stream(select(t.column('id',int)),batch_size=1) as rows: await anext(rows)
        assert not db.closed
        assert await db.one(select(t.column('id',int)).where(t.column('id',int).eq(1)))==1
        async with db.transaction():
            await db.execute(insert(t,{'id':9,'amount':amount,'moment':moment,'document':None}))
            async with db.stream(select(t.column('id',int)),batch_size=1) as rows: await anext(rows)
        assert native.execute(f'SELECT id FROM {t.sql} WHERE id=9').fetchone()==(9,)


@pytest.mark.asyncio
async def test_native_async_cancel_fetch_then_reuse(live_table):
    url,t,native=live_table
    native.execute(f'INSERT INTO {t.sql} (id) VALUES (1)')
    entered=asyncio.Event();outcomes=[]
    async def consume():
        async with await AsyncDatabase.connect(url) as db:
            try:
                async with db.stream(select(t.column('id',int)).where(Predicate('pg_sleep(20) IS NULL',())),batch_size=1) as rows:
                    entered.set();await anext(rows)
            except asyncio.CancelledError:
                outcomes.append(not db.closed)
                outcomes.append(await db.one(select(t.column('id',int)))==1)
                raise
    task=asyncio.create_task(consume());await asyncio.wait_for(entered.wait(),5)
    # Cancellation remains valid before or during FETCH: both must release lease.
    await asyncio.sleep(0.05);task.cancel()
    with pytest.raises(asyncio.CancelledError): await asyncio.wait_for(task,10)
    assert outcomes==[True,True]


def test_native_owned_stream_is_read_only_even_for_trusted_sql(live_table):
    url,t,native=live_table
    native.execute(f'INSERT INTO {t.sql} (id) VALUES (1)')
    function=f'"{t.schema}"."stream_write"'
    native.execute(f'CREATE FUNCTION {function}() RETURNS boolean LANGUAGE plpgsql AS $$ BEGIN INSERT INTO {t.sql} (id) VALUES (99); RETURN true; END $$')
    with Database.connect(url) as db:
        with pytest.raises(OrmError) as caught:
            with db.stream(select(t.column('id',int)).where(Predicate(function+'()',())),batch_size=1) as rows: list(rows)
        assert caught.value.sqlstate=='25006'
        assert native.execute(f'SELECT count(*) FROM {t.sql} WHERE id=99').fetchone()==(0,)
        assert db.one(select(t.column('id',int)))==1
