import asyncio
from dataclasses import asdict
import pytest
from neutron.orm import AsyncDatabase,Database,Mutation,OrmError,QueryObserver,insert,select
from .test_orm_live import live_table


def test_native_observer_returning_failure_and_stream_never_capture_secrets(live_table):
    url,table,native=live_table;secret='private-tenant-bound-value';observer=QueryObserver(capacity=8)
    with Database.connect(url,observer=observer) as db:
        assert db.one(insert(table,{'id':1,'label':secret}).returning(table.column('id',int)))==1
        assert db.one(select(table.column('label',str)))==secret
        with pytest.raises(OrmError) as failed:
            with db.transaction(): db.execute(insert(table,{'id':1,'label':secret}))
        assert failed.value.sqlstate=='23505'
        with db.stream(select(table.column('label',str)),batch_size=1) as rows: assert list(rows)==[secret]
    events=observer.drain()
    assert [event.operation for event in events]==['query','query','execute','stream']
    assert [event.sqlstate for event in events]==[None,None,'23505',None]
    assert observer.metrics.queries==2 and observer.metrics.executions==1 and observer.metrics.streams==1 and observer.metrics.failures==1
    assert secret not in repr([asdict(event) for event in events]) and url not in repr(events)
    assert native.execute(f'SELECT count(*),max(label) FROM {table.sql}').fetchone()==(1,secret)

@pytest.mark.asyncio
async def test_native_async_observer_cancellation_rollback_and_connection_reuse(live_table):
    url,table,native=live_table;observer=QueryObserver();ready=asyncio.Event()
    async with await AsyncDatabase.connect(url,observer=observer) as db:
        async def cancelled_operation():
            async with db.transaction():
                await db.execute(insert(table,{'id':1,'label':'cancelled-private-value'}))
                ready.set();await db.execute(Mutation('SELECT pg_catalog.pg_sleep(30)',()))
        task=asyncio.create_task(cancelled_operation());await ready.wait();task.cancel()
        with pytest.raises(asyncio.CancelledError): await task
        assert await db.all(select(table.column('id',int)))==[]
    events=observer.drain();assert len(events)==3
    assert events[1].outcome=='cancelled' and events[1].operation=='execute' and events[1].owned_transaction
    assert events[2].outcome=='ok' and observer.metrics.cancellations==1
    assert 'cancelled-private-value' not in repr(events) and url not in repr(events)
    assert native.execute(f'SELECT count(*) FROM {table.sql}').fetchone()==(0,)
