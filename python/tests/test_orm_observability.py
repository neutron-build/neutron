import asyncio
from dataclasses import asdict
import pytest
from neutron.orm import AsyncDatabase,ColumnSpec,Database,Mutation,QueryObserver,Table,select
from neutron.orm.observability import _measure
from .test_orm_clients import AsyncConnection,Connection
from .test_orm_stream import StreamConnection,AsyncStreamConnection,Q


def test_bounded_observer_redacts_values_identifiers_and_tracks_dispatch():
    secret='credential-and-tenant-secret'
    table=Table(secret,{'value':ColumnSpec(str,'text')})
    observer=QueryObserver(capacity=2);db=Database(Connection([{'value':secret}]),observer=observer)
    query=select(table.column('value',str)).where(table.column('value',str).eq(secret))
    assert db.one(query)==secret
    assert db.execute(Mutation('SELECT %s',(secret,)))==1
    assert db.one(query)==secret
    assert observer.metrics.queries==2 and observer.metrics.executions==1 and observer.metrics.dropped_events==1
    events=observer.drain();assert len(events)==2 and observer.drain()==()
    assert all(event.elapsed_ns>=0 and event.row_count==1 and event.outcome=='ok' for event in events)
    assert secret not in repr([asdict(event) for event in events]) and secret not in repr(observer.metrics)
    with pytest.raises(ValueError): db.execute(Mutation('COMMIT',()))
    assert observer.drain()==() and observer.metrics.executions==1
    for capacity in (0,4097,True):
        with pytest.raises(ValueError): QueryObserver(capacity=capacity)


def test_observer_failure_does_not_inspect_exception_messages_or_properties():
    class PrivateFailure(Exception):
        @property
        def sqlstate(self): raise AssertionError('arbitrary exception property inspected')
        def __str__(self): raise AssertionError('private exception rendered')
        def __repr__(self): raise AssertionError('private exception rendered')
    observer=QueryObserver()
    with pytest.raises(PrivateFailure):
        with _measure(observer,'query') as measurement:
            measurement.dispatch(owned=True);raise PrivateFailure('secret')
    event,=observer.drain();assert event.sqlstate is None and event.outcome=='error' and event.owned_transaction


def test_observer_stream_lifetime_counts_consumed_rows_once():
    observer=QueryObserver();db=Database(StreamConnection([{'id':1},{'id':2}]),observer=observer)
    with db.stream(Q,batch_size=1) as rows:
        assert next(rows)==1 and observer.drain()==()
    event,=observer.drain();assert event.operation=='stream' and event.row_count==1 and event.outcome=='ok'
    assert observer.metrics.streams==1 and observer.metrics.queries==0

@pytest.mark.asyncio
async def test_async_observer_cancellation_and_stream_counts():
    observer=QueryObserver();waiting=asyncio.Event();db=AsyncDatabase(AsyncConnection([{'id':1}],waiting),observer=observer)
    task=asyncio.create_task(db.one(Q));await asyncio.sleep(0);task.cancel()
    with pytest.raises(asyncio.CancelledError): await task
    event,=observer.drain();assert event.operation=='query' and event.outcome=='cancelled'
    assert observer.metrics.cancellations==1 and observer.metrics.failures==1
    stream_db=AsyncDatabase(AsyncStreamConnection([{'id':1},{'id':2}]),observer=observer)
    async with stream_db.stream(Q,batch_size=1) as rows:
        assert [value async for value in rows]==[1,2]
    event,=observer.drain();assert event.operation=='stream' and event.row_count==2 and event.outcome=='ok'


def test_observer_does_not_invoke_exception_metaclass_dictionary_access():
    calls=[]
    class HostileMeta(type):
        def __getattribute__(cls,name):
            if name=='__dict__': calls.append(name);raise AssertionError('exception metaclass callback invoked')
            return super().__getattribute__(name)
    class HostileFailure(Exception,metaclass=HostileMeta): pass
    original=HostileFailure('private');observer=QueryObserver()
    with pytest.raises(HostileFailure) as caught:
        with _measure(observer,'query') as measurement:
            measurement.dispatch(owned=True);raise original
    assert caught.value is original and calls==[]
    event,=observer.drain();assert event.outcome=='error' and event.sqlstate is None
    class CancellationFailure(Exception): sqlstate='57014'
    with pytest.raises(CancellationFailure):
        with _measure(observer,'query') as measurement:
            measurement.dispatch(owned=True);raise CancellationFailure()
    event,=observer.drain();assert event.outcome=='cancelled' and event.sqlstate=='57014'
