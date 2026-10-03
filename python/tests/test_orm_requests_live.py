import asyncio
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
import threading
import pytest
from neutron.orm import AsyncSessionRequests,ModelMapping,Mutation,OrmError,QueryObserver,SessionRequests
from .test_orm_live import live_table

@dataclass
class RequestRow:
    id: int
    label: str


def mapping_for(table):
    return ModelMapping(RequestRow,table,{'id':table.columns['id'],'label':table.columns['label']},primary_key=('id',))


def test_native_sync_requests_fresh_identity_failure_cleanup_and_stop(live_table):
    url,table,native=live_table;observer=QueryObserver();requests=SessionRequests(url,observer=observer)
    mapping=mapping_for(table)
    with requests.session() as first:
        obj=RequestRow(1,'private-request-value');first.add(mapping,obj)
    assert first._database.closed
    with requests.session() as second:
        loaded=second.get(mapping,1);assert loaded is not None and loaded is not obj
    assert second._database.closed
    with pytest.raises(RuntimeError):
        with requests.session() as failed:
            changed=failed.get(mapping,1);assert changed is not None
            changed.label='rolled-back';failed.flush();raise RuntimeError('application failure')
    assert failed._database.closed
    requests.shutdown()
    with pytest.raises(OrmError):
        with requests.session(): pass
    assert native.execute(f'SELECT label FROM {table.sql}').fetchone()==('private-request-value',)
    assert 'private-request-value' not in repr(observer.drain())


def test_native_sync_request_shutdown_drains_existing_worker(live_table):
    url,table,native=live_table;requests=SessionRequests(url);ready=threading.Event();release=threading.Event();mapping=mapping_for(table)
    def worker():
        with requests.session() as session:
            session.add(mapping,RequestRow(1,'worker-owned'));ready.set();assert release.wait(5)
        return session._database.closed
    with ThreadPoolExecutor(max_workers=2) as executor:
        request=executor.submit(worker);assert ready.wait(5)
        shutdown=executor.submit(requests.shutdown,grace_seconds=5)
        threading.Event().wait(0.3);assert not shutdown.done() # shutdown blocks while the worker owns a Session
        release.set();assert request.result(timeout=5);shutdown.result(timeout=5)
    assert native.execute(f'SELECT label FROM {table.sql}').fetchone()==('worker-owned',)

def test_native_sync_request_admission_slot_release_and_shutdown_deadline(live_table):
    url,table,native=live_table;mapping=mapping_for(table);requests=SessionRequests(url,max_active=1);ready=threading.Event();release=threading.Event()
    with requests.session():
        with pytest.raises(OrmError):  # nested owner refused
            with requests.session(): pass
    def holder():
        with requests.session(): ready.set();assert release.wait(5)
    with ThreadPoolExecutor(max_workers=2) as executor:
        held=executor.submit(holder);assert ready.wait(5)
        def other():
            with requests.session(): pass
        with pytest.raises(OrmError): executor.submit(other).result(timeout=5)  # cap reached by another thread
        with pytest.raises(OrmError): requests.shutdown(grace_seconds=0.05)  # worker still owns a Session at the deadline
        release.set();held.result(timeout=5)
    requests.shutdown()
    bad=SessionRequests('postgresql://127.0.0.1:1/none?connect_timeout=1',max_active=1)
    for _ in range(2):  # a failed open releases its slot, so the second attempt is refused for connect, not capacity
        with pytest.raises(Exception) as caught:
            with bad.session(): pass
        assert 'admission refused' not in str(caught.value)
    failing=SessionRequests(url,max_active=1)
    for _ in range(2):
        with pytest.raises(RuntimeError):
            with failing.session(): raise RuntimeError('application failure')
    with failing.session(): pass

def _sleeping_backends(native) -> int:
    return native.execute("SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE datname=current_database() AND pid<>pg_catalog.pg_backend_pid() AND query LIKE '%pg_sleep(30)%'").fetchone()[0]

@pytest.mark.asyncio
async def test_native_async_requests_shutdown_cancels_owner_and_rolls_back(live_table):
    url,table,native=live_table;observer=QueryObserver();requests=AsyncSessionRequests(url,observer=observer,cleanup_seconds=5);mapping=mapping_for(table);ready=asyncio.Event();sessions=[]
    async def request():
        async with requests.session() as session:
            sessions.append(session);session.add(mapping,RequestRow(1,'cancelled-private-value'));await session.flush()
            ready.set();await session._database.execute(Mutation('SELECT pg_catalog.pg_sleep(30)',()))
    task=asyncio.create_task(request());await ready.wait()
    assert requests.active_count==1
    await requests.shutdown(grace_seconds=0.01)
    with pytest.raises(asyncio.CancelledError): await task
    assert requests.active_count==0 and sessions[0]._database.closed
    with pytest.raises(OrmError):
        async with requests.session(): pass
    assert native.execute(f'SELECT count(*) FROM {table.sql}').fetchone()==(0,)
    for _ in range(50):  # the owning backend must be gone, not merely uncommitted
        if _sleeping_backends(native)==0: break
        await asyncio.sleep(0.1)
    assert _sleeping_backends(native)==0
    assert 'cancelled-private-value' not in repr(observer.drain())

@pytest.mark.asyncio
async def test_native_async_request_cap_across_tasks_and_slot_release(live_table):
    url,table,native=live_table;requests=AsyncSessionRequests(url,max_active=1);ready=asyncio.Event();release=asyncio.Event()
    async def holder():
        async with requests.session(): ready.set();await release.wait()
    task=asyncio.create_task(holder());await ready.wait()
    async def other():
        async with requests.session(): pass
    with pytest.raises(OrmError): await asyncio.create_task(other())
    release.set();await task
    assert requests.active_count==0
    for _ in range(2):  # body failure releases the slot for re-admission
        with pytest.raises(RuntimeError):
            async with requests.session(): raise RuntimeError('application failure')
        assert requests.active_count==0
    bad=AsyncSessionRequests('postgresql://127.0.0.1:1/none?connect_timeout=1',max_active=1)
    for _ in range(2):
        with pytest.raises(Exception) as caught:
            async with bad.session(): pass
        assert 'admission refused' not in str(caught.value) and bad.active_count==0
    await requests.shutdown()

@pytest.mark.asyncio
async def test_native_async_request_admission_and_fresh_model_ownership(live_table):
    url,table,native=live_table;requests=AsyncSessionRequests(url,max_active=1);mapping=mapping_for(table)
    async with requests.session() as first:
        first.add(mapping,RequestRow(1,'first'))
        with pytest.raises(OrmError):
            async with requests.session(): pass
    async with requests.session() as second:
        one=await second.get(mapping,1);assert one is not None
    async with requests.session() as third:
        two=await third.get(mapping,1);assert two is not None and two is not one
    assert first._database.closed and second._database.closed and third._database.closed
    await requests.shutdown()
