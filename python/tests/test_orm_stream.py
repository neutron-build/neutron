from collections import deque
from contextlib import contextmanager,asynccontextmanager
import asyncio
import pytest
from neutron.orm import AsyncDatabase,Database,ColumnSpec,OrmError,SessionBusyError,Table,select
from .test_orm_clients import Connection,Cursor,AsyncCursor,AsyncConnection

T=Table('items',{'id':ColumnSpec(int,'int4')})
Q=select(T.column('id',int))

class ServerCursor(Cursor):
    def __init__(self,rows): super().__init__(rows);self.pending=deque(rows);self.fetches=[];self.closed=False
    def fetchmany(self,n):
        self.fetches.append(n)
        return [self.pending.popleft() for _ in range(min(n,len(self.pending)))]
    def fetchall(self): raise AssertionError('whole fetch forbidden')
    def close(self): self.closed=True

class StreamConnection(Connection):
    def __init__(self,rows): super().__init__(rows);self.server=ServerCursor(rows);self.outcomes=[];self.controls=[]
    def cursor(self,**kw):
        if kw:
            assert kw['name'].startswith('neutron_stream_') and not kw['withhold']
            return self.server
        return Cursor([])
    @contextmanager
    def transaction(self):
        try: yield
        except BaseException: self.outcomes.append('rollback');raise
        else: self.outcomes.append('commit')


def test_owned_server_batches_busy_guard_terminal_and_early_rollback():
    conn=StreamConnection([{'id':i} for i in range(5)]);db=Database(conn)
    with db.stream(Q,batch_size=2) as rows:
        escaped=rows
        assert next(rows)==0
        with pytest.raises(SessionBusyError): db.all(Q)
        assert list(rows)==[1,2,3,4]
    assert conn.server.fetches==[2,2,2,2] and conn.server.closed
    assert conn.outcomes==['commit']
    with pytest.raises(OrmError,match='terminal'): next(escaped)
    conn=StreamConnection([{'id':1},{'id':2}]);db=Database(conn)
    with db.stream(Q,batch_size=1) as rows: assert next(rows)==1
    assert conn.outcomes==['rollback'] and not db.closed


def test_borrowed_stream_preserves_transaction_and_blocks_terminal_handle():
    conn=StreamConnection([{'id':1}]);db=Database(conn);tx=db.begin()
    with db.stream(Q,batch_size=1) as rows:
        assert next(rows)==1
        with pytest.raises(SessionBusyError): tx.commit()
        with pytest.raises(SessionBusyError): tx.rollback()
    assert tx.state=='active' and conn.outcomes==[]
    tx.commit();assert conn.outcomes==['commit']


def test_decode_failure_poison_and_invalid_batch():
    for invalid in (True,0,-1,10001,float('inf')):
        with pytest.raises(ValueError): Database(StreamConnection([])).stream(Q,batch_size=invalid)
    conn=StreamConnection([{'id':'bad'}]);db=Database(conn)
    with pytest.raises(RuntimeError,match='explicit rollback'):
        with db.transaction():
            with pytest.raises(ValueError):
                with db.stream(Q,batch_size=1) as rows: next(rows)
            with pytest.raises(OrmError): db.all(Q)
            raise RuntimeError('explicit rollback')

class AsyncServerCursor(ServerCursor):
    async def execute(self,*args): pass
    async def fetchmany(self,n): return super().fetchmany(n)
    async def close(self): self.closed=True

class AsyncStreamConnection(AsyncConnection):
    def __init__(self,rows): super().__init__(rows);self.server=AsyncServerCursor(rows);self.outcomes=[]
    def cursor(self,**kw): return self.server if kw else AsyncCursor([])
    @asynccontextmanager
    async def transaction(self):
        try: yield
        except BaseException: self.outcomes.append('rollback');raise
        else: self.outcomes.append('commit')

@pytest.mark.asyncio
async def test_async_bounded_iteration_and_early_exit():
    conn=AsyncStreamConnection([{'id':i} for i in range(5)]);db=AsyncDatabase(conn)
    async with db.stream(Q,batch_size=2) as rows:
        escaped=rows
        assert await anext(rows)==0
        with pytest.raises(SessionBusyError): await db.all(Q)
        assert [item async for item in rows]==[1,2,3,4]
    assert conn.server.fetches==[2,2,2,2] and conn.outcomes==['commit']
    with pytest.raises(OrmError): await anext(escaped)
    conn=AsyncStreamConnection([{'id':1}]);db=AsyncDatabase(conn)
    async with db.stream(Q,batch_size=1) as rows: assert await anext(rows)==1
    assert conn.outcomes==['rollback']


def test_borrowed_decoder_failure_cannot_commit():
    conn=StreamConnection([{'id':'bad'}]);db=Database(conn)
    with pytest.raises(OrmError,match='requires rollback'):
        with db.transaction():
            with pytest.raises(ValueError):
                with db.stream(Q,batch_size=1) as rows: next(rows)
    assert conn.outcomes==['rollback']


def test_cursor_close_failure_discards_connection():
    conn=StreamConnection([{'id':1}]);db=Database(conn)
    def fail(): raise RuntimeError('secret driver diagnostic')
    conn.server.close=fail
    with pytest.raises(OrmError) as caught:
        with db.stream(Q,batch_size=1) as rows: assert list(rows)==[1]
    assert 'secret' not in str(caught.value) and db.closed and conn.closed
    with pytest.raises(OrmError): db.all(Q)


def test_abandoned_borrowed_scope_cannot_read_buffered_row():
    conn=StreamConnection([{'id':1},{'id':2}]);db=Database(conn)
    stream=db.stream(Q,batch_size=2)
    with pytest.raises(OrmError,match='active stream'):
        with db.transaction():
            rows=stream.__enter__();assert next(rows)==1
    assert db.closed
    with pytest.raises(OrmError,match='terminal'): next(rows)
    stream.__exit__(None,None,None)
    assert not db._stream_lease


@pytest.mark.asyncio
async def test_async_owner_rejection_preserves_buffer_and_handle():
    conn=AsyncStreamConnection([{'id':1},{'id':2}]);db=AsyncDatabase(conn)
    tx=await db.begin()
    async with db.stream(Q,batch_size=2) as rows:
        assert await anext(rows)==1
        with pytest.raises(SessionBusyError): await asyncio.create_task(anext(rows))
        with pytest.raises(SessionBusyError): await tx.commit()
        assert await anext(rows)==2
    assert tx.state=='active';await tx.commit()


@pytest.mark.asyncio
async def test_async_cancel_fetch_rolls_back_and_releases():
    conn=AsyncStreamConnection([{'id':1}]);db=AsyncDatabase(conn);entered=asyncio.Event()
    async def block(n): entered.set();await asyncio.Event().wait()
    conn.server.fetchmany=block
    async def consume():
        async with db.stream(Q,batch_size=1) as rows: await anext(rows)
    task=asyncio.create_task(consume());await entered.wait();task.cancel()
    with pytest.raises(asyncio.CancelledError): await task
    assert conn.outcomes==['rollback'] and conn.server.closed and not db.closed
    assert not db._stream_lease and db._owner is None


@pytest.mark.asyncio
async def test_async_cleanup_repeated_cancellation_fences():
    conn=AsyncStreamConnection([{'id':1}]);db=AsyncDatabase(conn);fetch=asyncio.Event();closing=asyncio.Event()
    async def block(n): fetch.set();await asyncio.Event().wait()
    async def close(): closing.set();await asyncio.Event().wait()
    conn.server.fetchmany=block;conn.server.close=close
    async def consume():
        async with db.stream(Q,batch_size=1) as rows: await anext(rows)
    task=asyncio.create_task(consume());await fetch.wait();task.cancel();await closing.wait();task.cancel()
    with pytest.raises(asyncio.CancelledError): await task
    assert db.closed and not db._stream_lease and db._owner is None
    with pytest.raises(OrmError): await db.all(Q)


@pytest.mark.asyncio
async def test_async_cleanup_deadline_fences(monkeypatch):
    from neutron.orm import streaming
    monkeypatch.setattr(streaming,'CLEANUP_SECONDS',0.01)
    conn=AsyncStreamConnection([]);db=AsyncDatabase(conn)
    async def close(): await asyncio.Event().wait()
    conn.server.close=close
    with pytest.raises(asyncio.CancelledError):
        async with db.stream(Q,batch_size=1) as rows:
            assert [item async for item in rows]==[]
    assert db.closed and not db._stream_lease and db._owner is None


def test_stream_refuses_mutation_returning_and_cross_thread():
    import threading
    from neutron.orm import insert
    db=Database(StreamConnection([{'id':1},{'id':2}]))
    with pytest.raises(ValueError): db.stream(insert(T,{'id':1}).returning(T.column('id',int)),batch_size=1)
    with db.stream(Q,batch_size=2) as rows:
        assert next(rows)==1
        errors=[]
        def other():
            try: next(rows)
            except BaseException as exc: errors.append(exc)
        thread=threading.Thread(target=other);thread.start();thread.join()
        assert len(errors)==1 and isinstance(errors[0],SessionBusyError)
        assert list(rows)==[2]


def test_sync_keyboard_interrupt_rolls_back_and_failed_second_lease_does_not_clear_first():
    conn=StreamConnection([{'id':1}]);db=Database(conn)
    with db.stream(Q,batch_size=1) as rows:
        with pytest.raises(SessionBusyError):
            with db.stream(Q,batch_size=1): pass
        assert db._stream_lease
        assert list(rows)==[1]
    conn=StreamConnection([{'id':1}]);db=Database(conn)
    def interrupt(n): raise KeyboardInterrupt()
    conn.server.fetchmany=interrupt
    with pytest.raises(KeyboardInterrupt):
        with db.stream(Q,batch_size=1) as rows: next(rows)
    assert conn.outcomes==['rollback'] and not db.closed and not db._stream_lease


def test_caught_decode_failure_terminalizes_buffered_sync_iterator():
    conn=StreamConnection([{'id':'bad'},{'id':2}]);db=Database(conn)
    tx=db.begin()
    with db.stream(Q,batch_size=2) as rows:
        with pytest.raises(ValueError): next(rows)
        with pytest.raises(OrmError,match='terminal'): next(rows)
    assert conn.server.fetches==[2]
    with pytest.raises(OrmError,match='requires rollback'): tx.commit()
    assert conn.outcomes==['rollback']

@pytest.mark.asyncio
async def test_caught_decode_failure_terminalizes_buffered_async_iterator():
    conn=AsyncStreamConnection([{'id':'bad'},{'id':2}]);db=AsyncDatabase(conn)
    tx=await db.begin()
    async with db.stream(Q,batch_size=2) as rows:
        with pytest.raises(ValueError): await anext(rows)
        with pytest.raises(OrmError,match='terminal'): await anext(rows)
    assert conn.server.fetches==[2]
    with pytest.raises(OrmError,match='requires rollback'): await tx.commit()
    assert conn.outcomes==['rollback']
