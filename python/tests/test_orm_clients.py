import asyncio
from contextlib import contextmanager, asynccontextmanager
import pytest
from neutron.orm import AsyncDatabase, CardinalityError, ColumnSpec, Database, OrmError, SessionBusyError, Table, select

T=Table('t',{'id':ColumnSpec(int,'int4')})
QUERY=select(T.column('id',int))

class Cursor:
    rowcount=1
    def __init__(self,rows): self.rows=rows
    def __enter__(self): return self
    def __exit__(self,*args): pass
    def execute(self,*args): pass
    def fetchall(self): return self.rows
    def fetchmany(self,n): return self.rows[:n]

class Connection:
    def __init__(self,rows): self.rows=rows;self.closed=False
    def cursor(self): return Cursor(self.rows)
    def close(self): self.closed=True
    @contextmanager
    def transaction(self): yield

@pytest.mark.parametrize('rows', [[],[{'id':1},{'id':2}]])
def test_exact_one_cardinality(rows):
    with pytest.raises(CardinalityError): Database(Connection(rows)).one(QUERY)

def test_optional_multiple_and_closed():
    with pytest.raises(CardinalityError): Database(Connection([{'id':1},{'id':2}])).one_or_none(QUERY)
    db=Database(Connection([]));assert db.one_or_none(QUERY) is None
    db.close();db.close()
    with pytest.raises(OrmError): db.all(QUERY)

def test_profiles_refuse_before_driver_import():
    with pytest.raises(OrmError): Database.connect('do-not-connect',profile='unknown')

class AsyncCursor(Cursor):
    def __init__(self,rows,event=None): super().__init__(rows);self.event=event
    async def __aenter__(self): return self
    async def __aexit__(self,*args): pass
    async def execute(self,*args):
        if self.event: await self.event.wait()
    async def fetchall(self): return self.rows
    async def fetchmany(self,n): return self.rows[:n]

class AsyncConnection(Connection):
    def __init__(self,rows,event=None): super().__init__(rows);self.event=event
    def cursor(self): return AsyncCursor(self.rows,self.event)
    async def close(self): self.closed=True
    @asynccontextmanager
    async def transaction(self): yield

@pytest.mark.asyncio
async def test_concurrent_session_rejects_and_cancel_releases_guard():
    event=asyncio.Event();db=AsyncDatabase(AsyncConnection([{'id':1}],event))
    task=asyncio.create_task(db.all(QUERY));await asyncio.sleep(0)
    with pytest.raises(SessionBusyError): await db.all(QUERY)
    with pytest.raises(SessionBusyError): await db.close()
    task.cancel()
    with pytest.raises(asyncio.CancelledError): await task
    event.set();assert await db.one(QUERY)==1
    await db.close()

@pytest.mark.asyncio
async def test_transaction_task_ownership_even_between_queries():
    db=AsyncDatabase(AsyncConnection([{'id':1}]))
    async with db.transaction():
        with pytest.raises(SessionBusyError): await asyncio.create_task(db.all(QUERY))
        assert await db.one(QUERY)==1
        with pytest.raises(SessionBusyError): await db.close()
    await db.close()

class NativeFailure(Exception):
    def __init__(self,sqlstate): self.sqlstate=sqlstate

class CommitFailureConnection(Connection):
    def __init__(self,state): super().__init__([]);self.state=state
    @contextmanager
    def transaction(self):
        yield
        raise NativeFailure(self.state)

@pytest.mark.parametrize('state,outcome',[(None,'indeterminate'),('40001','aborted'),('08006','indeterminate'),('08007','indeterminate'),('40003','indeterminate'),('XX000','indeterminate')])
def test_commit_failure_keeps_known_or_unknown_outcome(state,outcome):
    db=Database(CommitFailureConnection(state))
    with pytest.raises(OrmError) as exc:
        with db.transaction(): pass
    assert exc.value.outcome==outcome
    assert exc.value.sqlstate==state
    assert isinstance(exc.value.__cause__,NativeFailure)


def test_transaction_does_not_mask_business_exception():
    db=Database(Connection([]))
    with pytest.raises(ValueError,match='business'):
        with db.transaction(): raise ValueError('business')


class RollbackFailureConnection(Connection):
    @contextmanager
    def transaction(self):
        try: yield
        finally: raise NativeFailure('08006')

def test_rollback_cleanup_failure_fences_connection():
    conn=RollbackFailureConnection([]);db=Database(conn)
    with pytest.raises(OrmError):
        with db.transaction(): raise ValueError('business')
    assert conn.closed
    with pytest.raises(OrmError,match='closed'): db.all(QUERY)

class FakePGConn:
    def __init__(self): self.finished=False
    def finish(self): self.finished=True

class SlowRollbackConnection(AsyncConnection):
    def __init__(self):
        super().__init__([]);self.cleanup_started=asyncio.Event();self.never=asyncio.Event();self.pgconn=FakePGConn()
    @asynccontextmanager
    async def transaction(self):
        try: yield
        finally:
            self.cleanup_started.set();await self.never.wait()

@pytest.mark.asyncio
async def test_repeated_cancellation_during_rollback_discards():
    conn=SlowRollbackConnection();db=AsyncDatabase(conn);body=asyncio.Event()
    async def work():
        async with db.transaction():
            body.set();await asyncio.Event().wait()
    task=asyncio.create_task(work());await body.wait()
    task.cancel();await conn.cleanup_started.wait();task.cancel()
    with pytest.raises(asyncio.CancelledError): await task
    assert conn.pgconn.finished
    with pytest.raises(OrmError,match='closed'): await db.all(QUERY)

@pytest.mark.asyncio
async def test_async_connect_sanitizes_driver_error(monkeypatch):
    import psycopg
    async def broken(*args,**kwargs): raise ValueError('postgresql://user:secret@host/db')
    monkeypatch.setattr(psycopg.AsyncConnection,'connect',broken)
    with pytest.raises(OrmError) as exc: await AsyncDatabase.connect('postgresql://user:secret@host/db')
    assert 'secret' not in str(exc.value)
    assert isinstance(exc.value.__cause__,ValueError)

def test_sync_connect_sanitizes_driver_error(monkeypatch):
    import psycopg
    def broken(*args,**kwargs): raise ValueError('postgresql://user:secret@host/db')
    monkeypatch.setattr(psycopg,'connect',broken)
    with pytest.raises(OrmError) as exc: Database.connect('postgresql://user:secret@host/db')
    assert 'secret' not in str(exc.value)
    assert isinstance(exc.value.__cause__,ValueError)


class SlowCommitConnection(AsyncConnection):
    def __init__(self):
        super().__init__([]);self.commit_started=asyncio.Event();self.never=asyncio.Event();self.pgconn=FakePGConn()
    @asynccontextmanager
    async def transaction(self):
        yield
        self.commit_started.set();await self.never.wait()

@pytest.mark.asyncio
async def test_commit_cancellation_preserves_cancel_semantics_and_unknown_outcome():
    from neutron.orm import CommitCancelledError
    conn=SlowCommitConnection();db=AsyncDatabase(conn)
    async def work():
        async with db.transaction(): pass
    task=asyncio.create_task(work());await conn.commit_started.wait();task.cancel()
    with pytest.raises(CommitCancelledError) as exc: await task
    assert isinstance(exc.value,asyncio.CancelledError)
    assert exc.value.outcome=='indeterminate'
    assert conn.pgconn.finished
