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

@pytest.mark.parametrize('state,outcome',[(None,'indeterminate'),('40001','aborted')])
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
