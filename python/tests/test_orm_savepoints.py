from contextlib import contextmanager,asynccontextmanager
import asyncio
import pytest
from neutron.orm import AsyncDatabase,Database,OrmError,SessionBusyError
from .test_orm_clients import AsyncConnection,Connection

class Failure(Exception):
    sqlstate='08006'

class BrokenSavepoint(Connection):
    def __init__(self,phase): super().__init__([]);self.phase=phase;self.count=0
    @contextmanager
    def transaction(self):
        self.count+=1;child=self.count>1
        if child and self.phase=='enter': raise Failure('private native value')
        try: yield
        except BaseException:
            if child and self.phase=='rollback': raise Failure('private native value')
            raise
        else:
            if child and self.phase=='release': raise Failure('private native value')

@pytest.mark.parametrize('phase',['enter','rollback','release'])
def test_savepoint_native_failure_redacts_and_fences(phase):
    db=Database(BrokenSavepoint(phase))
    with pytest.raises(OrmError) as caught:
        with db.transaction():
            with db.savepoint():
                if phase=='rollback': raise RuntimeError('body')
    assert caught.value.sqlstate=='08006' and 'private' not in str(caught.value)
    assert db.closed and db._owner is None and db._savepoint_depth==0

class CancelSavepoint(AsyncConnection):
    def __init__(self): super().__init__([]);self.count=0;self.closing=asyncio.Event()
    @asynccontextmanager
    async def transaction(self):
        self.count+=1;child=self.count>1
        try: yield
        except BaseException:
            if child:
                self.closing.set();await asyncio.Event().wait()
            raise

@pytest.mark.asyncio
async def test_repeated_savepoint_cancellation_fences_before_reuse():
    conn=CancelSavepoint();db=AsyncDatabase(conn);entered=asyncio.Event()
    async def consume():
        async with db.transaction():
            async with db.savepoint():
                entered.set();await asyncio.Event().wait()
    task=asyncio.create_task(consume());await entered.wait();task.cancel();await conn.closing.wait();task.cancel()
    with pytest.raises(asyncio.CancelledError): await task
    assert db.closed and db._owner is None and db._savepoint_depth==0


def test_savepoint_parent_handle_cannot_commit_or_rollback_mid_scope():
    db=Database(Connection([]));tx=db.begin()
    with db.savepoint():
        with pytest.raises(SessionBusyError): tx.commit()
        with pytest.raises(SessionBusyError): tx.rollback()
    tx.rollback()
