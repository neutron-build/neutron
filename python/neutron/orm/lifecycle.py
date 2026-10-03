"""Narrow transaction lifecycle adapter over the existing guarded contexts."""
from __future__ import annotations
import asyncio
import threading
from typing import TYPE_CHECKING, Any
from .core import OrmError, SessionBusyError
if TYPE_CHECKING:
    from .client import AsyncDatabase, Database

class _Rollback(Exception): pass

class TransactionHandle:
    def __init__(self, database: Database) -> None:
        self._database=database
        self._context=database.transaction()
        self._context.__enter__()
        self._owner=threading.get_ident()
        self.state='active'

    def _check(self) -> None:
        if threading.get_ident()!=self._owner: raise SessionBusyError('transaction belongs to another thread')
        if self.state!='active': raise OrmError('transaction handle is terminal')
        if self._database._savepoint_depth: raise SessionBusyError('transaction has an active savepoint')
        if self._database._stream_lease: raise SessionBusyError('transaction has an active stream lease')

    def commit(self) -> None:
        self._check();self.state='committing'
        try: self._context.__exit__(None,None,None)
        except BaseException as exc:
            self.state=getattr(exc,'outcome',None) or 'indeterminate'
            raise
        self.state='committed'

    def rollback(self) -> None:
        self._check();self.state='rolling-back'
        marker=_Rollback()
        try: self._context.__exit__(_Rollback,marker,None)
        except BaseException:
            self.state='indeterminate';raise
        self.state='aborted'

class AsyncTransactionHandle:
    def __init__(self, database: AsyncDatabase) -> None:
        self._database=database
        self._context=database.transaction()
        self._owner=asyncio.current_task()
        self.state='new'

    async def open(self) -> AsyncTransactionHandle:
        if self.state!='new' or asyncio.current_task() is not self._owner: raise SessionBusyError('transaction open ownership mismatch')
        try: await self._context.__aenter__()
        except BaseException:
            self.state='indeterminate';raise
        self.state='active';return self

    def _check(self) -> None:
        if asyncio.current_task() is not self._owner: raise SessionBusyError('transaction belongs to another task')
        if self.state!='active': raise OrmError('transaction handle is terminal')
        if self._database._savepoint_depth: raise SessionBusyError('transaction has an active savepoint')
        if self._database._stream_lease: raise SessionBusyError('transaction has an active stream lease')

    async def commit(self) -> None:
        self._check();self.state='committing'
        try: await self._context.__aexit__(None,None,None)
        except BaseException as exc:
            self.state=getattr(exc,'outcome',None) or 'indeterminate';raise
        self.state='committed'

    async def rollback(self) -> None:
        self._check();self.state='rolling-back'
        marker=_Rollback()
        try: await self._context.__aexit__(_Rollback,marker,None)
        except BaseException:
            self.state='indeterminate';raise
        self.state='aborted'
