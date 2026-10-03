"""Native async scalar mapped lifecycle using the common pure state engine."""
from __future__ import annotations
import asyncio
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, TypeVar
from .client import AsyncDatabase
from .core import CardinalityError, OrmError, SessionBusyError, delete, insert, select_row, update
from .lifecycle import AsyncTransactionHandle
from .mapping import ModelMapping
from .session import ConflictError, _SessionState
from .state import ObjectState
T=TypeVar('T')

class AsyncSession(_SessionState):
    _database: AsyncDatabase
    def __init__(self,database: AsyncDatabase,*,autobegin: bool=True,autoflush: bool=True,close_database: bool=False) -> None:
        super().__init__(autobegin=autobegin,autoflush=autoflush)
        self._database=database;self._transaction: AsyncTransactionHandle | None=None
        self._owner=asyncio.current_task();self._close_database=close_database
        if self._owner is None: raise SessionBusyError('AsyncSession requires an owning asyncio task')

    @classmethod
    async def connect(cls,url: str,*,autobegin: bool=True,autoflush: bool=True) -> AsyncSession:
        return cls(await AsyncDatabase.connect(url),autobegin=autobegin,autoflush=autoflush,close_database=True)

    def _owner_check(self) -> None:
        if asyncio.current_task() is not self._owner: raise SessionBusyError('mapped Session belongs to another task')

    async def _ensure_transaction(self) -> None:
        if self._transaction is None:
            if not self.autobegin: raise OrmError('autobegin disabled; use AsyncSession.begin')
            self._transaction=await self._database.begin()

    @asynccontextmanager
    async def begin(self) -> AsyncIterator[AsyncSession]:
        self._guard()
        if self._transaction is not None: raise SessionBusyError('mapped Session transaction already active')
        self._transaction=await self._database.begin()
        try:
            yield self
            await self.commit()
        except BaseException:
            if self._transaction is not None and not self._uncertain: await self.rollback()
            raise

    async def get(self,mapping: ModelMapping[T],*key: Any) -> T | None:
        self._guard();self._mapping(mapping)
        values=self._key_values(mapping,key)
        if self.autoflush: await self.flush()
        found=self._store.find(mapping,key)
        if found is not None: return found
        try:
            await self._ensure_transaction()
            row=await self._database.one_or_none(select_row(mapping.table,*mapping.field_columns.values()).where(self._predicate(mapping,values)))
            if row is None: return None
            obj=mapping.construct(row);self._store.attach(mapping,obj,new=False);return obj
        except BaseException:
            self._failed=True;raise

    async def flush(self) -> None:
        self._guard()
        try:
            plans=self._plan()
            if not plans: return
            await self._ensure_transaction()
            for record,action,values in plans:
                mapping=record.mapping
                if action=='delete':
                    count=await self._database.execute(delete(mapping.table,where=self._predicate(mapping,record.baseline,all_fields=True)))
                    if count!=1: raise ConflictError('stale mapped delete')
                    record.state=ObjectState.DELETED
                else:
                    mutation=insert(mapping.table,values) if action=='insert' else update(mapping.table,values,where=self._predicate(mapping,record.baseline,all_fields=True))
                    try: row=await self._database.one(mutation.returning_row(*mapping.field_columns.values()))
                    except CardinalityError as exc: raise ConflictError('mapped write did not affect exactly one row') from exc
                    self._flushed_row(record,row)
        except BaseException:
            self._failed=True;raise

    async def commit(self) -> None:
        self._guard();await self.flush()
        if self._transaction is None: return
        tx=self._transaction
        try: await tx.commit()
        except BaseException:
            if tx.state=='aborted': self._store.rollback()
            else: self._store.uncertain();self._uncertain=True
            self._failed=True;raise
        else: self._store.committed()
        finally: self._transaction=None

    async def rollback(self) -> None:
        self._guard(allow_failed=True)
        try:
            if self._transaction is not None: await self._transaction.rollback()
        except BaseException:
            self._store.uncertain();self._uncertain=True;self._failed=True;raise
        else: self._store.rollback();self._failed=False
        finally: self._transaction=None

    async def close(self) -> None:
        self._owner_check()
        if self._closed: return
        try:
            if not self._uncertain: await self.rollback()
        finally:
            self._store.detach_all();self._closed=True
            if self._close_database: await self._database.close()

    async def __aenter__(self) -> AsyncSession: self._guard();return self
    async def __aexit__(self,*_: object) -> None: await self.close()
