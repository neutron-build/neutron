"""Native synchronous/asynchronous psycopg connections with explicit ownership."""
from __future__ import annotations
import asyncio
from contextlib import contextmanager, asynccontextmanager
import threading
from typing import Any, AsyncIterator, Iterator, TypeVar
from .core import CardinalityError, Mutation, OrmError, Select, SessionBusyError

T=TypeVar('T')


def _profile(profile: str) -> None:
    if profile != 'postgres-direct': raise OrmError('unsupported/unknown execution profile; operation refused')


def _native(error: Exception, *, committing: bool = False) -> OrmError:
    # Native error text may carry parameter values; retain original in cause,
    # but do not reproduce it in a generic public message.
    state=getattr(error,'sqlstate',None)
    return OrmError('PostgreSQL operation failed',sqlstate=state,outcome=('aborted' if state else 'indeterminate') if committing else None)


class Database:
    """One native connection. Concurrent active use rejected; no tracked objects."""
    def __init__(self, connection: Any) -> None:
        self._conn=connection
        self._lock=threading.Lock()
        self._closed=False
        self._owner: int | None=None

    @classmethod
    def connect(cls, url: str, *, profile: str='postgres-direct') -> Database:
        _profile(profile)
        import psycopg
        from psycopg.rows import dict_row
        return cls(psycopg.connect(url,autocommit=True,row_factory=dict_row))

    @contextmanager
    def _use(self) -> Iterator[None]:
        if self._closed: raise OrmError('connection closed')
        if self._owner is not None and self._owner != threading.get_ident():
            raise SessionBusyError('transaction belongs to another thread')
        if not self._lock.acquire(blocking=False): raise SessionBusyError('concurrent active session use refused')
        try:
            if self._closed: raise OrmError("connection closed")
            yield
        finally: self._lock.release()

    def _read(self, query: Select[T], cardinality: str) -> list[T]:
        compiled=query.compile()
        with self._use():
            try:
                with self._conn.cursor() as cur:
                    cur.execute(compiled.sql,compiled.params)
                    rows=cur.fetchall() if cardinality=='many' else cur.fetchmany(2)
            except Exception as exc: raise _native(exc) from exc
        if cardinality != 'many' and len(rows)>1: raise CardinalityError('expected at most one row')
        if cardinality=='one' and not rows: raise CardinalityError('expected exactly one row')
        return [compiled.decode(row) for row in rows]

    def all(self, query: Select[T]) -> list[T]: return self._read(query,'many')
    def one(self, query: Select[T]) -> T: return self._read(query,'one')[0]
    def one_or_none(self, query: Select[T]) -> T | None:
        rows=self._read(query,'optional'); return rows[0] if rows else None

    def execute(self, statement: Mutation) -> int:
        with self._use():
            try:
                with self._conn.cursor() as cur:
                    cur.execute(statement.sql,statement.params)
                    return int(cur.rowcount)
            except Exception as exc: raise _native(exc) from exc

    @contextmanager
    def transaction(self) -> Iterator[Database]:
        if self._owner is not None: raise SessionBusyError('nested transaction unsupported in this slice')
        # Hold ownership across the entire native transaction; per-statement
        # guard rejects other threads even between queries.
        with self._use(): self._owner=threading.get_ident()
        committing=False
        try:
            with self._conn.transaction():
                yield self
                committing=True
        except OrmError: raise
        except Exception as exc:
            if hasattr(exc,"sqlstate"): raise _native(exc,committing=committing) from exc
            raise
        finally: self._owner=None

    def close(self) -> None:
        if self._closed: return
        if self._owner is not None: raise SessionBusyError('close during active transaction refused')
        with self._use():
            self._conn.close(); self._closed=True

    def __enter__(self) -> Database: return self
    def __exit__(self, *_: object) -> None: self.close()


class AsyncDatabase:
    """One native async connection; operation/transaction task ownership explicit."""
    def __init__(self, connection: Any) -> None:
        self._conn=connection
        self._busy=False
        self._closed=False
        self._owner: asyncio.Task[Any] | None=None

    @classmethod
    async def connect(cls,url: str,*,profile: str='postgres-direct') -> AsyncDatabase:
        _profile(profile)
        import psycopg
        from psycopg.rows import dict_row
        return cls(await psycopg.AsyncConnection.connect(url,autocommit=True,row_factory=dict_row))

    @asynccontextmanager
    async def _use(self) -> AsyncIterator[None]:
        if self._closed: raise OrmError('connection closed')
        if self._owner is not None and self._owner is not asyncio.current_task(): raise SessionBusyError('transaction belongs to another task')
        if self._busy: raise SessionBusyError('concurrent active session use refused')
        self._busy=True
        try: yield
        finally: self._busy=False

    async def _read(self,query: Select[T],cardinality: str) -> list[T]:
        compiled=query.compile()
        async with self._use():
            try:
                async with self._conn.cursor() as cur:
                    await cur.execute(compiled.sql,compiled.params)
                    rows=await cur.fetchall() if cardinality=='many' else await cur.fetchmany(2)
            except Exception as exc: raise _native(exc) from exc
        if cardinality!='many' and len(rows)>1: raise CardinalityError('expected at most one row')
        if cardinality=='one' and not rows: raise CardinalityError('expected exactly one row')
        return [compiled.decode(row) for row in rows]

    async def all(self,query: Select[T]) -> list[T]: return await self._read(query,'many')
    async def one(self,query: Select[T]) -> T: return (await self._read(query,'one'))[0]
    async def one_or_none(self,query: Select[T]) -> T | None:
        rows=await self._read(query,'optional'); return rows[0] if rows else None

    async def execute(self,statement: Mutation) -> int:
        async with self._use():
            try:
                async with self._conn.cursor() as cur:
                    await cur.execute(statement.sql,statement.params)
                    return int(cur.rowcount)
            except Exception as exc: raise _native(exc) from exc

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[AsyncDatabase]:
        if self._owner is not None: raise SessionBusyError('nested transaction unsupported in this slice')
        async with self._use(): self._owner=asyncio.current_task()
        committing=False
        try:
            async with self._conn.transaction():
                yield self
                committing=True
        except OrmError: raise
        except Exception as exc:
            if hasattr(exc,"sqlstate"): raise _native(exc,committing=committing) from exc
            raise
        finally: self._owner=None

    async def close(self) -> None:
        if self._closed: return
        if self._owner is not None: raise SessionBusyError('close during active transaction refused')
        async with self._use():
            await self._conn.close(); self._closed=True

    async def __aenter__(self) -> AsyncDatabase: return self
    async def __aexit__(self,*_: object) -> None: await self.close()
