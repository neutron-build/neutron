"""Context-owned native server cursors; no generator finalizer guarantees."""
from __future__ import annotations
import asyncio
from collections import deque
import socket
import threading
import time
from typing import Any, AsyncIterator, Awaitable, Callable, Generic, Iterator, TypeVar
from uuid import uuid4
from .client import AsyncDatabase, Database, _native
from .core import OrmError, Select, SessionBusyError
from .json_value import native_params
from .query import Query

T=TypeVar('T')
CLEANUP_SECONDS=5.0


def _validate(query: Select[T] | Query[T],batch_size: int) -> None:
    if not isinstance(query,(Select,Query)): raise ValueError('stream requires a read projection')
    if type(batch_size) is not int or not 1 <= batch_size <= 10000:
        raise ValueError('stream batch_size must be an integer from 1 through 10000')


def _sync_cleanup(db: Database,operation: Callable[[],Any],seconds: float=CLEANUP_SECONDS) -> None:
    if seconds<=0:
        db._discard();raise OrmError('stream cleanup timed out')
    expired=threading.Event();done=threading.Event();lock=threading.Lock()
    def fence() -> None:
        with lock:
            if done.is_set(): return
            expired.set();db._closed=True
            # Interrupt owner-thread libpq waiting without concurrent driver I/O.
            pgconn=getattr(db._conn,'pgconn',None)
            fd=getattr(pgconn,'socket',-1)
            if isinstance(fd,int) and fd>=0:
                channel=None
                try:
                    channel=socket.socket(fileno=fd);channel.shutdown(socket.SHUT_RDWR)
                except OSError: pass
                finally:
                    if channel is not None: channel.detach()
    timer=threading.Timer(max(0.0,seconds),fence);timer.daemon=True;timer.start()
    try:
        operation()
        if expired.is_set(): raise OrmError('stream cleanup timed out')
    except BaseException:
        db._discard();raise
    finally:
        with lock: done.set();timer.cancel()


async def _async_cleanup(db: AsyncDatabase,operation: Callable[[],Awaitable[Any]],seconds: float=CLEANUP_SECONDS) -> None:
    if seconds<=0:
        db._discard();raise OrmError('stream cleanup timed out')
    task=asyncio.current_task()
    if task is None: raise SessionBusyError('stream cleanup requires owning task')
    expired=False
    def fence() -> None:
        nonlocal expired
        expired=True;db._discard();task.cancel()
    timer=asyncio.get_running_loop().call_later(max(0.0,seconds),fence)
    try:
        await operation()
        if expired: raise OrmError('stream cleanup timed out')
    except BaseException:
        db._discard();raise
    finally: timer.cancel()


class Stream(Generic[T],Iterator[T]):
    def __init__(self,db: Database,query: Select[T] | Query[T],batch_size: int) -> None:
        _validate(query,batch_size)
        self._db=db;self._compiled=query.compile();self._batch_size=batch_size
        self._buffer: deque[Any]=deque();self._cursor: Any=None;self._tx: Any=None;self._lease: Any=None
        self._token: object | None=None;self._owner: int | None=None
        self._open=False;self._entered=False;self._drained=False;self._failed=False;self._acquired=False

    def __enter__(self) -> Iterator[T]:
        if self._entered: raise OrmError('stream context cannot be reused')
        self._entered=True;self._owner=threading.get_ident()
        try:
            if self._db._owner is None: self._tx=self._db.begin()
            self._lease=self._db._use();self._lease.__enter__();self._acquired=True;self._db._stream_lease=True
            self._token=self._db._tx_token;self._open=True
            if self._tx is not None:
                with self._db._conn.cursor() as control: control.execute('SET TRANSACTION READ ONLY')
            self._cursor=self._db._conn.cursor(name='neutron_stream_'+uuid4().hex,scrollable=False,withhold=False)
            self._cursor.execute(self._compiled.sql,native_params(self._compiled.params))
            return self
        except BaseException as exc:
            if self._acquired or self._tx is not None:
                self._failed=True;self._db._rollback_only=True
                try: self._finish(False)
                except BaseException as cleanup:
                    if isinstance(exc,KeyboardInterrupt): raise exc from cleanup
                    raise
            if isinstance(exc,Exception) and not isinstance(exc,(OrmError,ValueError)): raise _native(exc) from exc
            raise

    def _guard(self) -> None:
        if threading.get_ident()!=self._owner: raise SessionBusyError('stream belongs to another thread')
        if self._failed or not self._open or self._db.closed or self._token is not self._db._tx_token:
            raise OrmError('stream scope is terminal')

    def __next__(self) -> T:
        self._guard()
        if self._drained: raise StopIteration
        try:
            if not self._buffer:
                rows=self._cursor.fetchmany(self._batch_size)
                if len(rows)>self._batch_size: raise OrmError('native cursor exceeded batch bound')
                self._buffer.extend(rows)
                if not rows: self._drained=True;raise StopIteration
            return self._compiled.decode(self._buffer.popleft())
        except StopIteration:
            if not self._drained:
                self._failed=True;self._db._rollback_only=True
                raise OrmError('stream decoder cannot end iteration')
            raise
        except BaseException as exc:
            self._failed=True;self._db._rollback_only=True
            if isinstance(exc,Exception) and not isinstance(exc,(OrmError,ValueError,TypeError)): raise _native(exc) from exc
            raise

    def _finish(self,commit: bool) -> None:
        error: BaseException | None=None
        deadline=time.monotonic()+CLEANUP_SECONDS
        try:
            if self._cursor is not None and not self._db.closed and self._token is self._db._tx_token:
                _sync_cleanup(self._db,self._cursor.close,deadline-time.monotonic())
        except BaseException as exc: error=exc
        finally:
            self._open=False;self._buffer.clear()
            if self._acquired:
                self._acquired=False;self._db._stream_lease=False;self._lease.__exit__(None,None,None);self._lease=None
        if self._tx is not None:
            tx=self._tx;self._tx=None
            try:
                if self._db.closed: tx.rollback()  # bookkeeping only: fenced transaction does no native I/O
                else: _sync_cleanup(self._db,tx.commit if commit and error is None and not self._failed else tx.rollback,deadline-time.monotonic())
            except BaseException as exc:
                error=exc
                if self._db.closed and tx.state=='active': tx.rollback()
        if error is not None:
            if isinstance(error,Exception) and not isinstance(error,OrmError): raise _native(error) from error
            raise error

    def __exit__(self,kind: Any,error: Any,traceback: Any) -> None:
        if threading.get_ident()!=self._owner: raise SessionBusyError('stream belongs to another thread')
        if error is not None: self._failed=True;self._db._rollback_only=True
        try: self._finish(error is None and self._drained and not self._failed)
        except BaseException as cleanup:
            if isinstance(error,KeyboardInterrupt): raise error from cleanup
            raise


class AsyncStream(Generic[T],AsyncIterator[T]):
    def __init__(self,db: AsyncDatabase,query: Select[T] | Query[T],batch_size: int) -> None:
        _validate(query,batch_size)
        self._db=db;self._compiled=query.compile();self._batch_size=batch_size
        self._buffer: deque[Any]=deque();self._cursor: Any=None;self._tx: Any=None;self._lease: Any=None
        self._token: object | None=None;self._owner: asyncio.Task[Any] | None=None
        self._open=False;self._entered=False;self._drained=False;self._failed=False;self._acquired=False

    async def __aenter__(self) -> AsyncIterator[T]:
        if self._entered: raise OrmError('stream context cannot be reused')
        self._entered=True;self._owner=asyncio.current_task()
        if self._owner is None: raise SessionBusyError('stream requires owning task')
        try:
            if self._db._owner is None: self._tx=await self._db.begin()
            self._lease=self._db._use();await self._lease.__aenter__();self._acquired=True;self._db._stream_lease=True
            self._token=self._db._tx_token;self._open=True
            if self._tx is not None:
                async with self._db._conn.cursor() as control: await control.execute('SET TRANSACTION READ ONLY')
            self._cursor=self._db._conn.cursor(name='neutron_stream_'+uuid4().hex,scrollable=False,withhold=False)
            await self._cursor.execute(self._compiled.sql,native_params(self._compiled.params))
            return self
        except BaseException as exc:
            if self._acquired or self._tx is not None:
                self._failed=True;self._db._rollback_only=True
                try: await self._finish(False)
                except BaseException as cleanup:
                    if isinstance(exc,asyncio.CancelledError): raise exc from cleanup
                    raise
            if isinstance(exc,Exception) and not isinstance(exc,(OrmError,ValueError)): raise _native(exc) from exc
            raise

    def _guard(self) -> None:
        if asyncio.current_task() is not self._owner: raise SessionBusyError('stream belongs to another task')
        if self._failed or not self._open or self._db.closed or self._token is not self._db._tx_token:
            raise OrmError('stream scope is terminal')

    async def __anext__(self) -> T:
        self._guard()
        if self._drained: raise StopAsyncIteration
        try:
            if not self._buffer:
                rows=await self._cursor.fetchmany(self._batch_size)
                if len(rows)>self._batch_size: raise OrmError('native cursor exceeded batch bound')
                self._buffer.extend(rows)
                if not rows: self._drained=True;raise StopAsyncIteration
            return self._compiled.decode(self._buffer.popleft())
        except StopAsyncIteration:
            if not self._drained:
                self._failed=True;self._db._rollback_only=True
                raise OrmError('stream decoder cannot end iteration')
            raise
        except BaseException as exc:
            self._failed=True;self._db._rollback_only=True
            if isinstance(exc,Exception) and not isinstance(exc,(OrmError,ValueError,TypeError)): raise _native(exc) from exc
            raise

    async def _finish(self,commit: bool) -> None:
        error: BaseException | None=None
        deadline=time.monotonic()+CLEANUP_SECONDS
        try:
            if self._cursor is not None and not self._db.closed and self._token is self._db._tx_token:
                await _async_cleanup(self._db,self._cursor.close,deadline-time.monotonic())
        except BaseException as exc: error=exc
        finally:
            self._open=False;self._buffer.clear()
            if self._acquired:
                self._acquired=False;self._db._stream_lease=False;await self._lease.__aexit__(None,None,None);self._lease=None
        if self._tx is not None:
            tx=self._tx;self._tx=None
            try:
                if self._db.closed: await tx.rollback()  # bookkeeping only: no native await after fencing
                else: await _async_cleanup(self._db,tx.commit if commit and error is None and not self._failed else tx.rollback,deadline-time.monotonic())
            except BaseException as exc:
                error=exc
                if self._db.closed and tx.state=='active': await tx.rollback()
        if error is not None:
            if isinstance(error,Exception) and not isinstance(error,OrmError): raise _native(error) from error
            raise error

    async def __aexit__(self,kind: Any,error: Any,traceback: Any) -> None:
        if asyncio.current_task() is not self._owner: raise SessionBusyError('stream belongs to another task')
        if error is not None: self._failed=True;self._db._rollback_only=True
        try: await self._finish(error is None and self._drained and not self._failed)
        except BaseException as cleanup:
            if isinstance(error,asyncio.CancelledError): raise error from cleanup
            raise
