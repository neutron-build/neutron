"""Request-owned Sessions with finite admission and explicit shutdown draining."""
from __future__ import annotations
import asyncio
from contextlib import asynccontextmanager,contextmanager
import threading
from typing import AsyncIterator,Iterator
from .async_session import AsyncSession
from .client import AsyncDatabase
from .core import OrmError
from .observability import QueryObserver
from .session import Session
from .streaming import _sync_cleanup


def _seconds(value: float) -> None:
    if type(value) not in {float,int} or not 0<value<=60: raise ValueError('finite positive lifecycle deadline up to 60 seconds required')

class SessionRequests:
    """Thread-owned sync requests; the ASGI/WSGI server drains request workers."""
    def __init__(self,url: str,*,observer: QueryObserver|None=None,max_active: int=32,cleanup_seconds: float=5.0) -> None:
        if type(max_active) is not int or not 0<max_active<=4096: raise ValueError('finite request admission capacity required')
        _seconds(cleanup_seconds)
        self._url=url;self._observer=observer;self._max=max_active;self._cleanup=cleanup_seconds
        self._lock=threading.Lock();self._idle=threading.Event();self._idle.set();self._active: set[int]=set();self._stopping=False
    @contextmanager
    def session(self) -> Iterator[Session]:
        owner=threading.get_ident()
        with self._lock:
            if self._stopping or owner in self._active or len(self._active)>=self._max: raise OrmError('request Session admission refused')
            self._active.add(owner);self._idle.clear()
        session=None;body_error: BaseException|None=None
        try:
            session=Session.connect(self._url,observer=self._observer)
            with self._lock:
                if self._stopping: raise OrmError('request factory stopped during connect')
            yield session
            session.commit()
        except BaseException as exc:
            body_error=exc;raise
        finally:
            try:
                if session is not None:
                    try: _sync_cleanup(session._database,session.close,self._cleanup)
                    except BaseException as cleanup:
                        if body_error is not None and not isinstance(body_error,Exception): raise body_error from cleanup
                        if isinstance(cleanup,OrmError) or not isinstance(cleanup,Exception): raise
                        raise OrmError('request cleanup failed') from cleanup
            finally:
                with self._lock:
                    self._active.remove(owner)
                    if not self._active: self._idle.set()
    def shutdown(self,*,grace_seconds: float=5.0) -> None:
        _seconds(grace_seconds)
        with self._lock:
            if threading.get_ident() in self._active: raise OrmError('request cannot shut down its own Session factory')
            self._stopping=True
        if not self._idle.wait(grace_seconds): raise OrmError('sync request workers did not drain before shutdown deadline')

class AsyncSessionRequests:
    """One Session per owning request task; shared state contains no model cache."""
    def __init__(self,url: str,*,observer: QueryObserver|None=None,max_active: int=32,cleanup_seconds: float=5.0) -> None:
        if type(max_active) is not int or not 0<max_active<=4096: raise ValueError('finite request admission capacity required')
        _seconds(cleanup_seconds)
        self._url=url;self._observer=observer;self._max=max_active;self._cleanup=cleanup_seconds
        self._loop=asyncio.get_running_loop();self._active: dict[asyncio.Task[object],AsyncDatabase|None]={}
        self._idle=asyncio.Event();self._idle.set();self._stopping=False
    def _owner(self) -> asyncio.Task[object]:
        if asyncio.get_running_loop() is not self._loop: raise OrmError('request factory belongs to another event loop')
        task=asyncio.current_task()
        if task is None: raise OrmError('request Session needs an owning task')
        return task
    @property
    def active_count(self) -> int:
        self._owner();return len(self._active)
    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        owner=self._owner()
        if self._stopping or owner in self._active or len(self._active)>=self._max: raise OrmError('request Session admission refused')
        self._active[owner]=None;self._idle.clear();session=None;db=None;body_error: BaseException|None=None
        try:
            db=await AsyncDatabase.connect(self._url,observer=self._observer);self._active[owner]=db
            session=AsyncSession(db,close_database=True)
            if self._stopping: raise OrmError('request factory stopped during connect')
            yield session
            await session.commit()
        except BaseException as exc:
            body_error=exc;raise
        finally:
            try:
                if session is not None:
                    try:
                        async with asyncio.timeout(self._cleanup): await session.close()
                    except BaseException as cleanup:
                        assert db is not None
                        db._discard()
                        if body_error is not None and not isinstance(body_error,Exception): raise body_error from cleanup
                        if isinstance(cleanup,OrmError) or not isinstance(cleanup,Exception): raise
                        raise OrmError('request cleanup failed') from cleanup
                elif db is not None: db._discard()
            finally:
                self._active.pop(owner)
                if not self._active: self._idle.set()
    def _fence_active(self) -> None:
        for task,db in tuple(self._active.items()):
            if db is not None: db._discard()
            task.cancel()
    async def shutdown(self,*,grace_seconds: float=5.0) -> None:
        _seconds(grace_seconds);owner=self._owner()
        if owner in self._active: raise OrmError('request cannot shut down its own Session factory')
        self._stopping=True
        try:
            try:
                async with asyncio.timeout(grace_seconds): await self._idle.wait()
                return
            except TimeoutError: pass
            tasks=tuple(self._active)
            for task in tasks: task.cancel()
            if tasks:
                done,pending=await asyncio.wait(tasks,timeout=self._cleanup)
                failed=sum(1 for task in done if not task.cancelled() and task.exception() is not None) # retrieve failures without rendering private causes
                if pending:
                    self._fence_active();raise OrmError('async requests exhausted shutdown cleanup deadline')
                if failed: raise OrmError(f'{failed} async request cleanups failed during shutdown')
            if self._active: raise OrmError('async request cleanup did not release ownership')
        except BaseException:
            self._fence_active();raise
