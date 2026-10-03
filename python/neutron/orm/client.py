"""Native synchronous/asynchronous psycopg connections with explicit ownership."""
from __future__ import annotations
import asyncio
from contextlib import contextmanager, asynccontextmanager
import threading
from typing import Any, AsyncIterator, Iterator, Mapping, Sequence, TypeVar, TYPE_CHECKING
from .endpoint import EndpointIdentity, admit, startup_version
from .json_value import load_document, native_params
from .query import Query
from .core import CardinalityError, Compiled, Mutation, OrmError, Returning, Select, SessionBusyError

if TYPE_CHECKING:
    from .lifecycle import AsyncTransactionHandle, TransactionHandle

T=TypeVar('T')

def _decode_rows(query: Select[T] | Returning[T] | Query[T], compiled: Compiled[T], rows: Sequence[Mapping[str,Any]], cardinality: str) -> list[T]:
    if cardinality!='many' and len(rows)>1: raise CardinalityError('expected at most one row')
    if cardinality=='one' and not rows: raise CardinalityError('expected exactly one row')
    return [compiled.decode(row) for row in rows]


class CommitCancelledError(asyncio.CancelledError):
    """Cancellation during COMMIT; database outcome may be committed."""
    outcome='indeterminate'
    sqlstate=None


def _profile(profile: str) -> None:
    if profile != 'postgres-direct': raise OrmError('unsupported/unknown execution profile; operation refused')


def _native(error: Exception, *, committing: bool = False) -> OrmError:
    # Native error text may carry parameter values; retain original in cause,
    # but do not reproduce it in a generic public message.
    state=getattr(error,'sqlstate',None)
    definite = isinstance(state,str) and state[:2] in {'22','23','25','2D','40','42'} and state != '40003'
    return OrmError('PostgreSQL operation failed',sqlstate=state,outcome=('aborted' if definite else 'indeterminate') if committing else None)


class Database:
    """One native connection. Concurrent active use rejected; no tracked objects."""
    def __init__(self, connection: Any) -> None:
        self._endpoint_identity: EndpointIdentity | None = None
        self._conn=connection
        self._lock=threading.Lock()
        self._closed=False
        self._rollback_only=False
        self._owner: int | None=None

    @property
    def endpoint_identity(self) -> EndpointIdentity | None:
        return self._endpoint_identity

    @classmethod
    def connect(cls, url: str, *, profile: str='postgres-direct') -> Database:
        _profile(profile)
        try:
            import psycopg
            from psycopg.rows import dict_row
        except ImportError as exc:
            raise OrmError("Install neutron-framework[orm] for native PostgreSQL execution") from exc
        connection=None
        try:
            connection=psycopg.connect(url,autocommit=True,row_factory=dict_row)
            startup=connection.info.parameter_status('server_version')
            startup_version(startup)
            with connection.cursor() as cur:
                cur.execute('SELECT pg_catalog.version() AS orm_endpoint_version')
                row=cur.fetchone()
            identity=admit(startup,row['orm_endpoint_version'] if row is not None else None)
            from psycopg.types.json import set_json_loads
            set_json_loads(load_document,context=connection)
            result=cls(connection)
            result._endpoint_identity=identity
            return result
        except BaseException as exc:
            if connection is not None: connection.pgconn.finish()
            if isinstance(exc,(OrmError,KeyboardInterrupt,SystemExit)): raise
            if not isinstance(exc,Exception): raise
            raise OrmError("Unable to connect to PostgreSQL",sqlstate=getattr(exc,"sqlstate",None)) from exc

    @contextmanager
    def _use(self) -> Iterator[None]:
        if self._closed: raise OrmError('connection closed')
        if self._rollback_only: raise OrmError('transaction requires rollback')
        if self._owner is not None and self._owner != threading.get_ident():
            raise SessionBusyError('transaction belongs to another thread')
        if not self._lock.acquire(blocking=False): raise SessionBusyError('concurrent active session use refused')
        try:
            if self._closed: raise OrmError("connection closed")
            yield
        finally: self._lock.release()

    def _read(self, query: Select[T] | Returning[T] | Query[T], cardinality: str) -> list[T]:
        if isinstance(query,Returning) and self._owner is None:
            with self.transaction(): return self._read(query,cardinality)
        if isinstance(query,Query) and cardinality != 'many' and (query.row_limit is not None or query.row_offset is not None):
            raise ValueError('exact-one reads refuse pagination')
        compiled=query.compile()
        with self._use():
            try:
                with self._conn.cursor() as cur:
                    cur.execute(compiled.sql,native_params(compiled.params))
                    rows=cur.fetchall() if cardinality=='many' else cur.fetchmany(2)
            except Exception as exc:
                state=getattr(exc,"sqlstate",None)
                if state is None or str(state).startswith("08"): self._discard()
                raise _native(exc) from exc
        return self._decode(query,compiled,rows,cardinality)

    def _decode(self,query: Select[T] | Returning[T] | Query[T],compiled: Compiled[T],rows: Sequence[Mapping[str,Any]],cardinality: str) -> list[T]:
        try: return _decode_rows(query,compiled,rows,cardinality)
        except Exception:
            if isinstance(query,Returning): self._rollback_only=True
            raise

    def all(self, query: Select[T] | Returning[T] | Query[T]) -> list[T]: return self._read(query,'many')
    def one(self, query: Select[T] | Returning[T] | Query[T]) -> T: return self._read(query,'one')[0]
    def one_or_none(self, query: Select[T] | Returning[T] | Query[T]) -> T | None:
        rows=self._read(query,'optional'); return rows[0] if rows else None

    def execute(self, statement: Mutation) -> int:
        with self._use():
            try:
                with self._conn.cursor() as cur:
                    cur.execute(statement.sql,native_params(statement.params))
                    return int(cur.rowcount)
            except Exception as exc:
                state=getattr(exc,"sqlstate",None)
                if state is None or str(state).startswith("08"): self._discard()
                raise _native(exc) from exc

    @property
    def closed(self) -> bool: return self._closed

    def _discard(self) -> None:
        self._closed=True
        try: self._conn.close()
        except Exception: pass  # preserve the lifecycle failure, never reuse

    @contextmanager
    def transaction(self) -> Iterator[Database]:
        if self._owner is not None: raise SessionBusyError('nested transaction unsupported in this slice')
        with self._use(): self._owner=threading.get_ident()
        try:
            try:
                native=self._conn.transaction()
                native.__enter__()
            except Exception as exc:
                self._discard(); raise _native(exc) from exc
            try:
                yield self
                if self._rollback_only: raise OrmError("transaction requires rollback after invalid RETURNING result")
            except BaseException as body:
                try: native.__exit__(type(body),body,body.__traceback__)
                except BaseException as cleanup:
                    self._discard()
                    if isinstance(cleanup,Exception): raise _native(cleanup) from cleanup
                    raise
                if isinstance(body,OrmError) and body.outcome is None: body.outcome="aborted"
                raise
            else:
                try: native.__exit__(None,None,None)
                except BaseException as commit:
                    self._discard()
                    if isinstance(commit,asyncio.CancelledError):
                        raise CommitCancelledError() from commit
                    if isinstance(commit,Exception): raise _native(commit,committing=True) from commit
                    raise
        finally:
            self._owner=None
            self._rollback_only=False

    def begin(self) -> TransactionHandle:
        from .lifecycle import TransactionHandle
        return TransactionHandle(self)

    def close(self) -> None:
        if self._closed: return
        if self._owner is not None: raise SessionBusyError('close during active transaction refused')
        with self._use():
            self._closed=True
            try: self._conn.close()
            except Exception as exc:
                state=getattr(exc,"sqlstate",None)
                if state is None or str(state).startswith("08"): self._discard()
                raise _native(exc) from exc

    def __enter__(self) -> Database: return self
    def __exit__(self, *_: object) -> None: self.close()


class AsyncDatabase:
    """One native async connection; operation/transaction task ownership explicit."""
    def __init__(self, connection: Any) -> None:
        self._endpoint_identity: EndpointIdentity | None = None
        self._conn=connection
        self._busy=False
        self._closed=False
        self._rollback_only=False
        self._owner: asyncio.Task[Any] | None=None

    @property
    def endpoint_identity(self) -> EndpointIdentity | None:
        return self._endpoint_identity

    @classmethod
    async def connect(cls,url: str,*,profile: str='postgres-direct') -> AsyncDatabase:
        _profile(profile)
        try:
            import psycopg
            from psycopg.rows import dict_row
        except ImportError as exc:
            raise OrmError("Install neutron-framework[orm] for native PostgreSQL execution") from exc
        connection=None
        try:
            connection=await psycopg.AsyncConnection.connect(url,autocommit=True,row_factory=dict_row)
            startup=connection.info.parameter_status('server_version')
            startup_version(startup)
            async with connection.cursor() as cur:
                await cur.execute('SELECT pg_catalog.version() AS orm_endpoint_version')
                row=await cur.fetchone()
            identity=admit(startup,row['orm_endpoint_version'] if row is not None else None)
            from psycopg.types.json import set_json_loads
            set_json_loads(load_document,context=connection)
            result=cls(connection)
            result._endpoint_identity=identity
            return result
        except BaseException as exc:
            if connection is not None: connection.pgconn.finish()
            if isinstance(exc,OrmError) or not isinstance(exc,Exception): raise
            raise OrmError("Unable to connect to PostgreSQL",sqlstate=getattr(exc,"sqlstate",None)) from exc

    @asynccontextmanager
    async def _use(self) -> AsyncIterator[None]:
        if self._closed: raise OrmError('connection closed')
        if self._rollback_only: raise OrmError('transaction requires rollback')
        if self._owner is not None and self._owner is not asyncio.current_task(): raise SessionBusyError('transaction belongs to another task')
        if self._busy: raise SessionBusyError('concurrent active session use refused')
        self._busy=True
        try: yield
        finally: self._busy=False

    async def _read(self,query: Select[T] | Returning[T] | Query[T],cardinality: str) -> list[T]:
        if isinstance(query,Returning) and self._owner is None:
            async with self.transaction(): return await self._read(query,cardinality)
        if isinstance(query,Query) and cardinality != 'many' and (query.row_limit is not None or query.row_offset is not None):
            raise ValueError('exact-one reads refuse pagination')
        compiled=query.compile()
        async with self._use():
            try:
                async with self._conn.cursor() as cur:
                    await cur.execute(compiled.sql,native_params(compiled.params))
                    rows=await cur.fetchall() if cardinality=='many' else await cur.fetchmany(2)
            except asyncio.CancelledError:
                task=asyncio.current_task()
                if self._owner is None and task is not None and task.cancelling()>1: self._discard()
                raise
            except Exception as exc:
                state=getattr(exc,"sqlstate",None)
                if state is None or str(state).startswith("08"): self._discard()
                raise _native(exc) from exc
        return self._decode(query,compiled,rows,cardinality)

    def _decode(self,query: Select[T] | Returning[T] | Query[T],compiled: Compiled[T],rows: Sequence[Mapping[str,Any]],cardinality: str) -> list[T]:
        try: return _decode_rows(query,compiled,rows,cardinality)
        except Exception:
            if isinstance(query,Returning): self._rollback_only=True
            raise

    async def all(self,query: Select[T] | Returning[T] | Query[T]) -> list[T]: return await self._read(query,'many')
    async def one(self,query: Select[T] | Returning[T] | Query[T]) -> T: return (await self._read(query,'one'))[0]
    async def one_or_none(self,query: Select[T] | Returning[T] | Query[T]) -> T | None:
        rows=await self._read(query,'optional'); return rows[0] if rows else None

    async def execute(self,statement: Mutation) -> int:
        async with self._use():
            try:
                async with self._conn.cursor() as cur:
                    await cur.execute(statement.sql,native_params(statement.params))
                    return int(cur.rowcount)
            except asyncio.CancelledError:
                task=asyncio.current_task()
                if self._owner is None and task is not None and task.cancelling()>1: self._discard()
                raise
            except Exception as exc:
                state=getattr(exc,"sqlstate",None)
                if state is None or str(state).startswith("08"): self._discard()
                raise _native(exc) from exc

    @property
    def closed(self) -> bool: return self._closed

    def _discard(self) -> None:
        # Fencing is synchronous, before another cancellation can interrupt it.
        self._closed=True
        pgconn=getattr(self._conn,"pgconn",None)
        if pgconn is not None:
            try: pgconn.finish()
            except Exception: pass
        else:
            # Test/custom connections have no libpq handle. They stay fenced
            # while best-effort asynchronous disposal completes.
            task=asyncio.create_task(self._conn.close())
            def consume(done: asyncio.Task[Any]) -> None:
                if not done.cancelled(): done.exception()
            task.add_done_callback(consume)

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[AsyncDatabase]:
        if self._owner is not None: raise SessionBusyError('nested transaction unsupported in this slice')
        async with self._use(): self._owner=asyncio.current_task()
        try:
            try:
                native=self._conn.transaction()
                await native.__aenter__()
            except BaseException as entry:
                self._discard()
                if isinstance(entry,Exception): raise _native(entry) from entry
                raise
            try:
                yield self
                if self._rollback_only: raise OrmError("transaction requires rollback after invalid RETURNING result")
            except BaseException as body:
                try: await native.__aexit__(type(body),body,body.__traceback__)
                except BaseException as cleanup:
                    self._discard()
                    if isinstance(cleanup,Exception): raise _native(cleanup) from cleanup
                    raise
                if isinstance(body,OrmError) and body.outcome is None: body.outcome="aborted"
                raise
            else:
                try: await native.__aexit__(None,None,None)
                except BaseException as commit:
                    self._discard()
                    if isinstance(commit,asyncio.CancelledError):
                        raise CommitCancelledError() from commit
                    if isinstance(commit,Exception): raise _native(commit,committing=True) from commit
                    raise
        finally:
            self._owner=None
            self._rollback_only=False

    async def begin(self) -> AsyncTransactionHandle:
        from .lifecycle import AsyncTransactionHandle
        return await AsyncTransactionHandle(self).open()

    async def close(self) -> None:
        if self._closed: return
        if self._owner is not None: raise SessionBusyError('close during active transaction refused')
        async with self._use():
            self._closed=True
            try: await self._conn.close()
            except BaseException as cleanup:
                self._discard()
                if isinstance(cleanup,Exception): raise _native(cleanup) from cleanup
                raise

    async def __aenter__(self) -> AsyncDatabase: return self
    async def __aexit__(self,*_: object) -> None: await self.close()
