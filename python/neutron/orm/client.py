"""Native synchronous/asynchronous psycopg connections with explicit ownership."""
from __future__ import annotations
import asyncio
from contextlib import contextmanager, asynccontextmanager
import threading
from typing import Any, AsyncIterator, Iterator, Mapping, Sequence, TypeVar, TYPE_CHECKING, cast
from .endpoint import EndpointIdentity, admit, startup_version
from .json_value import load_document, native_params
from .query import Query
from .sql_admission import validate_scope_sql
from .vector_value import PgVector,VECTOR_SQL,admitted_vector,register_vector
from .composite_value import PgComposite,COMPOSITE_SQL,admitted_components,register_composite
from .catalog_value import CatalogType, PgDomain, PgEnum, TYPE_SQL, TABLE_SQL, admitted_type, register_catalog_values
from .core import ColumnSpec, Table, quote, CardinalityError, Compiled, Mutation, OrmError, Returning, Select, SessionBusyError

if TYPE_CHECKING:
    from .lifecycle import AsyncTransactionHandle, TransactionHandle
    from .streaming import AsyncStream, Stream

T=TypeVar('T')

def _decode_rows(query: Select[T] | Returning[T] | Query[T], compiled: Compiled[T], rows: Sequence[Mapping[str,Any]], cardinality: str) -> list[T]:
    if cardinality!='many' and len(rows)>1: raise CardinalityError('expected at most one row')
    if cardinality=='one' and not rows: raise CardinalityError('expected exactly one row')
    return [compiled.decode(row) for row in rows]


def _check_result_oids(compiled: Compiled[Any],cursor: Any) -> None:
    actual=tuple((item.name,item.type_code) for item in cursor.description or ())
    if actual!=compiled.result_oids: raise ValueError('native result SQL type/OID identity mismatch')


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


def _admit_native_root(connection: Any) -> None:
    if getattr(connection,'autocommit',None) is False:
        raise OrmError('native ORM root transactions require autocommit=True')
    status=getattr(getattr(connection,'info',None),'transaction_status',None)
    if status is not None and status!=0 and getattr(status,'name',None)!='IDLE':
        raise OrmError('native connection is not IDLE; external transaction adoption refused')


class Database:
    """One native connection. Concurrent active use rejected; no tracked objects."""
    def __init__(self, connection: Any) -> None:
        self._catalog_owner=object()
        self._catalog_specs: dict[tuple[str,str],ColumnSpec[Any]]={}
        self._native_binary=False
        self._endpoint_identity: EndpointIdentity | None = None
        self._conn=connection
        self._lock=threading.Lock()
        self._closed=False
        self._rollback_only=False
        self._stream_lease=False
        self._savepoint_depth=0
        self._tx_token: object | None=None
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
            from .pg_adapters import register_native_values
            register_native_values(connection)
            result=cls(connection)
            result._native_binary=True
            result._endpoint_identity=identity
            return result
        except BaseException as exc:
            if connection is not None: connection.pgconn.finish()
            if isinstance(exc,(OrmError,KeyboardInterrupt,SystemExit)): raise
            if not isinstance(exc,Exception): raise
            raise OrmError("Unable to connect to PostgreSQL",sqlstate=getattr(exc,"sqlstate",None)) from exc

    def _catalog_read(self,sql: str,params: tuple[object,...]) -> list[dict[str,Any]]:
        if not self._native_binary: raise OrmError('catalog admission requires a native connect client')
        with self._use():
            try:
                with self._conn.cursor() as cur:
                    cur.execute(sql,params);return list(cur.fetchall())
            except BaseException as exc:
                if self._owner is not None: self._rollback_only=True
                if not isinstance(exc,Exception): raise
                state=getattr(exc,'sqlstate',None)
                if state is None or str(state).startswith('08'): self._discard()
                raise _native(exc) from exc

    def enum_spec(self,schema: str,name: str,*,nullable: bool=False,generated: bool=False) -> ColumnSpec[PgEnum]:
        quote(schema);quote(name)
        identity=admitted_type(self._catalog_read(TYPE_SQL,(schema,name)),schema,name,'e',self._catalog_owner)
        cached=self._catalog_specs.get((schema,name))
        if cached is None or cached.native_type!=identity:
            register_catalog_values(self._conn,self._catalog_owner,identity)
            cached=ColumnSpec(PgEnum,'enum',native_type=identity);self._catalog_specs[(schema,name)]=cached
        from dataclasses import replace
        return cast(ColumnSpec[PgEnum],replace(cached,nullable=nullable,generated=generated))

    def vector_spec(self,schema: str,name: str='vector',*,nullable: bool=False,generated: bool=False) -> ColumnSpec[PgVector]:
        quote(schema);quote(name)
        identity=admitted_type(self._catalog_read(TYPE_SQL,(schema,name)),schema,name,'b',self._catalog_owner)
        admitted_vector(self._catalog_read(VECTOR_SQL,(identity.oid,)),identity)
        cached=self._catalog_specs.get((schema,name))
        if cached is None or cached.native_type!=identity:
            register_vector(self._conn,self._catalog_owner,identity)
            cached=ColumnSpec(PgVector,'vector',native_type=identity);self._catalog_specs[(schema,name)]=cached
        from dataclasses import replace
        return cast(ColumnSpec[PgVector],replace(cached,nullable=nullable,generated=generated))

    def composite_spec(self,schema: str,name: str,fields: Mapping[str,ColumnSpec[Any]],*,nullable: bool=False,generated: bool=False) -> ColumnSpec[PgComposite]:
        quote(schema);quote(name)
        identity=admitted_type(self._catalog_read(TYPE_SQL,(schema,name)),schema,name,'c',self._catalog_owner)
        components=admitted_components(self._catalog_read(COMPOSITE_SQL,(identity.oid,)),fields)
        cached=self._catalog_specs.get((schema,name))
        if cached is None or cached.native_type!=identity:
            register_composite(self._conn,self._catalog_owner,identity,components)
            cached=ColumnSpec(PgComposite,'composite',native_type=identity,composite_fields=components);self._catalog_specs[(schema,name)]=cached
        elif cached.composite_fields!=components: raise ValueError('composite component profile changed; reconstruct metadata')
        from dataclasses import replace
        return cast(ColumnSpec[PgComposite],replace(cached,nullable=nullable,generated=generated))

    def domain_spec(self,schema: str,name: str,base: ColumnSpec[T],*,nullable: bool=False,generated: bool=False) -> ColumnSpec[PgDomain[T]]:
        quote(schema);quote(name)
        identity=admitted_type(self._catalog_read(TYPE_SQL,(schema,name)),schema,name,'d',self._catalog_owner)
        if base.type_oid!=identity.base_oid or base.native_type is not None and (base.native_type.kind!='e' or base.native_type._owner is not self._catalog_owner): raise ValueError('domain declared base SQL type/OID mismatch')
        cached=self._catalog_specs.get((schema,name))
        if cached is None or cached.native_type!=identity:
            register_catalog_values(self._conn,self._catalog_owner,identity)
            cached=ColumnSpec(PgDomain,'domain',native_type=identity,domain_base=base);self._catalog_specs[(schema,name)]=cached
        elif cached.domain_base!=base: raise ValueError('domain base profile changed; reconstruct metadata')
        from dataclasses import replace
        return cast(ColumnSpec[PgDomain[T]],replace(cached,nullable=nullable,generated=generated))

    def catalog_table(self,name: str,columns: Mapping[str,ColumnSpec[Any]],*,schema: str='public') -> Table:
        quote(schema);quote(name)
        actual={row['name']:row for row in self._catalog_read(TABLE_SQL,(schema,name))}
        for column,spec in columns.items():
            row=actual.get(column)
            if row is None or row['oid']!=spec.type_oid: raise ValueError('catalog table column SQL type/OID mismatch')
            if not spec.nullable and not row['required'] and not (spec.native_type is not None and spec.native_type.required): raise ValueError('catalog nonnullable column requires native NOT NULL')
        return Table(name,columns,schema=schema,_catalog_owner=self._catalog_owner)

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
        if compiled.catalog_owner is not None and compiled.catalog_owner is not self._catalog_owner: raise ValueError('catalog query belongs to another connection')
        validate_scope_sql(compiled.sql)
        with self._use():
            try:
                with self._conn.cursor(**({'binary':True} if self._native_binary else {})) as cur:
                    cur.execute(compiled.sql,native_params(compiled.params))
                    if self._native_binary: _check_result_oids(compiled,cur)
                    rows=cur.fetchall() if cardinality=='many' else cur.fetchmany(2)
            except BaseException as exc:
                if self._owner is not None: self._rollback_only=True
                if not isinstance(exc,Exception): raise
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
        if statement.table is not None and statement.table._catalog_owner is not None and statement.table._catalog_owner is not self._catalog_owner: raise ValueError('catalog mutation belongs to another connection')
        validate_scope_sql(statement.sql,owned=self._owner is not None)
        with self._use():
            try:
                with self._conn.cursor(**({'binary':True} if self._native_binary else {})) as cur:
                    cur.execute(statement.sql,native_params(statement.params))
                    return int(cur.rowcount)
            except BaseException as exc:
                if self._owner is not None: self._rollback_only=True
                if not isinstance(exc,Exception): raise
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
        with self._use():
            _admit_native_root(self._conn)
            self._owner=threading.get_ident();self._tx_token=object()
        try:
            try:
                native=self._conn.transaction()
                native.__enter__()
            except Exception as exc:
                self._discard(); raise _native(exc) from exc
            try:
                yield self
                if self._stream_lease or self._savepoint_depth:
                    self._discard();raise OrmError('transaction ended with an active stream lease or savepoint',outcome='aborted')
                if self._rollback_only: raise OrmError("transaction requires rollback after dispatched failure or invalid RETURNING result")
            except BaseException as body:
                if self._closed: raise
                if self._stream_lease or self._savepoint_depth:
                    self._discard();raise OrmError('transaction ended with an active stream lease or savepoint',outcome='aborted') from body
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
            self._tx_token=None
            self._rollback_only=False

    @contextmanager
    def savepoint(self) -> Iterator[Database]:
        if self._owner is None: raise OrmError('savepoint requires an owned transaction')
        with self._use():
            native=self._conn.transaction()
            try: native.__enter__()
            except BaseException as exc:
                self._discard()
                if isinstance(exc,Exception) and not isinstance(exc,OrmError): raise _native(exc) from exc
                raise
        previous=self._rollback_only;self._savepoint_depth+=1
        try:
            try:
                yield self
                if self._rollback_only: raise OrmError('savepoint requires rollback')
            except BaseException as body:
                if self.closed: raise
                try:
                    with self._use_cleanup(): native.__exit__(type(body),body,body.__traceback__)
                except BaseException as exc:
                    self._discard()
                    if isinstance(exc,Exception) and not isinstance(exc,OrmError): raise _native(exc) from exc
                    raise
                raise
            else:
                try:
                    with self._use(): native.__exit__(None,None,None)
                except BaseException as exc:
                    self._discard()
                    if isinstance(exc,Exception) and not isinstance(exc,OrmError): raise _native(exc) from exc
                    raise
        finally:
            self._savepoint_depth-=1;self._rollback_only=previous

    @contextmanager
    def _use_cleanup(self) -> Iterator[None]:
        previous=self._rollback_only;self._rollback_only=False
        try:
            with self._use(): yield
        finally: self._rollback_only=previous

    def stream(self,query: Select[T] | Query[T],*,batch_size: int) -> Stream[T]:
        from .streaming import Stream
        return Stream(self,query,batch_size)

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
        self._catalog_owner=object()
        self._catalog_specs: dict[tuple[str,str],ColumnSpec[Any]]={}
        self._native_binary=False
        self._endpoint_identity: EndpointIdentity | None = None
        self._conn=connection
        self._busy=False
        self._closed=False
        self._rollback_only=False
        self._stream_lease=False
        self._savepoint_depth=0
        self._tx_token: object | None=None
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
            from .pg_adapters import register_native_values
            register_native_values(connection)
            result=cls(connection)
            result._native_binary=True
            result._endpoint_identity=identity
            return result
        except BaseException as exc:
            if connection is not None: connection.pgconn.finish()
            if isinstance(exc,OrmError) or not isinstance(exc,Exception): raise
            raise OrmError("Unable to connect to PostgreSQL",sqlstate=getattr(exc,"sqlstate",None)) from exc

    async def _catalog_read(self,sql: str,params: tuple[object,...]) -> list[dict[str,Any]]:
        if not self._native_binary: raise OrmError('catalog admission requires a native connect client')
        async with self._use():
            try:
                async with self._conn.cursor() as cur:
                    await cur.execute(sql,params);return list(await cur.fetchall())
            except asyncio.CancelledError:
                if self._owner is not None: self._rollback_only=True
                task=asyncio.current_task()
                if self._owner is None and task is not None and task.cancelling()>1: self._discard()
                raise
            except BaseException as exc:
                if self._owner is not None: self._rollback_only=True
                if not isinstance(exc,Exception): raise
                state=getattr(exc,'sqlstate',None)
                if state is None or str(state).startswith('08'): self._discard()
                raise _native(exc) from exc

    async def enum_spec(self,schema: str,name: str,*,nullable: bool=False,generated: bool=False) -> ColumnSpec[PgEnum]:
        quote(schema);quote(name)
        identity=admitted_type(await self._catalog_read(TYPE_SQL,(schema,name)),schema,name,'e',self._catalog_owner)
        cached=self._catalog_specs.get((schema,name))
        if cached is None or cached.native_type!=identity:
            register_catalog_values(self._conn,self._catalog_owner,identity)
            cached=ColumnSpec(PgEnum,'enum',native_type=identity);self._catalog_specs[(schema,name)]=cached
        from dataclasses import replace
        return cast(ColumnSpec[PgEnum],replace(cached,nullable=nullable,generated=generated))

    async def vector_spec(self,schema: str,name: str='vector',*,nullable: bool=False,generated: bool=False) -> ColumnSpec[PgVector]:
        quote(schema);quote(name)
        identity=admitted_type(await self._catalog_read(TYPE_SQL,(schema,name)),schema,name,'b',self._catalog_owner)
        admitted_vector(await self._catalog_read(VECTOR_SQL,(identity.oid,)),identity)
        cached=self._catalog_specs.get((schema,name))
        if cached is None or cached.native_type!=identity:
            register_vector(self._conn,self._catalog_owner,identity)
            cached=ColumnSpec(PgVector,'vector',native_type=identity);self._catalog_specs[(schema,name)]=cached
        from dataclasses import replace
        return cast(ColumnSpec[PgVector],replace(cached,nullable=nullable,generated=generated))

    async def composite_spec(self,schema: str,name: str,fields: Mapping[str,ColumnSpec[Any]],*,nullable: bool=False,generated: bool=False) -> ColumnSpec[PgComposite]:
        quote(schema);quote(name)
        identity=admitted_type(await self._catalog_read(TYPE_SQL,(schema,name)),schema,name,'c',self._catalog_owner)
        components=admitted_components(await self._catalog_read(COMPOSITE_SQL,(identity.oid,)),fields)
        cached=self._catalog_specs.get((schema,name))
        if cached is None or cached.native_type!=identity:
            register_composite(self._conn,self._catalog_owner,identity,components)
            cached=ColumnSpec(PgComposite,'composite',native_type=identity,composite_fields=components);self._catalog_specs[(schema,name)]=cached
        elif cached.composite_fields!=components: raise ValueError('composite component profile changed; reconstruct metadata')
        from dataclasses import replace
        return cast(ColumnSpec[PgComposite],replace(cached,nullable=nullable,generated=generated))

    async def domain_spec(self,schema: str,name: str,base: ColumnSpec[T],*,nullable: bool=False,generated: bool=False) -> ColumnSpec[PgDomain[T]]:
        quote(schema);quote(name)
        identity=admitted_type(await self._catalog_read(TYPE_SQL,(schema,name)),schema,name,'d',self._catalog_owner)
        if base.type_oid!=identity.base_oid or base.native_type is not None and (base.native_type.kind!='e' or base.native_type._owner is not self._catalog_owner): raise ValueError('domain declared base SQL type/OID mismatch')
        cached=self._catalog_specs.get((schema,name))
        if cached is None or cached.native_type!=identity:
            register_catalog_values(self._conn,self._catalog_owner,identity)
            cached=ColumnSpec(PgDomain,'domain',native_type=identity,domain_base=base);self._catalog_specs[(schema,name)]=cached
        elif cached.domain_base!=base: raise ValueError('domain base profile changed; reconstruct metadata')
        from dataclasses import replace
        return cast(ColumnSpec[PgDomain[T]],replace(cached,nullable=nullable,generated=generated))

    async def catalog_table(self,name: str,columns: Mapping[str,ColumnSpec[Any]],*,schema: str='public') -> Table:
        quote(schema);quote(name)
        actual={row['name']:row for row in await self._catalog_read(TABLE_SQL,(schema,name))}
        for column,spec in columns.items():
            row=actual.get(column)
            if row is None or row['oid']!=spec.type_oid: raise ValueError('catalog table column SQL type/OID mismatch')
            if not spec.nullable and not row['required'] and not (spec.native_type is not None and spec.native_type.required): raise ValueError('catalog nonnullable column requires native NOT NULL')
        return Table(name,columns,schema=schema,_catalog_owner=self._catalog_owner)

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
        if compiled.catalog_owner is not None and compiled.catalog_owner is not self._catalog_owner: raise ValueError('catalog query belongs to another connection')
        validate_scope_sql(compiled.sql)
        async with self._use():
            try:
                async with self._conn.cursor(**({'binary':True} if self._native_binary else {})) as cur:
                    await cur.execute(compiled.sql,native_params(compiled.params))
                    if self._native_binary: _check_result_oids(compiled,cur)
                    rows=await cur.fetchall() if cardinality=='many' else await cur.fetchmany(2)
            except asyncio.CancelledError:
                if self._owner is not None: self._rollback_only=True
                task=asyncio.current_task()
                if self._owner is None and task is not None and task.cancelling()>1: self._discard()
                raise
            except Exception as exc:
                if self._owner is not None: self._rollback_only=True
                state=getattr(exc,"sqlstate",None)
                if state is None or str(state).startswith("08"): self._discard()
                raise _native(exc) from exc
            except BaseException:
                if self._owner is not None: self._rollback_only=True
                raise
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
        if statement.table is not None and statement.table._catalog_owner is not None and statement.table._catalog_owner is not self._catalog_owner: raise ValueError('catalog mutation belongs to another connection')
        validate_scope_sql(statement.sql,owned=self._owner is not None)
        async with self._use():
            try:
                async with self._conn.cursor(**({'binary':True} if self._native_binary else {})) as cur:
                    await cur.execute(statement.sql,native_params(statement.params))
                    return int(cur.rowcount)
            except asyncio.CancelledError:
                if self._owner is not None: self._rollback_only=True
                task=asyncio.current_task()
                if self._owner is None and task is not None and task.cancelling()>1: self._discard()
                raise
            except Exception as exc:
                if self._owner is not None: self._rollback_only=True
                state=getattr(exc,"sqlstate",None)
                if state is None or str(state).startswith("08"): self._discard()
                raise _native(exc) from exc
            except BaseException:
                if self._owner is not None: self._rollback_only=True
                raise

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
        async with self._use():
            _admit_native_root(self._conn)
            self._owner=asyncio.current_task();self._tx_token=object()
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
                if self._stream_lease or self._savepoint_depth:
                    self._discard();raise OrmError('transaction ended with an active stream lease or savepoint',outcome='aborted')
                if self._rollback_only: raise OrmError("transaction requires rollback after dispatched failure or invalid RETURNING result")
            except BaseException as body:
                if self._closed: raise
                if self._stream_lease or self._savepoint_depth:
                    self._discard();raise OrmError('transaction ended with an active stream lease or savepoint',outcome='aborted') from body
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
            self._tx_token=None
            self._rollback_only=False

    @asynccontextmanager
    async def savepoint(self) -> AsyncIterator[AsyncDatabase]:
        if self._owner is None: raise OrmError('savepoint requires an owned transaction')
        async with self._use():
            native=self._conn.transaction()
            try: await native.__aenter__()
            except BaseException as exc:
                self._discard()
                if isinstance(exc,Exception) and not isinstance(exc,OrmError): raise _native(exc) from exc
                raise
        previous=self._rollback_only;self._savepoint_depth+=1
        try:
            try:
                yield self
                if self._rollback_only: raise OrmError('savepoint requires rollback')
            except BaseException as body:
                if self.closed: raise
                try:
                    async with self._use_cleanup(): await native.__aexit__(type(body),body,body.__traceback__)
                except BaseException as exc:
                    self._discard()
                    if isinstance(exc,Exception) and not isinstance(exc,OrmError): raise _native(exc) from exc
                    raise
                raise
            else:
                try:
                    async with self._use(): await native.__aexit__(None,None,None)
                except BaseException as exc:
                    self._discard()
                    if isinstance(exc,Exception) and not isinstance(exc,OrmError): raise _native(exc) from exc
                    raise
        finally:
            self._savepoint_depth-=1;self._rollback_only=previous

    @asynccontextmanager
    async def _use_cleanup(self) -> AsyncIterator[None]:
        previous=self._rollback_only;self._rollback_only=False
        try:
            async with self._use(): yield
        finally: self._rollback_only=previous

    def stream(self,query: Select[T] | Query[T],*,batch_size: int) -> AsyncStream[T]:
        from .streaming import AsyncStream
        return AsyncStream(self,query,batch_size)

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
