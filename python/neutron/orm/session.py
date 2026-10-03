"""Bounded synchronous scalar mapped Session, with explicit unsupported scopes."""
from __future__ import annotations
from contextlib import contextmanager
import threading
from typing import Any, Iterator, TypeVar
from .client import AsyncDatabase, Database
from .core import CardinalityError, OrmError, Predicate, SessionBusyError, delete, insert, select_row, update
from .lifecycle import TransactionHandle
from .mapping import ModelMapping
from .state import ObjectState, Record, StateStore
T=TypeVar('T')

class ConflictError(OrmError): pass

class _SessionState:
    _database: Database | AsyncDatabase
    def __init__(self,*,autobegin: bool=True,autoflush: bool=True) -> None:
        self._store=StateStore()
        self._mappings: dict[tuple[Any,...],ModelMapping[Any]]={}
        self._closed=False;self._failed=False;self._uncertain=False
        self.autobegin=autobegin;self.autoflush=autoflush

    def _owner_check(self) -> None: raise NotImplementedError

    def _guard(self,*,allow_failed: bool=False) -> None:
        self._owner_check()
        if self._closed: raise OrmError('mapped Session closed')
        if self._database.closed and not allow_failed: raise OrmError('mapped Session connection fenced; create a new Session')
        if self._uncertain: raise OrmError('mapped Session outcome indeterminate; use a new Session')
        if self._failed and not allow_failed: raise OrmError('mapped Session requires rollback')

    def _mapping(self,mapping: ModelMapping[T]) -> None:
        key=(mapping.table.schema,mapping.table.name)
        previous=self._mappings.get(key)
        if previous is not None and previous is not mapping: raise OrmError('mapping identity already registered with different metadata')
        self._mappings[key]=mapping

    def add(self,mapping: ModelMapping[T],obj: T) -> None:
        self._guard();self._mapping(mapping);mapping.writes(obj,inserting=True)
        self._store.attach(mapping,obj,new=True)

    def delete(self,obj: object) -> None:
        self._guard();record=self._store.records.get(id(obj))
        if record is None or record.state is not ObjectState.PERSISTENT: raise OrmError('delete requires a persistent tracked object')
        record.state=ObjectState.DELETE_PENDING

    def object_state(self,obj: object) -> ObjectState:
        self._owner_check()
        return self._store.object_state(obj)

    @contextmanager
    def no_autoflush(self) -> Iterator[None]:
        self._guard();previous=self.autoflush;self.autoflush=False
        try: yield
        finally: self.autoflush=previous

    def _predicate(self,mapping: ModelMapping[Any],values: dict[str,Any],*,all_fields: bool=False) -> Predicate:
        names=tuple(mapping.field_columns) if all_fields else mapping.primary_key
        conditions=[mapping.field_columns[name].eq(values[name]) for name in names]
        result=conditions[0]
        for condition in conditions[1:]: result=result & condition
        return result

    def _key_values(self,mapping: ModelMapping[Any],key: tuple[Any,...]) -> dict[str,Any]:
        if len(key)!=len(mapping.primary_key): raise ValueError('primary-key cardinality mismatch')
        values=dict(zip(mapping.primary_key,key))
        for name,value in values.items(): mapping.field_columns[name].spec.check(value)
        return values

    def _plan(self) -> list[tuple[Record[Any],str,dict[str,Any]]]:
        plans=[]
        # Validate every pending/dirty record before executing any SQL.
        for record in self._store.records.values():
            mapping=record.mapping
            if record.state is ObjectState.PENDING:
                plans.append((record,'insert',mapping.writes(record.obj,inserting=True)))
            elif record.state is ObjectState.PERSISTENT:
                dirty=self._store.dirty(record)
                if dirty:
                    mapping.writes(record.obj,inserting=False)
                    if any(mapping.field_columns[name].spec.generated for name in dirty): raise OrmError('generated field mutation refused')
                    plans.append((record,'update',{mapping.field_columns[name].name:value for name,value in dirty.items()}))
            elif record.state is ObjectState.DELETE_PENDING:
                self._store.dirty(record) # reject changed primary key
                plans.append((record,'delete',{}))
        return plans

    def _flushed_row(self,record: Record[Any],row: dict[str,Any]) -> None:
        values={name:row[column.name] for name,column in record.mapping.field_columns.items()}
        self._store.flushed(record,values)


class Session(_SessionState):
    _database: Database
    """Scalar dataclass identity/flush lifecycle. No relationships or lazy I/O."""
    def __init__(self,database: Database,*,autobegin: bool=True,autoflush: bool=True,close_database: bool=False) -> None:
        super().__init__(autobegin=autobegin,autoflush=autoflush)
        self._database=database;self._transaction: TransactionHandle | None=None
        self._owner=threading.get_ident();self._close_database=close_database

    @classmethod
    def connect(cls,url: str,*,autobegin: bool=True,autoflush: bool=True) -> Session:
        return cls(Database.connect(url),autobegin=autobegin,autoflush=autoflush,close_database=True)

    def _owner_check(self) -> None:
        if threading.get_ident()!=self._owner: raise SessionBusyError('mapped Session belongs to another thread')

    def _ensure_transaction(self) -> None:
        if self._transaction is None:
            if not self.autobegin: raise OrmError('autobegin disabled; use Session.begin')
            self._transaction=self._database.begin()

    @contextmanager
    def begin(self) -> Iterator[Session]:
        self._guard()
        if self._transaction is not None: raise SessionBusyError('mapped Session transaction already active')
        self._transaction=self._database.begin()
        try:
            yield self
            self.commit()
        except BaseException:
            if self._transaction is not None and not self._uncertain: self.rollback()
            raise

    def get(self,mapping: ModelMapping[T],*key: Any) -> T | None:
        self._guard();self._mapping(mapping)
        values=self._key_values(mapping,key)
        if self.autoflush: self.flush()
        found=self._store.find(mapping,key)
        if found is not None: return found
        try:
            self._ensure_transaction()
            row=self._database.one_or_none(select_row(mapping.table,*mapping.field_columns.values()).where(self._predicate(mapping,values)))
            if row is None: return None
            obj=mapping.construct(row);self._store.attach(mapping,obj,new=False);return obj
        except BaseException:
            self._failed=True;raise

    def flush(self) -> None:
        self._guard()
        try:
            plans=self._plan()
            if not plans: return
            self._ensure_transaction()
            for record,action,values in plans:
                mapping=record.mapping
                if action=='delete':
                    count=self._database.execute(delete(mapping.table,where=self._predicate(mapping,record.baseline,all_fields=True)))
                    if count!=1: raise ConflictError('stale mapped delete')
                    record.state=ObjectState.DELETED
                else:
                    mutation=insert(mapping.table,values) if action=='insert' else update(mapping.table,values,where=self._predicate(mapping,record.baseline,all_fields=True))
                    try: row=self._database.one(mutation.returning_row(*mapping.field_columns.values()))
                    except CardinalityError as exc: raise ConflictError('mapped write did not affect exactly one row') from exc
                    self._flushed_row(record,row)
        except BaseException:
            self._failed=True;raise

    def commit(self) -> None:
        self._guard();self.flush()
        if self._transaction is None: return
        tx=self._transaction
        try: tx.commit()
        except BaseException:
            if tx.state=='aborted': self._store.rollback()
            else: self._store.uncertain();self._uncertain=True
            self._failed=True;raise
        else: self._store.committed()
        finally: self._transaction=None

    def rollback(self) -> None:
        self._guard(allow_failed=True)
        try:
            if self._transaction is not None: self._transaction.rollback()
        except BaseException:
            self._store.uncertain();self._uncertain=True;self._failed=True;raise
        else: self._store.rollback();self._failed=False
        finally: self._transaction=None

    def close(self) -> None:
        self._owner_check()
        if self._closed: return
        try:
            if not self._uncertain: self.rollback()
        finally:
            self._store.detach_all();self._closed=True
            if self._close_database: self._database.close()

    def __enter__(self) -> Session: self._guard();return self
    def __exit__(self,*_: object) -> None: self.close()
