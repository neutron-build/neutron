"""Bounded synchronous scalar mapped Session, with explicit unsupported scopes."""
from __future__ import annotations
from contextlib import contextmanager
import threading
import inspect
from typing import Any, Callable, Iterator, Sequence, TypeVar
from .client import AsyncDatabase, Database
from .core import CardinalityError, OrmError, Predicate, SessionBusyError, delete, insert, select_row, update
from .lifecycle import TransactionHandle
from .events import EVENT_NAMES, EventName, SessionEvent
from .mapping import ModelMapping, same_column_value
from .state import ObjectState, Record, StateStore
from .relations import Association, LoadBudget, Relation, load_many, load_one
T=TypeVar('T')
P=TypeVar('P')
C=TypeVar('C')

class ConflictError(OrmError): pass

class _SessionState:
    _database: Database | AsyncDatabase
    _transaction: object | None
    def __init__(self,*,autobegin: bool=True,autoflush: bool=True) -> None:
        self._store=StateStore()
        self._mappings: dict[tuple[Any,...],ModelMapping[Any]]={}
        self._closed=False;self._failed=False;self._uncertain=False
        self.autobegin=autobegin;self.autoflush=autoflush
        self._listeners: dict[EventName,list[Callable[[SessionEvent],Any]]]={}
        self._emitting=False
        self._links: list[tuple[Relation[Any,Any],object,object]]=[]

    def _owner_check(self) -> None: raise NotImplementedError

    def _guard(self,*,allow_failed: bool=False) -> None:
        self._owner_check()
        if self._emitting: raise SessionBusyError('Session API reentrancy from event callback refused')
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

    def link(self,relation: Relation[P,C],parent: P,child: C) -> None:
        """Connect two tracked records; pending generated keys resolve at flush."""
        self._guard();relation.__post_init__()
        if relation.parent_fields!=relation.parent.primary_key:
            raise OrmError('graph link requires the complete declared parent primary key')
        for mapping,obj in ((relation.parent,parent),(relation.child,child)):
            record=self._store.records.get(id(obj))
            if record is None or record.mapping is not mapping or record.state not in {ObjectState.PENDING,ObjectState.PERSISTENT}:
                raise OrmError('graph link requires tracked pending/persistent mapped records')
        if any(relation.child.field_columns[name].spec.generated for name in relation.child_fields):
            raise OrmError('graph link cannot target generated child fields')
        for old,old_parent,old_child in self._links:
            if old_child is child and set(old.child_fields) & set(relation.child_fields):
                if old is relation and old_parent is parent: return
                raise OrmError('conflicting graph relationship ownership')
        self._links.append((relation,parent,child))

    def add_graph(self,relation: Relation[P,C],parent: P,children: Sequence[C]) -> None:
        self._guard();relation.__post_init__()
        if relation.parent_fields!=relation.parent.primary_key: raise OrmError('graph requires complete parent primary key')
        items=tuple(children)
        if len({id(obj) for obj in items})!=len(items): raise OrmError('duplicate graph child')
        if id(parent) not in self._store.records: relation.parent.writes(parent,inserting=True)
        for child in items:
            relation.child.writes(child,inserting=True,deferred_fields=frozenset(relation.child_fields))
        try:
            if id(parent) not in self._store.records: self.add(relation.parent,parent)
            for child in items:
                self._mapping(relation.child)
                self._store.attach(relation.child,child,new=True)
                self.link(relation,parent,child)
        except BaseException:
            self._failed=True;raise

    def _graph_fields(self,record: Record[Any]) -> frozenset[str]:
        return frozenset(name for rel,_,child in self._links if child is record.obj for name in rel.child_fields)

    def _resolve_links(self,record: Record[Any],*,require_complete: bool) -> None:
        for relation,parent,child in self._links:
            if child is not record.obj: continue
            parent_record=self._store.records.get(id(parent))
            if parent_record is None or parent_record.state not in {ObjectState.PENDING,ObjectState.PERSISTENT}:
                raise OrmError('linked parent is unavailable')
            values=relation.parent.snapshot(parent)
            if relation.parent.key(values) is None:
                if require_complete: raise OrmError('linked parent identity is unresolved')
                continue
            for left,right in zip(relation.parent_fields,relation.child_fields):
                value=values[left];relation.child.field_columns[right].spec.check(value)
                setattr(child,right,value)

    def _graph_order(self,plans: list[tuple[Record[Any],str,dict[str,Any]]]) -> list[tuple[Record[Any],str,dict[str,Any]]]:
        by_id={id(record.obj):(record,action,values) for record,action,values in plans}
        dependencies: dict[int,set[int]]={key:set() for key in by_id}
        for _,parent,child in self._links:
            parent_id=id(parent);child_id=id(child)
            if parent_id in by_id and child_id in by_id and by_id[parent_id][1]=='insert':
                dependencies[child_id].add(parent_id)
        ordered=[];remaining=dict(by_id)
        while remaining:
            ready=[key for key in remaining if not dependencies[key] & remaining.keys()]
            if not ready: raise OrmError('cyclic graph dependencies refuse before SQL')
            for key in ready: ordered.append(remaining.pop(key))
        return ordered

    def _attach_associations(self,relation: Relation[P,C],associations: tuple[Association[P,C],...]) -> tuple[Association[P,C],...]:
        result=[]
        for association in associations:
            children=[]
            for child in association.children:
                values=relation.child.snapshot(child);key=relation.child.key(values)
                if key is None: raise OrmError('related row requires complete identity')
                cached=self._store.find(relation.child,key)
                if cached is None:
                    self._store.attach(relation.child,child,new=False);cached=child
                children.append(cached)
            result.append(Association(association.parent,tuple(children)))
        return tuple(result)

    def delete(self,obj: object) -> None:
        self._guard();record=self._store.records.get(id(obj))
        if record is None or record.state is not ObjectState.PERSISTENT: raise OrmError('delete requires a persistent tracked object')
        record.state=ObjectState.DELETE_PENDING

    def _existing_input(self,mapping: ModelMapping[T],obj: T,discard_changes: bool) -> dict[str,Any]:
        self._guard();self._mapping(mapping)
        if type(discard_changes) is not bool: raise ValueError('discard_changes requires a boolean')
        values=mapping.snapshot(obj)
        self._store.check_existing_attach(mapping,obj,values)
        if not discard_changes:
            for name,column in mapping.field_columns.items(): column.spec.check(values[name])
        return values

    def _adopt_existing(self,mapping: ModelMapping[T],obj: T,original: dict[str,Any],row: dict[str,Any],discard_changes: bool) -> None:
        values={name:row[column.name] for name,column in mapping.field_columns.items()}
        for name,column in mapping.field_columns.items(): column.spec.check(values[name])
        # A caller or another task must not change input during the native read.
        current=mapping.snapshot(obj)
        if mapping.key(current)!=mapping.key(original): raise ConflictError('attach-existing primary key changed during read')
        if not discard_changes and any(not same_column_value(column.spec,current[name],values[name]) for name,column in mapping.field_columns.items()):
            raise ConflictError('attach-existing scalar values differ; explicitly discard changes')
        self._store.attach_existing(mapping,obj,values)

    def _refresh_record(self,obj: object,discard_changes: bool) -> Record[Any]:
        self._guard()
        if type(discard_changes) is not bool: raise ValueError('discard_changes requires a boolean')
        record=self._store.records.get(id(obj))
        if record is None or record.state is not ObjectState.PERSISTENT or record.was_new:
            raise OrmError('refresh requires an existing persistent tracked object')
        dirty=self._store.dirty(record) # Primary-key mutation always refuses.
        if dirty and not discard_changes: raise OrmError('refresh refuses dirty scalar state; explicitly discard changes')
        return record

    def detach(self,obj: object) -> None:
        self._guard()
        if self._transaction is not None: raise OrmError('detach refuses an active transaction')
        record=self._store.records.get(id(obj))
        if record is None: raise OrmError('detach requires a tracked object')
        self._store.detach(record)

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
            self._resolve_links(record,require_complete=record.state is not ObjectState.PENDING)
            if record.state is ObjectState.PENDING:
                plans.append((record,'insert',mapping.writes(record.obj,inserting=True,deferred_fields=self._graph_fields(record))))
            elif record.state is ObjectState.PERSISTENT:
                dirty=self._store.dirty(record)
                if dirty:
                    mapping.writes(record.obj,inserting=False)
                    if any(mapping.field_columns[name].spec.generated for name in dirty): raise OrmError('generated field mutation refused')
                    if mapping.version_field is not None:
                        name=mapping.version_field
                        if name in dirty: raise OrmError('application mutation of mapped version refused')
                        value=record.baseline[name]+1
                        mapping.field_columns[name].spec.check(value)
                        dirty[name]=value
                    plans.append((record,'update',{mapping.field_columns[name].name:value for name,value in dirty.items()}))
            elif record.state is ObjectState.DELETE_PENDING:
                self._store.dirty(record) # reject changed primary key
                plans.append((record,'delete',{}))
        return self._graph_order(plans)

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

    def listen(self,event: EventName,callback: Callable[[SessionEvent],None]) -> None:
        self._guard()
        if event not in EVENT_NAMES or not callable(callback): raise ValueError('invalid Session event listener')
        self._listeners.setdefault(event,[]).append(callback)

    def _emit(self,event: EventName,obj: object | None=None) -> None:
        self._emitting=True
        try:
            for callback in tuple(self._listeners.get(event,())):
                result=callback(SessionEvent(event,obj))
                if inspect.isawaitable(result):
                    if inspect.iscoroutine(result): result.close()
                    raise OrmError('synchronous Session listener returned an awaitable')
        finally: self._emitting=False

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

    def load_relation(self,relation: Relation[P,C],parents: Sequence[P],*,budget: LoadBudget,singular: bool=False) -> tuple[Association[P,C],...]:
        self._guard();self._mapping(relation.parent);self._mapping(relation.child)
        if type(singular) is not bool: raise ValueError('singular requires boolean')
        if any(id(parent) not in self._store.records or self._store.records[id(parent)].mapping is not relation.parent for parent in parents):
            raise OrmError('relation parents must belong to this Session')
        if self.autoflush: self.flush()
        try:
            self._ensure_transaction()
            associations=(load_one if singular else load_many)(self._database,relation,parents,budget=budget)
            return self._attach_associations(relation,associations)
        except BaseException:
            self._failed=True;raise

    def attach_existing(self,mapping: ModelMapping[T],obj: T,*,discard_changes: bool=False) -> T:
        values=self._existing_input(mapping,obj,discard_changes)
        try:
            self._ensure_transaction()
            query=select_row(mapping.table,*mapping.field_columns.values()).where(self._predicate(mapping,values))
            try: row=self._database.one(query)
            except CardinalityError as exc: raise ConflictError('attach-existing did not find exactly one row') from exc
            self._adopt_existing(mapping,obj,values,row,discard_changes)
            return obj
        except BaseException:
            self._failed=True;raise

    def refresh(self,obj: T,*,discard_changes: bool=False) -> T:
        record=self._refresh_record(obj,discard_changes)
        try:
            self._ensure_transaction()
            mapping=record.mapping
            query=select_row(mapping.table,*mapping.field_columns.values()).where(self._predicate(mapping,record.baseline))
            try: row=self._database.one(query)
            except CardinalityError as exc: raise ConflictError('refresh did not find exactly one existing row') from exc
            values={name:row[column.name] for name,column in mapping.field_columns.items()}
            self._store.refreshed(record,values)
            return obj
        except BaseException:
            self._failed=True;raise

    def flush(self) -> None:
        self._guard()
        try:
            if not self._plan(): return
            self._ensure_transaction()
            self._emit('before_flush')
            plans=self._plan()
            for record,action,_ in plans:
                self._emit('before_insert' if action=='insert' else 'before_update' if action=='update' else 'before_delete',record.obj)
            expected={(id(record.obj),action) for record,action,_ in plans}
            plans=self._plan()
            if {(id(record.obj),action) for record,action,_ in plans} - expected:
                raise OrmError('row callbacks introduced new write actions; refuse before SQL')
            for record,action,values in plans:
                mapping=record.mapping
                if action=='insert':
                    self._resolve_links(record,require_complete=True)
                    values=mapping.writes(record.obj,inserting=True)
                if action=='delete':
                    count=self._database.execute(delete(mapping.table,where=self._predicate(mapping,record.baseline,all_fields=True)))
                    if count!=1: raise ConflictError('stale mapped delete')
                    record.state=ObjectState.DELETED
                else:
                    mutation=insert(mapping.table,values) if action=='insert' else update(mapping.table,values,where=self._predicate(mapping,record.baseline,all_fields=True))
                    try: row=self._database.one(mutation.returning_row(*mapping.field_columns.values()))
                    except CardinalityError as exc: raise ConflictError('mapped write did not affect exactly one row') from exc
                    self._flushed_row(record,row)
            self._emit('after_flush')
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
        self._links.clear()
        self._emit('after_commit')

    def rollback(self) -> None:
        self._guard(allow_failed=True)
        try:
            if self._transaction is not None: self._transaction.rollback()
        except BaseException:
            self._store.uncertain();self._uncertain=True;self._failed=True;raise
        else: self._store.rollback();self._failed=False
        finally: self._transaction=None
        self._links.clear()
        self._emit('after_rollback')

    def close(self) -> None:
        self._owner_check()
        if self._emitting: raise SessionBusyError('Session close from event callback refused')
        if self._closed: return
        try:
            if not self._uncertain: self.rollback()
        finally:
            self._store.detach_all();self._closed=True
            if self._close_database: self._database.close()

    def __enter__(self) -> Session: self._guard();return self
    def __exit__(self,*_: object) -> None: self.close()
