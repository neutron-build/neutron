"""Bounded synchronous scalar mapped Session, with explicit unsupported scopes."""
from __future__ import annotations
from contextlib import contextmanager
import threading
import inspect
from typing import Any, Callable, Iterator, Mapping, Sequence, TypeVar
from .client import AsyncDatabase, Database
from .core import CardinalityError, OrmError, Predicate, SessionBusyError, delete, insert, select_row, update, Mutation, _bound_quote
from .lifecycle import TransactionHandle
from .events import EVENT_NAMES, EventName, SessionEvent, PostCommitError, PostCommitInterruptedError
from .instrumentation import expire_attributes
from .mapping import ModelMapping, same_column_value
from .state import ObjectState, Record, StateStore
from .relations import Association, LoadBudget, ManyToMany, OwnedRelation, Relation, RelationBudgetError, load_many, load_one
T=TypeVar('T')
P=TypeVar('P')
C=TypeVar('C')
L=TypeVar('L')

class ConflictError(OrmError): pass

class _SessionState:
    _database: Database | AsyncDatabase
    _transaction: object | None
    def __init__(self,*,autobegin: bool=True,autoflush: bool=True,expire_on_commit: bool=False) -> None:
        if type(expire_on_commit) is not bool: raise ValueError('expire_on_commit requires a boolean')
        self.expire_on_commit=expire_on_commit
        self._store=StateStore()
        self._mappings: dict[tuple[Any,...],ModelMapping[Any]]={}
        self._closed=False;self._failed=False;self._uncertain=False;self._postcommit_failed=False
        self.autobegin=autobegin;self.autoflush=autoflush
        self._listeners: dict[EventName,list[Callable[[SessionEvent],Any]]]={}
        self._emitting=False
        self._savepoint_depth=0
        self._links: list[tuple[Relation[Any,Any],object,object]]=[]
        self._deletions: list[tuple[object,object]]=[]

    def _owner_check(self) -> None: raise NotImplementedError

    def _guard(self,*,allow_failed: bool=False) -> None:
        self._owner_check()
        if self._emitting: raise SessionBusyError('Session API reentrancy from event callback refused')
        if self._closed: raise OrmError('mapped Session closed')
        if self._database.closed and not allow_failed: raise OrmError('mapped Session connection fenced; create a new Session')
        if self._postcommit_failed: raise OrmError('known commit state unavailable; use a new Session')
        if self._uncertain: raise OrmError('mapped Session outcome indeterminate; use a new Session')
        if self._failed and not allow_failed: raise OrmError('mapped Session requires rollback')

    def _mapping(self,mapping: ModelMapping[T]) -> None:
        if mapping.table._catalog_owner is not None and mapping.table._catalog_owner is not self._database._catalog_owner: raise OrmError('catalog mapping belongs to another connection')
        if self.expire_on_commit and not mapping.instrumented: raise OrmError('expire_on_commit requires instrumented mappings')
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
            if old_child is child and old is relation and old_parent is parent: return
        self._links.append((relation,parent,child))
        try: self._resolve_links(self._store.records[id(child)],require_complete=False)
        except BaseException:
            self._links.pop();raise

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

    def connect_many_to_many(self,relation: ManyToMany[P,C,L],parent: P,target: C,association: L) -> None:
        self._guard();relation.__post_init__()
        for mapping,obj in ((relation.parent.parent,parent),(relation.target.parent,target)):
            record=self._store.records.get(id(obj))
            if record is None or record.mapping is not mapping or record.state not in {ObjectState.PENDING,ObjectState.PERSISTENT}:
                raise OrmError('many-to-many parents must be tracked pending/persistent identities')
        through=relation.parent.child
        fields=frozenset(relation.parent.child_fields)|frozenset(relation.target.child_fields)
        through.writes(association,inserting=True,deferred_fields=fields)
        try:
            self._mapping(through);self._store.attach(through,association,new=True)
            self.link(relation.parent,parent,association);self.link(relation.target,target,association)
        except BaseException:
            self._failed=True;raise

    def disconnect_many_to_many(self,relation: ManyToMany[P,C,L],parent: P,target: C,association: L) -> None:
        self._guard();relation.__post_init__()
        record=self._store.records.get(id(association))
        if record is None or record.mapping is not relation.parent.child or record.state is not ObjectState.PERSISTENT:
            raise OrmError('disconnect requires a tracked persisted through row')
        for edge,obj in ((relation.parent,parent),(relation.target,target)):
            owner=self._store.records.get(id(obj))
            if owner is None or owner.mapping is not edge.parent or owner.state is not ObjectState.PERSISTENT:
                raise OrmError('disconnect requires tracked parent and target')
            if edge._key(edge.parent,edge.parent_fields,obj)!=edge._key(edge.child,edge.child_fields,association):
                raise ConflictError('through row does not connect requested identities')
        self._links=[edge for edge in self._links if edge[2] is not association]
        self.delete(association)

    def _graph_fields(self,record: Record[Any]) -> frozenset[str]:
        return frozenset(name for rel,_,child in self._links if child is record.obj for name in rel.child_fields)

    def _resolve_links(self,record: Record[Any],*,require_complete: bool) -> None:
        assignments: dict[str,Any]={}
        for relation,parent,child in self._links:
            if child is not record.obj: continue
            parent_record=self._store.records.get(id(parent))
            if parent_record is None or parent_record.state not in {ObjectState.PENDING,ObjectState.PERSISTENT}:
                raise OrmError('linked parent is unavailable')
            values=relation.parent.snapshot(parent)
            if relation.parent.key(values) is None and require_complete: raise OrmError('linked parent identity is unresolved')
            for left,right in zip(relation.parent_fields,relation.child_fields):
                value=values[left]
                if value is None: continue # Unset generated parent identity.
                spec=relation.child.field_columns[right].spec;spec.check(value)
                if right in assignments and not same_column_value(spec,assignments[right],value):
                    raise ConflictError('overlapping relation keys disagree, including tenant identity')
                assignments[right]=value
        # Validate all shared tenant/key assignments before modifying the child.
        for name,value in assignments.items(): setattr(record.obj,name,value)

    def _graph_order(self,plans: list[tuple[Record[Any],str,dict[str,Any]]]) -> list[tuple[Record[Any],str,dict[str,Any]]]:
        by_id={id(record.obj):(record,action,values) for record,action,values in plans}
        dependencies: dict[int,set[int]]={key:set() for key in by_id}
        for _,parent,child in self._links:
            parent_id=id(parent);child_id=id(child)
            if parent_id in by_id and child_id in by_id and by_id[parent_id][1]=='insert':
                dependencies[child_id].add(parent_id)
        for parent,child in self._deletions:
            if id(parent) in by_id and id(child) in by_id: dependencies[id(parent)].add(id(child))
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
                elif self._store.records[id(cached)].state is ObjectState.EXPIRED:
                    record=self._store.records[id(cached)]
                    if self._store.dirty(record): raise ConflictError('expired related identity has local changes')
                    self._store.refreshed(record,values)
                children.append(cached)
            result.append(Association(association.parent,tuple(children)))
        return tuple(result)

    def disconnect(self,relation: OwnedRelation[P,C],parent: P,child: C) -> None:
        self._guard();self._validate_owned_children(relation,parent,(child,))
        if any(not relation.child.field_columns[name].spec.nullable or name in relation.child.primary_key for name in relation.child_fields):
            raise OrmError('disconnect requires nullable non-primary child FK fields')
        self._links=[edge for edge in self._links if edge[2] is not child or edge[1] is not parent]
        for name in relation.child_fields: setattr(child,name,None)

    def remove_related(self,relation: OwnedRelation[P,C],parent: P,child: C) -> None:
        self._guard();self._validate_owned_children(relation,parent,(child,))
        if relation.orphan_delete:
            self._links=[edge for edge in self._links if edge[2] is not child or edge[1] is not parent]
            self.delete(child)
        else: self.disconnect(relation,parent,child)

    def _validate_owned_children(self,relation: OwnedRelation[P,C],parent: P,children: Sequence[C]) -> None:
        if not isinstance(relation,OwnedRelation): raise OrmError('owned graph operation requires explicit OwnedRelation')
        relation.__post_init__()
        for mapping,obj in ((relation.parent,parent),*((relation.child,child) for child in children)):
            record=self._store.records.get(id(obj))
            if record is None or record.mapping is not mapping or record.state is not ObjectState.PERSISTENT or record.was_new:
                raise OrmError('owned graph operation requires existing persistent tracked records')
        expected=relation._key(relation.parent,relation.parent_fields,parent)
        if any(relation._key(relation.child,relation.child_fields,child)!=expected for child in children):
            raise ConflictError('child is not connected to the requested parent identity')

    def _graph_registry(self,relation: OwnedRelation[Any,Any],descendants: tuple[OwnedRelation[Any,Any],...],budget: LoadBudget,max_depth: int) -> tuple[OwnedRelation[Any,Any],...]:
        self._guard();budget.check()
        if type(max_depth) is not int or not 0<max_depth<256: raise ValueError('finite positive graph depth below 256 required')
        if not isinstance(descendants,tuple): raise ValueError('descendant ownership registry requires immutable tuple')
        registry=(relation,*descendants)
        if len({id(item) for item in registry})!=len(registry): raise ValueError('duplicate ownership relation')
        for item in registry:
            if not isinstance(item,OwnedRelation): raise OrmError('delete_graph requires explicit ownership metadata')
            item.__post_init__();self._mapping(item.parent);self._mapping(item.child)
        return registry

    def _apply_graph_plan(self,plans: Sequence[tuple[OwnedRelation[Any,Any],Any,tuple[Any,...]]]) -> None:
        # Discovery and policy admission finish before the first object mutation.
        for relation,parent,children in plans: self._validate_owned_children(relation,parent,children)
        delete_objects: dict[int,Any]={}
        nullified: set[int]=set()
        for relation,parent,children in plans:
            delete_objects[id(parent)]=parent
            for child in children:
                if relation.on_delete=='delete': delete_objects[id(child)]=child
                elif relation.on_delete=='nullify': nullified.add(id(child))
        if nullified & delete_objects.keys(): raise OrmError('owned graph has conflicting delete/nullify ownership')
        for relation,parent,children in plans:
            for child in children:
                if relation.on_delete=='nullify': self.disconnect(relation,parent,child)
                self._deletions.append((parent,child))
        for obj in delete_objects.values(): self.delete(obj)

    def delete(self,obj: object) -> None:
        self._guard();record=self._store.records.get(id(obj))
        if record is None or record.state is not ObjectState.PERSISTENT: raise OrmError('delete requires a persistent tracked object')
        record.state=ObjectState.DELETE_PENDING

    def _existing_input(self,mapping: ModelMapping[T],obj: T,discard_changes: bool) -> dict[str,Any]:
        self._guard();self._mapping(mapping)
        if type(discard_changes) is not bool: raise ValueError('discard_changes requires a boolean')
        values=mapping._snapshot(obj) if discard_changes else mapping.snapshot(obj)
        self._store.check_existing_attach(mapping,obj,values)
        if not discard_changes:
            for name,column in mapping.field_columns.items(): column.spec.check(values[name])
        return values

    def _adopt_existing(self,mapping: ModelMapping[T],obj: T,original: dict[str,Any],row: dict[str,Any],discard_changes: bool) -> None:
        values={name:row[column.name] for name,column in mapping.field_columns.items()}
        for name,column in mapping.field_columns.items(): column.spec.check(values[name])
        # A caller or another task must not change input during the native read.
        current=mapping._snapshot(obj)
        if mapping.key(current)!=mapping.key(original): raise ConflictError('attach-existing primary key changed during read')
        if not discard_changes and any(not same_column_value(column.spec,current[name],values[name]) for name,column in mapping.field_columns.items()):
            raise ConflictError('attach-existing scalar values differ; explicitly discard changes')
        self._store.attach_existing(mapping,obj,values)

    def _merge_input(self,mapping: ModelMapping[T],obj: T,expected: Mapping[str,object]) -> tuple[dict[str,Any],dict[str,Any]]:
        self._guard();self._mapping(mapping);self._store.check_merge_source(obj)
        values=mapping.snapshot(obj);baseline=dict(expected)
        if set(baseline)!=set(mapping.field_columns): raise ValueError('merge requires complete expected scalar baseline')
        for name,column in mapping.field_columns.items():
            column.spec.check(values[name]);column.spec.check(baseline[name])
            if column.spec.generated and not same_column_value(column.spec,values[name],baseline[name]):
                raise OrmError('merge cannot patch a generated field')
        if mapping.key(values) is None or mapping.key(values)!=mapping.key(baseline):
            raise OrmError('merge requires unchanged complete primary key')
        if mapping.version_field is not None and values[mapping.version_field]!=baseline[mapping.version_field]:
            raise OrmError('merge cannot patch the optimistic version')
        return values,baseline

    def _merge_row(self,mapping: ModelMapping[T],source: T,values: dict[str,Any],baseline: dict[str,Any],row: dict[str,Any]) -> T:
        self._store.check_merge_source(source)
        current=mapping.snapshot(source)
        if any(not same_column_value(column.spec,current[name],values[name]) for name,column in mapping.field_columns.items()):
            raise ConflictError('merge source changed during native read')
        native={name:row[column.name] for name,column in mapping.field_columns.items()}
        if any(not same_column_value(column.spec,native[name],baseline[name]) for name,column in mapping.field_columns.items()):
            raise ConflictError('merge expected baseline differs from database')
        key=mapping.key(native)
        if key is None: raise OrmError('merge row lacks complete identity')
        target=self._store.find(mapping,key)
        if target is None:
            target=mapping.construct(row);self._store.attach(mapping,target,new=False)
        else:
            record=self._store.records[id(target)]
            if record.was_new or record.state is not ObjectState.PERSISTENT or self._store.dirty(record):
                raise ConflictError('merge target has local/uncommitted changes')
            self._store.refreshed(record,native)
        mapping.restore(target,values)
        return target

    def _refresh_record(self,obj: object,discard_changes: bool) -> Record[Any]:
        self._guard()
        if type(discard_changes) is not bool: raise ValueError('discard_changes requires a boolean')
        record=self._store.records.get(id(obj))
        if record is None or record.state not in {ObjectState.PERSISTENT,ObjectState.EXPIRED} or record.was_new:
            raise OrmError('refresh requires an existing persistent tracked object')
        dirty=self._store.dirty(record) # Primary-key mutation always refuses.
        if dirty and not discard_changes: raise OrmError('refresh refuses dirty scalar state; explicitly discard changes')
        return record

    def expire(self,obj: object,*fields: str,discard_changes: bool=False) -> None:
        self._guard()
        if type(discard_changes) is not bool: raise ValueError('discard_changes requires a boolean')
        record=self._store.records.get(id(obj))
        if record is None: raise OrmError('expire requires tracked object')
        names=frozenset(fields) if fields else frozenset(record.mapping.field_columns)
        if names-record.mapping.field_columns.keys(): raise ValueError('expired field outside mapping')
        self._store.expire(record,names,discard_changes=discard_changes)

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
            elif record.state is ObjectState.EXPIRED:
                if self._store.dirty(record): raise OrmError('expired object has edited fields; explicitly refresh')
            elif record.state is ObjectState.DELETE_PENDING:
                self._store.dirty(record) # reject changed primary key
                plans.append((record,'delete',{}))
        return self._graph_order(plans)

    def _bulk_mutation(self,mapping: ModelMapping[Any],values: Mapping[str,object] | None,where: Predicate) -> Mutation:
        self._guard();self._mapping(mapping)
        if values is None: return delete(mapping.table,where=where)
        forbidden={mapping.field_columns[name].name for name in mapping.primary_key}
        if forbidden & values.keys(): raise OrmError('bulk primary-key mutation refused')
        statement=update(mapping.table,values,where=where)
        if mapping.version_field is not None:
            name=mapping.field_columns[mapping.version_field].name
            if name in values: raise OrmError('bulk application version mutation refused')
            quoted=_bound_quote(name)
            suffix=' WHERE '+where.sql
            statement=Mutation(statement.sql[:-len(suffix)]+f', {quoted} = {quoted} + 1'+suffix,statement.params,mapping.table)
        return statement

    def _bulk_adopt(self,mapping: ModelMapping[Any],rows: list[dict[str,Any]],*,deleting: bool) -> int:
        for row in rows:
            values={name:row[column.name] for name,column in mapping.field_columns.items()}
            key=mapping.key(values)
            if key is None: raise OrmError('bulk returning requires complete identity')
            obj=self._store.find(mapping,key)
            if obj is None: continue
            record=self._store.records[id(obj)]
            if deleting: record.state=ObjectState.DELETED
            else: self._flushed_row(record,row)
        return len(rows)

    def _flushed_row(self,record: Record[Any],row: dict[str,Any]) -> None:
        values={name:row[column.name] for name,column in record.mapping.field_columns.items()}
        self._store.flushed(record,values)


class Session(_SessionState):
    _database: Database
    """Scalar dataclass identity/flush lifecycle. No relationships or lazy I/O."""
    def __init__(self,database: Database,*,autobegin: bool=True,autoflush: bool=True,close_database: bool=False,expire_on_commit: bool=False) -> None:
        super().__init__(autobegin=autobegin,autoflush=autoflush,expire_on_commit=expire_on_commit)
        self._database=database;self._transaction: TransactionHandle | None=None
        self._owner=threading.get_ident();self._close_database=close_database

    @classmethod
    def connect(cls,url: str,*,autobegin: bool=True,autoflush: bool=True,expire_on_commit: bool=False) -> Session:
        return cls(Database.connect(url),autobegin=autobegin,autoflush=autoflush,close_database=True,expire_on_commit=expire_on_commit)

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

    @contextmanager
    def savepoint(self) -> Iterator[Session]:
        self._guard();self.flush();self._ensure_transaction()
        checkpoint=self._store.checkpoint();links=self._links.copy();deletions=self._deletions.copy()
        self._savepoint_depth+=1
        try:
            with self._database.savepoint():
                yield self
                self.flush()
                if self._failed: raise OrmError('failed mapped savepoint requires rollback')
        except BaseException:
            if self._database.closed:
                self._store.uncertain();self._uncertain=True
            else:
                try: self._store.restore_checkpoint(checkpoint)
                except BaseException:
                    self._database._discard();self._store.uncertain();self._uncertain=True
                    raise
                self._links=links;self._deletions=deletions;self._failed=False
            raise
        finally: self._savepoint_depth-=1

    def get(self,mapping: ModelMapping[T],*key: Any) -> T | None:
        self._guard();self._mapping(mapping)
        values=self._key_values(mapping,key)
        if self.autoflush: self.flush()
        found=self._store.find(mapping,key)
        if found is not None:
            if self._store.records[id(found)].state is ObjectState.EXPIRED: return self.refresh(found)
            return found
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

    def delete_graph(self,relation: OwnedRelation[P,C],parent: P,*,budget: LoadBudget,descendants: tuple[OwnedRelation[Any,Any],...]=(),max_depth: int=32) -> None:
        registry=self._graph_registry(relation,descendants,budget,max_depth)
        plans: list[tuple[OwnedRelation[Any,Any],Any,tuple[Any,...]]]=[]
        seen: set[int]=set();rows=0
        def visit(obj: Any,depth: int,edges: tuple[OwnedRelation[Any,Any],...]) -> None:
            nonlocal rows
            if id(obj) in seen: raise OrmError('owned graph contains repeated or cyclic ownership')
            if depth>max_depth or len(seen)>=budget.max_parents: raise RelationBudgetError('owned graph traversal budget exceeded')
            seen.add(id(obj))
            for edge in edges:
                result=self.load_relation(edge,[obj],budget=LoadBudget(1,max(1,budget.max_rows-rows),budget.batch_size))
                children=result[0].children;rows+=len(children)
                if rows>budget.max_rows: raise RelationBudgetError('owned graph row budget exceeded')
                self._validate_owned_children(edge,obj,children)
                if children and edge.on_delete=='restrict': raise OrmError('owned relation restricts deleting a parent with children')
                plans.append((edge,obj,children))
                if edge.on_delete=='delete':
                    for child in children:
                        visit(child,depth+1,tuple(item for item in registry if item.parent is edge.child))
        try:
            visit(parent,0,(relation,))
            self._apply_graph_plan(plans)
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

    def merge(self,mapping: ModelMapping[T],obj: T,*,expected: Mapping[str,object]) -> T:
        values,baseline=self._merge_input(mapping,obj,expected)
        try:
            self._ensure_transaction()
            query=select_row(mapping.table,*mapping.field_columns.values()).where(self._predicate(mapping,baseline))
            try: row=self._database.one(query)
            except CardinalityError as exc: raise ConflictError('merge did not find exactly one existing row') from exc
            return self._merge_row(mapping,obj,values,baseline,row)
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
            if self._plan(): raise OrmError('after_flush callback introduced unflushed state; rollback required')
        except BaseException:
            self._failed=True;raise

    def bulk_update(self,mapping: ModelMapping[Any],values: Mapping[str,object],*,where: Predicate) -> int:
        statement=self._bulk_mutation(mapping,values,where)
        return self._bulk(mapping,statement,deleting=False)

    def bulk_delete(self,mapping: ModelMapping[Any],*,where: Predicate) -> int:
        statement=self._bulk_mutation(mapping,None,where)
        return self._bulk(mapping,statement,deleting=True)

    def _bulk(self,mapping: ModelMapping[Any],statement: Mutation,*,deleting: bool) -> int:
        try:
            self.flush();self._ensure_transaction()
            rows=self._database.all(statement.returning_row(*mapping.field_columns.values()))
            return self._bulk_adopt(mapping,rows,deleting=deleting)
        except BaseException:
            self._failed=True;raise

    def commit(self) -> None:
        self._guard()
        if self._savepoint_depth: raise SessionBusyError('commit during savepoint refused')
        self.flush()
        if self._transaction is None: return
        tx=self._transaction
        try: tx.commit()
        except BaseException:
            if tx.state=='aborted': self._store.rollback()
            else: self._store.uncertain();self._uncertain=True
            self._failed=True;raise
        else:
            try:
                self._store.committed();self._links.clear();self._deletions.clear()
            except BaseException as exc:
                self._store.fence_after_commit();self._postcommit_failed=True;self._failed=True;self._database._discard()
                if isinstance(exc,KeyboardInterrupt): raise PostCommitInterruptedError() from exc
                raise PostCommitError() from exc
        finally: self._transaction=None
        try:
            if self.expire_on_commit:
                for record in self._store.records.values(): self._store.expire(record,frozenset(record.mapping.field_columns),discard_changes=False)
            self._emit('after_commit')
        except KeyboardInterrupt as exc: raise PostCommitInterruptedError() from exc
        except Exception as exc: raise PostCommitError() from exc

    def rollback(self) -> None:
        self._guard(allow_failed=True)
        if self._savepoint_depth: raise SessionBusyError('rollback during savepoint refused')
        try:
            if self._transaction is not None: self._transaction.rollback()
        except BaseException:
            self._store.uncertain();self._uncertain=True;self._failed=True;raise
        else: self._store.rollback();self._failed=False
        finally: self._transaction=None
        self._links.clear();self._deletions.clear()
        self._emit('after_rollback')

    def close(self) -> None:
        self._owner_check()
        if self._emitting or self._savepoint_depth: raise SessionBusyError('Session close during event/savepoint refused')
        if self._closed: return
        expired={identity:record.expired_fields for identity,record in self._store.records.items() if record.state is ObjectState.EXPIRED}
        try:
            if not self._uncertain and not self._postcommit_failed: self.rollback()
        finally:
            for identity,names in expired.items():
                record=self._store.records.get(identity)
                if record is not None: expire_attributes(record.obj,names)
            self._store.detach_all();self._closed=True
            if self._close_database: self._database.close()

    def __enter__(self) -> Session: self._guard();return self
    def __exit__(self,*_: object) -> None: self.close()
