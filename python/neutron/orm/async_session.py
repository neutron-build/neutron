"""Native async scalar mapped lifecycle using the common pure state engine."""
from __future__ import annotations
import asyncio
import inspect
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Awaitable, Callable, Mapping, Sequence, TypeVar, cast
from .client import AsyncDatabase
from .core import CardinalityError, OrmError, Predicate, SessionBusyError, delete, insert, select_row, update, Mutation
from .lifecycle import AsyncTransactionHandle
from .events import EVENT_NAMES, EventName, SessionEvent, PostCommitError, PostCommitCancelledError, PostCommitInterruptedError
from .instrumentation import expire_attributes
from .mapping import ModelMapping
from .polymorphic import PolymorphicMapping, PolymorphicView
from .session import ConflictError, _SessionState
from .state import ObjectState
from .relations import Association, LoadBudget, OwnedRelation, Relation, RelationBudgetError, async_load_many, async_load_one
T=TypeVar('T')
P=TypeVar('P')
C=TypeVar('C')

class AsyncSession(_SessionState):
    _database: AsyncDatabase
    def __init__(self,database: AsyncDatabase,*,autobegin: bool=True,autoflush: bool=True,close_database: bool=False,expire_on_commit: bool=False) -> None:
        super().__init__(autobegin=autobegin,autoflush=autoflush,expire_on_commit=expire_on_commit)
        self._database=database;self._transaction: AsyncTransactionHandle | None=None
        self._owner=asyncio.current_task();self._close_database=close_database
        if self._owner is None: raise SessionBusyError('AsyncSession requires an owning asyncio task')

    @classmethod
    async def connect(cls,url: str,*,autobegin: bool=True,autoflush: bool=True,expire_on_commit: bool=False) -> AsyncSession:
        return cls(await AsyncDatabase.connect(url),autobegin=autobegin,autoflush=autoflush,close_database=True,expire_on_commit=expire_on_commit)

    def listen(self,event: EventName,callback: Callable[[SessionEvent],Awaitable[None] | None]) -> None:
        self._guard()
        if event not in EVENT_NAMES or not callable(callback): raise ValueError('invalid Session event listener')
        self._listeners.setdefault(event,[]).append(callback)

    async def _emit(self,event: EventName,obj: object | None=None) -> None:
        self._emitting=True
        try:
            for callback in tuple(self._listeners.get(event,())):
                result=callback(SessionEvent(event,obj))
                if inspect.isawaitable(result): await result
        finally: self._emitting=False

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

    @asynccontextmanager
    async def savepoint(self) -> AsyncIterator[AsyncSession]:
        self._guard();await self.flush();await self._ensure_transaction()
        checkpoint=self._store.checkpoint();links=self._links.copy();deletions=self._deletions.copy()
        self._savepoint_depth+=1
        try:
            async with self._database.savepoint():
                yield self
                await self.flush()
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

    async def get(self,mapping: ModelMapping[T]|PolymorphicView[T],*key: Any) -> T | None:
        view=mapping if isinstance(mapping,PolymorphicView) else None
        family: ModelMapping[T]=cast(ModelMapping[T],view.family) if view is not None else cast(ModelMapping[T],mapping)
        self._guard();self._mapping(family)
        values=self._key_values(family,key)
        if self.autoflush: await self.flush()
        found=self._store.find(family,key)
        if found is not None:
            if view is not None and type(found) is not view.model_type: return None
            if self._store.records[id(found)].state is ObjectState.EXPIRED: return await self.refresh(found)
            return found
        try:
            await self._ensure_transaction()
            predicate=self._predicate(family,values)
            if view is not None: predicate &= view.family.field_columns[view.family.discriminator].eq(view.tag)
            row=await self._database.one_or_none(select_row(family.table,*family.field_columns.values()).where(predicate))
            if row is None: return None
            obj=family.construct(row)
            if view is not None and type(obj) is not view.model_type: raise OrmError('native subtype selection returned a different class')
            self._store.attach(family,obj,new=False);return obj
        except BaseException:
            self._failed=True;raise

    async def select_polymorphic(self,mapping: PolymorphicMapping[T]|PolymorphicView[T],*,where: Predicate|None=None,max_rows: int=1000) -> tuple[T,...]:
        self._guard()
        if type(max_rows) is not int or not 0<max_rows<2**31-1: raise ValueError('finite positive polymorphic row budget required')
        family=cast(PolymorphicMapping[T],mapping.family) if isinstance(mapping,PolymorphicView) else mapping
        self._mapping(family);query=mapping.query()
        if where is not None: query=query.where(where)
        if self.autoflush: await self.flush()
        try:
            await self._ensure_transaction();objects=await self._database.all(query.limit(max_rows+1))
            if len(objects)>max_rows: raise OrmError('polymorphic selection row budget exceeded')
            return self._adopt_polymorphic(family,objects)
        except BaseException:
            self._failed=True;raise

    async def load_relation(self,relation: Relation[P,C],parents: Sequence[P],*,budget: LoadBudget,singular: bool=False) -> tuple[Association[P,C],...]:
        self._guard();self._mapping(relation.parent);self._mapping(relation.child)
        if type(singular) is not bool: raise ValueError('singular requires boolean')
        if any(id(parent) not in self._store.records or self._store.records[id(parent)].mapping is not relation.parent for parent in parents):
            raise OrmError('relation parents must belong to this Session')
        if self.autoflush: await self.flush()
        try:
            await self._ensure_transaction()
            loader=async_load_one if singular else async_load_many
            associations=await loader(self._database,relation,parents,budget=budget)
            return self._attach_associations(relation,associations)
        except BaseException:
            self._failed=True;raise

    async def delete_graph(self,relation: OwnedRelation[P,C],parent: P,*,budget: LoadBudget,descendants: tuple[OwnedRelation[Any,Any],...]=(),max_depth: int=32) -> None:
        registry=self._graph_registry(relation,descendants,budget,max_depth)
        plans: list[tuple[OwnedRelation[Any,Any],Any,tuple[Any,...]]]=[]
        seen: set[int]=set();rows=0
        async def visit(obj: Any,depth: int,edges: tuple[OwnedRelation[Any,Any],...]) -> None:
            nonlocal rows
            if id(obj) in seen: raise OrmError('owned graph contains repeated or cyclic ownership')
            if depth>max_depth or len(seen)>=budget.max_parents: raise RelationBudgetError('owned graph traversal budget exceeded')
            seen.add(id(obj))
            for edge in edges:
                result=await self.load_relation(edge,[obj],budget=LoadBudget(1,max(1,budget.max_rows-rows),budget.batch_size))
                children=result[0].children;rows+=len(children)
                if rows>budget.max_rows: raise RelationBudgetError('owned graph row budget exceeded')
                self._validate_owned_children(edge,obj,children)
                if children and edge.on_delete=='restrict': raise OrmError('owned relation restricts deleting a parent with children')
                plans.append((edge,obj,children))
                if edge.on_delete=='delete':
                    for child in children:
                        await visit(child,depth+1,tuple(item for item in registry if item.parent is edge.child))
        try:
            await visit(parent,0,(relation,))
            self._apply_graph_plan(plans)
        except BaseException:
            self._failed=True;raise

    async def attach_existing(self,mapping: ModelMapping[T],obj: T,*,discard_changes: bool=False) -> T:
        values=self._existing_input(mapping,obj,discard_changes)
        try:
            await self._ensure_transaction()
            query=select_row(mapping.table,*mapping.field_columns.values()).where(self._predicate(mapping,values))
            try: row=await self._database.one(query)
            except CardinalityError as exc: raise ConflictError('attach-existing did not find exactly one row') from exc
            self._adopt_existing(mapping,obj,values,row,discard_changes)
            return obj
        except BaseException:
            self._failed=True;raise

    async def merge(self,mapping: ModelMapping[T],obj: T,*,expected: Mapping[str,object]) -> T:
        values,baseline=self._merge_input(mapping,obj,expected)
        try:
            await self._ensure_transaction()
            query=select_row(mapping.table,*mapping.field_columns.values()).where(self._predicate(mapping,baseline))
            try: row=await self._database.one(query)
            except CardinalityError as exc: raise ConflictError('merge did not find exactly one existing row') from exc
            return self._merge_row(mapping,obj,values,baseline,row)
        except BaseException:
            self._failed=True;raise

    async def refresh(self,obj: T,*,discard_changes: bool=False) -> T:
        record=self._refresh_record(obj,discard_changes)
        try:
            await self._ensure_transaction()
            mapping=record.mapping
            query=select_row(mapping.table,*mapping.field_columns.values()).where(self._predicate(mapping,record.baseline))
            try: row=await self._database.one(query)
            except CardinalityError as exc: raise ConflictError('refresh did not find exactly one existing row') from exc
            values={name:row[column.name] for name,column in mapping.field_columns.items()}
            self._store.refreshed(record,values)
            return obj
        except BaseException:
            self._failed=True;raise

    async def flush(self) -> None:
        self._guard()
        try:
            if not self._plan(): return
            await self._ensure_transaction()
            await self._emit('before_flush')
            plans=self._plan()
            for record,action,_ in plans:
                await self._emit('before_insert' if action=='insert' else 'before_update' if action=='update' else 'before_delete',record.obj)
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
                    count=await self._database.execute(delete(mapping.table,where=self._predicate(mapping,record.baseline,all_fields=True)))
                    if count!=1: raise ConflictError('stale mapped delete')
                    record.state=ObjectState.DELETED
                else:
                    mutation=insert(mapping.table,values) if action=='insert' else update(mapping.table,values,where=self._predicate(mapping,record.baseline,all_fields=True))
                    try: row=await self._database.one(mutation.returning_row(*mapping.field_columns.values()))
                    except CardinalityError as exc: raise ConflictError('mapped write did not affect exactly one row') from exc
                    self._flushed_row(record,row)
            await self._emit('after_flush')
            if self._plan(): raise OrmError('after_flush callback introduced unflushed state; rollback required')
        except BaseException:
            self._failed=True;raise

    async def bulk_update(self,mapping: ModelMapping[Any],values: Mapping[str,object],*,where: Predicate) -> int:
        statement=self._bulk_mutation(mapping,values,where)
        return await self._bulk(mapping,statement,deleting=False)

    async def bulk_delete(self,mapping: ModelMapping[Any],*,where: Predicate) -> int:
        statement=self._bulk_mutation(mapping,None,where)
        return await self._bulk(mapping,statement,deleting=True)

    async def _bulk(self,mapping: ModelMapping[Any],statement: Mutation,*,deleting: bool) -> int:
        try:
            await self.flush();await self._ensure_transaction()
            rows=await self._database.all(statement.returning_row(*mapping.field_columns.values()))
            return self._bulk_adopt(mapping,rows,deleting=deleting)
        except BaseException:
            self._failed=True;raise

    async def commit(self) -> None:
        self._guard()
        if self._savepoint_depth: raise SessionBusyError('commit during savepoint refused')
        await self.flush()
        if self._transaction is None: return
        tx=self._transaction
        try: await tx.commit()
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
                if isinstance(exc,asyncio.CancelledError): raise PostCommitCancelledError() from exc
                raise PostCommitError() from exc
        finally: self._transaction=None
        try:
            if self.expire_on_commit:
                for record in self._store.records.values(): self._store.expire(record,frozenset(record.mapping.active_fields(record.obj)),discard_changes=False)
            await self._emit('after_commit')
        except asyncio.CancelledError as exc: raise PostCommitCancelledError() from exc
        except KeyboardInterrupt as exc: raise PostCommitInterruptedError() from exc
        except Exception as exc: raise PostCommitError() from exc

    async def rollback(self) -> None:
        self._guard(allow_failed=True)
        if self._savepoint_depth: raise SessionBusyError('rollback during savepoint refused')
        try:
            if self._transaction is not None: await self._transaction.rollback()
        except BaseException:
            self._store.uncertain();self._uncertain=True;self._failed=True;raise
        else: self._store.rollback();self._failed=False
        finally: self._transaction=None
        self._links.clear();self._deletions.clear()
        await self._emit('after_rollback')

    async def close(self) -> None:
        self._owner_check()
        if self._emitting or self._savepoint_depth: raise SessionBusyError('Session close during event/savepoint refused')
        if self._closed: return
        expired={identity:record.expired_fields for identity,record in self._store.records.items() if record.state is ObjectState.EXPIRED}
        try:
            if not self._uncertain and not self._postcommit_failed: await self.rollback()
        finally:
            for identity,names in expired.items():
                record=self._store.records.get(identity)
                if record is not None: expire_attributes(record.obj,names)
            self._store.detach_all();self._closed=True
            if self._close_database: await self._database.close()

    async def __aenter__(self) -> AsyncSession: self._guard();return self
    async def __aexit__(self,*_: object) -> None: await self.close()
