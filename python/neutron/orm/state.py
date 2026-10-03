"""Pure mapped-object transitions shared by synchronous and async Sessions."""
from __future__ import annotations
from copy import deepcopy
from dataclasses import dataclass
from enum import Enum
from weakref import WeakValueDictionary, ref
from threading import RLock
from typing import Any, Generic, TypeVar, cast
from .core import OrmError
from .mapping import ModelMapping, same_value
from .instrumentation import expire_attributes
T=TypeVar('T')
_OWNERS: WeakValueDictionary[int,StateStore]=WeakValueDictionary()
_OWNER_LOCK=RLock()

class ObjectState(str,Enum):
    TRANSIENT='transient'
    PENDING='pending'
    PERSISTENT='persistent'
    EXPIRED='expired'
    DELETE_PENDING='delete-pending'
    DELETED='deleted'
    DETACHED='detached'
    INDETERMINATE='indeterminate'

@dataclass
class Record(Generic[T]):
    mapping: ModelMapping[T]
    obj: T
    state: ObjectState
    baseline: dict[str,Any]
    original: dict[str,Any]
    was_new: bool=False
    expired_fields: frozenset[str]=frozenset()

class StateStore:
    def __init__(self) -> None:
        self.records: dict[int,Record[Any]]={}
        self.identities: dict[tuple[Any,...],Record[Any]]={}
        self._history: dict[int,tuple[Any,ObjectState]]={}

    def _identity(self,mapping: ModelMapping[Any],values: dict[str,Any]) -> tuple[Any,...] | None:
        key=mapping.key(values)
        return None if key is None else (mapping.model_type,mapping.table.schema,mapping.table.name,*key)

    def attach(self,mapping: ModelMapping[T],obj: T,*,new: bool) -> Record[T]:
        if id(obj) in self.records: raise OrmError('object already tracked')
        self._history.pop(id(obj),None)
        values=deepcopy(mapping.snapshot(obj))
        identity=self._identity(mapping,values)
        if identity is not None and identity in self.identities: raise OrmError('mapped identity already tracked')
        if not new and identity is None: raise OrmError("persistent object requires a complete primary key")
        with _OWNER_LOCK:
            owner=_OWNERS.get(id(obj))
            if owner is not None and owner is not self: raise OrmError("object attached to another Session")
            _OWNERS[id(obj)]=self
        record=Record(mapping,obj,ObjectState.PENDING if new else ObjectState.PERSISTENT,values.copy(),values,new)
        self.records[id(obj)]=record
        if identity is not None: self.identities[identity]=record
        return record

    def check_merge_source(self,obj: object) -> None:
        with _OWNER_LOCK:
            if _OWNERS.get(id(obj)) is not None:
                raise OrmError('merge source must be detached from every Session')

    def check_existing_attach(self,mapping: ModelMapping[Any],obj: object,values: dict[str,Any]) -> None:
        if id(obj) in self.records: raise OrmError('object already tracked; use refresh')
        identity=self._identity(mapping,values)
        if identity is None: raise OrmError('attach-existing requires a complete primary key')
        if identity in self.identities: raise OrmError('mapped identity already tracked by another object')
        with _OWNER_LOCK:
            if _OWNERS.get(id(obj)) is not None: raise OrmError('object already attached to a Session')

    def attach_existing(self,mapping: ModelMapping[T],obj: T,values: dict[str,Any]) -> Record[T]:
        # Recheck and claim ownership atomically after native I/O, before any
        # caller object mutation. No global primary-key identity cache exists.
        with _OWNER_LOCK:
            self.check_existing_attach(mapping,obj,mapping._snapshot(obj))
            if set(values)!=set(mapping.field_columns): raise OrmError('incomplete attach-existing projection')
            for name,column in mapping.field_columns.items(): column.spec.check(values[name])
            if self._identity(mapping,values)!=self._identity(mapping,mapping._snapshot(obj)):
                raise OrmError('attach-existing changed primary-key identity')
            snapshot=deepcopy(values)
            record=Record(mapping,obj,ObjectState.PERSISTENT,snapshot.copy(),snapshot,False)
            _OWNERS[id(obj)]=self
            self.records[id(obj)]=record
            identity=self._identity(mapping,snapshot)
            if identity is not None: self.identities[identity]=record
            self._history.pop(id(obj),None)
        mapping.restore(obj,deepcopy(snapshot))
        return record

    def find(self,mapping: ModelMapping[T],key: tuple[Any,...]) -> T | None:
        if len(key)!=len(mapping.primary_key): raise ValueError('primary-key cardinality mismatch')
        normalized=mapping.key(dict(zip(mapping.primary_key,key)))
        if normalized is None: return None
        record=self.identities.get((mapping.model_type,mapping.table.schema,mapping.table.name,*normalized))
        if record is None: return None
        if record.state is ObjectState.DELETED: return None
        if record.state is ObjectState.INDETERMINATE: raise OrmError('tracked identity unavailable')
        return cast(T,record.obj)

    def dirty(self,record: Record[Any]) -> dict[str,Any]:
        current=record.mapping._snapshot(record.obj)
        if record.mapping.key(current)!=record.mapping.key(record.baseline): raise OrmError('primary-key mutation unsupported')
        return {name:value for name,value in current.items() if not same_value(value,record.baseline[name])}

    def flushed(self,record: Record[Any],values: dict[str,Any]) -> None:
        record.mapping.restore(record.obj,values)
        record.baseline=deepcopy(values)
        record.state=ObjectState.PERSISTENT;record.expired_fields=frozenset()
        identity=self._identity(record.mapping,values)
        if identity is None: raise OrmError('flushed row has incomplete primary key')
        other=self.identities.get(identity)
        if other is not None and other is not record: raise OrmError('generated identity collision')
        self.identities[identity]=record

    def refreshed(self,record: Record[Any],values: dict[str,Any]) -> None:
        if self.records.get(id(record.obj)) is not record or record.state not in {ObjectState.PERSISTENT,ObjectState.EXPIRED} or record.was_new:
            raise OrmError('refresh requires an existing persistent tracked object')
        if set(values)!=set(record.mapping.field_columns): raise OrmError('incomplete refresh projection')
        for name,column in record.mapping.field_columns.items(): column.spec.check(values[name])
        if self._identity(record.mapping,values)!=self._identity(record.mapping,record.baseline):
            raise OrmError('refresh changed tracked primary-key identity')
        snapshot=deepcopy(values)
        record.mapping.restore(record.obj,deepcopy(snapshot))
        record.baseline=snapshot;record.state=ObjectState.PERSISTENT;record.expired_fields=frozenset()
        # Preserve original: rollback reconciles the pretransaction snapshot.

    def expire(self,record: Record[Any],names: frozenset[str],*,discard_changes: bool) -> None:
        if not record.mapping.instrumented: raise OrmError('expiration requires instrumented mapping')
        if record.state not in {ObjectState.PERSISTENT,ObjectState.EXPIRED} or record.was_new:
            raise OrmError('expire requires an existing persistent tracked object')
        if self.dirty(record):
            if not discard_changes: raise OrmError('expire refuses dirty state; explicitly discard changes')
            record.mapping.restore(record.obj,deepcopy(record.baseline))
        record.expired_fields=record.expired_fields | names
        record.state=ObjectState.EXPIRED
        expire_attributes(record.obj,record.expired_fields)

    def detach(self,record: Record[Any]) -> None:
        if self.records.get(id(record.obj)) is not record or record.state is not ObjectState.PERSISTENT or record.was_new:
            raise OrmError('detach requires an existing persistent tracked object')
        if self.dirty(record) or any(not same_value(value,record.original[name]) for name,value in record.baseline.items()):
            raise OrmError('detach refuses dirty or uncommitted scalar state')
        self.records.pop(id(record.obj))
        record.state=ObjectState.DETACHED
        self._remember(record.obj,ObjectState.DETACHED)
        self._release(record.obj)
        self._reindex()

    def committed(self) -> None:
        for record in list(self.records.values()):
            if record.state is ObjectState.DELETED:
                self.records.pop(id(record.obj));self._remember(record.obj,ObjectState.DELETED);self._release(record.obj);continue
            record.original=deepcopy(record.baseline);record.was_new=False
        self._reindex()

    def checkpoint(self) -> dict[int,tuple[Record[Any],ObjectState,dict[str,Any],dict[str,Any],bool,dict[str,Any],frozenset[str]]]:
        return {identity:(record,record.state,deepcopy(record.baseline),deepcopy(record.original),record.was_new,deepcopy(record.mapping._snapshot(record.obj)),record.expired_fields) for identity,record in self.records.items()}

    def restore_checkpoint(self,checkpoint: dict[int,tuple[Record[Any],ObjectState,dict[str,Any],dict[str,Any],bool,dict[str,Any],frozenset[str]]]) -> None:
        for identity,record in list(self.records.items()):
            if identity in checkpoint: continue
            record.mapping.restore(record.obj,deepcopy(record.original))
            state=ObjectState.TRANSIENT if record.was_new else ObjectState.DETACHED
            self.records.pop(identity);self._remember(record.obj,state);self._release(record.obj)
        for identity,(record,state,baseline,original,was_new,values,expired_fields) in checkpoint.items():
            record.mapping.restore(record.obj,deepcopy(values))
            record.state=state;record.baseline=deepcopy(baseline);record.original=deepcopy(original);record.was_new=was_new;record.expired_fields=expired_fields
            expire_attributes(record.obj,expired_fields)
            self.records[identity]=record
        self._reindex()

    def rollback(self) -> None:
        for record in list(self.records.values()):
            record.mapping.restore(record.obj,deepcopy(record.original))
            record.baseline=deepcopy(record.original);record.expired_fields=frozenset()
            if record.was_new:
                record.state=ObjectState.TRANSIENT;self.records.pop(id(record.obj));self._remember(record.obj,ObjectState.TRANSIENT);self._release(record.obj)
            else: record.state=ObjectState.PERSISTENT
        self._reindex()

    def uncertain(self) -> None:
        for record in self.records.values():
            record.state=ObjectState.INDETERMINATE
            if record.mapping.instrumented:
                record.expired_fields=frozenset(record.mapping.field_columns)
                expire_attributes(record.obj,record.expired_fields)

    def detach_all(self) -> None:
        for record in self.records.values():
            if record.state is not ObjectState.INDETERMINATE: record.state=ObjectState.DETACHED
            self._remember(record.obj,record.state)
            self._release(record.obj)
        self.records.clear();self.identities.clear()

    def object_state(self,obj: object) -> ObjectState:
        record=self.records.get(id(obj))
        if record is not None: return record.state
        previous=self._history.get(id(obj))
        return previous[1] if previous is not None and previous[0]() is obj else ObjectState.TRANSIENT

    def _remember(self,obj: object,state: ObjectState) -> None:
        owner=ref(self);identity=id(obj)
        def cleanup(reference: Any) -> None:
            store=owner()
            if store is not None and store._history.get(identity,(None,None))[0] is reference: store._history.pop(identity,None)
        self._history[identity]=(ref(obj,cleanup),state)

    def _release(self,obj: object) -> None:
        with _OWNER_LOCK:
            if _OWNERS.get(id(obj)) is self: _OWNERS.pop(id(obj),None)

    def _reindex(self) -> None:
        self.identities.clear()
        for record in self.records.values():
            identity=self._identity(record.mapping,record.baseline)
            if identity is not None: self.identities[identity]=record
