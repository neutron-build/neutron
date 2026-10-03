"""Pure mapped-object transitions shared by synchronous and async Sessions."""
from __future__ import annotations
from copy import deepcopy
from dataclasses import dataclass
from enum import Enum
from typing import Any, Generic, TypeVar
from .core import OrmError
from .mapping import ModelMapping, same_value
T=TypeVar('T')

class ObjectState(str,Enum):
    TRANSIENT='transient'
    PENDING='pending'
    PERSISTENT='persistent'
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

class StateStore:
    def __init__(self) -> None:
        self.records: dict[int,Record[Any]]={}
        self.identities: dict[tuple[Any,...],Record[Any]]={}

    def _identity(self,mapping: ModelMapping[Any],values: dict[str,Any]) -> tuple[Any,...] | None:
        key=mapping.key(values)
        return None if key is None else (mapping.model_type,mapping.table.schema,mapping.table.name,*key)

    def attach(self,mapping: ModelMapping[T],obj: T,*,new: bool) -> Record[T]:
        if id(obj) in self.records: raise OrmError('object already tracked')
        values=deepcopy(mapping.snapshot(obj))
        identity=self._identity(mapping,values)
        if identity is not None and identity in self.identities: raise OrmError('mapped identity already tracked')
        record=Record(mapping,obj,ObjectState.PENDING if new else ObjectState.PERSISTENT,values.copy(),values,new)
        self.records[id(obj)]=record
        if identity is not None: self.identities[identity]=record
        return record

    def find(self,mapping: ModelMapping[T],key: tuple[Any,...]) -> T | None:
        record=self.identities.get((mapping.model_type,mapping.table.schema,mapping.table.name,*key))
        if record is None: return None
        if record.state in {ObjectState.DELETED,ObjectState.INDETERMINATE}: raise OrmError('tracked identity unavailable')
        return record.obj

    def dirty(self,record: Record[Any]) -> dict[str,Any]:
        current=record.mapping.snapshot(record.obj)
        if record.mapping.key(current)!=record.mapping.key(record.baseline): raise OrmError('primary-key mutation unsupported')
        return {name:value for name,value in current.items() if not same_value(value,record.baseline[name])}

    def flushed(self,record: Record[Any],values: dict[str,Any]) -> None:
        record.mapping.restore(record.obj,values)
        record.baseline=deepcopy(values)
        record.state=ObjectState.PERSISTENT
        identity=self._identity(record.mapping,values)
        if identity is None: raise OrmError('flushed row has incomplete primary key')
        other=self.identities.get(identity)
        if other is not None and other is not record: raise OrmError('generated identity collision')
        self.identities[identity]=record

    def committed(self) -> None:
        for record in list(self.records.values()):
            if record.state is ObjectState.DELETED:
                self.records.pop(id(record.obj));continue
            record.original=deepcopy(record.baseline);record.was_new=False
        self._reindex()

    def rollback(self) -> None:
        for record in list(self.records.values()):
            record.mapping.restore(record.obj,deepcopy(record.original))
            record.baseline=deepcopy(record.original)
            if record.was_new:
                record.state=ObjectState.TRANSIENT;self.records.pop(id(record.obj))
            else: record.state=ObjectState.PERSISTENT
        self._reindex()

    def uncertain(self) -> None:
        for record in self.records.values(): record.state=ObjectState.INDETERMINATE

    def detach_all(self) -> None:
        for record in self.records.values(): record.state=ObjectState.DETACHED
        self.records.clear();self.identities.clear()

    def _reindex(self) -> None:
        self.identities.clear()
        for record in self.records.values():
            identity=self._identity(record.mapping,record.baseline)
            if identity is not None: self.identities[identity]=record
