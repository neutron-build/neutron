"""Finite library-owned scalar descriptors: expired reads never perform I/O."""
from __future__ import annotations
import inspect
import threading
import types
from typing import Any
from weakref import ref
from .core import OrmError

class ExpiredAttributeError(OrmError): pass

_EXPIRED: dict[int,tuple[Any,frozenset[str]]]={}
_LOCK=threading.RLock()
_MISSING=object()

def expire_attributes(obj: object,names: frozenset[str]) -> None:
    identity=id(obj)
    def cleanup(reference: Any) -> None:
        with _LOCK:
            if _EXPIRED.get(identity,(None,None))[0] is reference: _EXPIRED.pop(identity,None)
    with _LOCK:
        if names: _EXPIRED[identity]=(ref(obj,cleanup),names)
        else: _EXPIRED.pop(identity,None)

def _check(obj: object,name: str) -> None:
    with _LOCK:
        state=_EXPIRED.get(id(obj))
        if state is not None and state[0]() is obj and name in state[1]:
            raise ExpiredAttributeError('mapped attribute expired; explicitly refresh before access')

class _MappedField:
    def __init__(self,owner: type[Any],name: str,original: Any) -> None:
        self.owner=owner;self.name=name
        self.slot=original if isinstance(original,types.MemberDescriptorType) else None
        self.default=original if self.slot is None else _MISSING

    def __get__(self,obj: object | None,owner: type[Any] | None=None) -> Any:
        if obj is None: return self
        _check(obj,self.name);return self.read(obj)

    def __set__(self,obj: object,value: Any) -> None:
        _check(obj,self.name);self.write(obj,value)

    def __delete__(self,obj: object) -> None:
        raise OrmError('mapped scalar attribute deletion unsupported')

    def read(self,obj: object) -> Any:
        if self.slot is not None: return self.slot.__get__(obj,self.owner)
        data=object.__getattribute__(obj,'__dict__')
        if self.name in data: return data[self.name]
        if self.default is not _MISSING: return self.default
        raise AttributeError(self.name)

    def write(self,obj: object,value: Any) -> None:
        if self.slot is not None: self.slot.__set__(obj,value)
        else: object.__getattribute__(obj,'__dict__')[self.name]=value

def instrument_model(model: type[Any],names: tuple[str,...]) -> None:
    for name in names:
        existing=inspect.getattr_static(model,name,_MISSING)
        if type(existing) is _MappedField: continue
        setattr(model,name,_MappedField(model,name,existing))

def raw_values(obj: object,names: tuple[str,...]) -> dict[str,Any]:
    values={}
    for name in names:
        descriptor=inspect.getattr_static(type(obj),name,_MISSING)
        values[name]=descriptor.read(obj) if type(descriptor) is _MappedField else getattr(obj,name)
    return values

def restore_values(obj: object,values: dict[str,Any]) -> None:
    for name,value in values.items():
        descriptor=inspect.getattr_static(type(obj),name,_MISSING)
        if type(descriptor) is _MappedField: descriptor.write(obj,value)
        else: setattr(obj,name,value)
    expire_attributes(obj,frozenset())
