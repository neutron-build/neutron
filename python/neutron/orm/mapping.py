"""Explicit scalar dataclass mapping; no inheritance or relationship instrumentation."""
from __future__ import annotations
from dataclasses import MISSING, dataclass, fields, is_dataclass
import types
import inspect
from types import MappingProxyType
from typing import Any, Generic, Mapping, TypeVar, get_args, get_origin, get_type_hints, Union
from .core import Column, ColumnSpec, OMIT, OrmError, Table
from .json_value import JsonDocument
from decimal import Decimal
import datetime as dt

def same_value(left: Any,right: Any) -> bool:
    if isinstance(left,Decimal) and isinstance(right,Decimal) and left.is_nan() and right.is_nan(): return True
    return bool(left == right)

T=TypeVar('T')

@dataclass(frozen=True,eq=False,init=False)
class ModelMapping(Generic[T]):
    model_type: type[T]
    table: Table
    field_columns: Mapping[str,Column[Any]]
    primary_key: tuple[str,...]
    def __init__(self,model_type: type[T],table: Table,field_columns: Mapping[str,Column[Any]],*,primary_key: tuple[str,...]) -> None:
        if type(table) is not Table: raise ValueError('mapping requires an ordinary physical Table')
        if not is_dataclass(model_type): raise ValueError('scalar mapping requires a dataclass type')
        _validate_attribute_profile(model_type,tuple(field_columns))
        if not hasattr(model_type,'__weakref__'): raise ValueError('mapped dataclass needs weak reference support')
        if any(is_dataclass(base) for base in model_type.__bases__): raise ValueError('mapped inheritance unsupported in scalar profile')
        declared={f.name:f for f in fields(model_type)}
        if not field_columns or not primary_key or len(primary_key)!=len(set(primary_key)):
            raise ValueError('mapping and unique primary-key fields required')
        hints=get_type_hints(model_type)
        physical=set()
        for name,column in field_columns.items():
            if name not in declared or column.table is not table or table.columns.get(column.name) is not column or column.name in physical:
                raise ValueError('mapping requires unique declared fields and owned columns')
            if not declared[name].init: raise ValueError("mapped init=False dataclass fields unsupported")
            if column.spec.sql_type == 'json': raise ValueError('mapped JSON requires jsonb equality semantics')
            physical.add(column.name)
            annotation=hints.get(name)
            alternatives=set(get_args(annotation)) if get_origin(annotation) in {types.UnionType,Union} else {annotation}
            if column.spec.python_type not in alternatives or alternatives - {column.spec.python_type,type(None)}:
                raise ValueError('dataclass field type disagrees with scalar column')
            if column.spec.nullable and type(None) not in alternatives:
                raise ValueError('nullable column requires optional dataclass field')
        for name,field in declared.items():
            if name not in field_columns and field.init and field.default is MISSING and field.default_factory is MISSING:
                raise ValueError('required constructor field is unmapped')
        for name in primary_key:
            if name not in field_columns or field_columns[name].spec.nullable:
                raise ValueError('primary-key fields must be mapped nonnullable columns')
            if field_columns[name].spec.sql_type in {'json','jsonb'}:
                raise ValueError('JSON primary keys unsupported')
        params=getattr(model_type,'__dataclass_params__')
        if params.frozen: raise ValueError('mutable mapped dataclass required')
        object.__setattr__(self,"model_type",model_type)
        object.__setattr__(self,"table",table)
        object.__setattr__(self,"field_columns",MappingProxyType(dict(field_columns)))
        object.__setattr__(self,"primary_key",tuple(primary_key))

    def snapshot(self,obj: T) -> dict[str,Any]:
        _validate_attribute_profile(self.model_type,tuple(self.field_columns))
        if type(obj) is not self.model_type: raise ValueError('mapped model type mismatch; inheritance not implemented')
        return {name:getattr(obj,name) for name in self.field_columns}

    def key(self,values: Mapping[str,Any]) -> tuple[Any,...] | None:
        if any(self.field_columns[name].spec.sql_type in {'json','jsonb'} for name in self.primary_key):
            raise ValueError("JSON primary keys unsupported")
        key=[]
        for name in self.primary_key:
            value=values[name]
            if value is None or value is OMIT: return None
            column=self.field_columns[name];column.spec.check(value)
            if isinstance(value,Decimal) and not value.is_finite(): raise ValueError("nonfinite numeric primary keys unsupported")
            if column.spec.sql_type=='timestamptz': value=value.astimezone(dt.timezone.utc)
            key.append(value)
        return tuple(key)

    def construct(self,row: Mapping[str,Any]) -> T:
        _validate_attribute_profile(self.model_type,tuple(self.field_columns))
        values={}
        for name,column in self.field_columns.items():
            value=row[column.name];column.spec.check(value);values[name]=value
        obj=self.model_type(**values)
        _validate_attribute_profile(self.model_type,tuple(self.field_columns))
        if any(not same_value(getattr(obj,name),value) for name,value in values.items()):
            raise ValueError("model constructor changed persisted field values")
        return obj

    def restore(self,obj: T,values: Mapping[str,Any]) -> None:
        _validate_attribute_profile(self.model_type,tuple(self.field_columns))
        for name,value in values.items(): setattr(obj,name,value)

    def writes(self,obj: T,*,inserting: bool) -> dict[str,Any]:
        values=self.snapshot(obj);result={}
        for name,column in self.field_columns.items():
            value=values[name]
            if column.spec.generated:
                if inserting and value is not None and value is not OMIT:
                    raise ValueError('generated dataclass field must be unset for insert')
                continue
            column.spec.check(value)
            result[column.name]=value
        return result


def _json_equal(left: Any,right: Any) -> bool:
    if isinstance(left,bool) or isinstance(right,bool): return type(left) is bool and type(right) is bool and left is right
    if isinstance(left,(int,Decimal)) and isinstance(right,(int,Decimal)):
        return Decimal(left)==Decimal(right)
    if type(left) is not type(right): return False
    if isinstance(left,list): return len(left)==len(right) and all(_json_equal(a,b) for a,b in zip(left,right))
    if isinstance(left,dict): return left.keys()==right.keys() and all(_json_equal(left[key],right[key]) for key in left)
    return bool(left==right)


def same_column_value(spec: ColumnSpec[Any],left: Any,right: Any) -> bool:
    """Exact native scalar semantics for validating an unknown attach baseline."""
    spec.check(left);spec.check(right)
    if left is None or right is None: return left is right
    if type(left) is not spec.python_type or type(right) is not spec.python_type: return False
    if spec.sql_type=='timestamptz': return bool(left.astimezone(dt.timezone.utc)==right.astimezone(dt.timezone.utc))
    if spec.sql_type=='jsonb':
        if not isinstance(left,JsonDocument) or not isinstance(right,JsonDocument): return False
        return _json_equal(left.parsed(),right.parsed())
    return same_value(left,right)


def _validate_attribute_profile(model_type: type[Any],names: tuple[str,...]) -> None:
    """Finite scalar profile: attribute reads/restores cannot invoke user hooks."""
    if type(model_type) is not type: raise ValueError('mapped custom metaclasses unsupported')
    for name,expected in (('__getattribute__',object.__getattribute__),('__setattr__',object.__setattr__),('__delattr__',object.__delattr__)):
        if inspect.getattr_static(model_type,name) is not expected:
            raise ValueError('mapped custom attribute access/mutation hooks unsupported')
    if any('__getattr__' in vars(base) for base in model_type.__mro__):
        raise ValueError('mapped custom attribute access hooks unsupported')
    missing=object()
    for name in names:
        descriptor=inspect.getattr_static(model_type,name,missing)
        if descriptor is missing: continue
        if isinstance(descriptor,types.MemberDescriptorType):
            if descriptor.__objclass__ not in model_type.__mro__ or descriptor.__name__!=name:
                raise ValueError('mapped foreign/aliased slot descriptor unsupported')
            continue
        if any(inspect.getattr_static(type(descriptor),method,missing) is not missing for method in ('__get__','__set__','__delete__')):
            raise ValueError('mapped custom field descriptors unsupported')
