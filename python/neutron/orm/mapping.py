"""Explicit scalar dataclass mapping; no inheritance or relationship instrumentation."""
from __future__ import annotations
from copy import deepcopy
from dataclasses import MISSING, dataclass, fields, is_dataclass
import types
import inspect
from types import MappingProxyType
from typing import Any, Generic, Mapping, TypeVar, get_args, get_origin, get_type_hints, Union
from .core import Column, ColumnSpec, OMIT, OrmError, Table, _column_type
from .json_value import JsonDocument, MutableJson
from .pg_value import PgArray, PgRange
from .catalog_value import PgDomain, PgEnum
from .composite_value import PgComposite
from .vector_value import PgVector
from .multirange_value import PgMultirange
from .instrumentation import _MappedField, instrument_model, raw_values, restore_values
from decimal import Decimal
import datetime as dt

def same_value(left: Any,right: Any) -> bool:
    if isinstance(left,PgComposite) and isinstance(right,PgComposite):
        return left.identity is right.identity and len(left.fields)==len(right.fields) and all(same_value(a,b) for a,b in zip(left.fields,right.fields))
    if isinstance(left,PgMultirange) and isinstance(right,PgMultirange):
        return left.identity is right.identity and len(left.ranges)==len(right.ranges) and all(same_value(a,b) for a,b in zip(left.ranges,right.ranges))
    if isinstance(left,PgDomain) and isinstance(right,PgDomain):
        return left.identity is right.identity and same_value(left.value,right.value)
    if isinstance(left,PgArray) and isinstance(right,PgArray):
        return left.dimensions==right.dimensions and len(left.elements)==len(right.elements) and all(same_value(a,b) for a,b in zip(left.elements,right.elements))
    if isinstance(left,PgRange) and isinstance(right,PgRange):
        return (left.empty,left.lower_inclusive,left.upper_inclusive)==(right.empty,right.lower_inclusive,right.upper_inclusive) and same_value(left.lower,right.lower) and same_value(left.upper,right.upper)
    if isinstance(left,Decimal) and isinstance(right,Decimal) and left.is_nan() and right.is_nan(): return True
    return bool(left == right)

T=TypeVar('T')

@dataclass(frozen=True,eq=False,init=False)
class ModelMapping(Generic[T]):
    model_type: type[T]
    table: Table
    field_columns: Mapping[str,Column[Any]]
    primary_key: tuple[str,...]
    version_field: str | None
    instrumented: bool
    def __init__(self,model_type: type[T],table: Table,field_columns: Mapping[str,Column[Any]],*,primary_key: tuple[str,...],version_field: str | None=None,instrumented: bool=False) -> None:
        if type(instrumented) is not bool: raise ValueError('instrumented requires a boolean')
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
            values=alternatives-{type(None)}
            if len(values)!=1: raise ValueError('dataclass field type disagrees with scalar column')
            annotation_type=next(iter(values))
            _column_type(column.spec,annotation_type)
            if column.spec.python_type in {PgArray,PgRange,PgDomain} and get_origin(annotation_type) is not column.spec.python_type:
                raise ValueError('mapped arrays/ranges require a declared element type')
            if column.spec.nullable and type(None) not in alternatives:
                raise ValueError('nullable column requires optional dataclass field')
        for name,field in declared.items():
            if name not in field_columns and field.init and field.default is MISSING and field.default_factory is MISSING:
                raise ValueError('required constructor field is unmapped')
        for name in primary_key:
            if name not in field_columns or field_columns[name].spec.nullable:
                raise ValueError('primary-key fields must be mapped nonnullable columns')
            if not _primary_profile(field_columns[name].spec):
                raise ValueError('primary-key codec equality is outside qualified identity profile')
        if version_field is not None:
            if version_field not in field_columns or version_field in primary_key:
                raise ValueError('version field must be mapped outside the primary key')
            spec=field_columns[version_field].spec
            if spec.sql_type not in {'int2','int4','int8'} or spec.nullable or spec.generated:
                raise ValueError('version requires a nonnullable application integer column')
        params=getattr(model_type,'__dataclass_params__')
        if params.frozen: raise ValueError('mutable mapped dataclass required')
        object.__setattr__(self,"model_type",model_type)
        object.__setattr__(self,"table",table)
        object.__setattr__(self,"field_columns",MappingProxyType(dict(field_columns)))
        object.__setattr__(self,"primary_key",tuple(primary_key))
        object.__setattr__(self,"version_field",version_field)
        object.__setattr__(self,"instrumented",instrumented)
        if instrumented: instrument_model(model_type,tuple(field_columns))

    def accepts(self,obj: object) -> bool: return type(obj) is self.model_type

    def active_fields(self,obj: object) -> tuple[str,...]:
        if not self.accepts(obj): raise ValueError('object is outside mapping')
        return tuple(self.field_columns)

    def snapshot(self,obj: T) -> dict[str,Any]:
        _validate_attribute_profile(self.model_type,tuple(self.field_columns))
        if type(obj) is not self.model_type: raise ValueError('mapped model type mismatch; inheritance not implemented')
        return deepcopy({name:getattr(obj,name) for name in self.field_columns})

    def _snapshot(self,obj: T) -> dict[str,Any]:
        _validate_attribute_profile(self.model_type,tuple(self.field_columns))
        if type(obj) is not self.model_type: raise ValueError('mapped model type mismatch')
        return deepcopy(raw_values(obj,tuple(self.field_columns)))

    def key(self,values: Mapping[str,Any]) -> tuple[Any,...] | None:
        if any(not _primary_profile(self.field_columns[name].spec) for name in self.primary_key):
            raise ValueError('primary-key codec equality is outside qualified identity profile')
        key=[]
        for name in self.primary_key:
            value=values[name]
            if value is None or value is OMIT: return None
            column=self.field_columns[name];column.spec.check(value)
            value=_identity_value(column.spec,value)
            key.append(value)
        return tuple(key)

    def construct(self,row: Mapping[str,Any]) -> T:
        _validate_attribute_profile(self.model_type,tuple(self.field_columns))
        values={}
        for name,column in self.field_columns.items():
            value=column.spec.decode(row[column.name]);values[name]=value
        expected=deepcopy(values)
        obj=self.model_type(**values)
        _validate_attribute_profile(self.model_type,tuple(self.field_columns))
        if any(not same_value(getattr(obj,name),value) for name,value in expected.items()):
            raise ValueError("model constructor changed persisted field values")
        return obj

    def restore(self,obj: T,values: Mapping[str,Any]) -> None:
        _validate_attribute_profile(self.model_type,tuple(self.field_columns))
        restore_values(obj,deepcopy(dict(values)))

    def writes(self,obj: T,*,inserting: bool,deferred_fields: frozenset[str]=frozenset()) -> dict[str,Any]:
        if deferred_fields - self.field_columns.keys(): raise ValueError('deferred fields outside mapping')
        values=self.snapshot(obj);result={}
        for name,column in self.field_columns.items():
            value=values[name]
            if name in deferred_fields:
                if column.spec.generated: raise ValueError('generated child fields cannot be relation targets')
                continue
            if column.spec.generated:
                if inserting and value is not None and value is not OMIT:
                    raise ValueError('generated dataclass field must be unset for insert')
                continue
            column.spec.check(value)
            result[column.name]=value
        return result


def _primary_profile(spec: ColumnSpec[Any]) -> bool:
    if spec.domain_base is not None: return _primary_profile(spec.domain_base)
    return spec.sql_type not in {'json','jsonb','interval'} and spec.python_type not in {PgArray,PgRange,PgDomain,PgComposite,PgVector,PgMultirange}


def _identity_value(spec: ColumnSpec[Any],value: Any) -> Any:
    if isinstance(value,PgDomain):
        assert spec.domain_base is not None
        return PgDomain(_identity_value(spec.domain_base,value.value),value.identity)
    if isinstance(value,Decimal) and not value.is_finite(): raise ValueError('nonfinite numeric primary keys unsupported')
    return value.astimezone(dt.timezone.utc) if spec.sql_type=='timestamptz' else value


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
        if not isinstance(left,(JsonDocument,MutableJson)) or not isinstance(right,(JsonDocument,MutableJson)): return False
        return _json_equal(left.parsed(),right.parsed())
    return same_value(left,right)


def _validate_attribute_profile(model_type: type[Any],names: tuple[str,...],*,allow_inherited_instrumentation: bool=False) -> None:
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
        if type(descriptor) is _MappedField:
            if (descriptor.owner is not model_type and not (allow_inherited_instrumentation and descriptor.owner in model_type.__mro__)) or descriptor.name!=name: raise ValueError('foreign mapped instrumentation unsupported')
            continue
        if isinstance(descriptor,types.MemberDescriptorType):
            if descriptor.__objclass__ not in model_type.__mro__ or descriptor.__name__!=name:
                raise ValueError('mapped foreign/aliased slot descriptor unsupported')
            continue
        if any(inspect.getattr_static(type(descriptor),method,missing) is not missing for method in ('__get__','__set__','__delete__')):
            raise ValueError('mapped custom field descriptors unsupported')
