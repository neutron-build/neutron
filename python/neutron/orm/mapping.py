"""Explicit scalar dataclass mapping; no inheritance or relationship instrumentation."""
from __future__ import annotations
from dataclasses import MISSING, fields, is_dataclass
import types
from types import MappingProxyType
from typing import Any, Generic, Mapping, TypeVar, get_args, get_origin, get_type_hints, Union
from .core import Column, OMIT, OrmError, Table

T=TypeVar('T')

class ModelMapping(Generic[T]):
    def __init__(self,model_type: type[T],table: Table,field_columns: Mapping[str,Column[Any]],*,primary_key: tuple[str,...]) -> None:
        if not is_dataclass(model_type): raise ValueError('scalar mapping requires a dataclass type')
        declared={f.name:f for f in fields(model_type)}
        if not field_columns or not primary_key or len(primary_key)!=len(set(primary_key)):
            raise ValueError('mapping and unique primary-key fields required')
        hints=get_type_hints(model_type)
        physical=set()
        for name,column in field_columns.items():
            if name not in declared or column.table is not table or table.columns.get(column.name) is not column or column.name in physical:
                raise ValueError('mapping requires unique declared fields and owned columns')
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
        params=getattr(model_type,'__dataclass_params__')
        if params.frozen: raise ValueError('mutable mapped dataclass required')
        self.model_type=model_type;self.table=table
        self.field_columns=MappingProxyType(dict(field_columns));self.primary_key=primary_key

    def snapshot(self,obj: T) -> dict[str,Any]:
        if type(obj) is not self.model_type: raise ValueError('mapped model type mismatch; inheritance not implemented')
        return {name:getattr(obj,name) for name in self.field_columns}

    def key(self,values: Mapping[str,Any]) -> tuple[Any,...] | None:
        key=tuple(values[name] for name in self.primary_key)
        return None if any(v is None or v is OMIT for v in key) else key

    def construct(self,row: Mapping[str,Any]) -> T:
        values={}
        for name,column in self.field_columns.items():
            value=row[column.name];column.spec.check(value);values[name]=value
        return self.model_type(**values)

    def restore(self,obj: T,values: Mapping[str,Any]) -> None:
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
