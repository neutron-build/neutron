"""Explicit single-table discriminator registry and shared base identity."""
from __future__ import annotations
from copy import deepcopy
from dataclasses import MISSING,dataclass,fields,is_dataclass
from types import MappingProxyType,UnionType
from typing import Any,Generic,Mapping,TypeVar,Union,cast,get_args,get_origin,get_type_hints
from .core import Column,Table,_column_type
from .instrumentation import instrument_model,raw_values,restore_values
from .mapping import ModelMapping,_validate_attribute_profile,same_value
from .query import Query,Scope,field
from .relations import _MappedDecoder

B=TypeVar('B')
S=TypeVar('S')

@dataclass(frozen=True)
class PolymorphicView(Generic[S]):
    family: PolymorphicMapping[Any]
    model_type: type[S]
    tag: str
    def __post_init__(self) -> None:
        if self.family._tags.get(self.model_type)!=self.tag: raise ValueError('subtype view must belong to the declared registry')
    def query(self) -> Query[S]:
        query=self.family.query().where(self.family.field_columns[self.family.discriminator].eq(self.tag))
        return cast(Query[S],query)

class PolymorphicMapping(ModelMapping[B]):
    discriminator: str
    variants: Mapping[str,type[B]]
    _tags: Mapping[type[Any],str]
    _roles: Mapping[type[Any],tuple[str,...]]
    _required: Mapping[type[Any],frozenset[str]]
    immutable_fields: frozenset[str]

    def __init__(self,base_type: type[B],table: Table,field_columns: Mapping[str,Column[Any]],*,primary_key: tuple[str,...],discriminator: str,variants: Mapping[str,type[B]],version_field: str|None=None,instrumented: bool=False) -> None:
        if not is_dataclass(base_type) or type(base_type) is not type: raise ValueError('polymorphic base requires an ordinary dataclass')
        base_names={item.name for item in fields(base_type)}
        common={name:column for name,column in field_columns.items() if name in base_names}
        ModelMapping.__init__(self,base_type,table,common,primary_key=primary_key,version_field=version_field,instrumented=False)
        if discriminator not in common or discriminator in primary_key or discriminator==version_field: raise ValueError('discriminator must be a shared nonidentity field')
        spec=common[discriminator].spec
        if spec.python_type is not str or spec.nullable or spec.generated or spec.sql_type not in {'text','varchar'}:
            raise ValueError('discriminator requires a nonnullable application text column')
        if not variants or len(set(variants.values()))!=len(variants): raise ValueError('one unique immutable discriminator per registered class required')
        roles: dict[type[Any],tuple[str,...]]={};required: dict[type[Any],frozenset[str]]={}
        physical=set()
        for column in field_columns.values():
            if column.table is not table or table.columns.get(column.name) is not column or column.name in physical: raise ValueError('polymorphic fields require unique owned columns')
            physical.add(column.name)
        for tag,model in variants.items():
            if type(tag) is not str or not tag or type(model) is not type or not issubclass(model,base_type) or not is_dataclass(model) or getattr(model,'__dataclass_params__').frozen or not hasattr(model,'__weakref__'):
                raise ValueError('registered discriminator classes must be mutable weak-reference dataclasses in the base hierarchy')
            declarations={item.name:item for item in fields(model)}
            names=tuple(name for name in field_columns if name in declarations)
            if not set(common)<=set(names): raise ValueError('variants must share all base fields')
            _validate_attribute_profile(model,names,allow_inherited_instrumentation=instrumented);hints=get_type_hints(model);nonnull=set()
            for name in names:
                if not declarations[name].init: raise ValueError('polymorphic init=False fields unsupported')
                annotation=hints.get(name);alternatives=set(get_args(annotation)) if get_origin(annotation) in {Union,UnionType} else {annotation}
                types=alternatives-{type(None)}
                if len(types)!=1: raise ValueError('polymorphic annotation requires one native type with optional NULL')
                _column_type(field_columns[name].spec,next(iter(types)))
                if type(None) not in alternatives: nonnull.add(name)
                if field_columns[name].spec.sql_type=='json': raise ValueError('mapped JSON requires jsonb equality semantics')
            for name,column in field_columns.items():
                if name not in declarations and (not column.spec.nullable or column.spec.generated or (column.spec.native_type is not None and column.spec.native_type.required)): raise ValueError('inactive subtype fields require nullable nongenerated columns')
            for name,item in declarations.items():
                if name not in field_columns and item.init and item.default is MISSING and item.default_factory is MISSING: raise ValueError('required subtype constructor field is unmapped')
            roles[model]=names;required[model]=frozenset(nonnull)
        if any(name not in set().union(*(set(names) for names in roles.values())) for name in field_columns): raise ValueError('mapped subtype field belongs to no registered class')
        object.__setattr__(self,'field_columns',MappingProxyType(dict(field_columns)))
        object.__setattr__(self,'discriminator',discriminator);object.__setattr__(self,'variants',MappingProxyType(dict(variants)))
        object.__setattr__(self,'_tags',MappingProxyType({model:tag for tag,model in variants.items()}));object.__setattr__(self,'_roles',MappingProxyType(roles));object.__setattr__(self,'_required',MappingProxyType(required))
        object.__setattr__(self,'immutable_fields',frozenset({discriminator}));object.__setattr__(self,'instrumented',instrumented)
        if type(instrumented) is not bool: raise ValueError('instrumented requires boolean')
        if instrumented:
            # Each class owns its descriptors, including inherited dataclass slots.
            for model,names in roles.items(): instrument_model(model,names,own_inherited=True)

    def accepts(self,obj: object) -> bool: return type(obj) in self._roles

    def active_fields(self,obj: object) -> tuple[str,...]: return self._roles[self._class(obj)]

    def subtype(self,model_type: type[S]) -> PolymorphicView[S]:
        if model_type not in self._tags: raise ValueError('subtype is outside explicit discriminator registry')
        return PolymorphicView(self,model_type,self._tags[model_type])

    def _class(self,obj: object) -> type[Any]:
        model=type(obj)
        if model not in self._roles: raise ValueError('object class is outside explicit discriminator registry')
        _validate_attribute_profile(model,self._roles[model]);return model

    def _shape(self,model: type[Any],values: Mapping[str,Any]) -> None:
        if values[self.discriminator]!=self._tags[model]: raise ValueError('discriminator mutation or class/tag mismatch refused')
        for name,column in self.field_columns.items():
            value=values[name]
            if name not in self._roles[model] and value is not None: raise ValueError('inactive subtype column carries a value')
            if name in self._required[model] and value is None and not column.spec.generated: raise ValueError('required active subtype field is NULL')

    def snapshot(self,obj: B) -> dict[str,Any]:
        model=self._class(obj);values={name:getattr(obj,name) if name in self._roles[model] else None for name in self.field_columns}
        self._shape(model,values);return deepcopy(values)

    def _snapshot(self,obj: B) -> dict[str,Any]:
        model=self._class(obj);active=raw_values(obj,self._roles[model]);values={name:active.get(name) for name in self.field_columns}
        self._shape(model,values);return deepcopy(values)

    def construct(self,row: Mapping[str,Any]) -> B:
        values={name:column.spec.decode(row[column.name]) for name,column in self.field_columns.items()}
        tag=values[self.discriminator]
        if not isinstance(tag,str): raise ValueError('native discriminator requires text')
        model=self.variants.get(tag)
        if model is None: raise ValueError('unknown native discriminator value')
        self._shape(model,values);expected=deepcopy(values)
        obj: B=model(**{name:deepcopy(values[name]) for name in self._roles[model]})
        actual=self.snapshot(obj)
        if any(not same_value(actual[name],value) for name,value in expected.items()): raise ValueError('subtype constructor changed persisted fields')
        return obj

    def restore(self,obj: B,values: Mapping[str,Any]) -> None:
        model=self._class(obj);self._shape(model,values)
        restore_values(obj,deepcopy({name:values[name] for name in self._roles[model]}))

    def query(self) -> Query[B]:
        return Query(Scope(self.table),tuple(field(column) for column in self.field_columns.values()),_MappedDecoder(self))
