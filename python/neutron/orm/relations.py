"""Explicit bounded relation reads; no Session graph writes or lazy loading."""
from __future__ import annotations
from collections import Counter
from dataclasses import dataclass
from typing import Any, Generic, Mapping, Sequence, TypeVar
from uuid import UUID
from .client import AsyncDatabase, Database
from .core import CardinalityError, OrmError, Predicate
from .mapping import ModelMapping
from .query import Query, Scope, field

P=TypeVar('P')
C=TypeVar('C')
Key=tuple[tuple[type,object],...]

class RelationBudgetError(OrmError): pass
class UnsupportedRelationError(OrmError): pass

@dataclass(frozen=True)
class LoadBudget:
    max_parents: int
    max_rows: int
    batch_size: int

    def check(self) -> None:
        if any(type(value) is not int or not 0 < value < 2**63-1 for value in (self.max_parents,self.max_rows,self.batch_size)):
            raise ValueError('positive finite relation budgets required')

@dataclass(frozen=True)
class Association(Generic[P,C]):
    parent: P
    children: tuple[C,...]

@dataclass(frozen=True)
class _MappedDecoder(Generic[C]):
    mapping: ModelMapping[C]
    def __call__(self,row: Mapping[str,Any]) -> C:
        physical={column.name:row['p'+str(i)] for i,column in enumerate(self.mapping.field_columns.values())}
        return self.mapping.construct(physical)

@dataclass(frozen=True)
class Relation(Generic[P,C]):
    parent: ModelMapping[P]
    child: ModelMapping[C]
    parent_fields: tuple[str,...]
    child_fields: tuple[str,...]

    def __post_init__(self) -> None:
        if not isinstance(self.parent_fields,tuple) or not isinstance(self.child_fields,tuple):
            raise ValueError('relation key declarations must be immutable tuples')
        if not self.parent_fields or len(self.parent_fields)!=len(self.child_fields) or len(set(self.parent_fields))!=len(self.parent_fields) or len(set(self.child_fields))!=len(self.child_fields):
            raise ValueError('unique paired composite relation keys required')
        for left,right in zip(self.parent_fields,self.child_fields):
            if left not in self.parent.field_columns or right not in self.child.field_columns:
                raise ValueError('relation field outside mapped scope')
            a=self.parent.field_columns[left];b=self.child.field_columns[right]
            if a.spec.nullable or b.spec.nullable or a.spec.python_type is not b.spec.python_type or a.spec.sql_type!=b.spec.sql_type or a.spec.python_type not in {int,str,bool,UUID}:
                raise UnsupportedRelationError('relation keys require identical nonnullable integer/string/bool/UUID columns')

    def inverse(self) -> Relation[C,P]:
        return Relation(self.child,self.parent,self.child_fields,self.parent_fields)

    def query(self) -> Query[C]:
        columns=tuple(field(column) for column in self.child.field_columns.values())
        return Query(Scope(self.child.table),columns,_MappedDecoder(self.child))

    def _key(self,mapping: ModelMapping[Any],names: tuple[str,...],obj: object) -> Key:
        if type(obj) is not mapping.model_type:
            raise ValueError('relation object type outside mapped model')
        key=[]
        for name in names:
            value=getattr(obj,name);column=mapping.field_columns[name]
            column.spec.check(value)
            if type(value) is not column.spec.python_type:
                raise ValueError('relation requires exact scalar key types')
            key.append((type(value),value))
        return tuple(key)

@dataclass
class _Load(Generic[P,C]):
    relation: Relation[P,C]
    parents: tuple[P,...]
    query: Query[C]
    budget: LoadBudget
    singular: bool
    keys: tuple[Key,...]
    distinct: tuple[Key,...]
    multiplicity: Counter[Key]
    by_key: dict[Key,list[C]]
    total: int=0

    def batch(self,start: int) -> tuple[Query[C],frozenset[Key]]:
        requested=self.distinct[start:start+self.budget.batch_size]
        combined: Predicate | None=None
        for key in requested:
            term: Predicate | None=None
            for name,(_,value) in zip(self.relation.child_fields,key):
                item=self.relation.child.field_columns[name].eq(value)
                term=item if term is None else term & item
            if term is None: raise ValueError('empty relation key')
            combined=term if combined is None else combined | term
        if combined is None: raise ValueError('empty relation batch')
        query=self.query.where(combined).limit(self.budget.max_rows-self.total+1)
        if len(query.compile().params)>65535:
            raise UnsupportedRelationError('relation batch exceeds PostgreSQL parameter limit')
        return query,frozenset(requested)

    def consume(self,children: Sequence[C],requested: frozenset[Key]) -> None:
        if len(children)>self.budget.max_rows-self.total:
            raise RelationBudgetError('expanded relation row budget exceeded')
        for child in children:
            key=self.relation._key(self.relation.child,self.relation.child_fields,child)
            if key not in requested: raise OrmError('returned relation key was not requested')
            if self.multiplicity[key]>self.budget.max_rows-self.total:
                raise RelationBudgetError('expanded relation row budget exceeded')
            self.total+=self.multiplicity[key]
            group=self.by_key.setdefault(key,[]);group.append(child)
            if self.singular and len(group)>1: raise CardinalityError('expected at most one related child')

    def finish(self) -> tuple[Association[P,C],...]:
        # Copies follow budget validation; duplicate parents do not share mutable
        # dataclass child instances, and no tracked Session identity is inferred.
        result=[]
        for parent,key in zip(self.parents,self.keys):
            children=tuple(self.relation.child.construct({column.name:getattr(child,name) for name,column in self.relation.child.field_columns.items()}) for child in self.by_key.get(key,()))
            result.append(Association(parent,children))
        return tuple(result)


def _prepare(relation: Relation[P,C],parents: Sequence[P],query: Query[C] | None,budget: LoadBudget,singular: bool) -> _Load[P,C]:
    relation.__post_init__();budget.check()
    if len(parents)>budget.max_parents: raise RelationBudgetError('relation parent budget exceeded')
    query=relation.query() if query is None else query
    if query.row_limit is not None or query.row_offset is not None:
        raise UnsupportedRelationError('per-parent pagination unsupported; global limit/offset refused')
    if query.scope.table is not relation.child.table or query.scope.joins or not isinstance(query.decoder,_MappedDecoder) or query.decoder.mapping is not relation.child:
        raise UnsupportedRelationError('relation load requires its own mapped child query without joins')
    expected=tuple(relation.child.field_columns.values())
    if len(query.fields)!=len(expected) or any(item.outer or item.column is not column for item,column in zip(query.fields,expected)):
        raise UnsupportedRelationError('relation mapped projection cannot be replaced')
    query.compile() # Validate filters/order even for an empty parent input.
    items=tuple(parents)
    if len(items)>budget.max_parents: raise RelationBudgetError('relation parent budget exceeded')
    keys=tuple(relation._key(relation.parent,relation.parent_fields,parent) for parent in items)
    distinct=tuple(dict.fromkeys(keys))
    state=_Load(relation,items,query,budget,singular,keys,distinct,Counter(keys),{})
    # Parameter budget depends on key count and fixed caller filter, not data.
    # Validate the largest batch before dispatching any statement.
    if distinct: state.batch(0)
    return state


def load_many(db: Database,relation: Relation[P,C],parents: Sequence[P],*,budget: LoadBudget,query: Query[C] | None=None) -> tuple[Association[P,C],...]:
    return _load(db,relation,parents,budget,query,False)


def load_one(db: Database,relation: Relation[P,C],parents: Sequence[P],*,budget: LoadBudget,query: Query[C] | None=None) -> tuple[Association[P,C],...]:
    return _load(db,relation,parents,budget,query,True)


def _load(db: Database,relation: Relation[P,C],parents: Sequence[P],budget: LoadBudget,query: Query[C] | None,singular: bool) -> tuple[Association[P,C],...]:
    if db.closed: raise OrmError('connection closed')
    state=_prepare(relation,parents,query,budget,singular)
    for start in range(0,len(state.distinct),budget.batch_size):
        batch,requested=state.batch(start);state.consume(db.all(batch),requested)
    return state.finish()


async def async_load_many(db: AsyncDatabase,relation: Relation[P,C],parents: Sequence[P],*,budget: LoadBudget,query: Query[C] | None=None) -> tuple[Association[P,C],...]:
    return await _async_load(db,relation,parents,budget,query,False)


async def async_load_one(db: AsyncDatabase,relation: Relation[P,C],parents: Sequence[P],*,budget: LoadBudget,query: Query[C] | None=None) -> tuple[Association[P,C],...]:
    return await _async_load(db,relation,parents,budget,query,True)


async def _async_load(db: AsyncDatabase,relation: Relation[P,C],parents: Sequence[P],budget: LoadBudget,query: Query[C] | None,singular: bool) -> tuple[Association[P,C],...]:
    if db.closed: raise OrmError('connection closed')
    state=_prepare(relation,parents,query,budget,singular)
    for start in range(0,len(state.distinct),budget.batch_size):
        batch,requested=state.batch(start);state.consume(await db.all(batch),requested)
    return state.finish()
