"""Explicit PostgreSQL SQL core; no identity map or unit-of-work behavior."""
from __future__ import annotations
from dataclasses import dataclass
import datetime as dt
from decimal import Decimal
from types import MappingProxyType
from typing import Any, Callable, Generic, Iterable, Mapping, TypeVar, cast
from uuid import UUID

T = TypeVar('T')

class _WriteState:
    def __init__(self, name: str) -> None: self.name = name
    def __repr__(self) -> str: return self.name

OMIT = _WriteState('OMIT')
DEFAULT = _WriteState('DEFAULT')

class OrmError(Exception):
    """SQL core error retaining native PostgreSQL SQLSTATE and cause."""
    def __init__(self, message: str, *, sqlstate: str | None = None, outcome: str | None = None) -> None:
        super().__init__(message)
        self.sqlstate = sqlstate
        self.outcome = outcome

class CardinalityError(OrmError): pass
class SessionBusyError(OrmError): pass


def quote(name: str) -> str:
    if not isinstance(name, str) or not name or any(ord(c) < 32 or ord(c) == 127 for c in name):
        raise ValueError('identifier must be nonempty without control characters')
    return '"' + name.replace('"', '""') + '"'

# Column families deliberately finite; custom SQL type text cannot become SQL.
_TYPES: dict[str, type] = {'int2':int,'int4':int,'int8':int,'text':str,'varchar':str,'bool':bool,'numeric':Decimal,'uuid':UUID,'bytea':bytes,'timestamp':dt.datetime,'timestamptz':dt.datetime,'date':dt.date}

@dataclass(frozen=True)
class ColumnSpec(Generic[T]):
    python_type: type[T]
    sql_type: str
    nullable: bool = False
    generated: bool = False

    def __post_init__(self) -> None:
        if _TYPES.get(self.sql_type) is not self.python_type:
            raise ValueError('unsupported or mismatched column type profile')

    def check(self, value: object) -> None:
        if value is None:
            if not self.nullable: raise ValueError('NULL on nonnullable column')
            return
        if not isinstance(value, self.python_type) or (self.python_type is int and isinstance(value, bool)) or (self.sql_type == 'date' and isinstance(value, dt.datetime)):
            raise ValueError('column value has wrong native type')
        if isinstance(value, dt.datetime):
            aware = value.tzinfo is not None and value.utcoffset() is not None
            if aware != (self.sql_type == 'timestamptz'):
                raise ValueError('timestamp local/instant mode mismatch')
        if self.sql_type in {'int2','int4','int8'}:
            bits = {'int2':16,'int4':32,'int8':64}[self.sql_type]
            if not -(2**(bits-1)) <= cast(int,value) < 2**(bits-1): raise ValueError('integer out of PostgreSQL range')


@dataclass(frozen=True, eq=False, init=False)
class Table:
    name: str
    schema: str
    columns: Mapping[str, Column[Any]]
    def __init__(self, name: str, columns: Mapping[str, ColumnSpec[Any]], *, schema: str = 'public') -> None:
        quote(name); quote(schema)
        if not columns: raise ValueError('table requires columns')
        for key in columns: quote(key)
        object.__setattr__(self,"name",name)
        object.__setattr__(self,"schema",schema)
        object.__setattr__(self,"columns",MappingProxyType({key:Column(self,key,spec) for key,spec in columns.items()}))

    @property
    def sql(self) -> str: return f'{quote(self.schema)}.{quote(self.name)}'

    def column(self, name: str, python_type: type[T]) -> Column[T]:
        column = self.columns[name]
        if column.spec.python_type is not python_type or column.spec.nullable: raise ValueError('column type/nullability mismatch; use nullable_column for nullable fields')
        return cast(Column[T],column)

    def nullable_column(self, name: str, python_type: type[T]) -> Column[T | None]:
        column=self.columns[name]
        if column.spec.python_type is not python_type or not column.spec.nullable: raise ValueError("nullable column type mismatch")
        return cast(Column[T | None],column)


@dataclass(frozen=True)
class Predicate:
    sql: str
    params: tuple[object,...] = ()
    owners: frozenset[Table] = frozenset()

    def __and__(self, other: Predicate) -> Predicate:
        return Predicate(f'({self.sql}) AND ({other.sql})',self.params+other.params,self.owners|other.owners)
    def __or__(self, other: Predicate) -> Predicate:
        return Predicate(f'({self.sql}) OR ({other.sql})',self.params+other.params,self.owners|other.owners)
    def __bool__(self) -> bool: raise TypeError('SQL predicates cannot be Python booleans')

@dataclass(frozen=True)
class Column(Generic[T]):
    table: Table
    name: str
    spec: ColumnSpec[T]

    @property
    def sql(self) -> str: return f'{self.table.sql}.{quote(self.name)}'

    def eq(self, value: T | Column[T] | None) -> Predicate:
        if isinstance(value,Column):
            if value.spec.python_type is not self.spec.python_type: raise ValueError('incompatible column comparison')
            return Predicate(f'{self.sql} = {value.sql}',(),frozenset({self.table,value.table}))
        if value is None: return Predicate(f'{self.sql} IS NULL',(),frozenset({self.table}))
        self.spec.check(value)
        return Predicate(f'{self.sql} = %s',(value,),frozenset({self.table}))

    def in_(self, values: Iterable[T]) -> Predicate:
        values=tuple(values)
        if not values: return Predicate('FALSE',(),frozenset({self.table}))
        for value in values: self.spec.check(value)
        return Predicate(f'{self.sql} IN ({", ".join("%s" for _ in values)})',values,frozenset({self.table}))

@dataclass(frozen=True)
class Compiled(Generic[T]):
    sql: str
    params: tuple[object,...]
    decode: Callable[[Mapping[str,Any]],T]

@dataclass(frozen=True)
class Select(Generic[T]):
    table: Table
    columns: tuple[Column[Any],...]
    decoder: Callable[[Mapping[str,Any]],T]
    predicate: Predicate | None = None

    def where(self, condition: Predicate) -> Select[T]:
        _condition(self.table,condition)
        return Select(self.table,self.columns,self.decoder,condition if self.predicate is None else self.predicate & condition)

    def compile(self) -> Compiled[T]:
        sql=f'SELECT {", ".join(c.sql for c in self.columns)} FROM {self.table.sql}'
        if self.predicate is not None: sql+=' WHERE '+self.predicate.sql
        return Compiled(sql,self.predicate.params if self.predicate is not None else (),self.decoder)


def _condition(table: Table, condition: Predicate) -> None:
    if not isinstance(condition,Predicate) or condition.owners - {table}:
        raise ValueError('predicate references a foreign table; joins not implemented in this slice')


def select(column: Column[T]) -> Select[T]:
    if column.table.columns.get(column.name) is not column: raise ValueError("projection column is not owned by table")
    def decode(row: Mapping[str,Any]) -> T:
        value=row[column.name]; column.spec.check(value); return cast(T,value)
    return Select(column.table,(column,),decode)


def select_row(table: Table, *columns: Column[Any]) -> Select[dict[str,Any]]:
    columns=columns or tuple(table.columns.values())
    if any(c.table is not table or table.columns.get(c.name) is not c for c in columns) or len({c.name for c in columns}) != len(columns):
        raise ValueError('projection requires unique owned columns')
    def decode(row: Mapping[str,Any]) -> dict[str,Any]:
        result={}
        for column in columns:
            value=row[column.name]; column.spec.check(value); result[column.name]=value
        return result
    return Select(table,columns,decode)

@dataclass(frozen=True)
class Mutation:
    sql: str
    params: tuple[object,...]


def _writes(table: Table, values: Mapping[str,object]) -> list[tuple[Column[Any],object]]:
    result=[]
    for name,value in values.items():
        if name not in table.columns: raise ValueError('unknown write column')
        column=table.columns[name]
        if value is OMIT: continue
        if column.spec.generated: raise ValueError('generated column writes refused')
        if value is not DEFAULT: column.spec.check(value)
        result.append((column,value))
    return result


def insert(table: Table, values: Mapping[str,object]) -> Mutation:
    writes=_writes(table,values)
    if not writes: return Mutation(f'INSERT INTO {table.sql} DEFAULT VALUES',())
    return Mutation(f'INSERT INTO {table.sql} ({", ".join(quote(c.name) for c,_ in writes)}) VALUES ({", ".join("DEFAULT" if v is DEFAULT else "%s" for _,v in writes)})',tuple(v for _,v in writes if v is not DEFAULT))


def update(table: Table, values: Mapping[str,object], *, where: Predicate) -> Mutation:
    _condition(table,where)
    writes=_writes(table,values)
    if not writes: raise ValueError('update needs at least one present/default/NULL assignment')
    return Mutation(f'UPDATE {table.sql} SET {", ".join(quote(c.name)+" = "+("DEFAULT" if v is DEFAULT else "%s") for c,v in writes)} WHERE {where.sql}',tuple(v for _,v in writes if v is not DEFAULT)+where.params)


def delete(table: Table, *, where: Predicate) -> Mutation:
    _condition(table,where)
    return Mutation(f'DELETE FROM {table.sql} WHERE {where.sql}',where.params)
