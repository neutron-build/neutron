"""Explicit PostgreSQL SQL core; no identity map or unit-of-work behavior."""
from __future__ import annotations
from dataclasses import dataclass
import datetime as dt
from decimal import Decimal
from types import MappingProxyType
from typing import Any, Callable, Generic, Iterable, Mapping, TypeVar, cast, get_args, get_origin
from uuid import UUID

from .json_value import BoundJson, JsonDocument, MutableJson
from .catalog_value import BoundCatalog, CatalogType, PgDomain, PgEnum
from .pg_value import BoundArray, BoundRange, PgArray, PgRange, TimeOfDay, Interval

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

def _bound_quote(name: str) -> str:
    # psycopg percent placeholders are parsed even inside quoted identifiers.
    return quote(name).replace('%','%%')

# Column families deliberately finite; custom SQL type text cannot become SQL.
_TYPES: dict[str, type] = {'int2':int,'int4':int,'int8':int,'text':str,'varchar':str,'bool':bool,'numeric':Decimal,'uuid':UUID,'bytea':bytes,'timestamp':dt.datetime,'timestamptz':dt.datetime,'date':dt.date,'json':JsonDocument,'jsonb':JsonDocument,'time':TimeOfDay,'interval':Interval}

_RANGE_TYPES={'int4range':'int4','int8range':'int8','numrange':'numeric','daterange':'date','tsrange':'timestamp','tstzrange':'timestamptz'}
_ARRAY_TYPES=frozenset({'int2','int4','int8','text','varchar','bool','numeric','uuid','bytea','date','timestamp','timestamptz','time','interval'})

@dataclass(frozen=True)
class ColumnSpec(Generic[T]):
    python_type: type[T]
    sql_type: str
    nullable: bool = False
    generated: bool = False
    native_type: CatalogType|None=None
    domain_base: ColumnSpec[Any]|None=None

    def __post_init__(self) -> None:
        if self.native_type is not None:
            if not isinstance(self.native_type,CatalogType): raise ValueError('catalog type admission required')
            if self.sql_type=='enum' and self.python_type is PgEnum and self.native_type.kind=='e' and self.domain_base is None: return
            if self.sql_type=='domain' and self.python_type is PgDomain and self.native_type.kind=='d' and isinstance(self.domain_base,ColumnSpec) and self.domain_base.type_oid==self.native_type.base_oid and (self.domain_base.native_type is None or self.domain_base.native_type.kind=='e' and self.domain_base.native_type._owner is self.native_type._owner): return
            raise ValueError('catalog kind/base type mismatch')
        if self.domain_base is not None: raise ValueError('domain base requires catalog identity')
        if _TYPES.get(self.sql_type) is not self.python_type and not (self.sql_type=='jsonb' and self.python_type is MutableJson) and not (self.python_type is PgRange and self.sql_type in _RANGE_TYPES) and not (self.python_type is PgArray and self.sql_type.endswith('[]') and self.sql_type[:-2] in _ARRAY_TYPES):
            raise ValueError('unsupported or mismatched column type profile')

    @property
    def type_oid(self) -> int:
        if self.native_type is not None: return self.native_type.oid
        from .pg_adapters import ARRAY_OIDS, BUILTIN_OIDS
        return ARRAY_OIDS[self.sql_type] if self.sql_type.endswith('[]') else BUILTIN_OIDS[self.sql_type]

    @property
    def result_oid(self) -> int:
        return self.domain_base.type_oid if self.domain_base is not None else self.type_oid

    def decode(self,value: object) -> object:
        if self.domain_base is not None and value is not None and not isinstance(value,PgDomain):
            assert self.native_type is not None
            value=PgDomain(self.domain_base.decode(value),self.native_type)
        if self.python_type is MutableJson and isinstance(value,JsonDocument): value=MutableJson(value.parsed())
        self.check(value);return value

    def check(self, value: object) -> None:
        if value is None:
            if not self.nullable: raise ValueError('NULL on nonnullable column')
            return
        if not isinstance(value, self.python_type) or (self.python_type is int and isinstance(value, bool)) or (self.sql_type == 'date' and isinstance(value, dt.datetime)):
            raise ValueError('column value has wrong native type')
        if isinstance(value,(PgEnum,PgDomain)):
            if value.identity is not self.native_type: raise ValueError('enum/domain qualified identity mismatch')
            if isinstance(value,PgDomain):
                assert self.domain_base is not None
                self.domain_base.check(value.value)
        if isinstance(value,PgRange):
            element_type=_RANGE_TYPES[self.sql_type]
            range_element: ColumnSpec[Any]=ColumnSpec(_TYPES[element_type],element_type)
            for bound in (value.lower,value.upper):
                if bound is not None:
                    range_element.check(bound)
                    if isinstance(bound,Decimal) and not bound.is_finite(): raise ValueError('finite range numeric bounds required')
        if isinstance(value,PgArray):
            element: ColumnSpec[Any]=ColumnSpec(_TYPES[self.sql_type[:-2]],self.sql_type[:-2],nullable=True)
            for item in value.elements: element.check(item)
        if isinstance(value,MutableJson): value.text
        if isinstance(value, dt.datetime):
            aware = value.tzinfo is not None and value.utcoffset() is not None
            if aware != (self.sql_type == 'timestamptz'):
                raise ValueError('timestamp local/instant mode mismatch')
        if self.sql_type in {'int2','int4','int8'}:
            bits = {'int2':16,'int4':32,'int8':64}[self.sql_type]
            if not -(2**(bits-1)) <= cast(int,value) < 2**(bits-1): raise ValueError('integer out of PostgreSQL range')


def array_spec(element_type: type[T],sql_type: str,*,nullable: bool=False,generated: bool=False) -> ColumnSpec[PgArray[T]]:
    if sql_type not in _ARRAY_TYPES or _TYPES[sql_type] is not element_type: raise ValueError('unsupported array element profile')
    return cast(ColumnSpec[PgArray[T]],ColumnSpec(PgArray,sql_type+'[]',nullable,generated))


def range_spec(element_type: type[T],sql_type: str,*,nullable: bool=False,generated: bool=False) -> ColumnSpec[PgRange[T]]:
    if sql_type not in _RANGE_TYPES or _TYPES[_RANGE_TYPES[sql_type]] is not element_type: raise ValueError('unsupported range element profile')
    return cast(ColumnSpec[PgRange[T]],ColumnSpec(PgRange,sql_type,nullable,generated))


def _column_type(spec: ColumnSpec[Any],python_type: Any) -> None:
    if get_origin(python_type) is PgDomain:
        if spec.python_type is not PgDomain or spec.domain_base is None: raise ValueError('domain column type mismatch')
        _column_type(spec.domain_base,get_args(python_type)[0])
    elif get_origin(python_type) is PgRange:
        if spec.python_type is not PgRange or get_args(python_type)!=(_TYPES[_RANGE_TYPES[spec.sql_type]],):
            raise ValueError('range column element type mismatch')
    elif get_origin(python_type) is PgArray:
        if spec.python_type is not PgArray or get_args(python_type)!=(_TYPES[spec.sql_type[:-2]],):
            raise ValueError('array column element type mismatch')
    elif spec.python_type is not python_type: raise ValueError('column type mismatch')


@dataclass(frozen=True, eq=False, init=False)
class Table:
    name: str
    schema: str
    columns: Mapping[str, Column[Any]]
    _catalog_owner: object|None
    def __init__(self, name: str, columns: Mapping[str, ColumnSpec[Any]], *, schema: str = 'public', _catalog_owner: object|None=None) -> None:
        quote(name); quote(schema)
        if not columns: raise ValueError('table requires columns')
        for key,spec in columns.items():
            quote(key)
            if spec.native_type is not None and spec.native_type._owner is not _catalog_owner:
                raise ValueError('enum/domain columns require owning Database.catalog_table admission')
        object.__setattr__(self,"_catalog_owner",_catalog_owner)
        object.__setattr__(self,"name",name)
        object.__setattr__(self,"schema",schema)
        object.__setattr__(self,"columns",MappingProxyType({key:Column(self,key,spec) for key,spec in columns.items()}))

    @property
    def sql(self) -> str: return f'{quote(self.schema)}.{quote(self.name)}'

    @property
    def _bound_sql(self) -> str: return f'{_bound_quote(self.schema)}.{_bound_quote(self.name)}'

    @property
    def _bound_reference(self) -> str: return self._bound_sql

    @property
    def _source_params(self) -> tuple[object,...]: return ()

    def column(self, name: str, python_type: type[T]) -> Column[T]:
        column = self.columns[name]
        _column_type(column.spec,python_type)
        if column.spec.nullable: raise ValueError('column type/nullability mismatch; use nullable_column for nullable fields')
        return cast(Column[T],column)

    def nullable_column(self, name: str, python_type: type[T]) -> Column[T | None]:
        column=self.columns[name]
        _column_type(column.spec,python_type)
        if not column.spec.nullable: raise ValueError("nullable column type mismatch")
        return cast(Column[T | None],column)


@dataclass(frozen=True)
class Predicate:
    sql: str
    params: tuple[object,...] = ()
    owners: frozenset[Table] = frozenset()
    catalog_owners: frozenset[object]=frozenset()

    def __and__(self, other: Predicate) -> Predicate:
        return Predicate(f'({self.sql}) AND ({other.sql})',self.params+other.params,self.owners|other.owners,self.catalog_owners|other.catalog_owners)
    def __or__(self, other: Predicate) -> Predicate:
        return Predicate(f'({self.sql}) OR ({other.sql})',self.params+other.params,self.owners|other.owners,self.catalog_owners|other.catalog_owners)
    def __bool__(self) -> bool: raise TypeError('SQL predicates cannot be Python booleans')

@dataclass(frozen=True)
class Column(Generic[T]):
    table: Table
    name: str
    spec: ColumnSpec[T]

    @property
    def sql(self) -> str: return f'{self.table.sql}.{quote(self.name)}'

    @property
    def _bound_sql(self) -> str: return f'{self.table._bound_reference}.{_bound_quote(self.name)}'

    @property
    def _enum_comparison_type(self) -> CatalogType|None:
        spec=self.spec.domain_base if self.spec.domain_base is not None else self.spec
        return spec.native_type if spec.native_type is not None and spec.native_type.kind=='e' else None

    @property
    def _comparison_sql(self) -> str:
        identity=self._enum_comparison_type
        if identity is not None and self.spec.domain_base is not None:
            return '('+self._bound_sql+')::'+_bound_quote(identity.schema)+'.'+_bound_quote(identity.name)
        return self._bound_sql

    @property
    def _comparison_bind(self) -> str:
        identity=self._enum_comparison_type
        return '%s' if identity is None else '%s::'+_bound_quote(identity.schema)+'.'+_bound_quote(identity.name)

    def _owned(self) -> None:
        if self.table.columns.get(self.name) is not self:
            raise ValueError("predicate column is not owned by table")

    def eq(self, value: T | Column[T] | None) -> Predicate:
        self._owned()
        if self.spec.sql_type == "json" and value is not None: raise ValueError("json equality unsupported; use jsonb for equality semantics")
        if isinstance(value,Column):
            value._owned()
            if value.spec.sql_type == "json": raise ValueError("json equality unsupported")
            if value.spec.python_type is not self.spec.python_type or self.spec.native_type!=value.spec.native_type: raise ValueError('incompatible column comparison')
            operator='OPERATOR(pg_catalog.=)' if self._enum_comparison_type is not None else '='
            return Predicate(f'{self._comparison_sql} {operator} {value._comparison_sql}',(),frozenset({self.table,value.table}))
        if value is None: return Predicate(f'{self._bound_sql} IS NULL',(),frozenset({self.table}))
        self.spec.check(value)
        operator='OPERATOR(pg_catalog.=)' if self._enum_comparison_type is not None else '='
        return Predicate(f'{self._comparison_sql} {operator} {self._comparison_bind}',(_parameter(self,value),),frozenset({self.table}))

    def in_(self, values: Iterable[T]) -> Predicate:
        self._owned()
        if self.spec.sql_type == "json": raise ValueError("json membership unsupported; use jsonb")
        values=tuple(values)
        if not values: return Predicate('FALSE',(),frozenset({self.table}))
        for value in values: self.spec.check(value)
        if self._enum_comparison_type is not None:
            return Predicate(f'{self._comparison_sql} OPERATOR(pg_catalog.=) ANY(ARRAY[{", ".join(self._comparison_bind for _ in values)}])',tuple(_parameter(self,value) for value in values),frozenset({self.table}))
        return Predicate(f'{self._bound_sql} IN ({", ".join("%s" for _ in values)})',tuple(_parameter(self,value) for value in values),frozenset({self.table}))

@dataclass(frozen=True)
class Compiled(Generic[T]):
    sql: str
    params: tuple[object,...]
    decode: Callable[[Mapping[str,Any]],T]
    result_oids: tuple[tuple[str,int],...]=()
    catalog_owner: object|None=None

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
        if not self.columns or any(c.table is not self.table or self.table.columns.get(c.name) is not c for c in self.columns) or len({c.name for c in self.columns}) != len(self.columns):
            raise ValueError("projection requires unique owned columns")
        if self.predicate is not None: _condition(self.table,self.predicate)
        sql=f'SELECT {", ".join(c._bound_sql for c in self.columns)} FROM {self.table._bound_sql}'
        if self.predicate is not None: sql+=' WHERE '+self.predicate.sql
        return Compiled(sql,self.predicate.params if self.predicate is not None else (),self.decoder,tuple((column.name,column.spec.result_oid) for column in self.columns),_compiled_owner((self.table,),self.predicate))


def _compiled_owner(tables: Iterable[Table],*conditions: Predicate|None) -> object|None:
    owners={table._catalog_owner for table in tables if table._catalog_owner is not None}
    for condition in conditions:
        if condition is not None: owners.update(condition.catalog_owners)
    if len(owners)>1: raise ValueError('query combines catalog metadata from different connections')
    return next(iter(owners),None)


def _condition(table: Table, condition: Predicate) -> None:
    if not isinstance(condition,Predicate) or condition.owners - {table}:
        raise ValueError('predicate references a foreign table; joins not implemented in this slice')


def select(column: Column[T]) -> Select[T]:
    if column.table.columns.get(column.name) is not column: raise ValueError("projection column is not owned by table")
    def decode(row: Mapping[str,Any]) -> T:
        value=column.spec.decode(row[column.name]); return cast(T,value)
    return Select(column.table,(column,),decode)


def select_row(table: Table, *columns: Column[Any]) -> Select[dict[str,Any]]:
    columns=columns or tuple(table.columns.values())
    if any(c.table is not table or table.columns.get(c.name) is not c for c in columns) or len({c.name for c in columns}) != len(columns):
        raise ValueError('projection requires unique owned columns')
    def decode(row: Mapping[str,Any]) -> dict[str,Any]:
        result={}
        for column in columns:
            value=column.spec.decode(row[column.name]); result[column.name]=value
        return result
    return Select(table,columns,decode)

@dataclass(frozen=True)
class Mutation:
    sql: str
    params: tuple[object,...]
    table: Table | None = None

    def returning(self, column: Column[T]) -> Returning[T]:
        if self.table is None or column.table is not self.table:
            raise ValueError("RETURNING requires a column of the mutation table")
        projection=select(column)
        return Returning(self,projection)

    def returning_row(self, *columns: Column[Any]) -> Returning[dict[str,Any]]:
        if self.table is None: raise ValueError("raw mutation has no owned projection metadata")
        return Returning(self,select_row(self.table,*columns))

@dataclass(frozen=True)
class Returning(Generic[T]):
    mutation: Mutation
    projection: Select[T]

    def compile(self) -> Compiled[T]:
        if self.mutation.table is None or self.projection.table is not self.mutation.table or self.projection.predicate is not None:
            raise ValueError("invalid RETURNING projection")
        # Validate ownership using the ordinary projection compiler.
        self.projection.compile()
        fields=", ".join(_bound_quote(c.name) for c in self.projection.columns)
        return Compiled(self.mutation.sql+' RETURNING '+fields,self.mutation.params,self.projection.decoder,tuple((column.name,column.spec.result_oid) for column in self.projection.columns),self.projection.table._catalog_owner)


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
    if type(table) is not Table: raise ValueError('mutation requires an ordinary physical Table')
    writes=_writes(table,values)
    if not writes: return Mutation(f'INSERT INTO {table._bound_sql} DEFAULT VALUES',(),table)
    return Mutation(f'INSERT INTO {table._bound_sql} ({", ".join(_bound_quote(c.name) for c,_ in writes)}) VALUES ({", ".join("DEFAULT" if v is DEFAULT else "%s" for _,v in writes)})',tuple(_parameter(c,v) for c,v in writes if v is not DEFAULT),table)


def update(table: Table, values: Mapping[str,object], *, where: Predicate) -> Mutation:
    if type(table) is not Table: raise ValueError('mutation requires an ordinary physical Table')
    _condition(table,where)
    writes=_writes(table,values)
    if not writes: raise ValueError('update needs at least one present/default/NULL assignment')
    return Mutation(f'UPDATE {table._bound_sql} SET {", ".join(_bound_quote(c.name)+" = "+("DEFAULT" if v is DEFAULT else "%s") for c,v in writes)} WHERE {where.sql}',tuple(_parameter(c,v) for c,v in writes if v is not DEFAULT)+where.params,table)


def delete(table: Table, *, where: Predicate) -> Mutation:
    if type(table) is not Table: raise ValueError('mutation requires an ordinary physical Table')
    _condition(table,where)
    return Mutation(f'DELETE FROM {table._bound_sql} WHERE {where.sql}',where.params,table)


def _parameter(column: Column[Any], value: object) -> object:
    if isinstance(value,PgEnum): return BoundCatalog(value.label,value.identity)
    if isinstance(value,PgDomain): return BoundCatalog(value.value,value.identity,column.spec.domain_base)
    if isinstance(value,PgRange): return BoundRange(value,column.spec.sql_type)
    if isinstance(value,PgArray): return BoundArray(value,column.spec.sql_type)
    if isinstance(value,MutableJson): return BoundJson(JsonDocument(value.text),True)
    if isinstance(value, JsonDocument): return BoundJson(value, column.spec.sql_type == 'jsonb')
    return value
