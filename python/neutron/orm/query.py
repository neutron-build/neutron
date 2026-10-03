"""Bounded qualified joins with explicit, statically nullable projections."""
from __future__ import annotations
from dataclasses import dataclass, replace
from decimal import Decimal
from typing import Any, Callable, Generic, Mapping, TypeVar, cast
from .core import Column, ColumnSpec, Compiled, Predicate, Table, _bound_quote

T = TypeVar('T')
U = TypeVar('U')

@dataclass(frozen=True)
class Field(Generic[T]):
    column: Column[Any]
    outer: bool = False

    @property
    def expression_sql(self) -> str: return self.column._bound_sql

    @property
    def result_spec(self) -> ColumnSpec[Any]:
        return replace(self.column.spec,nullable=self.column.spec.nullable or self.outer,generated=False)

    def decode(self, value: object) -> T:
        if value is None and self.outer: return cast(T, None)
        self.column.spec.check(value)
        return cast(T, value)


def field(column: Column[T]) -> Field[T]:
    return Field(column)


def outer_field(column: Column[T]) -> Field[T | None]:
    """Required projection wrapper for columns on a LEFT JOIN's right side."""
    return Field(column, True)

@dataclass(frozen=True)
class Order:
    column: Column[Any]
    descending: bool = False
    nulls_first: bool = False

@dataclass(frozen=True)
class Join:
    table: Table
    condition: Predicate
    left: bool

@dataclass(frozen=True)
class Scope:
    table: Table
    joins: tuple[Join, ...] = ()
    correlated: frozenset[Table] = frozenset()

    @property
    def tables(self) -> frozenset[Table]:
        return frozenset((self.table, *(join.table for join in self.joins)))

    def _join(self, table: Table, on: Predicate, left: bool) -> Scope:
        if any(_source_key(owner) == _source_key(table) for owner in self.tables):
            raise ValueError('duplicate source identity requires distinct aliases')
        if not isinstance(on, Predicate) or table not in on.owners or not (on.owners & self.tables) or on.owners - (self.tables | {table}):
            raise ValueError('join must connect the new table to the existing scope')
        return replace(self, joins=self.joins + (Join(table, on, left),))

    def inner_join(self, table: Table, *, on: Predicate) -> Scope:
        return self._join(table, on, False)

    def left_join(self, table: Table, *, on: Predicate) -> Scope:
        return self._join(table, on, True)

    def correlate(self,*tables: Table) -> Scope:
        if not tables or any(table in self.tables for table in tables):
            raise ValueError('correlation requires distinct declared outer tables')
        return replace(self,correlated=self.correlated | frozenset(tables))

    def select(self, value: Field[T]) -> Query[T]:
        return Query(self, (value,), lambda row: value.decode(row['p0']))

    def select_pair(self, first: Field[T], second: Field[U]) -> Query[tuple[T, U]]:
        return Query(self, (first, second), lambda row: (first.decode(row['p0']), second.decode(row['p1'])))


def query_from(table: Table) -> Scope:
    return Scope(table)

@dataclass(frozen=True)
class Query(Generic[T]):
    scope: Scope
    fields: tuple[Field[Any], ...]
    decoder: Callable[[Mapping[str, Any]], T]
    predicate: Predicate | None = None
    ordering: tuple[Order, ...] = ()
    row_limit: int | None = None
    row_offset: int | None = None
    grouping: tuple[Column[Any], ...] = ()
    having_predicate: Predicate | None = None
    distinct_rows: bool = False
    set_terms: tuple[tuple[str,Query[T]], ...] = ()

    def where(self, condition: Predicate) -> Query[T]:
        if not isinstance(condition, Predicate) or condition.owners - (self.scope.tables | self.scope.correlated):
            raise ValueError('predicate outside query scope')
        return replace(self, predicate=condition if self.predicate is None else self.predicate & condition)

    def order_by(self, *orders: Order) -> Query[T]:
        if self.set_terms: raise ValueError('set-operation ordering requires a future output-label ordering API')
        if not orders: raise ValueError('order_by requires ordering columns')
        return replace(self, ordering=self.ordering + orders)

    def group_by(self,*columns: Column[Any]) -> Query[T]:
        if self.set_terms or not columns: raise ValueError('group_by requires columns on a plain query')
        for column in columns: self._owned(column)
        return replace(self,grouping=self.grouping+columns)

    def having(self,condition: Predicate) -> Query[T]:
        if self.set_terms: raise ValueError('HAVING cannot wrap a set operation')
        self.where(condition)
        return replace(self,having_predicate=condition if self.having_predicate is None else self.having_predicate & condition)

    def distinct(self) -> Query[T]:
        if self.set_terms: raise ValueError('DISTINCT cannot wrap a set operation')
        return replace(self,distinct_rows=True)

    def _set(self,operator: str,other: Query[T]) -> Query[T]:
        if self.ordering or self.row_limit is not None or self.row_offset is not None:
            raise ValueError('apply set operations before final ordering/pagination')
        if not isinstance(other,Query) or len(self.fields)!=len(other.fields):
            raise ValueError('set-operation projection arity mismatch')
        if any(a.result_spec!=b.result_spec for a,b in zip(self.fields,other.fields)):
            raise ValueError('set-operation result profiles must agree exactly')
        return replace(self,set_terms=self.set_terms+((operator,other),))

    def union(self,other: Query[T],*,all: bool=False) -> Query[T]:
        if type(all) is not bool: raise ValueError('UNION all requires a boolean')
        return self._set('UNION ALL' if all else 'UNION',other)

    def intersect(self,other: Query[T]) -> Query[T]: return self._set('INTERSECT',other)
    def except_(self,other: Query[T]) -> Query[T]: return self._set('EXCEPT',other)

    def limit(self, count: int) -> Query[T]:
        _count(count)
        return replace(self, row_limit=count)

    def offset(self, count: int) -> Query[T]:
        _count(count)
        return replace(self, row_offset=count)

    def _owned(self, column: Column[Any]) -> None:
        if column.table not in self.scope.tables or column.table.columns.get(column.name) is not column:
            raise ValueError('column outside query scope')

    def compile(self) -> Compiled[T]:
        if self.set_terms:
            if self.ordering: raise ValueError('set-operation source ordering unsupported')
            first=replace(self,set_terms=(),row_limit=None,row_offset=None).compile()
            sql='('+first.sql+')';params=first.params
            for operator,other in self.set_terms:
                if operator not in {'UNION','UNION ALL','INTERSECT','EXCEPT'}: raise ValueError('invalid set operator')
                replace(self,set_terms=(),ordering=(),row_limit=None,row_offset=None)._set(operator,other)
                compiled=other.compile()
                # Parentheses preserve explicitly composed left-to-right set semantics.
                sql='('+sql+' '+operator+' ('+compiled.sql+'))';params+=compiled.params
            if self.row_limit is not None:
                _count(self.row_limit);sql+=' LIMIT %s';params+=(self.row_limit,)
            if self.row_offset is not None:
                _count(self.row_offset);sql+=' OFFSET %s';params+=(self.row_offset,)
            return Compiled(sql,params,self.decoder)
        nullable = {join.table for join in self.scope.joins if join.left}
        if not self.fields: raise ValueError('empty projection')
        for item in self.fields:
            self._owned(item.column)
            if item.column.table in nullable and not item.outer and not isinstance(item,(Aggregate,RowNumber)):
                raise ValueError('LEFT JOIN projection requires outer_field nullable wrapper')
            if isinstance(item,RowNumber):
                for column in item.partition: self._owned(column)
                for order in item.orders:
                    self._owned(order.column);_validate_order(order)
            if item.outer and item.column.table not in nullable:
                raise ValueError('outer_field requires a LEFT JOIN right-side column')
        # Revalidate immutable AST, including directly constructed instances.
        scope = Scope(self.scope.table,correlated=self.scope.correlated)
        for join in self.scope.joins: scope = scope._join(join.table, join.condition, join.left)
        aggregate=any(isinstance(item,Aggregate) for item in self.fields)
        for column in self.grouping: self._owned(column)
        if self.grouping or aggregate:
            if any(not isinstance(item,(Aggregate,RowNumber)) and not any(item.column is col for col in self.grouping) for item in self.fields):
                raise ValueError('nonaggregate projections must appear in GROUP BY')
        if self.having_predicate is not None and not (self.grouping or aggregate):
            raise ValueError('HAVING requires grouping or aggregate projection')
        if type(self.distinct_rows) is not bool: raise ValueError('DISTINCT requires a boolean')
        sql = ('SELECT DISTINCT ' if self.distinct_rows else 'SELECT ') + ', '.join(f'{item.expression_sql} AS {_bound_quote("p" + str(i))}' for i, item in enumerate(self.fields))
        sql += ' FROM ' + self.scope.table._bound_sql
        params: tuple[object, ...] = ()
        for join in self.scope.joins:
            sql += (' LEFT JOIN ' if join.left else ' INNER JOIN ') + join.table._bound_sql + ' ON ' + join.condition.sql
            params += join.condition.params
        if self.predicate is not None:
            self.where(self.predicate)
            sql += ' WHERE ' + self.predicate.sql
            params += self.predicate.params
        if self.grouping:
            sql+=' GROUP BY '+', '.join(column._bound_sql for column in self.grouping)
        if self.having_predicate is not None:
            self.where(self.having_predicate)
            sql+=' HAVING '+self.having_predicate.sql;params+=self.having_predicate.params
        for order in self.ordering:
            _validate_order(order)
            self._owned(order.column)
        if self.ordering:
            sql += ' ORDER BY ' + ', '.join(order.column._bound_sql + (' DESC' if order.descending else ' ASC') + (' NULLS FIRST' if order.nulls_first else ' NULLS LAST') for order in self.ordering)
        if self.row_limit is not None:
            _count(self.row_limit); sql += ' LIMIT %s'; params += (self.row_limit,)
        if self.row_offset is not None:
            _count(self.row_offset); sql += ' OFFSET %s'; params += (self.row_offset,)
        return Compiled(sql, params, self.decoder)


def _count(count: int) -> None:
    if type(count) is not int or count < 0: raise ValueError('pagination requires a nonnegative integer')


class AliasedTable(Table):
    """Distinct read-only source identity; aliases never target ORM writes."""
    def __init__(self,source: Table,name: str) -> None:
        if type(source) is not Table: raise ValueError('alias requires an ordinary physical Table')
        super().__init__(name,{key:column.spec for key,column in source.columns.items()},schema=source.schema)
        object.__setattr__(self,'source',source)

    @property
    def _bound_sql(self) -> str:
        return self.source._bound_sql + ' AS ' + _bound_quote(self.name)

    @property
    def _bound_reference(self) -> str: return _bound_quote(self.name)


def alias(table: Table,name: str) -> Table:
    return AliasedTable(table,name)


def _source_key(table: Table) -> tuple[str,...]:
    return ('alias',table.name) if isinstance(table,AliasedTable) else ('physical',table.schema,table.name)


def exists(query: Query[Any]) -> Predicate:
    if not isinstance(query,Query): raise ValueError('EXISTS requires a typed Query')
    compiled=query.compile()
    return Predicate('EXISTS ('+compiled.sql+')',compiled.params,query.scope.correlated)


def in_query(column: Column[T],query: Query[T]) -> Predicate:
    column._owned()
    if not isinstance(query,Query) or len(query.fields)!=1:
        raise ValueError('IN subquery requires exactly one typed projection')
    source=query.fields[0].result_spec
    if source.sql_type!=column.spec.sql_type:
        raise ValueError('IN subquery column profiles must agree')
    compiled=query.compile()
    return Predicate(column._bound_sql+' IN ('+compiled.sql+')',compiled.params,query.scope.correlated | {column.table})


def _validate_order(order: Order) -> None:
    if not isinstance(order,Order) or type(order.descending) is not bool or type(order.nulls_first) is not bool:
        raise ValueError('ordering requires explicit boolean direction/null placement')


@dataclass(frozen=True)
class Aggregate(Field[T]):
    function: str = 'COUNT'

    @property
    def expression_sql(self) -> str:
        self.column._owned()
        if self.function not in {'COUNT','SUM','AVG','MIN','MAX'}: raise ValueError('unsupported aggregate')
        self.result_spec # Validate native promotion/type support before SQL.
        return self.function+'('+self.column._bound_sql+')'

    @property
    def result_spec(self) -> ColumnSpec[Any]:
        source=self.column.spec
        if self.function=='COUNT': return ColumnSpec(int,'int8')
        if self.function in {'SUM','AVG'}:
            if source.sql_type not in {'int2','int4','int8','numeric'}: raise ValueError('aggregate requires a supported numeric column')
            if self.function=='SUM' and source.sql_type in {'int2','int4'}: return ColumnSpec(int,'int8',nullable=True)
            return ColumnSpec(Decimal,'numeric',nullable=True)
        if self.function in {'MIN','MAX'} and source.sql_type in {'int2','int4','int8','numeric','text','varchar','date','timestamp','timestamptz'}:
            return replace(source,nullable=True,generated=False)
        raise ValueError('unsupported aggregate result profile')

    def decode(self,value: object) -> T:
        self.result_spec.check(value);return cast(T,value)

    def eq(self,value: T | None) -> Predicate:
        if value is None: return Predicate(self.expression_sql+' IS NULL',(),frozenset({self.column.table}))
        self.result_spec.check(value)
        return Predicate(self.expression_sql+' = %s',(value,),frozenset({self.column.table}))

    def gt(self,value: T) -> Predicate:
        if value is None: raise ValueError('aggregate ordering comparison refuses NULL')
        self.result_spec.check(value)
        return Predicate(self.expression_sql+' > %s',(value,),frozenset({self.column.table}))


def count(column: Column[Any]) -> Aggregate[int]: return Aggregate(column,function='COUNT')
def sum_value(column: Column[int] | Column[Decimal] | Column[int | None] | Column[Decimal | None]) -> Aggregate[int | Decimal | None]:
    return Aggregate(column,function='SUM')
def avg(column: Column[int] | Column[Decimal] | Column[int | None] | Column[Decimal | None]) -> Aggregate[Decimal | None]:
    return Aggregate(column,function='AVG')
def min_value(column: Column[T]) -> Aggregate[T | None]: return Aggregate(column,function='MIN')
def max_value(column: Column[T]) -> Aggregate[T | None]: return Aggregate(column,function='MAX')


@dataclass(frozen=True)
class RowNumber(Field[int]):
    partition: tuple[Column[Any], ...] = ()
    orders: tuple[Order, ...] = ()

    @property
    def result_spec(self) -> ColumnSpec[int]: return ColumnSpec(int,'int8')

    @property
    def expression_sql(self) -> str:
        clauses=[]
        if self.partition: clauses.append('PARTITION BY '+', '.join(column._bound_sql for column in self.partition))
        for order in self.orders: _validate_order(order)
        if self.orders:
            clauses.append('ORDER BY '+', '.join(order.column._bound_sql+(' DESC' if order.descending else ' ASC')+(' NULLS FIRST' if order.nulls_first else ' NULLS LAST') for order in self.orders))
        return 'ROW_NUMBER() OVER ('+' '.join(clauses)+')'

    def decode(self,value: object) -> int:
        self.result_spec.check(value);return cast(int,value)


def row_number(table: Table,*,partition_by: tuple[Column[Any], ...]=(),order_by: tuple[Order, ...]=()) -> RowNumber:
    return RowNumber(next(iter(table.columns.values())),partition=partition_by,orders=order_by)
