"""Bounded qualified joins with explicit, statically nullable projections."""
from __future__ import annotations
from dataclasses import dataclass, replace
from typing import Any, Callable, Generic, Mapping, TypeVar, cast
from .core import Column, Compiled, Predicate, Table, _bound_quote

T = TypeVar('T')
U = TypeVar('U')

@dataclass(frozen=True)
class Field(Generic[T]):
    column: Column[Any]
    outer: bool = False

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

    @property
    def tables(self) -> frozenset[Table]:
        return frozenset((self.table, *(join.table for join in self.joins)))

    def _join(self, table: Table, on: Predicate, left: bool) -> Scope:
        if any((owner.schema, owner.name) == (table.schema, table.name) for owner in self.tables):
            raise ValueError('duplicate physical table requires a future explicit alias API')
        if not isinstance(on, Predicate) or table not in on.owners or not (on.owners & self.tables) or on.owners - (self.tables | {table}):
            raise ValueError('join must connect the new table to the existing scope')
        return replace(self, joins=self.joins + (Join(table, on, left),))

    def inner_join(self, table: Table, *, on: Predicate) -> Scope:
        return self._join(table, on, False)

    def left_join(self, table: Table, *, on: Predicate) -> Scope:
        return self._join(table, on, True)

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

    def where(self, condition: Predicate) -> Query[T]:
        if not isinstance(condition, Predicate) or condition.owners - self.scope.tables:
            raise ValueError('predicate outside query scope')
        return replace(self, predicate=condition if self.predicate is None else self.predicate & condition)

    def order_by(self, *orders: Order) -> Query[T]:
        if not orders: raise ValueError('order_by requires ordering columns')
        return replace(self, ordering=self.ordering + orders)

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
        nullable = {join.table for join in self.scope.joins if join.left}
        if not self.fields: raise ValueError('empty projection')
        for item in self.fields:
            self._owned(item.column)
            if item.column.table in nullable and not item.outer:
                raise ValueError('LEFT JOIN projection requires outer_field nullable wrapper')
            if item.outer and item.column.table not in nullable:
                raise ValueError('outer_field requires a LEFT JOIN right-side column')
        # Revalidate immutable AST, including directly constructed instances.
        scope = Scope(self.scope.table)
        for join in self.scope.joins: scope = scope._join(join.table, join.condition, join.left)
        sql = 'SELECT ' + ', '.join(f'{item.column._bound_sql} AS {_bound_quote("p" + str(i))}' for i, item in enumerate(self.fields))
        sql += ' FROM ' + self.scope.table._bound_sql
        params: tuple[object, ...] = ()
        for join in self.scope.joins:
            sql += (' LEFT JOIN ' if join.left else ' INNER JOIN ') + join.table._bound_sql + ' ON ' + join.condition.sql
            params += join.condition.params
        if self.predicate is not None:
            self.where(self.predicate)
            sql += ' WHERE ' + self.predicate.sql
            params += self.predicate.params
        for order in self.ordering:
            if not isinstance(order, Order) or type(order.descending) is not bool or type(order.nulls_first) is not bool:
                raise ValueError('ordering requires explicit boolean direction/null placement')
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
