package orm

import (
	"context"
	"fmt"
	"reflect"
	"strings"
)

// Aggregate retains model ownership and the actual PostgreSQL result type.
// SUM/AVG(bigint) use exact Decimal; SQL NULL is an explicit Nullable result.
type Aggregate[M, T any] struct {
	info        *modelInfo
	field       *fieldInfo
	function    string
	distinct    bool
	destination func(*T, bool) any
	bound       func(T) any
}

func CountAll[M any](table Table[M]) Aggregate[M, int64] {
	return Aggregate[M, int64]{info: table.info, function: "COUNT", destination: func(v *int64, _ bool) any { return v }, bound: func(v int64) any { return v }}
}
func CountColumn[M, T any](column Column[M, T]) Aggregate[M, int64] {
	a := CountAll(Table[M]{column.info})
	field := column.field
	a.field = &field
	return a
}
func CountDistinct[M, T any](column Column[M, T]) Aggregate[M, int64] {
	a := CountColumn(column)
	a.distinct = true
	return a
}
func nullableAggregate[M, R, T any](column Column[M, T], function string) Aggregate[M, Nullable[R]] {
	field := column.field
	return Aggregate[M, Nullable[R]]{info: column.info, field: &field, function: function, destination: func(v *Nullable[R], null bool) any {
		if null {
			*v = Nullable[R]{}
			return nil
		}
		v.Valid = true
		return scanDestination(reflect.ValueOf(&v.Value).Elem())
	}, bound: func(v Nullable[R]) any {
		if !v.Valid {
			return nil
		}
		return snapshot(v.Value)
	}}
}
func Min[M, T any](column Column[M, T]) Aggregate[M, Nullable[T]] {
	return nullableAggregate[M, T](column, "MIN")
}
func Max[M, T any](column Column[M, T]) Aggregate[M, Nullable[T]] {
	return nullableAggregate[M, T](column, "MAX")
}
func SumInt64[M any](column Column[M, int64]) Aggregate[M, Nullable[Decimal]] {
	return nullableAggregate[M, Decimal](column, "SUM")
}
func AvgInt64[M any](column Column[M, int64]) Aggregate[M, Nullable[Decimal]] {
	return nullableAggregate[M, Decimal](column, "AVG")
}
func SumInt32[M any](column Column[M, int32]) Aggregate[M, Nullable[int64]] {
	return nullableAggregate[M, int64](column, "SUM")
}
func AvgInt32[M any](column Column[M, int32]) Aggregate[M, Nullable[Decimal]] {
	return nullableAggregate[M, Decimal](column, "AVG")
}
func SumDecimal[M any](column Column[M, Decimal]) Aggregate[M, Nullable[Decimal]] {
	return nullableAggregate[M, Decimal](column, "SUM")
}
func AvgDecimal[M any](column Column[M, Decimal]) Aggregate[M, Nullable[Decimal]] {
	return nullableAggregate[M, Decimal](column, "AVG")
}
func SumFloat64[M any](column Column[M, float64]) Aggregate[M, Nullable[float64]] {
	return nullableAggregate[M, float64](column, "SUM")
}
func AvgFloat64[M any](column Column[M, float64]) Aggregate[M, Nullable[float64]] {
	return nullableAggregate[M, float64](column, "AVG")
}
func (a Aggregate[M, T]) sql(info *modelInfo) (string, error) {
	if info == nil || a.info != info || a.destination == nil {
		return "", fmt.Errorf("orm: aggregate outside initialized table binding")
	}
	argument := "*"
	if a.field != nil {
		argument = quote(a.field.name)
	}
	if a.distinct {
		argument = "DISTINCT " + argument
	}
	return a.function + "(" + argument + ")", nil
}

type AggregatePredicate[M any] struct {
	info       *modelInfo
	expression string
	value      any
	operator   Comparison
	failure    error
}

func (a Aggregate[M, T]) Compare(op Comparison, value T) AggregatePredicate[M] {
	sql, err := a.sql(a.info)
	if a.bound == nil {
		return AggregatePredicate[M]{failure: fmt.Errorf("orm: uninitialized aggregate")}
	}
	return AggregatePredicate[M]{info: a.info, expression: sql, value: a.bound(value), operator: op, failure: err}
}
func renderHaving[M any](info *modelInfo, p AggregatePredicate[M], args *[]any) (string, error) {
	if p.failure != nil {
		return "", p.failure
	}
	if p.info != info || p.expression == "" {
		return "", fmt.Errorf("orm: HAVING outside aggregate binding")
	}
	switch p.operator {
	case Equal, NotEqual, Greater, GreaterEqual, Less, LessEqual:
	default:
		return "", fmt.Errorf("orm: invalid aggregate comparison")
	}
	if p.value == nil {
		if p.operator == Equal {
			return p.expression + " IS NULL", nil
		}
		if p.operator == NotEqual {
			return p.expression + " IS NOT NULL", nil
		}
		return "", fmt.Errorf("orm: ordered NULL aggregate comparison")
	}
	if err := validateScalarValue(p.value); err != nil {
		return "", err
	}
	*args = append(*args, p.value)
	return fmt.Sprintf("%s %s $%d", p.expression, p.operator, len(*args)), nil
}
func aggregateSQL[M, T any](aggregate Aggregate[M, T], group *fieldInfo, query Query[M], having []AggregatePredicate[M]) (string, []any, error) {
	expr, err := aggregate.sql(aggregate.info)
	if err != nil {
		return "", nil, err
	}
	if query.limit < 0 || query.offset < 0 {
		return "", nil, fmt.Errorf("orm: negative aggregate pagination")
	}
	projection := expr
	if group != nil {
		projection = quote(group.name) + ", " + expr
	} else if len(query.order) > 0 || query.limited || query.offset != 0 || len(having) > 0 {
		return "", nil, fmt.Errorf("orm: ungrouped aggregate refuses ordering/pagination/HAVING")
	}
	sql := "SELECT " + projection + " FROM " + aggregate.info.sqlName()
	args := []any{}
	if query.whereSet {
		where, failure := renderPredicate(aggregate.info, query.predicate.expr, &args)
		if failure != nil {
			return "", nil, failure
		}
		sql += " WHERE " + where
	}
	if group != nil {
		sql += " GROUP BY " + quote(group.name)
	}
	parts := make([]string, len(having))
	for i, predicate := range having {
		parts[i], err = renderHaving(aggregate.info, predicate, &args)
		if err != nil {
			return "", nil, err
		}
	}
	if len(parts) > 0 {
		sql += " HAVING (" + strings.Join(parts, ") AND (") + ")"
	}
	parts = make([]string, len(query.order))
	for i, order := range query.order {
		if order.info != aggregate.info || group == nil || order.field.index != group.index {
			return "", nil, fmt.Errorf("orm: grouped ordering requires the grouped field")
		}
		dir := " ASC"
		if order.descending {
			dir = " DESC"
		}
		parts[i] = quote(order.field.name) + dir + order.nulls
	}
	if len(parts) > 0 {
		sql += " ORDER BY " + strings.Join(parts, ", ")
	}
	if query.limited {
		args = append(args, query.limit)
		sql += fmt.Sprintf(" LIMIT $%d", len(args))
	}
	if query.offset > 0 {
		args = append(args, query.offset)
		sql += fmt.Sprintf(" OFFSET $%d", len(args))
	}
	if len(args) > 65535 {
		return "", nil, fmt.Errorf("orm: PostgreSQL parameter limit exceeded")
	}
	return sql, args, nil
}

// AggregateOne selects one ungrouped aggregate. It refuses pagination/order
// which could hide its mandatory single-row shape. Empty SUM/MIN/MAX/AVG return
// Nullable.Valid=false; COUNT returns zero, preserving native PostgreSQL types.
func AggregateOne[M, T any](ctx context.Context, db Executor, aggregate Aggregate[M, T], query Query[M]) (T, error) {
	var zero T
	if err := ready(ctx, db); err != nil {
		return zero, wrap("aggregate", err)
	}
	sql, args, err := aggregateSQL(aggregate, nil, query, nil)
	if err != nil {
		return zero, wrap("aggregate", err)
	}
	rows, err := db.Query(ctx, sql, args...)
	if err != nil {
		return zero, wrap("aggregate", err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return zero, wrap("aggregate", err)
		}
		return zero, wrap("aggregate", ErrNotFound)
	}
	raw := rows.RawValues()
	if len(raw) != 1 {
		return zero, wrap("aggregate", fmt.Errorf("orm: unexpected aggregate projection shape"))
	}
	var result T
	if err := rows.Scan(aggregate.destination(&result, raw[0] == nil)); err != nil {
		return zero, wrap("aggregate", err)
	}
	if rows.Next() {
		return zero, wrap("aggregate", ErrCardinality)
	}
	return result, wrap("aggregate", rows.Err())
}

// SelectGrouped returns typed group-key/aggregate pairs with explicit HAVING.
// Only the grouped key may appear in Query.OrderBy. Every input retains exact
// table binding; unsupported native aggregate codecs preserve PostgreSQL errors.
func SelectGrouped[M, K, T any](ctx context.Context, db Executor, group Column[M, K], aggregate Aggregate[M, T], query Query[M], having ...AggregatePredicate[M]) ([]Pair[K, T], error) {
	if err := ready(ctx, db); err != nil {
		return nil, wrap("grouped aggregate", err)
	}
	if group.info == nil || group.info != aggregate.info {
		return nil, wrap("grouped aggregate", fmt.Errorf("orm: grouped field outside aggregate binding"))
	}
	sql, args, err := aggregateSQL(aggregate, &group.field, query, having)
	if err != nil {
		return nil, wrap("grouped aggregate", err)
	}
	rows, err := db.Query(ctx, sql, args...)
	if err != nil {
		return nil, wrap("grouped aggregate", err)
	}
	defer rows.Close()
	result := []Pair[K, T]{}
	for rows.Next() {
		raw := rows.RawValues()
		if len(raw) != 2 {
			return nil, wrap("grouped aggregate", fmt.Errorf("orm: unexpected grouped projection shape"))
		}
		var pair Pair[K, T]
		if err := rows.Scan(scanDestination(reflect.ValueOf(&pair.First).Elem()), aggregate.destination(&pair.Second, raw[1] == nil)); err != nil {
			return nil, wrap("grouped aggregate", err)
		}
		result = append(result, pair)
	}
	return result, wrap("grouped aggregate", rows.Err())
}
