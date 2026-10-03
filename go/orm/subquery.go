package orm

import (
	"context"
	"fmt"
	"reflect"
	"strings"
)

// ScalarQuery is an immutable one-column subquery with an exact Go value type.
// Native SQL cardinality applies when used as a scalar: no row means SQL NULL;
// multiple rows produce PostgreSQL SQLSTATE 21000 unless explicitly paginated.
type ScalarQuery[M, T any] struct {
	column Column[M, T]
	query  Query[M]
	ctes   []CTE
}

func NewScalarQuery[M, T any](column Column[M, T], query Query[M]) (ScalarQuery[M, T], error) {
	result := ScalarQuery[M, T]{column: column, query: query}
	if _, _, err := result.compile(nil); err != nil {
		return ScalarQuery[M, T]{}, err
	}
	return result, nil
}
func ScalarFromCTE[M, T any](derived Derived[M], column Column[M, T], query Query[M]) (ScalarQuery[M, T], error) {
	if column.info == nil || column.info != derived.table.info {
		return ScalarQuery[M, T]{}, fmt.Errorf("orm: scalar column outside CTE binding")
	}
	result, err := NewScalarQuery(column, query)
	if err != nil {
		return result, err
	}
	result.ctes = []CTE{derived}
	return result, nil
}
func (s ScalarQuery[M, T]) compile(initial []any) (string, []any, error) {
	if s.column.info == nil {
		return "", nil, fmt.Errorf("orm: uninitialized scalar subquery")
	}
	prefix, args, err := compileCTEs(s.ctes, initial)
	if err != nil {
		return "", nil, err
	}
	sql, args, err := selectSQLArgs(Table[M]{s.column.info}, quote(s.column.field.name), s.query, args)
	return prefix + sql, args, err
}
func validComparison(op Comparison) bool {
	switch op {
	case Equal, NotEqual, Greater, GreaterEqual, Less, LessEqual:
		return true
	}
	return false
}
func InSubquery[M, I, T any](column Column[M, T], source ScalarQuery[I, T]) Predicate[M] {
	return subqueryPredicate(column, source, "IN")
}
func NotInSubquery[M, I, T any](column Column[M, T], source ScalarQuery[I, T]) Predicate[M] {
	return subqueryPredicate(column, source, "NOT IN")
}
func CompareSubquery[M, I, T any](column Column[M, T], op Comparison, source ScalarQuery[I, T]) Predicate[M] {
	return subqueryPredicate(column, source, string(op))
}
func subqueryPredicate[M, I, T any](column Column[M, T], source ScalarQuery[I, T], op string) Predicate[M] {
	return Predicate[M]{&expression{info: column.info, subquery: func(outer func(fieldInfo) string, args *[]any) (string, error) {
		if op != "IN" && op != "NOT IN" && !validComparison(Comparison(op)) {
			return "", fmt.Errorf("orm: invalid scalar subquery comparison")
		}
		sql, next, err := source.compile(*args)
		if err != nil {
			return "", err
		}
		*args = next
		return outer(column.field) + " " + op + " (" + sql + ")", nil
	}}}
}
func Exists[M, I any](table Table[M], source ModelQuery[I]) Predicate[M] {
	return Predicate[M]{&expression{info: table.info, subquery: func(_ func(fieldInfo) string, args *[]any) (string, error) {
		prefix, initial, err := compileCTEs(source.requiredCTEs(), *args)
		if err != nil {
			return "", err
		}
		sql, next, err := source.compile(initial)
		if err != nil {
			return "", err
		}
		*args = next
		return "EXISTS (" + prefix + sql + ")", nil
	}}}
}

// ExistsRelated uses exact relation edges to correlate native EXISTS. Filters,
// ordering and paging belong to the child subquery. It supports composite and
// self relations without unqualified inner columns shadowing the parent.
func ExistsRelated[P, C any](relation Relation[P, C], query Query[C]) Predicate[P] {
	return Predicate[P]{&expression{info: relation.parent.info, subquery: func(outer func(fieldInfo) string, args *[]any) (string, error) {
		sql, next, err := relatedSQL(relation, "", query, *args, outer)
		if err != nil {
			return "", err
		}
		*args = next
		return "EXISTS (" + sql + ")", nil
	}}}
}
func relatedSQL[P, C any](relation Relation[P, C], projection string, query Query[C], initial []any, outer func(fieldInfo) string) (string, []any, error) {
	if _, err := NewRelation(relation.parent, relation.child, relation.parts...); err != nil {
		return "", nil, err
	}
	alias := "__neutron_subquery_child"
	for alias == relation.parent.info.name || strings.HasPrefix(outer(relation.parts[0].parentField), quote(alias)+".") {
		alias += "x"
	}
	childField := func(field fieldInfo) string { return quote(alias) + "." + quote(field.name) }
	parts := make([]string, len(relation.parts))
	for i, part := range relation.parts {
		parts[i] = outer(part.parentField) + " = " + childField(part.childField)
	}
	correlation := Predicate[C]{&expression{info: relation.child.info, subquery: func(_ func(fieldInfo) string, _ *[]any) (string, error) {
		return "(" + strings.Join(parts, " AND ") + ")", nil
	}}}
	if query.whereSet {
		query = query.Where(And(query.predicate, correlation))
	} else {
		query = query.Where(correlation)
	}
	columns := "1"
	if projection != "" {
		columns = quote(alias) + "." + quote(projection)
	}
	return selectSQLFrom(relation.child, columns, query, initial, relation.child.info.sqlName()+" AS "+quote(alias), childField)
}

// SelectRelatedScalar returns a parent scalar plus the SQL nullable scalar
// subquery result. Missing children and a present SQL NULL share SQL's NULL
// result; this API does not infer row presence from NULL. Cardinality is checked
// by PostgreSQL, retaining native errors and owned transaction rollback.
func SelectRelatedScalar[P, C, A, T any](ctx context.Context, db Executor, relation Relation[P, C], parent Column[P, A], child Column[C, T], parentQuery Query[P], childQuery Query[C]) ([]Pair[A, Nullable[T]], error) {
	if err := ready(ctx, db); err != nil {
		return nil, wrap("related scalar", err)
	}
	if parent.info == nil || parent.info != relation.parent.info || child.info == nil || child.info != relation.child.info {
		return nil, wrap("related scalar", fmt.Errorf("orm: scalar projections outside relation binding"))
	}
	inner, args, err := relatedSQL(relation, child.field.name, childQuery, nil, func(field fieldInfo) string { return relation.parent.info.sqlName() + "." + quote(field.name) })
	if err != nil {
		return nil, wrap("related scalar", err)
	}
	sql, args, err := selectSQLArgs(relation.parent, quote(parent.field.name)+", ("+inner+")", parentQuery, args)
	if err != nil {
		return nil, wrap("related scalar", err)
	}
	rows, err := db.Query(ctx, sql, args...)
	if err != nil {
		return nil, wrap("related scalar", err)
	}
	defer rows.Close()
	result := []Pair[A, Nullable[T]]{}
	for rows.Next() {
		raw := rows.RawValues()
		if len(raw) != 2 {
			return nil, wrap("related scalar", fmt.Errorf("orm: scalar result shape mismatch"))
		}
		var pair Pair[A, Nullable[T]]
		var destination any
		if raw[1] != nil {
			pair.Second.Valid = true
			destination = scanDestination(reflect.ValueOf(&pair.Second.Value).Elem())
		}
		if err := rows.Scan(scanDestination(reflect.ValueOf(&pair.First).Elem()), destination); err != nil {
			return nil, wrap("related scalar", err)
		}
		result = append(result, pair)
	}
	if err := rows.Err(); err != nil {
		return nil, wrap("related scalar", err)
	}
	return result, nil
}
