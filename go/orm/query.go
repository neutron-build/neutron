package orm

import (
	"fmt"
	"strings"
)

type expression struct {
	kind     string
	info     *modelInfo
	field    fieldInfo
	value    any
	children []*expression
}

// Predicate is a bound expression with no public raw SQL interpolation.
// Its zero value is invalid for writes; reads may omit a predicate.
type Predicate[M any] struct{ expr *expression }

func (c Column[M, T]) Eq(value T) Predicate[M] {
	return Predicate[M]{&expression{kind: "=", info: c.info, field: c.field, value: snapshot(value)}}
}
func (c Column[M, T]) Ne(value T) Predicate[M] {
	return Predicate[M]{&expression{kind: "<>", info: c.info, field: c.field, value: snapshot(value)}}
}
func (c Column[M, T]) Gt(value T) Predicate[M] {
	return Predicate[M]{&expression{kind: ">", info: c.info, field: c.field, value: snapshot(value)}}
}
func (c Column[M, T]) Gte(value T) Predicate[M] {
	return Predicate[M]{&expression{kind: ">=", info: c.info, field: c.field, value: snapshot(value)}}
}
func (c Column[M, T]) Lt(value T) Predicate[M] {
	return Predicate[M]{&expression{kind: "<", info: c.info, field: c.field, value: snapshot(value)}}
}
func (c Column[M, T]) Lte(value T) Predicate[M] {
	return Predicate[M]{&expression{kind: "<=", info: c.info, field: c.field, value: snapshot(value)}}
}
func And[M any](predicates ...Predicate[M]) Predicate[M] { return combine("AND", predicates) }
func Or[M any](predicates ...Predicate[M]) Predicate[M]  { return combine("OR", predicates) }
func combine[M any](kind string, predicates []Predicate[M]) Predicate[M] {
	children := make([]*expression, len(predicates))
	for i, p := range predicates {
		children[i] = p.expr
	}
	return Predicate[M]{&expression{kind: kind, children: children}}
}

func renderPredicate(info *modelInfo, e *expression, args *[]any) (string, error) {
	return renderPredicateColumns(info, e, args, func(field fieldInfo) string { return quote(field.name) })
}
func renderPredicateColumns(info *modelInfo, e *expression, args *[]any, columnSQL func(fieldInfo) string) (string, error) {
	if e == nil {
		return "", fmt.Errorf("orm: explicit nonempty predicate required")
	}
	if e.kind == "AND" || e.kind == "OR" {
		if len(e.children) == 0 {
			return "", fmt.Errorf("orm: empty predicate group")
		}
		parts := make([]string, len(e.children))
		for i, c := range e.children {
			s, err := renderPredicateColumns(info, c, args, columnSQL)
			if err != nil {
				return "", err
			}
			parts[i] = s
		}
		return "(" + strings.Join(parts, " "+e.kind+" ") + ")", nil
	}
	if e.info == nil || e.info != info {
		return "", fmt.Errorf("orm: predicate belongs to another table scope")
	}
	if e.value == nil {
		if !e.field.nullable {
			return "", fmt.Errorf("orm: NULL for nonnullable column")
		}
		if e.kind == "=" {
			return columnSQL(e.field) + " IS NULL", nil
		}
		if e.kind == "<>" {
			return columnSQL(e.field) + " IS NOT NULL", nil
		}
		return "", fmt.Errorf("orm: ordered NULL comparison is invalid")
	}
	if err := validateScalarValue(e.value); err != nil {
		return "", err
	}
	*args = append(*args, e.value)
	return fmt.Sprintf("%s %s $%d", columnSQL(e.field), e.kind, len(*args)), nil
}

type Order[M any] struct {
	info       *modelInfo
	field      fieldInfo
	descending bool
}

func (c Column[M, T]) Asc() Order[M]  { return Order[M]{c.info, c.field, false} }
func (c Column[M, T]) Desc() Order[M] { return Order[M]{c.info, c.field, true} }

// Query has value-style builder methods; every call leaves the original intact.
type Query[M any] struct {
	predicate         Predicate[M]
	order             []Order[M]
	limit, offset     int
	limited, whereSet bool
}

func (q Query[M]) Where(p Predicate[M]) Query[M] { q.predicate = p; q.whereSet = true; return q }
func (q Query[M]) OrderBy(order ...Order[M]) Query[M] {
	q.order = append([]Order[M](nil), order...)
	return q
}
func (q Query[M]) Limit(limit int) Query[M]   { q.limit = limit; q.limited = true; return q }
func (q Query[M]) Offset(offset int) Query[M] { q.offset = offset; return q }

func selectSQL[M any](table Table[M], columns string, q Query[M]) (string, []any, error) {
	if table.info == nil {
		return "", nil, fmt.Errorf("orm: uninitialized table")
	}
	if q.limit < 0 || q.offset < 0 {
		return "", nil, fmt.Errorf("orm: negative limit/offset")
	}
	args := []any{}
	sql := "SELECT " + columns + " FROM " + table.info.sqlName()
	if q.whereSet {
		p, err := renderPredicate(table.info, q.predicate.expr, &args)
		if err != nil {
			return "", nil, err
		}
		sql += " WHERE " + p
	}
	if len(q.order) > 0 {
		parts := make([]string, len(q.order))
		for i, o := range q.order {
			if o.info != table.info {
				return "", nil, fmt.Errorf("orm: order belongs to another table scope")
			}
			dir := " ASC"
			if o.descending {
				dir = " DESC"
			}
			parts[i] = quote(o.field.name) + dir
		}
		sql += " ORDER BY " + strings.Join(parts, ", ")
	}
	if q.limited {
		args = append(args, q.limit)
		sql += fmt.Sprintf(" LIMIT $%d", len(args))
	}
	if q.offset > 0 {
		args = append(args, q.offset)
		sql += fmt.Sprintf(" OFFSET $%d", len(args))
	}
	return sql, args, nil
}
