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
	values   []any
	operator string
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
func Not[M any](predicate Predicate[M]) Predicate[M] {
	return Predicate[M]{&expression{kind: "NOT", children: []*expression{predicate.expr}}}
}

// In and NotIn bind each element, preserving SQL's three-valued NULL semantics.
// Empty membership is FALSE (IN) or TRUE (NOT IN). Values are snapshotted.
func (c Column[M, T]) In(values ...T) Predicate[M]    { return c.membership("IN", "", values) }
func (c Column[M, T]) NotIn(values ...T) Predicate[M] { return c.membership("NOT IN", "", values) }

type Comparison string

const (
	Equal        Comparison = "="
	NotEqual     Comparison = "<>"
	Greater      Comparison = ">"
	GreaterEqual Comparison = ">="
	Less         Comparison = "<"
	LessEqual    Comparison = "<="
)

// CompareAny/CompareAll implement finite-list ANY/ALL using bound scalar
// comparisons, with FALSE/TRUE for an empty list and native SQL NULL behavior.
func (c Column[M, T]) CompareAny(op Comparison, values ...T) Predicate[M] {
	return c.membership("ANY", string(op), values)
}
func (c Column[M, T]) CompareAll(op Comparison, values ...T) Predicate[M] {
	return c.membership("ALL", string(op), values)
}
func (c Column[M, T]) membership(kind, op string, values []T) Predicate[M] {
	bound := make([]any, len(values))
	for i, value := range values {
		bound[i] = snapshot(value)
	}
	return Predicate[M]{&expression{kind: kind, info: c.info, field: c.field, values: bound, operator: op}}
}
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
	if e.kind == "NOT" {
		if len(e.children) != 1 {
			return "", fmt.Errorf("orm: invalid NOT predicate")
		}
		inner, err := renderPredicateColumns(info, e.children[0], args, columnSQL)
		return "NOT (" + inner + ")", err
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
	if e.kind == "IN" || e.kind == "NOT IN" || e.kind == "ANY" || e.kind == "ALL" {
		if e.kind == "ANY" || e.kind == "ALL" {
			switch Comparison(e.operator) {
			case Equal, NotEqual, Greater, GreaterEqual, Less, LessEqual:
			default:
				return "", fmt.Errorf("orm: invalid quantified comparison")
			}
		}
		if len(e.values) == 0 {
			if e.kind == "NOT IN" || e.kind == "ALL" {
				return "TRUE", nil
			}
			return "FALSE", nil
		}
		parts := make([]string, len(e.values))
		for i, value := range e.values {
			if err := validateScalarValue(value); err != nil {
				return "", err
			}
			*args = append(*args, value)
			parts[i] = fmt.Sprintf("$%d", len(*args))
			if e.kind == "ANY" || e.kind == "ALL" {
				parts[i] = columnSQL(e.field) + " " + e.operator + " " + parts[i]
			}
		}
		if e.kind == "IN" || e.kind == "NOT IN" {
			return columnSQL(e.field) + " " + e.kind + " (" + strings.Join(parts, ", ") + ")", nil
		}
		join := " OR "
		if e.kind == "ALL" {
			join = " AND "
		}
		return "(" + strings.Join(parts, join) + ")", nil
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
	nulls      string
}

func (c Column[M, T]) Asc() Order[M] { return Order[M]{info: c.info, field: c.field} }
func (c Column[M, T]) Desc() Order[M] {
	return Order[M]{info: c.info, field: c.field, descending: true}
}
func (o Order[M]) NullsFirst() Order[M] { o.nulls = " NULLS FIRST"; return o }
func (o Order[M]) NullsLast() Order[M]  { o.nulls = " NULLS LAST"; return o }

// Query has value-style builder methods; every call leaves the original intact.
type Query[M any] struct {
	predicate         Predicate[M]
	order             []Order[M]
	limit, offset     int
	limited, whereSet bool
	distinct          bool
}

func (q Query[M]) Where(p Predicate[M]) Query[M] { q.predicate = p; q.whereSet = true; return q }
func (q Query[M]) OrderBy(order ...Order[M]) Query[M] {
	q.order = append([]Order[M](nil), order...)
	return q
}
func (q Query[M]) Limit(limit int) Query[M]   { q.limit = limit; q.limited = true; return q }
func (q Query[M]) Offset(offset int) Query[M] { q.offset = offset; return q }
func (q Query[M]) Distinct() Query[M]         { q.distinct = true; return q }

func selectSQL[M any](table Table[M], columns string, q Query[M]) (string, []any, error) {
	return selectSQLArgs(table, columns, q, nil)
}
func selectSQLArgs[M any](table Table[M], columns string, q Query[M], initial []any) (string, []any, error) {
	if table.info == nil {
		return "", nil, fmt.Errorf("orm: uninitialized table")
	}
	if q.limit < 0 || q.offset < 0 {
		return "", nil, fmt.Errorf("orm: negative limit/offset")
	}
	args := append([]any{}, initial...)
	selectWord := "SELECT "
	if q.distinct {
		selectWord = "SELECT DISTINCT "
	}
	sql := selectWord + columns + " FROM " + table.info.sqlName()
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
			parts[i] = quote(o.field.name) + dir + o.nulls
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
	if len(args) > 65535 {
		return "", nil, fmt.Errorf("orm: query exceeds PostgreSQL parameter limit")
	}
	return sql, args, nil
}

// CompileSelect validates and compiles a complete-model SELECT without a
// connection. SQL contains placeholders; Args contains detached scalar values.
type CompiledQuery struct {
	SQL  string
	Args []any
}

func CompileSelect[M any](table Table[M], query Query[M]) (CompiledQuery, error) {
	if table.info == nil {
		return CompiledQuery{}, fmt.Errorf("orm: uninitialized table")
	}
	sql, args, err := selectSQL(table, table.info.columns(), query)
	return CompiledQuery{sql, args}, err
}
