package orm

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// ModelQuery is an immutable complete-model SELECT/set-operation plan. Its
// result shape is the original mapped struct, never a partial zero-filled model.
type ModelQuery[M any] struct {
	table       Table[M]
	query       Query[M]
	kind        string
	left, right *ModelQuery[M]
}

func NewModelQuery[M any](table Table[M], query Query[M]) (ModelQuery[M], error) {
	if table.info == nil {
		return ModelQuery[M]{}, fmt.Errorf("orm: uninitialized model query")
	}
	if _, _, err := selectSQL(table, table.info.columns(), query); err != nil {
		return ModelQuery[M]{}, err
	}
	return ModelQuery[M]{table: table, query: query}, nil
}
func setModelQuery[M any](left, right ModelQuery[M], kind string) (ModelQuery[M], error) {
	if left.table.info == nil || right.table.info == nil {
		return ModelQuery[M]{}, fmt.Errorf("orm: set operation requires initialized complete-model plans")
	}
	// M is identical at compile time. Runtime metadata must retain identical
	// column order/types/names, including internal derived bindings.
	if !reflect.DeepEqual(left.table.info.fields, right.table.info.fields) {
		return ModelQuery[M]{}, fmt.Errorf("orm: set operation model projection shape differs")
	}
	return ModelQuery[M]{table: left.table, kind: kind, left: &left, right: &right}, nil
}
func Union[M any](left, right ModelQuery[M]) (ModelQuery[M], error) {
	return setModelQuery(left, right, "UNION")
}
func UnionAll[M any](left, right ModelQuery[M]) (ModelQuery[M], error) {
	return setModelQuery(left, right, "UNION ALL")
}
func Intersect[M any](left, right ModelQuery[M]) (ModelQuery[M], error) {
	return setModelQuery(left, right, "INTERSECT")
}
func Except[M any](left, right ModelQuery[M]) (ModelQuery[M], error) {
	return setModelQuery(left, right, "EXCEPT")
}
func (p ModelQuery[M]) compile(initial []any) (string, []any, error) {
	if p.table.info == nil {
		return "", nil, fmt.Errorf("orm: uninitialized model query")
	}
	if p.kind == "" {
		return selectSQLArgs(p.table, p.table.info.columns(), p.query, initial)
	}
	if p.left == nil || p.right == nil {
		return "", nil, fmt.Errorf("orm: uninitialized set branches")
	}
	left, args, err := p.left.compile(initial)
	if err != nil {
		return "", nil, err
	}
	right, args, err := p.right.compile(args)
	if err != nil {
		return "", nil, err
	}
	return "(" + left + ") " + p.kind + " (" + right + ")", args, nil
}
func SelectModels[M any](ctx context.Context, db Executor, plan ModelQuery[M]) ([]M, error) {
	if err := ready(ctx, db); err != nil {
		return nil, wrap("model query", err)
	}
	sql, args, err := plan.compile(nil)
	if err != nil {
		return nil, wrap("model query", err)
	}
	rows, err := db.Query(ctx, sql, args...)
	if err != nil {
		return nil, wrap("model query", err)
	}
	models, err := scanModels[M](rows, plan.table.info)
	return models, wrap("model query", err)
}

// Derived is a sealed typed CTE result. DerivedColumn creates columns for its
// new binding; original columns cannot accidentally filter the derived scope.
type Derived[M any] struct {
	table             Table[M]
	source            ModelQuery[M]
	relation          *Relation[M, M]
	maxDepth, maxRows int
	depth             string
}

func NewCTE[M any](alias string, source ModelQuery[M]) (Derived[M], error) {
	if err := identifier(alias); err != nil {
		return Derived[M]{}, err
	}
	if source.table.info == nil {
		return Derived[M]{}, fmt.Errorf("orm: CTE source must be initialized")
	}
	if _, _, err := source.compile(nil); err != nil {
		return Derived[M]{}, err
	}
	info := *source.table.info
	info.schema = ""
	info.name = alias
	return Derived[M]{table: Table[M]{&info}, source: source}, nil
}
func DerivedColumn[M, T any](derived Derived[M], goField string) (Column[M, T], error) {
	return NewColumn[M, T](derived.table, goField)
}

var ErrRecursiveBudget = errors.New("orm: recursive result budget exceeded")

// NewRecursiveCTE follows exact typed relation edges from a complete-model
// anchor. UNION ALL preserves graph multiplicity, including cycles, while an
// explicit finite maxDepth bounds recursion and maxRows bounds delivered rows.
// It does not certify acyclic input or bound PostgreSQL's internal work/memory.
func NewRecursiveCTE[M any](alias string, anchor ModelQuery[M], relation Relation[M, M], maxDepth, maxRows int) (Derived[M], error) {
	derived, err := NewCTE(alias, anchor)
	if err != nil {
		return Derived[M]{}, err
	}
	if anchor.table.info != relation.parent.info {
		return Derived[M]{}, fmt.Errorf("orm: recursive anchor outside parent relation binding")
	}
	if _, err := NewRelation(relation.parent, relation.child, relation.parts...); err != nil {
		return Derived[M]{}, err
	}
	if maxDepth < 0 || maxDepth > 128 || maxRows <= 0 || maxRows == int(^uint(0)>>1) {
		return Derived[M]{}, fmt.Errorf("orm: recursive depth 0..128 and finite positive row budget required")
	}
	depth := "__neutron_depth"
	seen := map[string]bool{}
	for _, field := range derived.table.info.fields {
		seen[field.name] = true
	}
	for seen[depth] {
		depth += "x"
	}
	derived.relation = &relation
	derived.maxDepth = maxDepth
	derived.maxRows = maxRows
	derived.depth = depth
	return derived, nil
}
func (d Derived[M]) compile(columns string, query Query[M]) (string, []any, error) {
	if d.table.info == nil {
		return "", nil, fmt.Errorf("orm: uninitialized derived query")
	}
	source, args, err := d.source.compile(nil)
	if err != nil {
		return "", nil, err
	}
	prefix := "WITH " + quote(d.table.info.name) + " AS (" + source + ") "
	if d.relation != nil {
		// Distinct private aliases prevent correlation shadowing even when the
		// user CTE alias equals a compiler's default internal alias.
		anchorAlias := "__neutron_anchor"
		for anchorAlias == d.table.info.name {
			anchorAlias += "x"
		}
		parentAlias := "__neutron_recursive_parent"
		for parentAlias == d.table.info.name {
			parentAlias += "x"
		}
		childAlias := "__neutron_recursive_child"
		for childAlias == d.table.info.name || childAlias == parentAlias {
			childAlias += "x"
		}
		anchorFields := make([]string, len(d.table.info.fields))
		childFields := make([]string, len(anchorFields))
		names := make([]string, len(anchorFields))
		for i, field := range d.table.info.fields {
			names[i] = quote(field.name)
			anchorFields[i] = quote(anchorAlias) + "." + quote(field.name)
			childFields[i] = quote(childAlias) + "." + quote(field.name)
		}
		parts := make([]string, len(d.relation.parts))
		for i, part := range d.relation.parts {
			parts[i] = quote(parentAlias) + "." + quote(part.parentField.name) + " = " + quote(childAlias) + "." + quote(part.childField.name)
		}
		args = append(args, d.maxDepth)
		recursive := "SELECT " + strings.Join(childFields, ", ") + ", " + quote(parentAlias) + "." + quote(d.depth) + " + 1 FROM " + quote(d.table.info.name) + " AS " + quote(parentAlias) + " INNER JOIN " + d.relation.child.info.sqlName() + " AS " + quote(childAlias) + " ON (" + strings.Join(parts, " AND ") + ") WHERE " + quote(parentAlias) + "." + quote(d.depth) + fmt.Sprintf(" < $%d", len(args))
		anchor := "SELECT " + strings.Join(anchorFields, ", ") + ", 0 FROM (" + source + ") AS " + quote(anchorAlias)
		prefix = "WITH RECURSIVE " + quote(d.table.info.name) + " (" + strings.Join(names, ", ") + ", " + quote(d.depth) + ") AS ((" + anchor + ") UNION ALL (" + recursive + ")) "
		if !query.limited || query.limit > d.maxRows+1 {
			query = query.Limit(d.maxRows + 1)
		}
	}
	sql, args, err := selectSQLArgs(d.table, columns, query, args)
	if err != nil {
		return "", nil, err
	}
	return prefix + sql, args, nil
}
func SelectDerived[M any](ctx context.Context, db Executor, derived Derived[M], query Query[M]) ([]M, error) {
	if err := ready(ctx, db); err != nil {
		return nil, wrap("derived", err)
	}
	if derived.table.info == nil {
		return nil, wrap("derived", fmt.Errorf("orm: uninitialized derived binding"))
	}
	sql, args, err := derived.compile(derived.table.info.columns(), query)
	if err != nil {
		return nil, wrap("derived", err)
	}
	rows, err := db.Query(ctx, sql, args...)
	if err != nil {
		return nil, wrap("derived", err)
	}
	models, err := scanModels[M](rows, derived.table.info)
	if err == nil && derived.relation != nil && len(models) > derived.maxRows {
		return nil, wrap("derived", ErrRecursiveBudget)
	}
	return models, wrap("derived", err)
}
func SelectDerivedColumn[M, T any](ctx context.Context, db Executor, derived Derived[M], column Column[M, T], query Query[M]) ([]T, error) {
	if err := ready(ctx, db); err != nil {
		return nil, wrap("derived column", err)
	}
	if column.info == nil || column.info != derived.table.info {
		return nil, wrap("derived column", fmt.Errorf("orm: column outside derived binding"))
	}
	sql, args, err := derived.compile(quote(column.field.name), query)
	if err != nil {
		return nil, wrap("derived column", err)
	}
	rows, err := db.Query(ctx, sql, args...)
	if err != nil {
		return nil, wrap("derived column", err)
	}
	defer rows.Close()
	result := []T{}
	for rows.Next() {
		var value T
		if err := rows.Scan(scanDestination(reflect.ValueOf(&value).Elem())); err != nil {
			return nil, wrap("derived column", err)
		}
		result = append(result, value)
		if derived.relation != nil && len(result) > derived.maxRows {
			return nil, wrap("derived column", ErrRecursiveBudget)
		}
	}
	return result, wrap("derived column", rows.Err())
}
