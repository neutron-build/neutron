package orm

import (
	"fmt"
	"strings"
)

type writeMode uint8

const (
	omitted writeMode = iota
	supplied
	useDefault
)

// Optional's zero value omits a column. Some preserves false, zero and empty
// string. Some((*T)(nil)) supplies NULL to a nullable column. Default requests
// SQL DEFAULT, allowing PostgreSQL to validate whether a default exists.
type Optional[T any] struct {
	value T
	mode  writeMode
}

func Some[T any](value T) Optional[T] { return Optional[T]{value: value, mode: supplied} }
func Default[T any]() Optional[T]     { return Optional[T]{mode: useDefault} }
func Omit[T any]() Optional[T]        { return Optional[T]{} }

// Assignment is constructed through Set to retain column/value type checking.
// Duplicate columns, including omitted assignments, are rejected.
type Assignment[M any] struct {
	info  *modelInfo
	field fieldInfo
	value any
	mode  writeMode
}

func Set[M, T any](column Column[M, T], value Optional[T]) Assignment[M] {
	return Assignment[M]{column.info, column.field, snapshot(value.value), value.mode}
}

func writeParts[M any](table Table[M], assignments []Assignment[M]) ([]string, []string, []any, error) {
	if table.info == nil {
		return nil, nil, nil, fmt.Errorf("orm: uninitialized table")
	}
	seen := map[string]bool{}
	columns := []string{}
	values := []string{}
	args := []any{}
	for _, a := range assignments {
		if a.info != table.info {
			return nil, nil, nil, fmt.Errorf("orm: assignment belongs to another table scope")
		}
		if seen[a.field.name] {
			return nil, nil, nil, fmt.Errorf("orm: duplicate write column %q", a.field.name)
		}
		seen[a.field.name] = true
		if a.mode == omitted {
			continue
		}
		columns = append(columns, quote(a.field.name))
		if a.mode == useDefault {
			values = append(values, "DEFAULT")
			continue
		}
		if a.value == nil && !a.field.nullable {
			return nil, nil, nil, fmt.Errorf("orm: NULL for nonnullable column")
		}
		if err := validateScalarValue(a.value); err != nil {
			return nil, nil, nil, err
		}
		args = append(args, a.value)
		values = append(values, fmt.Sprintf("$%d", len(args)))
	}
	return columns, values, args, nil
}

func insertSQL[M any](table Table[M], assignments []Assignment[M]) (string, []any, error) {
	columns, values, args, err := writeParts(table, assignments)
	if err != nil {
		return "", nil, err
	}
	sql := "INSERT INTO " + table.info.sqlName()
	if len(columns) == 0 {
		sql += " DEFAULT VALUES"
	} else {
		sql += " (" + strings.Join(columns, ", ") + ") VALUES (" + strings.Join(values, ", ") + ")"
	}
	return sql + " RETURNING " + table.info.columns(), args, nil
}

func updateSQL[M any](table Table[M], predicate Predicate[M], assignments []Assignment[M]) (string, []any, error) {
	columns, values, args, err := writeParts(table, assignments)
	if err != nil {
		return "", nil, err
	}
	if len(columns) == 0 {
		return "", nil, fmt.Errorf("orm: update has no supplied assignments")
	}
	parts := make([]string, len(columns))
	for i, c := range columns {
		parts[i] = c + " = " + values[i]
	}
	where, err := renderPredicate(table.info, predicate.expr, &args)
	if err != nil {
		return "", nil, err
	}
	return "UPDATE " + table.info.sqlName() + " SET " + strings.Join(parts, ", ") + " WHERE " + where, args, nil
}

func deleteSQL[M any](table Table[M], predicate Predicate[M]) (string, []any, error) {
	if table.info == nil {
		return "", nil, fmt.Errorf("orm: uninitialized table")
	}
	args := []any{}
	where, err := renderPredicate(table.info, predicate.expr, &args)
	if err != nil {
		return "", nil, err
	}
	return "DELETE FROM " + table.info.sqlName() + " WHERE " + where, args, nil
}
