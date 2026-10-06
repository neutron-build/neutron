package orm

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/jackc/pgx/v5"
)

// BoundColumn erases only a column's value type, retaining sealed model/table
// binding. Use BindColumn with generated typed columns for COPY/conflict keys.
type BoundColumn[M any] struct {
	info  *modelInfo
	field fieldInfo
}

func BindColumn[M, T any](column Column[M, T]) BoundColumn[M] {
	return BoundColumn[M]{column.info, column.field}
}
func boundColumns[M any](table Table[M], columns []BoundColumn[M]) ([]string, error) {
	if table.info == nil || len(columns) == 0 {
		return nil, fmt.Errorf("orm: initialized table and explicit columns required")
	}
	names := make([]string, len(columns))
	seen := map[int]bool{}
	for i, column := range columns {
		if column.info != table.info || seen[column.field.index] {
			return nil, fmt.Errorf("orm: duplicate or foreign bound column")
		}
		seen[column.field.index] = true
		names[i] = column.field.name
	}
	return names, nil
}

// InsertBatch validates every row before any transaction effect and executes
// bounded INSERT RETURNING statements in one operation-owned child savepoint.
// Core hooks are not implicitly invoked; use explicit graph/hook APIs for hooks.
func InsertBatch[M any](ctx context.Context, scope *Scope, table Table[M], rows [][]Assignment[M], maxRows int) (result []M, err error) {
	if maxRows <= 0 || len(rows) > maxRows {
		return nil, wrap("insert batch", ErrGraphBudget)
	}
	if table.info == nil {
		return nil, wrap("insert batch", fmt.Errorf("orm: uninitialized table"))
	}
	for _, row := range rows {
		if _, _, err := insertSQL(table, row); err != nil {
			return nil, wrap("insert batch", err)
		}
	}
	err = scope.Savepoint(ctx, func(child *Scope) error {
		result = make([]M, 0, len(rows))
		for _, row := range rows {
			model, failure := InsertOne(ctx, child, table, row...)
			if failure != nil {
				return failure
			}
			result = append(result, model)
		}
		return nil
	})
	if err != nil {
		result = nil
	}
	return result, wrap("insert batch", err)
}

// UpsertOne targets an explicit server-enforced unique key. An empty update
// column list means DO NOTHING and can return Valid=false. Other columns are
// assigned from EXCLUDED, including supplied zero/NULL values. It requires an
// owned Scope and savepoint; it neither infers keys nor retries mutations.
func UpsertOne[M any](ctx context.Context, scope *Scope, table Table[M], key []BoundColumn[M], assignments []Assignment[M], updates []BoundColumn[M]) (result Nullable[M], err error) {
	keys, err := boundColumns(table, key)
	if err != nil {
		return result, wrap("upsert", err)
	}
	columns, values, args, err := writeParts(table, assignments)
	if err != nil {
		return result, wrap("upsert", err)
	}
	if len(columns) == 0 {
		return result, wrap("upsert", fmt.Errorf("orm: upsert requires explicit insert columns"))
	}
	keySet := map[string]bool{}
	for _, name := range keys {
		keySet[name] = true
	}
	inserted := map[string]bool{}
	for _, assignment := range assignments {
		if assignment.mode != omitted {
			inserted[assignment.field.name] = true
		}
	}
	for _, name := range keys {
		if !inserted[name] {
			return result, wrap("upsert", fmt.Errorf("orm: conflict key must be supplied or defaulted"))
		}
	}
	quotedKeys := make([]string, len(keys))
	for i, name := range keys {
		quotedKeys[i] = quote(name)
	}
	sql := "INSERT INTO " + table.info.sqlName() + " (" + strings.Join(columns, ", ") + ") VALUES (" + strings.Join(values, ", ") + ") ON CONFLICT (" + strings.Join(quotedKeys, ", ") + ")"
	if len(updates) == 0 {
		sql += " DO NOTHING"
	} else {
		names, failure := boundColumns(table, updates)
		if failure != nil {
			return result, wrap("upsert", failure)
		}
		parts := make([]string, len(names))
		for i, name := range names {
			if keySet[name] || !inserted[name] {
				return result, wrap("upsert", fmt.Errorf("orm: upsert update must be supplied non-key column"))
			}
			parts[i] = quote(name) + " = EXCLUDED." + quote(name)
		}
		sql += " DO UPDATE SET " + strings.Join(parts, ", ")
	}
	if len(args) > 65535 {
		return result, wrap("upsert", fmt.Errorf("orm: PostgreSQL parameter limit exceeded"))
	}
	sql += " RETURNING " + table.info.columns()
	err = scope.Savepoint(ctx, func(child *Scope) error {
		rows, failure := child.Query(ctx, sql, args...)
		if failure != nil {
			return failure
		}
		models, failure := scanModels[M](rows, table.info)
		if failure != nil {
			return failure
		}
		if len(models) > 1 || (len(updates) > 0 && len(models) != 1) {
			return ErrCardinality
		}
		if len(models) == 1 {
			result = Nullable[M]{Value: models[0], Valid: true}
		}
		return nil
	})
	if err != nil {
		result = Nullable[M]{}
	}
	return result, wrap("upsert", err)
}

var ErrCopyBudget = errors.New("orm: COPY row budget exceeded")
var ErrCopyUnsupported = errors.New("orm: executor does not support native COPY")

type nativeCopier interface {
	CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error)
}

type boundedCopySource struct {
	source         pgx.CopyFromSource
	columns        []fieldInfo
	maxRows, count int
	failure        error
}

func (s *boundedCopySource) Next() bool {
	if s.failure != nil {
		return false
	}
	if !s.source.Next() {
		return false
	}
	if s.count == s.maxRows {
		s.failure = ErrCopyBudget
		return false
	}
	s.count++
	return true
}
func (s *boundedCopySource) Values() ([]any, error) {
	values, err := s.source.Values()
	if err != nil {
		s.failure = err
		return nil, err
	}
	if len(values) != len(s.columns) {
		s.failure = fmt.Errorf("orm: COPY value shape differs from bound columns")
		return nil, s.failure
	}
	result := make([]any, len(values))
	for i, value := range values {
		field := s.columns[i]
		typ := field.typ
		if field.nullable {
			typ = typ.Elem()
		}
		value = snapshot(value)
		if value == nil {
			if !field.nullable {
				s.failure = fmt.Errorf("orm: COPY NULL for nonnullable column")
				return nil, s.failure
			}
		} else if reflect.TypeOf(value) != typ {
			s.failure = fmt.Errorf("orm: COPY value type differs from bound column")
			return nil, s.failure
		}
		if err := validateScalarValue(value); err != nil {
			s.failure = err
			return nil, err
		}
		result[i] = value
	}
	return result, nil
}
func (s *boundedCopySource) Err() error { return errors.Join(s.failure, s.source.Err()) }

// CopyInto streams a native pgx COPY source with one row of validation buffering
// and a mandatory finite row budget. It owns a child savepoint: an excess row,
// source/codec error or native failure rolls back every copied row. Values are
// dynamically checked against the sealed typed column metadata. Sources must
// cooperate with cancellation; their arbitrary Next/Values methods cannot be
// forcibly interrupted. Scope/native pgx transaction ownership remains intact.
func CopyInto[M any](ctx context.Context, scope *Scope, table Table[M], columns []BoundColumn[M], maxRows int, source pgx.CopyFromSource) (count int64, err error) {
	names, err := boundColumns(table, columns)
	if err != nil {
		return 0, wrap("copy", err)
	}
	if maxRows <= 0 || maxRows == int(^uint(0)>>1) || source == nil {
		return 0, wrap("copy", fmt.Errorf("orm: finite positive COPY budget and source required"))
	}
	v := reflect.ValueOf(source)
	if v.Kind() == reflect.Pointer && v.IsNil() {
		return 0, wrap("copy", fmt.Errorf("orm: nil COPY source"))
	}
	fields := make([]fieldInfo, len(columns))
	for i, column := range columns {
		fields[i] = column.field
	}
	err = scope.Savepoint(ctx, func(child *Scope) error {
		copier, ok := child.owner.driver.(nativeCopier)
		if !ok {
			return ErrCopyUnsupported
		}
		op, failure := child.acquire(ctx)
		if failure != nil {
			return failure
		}
		defer op.finish()
		bounded := &boundedCopySource{source: source, columns: fields, maxRows: maxRows}
		count, failure = copier.CopyFrom(op.ctx, pgx.Identifier{table.info.schema, table.info.name}, names, bounded)
		failure = errors.Join(operationError(failure, op), bounded.Err())
		if failure == nil && count != int64(bounded.count) {
			failure = ErrCardinality
		}
		return failure
	})
	if err != nil {
		count = 0
	}
	return count, wrap("copy", err)
}
