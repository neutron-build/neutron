package orm

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

var ErrProjectionShape = errors.New("orm: bound SQL projection is incomplete or incompatible")
var ErrBoundBudget = errors.New("orm: bound SQL row budget exceeded")

// BoundSQL is a trusted developer SQL escape hatch. SQL text is never assembled
// from values; pgx owns placeholder parsing/protocol. It may contain native
// writes/CTEs and makes no read-only assertion. The complete-model result shape
// and native OIDs are validated before any row becomes a model.
type BoundSQL[M any] struct {
	table Table[M]
	sql   string
	args  []any
}

func NewBoundSQL[M any](table Table[M], statement string, args ...any) (BoundSQL[M], error) {
	if table.info == nil || strings.TrimSpace(statement) == "" || len(args) > 65535 {
		return BoundSQL[M]{}, fmt.Errorf("orm: initialized table, SQL and finite parameters required")
	}
	values := make([]any, len(args))
	for i, arg := range args {
		value := snapshot(arg)
		if value != nil && !supportedScalar(reflect.TypeOf(value)) {
			return BoundSQL[M]{}, ErrScalarValue
		}
		if err := validateScalarValue(value); err != nil {
			return BoundSQL[M]{}, err
		}
		values[i] = value
	}
	return BoundSQL[M]{table, statement, values}, nil
}
func CompileBound[M any](plan BoundSQL[M]) (CompiledQuery, error) {
	if plan.table.info == nil {
		return CompiledQuery{}, fmt.Errorf("orm: uninitialized bound SQL")
	}
	return CompiledQuery{SQL: plan.sql, Args: append([]any{}, plan.args...)}, nil
}
func rejectResult(rows pgx.Rows, err error) {
	if owned, ok := rows.(interface{ rejectResult(error) }); ok {
		owned.rejectResult(err)
	}
}

var projectionTypes = pgtype.NewMap()

func validateBoundProjection(rows pgx.Rows, info *modelInfo) error {
	fields := rows.FieldDescriptions()
	if len(fields) != len(info.fields) {
		return ErrProjectionShape
	}
	for i, field := range info.fields {
		actual := fields[i]
		if actual.Name != field.name {
			return fmt.Errorf("%w: field %s position %d", ErrProjectionShape, quote(field.name), i)
		}
		if len(info.catalogOIDs) > i {
			matched := false
			for _, oid := range info.catalogOIDs[i] {
				matched = matched || oid == actual.DataTypeOID
			}
			if !matched {
				return fmt.Errorf("%w: field %s OID %d", ErrProjectionShape, quote(field.name), actual.DataTypeOID)
			}
			continue
		}
		codec, ok := projectionTypes.TypeForOID(actual.DataTypeOID)
		if !ok {
			return fmt.Errorf("%w: field %s OID %d", ErrCodecUnsupported, quote(field.name), actual.DataTypeOID)
		}
		catalog := catalogCodec{oid: actual.DataTypeOID, kind: "b"}
		switch native := codec.Codec.(type) {
		case *pgtype.ArrayCodec:
			catalog.element = native.ElementType.OID
		case *pgtype.RangeCodec:
			catalog.kind = "r"
		}
		typ := field.typ
		if typ.Kind() == reflect.Pointer {
			typ = typ.Elem()
		}
		if !qualifiedCatalogCodec(typ, catalog) {
			return fmt.Errorf("%w: field %s OID %d", ErrProjectionShape, quote(field.name), actual.DataTypeOID)
		}
	}
	return nil
}
func SelectBound[M any](ctx context.Context, db Executor, plan BoundSQL[M], maxRows int) ([]M, error) {
	if err := ready(ctx, db); err != nil {
		return nil, wrap("bound SQL", err)
	}
	if plan.table.info == nil || maxRows <= 0 {
		return nil, wrap("bound SQL", fmt.Errorf("orm: bound plan and positive row budget required"))
	}
	rows, err := db.Query(ctx, plan.sql, plan.args...)
	if err != nil {
		return nil, wrap("bound SQL", err)
	}
	defer rows.Close()
	if err := validateBoundProjection(rows, plan.table.info); err != nil {
		rejectResult(rows, err)
		return nil, wrap("bound SQL", err)
	}
	result := []M{}
	for rows.Next() {
		if len(result) == maxRows {
			rejectResult(rows, ErrBoundBudget)
			return nil, wrap("bound SQL", ErrBoundBudget)
		}
		var model M
		value := reflect.ValueOf(&model).Elem()
		dest := make([]any, len(plan.table.info.fields))
		for i, field := range plan.table.info.fields {
			dest[i] = scanDestination(value.Field(field.index))
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, wrap("bound SQL", err)
		}
		result = append(result, model)
	}
	if err := rows.Err(); err != nil {
		return nil, wrap("bound SQL", err)
	}
	return result, nil
}
