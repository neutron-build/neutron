package orm

import (
	"context"
	"errors"
	"fmt"
	"reflect"
)

var ErrStreamBudget = errors.New("orm: stream row budget exceeded")

// Stream visits complete typed models with constant application buffering.
// maxRows is mandatory; a further row returns ErrStreamBudget before delivery.
// Returning false stops normally. Every path closes the native result, including
// callback panic. This uses pgx results, not a server DECLARE cursor; PostgreSQL
// may still execute or buffer the query. Borrowed transaction policy is unchanged.
func Stream[M any](ctx context.Context, db Executor, table Table[M], query Query[M], maxRows int, visit func(M) (bool, error)) (result error) {
	if err := ready(ctx, db); err != nil {
		return wrap("stream", err)
	}
	if maxRows <= 0 || maxRows == int(^uint(0)>>1) || visit == nil {
		return wrap("stream", fmt.Errorf("positive finite row budget and callback required"))
	}
	if table.info == nil {
		return wrap("stream", fmt.Errorf("uninitialized table"))
	}
	if !query.limited || query.limit > maxRows+1 {
		query = query.Limit(maxRows + 1)
	}
	sql, args, err := selectSQL(table, table.info.columns(), query)
	if err != nil {
		return wrap("stream", err)
	}
	rows, err := db.Query(ctx, sql, args...)
	if err != nil {
		return wrap("stream", err)
	}
	defer func() { rows.Close(); result = wrap("stream", errors.Join(result, rows.Err(), ctx.Err())) }()
	for count := 0; rows.Next(); count++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if count == maxRows {
			return ErrStreamBudget
		}
		var model M
		value := reflect.ValueOf(&model).Elem()
		dest := make([]any, len(table.info.fields))
		for i, field := range table.info.fields {
			dest[i] = scanDestination(value.Field(field.index))
		}
		if err := rows.Scan(dest...); err != nil {
			return err
		}
		more, err := visit(model)
		if err != nil {
			return err
		}
		if !more {
			return nil
		}
	}
	return nil
}

// SelectOptional refuses concealed cardinality like SelectOne but treats no row
// as Valid=false. It never presents omitted fields as legitimate model zeros.
func SelectOptional[M any](ctx context.Context, db Executor, table Table[M], query Query[M]) (Nullable[M], error) {
	model, err := SelectOne(ctx, db, table, query)
	if errors.Is(err, ErrNotFound) {
		return Nullable[M]{}, nil
	}
	return Nullable[M]{Value: model, Valid: err == nil}, err
}
