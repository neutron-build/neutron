package orm

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Executor is satisfied by pgxpool.Pool, pgx.Conn and pgx.Tx. The caller owns
// opening, closing and transaction boundaries. No query protocol option is
// inserted into arguments; the executor's PostgreSQL defaults are preserved.
type Executor interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

var ErrNotFound = errors.New("orm: no row found")
var ErrCardinality = errors.New("orm: expected exactly one row")

// Error preserves the original cause, including pgconn.PgError and context
// errors. Error text avoids server messages that can include parameter values;
// the retained Cause may contain sensitive PostgreSQL details and is opt-in.
type Error struct {
	Operation string
	Cause     error
}

func (e *Error) Error() string {
	if state := e.SQLState(); state != "" {
		return "orm: " + e.Operation + ": PostgreSQL SQLSTATE " + state
	}
	for _, known := range []error{ErrNotFound, ErrCardinality, context.Canceled, context.DeadlineExceeded} {
		if errors.Is(e.Cause, known) {
			return "orm: " + e.Operation + ": " + known.Error()
		}
	}
	return "orm: " + e.Operation + ": operation failed (inspect cause)"
}
func (e *Error) Unwrap() error { return e.Cause }
func (e *Error) SQLState() string {
	var p *pgconn.PgError
	if errors.As(e.Cause, &p) {
		return p.Code
	}
	return ""
}
func wrap(operation string, err error) error {
	if err == nil {
		return nil
	}
	return &Error{operation, err}
}

func ready(ctx context.Context, db Executor) error {
	if ctx == nil {
		return fmt.Errorf("orm: nil context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if db == nil {
		return fmt.Errorf("orm: nil executor")
	}
	v := reflect.ValueOf(db)
	if v.Kind() == reflect.Pointer && v.IsNil() {
		return fmt.Errorf("orm: nil executor")
	}
	return nil
}

func scanModels[M any](rows pgx.Rows, info *modelInfo) ([]M, error) {
	defer rows.Close()
	result := []M{}
	for rows.Next() {
		var model M
		v := reflect.ValueOf(&model).Elem()
		dest := make([]any, len(info.fields))
		for i, f := range info.fields {
			dest[i] = v.Field(f.index).Addr().Interface()
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}
		result = append(result, model)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func Select[M any](ctx context.Context, db Executor, table Table[M], query Query[M]) ([]M, error) {
	if err := ready(ctx, db); err != nil {
		return nil, wrap("select", err)
	}
	if table.info == nil {
		return nil, wrap("select", fmt.Errorf("uninitialized table"))
	}
	sql, args, err := selectSQL(table, table.info.columns(), query)
	if err != nil {
		return nil, wrap("select", err)
	}
	rows, err := db.Query(ctx, sql, args...)
	if err != nil {
		return nil, wrap("select", err)
	}
	result, err := scanModels[M](rows, table.info)
	return result, wrap("select", err)
}

// SelectOne checks actual cardinality; callers may not supply a limiting or
// offset query that could conceal extra matches. It fetches at most two rows.
func SelectOne[M any](ctx context.Context, db Executor, table Table[M], query Query[M]) (M, error) {
	var zero M
	if query.limited || query.offset != 0 {
		return zero, wrap("select one", fmt.Errorf("limit/offset not allowed for cardinality check"))
	}
	rows, err := Select(ctx, db, table, query.Limit(2))
	if err != nil {
		return zero, err
	}
	if len(rows) == 0 {
		return zero, wrap("select one", ErrNotFound)
	}
	if len(rows) != 1 {
		return zero, wrap("select one", ErrCardinality)
	}
	return rows[0], nil
}

func SelectColumn[M, T any](ctx context.Context, db Executor, column Column[M, T], query Query[M]) ([]T, error) {
	if err := ready(ctx, db); err != nil {
		return nil, wrap("project", err)
	}
	if column.info == nil {
		return nil, wrap("project", fmt.Errorf("uninitialized column"))
	}
	sql, args, err := selectSQL(Table[M]{column.info}, quote(column.field.name), query)
	if err != nil {
		return nil, wrap("project", err)
	}
	rows, err := db.Query(ctx, sql, args...)
	if err != nil {
		return nil, wrap("project", err)
	}
	defer rows.Close()
	result := []T{}
	for rows.Next() {
		var value T
		if err := rows.Scan(&value); err != nil {
			return nil, wrap("project", err)
		}
		result = append(result, value)
	}
	if err := rows.Err(); err != nil {
		return nil, wrap("project", err)
	}
	return result, nil
}

type Pair[A, B any] struct {
	First  A
	Second B
}

func SelectPair[M, A, B any](ctx context.Context, db Executor, first Column[M, A], second Column[M, B], query Query[M]) ([]Pair[A, B], error) {
	if err := ready(ctx, db); err != nil {
		return nil, wrap("project pair", err)
	}
	if first.info == nil || first.info != second.info {
		return nil, wrap("project pair", fmt.Errorf("columns require same initialized table scope"))
	}
	sql, args, err := selectSQL(Table[M]{first.info}, quote(first.field.name)+", "+quote(second.field.name), query)
	if err != nil {
		return nil, wrap("project pair", err)
	}
	rows, err := db.Query(ctx, sql, args...)
	if err != nil {
		return nil, wrap("project pair", err)
	}
	defer rows.Close()
	result := []Pair[A, B]{}
	for rows.Next() {
		var value Pair[A, B]
		if err := rows.Scan(&value.First, &value.Second); err != nil {
			return nil, wrap("project pair", err)
		}
		result = append(result, value)
	}
	if err := rows.Err(); err != nil {
		return nil, wrap("project pair", err)
	}
	return result, nil
}

// InsertOne executes one VALUES tuple (or DEFAULT VALUES) with RETURNING. It
// checks the returned result even if a trigger suppresses the insertion. A
// result/decode/network error does not imply rollback; callers must not replay
// ambiguous writes blindly and may use a caller-owned transaction for policy.
func InsertOne[M any](ctx context.Context, db Executor, table Table[M], assignments ...Assignment[M]) (M, error) {
	var zero M
	if err := ready(ctx, db); err != nil {
		return zero, wrap("insert", err)
	}
	sql, args, err := insertSQL(table, assignments)
	if err != nil {
		return zero, wrap("insert", err)
	}
	rows, err := db.Query(ctx, sql, args...)
	if err != nil {
		return zero, wrap("insert", err)
	}
	result, err := scanModels[M](rows, table.info)
	if err != nil {
		return zero, wrap("insert", err)
	}
	if len(result) != 1 {
		return zero, wrap("insert", ErrCardinality)
	}
	return result[0], nil
}

// Update and Delete require explicit valid predicates and return affected-row
// counts. They make no exactly-one or automatic rollback promise.
func Update[M any](ctx context.Context, db Executor, table Table[M], predicate Predicate[M], assignments ...Assignment[M]) (int64, error) {
	if err := ready(ctx, db); err != nil {
		return 0, wrap("update", err)
	}
	sql, args, err := updateSQL(table, predicate, assignments)
	if err != nil {
		return 0, wrap("update", err)
	}
	tag, err := db.Exec(ctx, sql, args...)
	if err != nil {
		return 0, wrap("update", err)
	}
	return tag.RowsAffected(), nil
}
func Delete[M any](ctx context.Context, db Executor, table Table[M], predicate Predicate[M]) (int64, error) {
	if err := ready(ctx, db); err != nil {
		return 0, wrap("delete", err)
	}
	sql, args, err := deleteSQL(table, predicate)
	if err != nil {
		return 0, wrap("delete", err)
	}
	tag, err := db.Exec(ctx, sql, args...)
	if err != nil {
		return 0, wrap("delete", err)
	}
	return tag.RowsAffected(), nil
}
