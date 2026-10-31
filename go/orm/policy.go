package orm

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
)

// ScopedTable binds an immutable application predicate (for example a composite
// tenant key) to explicit reads and writes. It never changes raw SQL or a Table.
type ScopedTable[M any] struct {
	table Table[M]
	scope Predicate[M]
}

func NewScopedTable[M any](table Table[M], predicate Predicate[M]) (ScopedTable[M], error) {
	if table.info == nil {
		return ScopedTable[M]{}, fmt.Errorf("orm: uninitialized scoped table")
	}
	args := []any{}
	if _, err := renderPredicate(table.info, predicate.expr, &args); err != nil {
		return ScopedTable[M]{}, err
	}
	return ScopedTable[M]{table, predicate}, nil
}
func (s ScopedTable[M]) condition(predicate Predicate[M]) (Predicate[M], error) {
	if s.table.info == nil {
		return Predicate[M]{}, fmt.Errorf("orm: uninitialized scoped table")
	}
	args := []any{}
	if _, err := renderPredicate(s.table.info, predicate.expr, &args); err != nil {
		return Predicate[M]{}, err
	}
	return And(s.scope, predicate), nil
}

// Query adds the scope to a caller query without mutating either input.
func (s ScopedTable[M]) Query(query Query[M]) Query[M] {
	if query.whereSet {
		return query.Where(And(s.scope, query.predicate))
	}
	return query.Where(s.scope)
}
func (s ScopedTable[M]) Select(ctx context.Context, db Executor, query Query[M]) ([]M, error) {
	return Select(ctx, db, s.table, s.Query(query))
}
func (s ScopedTable[M]) SelectOne(ctx context.Context, db Executor, query Query[M]) (M, error) {
	return SelectOne(ctx, db, s.table, s.Query(query))
}
func (s ScopedTable[M]) Update(ctx context.Context, db Executor, predicate Predicate[M], assignments ...Assignment[M]) (int64, error) {
	p, err := s.condition(predicate)
	if err != nil {
		return 0, wrap("scoped update", err)
	}
	return Update(ctx, db, s.table, p, assignments...)
}
func (s ScopedTable[M]) Delete(ctx context.Context, db Executor, predicate Predicate[M]) (int64, error) {
	p, err := s.condition(predicate)
	if err != nil {
		return 0, wrap("scoped delete", err)
	}
	return Delete(ctx, db, s.table, p)
}

// SoftDelete is opt-in, using one nullable timestamptz/time.Time column. The
// application scope remains present in active, deleted and restore paths.
// It does not intercept core Delete, hook repositories, associations or raw SQL.
type SoftDelete[M any] struct {
	scoped ScopedTable[M]
	column Column[M, *time.Time]
}

func NewSoftDelete[M any](scoped ScopedTable[M], column Column[M, *time.Time]) (SoftDelete[M], error) {
	if scoped.table.info == nil || column.info != scoped.table.info || !column.field.nullable {
		return SoftDelete[M]{}, fmt.Errorf("orm: soft-delete column outside initialized scope")
	}
	return SoftDelete[M]{scoped, column}, nil
}
func (p SoftDelete[M]) Active(query Query[M]) Query[M] {
	query = p.scoped.Query(query)
	return query.Where(And(query.predicate, p.column.Eq(nil)))
}

// IncludingDeleted applies application scope only; it is an independent value,
// so administrator reads/restoration cannot change another caller's active view.
func (p SoftDelete[M]) IncludingDeleted(query Query[M]) Query[M] { return p.scoped.Query(query) }
func (p SoftDelete[M]) Select(ctx context.Context, db Executor, query Query[M]) ([]M, error) {
	return Select(ctx, db, p.scoped.table, p.Active(query))
}
func (p SoftDelete[M]) Update(ctx context.Context, db Executor, predicate Predicate[M], assignments ...Assignment[M]) (int64, error) {
	for _, assignment := range assignments {
		if assignment.info == p.column.info && assignment.field.index == p.column.field.index {
			return 0, wrap("soft-delete update", fmt.Errorf("orm: use Remove or Restore to change deletion state"))
		}
	}
	condition, err := p.scoped.condition(predicate)
	if err != nil {
		return 0, wrap("soft-delete update", err)
	}
	return Update(ctx, db, p.scoped.table, And(condition, p.column.Eq(nil)), assignments...)
}
func (p SoftDelete[M]) Remove(ctx context.Context, db Executor, predicate Predicate[M], at time.Time) (int64, error) {
	condition, err := p.scoped.condition(predicate)
	if err != nil {
		return 0, wrap("soft-delete remove", err)
	}
	return Update(ctx, db, p.scoped.table, And(condition, p.column.Eq(nil)), Set(p.column, Some(&at)))
}
func (p SoftDelete[M]) Restore(ctx context.Context, db Executor, predicate Predicate[M]) (int64, error) {
	condition, err := p.scoped.condition(predicate)
	if err != nil {
		return 0, wrap("soft-delete restore", err)
	}
	return Update(ctx, db, p.scoped.table, And(condition, p.column.Ne(nil)), Set(p.column, Some((*time.Time)(nil))))
}

var ErrVersionConflict = errors.New("orm: optimistic version does not match")
var ErrScopeMutation = errors.New("orm: guarded mutation failed; rollback required")

// VersionedUpdate requires an owned Scope. It atomically matches the explicit
// predicate and expected version, applies zero-safe assignments and increments
// the int64 version. Zero or multiple affected rows poison that Scope, even if
// the caller swallows the error. Savepoint rollback may recover the parent.
func VersionedUpdate[M any](ctx context.Context, scope *Scope, table Table[M], predicate Predicate[M], version Column[M, int64], expected int64, assignments ...Assignment[M]) error {
	if err := ready(ctx, scope); err != nil {
		return wrap("versioned update", err)
	}
	if table.info == nil || version.info != table.info || expected < 0 || expected == math.MaxInt64 {
		return wrap("versioned update", fmt.Errorf("orm: initialized int64 version and finite nonnegative expected value required"))
	}
	for _, assignment := range assignments {
		if assignment.info == version.info && assignment.field.index == version.field.index {
			return wrap("versioned update", fmt.Errorf("orm: version assignment is owned by guard"))
		}
	}
	columns, values, args, err := writeParts(table, assignments)
	if err != nil {
		return wrap("versioned update", err)
	}
	if len(columns) == 0 {
		return wrap("versioned update", fmt.Errorf("orm: guarded update has no supplied assignments"))
	}
	where, err := renderPredicate(table.info, And(predicate, version.Eq(expected)).expr, &args)
	if err != nil {
		return wrap("versioned update", err)
	}
	if len(args) > 65535 {
		return wrap("versioned update", fmt.Errorf("orm: PostgreSQL parameter limit exceeded"))
	}
	parts := make([]string, 0, len(columns)+1)
	for i, column := range columns {
		parts = append(parts, column+" = "+values[i])
	}
	parts = append(parts, quote(version.field.name)+" = "+quote(version.field.name)+" + 1")
	sql := "UPDATE " + table.info.sqlName() + " SET " + strings.Join(parts, ", ") + " WHERE " + where
	if err := validateScopeSQL(sql); err != nil {
		return wrap("versioned update", err)
	}
	if err := validateScopeArguments(args); err != nil {
		return wrap("versioned update", err)
	}
	op, err := scope.acquire(ctx)
	if err != nil {
		return wrap("versioned update", err)
	}
	defer op.finish()
	tag, err := scope.owner.driver.Exec(op.ctx, sql, args...)
	err = operationError(err, op)
	if err != nil {
		scope.owner.mu.Lock()
		scope.failed = errors.Join(scope.failed, ErrScopeMutation, err)
		scope.owner.mu.Unlock()
		return wrap("versioned update", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	err = ErrVersionConflict
	if tag.RowsAffected() > 1 {
		err = ErrCardinality
	}
	scope.owner.mu.Lock()
	scope.failed = errors.Join(scope.failed, ErrScopeMutation, err)
	scope.owner.mu.Unlock()
	return wrap("versioned update", err)
}
