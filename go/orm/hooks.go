package orm

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrHookBusy     = errors.New("orm: hook write session already active")
	ErrHookReentry  = errors.New("orm: hook workflow reentry refused")
	ErrHookClosed   = errors.New("orm: hook context or write session is terminal")
	ErrHookWorkflow = errors.New("orm: hook workflow failed; rollback required")
	ErrHookLeak     = errors.New("orm: hook callback retained an active operation or rows")
)

type HookOperation uint8

const (
	HookCreate HookOperation = iota + 1
	HookModify
	HookRemove
)

type HookValueMode uint8

const (
	HookOmitted HookValueMode = iota
	HookSupplied
	HookDefault
)

// InspectedHookValue is a detached intent value, never a mutable write plan.
type InspectedHookValue[T any] struct {
	Mode  HookValueMode
	Value T
}

// WriteIntent is read-only, snapshot-bound input to a statement hook.
type WriteIntent[M any] struct {
	operation   HookOperation
	table       Table[M]
	assignments []Assignment[M]
	predicate   Predicate[M]
}

func (i WriteIntent[M]) Operation() HookOperation { return i.operation }
func (i WriteIntent[M]) Table() Table[M]          { return i.table }
func (i WriteIntent[M]) Condition() (Predicate[M], bool) {
	return i.predicate, i.operation != HookCreate
}
func InspectHookValue[M, T any](intent WriteIntent[M], column Column[M, T]) (InspectedHookValue[T], error) {
	var result InspectedHookValue[T]
	if intent.table.info == nil || column.info != intent.table.info {
		return result, fmt.Errorf("orm: hook value outside intent table")
	}
	for _, assignment := range intent.assignments {
		if assignment.field.index != column.field.index {
			continue
		}
		result.Mode = HookValueMode(assignment.mode)
		if assignment.mode != supplied || assignment.value == nil {
			return result, nil
		}
		value := reflect.ValueOf(assignment.value)
		if column.field.nullable {
			pointer := reflect.New(column.field.typ.Elem())
			pointer.Elem().Set(value)
			result.Value = pointer.Interface().(T)
		} else {
			result.Value = value.Interface().(T)
		}
		return result, nil
	}
	return result, nil
}

// WriteEvent describes one successful statement: model is its INSERT RETURNING
// snapshot only; update/delete report actual affected counts, including zero.
// It does not claim to represent the final row after additional hook SQL.
type WriteEvent[M any] struct {
	Operation    HookOperation
	AffectedRows int64
	Model        *M
}
type BeforeHook[M any] func(context.Context, *HookContext, WriteIntent[M]) error
type AfterHook[M any] func(context.Context, *HookContext, WriteEvent[M]) error
type CommitHook[M any] func(context.Context, WriteEvent[M]) error
type HookSet[M any] struct {
	BeforeWrite, BeforeCreate, BeforeUpdate, BeforeDelete []BeforeHook[M]
	AfterCreate, AfterUpdate, AfterDelete, AfterWrite     []AfterHook[M]
	AfterCommit                                           []CommitHook[M]
}

// HookRepository is immutable table/hook registration, with no implicit DB,
// method discovery or hook interception of raw SQL and borrowed Executors.
type HookRepository[M any] struct {
	table Table[M]
	hooks HookSet[M]
}

func NewHookRepository[M any](table Table[M], hooks HookSet[M]) (HookRepository[M], error) {
	var zero HookRepository[M]
	if table.info == nil {
		return zero, fmt.Errorf("orm: hooks require initialized table")
	}
	hooks.BeforeWrite = append([]BeforeHook[M](nil), hooks.BeforeWrite...)
	hooks.BeforeCreate = append([]BeforeHook[M](nil), hooks.BeforeCreate...)
	hooks.BeforeUpdate = append([]BeforeHook[M](nil), hooks.BeforeUpdate...)
	hooks.BeforeDelete = append([]BeforeHook[M](nil), hooks.BeforeDelete...)
	hooks.AfterCreate = append([]AfterHook[M](nil), hooks.AfterCreate...)
	hooks.AfterUpdate = append([]AfterHook[M](nil), hooks.AfterUpdate...)
	hooks.AfterDelete = append([]AfterHook[M](nil), hooks.AfterDelete...)
	hooks.AfterWrite = append([]AfterHook[M](nil), hooks.AfterWrite...)
	hooks.AfterCommit = append([]CommitHook[M](nil), hooks.AfterCommit...)
	for _, group := range [][]BeforeHook[M]{hooks.BeforeWrite, hooks.BeforeCreate, hooks.BeforeUpdate, hooks.BeforeDelete} {
		for _, callback := range group {
			if callback == nil {
				return zero, fmt.Errorf("orm: nil before hook")
			}
		}
	}
	for _, group := range [][]AfterHook[M]{hooks.AfterCreate, hooks.AfterUpdate, hooks.AfterDelete, hooks.AfterWrite} {
		for _, callback := range group {
			if callback == nil {
				return zero, fmt.Errorf("orm: nil after hook")
			}
		}
	}
	for _, callback := range hooks.AfterCommit {
		if callback == nil {
			return zero, fmt.Errorf("orm: nil commit hook")
		}
	}
	return HookRepository[M]{table, hooks}, nil
}

// HookTransactionOptions adds a cooperative synchronous dispatch deadline to
// native transaction settings. Zero DispatchTimeout means five seconds.
type HookTransactionOptions struct {
	Transaction     TransactionOptions
	DispatchTimeout time.Duration
}

// CommittedDispatchError means the DB already acknowledged COMMIT. Side effects
// may have occurred; replaying the write or automatically retrying hooks is unsafe.
type CommittedDispatchError struct {
	EventIndex, HookIndex int
	Cause                 error
}

func (e *CommittedDispatchError) Error() string {
	return "orm: transaction committed; post-commit dispatch failed (inspect cause)"
}
func (e *CommittedDispatchError) Unwrap() error   { return e.Cause }
func (e *CommittedDispatchError) Committed() bool { return true }

type CommittedHookPanic struct {
	EventIndex, HookIndex int
	Value                 any
}

func (e *CommittedHookPanic) Error() string {
	return "orm: transaction committed; post-commit hook panicked (inspect value)"
}
func (e *CommittedHookPanic) Committed() bool { return true }

type queuedHookEvent struct{ callbacks []func(context.Context) error }

// WithHookTransaction owns one native Scope. Only acknowledged COMMIT dispatches
// queued hooks, after connection release and session terminalization. It never
// retries writes/dispatch or promises durable/exactly-once notification delivery.
func WithHookTransaction(ctx context.Context, pool *pgxpool.Pool, options HookTransactionOptions, callback func(*WriteSession) error) error {
	if callback == nil {
		return fmt.Errorf("orm: hook transaction callback required")
	}
	timeout := options.DispatchTimeout
	if timeout < 0 {
		return fmt.Errorf("orm: negative hook dispatch timeout")
	}
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	events := []queuedHookEvent{}
	err := WithTransaction(ctx, pool, options.Transaction, func(scope *Scope) (result error) {
		session := newWriteSession(scope)
		defer func() { session.closeCallback() }()
		result = callback(session)
		result = errors.Join(result, session.closeCallback())
		session.mu.Lock()
		events = append(events, session.events...)
		session.mu.Unlock()
		return result
	})
	if err != nil {
		return err
	}
	dispatch, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for eventIndex, event := range events {
		for hookIndex, invoke := range event.callbacks {
			if err := dispatch.Err(); err != nil {
				return &CommittedDispatchError{eventIndex, hookIndex, err}
			}
			if err := invokeCommitted(dispatch, invoke, eventIndex, hookIndex); err != nil {
				return &CommittedDispatchError{eventIndex, hookIndex, err}
			}
			if err := dispatch.Err(); err != nil {
				return &CommittedDispatchError{eventIndex, hookIndex, err}
			}
		}
	}
	return nil
}
func invokeCommitted(ctx context.Context, invoke func(context.Context) error, event, hook int) (result error) {
	defer func() {
		if value := recover(); value != nil {
			panic(&CommittedHookPanic{event, hook, value})
		}
	}()
	return invoke(ctx)
}

func cloneHookModel[M any](model M, info *modelInfo) M {
	var result M
	source := reflect.ValueOf(model)
	target := reflect.ValueOf(&result).Elem()
	target.Set(source)
	for _, field := range info.fields {
		if !field.nullable {
			continue
		}
		value := source.Field(field.index)
		if value.IsNil() {
			continue
		}
		pointer := reflect.New(field.typ.Elem())
		pointer.Elem().Set(value.Elem())
		target.Field(field.index).Set(pointer)
	}
	return result
}
func cloneHookEvent[M any](event WriteEvent[M], info *modelInfo) WriteEvent[M] {
	if event.Model != nil {
		model := cloneHookModel(*event.Model, info)
		event.Model = &model
	}
	return event
}
func snapshotHookIntent[M any](operation HookOperation, table Table[M], predicate Predicate[M], assignments []Assignment[M]) WriteIntent[M] {
	values := append([]Assignment[M](nil), assignments...)
	for i := range values {
		values[i].value = snapshot(values[i].value)
	}
	return WriteIntent[M]{operation, table, values, predicate}
}
