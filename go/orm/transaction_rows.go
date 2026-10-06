package orm

import (
	"errors"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// scopeRows retains the scope operation lease for the whole result lifetime.
// It implements pgx.Rows without exposing the raw connection escape hatch.
type scopeRows struct {
	rows     pgx.Rows
	op       *operation
	mu       sync.Mutex
	errMu    sync.Mutex
	override error
	closed   bool
}

var _ pgx.Rows = (*scopeRows)(nil)

func (r *scopeRows) record(err error) {
	r.errMu.Lock()
	r.override = errors.Join(r.override, err)
	r.errMu.Unlock()
}
func (r *scopeRows) recorded() error { r.errMu.Lock(); defer r.errMu.Unlock(); return r.override }
func (r *scopeRows) enter() bool {
	if !r.mu.TryLock() {
		r.record(ErrConcurrentUse)
		return false
	}
	return true
}
func (r *scopeRows) closeLocked() {
	if !r.closed {
		r.rows.Close()
		r.closed = true
		r.op.finish()
	}
}
func (r *scopeRows) Close() { r.mu.Lock(); defer r.mu.Unlock(); r.closeLocked() }
func (r *scopeRows) Err() error {
	if !r.enter() {
		return ErrConcurrentUse
	}
	defer r.mu.Unlock()
	return errors.Join(r.recorded(), operationError(r.rows.Err(), r.op))
}
func (r *scopeRows) Next() bool {
	if !r.enter() {
		return false
	}
	defer r.mu.Unlock()
	if r.closed {
		return false
	}
	if r.recorded() != nil {
		r.closeLocked()
		return false
	}
	next := r.rows.Next()
	if !next {
		r.closeLocked()
	}
	return next
}
func (r *scopeRows) Scan(dest ...any) error {
	if !r.enter() {
		return ErrConcurrentUse
	}
	defer r.mu.Unlock()
	if r.closed {
		return ErrScopeClosed
	}
	err := r.rows.Scan(dest...)
	r.decodeFailure(err)
	return err
}
func (r *scopeRows) decodeFailure(err error) {
	if err == nil {
		return
	}
	r.record(err)
	owner := r.op.owner
	owner.mu.Lock()
	r.op.scope.failed = errors.Join(r.op.scope.failed, ErrScopeDecode, err)
	owner.mu.Unlock()
}
func (r *scopeRows) rejectResult(err error) {
	if !r.enter() {
		return
	}
	defer r.mu.Unlock()
	if !r.closed {
		r.decodeFailure(err)
	}
}
func (r *scopeRows) Values() ([]any, error) {
	if !r.enter() {
		return nil, ErrConcurrentUse
	}
	defer r.mu.Unlock()
	if r.closed {
		return nil, ErrScopeClosed
	}
	values, err := r.rows.Values()
	r.decodeFailure(err)
	return values, err
}
func (r *scopeRows) RawValues() [][]byte {
	if !r.enter() {
		return nil
	}
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	return r.rows.RawValues()
}
func (r *scopeRows) FieldDescriptions() []pgconn.FieldDescription {
	if !r.enter() {
		return nil
	}
	defer r.mu.Unlock()
	return r.rows.FieldDescriptions()
}
func (r *scopeRows) CommandTag() pgconn.CommandTag {
	if !r.enter() {
		return pgconn.CommandTag{}
	}
	defer r.mu.Unlock()
	return r.rows.CommandTag()
}

// Conn deliberately returns nil; leaking raw pgx access would bypass scope
// concurrency, child ownership and terminal checks.
func (r *scopeRows) Conn() *pgx.Conn { return nil }
