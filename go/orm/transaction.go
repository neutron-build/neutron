package orm

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrScopeClosed       = errors.New("orm: transaction scope is terminal")
	ErrParentSuspended   = errors.New("orm: parent scope is suspended by child savepoint")
	ErrConcurrentUse     = errors.New("orm: transaction operation already active")
	ErrScopeLeak         = errors.New("orm: callback returned with an active operation or rows")
	ErrTransactionBroken = errors.New("orm: transaction cleanup failed")
	ErrCommitAmbiguous   = errors.New("orm: commit outcome is indeterminate")
)

type CommitOutcome string

const (
	CommitNotAttempted CommitOutcome = "not_attempted"
	CommitRejected     CommitOutcome = "rejected"
	CommitUnknown      CommitOutcome = "indeterminate"
)

// TransactionError distinguishes cancellation before COMMIT from failure
// after COMMIT was invoked. Indeterminate outcomes must not be replayed blindly.
// Native causes and SQLSTATE remain accessible through errors.Is/errors.As.
type TransactionError struct {
	Outcome CommitOutcome
	Cause   error
}

func (e *TransactionError) Error() string {
	return "orm: transaction " + string(e.Outcome) + " (inspect cause)"
}
func (e *TransactionError) Unwrap() error { return e.Cause }
func (e *TransactionError) SQLState() string {
	var p *pgconn.PgError
	if errors.As(e.Cause, &p) {
		return p.Code
	}
	return ""
}

// TransactionOptions are initial PostgreSQL settings. Deferrable is accepted
// only for an explicit serializable, read-only transaction. Cleanup uses a
// separate bounded context, even when the callback context has been canceled.
type TransactionOptions struct {
	Isolation      pgx.TxIsoLevel
	Access         pgx.TxAccessMode
	Deferrable     pgx.TxDeferrableMode
	CleanupTimeout time.Duration
}

func (o TransactionOptions) validate() (pgx.TxOptions, time.Duration, error) {
	switch o.Isolation {
	case "", pgx.ReadCommitted, pgx.RepeatableRead, pgx.Serializable:
	default:
		return pgx.TxOptions{}, 0, fmt.Errorf("orm: unsupported transaction isolation")
	}
	switch o.Access {
	case "", pgx.ReadOnly, pgx.ReadWrite:
	default:
		return pgx.TxOptions{}, 0, fmt.Errorf("orm: unsupported transaction access")
	}
	switch o.Deferrable {
	case "", pgx.Deferrable, pgx.NotDeferrable:
	default:
		return pgx.TxOptions{}, 0, fmt.Errorf("orm: unsupported deferrable mode")
	}
	if o.Deferrable == pgx.Deferrable && (o.Isolation != pgx.Serializable || o.Access != pgx.ReadOnly) {
		return pgx.TxOptions{}, 0, fmt.Errorf("orm: deferrable requires serializable read-only")
	}
	if o.CleanupTimeout < 0 {
		return pgx.TxOptions{}, 0, fmt.Errorf("orm: negative cleanup timeout")
	}
	timeout := o.CleanupTimeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	return pgx.TxOptions{IsoLevel: o.Isolation, AccessMode: o.Access, DeferrableMode: o.Deferrable}, timeout, nil
}

type transactionDriver interface {
	Executor
	Commit(context.Context) error
	Rollback(context.Context) error
}
type operation struct {
	owner           *transactionOwner
	ctx, requestCtx context.Context
	scope           *Scope
	cancel          context.CancelFunc
	stopParent      func() bool
	done            chan struct{}
	once            sync.Once
}
type transactionOwner struct {
	mu             sync.Mutex
	lifecycle      sync.Mutex
	ctx            context.Context
	driver         transactionDriver
	current        *Scope
	op             *operation
	rows           *scopeRows
	closed         bool
	finalizing     bool
	broken         error
	scopes         []*Scope
	nextSavepoint  uint64
	cleanupTimeout time.Duration
	release        func()
	discard        func(context.Context, <-chan struct{}) error
}

// Scope is an Executor valid only during its owning callback. One operation
// lease is held until Exec completes or Query rows close/drain. Overlapping
// operations are rejected; sequential use from different goroutines is not
// identified or forbidden. Callbacks must not leave goroutines or rows behind.
// A child savepoint suspends all parent operations until its callback finishes.
// Raw statements are limited to a single data/query statement; explicit
// transaction controls, CALL, DO, statement-leading DDL and raw SAVEPOINT
// commands are refused. This keyword admission is not a complete SQL parser:
// SELECT INTO may create a table and EXPLAIN ANALYZE may execute its data query.
// Scope is not a SQL sandbox: trusted functions may still mutate session state.
type Scope struct {
	owner      *transactionOwner
	parent     *Scope
	savepoint  string
	closed     bool
	ctx        context.Context
	cancel     context.CancelFunc
	stopParent func() bool
}

var _ Executor = (*Scope)(nil)

func (s *Scope) acquire(ctx context.Context) (*operation, error) {
	if s == nil || s.owner == nil {
		return nil, ErrScopeClosed
	}
	if ctx == nil {
		return nil, fmt.Errorf("orm: nil operation context")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	o := s.owner
	o.mu.Lock()
	defer o.mu.Unlock()
	if s.closed || o.closed {
		return nil, ErrScopeClosed
	}
	if o.finalizing {
		return nil, ErrConcurrentUse
	}
	if o.broken != nil {
		return nil, errors.Join(ErrTransactionBroken, o.broken)
	}
	if o.current != s {
		return nil, ErrParentSuspended
	}
	if o.op != nil {
		return nil, ErrConcurrentUse
	}
	if err := o.ctx.Err(); err != nil {
		return nil, err
	}
	for ancestor := s; ancestor != nil; ancestor = ancestor.parent {
		if ancestor.ctx != nil {
			if err := ancestor.ctx.Err(); err != nil {
				return nil, err
			}
		}
	}
	opCtx, cancel := context.WithCancel(ctx)
	op := &operation{owner: o, ctx: opCtx, requestCtx: ctx, scope: s, cancel: cancel, done: make(chan struct{})}
	parentCtx := s.ctx
	if parentCtx == nil {
		parentCtx = o.ctx
	}
	op.stopParent = context.AfterFunc(parentCtx, cancel)
	o.op = op
	return op, nil
}
func (op *operation) finish() {
	op.once.Do(func() {
		op.stopParent()
		op.cancel()
		o := op.owner
		o.mu.Lock()
		if o.op == op {
			o.op = nil
			o.rows = nil
		}
		close(op.done)
		o.mu.Unlock()
	})
}
func operationError(err error, op *operation) error {
	if err == nil {
		return nil
	}
	if cause := op.requestCtx.Err(); cause != nil {
		return errors.Join(err, cause)
	}
	for ancestor := op.scope; ancestor != nil; ancestor = ancestor.parent {
		if ancestor.ctx != nil {
			if cause := ancestor.ctx.Err(); cause != nil {
				return errors.Join(err, cause)
			}
		}
	}
	if cause := op.owner.ctx.Err(); cause != nil {
		return errors.Join(err, cause)
	}
	return err
}

func (s *Scope) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if err := validateScopeSQL(sql); err != nil {
		return pgconn.CommandTag{}, err
	}
	if err := validateScopeArguments(args); err != nil {
		return pgconn.CommandTag{}, err
	}
	op, err := s.acquire(ctx)
	if err != nil {
		return pgconn.CommandTag{}, err
	}
	defer op.finish()
	tag, err := s.owner.driver.Exec(op.ctx, sql, args...)
	return tag, operationError(err, op)
}
func (s *Scope) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if err := validateScopeSQL(sql); err != nil {
		return nil, err
	}
	if err := validateScopeArguments(args); err != nil {
		return nil, err
	}
	op, err := s.acquire(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := s.owner.driver.Query(op.ctx, sql, args...)
	if err != nil {
		err = operationError(err, op)
		op.finish()
		return nil, err
	}
	wrapped := &scopeRows{rows: rows, op: op}
	o := s.owner
	o.mu.Lock()
	if o.op == op {
		o.rows = wrapped
	}
	terminal := o.closed || s.closed
	o.mu.Unlock()
	if terminal {
		wrapped.Close()
		return nil, ErrScopeClosed
	}
	return wrapped, nil
}

func validateScopeArguments(args []any) error {
	for _, arg := range args {
		if _, ok := arg.(pgx.QueryRewriter); ok {
			return fmt.Errorf("orm: query rewriters forbidden in owned transaction scope")
		}
	}
	return nil
}

// WithTransaction pins one pool connection, begins a native pgx transaction,
// runs the callback, and commits only a clean, uncanceled completed scope.
// Callback errors, panics and operation leaks roll back. Failed cleanup or
// indeterminate COMMIT discards the connection; no automatic retry occurs.
func WithTransaction(ctx context.Context, pool *pgxpool.Pool, options TransactionOptions, callback func(*Scope) error) (result error) {
	if ctx == nil || pool == nil || callback == nil {
		return &TransactionError{CommitNotAttempted, fmt.Errorf("orm: context, pool and callback required")}
	}
	pgOptions, timeout, err := options.validate()
	if err != nil {
		return &TransactionError{CommitNotAttempted, err}
	}
	if err := ctx.Err(); err != nil {
		return &TransactionError{CommitNotAttempted, err}
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return &TransactionError{CommitNotAttempted, err}
	}
	discard := func(clean context.Context, pending <-chan struct{}) error {
		native := conn.Hijack()
		// net.Conn.Close is concurrency-safe and interrupts driver I/O. Native
		// pgx cleanup waits for the operation lease; pgx.Conn is never used
		// concurrently for rollback/close while caller rows are active.
		socketErr := native.PgConn().Conn().Close()
		if errors.Is(socketErr, net.ErrClosed) {
			socketErr = nil
		}
		if pending != nil {
			select {
			case <-pending:
			case <-clean.Done():
				go func() {
					<-pending
					later, done := context.WithTimeout(context.Background(), timeout)
					defer done()
					_ = native.Close(later)
				}()
				return errors.Join(socketErr, clean.Err())
			}
		}
		return errors.Join(socketErr, native.Close(clean))
	}
	tx, err := conn.BeginTx(ctx, pgOptions)
	if err != nil {
		clean, done := context.WithTimeout(context.Background(), timeout)
		defer done()
		return &TransactionError{CommitNotAttempted, errors.Join(err, discard(clean, nil))}
	}
	o := &transactionOwner{ctx: ctx, driver: tx, cleanupTimeout: timeout, release: conn.Release, discard: discard}
	scope := &Scope{owner: o, ctx: ctx}
	o.current = scope
	o.scopes = []*Scope{scope}
	finished := false
	defer func() {
		value := recover()
		if !finished {
			cleanup := o.finishRoot(scope, fmt.Errorf("orm: callback terminated without completion"))
			if value != nil {
				if errors.Is(cleanup, ErrTransactionBroken) {
					panic(&PanicCleanupError{value, cleanup})
				}
				panic(value)
			}
		}
	}()
	err = callback(scope)
	result = o.finishRoot(scope, err)
	finished = true
	return result
}

// PanicCleanupError is raised only if an original callback panic also causes
// failed transaction cleanup. Otherwise the original panic value is re-raised.
type PanicCleanupError struct {
	Value   any
	Cleanup error
}

func (e *PanicCleanupError) Error() string {
	return "orm: callback panic with failed transaction cleanup"
}

func (o *transactionOwner) drain() (bool, <-chan struct{}, error) {
	o.mu.Lock()
	op := o.op
	rows := o.rows
	o.mu.Unlock()
	if op == nil {
		return false, nil, nil
	}
	op.cancel()
	if rows != nil {
		go rows.Close()
	}
	clean, done := context.WithTimeout(context.Background(), o.cleanupTimeout)
	defer done()
	select {
	case <-op.done:
		return true, nil, nil
	case <-clean.Done():
		return true, op.done, clean.Err()
	}
}

func (o *transactionOwner) finishRoot(root *Scope, callbackErr error) error {
	o.lifecycle.Lock()
	defer o.lifecycle.Unlock()
	o.mu.Lock()
	if o.closed {
		o.mu.Unlock()
		return &TransactionError{CommitNotAttempted, ErrScopeClosed}
	}
	o.closed = true
	root.closed = true
	childActive := o.current != root
	o.current = nil
	broken := o.broken
	for _, scope := range o.scopes {
		scope.closed = true
		if scope.stopParent != nil {
			scope.stopParent()
		}
		if scope.cancel != nil {
			scope.cancel()
		}
	}
	o.scopes = nil
	o.mu.Unlock()
	leaked, pending, drainErr := o.drain()
	if leaked || childActive {
		callbackErr = errors.Join(callbackErr, ErrScopeLeak)
	}
	clean, done := context.WithTimeout(context.Background(), o.cleanupTimeout)
	defer done()
	if drainErr != nil {
		err := errors.Join(callbackErr, ErrTransactionBroken, drainErr, o.discard(clean, pending))
		return &TransactionError{CommitNotAttempted, err}
	}
	callbackErr = errors.Join(callbackErr, broken, o.ctx.Err())
	if callbackErr != nil {
		rollbackErr := o.driver.Rollback(clean)
		if rollbackErr != nil {
			return &TransactionError{CommitNotAttempted, errors.Join(callbackErr, ErrTransactionBroken, rollbackErr, o.discard(clean, nil))}
		}
		if broken != nil {
			return &TransactionError{CommitNotAttempted, errors.Join(callbackErr, o.discard(clean, nil))}
		}
		o.release()
		return &TransactionError{CommitNotAttempted, callbackErr}
	}
	// This is the last application cancellation check before calling COMMIT.
	// Cancellation after this point may race network transmission and is
	// classified conservatively from the actual driver result.
	if err := o.ctx.Err(); err != nil {
		rollbackErr := o.driver.Rollback(clean)
		if rollbackErr != nil {
			return &TransactionError{CommitNotAttempted, errors.Join(err, ErrTransactionBroken, rollbackErr, o.discard(clean, nil))}
		}
		o.release()
		return &TransactionError{CommitNotAttempted, err}
	}
	err := o.driver.Commit(o.ctx)
	if err == nil {
		o.release()
		return nil
	}
	outcome := classifyCommit(err)
	if outcome == CommitUnknown {
		err = errors.Join(ErrCommitAmbiguous, err)
	}
	// Never recycle a connection following a failed COMMIT, even when the
	// server reports a definite rejection or no data could have been sent.
	return &TransactionError{outcome, errors.Join(err, o.discard(clean, nil))}
}

func classifyCommit(err error) CommitOutcome {
	if errors.Is(err, pgx.ErrTxCommitRollback) {
		return CommitRejected
	}
	var p *pgconn.PgError
	if errors.As(err, &p) {
		// Explicit definite COMMIT rejection cases. A nonempty SQLSTATE alone
		// is insufficient: class 08 and 40003 remain indeterminate.
		switch p.Code {
		case "40001", "40P01", "23502", "23503", "23505", "23514", "23P01", "25006", "25P02":
			return CommitRejected
		}
		return CommitUnknown
	}
	if pgconn.SafeToRetry(err) {
		return CommitNotAttempted
	}
	return CommitUnknown
}

// Savepoint owns a nested callback. Child error/panic rolls back to and
// releases its savepoint before the parent resumes. Failure of either cleanup
// step marks the whole transaction broken; parent operations then fail.
func (s *Scope) Savepoint(ctx context.Context, callback func(*Scope) error) (result error) {
	if callback == nil {
		return fmt.Errorf("orm: savepoint callback required")
	}
	op, err := s.acquire(ctx)
	if err != nil {
		return err
	}
	o := s.owner
	o.mu.Lock()
	o.nextSavepoint++
	name := fmt.Sprintf("neutron_sp_%d", o.nextSavepoint)
	o.mu.Unlock()
	_, err = o.driver.Exec(op.ctx, "SAVEPOINT "+quote(name))
	if err != nil {
		err = operationError(err, op)
		op.finish()
		return err
	}
	childCtx, childCancel := context.WithCancel(ctx)
	parentCtx := s.ctx
	if parentCtx == nil {
		parentCtx = o.ctx
	}
	child := &Scope{owner: o, parent: s, savepoint: name, ctx: childCtx, cancel: childCancel, stopParent: context.AfterFunc(parentCtx, childCancel)}
	o.mu.Lock()
	if o.closed || s.closed {
		o.mu.Unlock()
		child.stopParent()
		child.cancel()
		op.finish()
		return ErrScopeClosed
	}
	o.current = child
	o.scopes = append(o.scopes, child)
	o.mu.Unlock()
	op.finish()
	if err := child.ctx.Err(); err != nil {
		return child.finishChild(err)
	}
	finished := false
	defer func() {
		value := recover()
		if !finished {
			cleanup := child.finishChild(fmt.Errorf("orm: savepoint callback terminated without completion"))
			if value != nil {
				if errors.Is(cleanup, ErrTransactionBroken) {
					panic(&PanicCleanupError{value, cleanup})
				}
				panic(value)
			}
		}
	}()
	err = callback(child)
	result = child.finishChild(err)
	finished = true
	return result
}

func (s *Scope) finishChild(callbackErr error) error {
	o := s.owner
	o.lifecycle.Lock()
	defer o.lifecycle.Unlock()
	o.mu.Lock()
	if o.closed || s.closed {
		s.closed = true
		o.mu.Unlock()
		return errors.Join(callbackErr, ErrScopeClosed)
	}
	if s.ctx != nil {
		callbackErr = errors.Join(callbackErr, s.ctx.Err())
	}
	o.finalizing = true
	for _, candidate := range o.scopes {
		for parent := candidate; parent != nil; parent = parent.parent {
			if parent == s {
				candidate.closed = true
				if candidate != s {
					if candidate.stopParent != nil {
						candidate.stopParent()
					}
					if candidate.cancel != nil {
						candidate.cancel()
					}
				}
				break
			}
		}
	}
	wrongCurrent := o.current != s
	o.mu.Unlock()
	leaked, pending, drainErr := o.drain()
	if leaked || wrongCurrent {
		callbackErr = errors.Join(callbackErr, ErrScopeLeak)
	}
	clean, done := context.WithTimeout(context.Background(), o.cleanupTimeout)
	defer done()
	callbackErr = errors.Join(callbackErr, o.ctx.Err())
	if s.ctx != nil {
		callbackErr = errors.Join(callbackErr, s.ctx.Err())
	}
	var cleanupErr error
	if drainErr != nil {
		cleanupErr = drainErr
	} else {
		if callbackErr != nil {
			_, cleanupErr = o.driver.Exec(clean, "ROLLBACK TO SAVEPOINT "+quote(s.savepoint))
		}
		if cleanupErr == nil {
			_, cleanupErr = o.driver.Exec(clean, "RELEASE SAVEPOINT "+quote(s.savepoint))
		}
		if cleanupErr == nil && callbackErr == nil && s.ctx != nil {
			cleanupErr = s.ctx.Err()
		}
	}
	o.mu.Lock()
	if s.stopParent != nil {
		s.stopParent()
	}
	if s.cancel != nil {
		s.cancel()
	}
	if cleanupErr != nil {
		o.broken = errors.Join(ErrTransactionBroken, cleanupErr)
	}
	o.current = s.parent
	o.finalizing = false
	retained := o.scopes[:0]
	for _, candidate := range o.scopes {
		if !candidate.closed {
			retained = append(retained, candidate)
		}
	}
	o.scopes = retained
	o.mu.Unlock()
	if cleanupErr != nil {
		// Root owns final discard/release. The pending lease is preserved so
		// its finalizer waits or detaches safely, rather than racing rollback.
		_ = pending
		return errors.Join(callbackErr, ErrTransactionBroken, cleanupErr)
	}
	return callbackErr
}
