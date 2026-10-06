package orm

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// WriteSession owns hook workflows within one transaction callback. It does not
// identify goroutines: overlapping operations and callback reentry are refused.
// Raw SQL is explicit and never discovers or invokes repository hooks.
type WriteSession struct {
	mu           sync.Mutex
	scope        *Scope
	busy, closed bool
	failed       error
	token        *hookToken
	events       []queuedHookEvent
}
type hookToken struct {
	ctx    context.Context
	cancel context.CancelFunc
	valid  bool
	active int
}

func newWriteSession(scope *Scope) *WriteSession { return &WriteSession{scope: scope} }
func (s *WriteSession) fail(err error) {
	if err != nil {
		s.mu.Lock()
		s.failed = errors.Join(s.failed, ErrHookWorkflow, err)
		s.mu.Unlock()
	}
}
func (s *WriteSession) begin(ctx context.Context) error {
	if s == nil || s.scope == nil {
		return ErrHookClosed
	}
	if ctx == nil {
		return fmt.Errorf("orm: nil hook operation context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrHookClosed
	}
	if s.failed != nil {
		return s.failed
	}
	if s.token != nil {
		s.failed = errors.Join(ErrHookWorkflow, ErrHookReentry)
		return ErrHookReentry
	}
	if s.busy {
		return ErrHookBusy
	}
	s.busy = true
	return nil
}
func (s *WriteSession) end(err error) error {
	s.scope.owner.mu.Lock()
	scopeErr := s.scope.failed
	s.scope.owner.mu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.busy = false
	if s.closed {
		err = errors.Join(err, ErrHookClosed)
	}
	if err != nil || scopeErr != nil {
		s.failed = errors.Join(s.failed, ErrHookWorkflow, err, scopeErr)
	}
	return s.failed
}
func (s *WriteSession) closeCallback() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		if s.busy {
			s.failed = errors.Join(s.failed, ErrHookWorkflow, ErrHookLeak)
		}
		if s.token != nil {
			s.token.valid = false
			s.token.cancel()
		}
	}
	return s.failed
}
func (s *WriteSession) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if err := s.begin(ctx); err != nil {
		return pgconn.CommandTag{}, err
	}
	tag, err := s.scope.Exec(ctx, sql, args...)
	return tag, s.end(err)
}
func (s *WriteSession) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if err := s.begin(ctx); err != nil {
		return nil, err
	}
	rows, err := s.scope.Query(ctx, sql, args...)
	if err != nil {
		return nil, s.end(err)
	}
	return &hookRows{Rows: rows, finish: func(err error) { s.end(err) }}, nil
}

// Savepoint discards child notifications after rollback and merges them only
// after PostgreSQL acknowledges RELEASE. Recoverable child failures do not
// poison the parent write session.
func (s *WriteSession) Savepoint(ctx context.Context, callback func(*WriteSession) error) error {
	if callback == nil {
		return fmt.Errorf("orm: hook savepoint callback required")
	}
	if err := s.begin(ctx); err != nil {
		return err
	}
	completed := false
	defer func() {
		if !completed {
			value := recover()
			s.mu.Lock()
			s.busy = false
			s.mu.Unlock()
			if cleanup, ok := value.(*PanicCleanupError); ok {
				s.fail(cleanup)
			}
			if value != nil {
				panic(value)
			}
		}
	}()
	var events []queuedHookEvent
	err := s.scope.Savepoint(ctx, func(scope *Scope) (result error) {
		child := newWriteSession(scope)
		defer child.closeCallback()
		result = callback(child)
		result = errors.Join(result, child.closeCallback())
		child.mu.Lock()
		events = append(events, child.events...)
		child.mu.Unlock()
		return result
	})
	s.mu.Lock()
	s.busy = false
	if s.closed {
		err = errors.Join(err, ErrHookClosed)
	}
	if err == nil {
		s.events = append(s.events, events...)
	}
	if errors.Is(err, ErrTransactionBroken) {
		s.failed = errors.Join(s.failed, ErrHookWorkflow, err)
	}
	s.mu.Unlock()
	completed = true
	return err
}

// HookContext is a callback-scoped capability for explicit extra SQL. Captured
// contexts become terminal when the callback returns. Background operation
// contexts cannot bypass the callback's cancellation or transaction scope.
type HookContext struct {
	session *WriteSession
	token   *hookToken
}

func (h *HookContext) request(ctx context.Context) (context.Context, func(), error) {
	if h == nil || h.session == nil {
		return nil, nil, ErrHookClosed
	}
	s := h.session
	s.mu.Lock()
	if s.closed || s.token != h.token || !h.token.valid {
		s.mu.Unlock()
		return nil, nil, ErrHookClosed
	}
	if s.failed != nil {
		err := s.failed
		s.mu.Unlock()
		return nil, nil, err
	}
	if ctx == nil {
		err := errors.New("orm: nil hook SQL context")
		s.failed = errors.Join(s.failed, ErrHookWorkflow, err)
		s.mu.Unlock()
		return nil, nil, err
	}
	h.token.active++
	parent := h.token.ctx
	s.mu.Unlock()
	request, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(parent, cancel)
	var once sync.Once
	cleanup := func() { once.Do(func() { stop(); cancel(); s.mu.Lock(); h.token.active--; s.mu.Unlock() }) }
	if err := parent.Err(); err != nil {
		cleanup()
		s.fail(err)
		return nil, nil, err
	}
	if err := request.Err(); err != nil {
		cleanup()
		s.fail(err)
		return nil, nil, err
	}
	return request, cleanup, nil
}
func (h *HookContext) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	request, cleanup, err := h.request(ctx)
	if err != nil {
		return pgconn.CommandTag{}, err
	}
	defer cleanup()
	tag, err := h.session.scope.Exec(request, sql, args...)
	h.session.fail(err)
	return tag, err
}
func (h *HookContext) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	request, cleanup, err := h.request(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := h.session.scope.Query(request, sql, args...)
	if err != nil {
		cleanup()
		h.session.fail(err)
		return nil, err
	}
	return &hookRows{Rows: rows, finish: func(err error) { cleanup(); h.session.fail(err) }}, nil
}

type hookRows struct {
	pgx.Rows
	once   sync.Once
	finish func(error)
}

func (r *hookRows) closeWithError(err error) {
	r.once.Do(func() {
		r.Rows.Close()
		r.finish(errors.Join(err, r.Rows.Err()))
	})
}
func (r *hookRows) Close() { r.closeWithError(nil) }
func (r *hookRows) rejectResult(err error) {
	rejectResult(r.Rows, err)
	r.closeWithError(err)
}
func (r *hookRows) Next() bool {
	ok := r.Rows.Next()
	if !ok {
		r.Close()
	}
	return ok
}
func (r *hookRows) Scan(dest ...any) error {
	err := r.Rows.Scan(dest...)
	if err != nil {
		r.closeWithError(err)
	}
	return err
}
func (r *hookRows) Values() ([]any, error) {
	values, err := r.Rows.Values()
	if err != nil {
		r.closeWithError(err)
	}
	return values, err
}

func (s *WriteSession) invoke(ctx context.Context, callback func(*HookContext) error) (result error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	tokenCtx, cancel := context.WithCancel(ctx)
	stopParent := context.AfterFunc(s.scope.ctx, cancel)
	if err := s.scope.ctx.Err(); err != nil {
		stopParent()
		cancel()
		return err
	}
	token := &hookToken{ctx: tokenCtx, cancel: cancel, valid: true}
	s.mu.Lock()
	if s.closed || s.failed != nil {
		s.mu.Unlock()
		stopParent()
		cancel()
		return ErrHookClosed
	}
	s.token = token
	s.mu.Unlock()
	defer func() {
		contextErr := errors.Join(tokenCtx.Err(), s.scope.ctx.Err())
		stopParent()
		cancel()
		s.mu.Lock()
		token.valid = false
		pending := token.active > 0
		s.token = nil
		s.mu.Unlock()
		s.scope.owner.mu.Lock()
		active := s.scope.owner.op != nil && s.scope.owner.op.scope == s.scope
		s.scope.owner.mu.Unlock()
		if active || pending {
			result = errors.Join(result, ErrHookLeak)
		}
		result = errors.Join(result, contextErr)
		s.mu.Lock()
		result = errors.Join(result, s.failed)
		s.mu.Unlock()
		s.fail(result)
	}()
	return callback(&HookContext{session: s, token: token})
}
