// Package ormhttp provides bounded request-owned transactions for net/http and
// Neutron routes. Applications retain pool, HTTP server and exporter ownership.
package ormhttp

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/neutron-build/neutron/go/orm"
)

var ErrResponse = errors.New("ormhttp: response outside configured bounds")

// ApplicationError is returned by a Handler to roll back the request
// transaction and answer with an application-defined client error. Response
// must carry an explicit status from 400 through 499 and satisfy the same
// bounds as a committed Response; otherwise the request fails with the generic
// 503. The response is written only after the rollback completed cleanly, so an
// ApplicationError never reports success for a write. Its Error text is fixed
// and generic. A Handler may wrap it (errors.As is used), but any other error
// text is never sent to the client. Ordinary errors still answer a generic 503.
type ApplicationError struct {
	Response Response
}

func (e *ApplicationError) Error() string {
	return "ormhttp: application response requires rollback"
}

// Response is validated and copied before committing. Streaming/hijacking and
// arbitrary ResponseWriter access cannot precede the database commit boundary.
type Response struct {
	Status int
	Header http.Header
	Body   []byte
}

// RequestSession is borrowed for one request. WriteSession is the one hook
// write session bound to Scope: hooks, deferred graphs and the hook queue join
// the request transaction. It becomes terminal when the Handler returns and
// has no commit or rollback controls. AfterCommit hooks run only after the
// request transaction committed and before the response is written.
type RequestSession struct {
	Scope        *orm.Scope
	Executor     orm.Executor
	Metrics      *orm.QueryMetrics
	WriteSession *orm.WriteSession
}
type Handler func(context.Context, RequestSession, *http.Request) (Response, error)
type Options struct {
	Transaction      orm.TransactionOptions
	MaxResponseBytes int
	Observer         orm.QueryObserver // shared callback must support concurrent requests
	// HookDispatchTimeout cooperatively bounds AfterCommit dispatch; zero means
	// five seconds and negative values are refused.
	HookDispatchTimeout time.Duration
}

// Transactions leases one fresh Scope per admitted request. Shutdown stops new
// admissions, cancels active request scopes, and waits with the supplied budget.
// It does not close the borrowed pool or create goroutines.
type Transactions struct {
	pool    *pgxpool.Pool
	options Options
	mu      sync.Mutex
	closing bool
	next    uint64
	active  map[uint64]context.CancelFunc
	drained chan struct{}
	// transact is the owned transaction runner; tests substitute a fake.
	transact func(context.Context, func(*orm.Scope, *orm.WriteSession) error) error
}

func NewTransactions(pool *pgxpool.Pool, options Options) (*Transactions, error) {
	if pool == nil || options.MaxResponseBytes <= 0 || options.MaxResponseBytes > 64<<20 {
		return nil, ErrResponse
	}
	if options.HookDispatchTimeout < 0 {
		return nil, errors.New("ormhttp: negative hook dispatch timeout")
	}
	t := &Transactions{pool: pool, options: options, active: map[uint64]context.CancelFunc{}, drained: make(chan struct{})}
	t.transact = func(ctx context.Context, callback func(*orm.Scope, *orm.WriteSession) error) error {
		return orm.WithScopedHookTransaction(ctx, t.pool, orm.HookTransactionOptions{Transaction: t.options.Transaction, DispatchTimeout: t.options.HookDispatchTimeout}, callback)
	}
	return t, nil
}
func (t *Transactions) acquire(parent context.Context) (context.Context, func(), bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closing {
		return nil, nil, false
	}
	ctx, cancel := context.WithCancel(parent)
	t.next++
	id := t.next
	t.active[id] = cancel
	return ctx, func() {
		cancel()
		t.mu.Lock()
		defer t.mu.Unlock()
		delete(t.active, id)
		if t.closing && len(t.active) == 0 {
			close(t.drained)
		}
	}, true
}
func (t *Transactions) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return errors.New("ormhttp: shutdown context required")
	}
	t.mu.Lock()
	if !t.closing {
		t.closing = true
		if len(t.active) == 0 {
			close(t.drained)
		}
		for _, cancel := range t.active {
			cancel()
		}
	}
	drained := t.drained
	t.mu.Unlock()
	select {
	case <-drained:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func snapshotResponse(response Response, max int) (Response, error) {
	if response.Status == 0 {
		response.Status = http.StatusOK
	}
	if response.Status < 200 || response.Status > 599 || len(response.Body) > max || ((response.Status == 204 || response.Status == 304) && len(response.Body) > 0) {
		return Response{}, ErrResponse
	}
	if len(response.Header) > 128 {
		return Response{}, ErrResponse
	}
	size := 0
	for key, values := range response.Header {
		if key == "" {
			return Response{}, ErrResponse
		}
		for _, b := range []byte(key) {
			if !((b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || strings.ContainsRune("!#$%&'*+-.^_`|~", rune(b))) {
				return Response{}, ErrResponse
			}
		}
		size += len(key)
		for _, value := range values {
			size += len(value)
			if strings.ContainsAny(value, "\r\n\x00") {
				return Response{}, ErrResponse
			}
		}
		if size > 64<<10 {
			return Response{}, ErrResponse
		}
	}
	response.Header = response.Header.Clone()
	response.Body = append([]byte(nil), response.Body...)
	return response, nil
}

// snapshotApplicationResponse additionally requires an explicit 4xx status: the
// zero Status of an ordinary Response means 200 and is not a client error.
func snapshotApplicationResponse(response Response, max int) (Response, error) {
	if response.Status < 400 || response.Status > 499 {
		return Response{}, ErrResponse
	}
	return snapshotResponse(response, max)
}

// rolledBackCleanly reports that the transaction definitely rolled back, no
// COMMIT was attempted, the connection was not left in a broken or leaking
// state, and the request was not canceled or shut down.
func rolledBackCleanly(ctx context.Context, err error) bool {
	var transaction *orm.TransactionError
	if !errors.As(err, &transaction) || transaction.Outcome != orm.CommitNotAttempted || ctx.Err() != nil {
		return false
	}
	for _, refused := range []error{orm.ErrTransactionBroken, orm.ErrScopeLeak, orm.ErrHookLeak, orm.ErrCommitAmbiguous} {
		if errors.Is(err, refused) {
			return false
		}
	}
	return true
}
func (t *Transactions) Handler(callback Handler) (http.Handler, error) {
	if callback == nil {
		return nil, errors.New("ormhttp: request callback required")
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, release, ok := t.acquire(r.Context())
		if !ok {
			http.Error(w, "database requests are shutting down", http.StatusServiceUnavailable)
			return
		}
		defer release()
		var response Response
		var refusal *Response
		metrics := &orm.QueryMetrics{}
		err := t.transact(ctx, func(scope *orm.Scope, writes *orm.WriteSession) error {
			observed := orm.ObserveExecutor(scope, func(ctx context.Context, event orm.QueryEvent) {
				metrics.Observe(ctx, event)
				// ObserveExecutor isolates this closure's optional callback panic.
				if t.options.Observer != nil {
					t.options.Observer(ctx, event)
				}
			})
			candidate, err := callback(ctx, RequestSession{Scope: scope, Executor: observed, Metrics: metrics, WriteSession: writes}, r.WithContext(ctx))
			if err != nil {
				var application *ApplicationError
				if errors.As(err, &application) && application != nil {
					// Copied now, bounded before any rollback outcome is known. An
					// invalid response leaves refusal nil and fails closed to 503.
					if snapshot, snapshotErr := snapshotApplicationResponse(application.Response, t.options.MaxResponseBytes); snapshotErr == nil {
						refusal = &snapshot
					}
				}
				return err
			}
			response, err = snapshotResponse(candidate, t.options.MaxResponseBytes)
			return err
		})
		if err != nil {
			var committed *orm.CommittedDispatchError
			switch {
			case errors.As(err, &committed):
				// COMMIT was acknowledged but AfterCommit dispatch failed. 503 would
				// invite replay of a committed write, so this is a distinct 500.
				http.Error(w, "database request committed; follow-up processing failed", http.StatusInternalServerError)
			case refusal != nil && rolledBackCleanly(ctx, err):
				writeResponse(w, *refusal)
			default:
				// Commit failure can be indeterminate: the generic response never promises
				// rollback or recommends replay. Original errors remain in core APIs.
				http.Error(w, "database request did not complete", http.StatusServiceUnavailable)
			}
			return
		}
		writeResponse(w, response)
	}), nil
}
func writeResponse(w http.ResponseWriter, response Response) {
	for key, values := range response.Header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	w.WriteHeader(response.Status)
	_, _ = w.Write(response.Body)
}
