// Package ormhttp provides bounded request-owned transactions for net/http and
// Neutron routes. Applications retain pool, HTTP server and exporter ownership.
package ormhttp

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/neutron-build/neutron/go/orm"
)

var ErrResponse = errors.New("ormhttp: response outside configured bounds")

// Response is validated and copied before committing. Streaming/hijacking and
// arbitrary ResponseWriter access cannot precede the database commit boundary.
type Response struct {
	Status int
	Header http.Header
	Body   []byte
}
type RequestSession struct {
	Scope    *orm.Scope
	Executor orm.Executor
	Metrics  *orm.QueryMetrics
}
type Handler func(context.Context, RequestSession, *http.Request) (Response, error)
type Options struct {
	Transaction      orm.TransactionOptions
	MaxResponseBytes int
	Observer         orm.QueryObserver // shared callback must support concurrent requests
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
}

func NewTransactions(pool *pgxpool.Pool, options Options) (*Transactions, error) {
	if pool == nil || options.MaxResponseBytes <= 0 || options.MaxResponseBytes > 64<<20 {
		return nil, ErrResponse
	}
	return &Transactions{pool: pool, options: options, active: map[uint64]context.CancelFunc{}, drained: make(chan struct{})}, nil
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
		metrics := &orm.QueryMetrics{}
		err := orm.WithTransaction(ctx, t.pool, t.options.Transaction, func(scope *orm.Scope) error {
			observed := orm.ObserveExecutor(scope, func(ctx context.Context, event orm.QueryEvent) {
				metrics.Observe(ctx, event)
				// ObserveExecutor isolates this closure's optional callback panic.
				if t.options.Observer != nil {
					t.options.Observer(ctx, event)
				}
			})
			candidate, err := callback(ctx, RequestSession{Scope: scope, Executor: observed, Metrics: metrics}, r.WithContext(ctx))
			if err != nil {
				return err
			}
			response, err = snapshotResponse(candidate, t.options.MaxResponseBytes)
			return err
		})
		if err != nil {
			// Commit failure can be indeterminate: the generic response never promises
			// rollback or recommends replay. Original errors remain in core APIs.
			http.Error(w, "database request did not complete", http.StatusServiceUnavailable)
			return
		}
		for key, values := range response.Header {
			for _, value := range values {
				w.Header().Add(key, value)
			}
		}
		w.WriteHeader(response.Status)
		_, _ = w.Write(response.Body)
	}), nil
}
