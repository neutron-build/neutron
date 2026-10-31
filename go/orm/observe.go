package orm

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// QueryEvent has bounded categories only. SQL text, arguments, native error
// messages, credentials and connection metadata are never included.
type QueryEvent struct {
	Operation  string        // query or exec; not inferred from SQL text
	Duration   time.Duration // includes result consumption until close/exhaustion
	Rows       int64         // consumed rows for query, affected rows for exec
	ErrorClass string        // empty, canceled, deadline, postgres, decode, or failed
	SQLState   string        // validated five-character native SQLSTATE, otherwise empty
}

// QueryObserver runs synchronously in the operation context. Panics are isolated
// from database outcomes. Observers must return promptly and must not reuse the
// active transaction; no background worker or global observer is installed.
type QueryObserver func(context.Context, QueryEvent)

// ObserveExecutor decorates the supplied executor without owning its lifetime.
// Wrap each request/transaction's Scope to retain concurrency and result leases.
func ObserveExecutor(db Executor, observer QueryObserver) Executor {
	return &observedExecutor{db: db, observer: observer}
}

type observedExecutor struct {
	db       Executor
	observer QueryObserver
}

func safeObserve(ctx context.Context, observer QueryObserver, event QueryEvent) {
	if observer == nil {
		return
	}
	defer func() { _ = recover() }()
	observer(ctx, event)
}
func queryError(err error, decode bool) (string, string) {
	if err == nil {
		return "", ""
	}
	if errors.Is(err, context.Canceled) {
		return "canceled", ""
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline", ""
	}
	var native *pgconn.PgError
	if errors.As(err, &native) {
		state := native.Code
		if len(state) != 5 {
			state = ""
		} else {
			for _, b := range []byte(state) {
				if !(b >= '0' && b <= '9') && !(b >= 'A' && b <= 'Z') {
					state = ""
					break
				}
			}
		}
		return "postgres", state
	}
	if decode || errors.Is(err, ErrScopeDecode) {
		return "decode", ""
	}
	return "failed", ""
}
func (db *observedExecutor) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	started := time.Now()
	if err := ready(ctx, db.db); err != nil {
		return pgconn.CommandTag{}, err
	}
	tag, err := db.db.Exec(ctx, sql, args...)
	class, state := queryError(err, false)
	safeObserve(ctx, db.observer, QueryEvent{Operation: "exec", Duration: time.Since(started), Rows: tag.RowsAffected(), ErrorClass: class, SQLState: state})
	return tag, err
}
func (db *observedExecutor) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	started := time.Now()
	if err := ready(ctx, db.db); err != nil {
		return nil, err
	}
	rows, err := db.db.Query(ctx, sql, args...)
	if err != nil {
		class, state := queryError(err, false)
		safeObserve(ctx, db.observer, QueryEvent{Operation: "query", Duration: time.Since(started), ErrorClass: class, SQLState: state})
		return nil, err
	}
	return &observedRows{Rows: rows, ctx: ctx, observer: db.observer, started: started}, nil
}

type observedRows struct {
	pgx.Rows
	ctx         context.Context
	observer    QueryObserver
	started     time.Time
	once        sync.Once
	consumed    atomic.Int64
	errorMu     sync.Mutex
	resultError error
	decode      bool
}

func (r *observedRows) record(err error, decode bool) {
	if err == nil {
		return
	}
	r.errorMu.Lock()
	defer r.errorMu.Unlock()
	r.resultError = errors.Join(r.resultError, err)
	r.decode = r.decode || decode
}
func (r *observedRows) finish() {
	r.once.Do(func() {
		r.errorMu.Lock()
		err := errors.Join(r.resultError, r.Rows.Err())
		decode := r.decode
		r.errorMu.Unlock()
		class, state := queryError(err, decode)
		safeObserve(r.ctx, r.observer, QueryEvent{Operation: "query", Duration: time.Since(r.started), Rows: r.consumed.Load(), ErrorClass: class, SQLState: state})
	})
}
func (r *observedRows) Close() { r.Rows.Close(); r.finish() }
func (r *observedRows) Next() bool {
	if r.Rows.Next() {
		r.consumed.Add(1)
		return true
	}
	r.finish()
	return false
}
func (r *observedRows) Scan(dest ...any) error {
	err := r.Rows.Scan(dest...)
	r.record(err, true)
	return err
}
func (r *observedRows) Values() ([]any, error) {
	values, err := r.Rows.Values()
	r.record(err, true)
	return values, err
}
func (r *observedRows) Err() error {
	r.errorMu.Lock()
	err := r.resultError
	r.errorMu.Unlock()
	return errors.Join(err, r.Rows.Err())
}

// Forward result rejection so projection/budget refusal still poisons an owned
// Scope even when a caller swallows the returned ORM error.
func (r *observedRows) rejectResult(err error) {
	r.record(err, true)
	if rejector, ok := r.Rows.(interface{ rejectResult(error) }); ok {
		rejector.rejectResult(err)
	}
}

// QueryMetrics counts physical observed calls, not inferred SQL statements in a
// multi-statement string. Separate request instances prevent cross-request state.
type QueryMetrics struct {
	calls, queries, execs, rows, errors, canceled, nanoseconds atomic.Int64
}
type QueryMetricsSnapshot struct {
	Calls, Queries, Execs, Rows, Errors, Canceled int64
	Duration                                      time.Duration
}

func (m *QueryMetrics) Observe(_ context.Context, event QueryEvent) {
	m.calls.Add(1)
	if event.Operation == "query" {
		m.queries.Add(1)
	} else if event.Operation == "exec" {
		m.execs.Add(1)
	}
	m.rows.Add(event.Rows)
	m.nanoseconds.Add(int64(event.Duration))
	if event.ErrorClass != "" {
		m.errors.Add(1)
	}
	if event.ErrorClass == "canceled" || event.ErrorClass == "deadline" {
		m.canceled.Add(1)
	}
}

// Snapshot reads concurrent counters safely; during active calls individual
// counters may belong to adjacent updates. After quiescence the totals are exact.
func (m *QueryMetrics) Snapshot() QueryMetricsSnapshot {
	return QueryMetricsSnapshot{Calls: m.calls.Load(), Queries: m.queries.Load(), Execs: m.execs.Load(), Rows: m.rows.Load(), Errors: m.errors.Load(), Canceled: m.canceled.Load(), Duration: time.Duration(m.nanoseconds.Load())}
}
