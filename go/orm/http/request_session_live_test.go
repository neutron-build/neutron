package ormhttp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/neutron-build/neutron/go/orm"
)

type requestRecord struct {
	ID    int64  `db:"id"`
	Value string `db:"value"`
}

// requestLiveSetup mirrors the sibling live tests: skip without the disposable
// native URL unless NEUTRON_ORM_REQUIRE_LIVE=1, which makes absence a failure.
func requestLiveSetup(t *testing.T) (context.Context, *pgxpool.Pool, *pgx.Conn, string, string) {
	t.Helper()
	url := os.Getenv("NEUTRON_ORM_TEST_DATABASE_URL")
	if url == "" {
		if os.Getenv("NEUTRON_ORM_REQUIRE_LIVE") == "1" {
			t.Fatal("native URL required")
		}
		t.Skip("disposable native PostgreSQL required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	config, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal("invalid native configuration")
	}
	schema := fmt.Sprintf("orm_http_req_%d", time.Now().UnixNano())
	config.MaxConns = 1
	config.MinConns = 0
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	admin, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		clean, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		_ = admin.Close(clean)
	})
	if _, err := admin.Exec(ctx, `CREATE SCHEMA "`+schema+`"; CREATE TABLE "`+schema+`".records(id bigint PRIMARY KEY, value text NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		clean, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		if _, err := admin.Exec(clean, `DROP SCHEMA "`+schema+`" CASCADE`); err != nil {
			t.Error(err)
		}
	})
	return ctx, pool, admin, schema, `"` + schema + `".records`
}

func requestIDs(t *testing.T, ctx context.Context, admin *pgx.Conn, table string) []int64 {
	t.Helper()
	rows, err := admin.Query(ctx, "SELECT id FROM "+table+" ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	ids := []int64{}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return ids
}

func requestPoolReusable(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	bounded, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var value int
	if err := pool.QueryRow(bounded, "SELECT 1").Scan(&value); err != nil || value != 1 {
		t.Fatal("size-one pool not reusable", err)
	}
	if pool.Stat().AcquiredConns() != 0 {
		t.Fatal("request retained pool connection")
	}
}

type requestRetained struct {
	writes *orm.WriteSession
	scope  *orm.Scope
}

func TestPostgresRequestWriteSessionCommitRollbackAndRefusals(t *testing.T) {
	ctx, pool, admin, schema, table := requestLiveSetup(t)
	model, err := orm.NewTable[requestRecord](schema, "records")
	if err != nil {
		t.Fatal(err)
	}
	id, err := orm.NewColumn[requestRecord, int64](model, "ID")
	if err != nil {
		t.Fatal(err)
	}
	value, err := orm.NewColumn[requestRecord, string](model, "Value")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var order []string
	var hookContext *orm.HookContext
	var retained *requestRetained
	record := func(format string, args ...any) {
		mu.Lock()
		defer mu.Unlock()
		order = append(order, fmt.Sprintf(format, args...))
	}
	takeOrder := func() []string {
		mu.Lock()
		defer mu.Unlock()
		taken := order
		order = nil
		return taken
	}
	countRows := func(callback context.Context, query func(context.Context, string, ...any) (pgx.Rows, error)) (int, error) {
		rows, err := query(callback, "SELECT count(*) FROM "+table)
		if err != nil {
			return 0, err
		}
		defer rows.Close()
		var count int
		if !rows.Next() {
			return 0, errors.Join(errors.New("no count row"), rows.Err())
		}
		if err := rows.Scan(&count); err != nil {
			return 0, err
		}
		rows.Close()
		return count, rows.Err()
	}
	repo, err := orm.NewHookRepository(model, orm.HookSet[requestRecord]{
		BeforeWrite: []orm.BeforeHook[requestRecord]{func(callback context.Context, h *orm.HookContext, intent orm.WriteIntent[requestRecord]) error {
			supplied, err := orm.InspectHookValue(intent, id)
			if err != nil {
				return err
			}
			seen, err := countRows(callback, h.Query)
			if err != nil {
				return err
			}
			mu.Lock()
			hookContext = h
			mu.Unlock()
			record("before:%d seen=%d", supplied.Value, seen)
			return nil
		}},
		AfterCreate: []orm.AfterHook[requestRecord]{func(_ context.Context, _ *orm.HookContext, event orm.WriteEvent[requestRecord]) error {
			record("after-create:%d", event.Model.ID)
			return nil
		}},
		AfterCommit: []orm.CommitHook[requestRecord]{func(dispatch context.Context, event orm.WriteEvent[requestRecord]) error {
			// The request transaction has committed and released its only pooled
			// connection, so the size-one pool serves this independent read.
			var count int
			if err := pool.QueryRow(dispatch, "SELECT count(*) FROM "+table).Scan(&count); err != nil {
				return err
			}
			mu.Lock()
			held := retained
			mu.Unlock()
			if _, err := held.writes.Exec(dispatch, "SELECT 1"); !errors.Is(err, orm.ErrHookClosed) {
				return fmt.Errorf("write session not terminal before dispatch: %v", err)
			}
			record("committed:%d rows=%d", event.Model.ID, count)
			return nil
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	hookFailure := errors.New("native_hook_failure_canary")
	failing, err := orm.NewHookRepository(model, orm.HookSet[requestRecord]{
		AfterCreate: []orm.AfterHook[requestRecord]{func(context.Context, *orm.HookContext, orm.WriteEvent[requestRecord]) error { return hookFailure }},
		AfterCommit: []orm.CommitHook[requestRecord]{func(context.Context, orm.WriteEvent[requestRecord]) error {
			record("failing-repo-committed")
			return nil
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	dispatchFailure := errors.New("native_dispatch_canary")
	dispatching, err := orm.NewHookRepository(model, orm.HookSet[requestRecord]{
		AfterCommit: []orm.CommitHook[requestRecord]{func(context.Context, orm.WriteEvent[requestRecord]) error { return dispatchFailure }},
	})
	if err != nil {
		t.Fatal(err)
	}
	insert := func(callback context.Context, session RequestSession, hooks orm.HookRepository[requestRecord], row int64) error {
		_, err := orm.HookInsert(callback, session.WriteSession, hooks, orm.Set(id, orm.Some(row)), orm.Set(value, orm.Some("row")))
		return err
	}
	lifetime, err := NewTransactions(pool, Options{MaxResponseBytes: 32})
	if err != nil {
		t.Fatal(err)
	}
	bounded := []byte(`{"code":409}`)
	handler, err := lifetime.Handler(func(callback context.Context, session RequestSession, r *http.Request) (Response, error) {
		mu.Lock()
		retained = &requestRetained{writes: session.WriteSession, scope: session.Scope}
		mu.Unlock()
		if session.WriteSession == nil {
			return Response{}, errors.New("request write session missing")
		}
		switch r.URL.Path {
		case "/commit":
			// A direct Scope statement and two hooked statements share one transaction.
			if _, err := session.Scope.Exec(callback, "INSERT INTO "+table+" VALUES($1,'direct')", int64(10)); err != nil {
				return Response{}, err
			}
			for _, row := range []int64{1, 2} {
				if err := insert(callback, session, repo, row); err != nil {
					return Response{}, err
				}
			}
			return Response{Body: []byte("ok")}, nil
		case "/app-refusal":
			if _, err := session.Scope.Exec(callback, "INSERT INTO "+table+" VALUES($1,'direct')", int64(20)); err != nil {
				return Response{}, err
			}
			if err := insert(callback, session, repo, 21); err != nil {
				return Response{}, err
			}
			return Response{Body: []byte("success")}, &ApplicationError{Response: Response{Status: http.StatusConflict, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: bounded}}
		case "/ordinary-error":
			if err := insert(callback, session, repo, 30); err != nil {
				return Response{}, err
			}
			return Response{}, errors.New("native_secret_canary")
		case "/oversize-refusal":
			if err := insert(callback, session, repo, 40); err != nil {
				return Response{}, err
			}
			return Response{}, &ApplicationError{Response: Response{Status: http.StatusConflict, Body: []byte(strings.Repeat("x", 33))}}
		case "/success-status-refusal":
			if err := insert(callback, session, repo, 41); err != nil {
				return Response{}, err
			}
			return Response{}, &ApplicationError{Response: Response{Status: http.StatusOK, Body: []byte("lie")}}
		case "/swallowed-hook-failure":
			if err := insert(callback, session, failing, 50); !errors.Is(err, hookFailure) {
				return Response{}, fmt.Errorf("hook failure not reported: %w", err)
			}
			return Response{Body: []byte("pretend")}, nil
		case "/dispatch-failure":
			if err := insert(callback, session, dispatching, 60); err != nil {
				return Response{}, err
			}
			return Response{Body: []byte("committed")}, nil
		case "/panic":
			if err := insert(callback, session, repo, 70); err != nil {
				return Response{}, err
			}
			panic("native_request_panic")
		}
		return Response{}, errors.New("unknown route")
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	get := func(path string) (int, http.Header, string) {
		t.Helper()
		response, err := server.Client().Get(server.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		body, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatal(err)
		}
		return response.StatusCode, response.Header, string(body)
	}
	generic := "database request did not complete\n"

	// Commit: hooks and direct statements join one transaction; ordering holds.
	status, _, body := get("/commit")
	if status != http.StatusOK || body != "ok" {
		t.Fatal("commit response", status, body)
	}
	wantOrder := []string{"before:1 seen=1", "after-create:1", "before:2 seen=2", "after-create:2", "committed:1 rows=3", "committed:2 rows=3"}
	if got := takeOrder(); !reflect.DeepEqual(got, wantOrder) {
		t.Fatal("hook ordering within request transaction", got)
	}
	if got := requestIDs(t, ctx, admin, table); !reflect.DeepEqual(got, []int64{1, 2, 10}) {
		t.Fatal("native commit oracle", got)
	}
	mu.Lock()
	held, capturedHook := retained, hookContext
	mu.Unlock()
	// Use after the request finished is refused at every layer.
	if _, err := held.writes.Exec(ctx, "SELECT 1"); !errors.Is(err, orm.ErrHookClosed) {
		t.Fatal("retained write session usable", err)
	}
	if _, err := orm.HookInsert(ctx, held.writes, repo, orm.Set(id, orm.Some(int64(99))), orm.Set(value, orm.Some("late"))); !errors.Is(err, orm.ErrHookClosed) {
		t.Fatal("retained write session hooked statement", err)
	}
	if _, err := held.writes.Query(ctx, "SELECT 1"); !errors.Is(err, orm.ErrHookClosed) {
		t.Fatal("retained write session query", err)
	}
	if _, err := held.scope.Exec(ctx, "SELECT 1"); !errors.Is(err, orm.ErrScopeClosed) {
		t.Fatal("retained request scope usable", err)
	}
	if _, err := capturedHook.Exec(ctx, "SELECT 1"); !errors.Is(err, orm.ErrHookClosed) {
		t.Fatal("retained hook context usable", err)
	}
	requestPoolReusable(t, ctx, pool)

	// A 4xx application error rolls back both statement kinds and answers exactly
	// the bounded response; AfterCommit hooks never run.
	status, header, body = get("/app-refusal")
	if status != http.StatusConflict || body != string(bounded) || header.Get("Content-Type") != "application/json" {
		t.Fatal("application refusal response", status, header, body)
	}
	if got := takeOrder(); !reflect.DeepEqual(got, []string{"before:21 seen=4", "after-create:21"}) {
		t.Fatal("refusal hook sequence", got)
	}
	if got := requestIDs(t, ctx, admin, table); !reflect.DeepEqual(got, []int64{1, 2, 10}) {
		t.Fatal("4xx application error did not roll back", got)
	}
	requestPoolReusable(t, ctx, pool)

	// Ordinary errors remain the generic 503 and roll back.
	status, _, body = get("/ordinary-error")
	if status != http.StatusServiceUnavailable || body != generic || strings.Contains(body, "canary") {
		t.Fatal("ordinary error response", status, body)
	}
	takeOrder()

	// Invalid application responses fail closed to the generic 503 and roll back.
	for _, path := range []string{"/oversize-refusal", "/success-status-refusal"} {
		status, _, body = get(path)
		if status != http.StatusServiceUnavailable || body != generic {
			t.Fatal("invalid application response", path, status, body)
		}
	}
	takeOrder()

	// A hook failure swallowed by the application still rolls back; its own
	// success response is not delivered.
	status, _, body = get("/swallowed-hook-failure")
	if status != http.StatusServiceUnavailable || body != generic || strings.Contains(body, "canary") {
		t.Fatal("swallowed hook failure response", status, body)
	}
	for _, line := range takeOrder() {
		if line == "failing-repo-committed" {
			t.Fatal("hook ran after rollback")
		}
	}

	// Committed dispatch failure is distinct from rollback: row is durable and
	// the response reveals no internal text.
	status, _, body = get("/dispatch-failure")
	if status != http.StatusInternalServerError || body != "database request committed; follow-up processing failed\n" {
		t.Fatal("dispatch failure response", status, body)
	}
	if got := requestIDs(t, ctx, admin, table); !reflect.DeepEqual(got, []int64{1, 2, 10, 60}) {
		t.Fatal("rolled-back rows or missing committed row", got)
	}
	if _, err := admin.Exec(ctx, "DELETE FROM "+table+" WHERE id=60"); err != nil {
		t.Fatal(err)
	}
	requestPoolReusable(t, ctx, pool)

	// Application panics keep their behavior: roll back, release, re-panic.
	var caught any
	func() {
		defer func() { caught = recover() }()
		request := httptest.NewRequest(http.MethodGet, "/panic", nil)
		handler.ServeHTTP(httptest.NewRecorder(), request)
	}()
	if caught != "native_request_panic" {
		t.Fatal("panic value changed", caught)
	}
	takeOrder()
	if got := requestIDs(t, ctx, admin, table); !reflect.DeepEqual(got, []int64{1, 2, 10}) {
		t.Fatal("panic did not roll back", got)
	}
	requestPoolReusable(t, ctx, pool)
	shutdown, done := context.WithTimeout(context.Background(), 5*time.Second)
	defer done()
	if err := lifetime.Shutdown(shutdown); err != nil {
		t.Fatal("requests leaked admission", err)
	}
}
