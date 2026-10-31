package orm

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type observeFixture struct {
	rows pgx.Rows
	err  error
}

func (f observeFixture) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return f.rows, f.err
}
func (f observeFixture) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.NewCommandTag("UPDATE 2"), f.err
}
func TestQueryObserverRedactionPanicIsolationAndExactlyOnce(t *testing.T) {
	ctx := context.WithValue(context.Background(), observeContextKey{}, "request-a")
	var events []QueryEvent
	var metrics QueryMetrics
	observe := func(actual context.Context, event QueryEvent) {
		if actual != ctx {
			t.Fatal("context changed")
		}
		events = append(events, event)
		metrics.Observe(actual, event)
	}
	db := ObserveExecutor(observeFixture{rows: &fixtureRows{}}, observe)
	rows, err := db.Query(ctx, "SELECT 'SQL_literal_secret'", "bound_secret")
	if err != nil {
		t.Fatal(err)
	}
	if rows.Next() {
		t.Fatal("unexpected row")
	}
	rows.Close()
	rows.Close()
	if _, err := db.Exec(ctx, "UPDATE secret", "password_secret"); err != nil {
		t.Fatal(err)
	}
	totals := metrics.Snapshot()
	if len(events) != 2 || totals.Calls != 2 || totals.Queries != 1 || totals.Execs != 1 || totals.Rows != 2 || totals.Errors != 0 {
		t.Fatal("physical counts", totals)
	}
	native := &pgconn.PgError{Code: "22P02", Message: "bound_secret", Detail: "password_secret"}
	db = ObserveExecutor(observeFixture{err: native}, observe)
	if _, err := db.Query(ctx, "secret"); !errors.Is(err, native) {
		t.Fatal("native cause changed", err)
	}
	last := events[len(events)-1]
	if last.ErrorClass != "postgres" || last.SQLState != "22P02" {
		t.Fatal("bounded SQLSTATE", last)
	}
	db = ObserveExecutor(observeFixture{}, func(context.Context, QueryEvent) { panic("observer secret") })
	if _, err := db.Exec(ctx, "UPDATE harmless"); err != nil {
		t.Fatal("observer panic changed result", err)
	}
	class, state := queryError(&pgconn.PgError{Code: "secret"}, false)
	if class != "postgres" || state != "" {
		t.Fatal("invalid native SQLSTATE leaked")
	}
}

type observeContextKey struct{}
