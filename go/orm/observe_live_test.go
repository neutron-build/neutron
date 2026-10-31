package orm

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestPostgresQueryObserverRedactionResultRejectionAndCommitIsolation(t *testing.T) {
	ctx, pool, admin, records := liveTransactionSetup(t)
	schema := strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(strings.TrimSuffix(records, ".records"), `"`), `"`), `""`, `"`)
	table, err := NewPostgresTable[cteGroupModel](ctx, admin, schema, "records")
	if err != nil {
		t.Fatal(err)
	}
	var events []QueryEvent
	var metrics QueryMetrics
	observer := func(_ context.Context, event QueryEvent) { events = append(events, event); metrics.Observe(ctx, event) }
	// Both a bound input and native errors can contain secrets. No error cause
	// or SQL literal ever enters the event, including failed native conversion.
	db := ObserveExecutor(admin, observer)
	if _, err := db.Exec(ctx, "INSERT INTO "+records+" VALUES (1,$1)", "bound_secret_canary"); err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(ctx, "SELECT $1::integer,'literal_secret_canary'", "native_secret_canary")
	if err == nil {
		rows.Close()
		err = rows.Err()
	}
	if err == nil {
		t.Fatal("native conversion did not fail")
	}
	payload, err := json.Marshal(events)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"bound_secret_canary", "literal_secret_canary", "native_secret_canary"} {
		if strings.Contains(string(payload), secret) {
			t.Fatal("event leaked secret")
		}
	}
	if events[len(events)-1].SQLState != "22P02" {
		t.Fatal("native SQLSTATE event")
	}
	// Observed rows must forward internal shape rejection into Scope. A swallowed
	// ORM rejection therefore still rolls back writes made earlier in the scope.
	bad, err := NewBoundSQL(table, "SELECT id FROM "+records)
	if err != nil {
		t.Fatal(err)
	}
	err = WithTransaction(ctx, pool, TransactionOptions{}, func(scope *Scope) error {
		db := ObserveExecutor(scope, observer)
		if _, err := db.Exec(ctx, "UPDATE "+records+" SET value='rollback' WHERE id=1"); err != nil {
			return err
		}
		if _, err := SelectBound(ctx, db, bad, 10); !errors.Is(err, ErrProjectionShape) {
			return errors.New("expected projection refusal")
		}
		return nil
	})
	if !errors.Is(err, ErrScopeDecode) || !errors.Is(err, ErrProjectionShape) {
		t.Fatal("observation hid rollback requirement", err)
	}
	var native string
	if err := admin.QueryRow(ctx, "SELECT value FROM "+records+" WHERE id=1").Scan(&native); err != nil || native != "bound_secret_canary" {
		t.Fatal("native rollback oracle", err)
	}
	err = WithTransaction(ctx, pool, TransactionOptions{}, func(scope *Scope) error {
		db := ObserveExecutor(scope, func(context.Context, QueryEvent) { panic("logger failure") })
		_, err := db.Exec(ctx, "UPDATE "+records+" SET value='committed' WHERE id=1")
		return err
	})
	if err != nil {
		t.Fatal("observer changed commit", err)
	}
	if err := admin.QueryRow(ctx, "SELECT value FROM "+records+" WHERE id=1").Scan(&native); err != nil || native != "committed" {
		t.Fatal("native observer-isolated commit oracle", err)
	}
	totals := metrics.Snapshot()
	if totals.Calls != 4 || totals.Queries != 2 || totals.Execs != 2 || totals.Errors != 2 {
		t.Fatal("native physical-call count", totals)
	}
	requirePoolReuse(t, ctx, pool)
}
