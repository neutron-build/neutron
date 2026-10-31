package orm

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestPostgresBoundSQLProjectionCompletenessAndRollback(t *testing.T) {
	ctx, pool, admin, records := liveTransactionSetup(t)
	schema := strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(strings.TrimSuffix(records, ".records"), `"`), `"`), `""`, `"`)
	table, err := NewPostgresTable[cteGroupModel](ctx, admin, schema, "records")
	if err != nil {
		t.Fatal(err)
	}
	injected := `literal'; DROP TABLE records; --`
	if _, err := admin.Exec(ctx, "INSERT INTO "+records+" (id,value) VALUES (1,$1),(2,'two')", injected); err != nil {
		t.Fatal(err)
	}
	plan, err := NewBoundSQL(table, "SELECT id,value FROM "+records+" WHERE value=$1 ORDER BY id", injected)
	if err != nil {
		t.Fatal(err)
	}
	models, err := SelectBound(ctx, admin, plan, 2)
	if err != nil || !reflect.DeepEqual(models, []cteGroupModel{{1, injected}}) {
		t.Fatal("bound native SQL values interpolated", err)
	}
	for _, statement := range []string{
		"SELECT id FROM " + records,
		"SELECT value,id FROM " + records,
		"SELECT id,value AS omitted FROM " + records,
		"SELECT id::text AS id,value FROM " + records,
	} {
		bad, err := NewBoundSQL(table, statement)
		if err != nil {
			t.Fatal(err)
		}
		partial, err := SelectBound(ctx, admin, bad, 10)
		if partial != nil || !errors.Is(err, ErrProjectionShape) {
			t.Fatal("partial/incompatible shape decoded", statement, err)
		}
	}
	all, err := NewBoundSQL(table, "SELECT id,value FROM "+records+" ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	if partial, err := SelectBound(ctx, admin, all, 1); partial != nil || !errors.Is(err, ErrBoundBudget) {
		t.Fatal("bound SQL budget exposed partial result", err)
	}
	bad, err := NewBoundSQL(table, "SELECT id FROM "+records)
	if err != nil {
		t.Fatal(err)
	}
	err = WithTransaction(ctx, pool, TransactionOptions{}, func(scope *Scope) error {
		if _, err := scope.Exec(ctx, "UPDATE "+records+" SET value='must rollback' WHERE id=2"); err != nil {
			return err
		}
		_, err := SelectBound(ctx, scope, bad, 10)
		if !errors.Is(err, ErrProjectionShape) {
			return errors.New("projection failure missing")
		}
		return nil
	})
	if !errors.Is(err, ErrScopeDecode) || !errors.Is(err, ErrProjectionShape) {
		t.Fatal("swallowed projection rejection committed", err)
	}
	var native string
	if err := admin.QueryRow(ctx, "SELECT value FROM "+records+" WHERE id=2").Scan(&native); err != nil || native != "two" {
		t.Fatal("projection rejection rollback", err)
	}
	requirePoolReuse(t, ctx, pool)
}
