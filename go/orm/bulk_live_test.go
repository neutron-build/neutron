package orm

import (
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

func TestPostgresOwnedBulkUpsertAndCopy(t *testing.T) {
	ctx, pool, admin, records := liveTransactionSetup(t)
	schema := strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(strings.TrimSuffix(records, ".records"), `"`), `"`), `""`, `"`)
	table, tenant, id, value, _, _ := policyMetadata(t, schema)
	if _, err := admin.Exec(ctx, "CREATE TABLE "+table.info.sqlName()+" (tenant text NOT NULL,id bigint NOT NULL,value text NOT NULL,version bigint NOT NULL DEFAULT 0,deleted timestamptz,PRIMARY KEY(tenant,id))"); err != nil {
		t.Fatal(err)
	}
	row := func(t string, n int64, v string) []Assignment[policyModel] {
		return []Assignment[policyModel]{Set(tenant, Some(t)), Set(id, Some(n)), Set(value, Some(v))}
	}
	key := []BoundColumn[policyModel]{BindColumn(tenant), BindColumn(id)}
	err := WithTransaction(ctx, pool, TransactionOptions{}, func(scope *Scope) error {
		models, err := InsertBatch(ctx, scope, table, [][]Assignment[policyModel]{row("a", 1, "first"), row("b", 1, "foreign")}, 2)
		if err != nil || len(models) != 2 {
			t.Fatal(models, err)
		}
		updated, err := UpsertOne(ctx, scope, table, key, row("a", 1, ""), []BoundColumn[policyModel]{BindColumn(value)})
		if err != nil || !updated.Valid || updated.Value.Value != "" {
			t.Fatal(updated, err)
		}
		ignored, err := UpsertOne(ctx, scope, table, key, row("b", 1, "must not overwrite"), nil)
		if err != nil || ignored.Valid {
			t.Fatal(ignored, err)
		}
		inserted, err := UpsertOne(ctx, scope, table, key, row("a", 2, "inserted"), []BoundColumn[policyModel]{BindColumn(value)})
		if err != nil || !inserted.Valid || inserted.Value.ID != 2 {
			t.Fatal(inserted, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	assertRows := func(want int) {
		t.Helper()
		var count int
		if err := admin.QueryRow(ctx, "SELECT count(*) FROM "+table.info.sqlName()).Scan(&count); err != nil || count != want {
			t.Fatal(count, want, err)
		}
	}
	assertRows(3)
	var foreign string
	if err := admin.QueryRow(ctx, "SELECT value FROM "+table.info.sqlName()+" WHERE tenant='b' AND id=1").Scan(&foreign); err != nil || foreign != "foreign" {
		t.Fatal(foreign, err)
	}
	err = WithTransaction(ctx, pool, TransactionOptions{}, func(scope *Scope) error {
		_, err := InsertBatch(ctx, scope, table, [][]Assignment[policyModel]{row("a", 3, "must rollback"), row("a", 1, "duplicate")}, 2)
		if err == nil {
			t.Fatal("final batch row conflict accepted")
		}
		_, err = scope.Exec(ctx, "SELECT 1")
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	assertRows(3)
	columns := []BoundColumn[policyModel]{BindColumn(tenant), BindColumn(id), BindColumn(value)}
	err = WithTransaction(ctx, pool, TransactionOptions{}, func(scope *Scope) error {
		count, err := CopyInto(ctx, scope, table, columns, 2, pgx.CopyFromRows([][]any{{"a", int64(4), "copy"}, {"a", int64(5), "copy"}}))
		if err != nil || count != 2 {
			t.Fatal(count, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	assertRows(5)
	for _, test := range []struct {
		rows     [][]any
		budget   int
		expected error
	}{
		{[][]any{{"a", int64(6), "over"}, {"a", int64(7), "over"}}, 1, ErrCopyBudget},
		{[][]any{{"a", int64(6), "valid"}, {"a", "wrong type", "invalid"}}, 2, nil},
		{[][]any{{"a", int64(6), "valid"}, {"a", int64(1), "duplicate"}}, 2, nil},
	} {
		err = WithTransaction(ctx, pool, TransactionOptions{}, func(scope *Scope) error {
			count, err := CopyInto(ctx, scope, table, columns, test.budget, pgx.CopyFromRows(test.rows))
			if err == nil || count != 0 || (test.expected != nil && !errors.Is(err, test.expected)) {
				t.Fatal("failed COPY retained partial result", count, err)
			}
			_, err = scope.Exec(ctx, "SELECT 1")
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
		assertRows(5)
		requirePoolReuse(t, ctx, pool)
	}
}
