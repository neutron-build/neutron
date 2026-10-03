package orm

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestPostgresScopedSoftDeleteAndVersionGuards(t *testing.T) {
	ctx, pool, admin, records := liveTransactionSetup(t)
	schema := strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(strings.TrimSuffix(records, ".records"), `"`), `"`), `""`, `"`)
	table, tenant, id, value, version, deleted := policyMetadata(t, schema)
	if _, err := admin.Exec(ctx, "CREATE TABLE "+table.info.sqlName()+" (tenant text NOT NULL,id bigint NOT NULL,value text NOT NULL,version bigint NOT NULL,deleted timestamptz,PRIMARY KEY(tenant,id)); INSERT INTO "+table.info.sqlName()+" VALUES ('a',1,'first',0,NULL),('b',1,'foreign',0,NULL),('a',2,'second',0,NULL)"); err != nil {
		t.Fatal(err)
	}
	scoped, _ := NewScopedTable(table, tenant.Eq("a"))
	policy, _ := NewSoftDelete(scoped, deleted)
	at := time.Date(2026, 10, 3, 12, 0, 0, 123456000, time.UTC)
	if count, err := policy.Remove(ctx, pool, id.Eq(1), at); err != nil || count != 1 {
		t.Fatal(count, err)
	}
	active, err := policy.Select(ctx, pool, Query[policyModel]{}.OrderBy(id.Asc()))
	if err != nil || len(active) != 1 || active[0].ID != 2 {
		t.Fatal(active, err)
	}
	var nativeDeleted *time.Time
	var nativeValue string
	if err := admin.QueryRow(ctx, "SELECT deleted,value FROM "+table.info.sqlName()+" WHERE tenant='b' AND id=1").Scan(&nativeDeleted, &nativeValue); err != nil || nativeDeleted != nil || nativeValue != "foreign" {
		t.Fatal(nativeDeleted, nativeValue, err)
	}
	all, err := Select(ctx, pool, table, policy.IncludingDeleted(Query[policyModel]{}))
	if err != nil || len(all) != 2 {
		t.Fatal(all, err)
	}
	if count, err := policy.Update(ctx, pool, id.Eq(1), Set(value, Some("hidden"))); err != nil || count != 0 {
		t.Fatal(count, err)
	}
	if count, err := policy.Restore(ctx, pool, id.Eq(1)); err != nil || count != 1 {
		t.Fatal(count, err)
	}
	active, err = policy.Select(ctx, pool, Query[policyModel]{})
	if err != nil || len(active) != 2 {
		t.Fatal(active, err)
	}
	err = WithTransaction(ctx, pool, TransactionOptions{}, func(scope *Scope) error {
		return VersionedUpdate(ctx, scope, table, And(tenant.Eq("a"), id.Eq(1)), version, 0, Set(value, Some("")))
	})
	if err != nil {
		t.Fatal(err)
	}
	var v int64
	if err := admin.QueryRow(ctx, "SELECT value,version FROM "+table.info.sqlName()+" WHERE tenant='a' AND id=1").Scan(&nativeValue, &v); err != nil || nativeValue != "" || v != 1 {
		t.Fatal(nativeValue, v, err)
	}
	err = WithTransaction(ctx, pool, TransactionOptions{}, func(scope *Scope) error {
		if _, err := scope.Exec(ctx, "UPDATE "+table.info.sqlName()+" SET value='must rollback' WHERE tenant='a' AND id=2"); err != nil {
			return err
		}
		_ = VersionedUpdate(ctx, scope, table, And(tenant.Eq("a"), id.Eq(1)), version, 0, Set(value, Some("stale")))
		return nil
	})
	if !errors.Is(err, ErrVersionConflict) || !errors.Is(err, ErrScopeMutation) {
		t.Fatal(err)
	}
	if err := admin.QueryRow(ctx, "SELECT value FROM "+table.info.sqlName()+" WHERE tenant='a' AND id=2").Scan(&nativeValue); err != nil || nativeValue != "second" {
		t.Fatal("stale guard committed prior write", nativeValue, err)
	}
	// A non-unique explicit predicate must not commit a multi-row guarded change.
	err = WithTransaction(ctx, pool, TransactionOptions{}, func(scope *Scope) error {
		_ = VersionedUpdate(ctx, scope, table, id.Eq(1), version, 1, Set(value, Some("multi")))
		return nil
	})
	// At present only tenant a matches version 1. Make both match then check rollback.
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "UPDATE "+table.info.sqlName()+" SET version=4 WHERE id=1"); err != nil {
		t.Fatal(err)
	}
	err = WithTransaction(ctx, pool, TransactionOptions{}, func(scope *Scope) error {
		_ = VersionedUpdate(ctx, scope, table, id.Eq(1), version, 4, Set(value, Some("must rollback both")))
		return nil
	})
	if !errors.Is(err, ErrCardinality) || !errors.Is(err, ErrScopeMutation) {
		t.Fatal(err)
	}
	var changed int
	if err := admin.QueryRow(ctx, "SELECT count(*) FROM "+table.info.sqlName()+" WHERE value='must rollback both' OR (id=1 AND version<>4)").Scan(&changed); err != nil || changed != 0 {
		t.Fatal(changed, err)
	}
	requirePoolReuse(t, ctx, pool)
}
