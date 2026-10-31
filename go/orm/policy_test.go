package orm

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

type policyModel struct {
	Tenant  string     `db:"tenant"`
	ID      int64      `db:"id"`
	Value   string     `db:"value"`
	Version int64      `db:"version"`
	Deleted *time.Time `db:"deleted,nullable"`
}

func policyMetadata(t *testing.T, schema string) (Table[policyModel], Column[policyModel, string], Column[policyModel, int64], Column[policyModel, string], Column[policyModel, int64], Column[policyModel, *time.Time]) {
	t.Helper()
	table, err := NewTable[policyModel](schema, "policy_records")
	if err != nil {
		t.Fatal(err)
	}
	tenant, _ := NewColumn[policyModel, string](table, "Tenant")
	id, _ := NewColumn[policyModel, int64](table, "ID")
	value, _ := NewColumn[policyModel, string](table, "Value")
	version, _ := NewColumn[policyModel, int64](table, "Version")
	deleted, _ := NewColumn[policyModel, *time.Time](table, "Deleted")
	return table, tenant, id, value, version, deleted
}
func TestImmutableScopedSoftDeletePolicy(t *testing.T) {
	table, tenant, id, _, _, deleted := policyMetadata(t, "owned")
	scoped, err := NewScopedTable(table, tenant.Eq("a"))
	if err != nil {
		t.Fatal(err)
	}
	policy, err := NewSoftDelete(scoped, deleted)
	if err != nil {
		t.Fatal(err)
	}
	query := Query[policyModel]{}.Where(id.Eq(1))
	active, err := CompileSelect(table, policy.Active(query))
	if err != nil {
		t.Fatal(err)
	}
	admin, err := CompileSelect(table, policy.IncludingDeleted(query))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(active.SQL, `"deleted" IS NULL`) || strings.Contains(admin.SQL, `"deleted" IS NULL`) || !reflect.DeepEqual(active.Args, []any{"a", int64(1)}) || !reflect.DeepEqual(admin.Args, active.Args) {
		t.Fatal(active, admin)
	}
	original, err := CompileSelect(table, query)
	if err != nil || !reflect.DeepEqual(original.Args, []any{int64(1)}) {
		t.Fatal(original, err)
	}
	db := &associationRefusingExecutor{}
	if _, err := policy.Remove(context.Background(), db, Predicate[policyModel]{}, time.Now()); err == nil || db.called {
		t.Fatal("scope disguised missing user predicate", err)
	}
	_, _, _, _, _, foreign := policyMetadata(t, "owned")
	if _, err := NewSoftDelete(scoped, foreign); err == nil {
		t.Fatal("foreign deletion column accepted")
	}
}

type guardDriver struct {
	lifecycleDriver
	count string
	args  []any
}

func (d *guardDriver) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	d.execs++
	d.statements = append(d.statements, sql)
	d.args = append([]any(nil), args...)
	return pgconn.NewCommandTag("UPDATE " + d.count), nil
}
func TestVersionGuardPoisonAndValidation(t *testing.T) {
	table, tenant, id, value, version, _ := policyMetadata(t, "owned")
	for _, count := range []string{"0", "1", "2"} {
		d := &guardDriver{count: count}
		owner, scope, _, _ := fixtureOwner(context.Background(), d)
		err := VersionedUpdate(context.Background(), scope, table, And(tenant.Eq("a"), id.Eq(1)), version, 2, Set(value, Some("")))
		if !reflect.DeepEqual(d.args, []any{"", "a", int64(1), int64(2)}) || !strings.Contains(d.statements[0], `"version" = "version" + 1`) {
			t.Fatal(d)
		}
		if count == "1" {
			if err != nil {
				t.Fatal(err)
			}
		} else {
			want := ErrVersionConflict
			if count == "2" {
				want = ErrCardinality
			}
			if !errors.Is(err, want) {
				t.Fatal(err)
			}
			if _, err := scope.Exec(context.Background(), "SELECT 1"); !errors.Is(err, ErrScopeMutation) {
				t.Fatal("failed mutation scope reused", err)
			}
		}
		finish := owner.finishRoot(scope, nil)
		if count == "1" {
			if finish != nil || d.commits != 1 {
				t.Fatal(finish, d)
			}
		} else {
			if !errors.Is(finish, ErrScopeMutation) || d.commits != 0 || d.rollbacks != 1 {
				t.Fatal(finish, d)
			}
		}
	}
	d := &guardDriver{count: "1"}
	_, scope, _, _ := fixtureOwner(context.Background(), d)
	if err := VersionedUpdate(context.Background(), scope, table, Predicate[policyModel]{}, version, 0, Set(value, Some(""))); err == nil {
		t.Fatal("missing predicate accepted")
	}
	if err := VersionedUpdate(context.Background(), scope, table, id.Eq(1), version, 0, Set(version, Some(int64(1)))); err == nil {
		t.Fatal("version assignment accepted")
	}
	if d.execs != 0 {
		t.Fatal("invalid guard reached native SQL")
	}
}
