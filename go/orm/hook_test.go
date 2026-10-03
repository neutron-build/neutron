package orm

import (
	"context"
	"errors"
	"testing"
)

type hookTestModel struct {
	ID       int64   `db:"id"`
	Value    string  `db:"value"`
	Nullable *string `db:"nullable,nullable"`
}

func TestHookRegistrationAndIntentSnapshots(t *testing.T) {
	table, err := NewTable[hookTestModel]("public", "records")
	if err != nil {
		t.Fatal(err)
	}
	column, err := NewColumn[hookTestModel, *string](table, "Nullable")
	if err != nil {
		t.Fatal(err)
	}
	original := "before"
	assignment := Set(column, Some(&original))
	original = "changed"
	intent := snapshotHookIntent(HookCreate, table, Predicate[hookTestModel]{}, []Assignment[hookTestModel]{assignment})
	first, err := InspectHookValue(intent, column)
	if err != nil || first.Mode != HookSupplied || *first.Value != "before" {
		t.Fatal(first, err)
	}
	*first.Value = "hook mutation"
	second, _ := InspectHookValue(intent, column)
	if *second.Value != "before" {
		t.Fatal("intent pointer escaped")
	}
	before := []BeforeHook[hookTestModel]{func(context.Context, *HookContext, WriteIntent[hookTestModel]) error { return nil }}
	repo, err := NewHookRepository(table, HookSet[hookTestModel]{BeforeWrite: before})
	if err != nil {
		t.Fatal(err)
	}
	before[0] = nil
	if repo.hooks.BeforeWrite[0] == nil {
		t.Fatal("registration slice escaped")
	}
	if _, err := NewHookRepository(table, HookSet[hookTestModel]{AfterCommit: []CommitHook[hookTestModel]{nil}}); err == nil {
		t.Fatal("nil hook admitted")
	}
}
func TestHookSwallowedReentryRollsBack(t *testing.T) {
	driver := &lifecycleDriver{}
	owner, scope, _, _ := fixtureOwner(context.Background(), driver)
	session := newWriteSession(scope)
	table, _ := NewTable[hookTestModel]("public", "records")
	id, _ := NewColumn[hookTestModel, int64](table, "ID")
	repo, _ := NewHookRepository(table, HookSet[hookTestModel]{BeforeDelete: []BeforeHook[hookTestModel]{func(ctx context.Context, h *HookContext, _ WriteIntent[hookTestModel]) error {
		_, err := session.Exec(ctx, "SELECT 1")
		if !errors.Is(err, ErrHookReentry) {
			t.Fatal(err)
		}
		return nil
	}}})
	if _, err := HookDelete(context.Background(), session, repo, Eq(id, int64(1))); !errors.Is(err, ErrHookReentry) {
		t.Fatal(err)
	}
	if driver.execs != 0 {
		t.Fatal("SQL ran after swallowed reentry")
	}
	if err := owner.finishRoot(scope, session.closeCallback()); err == nil || driver.commits != 0 || driver.rollbacks != 1 {
		t.Fatal(err, driver)
	}
}
func TestHookCapturedContextAndChildEvents(t *testing.T) {
	driver := &lifecycleDriver{}
	_, scope, _, _ := fixtureOwner(context.Background(), driver)
	session := newWriteSession(scope)
	table, _ := NewTable[hookTestModel]("public", "records")
	id, _ := NewColumn[hookTestModel, int64](table, "ID")
	var retained *HookContext
	repo, _ := NewHookRepository(table, HookSet[hookTestModel]{BeforeDelete: []BeforeHook[hookTestModel]{func(_ context.Context, h *HookContext, _ WriteIntent[hookTestModel]) error { retained = h; return nil }}})
	if _, err := HookDelete(context.Background(), session, repo, Eq(id, int64(1))); err != nil {
		t.Fatal(err)
	}
	if _, err := retained.Exec(context.Background(), "SELECT 1"); !errors.Is(err, ErrHookClosed) {
		t.Fatal(err)
	}
	marker := errors.New("child rollback")
	if err := session.Savepoint(context.Background(), func(child *WriteSession) error {
		_, err := HookDelete(context.Background(), child, repo, Eq(id, int64(2)))
		if err != nil {
			return err
		}
		return marker
	}); !errors.Is(err, marker) {
		t.Fatal(err)
	}
	if len(session.events) != 1 {
		t.Fatal("rolled-back child event retained")
	}
	if err := session.Savepoint(context.Background(), func(child *WriteSession) error {
		_, err := HookDelete(context.Background(), child, repo, Eq(id, int64(3)))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if len(session.events) != 2 {
		t.Fatal("released child event not merged")
	}
}
