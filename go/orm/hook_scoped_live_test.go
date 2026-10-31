package orm

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestPostgresScopedHookTransaction(t *testing.T) {
	ctx, pool, admin, records := liveTransactionSetup(t)
	schema := strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(strings.TrimSuffix(records, ".records"), `"`), `"`), `""`, `"`)
	table, err := NewTable[hookLiveModel](schema, "records")
	if err != nil {
		t.Fatal(err)
	}
	id, _ := NewColumn[hookLiveModel, int64](table, "ID")
	value, _ := NewColumn[hookLiveModel, string](table, "Value")
	var order []string
	repo, err := NewHookRepository(table, HookSet[hookLiveModel]{
		BeforeWrite: []BeforeHook[hookLiveModel]{func(context.Context, *HookContext, WriteIntent[hookLiveModel]) error {
			order = append(order, "before")
			return nil
		}},
		AfterCreate: []AfterHook[hookLiveModel]{func(context.Context, *HookContext, WriteEvent[hookLiveModel]) error {
			order = append(order, "after")
			return nil
		}},
		AfterCommit: []CommitHook[hookLiveModel]{func(dispatch context.Context, event WriteEvent[hookLiveModel]) error {
			// COMMIT was acknowledged and the connection released: the size-one pool serves this read.
			var count int
			if err := pool.QueryRow(dispatch, "SELECT count(*) FROM "+records).Scan(&count); err != nil {
				return err
			}
			order = append(order, "committed")
			if count != 3 {
				t.Error("rows not durable at dispatch", count)
			}
			return nil
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var retainedScope *Scope
	var retainedSession *WriteSession
	err = WithScopedHookTransaction(ctx, pool, HookTransactionOptions{}, func(scope *Scope, session *WriteSession) error {
		retainedScope, retainedSession = scope, session
		if _, err := scope.Exec(ctx, "INSERT INTO "+records+" VALUES (1,'direct')"); err != nil {
			return err
		}
		for _, row := range []int64{2, 3} {
			if _, err := HookInsert(ctx, session, repo, Set(id, Some(row)), Set(value, Some("hooked"))); err != nil {
				return err
			}
		}
		// The hooked statements and the direct one are the same transaction.
		count, err := scopeSingleInt(ctx, scope, "SELECT count(*) FROM "+records)
		if err != nil || count != 3 {
			return errors.Join(errors.New("hooked and direct writes not in one transaction"), err)
		}
		if got, queryErr := nativeIDs(ctx, admin, records); queryErr != nil || len(got) != 0 {
			return errors.Join(errors.New("uncommitted rows visible to independent connection"), queryErr)
		}
		return nil
	})
	if err != nil || !reflect.DeepEqual(order, []string{"before", "after", "before", "after", "committed", "committed"}) {
		t.Fatal(err, order)
	}
	if got, err := nativeIDs(ctx, admin, records); err != nil || !reflect.DeepEqual(got, []int64{1, 2, 3}) {
		t.Fatal("native commit oracle", got, err)
	}
	if _, err := retainedSession.Exec(ctx, "SELECT 1"); !errors.Is(err, ErrHookClosed) {
		t.Fatal("retained session usable", err)
	}
	if _, err := retainedScope.Exec(ctx, "SELECT 1"); !errors.Is(err, ErrScopeClosed) {
		t.Fatal("retained scope usable", err)
	}
	requirePoolReuse(t, ctx, pool)

	// Callback error rolls back both statement kinds; hooks never dispatch.
	order = nil
	marker := errors.New("application failure")
	err = WithScopedHookTransaction(ctx, pool, HookTransactionOptions{}, func(scope *Scope, session *WriteSession) error {
		if _, err := scope.Exec(ctx, "INSERT INTO "+records+" VALUES (11,'direct')"); err != nil {
			return err
		}
		if _, err := HookInsert(ctx, session, repo, Set(id, Some(int64(12))), Set(value, Some("hooked"))); err != nil {
			return err
		}
		return marker
	})
	var committed *CommittedDispatchError
	if !errors.Is(err, marker) || errors.As(err, &committed) || !reflect.DeepEqual(order, []string{"before", "after"}) {
		t.Fatal(err, order)
	}
	if got, err := nativeIDs(ctx, admin, records); err != nil || !reflect.DeepEqual(got, []int64{1, 2, 3}) {
		t.Fatal("native rollback oracle", got, err)
	}
	requirePoolReuse(t, ctx, pool)

	// A swallowed hook failure still rolls back.
	hookFailure := errors.New("hook failure")
	failing, _ := NewHookRepository(table, HookSet[hookLiveModel]{AfterCreate: []AfterHook[hookLiveModel]{func(context.Context, *HookContext, WriteEvent[hookLiveModel]) error { return hookFailure }}})
	err = WithScopedHookTransaction(ctx, pool, HookTransactionOptions{}, func(_ *Scope, session *WriteSession) error {
		_, _ = HookInsert(ctx, session, failing, Set(id, Some(int64(21))), Set(value, Some("poisoned")))
		return nil
	})
	if !errors.Is(err, hookFailure) || !errors.Is(err, ErrHookWorkflow) {
		t.Fatal(err)
	}
	if got, err := nativeIDs(ctx, admin, records); err != nil || !reflect.DeepEqual(got, []int64{1, 2, 3}) {
		t.Fatal("swallowed failure committed", got, err)
	}

	// Panic rolls back and re-raises the original value.
	var caught any
	func() {
		defer func() { caught = recover() }()
		_ = WithScopedHookTransaction(ctx, pool, HookTransactionOptions{}, func(scope *Scope, session *WriteSession) error {
			if _, err := HookInsert(ctx, session, repo, Set(id, Some(int64(31))), Set(value, Some("panic"))); err != nil {
				return err
			}
			panic("scoped panic")
		})
	}()
	if caught != "scoped panic" {
		t.Fatal("panic changed", caught)
	}
	if got, err := nativeIDs(ctx, admin, records); err != nil || !reflect.DeepEqual(got, []int64{1, 2, 3}) {
		t.Fatal("panic committed", got, err)
	}
	requirePoolReuse(t, ctx, pool)
}
