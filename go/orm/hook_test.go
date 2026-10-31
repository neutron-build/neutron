package orm

import (
	"context"
	"errors"
	"testing"
	"time"
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
	if _, err := HookDelete(context.Background(), session, repo, id.Eq(int64(1))); !errors.Is(err, ErrHookReentry) {
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
	if _, err := HookDelete(context.Background(), session, repo, id.Eq(int64(1))); err != nil {
		t.Fatal(err)
	}
	if _, err := retained.Exec(context.Background(), "SELECT 1"); !errors.Is(err, ErrHookClosed) {
		t.Fatal(err)
	}
	marker := errors.New("child rollback")
	if err := session.Savepoint(context.Background(), func(child *WriteSession) error {
		_, err := HookDelete(context.Background(), child, repo, id.Eq(int64(2)))
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
		_, err := HookDelete(context.Background(), child, repo, id.Eq(int64(3)))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if len(session.events) != 2 {
		t.Fatal("released child event not merged")
	}
}

// Request admission is reserved before native Scope.acquire: a callback cannot
// return in that gap and permit a retained goroutine to escape leak detection.
func TestHookAdmittedRequestLeakBeforeNativeAcquire(t *testing.T) {
	driver := &lifecycleDriver{}
	_, scope, _, _ := fixtureOwner(context.Background(), driver)
	session := newWriteSession(scope)
	table, _ := NewTable[hookTestModel]("public", "records")
	id, _ := NewColumn[hookTestModel, int64](table, "ID")
	var cleanup func()
	repo, _ := NewHookRepository(table, HookSet[hookTestModel]{BeforeDelete: []BeforeHook[hookTestModel]{func(ctx context.Context, h *HookContext, _ WriteIntent[hookTestModel]) error {
		_, finish, err := h.request(ctx)
		cleanup = finish
		return err
	}}})
	if _, err := HookDelete(context.Background(), session, repo, id.Eq(1)); !errors.Is(err, ErrHookLeak) {
		t.Fatal("request gap escaped leak refusal", err)
	}
	cleanup()
	cleanup()
	if driver.execs != 0 {
		t.Fatal("statement ran after request leak")
	}
}

func TestHookRowsDecodeFinalizesOnceWithCause(t *testing.T) {
	for _, values := range []bool{false, true} {
		failure := errors.New("hook decode failed")
		native := &decodeFailureRows{failure: failure}
		calls := 0
		rows := &hookRows{Rows: native, finish: func(err error) {
			calls++
			if !errors.Is(err, failure) || !native.closed {
				t.Fatal("decode cause or native cleanup lost", err)
			}
		}}
		if values {
			_, _ = rows.Values()
		} else {
			_ = rows.Scan(new(int64))
		}
		rows.Close()
		rows.Close()
		if calls != 1 {
			t.Fatal("hook capability finalized more than once", calls)
		}
	}
}

func scopedHookRepo(t *testing.T, hooks HookSet[hookTestModel]) (HookRepository[hookTestModel], Column[hookTestModel, int64]) {
	t.Helper()
	table, err := NewTable[hookTestModel]("public", "records")
	if err != nil {
		t.Fatal(err)
	}
	id, err := NewColumn[hookTestModel, int64](table, "ID")
	if err != nil {
		t.Fatal(err)
	}
	repo, err := NewHookRepository(table, hooks)
	if err != nil {
		t.Fatal(err)
	}
	return repo, id
}

func TestRunHookSessionCommitSharesScopeOrdersAndTerminalizes(t *testing.T) {
	driver := &lifecycleDriver{}
	owner, scope, _, _ := fixtureOwner(context.Background(), driver)
	var order []string
	mark := func(label string) BeforeHook[hookTestModel] {
		return func(context.Context, *HookContext, WriteIntent[hookTestModel]) error {
			order = append(order, label)
			return nil
		}
	}
	commit := func(label string) CommitHook[hookTestModel] {
		return func(context.Context, WriteEvent[hookTestModel]) error {
			order = append(order, label)
			return nil
		}
	}
	repo, id := scopedHookRepo(t, HookSet[hookTestModel]{BeforeWrite: []BeforeHook[hookTestModel]{mark("before")}, AfterCommit: []CommitHook[hookTestModel]{commit("a"), commit("b")}})
	var retained *WriteSession
	var events []queuedHookEvent
	err := runHookSession(scope, func(sc *Scope, session *WriteSession) error {
		retained = session
		if sc != scope || session.scope != scope {
			t.Fatal("session not bound to the supplied scope")
		}
		if _, err := sc.Exec(context.Background(), "SELECT 'direct'"); err != nil {
			return err
		}
		if _, err := HookDelete(context.Background(), session, repo, id.Eq(int64(1))); err != nil {
			return err
		}
		if _, err := HookDelete(context.Background(), session, repo, id.Eq(int64(2))); err != nil {
			return err
		}
		return nil
	}, &events)
	if err != nil || len(events) != 2 {
		t.Fatal(err, len(events))
	}
	if len(driver.statements) != 3 || driver.statements[0] != "SELECT 'direct'" {
		t.Fatal("direct and hooked statements did not share one scope in order", driver.statements)
	}
	if _, err := retained.Exec(context.Background(), "SELECT 1"); !errors.Is(err, ErrHookClosed) {
		t.Fatal("retained session usable after callback", err)
	}
	if err := owner.finishRoot(scope, err); err != nil || driver.commits != 1 || driver.rollbacks != 0 {
		t.Fatal(err, driver)
	}
	if err := dispatchHookEvents(context.Background(), time.Second, events); err != nil {
		t.Fatal(err)
	}
	want := []string{"before", "before", "a", "b", "a", "b"}
	if len(order) != len(want) {
		t.Fatal(order)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatal("hook ordering", order)
		}
	}
}

func TestRunHookSessionRollbackPaths(t *testing.T) {
	marker := errors.New("application refusal")
	plain, id := scopedHookRepo(t, HookSet[hookTestModel]{})
	repo, err := NewHookRepository(plain.table, HookSet[hookTestModel]{BeforeDelete: []BeforeHook[hookTestModel]{func(context.Context, *HookContext, WriteIntent[hookTestModel]) error { return marker }}})
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name     string
		callback func(*Scope, *WriteSession) error
		check    func(error) bool
	}{
		{"callback error", func(_ *Scope, s *WriteSession) error {
			if _, err := HookDelete(context.Background(), s, plain, id.Eq(int64(1))); err != nil {
				return err
			}
			return marker
		}, func(err error) bool { return errors.Is(err, marker) }},
		{"swallowed hook failure", func(_ *Scope, s *WriteSession) error {
			if _, err := HookDelete(context.Background(), s, repo, id.Eq(int64(1))); !errors.Is(err, marker) {
				t.Fatal(err)
			}
			return nil
		}, func(err error) bool { return errors.Is(err, ErrHookWorkflow) && errors.Is(err, marker) }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			driver := &lifecycleDriver{}
			owner, scope, _, _ := fixtureOwner(context.Background(), driver)
			var events []queuedHookEvent
			err := runHookSession(scope, c.callback, &events)
			if !c.check(err) {
				t.Fatal(err)
			}
			if err := owner.finishRoot(scope, err); err == nil || driver.commits != 0 || driver.rollbacks != 1 {
				t.Fatal(err, driver)
			}
		})
	}
}

func TestRunHookSessionPanicTerminalizesSession(t *testing.T) {
	driver := &lifecycleDriver{}
	_, scope, _, _ := fixtureOwner(context.Background(), driver)
	var retained *WriteSession
	var events []queuedHookEvent
	var caught any
	func() {
		defer func() { caught = recover() }()
		_ = runHookSession(scope, func(_ *Scope, s *WriteSession) error {
			retained = s
			panic("callback panic")
		}, &events)
	}()
	if caught != "callback panic" || len(events) != 0 {
		t.Fatal(caught, len(events))
	}
	if _, err := retained.Exec(context.Background(), "SELECT 1"); !errors.Is(err, ErrHookClosed) {
		t.Fatal("session usable after panic", err)
	}
}

func TestRunHookSessionActiveRowsAreALeak(t *testing.T) {
	driver := &lifecycleDriver{}
	_, scope, _, _ := fixtureOwner(context.Background(), driver)
	var events []queuedHookEvent
	err := runHookSession(scope, func(_ *Scope, s *WriteSession) error {
		_, err := s.Query(context.Background(), "SELECT 1")
		return err
	}, &events)
	if !errors.Is(err, ErrHookLeak) || !errors.Is(err, ErrHookWorkflow) {
		t.Fatal("retained rows not refused", err)
	}
}

func TestDispatchHookEventsStopsAtFirstFailure(t *testing.T) {
	marker := errors.New("dispatch failure")
	var order []string
	hook := func(label string, err error) func(context.Context) error {
		return func(context.Context) error { order = append(order, label); return err }
	}
	events := []queuedHookEvent{{callbacks: []func(context.Context) error{hook("e0h0", nil), hook("e0h1", marker)}}, {callbacks: []func(context.Context) error{hook("e1h0", nil)}}}
	err := dispatchHookEvents(context.Background(), time.Second, events)
	var committed *CommittedDispatchError
	if !errors.As(err, &committed) || committed.EventIndex != 0 || committed.HookIndex != 1 || !errors.Is(err, marker) || len(order) != 2 {
		t.Fatal(err, order)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	order = nil
	err = dispatchHookEvents(canceled, time.Second, events)
	if !errors.As(err, &committed) || !errors.Is(err, context.Canceled) || len(order) != 0 {
		t.Fatal("canceled dispatch ran hooks", err, order)
	}
	var caught any
	func() {
		defer func() { caught = recover() }()
		_ = dispatchHookEvents(context.Background(), time.Second, []queuedHookEvent{{callbacks: []func(context.Context) error{func(context.Context) error { panic("postcommit") }}}})
	}()
	if panicValue, ok := caught.(*CommittedHookPanic); !ok || !panicValue.Committed() {
		t.Fatal("committed hook panic lost", caught)
	}
}
