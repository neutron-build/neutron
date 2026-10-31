package orm

import (
	"context"
	"errors"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"reflect"
	"testing"
)

type hookLiveModel struct {
	ID    int64  `db:"id"`
	Value string `db:"value"`
}

func TestPostgresOwnedHookWorkflows(t *testing.T) {
	ctx, pool, admin, records := liveTransactionSetup(t)
	schema := strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(strings.TrimSuffix(records, ".records"), `"`), `"`), `""`, `"`)
	table, err := NewTable[hookLiveModel](schema, "records")
	if err != nil {
		t.Fatal(err)
	}
	id, _ := NewColumn[hookLiveModel, int64](table, "ID")
	value, _ := NewColumn[hookLiveModel, string](table, "Value")
	count := func(want int) {
		t.Helper()
		var actual int
		if err := admin.QueryRow(ctx, "SELECT count(*) FROM "+table.info.sqlName()).Scan(&actual); err != nil || actual != want {
			t.Fatal("independent count", actual, want, err)
		}
	}
	var order []string
	var retained *WriteSession
	var hookContext *HookContext
	before := func(label string) BeforeHook[hookLiveModel] {
		return func(_ context.Context, h *HookContext, _ WriteIntent[hookLiveModel]) error {
			order = append(order, label)
			hookContext = h
			return nil
		}
	}
	after := func(label string) AfterHook[hookLiveModel] {
		return func(_ context.Context, _ *HookContext, _ WriteEvent[hookLiveModel]) error {
			order = append(order, label)
			return nil
		}
	}
	repo, _ := NewHookRepository(table, HookSet[hookLiveModel]{BeforeWrite: []BeforeHook[hookLiveModel]{before("before-write")}, BeforeCreate: []BeforeHook[hookLiveModel]{before("before-create")}, AfterCreate: []AfterHook[hookLiveModel]{after("after-create")}, AfterWrite: []AfterHook[hookLiveModel]{after("after-write")}, AfterCommit: []CommitHook[hookLiveModel]{func(dispatch context.Context, event WriteEvent[hookLiveModel]) error {
		order = append(order, "committed")
		count(1)
		requirePoolReuse(t, dispatch, pool)
		if _, err := retained.Exec(dispatch, "SELECT 1"); !errors.Is(err, ErrHookClosed) {
			t.Fatal(err)
		}
		if event.Model == nil || event.Model.ID != 1 {
			t.Fatal("insert snapshot", event)
		}
		return nil
	}}})
	err = WithHookTransaction(ctx, pool, HookTransactionOptions{}, func(s *WriteSession) error {
		retained = s
		_, err := HookInsert(ctx, s, repo, Set(id, Some(int64(1))), Set(value, Some("first")))
		return err
	})
	if err != nil || !reflect.DeepEqual(order, []string{"before-write", "before-create", "after-create", "after-write", "committed"}) {
		t.Fatal(err, order)
	}
	if _, err := hookContext.Exec(ctx, "SELECT 1"); !errors.Is(err, ErrHookClosed) {
		t.Fatal(err)
	}
	marker := errors.New("hook failure")
	failing, _ := NewHookRepository(table, HookSet[hookLiveModel]{AfterCreate: []AfterHook[hookLiveModel]{func(context.Context, *HookContext, WriteEvent[hookLiveModel]) error { return marker }}})
	err = WithHookTransaction(ctx, pool, HookTransactionOptions{}, func(s *WriteSession) error {
		_, _ = HookInsert(ctx, s, failing, Set(id, Some(int64(2))), Set(value, Some("rollback")))
		return nil
	})
	if !errors.Is(err, marker) {
		t.Fatal(err)
	}
	count(1)
	requirePoolReuse(t, ctx, pool)
	var notified []int64
	notification, _ := NewHookRepository(table, HookSet[hookLiveModel]{AfterCommit: []CommitHook[hookLiveModel]{func(_ context.Context, event WriteEvent[hookLiveModel]) error {
		notified = append(notified, event.Model.ID)
		return nil
	}}})
	err = WithHookTransaction(ctx, pool, HookTransactionOptions{}, func(s *WriteSession) error {
		if _, err := HookInsert(ctx, s, notification, Set(id, Some(int64(3))), Set(value, Some("parent"))); err != nil {
			return err
		}
		err := s.Savepoint(ctx, func(child *WriteSession) error {
			_, err := HookInsert(ctx, child, notification, Set(id, Some(int64(4))), Set(value, Some("child-rollback")))
			if err != nil {
				return err
			}
			return marker
		})
		if !errors.Is(err, marker) {
			return err
		}
		return s.Savepoint(ctx, func(child *WriteSession) error {
			_, err := HookInsert(ctx, child, notification, Set(id, Some(int64(5))), Set(value, Some("child-commit")))
			return err
		})
	})
	if err != nil || !reflect.DeepEqual(notified, []int64{3, 5}) {
		t.Fatal(err, notified)
	}
	count(3)
	dispatchFailure, _ := NewHookRepository(table, HookSet[hookLiveModel]{AfterCommit: []CommitHook[hookLiveModel]{func(context.Context, WriteEvent[hookLiveModel]) error { return marker }}})
	err = WithHookTransaction(ctx, pool, HookTransactionOptions{}, func(s *WriteSession) error {
		_, err := HookInsert(ctx, s, dispatchFailure, Set(id, Some(int64(6))), Set(value, Some("durable")))
		return err
	})
	var committed *CommittedDispatchError
	if !errors.As(err, &committed) || !committed.Committed() || !errors.Is(err, marker) {
		t.Fatal(err)
	}
	count(4)
	requirePoolReuse(t, ctx, pool)
	// Recovered hook panic still poisons the owned workflow and cannot commit.
	panicRepo, _ := NewHookRepository(table, HookSet[hookLiveModel]{AfterCreate: []AfterHook[hookLiveModel]{func(context.Context, *HookContext, WriteEvent[hookLiveModel]) error { panic(marker) }}})
	err = WithHookTransaction(ctx, pool, HookTransactionOptions{}, func(s *WriteSession) error {
		func() {
			defer func() { recover() }()
			_, _ = HookInsert(ctx, s, panicRepo, Set(id, Some(int64(7))), Set(value, Some("panic")))
		}()
		return nil
	})
	if !errors.Is(err, ErrHookWorkflow) {
		t.Fatal(err)
	}
	count(4)
	// Explicit validation failures happen before hooks and remain recoverable.
	plain, _ := NewHookRepository(table, HookSet[hookLiveModel]{})
	err = WithHookTransaction(ctx, pool, HookTransactionOptions{}, func(s *WriteSession) error {
		if _, err := HookInsert(ctx, s, plain, Set(id, Some(int64(8))), Set(id, Some(int64(9)))); err == nil {
			t.Fatal("duplicate admitted")
		}
		_, err := HookInsert(ctx, s, plain, Set(id, Some(int64(8))), Set(value, Some("recovered")))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	count(5)
	// Callback cancellation defeats Background extra-SQL attempts and returns the pool.
	canceled, cancel := context.WithCancel(ctx)
	cancelRepo, _ := NewHookRepository(table, HookSet[hookLiveModel]{BeforeCreate: []BeforeHook[hookLiveModel]{func(_ context.Context, h *HookContext, _ WriteIntent[hookLiveModel]) error {
		cancel()
		_, err := h.Exec(context.Background(), "SELECT 1")
		return err
	}}})
	err = WithHookTransaction(canceled, pool, HookTransactionOptions{}, func(s *WriteSession) error {
		_, err := HookInsert(context.Background(), s, cancelRepo, Set(id, Some(int64(9))), Set(value, Some("cancel")))
		return err
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	count(5)
	requirePoolReuse(t, ctx, pool)
}

func TestPostgresHookDispatchCommitOutcomes(t *testing.T) {
	ctx, basePool, admin, records := liveTransactionSetup(t)
	schema := strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(strings.TrimSuffix(records, ".records"), `"`), `"`), `""`, `"`)
	table, _ := NewTable[hookLiveModel](schema, "records")
	id, _ := NewColumn[hookLiveModel, int64](table, "ID")
	value, _ := NewColumn[hookLiveModel, string](table, "Value")
	dispatches := 0
	repo, _ := NewHookRepository(table, HookSet[hookLiveModel]{AfterCommit: []CommitHook[hookLiveModel]{func(context.Context, WriteEvent[hookLiveModel]) error { dispatches++; return nil }}})
	config, err := pgxpool.ParseConfig(os.Getenv("NEUTRON_ORM_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal("invalid native config")
	}
	if strings.HasPrefix(config.ConnConfig.Host, "/") {
		t.Fatal("commit proxy requires TCP")
	}
	proxy := startCommitAckProxy(t, net.JoinHostPort(config.ConnConfig.Host, strconv.Itoa(int(config.ConnConfig.Port))))
	config.ConnConfig.Host = "127.0.0.1"
	config.ConnConfig.Port = uint16(proxy.listener.Addr().(*net.TCPAddr).Port)
	config.ConnConfig.TLSConfig = nil
	config.ConnConfig.Fallbacks = nil
	config.MaxConns = 1
	config.MinConns = 0
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal("proxy pool creation failed")
	}
	t.Cleanup(pool.Close)
	err = WithHookTransaction(ctx, pool, HookTransactionOptions{}, func(s *WriteSession) error {
		_, err := HookInsert(ctx, s, repo, Set(id, Some(int64(1))), Set(value, Some("lost ack durable")))
		return err
	})
	var txErr *TransactionError
	if !errors.As(err, &txErr) || txErr.Outcome != CommitUnknown || dispatches != 0 || proxy.commits.Load() != 1 {
		t.Fatal("ambiguous commit dispatched or replayed", err, dispatches, proxy.commits.Load())
	}
	var count int
	if err := admin.QueryRow(ctx, "SELECT count(*) FROM "+table.info.sqlName()).Scan(&count); err != nil || count != 1 {
		t.Fatal("lost ack durable row missing", count, err)
	}
	requirePoolReuse(t, ctx, pool)
	// A deferred constraint fails at actual COMMIT after successful hooked statements.
	deferred := quote(schema) + `.deferred_rows`
	if _, err := admin.Exec(ctx, "CREATE TABLE "+deferred+" (id bigint UNIQUE DEFERRABLE INITIALLY DEFERRED,value text NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	deferredTable, _ := NewTable[hookLiveModel](schema, "deferred_rows")
	deferredID, _ := NewColumn[hookLiveModel, int64](deferredTable, "ID")
	deferredValue, _ := NewColumn[hookLiveModel, string](deferredTable, "Value")
	deferredRepo, _ := NewHookRepository(deferredTable, HookSet[hookLiveModel]{AfterCommit: repo.hooks.AfterCommit})
	err = WithHookTransaction(ctx, basePool, HookTransactionOptions{}, func(s *WriteSession) error {
		for range 2 {
			if _, err := HookInsert(ctx, s, deferredRepo, Set(deferredID, Some(int64(1))), Set(deferredValue, Some("rejected"))); err != nil {
				return err
			}
		}
		return nil
	})
	if !errors.As(err, &txErr) || txErr.Outcome != CommitRejected || dispatches != 0 {
		t.Fatal("rejected commit dispatched", err, dispatches)
	}
	if err := admin.QueryRow(ctx, "SELECT count(*) FROM "+deferred).Scan(&count); err != nil || count != 0 {
		t.Fatal("rejected commit durable", count, err)
	}
	requirePoolReuse(t, ctx, basePool)
	panicRepo, _ := NewHookRepository(table, HookSet[hookLiveModel]{AfterCommit: []CommitHook[hookLiveModel]{func(context.Context, WriteEvent[hookLiveModel]) error { panic("postcommit") }}})
	var caught any
	func() {
		defer func() { caught = recover() }()
		_ = WithHookTransaction(ctx, basePool, HookTransactionOptions{}, func(s *WriteSession) error {
			_, err := HookInsert(ctx, s, panicRepo, Set(id, Some(int64(2))), Set(value, Some("panic durable")))
			return err
		})
	}()
	committedPanic, ok := caught.(*CommittedHookPanic)
	if !ok || !committedPanic.Committed() {
		t.Fatal("postcommit panic outcome lost", caught)
	}
	if err := admin.QueryRow(ctx, "SELECT count(*) FROM "+table.info.sqlName()).Scan(&count); err != nil || count != 2 {
		t.Fatal("postcommit panic rolled back", count, err)
	}
	requirePoolReuse(t, ctx, basePool)
}
