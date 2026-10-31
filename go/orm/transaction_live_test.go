package orm

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func liveTransactionSetup(t *testing.T) (context.Context, *pgxpool.Pool, *pgx.Conn, string) {
	t.Helper()
	url := os.Getenv("NEUTRON_ORM_TEST_DATABASE_URL")
	if url == "" {
		if os.Getenv("NEUTRON_ORM_REQUIRE_LIVE") == "1" {
			t.Fatal("NEUTRON_ORM_TEST_DATABASE_URL required")
		}
		t.Skip("PostgreSQL transaction verification requires disposable test URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	config, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal("invalid test database config")
	}
	config.MaxConns = 1
	config.MinConns = 0
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal("test pool creation failed")
	}
	t.Cleanup(pool.Close)
	admin, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal("native oracle connection failed")
	}
	t.Cleanup(func() {
		clean, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		_ = admin.Close(clean)
	})
	schema := fmt.Sprintf("orm_tx_%d", time.Now().UnixNano())
	table := quote(schema) + `.records`
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+quote(schema)+"; CREATE TABLE "+table+" (id bigint PRIMARY KEY, value text NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		clean, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		if _, err := admin.Exec(clean, "DROP SCHEMA "+quote(schema)+" CASCADE"); err != nil {
			t.Error("owned transaction schema cleanup failed")
		}
	})
	return ctx, pool, admin, table
}

func nativeIDs(ctx context.Context, admin *pgx.Conn, table string) ([]int64, error) {
	rows, err := admin.Query(ctx, "SELECT id FROM "+table+" ORDER BY id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := []int64{}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
func scopeSingleInt(ctx context.Context, s *Scope, sql string) (int64, error) {
	rows, err := s.Query(ctx, sql)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var value int64
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return 0, err
		}
		return 0, ErrNotFound
	}
	if err := rows.Scan(&value); err != nil {
		return 0, err
	}
	if rows.Next() {
		return 0, ErrCardinality
	}
	return value, rows.Err()
}

func requirePoolReuse(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	bounded, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var value int
	if err := pool.QueryRow(bounded, "SELECT 1").Scan(&value); err != nil || value != 1 {
		t.Fatal("size-one pool not reusable", err)
	}
	if pool.Stat().AcquiredConns() != 0 {
		t.Fatal("scope retained pool connection")
	}
}

func TestPostgresOwnedTransactions(t *testing.T) {
	ctx, pool, admin, table := liveTransactionSetup(t)
	rollbackChild := errors.New("child application refusal")
	var capturedParent, capturedChild *Scope
	err := WithTransaction(ctx, pool, TransactionOptions{}, func(parent *Scope) error {
		capturedParent = parent
		if _, err := parent.Exec(ctx, "INSERT INTO "+table+" VALUES ($1,$2)", int64(1), "parent"); err != nil {
			return err
		}
		err := parent.Savepoint(ctx, func(child *Scope) error {
			capturedChild = child
			if _, err := parent.Exec(ctx, "INSERT INTO "+table+" VALUES (99,'forbidden')"); !errors.Is(err, ErrParentSuspended) {
				return fmt.Errorf("parent not suspended: %w", err)
			}
			if _, err := child.Exec(ctx, "INSERT INTO "+table+" VALUES ($1,$2)", int64(2), "child"); err != nil {
				return err
			}
			return rollbackChild
		})
		if !errors.Is(err, rollbackChild) {
			return fmt.Errorf("child outcome: %w", err)
		}
		// Unique failure aborts PostgreSQL transaction until ROLLBACK TO. The
		// savepoint helper must restore the parent, retaining SQLSTATE.
		err = parent.Savepoint(ctx, func(child *Scope) error {
			_, err := child.Exec(ctx, "INSERT INTO "+table+" VALUES ($1,$2)", int64(1), "duplicate")
			return err
		})
		var state *pgconn.PgError
		if !errors.As(err, &state) || state.Code != "23505" {
			return fmt.Errorf("unique SQLSTATE lost: %w", err)
		}
		if _, err := parent.Exec(ctx, "INSERT INTO "+table+" VALUES ($1,$2)", int64(3), "recovered"); err != nil {
			return err
		}
		count, err := scopeSingleInt(ctx, parent, "SELECT count(*) FROM "+table)
		if err != nil || count != 2 {
			return fmt.Errorf("parent savepoint state: %d %w", count, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	ids, err := nativeIDs(ctx, admin, table)
	if err != nil || !reflect.DeepEqual(ids, []int64{1, 3}) {
		t.Fatal("native savepoint state", ids, err)
	}
	for _, scope := range []*Scope{capturedParent, capturedChild} {
		if _, err := scope.Exec(ctx, "SELECT 1"); !errors.Is(err, ErrScopeClosed) {
			t.Fatal("terminal scope accepted", err)
		}
	}
	requirePoolReuse(t, ctx, pool)
	parentRefusal := errors.New("parent application refusal")
	err = WithTransaction(ctx, pool, TransactionOptions{}, func(parent *Scope) error {
		if _, err := parent.Exec(ctx, "INSERT INTO "+table+" VALUES (4,'parent rollback')"); err != nil {
			return err
		}
		if err := parent.Savepoint(ctx, func(child *Scope) error {
			_, err := child.Exec(ctx, "INSERT INTO "+table+" VALUES (5,'released child')")
			return err
		}); err != nil {
			return err
		}
		return parentRefusal
	})
	if !errors.Is(err, parentRefusal) {
		t.Fatal(err)
	}
	ids, err = nativeIDs(ctx, admin, table)
	if err != nil || !reflect.DeepEqual(ids, []int64{1, 3}) {
		t.Fatal("released child escaped outer rollback", ids, err)
	}
	panicValue := errors.New("application panic")
	func() {
		defer func() {
			if got := recover(); got != panicValue {
				t.Errorf("original panic changed: %v", got)
			}
		}()
		_ = WithTransaction(ctx, pool, TransactionOptions{}, func(s *Scope) error {
			if _, err := s.Exec(ctx, "INSERT INTO "+table+" VALUES (6,'panic')"); err != nil {
				return err
			}
			panic(panicValue)
		})
	}()
	ids, err = nativeIDs(ctx, admin, table)
	if err != nil || !reflect.DeepEqual(ids, []int64{1, 3}) {
		t.Fatal("panic write persisted", ids, err)
	}
	requirePoolReuse(t, ctx, pool)
}

func TestPostgresScopeRowsLeakAndConcurrency(t *testing.T) {
	ctx, pool, admin, table := liveTransactionSetup(t)
	err := WithTransaction(ctx, pool, TransactionOptions{}, func(s *Scope) error {
		rows, err := s.Query(ctx, "SELECT generate_series(1,3)")
		if err != nil {
			return err
		}
		if _, err := s.Exec(ctx, "SELECT 1"); !errors.Is(err, ErrConcurrentUse) {
			rows.Close()
			return errors.New("active rows allowed interleaving")
		}
		if err := s.Savepoint(ctx, func(*Scope) error { return nil }); !errors.Is(err, ErrConcurrentUse) {
			rows.Close()
			return errors.New("active rows allowed savepoint")
		}
		if rows.Conn() != nil {
			rows.Close()
			return errors.New("raw connection escape exposed")
		}
		rows.Close()
		return s.Savepoint(ctx, func(child *Scope) error {
			rows, err := child.Query(ctx, "SELECT generate_series(1,3)")
			if err != nil {
				return err
			}
			defer rows.Close()
			if _, err := s.Exec(ctx, "SELECT 1"); !errors.Is(err, ErrParentSuspended) {
				return errors.New("parent interleaved child")
			}
			if _, err := child.Exec(ctx, "SELECT 1"); !errors.Is(err, ErrConcurrentUse) {
				return errors.New("child interleaved rows")
			}
			return nil
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	requirePoolReuse(t, ctx, pool)
	err = WithTransaction(ctx, pool, TransactionOptions{CleanupTimeout: time.Second}, func(s *Scope) error {
		if _, err := s.Exec(ctx, "INSERT INTO "+table+" VALUES (1,'must rollback leaked rows')"); err != nil {
			return err
		}
		_, err := s.Query(ctx, "SELECT generate_series(1,3)")
		return err // deliberately leaked
	})
	if !errors.Is(err, ErrScopeLeak) {
		t.Fatal("leaked rows did not refuse commit", err)
	}
	ids, err := nativeIDs(ctx, admin, table)
	if err != nil || len(ids) != 0 {
		t.Fatal("leak committed data", ids, err)
	}
	requirePoolReuse(t, ctx, pool)
}

func TestPostgresTransactionCancellationAndServerTimeout(t *testing.T) {
	ctx, pool, admin, table := liveTransactionSetup(t)
	err := WithTransaction(ctx, pool, TransactionOptions{}, func(s *Scope) error {
		queryCtx, cancel := context.WithTimeout(ctx, 25*time.Millisecond)
		defer cancel()
		_, err := s.Exec(queryCtx, "SELECT pg_sleep(0.2)")
		return err
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("query deadline lost", err)
	}
	requirePoolReuse(t, ctx, pool)
	beforeCommit, cancel := context.WithCancel(ctx)
	err = WithTransaction(beforeCommit, pool, TransactionOptions{}, func(s *Scope) error {
		if _, err := s.Exec(beforeCommit, "INSERT INTO "+table+" VALUES (1,'cancel before commit')"); err != nil {
			return err
		}
		cancel()
		return nil
	})
	var txErr *TransactionError
	if !errors.As(err, &txErr) || txErr.Outcome != CommitNotAttempted || !errors.Is(err, context.Canceled) {
		t.Fatal("precommit cancellation misclassified", err)
	}
	ids, err := nativeIDs(ctx, admin, table)
	if err != nil || len(ids) != 0 {
		t.Fatal("precommit cancel persisted", ids, err)
	}
	requirePoolReuse(t, ctx, pool)
	err = WithTransaction(ctx, pool, TransactionOptions{}, func(parent *Scope) error {
		err := parent.Savepoint(ctx, func(child *Scope) error {
			rows, err := child.Query(ctx, "SELECT set_config('statement_timeout','30ms',true)")
			if err != nil {
				return err
			}
			for rows.Next() {
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return err
			}
			_, err = child.Exec(ctx, "SELECT pg_sleep(0.2)")
			return err
		})
		var p *pgconn.PgError
		if !errors.As(err, &p) || p.Code != "57014" || errors.Is(err, context.Canceled) {
			return fmt.Errorf("server timeout misclassified: %w", err)
		}
		_, err = parent.Exec(ctx, "INSERT INTO "+table+" VALUES (2,'server timeout recovered')")
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	ids, err = nativeIDs(ctx, admin, table)
	if err != nil || !reflect.DeepEqual(ids, []int64{2}) {
		t.Fatal("savepoint server-timeout recovery", ids, err)
	}
	requirePoolReuse(t, ctx, pool)
}

func TestPostgresReadOnlyDeferrableAndFailedOuterRecovery(t *testing.T) {
	ctx, pool, admin, table := liveTransactionSetup(t)
	err := WithTransaction(ctx, pool, TransactionOptions{Isolation: pgx.Serializable, Access: pgx.ReadOnly, Deferrable: pgx.Deferrable}, func(s *Scope) error {
		rows, err := s.Query(ctx, "SELECT current_setting('transaction_isolation'),current_setting('transaction_read_only'),current_setting('transaction_deferrable')")
		if err != nil {
			return err
		}
		var isolation, access, deferMode string
		if !rows.Next() {
			rows.Close()
			return errors.New("transaction settings missing")
		}
		err = rows.Scan(&isolation, &access, &deferMode)
		rows.Close()
		if err != nil {
			return err
		}
		if isolation != "serializable" || access != "on" || deferMode != "on" {
			return errors.New("native transaction settings mismatch")
		}
		_, err = s.Exec(ctx, "INSERT INTO "+table+" VALUES (1,'readonly must refuse')")
		return err
	})
	var state *pgconn.PgError
	if !errors.As(err, &state) || state.Code != "25006" {
		t.Fatal("read-only SQLSTATE lost", err)
	}
	requirePoolReuse(t, ctx, pool)
	// A caller that swallows a root statement error must not receive commit
	// success: PostgreSQL answers COMMIT with ROLLBACK for failed outer state.
	err = WithTransaction(ctx, pool, TransactionOptions{}, func(s *Scope) error {
		if _, err := s.Exec(ctx, "INSERT INTO "+table+" VALUES (1,'first')"); err != nil {
			return err
		}
		_, _ = s.Exec(ctx, "INSERT INTO "+table+" VALUES (1,'duplicate ignored')")
		return nil
	})
	var txErr *TransactionError
	if !errors.As(err, &txErr) || txErr.Outcome != CommitRejected || !errors.Is(err, pgx.ErrTxCommitRollback) {
		t.Fatal("failed outer commit reported success", err)
	}
	ids, err := nativeIDs(ctx, admin, table)
	if err != nil || len(ids) != 0 {
		t.Fatal("failed outer persisted", ids, err)
	}
	requirePoolReuse(t, ctx, pool)
}
