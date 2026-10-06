package orm

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func failNativeDecimalDecode(ctx context.Context, scope *Scope) error {
	rows, err := scope.Query(ctx, "SELECT 'NaN'::numeric")
	if err != nil {
		return err
	}
	defer rows.Close()
	if !rows.Next() {
		return fmt.Errorf("native decode row absent: %w", rows.Err())
	}
	var value Decimal
	return rows.Scan(&value)
}

func TestPostgresScopeDecodeFailureRollbackAndChildRecovery(t *testing.T) {
	ctx, pool, admin, table := liveTransactionSetup(t)
	err := WithTransaction(ctx, pool, TransactionOptions{}, func(scope *Scope) error {
		if _, err := scope.Exec(ctx, "INSERT INTO "+table+" VALUES (1,'must roll back')"); err != nil {
			return err
		}
		if err := failNativeDecimalDecode(ctx, scope); !errors.Is(err, ErrScalarValue) {
			return fmt.Errorf("expected native nonfinite refusal: %w", err)
		}
		if _, err := scope.Exec(ctx, "INSERT INTO "+table+" VALUES (2,'refused')"); !errors.Is(err, ErrScopeDecode) {
			return fmt.Errorf("poisoned scope admitted work: %w", err)
		}
		return nil // Swallowing a codec error cannot make the owned scope commit.
	})
	var txErr *TransactionError
	if !errors.Is(err, ErrScopeDecode) || !errors.Is(err, ErrScalarValue) || !errors.As(err, &txErr) || txErr.Outcome != CommitNotAttempted {
		t.Fatal("swallowed native decode failure committed", err)
	}
	ids, err := nativeIDs(ctx, admin, table)
	if err != nil || len(ids) != 0 {
		t.Fatal("root codec failure did not roll back native writes", ids, err)
	}
	requirePoolReuse(t, ctx, pool)
	err = WithTransaction(ctx, pool, TransactionOptions{}, func(parent *Scope) error {
		if _, err := parent.Exec(ctx, "INSERT INTO "+table+" VALUES (1,'parent before')"); err != nil {
			return err
		}
		err := parent.Savepoint(ctx, func(child *Scope) error {
			if _, err := child.Exec(ctx, "INSERT INTO "+table+" VALUES (2,'child must roll back')"); err != nil {
				return err
			}
			if err := failNativeDecimalDecode(ctx, child); !errors.Is(err, ErrScalarValue) {
				return fmt.Errorf("expected child native decode refusal: %w", err)
			}
			return nil
		})
		if !errors.Is(err, ErrScopeDecode) || !errors.Is(err, ErrScalarValue) {
			return fmt.Errorf("child failure not surfaced/recovered: %w", err)
		}
		_, err = parent.Exec(ctx, "INSERT INTO "+table+" VALUES (3,'parent after')")
		return err
	})
	if err != nil {
		t.Fatal("parent after child codec rollback", err)
	}
	ids, err = nativeIDs(ctx, admin, table)
	if err != nil || len(ids) != 2 || ids[0] != 1 || ids[1] != 3 {
		t.Fatal("native child rollback/parent commit oracle", ids, err)
	}
	requirePoolReuse(t, ctx, pool)
	// No database operation occurs for an invalid typed query, so validation
	// errors do not poison an otherwise usable scope.
	type record struct {
		ID    int64  `db:"id"`
		Value string `db:"value"`
	}
	err = WithTransaction(ctx, pool, TransactionOptions{}, func(scope *Scope) error {
		metadata, err := NewTable[record]("owned validation", "records")
		if err != nil {
			return err
		}
		column, err := NewColumn[record, int64](metadata, "ID")
		if err != nil {
			return err
		}
		if _, err := SelectColumn(ctx, scope, column, Query[record]{}.Where(Predicate[record]{})); err == nil {
			return errors.New("invalid query accepted")
		}
		_, err = scope.Exec(ctx, "INSERT INTO "+table+" VALUES (4,'validation remains usable')")
		return err
	})
	if err != nil {
		t.Fatal("preexecution validation poisoned scope", err)
	}
	ids, err = nativeIDs(ctx, admin, table)
	if err != nil || len(ids) != 3 || ids[2] != 4 {
		t.Fatal("validation continuation native oracle", ids, err)
	}
}
