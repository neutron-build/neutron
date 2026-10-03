package orm

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestChildScopeCancellationCannotBeBypassedByBackgroundContext(t *testing.T) {
	d := &lifecycleDriver{}
	o, parent, _, _ := fixtureOwner(context.Background(), d)
	ctx, cancel := context.WithCancel(context.Background())
	err := parent.Savepoint(ctx, func(child *Scope) error {
		cancel()
		if _, err := child.Exec(context.Background(), "SELECT 1"); !errors.Is(err, context.Canceled) {
			return errors.New("child cancellation bypassed")
		}
		return nil // finalizer must still honor the canceled child context
	})
	if !errors.Is(err, context.Canceled) || !strings.Contains(strings.Join(d.statements, ";"), "ROLLBACK TO SAVEPOINT") {
		t.Fatal(err, d.statements)
	}
	if _, err := parent.Exec(context.Background(), "SELECT 1"); err != nil {
		t.Fatal("parent not recovered after child cancellation", err)
	}
	if err := o.finishRoot(parent, nil); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresChildContextCancellation(t *testing.T) {
	ctx, pool, admin, table := liveTransactionSetup(t)
	err := WithTransaction(ctx, pool, TransactionOptions{}, func(parent *Scope) error {
		childCtx, cancel := context.WithCancel(ctx)
		err := parent.Savepoint(childCtx, func(child *Scope) error {
			if _, err := child.Exec(ctx, "INSERT INTO "+table+" VALUES (1,'child must rollback')"); err != nil {
				return err
			}
			cancel()
			if _, err := child.Exec(context.Background(), "INSERT INTO "+table+" VALUES (2,'background bypass')"); !errors.Is(err, context.Canceled) {
				return errors.New("canceled child accepted background operation")
			}
			return nil
		})
		if !errors.Is(err, context.Canceled) {
			return errors.New("child finalizer ignored cancellation")
		}
		_, err = parent.Exec(ctx, "INSERT INTO "+table+" VALUES (3,'parent recovered')")
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	ids, err := nativeIDs(ctx, admin, table)
	if err != nil || len(ids) != 1 || ids[0] != 3 {
		t.Fatal("child context rollback native state", ids, err)
	}
	requirePoolReuse(t, ctx, pool)
}
