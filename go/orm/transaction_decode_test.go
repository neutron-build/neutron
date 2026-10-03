package orm

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

type decodeFailureRows struct {
	fixtureRows
	failure error
	next    bool
}

func (r *decodeFailureRows) Next() bool {
	if r.next || r.closed {
		return false
	}
	r.next = true
	return true
}
func (r *decodeFailureRows) Scan(...any) error      { return r.failure }
func (r *decodeFailureRows) Values() ([]any, error) { return nil, r.failure }

type decodeFailureDriver struct {
	lifecycleDriver
	failure error
}

func (d *decodeFailureDriver) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return &decodeFailureRows{failure: d.failure}, nil
}

func TestScopeDecodeFailureCannotCommitWhenSwallowed(t *testing.T) {
	for _, values := range []bool{false, true} {
		failure := errors.New("fixture codec failed")
		driver := &decodeFailureDriver{failure: failure}
		owner, scope, releases, discards := fixtureOwner(context.Background(), driver)
		rows, err := scope.Query(context.Background(), "SELECT 1")
		if err != nil || !rows.Next() {
			t.Fatal("fixture query", err)
		}
		if values {
			_, err = rows.Values()
		} else {
			var value int64
			err = rows.Scan(&value)
		}
		if !errors.Is(err, failure) {
			t.Fatal("decoder error lost", err)
		}
		rows.Close()
		if _, err := scope.Exec(context.Background(), "SELECT 2"); !errors.Is(err, ErrScopeDecode) || !errors.Is(err, failure) {
			t.Fatal("failed scope admitted further operation", err)
		}
		err = owner.finishRoot(scope, nil)
		if !errors.Is(err, ErrScopeDecode) || !errors.Is(err, failure) || driver.commits != 0 || driver.rollbacks != 1 || *releases != 1 || *discards != 0 {
			t.Fatal("swallowed codec error committed/discarded clean connection", err)
		}
	}
}
func TestChildDecodeFailureRecoveredOnlyBySavepointRollback(t *testing.T) {
	failure := errors.New("fixture codec failed")
	driver := &decodeFailureDriver{failure: failure}
	owner, parent, releases, discards := fixtureOwner(context.Background(), driver)
	err := parent.Savepoint(context.Background(), func(child *Scope) error {
		rows, err := child.Query(context.Background(), "SELECT 1")
		if err != nil || !rows.Next() {
			return errors.New("fixture child query")
		}
		var value int64
		err = rows.Scan(&value)
		rows.Close()
		if !errors.Is(err, failure) {
			return errors.New("fixture child decode did not fail")
		}
		return nil // Even swallowing the decoder error must roll back the child.
	})
	if !errors.Is(err, ErrScopeDecode) || !errors.Is(err, failure) || owner.broken != nil {
		t.Fatal("child failure not locally recovered", err)
	}
	rolledBack := false
	for _, statement := range driver.statements {
		if strings.HasPrefix(statement, "ROLLBACK TO SAVEPOINT") {
			rolledBack = true
		}
	}
	if !rolledBack {
		t.Fatal("child codec failure released without rollback")
	}
	if _, err := parent.Exec(context.Background(), "SELECT 2"); err != nil {
		t.Fatal("recovered parent refused", err)
	}
	if err := owner.finishRoot(parent, nil); err != nil || driver.commits != 1 || *releases != 1 || *discards != 0 {
		t.Fatal("recovered parent could not commit", err)
	}
}
